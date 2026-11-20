"""Public entry point of the bitemporal fact attribution task.

The task takes a visible *fact* snapshot (orders) and a visible *dimension*
snapshot (rule revision records) from two local lake-table directories,
applies the as-of knowledge + business-time attribution rules and delivers a
single per-fact Parquet file plus a manifest into a brand-new output
directory.

Read-protection ordering (managed tables)
-----------------------------------------

* when both snapshots live in the same table directory, one read guard is
  acquired for both snapshots together, before any data or delete file of
  either snapshot is opened;
* when they live in two directories, each table's guard is acquired in turn
  and both are held before the first input file of either side is opened;
* every exit path (completion, validation failure, read failure, injected
  test failure) releases the claims; a killed process is covered by the
  stale-token sweep in the lifecycle layer.

Failure semantics
-----------------

All visible input is validated -- including rules no fact can ever match --
and any contract, mapping, revision, duplicate fact id, digest or lifecycle
error aborts the whole task.  The destination directory is created last, by
an atomic rename of a fully written staging directory; an already existing
output directory is never modified and no partial result is ever published.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from .attribution_config import AttributionConfig
from .attribution_engine import AttributionEngine
from .attribution_store import (
    AttributionStore,
    extract_dimensions,
    extract_facts,
    validate_side_schemas,
)
from .attribution_writer import RunStats, OutputSchema, write_output
from .errors import AttributionError
from .lifecycle.state import canonical_manifest_bytes
from .table import TableContract


@dataclass
class AttributionResult:
    output_dir: Path
    data_file: Path
    manifest_file: Path
    stats: RunStats
    fact_rows: int
    dimension_rows: int
    work_dir: Optional[Path]
    """Scratch directory when one was supplied explicitly (kept for
    inspection); ``None`` for auto-created scratch, which is removed."""


class FactAttributor:
    """Run the attribution task described by a mapping configuration."""

    def __init__(self, config, work_dir: Optional[str] = None,
                 batch_size: Optional[int] = None):
        if isinstance(config, (str, Path)):
            config = AttributionConfig.load(config)
        elif isinstance(config, dict):
            config = AttributionConfig.parse(config)
        if not isinstance(config, AttributionConfig):
            raise AttributionError(
                "config must be a path, a mapping dict or an "
                "AttributionConfig object"
            )
        self.cfg = config
        self._work_dir_override = work_dir or config.work_dir
        if batch_size is not None:
            if not isinstance(batch_size, int) or batch_size <= 0:
                raise AttributionError("batch_size must be a positive int")
            self.batch_size = batch_size
        else:
            self.batch_size = config.batch_size

    def attribute(self, after_guards: Optional[Callable[[], None]] = None
                  ) -> AttributionResult:
        cfg = self.cfg
        # Refuse an occupied destination before doing any work at all.
        dest = Path(cfg.output_dir)
        if dest.exists():
            from .errors import AttributionOutputError

            raise AttributionOutputError(
                f"output directory already exists; one-shot delivery "
                f"refuses to overwrite it: {dest}"
            )

        fact_root = Path(cfg.fact.table)
        dim_root = Path(cfg.dimension.table)
        fact_contract = TableContract.load(fact_root)
        dim_contract = TableContract.load(dim_root)
        fact_contract.snapshot(cfg.fact.snapshot)
        dim_contract.snapshot(cfg.dimension.snapshot)
        validate_side_schemas(cfg, fact_contract, dim_contract)

        owns_work_dir = self._work_dir_override is None
        work_dir = Path(
            self._work_dir_override
            or tempfile.mkdtemp(prefix="lakescan-attribute-")
        )
        work_dir.mkdir(parents=True, exist_ok=True)

        from .lifecycle.reader import read_preflight

        same_root = fact_root.resolve() == dim_root.resolve()
        fact_guard = dim_guard = None
        store: Optional[AttributionStore] = None
        try:
            # Both protections are in place before either side opens a file.
            if same_root:
                fact_guard = read_preflight(
                    fact_root,
                    [cfg.fact.snapshot, cfg.dimension.snapshot],
                    reason="attribute",
                )
                dim_guard = fact_guard
            else:
                fact_guard = read_preflight(
                    fact_root, [cfg.fact.snapshot], reason="attribute-fact"
                )
                dim_guard = read_preflight(
                    dim_root, [cfg.dimension.snapshot],
                    reason="attribute-dimension",
                )
            if after_guards is not None:
                after_guards()

            store = AttributionStore(str(work_dir / "attribution.sqlite"))
            fact_rows = extract_facts(
                store, fact_contract, cfg.fact,
                str(work_dir / "fact_scan"), self.batch_size, fact_guard,
            )
            # Deterministic mid-run failure injection for recovery tests:
            # simulates a read failure after one whole side was streamed.
            if os.environ.get("LAKESCAN_ATTRIB_INJECT_FAIL") == "after_facts":
                raise AttributionError(
                    "injected read failure after fact extraction"
                )
            dim_rows = extract_dimensions(
                store, dim_contract, cfg.dimension,
                str(work_dir / "dim_scan"), self.batch_size, dim_guard,
            )

            engine = AttributionEngine(store)
            output_schema = OutputSchema(
                cfg, fact_contract.schema(cfg.fact.schema_id),
                dim_contract.schema(cfg.dimension.schema_id),
            )
            stats = RunStats()
            manifest_extra = {
                "inputs": {
                    "fact": {
                        "table": str(fact_root),
                        "snapshot": cfg.fact.snapshot,
                        "schema_id": cfg.fact.schema_id,
                        "visible_rows": fact_rows,
                    },
                    "dimension": {
                        "table": str(dim_root),
                        "snapshot": cfg.dimension.snapshot,
                        "schema_id": cfg.dimension.schema_id,
                        "visible_rows": dim_rows,
                    },
                },
                "source_manifest_digests": {
                    "fact": _manifest_digest(fact_root),
                    "dimension": _manifest_digest(dim_root),
                },
                "mapping": _config_json(cfg),
            }
            out_dir = write_output(
                cfg, output_schema, engine.decisions(), stats, manifest_extra
            )
            return AttributionResult(
                output_dir=out_dir,
                data_file=out_dir / "attribution.parquet",
                manifest_file=out_dir / "manifest.json",
                stats=stats,
                fact_rows=fact_rows,
                dimension_rows=dim_rows,
                work_dir=work_dir if self._work_dir_override is not None
                else None,
            )
        finally:
            if store is not None:
                store.close()
            # close each distinct guard exactly once (ReadGuard.close is
            # idempotent, but the same-root case holds one shared token)
            guards = []
            if fact_guard is not None:
                guards.append(fact_guard)
            if dim_guard is not None and dim_guard is not fact_guard:
                guards.append(dim_guard)
            for guard in guards:
                guard.close()
            if owns_work_dir:
                shutil.rmtree(work_dir, ignore_errors=True)


def _manifest_digest(root: Path) -> dict:
    with open(root / "table.json", "rb") as fh:
        raw_bytes = fh.read()
    raw = json.loads(raw_bytes.decode("utf-8"))
    return {
        "table_json_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "canonical_manifest_sha256": hashlib.sha256(
            canonical_manifest_bytes(raw)).hexdigest(),
    }


def _config_json(cfg: AttributionConfig) -> dict:
    return {
        "fact": _side_json(cfg.fact),
        "dimension": _side_json(cfg.dimension),
        "output_dir": cfg.output_dir,
        "batch_size": cfg.batch_size,
    }


def _side_json(side) -> dict:
    return {
        "table": side.table,
        "snapshot": side.snapshot,
        "schema_id": side.schema_id,
        "roles": dict(side.roles),
        "payload": [
            {"field_id": c.field_id, "name": c.name} for c in side.payload
        ],
    }


def attribute_facts(config, work_dir: Optional[str] = None,
                    batch_size: Optional[int] = None,
                    after_guards: Optional[Callable[[], None]] = None
                    ) -> AttributionResult:
    """Convenience wrapper around :class:`FactAttributor`."""
    return FactAttributor(
        config, work_dir=work_dir, batch_size=batch_size
    ).attribute(after_guards=after_guards)
