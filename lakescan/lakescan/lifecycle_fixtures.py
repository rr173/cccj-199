"""Runtime-generated drill table for the retention/reclamation acceptance.

The graph (``parent`` links) is::

    R <- M1 <- M2
    R <- L
    R <- X          (X is an unreferenced leaf)

A branch reference points at M2 retaining two generations; a bookmark points
at L.  The fixture deliberately exercises physical-path sharing:

* ``dl_shared.parquet`` is an ACTIVE delete file of both L and X (two
  snapshots, one physical path);
* ``data_x_only.parquet`` and ``data_l_only.parquet`` are byte-identical data
  files at *different paths* -- digest equality never merges path identity, so
  retiring X may remove only the X-only path;
* R owns two data files so a batch_size=1 reader pauses between batches while
  remaining fully protected.

Nothing large is ever written or committed: all parquet files hold a handful
of rows.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .table import FORMAT, sha256_of

S_DATA = 1
S_POS = 90
S_EQ_K = 91

SCHEMAS = [
    {
        "schema_id": S_DATA,
        "fields": [
            {"field_id": 1, "name": "row_uid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "k", "type": {"kind": "int32"}},
        ],
    },
    {
        "schema_id": S_POS,
        "fields": [
            {"field_id": 1000, "name": "target_file_id",
             "type": {"kind": "utf8"}, "required": True},
            {"field_id": 1001, "name": "row_position", "type": {"kind": "int64"},
             "required": True},
        ],
    },
    {"schema_id": S_EQ_K,
     "fields": [{"field_id": 2, "name": "k", "type": {"kind": "int32"}}]},
]

_PART = {}  # empty partition everywhere in this drill


def _data_arrow():
    return pa.schema([("row_uid", pa.string()), ("k", pa.int32())])


def build_lifecycle_table(root) -> Path:
    root = Path(root)
    files_dir = root / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    def write_data(name, rows):
        rel = f"files/{name}.parquet"
        pq.write_table(
            pa.table(
                {"row_uid": [r[0] for r in rows],
                 "k": pa.array([r[1] for r in rows], pa.int32())},
                schema=_data_arrow(),
            ),
            root / rel,
        )
        return rel, sha256_of(root / rel)

    def write_eq(name, keys):
        rel = f"files/{name}.parquet"
        pq.write_table(
            pa.table({"k": pa.array(keys, pa.int32())},
                     schema=pa.schema([("k", pa.int32())])),
            root / rel,
        )
        return rel, sha256_of(root / rel)

    def write_pos(name, targets):
        rel = f"files/{name}.parquet"
        pq.write_table(
            pa.table({
                "target_file_id": [t[0] for t in targets],
                "row_position": pa.array([t[1] for t in targets], pa.int64()),
            }, schema=pa.schema([
                ("target_file_id", pa.string()), ("row_position", pa.int64())])),
            root / rel,
        )
        return rel, sha256_of(root / rel)

    # ---- R (two data files, one row group per row -> real batch boundaries)
    r1_rel, r1_sha = write_data("r1", [("r1-a", 1), ("r1-b", 2)])
    r2_rel, r2_sha = write_data("r2", [("r2-a", 3)])

    # ---- M1, M2 (main branch) ---------------------------------------------
    m1_rel, m1_sha = write_data("m1", [("m1-a", 10)])
    m2_rel, m2_sha = write_data("m2", [("m2-a", 20)])
    # an active POS delete of M2: valid target, removes row 0 of m2
    pos_m2_rel, pos_m2_sha = write_pos("pos_m2", [("m2", 0)])

    # ---- L (bookmark) ------------------------------------------------------
    l_rel, l_sha = write_data("l_only", [("l-a", 30), ("l-b", 31)])

    # ---- X (unreferenced leaf) --------------------------------------------
    x_rel, x_sha = write_data("data_x_only", [("x-a", 40), ("x-b", 41)])
    # byte-identical twin at a DIFFERENT path, used only by L
    twin = root / "files" / "data_l_only.parquet"
    pa.parquet.write_table(
        pa.table({"row_uid": ["x-a", "x-b"], "k": pa.array([40, 41], pa.int32())},
                 schema=_data_arrow()),
        twin,
    )
    twin_rel, twin_sha = "files/data_l_only.parquet", sha256_of(twin)
    assert twin_rel != x_rel and twin_sha == x_sha  # same bytes, distinct paths

    # ---- shared active delete file: EQ_DELETE referenced by L and X -------
    shared_rel, shared_sha = write_eq("dl_shared", [40])  # deletes k=40 rows

    def ent(file_id, kind, rel, sha, seq, schema_id, **kw):
        e = {
            "file_id": file_id, "kind": kind, "path": rel, "sha256": sha,
            "partition": dict(_PART), "data_seq": seq, "file_seq": seq,
            "schema_id": schema_id,
        }
        e.update(kw)
        return e

    snapshots = [
        {"snapshot_id": "R", "parent": None, "active_files": [
            ent("r1", "DATA", r1_rel, r1_sha, 1, S_DATA),
            ent("r2", "DATA", r2_rel, r2_sha, 1, S_DATA),
        ]},
        {"snapshot_id": "M1", "parent": "R", "active_files": [
            ent("r1", "DATA", r1_rel, r1_sha, 1, S_DATA),
            ent("r2", "DATA", r2_rel, r2_sha, 1, S_DATA),
            ent("m1", "DATA", m1_rel, m1_sha, 2, S_DATA),
        ]},
        {"snapshot_id": "M2", "parent": "M1", "active_files": [
            ent("r1", "DATA", r1_rel, r1_sha, 1, S_DATA),
            ent("r2", "DATA", r2_rel, r2_sha, 1, S_DATA),
            ent("m1", "DATA", m1_rel, m1_sha, 2, S_DATA),
            ent("m2", "DATA", m2_rel, m2_sha, 3, S_DATA),
            ent("pos_m2", "POS_DELETE", pos_m2_rel, pos_m2_sha, 4, S_POS),
        ]},
        {"snapshot_id": "L", "parent": "R", "active_files": [
            ent("r1", "DATA", r1_rel, r1_sha, 1, S_DATA),
            ent("r2", "DATA", r2_rel, r2_sha, 1, S_DATA),
            ent("l", "DATA", l_rel, l_sha, 2, S_DATA),
            ent("l_twin", "DATA", twin_rel, twin_sha, 2, S_DATA),
            ent("dl_shared", "EQ_DELETE", shared_rel, shared_sha, 5, S_EQ_K,
                eq_field_ids=[2]),
        ]},
        {"snapshot_id": "X", "parent": "R", "active_files": [
            ent("r1", "DATA", r1_rel, r1_sha, 1, S_DATA),
            ent("r2", "DATA", r2_rel, r2_sha, 1, S_DATA),
            ent("x", "DATA", x_rel, x_sha, 2, S_DATA),
            ent("dl_shared", "EQ_DELETE", shared_rel, shared_sha, 5, S_EQ_K,
                eq_field_ids=[2]),
        ]},
    ]

    table_json = {
        "format": FORMAT,
        "table_id": "lifecycle-drill",
        "schemas": SCHEMAS,
        "snapshots": snapshots,
    }
    with open(root / "table.json", "w", encoding="utf-8") as fh:
        json.dump(table_json, fh, indent=2)
    return root


# Hand-derived expectations used by the acceptance suite.  These are written
# from the fixture above, never reverse-engineered from the reclaimer output.
EXPECTED = {
    "graph": {"M2": ["M2", "M1"], "L": ["L"]},
    "refs": {
        "branches": {"main": {"target": "M2", "retain_generations": 2}},
        "bookmarks": {"bm_l": {"target": "L"}},
    },
    # The policy-only view: nothing names R or X.  A live R reader does not
    # rewrite the plan; it makes apply refuse the plan (PlanBlockedError).
    "first_plan": {
        "retained": {"M2", "M1", "L"},
        "retire": ["R", "X"],
        # while R is readable its r1/r2 paths stay protected; only the
        # X-exclusive physical path is actually deletable
        "delete_paths": ["files/data_x_only.parquet"],
        "reader_blocked_retire": ["R"],
        "protected_paths": {
            "files/dl_shared.parquet",       # shared active delete file
            "files/data_l_only.parquet",     # L-only twin path
            "files/r1.parquet", "files/r2.parquet",
            "files/m1.parquet", "files/m2.parquet",
            "files/pos_m2.parquet",
            "files/l_only.parquet",
        },
    },
    # once the R reader exits, the same policy retires R; X stays retired.
    "second_plan": {
        "retire": ["R"],
        "delete_paths": ["files/r1.parquet", "files/r2.parquet"],
    },
    # visible rows (file_id, row_position, row_uid) after all deletes applied
    "rows": {
        "R": [("r1", 0, "r1-a"), ("r1", 1, "r1-b"), ("r2", 0, "r2-a")],
        "M2": [("r1", 0, "r1-a"), ("r1", 1, "r1-b"), ("r2", 0, "r2-a"),
               ("m1", 0, "m1-a")],  # m2 row 0 removed by pos_m2
        "L": [
            ("r1", 0, "r1-a"), ("r1", 1, "r1-b"), ("r2", 0, "r2-a"),
            ("l", 0, "l-a"), ("l", 1, "l-b"),
            # l_twin row x-a (k=40) removed by dl_shared; x-b survives
            ("l_twin", 1, "x-b"),
        ],
        "X_before": [
            ("r1", 0, "r1-a"), ("r1", 1, "r1-b"), ("r2", 0, "r2-a"),
            # x-a (k=40) removed by the shared EQ delete; x-b survives
            ("x", 1, "x-b"),
        ],
    },
}
