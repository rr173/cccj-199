"""Runtime-generated fixtures for the bitemporal attribution task.

Two independent lake-table directories are generated on demand (the
repository carries no data artifacts):

* the *fact* table holds order snapshots (one snapshot per scenario);
* the *dimension* table holds rule-revision snapshots, including a snapshot
  that lake-deletes the rule-A revision published at time 8.

The core hand-derived story (entity ``K``; all business-time facts share
one entity key so every timing rule is exercised on the same key):

  rule A  priority 5  put@2 [0,100); put@8 [20,100); withdraw@12
  rule B  priority 4  put@4 [0,50)
  rule C  priority 4  put@7 [40,80)
  rule D  priority 4  put@6 [40,90)   (extra tie rule; makes the candidate
                                       total 3 at (bt=40, kc=12) while only
                                       two evidences are reported)

Expected decisions (all entity K unless noted):

  bt=10 kc=7   -> matched A@2        (B not yet published)
  bt=10 kc=8   -> matched B@4        (A's revised interval starts at 20)
  bt=40 kc=8   -> matched A@8        (revised A, highest priority)
  bt=40 kc=12  -> ambiguous B,C,D    count=3, evidences B then C (byte order)
  bt=50 kc=12  -> matched C          (B's interval is [0,50), 50 excluded;
                                      A withdrawn; D covers 50 too but C
                                      outranks? no -- both are priority 4,
                                      so the design uses D=[40,50) instead)
  bt=80 kc=12  -> unmatched
  entity J     -> never matched by K rules (separate join domain)

The intervals are chosen so each expectation above is exact; see
:data:`EXPECTED_DECISIONS`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .table import FORMAT, sha256_of

# ---------------------------------------------------------------------------
# schema ids
# ---------------------------------------------------------------------------

FACT_SCHEMA = 100
FACT_SCHEMA_RENAMED = 101
DIM_SCHEMA = 200
DIM_SCHEMA_RENAMED = 201
POS_SCHEMA = 900
EQ_SCHEMA = 901

FACT_SCHEMAS = [
    {
        "schema_id": FACT_SCHEMA,
        "fields": [
            {"field_id": 1, "name": "fact_id", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "entity_key", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 3, "name": "business_time", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 4, "name": "knowledge_cutoff",
             "type": {"kind": "int64"}, "required": True},
            {"field_id": 5, "name": "amount",
             "type": {"kind": "decimal", "precision": 12, "scale": 2}},
            {"field_id": 6, "name": "note", "type": {"kind": "utf8"}},
        ],
    },
    {
        # field renames with unchanged field ids; field 6 renamed and a new
        # field 7 reuses the physical name "note" with a NEW identity.
        "schema_id": FACT_SCHEMA_RENAMED,
        "fields": [
            {"field_id": 1, "name": "fact_uuid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 2, "name": "entity", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 3, "name": "biz_ts", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 4, "name": "as_of_ts", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 5, "name": "amount",
             "type": {"kind": "decimal", "precision": 12, "scale": 2}},
            {"field_id": 6, "name": "remark", "type": {"kind": "utf8"}},
            {"field_id": 7, "name": "note", "type": {"kind": "utf8"}},
        ],
    },
]

DIM_SCHEMAS = [
    {
        "schema_id": DIM_SCHEMA,
        "fields": [
            {"field_id": 11, "name": "entity_key", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 12, "name": "rule_id", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 13, "name": "published_at", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 14, "name": "operation", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 15, "name": "valid_from", "type": {"kind": "int64"}},
            {"field_id": 16, "name": "valid_to", "type": {"kind": "int64"}},
            {"field_id": 17, "name": "priority", "type": {"kind": "int64"}},
            {"field_id": 18, "name": "settlement_group",
             "type": {"kind": "utf8"}},
            {"field_id": 19, "name": "rate_bps", "type": {"kind": "int64"}},
        ],
    },
    {
        # renamed physical columns with the same field ids
        "schema_id": DIM_SCHEMA_RENAMED,
        "fields": [
            {"field_id": 11, "name": "ek", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 12, "name": "rid", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 13, "name": "pub_ts", "type": {"kind": "int64"},
             "required": True},
            {"field_id": 14, "name": "op", "type": {"kind": "utf8"},
             "required": True},
            {"field_id": 15, "name": "vstart", "type": {"kind": "int64"}},
            {"field_id": 16, "name": "vend", "type": {"kind": "int64"}},
            {"field_id": 17, "name": "prio", "type": {"kind": "int64"}},
            {"field_id": 18, "name": "grp", "type": {"kind": "utf8"}},
            {"field_id": 19, "name": "rate", "type": {"kind": "int64"}},
        ],
    },
    {
        "schema_id": POS_SCHEMA,
        "fields": [
            {"field_id": 1000, "name": "target_file_id",
             "type": {"kind": "utf8"}, "required": True},
            {"field_id": 1001, "name": "row_position",
             "type": {"kind": "int64"}, "required": True},
        ],
    },
    {
        "schema_id": EQ_SCHEMA,
        "fields": [
            {"field_id": 12, "name": "rule_id", "type": {"kind": "utf8"}},
            {"field_id": 13, "name": "published_at", "type": {"kind": "int64"}},
        ],
    },
]

FACT_ARROW = pa.schema([
    ("fact_id", pa.string()),
    ("entity_key", pa.string()),
    ("business_time", pa.int64()),
    ("knowledge_cutoff", pa.int64()),
    ("amount", pa.decimal128(12, 2)),
    ("note", pa.string()),
])
FACT_RENAMED_ARROW = pa.schema([
    ("fact_uuid", pa.string()),
    ("entity", pa.string()),
    ("biz_ts", pa.int64()),
    ("as_of_ts", pa.int64()),
    ("amount", pa.decimal128(12, 2)),
    ("remark", pa.string()),
    ("note", pa.string()),
])
DIM_ARROW = pa.schema([
    ("entity_key", pa.string()),
    ("rule_id", pa.string()),
    ("published_at", pa.int64()),
    ("operation", pa.string()),
    ("valid_from", pa.int64()),
    ("valid_to", pa.int64()),
    ("priority", pa.int64()),
    ("settlement_group", pa.string()),
    ("rate_bps", pa.int64()),
])
DIM_RENAMED_ARROW = pa.schema([
    ("ek", pa.string()),
    ("rid", pa.string()),
    ("pub_ts", pa.int64()),
    ("op", pa.string()),
    ("vstart", pa.int64()),
    ("vend", pa.int64()),
    ("prio", pa.int64()),
    ("grp", pa.string()),
    ("rate", pa.int64()),
])
POS_ARROW = pa.schema([("target_file_id", pa.string()),
                       ("row_position", pa.int64())])
# composite equality key (rule_id, published_at): targets only the A@8 row
EQ_ARROW = pa.schema([("rule_id", pa.string()),
                      ("published_at", pa.int64())])

PART = {"tenant": "settle"}

# ---------------------------------------------------------------------------
# hand-listed dimension records (file R1)
# columns: ek, rid, pub, op, vfrom, vto, prio, group, rate
# ---------------------------------------------------------------------------

DIM_RECORDS = [
    ("K", "A", 2, "put", 0, 100, 5, "G-A", 100),
    ("K", "A", 8, "put", 20, 100, 5, "G-A2", 101),
    ("K", "A", 12, "withdraw", None, None, None, None, None),
    ("K", "B", 4, "put", 0, 50, 4, "G-B", 200),
    ("K", "C", 7, "put", 40, 80, 4, "G-C", 300),
    ("K", "D", 6, "put", 40, 50, 4, "G-D", 400),
    # rule E on entity K that never matches any fact in the scenario; its
    # revisions still must be validated (duplicates must reject).
    ("K", "E", 3, "put", -1000, -900, 9, "G-E", 999),
    ("K", "E", 9, "put", -1000, -900, 9, "G-E2", 998),
    # rule G: open upper bound (valid_to NULL) published at 15; it covers the
    # largest int64 business time.  Published after kc=12 so the kc=12
    # expectations are unaffected.
    ("K", "G", 15, "put", 0, None, 2, "G-OPEN", 500),
    # withdraw-before-publish and withdraw-of-unknown-rule are legal:
    ("K", "F", 1, "withdraw", None, None, None, None, None),
    ("K", "F", 5, "put", 0, 10, 1, "G-F", 10),
    # another join domain, untouched by K facts
    ("J", "J1", 1, "put", -5, 5, 1, "G-J", 1),
]

# index (0-based) of the A@8 record inside R1 -- the row the lake-delete
# snapshots hide.
A8_ROW_POSITION = 1


def _dim_columns(rows, renamed=False):
    rows = list(rows)
    names = (["ek", "rid", "pub_ts", "op", "vstart", "vend", "prio",
              "grp", "rate"] if renamed else
             ["entity_key", "rule_id", "published_at", "operation",
              "valid_from", "valid_to", "priority", "settlement_group",
              "rate_bps"])
    int_names = {"vstart", "vend", "prio", "pub_ts", "rate",
                 "published_at", "valid_from", "valid_to",
                 "priority", "rate_bps"}
    out = {}
    for i, name in enumerate(names):
        vals = [r[i] for r in rows]
        out[name] = pa.array(vals, pa.int64()) if name in int_names else vals
    return out


def _fact_columns(rows, renamed=False):
    if rows and not isinstance(rows[0], (list, tuple)):
        rows = [rows]
    rows = list(rows)
    from decimal import Decimal

    def col(i):
        return [r[i] for r in rows]

    amounts = pa.array(
        [Decimal(v) if v is not None else None for v in col(4)],
        pa.decimal128(12, 2)) if rows else pa.array([], pa.decimal128(12, 2))
    if renamed:
        return {
            "fact_uuid": col(0),
            "entity": col(1),
            "biz_ts": pa.array(col(2), pa.int64()),
            "as_of_ts": pa.array(col(3), pa.int64()),
            "amount": amounts,
            "remark": col(5),
            # field 7 reuses the physical name "note" with a NEW identity;
            # absent in the original schema, null here.
            "note": [None] * len(rows),
        }
    return {
        "fact_id": col(0),
        "entity_key": col(1),
        "business_time": pa.array(col(2), pa.int64()),
        "knowledge_cutoff": pa.array(col(3), pa.int64()),
        "amount": amounts,
        "note": col(5),
    }


# ---------------------------------------------------------------------------
# fact rows of the main scenario
# (fact_id, entity_key, bt, kc, amount, note)
# ---------------------------------------------------------------------------

FACT_RECORDS = [
    ("f-10-k7", "K", 10, 7, "10.00", "kc7 A initial"),
    ("f-10-k8", "K", 10, 8, "11.00", "kc8 B wins"),
    ("f-40-k8", "K", 40, 8, "12.00", "kc8 A revised"),
    ("f-40-k12", "K", 40, 12, "13.00", "kc12 ambiguous 3"),
    ("f-50-k12", "K", 50, 12, "14.00", "kc12 C only"),
    ("f-80-k12", "K", 80, 12, "15.00", "kc12 unmatched"),
    ("f-0-k2", "K", 0, 2, "16.00", "left boundary included"),
    ("f-100-k8", "K", 100, 8, "17.00", "right boundary excluded"),
    ("f-min-k20", "K", -(2**63), 20, "18.00", "min int64 business time"),
    ("f-max-k20", "K", 2**63 - 1, 20, "19.00", "max int64, open upper bound"),
    ("f-j-0", "J", 0, 5, "20.00", "other entity"),
]

# hand-derived expectations keyed by fact id
EXPECTED_DECISIONS = {
    "f-10-k7": ("matched", "A", 2),
    "f-10-k8": ("matched", "B", 4),
    "f-40-k8": ("matched", "A", 8),
    "f-40-k12": ("ambiguous", None, None),
    "f-50-k12": ("matched", "C", 7),
    "f-80-k12": ("unmatched", None, None),
    "f-0-k2": ("matched", "A", 2),
    "f-100-k8": ("unmatched", None, None),
    # at kc=20 A is withdrawn; nothing with a finite upper bound covers the
    # extreme timestamps -- except rule G's open upper bound, which must
    # cover the largest int64 business time.
    "f-min-k20": ("unmatched", None, None),
    "f-max-k20": ("matched", "G", 15),
    "f-j-0": ("matched", "J1", 1),
}


def build_attribution_fixtures(base_dir) -> dict:
    """Build both fixture table directories.

    Returns a dict of useful identifiers (snapshot ids, file ids, the
    mapping config for the main scenario, ...).
    """
    base = Path(base_dir)
    fact_root = base / "facts"
    dim_root = base / "dimensions"
    for root in (fact_root / "files", dim_root / "files"):
        root.mkdir(parents=True, exist_ok=True)

    # ---- fact table ------------------------------------------------------
    f_rel, f_sha = _write(fact_root, "F1",
                          _fact_columns(FACT_RECORDS), FACT_ARROW)
    fr_rel, fr_sha = _write(
        fact_root, "F1R",
        _fact_columns([(r[0], r[1], r[2], r[3], r[4], r[5])
                       for r in FACT_RECORDS], renamed=True),
        FACT_RENAMED_ARROW)

    fact_snapshots = [
        {"snapshot_id": "facts_main", "parent": None,
         "active_files": [_entry("F1", f_rel, f_sha, FACT_SCHEMA, 5)]},
        {"snapshot_id": "facts_renamed", "parent": "facts_main",
         "active_files": [_entry("F1R", fr_rel, fr_sha,
                                 FACT_SCHEMA_RENAMED, 5)]},
    ]
    _write_contract(fact_root, "attr_facts", FACT_SCHEMAS, fact_snapshots)

    # ---- dimension table -------------------------------------------------
    r1_rel, r1_sha = _write(dim_root, "R1",
                            _dim_columns(DIM_RECORDS), DIM_ARROW)
    r1r_rel, r1r_sha = _write(dim_root, "R1R",
                              _dim_columns(DIM_RECORDS, renamed=True),
                              DIM_RENAMED_ARROW)
    # POS delete hiding the A@8 revision
    pos_rel, pos_sha = _write(dim_root, "POS_A8", {
        "target_file_id": ["R1"],
        "row_position": pa.array([A8_ROW_POSITION], pa.int64()),
    }, POS_ARROW)
    # EQ delete hiding the same row by the composite key
    # (rule_id, published_at) -- exercises EQ deletes without removing A@2
    eq_rel, eq_sha = _write(dim_root, "EQ_A8", {
        "rule_id": ["A"],
        "published_at": pa.array([8], pa.int64()),
    }, EQ_ARROW)

    dim_snapshots = [
        {"snapshot_id": "dims_full", "parent": None, "active_files": [
            _entry("R1", r1_rel, r1_sha, DIM_SCHEMA, 5)]},
        {"snapshot_id": "dims_renamed", "parent": "dims_full", "active_files": [
            _entry("R1R", r1r_rel, r1r_sha, DIM_SCHEMA_RENAMED, 5)]},
        {"snapshot_id": "dims_no_a8_pos", "parent": "dims_full", "active_files": [
            _entry("R1", r1_rel, r1_sha, DIM_SCHEMA, 5),
            _entry("POS_A8", pos_rel, pos_sha, POS_SCHEMA, 6,
                   kind="POS_DELETE")]},
        {
            # EQ deletes apply only when DATA.data_seq is strictly less than
            # the delete's data_seq, so this snapshot carries the same rows
            # in an older data file (seq 4 < EQ seq 6).
            "snapshot_id": "dims_no_a8_eq", "parent": "dims_full",
            "active_files": [
            _entry("R1", r1_rel, r1_sha, DIM_SCHEMA, 4),
            _entry("EQ_A8", eq_rel, eq_sha, EQ_SCHEMA, 6,
                   kind="EQ_DELETE", eq_field_ids=[12, 13])]},
    ]
    _write_contract(dim_root, "attr_dims", DIM_SCHEMAS, dim_snapshots)

    return {
        "fact_root": str(fact_root),
        "dim_root": str(dim_root),
        "fact_snapshot": "facts_main",
        "fact_snapshot_renamed": "facts_renamed",
        "dim_snapshot": "dims_full",
        "dim_snapshot_renamed": "dims_renamed",
        "dim_snapshot_no_a8_pos": "dims_no_a8_pos",
        "dim_snapshot_no_a8_eq": "dims_no_a8_eq",
    }


def main_mapping(fixtures: dict, output_dir: str,
                 dim_snapshot: str = "dims_full",
                 fact_snapshot: str = "facts_main") -> dict:
    """The mapping config used by the main scenario and the verifier."""
    return {
        "fact": {
            "table": fixtures["fact_root"],
            "snapshot": fact_snapshot,
            "schema": FACT_SCHEMA if fact_snapshot == "facts_main"
            else FACT_SCHEMA_RENAMED,
            "roles": {
                "fact_id": 1,
                "fact_entity_key": 2,
                "fact_business_time": 3,
                "fact_knowledge_cutoff": 4,
            },
            "fact_payload": [
                {"field_id": 5, "name": "fact_amount"},
                {"field_id": 6, "name": "fact_note"},
            ],
        },
        "dimension": {
            "table": fixtures["dim_root"],
            "snapshot": dim_snapshot,
            "schema": DIM_SCHEMA,
            "roles": {
                "dim_entity_key": 11,
                "dim_rule_id": 12,
                "dim_published_at": 13,
                "dim_operation": 14,
                "dim_valid_from": 15,
                "dim_valid_to": 16,
                "dim_priority": 17,
            },
            "dim_payload": [
                {"field_id": 18, "name": "settlement_group"},
                {"field_id": 19, "name": "rate_bps"},
            ],
        },
        "output_dir": output_dir,
    }


# ---------------------------------------------------------------------------
# large-scale generator: one hot entity, many overlapping rules and facts
# ---------------------------------------------------------------------------


def build_scale_fixtures(base_dir, n_rules: int = 400,
                         n_facts: int = 20000) -> dict:
    """Generate a hot-key scale table.

    All rules share one entity key and overlap the whole business-time range,
    so the simultaneous candidate set and the per-entity history both grow
    with ``n_rules``; facts cycle over business times and knowledge cutoffs.
    """
    from decimal import Decimal

    base = Path(base_dir)
    fact_root = base / "facts_scale"
    dim_root = base / "dims_scale"
    (fact_root / "files").mkdir(parents=True, exist_ok=True)
    (dim_root / "files").mkdir(parents=True, exist_ok=True)

    dim_rows = []
    # a small priority modulus guarantees many same-priority overlapping
    # rules, so ambiguous decisions and the two-evidence cap are exercised at
    # scale even though every rule covers the entire business-time range.
    priority_modulus = max(2, n_rules // 4)
    for i in range(n_rules):
        rid = f"R{i:06d}"
        # many revisions per rule to grow the history beyond memory batches
        for pub in range(0, 10):
            dim_rows.append(("HOT", rid, pub, "put", -10**6, 10**6,
                             i % priority_modulus, f"grp-{i}", i))
    # one withdraw per rule at the end to exercise inactive versions
    for i in range(n_rules):
        dim_rows.append(("HOT", f"R{i:06d}", 100, "withdraw", None, None,
                         None, None, None))
    _write(dim_root, "RD", _dim_columns(dim_rows), DIM_ARROW)
    _write_contract(dim_root, "attr_dims_scale", DIM_SCHEMAS, [
        {"snapshot_id": "dims", "parent": None, "active_files": [
            _entry("RD", "files/RD.parquet",
                   sha256_of(dim_root / "files/RD.parquet"),
                   DIM_SCHEMA, 5)]},
    ])

    fact_rows = []
    for i in range(n_facts):
        # cutoffs stay inside [10, 90): every rule's puts (pub 0..9) are
        # visible and the pub-100 withdraws are not, so rules genuinely
        # overlap and priority ties produce ambiguous decisions.
        fact_rows.append((
            f"fact{i:08d}", "HOT",
            (i % 2000) - 1000,          # business time cycles
            10 + (i % 80),              # cutoff cycles inside the put window
            Decimal("1.00"), "scale",
        ))
    _write(fact_root, "FS", _fact_columns(fact_rows), FACT_ARROW)
    _write_contract(fact_root, "attr_facts_scale", FACT_SCHEMAS, [
        {"snapshot_id": "facts", "parent": None, "active_files": [
            _entry("FS", "files/FS.parquet",
                   sha256_of(fact_root / "files/FS.parquet"),
                   FACT_SCHEMA, 5)]},
    ])
    return {
        "fact_root": str(fact_root),
        "dim_root": str(dim_root),
        "n_rules": n_rules,
        "n_facts": n_facts,
        "dim_rows": len(dim_rows),
    }


def scale_mapping(fixtures: dict, output_dir: str) -> dict:
    mapping = main_mapping(fixtures, output_dir)
    mapping["fact"]["snapshot"] = "facts"
    mapping["dimension"]["snapshot"] = "dims"
    return mapping


# ---------------------------------------------------------------------------
# low-level helpers
# ---------------------------------------------------------------------------


def _write(root: Path, name: str, columns: dict, arrow_schema) -> tuple:
    rel = f"files/{name}.parquet"
    p = root / rel
    pq.write_table(pa.table(columns, schema=arrow_schema), p)
    return rel, sha256_of(p)


def _entry(file_id: str, rel: str, sha: str, schema_id: int, seq: int,
           kind: str = "DATA", eq_field_ids=None) -> dict:
    e = {
        "file_id": file_id,
        "kind": kind,
        "path": rel,
        "sha256": sha,
        "partition": dict(PART),
        "data_seq": seq,
        "file_seq": seq,
        "schema_id": schema_id,
    }
    if eq_field_ids is not None:
        e["eq_field_ids"] = eq_field_ids
    return e


def _write_contract(root: Path, table_id: str, schemas: list,
                    snapshots: list) -> None:
    with open(root / "table.json", "w", encoding="utf-8") as fh:
        json.dump({
            "format": FORMAT,
            "table_id": table_id,
            "schemas": schemas,
            "snapshots": snapshots,
        }, fh, indent=2)
