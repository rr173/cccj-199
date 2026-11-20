"""Bitemporal attribution engine over the on-disk extracted stores.

Per fact (entity key ``ek``, business time ``bt``, knowledge cutoff ``kc``)
the engine reproduces, for every rule of the entity, the rule version that
was knowable at the cutoff:

* a version is the visible record with the greatest publication time
  ``pub <= kc`` (equality included) for rule identity ``(ek, rule_id)``;
* a selected ``withdraw`` makes the rule inactive;
* a selected ``put`` participates only when its half-open interval
  ``[valid_from, valid_to)`` contains ``bt`` (a null upper bound is no upper
  bound and therefore covers the largest int64 business time);
* among the surviving candidates the greatest priority wins; one candidate
  is ``matched``, none is ``unmatched``, several is ``ambiguous`` (with the
  total top-priority count and the first two evidences in UTF-8 byte order
  of rule ids).

Bounded-memory design (the inputs may each be far larger than memory, a
single entity may own a long revision history and many simultaneous
candidates)

* All state lives in SQLite with a capped page cache; Python only ever holds
  the current fact, the current decision and fixed-size chunks.
* ``curver`` holds, per rule identity, the version selected at the entity's
  current watermark cutoff.  Advancing the cutoff is one set-based indexed
  UPSERT that reads each revision row at most once over the whole task;
  retreating the cutoff rebuilds that entity's selection in one indexed set
  operation -- the engine never loops over an entity's full history once per
  fact in Python.
* The simultaneous candidate set is never materialized as a Python object
  and there is no fact x candidate product: winner selection, counting and
  the two-evidence extraction are aggregate / LIMIT queries over ``curver``,
  so hot entities with huge overlap stay on disk and only the winning row
  (or two evidence rows) crosses into Python.
* Facts are streamed in raw UTF-8 byte order of fact id, so output rows are
  produced already in the required logical order without a sort step.

Costs (F facts, R visible revision records, H the size of the largest
simultaneous candidate set, E entities): time O((F + R) log R) with a small
constant number of indexed statements per fact; disk O(F + R); Python
memory O(B) for a fixed streaming batch size B, independent of F, R, the
per-entity history length and H.
"""

from __future__ import annotations

from typing import Iterator

from .attribution_store import OP_PUT, AttributionStore

STATUS_MATCHED = "matched"
STATUS_UNMATCHED = "unmatched"
STATUS_AMBIGUOUS = "ambiguous"

# interval containment predicate shared by every candidate query:
# [vfrom, vto) contains bt; a null vto is no upper bound
_CONTAINS = "vfrom<=? AND (vto IS NULL OR vto>?)"


class AttributionEngine:
    """Streams decisions for all facts in fact-id UTF-8 byte order.

    Yields plain dicts whose keys are the engine-level output fields; the
    writer module turns them into typed Parquet columns.
    """

    def __init__(self, store: AttributionStore):
        self.db = store.db
        self._init_tables()
        self._prepare_statements()

    def _init_tables(self) -> None:
        db = self.db
        # current selected version per rule identity at the entity watermark
        db.execute(
            "CREATE TABLE curver("
            " ek BLOB NOT NULL, rid BLOB NOT NULL, pub INTEGER NOT NULL,"
            " op INTEGER NOT NULL, vfrom INTEGER, vto INTEGER, prio INTEGER,"
            " dim_payload TEXT, dim_file_id TEXT NOT NULL,"
            " dim_row_pos INTEGER NOT NULL, seq INTEGER NOT NULL,"
            " PRIMARY KEY(ek, rid)) WITHOUT ROWID"
        )
        db.execute(
            "CREATE INDEX curver_entity ON curver(ek, op, prio, rid)"
        )
        db.execute(
            "CREATE TABLE watermark(ek BLOB PRIMARY KEY, kc INTEGER NOT NULL)"
            " WITHOUT ROWID"
        )
        db.commit()
        # O(1) Python-side cache of the *current* entity's watermark.  Facts
        # arrive grouped by entity key for long runs (the common hot-key
        # case); this removes a SQLite round trip per fact while staying
        # bounded to one entry regardless of the number of entities.
        self._cached_ek = None
        self._cached_kc = None

    def _prepare_statements(self) -> None:
        # Parameterized SQL the connection caches and re-plans cheaply.
        self._sql_top = (
            "SELECT prio, COUNT(*) FROM curver "
            "WHERE ek=? AND op=? AND " + _CONTAINS +
            " GROUP BY prio ORDER BY prio DESC LIMIT 1"
        )
        self._sql_winner = (
            "SELECT rid, pub, dim_payload, dim_file_id, dim_row_pos "
            "FROM curver WHERE ek=? AND op=? AND prio=? AND " + _CONTAINS
        )
        self._sql_evidence = (
            "SELECT rid, pub, dim_file_id, dim_row_pos FROM curver "
            "WHERE ek=? AND op=? AND prio=? AND " + _CONTAINS +
            " ORDER BY rid LIMIT 2"
        )

    # ------------------------------------------------------------------
    # public stream
    # ------------------------------------------------------------------

    def decisions(self) -> Iterator[dict]:
        cur = self.db.execute(
            "SELECT fid, ek, bt, kc, fact_payload, fact_file_id, fact_row_pos "
            "FROM facts ORDER BY fid"
        )
        while True:
            chunk = cur.fetchmany(1000)
            if not chunk:
                break
            for row in chunk:
                fid, ek, bt, kc, fact_payload, fact_file, fact_pos = row
                self._sync_version(ek, kc)
                yield self._decide(
                    fid, ek, bt, kc, fact_payload, fact_file, fact_pos
                )
        self.finalize()

    # ------------------------------------------------------------------
    # version selection at the knowledge cutoff
    # ------------------------------------------------------------------

    def _sync_version(self, ek: bytes, kc: int) -> None:
        if self._cached_ek == ek:
            old_kc = self._cached_kc
            if old_kc == kc:
                return
            if kc > old_kc:
                self._advance(ek, old_kc, kc)
            else:
                self.db.execute("DELETE FROM curver WHERE ek=?", (ek,))
                self._seed(ek, kc)
            self._cached_kc = kc
            return
        # first fact of a new entity: persist the previous entity's final
        # watermark in one statement, then load/seed this entity's selection.
        if self._cached_ek is not None:
            self.db.execute(
                "UPDATE watermark SET kc=? WHERE ek=?",
                (self._cached_kc, self._cached_ek))
        row = self.db.execute(
            "SELECT kc FROM watermark WHERE ek=?", (ek,)
        ).fetchone()
        if row is None:
            self._seed(ek, kc)
            self.db.execute(
                "INSERT INTO watermark(ek, kc) VALUES (?, ?)", (ek, kc))
        else:
            old_kc = row[0]
            if kc > old_kc:
                self._advance(ek, old_kc, kc)
            elif kc < old_kc:
                self.db.execute("DELETE FROM curver WHERE ek=?", (ek,))
                self._seed(ek, kc)
        self._cached_ek = ek
        self._cached_kc = kc

    def finalize(self) -> None:
        """Persist the last entity's watermark after the stream ends."""
        if self._cached_ek is not None:
            self.db.execute(
                "UPDATE watermark SET kc=? WHERE ek=?",
                (self._cached_kc, self._cached_ek))
            self.db.commit()

    def _seed(self, ek: bytes, kc: int) -> None:
        """First selection for an entity: latest record per rule with pub<=kc."""
        self.db.execute(
            "INSERT INTO curver(ek, rid, pub, op, vfrom, vto, prio, "
            "dim_payload, dim_file_id, dim_row_pos, seq) "
            "SELECT ek, rid, pub, op, vfrom, vto, prio, dim_payload, "
            "dim_file_id, dim_row_pos, seq FROM dim "
            "WHERE ek=? AND pub<=? AND seq IN ("
            "  SELECT max(seq) FROM dim WHERE ek=? AND pub<=? GROUP BY rid"
            ")",
            (ek, kc, ek, kc),
        )

    def _advance(self, ek: bytes, old_kc: int, kc: int) -> None:
        """Move the watermark forward in one set-based statement.

        Only rules with a record in (old_kc, kc] can change identity, and the
        greatest ``seq`` per rule inside that window is exactly the new
        selected version (records never arrive out of publication order
        within one visible extraction).  Every revision row is still read at
        most once over the whole task; no per-rule Python history walk and no
        per-rule round trip happens here.
        """
        self.db.execute(
            "INSERT INTO curver(ek, rid, pub, op, vfrom, vto, prio, "
            "dim_payload, dim_file_id, dim_row_pos, seq) "
            "SELECT d.ek, d.rid, d.pub, d.op, d.vfrom, d.vto, d.prio, "
            "d.dim_payload, d.dim_file_id, d.dim_row_pos, d.seq "
            "FROM dim d JOIN ("
            "  SELECT rid, max(seq) AS mseq FROM dim "
            "  WHERE ek=? AND pub>? AND pub<=? GROUP BY rid"
            ") w ON w.mseq = d.seq "
            "ON CONFLICT(ek, rid) DO UPDATE SET "
            "pub=excluded.pub, op=excluded.op, vfrom=excluded.vfrom, "
            "vto=excluded.vto, prio=excluded.prio, "
            "dim_payload=excluded.dim_payload, "
            "dim_file_id=excluded.dim_file_id, "
            "dim_row_pos=excluded.dim_row_pos, seq=excluded.seq",
            (ek, old_kc, kc),
        )

    # ------------------------------------------------------------------
    # winner selection directly over curver (no candidate materialization)
    # ------------------------------------------------------------------

    def _decide(self, fid: bytes, ek: bytes, bt: int, kc: int,
                fact_payload: str, fact_file: str, fact_pos: int) -> dict:
        top = self.db.execute(
            self._sql_top, (ek, OP_PUT, bt, bt)
        ).fetchone()
        base = {
            "fid": fid.decode("utf-8"),
            "entity_key": ek.decode("utf-8"),
            "business_time": bt,
            "knowledge_cutoff": kc,
            "fact_payload": fact_payload,
            "fact_file_id": fact_file,
            "fact_row_position": fact_pos,
        }
        if top is None:
            base["status"] = STATUS_UNMATCHED
            return base
        prio, count = top
        if count == 1:
            rid_b, pub, payload, dfile, dpos = self.db.execute(
                self._sql_winner,
                (ek, OP_PUT, prio, bt, bt),
            ).fetchone()
            base["status"] = STATUS_MATCHED
            base["matched_rule_id"] = rid_b.decode("utf-8")
            base["matched_published_at"] = pub
            base["matched_dim_file_id"] = dfile
            base["matched_dim_row_position"] = dpos
            base["dim_payload"] = payload
            return base
        base["status"] = STATUS_AMBIGUOUS
        base["candidate_count"] = count
        evidences = self.db.execute(
            self._sql_evidence, (ek, OP_PUT, prio, bt, bt)
        ).fetchall()
        for idx, (rid_b, pub, dfile, dpos) in enumerate(evidences, start=1):
            base[f"evidence_{idx}_rule_id"] = rid_b.decode("utf-8")
            base[f"evidence_{idx}_published_at"] = pub
            base[f"evidence_{idx}_dim_file_id"] = dfile
            base[f"evidence_{idx}_dim_row_position"] = dpos
        return base
