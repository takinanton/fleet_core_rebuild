"""registry.py — durable placed_trigger_oids registry (entry-SM §2.2, F5).

THE LAW THIS MODULE IMPLEMENTS (fleet-wide, all 4 venues):

    "bot-own" IS DEFINED as `oid ∈ placed_trigger_oids`.

Consumers (design refs): orphan-trigger sweep (entry-SM §4.2.4), supersede
sweep (exit-engine §7 / step 7b), K6 crash-landed-SL adoption (entry-SM §5),
`_fenced(oid=…, placed_oids=…)` per-oid manual discriminator, exit attribution
(exit-engine §6 historic-oid corpus).

WRITE DISCIPLINE (entry-SM §2.2): the executor INSERTs the oid IMMEDIATELY
after any `trigger_sl`/`limit_reduce_only` placement is readback-confirmed,
BEFORE any dependent cancel or trades-row persist. This ordering is what makes
K12 recoverable (a kill after registry-INSERT leaves the new oid provably
OURS). Append-only: rows are NEVER deleted — cancelled/filled oids stay; they
are the historic exit-attribution corpus.

READ DISCIPLINE: `oids_ever_placed()` extends the P1 canonical
journal.oids_ever_placed union to: DB table ∪ legacy HL placed_trigger_oids.json
∪ trades.sl_order_id/tp1_order_id columns. Registry READ failure degrades the
consumer to its conservative coin-level fence (errs FENCED — never exposes a
manual trigger); registry WRITE failure is loud but never blocks a placement
that is already live on the exchange (mirrors P1 register_placed_trigger_oid).

Pure stdlib; importable with no venue SDKs and no `bot` package.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, Set

log = logging.getLogger(__name__)

VALID_KINDS = ("sl", "tp1", "protective")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS placed_trigger_oids (
    oid TEXT PRIMARY KEY,
    coin TEXT NOT NULL,
    trade_id INTEGER,
    placed_at TEXT NOT NULL,
    kind TEXT NOT NULL
);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _conn(db_path) -> Iterator[sqlite3.Connection]:
    """WAL + busy_timeout on every open (entry-SM §6 nado journal fix —
    engine journal opens WAL + busy_timeout=5000 fleet-wide)."""
    con = sqlite3.connect(str(db_path), timeout=5.0)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=5000")
        try:
            con.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:  # read-only fs etc. — non-fatal
            pass
        yield con
        con.commit()
    finally:
        con.close()


def ensure_table(con: sqlite3.Connection) -> None:
    con.executescript(_SCHEMA)


# ---------------------------------------------------------------- writes

def register_oid_conn(con: sqlite3.Connection, oid, coin: str,
                      trade_id: Optional[int] = None,
                      kind: str = "sl") -> bool:
    """INSERT the oid inside an already-open transaction. Append-only:
    INSERT OR IGNORE — re-registering is a no-op (idempotent). Returns True
    if the oid is registered after the call (new or pre-existing)."""
    if oid is None or str(oid) == "":
        return False
    if kind not in VALID_KINDS:
        raise ValueError(f"registry: kind must be one of {VALID_KINDS}, got {kind!r}")
    ensure_table(con)
    con.execute(
        "INSERT OR IGNORE INTO placed_trigger_oids(oid, coin, trade_id, placed_at, kind)"
        " VALUES (?, ?, ?, ?, ?)",
        (str(oid), str(coin), trade_id, _now_iso(), kind),
    )
    return True


def register_oid(db_path, oid, coin: str, trade_id: Optional[int] = None,
                 kind: str = "sl") -> bool:
    """Durably register a bot-placed trigger oid. COMMITS IMMEDIATELY —
    call this right after the placement readback-confirm and BEFORE any
    dependent cancel/persist (§2.2 write discipline / exit-engine §7 step 3a).

    Best-effort on I/O failure: logged loud, returns False, never raises into
    the placement path (the trigger is already live on the exchange; a missing
    registry row only degrades sweeps to their conservative FENCED direction).
    A ValueError on a bad `kind` is a caller bug and DOES raise."""
    if oid is None or str(oid) == "":
        return False
    if kind not in VALID_KINDS:
        raise ValueError(f"registry: kind must be one of {VALID_KINDS}, got {kind!r}")
    try:
        with _conn(db_path) as con:
            register_oid_conn(con, oid, coin, trade_id=trade_id, kind=kind)
        return True
    except Exception as e:  # noqa: BLE001 — deliberate best-effort seam
        log.critical("placed_trigger_oids: durable INSERT FAILED for oid=%s coin=%s "
                     "(%s) — sweeps degrade to conservative FENCED for this oid",
                     oid, coin, e)
        return False


# ----------------------------------------------------------------- reads

def registry_oids(db_path) -> Set[str]:
    """All oids in the DB registry table. Raises on DB failure — the CALLER
    decides the degrade direction (consumers must err FENCED / skip-cancel)."""
    with _conn(db_path) as con:
        ensure_table(con)
        return {str(r["oid"]) for r in
                con.execute("SELECT oid FROM placed_trigger_oids").fetchall()}


def is_bot_own(db_path, oid) -> bool:
    """Authoritative bot-own test for a single oid. Raises on DB failure."""
    if oid is None or str(oid) == "":
        return False
    with _conn(db_path) as con:
        ensure_table(con)
        row = con.execute("SELECT 1 FROM placed_trigger_oids WHERE oid=?",
                          (str(oid),)).fetchone()
        return row is not None


def _load_legacy_json(path: Optional[Path]) -> Set[str]:
    """HL legacy placed_trigger_oids.json (P1 journal registry) — fail-OPEN
    empty set on missing/corrupt file, mirroring journal._load_placed_trigger_oids
    (a legacy-read failure only foregoes the union member; the DB table and the
    oid columns still stand)."""
    if path is None:
        return set()
    try:
        p = Path(path)
        if p.exists():
            raw = json.loads(p.read_text(encoding="utf-8"))
            return {str(o) for o in (raw.get("oids", []) or []) if str(o) != ""}
    except Exception as e:  # noqa: BLE001
        log.warning("placed_trigger_oids: legacy json load failed (%s) — empty", e)
    return set()


def oids_ever_placed(db_path, legacy_json_path=None) -> Set[str]:
    """Fleet-wide historic bot-own oid corpus (entry-SM §2.2 migration note):

        DB placed_trigger_oids table
      ∪ legacy HL placed_trigger_oids.json (when present)
      ∪ trades.sl_order_id / trades.tp1_order_id columns (all rows)

    Extends P1 journal.oids_ever_placed; the union keeps every P1 helper
    working through the cutover. Raises on DB failure (caller errs FENCED)."""
    out: Set[str] = _load_legacy_json(legacy_json_path)
    with _conn(db_path) as con:
        ensure_table(con)
        out |= {str(r["oid"]) for r in
                con.execute("SELECT oid FROM placed_trigger_oids").fetchall()}
        cols = {r["name"] for r in con.execute("PRAGMA table_info(trades)").fetchall()}
        if {"sl_order_id", "tp1_order_id"} <= cols:
            for r in con.execute(
                    "SELECT sl_order_id, tp1_order_id FROM trades").fetchall():
                for oid in (r["sl_order_id"], r["tp1_order_id"]):
                    if oid is not None and str(oid) != "":
                        out.add(str(oid))
    return out


# ------------------------------------------------------------- migration

def seed_from_sources(con: sqlite3.Connection,
                      legacy_json_path=None) -> int:
    """Cutover seed (entry-SM §2.2 Migration): import (a) all
    trades.sl_order_id/tp1_order_id, (b) HL legacy json verbatim.
    ((c) sm_transitions oids are none pre-cutover.) Idempotent — INSERT OR
    IGNORE; re-running adds nothing. Returns the number of NEW rows inserted."""
    ensure_table(con)
    before = con.execute("SELECT COUNT(*) FROM placed_trigger_oids").fetchone()[0]
    cols = {r["name"] for r in con.execute("PRAGMA table_info(trades)").fetchall()}
    if {"sl_order_id", "tp1_order_id"} <= cols:
        for r in con.execute(
                "SELECT id, coin, sl_order_id, tp1_order_id FROM trades").fetchall():
            if r["sl_order_id"]:
                register_oid_conn(con, r["sl_order_id"], r["coin"],
                                  trade_id=r["id"], kind="sl")
            if r["tp1_order_id"]:
                register_oid_conn(con, r["tp1_order_id"], r["coin"],
                                  trade_id=r["id"], kind="tp1")
    for oid in sorted(_load_legacy_json(legacy_json_path)):
        # legacy json carries no coin/trade attribution — imported verbatim
        register_oid_conn(con, oid, "legacy_json", trade_id=None, kind="sl")
    after = con.execute("SELECT COUNT(*) FROM placed_trigger_oids").fetchone()[0]
    return int(after - before)
