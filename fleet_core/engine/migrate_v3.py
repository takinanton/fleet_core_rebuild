"""migrate_v3.py — trades.db migration to the P3 entry-SM schema.

Spec: p3_design_entry_sm.md §2 (columns/tables), §2.3 (status↔sm_state map),
§2.4 (existing-row mapping). Invoked at cutover step 4 (p3_rollout.md §3):

    python -m fleet_core.engine.migrate_v3 --db /path/to/trades.db

PROPERTIES (all load-bearing for the rollout):

  * ADDITIVE ONLY — new columns via ALTER TABLE ADD COLUMN, new tables via
    CREATE TABLE IF NOT EXISTS, one partial UNIQUE index on client_order_id.
    No column is dropped, renamed, retyped or rewritten; no row is deleted.
  * IDEMPOTENT / RE-RUNNABLE — column adds are guarded by PRAGMA table_info;
    row mapping only touches rows WHERE sm_state IS NULL; registry seeding is
    INSERT OR IGNORE. Running twice is a no-op the report proves (0 mapped).
  * REVERSIBLE (rollback semantics, entry-SM §2.3 / p3_rollout.md §4):
      - the OLD bot ignores every new column and never selects the new
        `status='aborted'` value, so a migrated DB runs under the old bot
        UNCHANGED — NO down-migration exists or is needed;
      - the engine MAINTAINS the legacy `status` column on every transition
        per the §2.3 map, so old tooling (trades-check / parity) keeps
        working against a post-cutover DB;
      - rollback is GATED on draining all non-terminal pre-OPEN rows
        (sm_state ∈ {INTENT, PENDING, FILLED, ABORTING} → they map to legacy
        status='pending' and would hit the degraded old-bot machinery);
        `trades.db.pre_p3_<date>` snapshot is the catastrophic fallback only.
  * VERIFIED — the report prints per-row mapping (pipe table) and asserts
    row counts by sm_state == pre-migration counts by status (rollout step 4
    gate). Exit 0 on success, 1 on any assertion failure.

Pure stdlib; importable with no venue SDKs and no `bot` package.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from fleet_core.engine import registry as _registry

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Schema (entry-SM §2.1 / §2.2)
# --------------------------------------------------------------------------

# column name -> SQL type/default (ADD COLUMN-safe: no UNIQUE/NOT NULL here)
NEW_TRADES_COLUMNS: List[Tuple[str, str]] = [
    ("sm_state", "TEXT"),                # the state machine itself
    ("sm_updated_at", "TEXT"),           # staleness detection for reconciler
    ("client_order_id", "TEXT"),         # dedupe key (UNIQUE via partial index)
    ("order_sent_at", "TEXT"),           # PENDING marker (crash disambiguation)
    ("fill_confirmed_at", "TEXT"),
    ("fill_oid", "TEXT"),                # oid-first exit attribution + reconcile
    ("sl_placed_px", "REAL"),            # venue-ACCEPTED (post-clamp) trigger px
    ("sl_confirmed_at", "TEXT"),
    ("entry_bar_ts", "INTEGER"),         # exists on HL already; add elsewhere
    ("atr14", "REAL"),                   # exists on HL already; add elsewhere
    ("tp1_frac_at_entry", "REAL"),       # freeze env TP1 frac at entry (ext §e)
    ("leverage_eff", "REAL"),            # min(env LEVERAGE, asset max) used
    ("limit_px", "REAL"),                # F3: frozen cap-breach reference
    ("abort_reason", "TEXT"),            # F3: durable BEFORE any unwind I/O
    ("origin", "TEXT"),                  # 'entry' | 'adopted_untracked' | 'migrated'
    ("management_class", "TEXT"),        # 'strategy'|'protect_only'|'manual_claimed'
]

NEW_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS sm_transitions (        -- append-only audit
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    at TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    detail TEXT                                    -- JSON evidence
);
CREATE INDEX IF NOT EXISTS idx_sm_transitions_trade ON sm_transitions(trade_id);

CREATE TABLE IF NOT EXISTS entry_cooldowns (       -- persists _ENTRY_ABORT_COOLDOWN
    coin TEXT PRIMARY KEY,
    until_ts REAL NOT NULL,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS placed_trigger_oids (   -- F5: durable bot-own registry
    oid TEXT PRIMARY KEY,
    coin TEXT NOT NULL,
    trade_id INTEGER,
    placed_at TEXT NOT NULL,
    kind TEXT NOT NULL                             -- 'sl' | 'tp1' | 'protective'
);

CREATE TABLE IF NOT EXISTS bot_state (             -- exists on all venues; belt+braces
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_sm_state ON trades(sm_state);
"""

UNIQUE_INDEX_SQL = ("CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_client_order_id "
                    "ON trades(client_order_id) WHERE client_order_id IS NOT NULL")

# §2.4 mapping: legacy status -> sm_state
_STATUS_TO_SM = {
    "pending": "PENDING",   # conservative; startup reconciler resolves as a crash
    "open": "OPEN",
    "closed": "CLOSED",
    "aborted": "ABORTED",   # only exists if the engine already ran (re-run case)
}

_TF_RE = re.compile(r"^(\d+)\s*([mhdw])$", re.IGNORECASE)
_TF_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tf_ms(tf: str) -> Optional[int]:
    m = _TF_RE.match(str(tf or "").strip())
    if not m:
        return None
    return int(m.group(1)) * _TF_MS[m.group(2).lower()]


def _iso_to_ms(s: str) -> Optional[int]:
    try:
        txt = str(s).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001 — legacy rows carry mixed formats
        return None


def _connect(db_path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path), timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    try:
        con.execute("PRAGMA journal_mode=WAL")  # §6: WAL fleet-wide
    except sqlite3.Error:
        pass
    return con


# --------------------------------------------------------------------------
# Schema application (shared with entry_sm.t_intent for fresh DBs)
# --------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection) -> List[str]:
    """Apply the ADDITIVE schema deltas (columns + tables + index). Idempotent.
    Returns the list of columns actually added on this call. Assumes a `trades`
    table exists (all four live DBs); creates the minimal P1 tables if the DB
    is brand-new (selftest / stage)."""
    have_trades = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='trades'"
    ).fetchone()
    if not have_trades:
        # Fresh DB (stage/selftest): create the P1-canonical trades shape first
        # (journal.py init_db schema, verbatim columns).
        con.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                coin TEXT NOT NULL,
                tf TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'long',
                entry REAL NOT NULL,
                entry_intended REAL,
                sl_initial REAL NOT NULL,
                sl_current REAL,
                tp1 REAL,
                size REAL NOT NULL,
                risk_dollars REAL NOT NULL,
                notional REAL,
                walk_slip_pct REAL,
                status TEXT NOT NULL DEFAULT 'open',
                entry_order_id TEXT,
                sl_order_id TEXT,
                tp1_order_id TEXT,
                tp1_partial_done INTEGER NOT NULL DEFAULT 0,
                tp1_fill_price REAL,
                opened_at TEXT,
                closed_at TEXT,
                exit_price REAL,
                exit_reason TEXT,
                pnl_dollars REAL,
                realized_r REAL,
                notes TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trades_coin_tf ON trades(coin, tf, status);
            CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                coin TEXT NOT NULL,
                tf TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'long',
                trigger_price REAL,
                entry_price REAL,
                sl_price REAL,
                reason TEXT NOT NULL,
                walk_slip_pct REAL,
                vol_1h_usd REAL
            );
        """)
    existing = {r["name"] for r in con.execute("PRAGMA table_info(trades)").fetchall()}
    added: List[str] = []
    for col, definition in NEW_TRADES_COLUMNS:
        if col not in existing:
            con.execute(f"ALTER TABLE trades ADD COLUMN {col} {definition}")
            added.append(col)
    con.executescript(NEW_TABLES_SQL)
    con.execute(UNIQUE_INDEX_SQL)
    return added


# --------------------------------------------------------------------------
# Row mapping (§2.4) — run once per venue at cutover; idempotent
# --------------------------------------------------------------------------

def _map_rows(con: sqlite3.Connection,
              venue_sl_px: Optional[Dict[str, float]] = None
              ) -> List[Dict[str, object]]:
    """Map every row WHERE sm_state IS NULL per §2.4. Returns per-row report
    dicts. Sets origin='migrated' + management_class='strategy' where NULL
    (pre-cutover rows are by definition the bot's own strategy rows).

    venue_sl_px (EF5, design §10): {sl_order_id -> LISTED trigger px} read
    back from the venue at cutover — sl_placed_px is seeded FROM THE VENUE'S
    LISTED px when the row's oid is resting; the §2.4 sl_current convention
    remains ONLY the labeled fallback (venue unreadable / oid not listed —
    the SL-liveness heal then owns it on the first tick).

    EF6 (design §10 one-time restore grace): every OPEN-mapped row arms
    bot_state restore_flag:<id>=1 → exit-engine pipeline step 3
    (restore_reconcile_pending) runs the through-px reconcile once, then the
    executor clears the flag."""
    now = _now_iso()
    report: List[Dict[str, object]] = []
    rows = con.execute(
        "SELECT id, coin, tf, status, sl_order_id, sl_current, entry_bar_ts, "
        "opened_at, notes FROM trades WHERE sm_state IS NULL ORDER BY id"
    ).fetchall()
    for r in rows:
        status = str(r["status"] or "").lower()
        sm = _STATUS_TO_SM.get(status)
        note = ""
        if sm is None:
            # Unknown legacy status: leave sm_state NULL, flag loud. NEVER guess
            # a state for money bookkeeping.
            report.append({"id": r["id"], "coin": r["coin"], "status": status,
                           "sm_state": "(UNMAPPED)", "note": "UNKNOWN legacy status "
                           "— left NULL, operator must resolve"})
            continue
        sets = ["sm_state=?", "sm_updated_at=?",
                "origin=COALESCE(origin,'migrated')",
                "management_class=COALESCE(management_class,'strategy')"]
        vals: List[object] = [sm, now]
        if sm == "OPEN":
            # EF6: one-time restore grace flag (consumed by exit-engine step 3)
            con.execute("INSERT OR REPLACE INTO bot_state(key, value) "
                        "VALUES (?, '1')", (f"restore_flag:{r['id']}",))
            note = "restore_grace_armed"
        listed_px = None
        if venue_sl_px is not None and r["sl_order_id"] is not None:
            listed_px = venue_sl_px.get(str(r["sl_order_id"]))
        if sm == "OPEN" and listed_px is not None:
            # EF5: seed FROM the venue's LISTED trigger px (design §10).
            sets.append("sl_placed_px=?")
            vals.append(float(listed_px))
            note += "; sl_placed_px:=venue LISTED trigger px"
        elif sm == "OPEN" and r["sl_order_id"] is not None and \
                r["sl_current"] is not None:
            # §2.4 fallback (LABELED): sl_current convention — used only when
            # the venue listing was unavailable or the oid is not resting;
            # first exit-engine tick re-verifies the SL live (heal owns it).
            sets.append("sl_placed_px=COALESCE(sl_placed_px, sl_current)")
            note += "; sl_placed_px:=sl_current (FALLBACK — venue listing " \
                    "unavailable/oid not listed)"
        # entry_bar_ts backfill (§2.4 last row): NULL -> opened_at floored to tf
        if r["entry_bar_ts"] is None and r["opened_at"]:
            tfms = _tf_ms(r["tf"])
            oms = _iso_to_ms(r["opened_at"])
            if tfms and oms:
                bar_ts = (oms // tfms) * tfms
                sets.append("entry_bar_ts=?")
                vals.append(bar_ts)
                sets.append("notes=COALESCE(notes,'') || ?")
                vals.append("; entry_bar_ts_backfilled_from_opened_at")
                note = (note + "; " if note else "") + "entry_bar_ts backfilled"
        vals.append(r["id"])
        con.execute(f"UPDATE trades SET {', '.join(sets)} "
                    "WHERE id=? AND sm_state IS NULL", vals)
        con.execute(
            "INSERT INTO sm_transitions(trade_id, at, from_state, to_state, detail)"
            " VALUES (?, ?, ?, ?, ?)",
            (r["id"], now, "", sm,
             json.dumps({"migrated": True, "legacy_status": status})),
        )
        report.append({"id": r["id"], "coin": r["coin"], "status": status,
                       "sm_state": sm, "note": note or "-"})
    return report


def migrate(db_path, legacy_registry_json=None, dry_run: bool = False,
            client=None) -> Dict[str, object]:
    """Full migration: schema + row map + registry seed + count verification.

    legacy_registry_json: path to HL's placed_trigger_oids.json (defaults to
    the file next to the DB when it exists — HL is the only venue that has
    one; absent elsewhere).

    client (EF5): a P2 ExchangeClient — migrate_v3 runs at cutover step 4
    WITH THE VENUE REACHABLE (p3_rollout.md §3), so when a client is supplied
    the OPEN-row sl_placed_px is seeded from the venue's LISTED trigger px
    (list_reduce_only_triggers readback, the px-bearing listing behind
    list_open_sl_orders). No client / read failure ⇒ the §2.4 sl_current
    fallback, LABELED in the per-row report. The cutover wrapper passes the
    venue binding; the bare CLI cannot construct one (engine ships no SDKs) —
    its report says which mode ran."""
    db_path = Path(db_path)
    if legacy_registry_json is None:
        candidate = db_path.parent / "placed_trigger_oids.json"
        legacy_registry_json = candidate if candidate.exists() else None

    venue_sl_px: Optional[Dict[str, float]] = None
    venue_seed_mode = "no_client_fallback_sl_current"
    if client is not None:
        try:
            venue_sl_px = {
                str(t.oid): float(t.trigger_px)
                for t in client.list_reduce_only_triggers()
                if getattr(t, "trigger_px", None) is not None}
            venue_seed_mode = "venue_listed_trigger_px"
        except Exception as e:  # noqa: BLE001 — ReadUnknown class: LABELED fallback
            venue_sl_px = None
            venue_seed_mode = "venue_list_read_failed_fallback_sl_current(%s)" % e
            log.warning("migrate_v3: venue trigger listing unreadable (%s) — "
                        "sl_placed_px seeds fall back to sl_current (LABELED)", e)

    con = _connect(db_path)
    try:
        # schema first (additive only — cannot change row/status counts), so a
        # brand-new stage DB gets its trades table before the pre-count.
        added = ensure_schema(con)
        pre_status: Dict[str, int] = {
            str(r["status"]): int(r["n"]) for r in con.execute(
                "SELECT status, COUNT(*) n FROM trades GROUP BY status").fetchall()}
        rows_report = _map_rows(con, venue_sl_px=venue_sl_px)
        seeded = _registry.seed_from_sources(con, legacy_json_path=legacy_registry_json)
        post_sm: Dict[str, int] = {
            str(r["sm_state"]): int(r["n"]) for r in con.execute(
                "SELECT sm_state, COUNT(*) n FROM trades GROUP BY sm_state").fetchall()}

        # Verification gate (rollout §3 step 4): counts by sm_state must equal
        # pre-migration counts by status under the §2.3/§2.4 map.
        expect: Dict[str, int] = {}
        for status, n in pre_status.items():
            sm = _STATUS_TO_SM.get(str(status).lower(), "(UNMAPPED)")
            key = sm if sm != "(UNMAPPED)" else "None"
            expect[key] = expect.get(key, 0) + n
        got = {("None" if k in ("None", "") else k): v for k, v in post_sm.items()}
        ok = all(got.get(k, 0) >= v for k, v in expect.items()) and \
            sum(got.values()) == sum(pre_status.values())

        if dry_run:
            con.rollback()
        else:
            con.commit()
    finally:
        con.close()

    return {
        "db": str(db_path),
        "dry_run": dry_run,
        "columns_added": added,
        "rows_mapped": rows_report,
        "registry_seeded_new": seeded,
        "pre_status_counts": pre_status,
        "post_sm_counts": post_sm,
        "counts_verified": bool(ok),
        "venue_seed_mode": venue_seed_mode,
    }


def format_report(rep: Dict[str, object]) -> str:
    lines = [
        f"migrate_v3 — {rep['db']} (dry_run={rep['dry_run']})",
        f"columns added: {', '.join(rep['columns_added']) or '(none — already migrated)'}",
        f"registry seeded (new oids): {rep['registry_seeded_new']}",
        f"sl_placed_px seed mode (EF5/§10): {rep.get('venue_seed_mode', '?')}",
        "",
        "| id | coin | legacy status | sm_state | note |",
        "|---|---|---|---|---|",
    ]
    for r in rep["rows_mapped"]:
        lines.append(f"| {r['id']} | {r['coin']} | {r['status']} | "
                     f"{r['sm_state']} | {r['note']} |")
    if not rep["rows_mapped"]:
        lines.append("| - | - | - | - | no rows needed mapping (idempotent re-run) |")
    lines += [
        "",
        f"pre  counts by status  : {rep['pre_status_counts']}",
        f"post counts by sm_state: {rep['post_sm_counts']}",
        f"COUNTS VERIFIED: {'GREEN' if rep['counts_verified'] else 'RED'}",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="P3 additive trades.db migration (entry-SM §2). Idempotent.")
    ap.add_argument("--db", required=True, help="path to the venue trades.db")
    ap.add_argument("--legacy-registry", default=None,
                    help="path to HL placed_trigger_oids.json (auto-detected "
                         "next to --db when omitted)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report only; roll back all writes")
    args = ap.parse_args(argv)
    rep = migrate(args.db, legacy_registry_json=args.legacy_registry,
                  dry_run=args.dry_run)
    print(format_report(rep))
    return 0 if rep["counts_verified"] else 1


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
