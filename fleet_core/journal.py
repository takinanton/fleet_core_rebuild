"""journal.py — SQLite trade log and rejected_signals log.

Schema reused from old nado_bot journal.py (source: backup bot/journal.py).
Added: v2-specific columns (tf, sl_initial, tp1, walk_slip_pct).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import os

from bot.config import DB_PATH

log = logging.getLogger(__name__)

# ── Append-only placed-SL/TP-oid registry (Audit MED 2026-06-20) ────────────────────────
# The orphan-trigger sweep (orphan_sweep.sweep_orphan_triggers) disambiguates a BOT-placed
# reduce-only SL/TP TRIGGER from a deliberate MANUAL ЮК one on the SAME shared unified account
# by the trigger's OWN oid: bot-placed -> sweepable, manual -> FENCED (survives). The DB
# sl_order_id/tp1_order_id COLUMNS only hold the LATEST oid (update_trade_*_order OVERWRITES),
# so a ROTATED chandelier-trail oid that stayed resting after a cancel-old blip leaves the
# column set and would mis-read as MANUAL -> a bot-own orphan never swept. Mirror
# resting_orders.py's _placed_oids: persist EVERY placed SL/TP oid (including every
# chandelier-trail re-place) to an APPEND-ONLY JSON registry that is never overwritten, and
# oids_ever_placed() unions it with the DB columns. A bot-own rotated oid is then ALWAYS in
# the registry -> sweepable; a manual oid is never registered -> fenced.
_PLACED_TRIGGER_OIDS_PATH: Path = DB_PATH.parent / "placed_trigger_oids.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _conn(db_path: Path = DB_PATH) -> Iterator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db(db_path: Path = DB_PATH) -> None:
    with _conn(db_path) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                coin TEXT NOT NULL,
                tf TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'long',
                entry REAL NOT NULL,
                entry_intended REAL,            -- pre-fill signal target px; exec-slip=(entry-entry_intended)/entry_intended
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
                notes TEXT,
                -- Audit MED (2026-06-19): persist the FROZEN signal-bar Wilder ATR14 and the
                -- ENTRY-bar ts (ms) so the chandelier ATR_e and the donchian 120-bar time-stop
                -- are RESTART-PERSISTENT. Without these the restore path re-seeds neither, so
                -- _ensure_state hits its lazy fallback (ATR recomputed off the latest closed
                -- bar -> trail width changes after a restart) and _donch_entry_ts defaults to 0
                -- (the in-memory bars-held counter resets to 0 -> the full 120-bar clock re-arms
                -- and the position can overstay its time-stop after any restart).
                atr14 REAL,
                entry_bar_ts INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_trades_coin_tf
                ON trades(coin, tf, status);

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

            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        # Migrate: add columns added in v2 if DB pre-exists
        existing = {r["name"] for r in con.execute("PRAGMA table_info(trades)").fetchall()}
        for col, definition in [
            ("walk_slip_pct", "REAL"),
            ("entry_intended", "REAL"),
            ("realized_r", "REAL"),
            ("sl_current", "REAL"),
            ("tp1", "REAL"),
            ("notional", "REAL"),
            ("tp1_order_id", "TEXT"),
            ("tp1_partial_done", "INTEGER NOT NULL DEFAULT 0"),
            ("tp1_fill_price", "REAL"),
            ("atr14", "REAL"),                # frozen signal-bar Wilder ATR14 (restart-persist)
            ("entry_bar_ts", "INTEGER"),      # entry/signal bar ts ms (restart-persist time-stop)
        ]:
            if col not in existing:
                con.execute(f"ALTER TABLE trades ADD COLUMN {col} {definition}")
                log.info("DB migrated: trades.%s added", col)

    log.info("DB initialized at %s", db_path)


# ---------- State helpers ----------

def get_state(key: str, default: Optional[str] = None) -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_state(key: str, value: str) -> None:
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO bot_state(key, value) VALUES (?, ?)",
            (key, value),
        )


# ---------- Trade insert / update ----------

def insert_trade(
    coin: str,
    tf: str,
    entry: float,
    sl_initial: float,
    tp1: float,
    size: float,
    risk_dollars: float,
    notional: float,
    entry_intended: Optional[float] = None,
    walk_slip_pct: Optional[float] = None,
    entry_order_id: Optional[str] = None,
    notes: Optional[str] = None,
    direction: str = "long",
    atr14: Optional[float] = None,
    entry_bar_ts: Optional[int] = None,
    db_path: Path = DB_PATH,
) -> int:
    """Insert a new open trade. Returns trade id.

    atr14 / entry_bar_ts (Audit MED 2026-06-19): the frozen signal-bar Wilder ATR14 and the
    entry/signal-bar ts (ms). Persisted so the restore/adopt path can re-seed the chandelier
    ATR_e and the donchian 120-bar time-stop after a restart (the in-memory stash set by
    trader.attempt_entry is lost on restart). Optional — legacy callers omit them (NULL rows
    fall back to the strategy's lazy recompute / in-memory bars counter, as before).
    """
    with _conn(db_path) as con:
        cur = con.execute(
            """
            INSERT INTO trades (
                created_at, coin, tf, direction, entry, entry_intended, sl_initial, sl_current,
                tp1, size, risk_dollars, notional, walk_slip_pct,
                status, entry_order_id, opened_at, notes, atr14, entry_bar_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
            """,
            (
                _now_iso(), coin, tf, direction,
                entry, entry_intended, sl_initial, sl_initial,
                tp1, size, risk_dollars, notional, walk_slip_pct,
                entry_order_id, _now_iso(), notes, atr14, entry_bar_ts,
            ),
        )
        trade_id = cur.lastrowid
    log.info("DB insert_trade id=%d %s %s entry=%.6f sl=%.6f tp1=%.6f", trade_id, coin, tf, entry, sl_initial, tp1)
    return trade_id


# ---------- write-db-row-PRE-order (panel must-fix 2026-06-21) ----------
# Journal a row with status='pending' BEFORE the entry order is submitted, then promote it to
# 'open' on fill (or delete it on no-fill). A crash between order-submit and journal then leaves
# a recoverable 'pending' trace that _reconcile_pending(startup) promotes (if a live position
# exists) or deletes — closing the crash-mid-entry naked window at the SOURCE. 'pending' rows are
# NOT 'open' → invisible to open_trades()/adopt/XNN_EXPECT_EMPTY_DB, so a leftover pending row
# never affects trading; the worst case is a stale row cleaned on the next restart reconcile.

def insert_pending(
    coin: str, tf: str, direction: str, entry_intended: float, sl_initial: float,
    tp1: float, size: float, risk_dollars: float, notional: float,
    atr14: Optional[float] = None, entry_bar_ts: Optional[int] = None,
    db_path: Path = DB_PATH,
) -> int:
    """Insert a status='pending' pre-order row. entry is the INTENDED px (placeholder, NOT NULL);
    promote_pending overwrites it with the actual fill. Returns row id."""
    with _conn(db_path) as con:
        cur = con.execute(
            """
            INSERT INTO trades (
                created_at, coin, tf, direction, entry, entry_intended, sl_initial, sl_current,
                tp1, size, risk_dollars, notional, status, opened_at, atr14, entry_bar_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)
            """,
            (
                _now_iso(), coin, tf, direction, entry_intended, entry_intended,
                sl_initial, sl_initial, tp1, size, risk_dollars, notional, atr14, entry_bar_ts,
            ),
        )
        trade_id = cur.lastrowid
    log.info("DB insert_pending id=%d %s %s (pre-order; promote-on-fill / delete-on-no-fill)",
             trade_id, coin, tf)
    return trade_id


def promote_pending(
    trade_id: int, entry: float, size: float,
    risk_dollars: Optional[float] = None, notional: Optional[float] = None,
    walk_slip_pct: Optional[float] = None, notes: Optional[str] = None,
    sl_initial: Optional[float] = None,
    db_path: Path = DB_PATH,
) -> None:
    """Promote a 'pending' row to 'open' with the ACTUAL fill. Guarded WHERE status='pending'
    so it is idempotent and never re-opens a closed row. sl_initial (optional) records the
    liq-guard-adjusted SL that actually shipped (sets sl_current to it too)."""
    sets = ["status='open'", "opened_at=?", "entry=?", "size=?"]
    vals: list = [_now_iso(), entry, size]
    if sl_initial is not None:
        sets.append("sl_initial=?"); vals.append(sl_initial)
        sets.append("sl_current=?"); vals.append(sl_initial)
    else:
        sets.append("sl_current=sl_initial")
    if risk_dollars is not None: sets.append("risk_dollars=?"); vals.append(risk_dollars)
    if notional is not None: sets.append("notional=?"); vals.append(notional)
    if walk_slip_pct is not None: sets.append("walk_slip_pct=?"); vals.append(walk_slip_pct)
    if notes is not None: sets.append("notes=?"); vals.append(notes)
    vals.append(trade_id)
    with _conn(db_path) as con:
        con.execute(f"UPDATE trades SET {', '.join(sets)} WHERE id=? AND status='pending'", vals)
    log.info("DB promote_pending id=%d -> open entry=%.6f size=%s", trade_id, entry, size)


def delete_pending(trade_id: int, db_path: Path = DB_PATH) -> None:
    """Delete a 'pending' row (no-fill / aborted entry). Guarded WHERE status='pending' so it can
    NEVER delete a promoted (open) or closed trade."""
    with _conn(db_path) as con:
        con.execute("DELETE FROM trades WHERE id=? AND status='pending'", (trade_id,))


def pending_trades(db_path: Path = DB_PATH) -> list[sqlite3.Row]:
    with _conn(db_path) as con:
        return con.execute(
            "SELECT * FROM trades WHERE status='pending' ORDER BY id"
        ).fetchall()


def update_trade_sl(trade_id: int, new_sl: float, db_path: Path = DB_PATH) -> None:
    with _conn(db_path) as con:
        con.execute(
            "UPDATE trades SET sl_current=? WHERE id=?",
            (new_sl, trade_id),
        )


def update_trade_sl_order(trade_id: int, sl_order_id: str, db_path: Path = DB_PATH) -> None:
    with _conn(db_path) as con:
        con.execute(
            "UPDATE trades SET sl_order_id=? WHERE id=?",
            (sl_order_id, trade_id),
        )


def update_trade_tp_order(trade_id: int, tp1_order_id: Optional[str], db_path: Path = DB_PATH) -> None:
    with _conn(db_path) as con:
        con.execute(
            "UPDATE trades SET tp1_order_id=? WHERE id=?",
            (tp1_order_id, trade_id),
        )


def mark_tp1_partial(
    trade_id: int,
    tp1_fill_price: float,
    remaining_size: float,
    new_sl: float,
    db_path: Path = DB_PATH,
) -> None:
    """Record the partial TP1 fill: flag done, store fill px, resize to remainder, BE-move SL.

    tp1_order_id is cleared (the resting limit was consumed by the fill).
    """
    with _conn(db_path) as con:
        con.execute(
            """
            UPDATE trades
            SET tp1_partial_done=1, tp1_fill_price=?, size=?, sl_current=?, tp1_order_id=NULL
            WHERE id=?
            """,
            (tp1_fill_price, remaining_size, new_sl, trade_id),
        )
    log.info(
        "DB mark_tp1_partial id=%d fill=%.6f remaining_size=%s be_sl=%.6f",
        trade_id, tp1_fill_price, remaining_size, new_sl,
    )


def close_trade(
    trade_id: int,
    exit_price: float,
    exit_reason: str,
    pnl_dollars: float,
    realized_r: float,
    db_path: Path = DB_PATH,
) -> None:
    with _conn(db_path) as con:
        con.execute(
            """
            UPDATE trades
            SET status='closed', closed_at=?, exit_price=?, exit_reason=?,
                pnl_dollars=?, realized_r=?
            WHERE id=?
            """,
            (_now_iso(), exit_price, exit_reason, pnl_dollars, realized_r, trade_id),
        )
    log.info(
        "DB close_trade id=%d reason=%s exit=%.6f pnl=%.2f r=%.2fR",
        trade_id, exit_reason, exit_price, pnl_dollars, realized_r,
    )


def open_trades(db_path: Path = DB_PATH) -> list[sqlite3.Row]:
    with _conn(db_path) as con:
        return con.execute(
            "SELECT * FROM trades WHERE status='open' ORDER BY opened_at"
        ).fetchall()


def coins_ever_traded(db_path: Path = DB_PATH) -> set:
    """Distinct coins with ANY trades.db row (status open OR closed).

    Audit HIGH (2026-06-19): the orphan-trigger sweep disambiguates a bot-OWNED
    xyz_ position from a MANUAL UK xyz_ position. An *open*-row check (open_trades)
    is the wrong signal there: an orphan reduce-only TRIGGER by definition appears
    AFTER its position closed, so the bot-own row has already flipped to 'closed'
    and a closed-bot-own xyz_ coin becomes indistinguishable from a manual one by
    the open-row test -> the sweep is neutered for the whole bot-owned leg. The
    POSITIVE, time-invariant marker is "has the bot ever traded this coin at all":
    a bot-owned coin always has >=1 row (open or closed); a manual position the
    bot never opened has NONE. Used by orphan_sweep._fenced to keep bot-own xyz_
    sweepable while exempting manual xyz_.
    """
    with _conn(db_path) as con:
        return {
            str(r["coin"])
            for r in con.execute("SELECT DISTINCT coin FROM trades").fetchall()
        }


def _load_placed_trigger_oids(path: Path = _PLACED_TRIGGER_OIDS_PATH) -> set:
    """Load the append-only placed-SL/TP-oid registry as a set of str. Fail-OPEN
    (empty set) on a missing/corrupt file: the sweep keeps its conservative coin-level
    fallback fence, so a registry read failure never makes the sweep cancel a manual
    trigger — it only foregoes the per-oid bot-own discriminator for this read."""
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            return {str(o) for o in (raw.get("oids", []) or []) if str(o) != ""}
    except Exception as e:
        log.warning("placed-trigger-oid registry: load failed (%s) — empty", e)
    return set()


def register_placed_trigger_oid(oid, path: Path = _PLACED_TRIGGER_OIDS_PATH) -> None:
    """APPEND `oid` to the persistent placed-SL/TP-oid registry (never overwritten).

    MUST be called on EVERY reduce-only SL/TP place the bot performs — initial entry SL,
    naked-heal re-place, the restored-position SL reconcile, EVERY chandelier-trail SL
    re-place, and the partial-TP limit — so a bot-own oid that later orphans (incl. a
    rotated trail oid the cancel-old missed) is ALWAYS recognised as bot-placed by the
    orphan-trigger sweep and swept; a manual ЮК oid is never registered -> fenced.

    Idempotent + atomic (tmp + os.replace), mirroring resting_orders._save_registry.
    Best-effort: a registry-write failure is logged but never blocks order placement
    (the SL/TP is already live on the exchange; the missing registry entry only degrades
    the sweep to its conservative coin-level fallback for THIS oid, which errs FENCED)."""
    if oid is None:
        return
    oid_s = str(oid)
    if oid_s == "":
        return
    try:
        cur = _load_placed_trigger_oids(path)
        if oid_s in cur:
            return  # already registered — append-only no-op
        cur.add(oid_s)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"oids": sorted(cur)}), encoding="utf-8")
        os.replace(tmp, path)
    except Exception as e:
        log.warning("placed-trigger-oid registry: append failed for %s (%s)", oid_s, e)


def oids_ever_placed(db_path: Path = DB_PATH) -> set:
    """Distinct exchange order-ids the bot has EVER placed as a protective trigger
    (sl_order_id OR tp1_order_id), across ALL trades.db rows (open OR closed), UNIONed
    with the append-only placed_trigger_oids.json registry, as a set of str.

    Audit MED (2026-06-20): the coin-granularity bot-own marker (coins_ever_traded)
    mis-classifies a MANUAL ЮК reduce-only SL TRIGGER as bot-own whenever the bot has
    ALSO ever traded that SAME xyz_ symbol (the us29 leg owns the entire xyz_ HIP-3
    universe, and the manual workflow places deliberate reduce-only stop triggers on the
    SAME instruments on the SAME shared unified account). A coin-level any-row test is
    only unambiguous when the bot-traded and manual symbol sets are disjoint — they are
    NOT here, so a PRE-PLACED manual SL trigger awaiting fill on a bot-traded coin (no
    live position, no open row) would be swept = manual position goes naked.

    The unambiguous, time-invariant marker is PER-OID: the orphan-trigger's oid matched
    against the oids the bot itself registered when it placed its SL/TP (the same oid the
    HL API returns in frontendOpenOrders -> list_reduce_only_triggers, and the same value
    update_trade_sl_order/update_trade_tp_order persist). A trigger whose oid is in this
    set is provably bot-placed (sweepable); a trigger whose oid is NOT — on any coin,
    bot-traded or not — is foreign/manual and must never be cancelled by the sweep. This
    mirrors resting_orders.startup_orphan_sweep's _placed_oids registry approach.

    The DB sl_order_id/tp1_order_id COLUMNS hold only the LATEST oid per trade
    (update_trade_*_order OVERWRITES), so a ROTATED chandelier-trail oid that stayed
    resting after a cancel-old blip leaves the column set and would mis-read as MANUAL.
    The append-only placed_trigger_oids.json registry (register_placed_trigger_oid,
    appended on EVERY place incl. every trail re-place) closes that gap; this UNIONs both
    so a bot-own rotated oid is ALWAYS recognised.
    """
    with _conn(db_path) as con:
        rows = con.execute(
            "SELECT sl_order_id, tp1_order_id FROM trades"
        ).fetchall()
    out: set = _load_placed_trigger_oids()  # append-only registry (rotated-oid safe)
    for r in rows:
        for oid in (r["sl_order_id"], r["tp1_order_id"]):
            if oid is not None and str(oid) != "":
                out.add(str(oid))
    return out


# ---------- Rejected signals ----------

def insert_rejected(
    coin: str,
    tf: str,
    trigger_price: Optional[float],
    entry_price: Optional[float],
    sl_price: Optional[float],
    reason: str,
    walk_slip_pct: Optional[float] = None,
    vol_1h_usd: Optional[float] = None,
    direction: str = "long",
    db_path: Path = DB_PATH,
) -> None:
    with _conn(db_path) as con:
        con.execute(
            """
            INSERT INTO rejected_signals (
                created_at, coin, tf, direction,
                trigger_price, entry_price, sl_price, reason,
                walk_slip_pct, vol_1h_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _now_iso(), coin, tf, direction,
                trigger_price, entry_price, sl_price, reason[:500],
                walk_slip_pct, vol_1h_usd,
            ),
        )
