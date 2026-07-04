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

from bot.config import DB_PATH

log = logging.getLogger(__name__)


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
                opened_at TEXT,
                closed_at TEXT,
                exit_price REAL,
                exit_reason TEXT,
                pnl_dollars REAL,
                realized_r REAL,
                notes TEXT
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
            ("realized_r", "REAL"),
            ("sl_current", "REAL"),
            ("tp1", "REAL"),
            ("notional", "REAL"),
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
    walk_slip_pct: Optional[float] = None,
    entry_order_id: Optional[str] = None,
    notes: Optional[str] = None,
    direction: str = "long",
    db_path: Path = DB_PATH,
) -> int:
    """Insert a new open trade. Returns trade id."""
    with _conn(db_path) as con:
        cur = con.execute(
            """
            INSERT INTO trades (
                created_at, coin, tf, direction, entry, sl_initial, sl_current,
                tp1, size, risk_dollars, notional, walk_slip_pct,
                status, entry_order_id, opened_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)
            """,
            (
                _now_iso(), coin, tf, direction,
                entry, sl_initial, sl_initial,
                tp1, size, risk_dollars, notional, walk_slip_pct,
                entry_order_id, _now_iso(), notes,
            ),
        )
        trade_id = cur.lastrowid
    log.info("DB insert_trade id=%d %s %s entry=%.6f sl=%.6f tp1=%.6f", trade_id, coin, tf, entry, sl_initial, tp1)
    return trade_id


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
