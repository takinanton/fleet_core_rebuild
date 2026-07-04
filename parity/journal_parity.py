#!/usr/bin/env python3
"""journal_parity.py — old-vs-new parity harness for bot/journal.py -> fleet_core/journal.py.

Run ON EACH BOT HOST inside its venv (stdlib only):

    python journal_parity.py --old /path/to/bot/journal.py --new /path/to/fleet_core/journal.py

NO network, NO live exchange, NO writes outside a private tempdir.
Loads BOTH files under synthetic module names with a FAKE 'bot'/'bot.config' injected into
sys.modules (mimicking the only config attribute journal.py uses: DB_PATH, a pathlib.Path —
same shape as every venue's bot/config.py `DB_PATH: Path = PROJECT_ROOT / "data" / "trades.db"`,
pointed into the tempdir). _now_iso is monkeypatched to a deterministic counter on both sides
so timestamps are bit-identical.

Checks:
  A. init_db on an EMPTY db: full schema compare (PRAGMA table_info of trades /
     rejected_signals / bot_state + index DDL). NEW may exceed OLD only by the documented
     APPENDED tail columns atr14 REAL / entry_bar_ts INTEGER on trades (NULL-able, no default);
     shared-column ORDER must be identical.
  B. LEGACY-db migration sim: build the pre-v2 legacy trades table (base columns common to all
     venues, i.e. everything NOT in any venue's additive migration list) + one legacy row; run
     old vs new init_db on identical copies; assert identical column-name->(type,notnull,dflt,pk)
     maps except the documented tail; legacy row survives byte-identically; init_db idempotent
     (second run changes nothing). Order compared name-set-wise here: additive ALTER order may
     legitimately differ (nado's old list had entry_intended last) — the dossier proves no
     consumer uses positional row access.
  C. Op-sequence parity: identical deterministic call sequences (insert_trade, insert_pending,
     promote_pending with AND without sl_initial when BOTH sides support it, delete_pending,
     pending_trades, update_trade_sl/_sl_order/_tp_order, mark_tp1_partial, close_trade,
     insert_rejected incl. reason[:500] truncation, set_state/get_state, open_trades,
     coins_ever_traded/oids_ever_placed/register_placed_trigger_oid when BOTH have them)
     against each side's own temp DB; dump ALL rows of all tables as name->value dicts and
     assert equality on every SHARED column; assert NEW-only columns are exactly a subset of
     {atr14, entry_bar_ts} and NULL wherever the old code could not have written them.
     Canonical-only superset helpers additionally smoke-run on NEW with sanity asserts.

Prints PASS/FAIL lines; exit 0 iff all PASS.
"""
from __future__ import annotations

import argparse
import importlib.util
import inspect
import shutil
import sqlite3
import sys
import tempfile
import types
from pathlib import Path

ALLOWED_NEW_COLS = {"atr14": "REAL", "entry_bar_ts": "INTEGER"}

FAILURES = []


def check(name: str, ok: bool, detail: str = "") -> None:
    line = f"{'PASS' if ok else 'FAIL'}: {name}"
    if detail and not ok:
        line += f" — {detail}"
    print(line)
    if not ok:
        FAILURES.append(name)


# ── module loading with fake bot.config ─────────────────────────────────────────────

def load_journal(src: Path, mod_name: str, db_path: Path):
    fake_bot = types.ModuleType("bot")
    fake_bot.__path__ = []  # mark as package
    fake_cfg = types.ModuleType("bot.config")
    fake_cfg.DB_PATH = db_path
    sys.modules["bot"] = fake_bot
    sys.modules["bot.config"] = fake_cfg
    fake_bot.config = fake_cfg
    spec = importlib.util.spec_from_file_location(mod_name, src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)

    counter = {"n": 0}

    def _fake_now_iso() -> str:
        counter["n"] += 1
        return f"2026-01-01T00:00:{counter['n'] % 60:02d}.{counter['n']:06d}+00:00"

    mod._now_iso = _fake_now_iso
    return mod


# ── schema helpers ───────────────────────────────────────────────────────────────────

def table_info(db: Path, table: str):
    con = sqlite3.connect(db)
    try:
        return [tuple(r) for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    finally:
        con.close()


def index_ddl(db: Path):
    con = sqlite3.connect(db)
    try:
        return sorted(
            (r[0], r[1] or "")
            for r in con.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        )
    finally:
        con.close()


def col_map(info):
    return {r[1]: (r[2], r[3], r[4], r[5]) for r in info}


def compare_schema(tag: str, old_db: Path, new_db: Path, order_strict: bool) -> None:
    oi, ni = table_info(old_db, "trades"), table_info(new_db, "trades")
    onames = [r[1] for r in oi]
    nnames = [r[1] for r in ni]
    extra = [c for c in nnames if c not in onames]
    missing = [c for c in onames if c not in nnames]
    check(f"{tag}: trades no columns lost", not missing, f"missing in new: {missing}")
    check(
        f"{tag}: trades new-only columns are the documented additive tail",
        all(c in ALLOWED_NEW_COLS for c in extra),
        f"unexpected new columns: {[c for c in extra if c not in ALLOWED_NEW_COLS]}",
    )
    if extra:
        tail = nnames[-len(extra):]
        check(
            f"{tag}: trades new columns appended at TAIL",
            sorted(tail) == sorted(extra),
            f"extra={extra} tail={tail}",
        )
        for c in extra:
            typ, notnull, dflt, pk = col_map(ni)[c]
            check(
                f"{tag}: trades.{c} is {ALLOWED_NEW_COLS[c]} NULL-able no-default",
                typ == ALLOWED_NEW_COLS[c] and notnull == 0 and dflt is None and pk == 0,
                f"got type={typ} notnull={notnull} dflt={dflt} pk={pk}",
            )
    om, nm = col_map(oi), col_map(ni)
    bad_defs = {c: (om[c], nm[c]) for c in onames if c in nm and om[c] != nm[c]}
    check(f"{tag}: trades shared column definitions identical", not bad_defs, str(bad_defs))
    if order_strict:
        shared_new_order = [c for c in nnames if c in onames]
        check(f"{tag}: trades shared column ORDER identical", shared_new_order == onames,
              f"old={onames} new-shared={shared_new_order}")
    for t in ("rejected_signals", "bot_state"):
        check(f"{tag}: {t} schema identical",
              table_info(old_db, t) == table_info(new_db, t),
              f"old={table_info(old_db, t)} new={table_info(new_db, t)}")
    check(f"{tag}: index DDL identical", index_ddl(old_db) == index_ddl(new_db),
          f"old={index_ddl(old_db)} new={index_ddl(new_db)}")


# ── row dump helpers ─────────────────────────────────────────────────────────────────

def dump_rows(db: Path, table: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()]
    finally:
        con.close()


def compare_rows(tag: str, old_db: Path, new_db: Path, atr_written: bool) -> None:
    for table in ("trades", "rejected_signals", "bot_state"):
        orows, nrows = dump_rows(old_db, table), dump_rows(new_db, table)
        check(f"{tag}: {table} row count equal ({len(orows)})", len(orows) == len(nrows),
              f"old={len(orows)} new={len(nrows)}")
        for i, (o, n) in enumerate(zip(orows, nrows)):
            shared = set(o) & set(n)
            diffs = {k: (o[k], n[k]) for k in shared if o[k] != n[k]}
            check(f"{tag}: {table} row{i} shared columns bit-identical", not diffs, str(diffs))
            new_only = set(n) - set(o)
            check(f"{tag}: {table} row{i} new-only cols ⊆ {{atr14,entry_bar_ts}}",
                  new_only <= set(ALLOWED_NEW_COLS), str(new_only - set(ALLOWED_NEW_COLS)))
            if not atr_written:
                nn = {k: n[k] for k in new_only if n[k] is not None}
                check(f"{tag}: {table} row{i} new-only cols all NULL", not nn, str(nn))


# ── scenario B legacy schema (pre-v2 base: columns absent from every migration list) ──

LEGACY_SQL = """
CREATE TABLE trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    coin TEXT NOT NULL,
    tf TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'long',
    entry REAL NOT NULL,
    sl_initial REAL NOT NULL,
    size REAL NOT NULL,
    risk_dollars REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    entry_order_id TEXT,
    sl_order_id TEXT,
    opened_at TEXT,
    closed_at TEXT,
    exit_price REAL,
    exit_reason TEXT,
    pnl_dollars REAL,
    notes TEXT
);
"""


def make_legacy_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.executescript(LEGACY_SQL)
        con.execute(
            "INSERT INTO trades (created_at, coin, tf, direction, entry, sl_initial, size,"
            " risk_dollars, status, opened_at, notes) VALUES"
            " ('2025-11-01T00:00:00+00:00','BTC','4h','long',50000.0,49000.0,0.5,500.0,"
            "'closed','2025-11-01T00:00:01+00:00','legacy-row')"
        )
        con.commit()
    finally:
        con.close()


# ── scenario C op sequence ───────────────────────────────────────────────────────────

def run_ops(mod, db: Path, use_sl_initial: bool, use_atr: bool, use_registry: bool):
    """Identical deterministic sequence on both sides (branch flags = capability
    INTERSECTION of old and new, so old and new always execute the same calls)."""
    out = {}
    mod.init_db(db)

    tid1 = mod.insert_trade(
        coin="BTC", tf="4h", entry=50000.5, sl_initial=49000.25, tp1=52000.75, size=0.5,
        risk_dollars=500.125, notional=25000.25, entry_intended=50010.0,
        walk_slip_pct=0.0002, entry_order_id="oid-entry-1", notes="parity-note",
        direction="short", db_path=db,
    )
    out["tid1"] = tid1
    mod.update_trade_sl(tid1, 49500.0, db_path=db)
    mod.update_trade_sl_order(tid1, "oid-sl-1", db_path=db)
    mod.update_trade_tp_order(tid1, "oid-tp-1", db_path=db)
    mod.mark_tp1_partial(tid1, 52000.75, 0.25, 50000.5, db_path=db)
    mod.close_trade(tid1, 51000.0, "tp", 250.0, 0.5, db_path=db)

    pid = mod.insert_pending(
        coin="ETH", tf="1d", direction="long", entry_intended=3000.0, sl_initial=2900.0,
        tp1=3200.0, size=1.5, risk_dollars=150.0, notional=4500.0, db_path=db,
    )
    out["pid"] = pid
    mod.promote_pending(pid, entry=3001.5, size=1.4, risk_dollars=149.0, notional=4202.1,
                        walk_slip_pct=0.0005, notes="promoted", db_path=db)

    pid2 = mod.insert_pending(
        coin="SOL", tf="4h", direction="short", entry_intended=100.0, sl_initial=104.0,
        tp1=95.0, size=10.0, risk_dollars=40.0, notional=1000.0, db_path=db,
    )
    out["pid2"] = pid2
    mod.delete_pending(pid2, db_path=db)

    pid3 = mod.insert_pending(
        coin="XRP", tf="1d", direction="long", entry_intended=2.0, sl_initial=1.9,
        tp1=2.2, size=1000.0, risk_dollars=100.0, notional=2000.0, db_path=db,
    )
    out["pid3"] = pid3
    pend = [dict(r) for r in mod.pending_trades(db_path=db)]
    out["pending_names"] = sorted({k for d in pend for k in d})
    out["pending_vals"] = [
        {k: d[k] for k in sorted(d) if k not in ALLOWED_NEW_COLS} for d in pend
    ]

    if use_sl_initial:
        mod.promote_pending(pid3, entry=2.01, size=990.0, sl_initial=1.895,
                            walk_slip_pct=0.005, notes="promoted-liqguard", db_path=db)
    else:
        mod.promote_pending(pid3, entry=2.01, size=990.0,
                            walk_slip_pct=0.005, notes="promoted-plain", db_path=db)

    # idempotence guards: re-promote / delete of a non-pending row must be no-ops
    mod.promote_pending(pid, entry=9999.0, size=9999.0, db_path=db)
    mod.delete_pending(pid, db_path=db)

    mod.insert_rejected(
        coin="DOGE", tf="4h", trigger_price=0.1, entry_price=0.101, sl_price=0.098,
        reason="R" * 600, walk_slip_pct=0.001, vol_1h_usd=123456.0, direction="short",
        db_path=db,
    )
    mod.insert_rejected("PEPE", "1d", None, None, None, "no-liq", db_path=db)

    mod.set_state("parity_key", "parity_value")
    out["state1"] = mod.get_state("parity_key")
    out["state2"] = mod.get_state("missing_key", "dflt")

    out["open_vals"] = [
        {k: d[k] for k in sorted(d) if k not in ALLOWED_NEW_COLS}
        for d in (dict(r) for r in mod.open_trades(db_path=db))
    ]

    if use_registry:
        mod.register_placed_trigger_oid("trig-oid-1")
        mod.register_placed_trigger_oid("trig-oid-1")  # idempotent append-only no-op
        mod.register_placed_trigger_oid(None)
        mod.register_placed_trigger_oid("")
        out["oids"] = sorted(mod.oids_ever_placed(db_path=db))
        out["coins"] = sorted(mod.coins_ever_traded(db_path=db))

    if use_atr:
        out["tid_atr"] = mod.insert_trade(
            coin="LINK", tf="4h", entry=20.0, sl_initial=19.0, tp1=22.0, size=5.0,
            risk_dollars=5.0, notional=100.0, direction="long",
            atr14=1.2345, entry_bar_ts=1700000000000, db_path=db,
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True, type=Path)
    ap.add_argument("--new", required=True, type=Path)
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="journal_parity_"))
    try:
        (tmp / "old").mkdir()
        (tmp / "new").mkdir()
        old_default_db = tmp / "old" / "trades.db"
        new_default_db = tmp / "new" / "trades.db"

        old = load_journal(args.old, "journal_old", old_default_db)
        new = load_journal(args.new, "journal_new", new_default_db)

        def has_param(mod, fn, p):
            return hasattr(mod, fn) and p in inspect.signature(getattr(mod, fn)).parameters

        both_sl = has_param(old, "promote_pending", "sl_initial") and has_param(new, "promote_pending", "sl_initial")
        both_atr = has_param(old, "insert_trade", "atr14") and has_param(new, "insert_trade", "atr14")
        both_reg = all(hasattr(m, "register_placed_trigger_oid") for m in (old, new))
        print(f"# capability intersection: sl_initial={both_sl} atr14={both_atr} registry={both_reg}")
        check("canonical (new) exports superset",
              all(hasattr(new, n) for n in (
                  "init_db", "get_state", "set_state", "insert_trade", "insert_pending",
                  "promote_pending", "delete_pending", "pending_trades", "update_trade_sl",
                  "update_trade_sl_order", "update_trade_tp_order", "mark_tp1_partial",
                  "close_trade", "open_trades", "coins_ever_traded", "oids_ever_placed",
                  "register_placed_trigger_oid", "_load_placed_trigger_oids", "insert_rejected")))
        check("no old export lost in new",
              not [n for n in dir(old) if not n.startswith("__") and callable(getattr(old, n))
                   and not hasattr(new, n)],
              str([n for n in dir(old) if not n.startswith("__") and callable(getattr(old, n))
                   and not hasattr(new, n)]))

        # A. fresh init_db schema
        a_old, a_new = tmp / "a_old.db", tmp / "a_new.db"
        old.init_db(a_old)
        new.init_db(a_new)
        compare_schema("A fresh", a_old, a_new, order_strict=True)

        # B. legacy migration
        b_old, b_new = tmp / "b_old.db", tmp / "b_new.db"
        make_legacy_db(b_old)
        shutil.copyfile(b_old, b_new)
        old.init_db(b_old)
        new.init_db(b_new)
        compare_schema("B legacy", b_old, b_new, order_strict=False)
        orow = dump_rows(b_old, "trades")
        nrow = dump_rows(b_new, "trades")
        check("B legacy: legacy row survives identically (shared cols)",
              len(orow) == len(nrow) == 1
              and {k: nrow[0][k] for k in orow[0] if k in nrow[0]} == orow[0],
              f"old={orow} new={nrow}")
        snap_o, snap_n = table_info(b_old, "trades"), table_info(b_new, "trades")
        old.init_db(b_old)
        new.init_db(b_new)
        check("B legacy: init_db idempotent (old)", table_info(b_old, "trades") == snap_o)
        check("B legacy: init_db idempotent (new)", table_info(b_new, "trades") == snap_n)

        # C. op-sequence parity (default-path get/set_state hit the per-module default DBs)
        ro = run_ops(old, old_default_db, both_sl, both_atr, both_reg)
        rn = run_ops(new, new_default_db, both_sl, both_atr, both_reg)
        check("C: returned ids identical",
              all(ro[k] == rn[k] for k in ("tid1", "pid", "pid2", "pid3")),
              f"old={[ro[k] for k in ('tid1', 'pid', 'pid2', 'pid3')]}"
              f" new={[rn[k] for k in ('tid1', 'pid', 'pid2', 'pid3')]}")
        check("C: state get/set identical",
              ro["state1"] == rn["state1"] == "parity_value"
              and ro["state2"] == rn["state2"] == "dflt")
        check("C: pending_trades column names — old ⊆ new, extras documented",
              set(ro["pending_names"]) <= set(rn["pending_names"])
              and set(rn["pending_names"]) - set(ro["pending_names"]) <= set(ALLOWED_NEW_COLS),
              f"old={ro['pending_names']} new={rn['pending_names']}")
        shared_names = set(ro["pending_names"]) & set(rn["pending_names"])
        check("C: pending_trades shared-column values identical",
              [{k: d[k] for k in d if k in shared_names} for d in ro["pending_vals"]]
              == [{k: d[k] for k in d if k in shared_names} for d in rn["pending_vals"]],
              f"old={ro['pending_vals']} new={rn['pending_vals']}")
        check("C: open_trades shared-column values identical",
              [{k: d[k] for k in d if k in shared_names} for d in ro["open_vals"]]
              == [{k: d[k] for k in d if k in shared_names} for d in rn["open_vals"]],
              f"old={ro['open_vals']} new={rn['open_vals']}")
        if both_reg:
            check("C: oids_ever_placed identical", ro["oids"] == rn["oids"],
                  f"old={ro['oids']} new={rn['oids']}")
            check("C: coins_ever_traded identical", ro["coins"] == rn["coins"],
                  f"old={ro['coins']} new={rn['coins']}")
        else:
            # canonical-only superset helpers: smoke-run on NEW with sanity asserts
            new.register_placed_trigger_oid("trig-oid-1")
            new.register_placed_trigger_oid("trig-oid-1")
            new.register_placed_trigger_oid(None)
            new.register_placed_trigger_oid("")
            oids = new.oids_ever_placed(db_path=new_default_db)
            coins = new.coins_ever_traded(db_path=new_default_db)
            check("C: canonical oids_ever_placed sane (new-only smoke)",
                  oids >= {"trig-oid-1", "oid-sl-1"} and "" not in oids and "None" not in oids,
                  f"got {sorted(oids)}")
            # SOL pending row was delete_pending'ed -> must NOT appear
            check("C: canonical coins_ever_traded sane (new-only smoke)",
                  coins >= {"BTC", "ETH", "XRP"} and "SOL" not in coins, f"got {sorted(coins)}")
        if both_atr:
            check("C: atr14-variant insert id identical", ro["tid_atr"] == rn["tid_atr"])
        compare_rows("C", old_default_db, new_default_db, atr_written=both_atr)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'ALL PASS' if not FAILURES else f'{len(FAILURES)} FAILURES: ' + ', '.join(FAILURES)}")
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
