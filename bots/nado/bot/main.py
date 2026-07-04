"""main.py — main loop: scan → entry → manage → repeat.

Usage:
  python -m bot.main                # live trading
  python -m bot.main --dry-run      # no orders placed, signals logged
  python -m bot.main --once         # single loop iteration then exit
  python -m bot.universe --print    # print filtered universe and exit

Loop interval: 60s (source: settings.loop_interval_sec, user spec).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Dict

from bot.config import PROJECT_ROOT, Settings, settings
from bot.exchange_nado import NadoClient as NadoClient_
from bot.journal import init_db, insert_rejected, open_trades
from bot.liquidity import SnapshotHolder, load_snapshot
from bot.scanner import Scanner
# XNN port 2026-06-11: strategy module swapped uk_v102 -> xnn (contract-identical).
from bot.strategy_xnn import Position, PositionManager
from bot.trader import attempt_entry, manage_open_position
from bot.universe import AssetTier, load_universe
from bot.orphan_sweep import sweep_orphan_triggers
import os

CRYPTO_SHORT_ONLY = os.getenv("CRYPTO_SHORT_ONLY", "").strip().lower() in ("1", "true", "yes", "on")
_NON_CRYPTO = {"XAUT", "XAG", "WTI", "QQQ", "SPY"}  # commodities/indices kept bidirectional

def _no_long_symbols(universe) -> set:
    return {a.symbol for a in universe if a.symbol.replace("-PERP", "").replace("/", "").upper() not in _NON_CRYPTO}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _build_client(cfg: Settings) -> NadoClient_:
    return NadoClient_(cfg)


def _restore_positions_from_db(client) -> Dict[str, Position]:
    """Restore in-memory position state from DB on bot restart.

    On restart, open trades in DB that still appear in exchange open_positions
    are restored. This ensures SL management continues after crash/restart.
    """
    positions: Dict[str, Position] = {}
    # XNN port 2026-06-11 (HL review-fix class, foreign-close guard): in DRY_RUN
    # adopt/restore is SKIPPED entirely — manage_open_position places REAL orders
    # (heal SL, trail cancel/replace, emergency market-close); the only way a "DRY"
    # bot can touch the exchange is via an adopted position (stale/copied DB row,
    # manual uk_signal_place row). No adoption -> no manage -> zero orders in DRY,
    # enforced in CODE (plus _dry_block belt-and-braces in trader.py).
    if settings.dry_run:
        log.warning("DRY_RUN: position adopt/restore SKIPPED (no positions will be "
                    "managed; manage-path places real orders)")
        return positions
    try:
        db_open = open_trades()
    except Exception as e:
        log.error("open_trades() DB query failed: %s", e)
        return positions

    try:
        exchange_pos = client.open_positions()
        # STARTUP BLIP GUARD: a transient-empty open_positions() at the restart instant would
        # drop every DB-open position from restore (orphan until next restart, loses in-memory
        # SL-trailing). If empty while DB has open trades, re-fetch before trusting it.
        if db_open and not exchange_pos:
            for _i in range(3):
                time.sleep(3.0)
                try:
                    exchange_pos = client.open_positions()
                except Exception:
                    continue
                if exchange_pos:
                    log.info("restore: open_positions recovered on retry %d", _i + 1)
                    break
    except Exception as e:
        log.error("open_positions() on startup failed: %s", e)
        return positions

    for row in db_open:
        coin = row["coin"]
        if coin not in exchange_pos:
            # DELIST-PHANTOM class fix 2026-07-02 (xyz_INTC on HL): a DB-open row absent from
            # the exchange at restore was skipped forever (never entered memory -> the manage
            # phantom-guard never saw it). Resolve HERE via REAL fills; keep the row (loud) only
            # when no close fill exists. Startup-only: at runtime the K=3 phantom-guard owns this.
            try:
                from bot.trader import _lookup_real_close_px, _record_close, _cancel_orphan_triggers
                _gr = row
                _gd = _gr["direction"] if "direction" in _gr.keys() else "long"
                _ghost = Position(
                    coin=coin, tf=_gr["tf"], entry_price=_gr["entry"],
                    sl_initial=_gr["sl_initial"],
                    sl_current=_gr["sl_current"] or _gr["sl_initial"],
                    tp1_price=_gr["tp1"] or _gr["entry"], size=_gr["size"],
                    bar_entry_idx=0, side=_gd,
                )
                _real = _lookup_real_close_px(
                    client, _ghost, sl_oid=_gr["sl_order_id"],
                    tp_oid=(_gr["tp1_order_id"] if "tp1_order_id" in _gr.keys() else None))
                if _real is not None:
                    for _r2 in (row,):
                        _record_close(trade_id=_r2["id"], pos=_ghost,
                                      exit_price=_real[0], exit_reason=_real[1])
                    if not settings.dry_run:
                        try:
                            _cancel_orphan_triggers(client, coin)
                        except Exception:
                            pass
                    log.warning("restore-resolve %s: not on exchange — closed stale DB row(s) "
                                "@%.6f via REAL fills (%s)", coin, _real[0], _real[1])
                else:
                    log.critical("restore-resolve %s: not on exchange and NO close fill found — "
                                 "row(s) kept open, resolve manually", coin)
            except Exception as _rre:
                log.critical("restore-resolve %s failed (%s) — row(s) kept open", coin, _rre)
            continue
        # SHORT support 2026-05-27 — restore side from DB direction column.
        direction = row["direction"] if "direction" in row.keys() else "long"
        entry = row["entry"]
        sl_initial = row["sl_initial"]
        rr = settings.raw_rr_target
        if row["tp1"]:
            tp1_recovered = row["tp1"]
        elif direction == "long":
            tp1_recovered = entry + rr * (entry - sl_initial)
        else:
            tp1_recovered = entry - rr * (sl_initial - entry)
        cur_size = row["size"]
        keys = row.keys()
        tp1_done = bool(row["tp1_partial_done"]) if "tp1_partial_done" in keys else False
        tp1_oid = row["tp1_order_id"] if "tp1_order_id" in keys else None
        frac = settings.tp1_partial_frac
        pos = Position(
            coin=coin,
            tf=row["tf"],
            entry_price=entry,
            sl_initial=sl_initial,
            sl_current=row["sl_current"] or sl_initial,
            tp1_price=tp1_recovered,
            size=cur_size,
            bar_entry_idx=0,
            side=direction,
        )
        pos.tp1_partial_done = tp1_done
        pos.tp1_hit = tp1_done
        pos.__dict__["_trade_id"] = row["id"]
        pos.__dict__["_sl_order_id"] = row["sl_order_id"]
        pos.__dict__["_tp1_order_id"] = None if tp1_done else tp1_oid
        pos.__dict__["_tp1_frac"] = frac
        pos.__dict__["_orig_size"] = (cur_size / (1.0 - frac)) if (tp1_done and frac < 1.0) else cur_size
        positions[coin] = pos
        log.info(
            "Restored position %s %s: entry=%.6f sl=%.6f size=%s",
            coin, row["tf"], row["entry"], pos.sl_current, row["size"],
        )

    if positions:
        log.info("Restored %d open positions from DB", len(positions))
    return positions



def _reconcile_pending(client) -> None:
    """write-db-row-PRE-order reconcile (ported from hl_combo_bot, panel must-fix 2026-06-21).
    A status='pending' row is a pre-order trace written by attempt_entry BEFORE order submit
    that never reached promote — i.e. a crash between order-submit and journal-promote. At
    startup, for each pending row:
      * a LIVE exchange position exists on the coin AND no 'open' row already covers it
            -> PROMOTE to 'open' (best-effort intended entry/size) so the normal restore/adopt
               path manages it + heals its SL (closes the crash-mid-entry naked window).
      * else (no live position, or already covered by an open row)
            -> DELETE the stale pending row.
    DRY: skipped (no live orders → no pending rows). Any read failure leaves pending rows
    intact (conservative — a later restart retries)."""
    if settings.dry_run:
        return
    from bot.journal import delete_pending, pending_trades, promote_pending
    try:
        pend = pending_trades()
    except Exception as e:
        log.warning("reconcile_pending: pending_trades() failed: %s — skipping", e)
        return
    if not pend:
        return
    try:
        ex = client.open_positions() or {}
    except Exception as e:
        log.warning("reconcile_pending: open_positions() failed: %s — leaving %d pending "
                    "row(s) for the next restart", e, len(pend))
        return
    try:
        open_coins = {str(r["coin"]) for r in (open_trades() or [])}
    except Exception:
        open_coins = set()
    for r in pend:
        coin = str(r["coin"])
        live = (coin in ex) or (f"{coin}-USD" in ex)
        if live and coin not in open_coins:
            promote_pending(r["id"], entry=r["entry"], size=r["size"])
            open_coins.add(coin)
            log.critical(
                "RECONCILE-PENDING %s: crash-mid-entry pending row id=%s PROMOTED to open "
                "(live position found) — restore/adopt will heal its SL. Verify entry/size "
                "vs exchange.", coin, r["id"],
            )
        else:
            delete_pending(r["id"])
            log.warning("reconcile_pending: deleted stale pending %s id=%s (live=%s already_open=%s)",
                        coin, r["id"], live, coin in open_coins)


def _protect_untracked_positions(client, open_positions) -> None:
    """SAFETY NET (ported from hl_combo_bot root fix 2026-05-31): place a protective SL on any
    EXCHANGE position that is (a) not tracked in-memory or DB, (b) not covered by a live
    reduce-only trigger, and (c) not fenced as manual/foreign (FX_EXCLUDE /
    UNIVERSE_SYMBOL_EXCLUDE / _is_fx — reuses orphan_sweep._fenced so the fence set is
    single-source). A naked orphan can appear from a crash in the fill→journal window or a
    fill during a restart; it rides the protective SL until adopted/manually resolved.
    DRY: skipped entirely (places real orders)."""
    if settings.dry_run:
        log.info("untracked-protect sweep: DRY_RUN — skipped entirely")
        return
    from bot.orphan_sweep import _fenced
    from bot.trader import _place_sl_with_retry, ensure_sl_inside_liq
    try:
        _ex = client.open_positions() or {}
    except Exception as e:
        log.error("untracked-protect: open_positions() failed (%s) — sweep skipped this boot", e)
        return
    try:
        _db_open_coins = {str(_r["coin"]) for _r in (open_trades() or [])}
    except Exception as _dbe:
        log.warning("untracked-protect: open_trades() failed (%s) — using in-memory view only", _dbe)
        _db_open_coins = set()
    for _coin, _v in _ex.items():
        if _coin in open_positions or _coin in _db_open_coins:
            continue  # tracked (or DB-open — the per-tick reconciler adopts it)
        if _fenced(_coin):
            log.info("untracked-protect skip %s: fenced manual/foreign", _coin)
            continue
        try:
            _szi = float(_v.get("szi", 0) or 0)
        except (TypeError, ValueError):
            continue
        if abs(_szi) <= 0:
            continue
        if _szi < 0 and not settings.short_enabled_tfs:
            log.critical("untracked-protect skip %s: SHORT position but bot is long-only "
                         "(short_enabled_tfs empty) — foreign/manual by construction, NOT touching", _coin)
            continue
        try:
            if client.list_open_sl_orders(_coin):
                continue  # already protected by a live trigger
        except Exception as _re:
            # read-back INDETERMINATE — cannot PROVE naked; never place a possibly-duplicate SL
            log.warning("untracked-protect %s: SL read-back failed (%s) — indeterminate, not touching", _coin, _re)
            continue
        _long = _szi > 0
        _side = "long" if _long else "short"
        _mark = client.mark_price(_coin)
        if not _mark or _mark <= 0:
            log.error("untracked-protect %s: no mark — STILL NAKED, manual", _coin)
            continue
        _sl = _mark * 0.94 if _long else _mark * 1.06  # ±6% protective stop (hl donor)
        try:
            _sl, _liq_act = ensure_sl_inside_liq(
                client=client, coin=_coin, side=_side, sl_px=_sl, size=abs(_szi),
            )
            if _liq_act not in ("ok_already_inside", "no_liq_data", "dry_skip"):
                log.warning("untracked-protect liq-guard %s: action=%s sl→%.6f", _coin, _liq_act, _sl)
        except Exception as _le:
            log.warning("untracked-protect %s: liq-guard threw (%s) — placing raw ±6%% SL", _coin, _le)
        _oid = _place_sl_with_retry(client, _coin, abs(_szi), _sl, side=_side)
        if _oid:
            log.warning("PROTECTED untracked naked position %s sz=%.4f SL=%.6f oid=%s",
                        _coin, _szi, _sl, _oid)
        else:
            log.error("untracked-protect %s: SL placement FAILED — STILL NAKED, manual", _coin)


def _reconcile_pending_entries(client, open_positions):
    # Per-tick reconciler: adopt DB-open entries (UK-SIG-BTC-SHORT heal fix 2026-06-07).
    # Resting entry -> placeholder blocks scanner. Filled entry -> proper adopt for heal.
    # XNN port 2026-06-11: skipped entirely in DRY_RUN (same foreign-close class guard
    # as _restore_positions_from_db — adopted rows would be MANAGED with real orders).
    if settings.dry_run:
        return
    from bot.journal import open_trades as _ot
    from bot.strategy_xnn import Position as _Pos
    try:
        exchange_pos = client.open_positions()
        for row in _ot():
            coin = row["coin"]
            if coin in open_positions:
                ph = open_positions[coin]
                if ph.__dict__.get("_pending_entry") and (
                    coin in exchange_pos or f"{coin}-USD" in exchange_pos
                ):
                    ph.__dict__.pop("_pending_entry", None)
                    log.info("Reconciler: %s filled, clearing pending flag", coin)
                continue
            direction = row["direction"] if "direction" in row.keys() else "long"
            in_exch = coin in exchange_pos or f"{coin}-USD" in exchange_pos
            pos = _Pos(
                coin=coin, tf=row["tf"],
                entry_price=float(row["entry"]),
                sl_initial=float(row["sl_initial"]),
                sl_current=float(row["sl_current"] or row["sl_initial"]),
                tp1_price=float(row["tp1"] or 0),
                size=float(row["size"]),
                bar_entry_idx=0, side=direction,
            )
            pos.__dict__["_trade_id"] = row["id"]
            pos.__dict__["_sl_order_id"] = row["sl_order_id"]
            if not in_exch:
                pos.__dict__["_pending_entry"] = True
                log.debug("Reconciler placeholder resting %s", coin)
            else:
                log.info("Reconciler adopted %s (filled, sl=%s)", coin, row["sl_order_id"])
            open_positions[coin] = pos
    except Exception as e:
        log.warning("Reconciler error: %s", e)

def _resolve_snapshot_path() -> Path:
    p = Path(settings.liq_snapshot_path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _bootstrap_snapshot(client) -> SnapshotHolder:
    """Load snapshot from disk; if missing or stale > max_age, run inline once.

    Never crashes — any exception during bootstrap is logged and the bot
    continues with an empty SnapshotHolder. Trader.py will then reject signals
    with a clear `liq_inline_fetch_failed` reason until the next cron tick
    repairs the snapshot. This is preferable to crashing on boot, which
    deprives the bot of its position-management loop and is hard to detect.
    """
    snap_path = _resolve_snapshot_path()
    snap = None
    try:
        snap = load_snapshot(snap_path)
        age_hours = (snap.age_seconds / 3600.0) if snap is not None else None
        needs_bootstrap = snap is None or (
            age_hours is not None and age_hours > settings.liq_snapshot_max_age_hours
        )
        if needs_bootstrap:
            log.warning(
                "Snapshot missing or stale (age=%s) — running bootstrap inline",
                f"{age_hours:.1f}h" if age_hours is not None else "n/a",
            )
            from bot.liquidity_snapshot import main as snapshot_main
            try:
                snapshot_main(force=True)
            except SystemExit:
                # liquidity_snapshot calls os._exit; only bare success path returns 0.
                # During import-as-module run we may bubble through — re-load below.
                pass
            snap = load_snapshot(snap_path)
    except Exception as e:
        log.error(
            "Snapshot bootstrap failed (continuing with empty snapshot): %s",
            e, exc_info=True,
        )
        snap = None

    if snap is None:
        log.error("FATAL: snapshot still missing after bootstrap — bot will reject all signals")
    elif snap.age_seconds > settings.liq_snapshot_max_age_hours * 3600:
        log.warning(
            "Snapshot stale: age=%.1fh > %.1fh — continuing but cron may be broken",
            snap.age_seconds / 3600.0, settings.liq_snapshot_max_age_hours,
        )

    return SnapshotHolder(snap)


# Stale-snapshot watchdog state — module-level so it persists across loop iterations.
_last_stale_warn_ts: float = 0.0


def _check_snapshot_age(holder: SnapshotHolder) -> None:
    """Emit WARNING/ERROR if snapshot ages past 36h / 72h respectively.

    Rate-limited to one log line per hour to avoid flooding the journal.
    TG is fleet-MUTED (project_tg_muted_fleet_2026_05_23), so this is journal-only.
    """
    global _last_stale_warn_ts
    snap = holder.current()
    if snap is None:
        return
    age_h = snap.age_seconds / 3600.0
    if age_h < 36.0:
        return
    now = time.time()
    if (now - _last_stale_warn_ts) < 3600.0:
        return
    _last_stale_warn_ts = now
    if age_h > 72.0:
        log.error(
            "Snapshot is %.1fh old (>72h) — daily cron likely broken; bot trading stale liquidity data",
            age_h,
        )
    else:
        log.warning(
            "Snapshot is %.1fh old (>36h) — verify cron 00:05 UTC ran successfully",
            age_h,
        )


def _start_snapshot_reloader(holder: SnapshotHolder, interval_sec: int = 600) -> threading.Thread:
    """Background thread: re-read snapshot from disk every interval_sec (default 10 min)
    if mtime changed. Daemon so it dies with main process."""
    snap_path = _resolve_snapshot_path()

    def _loop():
        while True:
            try:
                holder.maybe_reload(snap_path)
            except Exception as e:
                log.warning("snapshot reload error: %s", e)
            time.sleep(interval_sec)

    th = threading.Thread(target=_loop, name="snapshot-reloader", daemon=True)
    th.start()
    return th


# --- Loop-progress watchdog (2026-06-18: kills the recurring stall class — an
# unbounded-blocking SDK candles fetch on the per-cycle hot-path froze the single-thread
# loop for 3h+ while systemd still saw `active` (6 recurrences band-aided by manual
# restart). This daemon force-exits on no-progress so systemd (Restart=on-failure)
# self-heals. Class-level: independent of WHICH network call hangs. Heartbeat is
# time.monotonic() (clock-skew immune).
_loop_heartbeat = [time.monotonic()]


def _loop_progress_beat() -> None:
    _loop_heartbeat[0] = time.monotonic()


def _start_loop_watchdog(stall_limit_sec: float, check_interval_sec: float = 30.0) -> threading.Thread:
    def _watch() -> None:
        while True:
            time.sleep(check_interval_sec)
            stalled_for = time.monotonic() - _loop_heartbeat[0]
            if stalled_for > stall_limit_sec:
                log.critical(
                    "LOOP WATCHDOG: no main-loop progress for %.0fs (>%.0fs limit) "
                    "force-exiting so systemd restarts (recurring hung-fetch stall "
                    "class). 0 own positions managed in-process; foreign fenced.",
                    stalled_for, stall_limit_sec,
                )
                import os as _os
                _os._exit(1)
    th = threading.Thread(target=_watch, name="loop-watchdog", daemon=True)
    th.start()
    return th


def main_loop(dry_run: bool = False, run_once: bool = False) -> None:
    log.info("nado_bot_v2 starting (dry_run=%s, once=%s)", dry_run, run_once)
    log.info(
        "Config: tfs=%s leverage=%dx mm_cap=%.0f%% max_concurrent=%d "
        "risk=%.1f%% f1=%.1f liq_size_cap=%.1f%% liq_min_trade=$%.0f",
        settings.working_tfs, settings.leverage,
        settings.mm_cap_pct * 100, settings.max_concurrent,
        settings.risk_per_trade * 100, settings.f1_min_dist_ema20_atr,
        settings.liq_size_cap_pct * 100, settings.liq_min_trade_usd,
    )

    if dry_run:
        log.info("DRY-RUN mode: no orders will be placed")

    init_db()

    # XNN port 2026-06-11 (HL review-fix class): fresh-deploy startup assert. A brand-new
    # xnn deploy MUST start from an EMPTY trades.db (DEPLOY_CHECKLIST §2) — any pre-existing
    # DB-open row means a copied data/ dir or the old WorkingDirectory (systemd repoint
    # class, mem:feedback_deploy_repoint_splits_per_dir_state) and would be adopted +
    # MANAGED with real orders. Refuse to start instead of trusting procedure.
    if os.getenv("XNN_EXPECT_EMPTY_DB", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            _pre_open = open_trades()
        except Exception as _dbe:
            log.critical("XNN_EXPECT_EMPTY_DB=1: open_trades() failed (%s) — refusing to start", _dbe)
            sys.exit(1)
        if _pre_open:
            log.critical(
                "XNN_EXPECT_EMPTY_DB=1 but trades.db has %d open row(s) (e.g. %s) — "
                "WRONG data/ dir or copied DB. REFUSING TO START. Fix the deploy dir "
                "or unset XNN_EXPECT_EMPTY_DB after manual review.",
                len(_pre_open), ", ".join(sorted({r["coin"] for r in _pre_open})[:8]),
            )
            sys.exit(1)
        log.info("XNN_EXPECT_EMPTY_DB=1: trades.db has 0 open rows — OK")

    client = _build_client(settings)

    universe = load_universe(client)
    universe_tiers = {a.symbol: a.tier for a in universe}
    log.info("Universe: %d symbols (vol-floor + FX excluded)", len(universe))

    # WS-PUSH candle feed (2026-06-27): subscribe once per (coin,native-interval) to the
    # Vertex gateway latest_candlestick stream -> scans served from memory (0 indexer
    # get_candlesticks, 0 rate-limit, instant). REST stays the fallback. env-gated
    # NADO_WS_CANDLE=1. Resampled TFs (8h,30m) map to their native base (4h,15m): the WS
    # subscribes the base, and candles() builds the resampled frame from the base fast-path.
    if os.getenv("NADO_WS_CANDLE", "0") == "1":
        try:
            from bot.ws_candle_feed import WsCandleFeed
            # resampled-TF -> native base (mirror exchange_nado._RESAMPLE_FROM)
            _RESAMPLE_BASE = {"30m": "15m", "8h": "4h"}
            _cfg_tfs = []
            for _k in ("WORKING_TFS", "SHORT_TFS", "DONCHIAN_TFS"):
                _cfg_tfs += [t.strip() for t in os.getenv(_k, "").split(",") if t.strip()]
            _native_tfs = []
            for _t in _cfg_tfs:
                _native_tfs.append(_RESAMPLE_BASE.get(_t, _t))
            _native_tfs = list(dict.fromkeys(_native_tfs)) or ["4h"]
            # coin -> product_id for universe symbols the client could resolve
            _pid_map = {a.symbol: client._symbol_to_pid[a.symbol]
                        for a in universe if a.symbol in client._symbol_to_pid}
            _wsf = WsCandleFeed(_pid_map, _native_tfs)
            client._ws_feed = _wsf
            _wsf.start()
            log.info("WS candle feed started: %d coins x %s (native)",
                     len(_pid_map), _native_tfs)
        except Exception as _e:
            log.warning("WS candle feed start failed (REST fallback active): %s", _e)

    # Liquidity snapshot bootstrap + background reloader (10-min mtime check)
    snapshot_holder = _bootstrap_snapshot(client)
    _start_snapshot_reloader(snapshot_holder, interval_sec=600)
    _start_loop_watchdog(max(300.0, 8.0 * settings.loop_interval_sec))
    snap = snapshot_holder.current()
    if snap is not None:
        log.info(
            "Liquidity snapshot active: %d coins, generated_at=%s, age=%.1fh",
            len(snap.coins), snap.generated_at_utc, snap.age_seconds / 3600.0,
        )

    # Crash-mid-entry recovery: promote/delete 'pending' pre-order rows BEFORE restore,
    # so a promoted row is picked up by _restore_positions_from_db in the same boot.
    _reconcile_pending(client)

    # In-memory positions (restored from DB on restart)
    open_positions: Dict[str, Position] = _restore_positions_from_db(client)

    # SAFETY NET: protect any exchange position that is untracked + unprotected + unfenced
    # (crash in the fill→journal window, or a fill during restart). Never touches fenced
    # manual/foreign coins; skipped in DRY.
    try:
        _protect_untracked_positions(client, open_positions)
    except Exception as _pe:
        log.warning("untracked-position protect sweep failed: %s", _pe)

    # Cross-cycle debounce state for the orphan reduce-only TRIGGER sweep.
    orphan_trigger_seen: dict = {}

    scanner = Scanner(settings)
    position_manager = PositionManager(
        be_buffer_pct=settings.trail_after_tp_buffer_pct,
        vstop_pivot_window=settings.vstop_pivot_window,
        max_run_r=settings.max_run_r,
        vstop_buffer_pct=settings.vstop_buffer_pct,
        tp1_partial_frac=settings.tp1_partial_frac,
    )

    iteration = 0
    while True:
        iteration += 1
        t_loop_start = time.time()
        _loop_progress_beat()

        # Stale-snapshot watchdog (rate-limited to 1 line/hr; no-op if fresh)
        _check_snapshot_age(snapshot_holder)

        try:
            # --- Manage existing positions ---
            closed_coins = set()
            for coin, pos in list(open_positions.items()):
                try:
                    # Fetch latest bars for this position's TF
                    df = client.candles(coin, pos.tf, limit=50)
                    if df is not None and not df.empty:
                        from bot.strategy_xnn import compute_indicators
                        df = compute_indicators(df)

                    exit_reason = manage_open_position(
                        pos=pos,
                        client=client,
                        settings=settings,
                        position_manager=position_manager,
                        df_latest={pos.tf: df},
                    )
                    if exit_reason is not None:
                        closed_coins.add(coin)
                        log.info("Position closed %s: %s", coin, exit_reason)
                except Exception as e:
                    log.error("manage_open_position(%s) error: %s", coin, e, exc_info=True)

            for coin in closed_coins:
                open_positions.pop(coin, None)

            _reconcile_pending_entries(client, open_positions)

            # --- Sweep orphan reduce-only TRIGGER orders (class-fix 2026-06-15) ---
            # Cancel SL/TP triggers left resting after their position closed (the
            # reconciler only cancels triggers while actively managing a position).
            # Exempts manual/foreign + any coin with a live position; fail-safe.
            if not settings.dry_run:
                try:
                    sweep_orphan_triggers(client, open_positions, orphan_trigger_seen, log)
                except Exception as _se:
                    log.warning("orphan-sweep dispatch error: %s", _se)

            # --- Scan for new signals (runs even in dry-run; gate is per-signal below) ---
            no_long = _no_long_symbols(universe) if CRYPTO_SHORT_ONLY else set()
            # --- ENTRY-LATENCY STREAMING (2026-07-02, mirror of hl_combo_bot fix) ---
            # scan_all_coins walks the WHOLE universe (throttled per-coin) before the
            # batch entry loop -> a breakout detected early waited the whole ~minutes
            # pass before entry (adverse slip / missed fast breakouts). Nado is ALL
            # native crypto (donchian/xnn, pass-through, NO xyz/us29 selection leg) so
            # EVERY signal is safe to enter the instant it is detected. Shared helper
            # _try_enter_signal is called both by the stream callback (during the scan)
            # and by the fallback batch loop (after). Coins entered by the callback are
            # already in open_positions -> the batch loop skips them (no double entry).
            # attempt_entry re-checks bar_age + a LIVE-mark breakout_invalidated guard,
            # so entering earlier can only HELP. Streaming is OFF in dry_run.
            def _try_enter_signal(signal, _via_stream=False):
                """Enter one signal. Returns 'entered' | 'skip' | 'stop' ('stop' = cap break)."""
                if len(open_positions) >= settings.max_concurrent:
                    log.info(
                        "SKIP signal %s %s: max_concurrent=%d reached",
                        signal.coin, signal.tf, settings.max_concurrent,
                    )
                    return "stop"

                if signal.coin in open_positions:
                    return "skip"  # cross-TF dedup (also caught in scanner, defensive)

                # Bar age for this signal
                bar_age = scanner.bar_age_sec(signal.tf)

                if dry_run:
                    log.info(
                        "[DRY-RUN] Signal %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1=%.2f bar_age=%.0fs",
                        signal.coin, signal.tf,
                        signal.trigger_price, signal.sl_price,
                        signal.tp1_price, signal.f1_dist, bar_age,
                    )
                    # XNN audit fix 2026-06-11: DRY used to write NOTHING to the DB —
                    # the §8 flip gate (rejected_signals profile) and the strategy_xnn
                    # docstring promise ("signal distribution measured in DRY") were
                    # unfulfillable; an empty table was indistinguishable from "no
                    # signals". Journal each DRY signal with a distinct reason so
                    # frequency/symbol/TF can be measured from the DB as intended.
                    try:
                        insert_rejected(
                            coin=signal.coin, tf=signal.tf,
                            trigger_price=signal.trigger_price,
                            entry_price=signal.entry_price,
                            sl_price=signal.sl_price,
                            reason="dry_run_signal",
                            direction=signal.side,
                        )
                    except Exception as _je:
                        log.warning("DRY journal insert_rejected failed: %s", _je)
                    return "skip"

                try:
                    pos = attempt_entry(
                        signal=signal,
                        client=client,
                        settings=settings,
                        universe_tiers=universe_tiers,
                        bar_age_sec=bar_age,
                        snapshot_holder=snapshot_holder,
                    )
                except Exception as _ee:
                    log.error(
                        "attempt_entry(%s %s, via_stream=%s) raised: %s",
                        signal.coin, signal.tf, _via_stream, _ee, exc_info=True,
                    )
                    return "skip"

                if pos is not None:
                    # CRITICAL: track the fill as the VERY FIRST statement so a later
                    # exception can never leave it untracked or let the batch loop
                    # double-place on the same coin.
                    open_positions[signal.coin] = pos
                    log.info(
                        "ENTRY OK %s %s: entry=%.6f sl=%.6f size=%s%s",
                        pos.coin, pos.tf, pos.entry_price, pos.sl_current, pos.size,
                        " [stream]" if _via_stream else "",
                    )
                    return "entered"
                return "skip"

            # Stream-entry plumbing: enter each signal the instant scan_all_coins
            # detects it, capped per scan for 429-safety. OFF in dry_run.
            _stream_state = {"stopped": False, "streamed": 0}
            _STREAM_MAX = int(os.getenv("NADO_STREAM_MAX_PER_SCAN", "8"))

            def _on_crypto_signal(sig):
                if _stream_state["stopped"] or _stream_state["streamed"] >= _STREAM_MAX:
                    return
                _res = _try_enter_signal(sig, _via_stream=True)
                if _res == "stop":
                    _stream_state["stopped"] = True
                elif _res == "entered":
                    _stream_state["streamed"] += 1

            _stream_cb = (
                _on_crypto_signal
                if (not dry_run and os.getenv("CRYPTO_STREAM_ENTRY", "1") == "1")
                else None
            )

            signals = scanner.scan_all_coins(
                coins=universe,
                client=client,
                open_positions=open_positions,
                no_long_symbols=no_long,
                on_crypto_signal=_stream_cb,
            )

            # Fallback batch loop: enters anything the stream skipped (cap hit, cb off,
            # or cb error). Coins already streamed are in open_positions -> skipped.
            for signal in signals:
                if _try_enter_signal(signal) == "stop":
                    break

        except Exception as e:
            log.error("Main loop error (iteration %d): %s", iteration, e, exc_info=True)

        if run_once:
            log.info("--once flag: exiting after single iteration")
            break

        # Sleep until next loop tick
        elapsed = time.time() - t_loop_start
        # BOUNDARY-ALIGNED WAKE (2026-06-23): keep ~loop_interval cadence for position
        # management, but shorten the sleep so the loop wakes right after a bar CLOSES
        # (00/08/16 UTC for 8h) -> entry-detect lag ~2s instead of up to loop_interval.
        # Floor 1s (never busy-loop); capped at loop_interval (never starve mgmt).
        try:
            from bot.config import TF_MS as _TFMS
            _atfs = set(settings.working_tfs) | set(getattr(settings, "short_enabled_tfs", []) or [])
            _nm = int(time.time() * 1000)
            _tnb = min((((_nm // _TFMS[_t]) + 1) * _TFMS[_t] - _nm) / 1000.0 for _t in _atfs if _t in _TFMS)
            _age = min((_nm - (_nm // _TFMS[_t]) * _TFMS[_t]) / 1000.0 for _t in _atfs if _t in _TFMS)
            _base = max(0.0, settings.loop_interval_sec - elapsed)
            _sleep = min(_base, 0.5) if _age < 15.0 else min(_base, _tnb + 0.1)
            sleep_sec = max(0.2, _sleep)
        except Exception:
            sleep_sec = max(0, settings.loop_interval_sec - elapsed)
        log.debug("Loop done in %.1fs, sleeping %.1fs (boundary-aligned)", elapsed, sleep_sec)
        time.sleep(sleep_sec)

    log.info("nado_bot_v2 exiting cleanly")
    import os as _os
    _os._exit(0)  # SSH remote python must exit cleanly (MEMORY feedback_ssh_remote_python_must_exit_cleanly)


def cli_universe(client) -> None:
    from bot.universe import print_universe
    print_universe(client)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="nado_xnn_bot — XNN long/short on Vertex/Nado")
    parser.add_argument("--dry-run", action="store_true", help="Scan signals but do not place orders")
    parser.add_argument("--once", action="store_true", help="Run one loop iteration then exit")
    args = parser.parse_args()
    # XNN port 2026-06-11: DRY_RUN can come from the flag OR the env (systemd
    # ExecStart is `python -m bot.main` with no flag — env is the only channel there).
    # XNN audit fix 2026-06-11: the flag used to gate ONLY the entry branch
    # (main_loop param); adopt/restore (main.py), per-tick reconciler and every
    # trader._dry_block gate read settings.dry_run from env. `--dry-run` with
    # .env DRY_RUN=0 therefore adopted DB positions and sent REAL orders (heal SL,
    # trail cancel+place, emergency close) — violating the docstring promise.
    # Force the GLOBAL flag so ALL order paths see dry (Settings is frozen ->
    # object.__setattr__; trader imports the same settings object).
    if args.dry_run and not settings.dry_run:
        object.__setattr__(settings, "dry_run", True)
        log.warning(
            "--dry-run flag overrides env DRY_RUN=0: settings.dry_run forced True "
            "(adopt/restore, reconciler and all trader order paths are now blocked)"
        )
    main_loop(dry_run=(args.dry_run or settings.dry_run), run_once=args.once)
