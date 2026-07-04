"""main.py — unified bot main loop for Extended + Pacifica + Hyperliquid perps.

Usage:
  python -m bot.main                # live trading
  python -m bot.main --dry-run      # signals logged, no orders placed
  python -m bot.main --once         # single loop iteration then exit
  python -m bot.universe            # print filtered universe and exit

Exchange dispatch: settings.exchange in {"extended","pacifica","hyperliquid"} chooses client.
Strategy: UK v102 ZigZag breakout with per-TF F1/F2/F3 filters.
Bot A: WORKING_TFS=1d,8h,4h  |  Bot B: WORKING_TFS=2h,1h  (set in .env.a / .env.b)
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Dict

from bot.config import PROJECT_ROOT, Settings, settings
from bot.journal import init_db, open_trades
from bot.liquidity import SnapshotHolder, load_snapshot
from bot.scanner import Scanner
from bot.strategy_uk_v102 import Position, PositionManager
from bot.trader import attempt_entry, manage_open_position
from bot.universe import AssetTier, load_universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _build_client(cfg: Settings):
    """Dispatch exchange adapter based on settings.exchange."""
    if cfg.exchange == "pacifica":
        from bot.exchange_pacifica import PacificaClient
        return PacificaClient(cfg)
    if cfg.exchange == "extended":
        from bot.exchange_extended import ExtendedClient
        return ExtendedClient(cfg)
    if cfg.exchange == "hyperliquid":
        from bot.exchange_hl import HLClient
        return HLClient(cfg)
    raise RuntimeError(f"Unknown exchange={cfg.exchange!r}")


def _restore_positions_from_db(client) -> Dict[str, Position]:
    positions: Dict[str, Position] = {}
    try:
        exchange_pos = client.open_positions()
    except Exception as e:
        log.error("open_positions() on startup failed: %s", e)
        return positions

    try:
        db_open = open_trades()
    except Exception as e:
        log.error("open_trades() DB query failed: %s", e)
        return positions

    for row in db_open:
        coin = row["coin"]
        # Accept both bare coin and "COIN-USD" suffix (Extended-style)
        if coin not in exchange_pos and f"{coin}-USD" not in exchange_pos:
            log.info(
                "DB trade id=%d %s not on exchange — skipping restore",
                row["id"], coin,
            )
            continue
        # SHORT support: restore side from DB direction column (default long for old rows).
        direction = row["direction"] if "direction" in row.keys() else "long"
        if direction == "long":
            tp1_fallback = row["entry"] + 1.5 * (row["entry"] - row["sl_initial"])
        else:  # short — TP1 below entry
            tp1_fallback = row["entry"] - 1.5 * (row["sl_initial"] - row["entry"])
        pos = Position(
            coin=coin,
            tf=row["tf"],
            entry_price=row["entry"],
            sl_initial=row["sl_initial"],
            sl_current=row["sl_current"] or row["sl_initial"],
            tp1_price=row["tp1"] or tp1_fallback,
            size=row["size"],
            bar_entry_idx=0,
            side=direction,
        )
        pos.__dict__["_trade_id"] = row["id"]
        pos.__dict__["_sl_order_id"] = row["sl_order_id"]
        # 50/50 exit recovery (persisted): if the partial already filled, DB size is the
        # remainder and sl_current is breakeven — reconstruct the pre-partial original.
        keys = row.keys()
        tp1_done = bool(row["tp1_partial_done"]) if "tp1_partial_done" in keys else False
        pos.tp1_partial_done = tp1_done
        frac = settings.tp1_partial_frac if (tp1_done and settings.tp1_partial_frac < 1.0) else 0.0
        pos.__dict__["_orig_size"] = row["size"] / (1.0 - frac) if frac > 0 else row["size"]
        pos.__dict__["_sl_placed_px"] = pos.sl_current
        # restore the resting TP oid only if the partial has NOT filled yet (else it's gone)
        pos.__dict__["_tp_oid"] = (
            row["tp1_order_id"] if ("tp1_order_id" in keys and not tp1_done) else None
        )
        positions[coin] = pos
        log.info(
            "Restored position %s %s %s: entry=%.6f sl=%.6f size=%s",
            coin, row["tf"], direction, row["entry"], pos.sl_current, row["size"],
        )

    if positions:
        log.info("Restored %d open positions from DB", len(positions))
    return positions


def _resolve_snapshot_path() -> Path:
    p = Path(settings.liq_snapshot_path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _bootstrap_snapshot_hl(universe: list) -> None:
    """Write a stub liquidity snapshot from HL universe vol data.

    For Hyperliquid the metaAndAssetCtxs response already contains dayNtlVlm
    (24h notional vol). We use this as avg_1h_vol_usd = dayNtlVlm / 24 so the
    snapshot gate doesn't block entries. The liveness and orderbook checks are
    not needed for HL (it's a perpetual DEX with continuous markets).

    Written to LIQ_SNAPSHOT_PATH. Refreshed every UNIVERSE_REFRESH_MIN alongside
    universe refresh.
    """
    import json
    from datetime import datetime, timezone
    snap_path = _resolve_snapshot_path()
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    coins: dict = {}
    for asset in universe:
        avg_1h = asset.vol_24h_usd / 24.0 if asset.vol_24h_usd > 0 else 1000.0
        coins[asset.symbol] = {
            "avg_1h_vol_usd": round(avg_1h, 2),
            "spread_pct": 0.0,
            "depth_top20_usd": 0.0,
            "liveness_pct": 1.0,
            "_source": "hl_universe_dayNtlVlm",
        }
    snapshot_data = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "coins": coins,
    }
    snap_path.write_text(json.dumps(snapshot_data, indent=2))
    log.info("HL stub liquidity snapshot written: %d coins → %s", len(coins), snap_path)


def _bootstrap_snapshot(universe: list | None = None) -> SnapshotHolder:
    snap_path = _resolve_snapshot_path()
    snap = None

    # HL uses stub snapshot seeded from universe vol data
    if settings.exchange == "hyperliquid" and universe is not None:
        try:
            snap_check = load_snapshot(snap_path)
            age_hours = (snap_check.age_seconds / 3600.0) if snap_check is not None else None
            if snap_check is None or (age_hours is not None and age_hours > 25.0):
                _bootstrap_snapshot_hl(universe)
        except Exception as e:
            log.warning("HL snapshot bootstrap failed — will retry inline: %s", e)
            _bootstrap_snapshot_hl(universe)
        snap = load_snapshot(snap_path)
        if snap is not None:
            log.info("HL liquidity snapshot: %d coins, age=%.1fh",
                     len(snap.coins), snap.age_seconds / 3600.0)
        return SnapshotHolder(snap)

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
                pass
            snap = load_snapshot(snap_path)
    except Exception as e:
        log.error("Snapshot bootstrap failed (continuing): %s", e, exc_info=True)
        snap = None

    if snap is None:
        log.error("FATAL: snapshot still missing after bootstrap — bot will reject signals")
    elif snap.age_seconds > settings.liq_snapshot_max_age_hours * 3600:
        log.warning(
            "Snapshot stale: age=%.1fh > %.1fh",
            snap.age_seconds / 3600.0, settings.liq_snapshot_max_age_hours,
        )
    return SnapshotHolder(snap)


_last_stale_warn_ts: float = 0.0


def _check_snapshot_age(holder: SnapshotHolder) -> None:
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
        log.error("Snapshot is %.1fh old (>72h) — daily cron broken", age_h)
    else:
        log.warning("Snapshot is %.1fh old (>36h) — verify cron", age_h)


def _start_snapshot_reloader(holder: SnapshotHolder, interval_sec: int = 600) -> threading.Thread:
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


def main_loop(dry_run: bool = False, run_once: bool = False) -> None:
    log.info(
        "bot starting: exchange=%s dry_run=%s once=%s",
        settings.exchange, dry_run, run_once,
    )
    log.info(
        "Config: tfs=%s lev=%dx mm_cap=%.0f%% max_conc=%d max_opens_day=%d "
        "risk=%.2f%% network=%s liq_size_cap=%.1f%%",
        settings.working_tfs, settings.leverage,
        settings.mm_cap_pct * 100, settings.max_concurrent,
        settings.max_opens_per_day, settings.risk_per_trade * 100,
        settings.network, settings.liq_size_cap_pct * 100,
    )
    for tf in settings.working_tfs:
        flt = settings.get_tf_filters(tf)
        log.info("  LONG TF %s: f1=%.2f f2=%.1f f3=%.0f", tf, flt.f1, flt.f2, flt.f3)
    if settings.short_enabled_tfs:
        log.info("SHORT enabled on TFs: %s (require_ema50_down=%s)",
                 settings.short_enabled_tfs, settings.require_ema50_down)
        for tf in settings.short_enabled_tfs:
            sflt = settings.get_tf_short_filters(tf)
            log.info("  SHORT TF %s: f1=%.2f f2(maxRSI)=%.1f f3=%.0f", tf, sflt.f1, sflt.f2, sflt.f3)
    else:
        log.info("SHORT disabled (SHORT_TFS empty) — long-only")
    if dry_run:
        log.info("DRY-RUN: no orders will be placed")

    init_db()
    client = _build_client(settings)

    universe = load_universe(force_refresh=True)
    universe_tiers = {a.symbol: a.tier for a in universe}
    log.info("Universe: %d symbols (TIER1+2)", len(universe))

    snapshot_holder = _bootstrap_snapshot(universe=universe)
    _start_snapshot_reloader(snapshot_holder, interval_sec=600)
    snap = snapshot_holder.current()
    if snap is not None:
        log.info(
            "Liquidity snapshot active: %d coins, generated_at=%s, age=%.1fh",
            len(snap.coins), snap.generated_at_utc, snap.age_seconds / 3600.0,
        )

    open_positions: Dict[str, Position] = _restore_positions_from_db(client)

    scanner = Scanner(settings)
    position_manager = PositionManager(
        be_buffer_pct=settings.trail_after_tp_buffer_pct,
        vstop_pivot_window=settings.vstop_pivot_window,
        max_run_r=settings.max_run_r,
        vstop_buffer_pct=settings.vstop_buffer_pct,
        tp1_partial_frac=settings.tp1_partial_frac,
    )

    # Daily opens counter (UTC day rollover)
    opens_today = 0
    current_day = time.gmtime().tm_yday
    iteration = 0
    last_universe_refresh = time.time()

    while True:
        iteration += 1
        t_loop_start = time.time()

        # Daily counter reset
        day_now = time.gmtime().tm_yday
        if day_now != current_day:
            log.info("UTC day rollover %d→%d — resetting opens_today (was %d)",
                     current_day, day_now, opens_today)
            current_day = day_now
            opens_today = 0

        _check_snapshot_age(snapshot_holder)

        refresh_interval = settings.universe_refresh_min * 60
        if time.time() - last_universe_refresh > refresh_interval:
            try:
                universe = load_universe(force_refresh=True)
                universe_tiers = {a.symbol: a.tier for a in universe}
                log.info("Universe refreshed: %d symbols", len(universe))
                last_universe_refresh = time.time()
                # HL: refresh stub snapshot alongside universe
                if settings.exchange == "hyperliquid":
                    try:
                        _bootstrap_snapshot_hl(universe)
                        new_snap = load_snapshot(_resolve_snapshot_path())
                        if new_snap is not None:
                            snapshot_holder._snap = new_snap
                    except Exception as _se:
                        log.warning("HL snapshot refresh failed: %s", _se)
            except Exception as e:
                log.error("Universe refresh failed: %s", e)

        try:
            closed_coins = set()
            for coin, pos in list(open_positions.items()):
                try:
                    df = client.candles(coin, pos.tf, limit=50)
                    if df is not None and not df.empty:
                        from bot.strategy_uk_v102 import compute_indicators
                        df = compute_indicators(df)

                    exit_reason = manage_open_position(
                        pos=pos, client=client, settings=settings,
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

            # Live-account dedup (a/b SHARE one HL account): block coins held by
            # EITHER bot on the exchange, not just this bot's in-memory positions.
            # Prevents self-hedge (a long BTC + b short BTC) + reduce_only SL crossfire.
            try:
                account_coins = set(client.open_positions().keys())
            except Exception as e:
                log.warning("account positions fetch for dedup failed: %s — using in-memory", e)
                account_coins = set(open_positions.keys())

            signals = scanner.scan_all_coins(
                coins=universe, client=client, open_positions=open_positions,
                account_coins=account_coins,
            )

            for signal in signals:
                if len(open_positions) >= settings.max_concurrent:
                    log.info("SKIP %s %s: max_concurrent=%d reached",
                             signal.coin, signal.tf, settings.max_concurrent)
                    break
                if settings.max_opens_per_day > 0 and opens_today >= settings.max_opens_per_day:
                    log.info("SKIP %s %s: max_opens_per_day=%d reached",
                             signal.coin, signal.tf, settings.max_opens_per_day)
                    break
                if signal.coin in open_positions or signal.coin in account_coins:
                    continue

                bar_age = scanner.bar_age_sec(signal.tf)
                if dry_run:
                    log.info(
                        "[DRY-RUN] Signal %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1=%.2f bar_age=%.0fs",
                        signal.coin, signal.tf,
                        signal.trigger_price, signal.sl_price,
                        signal.tp1_price, signal.f1_dist, bar_age,
                    )
                    continue

                pos = attempt_entry(
                    signal=signal, client=client, settings=settings,
                    universe_tiers=universe_tiers, bar_age_sec=bar_age,
                    snapshot_holder=snapshot_holder,
                )
                if pos is not None:
                    open_positions[signal.coin] = pos
                    opens_today += 1
                    log.info(
                        "ENTRY OK %s %s: entry=%.6f sl=%.6f size=%s opens_today=%d",
                        pos.coin, pos.tf, pos.entry_price, pos.sl_current, pos.size,
                        opens_today,
                    )
        except Exception as e:
            log.error("Main loop error (iter %d): %s", iteration, e, exc_info=True)

        if run_once:
            log.info("--once flag: exiting after single iteration")
            break

        elapsed = time.time() - t_loop_start
        sleep_sec = max(0, settings.loop_interval_sec - elapsed)
        log.debug("Loop done in %.1fs, sleeping %.1fs", elapsed, sleep_sec)
        time.sleep(sleep_sec)

    log.info("bot exiting cleanly")
    import os as _os
    _os._exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extended/pacifica/hyperliquid bot — UK v102 per-TF")
    parser.add_argument("--dry-run", action="store_true", help="Scan but do not place orders")
    parser.add_argument("--once", action="store_true", help="Run one iteration then exit")
    args = parser.parse_args()
    # DRY_RUN can be set via flag or env var (systemd EnvironmentFile support)
    dry_run = args.dry_run or settings.dry_run
    main_loop(dry_run=dry_run, run_once=args.once)
