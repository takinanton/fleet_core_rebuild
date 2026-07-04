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
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import Dict

from bot.config import PROJECT_ROOT, Settings, settings
from bot.journal import (
    init_db, open_trades, pending_trades, promote_pending, delete_pending,
)
from bot.liquidity import SnapshotHolder, load_snapshot
from bot.scanner import Scanner
# XNN port 2026-06-10: strategy module repointed uk_v102 -> xnn (same contract).
from bot.strategy_us29 import Position, PositionManager
# Exit-side routing (audit HIGH 2026-06-19): the manage/exit path must use the SAME
# leg's PositionManager that opened the position. xyz_* (HIP-3 stocks) -> us29 PM;
# native crypto perps -> donchian PM (chandelier ATR trail + 4R TP + 120-bar time_stop).
# Both PMs share an IDENTICAL ctor + (update_sl_on_new_bar/apply_partial_be/check_sl_hit)
# interface, so routing is purely instance-selection at the call site.
from bot.strategy_donchian import PositionManager as DonchianPositionManager
from bot import strategy_us29 as _strat_us29_mod
from bot import strategy_donchian as _strat_donchian_mod
from bot.trader import attempt_entry, manage_open_position, _place_sl_with_retry, _ensure_flat, ensure_sl_inside_liq
from bot.resting_orders import RestingOrderManager
from bot.journal import insert_trade
from bot.universe import AssetTier, load_universe, apply_depth_gate
from bot.orphan_sweep import sweep_orphan_triggers
import os

CRYPTO_SHORT_ONLY = os.getenv("CRYPTO_SHORT_ONLY", "").strip().lower() in ("1", "true", "yes", "on")
DEPTH_GATE_ENABLE = os.getenv("DEPTH_GATE_ENABLE", "true").strip().lower() not in ("0", "false", "no", "off")
# XNN port 2026-06-10: RestingOrderManager entry logic is inline Donchian (uk_v102), NOT a
# strategy plugin — with XNN it would trade the OLD strategy. Default keeps legacy behavior;
# the xnn .env MUST set RESTING_ORDERS_ENABLE=0.
RESTING_ORDERS_ENABLE = os.getenv("RESTING_ORDERS_ENABLE", "false").strip().lower() not in ("0", "false", "no", "off")  # cfg-1 fail-safe default (2026-06-20): a dropped env key must NOT silently arm the uk_v102 inline-Donchian resting entry path that places REAL orders
# XNN port 2026-06-10: comma-separated coin PREFIXES the startup untracked-protect sweep must
# NOT touch (foreign positions, e.g. the 8 xyz_* held by the stopped uk_v102 bots). Empty = no skip.
FOREIGN_SKIP_PREFIXES = tuple(
    p.strip() for p in os.getenv("FOREIGN_SKIP_PREFIXES", "").split(",") if p.strip()
)

# Audit HIGH (2026-06-19) — manual-xyz naked-trap exemption.
# (mem:project_hl_xnn_hip3_foreign_skip_naked_trap_2026_06_18 — FOREIGN_SKIP_PREFIXES-
#  matches-own-universe class.) The combo bot OWNS the xyz_ leg (us29/F4_vb), so xyz_
# CANNOT be blanket foreign-skipped (its own xyz positions carry DB rows and must be
# adopted/managed). But the unified HL account is SHARED with MANUAL ЮК xyz positions
# (mem:project_manual_positions_live) which carry NO trades.db row — and the untracked-
# protect sweep + naked sentinel place a bot ±6% SL on ANY DB-less, SL-less position,
# which would fire BEFORE a manual position's wide static SL = off-plan close.
#
# MANUAL_POSITION_PREFIXES = coin prefixes the protect/adopt path must treat as foreign
# (NOT auto-SL, NOT adopt) EVEN when FOREIGN_SKIP_PREFIXES is empty — for manual user
# positions on the shared account. Distinct from FOREIGN_SKIP_PREFIXES (stopped-bot
# positions) so the two operator decisions stay independent.
MANUAL_POSITION_PREFIXES = tuple(
    p.strip() for p in os.getenv("MANUAL_POSITION_PREFIXES", "").split(",") if p.strip()
)
# Explicit operator acknowledgement that this combo bot may run live on an HL account
# SHARED with manual xyz positions and the live us29/xnn services. Required to flip
# DRY_RUN=0 while HIP-3 (xyz_) is enabled and no fence protects manual positions —
# see the startup naked-trap assert in run(). Default empty = NOT acknowledged.
COMBO_ACK_SHARED_ACCOUNT = os.getenv("COMBO_ACK_SHARED_ACCOUNT", "").strip().lower() in (
    "1", "true", "yes", "on",
)

# ── US29 CANONICAL selection = F4_vb (top-M of the day + SPY/SMA200 index regime gate) ──
# B2 (2026-06-19): the LEGACY expanding-percentile TopKPool (US29_TOPK_*) is the pool30
# design that emit_us29_stocks flags as a PARITY FAIL (14.65%/DD83.3). It is REPLACED by
# the deployed winning cell F4_vb:
#   * top-M=3 of the day's candidates by (-score, coin)  (US29_TOP_M)
#   * index regime gate: block ALL new entries when the regime-coin close < SMA200
#     (US29_REGIME_GATE / US29_REGIME_COIN, default xyz_SP500 — a NEVER-traded REGIME coin).
# Mirrors scripts/emit_us29_stocks.py select_kept(topm) + regime_at(spy200). See
# bot/selector_us29.py. The TopKPool path is kept ONLY as an explicit opt-in fallback
# (US29_TOPK_ENABLE=1 AND US29_SELECTOR=pool30) for diagnostics — default is F4_vb.
US29_SELECTOR = os.getenv("US29_SELECTOR", "topm").strip().lower()   # topm (F4_vb) | pool30 (legacy)
US29_TOP_M = int(os.getenv("US29_TOP_M", "3"))
US29_REGIME_GATE = os.getenv("US29_REGIME_GATE", "1").strip().lower() not in ("0", "false", "no", "off")
US29_REGIME_COIN = os.getenv("US29_REGIME_COIN", "xyz_SP500").strip()
US29_REGIME_SMA_N = int(os.getenv("US29_REGIME_SMA_N", "200"))

# LEGACY pool30 knobs (used ONLY when US29_SELECTOR=pool30). Default selector is topm so
# these are inert by default — the pool30 path is the rejected-parity design (audit B2).
US29_TOPK_ENABLE = (
    US29_SELECTOR == "pool30"
    and os.getenv("US29_TOPK_ENABLE", "true").strip().lower() not in ("0", "false", "no", "off")
)
US29_TOPK_PCT = float(os.getenv("US29_TOPK_PCT", "70"))
US29_TOPK_POOL_PATH = os.getenv("US29_TOPK_POOL_PATH", "data/us29_topk_pool.json")
US29_TOPK_MIN_POOL = int(os.getenv("US29_TOPK_MIN_POOL", "30"))
US29_TOPK_POOL_MAX = int(os.getenv("US29_TOPK_POOL_MAX", "500000"))

# a/b shared-DB ownership (fix 2026-06-07; mem:project_hl_restart_emergency_close_stale_db_rows).
# hl-bot-a and hl-bot-b share ONE data/trades.db. Each instance must adopt+manage ONLY the
# coins whose live position belongs to its WORKING_TFS, else both restore+manage every coin →
# reduce-only SL crossfire + double emergency-close on restart. _FLEET_TFS mirrors the union
# of .env.a (1d,8h,4h) and .env.b (30m,2h,1h); it is used only as an orphan backstop so a coin
# whose TF is outside the partition is still adopted by the primary instance (never unmanaged).
# Keep _FLEET_TFS in sync if a TF is added to either .env.
_FLEET_TFS = {"1d", "8h", "4h", "2h", "1h", "30m"}
_IS_PRIMARY = "1d" in (settings.working_tfs or [])  # bot A (senior TFs) = orphan-TF catch-all

def _no_long_symbols(universe) -> set:
    # crypto = HL native; xyz_* HIP-3 (stocks/commodities/idx/fx) stays bidirectional
    return {a.symbol for a in universe if not a.symbol.startswith("xyz_")}

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


def _select_winning_row(client, coin: str, rows: list):
    """Choose the ONE DB-open row that represents the live position for `coin` (fix a).

    Multiple open rows accumulate per coin from a+b shared-DB history (per-TF duplicates)
    and phantom-closes. The real position is the one whose sl_order_id is CURRENTLY resting
    on the exchange — adopt THAT row, because its sl_current is the price the live SL is
    actually protecting. If none match (lookup failed / SL re-placed past the stored oid /
    genuinely naked) fall back to the most-recent row (max id). This is the class fix for
    the 06-07 restart that adopted a stale wrong-SL row and gap_through_sl-closed 5 live
    positions (was: first-encountered by opened_at).
    """
    if len(rows) == 1:
        return rows[0]
    try:
        live_oids = {str(o) for o in (client.list_open_sl_orders(coin) or [])}
    except Exception as e:
        log.warning("winning-row SL lookup failed for %s: %s — using max-id", coin, e)
        live_oids = set()
    if live_oids:
        live_rows = [r for r in rows if r["sl_order_id"] and str(r["sl_order_id"]) in live_oids]
        if live_rows:
            chosen = max(live_rows, key=lambda r: r["id"])
            log.info("adopt %s: %d open rows — chose id=%d (live SL oid=%s) over %s",
                     coin, len(rows), chosen["id"], chosen["sl_order_id"],
                     [r["id"] for r in rows if r["id"] != chosen["id"]])
            return chosen
    chosen = max(rows, key=lambda r: r["id"])
    log.info("adopt %s: %d open rows, no live-SL match — chose most-recent id=%d",
             coin, len(rows), chosen["id"])
    return chosen


def _reconcile_pending(client) -> None:
    """write-db-row-PRE-order reconcile (panel must-fix 2026-06-21). A status='pending' row is a
    pre-order trace written by attempt_entry BEFORE order submit that never reached promote — i.e.
    a crash between order-submit and journal-promote. At startup, for each pending row:
      * a LIVE exchange position exists on the coin AND no 'open' row already covers it
            -> PROMOTE to 'open' (best-effort intended entry/size) so the normal adopt path manages
               it + heals its SL (closes the crash-mid-entry naked window at the SOURCE).
      * else (no live position, or already covered by an open row)
            -> DELETE the stale pending row.
    DRY: skipped (no live orders → no pending rows). Any read failure leaves pending rows intact
    (conservative — a later restart retries; the UNRECONCILED alarm still covers the naked case)."""
    if settings.dry_run:
        return
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
        log.warning("reconcile_pending: open_positions() failed: %s — leaving %d pending row(s) "
                    "for the next restart", e, len(pend))
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
                "(live position found) — adopt will heal its SL. Verify entry/size vs exchange.",
                coin, r["id"],
            )
        else:
            delete_pending(r["id"])
            log.warning("reconcile_pending: deleted stale pending %s id=%s (live=%s already_open=%s)",
                        coin, r["id"], live, coin in open_coins)


def _adopt_db_open_positions(
    client,
    existing: Dict[str, Position] | None = None,
    startup: bool = False,
) -> Dict[str, Position]:
    """Adopt DB-open trades that are live on the exchange but not yet tracked in-memory.

    Single source for BOTH restart-restore (startup=True) and the per-tick reconciler
    (startup=False). The per-tick call is the class fix for naked manual/resting entries
    (2026-06-07; mem:feedback_order_placement_readback_and_invariants): a stop-entry that
    fills WHILE the bot runs but is not in the in-memory dict (e.g. a position dropped by a
    prior phantom-close, or a manual fill with a DB row) would otherwise never be managed →
    its SL is never healed → NAKED until a post-fill restart. Re-running adopt every loop
    picks it up within ONE tick, after which the SL-liveness guard in manage_open_position
    places/confirms the SL (≤1 further tick). Only coins with a DB-open trade row are adopted;
    in-memory positions are preserved as-is (never clobber live trail state).
    """
    positions: Dict[str, Position] = dict(existing) if existing else {}
    # XNN port 2026-06-10 (review fix, foreign-close class guard): in DRY_RUN adopt is
    # SKIPPED entirely — manage_open_position places REAL orders (heal SL, trail
    # cancel/replace, emergency market-close) with no dry gate of its own; the only way
    # a "DRY" bot can touch the exchange is via an adopted position (stale/copied DB row,
    # manual insert). No adoption -> no manage -> zero orders in DRY, enforced in CODE.
    if settings.dry_run:
        if startup:
            log.warning("DRY_RUN: position adopt/restore SKIPPED (no positions will be "
                        "managed; manage-path places real orders and has no dry gate)")
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
        if startup and db_open and not exchange_pos:
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
        log.error("open_positions() in adopt failed: %s", e)
        return positions

    # Group open rows by coin: a coin can carry stale duplicate rows across TFs (a+b share
    # ONE trades.db). Pick ONE winning row per coin (live-SL row, else max id) and let only
    # the owning instance (by the winner's TF) adopt+manage it — so a (1d/8h/4h) and b
    # (30m/2h/1h) never both restore+manage the same exchange position. (fix a + c, 2026-06-07)
    by_coin: Dict[str, list] = {}
    for _row in db_open:
        by_coin.setdefault(_row["coin"], []).append(_row)

    my_tfs = set(settings.working_tfs or [])
    for coin, rows in sorted(by_coin.items()):
        # Already tracked in-memory (live trail state) — never clobber.
        if coin in positions:
            continue
        # XNN port 2026-06-10 (review fix, CODE guard not procedure): NEVER adopt a
        # FOREIGN-prefix coin (e.g. the 8 xyz_* held by the stopped uk_v102 bots).
        # Previously FOREIGN_SKIP_PREFIXES protected only the untracked-protect sweep;
        # adopt would still pick up any DB-open row whose coin is live on the exchange
        # (a copied data/ dir or a start from the old WorkingDirectory) and then MANAGE
        # it — repeat of the 06-07 mass-close class. Adopt = manage = real orders.
        if FOREIGN_SKIP_PREFIXES and coin.startswith(FOREIGN_SKIP_PREFIXES):
            log.error("adopt SKIP %s: FOREIGN prefix (%s) — refusing to adopt/manage a "
                      "foreign position; clean the DB row manually",
                      coin, ",".join(FOREIGN_SKIP_PREFIXES))
            continue
        # Accept both bare coin and "COIN-USD" suffix (Extended-style)
        if coin not in exchange_pos and f"{coin}-USD" not in exchange_pos:
            if startup:
                # DELIST-PHANTOM class fix 2026-07-02 (xyz_INTC on HL): a DB-open row absent from
                # the exchange at restore was skipped forever (never entered memory -> the manage
                # phantom-guard never saw it). Resolve HERE via REAL fills; keep the row (loud) only
                # when no close fill exists. Startup-only: at runtime the K=3 phantom-guard owns this.
                try:
                    from bot.trader import _lookup_real_close_px, _record_close, _cancel_orphan_triggers
                    _gr = _select_winning_row(client, coin, rows)
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
                        for _r2 in rows:
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
        row = _select_winning_row(client, coin, rows)
        win_tf = row["tf"]
        # Ownership: only the instance whose WORKING_TFS covers the winning (live) row
        # manages this coin; the primary instance is the backstop for any out-of-partition TF.
        if not (win_tf in my_tfs or (_IS_PRIMARY and win_tf not in _FLEET_TFS)):
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
        # Restore-grace: the SL price here is DB-derived and unverified. The first managed
        # tick runs a one-time reconcile (_reconcile_restored_sl) that NEVER emergency-closes
        # a restored position on a stale 'through' SL — it re-protects instead. (fix b)
        pos.__dict__["_restore_grace"] = True

        # Audit MED (2026-06-19): RE-SEED the frozen chandelier ATR_e and the entry-bar ts so
        # BOTH legs survive a restart with bit-exact exit fidelity. trader.attempt_entry stashes
        # these on pos.__dict__ at fresh-open ONLY (in-memory) — a restart loses them, so without
        # this the PM's _ensure_state hits its lazy fallback:
        #   * _donch_atr_e/_us29_atr_e unset -> ATR14 recomputed over the latest closed-bar frame
        #     (>=1 bar after entry) -> the chandelier trail width (hh - MULT*atr_e) CHANGES after
        #     a restart -> the F4-freeze the trader installed is undone on every restart.
        #   * _donch_entry_ts unset -> defaults to 0 -> the donchian 120-bar time-stop falls back
        #     to the in-memory _donch_bars counter that reset to 0 -> the full 120-bar clock
        #     re-arms and a 119/120-bar position can overstay its time-stop across restarts.
        # Route the leg-correct keys by the SAME xyz_ prefix the scanner / attempt_entry route on
        # (xyz_* -> strategy_us29 keys; native crypto -> strategy_donchian keys). entry_bar_ts is
        # the persisted signal-bar ts; fall back to opened_at (wall-clock fill ms) for legacy rows
        # that predate the column. NULL atr14 leaves the PM's lazy fallback in place (legacy rows).
        _is_xyz = str(coin).startswith("xyz_")
        _atr_key = "_us29_atr_e" if _is_xyz else "_donch_atr_e"
        _ts_key = "_us29_entry_ts" if _is_xyz else "_donch_entry_ts"
        _atr_db = float(row["atr14"]) if ("atr14" in keys and row["atr14"] is not None) else 0.0
        if _atr_db > 0:
            pos.__dict__[_atr_key] = _atr_db
        _ets = int(row["entry_bar_ts"]) if ("entry_bar_ts" in keys and row["entry_bar_ts"] is not None) else 0
        if _ets <= 0 and ("opened_at" in keys) and row["opened_at"]:
            try:
                from datetime import datetime as _dt
                _ets = int(_dt.fromisoformat(row["opened_at"]).timestamp() * 1000)
            except Exception:
                _ets = 0
        if _ets > 0:
            pos.__dict__[_ts_key] = _ets

        positions[coin] = pos
        log.info(
            "%s position %s %s %s: entry=%.6f sl=%.6f size=%s sl_oid=%s",
            "Restored" if startup else "ADOPTED (filled while running)",
            coin, row["tf"], direction, row["entry"], pos.sl_current, row["size"],
            pos.__dict__.get("_sl_order_id"),
        )

    if startup and positions:
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
            # Pass-through depth: HL stub has no L2 orderbook; gate MUST NOT block HL
            # entries (per design — liquidity enforced by size-fallback at entry, not
            # this coarse pre-filter). Without this, depth=0 -> gate excludes ALL coins.
            "depth_at_0.5pct_usd": 1e15,
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
            # Audit MED FIX-2 (2026-06-19): COVERAGE check, not just age.
            # The HL stub snapshot is a pass-through (depth=1e15) seeded from the CURRENT
            # universe, so it must cover every universe symbol. A snapshot generated by an
            # earlier run with a DIFFERENT universe (e.g. an xyz_-only snapshot) leaves
            # native crypto perps ABSENT -> apply_depth_gate sees n_with_profile>0 (xyz_
            # present) -> fail_closed=True -> EVERY native coin (profile=None) is excluded,
            # silently killing the entire crypto-perp -> strategy_donchian leg. The old
            # age>25h-only gate would not regenerate for up to ~25h. Regenerate whenever the
            # snapshot does NOT cover the current universe (any universe symbol missing).
            # Regen is safe + cheap: the stub is a deterministic pass-through.
            missing_cov = 0
            if snap_check is not None:
                _snap_syms = set(snap_check.coins.keys())
                missing_cov = sum(1 for a in universe if a.symbol not in _snap_syms)
            stale_age = age_hours is not None and age_hours > 25.0
            if snap_check is None or stale_age or missing_cov > 0:
                if missing_cov > 0 and not stale_age and snap_check is not None:
                    log.warning(
                        "HL snapshot regenerating: %d/%d universe symbols ABSENT from snapshot "
                        "(coverage gap, age=%.1fh) — stale snapshot would fail-close the depth "
                        "gate on the missing (native) coins",
                        missing_cov, len(universe), age_hours if age_hours is not None else -1.0,
                    )
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


# --- Loop-progress watchdog (ported from nado 2026-06-18 fix; kills the recurring
# silent-stall class — an unbounded-blocking SDK call on the hot path freezes the
# single-thread loop while systemd still sees `active`. Daemon force-exits on
# no-progress so systemd (Restart=on-failure) self-heals. Heartbeat=time.monotonic()
# (clock-skew immune).
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
                    "force-exiting so systemd restarts (recurring hung-fetch stall class).",
                    stalled_for, stall_limit_sec,
                )
                import os as _os
                _os._exit(1)
    th = threading.Thread(target=_watch, name="loop-watchdog", daemon=True)
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

    # XNN port 2026-06-10 (review fix): fresh-deploy startup assert. A brand-new xnn
    # deploy MUST start from an EMPTY trades.db (DEPLOY_CHECKLIST §2) — any pre-existing
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

    # ENV-FOOTGUN startup assert (2026-06-20). The donchian crypto leg's window N
    # (env DONCHIAN_K, config DEFAULT 20!) and TFs (env DONCHIAN_TFS) SILENTLY override
    # the validated v10d config (N=15, 8h-only). A wrong/empty env -> the bot trades an
    # UNvalidated config (4h bt = DD74%) with zero alarm. Refuse to start unless the
    # EFFECTIVE donchian config == validated. Code-pinned knobs (TP_R_MULTIPLE=999,
    # TRAIL_PIVOT_WINDOW=2, SL_BUFFER_PCT) are NOT env-overridable -> not asserted here.
    # (mem:project_combo_is_our_live_strategy_2026_06_20 ENV-FOOTGUN; class-rule:
    #  validated param overridable by env MUST have startup assert env==validated.)
    _vd_n = int(_strat_donchian_mod.DONCHIAN_N)
    _eff_k = settings.get_tf_filters("8h").donchian_k
    _eff_n = int(_eff_k) if _eff_k and int(_eff_k) > 0 else _vd_n
    if _eff_n != _vd_n:
        log.critical(
            "CONFIG-ASSERT: donchian leg N=%d != validated %d (env DONCHIAN_K override; "
            "config default is 20). Set .env.combo DONCHIAN_K=%d. REFUSING to trade "
            "unvalidated config.", _eff_n, _vd_n, _vd_n,
        )
        sys.exit(1)
    if _strat_donchian_mod.DONCHIAN_TFS != frozenset({"8h"}):
        log.critical(
            "CONFIG-ASSERT: donchian leg TFS=%s != validated {'8h'} (env DONCHIAN_TFS "
            "override; 4h bt = DD74%%). Set .env.combo DONCHIAN_TFS=8h. REFUSING to "
            "trade unvalidated config.", sorted(_strat_donchian_mod.DONCHIAN_TFS),
        )
        sys.exit(1)
    log.info("CONFIG-ASSERT: donchian leg N=%d TFS=%s == validated v10d — OK",
             _eff_n, sorted(_strat_donchian_mod.DONCHIAN_TFS))

    # ENV-FOOTGUN startup assert — US29 stock leg (2026-06-20, symmetry with donchian
    # cfg-2/cfg-3). The us29 validated config = F4_vb: SELECTOR=topm, TOP_M=3, REGIME_GATE
    # on, REGIME_COIN=xyz_SP500, SMA_N=200, REGIME_EXIT on. All are env-overridable money/
    # risk knobs — a wrong/empty env silently trades an UNvalidated us29 config with no
    # alarm (asymmetric vs the donchian assert above). Refuse to start unless effective ==
    # validated, with an explicit US29_ACK_UNVALIDATED=1 escape hatch for diagnostics.
    _us29_exit = os.getenv("US29_REGIME_EXIT", "0").strip().lower() not in ("0", "false", "no", "off", "")  # S3-1: incl "" to match trader.py:55 (empty=off) — kill assert-vs-runtime divergence
    _us29_ack = os.getenv("US29_ACK_UNVALIDATED", "").strip().lower() in ("1", "true", "yes", "on")
    _us29_bad = []
    if US29_SELECTOR != "topm":
        _us29_bad.append(f"US29_SELECTOR={US29_SELECTOR}!=topm")
    if US29_TOP_M != 3:
        _us29_bad.append(f"US29_TOP_M={US29_TOP_M}!=3")
    if not US29_REGIME_GATE:
        _us29_bad.append("US29_REGIME_GATE=off!=on")
    if US29_REGIME_COIN != "xyz_SP500":
        _us29_bad.append(f"US29_REGIME_COIN={US29_REGIME_COIN}!=xyz_SP500")
    if US29_REGIME_SMA_N != 200:
        _us29_bad.append(f"US29_REGIME_SMA_N={US29_REGIME_SMA_N}!=200")
    if not _us29_exit:
        _us29_bad.append("US29_REGIME_EXIT=off!=on")
    # S4-1: MAX_RUN_R is a SHARED PM cap; validated config for BOTH legs = no R-cap (=1000).
    # config default is now 1000 (fail-safe), but a stray low env would silently cap the now-
    # openable us29 xyz leg at +N R (premature exit, diverges from validated F4_vb no-cap).
    if int(getattr(settings, "max_run_r", 0) or 0) < 1000:
        _us29_bad.append(f"MAX_RUN_R={getattr(settings, 'max_run_r', None)}<1000(no-cap)")
    # cfg-3 contradiction: REGIME_EXIT=1 silently does NOTHING if REGIME_GATE is off
    # (the gate object that exit-on-flip reads is never constructed). Hard-fail — never
    # run with a money knob that the operator believes is armed but is inert.
    if _us29_exit and not US29_REGIME_GATE:
        log.critical(
            "CONFIG-ASSERT: US29_REGIME_EXIT=1 but US29_REGIME_GATE=off — exit-on-flip "
            "silently does NOTHING (regime gate not built). Enable US29_REGIME_GATE or "
            "unset US29_REGIME_EXIT. REFUSING to start with a dead money knob."
        )
        sys.exit(1)
    if _us29_bad and not _us29_ack:
        log.critical(
            "CONFIG-ASSERT: us29 leg != validated F4_vb: %s. Fix .env.combo or set "
            "US29_ACK_UNVALIDATED=1 to override. REFUSING to trade unvalidated config.",
            "; ".join(_us29_bad),
        )
        sys.exit(1)
    if _us29_bad:
        log.warning("CONFIG-ASSERT(us29): UNVALIDATED config ACKed (US29_ACK_UNVALIDATED=1): %s",
                    "; ".join(_us29_bad))
    else:
        log.info("CONFIG-ASSERT: us29 leg == validated F4_vb "
                 "(topm/TOP_M=3/REGIME_GATE/xyz_SP500/SMA200/EXIT) — OK")

    client = _build_client(settings)

    universe = load_universe(force_refresh=True)
    universe_tiers = {a.symbol: a.tier for a in universe}
    log.info("Universe: %d symbols (TIER1+2)", len(universe))

    # WS-PUSH candle feed (2026-06-24): subscribe once per (crypto-coin, interval) to HL
    # WS -> scans from memory, 0 REST /kline, 0 429. xyz_/HIP-3 NOW included (HL WS serves HIP-3 candles, verified 2026-06-27). REST fallback
    # built into candles(). env-gated HL_WS_CANDLE.
    if os.getenv("HL_WS_CANDLE", "0") == "1":
        try:
            from bot.ws_candle_feed import WsCandleFeed
            from bot.exchange_hl import coin_to_api
            _ws_coins = [coin_to_api(_a.symbol) for _a in universe]
            _ws_ivs = list(settings.working_tfs or [])
            _wsf = WsCandleFeed(_ws_coins, _ws_ivs)
            client._ws_feed = _wsf
            _wsf.start()
            log.info("HL WS-candle feed attached: %d coins x %d intervals (REST fallback on)",
                     len(_ws_coins), len(_ws_ivs))
        except Exception as _e:
            log.warning("HL WS-candle attach failed (REST fallback): %s", _e)

    # Audit HIGH (2026-06-19) — HIP-3 manual-position naked-trap STARTUP ASSERT.
    # (mem:project_hl_xnn_hip3_foreign_skip_naked_trap_2026_06_18 — the proposed
    #  "HIP3_ENABLE + fence-matches-universe -> CRITICAL/abort" guard, never added.)
    # Once DRY_RUN=0, the untracked-protect sweep (below) + naked sentinel place a bot
    # ±6% SL on ANY DB-less, SL-less exchange position — which is EXACTLY a manual ЮК
    # xyz position on this SHARED unified account (manual positions carry no trades.db
    # row). That ±6% SL fires before the manual position's wide static SL = off-plan
    # close. The combo bot OWNS the xyz_ leg so xyz_ cannot be blanket foreign-skipped;
    # the only safe go-live paths are (a) a MANUAL_POSITION_PREFIXES fence that exempts
    # the manual coins, or (b) an explicit operator ack that the shared-account risk is
    # accepted/handled. Refuse to start live otherwise. DRY (DRY_RUN=1) is always safe:
    # the sweep + sentinel are independently DRY-gated, so this assert is skipped in DRY.
    _hip3_on = os.getenv("UNIVERSE_HIP3_ENABLE", "1").strip().lower() not in (
        "0", "false", "no", "off",
    )
    _has_xyz = any(getattr(a, "symbol", "").startswith("xyz_") for a in universe)
    _manual_fence = bool(MANUAL_POSITION_PREFIXES) or any(
        p.startswith("xyz_") or p == "xyz" for p in FOREIGN_SKIP_PREFIXES
    )
    if (not dry_run) and _hip3_on and _has_xyz and not _manual_fence and not COMBO_ACK_SHARED_ACCOUNT:
        log.critical(
            "NAKED-TRAP ABORT: DRY_RUN=0 with HIP-3 xyz_ enabled (%d xyz_ symbols in "
            "universe) but NO fence protects MANUAL xyz positions on the shared unified "
            "account (FOREIGN_SKIP_PREFIXES=%s, MANUAL_POSITION_PREFIXES=%s). The "
            "untracked-protect sweep / naked sentinel would place a bot ±6%% SL on any "
            "manual ЮК xyz position (no DB row) = off-plan close before its wide static "
            "SL. REFUSING TO START. Either set MANUAL_POSITION_PREFIXES=xyz_ (exempt "
            "manual positions) OR set COMBO_ACK_SHARED_ACCOUNT=1 after deciding account "
            "ownership. (mem:project_hl_xnn_hip3_foreign_skip_naked_trap_2026_06_18)",
            sum(1 for a in universe if getattr(a, "symbol", "").startswith("xyz_")),
            ",".join(FOREIGN_SKIP_PREFIXES) or "(empty)",
            ",".join(MANUAL_POSITION_PREFIXES) or "(empty)",
        )
        sys.exit(1)
    if dry_run and _hip3_on and _has_xyz and not _manual_fence and not COMBO_ACK_SHARED_ACCOUNT:
        # DRY foot-gun visibility: do NOT abort in DRY (the sweep+sentinel are DRY-gated),
        # but make the missing manual-fence LOUD so it is fixed before any DRY_RUN=0 flip.
        log.critical(
            "NAKED-TRAP (DRY, no abort): HIP-3 xyz_ enabled but NO fence protects MANUAL "
            "xyz positions (FOREIGN_SKIP_PREFIXES=%s MANUAL_POSITION_PREFIXES=%s). Set "
            "MANUAL_POSITION_PREFIXES=xyz_ before flipping DRY_RUN=0. "
            "(mem:project_hl_xnn_hip3_foreign_skip_naked_trap_2026_06_18)",
            ",".join(FOREIGN_SKIP_PREFIXES) or "(empty)",
            ",".join(MANUAL_POSITION_PREFIXES) or "(empty)",
        )
    if _hip3_on and _has_xyz:
        log.info(
            "naked-trap guard: HIP-3 xyz_ on; manual-fence=%s ack=%s dry=%s (live=%s)",
            _manual_fence, COMBO_ACK_SHARED_ACCOUNT, dry_run, not dry_run,
        )

    snapshot_holder = _bootstrap_snapshot(universe=universe)
    _start_snapshot_reloader(snapshot_holder, interval_sec=600)
    _start_loop_watchdog(max(1800.0, 30.0 * settings.loop_interval_sec))
    snap = snapshot_holder.current()
    if snap is not None:
        log.info(
            "Liquidity snapshot active: %d coins, generated_at=%s, age=%.1fh",
            len(snap.coins), snap.generated_at_utc, snap.age_seconds / 3600.0,
        )

    # DEPTH_GATE: apply after snapshot so gate has depth data.
    # Fail-open if snapshot missing (gate disabled = entire universe passes).
    try:
        _eq_gate = client.account_value()
    except Exception:
        _eq_gate = 0.0
    _worst_order = (settings.risk_per_trade * _eq_gate / max(settings.min_sl_dist_pct, 1e-6)) if _eq_gate > 0 else None
    universe = apply_depth_gate(
        universe, snap, settings.liq_size_cap_pct, enabled=DEPTH_GATE_ENABLE,
        worst_order_notional_usd=_worst_order,
    )
    log.info(
        "Post-depth-gate universe: %d symbols (DEPTH_GATE_ENABLE=%s)",
        len(universe), DEPTH_GATE_ENABLE,
    )

    # write-db-row-PRE-order reconcile (panel 2026-06-21): promote/delete any 'pending' pre-order
    # rows left by a crash mid-entry BEFORE adopt, so a promoted one is picked up + SL-healed here.
    _reconcile_pending(client)
    open_positions: Dict[str, Position] = _adopt_db_open_positions(client, startup=True)

    # Cross-cycle debounce state for the orphan reduce-only TRIGGER sweep.
    orphan_trigger_seen: dict = {}

    # Resting stop-limit entry manager (FLAG 4 startup sweep runs before first tick)
    resting_manager = RestingOrderManager()
    resting_manager.dry_run = dry_run
    if settings.exchange == "hyperliquid":
        # Audit MED FIX-2 (2026-06-19): gate ONLY the startup ENTRY sweep by
        # RESTING_ORDERS_ENABLE (NOT the untracked-protect safety net below, which must
        # always run). The bot only ever PLACES non-reduce-only entry triggers via the
        # resting path (refresh(), also RESTING_ORDERS_ENABLE-gated at the tick loop
        # below). When the flag is OFF the bot places no entry triggers, so any entry
        # trigger on the shared unified account is foreign/manual — running the sweep
        # then would only risk a manual ЮК stop-buy (it is prefix-fenced, but skipping
        # removes the risk class entirely). When the flag is ON the sweep runs AND uses
        # the persisted bot-own oid registry to cancel our own warm-restart orphans
        # (incl. bot-own xyz_) while the prefix fence still preserves manual triggers.
        if RESTING_ORDERS_ENABLE:
            resting_manager.startup_orphan_sweep(client, open_positions)
        else:
            log.info(
                "startup_orphan_sweep: SKIPPED (RESTING_ORDERS_ENABLE=0 — bot places no "
                "entry triggers; nothing bot-own to sweep, manual triggers untouched)"
            )
        # SAFETY NET (root fix 2026-05-31): protect any EXCHANGE position not tracked in
        # DB and lacking an SL — a naked orphan from a resting fill during a restart/runtime
        # window (universe expansion surfaced DOGE/TRX naked). account_coins guard prevents
        # re-entry; position rides the protective SL until manually adopted.
        try:
            _ex = client.open_positions() or {}
            # Audit HIGH (2026-06-19) refine: MANUAL exemption must be DB-ROW-aware.
            # A coin is MANUAL only if it matches MANUAL_POSITION_PREFIXES AND has NO
            # trades.db open row. The bot owns the us29 xyz_ leg -> those positions DO
            # carry an open row -> NOT manual -> still get protective SL coverage.
            try:
                _db_open_coins = {str(_r["coin"]) for _r in (open_trades() or [])}
            except Exception as _dbe:
                log.warning("untracked-protect: open_trades() failed (%s) -> treating "
                            "all matching-prefix coins as manual (fail-safe)", _dbe)
                _db_open_coins = set()
            for _coin, _v in _ex.items():
                # XNN port 2026-06-10 (foreign-safety): never place a protective SL on a
                # FOREIGN position (e.g. xyz_* held by the stopped uk_v102 bots — their
                # static SLs may be transiently invisible; a bot-added ±6% SL would close
                # them off-plan). Also: this sweep places REAL orders — skip it in DRY.
                if dry_run:
                    log.info("untracked-protect sweep: DRY_RUN — skipped entirely")
                    break
                if FOREIGN_SKIP_PREFIXES and _coin.startswith(FOREIGN_SKIP_PREFIXES):
                    log.info("untracked-protect skip %s: foreign prefix (FOREIGN_SKIP_PREFIXES)", _coin)
                    continue
                # Audit HIGH (2026-06-19): exempt MANUAL positions (e.g. manual ЮК xyz on
                # the shared unified account, which carry NO trades.db row). Without this,
                # a manual position with a wide static SL but no *visible* reduce-only SL
                # order gets a bot ±6% SL that fires off-plan. Distinct from the foreign-
                # bot fence above. (mem:project_manual_positions_live)
                if (MANUAL_POSITION_PREFIXES and _coin.startswith(MANUAL_POSITION_PREFIXES)
                        and _coin not in _db_open_coins):
                    # UNRECONCILED guard (panel must-fix #5, 2026-06-21): a manual-prefix xyz_ with
                    # no db-row is NORMALLY a user manual position (skip). BUT if it is NAKED (no
                    # live reduce-only SL on the book), we CANNOT prove own-vs-manual — a BOT
                    # position whose db-row was lost (crash before journal insert / db reset) would
                    # be silently mis-classified manual and ride NAKED with no trail/SL-heal. Raise
                    # CRITICAL instead of defaulting to manual; do NOT auto-SL (a true manual pos
                    # must not get a bot ±6% SL = the original naked-trap). Loud → operator decides.
                    _has_sl = True
                    try:
                        _has_sl = bool(client.list_open_sl_orders(_coin))
                    except Exception:
                        _has_sl = True  # can't prove naked → don't false-alarm
                    if not _has_sl:
                        log.critical(
                            "UNRECONCILED %s: manual-prefix xyz_ with NO trades.db row AND NO live "
                            "SL on book — cannot classify own-vs-manual. If this is a BOT position "
                            "with a lost db-row it is riding NAKED (no trail/heal). NOT auto-"
                            "protected (could be manual). MANUAL REVIEW REQUIRED.", _coin,
                        )
                    else:
                        log.info("untracked-protect skip %s: manual position "
                                 "(MANUAL_POSITION_PREFIXES, no trades.db row, has live SL)", _coin)
                    continue
                try:
                    _szi = float(_v.get("szi", _v.get("size", 0)) or 0)
                except (TypeError, ValueError):
                    continue
                if abs(_szi) <= 0 or _coin in open_positions:
                    continue
                if _szi < 0 and not settings.short_enabled_tfs:
                    log.critical("untracked-protect skip %s: SHORT position but bot is long-only "
                                 "(short_enabled_tfs empty) — foreign/manual by construction, NOT touching", _coin)
                    continue
                try:
                    if client.list_open_sl_orders(_coin):
                        continue
                except Exception:
                    pass
                _long = _szi > 0
                _side = "long" if _long else "short"
                _mark = client.mark_price(_coin)
                if not _mark or _mark <= 0:
                    log.error("untracked-protect %s: no mark — STILL NAKED, manual", _coin)
                    continue
                _sl = _mark * 0.94 if _long else _mark * 1.06
                # sl5-2 (2026-06-20): route the protective ±6% SL through the liq-guard so a
                # thin high-lev ISOLATED xyz_ does not get an SL resting OUTSIDE liquidation
                # (REMEDY-A add-margin / REMEDY-B clamp). The raw ±6% bypassed the guard.
                try:
                    _sl, _liq_act = ensure_sl_inside_liq(
                        client=client, coin=_coin, side=_side, sl_px=_sl, size=abs(_szi),
                    )
                    if _liq_act not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
                        log.warning("untracked-protect liq-guard %s: action=%s sl→%.6f", _coin, _liq_act, _sl)
                except Exception as _le:
                    log.warning("untracked-protect %s: liq-guard threw (%s) — placing raw ±6%% SL", _coin, _le)
                _oid = _place_sl_with_retry(client, _coin, abs(_szi), _sl, side=_side)
                if _oid:
                    log.warning("PROTECTED untracked naked position %s sz=%.4f SL=%.6f oid=%s", _coin, _szi, _sl, _oid)
                else:
                    log.error("untracked-protect %s: SL placement FAILED — STILL NAKED, manual", _coin)
        except Exception as _e:
            log.warning("untracked-position protect sweep failed: %s", _e)

    scanner = Scanner(settings)
    # Audit HIGH (2026-06-19): build ONE PositionManager PER LEG. A crypto position
    # opened by the donchian breakout must be managed by the donchian PM (chandelier
    # ATR trail + 4R TP + 120-bar time_stop), NOT the us29 F4_vb PM. _pm_for(coin)
    # routes by symbol (xyz_* -> us29, else donchian), mirroring scanner._strat_for.
    _pm_kwargs = dict(
        be_buffer_pct=settings.trail_after_tp_buffer_pct,
        vstop_pivot_window=settings.vstop_pivot_window,
        max_run_r=settings.max_run_r,
        vstop_buffer_pct=settings.vstop_buffer_pct,
        tp1_partial_frac=settings.tp1_partial_frac,
    )
    position_manager_us29 = PositionManager(**_pm_kwargs)
    position_manager_donchian = DonchianPositionManager(**_pm_kwargs)
    # Back-compat alias: legacy single-PM name still points at the us29 PM, but the
    # manage loop selects via _pm_for() — do NOT pass `position_manager` blindly.
    position_manager = position_manager_us29

    def _is_xyz_coin(sym: str) -> bool:
        return isinstance(sym, str) and sym.startswith("xyz_")

    def _pm_for(sym: str):
        """PositionManager handling this coin's EXIT (us29 for xyz_*, donchian otherwise)."""
        return position_manager_us29 if _is_xyz_coin(sym) else position_manager_donchian

    def _compute_indicators_for(sym: str):
        """compute_indicators of the leg that owns this coin (xyz_* -> us29, else donchian)."""
        mod = _strat_us29_mod if _is_xyz_coin(sym) else _strat_donchian_mod
        return mod.compute_indicators

    # Manage-loop candle window MUST cover the donchian time-stop horizon so the
    # restart-persistent recovery in strategy_donchian (bars_held = max(_donch_bars,
    # count(entry_ts < ts <= cur_bar_ts))) can actually reach MAX_HOLD_BARS after a
    # restart. A fixed limit=50 capped ts_held at ~50 < 120, so a donchian (8h)
    # position older than 50 bars that survived a restart overstayed its 120-bar
    # time-stop by up to ~70 bars (Audit MED 2026-06-19). The us29 leg has NO time-
    # stop and only a raise-only hh-recompute (can stall, never miss a hard exit),
    # but we fetch the same generous window so its restart hh-recovery is also full-
    # fidelity. Buffer of 10 absorbs partial/forming bars + ts-dedup so the closed-
    # bar count clears 120.
    _MANAGE_CANDLE_LIMIT = max(50, int(_strat_donchian_mod.MAX_HOLD_BARS) + 10)

    def _manage_candle_limit(sym: str) -> int:
        """Per-tick manage fetch window. Both legs use a window >= the donchian time-
        stop horizon so a restarted position's bars-held recovery is not capped by the
        candle-window size (which otherwise defeated the bit-exact time-stop fix)."""
        return _MANAGE_CANDLE_LIMIT

    # ── US29 selection layer ────────────────────────────────────────────────────────────
    # CANONICAL = F4_vb (top-M of the day + SPY/SMA200 index regime gate). The legacy
    # expanding-percentile TopKPool (pool30) is opt-in ONLY (US29_SELECTOR=pool30).
    from bot.selector_us29 import RegimeGate, select_topm
    _us29_regime_gate = None
    _us29_topk = None
    if US29_SELECTOR == "topm":
        if US29_REGIME_GATE:
            _us29_regime_gate = RegimeGate(
                client, regime_coin=US29_REGIME_COIN, sma_n=US29_REGIME_SMA_N,
                candles_limit=int(os.getenv("SCAN_CANDLES_LIMIT", "3000")),
            )
            log.info(
                "US29 SELECTOR=topm (F4_vb): top-%d of the day by (-score,coin); REGIME GATE ON "
                "(index=%s SMA%d — block new entries when close<SMA200)",
                US29_TOP_M, US29_REGIME_COIN, US29_REGIME_SMA_N,
            )
        else:
            log.warning(
                "US29 SELECTOR=topm: top-%d of the day; REGIME GATE OFF (US29_REGIME_GATE=0) "
                "— F4_vb edge requires the SPY/SMA200 gate; running WITHOUT it", US29_TOP_M,
            )
    elif US29_SELECTOR == "pool30":
        from bot.topk_pool import TopKPool
        if US29_TOPK_ENABLE:
            _pool_path = US29_TOPK_POOL_PATH
            if not os.path.isabs(_pool_path):
                _pool_path = str(PROJECT_ROOT / _pool_path)
            _us29_topk = TopKPool(_pool_path, pct=US29_TOPK_PCT,
                                  min_pool=US29_TOPK_MIN_POOL, pool_max=US29_TOPK_POOL_MAX)
            log.warning(
                "US29 SELECTOR=pool30 (LEGACY, parity-FAIL 14.65%%/DD83.3): TOPK gate pct=%.0f "
                "min_pool=%d pool_max=%d path=%s — NOT the validated F4_vb edge",
                US29_TOPK_PCT, US29_TOPK_MIN_POOL, US29_TOPK_POOL_MAX, _pool_path,
            )
        else:
            log.warning("US29 SELECTOR=pool30 with TOPK DISABLED — accepting ALL signals")
    else:
        raise RuntimeError(f"US29_SELECTOR={US29_SELECTOR!r} invalid (expected 'topm' or 'pool30')")

    # Daily opens counter (UTC day rollover)
    opens_today = 0
    current_day = time.gmtime().tm_yday
    iteration = 0
    last_universe_refresh = time.time()

    # Startup de-sync: both bot procs are started by systemd at the same instant
    # and cold-fetch the full universe in lockstep → 429 burst on one shared IP.
    # A random pre-scan offset (per proc) breaks the lockstep.
    _start_jitter = random.uniform(0, float(os.getenv("HL_START_JITTER_SEC", "25")))
    log.info("Startup de-sync: sleeping %.1fs before first scan loop", _start_jitter)
    time.sleep(_start_jitter)

    # ── WS-STORE PRE-WARM (2026-06-27, background) ───────────────────────────────────────
    # Problem: the FIRST FULL scan after a restart that lands near a bar boundary runs
    # entirely on cold REST — the WS push-store (client._ws_feed) is empty until each
    # (coin,tf) makes its first candles() call, which only then seeds the store (236-304s
    # cold scan). Fix: warm the store by serially fetching candles() for every
    # (coin,interval); candles() already seeds _ws_feed from its REST df_full on every
    # fetch (exchange_hl WS-seed, correct finality), so after this the scan is served from
    # WS, not cold REST. Crypto AND xyz_ (HIP-3, now WS-served) coins are warmed.
    #
    # CRITICAL: this runs in a BACKGROUND DAEMON THREAD, NOT inline before the while-loop.
    # The full warm is ~hundreds of throttled REST fetches (minutes); running it inline
    # would block the main loop's POSITION MANAGEMENT (trail/exit re-checks on open
    # positions) for that whole time. The first full scan is bar-close-triggered (next
    # 4h/8h/1d boundary, typically 10s of minutes away), so a background warm has ample
    # time to fill the store before then while manage/exit runs unblocked from t=0.
    # Strictly additive candle-plumbing: per-coin errors swallowed; REST fallback intact;
    # the loop's own candles() calls share the same bar-aligned cache (no double-fetch).
    if getattr(client, "_ws_feed", None) is not None:
        def _ws_prewarm():
            try:
                _pw_limit = int(os.getenv("SCAN_CANDLES_LIMIT", "300"))
                _pw_tfs = list(settings.working_tfs or [])
                _pw_n = 0
                _pw_t0 = time.time()
                for _pw_asset in universe:
                    _pw_sym = getattr(_pw_asset, "symbol", None)
                    if not _pw_sym:
                        continue
                    for _pw_tf in _pw_tfs:
                        try:
                            _pw_df = client.candles(_pw_sym, _pw_tf, limit=_pw_limit)
                            # candles() already seeds _ws_feed from df_full (forming bar
                            # incl, correct finality). NOT re-seeding the closed-only df
                            # here — that would mark the latest closed bar non-final and
                            # defeat get_df. The internal seed is the authoritative warm.
                            if _pw_df is not None and len(_pw_df) > 0:
                                _pw_n += 1
                        except Exception as _pw_e:
                            log.debug("WS pre-warm %s %s skipped: %s", _pw_sym, _pw_tf, _pw_e)
                try:
                    _pw_stats = client._ws_feed.stats()
                except Exception:
                    _pw_stats = {}
                log.info(
                    "WS-STORE PRE-WARM done: %d/(coins=%d x tfs=%d) fetched in %.1fs; "
                    "ws_feed=%s — first full scan served from WS, not cold REST",
                    _pw_n, len(universe), len(_pw_tfs), time.time() - _pw_t0, _pw_stats,
                )
            except Exception as _pw_outer:
                log.warning("WS-STORE PRE-WARM thread failed (REST fallback intact): %s", _pw_outer)
        try:
            threading.Thread(target=_ws_prewarm, name="ws-prewarm", daemon=True).start()
            log.info("WS-STORE PRE-WARM started in background (manage loop unblocked)")
        except Exception as _pw_start:
            log.warning("WS-STORE PRE-WARM thread start failed (REST fallback intact): %s", _pw_start)

    # ── CANDIDATE WARM-KEEPER (2026-06-29): keep the just-closed bar of near-trigger coins
    # PRIMED at each boundary so the scan serves them instantly instead of waiting 9-54s for
    # HL's sparse WS push. Additive/parity-safe (same candles() bar), rate-safe (hard-capped,
    # throttled), places NO orders. Background daemon — manage loop unblocked.
    try:
        from bot.candidate_warmer import start_candidate_warmer
        start_candidate_warmer(client, universe, settings, log)
    except Exception as _cw_e:
        log.warning("candidate-warmer start failed (additive, scan unaffected): %s", _cw_e)

    # LEG-SPLIT (2026-06-29): us29(xyz 4h/1d) and crypto(8h) are independent legs sharing no TF;
    # entering us29 BEFORE the crypto-8h scan removes the ~20s 8h-walk from a 4h entry. Parity-safe
    # by construction (legs share no TF -> signaled_coins/topm-pool/entry-order/caps preserved).
    _LEG_SPLIT = os.getenv("LEG_SPLIT_SCAN", "0") == "1"
    _US29_LEG_TFS = {t for t in ("4h", "1d") if t in set(settings.working_tfs)}
    _CRYPTO_LEG_TFS = set(settings.working_tfs) - {"4h", "1d"}
    # CRYPTO-FIRST (2026-07-02): crypto(8h) is the pass-through money leg and the cheapest
    # walk; run it BEFORE the ~200s xyz us29 walk so crypto entries never wait behind stocks.
    # Parity-safe: legs share no TF (assert above). Kill-switch CRYPTO_LEG_FIRST (default 1).
    _CRYPTO_FIRST = os.getenv("CRYPTO_LEG_FIRST", "1") == "1"
    if _LEG_SPLIT:
        _LEG_GROUPS = (_CRYPTO_LEG_TFS, _US29_LEG_TFS) if _CRYPTO_FIRST else (_US29_LEG_TFS, _CRYPTO_LEG_TFS)
        log.info("LEG-SPLIT scan ON (crypto_first=%s): %s then %s", _CRYPTO_FIRST, sorted(_LEG_GROUPS[0]), sorted(_LEG_GROUPS[1]))
    else:
        _LEG_GROUPS = (None,)
    while True:
        _loop_progress_beat()
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
                # Re-apply depth gate after refresh (snapshot may also have refreshed)
                _snap_now = snapshot_holder.current()
                try:
                    _eq_gate = client.account_value()
                except Exception:
                    _eq_gate = 0.0
                _worst_order = (settings.risk_per_trade * _eq_gate / max(settings.min_sl_dist_pct, 1e-6)) if _eq_gate > 0 else None
                universe = apply_depth_gate(
                    universe, _snap_now, settings.liq_size_cap_pct, enabled=DEPTH_GATE_ENABLE,
                    worst_order_notional_usd=_worst_order,
                )
                log.info("Universe refreshed: %d symbols (post-depth-gate)", len(universe))
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
            # PER-TICK RECONCILER (class fix 2026-06-07): adopt any DB-open position that is
            # live on the exchange but not tracked in-memory (e.g. a resting/manual stop-entry
            # that filled mid-run, or a position dropped by a prior phantom-close). Runs BEFORE
            # the manage loop so the SL-liveness guard heals it the same tick — bounding the
            # naked window to ≤1 tick. Never clobbers a tracked position.
            try:
                open_positions = _adopt_db_open_positions(client, open_positions)
            except Exception as _ae:
                log.warning("per-tick adopt failed: %s", _ae)
            closed_coins = set()
            _regime_off_now = False
            if _us29_regime_gate is not None:
                try:
                    _rg_on, _, _ = _us29_regime_gate.state()
                    _regime_off_now = not _rg_on
                except Exception:
                    _regime_off_now = False  # fail-open: never force-exit on a gate error
            for coin, pos in list(open_positions.items()):
                try:
                    df = client.candles(coin, pos.tf, limit=_manage_candle_limit(coin))
                    if df is not None and not df.empty:
                        # Route indicators per-leg: a crypto position needs the donchian
                        # indicator stack (ATR/donchian), an xyz_ position the us29 stack.
                        df = _compute_indicators_for(coin)(df)

                    exit_reason = manage_open_position(
                        pos=pos, client=client, settings=settings,
                        position_manager=_pm_for(coin),
                        df_latest={pos.tf: df},
                        regime_off=_regime_off_now,
                    )
                    if exit_reason is not None:
                        closed_coins.add(coin)
                        log.info("Position closed %s: %s", coin, exit_reason)
                except Exception as e:
                    log.error("manage_open_position(%s) error: %s", coin, e, exc_info=True)

            for coin in closed_coins:
                open_positions.pop(coin, None)

            # --- Sweep orphan reduce-only TRIGGER orders (class-fix 2026-06-15) ---
            # Cancel SL/TP triggers left resting after their position closed (incl.
            # HIP-3 dexes). Exempts manual/foreign + any coin with a live position;
            # fail-safe (skips on any indeterminate read).
            if not dry_run:
                try:
                    sweep_orphan_triggers(client, open_positions, orphan_trigger_seen, log)
                except Exception as _se:
                    log.warning("orphan-sweep dispatch error: %s", _se)

            # Live-account dedup (a/b SHARE one HL account): block coins held by
            # EITHER bot on the exchange, not just this bot's in-memory positions.
            # Prevents self-hedge (a long BTC + b short BTC) + reduce_only SL crossfire.
            try:
                account_coins = set(client.open_positions().keys())
            except Exception as e:
                log.warning("account positions fetch for dedup failed: %s — using in-memory", e)
                account_coins = set(open_positions.keys())

            no_long = _no_long_symbols(universe) if CRYPTO_SHORT_ONLY else set()

            # --- Resting stop-limit management (runs EVERY tick, not bar-close gated) ---
            # XNN port 2026-06-10: gated by RESTING_ORDERS_ENABLE — the resting path's entry
            # logic is inline Donchian (uk_v102), NOT the plugged strategy. xnn .env sets 0.
            if settings.exchange == "hyperliquid" and RESTING_ORDERS_ENABLE:
                # Fetch equity for sizing (cached 5s inside account_value())
                try:
                    _equity = client.account_value()
                except Exception as _ev:
                    log.warning("account_value() for resting failed: %s", _ev)
                    _equity = 0.0
                resting_manager.refresh(
                    coins=universe,
                    client=client,
                    open_positions=open_positions,
                    account_coins=account_coins,
                    settings=settings,
                    n_open=len(open_positions),
                    equity=_equity,
                    snapshot_holder=snapshot_holder,
                )
                # Detect resting fills and handle SL + journal IMMEDIATELY
                # (same loop iteration — minimises naked-SL window to < 1 loop tick)
                # account_positions is the same dict used for account_coins above
                try:
                    _account_positions = client.open_positions()
                except Exception as _ope:
                    log.warning("open_positions() for resting fill detect failed: %s", _ope)
                    _account_positions = {}
                resting_fills = resting_manager.detect_resting_fills(
                    _account_positions, open_positions,
                )
                for _rkey, _entry_px, _filled_sz in resting_fills:
                    _coin, _tf, _side = _rkey
                    _ro = resting_manager.consume_fill(_rkey)
                    if _ro is None:
                        continue
                    log.info(
                        "RESTING FILL %s %s %s: entry=%.6f sz=%.4f — placing SL",
                        _coin, _tf, _side, _entry_px, _filled_sz,
                    )
                    # CHANGE C — FLAG 3: partial-fill min-size guard.
                    # If the filled notional is below liq_min_trade_usd the position is
                    # uneconomic (fees + spread > expected PnL). Close it immediately
                    # with market reduce-only instead of placing a protective SL that
                    # would never recoup costs.
                    _fill_notional = _filled_sz * _entry_px
                    if _fill_notional < settings.liq_min_trade_usd:
                        log.warning(
                            "RESTING FILL min-size: %s %s %s notional=$%.2f < min=$%.2f "
                            "— closing dust position (market reduce-only)",
                            _coin, _tf, _side, _fill_notional, settings.liq_min_trade_usd,
                        )
                        _ensure_flat(client, _coin, is_buy_open=(_side == "long"))
                        resting_manager.cancel_all_for_coin(client, _coin)
                        continue
                    _sl_oid = _place_sl_with_retry(
                        client=client, coin=_coin, size=_filled_sz,
                        sl_price=_ro.sl_price, side=_side,
                    )
                    if _sl_oid is None:
                        log.error(
                            "RESTING FILL SL FAILED 3x %s %s — emergency close",
                            _coin, _tf,
                        )
                        _ensure_flat(client, _coin, is_buy_open=(_side == "long"))
                        continue
                    _slip = ((_entry_px - _ro.trigger_px) / _ro.trigger_px) if _side == "long" \
                        else ((_ro.trigger_px - _entry_px) / _ro.trigger_px)
                    _risk_d = _ro.size * _ro.sl_dist_abs
                    _trade_id = insert_trade(
                        coin=_coin, tf=_tf,
                        entry=_entry_px,
                        entry_intended=_ro.trigger_px,
                        sl_initial=_ro.sl_price,
                        tp1=_ro.tp1_price,
                        size=_filled_sz,
                        risk_dollars=_risk_d,
                        notional=_filled_sz * _entry_px,
                        walk_slip_pct=_slip,
                        entry_order_id=None,
                        notes=(
                            f"resting_fill f1_dist={_ro.f1_dist:.2f} "
                            f"pivot_h={_ro.pivot_high:.6f} pivot_l={_ro.pivot_low:.6f}"
                        ),
                        direction=_side,
                    )
                    from bot.journal import update_trade_sl_order
                    if _sl_oid and _trade_id:
                        update_trade_sl_order(_trade_id, _sl_oid)
                    _pos = Position(
                        coin=_coin, tf=_tf,
                        entry_price=_entry_px,
                        sl_initial=_ro.sl_price,
                        sl_current=_ro.sl_price,
                        tp1_price=_ro.tp1_price,
                        size=_filled_sz,
                        bar_entry_idx=0,
                        side=_side,
                    )
                    _pos.__dict__["_trade_id"] = _trade_id
                    _pos.__dict__["_sl_order_id"] = _sl_oid
                    _pos.__dict__["_orig_size"] = _filled_sz
                    _pos.__dict__["_sl_placed_px"] = _ro.sl_price
                    _pos.__dict__["_tp_oid"] = None
                    open_positions[_coin] = _pos
                    opens_today += 1
                    # Cancel any other resting orders for this coin
                    resting_manager.cancel_all_for_coin(client, _coin)
                    client.invalidate_positions_cache()
                    log.info(
                        "RESTING ENTRY OK %s %s %s: entry=%.6f sl=%.6f sz=%.4f sl_oid=%s",
                        _coin, _tf, _side, _entry_px, _ro.sl_price, _filled_sz, _sl_oid,
                    )

            # STREAMING ENTRY (2026-07-02): ONE entry-decision path shared by the post-scan
            # batch loop AND the mid-scan crypto callback so they cannot diverge. Crypto
            # (pass-through) enters the instant it is detected; xyz/us29 stays batch (topM
            # needs the whole list). nonlocal opens_today (no box), commit open_positions
            # FIRST, cap streamed order-REST/scan (429-safety), env kill-switches.
            _stream_state = {"stopped": False, "streamed": 0}
            _STREAM_MAX = int(os.getenv("CRYPTO_STREAM_MAX_PER_SCAN", "8"))

            def _try_enter_signal(signal, _via_stream=False):
                nonlocal opens_today
                if len(open_positions) >= settings.max_concurrent:
                    log.info("SKIP %s %s: max_concurrent=%d reached", signal.coin, signal.tf, settings.max_concurrent)
                    return "stop"
                if settings.max_opens_per_day > 0 and opens_today >= settings.max_opens_per_day:
                    log.info("SKIP %s %s: max_opens_per_day=%d reached", signal.coin, signal.tf, settings.max_opens_per_day)
                    return "stop"
                if signal.coin in open_positions or signal.coin in account_coins:
                    return "skip"
                if signal.coin in resting_manager.tracked_coins():
                    log.debug("SKIP bar-close %s %s: resting order already live", signal.coin, signal.tf)
                    return "skip"
                bar_age = scanner.bar_age_sec(signal.tf)
                if dry_run:
                    log.info("[DRY-RUN] Signal %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1=%.2f bar_age=%.0fs",
                             signal.coin, signal.tf, signal.trigger_price, signal.sl_price,
                             signal.tp1_price, signal.f1_dist, bar_age)
                    return "skip"
                try:
                    pos = attempt_entry(signal=signal, client=client, settings=settings,
                                        universe_tiers=universe_tiers, bar_age_sec=bar_age,
                                        snapshot_holder=snapshot_holder)
                except Exception as _ee:
                    log.error("attempt_entry %s %s raised (no order tracked): %s", signal.coin, signal.tf, _ee, exc_info=True)
                    return "skip"
                if pos is None:
                    return "skip"
                # COMMIT open_positions FIRST (adversary risk#3): before any bookkeeping that
                # could throw, so a later raise can never leave the fill untracked / let the
                # batch loop double-place the same coin.
                open_positions[signal.coin] = pos
                opens_today += 1
                log.info("ENTRY OK %s %s: entry=%.6f sl=%.6f size=%s opens_today=%d%s",
                         pos.coin, pos.tf, pos.entry_price, pos.sl_current, pos.size,
                         opens_today, " [stream]" if _via_stream else "")
                try:
                    if hasattr(client, "verify_builder_on_recent_fill"):
                        _bok, _bdet = client.verify_builder_on_recent_fill(pos.coin)
                        if not _bok:
                            log.error("BUILDER-MONITOR %s: %s — airdrop attribution BROKEN, "
                                      "INVESTIGATE (risk: account shutdown / lost airdrop)", pos.coin, _bdet)
                        else:
                            log.info("builder-monitor %s: %s", pos.coin, _bdet)
                except Exception as _be:
                    log.warning("builder-monitor %s: check threw (non-fatal): %s", pos.coin, _be)
                return "entered"

            def _on_crypto_signal(sig):
                if _stream_state["stopped"] or _stream_state["streamed"] >= _STREAM_MAX:
                    return
                r = _try_enter_signal(sig, _via_stream=True)
                if r == "stop":
                    _stream_state["stopped"] = True
                elif r == "entered":
                    _stream_state["streamed"] += 1

            for _leg_tfs in _LEG_GROUPS:
                _stream_state["stopped"] = False
                _stream_state["streamed"] = 0
                _stream_on = (not dry_run and os.getenv("CRYPTO_STREAM_ENTRY", "1") == "1"
                              and (_leg_tfs is None or _leg_tfs == _CRYPTO_LEG_TFS))
                signals = scanner.scan_all_coins(
                    coins=universe, client=client, open_positions=open_positions,
                    account_coins=account_coins,
                    no_long_symbols=no_long,
                    tfs=_leg_tfs,
                    on_crypto_signal=(_on_crypto_signal if _stream_on else None),
                )

                # ── PER-LEG selection: the F4_vb top-M + SPY/SMA200 regime gate are an
                # EQUITIES-only filter and MUST apply ONLY to the xyz_ (US29 stocks) leg.
                # COMBO-FIX (2026-06-19): split the unified candidate list by leg BEFORE
                # selection. The crypto/donchian breakouts have NO relation to the S&P500
                # regime and are NOT part of the US29 per-day top-M cap, so:
                #   * xyz_   signals -> US29 selector (top-M of the day + regime gate)
                #   * crypto signals -> pass through unfiltered (donchian owns its own exits;
                #                       MAX_OPENS_PER_DAY below is the shared per-day backstop).
                # Prior bug: select_topm(regime_on=False) -> [] suppressed ALL signals incl.
                # native crypto, and top-M=3 over the combined list let stocks evict crypto.
                _xyz_sigs = [s for s in signals if _is_xyz_coin(getattr(s, "coin", ""))]
                _crypto_sigs = [s for s in signals if not _is_xyz_coin(getattr(s, "coin", ""))]
                if US29_SELECTOR == "topm":
                    _regime_on = True
                    if _us29_regime_gate is not None:
                        _on, _spy_c, _spy_sma = _us29_regime_gate.state()
                        _regime_on = _on
                        if not _on and _xyz_sigs:
                            log.info(
                                "US29 REGIME OFF: %s close=%s <= SMA%d=%s — blocking xyz_ stock "
                                "entries this cycle (%d xyz candidate(s) suppressed; %d crypto "
                                "signal(s) UNAFFECTED; held positions keep trailing)",
                                US29_REGIME_COIN, _spy_c, US29_REGIME_SMA_N, _spy_sma,
                                len(_xyz_sigs), len(_crypto_sigs),
                            )
                    _n_xyz_before = len(_xyz_sigs)
                    _xyz_kept = select_topm(_xyz_sigs, US29_TOP_M, _regime_on)
                    if _n_xyz_before:
                        log.info(
                            "US29 TOPM gate (xyz only): %d/%d kept (top_m=%d regime_on=%s); "
                            "crypto leg passes %d signal(s) through",
                            len(_xyz_kept), _n_xyz_before, US29_TOP_M, _regime_on,
                            len(_crypto_sigs),
                        )
                    # Crypto donchian breakouts bypass the equities filter entirely.
                    signals = _xyz_kept + _crypto_sigs
                elif _us29_topk is not None and _xyz_sigs:
                    # LEGACY pool30 path (opt-in only) — also xyz-only; crypto passes through.
                    signals = _us29_topk.filter_and_update(_xyz_sigs) + _crypto_sigs

                for signal in signals:
                    if _try_enter_signal(signal) == "stop":
                        break
        except Exception as e:
            log.error("Main loop error (iter %d): %s", iteration, e, exc_info=True)

        if run_once:
            log.info("--once flag: exiting after single iteration")
            break

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
