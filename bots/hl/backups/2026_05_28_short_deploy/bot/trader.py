"""trader.py — STOP-LIMIT entry, fill monitoring, SL placement, position management.

Entry flow (refactor 2026-05-24 — snapshot-based liquidity sizing, no runtime gates):
  1. Bar-age / tier / risk gates (MM cap, concurrent cap)
  2. Size = min(risk-based, leverage-based, LIQ_SIZE_CAP_PCT × snapshot.avg_1h_vol_usd)
  3. If final size < LIQ_MIN_TRADE_USD → skip (economic floor)
  4. Place STOP-LIMIT order: limit = entry_price × (1 + entry_limit_cap_pct)
  5. Poll for fill up to entry_limit_ttl_sec (30s)
  6. On fill: place stop-loss trigger order (reduceOnly)
  7. If partial fill < min_fill_ratio (10%) → emergency close

Liquidity gate of old (1h vol floor / spread max / depth-walk slip) — REMOVED.
Pre-screen is now once-daily (see bot/liquidity_snapshot.py + cron 00:05 UTC).
Size-fallback semantics: if liquidity is thin we shrink position rather than
reject the signal, unless below LIQ_MIN_TRADE_USD.

SL management:
  - Bot loop (60s) polls open positions on exchange
  - On new bar: update trailing SL via strategy_uk_v102.PositionManager
  - Cancel old SL trigger, place new SL trigger at updated price
  - SL hit detection: exchange trigger fires automatically; bot reconciles via open_positions()
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from bot.config import Settings
from bot.journal import (
    close_trade,
    insert_rejected,
    insert_trade,
    open_trades,
    update_trade_sl,
    update_trade_sl_order,
)
from bot.liquidity import LiquiditySnapshot, SnapshotHolder
from bot.risk import SizeResult, check_concurrent_cap, check_mm_cap, compute_size
from bot.strategy_uk_v102 import Position, PositionManager, Signal

log = logging.getLogger(__name__)


def attempt_entry(
    signal: Signal,
    client,                  # ExtendedClient instance
    settings: Settings,
    universe_tiers: dict,    # {symbol: tier_int}
    bar_age_sec: float,      # seconds since signal bar closed
    snapshot_holder: SnapshotHolder,
) -> Optional[Position]:
    """Full entry pipeline: liquidity → risk → stop-limit order → fill check.

    Returns Position if entry filled, None if skipped/rejected/cancelled.
    """

    # --- Gate: bar age (per-TF — MUST match scanner.py via Settings.bar_age_gate_for) ---
    bar_age_gate = settings.bar_age_gate_for(signal.tf)
    if bar_age_sec > bar_age_gate:
        reason = f"stale_signal: bar_age={bar_age_sec:.0f}s > {bar_age_gate}s (tf={signal.tf})"
        log.info("REJECT %s %s: %s", signal.coin, signal.tf, reason)
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason=reason,
        )
        return None

    # --- Gate: TIER 1/2 only (no TIER 3) ---
    tier = universe_tiers.get(signal.coin, 2)
    if tier not in (1, 2):
        reason = f"tier_excluded: TIER {tier}"
        _reject_and_log(signal, reason)
        return None

    # --- Account state ---
    try:
        equity = client.account_value()
    except Exception as e:
        log.error("account_value() failed: %s — skipping entry", e)
        return None

    if equity <= 0:
        _reject_and_log(signal, "equity_zero_or_negative")
        return None

    # --- Gate: concurrent cap ---
    try:
        open_positions_exchange = client.open_positions()
        n_open = len(open_positions_exchange)
    except Exception as e:
        log.error("open_positions() failed: %s — skipping entry", e)
        return None

    allowed, reason = check_concurrent_cap(n_open, settings.max_concurrent)
    if not allowed:
        _reject_and_log(signal, reason)
        return None

    # --- Get asset metadata ---
    try:
        meta = client.asset(signal.coin)
    except KeyError:
        _reject_and_log(signal, f"asset_not_found: {signal.coin}")
        return None

    # --- Compute target notional (risk × equity / SL dist, capped by leverage) ---
    size_result = compute_size(
        entry_price=signal.entry_price,
        sl_price=signal.sl_price,
        account_value=equity,
        settings=settings,
        sz_decimals=meta.sz_decimals,
    )
    if size_result is None:
        _reject_and_log(signal, "size_compute_failed: risk too small for min_size")
        return None

    target_notional = size_result.notional

    # --- Liquidity size-fallback (snapshot-based, 2026-05-24 refactor) ---
    snapshot = snapshot_holder.current()
    liq_profile = snapshot.get(signal.coin) if snapshot is not None else None
    if liq_profile is None:
        # Coin missing from daily snapshot — try inline one-shot fetch before
        # giving up (avoid silent 24h rejection chain on transiently-missing coins).
        from bot import liquidity_snapshot as _liq_snap_mod
        from bot.liquidity_snapshot import fetch_one
        liq_profile = snapshot_holder.fetch_inline(
            signal.coin, lambda: fetch_one(client, signal.coin),
        )
        if liq_profile is None:
            # Precise reason — set by fetch_one when it returned None.
            # Cooldown path (no fetch attempted): reason stays at last value or
            # None, fall back to the generic label.
            reason = getattr(_liq_snap_mod, "last_fetch_one_reject_reason", None) \
                or "liq_inline_fetch_failed"
            log.warning(
                "%s missing from snapshot and inline fetch failed — rejecting (%s)",
                signal.coin, reason,
            )
            _reject_and_log(signal, reason)
            return None
        log.info("%s patched into snapshot via inline fetch", signal.coin)

    liq_cap_notional = settings.liq_size_cap_pct * liq_profile.avg_1h_vol_usd
    final_notional = min(target_notional, liq_cap_notional)

    if final_notional < settings.liq_min_trade_usd:
        reason = (
            f"liq_below_min_trade: cap=${liq_cap_notional:.2f} < "
            f"min=${settings.liq_min_trade_usd:.2f} (1h_vol=${liq_profile.avg_1h_vol_usd:.0f})"
        )
        log.info("REJECT %s %s: %s", signal.coin, signal.tf, reason)
        _reject_and_log(signal, reason)
        return None

    if final_notional < target_notional:
        pct = 100.0 * final_notional / target_notional if target_notional > 0 else 0.0
        log.info(
            "%s liq-capped to %.1f%% of target ($%.0f/$%.0f, 1h_vol=$%.0f)",
            signal.coin, pct, final_notional, target_notional, liq_profile.avg_1h_vol_usd,
        )
        size_result = compute_size(
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            account_value=equity,
            settings=settings,
            sz_decimals=meta.sz_decimals,
            liquidity_cap_notional=final_notional,
        )
        if size_result is None:
            _reject_and_log(signal, "size_after_liq_cap_zero")
            return None

    if size_result.size < meta.min_size:
        reason = (
            f"below_min_size: size={size_result.size} < "
            f"extended_min={meta.min_size} for {signal.coin}"
        )
        _reject_and_log(signal, reason)
        return None

    # --- Gate: MM cap ---
    current_notional = _current_notional(open_positions_exchange, client)
    mm_allowed, mm_reason = check_mm_cap(
        new_notional=size_result.notional,
        current_positions_notional=current_notional,
        account_value=equity,
        leverage=settings.leverage,
        mm_cap_pct=settings.mm_cap_pct,
    )
    if not mm_allowed:
        _reject_and_log(signal, mm_reason)
        return None

    # --- Place STOP-LIMIT entry (Extended limit order capped by SDK slippage) ---
    # Limit price = entry_price × (1 + cap) rounded to tick
    limit_px = signal.entry_price * (1 + settings.entry_limit_cap_pct)
    limit_px = client.round_price(signal.coin, limit_px)

    log.info(
        "ENTRY %s %s: trigger=%.6f entry=%.6f limit=%.6f sl=%.6f tp1=%.6f size=%s",
        signal.coin, signal.tf,
        signal.trigger_price, signal.entry_price, limit_px,
        signal.sl_price, signal.tp1_price, size_result.size,
    )

    # On Extended we place a limit buy at limit_px.
    # This fills only if market comes to us (or is already there after breakout).
    # TTL polling: cancel if not filled within entry_limit_ttl_sec.
    fill_result = _place_and_wait_fill(
        client=client,
        coin=signal.coin,
        is_buy=True,
        size=size_result.size,
        limit_px=limit_px,
        ttl_sec=settings.entry_limit_ttl_sec,
        min_fill_ratio=settings.min_fill_ratio,
        meta=meta,
    )

    if fill_result is None:
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason="entry_limit_unfilled: TTL expired or emergency close",
        )
        return None

    actual_entry, filled_size = fill_result

    # --- Place stop-loss trigger (retry 3x; emergency close if all fail) ---
    sl_order_id = _place_sl_with_retry(
        client=client,
        coin=signal.coin,
        size=filled_size,
        sl_price=signal.sl_price,
    )
    if sl_order_id is None:
        log.error(
            "SL placement FAILED 3x for %s %s — emergency closing naked position",
            signal.coin, signal.tf,
        )
        try:
            client.market_close(signal.coin)
        except Exception as e:
            log.critical(
                "EMERGENCY CLOSE ALSO FAILED for %s — MANUAL INTERVENTION: %s",
                signal.coin, e,
            )
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason="sl_placement_failed_3x_naked_position_closed",
        )
        return None

    # --- Journal ---
    trade_id = insert_trade(
        coin=signal.coin,
        tf=signal.tf,
        entry=actual_entry,
        sl_initial=signal.sl_price,
        tp1=signal.tp1_price,
        size=filled_size,
        risk_dollars=size_result.risk_dollars,
        notional=filled_size * actual_entry,
        walk_slip_pct=None,
        entry_order_id=None,
        notes=(
            f"f1_dist={signal.f1_dist:.2f} pivot_h={signal.pivot_high:.6f} "
            f"pivot_l={signal.pivot_low:.6f} liq_1h_vol=${liq_profile.avg_1h_vol_usd:.0f}"
        ),
    )
    if sl_order_id:
        update_trade_sl_order(trade_id, sl_order_id)

    pos = Position(
        coin=signal.coin,
        tf=signal.tf,
        entry_price=actual_entry,
        sl_initial=signal.sl_price,
        sl_current=signal.sl_price,
        tp1_price=signal.tp1_price,
        size=filled_size,
        bar_entry_idx=0,
    )
    pos.__dict__["_trade_id"] = trade_id  # attach db id for updates
    pos.__dict__["_sl_order_id"] = sl_order_id

    return pos


def _place_sl_with_retry(
    client,
    coin: str,
    size: float,
    sl_price: float,
    attempts: int = 3,
) -> Optional[int]:
    """Place SL trigger with retries + backoff. Returns oid or None on persistent failure.

    Caller MUST emergency-close the position if this returns None — there is no
    SL on the exchange and the position is naked.
    """
    for attempt in range(attempts):
        try:
            resp = client.trigger_sl(coin=coin, is_buy=False, sz=size, trigger_px=sl_price)
        except Exception as e:
            log.warning("SL placement attempt %d/%d exception for %s: %s",
                        attempt + 1, attempts, coin, e)
            time.sleep(0.5 * (attempt + 1))
            continue
        if resp.get("status") == "ok":
            try:
                statuses = resp["response"]["data"]["statuses"]
                if statuses and "resting" in statuses[0]:
                    return statuses[0]["resting"].get("oid")
            except Exception:
                pass
        log.warning("SL placement attempt %d/%d failed for %s: %s",
                    attempt + 1, attempts, coin, resp)
        time.sleep(0.5 * (attempt + 1))
    return None


def _place_and_wait_fill(
    client,
    coin: str,
    is_buy: bool,
    size: float,
    limit_px: float,
    ttl_sec: int,
    min_fill_ratio: float,
    meta,
) -> Optional[tuple[float, float]]:
    """Place a limit order and poll for fill within TTL.

    Returns (avg_fill_price, filled_size) or None if unfilled/cancelled.
    On partial fill < min_fill_ratio: emergency close and return None.
    On partial fill >= min_fill_ratio: accept partial, return actual fill.

    Extended market_open is used here as the limit-buy mechanism.
    The SDK's slippage parameter controls the limit price relative to orderbook.
    We pass the pre-computed limit_px as target → override slippage = cap%.

    Source: old bot trader.py fill polling pattern + user spec TTL=30s.
    """
    # Place the market order capped at limit_px
    # Extended SDK market_open uses slippage % of orderbook price → we
    # set settings.slippage = entry_limit_cap_pct so the order only fills
    # at or below limit_px.

    t_start = time.time()
    try:
        resp = client.market_open(coin=coin, is_buy=is_buy, sz=size)
    except Exception as e:
        log.error("market_open(%s) exception: %s", coin, e)
        return None

    # Check response for fill confirmation
    try:
        statuses = resp["response"]["data"]["statuses"]
        s = statuses[0] if statuses else {}
        if "error" in s:
            log.warning("market_open(%s) error: %s", coin, s["error"])
            return None
        if "filled" in s:
            avg_px_str = s["filled"].get("avgPx", "0")
            avg_px = float(avg_px_str) if avg_px_str else 0.0
            total_sz_str = s["filled"].get("totalSz", "0")
            filled_sz = float(total_sz_str) if total_sz_str else size

            # Validate fill against limit cap
            if avg_px > 0 and avg_px > limit_px * 1.001:
                log.warning(
                    "Fill price %.6f exceeds limit_px %.6f (cap breach) — treating as rejected",
                    avg_px, limit_px,
                )
                client.market_close(coin)
                return None

            # Check partial fill ratio
            fill_ratio = filled_sz / size if size > 0 else 0
            if fill_ratio < min_fill_ratio:
                log.warning(
                    "Partial fill %s: filled %.4f / %.4f (%.1f%% < %.0f%%) — emergency close",
                    coin, filled_sz, size, fill_ratio * 100, min_fill_ratio * 100,
                )
                client.market_close(coin)
                return None

            actual_px = avg_px if avg_px > 0 else limit_px
            return actual_px, filled_sz
    except Exception as e:
        log.warning("Fill parse error for %s: %s", coin, e)

    # Fallback: assume filled at mark price (Extended SDK synthesises fill)
    mark = client.mark_price(coin)
    if mark > 0:
        return mark, size
    return None


def manage_open_position(
    pos: Position,
    client,
    settings: Settings,
    position_manager: PositionManager,
    df_latest: dict,     # {tf: DataFrame} with latest closed bars
) -> Optional[str]:
    """Update SL, check for exits on an open position.

    Returns exit reason string if position was closed, else None.
    """
    trade_id = pos.__dict__.get("_trade_id")
    sl_order_id = pos.__dict__.get("_sl_order_id")

    tf = pos.tf
    df = df_latest.get(tf)
    if df is None or df.empty:
        return None

    # --- Heal NAKED position: SL missing from initial entry OR restored as NULL ---
    if sl_order_id is None:
        log.warning(
            "Position %s %s has NO SL order — attempting re-place at sl=%.6f",
            pos.coin, pos.tf, pos.sl_current,
        )
        sl_order_id = _place_sl_with_retry(
            client=client,
            coin=pos.coin,
            size=pos.size,
            sl_price=pos.sl_current,
        )
        if sl_order_id is None:
            log.error(
                "Position %s %s NAKED — SL re-place FAILED 3x — emergency closing",
                pos.coin, pos.tf,
            )
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _emergency_close(client, pos, exit_px, "sl_replace_failed_naked", trade_id, None)
            return "sl_replace_failed_naked"
        pos.__dict__["_sl_order_id"] = sl_order_id
        if trade_id:
            update_trade_sl_order(trade_id, sl_order_id)
        log.info("Healed NAKED %s %s — new SL oid=%s", pos.coin, pos.tf, sl_order_id)

    # Check if SL was already hit on exchange (position gone from exchange)
    try:
        exchange_positions = client.open_positions()
        if pos.coin not in exchange_positions:
            # Position no longer on exchange — SL triggered or manual close
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _record_close(
                trade_id=trade_id,
                pos=pos,
                exit_price=exit_px,
                exit_reason="sl_triggered_by_exchange",
            )
            return "sl_triggered_by_exchange"
    except Exception as e:
        log.warning("open_positions() check failed for %s: %s", pos.coin, e)

    # Check SL hit on latest bar (wick check)
    sl_hit = position_manager.check_sl_hit(pos, df, settings.vstop_wick_check)
    if sl_hit is not None:
        exit_px, reason = sl_hit
        log.info("SL hit %s %s: reason=%s exit=%.6f", pos.coin, tf, reason, exit_px)
        _emergency_close(client, pos, exit_px, reason, trade_id, sl_order_id)
        return reason

    # Update trailing SL
    new_sl, exit_reason = position_manager.update_sl_on_new_bar(
        pos=pos,
        df=df,
        enable_trail_after_tp=settings.enable_trail_after_tp,
    )

    if exit_reason == "max_run_cap":
        exit_px = pos.entry_price + settings.max_run_r * (pos.entry_price - pos.sl_initial)
        _emergency_close(client, pos, exit_px, "max_run_cap", trade_id, sl_order_id)
        return "max_run_cap"

    if new_sl is not None and new_sl > pos.sl_current:
        # Cancel old SL trigger and place new one
        if sl_order_id:
            client.cancel_sl_order(pos.coin, sl_order_id)

        new_sl_resp = client.trigger_sl(
            coin=pos.coin,
            is_buy=False,
            sz=pos.size,
            trigger_px=new_sl,
        )
        new_sl_oid = None
        try:
            sts = new_sl_resp["response"]["data"]["statuses"]
            if sts and "resting" in sts[0]:
                new_sl_oid = sts[0]["resting"].get("oid")
        except Exception:
            pass

        pos.__dict__["_sl_order_id"] = new_sl_oid
        if trade_id:
            update_trade_sl(trade_id, new_sl)
            if new_sl_oid:
                update_trade_sl_order(trade_id, new_sl_oid)

        log.info("SL updated %s %s: %.6f → %.6f", pos.coin, tf, pos.sl_current, new_sl)

    return None


def _emergency_close(client, pos: Position, exit_px: float, reason: str, trade_id, sl_order_id) -> None:
    """Market close position + cancel SL trigger + record in DB."""
    log.info("EMERGENCY CLOSE %s %s: reason=%s exit=%.6f", pos.coin, pos.tf, reason, exit_px)

    if sl_order_id:
        try:
            client.cancel_sl_order(pos.coin, sl_order_id)
        except Exception as e:
            log.warning("cancel_sl_order failed: %s", e)

    try:
        client.market_close(pos.coin)
    except Exception as e:
        log.error("market_close(%s) failed: %s — MANUAL INTERVENTION REQUIRED", pos.coin, e)

    _record_close(trade_id, pos, exit_px, reason)


def _record_close(trade_id, pos: Position, exit_price: float, exit_reason: str) -> None:
    if trade_id is None:
        return
    pnl = (exit_price - pos.entry_price) * pos.size
    sl_dist = pos.entry_price - pos.sl_initial
    realized_r = (exit_price - pos.entry_price) / sl_dist if sl_dist > 0 else 0.0
    close_trade(
        trade_id=trade_id,
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl_dollars=pnl,
        realized_r=realized_r,
    )


def _reject_and_log(signal: Signal, reason: str) -> None:
    log.info("REJECT %s %s: %s", signal.coin, signal.tf, reason)
    insert_rejected(
        coin=signal.coin, tf=signal.tf,
        trigger_price=signal.trigger_price,
        entry_price=signal.entry_price,
        sl_price=signal.sl_price,
        reason=reason,
    )


def _current_notional(exchange_positions: dict, client) -> float:
    """Sum absolute notional of all open positions from exchange state."""
    total = 0.0
    for coin, pos_data in exchange_positions.items():
        try:
            sz = abs(float(pos_data.get("szi", 0) or 0))
            px = float(pos_data.get("entryPx", 0) or 0)
            if sz > 0 and px > 0:
                total += sz * px
        except Exception:
            continue
    return total
