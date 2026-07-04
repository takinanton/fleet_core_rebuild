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
import math
import os
import time
from typing import Optional

from bot.config import Settings, FX_EXCLUDE, FOREIGN_SKIP_PREFIXES
from bot.config import settings as _global_settings
from bot.journal import (
    close_trade,
    insert_rejected,
    insert_trade,
    mark_tp1_partial,
    open_trades,
    update_trade_sl,
    update_trade_sl_order,
    update_trade_tp_order,
)
from bot.liquidity import LiquiditySnapshot, SnapshotHolder
from bot.risk import SizeResult, check_concurrent_cap, check_mm_cap, compute_size
from bot.strategy_xnn import Position, PositionManager, Signal

log = logging.getLogger(__name__)

# ── XNN patch 2026-06-11 (canon §0.8): DRY=0 orders enforced IN CODE ───────────────────
# main.py's dry_run flag only gates the entry branch. Every order-touching path below
# (entry, SL place/heal, trail cancel+place, TP1, market_close, _ensure_flat,
# emergency-close) is additionally gated here so a DRY bot can NEVER place/cancel/close
# anything regardless of caller bugs. With a fresh empty trades.db these paths are
# no-ops anyway — this guard makes the class impossible, not just unlikely.
DRY_RUN: bool = os.getenv("DRY_RUN", "").strip().lower() in ("1", "true", "yes", "on")


def _dry_block(action: str) -> bool:
    """True (and LOUD log) if order actions must be blocked because DRY_RUN=1.

    Appearance of '[DRY-RUN] BLOCKED' in journalctl during DRY soak = a position/order
    path was reached in DRY — investigate (it should be unreachable with an empty DB)."""
    if DRY_RUN:
        log.error("[DRY-RUN] BLOCKED order action: %s", action)
        return True
    return False

# Re-entry cooldown after a post-fill cap-breach / partial-fill abort. The pre-send
# price gate can't stop a market order from filling PAST the cap during the send
# (slippage), so a still-valid signal would re-fire on the very next scan and
# double-fill (root cause of the XRP double market fill 2026-05-30). Block the same
# coin from re-entry for a short window after such an abort. Skip-only — never naked.
_ENTRY_ABORT_COOLDOWN: dict = {}
ENTRY_ABORT_COOLDOWN_SEC = 900.0  # 15 min — covers the next-scan re-fire window

# Trail SL re-place threshold vs the price ACTUALLY resting on the exchange
# (_sl_placed_px). Canon parity (hl_xnn_prep trader.py:74) — avoids cancel/replace
# churn on sub-bps trail moves.
_SL_REPLACE_THRESH = 0.0005  # 5 bps


def _register_entry_abort(coin: str) -> None:
    try:
        _ENTRY_ABORT_COOLDOWN[coin] = time.time() + ENTRY_ABORT_COOLDOWN_SEC
    except Exception:
        pass


def _in_entry_cooldown(coin: str) -> bool:
    until = _ENTRY_ABORT_COOLDOWN.get(coin, 0.0)
    if until and time.time() < until:
        return True
    if until:
        _ENTRY_ABORT_COOLDOWN.pop(coin, None)
    return False


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
    if _dry_block(f"attempt_entry {signal.coin} {signal.tf} {signal.side}"):
        return None

    # --- Gate: bar age (per-TF — MUST match scanner.py via Settings.bar_age_gate_for) ---
    bar_age_gate = settings.bar_age_gate_for(signal.tf)
    if bar_age_sec > bar_age_gate:
        reason = f"stale_signal: bar_age={bar_age_sec:.0f}s > {bar_age_gate}s (tf={signal.tf})"
        log.info("REJECT %s %s %s: %s", signal.coin, signal.tf, signal.side, reason)
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason=reason,
            direction=signal.side,
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
        # Alias-dedup (2026-06-11): open_positions() returns each position under TWO
        # keys — 'BTC-USD' AND 'BTC' point at the SAME entry dict
        # (exchange_extended.py:412-417). len() of the raw dict double-counts.
        n_open = len({id(v) for v in open_positions_exchange.values()})
    except Exception as e:
        log.error("open_positions() failed: %s — skipping entry", e)
        return None

    # --- Gate: coin already held on the ACCOUNT (any owner) — code guard ---
    # Same-account merge guard (mirror of HL canon account_coins dedup, canon
    # main.py:615-755). Old extended-bot a/b run on the SAME vault until the flip:
    # if this coin is already held — by us OR by a foreign bot — entering would
    # MERGE into one cross-margin position, and manage/_ensure_flat/market_close
    # would later close the FOREIGN size too. FOREIGN_EXCLUDE_COINS (checklist §6.3)
    # is now the second line of defense, not the only one.
    if signal.coin in open_positions_exchange \
            or f"{signal.coin}-USD" in open_positions_exchange:
        _reject_and_log(signal, "account_position_exists")
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

    # --- XNN patch 2026-06-11 (canon §0.8): per-asset effective leverage ----------------
    # eff_lev = min(LEVERAGE, asset.max_leverage), actually SET on the exchange via
    # update_leverage() before EVERY entry (abort on fail — never assume), and used in
    # BOTH the sizing leverage-cap and the MM-cap margin math below. .env LEVERAGE=100
    # = always-above ceiling so the per-asset MAX binds (user rule: lev=MAX per asset,
    # control is RISK not leverage). Extended SDK 1.4.x = cross-only.
    eff_lev = max(1, min(int(settings.leverage), int(meta.max_leverage)))
    _lev_resp = client.update_leverage(signal.coin, eff_lev)
    if _lev_resp is None:
        _reject_and_log(signal, f"update_leverage_failed: {eff_lev}x not confirmed — abort entry")
        return None

    # --- Compute target notional (risk × equity / SL dist, capped by leverage) ---
    size_result = compute_size(
        entry_price=signal.entry_price,
        sl_price=signal.sl_price,
        account_value=equity,
        settings=settings,
        sz_decimals=meta.sz_decimals,
        leverage_eff=eff_lev,
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
            leverage_eff=eff_lev,
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
        leverage=eff_lev,   # XNN patch: margin math on the leverage ACTUALLY set
        mm_cap_pct=settings.mm_cap_pct,
    )
    if not mm_allowed:
        _reject_and_log(signal, mm_reason)
        return None

    # --- Place STOP-LIMIT entry (Extended limit order capped by SDK slippage) ---
    # Limit price: side-aware cap (see below).
    # Side-aware cap: long fills capped above (1+cap), short capped below (1-cap).
    _cap = settings.entry_limit_cap_pct
    if signal.side == "long":
        limit_px = signal.entry_price * (1 + _cap)
    else:
        limit_px = signal.entry_price * (1 - _cap)
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
    # SHORT support 2026-05-27 — side-aware entry
    is_long = (signal.side == "long")
    # --- Price gate (A) 2026-05-30: don't chase if mark already past cap-limit ---
    try:
        _mark_now = client.mark_price(signal.coin)
    except Exception:
        _mark_now = 0.0
    if _mark_now and _mark_now > 0 and (
        (is_long and _mark_now > limit_px)
        or ((not is_long) and _mark_now < limit_px)
    ):
        log.info(
            "PRICE-GATE skip %s %s: mark %.6f past cap-limit %.6f (%s) — not chasing",
            signal.coin, signal.tf, _mark_now, limit_px, signal.side,
        )
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason=f"price_gate_skip: mark {_mark_now:.6f} past cap-limit {limit_px:.6f} ({signal.side})",
            direction=signal.side,
        )
        return None
    fill_result = _place_and_wait_fill(
        client=client,
        coin=signal.coin,
        is_buy=is_long,                # long buys, short sells
        size=size_result.size,
        limit_px=limit_px,
        trigger_px=signal.entry_price,  # STOP-LIMIT: confirm continuation at trigger
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
            direction=signal.side,
        )
        return None

    actual_entry, filled_size = fill_result
    # Orphan fix (2026-05-31): invalidate positions cache after a confirmed open so
    # the next manage_open_position() tick refetches fresh instead of reading the
    # stale PRE-open cache (seeded by the concurrent-cap/anti-dup open_positions()
    # call) and false-recording closed_by_exchange. Mirrors HL orphan root fix.
    client.invalidate_positions_cache()
    # --- Slip persist (B) 2026-05-30: signed adverse entry slip vs intended (+=worse) ---
    _slip = ((actual_entry - signal.entry_price) / signal.entry_price) if is_long \
        else ((signal.entry_price - actual_entry) / signal.entry_price)

    # --- SL-inside-liquidation guard (2026-06-11, feedback_sl_must_be_inside_liquidation) ---
    # Force-cross happened pre-order (update_leverage is_cross=True at :187; Extended is
    # cross-only anyway). If the live per-position liqPx still sits inside the structural
    # SL, ensure_sl_inside_liq clamps the SL strictly inside liq+buffer (REMEDY-B; Extended
    # has no add-margin so REMEDY-A is a no-op). The value that actually goes to the
    # exchange = sl_to_place.
    sl_to_place, _liq_action = ensure_sl_inside_liq(
        client=client, coin=signal.coin, side=signal.side,
        sl_px=signal.sl_price, size=filled_size,
    )
    if _liq_action not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
        log.warning("SL liq-guard %s %s: action=%s sl %.6f→%.6f",
                    signal.coin, signal.tf, _liq_action, signal.sl_price, sl_to_place)

    # --- Place stop-loss trigger (retry 3x; emergency close if all fail) ---
    # SHORT: SL ABOVE entry → trigger BUYS to close (is_buy=True via side=short).
    sl_order_id = _place_sl_with_retry(
        client=client,
        coin=signal.coin,
        size=filled_size,
        sl_price=sl_to_place,
        side=signal.side,
    )
    # Post-invariant (rule step 3): SL must rest inside liq+buffer or the position is
    # unsafe → close it. REMEDY-B guarantees inside-ness; this catches a still-isolated
    # tight liq where clamp somehow didn't land — never hold an SL past liquidation.
    if sl_order_id is not None and not _sl_inside_liq_ok(client, signal.coin, signal.side, sl_to_place):
        log.critical(
            "INVARIANT FAIL %s %s: SL %.6f still OUTSIDE liquidation after guard — "
            "emergency closing rather than hold an unprotected position",
            signal.coin, signal.tf, sl_to_place,
        )
        try:
            client.cancel_sl_order(signal.coin, sl_order_id)
        except Exception as e:
            log.warning("cancel SL after invariant-fail %s: %s", signal.coin, e)
        try:
            client.market_close(signal.coin)
        except Exception as e:
            log.critical("EMERGENCY CLOSE (invariant) FAILED for %s — MANUAL: %s", signal.coin, e)
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason=f"sl_outside_liquidation_invariant_fail (action={_liq_action})",
            direction=signal.side,
        )
        return None
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
            direction=signal.side,
        )
        return None

    # --- Journal ---
    trade_id = insert_trade(
        coin=signal.coin,
        tf=signal.tf,
        entry=actual_entry,
        entry_intended=signal.entry_price,
        sl_initial=signal.sl_price,
        tp1=signal.tp1_price,
        size=filled_size,
        risk_dollars=size_result.risk_dollars,
        notional=filled_size * actual_entry,
        walk_slip_pct=_slip,
        entry_order_id=None,
        notes=(
            f"f1_dist={signal.f1_dist:.2f} pivot_h={signal.pivot_high:.6f} "
            f"pivot_l={signal.pivot_low:.6f} liq_1h_vol=${liq_profile.avg_1h_vol_usd:.0f}"
        ),
        direction=signal.side,
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
        side=signal.side,
    )
    pos.__dict__["_trade_id"] = trade_id  # attach db id for updates
    pos.__dict__["_sl_order_id"] = sl_order_id
    pos.__dict__["_orig_size"] = filled_size
    pos.__dict__["_sl_placed_px"] = signal.sl_price  # SL price currently on exchange (canon :432)
    pos.__dict__["_tp1_frac"] = settings.tp1_partial_frac

    # --- Place tp1_partial_frac reduce-only LIMIT @ TP1 (1.618R), maker — cheaper than a
    # market TP. BEST-EFFORT on top of the already-resting SL: a failure = trail-only, never naked.
    tp1_order_id = _place_tp1_partial(client, signal, filled_size, settings, meta)
    pos.__dict__["_tp1_order_id"] = tp1_order_id
    if tp1_order_id is not None and trade_id:
        update_trade_tp_order(trade_id, str(tp1_order_id))

    return pos


def ensure_sl_inside_liq(client, coin: str, side: str, sl_px: float, size: float):
    """Guarantee the SL rests strictly INSIDE liquidation by LIQ_SL_BUFFER_PCT.

    Rule (2026-06-11, feedback_sl_must_be_inside_liquidation): an SL outside liquidation
    is worthless — the position is force-liquidated before the stop ever fires, and on a
    thin isolated market the liqPx sits close to entry. This enforces, for OUR entry/trail
    SL only (never foreign / manual positions — callers already gate on that):

      long  : SL_px must be >  liqPx × (1 + buf)
      short : SL_px must be <  liqPx × (1 - buf)

    Mechanism:
      (1) Entry already force-crosses (trader.py update_leverage is_cross=True). Extended
          (x10 SDK 1.4.x) is CROSS-ONLY by design, so a cross position has account-level
          liquidation → position_liquidation() reports margin_mode="cross" and liq_px None
          → treated SAFE here (account-level safety, nothing to clamp to a number).
      (2) If the venue ever reports ISOLATED (thin coin) OR liqPx is still inside the SL:
          REMEDY-A (preferred): add isolated margin so liqPx moves past SL+buffer, re-read
            liqPx, repeat ≤3 iters. Requires client.add_isolated_margin — Extended has NO
            such API (SDK cross-only) so this branch is skipped and we fall to REMEDY-B.
          REMEDY-B (fallback): clamp SL strictly inside liq+buffer + LOUD log.WARNING.
      (3) Post-invariant is asserted by the CALLER (assert-or-emergency-close). This fn
          returns the SL that should actually go to the exchange + an action tag.

    Idempotent: if already safe → no margin added, SL returned unchanged.
    DRY_RUN: no margin / no live read mutation — logs intent and returns SL unchanged.

    Returns (final_sl_px, action) where action ∈ {"dry_skip", "no_position",
      "cross_account_safe", "already_safe", "add_margin", "clamp", "add_margin+clamp"}.
    """
    buf = getattr(_global_settings, "liq_sl_buffer_pct", 0.02)
    is_long = (side == "long")

    def _inside(sl: float, liq: float) -> bool:
        if liq is None or liq <= 0:
            return True  # cross / no per-position liq → account-level safety
        return (sl > liq * (1.0 + buf)) if is_long else (sl < liq * (1.0 - buf))

    def _target_sl_edge(liq: float) -> float:
        # SL clamped exactly to the inside edge of liq+buffer.
        return liq * (1.0 + buf) if is_long else liq * (1.0 - buf)

    # DRY: never touch the exchange. Report what WOULD happen, return SL unchanged.
    if _dry_block(f"ensure_sl_inside_liq({coin} {side} sl={sl_px})"):
        return sl_px, "dry_skip"

    # --- Read live liquidation for the position ---
    liq_info = None
    if hasattr(client, "position_liquidation"):
        try:
            liq_info = client.position_liquidation(coin)
        except Exception as e:
            log.warning("ensure_sl_inside_liq: position_liquidation(%s) threw: %s", coin, e)
            liq_info = None
    if liq_info is None:
        # No open position yet (entry races the fill→state cache) or no liq reading.
        # Cannot prove unsafe → leave SL as-is; the SL-readback invariant still guards naked.
        return sl_px, "no_position"

    liq_px = liq_info.get("liq_px")
    margin_mode = str(liq_info.get("margin_mode", "cross")).lower()

    # Cross position → account-level liquidation, liq_px is None. Force-cross at entry is
    # exactly remedy (1): risk-sized cross positions liquidate far from any structural SL.
    if liq_px is None or liq_px <= 0:
        return sl_px, "cross_account_safe"

    # Already strictly inside → idempotent no-op (do NOT top up margin again).
    if _inside(sl_px, liq_px):
        return sl_px, "already_safe"

    action_parts = []

    # --- REMEDY-A: add isolated margin until liqPx clears SL by buffer (≤3 iters) ---
    # Only meaningful on an ISOLATED position with a real per-position liqPx AND an
    # adapter that can top up margin. Extended is cross-only with NO add_isolated_margin
    # → this whole block is skipped and we go straight to REMEDY-B clamp.
    if margin_mode == "isolated" and hasattr(client, "add_isolated_margin"):
        for it in range(3):
            # Notional protected; estimate USD to shift liqPx to the SL edge plus a margin.
            # liqPx ≈ entry - (margin/size) for long; moving liqPx by Δpx costs ≈ size×Δpx.
            need_px = _target_sl_edge(liq_px)
            delta_px = abs(need_px - liq_px)
            # 1.25× headroom so one shot usually clears the buffer; min $1 to avoid no-op.
            add_usd = max(1.0, size * delta_px * 1.25)
            log.warning(
                "ensure_sl_inside_liq REMEDY-A %s %s iter=%d: liqPx=%.6f vs SL=%.6f "
                "(buf=%.4f) → add isolated margin $%.2f",
                coin, side, it + 1, liq_px, sl_px, buf, add_usd,
            )
            resp = client.add_isolated_margin(coin, add_usd)
            if resp is None:
                log.warning("ensure_sl_inside_liq REMEDY-A %s: add_isolated_margin failed "
                            "→ falling back to REMEDY-B clamp", coin)
                break
            action_parts.append("add_margin")
            # Re-read liqPx after the top-up.
            try:
                liq_info = client.position_liquidation(coin)
            except Exception:
                liq_info = None
            if not liq_info:
                break
            new_liq = liq_info.get("liq_px")
            if new_liq is None or new_liq <= 0:
                # Flipped to cross or liq vanished → account-level safe.
                return sl_px, "+".join(action_parts) or "cross_account_safe"
            liq_px = new_liq
            if _inside(sl_px, liq_px):
                log.info("ensure_sl_inside_liq REMEDY-A %s: SL %.6f now inside liqPx %.6f "
                         "(buf=%.4f) after %d top-up(s)", coin, sl_px, liq_px, buf, it + 1)
                return sl_px, "+".join(action_parts)

    # --- REMEDY-B: clamp SL strictly inside liq + buffer (fallback / always-correct) ---
    edge = _target_sl_edge(liq_px)
    new_sl = max(sl_px, edge) if is_long else min(sl_px, edge)
    try:
        new_sl = client.round_price(coin, new_sl)
    except Exception:
        pass
    # Rounding could nudge back outside the buffer by a tick; nudge one tick further in.
    if not _inside(new_sl, liq_px):
        bump = liq_px * buf * 0.5
        new_sl = (new_sl + bump) if is_long else (new_sl - bump)
        try:
            new_sl = client.round_price(coin, new_sl)
        except Exception:
            pass
    action_parts.append("clamp")
    log.warning(
        "ensure_sl_inside_liq REMEDY-B CLAMP %s %s: liqPx=%.6f buf=%.4f "
        "old_sl=%.6f → new_sl=%.6f (SL was OUTSIDE liquidation)",
        coin, side, liq_px, buf, sl_px, new_sl,
    )
    return new_sl, "+".join(action_parts)


def _sl_inside_liq_ok(client, coin: str, side: str, sl_px: float) -> bool:
    """Invariant check: is `sl_px` strictly inside the live liqPx + buffer?

    True when SAFE. Cross / no-position / no-liq-reading → True (account-level safety
    or nothing to clamp). DRY_RUN → True (no live mutation, guard is informational).
    Used post-placement to assert-or-emergency-close (rule step (3)).
    """
    if DRY_RUN:
        return True
    buf = getattr(_global_settings, "liq_sl_buffer_pct", 0.02)
    if not hasattr(client, "position_liquidation"):
        return True
    try:
        info = client.position_liquidation(coin)
    except Exception:
        return True  # can't prove unsafe on a transient read failure
    if not info:
        return True
    liq = info.get("liq_px")
    if liq is None or liq <= 0:
        return True  # cross / account-level liquidation
    return (sl_px > liq * (1.0 + buf)) if side == "long" else (sl_px < liq * (1.0 - buf))


def _place_sl_with_retry(
    client,
    coin: str,
    size: float,
    sl_price: float,
    attempts: int = 3,
    side: str = "long",
) -> Optional[int]:
    """Place SL trigger with retries + backoff. Returns oid or None on persistent failure.

    side: "long"  → SL below entry, trigger SELLS to close (is_buy=False).
          "short" → SL above entry, trigger BUYS to close (is_buy=True).
    Caller MUST emergency-close the position if this returns None.
    """
    if _dry_block(f"place_sl {coin} sz={size} @ {sl_price}"):
        return None
    is_buy_to_close = (side == "short")
    for attempt in range(attempts):
        try:
            resp = client.trigger_sl(coin=coin, is_buy=is_buy_to_close, sz=size, trigger_px=sl_price)
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


def _parse_resting_oid(resp) -> Optional[int]:
    """Extract a resting order id from an HL-style order response, or None."""
    try:
        sts = resp["response"]["data"]["statuses"]
        if sts and "resting" in sts[0]:
            return sts[0]["resting"].get("oid")
    except Exception:
        pass
    return None


def _round_down_size(sz: float, sz_decimals: int) -> float:
    q = 10 ** int(sz_decimals)
    return math.floor(sz * q) / q


def _place_tp1_partial(client, signal: Signal, filled_size: float,
                       settings: Settings, meta) -> Optional[int]:
    """Resting reduce-only maker LIMIT for tp1_partial_frac of size at TP1 (1.618R).

    Returns the resting oid, or None (frac<=0, size below min, rejected, or exception).
    Never raises — the partial TP is best-effort layered on the already-resting SL.
    """
    frac = getattr(settings, "tp1_partial_frac", 0.5)
    if frac <= 0:
        return None
    if _dry_block(f"tp1_partial {signal.coin}"):
        return None
    try:
        tp_sz = _round_down_size(filled_size * frac, meta.sz_decimals)
        if tp_sz < meta.min_size:
            log.info("TP1 partial %s: size %.8f < min_size %s — trail-only",
                     signal.coin, tp_sz, meta.min_size)
            return None
        tp_px = client.round_price(signal.coin, signal.tp1_price)
        resp = client.limit_reduce_only(
            coin=signal.coin,
            is_buy=(signal.side == "short"),   # close side: long→SELL, short→BUY
            sz=tp_sz, px=tp_px,
        )
        oid = _parse_resting_oid(resp)
        if oid is None:
            log.warning("TP1 partial %s not resting (%s) — trail-only",
                        signal.coin, str(resp)[:160])
        else:
            log.info("TP1 partial %s: %.8f @ %.6f oid=%s", signal.coin, tp_sz, tp_px, oid)
        return oid
    except Exception as e:
        log.warning("TP1 partial placement failed %s: %s — trail-only", signal.coin, e)
        return None


def _cancel_tp_if_any(client, pos: Position) -> None:
    """Cancel a still-resting partial-TP limit (best-effort)."""
    tp_oid = pos.__dict__.get("_tp1_order_id")
    if not tp_oid:
        return
    if _dry_block(f"cancel_tp {pos.coin} oid={tp_oid}"):
        return
    try:
        client.cancel_sl_order(pos.coin, tp_oid)
    except Exception as e:
        log.warning("cancel TP1 limit failed %s oid=%s: %s", pos.coin, tp_oid, e)
    pos.__dict__["_tp1_order_id"] = None


def _handle_tp1_partial(client, pos: Position, remaining_size: float,
                        settings: Settings, trade_id) -> bool:
    """The partial TP limit filled on exchange: book it, resize to remainder, SL→BE, re-place SL.

    Returns True if the remainder was emergency-closed (caller should stop managing this pos).
    """
    is_long = (getattr(pos, "side", "long") == "long")
    be_buffer = settings.trail_after_tp_buffer_pct
    if is_long:
        new_sl = max(pos.sl_current, pos.entry_price * (1.0 - be_buffer))
    else:
        new_sl = min(pos.sl_current, pos.entry_price * (1.0 + be_buffer))

    log.info("TP1 PARTIAL filled %s %s (%s): size %.8f→%.8f, SL %.6f→BE %.6f",
             pos.coin, pos.tf, pos.side, pos.size, remaining_size, pos.sl_current, new_sl)

    pos.tp1_partial_done = True
    pos.tp1_hit = True
    pos.size = remaining_size
    pos.sl_current = new_sl
    pos.__dict__["_tp1_order_id"] = None  # consumed by the fill

    # Re-place SL for the remainder at BE: cancel old, place new (retry; emergency if it fails).
    old_sl_oid = pos.__dict__.get("_sl_order_id")
    if old_sl_oid:
        try:
            client.cancel_sl_order(pos.coin, old_sl_oid)
        except Exception as e:
            log.warning("cancel old SL after partial failed %s: %s", pos.coin, e)
    new_oid = _place_sl_with_retry(
        client=client, coin=pos.coin, size=remaining_size,
        sl_price=new_sl, side=getattr(pos, "side", "long"),
    )
    pos.__dict__["_sl_order_id"] = new_oid
    if new_oid is not None:
        pos.__dict__["_sl_placed_px"] = new_sl  # canon parity: track px resting on exchange
    if trade_id:
        mark_tp1_partial(trade_id, pos.tp1_price, remaining_size, new_sl)
        if new_oid:
            update_trade_sl_order(trade_id, new_oid)
    if new_oid is None:
        log.error("Post-partial SL re-place FAILED %s %s — emergency closing remainder",
                  pos.coin, pos.tf)
        exit_px = client.mark_price(pos.coin) or new_sl
        _emergency_close(client, pos, exit_px, "post_partial_sl_failed", trade_id, None)
        return True
    return False


def _sl_confirmed_live(client, coin: str, sl_order_id) -> bool:
    """True iff a live SL (TPSL) order with `sl_order_id` is CONFIRMED on the exchange.

    Class guard for "never naked >1 tick" (mirror of Nado/Pacifica read-back fix 2026-06-07;
    see mem:feedback_order_placement_readback_and_invariants). Robust in BOTH directions:
      - stored oid is None/falsy     -> no SL regardless of API -> NOT live (always heal).
      - stored oid present, list ok  -> live iff oid is among the coin's open TPSL orders
        (catches a STALE oid: cancelled / expired / triggered, or a trail place-before-cancel
        that stored a dead oid). list_open_sl_orders queries OrderType.TPSL only.
      - stored oid present, list threw/empty-on-error -> list_open_sl_orders already returns []
        on SDK error (logs a warning); to avoid a duplicate-SL storm / spurious emergency-close
        on a transient blip we re-confirm via a guarded second read and assume live if the read
        itself errored. (A genuinely-dead SL with a flaky API is still caught on a clean tick.)
    """
    if not sl_order_id:
        return False
    try:
        live = client.list_open_sl_orders(coin)
    except Exception as e:
        log.warning("SL-liveness list threw for %s: %s — assuming live (stored oid=%s)",
                    coin, e, sl_order_id)
        return True
    try:
        return any(str(o) == str(sl_order_id) for o in (live or []))
    except Exception:
        return False


def _confirm_sl_live_poll(client, coin: str, sl_order_id, attempts: int = 4) -> bool:
    """Confirm a freshly-placed SL is live, tolerating exchange eventual-consistency.

    A just-created TPSL can lag in get_open_orders for a second or two (fresh-open lag class).
    Poll a few times before declaring failure so we never emergency-close a position that is in
    fact protected. Bounded — total wait ~ a few seconds, well inside one tick.
    """
    for i in range(attempts):
        if _sl_confirmed_live(client, coin, sl_order_id):
            return True
        time.sleep(0.75 * (i + 1))
    return False


def _ensure_flat(client, coin: str, is_buy_open: bool, known_filled_sz: float = 0.0) -> bool:
    """After an aborted entry (cap breach / partial fill), GUARANTEE we end flat.

    market_close() can miss a position (stale cache / transient API error) and
    silently leave a naked, untracked orphan. Retry the close; if a residual
    remains, place a protective reduce-only SL so the position is never left
    naked. Root fix - audit 2026-05-30 (XRP orphan incident).
    """
    if _dry_block(f"ensure_flat {coin}"):
        return True  # nothing was ever opened in DRY — report flat, place nothing
    def _pos():
        client.invalidate_positions_cache()
        ps = client.open_positions()
        return ps.get(coin) or ps.get(client._market(coin).name)
    if known_filled_sz and known_filled_sz > 0:
        # Caller CONFIRMED a fill; /positions lags the fill by seconds — poll until
        # it materialises so we never declare "flat" on a not-yet-propagated fill
        # (orphan race 2026-05-30).
        for _ in range(6):
            try:
                _p = _pos()
            except Exception:
                _p = None
            if _p:
                break
            time.sleep(1.0)
    for _ in range(3):
        try:
            pos = _pos()
        except Exception:
            pos = "?"  # unknown -> force a close attempt
        if pos is None:
            return True  # confirmed flat
        client.market_close(coin)
        time.sleep(1.0)
    try:
        pos = _pos()
    except Exception:
        pos = None
    if not pos:
        return True
    try:
        sz = abs(float(pos.get("szi", 0)))
        mark = client.mark_price(coin)
        if sz > 0 and mark > 0:
            trig = mark * 0.975 if is_buy_open else mark * 1.025
            client.trigger_sl(coin, is_buy=(not is_buy_open), sz=sz, trigger_px=trig)
            log.error("ABORTED ENTRY %s left residual %.6f after 3x market_close - placed protective SL @ ~%.4f", coin, sz, trig)
    except Exception as e:
        log.error("ABORTED ENTRY %s: FAILED to place protective SL on residual: %s", coin, e)
    return False


def _place_and_wait_fill(
    client,
    coin: str,
    is_buy: bool,
    size: float,
    limit_px: float,
    ttl_sec: int,
    min_fill_ratio: float,
    meta,
    trigger_px: float = 0.0,
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

    if _in_entry_cooldown(coin):
        log.info("Entry cooldown active for %s — skipping (recent post-fill abort)", coin)
        return None

    # ── STOP-LIMIT continuation gate (2026-05-31, user spec) ─────────────
    # The send below is a marketable market_open. If price has retraced PAST the
    # breakout trigger since the signal bar closed, sending now buys/sells into a
    # possible reversal. Behave like a buy/sell-STOP-LIMIT at trigger_px (capped at
    # limit_px): poll up to ttl_sec, send only once mark is in the fillable band
    # [trigger, limit] (long) / [limit, trigger] (short) = continuation confirmed
    # AND within cap. Past cap → skip (no chase); short of trigger → wait; ttl
    # expiry → skip. Common case (mark already at/through trigger) breaks on the
    # first poll = zero added latency. Open positions unaffected (SL rests on
    # exchange). trigger_px<=0 → legacy immediate send.
    if trigger_px and trigger_px > 0:
        _sc_deadline = time.time() + max(1, int(ttl_sec))
        while True:
            try:
                _mk = client.mark_price(coin)
            except Exception:
                _mk = 0.0
            if _mk and _mk > 0:
                if is_buy:
                    if _mk > limit_px:
                        log.info("STOP-entry skip %s: mark %.6f past cap-limit %.6f (long) - not chasing", coin, _mk, limit_px)
                        return None
                    if _mk >= trigger_px:
                        break
                else:
                    if _mk < limit_px:
                        log.info("STOP-entry skip %s: mark %.6f past cap-limit %.6f (short) - not chasing", coin, _mk, limit_px)
                        return None
                    if _mk <= trigger_px:
                        break
            if time.time() >= _sc_deadline:
                log.info("STOP-entry skip %s: price never reclaimed trigger %.6f within %ss (mark=%.6f, %s)", coin, trigger_px, ttl_sec, _mk, "buy" if is_buy else "sell")
                return None
            time.sleep(1.5)

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
            _breach = (avg_px > limit_px * 1.001) if is_buy else (avg_px < limit_px * 0.999)
            if avg_px > 0 and _breach:
                log.warning(
                    "Fill price %.6f exceeds limit_px %.6f (cap breach) — treating as rejected",
                    avg_px, limit_px,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            # Check partial fill ratio
            fill_ratio = filled_sz / size if size > 0 else 0
            if fill_ratio < min_fill_ratio:
                log.warning(
                    "Partial fill %s: filled %.4f / %.4f (%.1f%% < %.0f%%) — emergency close",
                    coin, filled_sz, size, fill_ratio * 100, min_fill_ratio * 100,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            actual_px = avg_px if avg_px > 0 else limit_px
            return actual_px, filled_sz
    except Exception as e:
        log.warning("Fill parse error for %s: %s", coin, e)

    # Phantom-fill fix (2026-05-31): an UNPARSEABLE response does NOT prove a fill.
    # The old code assumed filled-at-mark here, which could journal a non-existent
    # position + place an SL on nothing (phantom). Instead CONFIRM against the
    # exchange: poll open_positions briefly (the /positions read lags a real fill by
    # seconds, cf. _ensure_flat) and only treat as filled if a position materialises;
    # return the CONFIRMED size. Else return None = unfilled (no phantom).
    _confirmed_sz = 0.0
    for _ in range(4):
        try:
            client.invalidate_positions_cache()
            _p = client.open_positions().get(coin)
        except Exception as _e2:
            log.warning("open_positions() confirm failed for %s: %s", coin, _e2)
            _p = None
        if _p:
            _confirmed_sz = abs(float(_p.get("szi", 0) or 0))
            if _confirmed_sz > 0:
                break
        time.sleep(1.0)
    if _confirmed_sz > 0:
        mark = client.mark_price(coin)
        if mark > 0:
            log.info(
                "Fill CONFIRMED via open_positions for %s: szi=%.6f @~%.6f (order response was unparseable)",
                coin, _confirmed_sz, mark,
            )
            return mark, _confirmed_sz
    log.warning(
        "No confirmed position for %s after unparseable response — treating as UNFILLED (phantom-fill avoided)",
        coin,
    )
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
    # XNN patch 2026-06-11 (canon §0.8): in DRY there must be no managed positions at all
    # (adopt is off, entries blocked). If one somehow exists, refuse to touch it — the
    # SL-heal / trail / emergency paths below all place or cancel orders.
    if _dry_block(f"manage_open_position {pos.coin} {pos.tf}"):
        return None
    # XNN patch 2026-06-11 (canon §0.9): foreign-prefix positions are NEVER managed.
    if FOREIGN_SKIP_PREFIXES and any(pos.coin.startswith(p) for p in FOREIGN_SKIP_PREFIXES):
        log.error("manage: %s matches FOREIGN_SKIP_PREFIXES — refusing to manage foreign position", pos.coin)
        return None
    # FX_EXCLUDE EXEMPT (2026-06-07): manual/excluded coins (e.g. BNB, BTC — UK-SIG paused)
    # are managed by hand. The per-tick naked-heal / SL-liveness guard / emergency-close logic
    # below must NEVER run for them — it would re-add a removed SL (BNB no-SL design) or even
    # emergency-close a manual position. Hard early-return is the safety net on top of the
    # adopt-side exempt in main.py (covers any position that is somehow already tracked).
    _bare = pos.coin[:-4] if pos.coin.endswith("-USD") else pos.coin
    if _bare in FX_EXCLUDE or pos.coin in FX_EXCLUDE:
        return None

    trade_id = pos.__dict__.get("_trade_id")
    sl_order_id = pos.__dict__.get("_sl_order_id")

    # UK-SIG pending-entry guard (2026-06-07): resting entry not yet filled.
    if pos.__dict__.get("_pending_entry"):
        log.debug("manage: %s is pending entry — skip", pos.coin)
        return None

    tf = pos.tf
    df = df_latest.get(tf)
    if df is None or df.empty:
        return None

    # --- CLASS GUARD: a position must NEVER be without a CONFIRMED-LIVE SL for >1 tick. ---
    # Not enough to check `sl_order_id is None`: the stored oid can be stale (SL cancelled /
    # expired / triggered on the exchange, or a trail place-before-cancel that stored a dead
    # oid — Extended TPSL had a 1h-TTL expiry class, cf. AAVE 2026-05-11). Confirm against the
    # exchange EVERY tick via read-back; if no live SL is protecting this position, immediately
    # (re-)place and READ BACK to confirm it is live. If still unconfirmed → emergency-close.
    # (class fix 2026-06-07; mem:feedback_order_placement_readback_and_invariants)
    if not _sl_confirmed_live(client, pos.coin, sl_order_id):
        # Size the heal-SL to the LIVE filled position when readable (handles a partially-filled
        # resting entry, avoids an oversized reduce-only stop). Fall back to tracked size — a
        # reduce-only stop is clamped to position size on the exchange, so it can't over-close.
        heal_size = pos.size
        try:
            _ex = client.open_positions().get(pos.coin) or client.open_positions().get(f"{pos.coin}-USD")
            if _ex is not None:
                _live_sz = abs(float(_ex.get("szi", 0) or 0))
                if _live_sz > 0:
                    heal_size = _live_sz
        except Exception:
            pass
        # Liq-guard the heal SL too (2026-06-11) — a re-placed SL must also rest inside liquidation.
        _heal_side = getattr(pos, "side", "long")
        heal_sl, _heal_action = ensure_sl_inside_liq(
            client=client, coin=pos.coin, side=_heal_side,
            sl_px=pos.sl_current, size=heal_size,
        )
        log.warning(
            "Position %s %s (%s) has NO confirmed-live SL (stored oid=%s) — re-placing sz=%s at sl=%.6f (liq-guard=%s)",
            pos.coin, pos.tf, _heal_side, sl_order_id, heal_size, heal_sl, _heal_action,
        )
        sl_order_id = _place_sl_with_retry(
            client=client,
            coin=pos.coin,
            size=heal_size,
            sl_price=heal_sl,
            side=_heal_side,
        )
        # READ BACK: don't trust the place response — confirm a live SL exists (poll briefly to
        # tolerate fresh-order propagation lag before any emergency close).
        if sl_order_id is None or not _confirm_sl_live_poll(client, pos.coin, sl_order_id):
            log.error(
                "Position %s %s NAKED — SL re-place/confirm FAILED — emergency closing",
                pos.coin, pos.tf,
            )
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _emergency_close(client, pos, exit_px, "sl_replace_failed_naked", trade_id, sl_order_id)
            return "sl_replace_failed_naked"
        pos.__dict__["_sl_order_id"] = sl_order_id
        pos.__dict__["_sl_placed_px"] = heal_sl  # canon — heal re-anchors placed px to liq-guarded SL
        pos.sl_current = heal_sl  # keep tracked SL == on-exchange SL (may be liq-clamped)
        if trade_id:
            update_trade_sl_order(trade_id, sl_order_id)
        log.info("Healed NAKED %s %s — SL oid=%s confirmed live on exchange", pos.coin, pos.tf, sl_order_id)

    # Check exchange state: full close (gone) OR partial TP fill (size shrank, still open).
    try:
        exchange_positions = client.open_positions()
        ex = exchange_positions.get(pos.coin) or exchange_positions.get(f"{pos.coin}-USD")
        if ex is None:
            # PHANTOM-CLOSE GUARD (class fix): open_positions() (and Pacifica /orders) can blip
            # empty right after a fresh open (exchange eventual-consistency) -> false
            # 'sl_triggered_by_exchange' -> orphaned naked position. Require the position to
            # stay gone >=90s AND to have actually disappeared (not never-appeared) before
            # trusting empty open_positions() as a real close.
            _gone_now = time.time()
            _first_gone = pos.__dict__.get("_first_gone_ts")
            if _first_gone is None:
                pos.__dict__["_first_gone_ts"] = _gone_now
                log.info("phantom-guard: %s gone from open_positions -- defer to confirm", pos.coin)
                return None
            if _gone_now - _first_gone < 90.0:
                return None
            # Position no longer on exchange — SL triggered, TP-then-SL, or manual close.
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _cancel_tp_if_any(client, pos)
            _record_close(
                trade_id=trade_id,
                pos=pos,
                exit_price=exit_px,
                exit_reason="sl_triggered_by_exchange",
            )
            return "sl_triggered_by_exchange"
        pos.__dict__.pop("_first_gone_ts", None)  # seen live again -> reset phantom-guard
        # Partial TP1 fill: exchange size dropped meaningfully while position is still open.
        if not getattr(pos, "tp1_partial_done", False):
            cur_sz = abs(float(ex.get("szi", 0) or 0))
            orig_sz = pos.__dict__.get("_orig_size", pos.size)
            if cur_sz > 0 and orig_sz > 0 and cur_sz < orig_sz * 0.9:
                if _handle_tp1_partial(client, pos, cur_sz, settings, trade_id):
                    return "post_partial_sl_failed"
    except Exception as e:
        log.warning("open_positions()/partial check failed for %s: %s", pos.coin, e)

    # SL order id may have changed if a TP1 partial just filled (SL re-placed on remainder).
    sl_order_id = pos.__dict__.get("_sl_order_id")

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
        sl_dist_abs = abs(pos.entry_price - pos.sl_initial)
        if getattr(pos, "side", "long") == "long":
            exit_px = pos.entry_price + settings.max_run_r * sl_dist_abs
        else:
            exit_px = pos.entry_price - settings.max_run_r * sl_dist_abs
        _emergency_close(client, pos, exit_px, "max_run_cap", trade_id, sl_order_id)
        return "max_run_cap"

    # Re-place the exchange SL whenever the structural trail (or partial-BE) moved
    # pos.sl_current. CANON BLOCK (hl_xnn_prep trader.py:957-988), replaces the legacy
    # ext `sl_improved` check which was DEAD CODE: the PM ratchets pos.sl_current
    # BEFORE returning (strategy_xnn.py:367-374), so `new_sl > pos.sl_current` was
    # always False and the exchange SL trigger stayed at sl_initial forever (proven
    # live 2026-06-11: 0 "SL updated" in 30d extended-bot-a/b journal vs 31 wick_sl
    # emergency exits). Compare against the price ACTUALLY resting on the exchange
    # (_sl_placed_px), with a small threshold to avoid churn on sub-bps moves.
    _trail_side = getattr(pos, "side", "long")
    # Liq-guard the trailed SL BEFORE deciding to re-place (2026-06-11): on an isolated
    # thin coin the ratcheted SL can land outside liqPx; clamp inside liq+buffer (REMEDY-B;
    # Extended cross-only so add-margin REMEDY-A is a no-op) so the value compared + sent
    # is the one that will actually rest inside liquidation.
    target_sl, _trail_action = ensure_sl_inside_liq(
        client=client, coin=pos.coin, side=_trail_side,
        sl_px=pos.sl_current, size=pos.size,
    )
    placed_sl = pos.__dict__.get("_sl_placed_px")
    moved = placed_sl is None or abs(target_sl - placed_sl) / max(placed_sl, 1e-9) > _SL_REPLACE_THRESH
    if moved:
        if _trail_action not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
            log.warning("SL trail liq-guard %s %s: action=%s sl_current=%.6f → place=%.6f",
                        pos.coin, tf, _trail_action, pos.sl_current, target_sl)
        # PLACE-BEFORE-CANCEL: place the new reduce-only SL first, then cancel the old.
        # Two brief reduce-only SLs are harmless (the looser one no-ops after close);
        # if placement fails, the OLD SL stays intact → position is never naked.
        new_sl_oid = _place_sl_with_retry(
            client=client, coin=pos.coin, size=pos.size,
            sl_price=target_sl, side=_trail_side,
        )
        if new_sl_oid is not None:
            # Post-invariant (rule step 3): the SL now resting must be inside liq+buffer.
            # If not (clamp under-shot on a tight isolated liq), don't keep a position whose
            # stop is past liquidation — emergency-close. cancel the just-placed oid first.
            if not _sl_inside_liq_ok(client, pos.coin, _trail_side, target_sl):
                log.critical(
                    "INVARIANT FAIL (trail) %s %s: SL %.6f OUTSIDE liquidation after guard — emergency closing",
                    pos.coin, tf, target_sl,
                )
                try:
                    client.cancel_sl_order(pos.coin, new_sl_oid)
                except Exception as e:
                    log.warning("cancel SL after trail invariant-fail %s: %s", pos.coin, e)
                exit_px = client.mark_price(pos.coin) or target_sl
                _emergency_close(client, pos, exit_px, "sl_outside_liquidation_trail", trade_id, sl_order_id)
                return "sl_outside_liquidation_trail"
            if sl_order_id and str(sl_order_id) != str(new_sl_oid):
                try:
                    client.cancel_sl_order(pos.coin, sl_order_id)
                except Exception as e:
                    log.warning("cancel old SL %s failed (harmless, reduce-only): %s", pos.coin, e)
            pos.__dict__["_sl_order_id"] = new_sl_oid
            pos.__dict__["_sl_placed_px"] = target_sl
            pos.sl_current = target_sl  # keep tracked SL == on-exchange SL (may be liq-clamped)
            if trade_id:
                update_trade_sl(trade_id, target_sl)
                update_trade_sl_order(trade_id, new_sl_oid)
            log.info("SL re-placed %s %s (%s): → %.6f sz=%.4f oid=%s",
                     pos.coin, tf, _trail_side, target_sl, pos.size, new_sl_oid)
        else:
            log.warning("SL re-place FAILED %s %s — keeping existing SL (not naked)", pos.coin, tf)

    return None


def _emergency_close(client, pos: Position, exit_px: float, reason: str, trade_id, sl_order_id) -> None:
    """Market close + cancel SL/TP + record in DB.

    Close-and-verify BEFORE cancelling the protective SL (2026-05-31 orphan fix):
    market_close() RETURNS an error-dict (not raises) on SDK failure, so the old
    bare try/except treated a failed close as success, then recorded the trade
    closed unconditionally — leaving a naked, untracked position. _ensure_flat
    re-checks the actual position (robust to the error-return), retries the close,
    and places a protective reduce-only SL on any residual. The resting SL is
    reduce-only, so keeping it live during the close can never over-close/flip.
    Only record close once flat; on a residual keep the trade DB-open for recovery.
    """
    if _dry_block(f"emergency_close {pos.coin} reason={reason}"):
        return
    log.info("EMERGENCY CLOSE %s %s: reason=%s exit=%.6f", pos.coin, pos.tf, reason, exit_px)

    _cancel_tp_if_any(client, pos)

    flat = _ensure_flat(client, pos.coin, is_buy_open=(getattr(pos, "side", "long") == "long"))

    if sl_order_id:
        try:
            client.cancel_sl_order(pos.coin, sl_order_id)
        except Exception as e:
            log.warning("cancel_sl_order failed: %s", e)

    if flat:
        _record_close(trade_id, pos, exit_px, reason)
    else:
        log.critical(
            "EMERGENCY CLOSE %s left residual after retries — protective SL placed; "
            "trade kept DB-open for restart recovery", pos.coin,
        )


def _record_close(trade_id, pos: Position, exit_price: float, exit_reason: str) -> None:
    if trade_id is None:
        return
    is_long = (getattr(pos, "side", "long") == "long")
    sl_dist = (pos.entry_price - pos.sl_initial) if is_long else (pos.sl_initial - pos.entry_price)

    def _r(px: float) -> float:
        if sl_dist <= 0:
            return 0.0
        return ((px - pos.entry_price) if is_long else (pos.entry_price - px)) / sl_dist

    def _pnl(px: float, sz: float) -> float:
        return ((px - pos.entry_price) if is_long else (pos.entry_price - px)) * sz

    if getattr(pos, "tp1_partial_done", False):
        # frac booked at tp1, remainder (pos.size) exits here. Size-weighted R + summed PnL.
        frac = pos.__dict__.get("_tp1_frac", 0.5)
        rem_size = pos.size
        orig_size = rem_size / (1.0 - frac) if frac < 1.0 else rem_size
        booked_size = max(orig_size - rem_size, 0.0)
        pnl = _pnl(pos.tp1_price, booked_size) + _pnl(exit_price, rem_size)
        realized_r = frac * _r(pos.tp1_price) + (1.0 - frac) * _r(exit_price)
    else:
        pnl = _pnl(exit_price, pos.size)
        realized_r = _r(exit_price)

    close_trade(
        trade_id=trade_id,
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl_dollars=pnl,
        realized_r=realized_r,
    )


def _reject_and_log(signal: Signal, reason: str) -> None:
    log.info("REJECT %s %s %s: %s", signal.coin, signal.tf, signal.side, reason)
    insert_rejected(
        coin=signal.coin, tf=signal.tf,
        trigger_price=signal.trigger_price,
        entry_price=signal.entry_price,
        sl_price=signal.sl_price,
        reason=reason,
        direction=signal.side,
    )


def _current_notional(exchange_positions: dict, client) -> float:
    """Sum absolute notional of all open positions from exchange state.

    Alias-dedup (2026-06-11): exchange_extended.open_positions() returns each position
    under TWO keys — full market name ('BTC-USD') AND bare coin ('BTC') — pointing at
    the SAME entry dict (exchange_extended.py:412-417). Iterating .items() raw counted
    every position TWICE → existing margin doubled in check_mm_cap → the MM-cap 50%
    effectively bound at ~25% real margin (parity break vs the bt sim's MM50). Dedup
    by entry-dict identity, which is alias-shape-agnostic.
    """
    total = 0.0
    seen: set = set()
    for coin, pos_data in exchange_positions.items():
        if id(pos_data) in seen:
            continue
        seen.add(id(pos_data))
        try:
            sz = abs(float(pos_data.get("szi", 0) or 0))
            px = float(pos_data.get("entryPx", 0) or 0)
            if sz > 0 and px > 0:
                total += sz * px
        except Exception:
            continue
    return total
