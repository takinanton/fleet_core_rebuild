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
import os
import time
from typing import Optional

from bot.config import Settings
from bot.config import settings as _global_settings
from bot.journal import (
    close_trade,
    delete_pending,
    insert_pending,
    insert_rejected,
    insert_trade,
    mark_tp1_partial,
    open_trades,
    promote_pending,
    register_placed_trigger_oid,
    update_trade_sl,
    update_trade_sl_order,
    update_trade_tp_order,
)
from bot.liquidity import LiquiditySnapshot, SnapshotHolder
from bot.risk import SizeResult, check_concurrent_cap, check_mm_cap, compute_size
# XNN port 2026-06-10: strategy module repointed uk_v102 -> xnn (same contract).
from bot.strategy_us29 import Position, PositionManager, Signal

log = logging.getLogger(__name__)

# US29 EXIT-ON-FLIP (regime de-risk, 2026-06-20). Default OFF = bit-parity with the
# deployed block-only gate. When ON, manage_open_position flattens open xyz_ longs at the
# next tick once the broad-market regime is OFF (xyz_SP500 close < SMA200, causal). Validated
# on the live F4_vb top-3 selector: full-cycle maxDD 47%->39%, 2008 -23%->-2%, 2022 -26%->-15%
# at deployed lev3 (no global leverage change). See project_combo_is_our_live_strategy_2026_06_20.
_US29_REGIME_EXIT = os.getenv("US29_REGIME_EXIT", "0").strip().lower() not in ("0", "false", "no", "off", "")

# Re-entry cooldown after a post-fill cap-breach / partial-fill abort. The pre-send
# price gate can't stop a market order from filling PAST the cap during the send
# (slippage), so a still-valid signal would re-fire on the very next scan and
# double-fill (root cause of the XRP double market fill 2026-05-30). Block the same
# coin from re-entry for a short window after such an abort. Skip-only — never naked.
_ENTRY_ABORT_COOLDOWN: dict = {}
ENTRY_ABORT_COOLDOWN_SEC = 900.0  # 15 min — covers the next-scan re-fire window

# PHANTOM-GUARD (class fix 2026-06-23, xyz_NATGAS incident). A DB-open coin can be ABSENT from
# live open_positions (a legit SL/manual close the dedicated detectors missed, OR a true phantom
# row). The OLD guard kept a 90s timer in pos.__dict__["_first_gone_ts"]; that dict is rebuilt
# every tick by the per-tick adopt/restore path, so the timer reset to None each loop and NEVER
# fired -> "defer to confirm" forever, while the SL-heal re-placed an orphan reduce-only trigger
# on the non-existent position every ~60s (HL auto-cancels it -> 429-churn + orphan-trigger litter).
# Fix: a MODULE-LEVEL consecutive-miss counter (keyed by coin) that survives pos rebuilds; auto-
# close + orphan-cancel after K consecutive confirmed-absent ticks; reset to 0 on any live sighting.
_PHANTOM_MISS: dict = {}            # coin -> consecutive ticks absent from live open_positions
PHANTOM_MISS_CLOSE_K = 3           # close the stale DB row after this many consecutive misses


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

# Skip SL re-placement when the structural trail moves it by less than this
# (relative) amount — avoids cancel/replace churn on sub-bps ratchets.
_SL_REPLACE_THRESH = 0.0005  # 5 bps

# Re-anchor distance for a freshly-restored position whose DB SL reads "through" (wrong
# side of mark) — almost always a stale/duplicate DB row, NOT a real gap. We re-protect
# (never close) by anchoring a fresh SL this far from mark; the structural trail tightens
# it on the next bar. (fix b 2026-06-07; mem:project_hl_restart_emergency_close_stale_db_rows)
_RESTORE_REANCHOR_PCT = 0.06


def _dry_block(what: str) -> bool:
    """XNN port 2026-06-10 (review fix): DRY_RUN must mean ZERO orders, enforced in CODE.

    Before this guard dry_run gated only bar-close entries (main.py) + resting paths;
    manage/heal/trail/emergency-close placed REAL orders even in DRY the moment any
    position object existed (adopted DB row / manual insert / brief live flip).
    Returns True (= block) when the GLOBAL settings say dry_run. Loud on every block —
    a blocked order in DRY is always worth seeing in the journal.
    """
    if _global_settings.dry_run:
        log.warning("[DRY-RUN] BLOCKED %s — no order sent", what)
        return True
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
    # Belt-and-braces: main.py already skips attempt_entry in DRY; this guard makes the
    # class (any future caller) safe at the source.
    if _dry_block(f"attempt_entry({signal.coin} {signal.tf} {signal.side})"):
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

    # --- EFFECTIVE leverage (review fix 2026-06-10) ---
    # Before: margin math assumed settings.leverage (10x) while the exchange kept each
    # coin's CURRENT account leverage (alts often max 3-5x) and update_leverage() was
    # never called -> real margin could be 2-3x the model, MM50 "passed" while actual
    # cross-margin pressure was far higher (shared pool with foreign positions).
    # Now: eff_lev = min(settings.leverage, asset.max_leverage) is (a) actually SET on
    # the exchange pre-order (abort entry if that fails), (b) used in sizing cap and
    # (c) used in the MM-cap margin model.
    _meta_max_lev = int(getattr(meta, "max_leverage", 0) or 0)
    eff_lev = min(settings.leverage, _meta_max_lev) if _meta_max_lev > 0 else settings.leverage
    if eff_lev < settings.leverage:
        log.info("%s: asset max_leverage=%dx < LEVERAGE=%dx — using %dx",
                 signal.coin, _meta_max_lev, settings.leverage, eff_lev)

    # --- Compute target notional (risk × equity / SL dist, capped by EFFECTIVE leverage) ---
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

    # --- Gate: MM cap — measured against LIVE exchange margin, not a model ---
    # existing_margin = REAL account-level margin used by ALL positions (manual/foreign
    # included, each on its own leverage). margin_used_usd() raises on read failure; we
    # fail CLOSED when positions exist and we cannot read margin — never fall back to the
    # old notional/leverage model (that mis-priced foreign positions: Ext under-counted,
    # Nado over-counted → false block at real 10%).
    try:
        existing_margin_usd = client.margin_used_usd()
    except Exception as e:
        if open_positions_exchange:
            log.warning(
                "MM-cap fail-closed: margin_used_usd() failed with %d open position(s); "
                "refusing entry for %s: %s",
                len(open_positions_exchange), signal.coin, e,
            )
            _reject_and_log(signal, f"mm_cap_margin_read_failed: {e}")
            return None
        # No positions on the exchange → no existing margin to account for.
        log.warning("margin_used_usd() failed but no open positions; treating existing margin as $0: %s", e)
        existing_margin_usd = 0.0
    mm_allowed, mm_reason = check_mm_cap(
        new_notional=size_result.notional,
        eff_lev=eff_lev,
        existing_margin_usd=existing_margin_usd,
        account_value=equity,
        mm_cap_pct=settings.mm_cap_pct,
    )
    if not mm_allowed:
        _reject_and_log(signal, mm_reason)
        return None

    # --- SET the leverage on the exchange BEFORE any order (review fix 2026-06-10) ---
    # update_leverage existed (exchange_hl.py:851) but had ZERO call sites — positions
    # opened on whatever leverage the coin happened to carry. Abort the entry if the
    # exchange refuses: margin model above is only valid once eff_lev is actually set.
    if hasattr(client, "update_leverage"):
        # MARGIN MODE per leg (fix 2026-06-20): HIP-3 xyz_ assets are ISOLATED-ONLY —
        # 79/92 xyz report onlyIsolated=True and HL rejects cross with "Cross margin is
        # not allowed for this asset" → every us29 xyz_ entry was rejected here (live:
        # 8/8 URNM/DELL/RIVN/PURRDAT, 0 xyz positions ever opened). Crypto (native perp)
        # keeps cross (TRX live-confirmed). isolated is always permitted for the 13 cross-
        # capable xyz too, so prefix-routing is safe. The downstream SL machinery already
        # handles isolated (ensure_sl_inside_liq REMEDY-A add-margin / REMEDY-B clamp).
        _is_cross = not signal.coin.startswith("xyz_")
        _lev_resp = client.update_leverage(signal.coin, eff_lev, is_cross=_is_cross)
        _lev_ok = _lev_resp is not None and (
            not isinstance(_lev_resp, dict) or _lev_resp.get("status") in (None, "ok")
        )
        if not _lev_ok:
            _reject_and_log(signal, f"leverage_set_failed: {eff_lev}x cross={_is_cross} resp={_lev_resp}")
            return None

    # --- LEG ROUTING: crypto (native perp, not xyz_) vs xyz_ HIP-3 stock leg ---
    # BOTH legs: IMMEDIATE market-at-close entry. Deployed sim (uk_v10d_combo) is
    # CLOSE-CONFIRM -- fire when close[i] crosses the channel, entry = close[i] (market),
    # NO reclaim/retest. The stop-limit continuation gate waited up to 30s for price to
    # reclaim the trigger; on a 24/7 perp that already closed past the channel the price
    # had moved on -> 0/9 fills + a live fill that diverged from the sim's close[i].
    # Crypto got the immediate path 2026-06-23; the xyz_ HIP-3 leg was LEFT on the reclaim
    # gate (fix-propagation gap) -> +25s latency + parity break (e.g. xyz_MU 2026-06-28:
    # waited ~25s, filled 1144.43 vs next-open ~1143.4). 2026-06-28: route BOTH legs through
    # the immediate-market path. Set _entry_immediate=False ONLY to restore the (sim-
    # divergent) reclaim gate.
    _is_crypto_leg = not signal.coin.startswith("xyz_")
    _entry_immediate = True

    # --- Place STOP-LIMIT entry (Extended limit order capped by SDK slippage) ---
    # Limit price = entry_price × (1 + cap) rounded to tick
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

    # LONG → limit BUY; SHORT → limit SELL (is_buy=False). Fills only if market
    # comes to us (or already past the breakout/breakdown level).
    is_long = (signal.side == "long")
    # --- Price gate (A) 2026-05-30: don't chase if mark already past cap-limit ---
    # CRYPTO LEG EXCEPTION (2026-06-23): the price gate kills fast Donchian breakouts.
    # The signal fires AT close[i]; the gate fires immediately after; on a live 24/7 perp
    # the breakout already ran past limit_px by the time the gate checks. A market-with-
    # slippage-cap already bounds the fill price (entry_limit_cap_pct = 0.5%), so there
    # is no need for a pre-send skip -- the fill either lands inside 0.5% or aborts with
    # a cap-breach + 15min cooldown (the existing path below). Skip the gate for crypto.
    # 2026-06-28: _entry_immediate=True for BOTH legs → this legacy xyz pre-send price-gate
    # is bypassed; the immediate-path breakout-validity guard (±cap band) replaces it.
    if not _entry_immediate:
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
    # write-db-row-PRE-order (panel must-fix 2026-06-21): journal a 'pending' row BEFORE the order
    # is submitted, so a crash between submit and fill-confirm leaves a recoverable trace
    # (_reconcile_pending at startup promotes it if a live position exists, else deletes it)
    # instead of a naked untracked position. Promoted to 'open' on fill; deleted on every no-fill
    # / abort path below. 'pending' is invisible to open_trades()/adopt → never affects trading.
    pending_id = insert_pending(
        coin=signal.coin, tf=signal.tf, direction=signal.side,
        entry_intended=signal.entry_price, sl_initial=signal.sl_price,
        tp1=signal.tp1_price, size=size_result.size,
        risk_dollars=size_result.risk_dollars, notional=size_result.notional,
        atr14=(float(getattr(signal, "atr14", 0.0) or 0.0) or None),
        entry_bar_ts=(int(getattr(signal, "bar_ts", 0) or 0) or None),
    )

    if _entry_immediate:
        # ── IMMEDIATE market-at-close entry (crypto since 2026-06-23; xyz_ since 2026-06-28) ──
        # Fire ONE market order the instant the bar closes above/below the channel.
        # client.market_open uses settings.slippage (= entry_limit_cap_pct = 0.5%) as
        # the SDK slippage bound: the order fills only if the market stays within 0.5%
        # of the orderbook mid at send time. On a partial/no-fill we route through the
        # same no-fill cleanup path (delete_pending + insert_rejected) as the xyz_ leg.
        # No reclaim wait, no 30s TTL, no stop-limit: this is a pure MARKET order.
        if _in_entry_cooldown(signal.coin):
            log.info("Entry cooldown active for %s — skipping (recent post-fill abort)", signal.coin)
            delete_pending(pending_id)
            insert_rejected(
                coin=signal.coin, tf=signal.tf,
                trigger_price=signal.trigger_price,
                entry_price=signal.entry_price,
                sl_price=signal.sl_price,
                reason="entry_cooldown_active",
                direction=signal.side,
            )
            return None
        # --- Pre-send breakout-validity guard (2026-06-25 propagated; root Extended -4.94% stale fill) ---
        _cap_g = settings.entry_limit_cap_pct
        _mk_g = client.mark_price(signal.coin) or 0.0
        if _mk_g > 0:
            _lo_g = signal.entry_price * (1 - _cap_g)
            _hi_g = signal.entry_price * (1 + _cap_g)
            if (is_long and _mk_g < _lo_g) or ((not is_long) and _mk_g > _hi_g):
                log.info("REJECT %s %s %s: breakout_invalidated mark=%.6f outside band [%.6f,%.6f]", signal.coin, signal.tf, signal.side, _mk_g, _lo_g, _hi_g)
                delete_pending(pending_id)
                insert_rejected(coin=signal.coin, tf=signal.tf, trigger_price=signal.trigger_price, entry_price=signal.entry_price, sl_price=signal.sl_price, reason="breakout_invalidated", direction=signal.side)
                return None
        log.info(
            "MARKET-ENTRY %s %s: close=%.6f sl=%.6f tp1=%.6f size=%s slippage_cap=%.4f",
            signal.coin, signal.tf,
            signal.entry_price, signal.sl_price, signal.tp1_price,
            size_result.size, settings.entry_limit_cap_pct,
        )
        fill_result = _market_fill_crypto(
            client=client,
            coin=signal.coin,
            is_buy=is_long,
            size=size_result.size,
            limit_px=limit_px,
            min_fill_ratio=settings.min_fill_ratio,
        )
    else:
        # ── DISABLED FALLBACK (_entry_immediate=False): STOP-LIMIT reclaim gate. ──
        # Kept for rollback only — diverges from the close-confirm sim (entry=close[i]).
        fill_result = _place_and_wait_fill(
            client=client,
            coin=signal.coin,
            is_buy=is_long,
            size=size_result.size,
            limit_px=limit_px,
            trigger_px=signal.entry_price,  # STOP-LIMIT: confirm continuation at trigger
            ttl_sec=settings.entry_limit_ttl_sec,
            min_fill_ratio=settings.min_fill_ratio,
            meta=meta,
        )

    if fill_result is None:
        delete_pending(pending_id)  # no fill → drop the pre-order row
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
    # Orphan fix (2026-05-31): invalidate positions cache after a confirmed open.
    # The concurrent-cap gate + anti-dup loop seed open_positions() 20s cache with
    # the PRE-open snapshot; without this the next manage_open_position() tick reads
    # that stale cache, finds the just-opened position absent, and records a false
    # "closed_by_exchange" - orphaning the live SL-protected position (root cause of
    # untracked HL orphans TRX/xyz_COST/xyz_EWJ/xyz_GBP, 2026-05-29).
    client.invalidate_positions_cache()
    # --- Slip persist (B) 2026-05-30: signed adverse entry slip vs intended (+=worse) ---
    _slip = ((actual_entry - signal.entry_price) / signal.entry_price) if is_long \
        else ((signal.entry_price - actual_entry) / signal.entry_price)

    # --- SL-inside-liquidation guard (2026-06-11; per-leg margin 2026-06-20) ---
    # Margin mode is per-leg now (trader.py update_leverage call above): crypto = CROSS
    # (account-level liqPx None = safe), xyz_ HIP-3 = ISOLATED by design (is_cross=False —
    # the venue is isolated-only). For an ISOLATED position the per-position liqPx can sit
    # inside the SL; ensure_sl_inside_liq adds isolated margin (REMEDY-A) or clamps the SL
    # (REMEDY-B) so the SL that goes to the exchange rests strictly INSIDE liquidation+buffer.
    # DO NOT delete the REMEDY path — it is LIVE for every xyz_ entry.
    sl_to_place, _liq_action = ensure_sl_inside_liq(
        client=client, coin=signal.coin, side=signal.side,
        sl_px=signal.sl_price, size=filled_size,
    )
    if _liq_action not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
        log.warning("SL liq-guard %s %s: action=%s sl %.6f→%.6f",
                    signal.coin, signal.tf, _liq_action, signal.sl_price, sl_to_place)

    # --- Place stop-loss trigger (retry 3x; emergency close if all fail) ---
    # SHORT: SL is ABOVE entry → trigger BUYS to close (handled via side=signal.side).
    sl_order_id = _place_sl_with_retry(
        client=client,
        coin=signal.coin,
        size=filled_size,
        sl_price=sl_to_place,
        side=signal.side,
    )
    # Post-invariant: SL must be inside liq+buffer or the position is unsafe → close it.
    # (REMEDY-B guarantees inside-ness; this also catches a still-isolated tight liq where
    #  add-margin couldn't and clamp somehow didn't land — never leave SL past liquidation.)
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
        _ensure_flat(client, signal.coin, is_buy_open=is_long)
        delete_pending(pending_id)  # position emergency-closed → drop the pre-order row
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
        # Orphan fix (2026-05-31): _ensure_flat retries the close AND places a
        # protective reduce-only SL on any residual, so a silently-missed
        # market_close can never leave a naked, untracked position (the prior
        # bare market_close ignored its return + had no residual guard).
        if not _ensure_flat(client, signal.coin, is_buy_open=is_long):
            log.critical(
                "EMERGENCY CLOSE/PROTECT FAILED for %s — MANUAL INTERVENTION REQUIRED",
                signal.coin,
            )
        delete_pending(pending_id)  # naked position closed → drop the pre-order row
        insert_rejected(
            coin=signal.coin, tf=signal.tf,
            trigger_price=signal.trigger_price,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            reason="sl_placement_failed_3x_naked_position_closed",
            direction=signal.side,
        )
        return None

    # --- Journal --- write-db-row-PRE-order (panel 2026-06-21): the row was written 'pending'
    # BEFORE the order submit (above); PROMOTE it to 'open' now with the ACTUAL fill. The frozen
    # atr14 / entry_bar_ts were persisted at insert_pending (restart-persistent chandelier ATR_e
    # + donchian 120-bar time-stop); promote overwrites only the post-fill fields (entry, size,
    # risk, notional, slip, notes). On a crash between submit and HERE the pending row +
    # _reconcile_pending(startup) recover the position (adopt heals SL) instead of leaving it naked.
    promote_pending(
        pending_id,
        entry=actual_entry,
        size=filled_size,
        risk_dollars=size_result.risk_dollars,
        notional=filled_size * actual_entry,
        walk_slip_pct=_slip,
        notes=(
            f"f1_dist={signal.f1_dist:.2f} pivot_h={signal.pivot_high:.6f} "
            f"pivot_l={signal.pivot_low:.6f} liq_1h_vol=${liq_profile.avg_1h_vol_usd:.0f}"
        ),
    )
    trade_id = pending_id
    if sl_order_id:
        update_trade_sl_order(trade_id, sl_order_id)

    pos = Position(
        coin=signal.coin,
        tf=signal.tf,
        entry_price=actual_entry,
        sl_initial=signal.sl_price,           # strategy intent (R-distance / max_run_cap math)
        sl_current=sl_to_place,               # SL actually resting on the exchange (liq-guarded)
        tp1_price=signal.tp1_price,
        size=filled_size,
        bar_entry_idx=0,
        side=signal.side,
    )
    pos.__dict__["_trade_id"] = trade_id  # attach db id for updates
    pos.__dict__["_sl_order_id"] = sl_order_id
    pos.__dict__["_orig_size"] = filled_size
    pos.__dict__["_sl_placed_px"] = sl_to_place  # SL price currently on exchange (liq-guarded)

    # F4 (2026-06-19): FREEZE the chandelier ATR_e at the ENTRY/SIGNAL bar, deterministically.
    # The reference (cash_ranker_sim_v2._simulate_exit: atr_e=at[e_idx]) freezes Wilder ATR14
    # at the signal bar and trails chandelier = max(prev, runmax_high - MULT*atr_e_frozen) for
    # the position's life. The PM previously recomputed ATR_e LAZILY on the first manage tick
    # over a frame ending at the LATEST closed bar (>= 1 bar after entry) -> timing-dependent,
    # non-reproducible trail width. signal.atr14 IS the signal-bar Wilder ATR14 (us29_core
    # meta.atr14 / donchian _signal_bar_atr14). Stash it here so the PM uses it directly; the PM
    # keeps its own _signal_bar_atr14 only as a fallback when this is missing/<=0. _hh inits to
    # entry_price (runmax-high since entry).
    #
    # Audit MED (2026-06-19): the stash MUST be ROUTED PER LEG. attempt_entry serves BOTH legs
    # (us29 xyz_ HIP-3 stocks AND donchian native-crypto, dispatched by scanner._strat_for on
    # the xyz_ prefix). Previously it wrote ONLY the _us29_* keys for every leg, so the donchian
    # PositionManager — which reads _donch_atr_e / _donch_hh / _donch_entry_ts — never saw the
    # frozen value: _donch_atr_e was never set, _ensure_state ALWAYS hit its lazy fallback and
    # recomputed ATR off the latest closed bar (>=1 bar after entry) => the crypto leg's
    # chandelier trail width was timing-dependent / non-reproducible and diverged from the
    # ported uk_v10c_donchian_calmar backtest. The frozen value sat unreachable in _us29_atr_e.
    # Route by the same xyz_ prefix the scanner routes on, writing the leg-correct keys.
    _is_xyz_leg = str(signal.coin).startswith("xyz_")
    _atr_key = "_us29_atr_e" if _is_xyz_leg else "_donch_atr_e"
    _hh_key = "_us29_hh" if _is_xyz_leg else "_donch_hh"
    _ts_key = "_us29_entry_ts" if _is_xyz_leg else "_donch_entry_ts"
    _sig_atr = float(getattr(signal, "atr14", 0.0) or 0.0)
    if _sig_atr > 0:
        pos.__dict__[_atr_key] = _sig_atr
    pos.__dict__[_hh_key] = float(actual_entry)
    # F7: entry-bar ts (ms) so the PM can recompute hh = max(entry, High over ALL bars since
    # entry) from the manage frame each tick (restart-robust; incorporates bars skipped during
    # downtime), not just the latest bar's high. For the donchian leg this same entry_ts also
    # makes the 120-bar time-stop restart-persistent (Audit MED 2026-06-19): bars-held is
    # derived from closed bars at/after entry_ts, not an in-memory-only counter that resets to 0
    # on restart.
    _sig_bar_ts = int(getattr(signal, "bar_ts", 0) or 0)
    if _sig_bar_ts > 0:
        pos.__dict__[_ts_key] = _sig_bar_ts

    # --- 50% partial take-profit: resting reduce-only MAKER limit @ 161.8% fib ---
    # Best-effort: a failure leaves the position fully covered by its (reduce-only) SL,
    # so it can never create a naked position. is_buy closes the side (short→buy).
    tp_oid = None
    tp_frac = getattr(settings, "tp1_partial_frac", 0.0)
    if tp_frac > 0:
        tp_size = filled_size * tp_frac
        try:
            tp_resp = client.limit_reduce_only(
                coin=signal.coin,
                is_buy=(signal.side == "short"),
                sz=tp_size,
                limit_px=signal.tp1_price,
            )
            if tp_resp.get("status") == "ok":
                sts = tp_resp["response"]["data"]["statuses"]
                if sts and "resting" in sts[0]:
                    tp_oid = sts[0]["resting"].get("oid")
                    # APPEND the reduce-only TP oid to the bot-own registry (Audit MED
                    # 2026-06-20): the sweep also enumerates reduce-only TP triggers, so a
                    # bot-own TP that later orphans (position closed by SL, TP left resting)
                    # must be recognised as bot-placed and swept — never mistaken for manual.
                    register_placed_trigger_oid(tp_oid)
            log.info("Partial TP %s %s: %.4f @ %.6f (50%% maker, oid=%s)",
                     signal.coin, signal.tf, tp_size, signal.tp1_price, tp_oid)
        except Exception as e:
            log.warning("Partial TP limit place failed for %s (non-fatal): %s", signal.coin, e)
    pos.__dict__["_tp_oid"] = tp_oid
    if tp_oid and trade_id:
        update_trade_tp_order(trade_id, tp_oid)  # persist resting TP oid for restart cleanup

    return pos


def ensure_sl_inside_liq(client, coin: str, side: str, sl_px: float, size: float):
    """Guarantee the SL rests strictly INSIDE liquidation by LIQ_SL_BUFFER_PCT.

    Rule (2026-06-11, feedback_sl_must_be_inside_liquidation): an SL outside liquidation
    is worthless — the position is force-liquidated before the stop ever fires, and on a
    thin HIP-3 isolated market the liqPx sits close to entry. This enforces, for OUR
    entry/trail SL only (never foreign / manual positions — callers gate on that):

      long  : SL_px must be >  liqPx × (1 + buf)
      short : SL_px must be <  liqPx × (1 - buf)

    Mechanism:
      (1) Entry already force-crosses (trader.py update_leverage is_cross=True). A cross
          position has account-level liquidation → position_liquidation() returns liq_px
          None → treated SAFE here (account-level safety, nothing to clamp to a number).
      (2) If the venue forces/keeps ISOLATED (thin coin) OR liqPx is still inside the SL:
          REMEDY-A (preferred): add isolated margin so liqPx moves past SL+buffer,
            re-read liqPx, repeat ≤3 iters. Requires client.add_isolated_margin.
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
    # sl5-1 fix (2026-06-20): on a FRESH entry the clearinghouse can lag the fill, so the
    # first read returns None. For HIP-3 isolated coins (xyz_) that None used to skip the
    # liq-guard AND the post-placement invariant (both fail-OPEN) → an isolated SL could
    # rest BEHIND liquidation for a bounded window. Poll a few times for the materialized
    # per-position liqPx before giving up. Only isolated xyz_ needs this (crypto is cross =
    # account-level safe, liqPx None by design) → poll=1 for crypto keeps entry latency flat.
    liq_info = None
    if hasattr(client, "position_liquidation"):
        _poll = 6 if coin.startswith("xyz_") else 1
        for _i in range(_poll):
            try:
                liq_info = (client.position_liquidation(coin, fresh=True)
                            if _i > 0 else client.position_liquidation(coin))
            except TypeError:
                try:
                    liq_info = client.position_liquidation(coin)
                except Exception as e:
                    log.warning("ensure_sl_inside_liq: position_liquidation(%s) threw: %s", coin, e)
                    liq_info = None
            except Exception as e:
                log.warning("ensure_sl_inside_liq: position_liquidation(%s) threw: %s", coin, e)
                liq_info = None
            # Usable reading = a real per-position liqPx (isolated). Keep polling while the
            # position has not materialized yet (None info or no liq_px) on an xyz_ coin.
            if liq_info is not None and liq_info.get("liq_px"):
                break
            if _i < _poll - 1:
                time.sleep(1.0)
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
    # Only meaningful on an ISOLATED position with a real per-position liqPx.
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
            # Re-read liqPx after the top-up. fresh=True bypasses the 10s user_state
            # cache so the loop observes its OWN deposit and converges (else it would
            # re-read the stale pre-top-up liqPx and over-fund up to 3x).
            try:
                liq_info = client.position_liquidation(coin, fresh=True)
            except TypeError:
                # Older client without the fresh kwarg — fall back to plain re-read.
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
    if _global_settings.dry_run:
        return True
    buf = getattr(_global_settings, "liq_sl_buffer_pct", 0.02)
    if not hasattr(client, "position_liquidation"):
        return True
    # S1-1 fix (2026-06-20): for a FRESH isolated xyz_ entry the clearinghouse can lag the
    # fill, so a single read returns None and this post-placement invariant would fail-OPEN
    # (return True) on a behind-liq SL → the unsafe SL passes the entry assert. Poll for the
    # materialized per-position liqPx (mirrors ensure_sl_inside_liq's entry poll) before
    # concluding "safe". Crypto is cross (liqPx None by design) → poll=1, no behavior change.
    _poll = 6 if coin.startswith("xyz_") else 1
    info = None
    for _i in range(_poll):
        try:
            info = (client.position_liquidation(coin, fresh=True)
                    if _i > 0 else client.position_liquidation(coin))
        except TypeError:
            try:
                info = client.position_liquidation(coin)
            except Exception:
                info = None
        except Exception:
            info = None  # can't prove unsafe on a transient read failure
        if info and info.get("liq_px"):
            break
        if _i < _poll - 1:
            time.sleep(1.0)
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

    Caller MUST emergency-close the position if this returns None — there is no
    SL on the exchange and the position is naked.

    LONG: SL below entry → trigger SELLS to close (is_buy=False).
    SHORT: SL above entry → trigger BUYS to close (is_buy=True).
    """
    if _dry_block(f"trigger_sl({coin} sz={size} px={sl_price})"):
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
                    _oid = statuses[0]["resting"].get("oid")
                    # APPEND to the persistent bot-own SL/TP oid registry (Audit MED
                    # 2026-06-20). This is the SINGLE chokepoint for EVERY reduce-only SL
                    # the bot places — entry, naked-heal, restored-reconcile, and EVERY
                    # chandelier-trail re-place — so every rotated trail oid is durably
                    # registered as bot-placed and the orphan-trigger sweep can sweep it
                    # (while a never-registered manual ЮК oid stays fenced). Append-only,
                    # best-effort: the SL is already live; a registry failure only degrades
                    # the sweep to its conservative coin-level fallback for this oid.
                    register_placed_trigger_oid(_oid)
                    return _oid
            except Exception:
                pass
        log.warning("SL placement attempt %d/%d failed for %s: %s",
                    attempt + 1, attempts, coin, resp)
        time.sleep(0.5 * (attempt + 1))
    return None


def _sl_confirmed_live(client, coin: str, sl_order_id) -> bool:
    """True iff a live stop/reduce-only order with `sl_order_id` is CONFIRMED on the exchange.

    Class guard for "never naked >1 tick" (mirror of Pacifica/Nado read-back fix
    2026-06-07; see mem:feedback_order_placement_readback_and_invariants). Robust in
    BOTH failure directions:
      - stored oid is None/falsy           -> no SL regardless of API -> NOT live (always heal;
        the restored-NULL / fresh-adopt case).
      - stored oid present, list ok        -> live iff oid is among the coin's open stop /
        reduce-only / trigger orders (catches a STALE oid: cancelled / expired / triggered, or
        a trail place-before-cancel that stored a dead oid).
      - stored oid present, list threw     -> can't prove it's gone; return True to avoid a
        duplicate-SL storm / spurious emergency-close on a transient blip (a genuinely-dead SL
        with a flaky API is still caught on the next clean tick).
    Matching by oid only: trigger_sl always places a reduce-only stop on the correct close
    side and the trail path resizes it, so a present-and-resting order with our oid IS the SL.
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

    A just-created trigger can lag in frontendOpenOrders for a second or two (fresh-open
    lag class). Poll a few times before declaring failure so we never emergency-close a
    position that is in fact protected. Bounded — total wait ~ a few seconds, well inside
    one 60s tick.
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
    if _dry_block(f"_ensure_flat({coin})"):
        return True  # DRY: pretend flat — never send market_close/trigger_sl
    if known_filled_sz and known_filled_sz > 0:
        # Caller CONFIRMED a fill (saw avgPx/totalSz); the /positions read lags the
        # fill by seconds, so an empty read here is NOT proof of flat. Poll until the
        # position materialises before the close loop runs, so we never declare
        # "flat" on a not-yet-propagated fill (XRP orphan race 2026-05-30).
        for _ in range(6):
            try:
                client.invalidate_positions_cache()
                _p = client.open_positions().get(coin)
            except Exception:
                _p = None
            if _p:
                break
            time.sleep(1.0)
    for _ in range(3):
        try:
            client.invalidate_positions_cache()
            pos = client.open_positions().get(coin)
        except Exception:
            pos = "?"  # unknown -> force a close attempt
        if pos is None:
            return True  # confirmed flat
        client.market_close(coin)
        time.sleep(1.0)
    try:
        client.invalidate_positions_cache()
        pos = client.open_positions().get(coin)
    except Exception:
        pos = None
    if not pos:
        return True
    try:
        sz = abs(float(pos.get("szi", 0)))
        mark = client.mark_price(coin)
        if sz > 0 and mark > 0:
            _side = "long" if is_buy_open else "short"
            trig = mark * 0.975 if is_buy_open else mark * 1.025
            # S4-3 fix (2026-06-20): route the residual protective SL through the liq-guard too —
            # this was the ONE SL path bypassing ensure_sl_inside_liq, newly reachable for a thin
            # isolated xyz_ residual (could otherwise rest behind liquidation).
            try:
                trig, _act = ensure_sl_inside_liq(client=client, coin=coin, side=_side, sl_px=trig, size=sz)
            except Exception as _le:
                log.warning("_ensure_flat %s: liq-guard threw (%s) — using raw ±2.5%% residual SL", coin, _le)
            client.trigger_sl(coin=coin, is_buy=(not is_buy_open), sz=sz, trigger_px=trig)
            log.error("ABORTED ENTRY %s left residual %.6f after 3x market_close - placed protective SL @ ~%.4f", coin, sz, trig)
    except Exception as e:
        log.error("ABORTED ENTRY %s: FAILED to place protective SL on residual: %s", coin, e)
    return False


def _confirm_fill_via_positions(client, coin: str, tag: str = "") -> tuple | None:
    """Poll open_positions to confirm a fill whose order response was lost.

    Shared by the UNPARSEABLE-response path AND the market_open EXCEPTION path
    (2026-07-02 fleet propagation of the Extended WS-confirm fix): HL's
    exchange.market_open() got a hard (connect=5, read=20) session timeout on
    2026-07-02, so a slow accept can now RAISE after the order already landed on
    the book. Returning None on that exception (old behaviour) left the fill
    tracked only by the naked-sentinel, which protects it with a WIDE +-6% SL
    instead of the strategy SL. Poll the exchange first: if a position
    materialised, adopt it as a real fill (caller attaches the strategy SL).
    Returns (mark_px, confirmed_sz) or None if no position appears.
    """
    _confirmed_sz = 0.0
    for _ in range(4):
        try:
            client.invalidate_positions_cache()
            _p = client.open_positions().get(coin)
        except Exception as _e2:
            log.warning("%sopen_positions() confirm failed for %s: %s", tag, coin, _e2)
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
                "%sfill CONFIRMED via open_positions for %s: szi=%.6f @~%.6f "
                "(order response unparseable/exception)",
                tag, coin, _confirmed_sz, mark,
            )
            return mark, _confirmed_sz
    return None


def _market_fill_crypto(
    client,
    coin: str,
    is_buy: bool,
    size: float,
    limit_px: float,
    min_fill_ratio: float,
) -> tuple | None:
    """Immediate market entry for the crypto/donchian leg (2026-06-23).

    Replaces the STOP-LIMIT continuation gate for native-perp coins (not xyz_).
    Calls client.market_open (SDK slippage=settings.slippage = entry_limit_cap_pct
    = 0.5%) which fills only if the orderbook mid is within 0.5% of the send price.
    If the fill price exceeds limit_px (cap breach) or fill_ratio < min_fill_ratio:
    emergency-close and register abort cooldown (same path as _place_and_wait_fill).

    Returns (avg_fill_price, filled_size) or None on no-fill / cap-breach.
    Does NOT call _in_entry_cooldown (caller already checked).
    """
    try:
        resp = client.market_open(coin=coin, is_buy=is_buy, sz=size)
    except Exception as e:
        # market_open may raise AFTER the order landed (session read-timeout on a
        # slow accept). Confirm against the exchange before declaring unfilled —
        # else a landed fill is left to the naked-sentinel's wide SL, not strategy SL.
        log.error("CRYPTO-MARKET market_open(%s) exception: %s -- confirming via positions", coin, e)
        return _confirm_fill_via_positions(client, coin, tag="CRYPTO-MARKET ")

    # Parse fill response (same structure as _place_and_wait_fill)
    try:
        statuses = resp["response"]["data"]["statuses"]
        s = statuses[0] if statuses else {}
        if "error" in s:
            log.warning("CRYPTO-MARKET market_open(%s) error: %s", coin, s["error"])
            return None
        if "filled" in s:
            avg_px_str = s["filled"].get("avgPx", "0")
            avg_px = float(avg_px_str) if avg_px_str else 0.0
            total_sz_str = s["filled"].get("totalSz", "0")
            filled_sz = float(total_sz_str) if total_sz_str else size

            # Cap-breach check (side-aware): reject fill outside 0.5% slippage window
            _breach = (avg_px > limit_px * 1.001) if is_buy else (avg_px < limit_px * 0.999)
            if avg_px > 0 and _breach:
                log.warning(
                    "CRYPTO-MARKET fill %.6f exceeds limit_px %.6f (cap breach) "
                    "for %s -- treating as rejected",
                    avg_px, limit_px, coin,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            # Partial-fill check
            fill_ratio = filled_sz / size if size > 0 else 0
            if fill_ratio < min_fill_ratio:
                log.warning(
                    "CRYPTO-MARKET partial fill %s: %.4f/%.4f (%.1f%% < %.0f%%) "
                    "-- emergency close",
                    coin, filled_sz, size, fill_ratio * 100, min_fill_ratio * 100,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            actual_px = avg_px if avg_px > 0 else limit_px
            log.info(
                "CRYPTO-MARKET fill %s: avg=%.6f sz=%.6f (slip vs close: %.4f%%)",
                coin, actual_px, filled_sz,
                100.0 * (actual_px - limit_px / (1 + 0.005)) / (limit_px / (1 + 0.005)),
            )
            return actual_px, filled_sz
    except Exception as e:
        log.warning("CRYPTO-MARKET fill parse error for %s: %s", coin, e)

    # Unparseable response: confirm via open_positions (phantom-fill prevention)
    _confirmed_sz = 0.0
    for _ in range(4):
        try:
            client.invalidate_positions_cache()
            _p = client.open_positions().get(coin)
        except Exception as _e2:
            log.warning("CRYPTO-MARKET open_positions() confirm failed for %s: %s", coin, _e2)
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
                "CRYPTO-MARKET fill CONFIRMED via open_positions for %s: szi=%.6f @~%.6f "
                "(order response was unparseable)",
                coin, _confirmed_sz, mark,
            )
            return mark, _confirmed_sz
    log.warning(
        "CRYPTO-MARKET no confirmed position for %s after unparseable response "
        "-- treating as UNFILLED (phantom-fill avoided)",
        coin,
    )
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
        # See _confirm_fill_via_positions: a post-2026-07-02 session read-timeout can
        # raise after the order landed; poll positions before declaring unfilled.
        log.error("market_open(%s) exception: %s -- confirming via positions", coin, e)
        return _confirm_fill_via_positions(client, coin, tag="")

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

            # Validate fill against limit cap (side-aware 2026-05-30)
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


def _reconcile_restored_sl(client, pos: Position, trade_id, settings: Settings) -> bool:
    """One-time SL reconcile for a freshly-RESTORED/ADOPTED position (fix b, 2026-06-07).

    A just-restored position's sl_current came from a DB row that may be stale/duplicate.
    If that price already reads 'through' (long: sl >= mark; short: sl <= mark) the SL-hit
    check would emergency MARKET-CLOSE it on the first tick — exactly what force-closed 5
    live positions on the 06-07 restart. A 'through' SL on a just-restored position is
    almost always the WRONG row, not a real gap, so we NEVER close here:
      - correct, live SL on the right side of mark -> keep as-is.
      - missing / stale / 'through' SL             -> place a fresh protective SL (anchored
        to mark if the DB price is through, else re-place at the DB price) and adopt its oid.
    The position stays open and protected; the structural trail manages it from next tick.

    Returns True when reconcile is settled (caller clears grace); False on a transient
    (no mark / placement failed) so the caller retries next tick WITHOUT closing.
    """
    coin = pos.coin
    side = getattr(pos, "side", "long")
    stored_oid = pos.__dict__.get("_sl_order_id")
    try:
        mark = client.mark_price(coin) or 0.0
    except Exception as e:
        log.warning("restore-reconcile %s: mark fetch threw: %s", coin, e)
        mark = 0.0
    if mark <= 0:
        log.warning("restore-reconcile %s: no mark yet — deferring (position untouched)", coin)
        return False

    through = (side == "long" and pos.sl_current >= mark) or \
              (side == "short" and pos.sl_current <= mark)

    # Correct, live SL already on the right side of mark → nothing to do.
    if not through and _sl_confirmed_live(client, coin, stored_oid):
        log.info("restore-reconcile %s %s: SL %.6f confirmed live (mark=%.6f) — OK",
                 coin, pos.tf, pos.sl_current, mark)
        return True

    if through:
        new_sl = mark * (1.0 - _RESTORE_REANCHOR_PCT) if side == "long" \
            else mark * (1.0 + _RESTORE_REANCHOR_PCT)
        log.warning(
            "restore-reconcile %s %s (%s): DB SL %.6f is THROUGH mark %.6f (stale row) "
            "— re-anchoring to %.6f, NOT closing", coin, pos.tf, side,
            pos.sl_current, mark, new_sl,
        )
    else:
        new_sl = pos.sl_current  # valid side, just not confirmed-live → re-place same px
        log.warning("restore-reconcile %s %s (%s): SL not confirmed live — re-placing at %.6f",
                    coin, pos.tf, side, new_sl)

    new_oid = _place_sl_with_retry(client, coin, pos.size, new_sl, side=side)
    if new_oid and _confirm_sl_live_poll(client, coin, new_oid):
        if stored_oid and str(stored_oid) != str(new_oid):
            try:
                client.cancel_sl_order(coin, stored_oid)
            except Exception:
                pass
        pos.sl_current = new_sl
        pos.__dict__["_sl_order_id"] = new_oid
        pos.__dict__["_sl_placed_px"] = new_sl
        if trade_id:
            update_trade_sl(trade_id, new_sl)
            update_trade_sl_order(trade_id, new_oid)
        log.warning("restore-reconcile %s %s: protected at SL=%.6f oid=%s (NOT closed)",
                    coin, pos.tf, new_sl, new_oid)
        return True
    # Placement failed: leave the (old) SL untouched and retry next tick — never close.
    log.error("restore-reconcile %s: protective SL placement FAILED — will retry next tick", coin)
    return False


def manage_open_position(
    pos: Position,
    client,
    settings: Settings,
    position_manager: PositionManager,
    df_latest: dict,     # {tf: DataFrame} with latest closed bars
    regime_off: bool = False,   # US29 broad-market regime OFF (xyz_SP500 < SMA200) this tick
) -> Optional[str]:
    """Update SL, check for exits on an open position.

    Returns exit reason string if position was closed, else None.
    """
    # XNN port 2026-06-10 (review fix): manage places REAL orders (heal SL, trail
    # cancel/replace, emergency market-close) and previously had NO dry gate at all —
    # the "DRY = zero orders" guarantee rested solely on an empty trades.db. Hard gate.
    if _dry_block(f"manage_open_position({pos.coin} {pos.tf})"):
        return None
    trade_id = pos.__dict__.get("_trade_id")
    sl_order_id = pos.__dict__.get("_sl_order_id")

    # FIX (b) 2026-06-07: a freshly-RESTORED position gets a one-time SL reconcile BEFORE any
    # SL-hit / heal logic — never emergency-close a restored position on a stale DB SL. After
    # reconcile, drop grace and proceed with normal management on a verified SL.
    if pos.__dict__.get("_restore_grace"):
        if _reconcile_restored_sl(client, pos, trade_id, settings):
            pos.__dict__.pop("_restore_grace", None)
            sl_order_id = pos.__dict__.get("_sl_order_id")  # may have been re-placed
        else:
            return None  # transient (no mark / placement retry) — never close mid-restore

    # ── US29 EXIT-ON-FLIP (2026-06-20): flatten open xyz_ longs when broad regime is OFF ──
    # Deployed gate is block-only (never exits) -> open longs ride bears down on the ATR×7
    # trail alone (full-cycle 2008 -23%, maxDD -47%). When US29_REGIME_EXIT=1 AND the
    # xyz_SP500 regime is OFF this tick (prior close < SMA200, causal), force-flatten this
    # xyz_ long at the current mark (next-open proxy). Default OFF = bit-parity with today.
    if (regime_off and _US29_REGIME_EXIT
            and str(getattr(pos, "coin", "")).startswith("xyz_")):
        _rx_px = client.mark_price(pos.coin) or getattr(pos, "sl_current", None) or pos.entry_price
        log.warning("US29 regime-flat exit %s @ %.6f (xyz_SP500 < SMA200)", pos.coin, float(_rx_px))
        _emergency_close(client, pos, float(_rx_px), "regime_flat", trade_id, sl_order_id)
        return "regime_flat"


    # ── PHANTOM-GUARD (class fix 2026-06-23, xyz_NATGAS): read live presence ONCE up front and
    # branch on it BEFORE any SL-heal. A coin ABSENT from live open_positions must NEVER be healed
    # (placing a reduce-only SL on a non-existent position rests an orphan that HL auto-cancels →
    # re-place churn every loop). Resolve the absence deterministically instead of deferring forever:
    #   • CONFIRMED ABSENT for K consecutive ticks → auto-close the stale DB row + cancel orphan
    #     triggers + STOP. The miss counter is MODULE-LEVEL (survives the per-tick pos rebuild that
    #     defeated the old pos.__dict__ 90s _first_gone_ts guard).
    #   • CONFIRMED PRESENT → reset the counter, fall through to normal SL-heal/manage.
    #   • read UNKNOWN (open_positions threw) → do NOT auto-close and do NOT count a miss; fall
    #     through so a real position on a flaky API still gets its SL protected (bias-to-protect).
    try:
        exchange_positions = client.open_positions()
        _live_read_ok = True
        _live_present = (pos.coin in exchange_positions
                         or f"{pos.coin}-USD" in exchange_positions)
    except Exception as e:
        log.warning("open_positions() read failed for %s: %s — presence UNKNOWN (no phantom decision)",
                    pos.coin, e)
        exchange_positions = {}
        _live_read_ok = False
        _live_present = None

    if _live_read_ok and not _live_present:
        _miss = _PHANTOM_MISS.get(pos.coin, 0) + 1
        _PHANTOM_MISS[pos.coin] = _miss
        if _miss < PHANTOM_MISS_CLOSE_K:
            log.info("phantom-guard: %s absent from live open_positions (miss %d/%d) — NOT healing, deferring",
                     pos.coin, _miss, PHANTOM_MISS_CLOSE_K)
            return None
        # K consecutive confirmed-absent ticks. One FINAL fresh re-read so a single eventual-
        # consistency blip at tick K can't auto-close a position that is in fact still live.
        # open_positions() is 20s-cached, so invalidate first or this re-read just returns the
        # same cached snapshot (defeating the confirm). Best-effort — not all clients cache.
        try:
            client.invalidate_positions_cache()
        except Exception:
            pass
        try:
            _final = client.open_positions()
        except Exception as e:
            log.warning("phantom-guard: %s final re-read failed (%s) — deferring auto-close", pos.coin, e)
            return None
        if pos.coin in _final or f"{pos.coin}-USD" in _final:
            log.info("phantom-guard: %s reappeared on final re-read — aborting auto-close, managing", pos.coin)
            exchange_positions = _final
            _live_present = True
            _PHANTOM_MISS.pop(pos.coin, None)
        else:
            log.error("phantom-guard: %s absent from live open_positions for %d consecutive ticks (and on "
                      "final re-read) — auto-closing stale DB row + cancelling orphan triggers (no position)",
                      pos.coin, PHANTOM_MISS_CLOSE_K)
            _real = _lookup_real_close_px(client, pos, sl_oid=sl_order_id)
            if _real is not None:
                _cancel_tp_limit(client, pos)
                _cancel_orphan_triggers(client, pos.coin)
                _record_close(trade_id=trade_id, pos=pos, exit_price=_real[0], exit_reason=_real[1])
                _PHANTOM_MISS.pop(pos.coin, None)
                log.warning("phantom-guard: %s REAL close via fills @%.6f (%s) — recorded TRUE exit, not phantom@mark",
                            pos.coin, _real[0], _real[1])
                return _real[1]
            _cancel_tp_limit(client, pos)
            _cancel_orphan_triggers(client, pos.coin)
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _record_close(trade_id=trade_id, pos=pos, exit_price=exit_px,
                          exit_reason="phantom_no_exchange_position")
            _PHANTOM_MISS.pop(pos.coin, None)
            return "phantom_no_exchange_position"

    if _live_present:
        _PHANTOM_MISS.pop(pos.coin, None)  # seen live → reset the consecutive-miss counter

    # DELIST-PHANTOM class fix 2026-07-02 (xyz_INTC on HL): candle availability must NEVER
    # gate position-existence reconciliation — a coin delisted from meta loses candles AND
    # its position; the old df-early-return ABOVE the phantom-guard skipped the guard every
    # tick, leaving the stale DB row open forever (WARN-spam, no resolution). The df gate now
    # runs AFTER the guard, so a vanished position still auto-resolves via real fills.
    tf = pos.tf
    df = df_latest.get(tf)
    if df is None or df.empty:
        return None

    # --- CLASS GUARD: a position must NEVER be without a CONFIRMED-LIVE SL for >1 tick. ---
    # Not enough to check `sl_order_id is None`: the stored oid can be stale (SL cancelled /
    # expired / triggered on the exchange, or a trail place-before-cancel that stored a dead
    # oid). Confirm against the exchange EVERY tick via read-back; if no live SL is protecting
    # this position, immediately (re-)place and READ BACK to confirm it is live. If it still
    # can't be confirmed → emergency-close (atomic-SL-or-emergency-close).
    # (root fix 2026-06-07; mem:feedback_order_placement_readback_and_invariants)
    if not _sl_confirmed_live(client, pos.coin, sl_order_id):
        # Size the heal-SL to the LIVE filled position when readable (handles a resting entry
        # that has only partially filled, and avoids an oversized reduce-only stop). Fall back
        # to the tracked size if the position read is unavailable — a reduce-only stop is
        # clamped to position size on the exchange, so the fallback can never over-close.
        heal_size = pos.size
        try:
            # Reuse the presence read taken at the top of this tick (no duplicate open_positions()
            # call). exchange_positions is {} on an UNKNOWN read → fall back to tracked size; a
            # reduce-only stop is clamped to position size on the exchange, so it never over-closes.
            _ex = exchange_positions.get(pos.coin) or exchange_positions.get(f"{pos.coin}-USD")
            if _ex is not None:
                _live_sz = abs(float(_ex.get("szi", 0) or 0))
                if _live_sz > 0:
                    heal_size = _live_sz
        except Exception:
            pass
        # Liq-guard the heal SL too — a re-placed SL must also rest inside liquidation.
        _heal_side = getattr(pos, "side", "long")
        heal_sl, _heal_action = ensure_sl_inside_liq(
            client=client, coin=pos.coin, side=_heal_side,
            sl_px=pos.sl_current, size=heal_size,
        )
        log.warning(
            "Position %s %s (%s) has NO confirmed-live SL (stored oid=%s) — re-placing sz=%s at sl=%.6f (liq-guard=%s)",
            pos.coin, pos.tf, _heal_side, sl_order_id, heal_size, heal_sl, _heal_action,
        )
        if _heal_action == "no_position":
            log.info("SL-heal skip %s: liq-guard=no_position (gone) — defer to phantom-guard, NOT churning triggers", pos.coin)
            return None
        sl_order_id = _place_sl_with_retry(
            client=client,
            coin=pos.coin,
            size=heal_size,
            sl_price=heal_sl,
            side=_heal_side,
        )
        # READ BACK: don't trust the place response — confirm a live SL exists (poll briefly
        # to tolerate fresh-order propagation lag before any emergency close).
        if sl_order_id is None or not _confirm_sl_live_poll(client, pos.coin, sl_order_id):
            log.error(
                "Position %s %s NAKED — SL re-place/confirm FAILED — emergency closing",
                pos.coin, pos.tf,
            )
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _emergency_close(client, pos, exit_px, "sl_replace_failed_naked", trade_id, sl_order_id)
            return "sl_replace_failed_naked"
        pos.__dict__["_sl_order_id"] = sl_order_id
        pos.__dict__["_sl_placed_px"] = heal_sl
        pos.sl_current = heal_sl  # keep tracked SL == on-exchange SL
        if trade_id:
            update_trade_sl_order(trade_id, sl_order_id)
        log.info("Healed NAKED %s %s — SL oid=%s confirmed live on exchange", pos.coin, pos.tf, sl_order_id)

    # Partial-fill detection: 50% fib limit filled → SL→breakeven on the remainder.
    # (Full-close / phantom detection is handled UP FRONT by the PHANTOM-GUARD block above, which
    # already read exchange_positions this tick and returns on a confirmed-absent position — so
    # reaching here means the position is live: reuse that read, never re-poll open_positions().)
    if _live_read_ok and _live_present:
        try:
            _detect_partial_fill(pos, exchange_positions, position_manager)
        except Exception as e:
            log.warning("partial-fill detect failed for %s: %s", pos.coin, e)

    # ── exit precedence (Audit MED 2026-06-20): mirror the validated bt-1 engine ──
    # bt-1 harness/engine.py:1699-1728 computes the trailing-SL `stop_resolved` (gap_exit /
    # vstop_wick at the SL that stood ENTERING the bar) but then lets strategy.maybe_exit()
    # take PRECEDENCE:  `if strat_exit is not None: ... elif stop_resolved is not None:`.
    # uk_v10c_donchian_calmar.maybe_exit() returns the STRATEGY exits 'time_stop' (close) and
    # 'tp' (4R take). The old live order INVERTED this: check_sl_hit (trailing wick/gap at
    # pos.sl_current) ran FIRST and returned, so on a wide-range donchian 8h bar where the
    # chandelier-trailed SL is wicked AND high>=entry+4R (or bars_held>=120), the backtest
    # exits at +4R / close but the live bot exited at the trailed wick (far below 4R) — a
    # per-leg exit-fidelity divergence from the ported uk_v10c spec. Money-safety is
    # unaffected either way (the SL still fires); this only restores backtest parity.
    #
    # Fix: evaluate the strategy exit FIRST (update_sl_on_new_bar emits the reason), and let
    # the STRATEGY-EXIT class ('tp','time_stop') pre-empt the trailing-SL hit.
    #
    # The trailing-SL LEVEL the check resolves against is leg-specific (Audit MED 2026-06-20
    # CORRECTION — the earlier version of this comment had it backwards). bt-1 resolves bar i's
    # stop against the SL that stood ENTERING bar i (set on the previous iteration), THEN trails
    # for bar i+1.  • us29 leg: its chandelier ratchet masks highs with `ts < cur_bar_ts` (F8,
    # strategy_us29.py:290-294), so the POST-ratchet SL already equals that entering-bar level
    # (highs<i) → test the post-ratchet SL.  • donchian leg: its pivot trail uses up_to_idx=i-1
    # (strategy_donchian.py:_trail_long_sl; NO ts<cur_bar_ts high-mask — lines 441-455 there are
    # time-stop/state code, not a high-mask), so the post-ratchet SL is one bar AHEAD of bt-1's
    # resolve level → test the PRE-ratchet SL (== bt-1's bar-i level) and keep the ratchet for
    # the next bar. See the leg-gated block at `_is_donchian_leg` below. us29 ordering (inert
    # 'max_run_cap', MAX_RUN_R=1000, honored AFTER the SL check) is otherwise unchanged.
    _STRAT_EXIT_PRECEDENCE = ("tp", "time_stop")

    # ── trailing-SL one-bar-AHEAD correction (Audit MED 2026-06-20, donchian leg) ──
    # The donchian leg's PM (strategy_donchian.update_sl_on_new_bar) ratchets pos.sl_current
    # via `_trail_long_sl(lows, i-1, ...)` — i.e. it scans pivots through up_to_idx = i-1
    # (max_pivot_idx = (i-1)-window). bt-1 (harness/engine.py:1696-1812 Python fallback)
    # RESOLVES bar i's stop against the sl_current that was stored on the PREVIOUS iteration —
    # which was trailed with `_trail_long_sl(lo, (i-1)-1 = i-2, ...)` (max_pivot_idx = i-2-window)
    # — and only AFTER the stop is resolved (no exit) trails to `_trail_long_sl(lo, i-1, ...)`
    # for bar i+1. So bt-1 resolves bar i against a trail one bar STALER than the donchian PM's
    # post-ratchet value: testing check_sl_hit against the POST-ratchet SL makes the live trail
    # LEAD bt-1 by exactly one bar at stop-resolution (a strict pivot-low confirmable through the
    # i-1 window fires the live wick_sl on bar i while bt-1 still holds). PROVEN (window=3,
    # buf=0.003): a strict pivot-low at idx7 ratchets the donchian SL to pivot*(1-buf) on bar 11
    # while bt-1's resolve-SL is still the entry-stop; a bar-11 low between the two exits live but
    # not bt-1. (Issues 1 & 2.) FIX: resolve the donchian leg's check_sl_hit against the
    # PRE-RATCHET SL (the stop that stood ENTERING bar i, == bt-1's resolve level), then keep the
    # ratchet for the next bar / exchange placement.
    #
    # us29 leg is NOT corrected: its chandelier ratchet (strategy_us29.update_sl_on_new_bar)
    # computes the SL from hh_prior masked with `ts < cur_bar_ts` (F7/F8) — highs through bar
    # i-1 only — so its POST-ratchet SL ALREADY equals bt-1's resolve level for bar i (its
    # reference cash_ranker_sim_v2 ratchets the chandelier AFTER the stop check from highs<i).
    # Testing the post-ratchet SL is correct there; only the donchian pivot-trail (up_to_idx=i-1,
    # no ts<cur_bar_ts mask) leads, hence the leg gate below.
    _is_donchian_leg = not str(getattr(pos, "coin", "")).startswith("xyz_")
    _sl_pre_ratchet = pos.sl_current  # bt-1's bar-i resolve level (donchian leg)

    # Update trailing SL (ratchets pos.sl_current; emits the strategy/R-cap exit reason).
    new_sl, exit_reason = position_manager.update_sl_on_new_bar(
        pos=pos,
        df=df,
        enable_trail_after_tp=settings.enable_trail_after_tp,
    )
    _sl_after_ratchet = pos.sl_current  # ratcheted SL to re-place if no exit fires

    def _close_on_reason(_reason: str) -> str:
        # Per-reason exit_px (side-aware):
        #   max_run_cap -> entry +/- max_run_r * sl_dist (R-cap math)
        #   tp          -> tp1_price (4R take; donchian is long-only)
        #   time_stop / any other -> mark price (close at market)
        _side = getattr(pos, "side", "long")
        sl_dist_abs = abs(pos.entry_price - pos.sl_initial)
        if _reason == "max_run_cap":
            if _side == "long":
                _exit_px = pos.entry_price + settings.max_run_r * sl_dist_abs
            else:
                _exit_px = pos.entry_price - settings.max_run_r * sl_dist_abs
        elif _reason == "tp":
            # 4R take. Donchian PM only emits 'tp' for long; mirror the strategy's
            # tp1 = entry + TP_R_MULTIPLE * sl_dist (sl_dist = entry - sl_initial).
            tp_r = float(getattr(pos, "tp1_price", 0.0) or 0.0)
            if _side == "long" and tp_r > 0.0:
                _exit_px = tp_r
            elif _side == "long":
                _exit_px = pos.entry_price + settings.max_run_r * sl_dist_abs
            else:
                _exit_px = pos.entry_price - settings.max_run_r * sl_dist_abs
        else:
            # time_stop and any future reason: close at the current mark.
            _exit_px = client.mark_price(pos.coin) or pos.sl_current
        _emergency_close(client, pos, _exit_px, _reason, trade_id, sl_order_id)
        return _reason

    # (1) STRATEGY exit ('tp'/'time_stop') takes PRECEDENCE over the trailing SL (bt-1 parity).
    if exit_reason in _STRAT_EXIT_PRECEDENCE:
        return _close_on_reason(exit_reason)

    # (2) else: trailing-SL hit on the latest bar — resolve against pos.sl_current.
    #   • us29 leg: tested against the POST-RATCHET SL (pos.sl_current == _sl_after_ratchet).
    #     Its chandelier ratchet masks highs with `ts < cur_bar_ts` (F8), so the post-ratchet
    #     SL already == bt-1's bar-i resolve level (highs<i) and excludes bar i's own high.
    #   • donchian leg (Audit MED 2026-06-20, Issue 1 — fixed at the SOURCE in
    #     strategy_donchian.update_sl_on_new_bar): the pivot trail no longer mutates
    #     pos.sl_current WITHIN a bar. It promotes the previous bar's STAGED trail (trail_{i-1})
    #     into pos.sl_current ONLY on the first tick of a new bar, then STAGES the freshly
    #     computed trail for the next bar. So for ALL ~480 re-presentation ticks of bar i,
    #     pos.sl_current == trail_{i-2} == bt-1's bar-i resolve level — the exact value the
    #     on-exchange reduce-only SL also rests at (placement uses pos.sl_current below). The
    #     in-bar one-bar LEAD (and the compounding early exchange trigger) are eliminated at the
    #     ratchet; check_sl_hit against pos.sl_current is now bt-1-faithful every tick.
    #     CORRECTION (Issue 1 FINAL): on a trail-STEP bar, pos.sl_current after the PM call
    #     == _sl_after_ratchet == trail_{i-2} (bt-1's bar-i level), but _sl_pre_ratchet ==
    #     trail_{i-3} (the prior bar's promoted level) — they DIFFER. The old swap forced
    #     pos.sl_current back to _sl_pre_ratchet (trail_{i-3}) for the resolve, one bar STALER
    #     than bt-1: a bar-i low in (trail_{i-3}, trail_{i-2}] stopped bt-1 but the live swap
    #     HELD one extra bar (LOOSER exit, still inside liq — not naked). The swap is REMOVED;
    #     resolve directly against pos.sl_current, which the staging ratchet already pins to
    #     bt-1's bar-i level for every tick (donchian) / via the ts<cur_bar_ts mask (us29).
    # Both legs: resolve check_sl_hit against pos.sl_current AS-IS — it is already bt-1's
    # bar-i resolve level for EITHER leg, so NO pre/post-ratchet swap is needed (Audit MED
    # 2026-06-20, Issue 1 FINAL — the swap here USED to force the donchian leg back to
    # _sl_pre_ratchet, which is now one bar STALER than bt-1 and over-corrected the trail
    # into a one-bar LAG; removed). The _is_donchian_leg gate + the _sl_pre_ratchet /
    # _sl_after_ratchet captures now feed a debug regression sentinel only (below): if a
    # future change ever re-introduces an in-bar ratchet, post != pre will flag the bar.
    if _is_donchian_leg and _sl_after_ratchet != _sl_pre_ratchet:
        log.debug(
            "donchian trail-step %s %s: sl %.6f -> %.6f (resolving against post-ratchet, "
            "== bt-1 bar-i level)", pos.coin, tf, _sl_pre_ratchet, _sl_after_ratchet,
        )
    # ── ENTRY-BAR EXIT SUPPRESSION (Audit MED 2026-06-20, donchian leg) ──
    # bt-1 NEVER resolves an exit on the entry/signal bar E: its bar loop runs the EXIT block only
    # when open_pos is not None, and on bar E open_pos is still None (the position is created in the
    # ENTRY block, AFTER the exit block), so the earliest exit eval is E+1 (validated run
    # 8f870f1280dc_..._4df027/trades.parquet: open_bars min=1, open_bars==0 count=0 over 814 trades).
    # The live manage loop, by contrast, is first invoked while the latest CLOSED bar still IS E
    # (_donch_last_bar_ts inits to 0). update_sl_on_new_bar already suppresses its own time-stop/4R-
    # TP/trail on E (returns (None,None)); the trader's check_sl_hit is an INDEPENDENT wick/gap
    # resolution that must ALSO be suppressed on E, else a wide-range breakout bar whose own low
    # <= sl_initial would market-close the live position one bar EARLIER than bt-1's E+1 minimum.
    # Donchian leg ONLY (us29 leg's off-by-one is handled in strategy_us29 / its own ratchet); the
    # us29 PM has no is_entry_bar(), so gate on the predicate's presence too. Money-safety is
    # unaffected: the reduce-only SL still rests on the exchange (class guard above), so a genuine
    # gap-through on E is still caught by the exchange — we only suppress the bot's OWN early
    # market-close on E to match bt-1's no-exit-on-E lifecycle.
    _entry_bar_now = False
    if _is_donchian_leg:
        _is_entry_bar_fn = getattr(position_manager, "is_entry_bar", None)
        if callable(_is_entry_bar_fn):
            try:
                _entry_bar_now = bool(_is_entry_bar_fn(pos, df))
            except Exception:
                _entry_bar_now = False
    if _entry_bar_now:
        log.debug(
            "donchian entry-bar %s %s: suppressing check_sl_hit on E (bt-1 has no exit on entry "
            "bar; earliest exit E+1) — exchange reduce-only SL still protects", pos.coin, tf,
        )
        sl_hit = None
    else:
        sl_hit = position_manager.check_sl_hit(pos, df, settings.vstop_wick_check)
    if sl_hit is not None:
        exit_px, reason = sl_hit
        # PHANTOM-WICK GUARD (root: project_hl_wick_sl_exit_records_sl_ref_not_actual_fill).
        # wick_sl is derived from the FORMING bar's Low, which on a thin HIP-3 book captures a
        # single phantom mark spike that never actually traded — a fresh entry self-stops 3s in
        # at a fabricated -1R. Confirm the pierce against the LIVE mark before force-closing: if
        # price is back on the safe side of the SL, the wick was transient → DO NOT market-close
        # (the exchange reduce-only SL still rests and catches a REAL breach; a genuine breach
        # leaves the mark beyond the SL and falls through to close as before).
        # GAP EXTENDED (2026-06-29, invariant-harness audit): gap_through_sl was ALSO spurious on
        # thin HIP-3 — 3 trades (DELL/EWY/INTC) self-closed 2-33s after entry at a price ABOVE the
        # SL (exit > sl ⇒ the level was never really breached) for ~-$590 of fabricated loss. Same
        # transient-pierce class as wick — apply the SAME live-mark confirm. The exchange reduce-only
        # SL still catches a REAL gap; fail-safe: any mark-read failure → close.
        if reason in ("wick_sl", "gap_through_sl") and _wick_is_phantom(client, pos):
            log.warning(
                "%s PHANTOM %s %s: pierce of SL %.6f NOT confirmed by live mark (back inside) — "
                "NOT closing (exchange reduce-only SL rests; real breach still caught next tick)",
                reason, pos.coin, tf, pos.sl_current,
            )
        else:
            log.info("SL hit %s %s: reason=%s exit=%.6f", pos.coin, tf, reason, exit_px)
            _emergency_close(client, pos, exit_px, reason, trade_id, sl_order_id)
            return reason

    # (3) else: honor any non-precedence non-None reason (us29 'max_run_cap' R-cap; inert at
    # MAX_RUN_R=1000). Same relative position vs the SL check as before this audit.
    if exit_reason is not None:
        return _close_on_reason(exit_reason)

    # Re-place the exchange SL whenever the structural trail (or partial-BE) moved
    # pos.sl_current. The PM already ratcheted sl_current, so we compare against the
    # price actually resting on the exchange (_sl_placed_px), with a small threshold
    # to avoid churn on sub-bps moves.
    _trail_side = getattr(pos, "side", "long")
    # Liq-guard the trailed SL BEFORE deciding to re-place: on an isolated thin coin the
    # ratcheted SL can land outside liqPx; add-margin (REMEDY-A) / clamp (REMEDY-B) first,
    # so the value compared + sent is the one that will actually rest inside liquidation.
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
            # Post-invariant: the SL now resting must be inside liq+buffer. If not (e.g.
            # add-margin couldn't and clamp under-shot), don't keep a position whose stop is
            # past liquidation — emergency-close. cancel the just-placed oid first.
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
            if sl_order_id and sl_order_id != new_sl_oid:
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


def _wick_is_phantom(client, pos) -> bool:
    """True when a wick_sl trigger (derived from the FORMING bar's Low) is NOT confirmed by the
    live mark — i.e. price is back on the safe side of the SL, so the bar-low pierce was a
    transient/phantom mark spike (common on thin HIP-3 books) rather than a real breach the
    exchange reduce-only SL would itself have filled. Conservative/fail-safe: returns True ONLY
    when the live mark is unambiguously inside the stop; any read failure → False (act on the
    trigger and close — never suppress a possible real breach on bad data)."""
    try:
        mk = client.mark_price(pos.coin)
    except Exception:
        return False
    if mk is None or float(mk) <= 0:
        return False
    mk = float(mk)
    sl = float(pos.sl_current)
    if getattr(pos, "side", "long") == "long":
        return mk > sl
    return mk < sl


def _emergency_close(client, pos: Position, exit_px: float, reason: str, trade_id, sl_order_id) -> None:
    """Market close + cancel SL/TP + record in DB.

    Close-and-verify BEFORE cancelling the protective SL (2026-05-31 orphan fix):
    market_close() can silently miss (stale cache / transient API / partial), and
    the old order cancelled the SL FIRST then recorded the trade closed
    unconditionally — leaving a naked, untracked position if the close missed.
    The resting SL is reduce-only, so keeping it live during the close can never
    over-close or flip. Only record close once _ensure_flat confirms flat; on a
    residual it has placed a protective SL and we keep the trade DB-open for
    restart recovery.
    """
    if _dry_block(f"_emergency_close({pos.coin} reason={reason})"):
        return
    log.info("EMERGENCY CLOSE %s %s: reason=%s exit=%.6f", pos.coin, pos.tf, reason, exit_px)

    _cancel_tp_limit(client, pos)

    flat = _ensure_flat(client, pos.coin, is_buy_open=(getattr(pos, "side", "long") == "long"))

    if sl_order_id:
        try:
            client.cancel_sl_order(pos.coin, sl_order_id)
        except Exception as e:
            log.warning("cancel_sl_order failed: %s", e)

    if flat:
        # Record the ACTUAL market-close fill, not the trigger ref (exit_px == SL price for
        # wick_sl/gap_through). An emergency MARKET close fills at the live book, often far from
        # the SL ref on a thin HIP-3 wick — recording the SL ref fabricated a -$377 / -1R on a
        # position that really exited flat (-$0.28). Mirror the phantom-guard: read the true VWAP
        # from fills; fall back to the trigger only if the lookup fails. Readback-or-flag invariant.
        _real = _lookup_real_close_px(client, pos, sl_oid=sl_order_id)
        _exit = _real[0] if _real is not None else exit_px
        if _real is not None and abs(_exit - exit_px) / max(abs(exit_px), 1e-9) > 0.01:
            log.warning(
                "EMERGENCY CLOSE %s: recorded REAL fill @%.6f (trigger ref was %.6f, reason=%s) "
                "— DB exit/pnl/R from actual fill, not SL ref", pos.coin, _exit, exit_px, reason,
            )
        _record_close(trade_id, pos, _exit, reason)
    else:
        log.critical(
            "EMERGENCY CLOSE %s left residual after retries — protective SL placed; "
            "trade kept DB-open for restart recovery", pos.coin,
        )


def _detect_partial_fill(pos: Position, exchange_positions: dict, position_manager) -> None:
    """50% fib limit filled (position shrank but still open) → SL→breakeven on the remainder."""
    if getattr(pos, "tp1_partial_done", False):
        return
    orig = pos.__dict__.get("_orig_size", pos.size)
    data = exchange_positions.get(pos.coin) or exchange_positions.get(f"{pos.coin}-USD")
    if not data or orig <= 0:
        return
    try:
        cur_size = abs(float(data.get("szi", 0) or 0))
    except Exception:
        return
    if cur_size <= 0 or cur_size >= orig * 0.9:
        return  # not (yet) partially filled
    pos.tp1_partial_done = True
    pos.size = cur_size  # remainder; reduce-only SL re-placed at this size next
    be = position_manager.apply_partial_be(pos)
    pos.__dict__["_tp_oid"] = None  # 50% limit fully filled — nothing left to cancel
    log.info("PARTIAL TP filled %s %s (%s): size %.4f→%.4f, SL→BE=%.6f",
             pos.coin, pos.tf, getattr(pos, "side", "long"), orig, cur_size,
             be if be is not None else pos.sl_current)
    tid = pos.__dict__.get("_trade_id")
    if tid:
        mark_tp1_partial(tid, pos.tp1_price, cur_size,
                         be if be is not None else pos.sl_current)


def _cancel_orphan_triggers(client, coin: str) -> int:
    """Cancel ALL resting reduce-only stop/trigger orders for `coin` (orphan cleanup).

    Called ONLY when the position is CONFIRMED absent from the exchange (phantom auto-close):
    any reduce-only stop still resting then is by definition an orphan (nothing to reduce).
    Best-effort — never raises; if the order list can't be fetched (e.g. a dex 429) we skip and
    let the bot's orphan_sweep backstop catch it later. Returns the count cancelled.
    """
    n = 0
    try:
        oids = client.list_open_sl_orders(coin) or []
    except Exception as e:
        log.warning("phantom-guard: orphan-trigger list failed for %s (%s) — skip cancel", coin, e)
        return 0
    for oid in oids:
        try:
            client.cancel_sl_order(coin, oid)
            n += 1
            log.warning("phantom-guard: cancelled orphan reduce-only trigger %s for %s (no live position)",
                        oid, coin)
        except Exception as e:
            log.warning("phantom-guard: cancel orphan trigger %s for %s failed: %s", oid, coin, e)
    return n


def _cancel_tp_limit(client, pos: Position) -> None:
    """Cancel the resting 50% fib reduce-only limit (orphan cleanup on close)."""
    tp_oid = pos.__dict__.get("_tp_oid")
    if tp_oid:
        try:
            client.cancel_sl_order(pos.coin, tp_oid)  # generic cancel-by-oid
        except Exception as e:
            log.warning("cancel partial-TP limit %s failed (harmless): %s", pos.coin, e)
        pos.__dict__["_tp_oid"] = None


def _lookup_real_close_px(client, pos, sl_oid=None, tp_oid=None):
    """ROOT FIX (2026-06-23): when a tracked position vanished, find its REAL close from
    exchange fills (SL/TP/liq) so _record_close logs the true exit px/PnL — not a fabricated
    mark@phantom. One position per coin, so the most-recent Close fills summing to pos.size
    ARE this position's close. Returns (exit_px, reason) or None (true phantom: no close fill)."""
    try:
        fills = client.user_fills() or []
    except Exception:
        return None
    api = pos.coin.replace("xyz_", "xyz:") if str(pos.coin).startswith("xyz_") else pos.coin
    cl = [f for f in fills if str(f.get("coin", "")) == api and str(f.get("dir", "")).startswith("Close")]
    if not cl:
        return None
    cl.sort(key=lambda f: int(f.get("time", 0) or 0), reverse=True)
    target = abs(float(pos.size)); acc = 0.0; num = 0.0
    used_oids = set(); saw_liq = False
    for f in cl:
        sz = abs(float(f.get("sz", 0) or 0)); px = float(f.get("px", 0) or 0)
        if sz <= 0 or px <= 0:
            continue
        acc += sz; num += sz * px
        _o = f.get("oid")
        if _o is not None:
            used_oids.add(str(_o))
        if f.get("liquidation"):
            saw_liq = True
        if acc >= target * 0.95:
            break
    if acc <= 0:
        return None
    exit_px = num / acc
    # LABEL FIX (2026-07-01): classify the real fill against the TRAILED stop (sl_current) first,
    # then initial, before the generic fallback. Was keyed on sl_initial -> trailed stop-outs
    # mislabeled generic liq_or_manual. Price/pnl/R unchanged.
    sl_cur = float(getattr(pos, "sl_current", 0) or 0)
    sl_ini = float(getattr(pos, "sl_initial", 0) or 0)
    tp = float(getattr(pos, "tp1_price", 0) or 0)
    def _norm(x):
        return str(x) if x not in (None, "") else None
    slo = _norm(sl_oid) or _norm(getattr(pos, "sl_order_id", None))
    tpo = _norm(tp_oid) or _norm(getattr(pos, "tp1_order_id", None))
    # ROOT FIX 2026-07-01 (feedback_never_touch_bot_trades_myself): attribute the exit by the
    # closing fill's OID vs OUR resting orders FIRST -- authoritative regardless of price. A bot
    # SL/trail on a thin HIP-3 book fills favourably (>1% off the SL ref) which the old price-only
    # matcher dumped to reason='manual'. The operator NEVER closes bot trades, so 'manual' was a
    # lie masking a real bot-SL exit. Price-tolerance is now only a fallback; an unattributable
    # close is flagged 'unknown_investigate' (keep hunting), NEVER 'manual'.
    reason = None
    if saw_liq:
        reason = "liquidation"
    elif slo and slo in used_oids:
        reason = "trail_sl" if (sl_cur > 0 and sl_ini > 0 and abs(sl_cur - sl_ini) / sl_ini > 1e-6) else "sl"
    elif tpo and tpo in used_oids:
        reason = "tp"
    if reason is None:
        if sl_cur > 0 and abs(exit_px - sl_cur) / sl_cur < 0.01:
            reason = "trail_sl"
        elif sl_ini > 0 and abs(exit_px - sl_ini) / sl_ini < 0.01:
            reason = "sl"
        elif tp > 0 and abs(exit_px - tp) / tp < 0.01:
            reason = "tp"
    if reason is None:
        reason = "unknown_investigate"
        log.error("phantom-guard: %s close UNATTRIBUTED to our SL/TP/liq (exit %.6f sl_cur %.6f "
                  "sl_ini %.6f tp %.6f oids %s) -- NOT labeling manual; keep root-causing",
                  pos.coin, exit_px, sl_cur, sl_ini, tp, sorted(used_oids))
    return (exit_px, reason)


def _record_close(trade_id, pos: Position, exit_price: float, exit_reason: str) -> None:
    if trade_id is None:
        return
    if getattr(pos, "side", "long") == "long":
        pnl = (exit_price - pos.entry_price) * pos.size
        sl_dist = pos.entry_price - pos.sl_initial
        realized_r = (exit_price - pos.entry_price) / sl_dist if sl_dist > 0 else 0.0
    else:  # short — profit when exit < entry
        pnl = (pos.entry_price - exit_price) * pos.size
        sl_dist = pos.sl_initial - pos.entry_price
        realized_r = (pos.entry_price - exit_price) / sl_dist if sl_dist > 0 else 0.0
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
