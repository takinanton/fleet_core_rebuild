"""trader.py — entry, fill monitoring, SL placement, position management.

Entry flow (refactor 2026-05-24 — snapshot-based liquidity sizing, no runtime gates):
  1. Bar-age / tier / risk gates (MM cap, concurrent cap)
  2. Size = min(risk-based, leverage-based, LIQ_SIZE_CAP_PCT × snapshot.avg_1h_vol_usd)
  3. If final size < LIQ_MIN_TRADE_USD → skip (economic floor)
  4a. Crypto (donchian, NOT xyz_): immediate market_open at bar close (2026-06-23)
  4b. xyz_ / other legs: STOP-LIMIT order; poll for fill up to entry_limit_ttl_sec
  5. On fill: place stop-loss trigger order (reduceOnly) via Pacifica stop endpoint
  6. If partial fill < min_fill_ratio (10%) → emergency close

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
import time
from typing import Optional

from bot.config import FOREIGN_SKIP_PREFIXES, Settings
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
    update_trade_sl,
    update_trade_sl_order,
    update_trade_tp_order,
)
from bot.liquidity import LiquiditySnapshot, SnapshotHolder
from bot.risk import SizeResult, check_concurrent_cap, check_mm_cap, compute_size
from bot.strategy_xnn import Position, PositionManager, Signal

log = logging.getLogger(__name__)


def _dry_block(what: str) -> bool:
    """XNN port 2026-06-11 (canon §0#8): DRY_RUN must mean ZERO orders, enforced in CODE.

    Before this guard dry_run gated only bar-close entries (main.py CLI flag);
    manage/heal/trail/emergency-close would run against the PacificaClient mock layer
    the moment any position object existed (adopted DB row / manual insert) — producing
    mock fills the live GET endpoints can't see (heal-cycle noise class) and, if the
    client-side DRY env ever diverged from the bot's, REAL orders. Returns True
    (= block) when the GLOBAL settings say dry_run. Loud on every block — a blocked
    order in DRY is always worth seeing in the journal.
    """
    if _global_settings.dry_run:
        log.warning("[DRY-RUN] BLOCKED %s — no order sent", what)
        return True
    return False

# Re-entry cooldown after a post-fill cap-breach / partial-fill abort. The pre-send
# price gate can't stop a market order from filling PAST the cap during the send
# (slippage), so a still-valid signal would re-fire on the very next scan and
# double-fill (root cause of the XRP double market fill 2026-05-30). Block the same
# coin from re-entry for a short window after such an abort. Skip-only — never naked.
_ENTRY_ABORT_COOLDOWN: dict = {}
ENTRY_ABORT_COOLDOWN_SEC = 900.0  # 15 min — covers the next-scan re-fire window

# PHANTOM-GUARD (class fix 2026-06-23, ported from hl_combo_bot xyz_NATGAS incident). A DB-open
# coin can be ABSENT from live open_positions (a legit close the detectors missed, OR a true
# phantom row). The OLD guard kept a 90s timer in pos.__dict__["_first_gone_ts"]; that dict is
# rebuilt each tick by the per-tick adopt/restore path, so the timer reset to None every loop and
# NEVER fired -> "defer to confirm" forever, while the SL-heal re-placed a reduce-only trigger on
# the non-existent position every loop (exchange auto-cancels it -> rate-limit churn + orphan litter).
# Pacifica also had a 'sl_still_active' suppression that could hang FOREVER on an orphaned-resting
# SL (open_positions empty + SL still rests). Fix: a MODULE-LEVEL consecutive-miss counter (keyed
# by coin) that survives pos rebuilds; auto-close + orphan-cancel (cancels that resting orphan SL)
# after K consecutive confirmed-absent ticks; reset to 0 on any live sighting.
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

    # --- Gate: FOREIGN / untracked exchange-position collision (fix 2026-06-11) ---
    # Same netted account as the legacy uk_v102 pacifica-bot-a/b. The checklist used to
    # CLAIM the scan skips coins with foreign exchange positions — it never did (scanner
    # checks only this bot's in-memory dict). A market_open here would NET against
    # (reduce/close) the old bot's position, and the abort path (_ensure_flat →
    # market_close) flattens the WHOLE symbol including foreign size. main.py already
    # filters coins THIS bot tracks before calling us, so any exchange position on
    # signal.coin at this point is foreign (or an own orphan) — both mean: never enter.
    # Skip-only (reject + journal), no orders.
    if FOREIGN_SKIP_PREFIXES and signal.coin.startswith(FOREIGN_SKIP_PREFIXES):
        _reject_and_log(signal, "foreign_prefix_skip: FOREIGN_SKIP_PREFIXES match")
        return None
    if (signal.coin in open_positions_exchange
            or f"{signal.coin}-USD" in open_positions_exchange):
        _reject_and_log(
            signal,
            "foreign_position_collision: exchange holds an untracked position on "
            "this coin (netted account) — refusing to enter",
        )
        return None

    # --- Get asset metadata ---
    try:
        meta = client.asset(signal.coin)
    except KeyError:
        _reject_and_log(signal, f"asset_not_found: {signal.coin}")
        return None

    # --- EFFECTIVE leverage (xnn port 2026-06-11, canon §0#8) ---
    # Before: margin math assumed settings.leverage while the exchange kept each coin's
    # CURRENT account leverage and update_leverage() had ZERO call sites (exchange_pacifica
    # update_leverage existed unused). Now: eff_lev = min(settings.leverage,
    # asset.max_leverage) is (a) actually SET on the exchange before the order and
    # (b) used in sizing cap + MM-cap margin math.
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

    # UNIT FIX (2026-07-02): Pacifica /info min_order_size is USD NOTIONAL (flat "10" for
    # BTC and MON alike; max "5000000" — absurd as units), NOT base-asset units. The ported
    # Extended comparison (size < min) silently blocked EVERY high-priced coin forever
    # (BTC size 0.01188 "< 10" while its notional was $731 — legal). Compare NOTIONAL.
    _entry_px_ref = float(getattr(signal, "trigger_price", 0) or 0)
    _notional_usd = size_result.size * _entry_px_ref
    if _notional_usd < meta.min_size:
        reason = (
            f"below_min_notional: ${_notional_usd:.2f} (size={size_result.size}) < "
            f"venue_min=${meta.min_size} for {signal.coin}"
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

    # --- SET the leverage on the exchange BEFORE any order (xnn port 2026-06-11) ---
    # update_leverage existed (exchange_pacifica.py) but had ZERO call sites — positions
    # opened on whatever leverage the coin happened to carry. Abort the entry if the
    # exchange refuses: the margin model above is only valid once eff_lev is actually set.
    if hasattr(client, "update_leverage"):
        _lev_resp = client.update_leverage(signal.coin, eff_lev)
        _lev_ok = _lev_resp is not None and (
            not isinstance(_lev_resp, dict) or _lev_resp.get("status") in (None, "ok")
        )
        if not _lev_ok:
            _reject_and_log(signal, f"leverage_set_failed: {eff_lev}x resp={_lev_resp}")
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

    # SHORT support 2026-05-27 — side-aware entry
    is_long = (signal.side == "long")

    # --- LEG ROUTING: crypto (native Pacifica perp) vs any other leg ─────────────────
    # Crypto leg: IMMEDIATE market-at-close (2026-06-23). The old stop-limit continuation
    # gate polled for 30s waiting for price to reclaim the trigger; on a 24/7 perp that
    # already closed above the Donchian channel, price had moved on → 0 fills in live
    # testing ("STOP-entry skip: price never reclaimed trigger"). Backtest proves entry at
    # close[i] (market fill) — no gate, no delay, no price check.
    # PRICE-GATE BYPASS for crypto: limit_px (entry × (1+0.25%)) already bounds the fill;
    # the market_open SDK slippage cap enforces the same bound on the exchange; no need to
    # pre-reject based on the mark price — that is the gate that was killing all fills.
    # Any coin without an xyz_ prefix is a native Pacifica crypto perp for this bot.
    _is_crypto_leg = not signal.coin.startswith("xyz_")

    if _is_crypto_leg:
        # ── CRYPTO LEG: immediate market-at-close entry (2026-06-23) ─────────────────
        if _in_entry_cooldown(signal.coin):
            log.info("Entry cooldown active for %s — skipping (recent post-fill abort)", signal.coin)
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
                insert_rejected(coin=signal.coin, tf=signal.tf, trigger_price=signal.trigger_price, entry_price=signal.entry_price, sl_price=signal.sl_price, reason="breakout_invalidated", direction=signal.side)
                return None
        # write-db-row-PRE-order (ported from hl_combo_bot crash-window fix): journal a
        # 'pending' row BEFORE the order is submitted, so a crash/kill between submit and
        # journal leaves a recoverable trace (_reconcile_pending at startup promotes it if
        # a live position exists, else deletes it) instead of a live position with NO db
        # row — invisible to the startup adopt forever. Promoted to 'open' on fill; deleted
        # on every no-fill / abort path below. 'pending' is invisible to open_trades()/adopt.
        pending_id = insert_pending(
            coin=signal.coin, tf=signal.tf, direction=signal.side,
            entry_intended=signal.entry_price, sl_initial=signal.sl_price,
            tp1=signal.tp1_price, size=size_result.size,
            risk_dollars=size_result.risk_dollars, notional=size_result.notional,
        )
        log.info(
            "CRYPTO-MARKET-ENTRY %s %s: close=%.6f sl=%.6f tp1=%.6f size=%s slippage_cap=%.4f",
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
        # ── Non-crypto (xyz_ or other) leg: existing STOP-LIMIT gate ─────────────────
        # Price gate (A) 2026-05-30: don't chase if mark already past cap-limit.
        # Preserved for non-crypto because those legs DO use the stop-limit reclaim path.
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
        # write-db-row-PRE-order (see crypto branch above): recoverable 'pending' trace
        # before the order goes out; promoted on fill, deleted on every no-fill path.
        pending_id = insert_pending(
            coin=signal.coin, tf=signal.tf, direction=signal.side,
            entry_intended=signal.entry_price, sl_initial=signal.sl_price,
            tp1=signal.tp1_price, size=size_result.size,
            risk_dollars=size_result.risk_dollars, notional=size_result.notional,
        )
        fill_result = _place_and_wait_fill(
            client=client,
            coin=signal.coin,
            is_buy=is_long,
            size=size_result.size,
            limit_px=limit_px,
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
            reason="entry_limit_unfilled: TTL expired or emergency close" if not _is_crypto_leg else "crypto_market_fill_failed: no fill or cap-breach",
            direction=signal.side,
        )
        return None

    actual_entry, filled_size = fill_result
    # --- Slip persist (B) 2026-05-30: signed adverse entry slip vs intended (+=worse) ---
    _slip = ((actual_entry - signal.entry_price) / signal.entry_price) if is_long \
        else ((signal.entry_price - actual_entry) / signal.entry_price)

    # --- SL-inside-liquidation guard (2026-06-11, feedback_sl_must_be_inside_liquidation) ---
    # Force-cross happened pre-order (update_leverage is_cross=True). If the venue forced/kept
    # ISOLATED on a thin coin the per-position liqPx can sit inside the SL; ensure_sl_inside_liq
    # adds isolated margin (REMEDY-A, HL only) or clamps the SL (REMEDY-B) so the SL that
    # actually goes to the exchange rests strictly INSIDE liquidation+buffer.
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
    # Post-invariant (rule step 3): SL must rest inside liq+buffer or the position is unsafe
    # → emergency close. REMEDY-B guarantees inside-ness; this catches a still-isolated tight
    # liq where add-margin couldn't and clamp somehow didn't land. Never hold SL past liq.
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
        # VERIFIED close (audit 2026-07-02, donor hl): a bare market_close returns an
        # ok-shaped error dict on failure — _ensure_flat re-reads the position until
        # confirmed flat, retries the close, and places a protective SL on any residual.
        if _ensure_flat(client, signal.coin, is_buy_open=is_long, known_filled_sz=filled_size):
            delete_pending(pending_id)  # position emergency-closed → drop the pre-order row
        else:
            log.critical(
                "EMERGENCY CLOSE/PROTECT FAILED after invariant-fail %s — residual live; "
                "pending row id=%s KEPT (restart reconcile promotes it; untracked-protect "
                "sweep keeps re-protecting) — MANUAL REVIEW",
                signal.coin, pending_id,
            )
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
        # VERIFIED close (audit 2026-07-02, donor hl): the old bare market_close ignored
        # its return — an ok-shaped error dict left a NAKED UNTRACKED position with no db
        # row. _ensure_flat re-reads the position until confirmed flat (bounded retries),
        # retries the close, and places a protective reduce-only SL on any residual.
        if _ensure_flat(client, signal.coin, is_buy_open=is_long, known_filled_sz=filled_size):
            delete_pending(pending_id)  # naked position closed → drop the pre-order row
        else:
            log.critical(
                "EMERGENCY CLOSE/PROTECT FAILED for %s — residual live after SL-fail abort; "
                "pending row id=%s KEPT (restart reconcile promotes it; untracked-protect "
                "sweep keeps re-protecting) — MANUAL INTERVENTION REQUIRED",
                signal.coin, pending_id,
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

    # --- Journal --- write-db-row-PRE-order: the row was written 'pending' BEFORE the order
    # submit (above); PROMOTE it to 'open' now with the ACTUAL fill. On a crash between submit
    # and HERE the pending row + _reconcile_pending(startup) recover the position (adopt heals
    # its SL) instead of leaving a live position with no db row.
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
        sl_initial=signal.sl_price,         # strategy intent (R-distance / max_run_cap math)
        sl_current=sl_to_place,             # SL actually resting on the exchange (liq-guarded)
        tp1_price=signal.tp1_price,
        size=filled_size,
        bar_entry_idx=0,
        side=signal.side,
    )
    pos.__dict__["_trade_id"] = trade_id  # attach db id for updates
    pos.__dict__["_sl_order_id"] = sl_order_id
    pos.__dict__["_orig_size"] = filled_size
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
    if _global_settings.dry_run:
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
                    return statuses[0]["resting"].get("oid")
            except Exception:
                pass
        log.warning("SL placement attempt %d/%d failed for %s: %s",
                    attempt + 1, attempts, coin, resp)
        time.sleep(0.5 * (attempt + 1))
    return None


def _sl_confirmed_live(client, coin: str, sl_order_id) -> bool:
    """True iff a live reduce-only stop order is CONFIRMED protecting `coin`.

    Class guard for "never naked >1 tick". Distinguishes a definitive exchange answer
    from an API error so we are robust in BOTH failure directions:
      - stored oid is None/falsy  -> there is no SL regardless of the API -> NOT live
        (always heal; this is the id=10 / restored-NULL / fresh-adopt case).
      - stored oid present, GET ok -> live iff that oid is among the open reduce-only
        stops for the coin (catches a stale oid: cancelled / expired / triggered).
      - stored oid present, GET errored -> we can't prove it's gone; return True to
        avoid a duplicate-SL storm / spurious emergency-close on a transient blip.
        (A genuinely-dead SL with a flaky API is still caught on the next clean tick.)
    Matching by oid only (not size/side/trigger): trigger_sl always places a reduce-only
    stop on the correct close side, and the trail path resizes the SL to the live size,
    so a present-and-resting reduce-only stop with our oid IS the protective SL. Size/side
    drift is reconciled by the trail re-place, not by tearing down a live stop here.
    """
    if not sl_order_id:
        return False
    try:
        resp = client._signed_request("GET", "/orders", "get_orders", {})
    except Exception as e:
        log.warning("SL-liveness GET threw for %s: %s — assuming live (stored oid=%s)", coin, e, sl_order_id)
        return True
    if not resp.get("success"):
        log.warning("SL-liveness GET not-ok for %s: %s — assuming live (stored oid=%s)",
                    coin, str(resp.get("error", resp))[:120], sl_order_id)
        return True
    for o in resp.get("data") or []:
        if o.get("symbol") != coin:
            continue
        if not o.get("reduce_only"):
            continue  # entry stops are reduce_only=False — never count them as an SL
        if "stop" not in (o.get("order_type") or "").lower():
            continue
        if str(o.get("order_id") or o.get("id")) == str(sl_order_id):
            return True
    return False


def _confirm_sl_live_poll(client, coin: str, sl_order_id, attempts: int = 4) -> bool:
    """Confirm a freshly-placed SL is live, tolerating exchange eventual-consistency.

    A just-created stop can lag in /orders for a second or two (fresh-open lag class).
    Poll a few times before declaring failure so we don't emergency-close a position that
    is in fact protected. Bounded — total wait ~ a few seconds, far inside one 60s tick.
    """
    for i in range(attempts):
        if _sl_confirmed_live(client, coin, sl_order_id):
            return True
        time.sleep(0.75 * (i + 1))
    return False


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
    try:
        tp_sz = _round_down_size(filled_size * frac, meta.sz_decimals)
        # UNIT FIX (2026-07-02): min_order_size is USD notional (see entry gate) — compare
        # the reduce-only TP notional at its limit price, not raw units.
        _tp_px_ref = float(getattr(signal, "tp1_price", 0) or 0)
        if tp_sz * _tp_px_ref < meta.min_size:
            log.info("TP1 partial %s: notional $%.2f (size %.8f) < venue_min $%s — trail-only",
                     signal.coin, tp_sz * _tp_px_ref, tp_sz, meta.min_size)
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


def _ensure_flat(client, coin: str, is_buy_open: bool, known_filled_sz: float = 0.0) -> bool:
    """After an aborted entry (cap breach / partial fill), GUARANTEE we end flat.

    market_close() can miss a just-opened position (stale cache / transient API
    error) and silently leave a naked, untracked orphan. Retry the close; if a
    residual still remains, place a protective reduce-only SL so the position is
    never left naked. Root fix - audit 2026-05-30 (XRP orphan incident).
    """
    if _dry_block(f"_ensure_flat({coin})"):
        return True
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
    except Exception as e:
        # FAIL-CLOSED (audit 2026-07-02): the final verification read failing does NOT
        # prove flat — the old `pos = None` here returned True (confirmed flat) on a
        # broken read, silently declaring an unknown state safe. Report NOT-flat so the
        # caller escalates (CRITICAL + pending row kept / untracked sweep backstop).
        log.error("_ensure_flat %s: final position read FAILED (%s) — cannot confirm flat", coin, e)
        return False
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
    Calls client.market_open() — PacificaClient uses settings.slippage
    (= entry_limit_cap_pct = 0.25%) as the SDK slippage bound; the order fills
    only if the orderbook mid stays within 0.25% of the send price.

    On cap-breach (avg fill > limit_px + 0.1% buffer) or fill_ratio < min_fill_ratio:
    emergency-close via _ensure_flat + register abort cooldown.

    On unparseable response: confirm via open_positions() poll (phantom-fill guard).

    Returns (avg_fill_price, filled_size) or None on no-fill / cap-breach.
    Does NOT call _in_entry_cooldown — caller already checked before invoking this.
    Does NOT place SL — the caller (attempt_entry) places the SL atomically after.
    This is a VAULT bot: never leave a position naked.
    """
    try:
        resp = client.market_open(coin=coin, is_buy=is_buy, sz=size)
    except Exception as e:
        log.error("CRYPTO-MARKET market_open(%s) exception: %s", coin, e)
        return None

    # Parse fill response (HL-shape wrapper produced by PacificaClient._to_filled_resp)
    try:
        statuses = resp["response"]["data"]["statuses"]
        s = statuses[0] if statuses else {}
        if "error" in s:
            log.warning("CRYPTO-MARKET market_open(%s) error in status: %s", coin, s["error"])
            return None
        if "filled" in s:
            avg_px_str = s["filled"].get("avgPx", "0")
            avg_px = float(avg_px_str) if avg_px_str else 0.0
            total_sz_str = s["filled"].get("totalSz", "0")
            filled_sz = float(total_sz_str) if total_sz_str else size

            # Cap-breach check (side-aware): fill must land inside the 0.25% slippage window.
            # Add 0.1% tolerance for exchange rounding on the fill price.
            _breach = (avg_px > limit_px * 1.001) if is_buy else (avg_px < limit_px * 0.999)
            if avg_px > 0 and _breach:
                log.warning(
                    "CRYPTO-MARKET fill %.6f exceeds limit_px %.6f (cap breach) "
                    "for %s — treating as rejected, emergency-close",
                    avg_px, limit_px, coin,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            # Partial-fill check: Pacifica market orders can partially fill on thin books.
            fill_ratio = filled_sz / size if size > 0 else 0.0
            if fill_ratio < min_fill_ratio:
                log.warning(
                    "CRYPTO-MARKET partial fill %s: %.4f/%.4f (%.1f%% < %.0f%%) "
                    "— emergency close",
                    coin, filled_sz, size, fill_ratio * 100, min_fill_ratio * 100,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            actual_px = avg_px if avg_px > 0 else limit_px
            slip_pct = 100.0 * (actual_px - limit_px) / limit_px if limit_px > 0 else 0.0
            log.info(
                "CRYPTO-MARKET fill %s: avg=%.6f sz=%.6f ratio=%.1f%% slip_vs_close_est=%.4f%%",
                coin, actual_px, filled_sz, fill_ratio * 100, slip_pct,
            )
            return actual_px, filled_sz
    except Exception as e:
        log.warning("CRYPTO-MARKET fill parse error for %s: %s — attempting phantom-fill confirm", coin, e)

    # Unparseable response: confirm via open_positions() (phantom-fill prevention).
    # PacificaClient.market_open polls /orders/history up to 5s; if that poll times
    # out it returns an error-wrapped response that lands here. The position may or
    # may not exist — verify before returning None (which would leave a naked position).
    _confirmed_sz = 0.0
    _confirmed_entry_px = 0.0
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
                try:
                    _confirmed_entry_px = float(_p.get("entryPx", 0) or 0)
                except (TypeError, ValueError):
                    _confirmed_entry_px = 0.0
                break
        time.sleep(1.0)
    if _confirmed_sz > 0:
        # NEVER infer "unfilled" from a bad mark read once the position readback CONFIRMED
        # a fill (audit 2026-07-02): mark_price()==0.0 here used to fall through to the
        # UNFILLED branch, leaving a CONFIRMED live position naked and untracked. Price
        # fallback chain: live mark → position entryPx (exchange truth) → limit_px bound.
        mark = client.mark_price(coin)
        px = mark if mark and mark > 0 else (
            _confirmed_entry_px if _confirmed_entry_px > 0 else limit_px)
        if not mark or mark <= 0:
            log.error(
                "CRYPTO-MARKET %s: mark_price unreadable (%.6f) after CONFIRMED fill — "
                "using %s=%.6f (position IS live, never treating as unfilled)",
                coin, mark or 0.0,
                "entryPx" if _confirmed_entry_px > 0 else "limit_px", px,
            )
        log.info(
            "CRYPTO-MARKET fill CONFIRMED via open_positions for %s: szi=%.6f @~%.6f "
            "(order response was unparseable — phantom-fill guard passed)",
            coin, _confirmed_sz, px,
        )
        return px, _confirmed_sz
    log.warning(
        "CRYPTO-MARKET no confirmed position for %s after unparseable response "
        "— treating as UNFILLED (phantom-fill avoided)",
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

    # Phantom-fill fix (xnn port 2026-06-11, canon parity): an UNPARSEABLE response does
    # NOT prove a fill. The old code assumed filled-at-mark here, which could journal a
    # non-existent position + place an SL on nothing (phantom). Instead CONFIRM against
    # the exchange: poll open_positions briefly (the /positions read lags a real fill by
    # seconds, cf. _ensure_flat) and only treat as filled if a position materialises;
    # return the CONFIRMED size. Else return None = unfilled (no phantom).
    _confirmed_sz = 0.0
    _confirmed_entry_px = 0.0
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
                try:
                    _confirmed_entry_px = float(_p.get("entryPx", 0) or 0)
                except (TypeError, ValueError):
                    _confirmed_entry_px = 0.0
                break
        time.sleep(1.0)
    if _confirmed_sz > 0:
        # NEVER infer "unfilled" from a bad mark read once the position readback CONFIRMED
        # a fill (audit 2026-07-02) — a 0.0 mark used to fall through to the UNFILLED
        # branch, leaving the confirmed position naked/untracked. Fallback chain:
        # live mark → position entryPx (exchange truth) → limit_px bound.
        mark = client.mark_price(coin)
        px = mark if mark and mark > 0 else (
            _confirmed_entry_px if _confirmed_entry_px > 0 else limit_px)
        if not mark or mark <= 0:
            log.error(
                "%s: mark_price unreadable (%.6f) after CONFIRMED fill — using %s=%.6f "
                "(position IS live, never treating as unfilled)",
                coin, mark or 0.0,
                "entryPx" if _confirmed_entry_px > 0 else "limit_px", px,
            )
        log.info(
            "Fill CONFIRMED via open_positions for %s: szi=%.6f @~%.6f (order response was unparseable)",
            coin, _confirmed_sz, px,
        )
        return px, _confirmed_sz
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
    # XNN port 2026-06-11 (canon §0#8): the entire manage path (heal SL, trail
    # cancel/replace, partial handling, emergency close) places real orders and has no
    # per-call dry gates — in DRY it must not run at all (positions can only exist here
    # via an adopted DB row, which DRY adopt-skip already prevents; this is the
    # belt-and-braces source guard).
    if _dry_block(f"manage_open_position({pos.coin} {pos.tf})"):
        return None
    trade_id = pos.__dict__.get("_trade_id")
    sl_order_id = pos.__dict__.get("_sl_order_id")


    # ── PHANTOM-GUARD (class fix 2026-06-23, ported from hl_combo_bot xyz_NATGAS): read live
    # presence ONCE up front and branch BEFORE any SL-heal. A coin ABSENT from live open_positions
    # must NEVER be healed (placing a reduce-only SL on a non-existent position rests an orphan the
    # exchange auto-cancels → re-place churn every loop). Resolve the absence deterministically:
    #   • CONFIRMED ABSENT for K consecutive ticks → auto-close stale DB row + cancel orphan
    #     triggers + STOP. Counter is MODULE-LEVEL (survives the per-tick pos rebuild that defeated
    #     the old pos.__dict__ 90s _first_gone_ts guard). This also supersedes the old
    #     'sl_still_active' suppression, which could hang forever on an orphaned-resting SL.
    #   • CONFIRMED PRESENT → reset counter, fall through to normal SL-heal/manage.
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
        # K consecutive confirmed-absent ticks. One FINAL fresh re-read (open_positions is cached,
        # so invalidate first) before closing, so a single eventual-consistency blip can't auto-
        # close a position that is in fact still live.
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
            _real = _lookup_real_close_px(client, pos)
            if _real is not None:
                _cancel_tp_if_any(client, pos)
                _cancel_orphan_triggers(client, pos.coin)
                _record_close(trade_id=trade_id, pos=pos, exit_price=_real[0], exit_reason=_real[1])
                _PHANTOM_MISS.pop(pos.coin, None)
                log.warning("phantom-guard: %s REAL close via fills @%.6f (%s) — recorded TRUE exit, not phantom@mark",
                            pos.coin, _real[0], _real[1])
                return _real[1]
            _cancel_tp_if_any(client, pos)
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
    # expired / triggered-without-close on the exchange, or a trail-replace that stored a
    # dead oid). Confirm against the exchange every tick; if no live reduce-only stop is
    # protecting this position, immediately (re-)place and READ BACK to confirm it is live.
    # If it still can't be confirmed → emergency-close (atomic-SL-or-emergency-close).
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
        # READ BACK: don't trust the place response — confirm a live reduce-only stop exists
        # (poll briefly to tolerate fresh-order propagation lag before any emergency close).
        if sl_order_id is None or not _confirm_sl_live_poll(client, pos.coin, sl_order_id):
            log.error(
                "Position %s %s NAKED — SL re-place/confirm FAILED — emergency closing",
                pos.coin, pos.tf,
            )
            exit_px = client.mark_price(pos.coin) or pos.sl_current
            _emergency_close(client, pos, exit_px, "sl_replace_failed_naked", trade_id, sl_order_id)
            return "sl_replace_failed_naked"
        pos.__dict__["_sl_order_id"] = sl_order_id
        pos.sl_current = heal_sl  # keep tracked SL == on-exchange (liq-guarded) SL
        if trade_id:
            update_trade_sl_order(trade_id, sl_order_id)
        log.info("Healed NAKED %s %s — SL oid=%s confirmed live on exchange", pos.coin, pos.tf, sl_order_id)

    # Partial TP1 fill detection (full-close / phantom now handled UP FRONT by the PHANTOM-GUARD
    # block above, which already read exchange_positions this tick and returns on a confirmed-
    # absent position — reaching here means the position is live: reuse that read, never re-poll).
    if _live_read_ok and _live_present:
        try:
            ex = exchange_positions.get(pos.coin) or exchange_positions.get(f"{pos.coin}-USD")
            if ex is not None and not getattr(pos, "tp1_partial_done", False):
                cur_sz = abs(float(ex.get("szi", 0) or 0))
                orig_sz = pos.__dict__.get("_orig_size", pos.size)
                if cur_sz > 0 and orig_sz > 0 and cur_sz < orig_sz * 0.9:
                    if _handle_tp1_partial(client, pos, cur_sz, settings, trade_id):
                        return "post_partial_sl_failed"
        except Exception as e:
            log.warning("partial-fill check failed for %s: %s", pos.coin, e)

    # SL order id may have changed if a TP1 partial just filled (SL re-placed on remainder).
    sl_order_id = pos.__dict__.get("_sl_order_id")

    # ── exit precedence (v100 port 2026-06-21): mirror bt-1 engine + nado/ext pattern ──
    # bt-1 engine evaluates strategy.maybe_exit() BEFORE trailing-SL wick/gap resolution.
    # uk_v100 PM emits 'death_cross' (and inert 'tp'/'time_stop') from update_sl_on_new_bar.
    # ORDER: (1) update_sl_on_new_bar FIRST (emits exit_reason + stages trail),
    #        (2) if exit_reason in ('death_cross','tp','time_stop') -> STRATEGY EXIT,
    #        (3) else check_sl_hit against pos.sl_current AS-IS (staging PM has already
    #            promoted the correct bar-i resolve level into pos.sl_current),
    #        (4) else max_run_cap (inert at MAX_RUN_R=1000),
    #        (5) else sl_improved trail -> re-place exchange SL.
    _STRAT_EXIT_PRECEDENCE = ("death_cross", "tp", "time_stop")

    # (1) Update trailing SL FIRST — ratchets pos.sl_current; emits strategy exit reason.
    new_sl, exit_reason = position_manager.update_sl_on_new_bar(
        pos=pos,
        df=df,
        enable_trail_after_tp=settings.enable_trail_after_tp,
    )

    def _close_on_reason(_reason: str) -> str:
        """Per-reason exit_px — mirrors nado/ext _close_on_reason."""
        _side = getattr(pos, "side", "long")
        sl_dist_abs = abs(pos.entry_price - pos.sl_initial)
        if _reason == "max_run_cap":
            _exit_px = (pos.entry_price + settings.max_run_r * sl_dist_abs if _side == "long"
                        else pos.entry_price - settings.max_run_r * sl_dist_abs)
        elif _reason == "tp":
            _tp = float(getattr(pos, "tp1_price", 0.0) or 0.0)
            _exit_px = _tp if (_side == "long" and _tp > 0.0) else (
                pos.entry_price + settings.max_run_r * sl_dist_abs if _side == "long"
                else pos.entry_price - settings.max_run_r * sl_dist_abs)
        else:
            # death_cross, time_stop, and any future strategy exit: close at mark price.
            _exit_px = client.mark_price(pos.coin) or pos.sl_current
        _emergency_close(client, pos, _exit_px, _reason, trade_id, sl_order_id)
        return _reason

    # (2) STRATEGY exit (death_cross / tp / time_stop) takes precedence over SL hit.
    if exit_reason in _STRAT_EXIT_PRECEDENCE:
        return _close_on_reason(exit_reason)

    # (3) Check SL hit on latest bar (wick / gap-through) against pos.sl_current AS-IS.
    # The staging PM promotes trail_{i-1} into pos.sl_current only on the first tick of
    # a new bar, so pos.sl_current is the correct bt-1 bar-i resolve level here.
    sl_hit = position_manager.check_sl_hit(pos, df, settings.vstop_wick_check)
    if sl_hit is not None:
        exit_px, reason = sl_hit
        # PHANTOM-WICK GUARD (root: HL xyz_MU 2026-06-26,
        # project_hl_wick_sl_exit_records_sl_ref_not_actual_fill). wick_sl is derived from the
        # FORMING bar's Low; on a thin book a single phantom mark spike that never traded
        # self-stops a fresh position at a fabricated -1R. Confirm the pierce against the LIVE
        # mark before force-closing: if price is back on the safe side of the SL the wick was
        # transient -> do NOT market-close (exchange reduce-only SL still rests and catches a
        # REAL breach). gap_through_sl is Open-based -> left immediate. Fail-safe: mark-read
        # failure -> close.
        if reason == "wick_sl" and _wick_is_phantom(client, pos):
            log.warning(
                "wick_sl PHANTOM %s %s: bar-low pierced SL %.6f but live mark back inside -- "
                "NOT closing (exchange reduce-only SL rests; real breach caught next tick)",
                pos.coin, tf, pos.sl_current,
            )
        else:
            log.info("SL hit %s %s: reason=%s exit=%.6f", pos.coin, tf, reason, exit_px)
            _emergency_close(client, pos, exit_px, reason, trade_id, sl_order_id)
            return reason

    # (4) max_run_cap (inert at MAX_RUN_R=1000; honored after the SL check).
    if exit_reason == "max_run_cap":
        return _close_on_reason("max_run_cap")

    is_long = (getattr(pos, "side", "long") == "long")
    # ROOT FIX 2026-07-01 (bot-deep-audit, project_trail_stop_sl_record_stale): the PM already
    # ratchets pos.sl_current and returns new_sl == that promoted level ONLY when the staged trail
    # advanced this bar (else None). The old `new_sl > pos.sl_current` compared the promoted level
    # against ITSELF -> always False -> the exchange SL trigger never trailed (stuck at sl_initial)
    # and DB sl_current stayed stale. `new_sl is not None` IS the advance signal. Canon: extended/hl.
    sl_improved = new_sl is not None
    if sl_improved:
        _prev_sl = pos.sl_current  # for the old→new log line
        # Liq-guard the trailed SL BEFORE placing: on an isolated thin coin the ratcheted SL
        # can land outside liqPx; add-margin (REMEDY-A, HL only) / clamp (REMEDY-B) first so the
        # value actually sent rests strictly inside liquidation+buffer.
        target_sl, _trail_action = ensure_sl_inside_liq(
            client=client, coin=pos.coin, side=("long" if is_long else "short"),
            sl_px=new_sl, size=pos.size,
        )
        if _trail_action not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
            log.warning("SL trail liq-guard %s %s: action=%s new_sl=%.6f → place=%.6f",
                        pos.coin, tf, _trail_action, new_sl, target_sl)
        # Cancel old SL trigger and place new one
        if sl_order_id:
            client.cancel_sl_order(pos.coin, sl_order_id)

        new_sl_resp = client.trigger_sl(
            coin=pos.coin,
            is_buy=(not is_long),
            sz=pos.size,
            trigger_px=target_sl,
        )
        new_sl_oid = None
        try:
            sts = new_sl_resp["response"]["data"]["statuses"]
            if sts and "resting" in sts[0]:
                new_sl_oid = sts[0]["resting"].get("oid")
        except Exception:
            pass

        # Post-invariant (rule step 3): the SL now resting must be inside liq+buffer. If not
        # (add-margin couldn't and clamp under-shot) don't keep a position whose stop is past
        # liquidation — emergency-close. cancel the just-placed oid first.
        if new_sl_oid is not None and not _sl_inside_liq_ok(client, pos.coin, ("long" if is_long else "short"), target_sl):
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

        pos.__dict__["_sl_order_id"] = new_sl_oid
        pos.sl_current = target_sl  # keep tracked SL == on-exchange SL (may be liq-clamped)
        if trade_id:
            update_trade_sl(trade_id, target_sl)
            if new_sl_oid:
                update_trade_sl_order(trade_id, new_sl_oid)

        log.info("SL updated %s %s: %.6f → %.6f", pos.coin, tf, _prev_sl, target_sl)

    return None


def _wick_is_phantom(client, pos) -> bool:
    """True when a wick_sl trigger (from the FORMING bar's Low) is NOT confirmed by the live
    mark -- price is back on the safe side of the SL, so the bar-low pierce was a transient
    phantom mark spike (thin book) not a real breach the exchange reduce-only SL would fill.
    Fail-safe: returns True ONLY when the live mark is unambiguously inside the stop; any read
    failure -> False (act on the trigger and close -- never suppress a possible real breach)."""
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
    market_close() RETURNS an error-dict (not raises) on SDK failure, so the old
    bare try/except treated a failed close as success, then recorded the trade
    closed unconditionally — leaving a naked, untracked position. _ensure_flat
    re-checks the actual position (robust to the error-return), retries the close,
    and places a protective reduce-only SL on any residual. The resting SL is
    reduce-only, so keeping it live during the close can never over-close/flip.
    Only record close once flat; on a residual keep the trade DB-open for recovery.
    """
    if _dry_block(f"_emergency_close({pos.coin} reason={reason})"):
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
        # Record the ACTUAL market-close fill, not the trigger ref (exit_px == SL price for
        # wick_sl/gap_through). An emergency MARKET close fills at the live book, often far from
        # the SL ref on a thin book -- recording the SL ref fabricates pnl/realized_r on a
        # position that really exited near flat. Mirror the phantom-guard: read the true VWAP
        # from fills; fall back to the trigger only if the lookup fails. Readback-or-flag.
        _real = _lookup_real_close_px(client, pos)
        _exit = _real[0] if _real is not None else exit_px
        if _real is not None and abs(_exit - exit_px) / max(abs(exit_px), 1e-9) > 0.01:
            log.warning(
                "EMERGENCY CLOSE %s: recorded REAL fill @%.6f (trigger ref was %.6f, reason=%s) "
                "-- DB exit/pnl/R from actual fill, not SL ref", pos.coin, _exit, exit_px, reason,
            )
        _record_close(trade_id, pos, _exit, reason)
    else:
        log.critical(
            "EMERGENCY CLOSE %s left residual after retries — protective SL placed; "
            "trade kept DB-open for restart recovery", pos.coin,
        )


def _cancel_orphan_triggers(client, coin: str) -> int:
    """Cancel ALL resting reduce-only stop/trigger orders for `coin` (orphan cleanup).

    Called ONLY when the position is CONFIRMED absent from the exchange (phantom auto-close):
    any reduce-only stop still resting then is by definition an orphan (nothing to reduce).
    Best-effort — never raises; if the list can't be fetched we skip and let the orphan-sweep
    backstop catch it. Returns the count cancelled.
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


def _lookup_real_close_px(client, pos, sl_oid=None, tp_oid=None):
    """ROOT FIX (2026-06-23): when a tracked position vanished, find its REAL close from
    exchange fills (SL/TP/liq) so _record_close logs the true exit px/PnL — not a fabricated
    mark@phantom. One position per coin, so the most-recent Close fills summing to pos.size
    ARE this position's close. Returns (exit_px, reason) or None (true phantom: no close fill).
    SIG-PARITY FIX (2026-07-02): accept sl_oid/tp_oid like HL — the startup restore-resolve call
    site (main.py) passes sl_oid=; the old 2-arg signature raised TypeError there, the blanket
    except kept the stale row open forever (delist-phantom class resurrected). Also ports HL's
    OID-FIRST attribution (2026-07-01): Pacifica fills carry oid=order_id — match the closing
    fills' order ids against OUR resting SL/TP orders before the price-tolerance fallback."""
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
    used_oids = set()
    for f in cl:
        sz = abs(float(f.get("sz", 0) or 0)); px = float(f.get("px", 0) or 0)
        if sz <= 0 or px <= 0:
            continue
        acc += sz; num += sz * px
        _o = f.get("oid")  # adapter maps order_id (trade id only as fallback)
        if _o not in (None, ""):
            used_oids.add(str(_o))
        if acc >= target * 0.95:
            break
    if acc <= 0:
        return None
    exit_px = num / acc
    sl_cur = float(getattr(pos, "sl_current", 0) or 0)
    sl_ini = float(getattr(pos, "sl_initial", 0) or 0)
    tp = float(getattr(pos, "tp1_price", 0) or 0)
    def _norm(x):
        return str(x) if x not in (None, "") else None
    slo = _norm(sl_oid) or _norm(pos.__dict__.get("_sl_order_id"))
    tpo = _norm(tp_oid) or _norm(pos.__dict__.get("_tp1_order_id"))
    reason = None
    if slo and slo in used_oids:
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
        # NO-SILENT-MANUAL 2026-07-01 (feedback_never_touch_bot_trades_myself + no_end_analysis_
        # until_root): operator NEVER closes bot trades, so an unattributable bot-tracked close is
        # NOT "manual" -- flag it and keep root-causing, never a silent human-close label.
        reason = "unknown_investigate"
        log.error("phantom-guard: %s close UNATTRIBUTED to our SL/TP (exit %.6f sl_cur %.6f "
                  "sl_ini %.6f tp %.6f oids %s) -- NOT labeling manual; keep root-causing",
                  pos.coin, exit_px, sl_cur, sl_ini, tp, sorted(used_oids))
    return (exit_px, reason)


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

