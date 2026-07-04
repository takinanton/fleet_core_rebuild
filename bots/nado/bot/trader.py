"""trader.py — STOP-LIMIT entry, fill monitoring, SL placement, position management.

Entry flow (refactor 2026-06-23 — market-at-close for crypto perps):
  1. Bar-age / tier / risk gates (MM cap, concurrent cap)
  2. Size = min(risk-based, leverage-based, LIQ_SIZE_CAP_PCT × snapshot.avg_1h_vol_usd)
  3. If final size < LIQ_MIN_TRADE_USD → skip (economic floor)
  4. Fire ONE market order immediately at bar close (_market_fill_crypto).
     Slippage cap = entry_limit_cap_pct (0.25% default): fill only within that window.
     Cap-breach → emergency close + 15min cooldown. No 30s reclaim wait.
  5. On fill: place stop-loss trigger order (reduceOnly, atomic, never naked)
  6. If partial fill < min_fill_ratio (10%) → emergency close + cooldown
  (Old STOP-LIMIT continuation gate caused 0 fills: "STOP-entry skip: price never
   reclaimed trigger within 30s" — fast breakouts already moved in 24/7 perp market.)

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

from bot.config import Settings
from bot.journal import (
    close_trade,
    delete_pending,
    insert_pending,
    insert_rejected,
    mark_tp1_partial,
    open_trades,
    promote_pending,
    update_trade_sl,
    update_trade_sl_order,
    update_trade_tp_order,
)
from bot.config import settings as _global_settings
from bot.liquidity import LiquiditySnapshot, SnapshotHolder
from bot.risk import SizeResult, check_concurrent_cap, check_mm_cap, compute_size
# XNN port 2026-06-11: strategy module swapped uk_v102 -> xnn (contract-identical).
from bot.strategy_xnn import Position, PositionManager, Signal

log = logging.getLogger(__name__)


def _dry_block(what: str) -> bool:
    """XNN port 2026-06-11 (HL review-fix class): DRY_RUN must mean ZERO orders,
    enforced in CODE.

    Before this guard dry_run gated only bar-close entries (main.py); manage/heal/
    trail/emergency-close placed REAL orders even in DRY the moment any position
    object existed (adopted DB row / manual insert / brief live flip). Returns True
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
# NEVER fired -> "defer to confirm" forever, while the SL-heal re-placed an SL on the non-existent
# position (orphan litter + rate-limit churn). Fix: a MODULE-LEVEL consecutive-miss counter (keyed
# by coin) that survives pos rebuilds; auto-close + orphan-cancel after K consecutive confirmed-
# absent ticks; reset to 0 on any live sighting.
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


def ensure_sl_inside_liq(
    client,
    coin: str,
    side: str,
    sl_px: float,
    size: float,
) -> tuple:
    """Guarantee the stop-loss sits STRICTLY INSIDE the liquidation price.

    User rule 2026-06-11 (feedback_sl_must_be_inside_liquidation): every bot SL must
    be inside liquidation by LIQ_SL_BUFFER_PCT (env, default 0.02), enforced at entry
    AND on every trail update. A SL placed at/beyond liquidation is useless — the
    exchange liquidates first, at a worse fill, with no protective exit having fired.

    Mechanism (returns the SL that should actually go to the exchange):
      (1) FORCE-CROSS: ask the adapter to put the coin on cross margin (cross →
          liquidation is account-level, far away for risk-sized positions). On Nado
          cross is already the per-subaccount default and update_leverage(is_cross=True)
          is a documented no-op stub, so this is best-effort and never fatal.
      (2) READ LIVE liqPx via client.position_liquidation(coin). If the SL is not
          inside liq+buffer:
            REMEDY-A (preferred — "pour a little margin into the isolate"): add
              isolated margin until liqPx clears the SL by the buffer, re-read, repeat
              <=3x. Requires an atomic add/update-isolated-margin primitive on the
              adapter (client.add_isolated_margin / client.update_isolated_margin).
              The Nado SDK exposes NO such primitive (isolated_margin can only be set
              at order-placement time via build_appendix), so on Nado REMEDY-A is
              UNAVAILABLE and we fall straight through to REMEDY-B. See open TODO.
            REMEDY-B (fallback, always available): clamp the SL strictly inside liq
              (long: max(sl, liq*(1+buf)); short: min(sl, liq*(1-buf))) + LOUD WARNING.
      (3) Idempotent: if the SL is already safe, do nothing (no repeated margin top-ups).

    Returns (final_sl_px, action) where action is one of:
      "ok_already_inside" | "no_liq_data" | "cross_only_safe" |
      "remedy_a_added_margin" | "remedy_b_clamped" | "remedy_b_clamped_after_a" |
      "dry_skip".

    Only ever called for OUR OWN entry/trail SLs — never for foreign/manual positions
    (the caller reaches this only on a bot-owned signal/Position). DRY_RUN: log only,
    return the clamped price WITHOUT sending any force-cross / add-margin order so the
    journal still shows what WOULD ship.
    """
    buf = float(getattr(_global_settings, "liq_sl_buffer_pct", 0.02) or 0.02)
    is_long = (side == "long")
    dry = bool(getattr(_global_settings, "dry_run", False))

    def _inside(liq: float, s: float) -> bool:
        # SL strictly inside liq by >= buffer. long: s > liq*(1+buf); short: s < liq*(1-buf).
        if liq is None or liq <= 0:
            return True  # no usable liq fence -> treat as inside (cross account-level)
        return (s > liq * (1.0 + buf)) if is_long else (s < liq * (1.0 - buf))

    def _clamp(liq: float, s: float) -> float:
        if is_long:
            return max(s, liq * (1.0 + buf))
        return min(s, liq * (1.0 - buf))

    def _read_liq():
        try:
            d = client.position_liquidation(coin)
        except Exception as e:  # adapter missing / fetch fail -> no fence
            log.warning("ensure_sl_inside_liq: position_liquidation(%s) raised: %s", coin, e)
            return None
        if not d:
            return None
        try:
            lp = float(d.get("liq_px") or 0.0)
        except (TypeError, ValueError):
            return None
        return lp if lp > 0 else None

    # --- (1) FORCE-CROSS (best-effort; cross => far account-level liq) ---
    if not dry and hasattr(client, "update_leverage"):
        try:
            _lev = int(getattr(_global_settings, "leverage", 5) or 5)
            client.update_leverage(coin, _lev, is_cross=True)
        except TypeError:
            # adapter without is_cross kwarg (e.g. some HL/Extended builds): cross is
            # already the account default there; don't fail the guard over a kwarg.
            try:
                client.update_leverage(coin, _lev)
            except Exception as e:
                log.debug("ensure_sl_inside_liq: force-cross update_leverage(%s) skipped: %s", coin, e)
        except Exception as e:
            log.debug("ensure_sl_inside_liq: force-cross update_leverage(%s) skipped: %s", coin, e)

    # --- (2) READ LIVE liqPx ---
    liq = _read_liq()
    if liq is None:
        # Cross account-level liq with no usable per-position fence (or no position yet)
        # -> nothing to clamp against. SL stands as-is. (Live Nado returns usable liq_px
        # for cross BNB/ONDO, so this branch is the genuine no-data case.)
        return (sl_px, "no_liq_data")

    if _inside(liq, sl_px):
        return (sl_px, "ok_already_inside")

    # SL is NOT inside liq+buffer -> remediate.
    log.warning(
        "SL OUTSIDE LIQ %s (%s): sl=%.8f liqPx=%.8f buf=%.4f size=%.6f — remediating",
        coin, side, sl_px, liq, buf, size,
    )

    # --- REMEDY-A: add isolated margin until liqPx clears SL (<=3 iters) ---
    add_margin_fn = (
        getattr(client, "add_isolated_margin", None)
        or getattr(client, "update_isolated_margin", None)
        or getattr(client, "add_margin", None)
    )
    if add_margin_fn is not None and callable(add_margin_fn):
        for _i in range(3):
            # Margin needed so liq moves past SL+buffer. Distance to close in price terms
            # times size ≈ extra margin (1.3x cushion). Best-effort; re-read confirms.
            if is_long:
                target_liq = sl_px / (1.0 + buf)
                px_gap = max(liq - target_liq, 0.0)
            else:
                target_liq = sl_px / (1.0 - buf)
                px_gap = max(target_liq - liq, 0.0)
            add_usd = px_gap * abs(size) * 1.3
            if add_usd <= 0:
                break
            if dry:
                log.warning(
                    "[DRY-RUN] REMEDY-A would add ~$%.2f isolated margin to %s (iter %d) — skipped",
                    add_usd, coin, _i + 1,
                )
                break
            try:
                add_margin_fn(coin, add_usd)
            except Exception as e:
                log.warning("ensure_sl_inside_liq REMEDY-A add-margin %s failed: %s", coin, e)
                break
            liq = _read_liq()
            if liq is None:
                break
            if _inside(liq, sl_px):
                log.info("REMEDY-A success %s: added margin, liqPx now %.8f (sl=%.8f inside+buf)",
                         coin, liq, sl_px)
                return (sl_px, "remedy_a_added_margin")
        # fell through: add-margin insufficient -> clamp below

    # --- REMEDY-B: clamp SL strictly inside liq + LOUD WARNING ---
    new_sl = _clamp(liq, sl_px)
    new_sl = client.round_price(coin, new_sl) if hasattr(client, "round_price") else new_sl
    action = "remedy_b_clamped_after_a" if (add_margin_fn is not None) else "remedy_b_clamped"
    if dry:
        log.warning(
            "[DRY-RUN] REMEDY-B would clamp SL %s (%s): old=%.8f -> new=%.8f (liqPx=%.8f buf=%.4f) — not sent",
            coin, side, sl_px, new_sl, liq, buf,
        )
        return (new_sl, "dry_skip")
    log.warning(
        "REMEDY-B CLAMP SL %s (%s): old=%.8f -> new=%.8f INSIDE liqPx=%.8f (buf=%.4f) "
        "[add-margin %s]",
        coin, side, sl_px, new_sl, liq, buf,
        "unavailable" if add_margin_fn is None else "insufficient",
    )
    return (new_sl, action)


def _assert_sl_inside_liq_or_close(
    client, coin: str, side: str, final_sl_px: float, size: float,
    pos=None, trade_id=None, sl_order_id=None,
) -> bool:
    """Post-remediation invariant (step 3): final SL MUST be inside liq+buffer, else
    the position is emergency-closed (never leave a position whose SL is past liq).

    Returns True if invariant holds (or no liq fence / DRY), False if it emergency-closed.
    """
    buf = float(getattr(_global_settings, "liq_sl_buffer_pct", 0.02) or 0.02)
    is_long = (side == "long")
    if bool(getattr(_global_settings, "dry_run", False)):
        return True
    try:
        d = client.position_liquidation(coin)
        liq = float(d.get("liq_px") or 0.0) if d else 0.0
    except Exception:
        return True  # can't read fence -> don't force-close on a read failure
    if liq <= 0:
        return True  # cross account-level, no per-position fence
    inside = (final_sl_px > liq * (1.0 + buf)) if is_long else (final_sl_px < liq * (1.0 - buf))
    if inside:
        return True
    log.critical(
        "INVARIANT VIOLATED %s (%s): final SL %.8f STILL outside liqPx %.8f (buf=%.4f) "
        "after remediation — EMERGENCY CLOSING (never hold a position with SL past liq)",
        coin, side, final_sl_px, liq, buf,
    )
    try:
        if pos is not None:
            exit_px = client.mark_price(coin) or final_sl_px
            _emergency_close(client, pos, exit_px, "sl_outside_liq_unfixable", trade_id, sl_order_id)
        else:
            client.market_close(coin)
    except Exception as e:
        log.critical("EMERGENCY CLOSE FAILED %s (SL-outside-liq) — MANUAL: %s", coin, e)
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

    # --- EFFECTIVE leverage (P1b knob-parity port 2026-07-02, canon pacifica §0#8 fix 2026-06-11) ---
    # nado sized off raw .env LEVERAGE even when the asset's max leverage is lower
    # (audit 2026-07-02 divergence #1). eff_lev = min(settings.leverage, asset.max_leverage)
    # is used in the sizing cap + MM-cap margin math. NOTE: no update_leverage-at-entry here —
    # nado is cross-margin per-subaccount default; this is the sizing/MM parity only.
    _meta_max_lev = int(getattr(meta, "max_leverage", 0) or 0)
    eff_lev = min(settings.leverage, _meta_max_lev) if _meta_max_lev > 0 else settings.leverage
    if eff_lev < settings.leverage:
        log.info("%s: asset max_leverage=%dx < LEVERAGE=%dx — using %dx",
                 signal.coin, _meta_max_lev, settings.leverage, eff_lev)

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
            leverage_eff=eff_lev,
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

    # --- Gate: MM cap — measured against LIVE exchange margin, not a model ---
    # existing_margin = REAL account-wide initial margin used by ALL positions (manual/
    # foreign included, each on its own weight/leverage), via the SDK MarginManager.
    # margin_used_usd() RAISES on read failure; we fail CLOSED when positions exist and
    # margin cannot be read — never fall back to the old notional/leverage model (it
    # mis-priced foreign positions: over-counted ~29% on Nado -> false block at real ~10%).
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
        # No positions on the exchange -> no existing margin to account for.
        log.warning("margin_used_usd() failed but no open positions; existing margin = $0: %s", e)
        existing_margin_usd = 0.0
    # MM-cap denominator must be TRUE equity incl uPnL (account_value() returns Nado
    # INITIAL health = funds-available, which understates equity by uPnL + haircut).
    try:
        mm_equity = client.equity_with_upnl()
    except Exception as e:
        log.warning(
            "MM-cap fail-closed: equity_with_upnl() failed; refusing entry for %s: %s",
            signal.coin, e,
        )
        _reject_and_log(signal, f"mm_cap_equity_read_failed: {e}")
        return None
    mm_allowed, mm_reason = check_mm_cap(
        new_notional=size_result.notional,
        eff_lev=eff_lev,
        existing_margin_usd=existing_margin_usd,
        account_value=mm_equity,
        mm_cap_pct=settings.mm_cap_pct,
    )
    if not mm_allowed:
        _reject_and_log(signal, mm_reason)
        return None

    # --- IMMEDIATE MARKET-AT-CLOSE ENTRY (2026-06-23) ---
    # This bot is crypto-only (DONCHIAN_TFS=8h, native Nado perps, no xyz_ leg).
    # The old STOP-LIMIT continuation gate waited 30s for price to reclaim the breakout
    # trigger; on a 24/7 perp that already closed above the Donchian channel, price had
    # already moved by the time the gate polled -> 0 fills, logged as
    # "STOP-entry skip: price never reclaimed trigger within 30s".
    # Fix: fire ONE market order immediately at bar close (same as the HL combo bot's
    # crypto leg, 2026-06-23). The slippage cap (entry_limit_cap_pct = 0.25% default)
    # bounds the fill; a fill worse than that triggers emergency close + 15min cooldown.
    # Price gate removed for crypto: a market-with-slippage-cap already bounds the fill.

    # Limit price = slippage cap ceiling (side-aware); used as cap-breach bound.
    _cap = settings.entry_limit_cap_pct
    if signal.side == "long":
        limit_px = signal.entry_price * (1 + _cap)
    else:
        limit_px = signal.entry_price * (1 - _cap)
    limit_px = client.round_price(signal.coin, limit_px)

    is_long = (signal.side == "long")

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
    log.info(
        "CRYPTO-MARKET-ENTRY %s %s: close=%.6f cap_limit=%.6f sl=%.6f tp1=%.6f size=%s slippage_cap=%.4f",
        signal.coin, signal.tf,
        signal.entry_price, limit_px, signal.sl_price, signal.tp1_price,
        size_result.size, _cap,
    )

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

    # write-db-row-PRE-order (ported from hl_combo_bot, panel must-fix 2026-06-21): journal a
    # 'pending' row BEFORE the order is submitted, so a crash between submit and fill-confirm
    # leaves a recoverable trace (main._reconcile_pending at startup promotes it if a live
    # position exists, else deletes it) instead of a naked untracked position. Promoted to
    # 'open' on fill; deleted on every no-fill / abort path below. 'pending' is invisible to
    # open_trades()/adopt → never affects trading.
    pending_id = insert_pending(
        coin=signal.coin, tf=signal.tf, direction=signal.side,
        entry_intended=signal.entry_price, sl_initial=signal.sl_price,
        tp1=signal.tp1_price, size=size_result.size,
        risk_dollars=size_result.risk_dollars, notional=size_result.notional,
    )

    fill_result = _market_fill_crypto(
        client=client,
        coin=signal.coin,
        is_buy=is_long,
        size=size_result.size,
        limit_px=limit_px,
        min_fill_ratio=settings.min_fill_ratio,
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
    # Orphan fix (2026-05-31): invalidate positions cache after a confirmed open so
    # the next manage_open_position() tick refetches fresh instead of reading the
    # stale PRE-open cache (seeded by the concurrent-cap/anti-dup open_positions()
    # call) and false-recording closed_by_exchange. Mirrors HL orphan root fix.
    client.invalidate_positions_cache()
    # --- Slip persist (B) 2026-05-30: signed adverse entry slip vs intended (+=worse) ---
    _slip = ((actual_entry - signal.entry_price) / signal.entry_price) if is_long \
        else ((signal.entry_price - actual_entry) / signal.entry_price)

    # --- LIQ GUARD (2026-06-11 feedback_sl_must_be_inside_liquidation): the SL that
    # actually ships MUST be strictly inside liquidation + LIQ_SL_BUFFER_PCT. Position
    # is live now (post-fill) so liqPx is readable. Force-cross + (REMEDY-A add-margin |
    # REMEDY-B clamp). entry_sl = what goes to the exchange / journal / Position. ---
    entry_sl, _liq_action = ensure_sl_inside_liq(
        client=client, coin=signal.coin, side=signal.side,
        sl_px=signal.sl_price, size=filled_size,
    )
    if _liq_action not in ("ok_already_inside", "no_liq_data"):
        log.info("ENTRY liq-guard %s %s: sl %.6f -> %.6f (%s)",
                 signal.coin, signal.tf, signal.sl_price, entry_sl, _liq_action)

    # --- Place stop-loss trigger (retry 3x; emergency close if all fail) ---
    # SHORT: SL ABOVE entry → trigger BUYS to close (is_buy=True via side=short).
    sl_order_id = _place_sl_with_retry(
        client=client,
        coin=signal.coin,
        size=filled_size,
        sl_price=entry_sl,
        side=signal.side,
    )
    if sl_order_id is None:
        log.error(
            "SL placement FAILED 3x for %s %s — emergency closing naked position",
            signal.coin, signal.tf,
        )
        # Verified close (donor hl _ensure_flat, orphan root fix 2026-05-31): the old bare
        # market_close() trusted an ok-shaped dict — a silently-missed close left a naked,
        # UNTRACKED position. _ensure_flat confirms flat via position readback, retries the
        # close, and places a protective reduce-only SL on any residual.
        if not _ensure_flat(client, signal.coin, is_buy_open=is_long,
                            known_filled_sz=filled_size):
            log.critical(
                "EMERGENCY CLOSE/PROTECT FAILED for %s — MANUAL INTERVENTION REQUIRED",
                signal.coin,
            )
        delete_pending(pending_id)  # naked position closed/protected → drop the pre-order row
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
    # and HERE the pending row + main._reconcile_pending(startup) recover the position (adopt
    # heals its SL) instead of leaving it naked and untracked.
    promote_pending(
        pending_id,
        entry=actual_entry,
        size=filled_size,
        risk_dollars=size_result.risk_dollars,
        notional=filled_size * actual_entry,
        walk_slip_pct=_slip,
        sl_initial=entry_sl,
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
        sl_initial=entry_sl,
        sl_current=entry_sl,
        tp1_price=signal.tp1_price,
        size=filled_size,
        bar_entry_idx=0,
        side=signal.side,
    )
    pos.__dict__["_trade_id"] = trade_id  # attach db id for updates
    pos.__dict__["_sl_order_id"] = sl_order_id

    # --- INVARIANT (step 3): SL now resting on exchange MUST be inside liq+buffer,
    # else emergency-close (never hold a position whose SL is past liquidation). ---
    _assert_sl_inside_liq_or_close(
        client, signal.coin, signal.side, entry_sl, filled_size,
        pos=pos, trade_id=trade_id, sl_order_id=sl_order_id,
    )
    pos.__dict__["_orig_size"] = filled_size
    pos.__dict__["_tp1_frac"] = settings.tp1_partial_frac

    # --- Place tp1_partial_frac reduce-only LIMIT @ TP1 (1.618R), maker — cheaper than a
    # market TP. BEST-EFFORT on top of the already-resting SL: a failure = trail-only, never naked.
    tp1_order_id = _place_tp1_partial(client, signal, filled_size, settings, meta)
    pos.__dict__["_tp1_order_id"] = tp1_order_id
    if tp1_order_id is not None and trade_id:
        update_trade_tp_order(trade_id, str(tp1_order_id))

    return pos


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
                    oid = statuses[0]["resting"].get("oid")
                    # CLASS GUARD (2026-06-07): independent trader-level read-back.
                    # The exchange layer already confirms via _confirm_trigger_live, but
                    # we re-verify here so a dead/naked SL is impossible even if a future
                    # exchange-wrapper change regresses. An SL counts as placed ONLY
                    # after the live trigger service shows an active order on this coin.
                    if _sl_live_on_exchange(client, coin, oid):
                        return oid
                    log.warning(
                        "SL attempt %d/%d %s: response said resting (oid=%s) but read-back "
                        "found NO live trigger — retrying (not accepting as placed)",
                        attempt + 1, attempts, coin, oid,
                    )
            except Exception:
                pass
        log.warning("SL placement attempt %d/%d failed for %s: %s",
                    attempt + 1, attempts, coin, resp)
        time.sleep(0.5 * (attempt + 1))
    return None


def _sl_live_on_exchange(client, coin: str, oid, attempts: int = 3) -> bool:
    """True iff an active stop/trigger order exists on `coin` (read back from the
    exchange). Matches the returned oid when available, else accepts any live
    trigger on the coin (covers exchanges whose oid representation differs between
    the place-response and the open-orders query). Read-back retried briefly to
    absorb indexer propagation lag.

    XNN port 2026-06-11 (exception-swallow class fix): list_open_sl_orders now RAISES
    on a network/API failure (was: silent []). A failed read-back is INDETERMINATE,
    not "no SL" — treating it as missing caused the false-naked → duplicate-SL /
    spurious-emergency-close class. If every attempt raised, ASSUME LIVE: this path
    only runs after the exchange-level _confirm_trigger_live already confirmed the
    trigger (exchange_nado.trigger_sl), so assume-live is the anti-duplicate
    fail-closed direction; a genuinely dead SL is healed on the next clean tick."""
    _all_raised = True
    for i in range(attempts):
        try:
            live = client.list_open_sl_orders(coin) or []
            _all_raised = False
        except Exception as e:
            log.warning("_sl_live_on_exchange(%s) read-back error: %s", coin, e)
            time.sleep(0.4 * (i + 1))
            continue
        if live:
            if oid is None or str(oid) in {str(x) for x in live}:
                return True
            # oid mismatch but a live trigger exists on the coin — still protected.
            return True
        time.sleep(0.4 * (i + 1))
    if _all_raised:
        log.error(
            "_sl_live_on_exchange(%s): ALL %d read-backs raised — INDETERMINATE; "
            "assuming SL live (exchange-level confirm already passed) to avoid a "
            "duplicate-SL storm", coin, attempts,
        )
        return True
    return False


def _sl_confirmed_live(client, coin: str, sl_order_id) -> bool:
    """True iff the STORED SL oid is CONFIRMED live on the exchange trigger service.

    Class guard (ported from pacifica/hl 2026-07-02): "never naked >1 tick". Not enough
    to check `sl_order_id is None` — the stored oid can be STALE (SL cancelled / expired /
    died on the exchange after a trail-replace). Matching must be OID-EXACT on Nado: the
    partial-TP is ALSO a reduce-only trigger on the same coin, so "any live trigger"
    (the _sl_live_on_exchange placement read-back) would mask a dead SL behind a live TP.
    Failure directions:
      - stored oid None/falsy            -> NOT live (heal).
      - read ok, oid among live triggers -> live.
      - read ok, oid absent              -> NOT live (stale oid -> heal).
      - read RAISED (list_open_sl_orders fails loud) -> INDETERMINATE -> assume live
        (anti duplicate-SL/emergency-close churn; a truly dead SL is caught on the
        next clean tick)."""
    if not sl_order_id:
        return False
    try:
        live = client.list_open_sl_orders(coin) or []
    except Exception as e:
        log.warning("SL-liveness read-back threw for %s: %s — INDETERMINATE, assuming live "
                    "(stored oid=%s)", coin, e, sl_order_id)
        return True
    return str(sl_order_id) in {str(x) for x in live}


def _confirm_sl_live_poll(client, coin: str, sl_order_id, attempts: int = 4) -> bool:
    """Confirm a freshly-placed SL is live, tolerating exchange eventual-consistency.

    A just-created trigger can lag the trigger-service listing by a second or two
    (fresh-open lag class). Poll a few times before declaring failure so we don't
    emergency-close a position that is in fact protected. Bounded — total wait a few
    seconds, far inside one 60s tick."""
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
    if _dry_block(f"_place_tp1_partial({signal.coin})"):
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
    if _dry_block(f"_cancel_tp_if_any({pos.coin} oid={tp_oid})"):
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
) -> "Optional[tuple[float, float]]":
    """Immediate market entry for the Donchian/crypto leg (2026-06-23).

    Replaces the STOP-LIMIT continuation gate (which waited 30s for price to
    reclaim the breakout trigger — 0 fills on fast 24/7 perp moves).

    Flow:
      1. client.market_open fires ONE market order at bar close.
         Nado SDK slippage param (settings.slippage = entry_limit_cap_pct = 0.25%)
         bounds the fill: only executes if orderbook mid is within 0.25%.
      2. Response parsed from HL-shape wrapper (_wrap_open_resp returns avgPx=mark).
      3. Cap-breach: fill > limit_px * 1.001 (long) or < limit_px * 0.999 (short)
         -> emergency close + 15min cooldown.
      4. Partial fill < min_fill_ratio -> emergency close + cooldown.
      5. Unparseable response -> confirm via open_positions (phantom-fill guard).

    Nado-specific (2026-07-02): _wrap_open_resp now reads back the REAL fill from the
    per-product matches feed (VWAP avgPx, cum totalSz). If the readback is unavailable
    it returns an 'unconfirmed' status (no 'filled' key) — that falls through to the
    open_positions confirm-poll below (step 5), never fabricated fill numbers.

    Returns (avg_fill_price, filled_size) or None on no-fill / cap-breach.
    Does NOT check _in_entry_cooldown — caller already checked.
    """
    try:
        resp = client.market_open(coin=coin, is_buy=is_buy, sz=size)
    except Exception as e:
        log.error("CRYPTO-MARKET market_open(%s) exception: %s", coin, e)
        return None

    # Parse HL-shape fill response (_wrap_open_resp contract)
    try:
        statuses = resp["response"]["data"]["statuses"]
        s = statuses[0] if statuses else {}
        if "error" in s:
            log.warning("CRYPTO-MARKET market_open(%s) error status: %s", coin, s["error"])
            return None
        if "filled" in s:
            avg_px_str = s["filled"].get("avgPx", "0")
            avg_px = float(avg_px_str) if avg_px_str else 0.0
            total_sz_str = s["filled"].get("totalSz", "0")
            filled_sz = float(total_sz_str) if total_sz_str else size

            # Cap-breach: fill landed outside the slippage window
            _breach = (avg_px > limit_px * 1.001) if is_buy else (avg_px < limit_px * 0.999)
            if avg_px > 0 and _breach:
                log.warning(
                    "CRYPTO-MARKET cap-breach %s: fill=%.6f exceeds cap_limit=%.6f (%s) "
                    "— emergency close + 15min cooldown",
                    coin, avg_px, limit_px, "long" if is_buy else "short",
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            # Partial fill check
            fill_ratio = filled_sz / size if size > 0 else 0.0
            if fill_ratio < min_fill_ratio:
                log.warning(
                    "CRYPTO-MARKET partial fill %s: %.6f/%.6f (%.1f%% < %.0f%%) "
                    "— emergency close + cooldown",
                    coin, filled_sz, size, fill_ratio * 100, min_fill_ratio * 100,
                )
                _register_entry_abort(coin)
                _ensure_flat(client, coin, is_buy, known_filled_sz=filled_sz)
                return None

            actual_px = avg_px if avg_px > 0 else limit_px
            log.info(
                "CRYPTO-MARKET fill %s: avg_px=%.6f filled_sz=%.6f cap_limit=%.6f",
                coin, actual_px, filled_sz, limit_px,
            )
            return actual_px, filled_sz
    except Exception as e:
        log.warning("CRYPTO-MARKET fill parse error for %s: %s", coin, e)

    # Unparseable response: confirm via open_positions (phantom-fill guard).
    # Nado /positions read can lag the fill by a few seconds; poll up to 4s.
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
                "CRYPTO-MARKET fill CONFIRMED via open_positions %s: szi=%.6f @~%.6f "
                "(order response was unparseable — phantom-fill avoided)",
                coin, _confirmed_sz, mark,
            )
            return mark, _confirmed_sz
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
    # XNN port 2026-06-11: the WHOLE manage path emits orders (heal trigger_sl, trail
    # cancel+place, emergency market_close) with no per-call dry gates of its own.
    # In DRY no position should ever reach here (adopt/reconcile skipped, entries
    # gated) — if one does, block loudly instead of trading.
    if _dry_block(f"manage_open_position({pos.coin} {pos.tf})"):
        return None
    trade_id = pos.__dict__.get("_trade_id")
    sl_order_id = pos.__dict__.get("_sl_order_id")

    # UK-SIG pending-entry guard: placeholder position (resting entry, no fill yet)
    # blocks scanner but should not attempt SL-heal or close until fill arrives.
    if pos.__dict__.get("_pending_entry"):
        log.debug("manage_open_position: %s is pending entry — skip manage", pos.coin)
        return None


    # ── PHANTOM-GUARD (class fix 2026-06-23, ported from hl_combo_bot xyz_NATGAS): read live
    # presence ONCE up front and branch BEFORE any SL-heal. A coin ABSENT from live open_positions
    # must NEVER be healed (placing an SL on a non-existent position rests an orphan → churn).
    # Resolve the absence deterministically instead of deferring forever:
    #   • CONFIRMED ABSENT for K consecutive ticks → auto-close stale DB row + cancel orphan
    #     triggers + STOP. Counter is MODULE-LEVEL (survives the per-tick pos rebuild that defeated
    #     the old pos.__dict__ 90s _first_gone_ts guard).
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
                log.warning("phantom-guard: %s REAL close via matches @%.6f (%s) — recorded TRUE exit, not phantom@mark",
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

    # --- CLASS GUARD (2026-07-02, ported from pacifica/hl): a position must NEVER be without
    # a CONFIRMED-LIVE SL for >1 tick. The old `sl_order_id is None` check missed a STALE oid
    # (SL trigger cancelled / expired / died ON the exchange) — such a position stayed naked
    # FOREVER. Confirm the stored oid against the live trigger service every manage tick; if
    # it is not live, (re-)place and READ BACK; if that still fails → emergency-close
    # (atomic-SL-or-emergency-close). Read-back failure = INDETERMINATE → assume live.
    if not _sl_confirmed_live(client, pos.coin, sl_order_id):
        # Size the heal-SL to the LIVE position when readable (reuse this tick's presence
        # read — exchange_positions is {} on an UNKNOWN read → fall back to tracked size;
        # a reduce-only trigger is clamped to position size on the exchange, so the
        # fallback can never over-close).
        heal_size = pos.size
        try:
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
            "Position %s %s (%s) has NO confirmed-live SL (stored oid=%s) — re-placing "
            "sz=%s at sl=%.6f (liq-guard=%s)",
            pos.coin, pos.tf, _heal_side, sl_order_id, heal_size, heal_sl, _heal_action,
        )
        sl_order_id = _place_sl_with_retry(
            client=client,
            coin=pos.coin,
            size=heal_size,
            sl_price=heal_sl,
            side=_heal_side,
        )
        # READ BACK: don't trust the place response — confirm the live trigger exists
        # (poll briefly to absorb fresh-order propagation lag before any emergency close).
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
            update_trade_sl(trade_id, heal_sl)
            update_trade_sl_order(trade_id, sl_order_id)
        log.info("Healed NAKED %s %s — SL oid=%s confirmed live on exchange",
                 pos.coin, pos.tf, sl_order_id)

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

    # ── exit precedence (v10d port 2026-06-21): mirror bt-1 engine + combo bot ──
    # bt-1 engine evaluates strategy.maybe_exit() (time_stop / tp) BEFORE trailing-SL
    # wick/gap resolution. The donchian PM emits 'time_stop' and 'tp' (TP_R_MULTIPLE=999
    # so 'tp' never fires in practice, but wired for contract completeness).
    # ORDER: (1) update_sl_on_new_bar FIRST (emits exit_reason + stages trail),
    #        (2) if exit_reason in ('tp','time_stop') -> STRATEGY EXIT (precedence),
    #        (3) else check_sl_hit against pos.sl_current AS-IS (the staging PM has
    #            already promoted the correct bar-i resolve level into pos.sl_current),
    #        (4) else max_run_cap (inert at MAX_RUN_R=1000),
    #        (5) else sl_improved trail -> re-place exchange SL.
    _STRAT_EXIT_PRECEDENCE = ("tp", "time_stop")

    # (1) Update trailing SL FIRST — ratchets pos.sl_current; emits strategy exit reason.
    new_sl, exit_reason = position_manager.update_sl_on_new_bar(
        pos=pos,
        df=df,
        enable_trail_after_tp=settings.enable_trail_after_tp,
    )

    def _close_on_reason(_reason: str) -> str:
        """Per-reason exit_px (mirrors combo bot _close_on_reason)."""
        _side = getattr(pos, "side", "long")
        sl_dist_abs = abs(pos.entry_price - pos.sl_initial)
        if _reason == "max_run_cap":
            _exit_px = (pos.entry_price + settings.max_run_r * sl_dist_abs if _side == "long"
                        else pos.entry_price - settings.max_run_r * sl_dist_abs)
        elif _reason == "tp":
            # 4R take: use tp1_price (= entry + TP_R_MULTIPLE*sl_dist).
            _tp = float(getattr(pos, "tp1_price", 0.0) or 0.0)
            _exit_px = _tp if (_side == "long" and _tp > 0.0) else (
                pos.entry_price + settings.max_run_r * sl_dist_abs if _side == "long"
                else pos.entry_price - settings.max_run_r * sl_dist_abs)
        else:
            # time_stop and any future strategy exit: close at mark price.
            _exit_px = client.mark_price(pos.coin) or pos.sl_current
        _emergency_close(client, pos, _exit_px, _reason, trade_id, sl_order_id)
        return _reason

    # (2) STRATEGY exit ('tp' / 'time_stop') takes precedence over trailing-SL hit.
    if exit_reason in _STRAT_EXIT_PRECEDENCE:
        return _close_on_reason(exit_reason)

    # (3) Check SL hit on latest bar (wick / gap-through) against pos.sl_current AS-IS.
    # The donchian staging PM (strategy_donchian_v10d) promotes trail_{i-1} into
    # pos.sl_current ONLY on the first tick of a new bar, so pos.sl_current is already
    # bt-1's bar-i resolve level for every tick of bar i — no pre/post-ratchet swap needed.
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
    # ROOT FIX 2026-07-01 (bot-deep-audit, project_trail_stop_sl_record_stale): the PM
    # (strategy_xnn.update_sl_on_new_bar) ALREADY ratchets pos.sl_current and returns
    # new_sl == that promoted level ONLY on a bar where the staged trail actually advanced
    # (else new_sl is None). The old guard `new_sl > pos.sl_current` compared the promoted
    # level against ITSELF -> always False -> the exchange SL trigger NEVER trailed (proven
    # live: all 8 open positions rested at sl_initial) and DB sl_current stayed stale,
    # defeating the invariant harness. `new_sl is not None` IS the advance signal. Mirrors
    # the canon fix already live in extended-bot / hl_combo_bot.
    sl_improved = new_sl is not None
    if sl_improved:
        # --- LIQ GUARD on the trailed SL (2026-06-11): the SL that ships on every
        # trail update must also be strictly inside liquidation + buffer. Clamp only
        # ever moves it in the protective direction, so it stays a valid trail. ---
        trail_sl, _liq_action = ensure_sl_inside_liq(
            client=client, coin=pos.coin, side=getattr(pos, "side", "long"),
            sl_px=new_sl, size=pos.size,
        )
        if _liq_action not in ("ok_already_inside", "no_liq_data"):
            log.info("TRAIL liq-guard %s %s: sl %.6f -> %.6f (%s)",
                     pos.coin, tf, new_sl, trail_sl, _liq_action)

        # PLACE-BEFORE-CANCEL (2026-07-01): place the new reduce-only trigger FIRST; cancel
        # the old only after the new one rests. If placement fails the OLD SL stays intact ->
        # never naked (the previous cancel-then-place opened a brief naked window every step).
        new_sl_resp = client.trigger_sl(
            coin=pos.coin,
            is_buy=(not is_long),
            sz=pos.size,
            trigger_px=trail_sl,
        )
        new_sl_oid = None
        try:
            sts = new_sl_resp["response"]["data"]["statuses"]
            if sts and "resting" in sts[0]:
                new_sl_oid = sts[0]["resting"].get("oid")
        except Exception:
            pass

        if new_sl_oid is not None:
            if sl_order_id and str(sl_order_id) != str(new_sl_oid):
                try:
                    client.cancel_sl_order(pos.coin, sl_order_id)
                except Exception as e:
                    log.warning("cancel old SL %s failed (harmless, reduce-only): %s", pos.coin, e)
            pos.__dict__["_sl_order_id"] = new_sl_oid
            pos.sl_current = trail_sl  # keep tracked SL == on-exchange SL (may be liq-clamped)
            if trade_id:
                update_trade_sl(trade_id, trail_sl)
                update_trade_sl_order(trade_id, new_sl_oid)
            log.info("SL updated %s %s: %.6f → %.6f", pos.coin, tf, pos.sl_current, trail_sl)
        else:
            log.warning("SL trail re-place FAILED %s %s -- keeping existing SL (not naked)", pos.coin, tf)

        # INVARIANT (step 3): trailed SL on exchange MUST be inside liq+buffer.
        _assert_sl_inside_liq_or_close(
            client, pos.coin, getattr(pos, "side", "long"), trail_sl, pos.size,
            pos=pos, trade_id=trade_id, sl_order_id=new_sl_oid,
        )

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
    market_close() can silently miss a position (stale cache / transient API /
    error-return), and the old order cancelled the SL FIRST then recorded the
    trade closed unconditionally — leaving a naked, untracked position. _ensure_flat
    re-checks the actual position, retries the close, and places a protective
    reduce-only SL on any residual. The resting SL is reduce-only, so keeping it
    live during the close can never over-close/flip. Only record close once flat;
    on a residual keep the trade DB-open for restart recovery.
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
    """Nado ROOT FIX (2026-06-23): user_fills() is empty on Nado (SDK limit), so find the
    REAL close from per-product matches (like compute_realized_pnl): newest closing-side
    matches summing to pos.size = this position's close. Returns (exit_px, reason) or None.
    SIG-PARITY FIX (2026-07-02): accept sl_oid/tp_oid like HL — the startup restore-resolve call
    site (main.py) passes sl_oid=; the old 2-arg signature raised TypeError there, the blanket
    except kept the stale row open forever (delist-phantom class resurrected).
    OID-FIRST (2026-07-02): IndexerMatch.digest of a FIRED trigger-SL equals the digest we store
    as sl_order_id (proven live: WLFI-PERP trail_sl, match 65624651) — attribute the exit by the
    closing fills' digests vs OUR SL/TP orders first; price tolerance is only the fallback."""
    try:
        coin = pos.coin
        pid = client._pid(coin) if hasattr(client, "_pid") else None
        if pid is None and hasattr(client, "_symbol_to_pid"):
            pid = client._symbol_to_pid.get(coin)
        if pid is None:
            return None
        fills = client._fetch_matches_for_product(pid, limit=200) or []
    except Exception:
        return None
    side = getattr(pos, "side", "long")
    closing_side = "sell" if side == "long" else "buy"
    fs = sorted((f for f in fills if (f.get("symbol") == coin or f.get("coin") == coin)),
                key=lambda f: int(f.get("submission_idx", 0) or 0), reverse=True)
    target = max(abs(float(pos.size)) * 0.95, 1e-4)
    cum = 0.0; num = 0.0
    used_oids = set()
    for f in fs:
        if f.get("side") != closing_side:
            continue
        amt = float(f.get("amount", 0) or 0); px = float(f.get("price", 0) or 0)
        if amt <= 0 or px <= 0:
            continue
        cum += amt; num += amt * px
        _d = f.get("digest")
        if _d not in (None, ""):
            used_oids.add(str(_d).lower())
        if cum >= target:
            break
    if cum <= 0:
        return None
    exit_px = num / cum
    tp = float(getattr(pos, "tp1_price", 0) or 0)
    reason = None
    # LABEL FIX (2026-07-01): classify the real VWAP fill against the TRAILED stop before the
    # generic liq/manual fallback. sl_current = promoted (trailed) level, sl_initial = original.
    sl_cur = float(getattr(pos, "sl_current", 0) or 0)
    sl_ini = float(getattr(pos, "sl_initial", 0) or 0)
    def _norm(x):
        return str(x).lower() if x not in (None, "") else None
    slo = _norm(sl_oid) or _norm(pos.__dict__.get("_sl_order_id"))
    tpo = _norm(tp_oid) or _norm(pos.__dict__.get("_tp1_order_id"))
    if slo and slo in used_oids:
        reason = "trail_sl" if (sl_cur > 0 and sl_ini > 0 and abs(sl_cur - sl_ini) / sl_ini > 1e-6) else "sl"
    elif tpo and tpo in used_oids:
        reason = "tp"
    elif tp > 0 and abs(exit_px - tp) / tp < 0.01:
        reason = "tp"
    elif sl_cur > 0 and abs(exit_px - sl_cur) / sl_cur < 0.01:
        reason = "trail_sl"
    elif sl_ini > 0 and abs(exit_px - sl_ini) / sl_ini < 0.01:
        reason = "sl"
    if reason is None:
        # NO-SILENT-MANUAL 2026-07-01 (feedback_never_touch_bot_trades_myself + no_end_analysis_
        # until_root): operator NEVER closes bot trades, so an unattributable bot-tracked close is
        # NOT "liq_or_manual" -- flag it and keep root-causing, never a silent human-close label.
        reason = "unknown_investigate"
        log.error("phantom-guard: %s close UNATTRIBUTED to our SL/TP (exit %.6f sl_cur %.6f "
                  "sl_ini %.6f tp %.6f digests %s) -- NOT labeling liq_or_manual; keep root-causing",
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
