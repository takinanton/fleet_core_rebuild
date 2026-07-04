"""resting_orders.py — pre-placed resting stop-limit entry orders.

DESIGN (step 2):
  Each main loop tick (not only on new_bar_closed), for every eligible
  (coin, tf, side) we:
    1. Compute the FORMING trigger from current candles (same window used in
       scan_for_signal / scan_for_short_signal: rolling max/min of last
       donchian_k bars, +/- tick).
    2. Skip if price is ALREADY past the trigger (intra-bar breakout complete;
       resting order would be immediately marketable — let existing bar-close
       scan handle it next bar, or it fills intra-bar if already placed).
    3. If no resting order exists → place a stop-limit (trigger=forming_trigger,
       limit=trigger*(1+cap) long / *(1-cap) short). NOT reduce_only.
    4. If the forming trigger moved (rolling-max raised for long, lowered for
       short) → cancel old and place at new level.
    5. Cancel the resting order if:
         - a position opens on that coin (any bot on the shared account), OR
         - the coin leaves the universe, OR
         - the setup invalidates (trend reverses: close < ema50 for long, > ema50 for short), OR
         - max_concurrent reached.
    6. On fill: the exchange fires the trigger → position appears in
       open_positions() on the next loop tick → manage_open_position() handles
       the SL / journal path exactly as for the bar-close entry.
       BUT: we also need to detect the fill ourselves and invoke the post-fill
       SL+journal path immediately (same bar). We do this by polling
       open_positions() after placing/refreshing resting orders; if a coin
       that had a resting order now shows a position with no recorded Position
       object → call attempt_entry_post_fill() to place SL + journal.

EXCHANGE WIRE FORMAT (HL):
  Resting stop-limit entry = non-reduce-only trigger order:
    order_type = {"trigger": {"triggerPx": trigger_px, "isMarket": False, "tpsl": "tp"}}
    reduce_only = False
    is_buy = True (long) / False (short)
    limit_px = trigger * (1 + cap) long / * (1 - cap) short

  When triggerPx is breached, HL sends the limit order at limit_px.
  If limit_px is also breached (gap / slippage) the order does NOT fill
  (same cap semantics as the current live flow).

  Source: exchange_hl.py trigger_tp (isMarket=False) + SDK docs "trigger"
  order_type, reduce_only defaults to False in Exchange.order.

  TESTNET-VERIFY-BEFORE-DEPLOY: FLAG 2 — HL API accepts reduce_only=False +
  tpsl="tp" as an entry trigger. Wire format matches SDK Exchange.order
  signature (see exchange_hl.resting_stop_limit). Needs a testnet order to
  confirm the resting oid is returned correctly for non-reduce-only triggers.

KEY INVARIANTS:
  - At most ONE resting order per (coin, tf, side) at all times.
  - On universe-refresh or max_concurrent: cancel ALL resting orders for
    removed / excess coins before placing new ones.
  - State is in-memory only (no DB): startup orphan sweep cancels any
    leftover resting entry triggers on the exchange that have no matching
    tracked key (startup_orphan_sweep).
  - We DO compute size at placement time: compute_size() with current equity
    and snapshot data. Same formula as attempt_entry. This is the real order
    size sent to HL (sz=0 is not valid).

POST-FILL PATH (step 6 detail):
  When a resting order fills intra-bar, the position appears in
  client.open_positions() with no matching Position in open_positions dict.
  The existing _restore_positions_from_db() handles restart recovery, but
  we need to handle the LIVE case within the loop.
  APPROACH: after the resting-order maintenance pass, call
  detect_resting_fills() which:
    a. Reads client.open_positions() (already fetched for account_coins dedup).
    b. For each coin with a resting order key AND a live position AND no
       open_positions[coin] → it was a resting fill.
    c. Returns (key, entry_price, filled_size) tuples for caller to handle.
       Caller immediately calls _place_sl_with_retry + insert_trade + Position.
       Clears the resting order key via consume_fill().
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from bot.config import PROJECT_ROOT, Settings
# XNN port 2026-06-10: strategy module repointed uk_v102 -> xnn (same contract).
# NOTE: this manager's ENTRY logic is still inline Donchian (NOT xnn) — it MUST stay
# disabled via RESTING_ORDERS_ENABLE=0 in the xnn .env (gate in main.py).
from bot.strategy_us29 import _estimate_tick, compute_indicators

log = logging.getLogger(__name__)

# (coin, tf, side) → (oid, trigger_px, limit_px, placed_at)
RestingKey = Tuple[str, str, str]


@dataclass
class RestingOrder:
    oid: int
    trigger_px: float
    limit_px: float
    placed_at: float = field(default_factory=time.time)
    # Signal metadata needed for post-fill journaling
    sl_price: float = 0.0
    tp1_price: float = 0.0
    pivot_high: float = 0.0
    pivot_low: float = 0.0
    f1_dist: float = 0.0
    atr14: float = 0.0
    size: float = 0.0       # size sent to exchange (computed at placement time)
    sl_dist_abs: float = 0.0  # abs(trigger - sl) for risk_dollars computation


class RestingOrderManager:
    """Maintains at most one resting stop-limit per (coin, tf, side).

    Lifecycle:
      refresh(coins, client, open_positions, account_coins, settings,
              n_open, equity, snapshot_holder)
        → called every loop tick BEFORE scanner.scan_all_coins
        → places / cancels / re-prices resting orders (with real sz)
        → returns nothing; fill detection is via detect_resting_fills()

      startup_orphan_sweep(client, open_positions)
        → called ONCE on bot start
        → cancels any open non-reduce-only entry triggers on the exchange
          that are NOT tracked in this manager AND have no matching position

      detect_resting_fills(account_positions, open_positions)
        → returns [(key, entry_px, filled_sz)] for intra-bar fills
        → caller handles SL + journal IMMEDIATELY (same loop iteration)
    """

    def __init__(self, registry_path: Optional[Path] = None):
        # {(coin, tf, side): RestingOrder}
        self._orders: Dict[RestingKey, RestingOrder] = {}
        self.dry_run: bool = False
        # Audit MED FIX-2 (2026-06-19): PERSISTED bot-own resting-oid registry.
        # _orders is purely in-memory, so a warm restart loses all tracking of
        # live resting entry triggers (resting_orders.py KEY INVARIANTS). The
        # startup_orphan_sweep fences ANY xyz_ entry trigger by prefix (combo bot
        # OWNS the xyz_ leg via MANUAL_POSITION_PREFIXES=xyz_) and CANNOT tell a
        # bot-own xyz_ entry stop-buy awaiting first fill (no trades.db row yet)
        # apart from a deliberate MANUAL ЮК xyz_ entry trigger (also no db row).
        # Result: a bot-own xyz_ entry trigger would survive the warm-restart sweep
        # and later fill UNTRACKED/NAKED (no SL adoption) = the exact orphan class
        # the sweeps exist to prevent. Persisting the oids the bot itself places to
        # disk gives the sweep a faithful bot-own discriminator that survives the
        # in-memory state loss: an orphan whose oid IS in the registry is bot-own ->
        # cancel regardless of prefix; an oid NOT in the registry stays prefix-fenced
        # (manual/foreign preserved).
        self._registry_path: Path = (
            registry_path
            if registry_path is not None
            else (PROJECT_ROOT / "data" / "resting_oids.json")
        )
        self._placed_oids: Set[int] = self._load_registry()

    # --- Persisted bot-own oid registry (warm-restart discriminator) ---------
    def _load_registry(self) -> Set[int]:
        try:
            if self._registry_path.exists():
                raw = json.loads(self._registry_path.read_text(encoding="utf-8"))
                oids = {int(o) for o in (raw.get("oids", []) or [])}
                log.info(
                    "resting registry: loaded %d bot-own oid(s) from %s",
                    len(oids), self._registry_path,
                )
                return oids
        except Exception as e:
            # Fail-OPEN on the registry read (empty set). The startup sweep keeps its
            # prefix fence as the conservative floor; a missing/corrupt registry never
            # makes the sweep cancel a manual trigger — it only foregoes the bot-own
            # discriminator for this boot (degrades to the prior prefix-only behavior).
            log.warning("resting registry: load failed (%s) — starting empty", e)
        return set()

    def _save_registry(self) -> None:
        if getattr(self, "dry_run", False):
            return
        try:
            self._registry_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._registry_path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps({"oids": sorted(self._placed_oids)}), encoding="utf-8"
            )
            os.replace(tmp, self._registry_path)
        except Exception as e:
            log.warning("resting registry: save failed (%s) — in-memory only", e)

    def _register_oid(self, oid: int) -> None:
        try:
            oid_i = int(oid)
        except (TypeError, ValueError):
            return
        if oid_i not in self._placed_oids:
            self._placed_oids.add(oid_i)
            self._save_registry()

    def _unregister_oid(self, oid: int) -> None:
        try:
            oid_i = int(oid)
        except (TypeError, ValueError):
            return
        if oid_i in self._placed_oids:
            self._placed_oids.discard(oid_i)
            self._save_registry()

    def active_keys(self) -> set:
        return set(self._orders.keys())

    def get(self, key: RestingKey) -> Optional[RestingOrder]:
        return self._orders.get(key)

    def tracked_coins(self) -> set:
        """All coins with at least one active resting order."""
        return {k[0] for k in self._orders}

    def _compute_size(
        self,
        client,
        coin: str,
        trigger_px: float,
        sl_px: float,
        equity: float,
        snapshot_holder,
        settings: Settings,
    ) -> Tuple[float, float]:
        """Compute order size for placement. Returns (size, risk_dollars) or (0,0) on failure.

        Uses same compute_size() call as attempt_entry:
          risk_dollars = equity * risk_per_trade
          size = risk_dollars / sl_dist, capped by leverage + liq.
        """
        from bot.risk import compute_size

        if equity <= 0:
            return 0.0, 0.0

        try:
            meta = client.asset(coin)
            sz_decimals = meta.sz_decimals
            min_size = meta.min_size
        except Exception as e:
            log.debug("resting size: asset(%s) failed: %s", coin, e)
            return 0.0, 0.0

        # Liquidity cap (best-effort: skip if snapshot missing — still place with risk-only size)
        liq_cap = None
        if snapshot_holder is not None:
            try:
                snap = snapshot_holder.current()
                if snap is not None:
                    profile = snap.get(coin)
                    if profile is not None:
                        liq_cap = settings.liq_size_cap_pct * profile.avg_1h_vol_usd
            except Exception:
                pass

        result = compute_size(
            entry_price=trigger_px,
            sl_price=sl_px,
            account_value=equity,
            settings=settings,
            sz_decimals=sz_decimals,
            liquidity_cap_notional=liq_cap,
        )
        if result is None or result.size < min_size:
            return 0.0, 0.0

        return result.size, result.risk_dollars

    def _place_resting(
        self,
        client,
        coin: str,
        tf: str,
        side: str,
        trigger_px: float,
        limit_px: float,
        sl_price: float,
        tp1_price: float,
        pivot_high: float,
        pivot_low: float,
        f1_dist: float,
        atr14: float,
        equity: float,
        snapshot_holder,
        settings: Settings,
    ) -> Optional[RestingOrder]:
        """Compute size and place a non-reduce-only stop-limit order.

        Returns RestingOrder with real sz, or None on failure.
        sz=0 is rejected by HL — we compute size from account equity here.
        """
        sz, risk_dollars = self._compute_size(
            client=client, coin=coin,
            trigger_px=trigger_px, sl_px=sl_price,
            equity=equity, snapshot_holder=snapshot_holder,
            settings=settings,
        )
        if sz <= 0:
            log.debug(
                "resting_place(%s %s %s): size=0 after compute — skip",
                coin, tf, side,
            )
            return None

        if getattr(self, "dry_run", False):
            log.info("[DRY-RUN] would PLACE resting %s %s %s sz=%.4f trig=%.6f lim=%.6f sl=%.6f", coin, tf, side, sz, trigger_px, limit_px, sl_price)
            return None
        is_buy = (side == "long")
        try:
            resp = client.resting_stop_limit(
                coin=coin,
                is_buy=is_buy,
                sz=sz,
                trigger_px=trigger_px,
                limit_px=limit_px,
            )
        except Exception as e:
            log.warning("resting_stop_limit(%s %s %s) failed: %s", coin, tf, side, e)
            return None
        try:
            sts = resp["response"]["data"]["statuses"]
            if sts and "resting" in sts[0]:
                oid = sts[0]["resting"].get("oid")
                if oid:
                    sl_dist_abs = abs(trigger_px - sl_price)
                    # FIX-2: record bot-own oid BEFORE returning so a crash between
                    # placement and the caller's self._orders[key]=ro assignment still
                    # leaves a durable bot-own marker for the next startup sweep.
                    self._register_oid(int(oid))
                    return RestingOrder(
                        oid=int(oid),
                        trigger_px=trigger_px,
                        limit_px=limit_px,
                        sl_price=sl_price,
                        tp1_price=tp1_price,
                        pivot_high=pivot_high,
                        pivot_low=pivot_low,
                        f1_dist=f1_dist,
                        atr14=atr14,
                        size=sz,
                        sl_dist_abs=sl_dist_abs,
                    )
            if sts and "error" in sts[0]:
                log.warning("resting_stop_limit(%s) error: %s", coin, sts[0]["error"])
        except Exception as e:
            log.warning("resting_stop_limit(%s) response parse failed: %s", coin, e)
        return None

    def _cancel(self, client, coin: str, oid: int) -> None:
        if getattr(self, "dry_run", False):
            log.info("[DRY-RUN] would CANCEL resting %s oid=%s", coin, oid)
            return
        try:
            client.cancel_sl_order(coin, oid)
        except Exception as e:
            log.warning("cancel resting order %s oid=%s failed (harmless): %s", coin, oid, e)
        finally:
            # FIX-2: drop from the bot-own registry once we have attempted the cancel
            # (the order is gone or going) so the registry doesn't accumulate stale oids.
            self._unregister_oid(oid)

    def cancel_all_for_coin(self, client, coin: str) -> None:
        """Cancel all resting orders for a coin (e.g. position just opened)."""
        to_remove = [k for k in self._orders if k[0] == coin]
        for k in to_remove:
            ro = self._orders.pop(k)
            self._cancel(client, coin, ro.oid)
            log.debug("Cancelled resting order %s %s %s oid=%s", coin, k[1], k[2], ro.oid)

    def cancel_all(self, client) -> None:
        """Cancel every resting order (e.g. shutdown / universe wipe)."""
        for k, ro in list(self._orders.items()):
            self._cancel(client, k[0], ro.oid)
        self._orders.clear()

    def startup_orphan_sweep(self, client, open_positions: dict) -> None:
        """Cancel open non-reduce-only entry triggers with no matching position.

        Called ONCE on bot start. Handles the case where the bot was restarted
        while resting orders were live on the exchange (in-memory state was lost).

        Safe: only touches non-reduce-only trigger orders (NOT the protective SL
        reduce-only triggers that manage_open_position relies on).

        FLAG 4: list_open_sl_orders filters stop|isTrigger|reduceOnly — it would
        catch reduce_only SL triggers but not necessarily reduce_only=False entry
        triggers. list_open_entry_trigger_orders() is a dedicated method for this.
        If the exchange client doesn't have that method (non-HL clients), falls
        back gracefully.
        """
        if not hasattr(client, "list_open_entry_trigger_orders"):
            log.debug("startup_orphan_sweep: client has no list_open_entry_trigger_orders — skip")
            return
        try:
            orphan_candidates = client.list_open_entry_trigger_orders()
        except Exception as e:
            log.warning("startup_orphan_sweep: list_open_entry_trigger_orders failed: %s", e)
            return

        if not orphan_candidates:
            log.info("startup_orphan_sweep: no open entry triggers on exchange")
            return

        # Positions on exchange (coin → data)
        try:
            exchange_positions = client.open_positions()
        except Exception as e:
            log.warning("startup_orphan_sweep: open_positions failed — aborting sweep: %s", e)
            return

        # Audit MEDIUM (2026-06-19): manual/foreign FENCE for non-reduce-only ENTRY triggers.
        # This sweep cancels ANY entry trigger with no live position and was previously fenced
        # by NOTHING (no MANUAL_POSITION_PREFIXES / FOREIGN_SKIP_PREFIXES check) — unlike the
        # reduce-only orphan sweep (orphan_sweep._fenced) and the untracked-protect sweep
        # (main.py:613/621). On the SHARED unified HL account the manual ЮК workflow places
        # deliberate stop-BUY ENTRY triggers awaiting fill (mem:feedback_uk_complete_trade_workflow);
        # such an entry has NO position yet, so without this fence it would be cancelled on every
        # bot startup. Currently latent (DRY_RUN -> _cancel no-ops) but the combo is being prepped
        # for live, and this is exactly the manual-position-protection class the prefix fences exist
        # for. db_open is NOT a useful discriminator here (a manual entry awaiting fill carries no
        # trades.db open row either) -> fence by prefix, the HIGH-risk-safe direction.
        try:
            from bot.config import MANUAL_POSITION_PREFIXES, FOREIGN_SKIP_PREFIXES
        except Exception as _ce:
            # Fail-safe LOUD: if the fence symbols cannot be imported, fence EVERYTHING (skip the
            # whole sweep) rather than risk cancelling a deliberate manual entry trigger.
            log.error(
                "startup_orphan_sweep: prefix-fence import FAILED (%s) — skipping sweep "
                "(fail-safe: never cancel a possibly-manual entry trigger)", _ce,
            )
            return

        def _fenced_entry(_coin: str) -> bool:
            base = str(_coin).upper().replace("-PERP", "").replace("-USD", "")
            for _p in (MANUAL_POSITION_PREFIXES or ()):
                if _p and base.startswith(str(_p).upper()):
                    return True
            for _p in (FOREIGN_SKIP_PREFIXES or ()):
                if _p and base.startswith(str(_p).upper()):
                    return True
            return False

        cancelled = 0
        kept = 0
        fenced = 0
        for entry in orphan_candidates:
            coin = entry.get("coin", "")
            oid = entry.get("oid")
            if not coin or not oid:
                continue
            # FIX-2: bot-own discriminator FIRST. An entry trigger whose oid is in the
            # persisted bot-own registry is one WE placed (in-memory state was lost on the
            # warm restart). It MUST be cancelled even if its coin matches the prefix fence
            # (the combo bot OWNS the xyz_ leg, so a bot-own xyz_ entry awaiting first fill
            # would otherwise be wrongly fenced and later fill naked/untracked). Manual ЮК
            # xyz_ triggers were never placed by us -> not in the registry -> prefix fence
            # below still preserves them.
            _is_bot_own = False
            try:
                _is_bot_own = int(oid) in self._placed_oids
            except (TypeError, ValueError):
                _is_bot_own = False
            if _is_bot_own:
                log.info(
                    "startup_orphan_sweep: cancelling BOT-OWN orphan entry trigger oid=%s %s "
                    "(in persisted registry — in-memory state lost on restart)", oid, coin,
                )
                self._cancel(client, coin, oid)
                cancelled += 1
                continue
            # Manual/foreign fence: a deliberately pre-placed manual stop-BUY entry (no position
            # yet) on the shared account must survive bot startup.
            if _fenced_entry(coin):
                fenced += 1
                log.info(
                    "startup_orphan_sweep: keep oid=%s %s (manual/foreign prefix — fenced)", oid, coin,
                )
                continue
            # Has a live position → keep the order (it may be a re-entry after partial close;
            # or it may be a legit pre-placed order we want to preserve on a warm restart).
            # Conservatively: if position exists, keep.
            if coin in exchange_positions or f"{coin}-USD" in exchange_positions:
                kept += 1
                log.debug(
                    "startup_orphan_sweep: keep oid=%d %s (position exists)", oid, coin,
                )
                continue
            # No position, not tracked in this manager's in-memory state → orphan
            log.info(
                "startup_orphan_sweep: cancelling orphan entry trigger oid=%d %s", oid, coin,
            )
            self._cancel(client, coin, oid)
            cancelled += 1

        log.info(
            "startup_orphan_sweep done: cancelled=%d kept=%d fenced=%d (from %d candidates)",
            cancelled, kept, fenced, len(orphan_candidates),
        )

    def refresh(
        self,
        coins: list,                   # list[AssetTier]
        client,
        open_positions: dict,          # {coin: Position} — this bot
        account_coins: set,            # coins held on exchange (both bots)
        settings: Settings,
        n_open: int,                   # current concurrent open count
        equity: float = 0.0,           # account equity for sizing (0 = skip placement)
        snapshot_holder=None,          # LiquiditySnapshotHolder (optional, for liq cap)
    ) -> None:
        """Maintain resting orders. Called every loop tick.

        Does NOT return fills — fill detection is separate (detect_resting_fills).
        equity=0.0 is allowed but will produce size=0 → no orders placed. Caller
        should pass current equity every tick so placement uses fresh account state.
        """
        universe_symbols = {a.symbol for a in coins}

        from bot.config import TF_MS
        all_tfs = sorted(
            set(settings.working_tfs) | set(settings.short_enabled_tfs),
            key=lambda t: TF_MS.get(t, 0),
        )

        # --- Cancel resting orders for coins that left universe or have a position ---
        for key in list(self._orders.keys()):
            coin, tf, side = key
            ro = self._orders[key]
            should_cancel = (
                coin not in universe_symbols
                or coin in account_coins
                or coin in open_positions
                or n_open >= settings.max_concurrent
            )
            if should_cancel:
                self._cancel(client, coin, ro.oid)
                del self._orders[key]
                log.debug(
                    "Cancelled resting %s %s %s oid=%s (position/universe/cap)",
                    coin, tf, side, ro.oid,
                )

        if equity <= 0:
            log.debug("resting refresh: equity=%.2f — skip placement", equity)
            return

        # --- For each eligible (coin, tf, side): maintain resting order ---
        for tf in all_tfs:
            do_long = tf in settings.working_tfs
            do_short = settings.short_enabled_for(tf)
            if not (do_long or do_short):
                continue

            lf = settings.get_tf_filters(tf)
            sf = settings.get_tf_short_filters(tf)

            for asset in coins:
                coin = asset.symbol
                # Skip coins that already have a position or are at capacity
                if coin in account_coins or coin in open_positions:
                    continue
                if n_open >= settings.max_concurrent:
                    break  # at cap — no new resting orders

                # Fetch candles (reuse client cache — same call as scanner)
                try:
                    df = client.candles(coin, tf, limit=300)
                except Exception as e:
                    log.debug("resting candles(%s, %s) failed: %s", coin, tf, e)
                    continue
                min_k_required = max(lf.donchian_k, sf.donchian_k) + 2
                if df is None or len(df) < min_k_required:
                    continue
                try:
                    df = compute_indicators(df)
                except Exception:
                    continue

                i = len(df) - 1
                close_i = float(df["Close"].iloc[i])
                ema50_i = float(df["ema50"].iloc[i])
                ema20_i = float(df["ema20"].iloc[i])
                atr14_i = float(df["atr14"].iloc[i])
                tick = _estimate_tick(close_i)

                # LONG resting
                if do_long:
                    key: RestingKey = (coin, tf, "long")
                    # Trend guard
                    if settings.require_ema50_up and close_i <= ema50_i:
                        if key in self._orders:
                            self._cancel(client, coin, self._orders[key].oid)
                            del self._orders[key]
                        continue

                    hh = float(df["High"].iloc[i - lf.donchian_k:i].max())
                    forming_trigger = hh + tick
                    limit_px = client.round_price(
                        coin, forming_trigger * (1 + settings.entry_limit_cap_pct)
                    )
                    forming_trigger = client.round_price(coin, forming_trigger)

                    # Don't place if price already past trigger (breakout already happened)
                    if close_i > limit_px:
                        if key in self._orders:
                            self._cancel(client, coin, self._orders[key].oid)
                            del self._orders[key]
                        continue

                    existing = self._orders.get(key)
                    if existing is None:
                        # F1 guard before placing
                        if lf.f1 > 0 and atr14_i > 0:
                            f1_dist = (forming_trigger - ema20_i) / atr14_i
                            if f1_dist < lf.f1:
                                continue
                        else:
                            f1_dist = 0.0

                        ll = float(df["Low"].iloc[i - lf.donchian_k:i].min())
                        sl_px = client.round_price(coin, ll - tick)
                        if sl_px >= forming_trigger:
                            continue
                        sl_dist = forming_trigger - sl_px
                        sl_dist_pct = sl_dist / forming_trigger
                        max_sl = settings.tf_max_sl.get(tf, 0.10)
                        if sl_dist_pct < settings.min_sl_dist_pct or sl_dist_pct > max_sl:
                            continue
                        tp1 = forming_trigger + settings.raw_rr_target * sl_dist

                        ro = self._place_resting(
                            client, coin, tf, "long",
                            trigger_px=forming_trigger, limit_px=limit_px,
                            sl_price=sl_px, tp1_price=tp1,
                            pivot_high=hh, pivot_low=ll,
                            f1_dist=f1_dist, atr14=atr14_i,
                            equity=equity, snapshot_holder=snapshot_holder,
                            settings=settings,
                        )
                        if ro is not None:
                            self._orders[key] = ro
                            log.info(
                                "RESTING LONG %s %s: trigger=%.6f limit=%.6f sl=%.6f sz=%.4f oid=%s",
                                coin, tf, forming_trigger, limit_px, sl_px, ro.size, ro.oid,
                            )
                    else:
                        # Already have a resting order — re-price if trigger moved
                        trigger_moved = abs(forming_trigger - existing.trigger_px) > tick * 0.5
                        if trigger_moved and forming_trigger > existing.trigger_px:
                            # Cancel old, place new
                            self._cancel(client, coin, existing.oid)
                            del self._orders[key]

                            ll = float(df["Low"].iloc[i - lf.donchian_k:i].min())
                            sl_px = client.round_price(coin, ll - tick)
                            if sl_px >= forming_trigger:
                                continue
                            sl_dist = forming_trigger - sl_px
                            sl_dist_pct = sl_dist / forming_trigger
                            if sl_dist_pct < settings.min_sl_dist_pct or sl_dist_pct > settings.tf_max_sl.get(tf, 0.10):
                                continue
                            tp1 = forming_trigger + settings.raw_rr_target * sl_dist
                            f1_dist = (forming_trigger - ema20_i) / atr14_i if atr14_i > 0 else 0.0

                            ro = self._place_resting(
                                client, coin, tf, "long",
                                trigger_px=forming_trigger, limit_px=limit_px,
                                sl_price=sl_px, tp1_price=tp1,
                                pivot_high=hh, pivot_low=ll,
                                f1_dist=f1_dist, atr14=atr14_i,
                                equity=equity, snapshot_holder=snapshot_holder,
                                settings=settings,
                            )
                            if ro is not None:
                                self._orders[key] = ro
                                log.info(
                                    "RESTING LONG re-price %s %s: trigger %.6f→%.6f sz=%.4f oid=%s",
                                    coin, tf, existing.trigger_px, forming_trigger, ro.size, ro.oid,
                                )

                # SHORT resting (mirror)
                if do_short:
                    key_s: RestingKey = (coin, tf, "short")
                    if settings.require_ema50_down and close_i >= ema50_i:
                        if key_s in self._orders:
                            self._cancel(client, coin, self._orders[key_s].oid)
                            del self._orders[key_s]
                        continue

                    ll_s = float(df["Low"].iloc[i - sf.donchian_k:i].min())
                    forming_trigger_s = ll_s - tick
                    limit_px_s = client.round_price(
                        coin, forming_trigger_s * (1 - settings.entry_limit_cap_pct)
                    )
                    forming_trigger_s = client.round_price(coin, forming_trigger_s)

                    if close_i < limit_px_s:
                        if key_s in self._orders:
                            self._cancel(client, coin, self._orders[key_s].oid)
                            del self._orders[key_s]
                        continue

                    existing_s = self._orders.get(key_s)
                    if existing_s is None:
                        if sf.f1 > 0 and atr14_i > 0:
                            f1_dist_s = (ema20_i - forming_trigger_s) / atr14_i
                            if f1_dist_s < sf.f1:
                                continue
                        else:
                            f1_dist_s = 0.0

                        hh_s = float(df["High"].iloc[i - sf.donchian_k:i].max())
                        sl_px_s = client.round_price(coin, hh_s + tick)
                        if sl_px_s <= forming_trigger_s:
                            continue
                        sl_dist_s = sl_px_s - forming_trigger_s
                        sl_dist_pct_s = sl_dist_s / abs(forming_trigger_s)
                        if sl_dist_pct_s < settings.min_sl_dist_pct or sl_dist_pct_s > settings.tf_max_sl.get(tf, 0.10):
                            continue
                        tp1_s = forming_trigger_s - settings.raw_rr_target * sl_dist_s

                        ro_s = self._place_resting(
                            client, coin, tf, "short",
                            trigger_px=forming_trigger_s, limit_px=limit_px_s,
                            sl_price=sl_px_s, tp1_price=tp1_s,
                            pivot_high=hh_s, pivot_low=ll_s,
                            f1_dist=f1_dist_s, atr14=atr14_i,
                            equity=equity, snapshot_holder=snapshot_holder,
                            settings=settings,
                        )
                        if ro_s is not None:
                            self._orders[key_s] = ro_s
                            log.info(
                                "RESTING SHORT %s %s: trigger=%.6f limit=%.6f sl=%.6f sz=%.4f oid=%s",
                                coin, tf, forming_trigger_s, limit_px_s, sl_px_s, ro_s.size, ro_s.oid,
                            )
                    else:
                        # Re-price only if trigger moved LOWER
                        trigger_moved_s = abs(forming_trigger_s - existing_s.trigger_px) > tick * 0.5
                        if trigger_moved_s and forming_trigger_s < existing_s.trigger_px:
                            self._cancel(client, coin, existing_s.oid)
                            del self._orders[key_s]

                            hh_s = float(df["High"].iloc[i - sf.donchian_k:i].max())
                            sl_px_s = client.round_price(coin, hh_s + tick)
                            if sl_px_s <= forming_trigger_s:
                                continue
                            sl_dist_s = sl_px_s - forming_trigger_s
                            sl_dist_pct_s = sl_dist_s / abs(forming_trigger_s)
                            if sl_dist_pct_s < settings.min_sl_dist_pct or sl_dist_pct_s > settings.tf_max_sl.get(tf, 0.10):
                                continue
                            tp1_s = forming_trigger_s - settings.raw_rr_target * sl_dist_s
                            f1_dist_s = (ema20_i - forming_trigger_s) / atr14_i if atr14_i > 0 else 0.0

                            ro_s = self._place_resting(
                                client, coin, tf, "short",
                                trigger_px=forming_trigger_s, limit_px=limit_px_s,
                                sl_price=sl_px_s, tp1_price=tp1_s,
                                pivot_high=hh_s, pivot_low=ll_s,
                                f1_dist=f1_dist_s, atr14=atr14_i,
                                equity=equity, snapshot_holder=snapshot_holder,
                                settings=settings,
                            )
                            if ro_s is not None:
                                self._orders[key_s] = ro_s
                                log.info(
                                    "RESTING SHORT re-price %s %s: trigger %.6f→%.6f sz=%.4f oid=%s",
                                    coin, tf, existing_s.trigger_px, forming_trigger_s, ro_s.size, ro_s.oid,
                                )

    def detect_resting_fills(
        self,
        account_positions: dict,   # from client.open_positions() — already fetched
        open_positions: dict,      # this bot's tracked positions
    ) -> list:
        """Check if any resting order filled intra-bar.

        Returns list of (key, entry_price, filled_size) for filled orders.
        Caller handles post-fill SL + journal using the RestingOrder metadata.
        """
        filled = []
        for key in list(self._orders.keys()):
            coin, tf, side = key
            # If position now exists on exchange but bot doesn't track it → fill detected
            pos_data = account_positions.get(coin) or account_positions.get(f"{coin}-USD")
            if pos_data is not None and coin not in open_positions:
                try:
                    szi = float(pos_data.get("szi", 0) or 0)
                    entry_px = float(pos_data.get("entryPx", 0) or 0)
                    if abs(szi) > 0 and entry_px > 0:
                        # Verify side matches
                        expected_long = (side == "long")
                        actual_long = (szi > 0)
                        if expected_long == actual_long:
                            filled.append((key, entry_px, abs(szi)))
                except Exception as e:
                    log.warning("detect_resting_fills parse error %s: %s", coin, e)
        return filled

    def consume_fill(self, key: RestingKey) -> Optional[RestingOrder]:
        """Remove and return the RestingOrder after a fill is confirmed."""
        return self._orders.pop(key, None)
