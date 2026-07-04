"""scanner.py — multi-TF scanner with per-TF F1/F2/F3 injection.
BIDIRECTIONAL (long + short) since 2026-05-27.

Each TF scanned for LONG signal first; if SHORT enabled for that TF
(settings.short_enabled_for(tf)), also scanned for SHORT signal.

Cross-side dedup (user spec Q2 2026-05-27):
  If position open on BTC (any side, any TF) → skip BTC on all TFs this scan.
  Prevents self-cancelling hedge (long BTC + short BTC at same time).
"""
from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional

import pandas as pd
from bot.warmup_backfill import backfill_warmup

from bot.config import Settings, TF_MS
from bot.strategy_xnn import (
    Signal, compute_indicators, scan_for_signal, scan_for_short_signal,
)
from bot.universe import AssetTier

log = logging.getLogger(__name__)

# XNN port 2026-06-11 (canon §0#3): candle window for the scan. The old hardcoded 300
# is too small for XNN slow EMA pairs (377/610) + adaptive-fit windows (4h fit=360) +
# EWM seed-residual convergence. Set SCAN_CANDLES_LIMIT=3000 in the xnn .env.
SCAN_CANDLES_LIMIT = int(os.getenv("SCAN_CANDLES_LIMIT", "300"))


class Scanner:
    """Multi-TF scanner with per-TF F1/F2/F3 (long + short) filter injection."""

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._last_bar_ts: Dict[str, int] = {}
        # Depth guard (fix 2026-06-11): (coin, tf) pairs already warned about a
        # short candle window — warn LOUD but once, not every scan.
        self._depth_warned: set = set()

    def _current_bar_start_ms(self, tf: str) -> int:
        ms = TF_MS.get(tf, 3_600_000)
        now_ms = int(time.time() * 1000)
        return (now_ms // ms) * ms

    def _prev_bar_start_ms(self, tf: str) -> int:
        ms = TF_MS.get(tf, 3_600_000)
        now_ms = int(time.time() * 1000)
        current_bar = (now_ms // ms) * ms
        return current_bar - ms

    def new_bar_closed(self, tf: str) -> bool:
        prev_bar = self._prev_bar_start_ms(tf)
        last_seen = self._last_bar_ts.get(tf, 0)
        if prev_bar > last_seen:
            self._last_bar_ts[tf] = prev_bar
            log.debug("New bar closed: tf=%s bar_start_ms=%d", tf, prev_bar)
            return True
        return False

    @staticmethod
    def _df_last_ts_ms(df) -> int:
        """ts (ms UTC) of the last row of a candles df; 0 if unavailable."""
        try:
            return int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        except Exception:
            return 0

    def bar_age_sec(self, tf: str) -> float:
        ms = TF_MS.get(tf, 3_600_000)
        now_ms = int(time.time() * 1000)
        current_bar_start = (now_ms // ms) * ms
        return (now_ms - current_bar_start) / 1000.0

    def scan_all_coins(
        self,
        coins: list[AssetTier],
        client,
        open_positions: dict,
        no_long_symbols: set | None = None,
        on_crypto_signal=None,
    ) -> list[Signal]:
        """Scan coins on each TF that just closed.

        TF iteration = UNION(working_tfs, short_enabled_tfs) — so SHORT can scan
        on TFs where LONG is NOT enabled (independent edge per side).
        Per-TF: scan LONG if tf in working_tfs; scan SHORT if short_enabled_for(tf).
        """
        signals: list[Signal] = []
        s = self.cfg
        no_long = no_long_symbols or set()

        all_scan_tfs = sorted(set(s.working_tfs) | set(s.short_enabled_tfs))

        for tf in all_scan_tfs:
            if not self.new_bar_closed(tf):
                continue

            age_sec = self.bar_age_sec(tf)
            gate = s.bar_age_gate_for(tf)
            if age_sec > gate:
                log.warning("Bar too stale for %s: age=%.0fs > %ds — skip scan", tf, age_sec, gate)
                continue

            long_enabled = (tf in s.working_tfs)
            short_enabled = s.short_enabled_for(tf)
            long_filters = s.get_tf_filters(tf) if long_enabled else None
            short_filters = s.get_tf_short_filters(tf) if short_enabled else None

            log.info(
                "Scanning %d coins tf=%s age=%.0fs LONG[%s] SHORT[%s]",
                len(coins), tf, age_sec,
                f"filt_thr mindist_ema20={long_filters.f1:.2f} minRSI={long_filters.f2:.1f} volcap_usd={long_filters.f3:.0f}(0=off,NOT counts)"
                if long_enabled else "DISABLED",
                f"filt_thr mindist_ema20={short_filters.f1:.2f} maxRSI={short_filters.f2:.1f} volcap_usd={short_filters.f3:.0f}(0=off,NOT counts)"
                if short_enabled else "DISABLED",
            )

            # FAIL-LOUD guard (signal-blind / all-items-skipped class,
            # feedback_failsafe_skip_on_every_item_is_silent_disable): count coins
            # actually ATTEMPTED for a candle fetch (n_scanned) vs those whose candles
            # came back empty/None/fetch-failed (n_empty). If EVERY attempted coin on
            # this TF failed the candle fetch, the scan returns no signals — which is
            # indistinguishable from a quiet market. Emit ERROR at the per-TF tail.
            # PURELY additive: no signal/gate logic changed.
            n_scanned = 0
            n_empty = 0

            for asset in coins:
                sym = asset.symbol

                # Cross-side dedup: if ANY position open on this coin (long or short, any TF) → skip
                if sym in open_positions:
                    log.debug(
                        "Cross-side skip %s %s: position open side=%s tf=%s",
                        sym, tf,
                        getattr(open_positions[sym], "side", "?"),
                        open_positions[sym].tf,
                    )
                    continue

                # Coin attempted for a candle fetch (FAIL-LOUD accounting).
                n_scanned += 1
                try:
                    df = client.candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                except Exception as e:
                    log.warning("candles(%s, %s) failed: %s", sym, tf, e)
                    n_empty += 1  # candle fetch FAILED (data-failure path)
                    continue
                if df is None or len(df) < 100:
                    n_empty += 1  # candles empty/None/insufficient (data-failure path)
                    continue

                # DEPTH GUARD (LOW fix 2026-06-11): SCAN_CANDLES_LIMIT=3000 is what we
                # REQUEST; the Pacifica /kline server-side cap is unverified (§9#1). A
                # silent cap < ~700 bars breaks the 377/610-EMA union gates + EWM
                # seed-residual vs bt while logs stay green. Loud once per (coin, tf).
                # PARITY GUARD (2026-06-12): EMA-610 needs >=MIN_SIGNAL_BARS bars to converge;
                # fewer = seed-residual transient that DIVERGES from bt (full-history insts).
                # Root usually instrument listing-age — Pacifica BTC-PERP since 2025-06-09 ->
                # ~368 daily bars, NOT a /kline cap (window older than first bar returns 0).
                # Under-seeded indicator must NOT emit a signal -> SKIP (coin,tf) until bars
                # accrue (auto-clears). memory project_pacifica_1d_ema610_starved_listing_age_2026_06_12
                _min_sig_bars = int(os.getenv("MIN_SIGNAL_BARS", "610"))
                df = backfill_warmup(df, sym, tf, _min_sig_bars)
                if len(df) < _min_sig_bars:
                    if (sym, tf) not in self._depth_warned:
                        self._depth_warned.add((sym, tf))
                        log.warning(
                            "PARITY SKIP %s %s: %d bars < %d (EMA-610 warmup) — insufficient "
                            "history (listing-age or /kline cap); skipping until bars accrue",
                            sym, tf, len(df), _min_sig_bars,
                        )
                    continue

                # FRESHNESS GUARD (xnn port 2026-06-11, canon §0#11): the decision bar
                # MUST be the bar whose close triggered this scan (self._last_bar_ts[tf]).
                # Cache offset-jitter / a lagging API can serve a df ending at bar N-1 —
                # the strategy would then evaluate counted_now on the WRONG bar and bar
                # N's signal is lost forever (scan runs once per bar). Force a refetch
                # past the cache; if still stale, SKIP LOUD (never decide on N-1 silently).
                _expected_ts = self._last_bar_ts.get(tf, 0)
                _last_ts = self._df_last_ts_ms(df)
                if _expected_ts and _last_ts and _last_ts < _expected_ts:
                    if hasattr(client, "invalidate_candles_cache"):
                        client.invalidate_candles_cache(sym, tf)
                        try:
                            df = client.candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                        except Exception as e:
                            log.warning("fresh refetch candles(%s, %s) failed: %s", sym, tf, e)
                            continue
                        _last_ts = self._df_last_ts_ms(df)
                    if df is None or len(df) < 100 or (_last_ts and _last_ts < _expected_ts):
                        log.warning(
                            "STALE candles %s %s: last closed bar %s < expected %s "
                            "after forced refetch — skipping this bar (parity guard)",
                            sym, tf, _last_ts, _expected_ts,
                        )
                        continue

                try:
                    df = compute_indicators(df)
                except Exception as e:
                    log.warning("compute_indicators(%s, %s) failed: %s", sym, tf, e)
                    continue

                # ----------- LONG scan (only if enabled on this TF) -----------
                if long_enabled and sym not in no_long:
                    long_sig = scan_for_signal(
                        df=df, coin=sym, tf=tf,
                        zigzag_length=s.zigzag_raw_length,
                        raw_rr_target=s.raw_rr_target,
                        require_ema50_up=s.require_ema50_up,
                        f1_min_dist_ema20_atr=long_filters.f1,
                        f2_min_rsi14=long_filters.f2,
                        f3_max_dollar_vol_usd=long_filters.f3,
                        tf_max_sl=s.tf_max_sl,
                        min_sl_dist_pct=s.min_sl_dist_pct,
                    )
                    if long_sig is not None:
                        log.info(
                            "SIGNAL LONG %s %s: trig=%.6f sl=%.6f tp1=%.6f f1=%.2f",
                            sym, tf, long_sig.trigger_price, long_sig.sl_price,
                            long_sig.tp1_price, long_sig.f1_dist,
                        )
                        signals.append(long_sig)
                        # STREAM ENTRY (entry-latency fix): enter the instant the
                        # crypto breakout is detected, not after the whole universe
                        # pass. All Pacifica coins are crypto -> no selection leg.
                        if on_crypto_signal is not None:
                            try:
                                on_crypto_signal(long_sig)
                            except Exception as e:
                                log.error("on_crypto_signal(%s) cb error (entry may be lost): %s", sym, e, exc_info=True)
                        continue  # one side per coin per scan

                # ----------- SHORT scan -----------
                if not short_enabled:
                    continue
                short_sig = scan_for_short_signal(
                    df=df, coin=sym, tf=tf,
                    zigzag_length=s.zigzag_raw_length,
                    raw_rr_target=s.raw_rr_target,
                    require_ema50_down=s.require_ema50_down,
                    f1_min_dist_ema20_atr=short_filters.f1,
                    f2_max_rsi14=short_filters.f2,
                    f3_max_dollar_vol_usd=short_filters.f3,
                    tf_max_sl=s.tf_max_sl,
                    min_sl_dist_pct=s.min_sl_dist_pct,
                )
                if short_sig is not None:
                    log.info(
                        "SIGNAL SHORT %s %s: trig=%.6f sl=%.6f tp1=%.6f f1=%.2f",
                        sym, tf, short_sig.trigger_price, short_sig.sl_price,
                        short_sig.tp1_price, short_sig.f1_dist,
                    )
                    signals.append(short_sig)
                    # STREAM ENTRY (entry-latency fix): see LONG note above.
                    if on_crypto_signal is not None:
                        try:
                            on_crypto_signal(short_sig)
                        except Exception as e:
                            log.error("on_crypto_signal(%s) cb error (entry may be lost): %s", sym, e, exc_info=True)

            # FAIL-LOUD per-TF tail: every attempted coin had empty/failed candles.
            if n_scanned > 0 and n_empty == n_scanned:
                log.error(
                    "FAIL-LOUD tf=%s: candles empty/failed for ALL %d scanned coins — "
                    "indexer down or limit rejected; bot is signal-blind on this TF, "
                    "NOT a quiet-gates situation",
                    tf, n_scanned,
                )

        return signals
