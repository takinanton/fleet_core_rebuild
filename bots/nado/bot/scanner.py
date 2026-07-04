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
# XNN port 2026-06-11: strategy module swapped uk_v102 -> xnn (same contract; uk kwargs
# accepted-and-ignored by the adapter, XNN per-TF config embedded in strategy_xnn).
from bot.strategy_xnn import (
    Signal, compute_indicators, scan_for_signal, scan_for_short_signal,
)
from bot.universe import AssetTier

log = logging.getLogger(__name__)

# XNN port 2026-06-11: deep candle window for slow EMA pairs (377/610 union candidates)
# + EWM seed-residual convergence. Hardcoded 300 = parity FAIL for 1d/8h union mode.
# Env so the value can be raised without code change (canon: 3000).
SCAN_CANDLES_LIMIT = int(os.getenv("SCAN_CANDLES_LIMIT", "300"))


class Scanner:
    """Multi-TF scanner with per-TF F1/F2/F3 (long + short) filter injection."""

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._last_bar_ts: Dict[str, int] = {}
        # XNN audit fix 2026-06-11: one-time per-TF depth warning (silent indexer
        # truncation = quiet EMA/parity drift on 1d/8h union mode).
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

            # XNN audit fix 2026-06-11: distinguish "indexer errors/truncates" from
            # "thin coins". (a) silent depth truncation -> one-time WARNING on a
            # top-liquid coin (parity risk, checklist §7 probe); (b) EVERY coin
            # empty on this TF -> FAIL-LOUD error (indexer broken / limit rejected,
            # bot would otherwise sit signal-less forever looking like quiet gates).
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

                n_scanned += 1
                try:
                    df = client.candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                except Exception as e:
                    log.warning("candles(%s, %s) failed: %s", sym, tf, e)
                    n_empty += 1
                    continue
                if df is None or len(df) == 0:
                    n_empty += 1
                    continue
                if (
                    tf not in self._depth_warned
                    and sym.split("-")[0] == "BTC"
                    and len(df) < SCAN_CANDLES_LIMIT * 0.9
                ):
                    self._depth_warned.add(tf)
                    log.warning(
                        "Indexer depth tf=%s %s: %d bars < 0.9×%d requested — "
                        "short history (OK if young venue/seed-residual) OR silent "
                        "truncation (parity risk, verify per DEPLOY_CHECKLIST §7 probe)",
                        tf, sym, len(df), SCAN_CANDLES_LIMIT,
                    )
                if len(df) < 100:
                    continue
                # PARITY GUARD (2026-06-12): EMA-610 needs >=MIN_SIGNAL_BARS bars to converge;
                # fewer = seed-residual transient that DIVERGES from bt (full-history insts).
                # Usually young-listing alt (too few daily bars). Under-seeded indicator must
                # NOT emit a signal -> SKIP (coin,tf) until bars accrue. memory
                # project_pacifica_1d_ema610_starved_listing_age_2026_06_12
                _min_sig_bars = int(os.getenv("MIN_SIGNAL_BARS", "610"))
                df = backfill_warmup(df, sym, tf, _min_sig_bars)
                if len(df) < _min_sig_bars:
                    if not hasattr(self, "_parity_skipped"):
                        self._parity_skipped = set()
                    if (sym, tf) not in self._parity_skipped:
                        self._parity_skipped.add((sym, tf))
                        log.warning(
                            "PARITY SKIP %s %s: %d bars < %d (EMA-610 warmup) — insufficient "
                            "history; skipping until bars accrue (signal != bt otherwise)",
                            sym, tf, len(df), _min_sig_bars,
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
                    if on_crypto_signal is not None:
                        try:
                            on_crypto_signal(short_sig)
                        except Exception as e:
                            log.error("on_crypto_signal(%s) cb error (entry may be lost): %s", sym, e, exc_info=True)

            if n_scanned > 0 and n_empty == n_scanned:
                log.error(
                    "FAIL-LOUD tf=%s: candles empty/failed for ALL %d scanned coins "
                    "(limit=%d) — indexer down or limit rejected; bot is signal-blind "
                    "on this TF, NOT a quiet-gates situation",
                    tf, n_scanned, SCAN_CANDLES_LIMIT,
                )

        return signals
