"""scanner.py — multi-TF scanner with per-TF F1/F2/F3 injection.

Scans working_tfs every loop iteration. New bar detection:
  Each TF has its own "last bar timestamp seen" tracker.
  On new bar → run signal scan on that TF for all coins in universe.

Cross-TF deduplication (user spec):
  if position open on BTC (any TF) → skip BTC on all TFs this scan
  max_concurrent=5 across ALL TFs combined

Per-TF F1/F2/F3 (user spec — first-class feature):
  Before each coin scan on a given TF, settings.get_tf_filters(tf) is called.
  This returns the per-TF PerTFFilters with f1/f2/f3 for that TF.
  scan_for_signal receives these values directly → no global state mutation.

Bar timing (UTC):
  1h: bars open every hour on the hour
  2h: bars open at 00:00, 02:00, ... UTC
  4h: bars open at 00:00, 04:00, ... UTC
  8h: bars open at 00:00, 08:00, ... UTC
  1d: bars open at 00:00 UTC

Bar age gate (per TF):
  Grace window = half the bar duration. Scanner logs stale bars but skips them.
  Override via BAR_AGE_MAX_SEC env var (sets all TFs to same window).
"""
from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import pandas as pd

from bot.config import Settings, TF_MS, settings as global_settings
from bot.strategy_uk_v102 import Signal, compute_indicators, scan_for_signal
from bot.universe import AssetTier

log = logging.getLogger(__name__)

# Per-TF bar-age gates now live in Settings.per_tf_bar_age_sec; see
# Settings.bar_age_gate_for(tf) — single source of truth shared with trader.py.


class Scanner:
    """Multi-TF scanner with per-TF F1/F2/F3 filter injection."""

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._last_bar_ts: Dict[str, int] = {}  # {tf: last_seen_bar_start_ms}

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
        """True if a new bar has closed since last check."""
        prev_bar = self._prev_bar_start_ms(tf)
        last_seen = self._last_bar_ts.get(tf, 0)
        if prev_bar > last_seen:
            self._last_bar_ts[tf] = prev_bar
            log.debug("New bar closed: tf=%s bar_start_ms=%d", tf, prev_bar)
            return True
        return False

    def bar_age_sec(self, tf: str) -> float:
        """Seconds elapsed since the last closed bar opened (i.e. age since bar close).

        For 4h TF at 16:27 UTC: current bar started at 16:00, so age = 27min = 1620s.
        """
        ms = TF_MS.get(tf, 3_600_000)
        now_ms = int(time.time() * 1000)
        current_bar_start = (now_ms // ms) * ms
        return (now_ms - current_bar_start) / 1000.0

    def scan_all_coins(
        self,
        coins: list[AssetTier],
        client,                          # ExtendedClient instance
        open_positions: dict,            # {coin: Position} from in-memory state
    ) -> list[Signal]:
        """Scan all coins on all TFs where a new bar has closed.

        Per-TF F1/F2/F3 injected via settings.get_tf_filters(tf) before each TF scan.
        Returns list of new signals deduplicated against open_positions.
        """
        signals: list[Signal] = []
        s = self.cfg

        for tf in s.working_tfs:
            if not self.new_bar_closed(tf):
                continue

            age_sec = self.bar_age_sec(tf)
            # Per-TF gate owned by Settings — trader.py uses the same helper so
            # the two callers can never silently disagree (incident pre-2026-05-24).
            gate = s.bar_age_gate_for(tf)
            if age_sec > gate:
                log.warning(
                    "Bar too stale for %s: age=%.0fs > gate=%ds — skipping scan",
                    tf, age_sec, gate,
                )
                continue

            # Resolve per-TF F1/F2/F3 — FIRST-CLASS FEATURE, not afterthought
            tf_filters = s.get_tf_filters(tf)
            log.info(
                "Scanning %d coins on tf=%s (bar_age=%.0fs) "
                "f1=%.2f f2=%.1f f3=%.0f",
                len(coins), tf, age_sec,
                tf_filters.f1, tf_filters.f2, tf_filters.f3,
            )

            for asset in coins:
                sym = asset.symbol

                # Cross-TF dedup: skip if any position open on this coin
                if sym in open_positions:
                    log.debug(
                        "Cross-TF skip %s %s: position open on %s",
                        sym, tf, open_positions[sym].tf,
                    )
                    continue

                try:
                    df = client.candles(sym, tf, limit=300)
                except Exception as e:
                    log.warning("candles(%s, %s) failed: %s", sym, tf, e)
                    continue

                if df is None or len(df) < 100:
                    continue

                try:
                    df = compute_indicators(df)
                except Exception as e:
                    log.warning("compute_indicators(%s, %s) failed: %s", sym, tf, e)
                    continue

                signal = scan_for_signal(
                    df=df,
                    coin=sym,
                    tf=tf,
                    zigzag_length=s.zigzag_raw_length,
                    raw_rr_target=s.raw_rr_target,
                    require_ema50_up=s.require_ema50_up,
                    f1_min_dist_ema20_atr=tf_filters.f1,   # per-TF F1
                    f2_min_rsi14=tf_filters.f2,             # per-TF F2
                    f3_max_dollar_vol_usd=tf_filters.f3,    # per-TF F3
                    tf_max_sl=s.tf_max_sl,
                    min_sl_dist_pct=s.min_sl_dist_pct,
                )

                if signal is not None:
                    log.info(
                        "SIGNAL %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1_val=%.2f",
                        sym, tf, signal.trigger_price,
                        signal.sl_price, signal.tp1_price, signal.f1_dist,
                    )
                    signals.append(signal)

        return signals
