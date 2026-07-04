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
import os
import time
from typing import Dict, Optional

import pandas as pd
from bot.warmup_backfill import backfill_warmup

from bot.config import Settings, TF_MS, settings as global_settings
# COMBO DUAL-ROUTER 2026-06-19: ONE bot, ONE HL account, TWO legs.
#   (A) native crypto-perp coins  -> strategy_donchian (Donchian-8h breakout, ported uk_v10c)
#   (B) xyz_* HIP-3 tokenized stx -> strategy_us29     (US29 / F4_vb fresh-stack reclaim)
# Both modules export the IDENTICAL 7-name contract (Signal, compute_indicators,
# scan_for_signal, scan_for_short_signal, PositionManager, _estimate_tick, Position),
# so the only thing that differs per coin is WHICH module's functions we call. We import
# both modules whole and dispatch per-coin by symbol prefix (see _strat_for / route_*).
# XNN port 2026-06-10: strategy module repointed uk_v102 -> xnn (same contract).
from bot import strategy_us29 as _strat_us29
from bot import strategy_donchian as _strat_donchian
# Signal/compute_indicators are contract-identical across both modules; re-export the us29
# Signal type for isinstance/annotation back-compat (the donchian Signal is a structurally
# identical dataclass — callers only read attributes, never isinstance-gate on it).
from bot.strategy_us29 import Signal, compute_indicators
from bot.universe import AssetTier, REGIME_COINS

# ── COMBO router: pick the strategy module for a given coin ──────────────────────────────
# xyz_* (HIP-3 tokenized stocks, e.g. xyz_GOLD/xyz_VIX) -> us29 leg; everything else
# (native HL crypto perps, e.g. BTC/ETH/SOL) -> donchian leg. Explicit + minimal: a single
# prefix test. REGIME_COINS (xyz_VIX/xyz_DXY/xyz_SP500) never reach the entry scan (skipped
# upstream in scan_all_coins), so they don't need routing.
def _is_xyz(sym: str) -> bool:
    return isinstance(sym, str) and sym.startswith("xyz_")


def _strat_for(sym: str):
    """Return the strategy MODULE handling this coin (us29 for xyz_*, donchian otherwise)."""
    return _strat_us29 if _is_xyz(sym) else _strat_donchian


def _min_signal_bars_for(sym: str) -> int:
    """Per-LEG minimum-bars floor for the parity/warmup skip gate (SCOPE fix 2026-06-19).

    Audit MED: the parity-skip gate applied ONE global MIN_SIGNAL_BARS (210, set for the
    us29 leg which needs 201 closed bars) to EVERY coin/tf BEFORE per-leg routing. The
    donchian crypto leg only needs WARMUP_BARS = DONCHIAN_N+5 = 25 bars, so a native-perp
    with 25-209 bars of 8h history had a VALID Donchian breakout suppressed even though
    its leg was fully warmed. Route the floor per leg instead:
      - xyz_* (us29 leg)        -> MIN_SIGNAL_BARS        (env, default 210; needs 201)
      - native crypto (donchian)-> DONCHIAN_MIN_SIGNAL_BARS (env, default WARMUP_BARS=25)
    The gate still fails safe (suppress, never wrong-trade); this only RESTORES donchian
    coverage that the us29 floor was eating. WARMUP_BACKFILL mitigates for deep-source
    coins but does not cover native perps with no external history.
    """
    if _is_xyz(sym):
        return int(os.getenv("MIN_SIGNAL_BARS", "210"))
    return int(os.getenv("DONCHIAN_MIN_SIGNAL_BARS", str(_strat_donchian.WARMUP_BARS)))

# XNN port 2026-06-10: slow EMA pairs (377/610) + window-seed EMA convergence need a
# DEEP candle window — 300 bars is NOT enough (seed residual + pairs don't fit).
# HL candles_snapshot serves up to 5000 bars in one call. Default preserves legacy 300.
SCAN_CANDLES_LIMIT = int(os.getenv("SCAN_CANDLES_LIMIT", "300"))

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
        """True if a new bar has closed since last check (peek AND consume)."""
        prev_bar = self._prev_bar_start_ms(tf)
        last_seen = self._last_bar_ts.get(tf, 0)
        if prev_bar > last_seen:
            self._last_bar_ts[tf] = prev_bar
            log.debug("New bar closed: tf=%s bar_start_ms=%d", tf, prev_bar)
            return True
        return False

    def _new_bar_available(self, tf: str) -> bool:
        """Peek: a new closed bar exists since the last COMMITTED scan. Does NOT consume the
        latch — so a SCAN_BAR_DELAY_SEC decollide gate can defer the scan without dropping
        the bar (scandelay/latch fix 2026-06-20)."""
        return self._prev_bar_start_ms(tf) > self._last_bar_ts.get(tf, 0)

    def _commit_bar(self, tf: str) -> None:
        """Consume the latch — mark the just-closed bar as scanned (call only once the
        decollide + we are about to actually scan this bar)."""
        self._last_bar_ts[tf] = self._prev_bar_start_ms(tf)

    @staticmethod
    def _df_last_ts_ms(df) -> int:
        """ts (ms UTC) of the last row of a candles df; 0 if unavailable."""
        try:
            return int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        except Exception:
            return 0

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
        client,                          # exchange client
        open_positions: dict,            # {coin: Position} in-memory (this bot's managed positions)
        account_coins: Optional[set] = None,  # coins held on the SHARED exchange account (a/b hedge guard)
        no_long_symbols: set | None = None,
        tfs: set | None = None,           # LEG-SPLIT (2026-06-29): restrict scan to this TF subset (None = all)
        on_crypto_signal=None,            # STREAM (2026-07-02): called with each non-xyz signal the instant it is detected (enter-on-detect); xyz never passed.
    ) -> list[Signal]:
        """Scan coins on each TF whose bar just closed — LONG + SHORT.

        TF set = UNION(working_tfs, short_enabled_tfs). Per TF:
          - LONG scan if tf in working_tfs (per-TF F1/F2/F3 via get_tf_filters)
          - SHORT scan if short_enabled_for(tf) (per-TF via get_tf_short_filters)

        Skip a coin this scan if it is:
          - already managed by THIS bot (open_positions), OR
          - held on the shared account by EITHER bot (account_coins — prevents a/b self-hedge), OR
          - already produced a signal this scan (cross-side dedup; LONG has precedence).
        """
        signals: list[Signal] = []
        s = self.cfg
        account_coins = account_coins or set()
        no_long = no_long_symbols or set()
        signaled_coins: set = set()

        all_scan_tfs = sorted(
            set(s.working_tfs) | set(s.short_enabled_tfs),
            key=lambda t: TF_MS.get(t, 0),
        )
        if tfs is not None:                                  # LEG-SPLIT: scan only this leg's TFs
            all_scan_tfs = [t for t in all_scan_tfs if t in tfs]

        for tf in all_scan_tfs:
            if not self._new_bar_available(tf):       # peek — does NOT consume the latch
                continue

            age_sec = self.bar_age_sec(tf)
            # DECOLLIDE (scandelay fix 2026-06-20): wait until the bar is SCAN_BAR_DELAY_SEC
            # old before scanning so this bot's top-of-bar candle burst lands AFTER the
            # co-located hl-xnn / hl-us29 bots clear theirs on the SHARED IP (429 storm).
            # The gate sits BEFORE _commit_bar, so a too-early tick is re-checked next loop
            # and the bar is NOT silently dropped. Bounded above by the stale gate below
            # (delay 600s ≪ 4h/8h/1d gates 7200/14400/43200s), so the scan window always exists.
            _delay = int(os.getenv("SCAN_BAR_DELAY_SEC", "0"))
            if _delay > 0 and age_sec < _delay:
                continue

            # Commit the latch — we are scanning this bar now (once per bar).
            self._commit_bar(tf)

            # Per-TF gate owned by Settings — trader.py uses the same helper so
            # the two callers can never silently disagree (incident pre-2026-05-24).
            gate = s.bar_age_gate_for(tf)
            if age_sec > gate:
                log.warning(
                    "Bar too stale for %s: age=%.0fs > gate=%ds — skipping scan",
                    tf, age_sec, gate,
                )
                continue

            # silent-429-drop fix (2026-06-20): track coins dropped this bar because their
            # candles came back empty from a 429/fetch-exhaustion (NOT young listings) so a
            # 429 storm that darks the scan is LOUD + summarized, never a silent disable.
            _dropped_fail: list[str] = []

            do_long = tf in s.working_tfs
            do_short = s.short_enabled_for(tf)
            if not (do_long or do_short):
                continue

            lf = s.get_tf_filters(tf)
            sf = s.get_tf_short_filters(tf)
            log.info(
                "Scanning %d coins tf=%s (age=%.0fs) "
                "long=%s filt_thr[mindist_ema20=%.2f minRSI=%.1f volcap_usd=%.0f|0=off,NOT counts] short=%s filt_thr[mindist_ema20=%.2f maxRSI=%.1f volcap_usd=%.0f|0=off,NOT counts]",
                len(coins), tf, age_sec,
                do_long, lf.f1, lf.f2, lf.f3,
                do_short, sf.f1, sf.f2, sf.f3,
            )

            for asset in coins:
                sym = asset.symbol

                # B3 (2026-06-19): regime-indicator coins (xyz_VIX/xyz_DXY/xyz_SP500) are
                # force-included in the universe for context/regime ONLY and must NEVER be
                # entered. They carry note='regime_indicator'/'regime_indicator_forced' AND
                # are in universe.REGIME_COINS. Skip them from the ENTRY scan entirely (they
                # still load as candles elsewhere for the regime gate). Without this, a
                # long-only fresh-stack-reclaim would emit a real LONG on a regime coin.
                if getattr(asset, "note", "") in ("regime_indicator", "regime_indicator_forced") \
                        or sym in REGIME_COINS:
                    continue

                # Dedup: this bot's open, account-held (either bot), or already signaled this scan
                if sym in open_positions or sym in account_coins or sym in signaled_coins:
                    continue

                # COMBO TF-route skip (2026-06-23): fetch only TFs this coin's leg trades.
                # donchian(crypto)->DONCHIAN_TFS, us29(xyz_)->US29_TF_CONFIG; strategy returns
                # None off-TF anyway -> skipping the hollow fetch is behavior-identical.
                if not do_short:
                    if _is_xyz(sym):
                        if _strat_us29.US29_TF_CONFIG.get(tf) is None:
                            continue
                    elif tf not in _strat_donchian.DONCHIAN_TFS:
                        continue

                try:
                    # CHANGE 2 (2026-06-23): donchian crypto leg fetches 8h bars from
                    # Binance USD-M futures first (lowest global latency, removes load
                    # from HL's rate-limited candles endpoint; bar alignment verified:
                    # both use UTC 00/08/16 boundaries). xyz_ stocks have no Binance
                    # USDT-M equivalent -> always use HL. On ANY Binance error/unknown
                    # symbol the fallback path below calls HL candles() as before.
                    _use_binance = (
                        not _is_xyz(sym)
                        and tf in ("8h",)  # donchian leg only; expand if needed
                        and hasattr(client, "binance_candles")
                    )
                    df = None
                    if _use_binance:
                        try:
                            df = client.binance_candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                        except Exception as _be:
                            log.debug("binance_candles(%s, %s) failed (%s) — falling back to HL", sym, tf, _be)
                            df = None
                    if df is None:
                        df = client.candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                except Exception as e:
                    log.warning("candles(%s, %s) failed: %s", sym, tf, e)
                    continue

                # Pre-backfill sanity floor — PER LEG (SCOPE fix 2026-06-19). The generic
                # < 100 floor predated the combo dual-router and would drop a donchian
                # crypto coin with 25-99 raw bars BEFORE backfill+the per-leg parity gate
                # could admit it (donchian needs only WARMUP_BARS=25). Cap the pre-floor at
                # the leg's own min-bars so it never exceeds the authoritative gate below;
                # the us29 leg keeps the 100 floor (its real floor is 210 anyway).
                _pre_floor = min(100, _min_signal_bars_for(sym))
                if df is None or len(df) < _pre_floor:
                    # silent-429-drop fix (2026-06-20): an empty/short df from a 429-exhausted
                    # fetch (fail-cached) means the coin is INVISIBLE to the live scan = silent
                    # disable. Distinguish it from a genuine young listing (handled by the
                    # PARITY-SKIP gate) and make it LOUD (once per coin) + summarized per scan.
                    if hasattr(client, "candles_in_fail_cache") and client.candles_in_fail_cache(sym, tf):
                        _dropped_fail.append(sym)
                        if not hasattr(self, "_scan_drop_logged"):
                            self._scan_drop_logged = set()
                        if (sym, tf) not in self._scan_drop_logged:
                            self._scan_drop_logged.add((sym, tf))
                            log.warning(
                                "SCAN-DROP %s %s: candles empty (429/fetch-exhausted, "
                                "fail-cached) — coin INVISIBLE to scan until cache clears",
                                sym, tf,
                            )
                    continue
                # PARITY GUARD (2026-06-12): an under-seeded indicator emits a signal that
                # DIVERGES from the full-history backtest, so skip (coin,tf) until enough
                # bars accrue. memory project_pacifica_1d_ema610_starved_listing_age.
                # SCOPE fix (2026-06-19): route the min-bars floor PER LEG (_min_signal_bars_for)
                # instead of one global value. The us29 leg needs 201 closed bars
                # (MIN_SIGNAL_BARS=210); the donchian crypto leg needs only WARMUP_BARS=25.
                # A single global floor (=210) silently suppressed VALID Donchian breakouts on
                # native perps with 25-209 bars of 8h history. Floor is chosen by the SAME
                # prefix test the router uses below, so gate + routing can never disagree.
                _min_sig_bars = _min_signal_bars_for(sym)
                df = backfill_warmup(df, sym, tf, _min_sig_bars)
                if len(df) < _min_sig_bars:
                    if not hasattr(self, "_parity_skipped"):
                        self._parity_skipped = set()
                    if (sym, tf) not in self._parity_skipped:
                        self._parity_skipped.add((sym, tf))
                        log.warning(
                            "PARITY SKIP %s %s: %d bars < %d (per-leg warmup floor) — "
                            "insufficient history; skipping until bars accrue (signal != bt "
                            "otherwise)",
                            sym, tf, len(df), _min_sig_bars,
                        )
                    continue

                # FRESHNESS GUARD (review fix 2026-06-10): the decision bar MUST be the
                # bar whose close triggered this scan (self._last_bar_ts[tf]). Cache
                # offset-jitter / a lagging API can serve a df ending at bar N-1 — the
                # strategy would then evaluate counted_now on the WRONG bar and bar N's
                # signal is lost forever (scan runs once per bar). Force a refetch past
                # the cache; if still stale, SKIP LOUD (never decide on N-1 silently).
                _expected_ts = self._last_bar_ts.get(tf, 0)
                _last_ts = self._df_last_ts_ms(df)
                if _expected_ts and _last_ts and _last_ts < _expected_ts:
                    if hasattr(client, "invalidate_candles_cache"):
                        client.invalidate_candles_cache(sym, tf)
                        try:
                            # CHANGE 2 (2026-06-23): Binance-first for crypto refetch too.
                            # Binance has no stale-cache issue (no per-coin offset jitter),
                            # so a freshness refetch from Binance is always immediate.
                            _use_binance_rf = (
                                not _is_xyz(sym)
                                and tf in ("8h",)
                                and hasattr(client, "binance_candles")
                            )
                            df = None
                            if _use_binance_rf:
                                try:
                                    df = client.binance_candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                                except Exception as _be2:
                                    log.debug("binance_candles refetch(%s, %s) failed (%s) — HL fallback", sym, tf, _be2)
                                    df = None
                            if df is None:
                                df = client.candles(sym, tf, limit=SCAN_CANDLES_LIMIT)
                        except Exception as e:
                            log.warning("fresh refetch candles(%s, %s) failed: %s", sym, tf, e)
                            continue
                        _last_ts = self._df_last_ts_ms(df)
                    # Per-leg floor (SCOPE fix 2026-06-19): same reasoning as the pre-floor
                    # above — don't drop a valid donchian refetch (25-99 raw bars) on a us29
                    # floor. The leg's scan_for_signal still enforces its own warmup.
                    _refetch_floor = min(100, _min_signal_bars_for(sym))
                    if df is None or len(df) < _refetch_floor or (_last_ts and _last_ts < _expected_ts):
                        log.warning(
                            "STALE candles %s %s: last closed bar %s < expected %s "
                            "after forced refetch — skipping this bar (parity guard)",
                            sym, tf, _last_ts, _expected_ts,
                        )
                        continue

                # COMBO router: choose the leg's strategy module for THIS coin.
                #   xyz_* -> strategy_us29 (stocks leg) ; native crypto -> strategy_donchian.
                strat = _strat_for(sym)

                try:
                    df = strat.compute_indicators(df)
                except Exception as e:
                    log.warning("compute_indicators(%s, %s) failed: %s", sym, tf, e)
                    continue

                # LONG first (precedence). If it fires, skip SHORT for this coin.
                # Both legs are long-only (donchian.scan_for_short_signal / us29.scan_for_short_signal
                # both return None), but the SHORT path is preserved verbatim for contract parity.
                signal = None
                if do_long and sym not in no_long:
                    signal = strat.scan_for_signal(
                        df=df, coin=sym, tf=tf,
                        donchian_k=lf.donchian_k,
                        raw_rr_target=s.raw_rr_target,
                        require_ema50_up=s.require_ema50_up,
                        f1_min_dist_ema20_atr=lf.f1,
                        f2_min_rsi14=lf.f2,
                        f3_max_dollar_vol_usd=lf.f3,
                        tf_max_sl=s.tf_max_sl,
                        min_sl_dist_pct=s.min_sl_dist_pct,
                    )
                if signal is None and do_short:
                    signal = strat.scan_for_short_signal(
                        df=df, coin=sym, tf=tf,
                        donchian_k=sf.donchian_k,  # SHORT-specific K (TF_<TF>_SHORT_K); may differ from long
                        raw_rr_target=s.raw_rr_target,
                        require_ema50_down=s.require_ema50_down,
                        f1_min_dist_ema20_atr=sf.f1,
                        f2_max_rsi14=sf.f2,
                        f3_max_dollar_vol_usd=sf.f3,
                        tf_max_sl=s.tf_max_sl,
                        min_sl_dist_pct=s.min_sl_dist_pct,
                    )

                if signal is not None:
                    log.info(
                        "SIGNAL %s %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1_val=%.2f",
                        sym, tf, signal.side, signal.trigger_price,
                        signal.sl_price, signal.tp1_price, signal.f1_dist,
                    )
                    signals.append(signal)
                    signaled_coins.add(sym)
                    # STREAM (2026-07-02): enter crypto (pass-through) signals the instant
                    # they are detected, not after the full universe walk. xyz NEVER streamed
                    # (topM needs the whole list). Downstream of the freshness guard, so this
                    # is a just-closed-bar signal by construction. LOUD on failure (entry may be lost).
                    if on_crypto_signal is not None and not _is_xyz(sym):
                        try:
                            on_crypto_signal(signal)
                        except Exception as _cbe:
                            log.error("on_crypto_signal(%s) callback error (entry may be lost): %s", sym, _cbe, exc_info=True)

            if _dropped_fail:
                log.error(
                    "SCAN COVERAGE GAP tf=%s: %d/%d coins dropped on candles fetch-failure "
                    "(429/exhausted) — NOT scanned this bar: %s",
                    tf, len(_dropped_fail), len(coins),
                    ",".join(sorted(_dropped_fail)[:20]) + ("…" if len(_dropped_fail) > 20 else ""),
                )

        return signals
