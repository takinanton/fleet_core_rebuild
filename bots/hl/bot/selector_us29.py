"""selector_us29.py — canonical US29 F4_vb selection layer (replaces the legacy TopKPool).

This is the CANONICAL deployed-edge selector. The bt-1 winning cell is F4_vb
(scripts/emit_us29_stocks.py @08aada7, us29_causal_sweep F4_vb):

  sel = topm  M=3     : of the day's candidate signals, keep ONLY the top-3 by
                        (-score, coin) where score = (close-EMA20)/WilderATR14 at the
                        signal bar. NO expanding-percentile history threshold (the legacy
                        pool30 cut — which emit_us29_stocks flags as a PARITY FAIL,
                        14.65%/DD83.3 — is REMOVED, not gated).
  regime = spy200     : block ALL new entries when the index close < SMA200 at the signal
                        close. F4_vb uses SPY; the bot uses xyz_SP500 (the HL HIP-3 S&P500
                        index perp, a REGIME coin that is NEVER traded) as the proxy. SMA200
                        is computed CAUSALLY (cumsum) from the index 1d closes. Pre-history
                        (fewer than 200 bars) -> regime ON (the sim's labeled caveat).
                        When the gate is OFF, ZERO new entries are emitted; held positions
                        keep trailing bot-side (the gate never exits a position).

Mirrors scripts/emit_us29_stocks.py: regime_at() (causal cumsum SMA200, searchsorted) and
select_kept() (cands_sorted[:top_m] if regime_on else []).

Env knobs (parsed in main.py and passed in):
  US29_TOP_M=3            top-M of the day
  US29_REGIME_GATE=1      enable the index-SMA200 gate
  US29_REGIME_COIN=xyz_SP500   regime proxy coin (must be a non-traded REGIME coin)
  US29_REGIME_SMA_N=200   SMA window
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

REGIME_SMA_N_DEFAULT = 200

import os

# Regime backfill (2026-06-20): prepend deep, scale-aligned, 24/7-ffilled S&P500 history so
# SMA200 is computable on the YOUNG HIP-3 proxy (xyz_SP500 ~94 venue bars < 200 -> SMA200
# uncomputable -> compute_regime pins regime ON, gate inert). Same local dir warmup uses.
_REGIME_BF_DIR = os.getenv(
    "WARMUP_LOCAL_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "data", "xyz_backfill"),
)
_REGIME_BF_ON = os.getenv("US29_REGIME_BACKFILL", "0").strip().lower() in ("1", "true", "yes", "on")


def splice_regime_backfill(df, coin, sma_n, local_dir=None):
    """Prepend scale-aligned 24/7-ffilled index history so SMA(sma_n) is computable on a young
    HIP-3 regime proxy. Reads <local_dir>/1d/<coin>.{parquet,csv.gz}. Two guards mirror
    warmup_backfill (fail-safe: any miss/guard-fail/error returns the RAW venue df -> the
    pre-existing inert-ON behaviour, never a wrong-scale regime that could force-exit a long)."""
    try:
        if df is None or len(df) == 0 or "Close" not in df:
            return df
        if len(df) >= sma_n:                       # enough venue history already -> no-op
            return df
        base = os.path.join(local_dir or _REGIME_BF_DIR, "1d", coin)
        path = None
        for ext in (".parquet", ".csv.gz"):
            if os.path.exists(base + ext):
                path = base + ext
                break
        if path is None:
            log.warning("US29 REGIME backfill: no file %s.{parquet,csv.gz} — raw venue df "
                        "(SMA%d stays uncomputable, gate inert ON)", base, sma_n)
            return df
        bf = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
        cols = {c.lower(): c for c in bf.columns}
        if not all(k in cols for k in ("ts", "close")):
            return df
        bf = bf.sort_values(cols["ts"])
        own_first_ms = int(pd.Timestamp(df["time"].iloc[0]).value // 10**6)
        own_recent = float(pd.Series([float(x) for x in df["Close"].iloc[-5:]]).median())
        src_recent = float(pd.Series([float(x) for x in bf[cols["close"]].iloc[-5:]]).median())
        ratio = (src_recent / own_recent) if own_recent > 0 else 0.0
        if not (0.9 <= ratio <= 1.111):
            log.warning("US29 REGIME backfill REJECT %s: src %.2f vs venue %.2f ratio=%.3f "
                        "(wrong scale/asset) — raw venue df", coin, src_recent, own_recent, ratio)
            return df
        tail = bf[bf[cols["ts"]] < own_first_ms]
        if len(tail) == 0:
            return df
        tf_ms = 86400000

        def _gf(ms):
            ms = sorted(int(x) for x in ms)
            return (sum(1 for i in range(1, len(ms)) if ms[i] - ms[i - 1] > 1.5 * tf_ms)
                    / (len(ms) - 1)) if len(ms) > 2 else 0.0
        own_gap = _gf(int(pd.Timestamp(t).value // 10**6) for t in df["time"])
        tail_gap = _gf(tail[cols["ts"]].tolist())
        if abs(own_gap - tail_gap) > 0.03:
            log.warning("US29 REGIME backfill REJECT %s: cadence venue=%.3f tail=%.3f "
                        "(session-gapped vs 24/7) — raw venue df", coin, own_gap, tail_gap)
            return df
        bft = pd.DataFrame({
            "time": pd.to_datetime(tail[cols["ts"]], unit="ms", utc=True).astype(df["time"].dtype),
            "Open": tail[cols.get("open", cols["close"])].astype(float),
            "High": tail[cols.get("high", cols["close"])].astype(float),
            "Low": tail[cols.get("low", cols["close"])].astype(float),
            "Close": tail[cols["close"]].astype(float),
            "Volume": (tail[cols["volume"]].astype(float) if "volume" in cols else 0.0),
        })
        out = (pd.concat([bft, df], ignore_index=True)
               .drop_duplicates(subset="time", keep="last")
               .sort_values("time").reset_index(drop=True))
        log.info("US29 REGIME backfill %s: %d venue + %d hist -> %d bars (SMA%d computable)",
                 coin, len(df), len(out) - len(df), len(out), sma_n)
        return out
    except Exception as e:
        log.warning("US29 REGIME backfill(%s) failed: %s — raw venue df", coin, e)
        return df


def _score(s) -> float:
    """Rank score (close-EMA20)/ATR14, stored on Signal.f1_dist. NaN/missing -> -1e9
    (mirrors emit_us29_stocks: NaN trend_strength -> -1e9)."""
    try:
        v = float(getattr(s, "f1_dist", float("nan")))
        return v if np.isfinite(v) else -1e9
    except Exception:
        return -1e9


def compute_regime(
    df: Optional[pd.DataFrame],
    sma_n: int = REGIME_SMA_N_DEFAULT,
) -> tuple[bool, Optional[float], Optional[float]]:
    """Causal SPY>SMA200 regime from the regime-coin 1d candles df.

    Returns (regime_on, index_close, index_sma200).
      regime_on = index_close > SMA200 at the LAST closed bar.
      Pre-history (n < sma_n) -> ON (True), index_sma200 None (bt labeled caveat).
      df None/empty -> ON (fail-OPEN so a missing regime feed never silently halts trading;
        the same convention the bt uses for pre-SPY history). Logged by caller.

    SMA200 is a simple trailing mean of the last `sma_n` closes (cumsum, like the bt mirror).
    """
    if df is None or len(df) == 0 or "Close" not in df:
        return True, None, None
    cl = df["Close"].to_numpy(dtype=float)
    n = cl.shape[0]
    last_close = float(cl[-1])
    if n < sma_n:
        # pre-history: regime ON (labeled caveat — bt regime_at returns ON when k<0)
        return True, last_close, None
    sma200 = float(np.mean(cl[n - sma_n:n]))
    return (last_close > sma200), last_close, sma200


class RegimeGate:
    """Caches the regime-coin 1d candles + computed regime; refreshes at most every
    `refresh_sec`. block_new_entries() returns True when the index is BELOW its SMA200."""

    def __init__(
        self,
        client,
        regime_coin: str = "xyz_SP500",
        sma_n: int = REGIME_SMA_N_DEFAULT,
        candles_limit: int = 3000,
        refresh_sec: float = 3600.0,
    ):
        self.client = client
        self.regime_coin = regime_coin
        self.sma_n = int(sma_n)
        self.candles_limit = int(candles_limit)
        self.refresh_sec = float(refresh_sec)
        self._last_ts: float = 0.0
        self._on: bool = True
        self._close: Optional[float] = None
        self._sma: Optional[float] = None

    def _refresh(self) -> None:
        try:
            df = self.client.candles(self.regime_coin, "1d", limit=self.candles_limit)
        except Exception as e:
            log.warning(
                "US29 REGIME: candles(%s,1d) failed: %s — regime ON (fail-open this cycle)",
                self.regime_coin, e,
            )
            self._on, self._close, self._sma = True, None, None
            self._last_ts = time.time()
            return
        if _REGIME_BF_ON:
            df = splice_regime_backfill(df, self.regime_coin, self.sma_n)
        on, close, sma = compute_regime(df, self.sma_n)
        if df is None or len(df) == 0:
            log.warning(
                "US29 REGIME: %s 1d candles empty — regime ON (fail-open); index gate "
                "cannot block entries until the regime feed returns", self.regime_coin,
            )
        # REGIME-INERT loud guard (panel must-fix #5, 2026-06-21): the gate resolved ON but with
        # NO real SMA200 (sma is None) while candles DID return — i.e. pre-history / backfill
        # ineffective (<sma_n bars after splice). Observable behaviour is IDENTICAL to a healthy
        # bull-gate, so an operator watching a clean log during a real bear would believe bear
        # protection is active when it is ABSENT (both entry-block AND exit-on-flip inert). Make it
        # LOUD: ERROR once on entry into the inert state, WARNING each refresh while it persists,
        # WARNING on recovery. Add-only; does NOT change the gate decision (still fail-open ON).
        _nbars = len(df) if df is not None else 0
        _inert = (on is True and sma is None and _nbars > 0)
        if _inert:
            if not getattr(self, "_inert_logged", False):
                log.error(
                    "REGIME-INERT %s: gate pinned ON with NO SMA%d (only %d 1d bars < %d; "
                    "pre-history/backfill ineffective) — BEAR PROTECTION ABSENT: entry-block AND "
                    "exit-on-flip BOTH inert until history/backfill reaches %d bars. MANUAL REVIEW.",
                    self.regime_coin, self.sma_n, _nbars, self.sma_n, self.sma_n,
                )
                self._inert_logged = True
            else:
                log.warning("REGIME-INERT %s: still inert (%d < %d 1d bars) — bear protection ABSENT",
                            self.regime_coin, _nbars, self.sma_n)
        else:
            if getattr(self, "_inert_logged", False):
                log.warning("REGIME-RECOVERED %s: SMA%d now computable (%d bars) — gate ARMED",
                            self.regime_coin, self.sma_n, _nbars)
            self._inert_logged = False
        self._on, self._close, self._sma = on, close, sma
        self._last_ts = time.time()

    def state(self) -> tuple[bool, Optional[float], Optional[float]]:
        if time.time() - self._last_ts > self.refresh_sec:
            self._refresh()
        return self._on, self._close, self._sma

    def block_new_entries(self) -> bool:
        """True when the index regime is OFF (close < SMA200) -> emit ZERO new entries."""
        on, _close, _sma = self.state()
        return not on


def select_topm(
    signals: list,
    top_m: int,
    regime_on: bool,
) -> list:
    """F4_vb selection: sort candidates by (-score, coin), keep top-M ONLY when regime is ON;
    ZERO when regime is OFF. Mirrors emit_us29_stocks.select_kept(selector='topm')."""
    if not signals:
        return []
    if not regime_on:
        return []
    ordered = sorted(signals, key=lambda s: (-_score(s), getattr(s, "coin", "")))
    return ordered[: max(0, int(top_m))]
