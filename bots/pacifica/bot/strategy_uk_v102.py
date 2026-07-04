"""strategy_uk_v102.py — UK v102 ZigZag breakout strategy, BIDIRECTIONAL (long + short).

Source files (verified on bt-1 2026-05-24/27):
  /root/hl-backtest/strategies/uk_v75_zigzag_raw.py   — core entry logic (long)
  /root/hl-backtest/strategies/uk_v84_optimal.py       — trail-after-TP exit
  /root/hl-backtest/strategies/uk_v102_ib_filtered.py  — F1/F2/F3 filters (long)
  /root/hl-backtest/strategies/uk_v102_short.py        — short-side mirror (2026-05-27)

LONG entry (literal port of uk_v75_zigzag_raw.py maybe_enter):
  1. Close > EMA50 (require_ema50_up filter)
  2. Find last ZigZag pivot high (length=5 window)
  3. Find last ZigZag pivot low
  4. pivot_high must be ABOVE pivot_low (uptrend context)
  5. Close > pivot_high → trigger breakout
  6. entry = pivot_high + tick_size (stop-buy trigger)
  7. SL = pivot_low - tick_size
  8. SL bounds: min_sl_dist_pct=0.5% .. max_sl_dist_pct per TF (4h=5%, 1d=10%)
  9. TP1 = entry + 1.5 × (entry - SL)

SHORT entry (MIRROR of LONG, added 2026-05-27 — bt-1 backtest validated +0.82/+1.25/+2.03R per TF):
  1. Close < EMA50 (require_ema50_down filter)
  2. Find last ZigZag pivot high (same)
  3. Find last ZigZag pivot low (same)
  4. pivot_low must be BELOW pivot_high (downtrend context — same as uptrend symmetric)
  5. Close < pivot_low → trigger breakdown
  6. entry = pivot_low - tick_size (stop-sell trigger)
  7. SL = pivot_high + tick_size
  8. SL bounds: same per-TF caps
  9. TP1 = entry - 1.5 × (SL - entry)

F1 LONG: (close - ema20) / atr14 >= thr (close ABOVE ema20)
F1 SHORT: (ema20 - close) / atr14 >= thr (close BELOW ema20)
F2 LONG: rsi14 >= thr (overbought confirmation)
F2 SHORT: rsi14 <= thr (oversold confirmation; param name f2_max_rsi14)
F3 SHARED: close*volume <= thr (skip mega-caps; symmetric)
0 = disabled.

Trail-after-TP (uk_v84_optimal.py TRAIL_TP_TFS = {1d, 8h}):
  LONG: After TP1 → SL = entry + buffer; trail UP on recent_low − tick; ratchet UP.
  SHORT: After TP1 → SL = entry − buffer; trail DOWN on recent_high + tick; ratchet DOWN.
  max_run_r cap symmetric.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)


@dataclass
class Signal:
    coin: str
    tf: str
    side: str                    # "long" | "short"
    trigger_price: float         # pivot high (long) | pivot low (short) — breakout level
    entry_price: float           # trigger ± tick (stop-limit price)
    sl_price: float              # pivot low − tick (long) | pivot high + tick (short)
    tp1_price: float             # entry ± 1.5R depending on side
    sl_dist_pct: float           # |entry − sl| / entry
    pivot_high: float
    pivot_low: float
    bar_ts: int                  # timestamp of signal bar (ms UTC)
    atr14: float
    ema20: float
    f1_dist: float               # (close − ema20)/atr14 for long; (ema20 − close)/atr14 for short


@dataclass
class Position:
    coin: str
    tf: str
    entry_price: float
    sl_initial: float
    sl_current: float
    tp1_price: float
    size: float                  # base asset units (always positive — side stored separately)
    bar_entry_idx: int
    side: str = "long"           # "long" | "short"
    tp1_hit: bool = False        # tp1 (161.8% fib) reached → 50% booked
    tp1_partial_done: bool = False  # 50% reduce-only fill confirmed
    trail_sl: Optional[float] = None


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute EMA20, EMA50, ATR14 on a OHLCV DataFrame."""
    df = df.copy()
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(span=14, adjust=False).mean()
    return df


# ---------------------------------------------------------------------------
# LONG scan (unchanged from prior v102 port — preserved for parity with bt-1)
# ---------------------------------------------------------------------------
def scan_for_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    zigzag_length: int,
    raw_rr_target: float,
    require_ema50_up: bool,
    f1_min_dist_ema20_atr: float,
    tf_max_sl: dict,
    min_sl_dist_pct: float,
    f2_min_rsi14: float = 0.0,
    f3_max_dollar_vol_usd: float = 0.0,
) -> Optional[Signal]:
    """LONG scan — see module docstring."""
    if len(df) < 100:
        return None
    i = len(df) - 1
    close_i = float(df["Close"].iloc[i])
    ema50_i = float(df["ema50"].iloc[i])
    ema20_i = float(df["ema20"].iloc[i])
    atr14_i = float(df["atr14"].iloc[i])

    if require_ema50_up and close_i <= ema50_i:
        return None

    if f1_min_dist_ema20_atr > 0:
        if atr14_i <= 0 or ema20_i <= 0:
            return None
        f1_dist = (close_i - ema20_i) / atr14_i
        if f1_dist < f1_min_dist_ema20_atr:
            return None
    else:
        f1_dist = 0.0

    if f2_min_rsi14 > 0:
        rsi14_i = _compute_rsi14_last(df)
        if rsi14_i is not None and rsi14_i < f2_min_rsi14:
            return None

    if f3_max_dollar_vol_usd > 0:
        vol_i = float(df["Volume"].iloc[i])
        if vol_i * close_i > f3_max_dollar_vol_usd:
            return None

    last_pivot_high, last_pivot_low = _find_zigzag_pivots(df, i, zigzag_length)
    if last_pivot_high is None or last_pivot_low is None:
        return None
    if last_pivot_high[1] <= last_pivot_low[1]:
        return None

    trigger = last_pivot_high[1]
    if close_i <= trigger:
        return None

    tick_size = _estimate_tick(close_i)
    entry = trigger + tick_size
    open_i = float(df["Open"].iloc[i])
    if open_i > entry:
        entry = open_i
    sl = last_pivot_low[1] - tick_size
    if sl >= entry:
        return None

    sl_dist = entry - sl
    sl_dist_pct = sl_dist / entry
    max_sl_pct = tf_max_sl.get(tf, 0.10)
    if sl_dist_pct < min_sl_dist_pct or sl_dist_pct > max_sl_pct:
        return None

    tp1 = entry + raw_rr_target * sl_dist
    try:
        bar_ts = int(df["time"].iloc[i].value // 10**6)
    except Exception:
        bar_ts = 0

    return Signal(
        coin=coin, tf=tf, side="long",
        trigger_price=trigger, entry_price=entry, sl_price=sl, tp1_price=tp1,
        sl_dist_pct=sl_dist_pct, pivot_high=last_pivot_high[1], pivot_low=last_pivot_low[1],
        bar_ts=bar_ts, atr14=atr14_i, ema20=ema20_i, f1_dist=f1_dist,
    )


# ---------------------------------------------------------------------------
# SHORT scan (new 2026-05-27 — bt-1 backtest: uk_v102_short.py, validated configs)
# ---------------------------------------------------------------------------
def scan_for_short_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    zigzag_length: int,
    raw_rr_target: float,
    require_ema50_down: bool,
    f1_min_dist_ema20_atr: float,
    tf_max_sl: dict,
    min_sl_dist_pct: float,
    f2_max_rsi14: float = 0.0,
    f3_max_dollar_vol_usd: float = 0.0,
) -> Optional[Signal]:
    """SHORT scan — mirror of scan_for_signal."""
    if len(df) < 100:
        return None
    i = len(df) - 1
    close_i = float(df["Close"].iloc[i])
    ema50_i = float(df["ema50"].iloc[i])
    ema20_i = float(df["ema20"].iloc[i])
    atr14_i = float(df["atr14"].iloc[i])

    if require_ema50_down and close_i >= ema50_i:
        return None

    if f1_min_dist_ema20_atr > 0:
        if atr14_i <= 0 or ema20_i <= 0:
            return None
        f1_dist = (ema20_i - close_i) / atr14_i  # MIRROR: distance BELOW ema20
        if f1_dist < f1_min_dist_ema20_atr:
            return None
    else:
        f1_dist = 0.0

    if f2_max_rsi14 > 0:
        rsi14_i = _compute_rsi14_last(df)
        if rsi14_i is not None and rsi14_i > f2_max_rsi14:  # MIRROR: max not min
            return None

    if f3_max_dollar_vol_usd > 0:
        vol_i = float(df["Volume"].iloc[i])
        if vol_i * close_i > f3_max_dollar_vol_usd:  # symmetric
            return None

    last_pivot_high, last_pivot_low = _find_zigzag_pivots(df, i, zigzag_length)
    if last_pivot_high is None or last_pivot_low is None:
        return None
    # Downtrend context: latest pivot LOW must be BELOW latest pivot HIGH (same as uptrend structure)
    if last_pivot_low[1] >= last_pivot_high[1]:
        return None

    # Breakdown trigger: close < pivot_low (MIRROR of breakout: close > pivot_high)
    trigger = last_pivot_low[1]
    if close_i >= trigger:
        return None

    tick_size = _estimate_tick(close_i)
    entry = trigger - tick_size
    # If bar opened BELOW entry (gap down) → entry = open (MIRROR of gap-up handling)
    open_i = float(df["Open"].iloc[i])
    if open_i < entry:
        entry = open_i
    sl = last_pivot_high[1] + tick_size  # MIRROR: above pivot high
    if sl <= entry:
        return None

    sl_dist = sl - entry
    sl_dist_pct = sl_dist / entry
    max_sl_pct = tf_max_sl.get(tf, 0.10)
    if sl_dist_pct < min_sl_dist_pct or sl_dist_pct > max_sl_pct:
        return None

    tp1 = entry - raw_rr_target * sl_dist  # MIRROR: TP below entry
    try:
        bar_ts = int(df["time"].iloc[i].value // 10**6)
    except Exception:
        bar_ts = 0

    return Signal(
        coin=coin, tf=tf, side="short",
        trigger_price=trigger, entry_price=entry, sl_price=sl, tp1_price=tp1,
        sl_dist_pct=sl_dist_pct, pivot_high=last_pivot_high[1], pivot_low=last_pivot_low[1],
        bar_ts=bar_ts, atr14=atr14_i, ema20=ema20_i, f1_dist=f1_dist,
    )


def _find_zigzag_pivots(df: pd.DataFrame, i: int, L: int):
    """Return (last_pivot_high, last_pivot_low) tuples (idx, price) or (None, None)."""
    last_pivot_high = None
    last_pivot_low = None
    for j in range(i - L - 1, max(0, i - 200), -1):
        left = j - L
        right = j + L + 1
        if left < 0 or right > len(df):
            continue
        seg_h = df["High"].iloc[left:right].values
        seg_l = df["Low"].iloc[left:right].values
        if len(seg_h) == 0:
            continue
        if last_pivot_high is None and float(df["High"].iloc[j]) == float(np.max(seg_h)):
            last_pivot_high = (j, float(df["High"].iloc[j]))
        if last_pivot_low is None and float(df["Low"].iloc[j]) == float(np.min(seg_l)):
            last_pivot_low = (j, float(df["Low"].iloc[j]))
        if last_pivot_high is not None and last_pivot_low is not None:
            break
    return last_pivot_high, last_pivot_low


def _compute_rsi14_last(df: pd.DataFrame) -> Optional[float]:
    try:
        close = df["Close"]
        if len(close) < 15:
            return None
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(com=13, adjust=False).mean()
        avg_loss = loss.ewm(com=13, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        rsi = 100 - (100 / (1 + rs))
        val = float(rsi.iloc[-1])
        return val if not math.isnan(val) else None
    except Exception:
        return None


def _estimate_tick(price: float) -> float:
    if price <= 0:
        return 0.0001
    magnitude = 10 ** math.floor(math.log10(price))
    tick = magnitude * 0.0001
    return max(0.0001, min(tick, 0.01 * price))


class PositionManager:
    """Bidirectional position manager — trail-after-TP for long AND short."""

    def __init__(self, be_buffer_pct: float, vstop_pivot_window: int,
                 max_run_r: float, vstop_buffer_pct: float,
                 tp1_partial_frac: float = 0.5):
        # be_buffer_pct: SL→breakeven buffer applied at TP1.
        # vstop_pivot_window / vstop_buffer_pct: structural trail (validated E0 = 3 / 0.003).
        self._be_buffer = be_buffer_pct
        self._vstop_window = vstop_pivot_window
        self._vstop_buffer = vstop_buffer_pct
        self._max_run_r = max_run_r
        self._tp1_frac = tp1_partial_frac

    def _structural_sl(self, pos: "Position", i: int, df: pd.DataFrame) -> Optional[float]:
        """Structural vstop stop candidate from recent swing extreme (ratchet target)."""
        start = max(0, i - self._vstop_window)
        if start >= i:
            return None
        if pos.side == "long":
            recent_low = float(df["Low"].iloc[start:i + 1].min())
            return recent_low * (1.0 - self._vstop_buffer)
        recent_high = float(df["High"].iloc[start:i + 1].max())
        return recent_high * (1.0 + self._vstop_buffer)

    def update_sl_on_new_bar(
        self,
        pos: Position,
        df: pd.DataFrame,
        enable_trail_after_tp: bool = True,
    ) -> tuple[Optional[float], Optional[str]]:
        """Structural vstop trail FROM ENTRY on ALL TFs (ratchet only) + max_run cap.
        Returns (new_sl, exit_reason). The 50% partial at 161.8% fib is a resting
        reduce-only LIMIT placed by trader.py (maker — cheaper than a market TP),
        NOT commanded here; SL is resized to the remainder when that limit fills.
        """
        if df.empty:
            return None, None
        i = len(df) - 1
        close_i = float(df["Close"].iloc[i])
        if pos.side == "long":
            sl_dist = pos.entry_price - pos.sl_initial
        else:
            sl_dist = pos.sl_initial - pos.entry_price
        if sl_dist <= 0:
            return None, None

        # max_run_r cap on the (remaining) position
        cur_r = ((close_i - pos.entry_price) if pos.side == "long"
                 else (pos.entry_price - close_i)) / sl_dist
        if cur_r >= self._max_run_r:
            log.info("MAX_RUN_R %s %s %s @ %.6f", pos.coin, pos.tf, pos.side, close_i)
            return None, "max_run_cap"

        # Structural vstop trail FROM ENTRY (ratchet only), ALL TFs
        cand = self._structural_sl(pos, i, df)
        new_sl = None
        if cand is not None:
            if pos.side == "long" and cand > pos.sl_current:
                pos.sl_current = cand
                pos.trail_sl = cand
                new_sl = cand
            elif pos.side == "short" and cand < pos.sl_current:
                pos.sl_current = cand
                pos.trail_sl = cand
                new_sl = cand
        return new_sl, None

    def check_sl_hit(
        self,
        pos: Position,
        df: pd.DataFrame,
        vstop_wick_check: bool,
    ) -> Optional[tuple[float, str]]:
        if df.empty:
            return None

        bar = df.iloc[-1]
        o = float(bar["Open"])
        h = float(bar["High"])
        l = float(bar["Low"])
        sl = pos.sl_current

        if pos.side == "long":
            # Gap DOWN through SL
            if o < sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and l <= sl:
                return (sl, "wick_sl")
        else:  # short — gap UP through SL
            if o > sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and h >= sl:
                return (sl, "wick_sl")

        return None
