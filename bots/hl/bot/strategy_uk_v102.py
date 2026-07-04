"""strategy_uk_v102.py — UK v102 ZigZag breakout strategy ported from bt-1 engine.

Source files (verified on bt-1 2026-05-24):
  /root/hl-backtest/strategies/uk_v75_zigzag_raw.py   — core entry logic
  /root/hl-backtest/strategies/uk_v84_optimal.py       — trail-after-TP exit
  /root/hl-backtest/strategies/uk_v102_ib_filtered.py  — F1/F2/F3 filters

Entry logic (Donchian forming-high breakout, replaces lagging zigzag pivot):
  1. Close > EMA50 (require_ema50_up filter)
  2. hh = rolling max High of last donchian_k bars BEFORE signal bar
  3. ll = rolling min Low of same window
  4. trigger = hh + tick_size (stop-buy level)
  5. Close > trigger -> breakout confirmed
  6. entry = trigger + tick_size (stop-limit price)
  7. SL = ll - tick_size
  8. SL bounds: min_sl_dist_pct=0.5% .. max_sl_dist_pct per TF (4h=5%, 1d=10%)
  9. TP1 = entry + 1.5 x (entry - SL)

F1 filter (uk_v102_ib_filtered, audit-corrected threshold=2.5 for 4h+1d):
  (close - ema20) / atr14 >= 2.5
  (z=+3.35*** vs F1=2.0 on 4h Nado-subset)

F2/F3: disabled (threshold=0).

Trail-after-TP (uk_v84_optimal.py maybe_exit, TRAIL_TP_TFS = {1d, 8h}):
  ONLY on 1d (since we're on 4h+1d, 8h is dead branch).
  4h positions exit on: TP1 hit (fixed 1.5R), vstop_structure, or SL.
  4h trail intentionally DISABLED per bt-1 v83 finding: "4h sumR dropped (-29%) — trail too aggressive on 4h".
  1d trail: After TP1 hit move SL to entry + 0.3% buffer, then trail via recent 5-bar low, cap at max_run_r=5.0R.

NOTE: This is a pure ZigZag pivot breakout — NOT UK ABC correction methodology.
The strategy name reflects the backtest version lineage, not the entry pattern.
Per MEMORY (project_nado_uk_v102_8h_2026_05_24): "Strategy code = ZigZag breakout
+ ATR-momentum gate (NOT true UK ABC)".
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
    atr14: float                 # for diagnostics
    ema20: float
    f1_dist: float               # (close−ema20)/atr14 long; (ema20−close)/atr14 short


@dataclass
class Position:
    coin: str
    tf: str
    entry_price: float
    sl_initial: float
    sl_current: float
    tp1_price: float
    size: float                  # base asset units (always positive — side stored separately)
    bar_entry_idx: int           # bar index when position opened
    side: str = "long"           # "long" | "short"
    tp1_hit: bool = False
    trail_sl: Optional[float] = None
    tp1_partial_done: bool = False  # 50% reduce-only fib limit fill confirmed


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute EMA20, EMA50, ATR14 on a OHLCV DataFrame.

    Input columns: Open, High, Low, Close, Volume
    Adds: ema20, ema50, atr14

    Source: uk_v102_ib_filtered.py requires atr14, ema20 on window.
    """
    df = df.copy()
    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()

    # ATR14 = EMA14 of TrueRange (Wilder's smoothing)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(span=14, adjust=False).mean()

    return df


def scan_for_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,
    raw_rr_target: float,
    require_ema50_up: bool,
    f1_min_dist_ema20_atr: float,
    tf_max_sl: dict,
    min_sl_dist_pct: float,
    f2_min_rsi14: float = 0.0,
    f3_max_dollar_vol_usd: float = 0.0,
) -> Optional[Signal]:
    """Scan the latest closed bar of df for a breakout signal.

    df must have indicators computed (compute_indicators). Last bar = latest
    closed bar (forming bar already dropped by candle fetcher).

    Returns Signal if all gates pass, else None.

    Ported from uk_v75_zigzag_raw.py maybe_enter + uk_v102_ib_filtered.py maybe_enter.

    Per-TF F1/F2/F3 injected by scanner.py per call — do not read from global settings here.
    F2/F3 = 0 means disabled (source: uk_v102_ib_filtered.py convention).
    """
    if len(df) < donchian_k + 2:
        return None

    i = len(df) - 1   # latest closed bar index
    close_i = float(df["Close"].iloc[i])
    ema50_i = float(df["ema50"].iloc[i])
    ema20_i = float(df["ema20"].iloc[i])
    atr14_i = float(df["atr14"].iloc[i])

    # Trend filter (source: uk_v75 require_ema50_up)
    if require_ema50_up and close_i <= ema50_i:
        return None

    # F1: ATR-momentum gate (source: uk_v102_ib_filtered f1_min_dist_ema20_atr; per-TF injected)
    if f1_min_dist_ema20_atr > 0:
        if atr14_i <= 0 or ema20_i <= 0:
            log.debug("F1 skip %s %s: invalid atr14=%s ema20=%s", coin, tf, atr14_i, ema20_i)
            return None
        f1_dist = (close_i - ema20_i) / atr14_i
        if f1_dist < f1_min_dist_ema20_atr:
            return None
    else:
        f1_dist = 0.0

    # F2: RSI14 gate (source: uk_v102_ib_filtered f2_min_rsi14; per-TF injected)
    # 0 = disabled; if enabled, requires rsi14 >= threshold
    if f2_min_rsi14 > 0:
        rsi14_i = _compute_rsi14_last(df)
        if rsi14_i is not None and rsi14_i < f2_min_rsi14:
            return None

    # F3: dollar-volume cap gate (source: uk_v102_ib_filtered f3_max_dollar_vol_usd; per-TF injected)
    # 0 = disabled; if enabled, rejects coins with last-bar dollar_volume > threshold
    if f3_max_dollar_vol_usd > 0:
        vol_i = float(df["Volume"].iloc[i])
        dollar_vol = vol_i * close_i
        if dollar_vol > f3_max_dollar_vol_usd:
            return None

    # Donchian forming resistance/support (replaces confirmed zigzag pivot).
    # Window = last donchian_k bars BEFORE the signal bar (excludes bar i).
    # hh = rolling max High (forming resistance price has not yet broken)
    # ll = rolling min Low (SL anchor)
    hh = float(df["High"].iloc[i - donchian_k:i].max())
    ll = float(df["Low"].iloc[i - donchian_k:i].min())

    # Estimate tick_size from existing metadata if available, else heuristic
    tick_size = _estimate_tick(close_i)

    # Trigger = forming high + 1 tick (stop-buy level)
    trigger = hh + tick_size

    # Breakout gate: close must have crossed the forming resistance
    if close_i <= trigger:
        return None

    # Entry = trigger + tick (stop-limit price, one tick above trigger)
    entry = trigger + tick_size

    # If bar opened above entry (gap up) -> entry = open price (from v75)
    open_i = float(df["Open"].iloc[i])
    if open_i > entry:
        entry = open_i

    sl = ll - tick_size
    if sl >= entry:
        return None

    # SL distance bounds (source: uk_v75 default_config + uk_v84 TF_MAX_SL)
    sl_dist = entry - sl
    sl_dist_pct = sl_dist / entry
    max_sl_pct = tf_max_sl.get(tf, 0.10)

    if sl_dist_pct < min_sl_dist_pct:
        return None
    if sl_dist_pct > max_sl_pct:
        return None

    # TP1 = entry + 1.5 × risk (source: uk_v75 raw_rr_target=1.5)
    tp1 = entry + raw_rr_target * sl_dist

    # Bar timestamp (ms UTC)
    try:
        bar_ts = int(df["time"].iloc[i].value // 10**6)
    except Exception:
        bar_ts = 0

    return Signal(
        coin=coin,
        tf=tf,
        side="long",
        trigger_price=trigger,
        entry_price=entry,
        sl_price=sl,
        tp1_price=tp1,
        sl_dist_pct=sl_dist_pct,
        pivot_high=hh,
        pivot_low=ll,
        bar_ts=bar_ts,
        atr14=atr14_i,
        ema20=ema20_i,
        f1_dist=f1_dist,
    )


def _find_zigzag_pivots(df: pd.DataFrame, i: int, L: int):
    """Return (last_pivot_high, last_pivot_low) as (idx, price) tuples or (None, None).

    Shared by long + short scans (source: uk_v75 zigzag loop, verbatim).
    """
    last_pivot_high: Optional[tuple] = None
    last_pivot_low: Optional[tuple] = None
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


def scan_for_short_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,
    raw_rr_target: float,
    require_ema50_down: bool,
    f1_min_dist_ema20_atr: float,
    tf_max_sl: dict,
    min_sl_dist_pct: float,
    f2_max_rsi14: float = 0.0,
    f3_max_dollar_vol_usd: float = 0.0,
) -> Optional[Signal]:
    """SHORT scan — mirror of scan_for_signal (bt-1 uk_v102_short validated 2026-05-27).

      1. close < EMA50 (require_ema50_down)
      2/3. last ZigZag pivot high + low (same finder)
      4. pivot_low BELOW pivot_high (downtrend context)
      5. close < pivot_low → breakdown trigger
      6. entry = pivot_low - tick (stop-sell)
      7. SL = pivot_high + tick
      9. TP1 = entry - 1.5 × (SL - entry)

    F1 SHORT: (ema20 - close)/atr14 >= thr ; F2 SHORT: rsi14 <= thr ; F3 symmetric.
    Per-TF F1/F2/F3 injected by scanner.py. 0 = disabled.
    """
    if len(df) < donchian_k + 2:
        return None

    i = len(df) - 1
    close_i = float(df["Close"].iloc[i])
    ema50_i = float(df["ema50"].iloc[i])
    ema20_i = float(df["ema20"].iloc[i])
    atr14_i = float(df["atr14"].iloc[i])

    # Trend filter (mirror require_ema50_up): close BELOW ema50
    if require_ema50_down and close_i >= ema50_i:
        return None

    # F1 SHORT: distance BELOW ema20 (per-TF injected)
    if f1_min_dist_ema20_atr > 0:
        if atr14_i <= 0 or ema20_i <= 0:
            log.debug("F1 skip %s %s: invalid atr14=%s ema20=%s", coin, tf, atr14_i, ema20_i)
            return None
        f1_dist = (ema20_i - close_i) / atr14_i
        if f1_dist < f1_min_dist_ema20_atr:
            return None
    else:
        f1_dist = 0.0

    # F2 SHORT: rsi14 <= threshold (oversold confirmation; param is MAX)
    if f2_max_rsi14 > 0:
        rsi14_i = _compute_rsi14_last(df)
        if rsi14_i is not None and rsi14_i > f2_max_rsi14:
            return None

    # F3: dollar-volume cap (symmetric with long)
    if f3_max_dollar_vol_usd > 0:
        vol_i = float(df["Volume"].iloc[i])
        if vol_i * close_i > f3_max_dollar_vol_usd:
            return None

    # Donchian forming support/resistance (mirror of long).
    # Window = last donchian_k bars BEFORE the signal bar (excludes bar i).
    # ll = rolling min Low (forming support price has not yet broken)
    # hh = rolling max High (SL anchor)
    ll = float(df["Low"].iloc[i - donchian_k:i].min())
    hh = float(df["High"].iloc[i - donchian_k:i].max())

    tick_size = _estimate_tick(close_i)

    # Trigger = forming low - 1 tick (stop-sell level)
    trigger = ll - tick_size

    # Breakdown gate: close must have crossed below the forming support
    if close_i >= trigger:
        return None

    # Entry = trigger - tick (stop-limit price, one tick below trigger)
    entry = trigger - tick_size

    # If bar opened below entry (gap down) -> entry = open price (mirror of gap-up)
    open_i = float(df["Open"].iloc[i])
    if open_i < entry:
        entry = open_i

    sl = hh + tick_size
    if sl <= entry:
        return None

    sl_dist = sl - entry
    sl_dist_pct = sl_dist / entry
    max_sl_pct = tf_max_sl.get(tf, 0.10)
    if sl_dist_pct < min_sl_dist_pct:
        return None
    if sl_dist_pct > max_sl_pct:
        return None

    # TP1 = entry - 1.5 × risk (mirror)
    tp1 = entry - raw_rr_target * sl_dist

    try:
        bar_ts = int(df["time"].iloc[i].value // 10**6)
    except Exception:
        bar_ts = 0

    return Signal(
        coin=coin,
        tf=tf,
        side="short",
        trigger_price=trigger,
        entry_price=entry,
        sl_price=sl,
        tp1_price=tp1,
        sl_dist_pct=sl_dist_pct,
        pivot_high=hh,
        pivot_low=ll,
        bar_ts=bar_ts,
        atr14=atr14_i,
        ema20=ema20_i,
        f1_dist=f1_dist,
    )


def _compute_rsi14_last(df: pd.DataFrame) -> Optional[float]:
    """Compute RSI14 on the last bar. Returns None if insufficient data.

    Source: uk_v102_ib_filtered.py F2 filter uses RSI14 Wilder smoothing.
    Only called when f2_min_rsi14 > 0 (per-TF, injected by scanner).
    """
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
    """Heuristic tick size when live metadata not yet available.

    Source: Nado exchange_nado.py _meta_cache falls back to tick=0.0001.
    We scale by price magnitude to avoid rounding errors on high-priced assets.
    """
    if price <= 0:
        return 0.0001
    magnitude = 10 ** math.floor(math.log10(price))
    # Standard: tick = 0.01% of price magnitude
    tick = magnitude * 0.0001
    # Clamp to reasonable range
    return max(0.0001, min(tick, 0.01 * price))


class PositionManager:
    """Structural vstop-trail-from-entry (ratchet only) on ALL TFs + max_run cap.

    50/50 exit (2026-05-29): the 50% partial @ 161.8% fib is a resting reduce-only
    MAKER limit placed by trader.py — NOT commanded here. After it fills, trader calls
    apply_partial_be() so the runner's SL ratchets to >= breakeven. SL is always
    reduce-only, so a full-size SL safely covers whatever size remains after a partial.
    """

    def __init__(
        self,
        be_buffer_pct: float,
        vstop_pivot_window: int,
        max_run_r: float,
        vstop_buffer_pct: float,
        tp1_partial_frac: float = 0.5,
    ):
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

        Returns (new_sl, exit_reason). `enable_trail_after_tp` kept for call-site
        compatibility; the structural trail is always active now.
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

    def apply_partial_be(self, pos: "Position") -> Optional[float]:
        """After the 50% partial TP fills, ratchet the remainder's SL to >= breakeven.

        Returns the new SL if it moved (caller re-places the trigger), else None.
        """
        if pos.side == "long":
            be = pos.entry_price * (1.0 - self._be_buffer)
            if be > pos.sl_current:
                pos.sl_current = be
                pos.trail_sl = be
                return be
        else:
            be = pos.entry_price * (1.0 + self._be_buffer)
            if be < pos.sl_current:
                pos.sl_current = be
                pos.trail_sl = be
                return be
        return None

    def check_sl_hit(
        self,
        pos: Position,
        df: pd.DataFrame,
        vstop_wick_check: bool,
    ) -> Optional[tuple[float, str]]:
        """Check if SL was hit on latest bar. Returns (exit_price, reason) or None.

        Wick check enabled (fleet-wide rule): Low <= SL triggers at SL price.
        Gap check: Open < SL → fill at Open.
        Source: vstop_structure.py find_structure_exit wick-mode logic.
        """
        if df.empty:
            return None

        bar = df.iloc[-1]
        o = float(bar["Open"])
        h = float(bar["High"])
        l = float(bar["Low"])
        sl = pos.sl_current

        if pos.side == "long":
            # SL below entry: gap DOWN through SL fills at open; wick Low <= SL
            if o < sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and l <= sl:
                return (sl, "wick_sl")
        else:  # short — SL above entry: gap UP through SL fills at open; wick High >= SL
            if o > sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and h >= sl:
                return (sl, "wick_sl")

        return None
