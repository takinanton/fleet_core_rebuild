"""us29_core.py — US29 signal/exit engine (pure functions on a candles DataFrame).

Mirrors how strategy_xnn delegates to bot.xnn_core: strategy_us29.py is a thin
framework-contract shell; ALL US29 math lives here. Parity target =
/root/hl-backtest/scripts/emit_us29_stocks.py (signal_arrays/candidate) +
cash_ranker_sim_v2.py (_ema, WilderATR, _crypto_signal_bars, _simulate_exit).

STRATEGY (long-only, "fresh-stack reclaim"):
  * Causal seeded EMAs (out[0]=close[0]; k=2/(p+1)) for 20/50/100/200.
  * Wilder ATR14 (seeded ATR[13]=mean(TR[0:14])).
  * On the last CLOSED bar j=len(df)-1:
      aligned[j] = close>EMA20 & EMA20>EMA50 & EMA50>EMA100
      fresh[j]   = aligned[j] AND NOT aligned[j-1]     (alignment JUST formed)
      regime[j]  = close[j] > EMA200[j]
      signal[j]  = fresh[j] & regime[j] & j>=WARMUP(201)
  * SL: sl_raw = nanmin(low[j-9..j]) (SWING_LB=10 incl j); ref=close[j];
      degenerate-skip if sl_raw<=0 or sl_raw>=ref; FLOOR 0.5%:
      if (ref-sl_raw)/ref < 0.005 -> sl0=ref*(1-0.005) else sl0=sl_raw.
  * RANK score = (close[j]-EMA20[j]) / ATR14[j]; -1e9 if ATR<=0/NaN.
  * Chandelier RAISE-ONLY trail (PositionManager owns the hh + frozen ATR_e state):
      cand = hh - MULT*ATR_e (long); return only if cand>current_sl. MULT=7.

The cross-universe causal TOP-K (top-30%) gate is NOT here — it is a cross-coin
pooled-score decision and lives in main.py main_loop (per spec section E).
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ───────────────────────── US29 params (from build_spec result.us29_logic.params) ──────
EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 100
REGIME_EMA = 200
ATR_LEN = 14
SWING_LB = 10            # swing-low window INCLUDING the signal bar
RISK_FLOOR = 0.005       # 0.5% min SL distance floor
CHANDELIER_ATR_MULT = 7  # VALIDATED config (NOT the F4_vb default of 5)
WARMUP_BARS = 201        # max(slowEMA(100)+1, REGIME_EMA(200)+1)
SCORE_NAN_FILL = -1e9


# ───────────────────────── indicators (causal, seeded) ─────────────────────────────────
def ema(c: np.ndarray, p: int) -> np.ndarray:
    """Causal seeded EMA: out[0]=c[0]; k=2/(p+1); out[i]=c[i]*k+out[i-1]*(1-k)."""
    c = np.asarray(c, dtype=float)
    n = c.shape[0]
    out = np.empty(n, dtype=float)
    if n == 0:
        return out
    k = 2.0 / (p + 1.0)
    out[0] = c[0]
    for i in range(1, n):
        out[i] = c[i] * k + out[i - 1] * (1.0 - k)
    return out


def wilder_atr14(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                 length: int = ATR_LEN) -> np.ndarray:
    """Wilder ATR (RMA of True Range).

    TR[0]=H[0]-L[0]; TR[i]=max(H-L, |H-Cprev|, |L-Cprev|);
    ATR[length-1]=mean(TR[0:length]); ATR[i]=(ATR[i-1]*(length-1)+TR[i])/length.
    Indices < length-1 are NaN (not yet seeded).
    """
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)
    n = high.shape[0]
    atr = np.full(n, np.nan, dtype=float)
    if n == 0:
        return atr
    tr = np.empty(n, dtype=float)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
    if n < length:
        return atr
    seed = np.mean(tr[0:length])
    atr[length - 1] = seed
    for i in range(length, n):
        atr[i] = (atr[i - 1] * (length - 1) + tr[i]) / length
    return atr


# ───────────────────────── signal scan ─────────────────────────────────────────────────
def scan_signal(df: pd.DataFrame, cfg: Optional[dict] = None) -> Optional[dict]:
    """Fresh-stack reclaim LONG on the last CLOSED bar j=len(df)-1.

    Returns {signal_idx, entry_price, sl_price, side, meta:{atr14,score,ema_fast_i,count}}
    or None. Long-only.
    """
    if df is None or len(df) < WARMUP_BARS + 1:
        return None
    if cfg is not None and not cfg.get("allow_long", True):
        return None

    c = df["Close"].to_numpy(dtype=float)
    h = df["High"].to_numpy(dtype=float)
    l = df["Low"].to_numpy(dtype=float)
    n = c.shape[0]
    j = n - 1
    if j < WARMUP_BARS:
        return None

    e20 = ema(c, EMA_FAST)
    e50 = ema(c, EMA_MID)
    e100 = ema(c, EMA_SLOW)
    e200 = ema(c, REGIME_EMA)
    atr = wilder_atr14(h, l, c)

    aligned_j = (c[j] > e20[j]) and (e20[j] > e50[j]) and (e50[j] > e100[j])
    aligned_jm1 = (c[j - 1] > e20[j - 1]) and (e20[j - 1] > e50[j - 1]) and (e50[j - 1] > e100[j - 1])
    fresh = aligned_j and (not aligned_jm1)
    regime = c[j] > e200[j]
    if not (fresh and regime):
        return None

    # SL: swing-low-10 incl j, floored 0.5%
    lo_window = l[max(0, j - (SWING_LB - 1)): j + 1]
    sl_raw = float(np.nanmin(lo_window)) if lo_window.size else np.nan
    ref = float(c[j])
    if not np.isfinite(sl_raw) or sl_raw <= 0 or sl_raw >= ref or ref <= 0:
        return None  # degenerate-guard
    if (ref - sl_raw) / ref < RISK_FLOOR:
        sl0 = ref * (1.0 - RISK_FLOOR)
    else:
        sl0 = sl_raw

    atr_j = float(atr[j])
    if not np.isfinite(atr_j) or atr_j <= 0:
        score = SCORE_NAN_FILL
    else:
        score = (ref - float(e20[j])) / atr_j

    return {
        "signal_idx": j,
        "entry_price": ref,         # = close[j]; framework fills via stop-limit, not next_open
        "sl_price": float(sl0),
        "side": "long",
        "meta": {
            "atr14": atr_j if np.isfinite(atr_j) else 0.0,
            "score": float(score),
            "ema_fast_i": float(e20[j]),
            "ema200": float(e200[j]),
            "count": int(n),
        },
    }


def signal_bar_atr14(df: pd.DataFrame) -> float:
    """Wilder ATR14 at the last bar of df (used by PositionManager to freeze ATR_e at entry).

    Fallback nanmedian(ATR14 over 21 bars ending at last bar) if the last value is NaN.
    Returns 0.0 if nothing usable (caller applies final sl_dist fallback).
    """
    if df is None or len(df) == 0:
        return 0.0
    h = df["High"].to_numpy(dtype=float)
    l = df["Low"].to_numpy(dtype=float)
    c = df["Close"].to_numpy(dtype=float)
    atr = wilder_atr14(h, l, c)
    last = float(atr[-1])
    if np.isfinite(last) and last > 0:
        return last
    tail = atr[-21:]
    med = float(np.nanmedian(tail)) if tail.size else np.nan
    if np.isfinite(med) and med > 0:
        return med
    return 0.0


def trail_stop(df: pd.DataFrame, i: int, mult: float, atr_e: float,
               side: str, current_sl: float, hh: float) -> Optional[float]:
    """Chandelier RAISE-ONLY candidate. Caller (PositionManager) persists atr_e + hh.

    cand = hh - mult*atr_e (long). Returns cand only if it RAISES current_sl, else None.
    (US29 is long-only; the short branch is provided for interface symmetry but unused.)
    """
    if atr_e is None or not np.isfinite(atr_e) or atr_e <= 0:
        return None
    if side == "long":
        cand = hh - mult * atr_e
        if current_sl is None or cand > current_sl:
            return float(cand)
        return None
    else:  # short (unused in US29)
        cand = hh + mult * atr_e
        if current_sl is None or cand < current_sl:
            return float(cand)
        return None
