"""xnn_core.py — PURE XNN signal math, ported 1:1 from xnn_long_stateless.py.

PARITY BY CONSTRUCTION (2026-06-10):
  Every decision function below is a VERBATIM copy of the corresponding method in
  /Users/ak/Desktop/HL/hl_xnn_prep/xnn_long_stateless.py (the canonical bt strategy),
  with only mechanical transforms:
    * `self.config` -> `cfg` dict argument (same keys, same defaults via .get()).
    * `self._emas_pair(w, f, s)` id()-keyed cache -> per-call `memo` dict keyed (f, s).
      Both are pure functions of (window, pair) — identical values, but the live-bot
      id()-reuse-after-GC hazard (long-lived process) is structurally impossible here.
    * methods -> module-level functions taking an explicit `w` window object.
  NO imports from the bot framework, exchange clients, or harness. pandas/numpy only.

WINDOW CONTRACT: `w` exposes .close/.high/.low/.atr14 as float arrays over CLOSED bars
only (forming bar must already be dropped by the caller — exchange_hl.candles does this,
exchange_hl.py:493-495). Index i = decision bar (latest closed bar).

ATR14: VERIFIED 2026-06-10 against bt-1 harness source (ssh bt-1,
~/hl-backtest/scripts/indicators/precompute.py:60-68, ATR_P=14 at :34):
  TR = max(H-L, |H-prev_close|, |L-prev_close|); atr14 = tr.ewm_mean(alpha=1/14,
  adjust=False)  — polars ewm_mean == pandas ewm(...).mean() for adjust=False.
compute_xnn_atr14 below is the SAME formula (Wilder RMA). Only residual delta vs bt:
bt seeds the EWM at full-history bar 0, live at window bar 0 — alpha=1/14 seed residual
decays e^(-n/14), negligible after ~100 bars (SCAN_CANDLES_LIMIT=3000).
"""
from __future__ import annotations

import sys
from typing import Optional

import numpy as np
import pandas as pd

# Verbatim from xnn_long_stateless.py:34-35 (note key "1W" — bt spelling).
TF_MAX_SL = {"15m": 0.04, "30m": 0.05, "1h": 0.06, "2h": 0.07,
             "4h": 0.08, "8h": 0.10, "1d": 0.15, "1W": 0.25}

# Engine warmup seed index — xnn_long_stateless.py:40. EMA recursion seeds at bar 20
# of the window; the window START therefore matters (sliding 300-bar window != bt
# full-history window). Mitigation: fetch a LONG window (SCAN_CANDLES_LIMIT=3000) so
# the seed residual in even the 610-EMA decays to <1e-3 by the decision bar.
_EMA_SEED_I = 20


class Window:
    """Minimal stand-in for the harness window object (only the attrs xnn reads)."""
    __slots__ = ("close", "high", "low", "atr14", "ema200", "xs_mom_rank")

    def __init__(self, close, high, low, atr14, ema200=None):
        self.close = close
        self.high = high
        self.low = low
        self.atr14 = atr14
        self.ema200 = ema200          # only needed when use_trend_gate=true (OFF in deploy cfg)
        self.xs_mom_rank = None       # xs rank not available live; rs_top_frac=0 -> gate inert


def compute_xnn_atr14(high, low, close) -> np.ndarray:
    """ATR14 = Wilder RMA (ewm alpha=1/14, adjust=False) of True Range.

    VERIFIED == bt-1 harness formula (precompute.py:60-68) — see module docstring.
    """
    h = pd.Series(np.asarray(high, dtype=float))
    l = pd.Series(np.asarray(low, dtype=float))
    c = pd.Series(np.asarray(close, dtype=float))
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / 14.0, adjust=False).mean().to_numpy()


def make_window(df: pd.DataFrame) -> Window:
    """Build a Window from an OHLC DataFrame of CLOSED bars (cols Open/High/Low/Close)."""
    close = df["Close"].to_numpy(dtype=float)
    high = df["High"].to_numpy(dtype=float)
    low = df["Low"].to_numpy(dtype=float)
    atr14 = compute_xnn_atr14(high, low, close)
    return Window(close=close, high=high, low=low, atr14=atr14)


def assert_canonical_gates(cfg: dict) -> None:
    # Verbatim port of xnn_long_stateless._assert_canonical_gates (xnn:84-102).
    # FORCING-FN (2026-06-06): xnn is DEFINED by its entry gates; fail-closed unless
    # caller explicitly opts out via allow_ungated=true (deploy cfg msi=1 DOES opt out —
    # divergence from canonical gates is EXPLICIT and logged).
    c = cfg
    if bool(c.get("allow_ungated", False)):
        print("WARNING xnn_core: allow_ungated=True -> WITHOUT canonical gates "
              "(bare reclaim, NOT xnn); numbers are NOT an xnn edge.", file=sys.stderr)
        return
    miss = []
    if int(c.get("min_signal_idx", 1) or 1) < 2: miss.append("min_signal_idx>=2")
    if int(c.get("corr_touch_lk", 0) or 0) <= 0: miss.append("corr_touch_lk>0")
    if int(c.get("slow_slope_lk", 0) or 0) <= 0: miss.append("slow_slope_lk>0")
    if float(c.get("sig_min_rally_pct", 0.0) or 0.0) <= 0: miss.append("sig_min_rally_pct>0")
    if miss:
        raise ValueError("xnn_core WITHOUT canonical entry gates: missing " +
            ", ".join(miss) + ". xnn is DEFINED by these (2nd-correction reclaim that TESTS "
            "the slow EMA). Set them; to test ungated set allow_ungated=true.")


# ─── window-pure EMA (deterministic over the whole window) ────────────────
def _emas_pair(w: Window, f: int, s: int, memo: dict):
    """Verbatim port of xnn_long_stateless._emas_pair (xnn:114-141); cache -> per-call memo."""
    key = (f, s)
    hit = memo.get(key)
    if hit is not None:
        return hit
    cl = w.close
    n = len(cl)
    kf = 2.0 / (f + 1)
    ks = 2.0 / (s + 1)
    ef = [None] * n
    es = [None] * n
    seed = _EMA_SEED_I
    if seed >= n:                    # window shorter than warmup (degenerate) — no EMA
        memo[key] = (ef, es)
        return ef, es
    pf = ps = cl[seed]
    ef[seed] = pf; es[seed] = ps
    for k in range(seed + 1, n):
        c = cl[k]
        pf = pf + kf * (c - pf)
        ps = ps + ks * (c - ps)
        ef[k] = pf; es[k] = ps
    memo[key] = (ef, es)
    return ef, es


def _select_ema_pair(w: Window, i: int, cfg: dict, memo: dict):
    """Verbatim port of xnn_long_stateless._select_ema_pair (xnn:143-186).
    Returns (f, s, ef, es) of best pair or None (pair ids added for diagnostics only —
    selection logic unchanged)."""
    cands = cfg.get("ema_candidates") or [[int(cfg["emf"]), int(cfg["ems"])]]
    W = int(cfg.get("ema_fit_window", 60) or 60)
    min_slope = float(cfg.get("ema_min_slope_atr", 0.0) or 0.0)
    atr = w.atr14[i] if i < len(w.atr14) else 0.0
    best = None
    best_score = None
    for pair in cands:
        f, s = int(pair[0]), int(pair[1])
        ef, es = _emas_pair(w, f, s, memo)
        j0 = i - W
        if j0 < _EMA_SEED_I or i >= len(es) or es[i] is None or es[j0] is None or ef[i] is None:
            continue
        if atr > 0 and abs(es[i] - es[j0]) < min_slope * atr:   # no real trend on this scale
            continue
        above = below = crosses = nbar = 0
        prev = None
        for kk in range(j0, i + 1):
            a, b, c = ef[kk], es[kk], w.close[kk]
            if a is None or b is None:
                continue
            nbar += 1
            if c >= b:
                above += 1
            else:
                below += 1
            sign = 1 if a > b else -1
            if prev is not None and sign != prev:
                crosses += 1
            prev = sign
        if nbar < W // 2:
            continue
        score = (max(above, below) / nbar) - (crosses / nbar)   # one-sided & clean channel
        if best_score is None or score > best_score:
            best_score = score
            best = (f, s, ef, es)
    return best


def _qualifying_pairs(w: Window, i: int, cfg: dict, memo: dict):
    """Verbatim port of xnn_long_stateless._qualifying_pairs (xnn:188-228).
    Returns list of (f, s, ef, es) for ALL qualifying pairs (pair ids added for
    diagnostics only — qualification logic unchanged)."""
    cands = cfg.get("ema_candidates") or [[int(cfg["emf"]), int(cfg["ems"])]]
    W = int(cfg.get("ema_fit_window", 60) or 60)
    min_slope = float(cfg.get("ema_min_slope_atr", 0.0) or 0.0)
    max_xf = float(cfg.get("ema_fit_max_cross_frac", 0.05) or 0.05)    # <=3 crosses per 60 bars
    min_os = float(cfg.get("ema_fit_min_onesided", 0.70) or 0.70)      # user canon pct>=0.70
    atr = w.atr14[i] if i < len(w.atr14) else 0.0
    out = []
    for pair in cands:
        f, s = int(pair[0]), int(pair[1])
        ef, es = _emas_pair(w, f, s, memo)
        j0 = i - W
        if j0 < _EMA_SEED_I or i >= len(es) or es[i] is None or es[j0] is None or ef[i] is None:
            continue
        if atr > 0 and abs(es[i] - es[j0]) < min_slope * atr:
            continue
        above = below = crosses = nbar = 0
        prev = None
        for kk in range(j0, i + 1):
            a, b, c = ef[kk], es[kk], w.close[kk]
            if a is None or b is None:
                continue
            nbar += 1
            if c >= b:
                above += 1
            else:
                below += 1
            sign = 1 if a > b else -1
            if prev is not None and sign != prev:
                crosses += 1
            prev = sign
        if nbar < W // 2:
            continue
        if crosses <= max_xf * nbar and (max(above, below) / nbar) >= min_os:
            out.append((f, s, ef, es))
    return out


# ─── pure helpers (window-indexed, no call-history state) ──────────────────
def _er(w: Window, i: int, n: int):
    # Verbatim port of xnn_long_stateless._er (xnn:231-236).
    if i < n:
        return 0.0
    change = abs(w.close[i] - w.close[i - n])
    vol = sum(abs(w.close[k] - w.close[k - 1]) for k in range(i - n + 1, i + 1))
    return change / vol if vol > 0 else 0.0


def _strong(w: Window, i: int, up: bool, cfg: dict):
    # Verbatim port of xnn_long_stateless._strong (xnn:238-245). Only reached when
    # use_trend_gate=true (OFF in deploy cfg); requires w.ema200 (guarded).
    atr = w.atr14[i]; lb = int(cfg["slope_lookback"])
    if atr <= 0 or i < lb:
        return False
    if w.ema200 is None:
        # FORCING-FN (review 2026-06-10): canonical _strong (xnn:242) READS w.ema200 —
        # a silent False here would reject ALL entries the day someone flips
        # use_trend_gate=true with a live Window that has no ema200 (default None).
        # Fail LOUD instead of silently trading zero signals.
        raise ValueError(
            "xnn_core._strong: use_trend_gate=true but Window.ema200 is None — "
            "live caller must supply ema200 (make_window does not compute it)")
    slope = (w.ema200[i] - w.ema200[i - lb]) / atr
    if _er(w, i, int(cfg["er_window"])) < float(cfg["er_thr"]):
        return False
    return slope >= float(cfg["slope_thr"]) if up else slope <= -float(cfg["slope_thr"])


def _rs_ok(w: Window, i: int, side: str, cfg: dict):
    # Verbatim port of xnn_long_stateless._rs_ok (xnn:247-257).
    frac = float(cfg.get("rs_top_frac", 0.0) or 0.0)
    if frac <= 0:
        return True
    rs = getattr(w, "xs_mom_rank", None)
    if rs is None or i >= len(rs):
        return True
    v = rs[i]
    if v != v:
        return False
    return v >= (1.0 - frac) if side == "long" else v <= frac


def _clean_ok(w: Window, ef, es, i: int, efk, esk, c, cfg: dict, side: str = "long"):
    """Verbatim port of xnn_long_stateless._clean_ok (xnn:259-276)."""
    clk = int(cfg.get("clean_lk", 0) or 0)
    if clk > 0:
        if i - _EMA_SEED_I < clk:     # not enough EMA-defined history (mirror: len(hist)<clk)
            return False
        for k in range(i - clk + 1, i + 1):
            a, b = ef[k], es[k]
            if a is None or b is None:
                return False
            if ((a - b) if side == "long" else (b - a)) <= 0:   # cross = not clean
                return False
    ms = float(cfg.get("min_sep_pct", 0.0) or 0.0)
    if ms > 0 and c > 0 and (((efk - esk) if side == "long" else (esk - efk)) / c) < ms:
        return False
    return True


def _corr_ok(w: Window, ef, es, i: int, esk, cfg: dict):
    """Verbatim port of xnn_long_stateless._corr_ok (xnn:278-314), incl. 2026-06-05
    contiguous-pullback FIX and FIX#2 (close<emaS in pullback = break -> reject)."""
    clk = int(cfg.get("corr_touch_lk", 0) or 0)
    if clk > 0:
        if es[i - 1] is not None and w.close[i - 1] < es[i - 1]:
            return False
        lo_min = w.low[i - 1]
        k = i - 2
        steps = 1
        while k >= _EMA_SEED_I and steps < clk:
            efk, eskk = ef[k], es[k]
            if efk is None or w.close[k] >= efk:   # pullback ended (closed back above fast EMA)
                break
            if eskk is not None and w.close[k] < eskk:   # closed below slow EMA = break -> reject
                return False
            if w.low[k] < lo_min:
                lo_min = w.low[k]
            k -= 1
            steps += 1
        if lo_min > esk:
            return False
    slk = int(cfg.get("slow_slope_lk", 0) or 0)
    if slk > 0:
        j = i - slk
        if j < _EMA_SEED_I or es[j] is None or esk <= es[j]:   # slow EMA not rising
            return False
    return True


def _corr_ok_short(w: Window, ef, es, i: int, esk, cfg: dict):
    """Verbatim port of xnn_long_stateless._corr_ok_short (xnn:316-344)."""
    clk = int(cfg.get("corr_touch_lk", 0) or 0)
    if clk > 0:
        if es[i - 1] is not None and w.close[i - 1] > es[i - 1]:   # close ABOVE slow = break
            return False
        hi_max = w.high[i - 1]
        k = i - 2
        steps = 1
        while k >= _EMA_SEED_I and steps < clk:
            efk, eskk = ef[k], es[k]
            if efk is None or w.close[k] <= efk:   # rally ended (closed back below fast EMA)
                break
            if eskk is not None and w.close[k] > eskk:   # closed above slow EMA = break -> reject
                return False
            if w.high[k] > hi_max:
                hi_max = w.high[k]
            k -= 1
            steps += 1
        if hi_max < esk:   # rally didn't reach slow EMA (no real correction depth)
            return False
    slk = int(cfg.get("slow_slope_lk", 0) or 0)
    if slk > 0:
        j = i - slk
        if j < _EMA_SEED_I or es[j] is None or esk >= es[j]:   # slow EMA not falling
            return False
    return True


def _qual_short(w: Window, ef, es, i: int, cfg: dict):
    """Verbatim port of xnn_long_stateless._qual_short (xnn:346-364)."""
    efk, esk = ef[i], es[i]
    if efk is None or esk is None:
        return False
    c = w.close[i]; cp = w.close[i - 1]; atr = w.atr14[i]
    short_reclaim = (efk < esk and cp > efk and c < efk)
    if not (short_reclaim and atr > 0):
        return False
    gate = cfg.get("use_trend_gate", False)
    if gate and not _strong(w, i, False, cfg):
        return False
    if not _clean_ok(w, ef, es, i, efk, esk, c, cfg, side="short"):
        return False
    if not _corr_ok_short(w, ef, es, i, esk, cfg):
        return False
    return True


def _qual_long(w: Window, ef, es, i: int, cfg: dict):
    """Verbatim port of xnn_long_stateless._qual_long (xnn:366-383)."""
    efk, esk = ef[i], es[i]
    if efk is None or esk is None:
        return False
    c = w.close[i]; cp = w.close[i - 1]; atr = w.atr14[i]
    long_reclaim = (efk > esk and cp < efk and c > efk)
    if not (long_reclaim and atr > 0):
        return False
    gate = cfg.get("use_trend_gate", False)
    if gate and not _strong(w, i, True, cfg):
        return False
    if not _clean_ok(w, ef, es, i, efk, esk, c, cfg):
        return False
    if not _corr_ok(w, ef, es, i, esk, cfg):
        return False
    return True


def _series_count_at(w: Window, ef, es, i: int, cfg: dict):
    """Verbatim port of xnn_long_stateless._series_count_at (xnn:385-473), incl. the
    2026-06-04 ONE-COUNT-PER-DISTINCT-CORRECTION root-cause fix (close-based up-leg +
    intervening close<emaF pullback)."""
    efi, esi = ef[i], es[i]
    if efi is None or esi is None:
        return 0, False
    ci = w.close[i]
    # bar i must itself be inside the (non-broken) series, else count resets to 0 here.
    if efi <= esi or ci < esi:
        return 0, False
    gap = int(cfg.get("sig_min_gap_bars", 0) or 0)
    hp = float(cfg.get("sig_min_height_pct", 0.0) or 0.0)
    rally = float(cfg.get("sig_min_rally_pct", 0.0) or 0.0)
    start = None
    b = i - 1
    while b >= _EMA_SEED_I:
        efb, esb = ef[b], es[b]
        if efb is None or esb is None:
            start = b + 1
            break
        if efb <= esb or w.close[b] < esb:   # break bar
            start = b + 1
            break
        b -= 1
    if start is None:
        start = _EMA_SEED_I
    # forward replay over [start, i]
    count = 0
    last_bar = None; last_px = None
    up_leg = False        # since last count: a CLOSE rallied >= rally% above last_px
    pulled_back = False   # since that up-leg: a CLOSE dipped below emaF (new correction)
    for k in range(start, i + 1):
        efk, esk = ef[k], es[k]
        if efk is None or esk is None or efk <= esk or w.close[k] < esk:
            count = 0; last_bar = last_px = None
            up_leg = False; pulled_back = False
            continue
        ck = w.close[k]
        if last_px is not None:
            if ck >= last_px * (1.0 + rally):       # real up-leg (close-based)
                up_leg = True
            if up_leg and ck < efk:                 # pulled back below fast EMA after up-leg
                pulled_back = True
        if _qual_long(w, ef, es, k, cfg):
            if last_bar is None:
                sep_ok = True                        # first counted signal of the series
            else:
                sep_ok = ((k - last_bar) >= gap
                          and up_leg and pulled_back
                          and ck >= last_px * (1.0 + hp))
            if sep_ok:
                count += 1
                last_bar = k; last_px = ck
                up_leg = False; pulled_back = False   # reset for the NEXT correction
    counted_now = (last_bar == i)
    return count, counted_now


def _series_count_short_at(w: Window, ef, es, i: int, cfg: dict):
    """Verbatim port of xnn_long_stateless._series_count_short_at (xnn:522-576)."""
    efi, esi = ef[i], es[i]
    if efi is None or esi is None:
        return 0, False
    ci = w.close[i]
    if efi >= esi or ci > esi:
        return 0, False
    gap = int(cfg.get("sig_min_gap_bars", 0) or 0)
    hp = float(cfg.get("sig_min_height_pct", 0.0) or 0.0)
    rally = float(cfg.get("sig_min_rally_pct", 0.0) or 0.0)
    start = None
    b = i - 1
    while b >= _EMA_SEED_I:
        efb, esb = ef[b], es[b]
        if efb is None or esb is None:
            start = b + 1; break
        if efb >= esb or w.close[b] > esb:        # break bar (mirror of long)
            start = b + 1; break
        b -= 1
    if start is None:
        start = _EMA_SEED_I
    count = 0
    last_bar = None; last_px = None
    down_leg = False       # since last count: a CLOSE fell >= rally% below last_px
    pushed_up = False      # since that down-leg: a CLOSE rose above emaF (new correction)
    for k in range(start, i + 1):
        efk, esk = ef[k], es[k]
        if efk is None or esk is None or efk >= esk or w.close[k] > esk:
            count = 0; last_bar = last_px = None
            down_leg = False; pushed_up = False
            continue
        ck = w.close[k]
        if last_px is not None:
            if ck <= last_px * (1.0 - rally):     # real down-leg (close-based)
                down_leg = True
            if down_leg and ck > efk:             # pushed back above fast EMA after down-leg
                pushed_up = True
        if _qual_short(w, ef, es, k, cfg):
            if last_bar is None:
                sep_ok = True                     # first counted signal of the series
            else:
                sep_ok = ((k - last_bar) >= gap
                          and down_leg and pushed_up
                          and ck <= last_px * (1.0 - hp))
            if sep_ok:
                count += 1
                last_bar = k; last_px = ck
                down_leg = False; pushed_up = False
    counted_now = (last_bar == i)
    return count, counted_now


# ─── entry: pure per-bar decision (port of maybe_enter, xnn:476-520) ─────────
def scan_signal(df_closed_bars: pd.DataFrame, cfg: dict) -> Optional[dict]:
    """Pure per-bar XNN entry decision at the LATEST closed bar of df.

    Port of xnn_long_stateless.maybe_enter (xnn:476-520) with two EXPLICIT deltas:
      1. ctx.open_positions >= cfg.max_concurrent gate DROPPED — concurrency is owned
         by the FRAMEWORK (main.py max_concurrent gate). bt ran with max_concurrent=5;
         live runs 999 -> documented divergence (see DEPLOY_CHECKLIST open questions).
      2. Returns a plain dict (no harness Signal class).

    Returns {side, entry_price, sl_price, signal_idx, meta{...}} or None.
    """
    w = make_window(df_closed_bars)
    i = len(w.close) - 1
    memo: dict = {}
    if i < int(cfg["ems"]) + 2 or i < _EMA_SEED_I + 2:
        return None
    if cfg.get("ema_adaptive", False):
        if str(cfg.get("ema_adaptive_mode", "best")) == "union":
            pairs = _qualifying_pairs(w, i, cfg, memo)   # ANY clean pair with a signal
        else:
            best = _select_ema_pair(w, i, cfg, memo)     # one best-fit pair per bar
            pairs = [best] if best is not None else []
    else:
        f0, s0 = int(cfg["emf"]), int(cfg["ems"])
        ef0, es0 = _emas_pair(w, f0, s0, memo)
        pairs = [(f0, s0, ef0, es0)]
    if not pairs:
        return None
    c = w.close[i]; atr = w.atr14[i]
    if atr <= 0:
        return None
    if c < float(cfg.get("min_price", 0.0) or 0.0):
        return None
    msi = int(cfg.get("min_signal_idx", 1) or 1)
    k = int(cfg["sl_lookback"]); buf = float(cfg["sl_atr_buf"])
    mn, mx = float(cfg["min_sl_dist_pct"]), float(cfg["max_sl_dist_pct"])
    for f, s, ef, es in pairs:
        if i >= len(ef) or ef[i] is None:
            continue
        if cfg.get("allow_long", True):
            count, counted_now = _series_count_at(w, ef, es, i, cfg)
            if counted_now and count >= msi and _rs_ok(w, i, "long", cfg):
                sl = min(w.low[i - k:i + 1]) - buf * atr
                if sl < c:
                    d = (c - sl) / c
                    if mn <= d <= mx:
                        return {
                            "side": "long", "entry_price": float(c), "sl_price": float(sl),
                            "signal_idx": i,
                            "meta": {"pair": (f, s), "count": count, "atr14": float(atr),
                                     "ema_fast_i": float(ef[i]), "ema_slow_i": float(es[i]),
                                     "sl_dist_pct": float(d), "pattern": "xnn_long"},
                        }
        if cfg.get("allow_short", False):
            cnt_s, counted_s = _series_count_short_at(w, ef, es, i, cfg)
            if counted_s and cnt_s >= msi and _rs_ok(w, i, "short", cfg):
                sl = max(w.high[i - k:i + 1]) + buf * atr
                if sl > c:
                    d = (sl - c) / c
                    if mn <= d <= mx:
                        return {
                            "side": "short", "entry_price": float(c), "sl_price": float(sl),
                            "signal_idx": i,
                            "meta": {"pair": (f, s), "count": cnt_s, "atr14": float(atr),
                                     "ema_fast_i": float(ef[i]), "ema_slow_i": float(es[i]),
                                     "sl_dist_pct": float(d), "pattern": "xnn_short"},
                        }
    return None


# ─── trail (bt-engine vstop: PIVOT-based ratchet; pure fn) ───────────────────
def trail_stop(df: pd.DataFrame, i: int, window: int, buffer_pct: float,
               side: str = "long", current_sl: float = 0.0) -> Optional[float]:
    """VERBATIM port of the bt ENGINE 'trail' exit (exit_mode='trail' for ALL xnn runs):
    ssh bt-1 ~/hl-backtest/harness/engine.py:2101-2135 (_trail_long_sl/_trail_short_sl),
    called engine.py:1795-1797 with up_to_idx = i-1 and config vstop
    {pivot_window (default 3, engine.py:1207 — xnn configs do NOT override),
     buffer_pct (xnn deploy 0.15)}.

    Formula: most-recent FRACTAL pivot (low strictly below `window` neighbours each
    side / high strictly above), SL candidate = pivot*(1∓buffer), RATCHET vs current_sl
    (long: max, short: min; short current_sl<=0 → take proposed). NOT min-of-window —
    the previous uk-PM swing-extreme formula diverged from bt and was replaced
    2026-06-10 (review finding: trail equivalence unproven → now proven by source).
    Returns the ratcheted SL (== current_sl when no pivot / no improvement).
    """
    lows_or_highs = (df["Low"] if side == "long" else df["High"]).to_numpy(dtype=float)
    up_to_idx = i - 1                      # engine.py:1795 passes i-1 (pivot needs `window`
    if up_to_idx < 2 * window:             # confirmed bars AFTER it; bar i not yet usable)
        return current_sl if current_sl > 0 else None
    max_pivot_idx = up_to_idx - window
    if side == "long":
        lo = lows_or_highs
        for k in range(max_pivot_idx, window - 1, -1):
            val = lo[k]
            is_piv = True
            for j in range(1, window + 1):
                if lo[k - j] <= val or lo[k + j] <= val:
                    is_piv = False
                    break
            if is_piv:
                proposed = val * (1.0 - buffer_pct)
                return max(proposed, current_sl)
        return current_sl if current_sl > 0 else None
    hi = lows_or_highs
    for k in range(max_pivot_idx, window - 1, -1):
        val = hi[k]
        is_piv = True
        for j in range(1, window + 1):
            if hi[k - j] >= val or hi[k + j] >= val:
                is_piv = False
                break
        if is_piv:
            proposed = val * (1.0 + buffer_pct)
            if current_sl <= 0.0:
                return proposed
            return min(proposed, current_sl)
    return current_sl if current_sl > 0 else None
