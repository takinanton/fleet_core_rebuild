"""strategy_xnn.py — XNN adapter for the HL bot framework (drop-in for strategy_uk_v102).

MODULE CONTRACT (importers: main.py, scanner.py, trader.py, resting_orders.py):
  Signal, Position, compute_indicators, scan_for_signal, scan_for_short_signal,
  PositionManager, _estimate_tick — same names/signatures as strategy_uk_v102.

SIGNAL MATH lives in bot/xnn_core.py (verbatim 1:1 port of xnn_long_stateless.py —
parity by construction). This module only ADAPTS:
  * scanner kwargs (donchian_k / f1 / ...) are ACCEPTED but IGNORED — XNN per-TF config
    is EMBEDDED below (XNN_TF_CONFIG); uk .env knobs (F1/F2/F3, DONCHIAN_K,
    REQUIRE_EMA50_*, ZIGZAG_RAW_LENGTH, RAW_RR_TARGET) are INERT for signal generation.
  * XNN entry semantics: entry AT CLOSE of the signal bar. Mapped onto the framework
    stop-limit pipeline as trigger_price = entry_price = signal close. trader.py then:
      - price-gate skip if mark already > close*(1+ENTRY_LIMIT_CAP_PCT)  (trader.py:256)
      - continuation-gate waits mark>=close within TTL, fills in [close, close*(1+cap)]
        (trader.py:590-613)
    => live fill ∈ [close, close*1.0024] OR skip; bt fill = close exactly. The skip /
    slip distribution vs bt MUST be measured in DRY (rejected_signals) before flip.
  * tp1_price is FICTIVE (journal NOT-NULL + Position field): entry + 1.618R (long).
    No TP order is placed when TP1_PARTIAL_FRAC=0 (mandatory in .env — XNN has no
    partial TP in bt).
  * exit: structural vstop ratchet (PositionManager below = verbatim uk_v102 PM logic,
    golden-sim-validated implementation of bt vstop_structure) with VSTOP_BUFFER_PCT=0.15.
    MAX_RUN_R must be set inert-high (1000) — XNN bt has no R-cap.

PER-TF DEPLOY CONFIG (user spec 2026-06-10):
  1d: adaptive UNION long-only, gates(corr_touch=10, slow_slope=20, gap=2, sl_lookback=10),
      min_sep_pct=0.004, clean_lk=0, ema_min_slope_atr=1.25
  8h: adaptive UNION long-only, gates(30, 60, 6, 30), sep=0.008, clean=0, slope_atr=1.25
  4h: FIXED emf=89 ems=144, long+short, gates(60, 120, 12, 60), sep=0, clean_lk=20, slope off
  all: min_signal_idx=1, allow_ungated=true (EXPLICIT divergence from canonical msi>=2),
      sig_min_rally_pct=0.02, exit=trail vstop buffer 15%,
      ema_candidates/fit from xnn_long_stateless defaults; ema_fit_window scaled by TF
      (1d=60, 8h=180, 4h=360 — 4h inert, fixed pair).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import numpy as np

from bot import xnn_core

log = logging.getLogger(__name__)


# ───────────────────────── Signal / Position (framework contract) ──────────────────────
@dataclass
class Signal:
    # Field set identical to strategy_uk_v102.Signal (trader.py + journal read all of these).
    coin: str
    tf: str
    side: str                    # "long" | "short"
    trigger_price: float         # XNN: = signal-bar close (continuation-gate trigger)
    entry_price: float           # XNN: = signal-bar close
    sl_price: float              # min(low[i-k:i+1]) - 1.0*atr14 (long); mirror short
    tp1_price: float             # FICTIVE journal value: entry ± 1.618R (no TP order placed)
    sl_dist_pct: float           # |entry − sl| / entry
    pivot_high: float            # diagnostics: max High over sl_lookback window
    pivot_low: float             # diagnostics: min Low over sl_lookback window (SL anchor)
    bar_ts: int                  # timestamp of signal bar (ms UTC)
    atr14: float                 # xnn ATR14 at signal bar
    ema20: float                 # diagnostics: FAST EMA of the firing pair (NOT ema20)
    f1_dist: float               # diagnostics: series count (NOT uk F1 distance)


@dataclass
class Position:
    # Verbatim copy of strategy_uk_v102.Position (strategy_uk_v102.py:67-81).
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


# ───────────────────────── indicators (framework contract) ─────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Verbatim copy of strategy_uk_v102.compute_indicators (strategy_uk_v102.py:83-108).

    Used by main.py manage-loop / resting_orders. XNN signal math does NOT use these
    columns — xnn_core computes its own EMAs + ATR from raw OHLC (parity isolation).
    """
    df = df.copy()
    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(span=14, adjust=False).mean()

    return df


def _estimate_tick(price: float) -> float:
    """Verbatim copy of strategy_uk_v102._estimate_tick (strategy_uk_v102.py:413-425)."""
    if price <= 0:
        return 0.0001
    magnitude = 10 ** math.floor(math.log10(price))
    tick = magnitude * 0.0001
    return max(0.0001, min(tick, 0.01 * price))


# ───────────────────────── XNN per-TF embedded config ──────────────────────────────────
# Defaults copied from xnn_long_stateless.Strategy.default_config (xnn:46-72), with the
# deploy-wide overrides applied (min_signal_idx=1 + allow_ungated=true, sig_min_rally=0.02,
# raw_rr_target=1.618, exit trail).
_XNN_BASE: dict = {
    "emf": 34, "ems": 55,
    "ema_adaptive": False,
    "ema_adaptive_mode": "best",
    "ema_candidates": [[34, 55], [55, 89], [89, 144], [144, 233], [233, 377], [377, 610]],
    "ema_fit_window": 60, "ema_min_slope_atr": 0.5,
    "ema_fit_max_cross_frac": 0.05, "ema_fit_min_onesided": 0.70,
    "use_trend_gate": False,
    "slope_lookback": 20, "slope_thr": 0.30,
    "er_window": 20, "er_thr": 0.35,
    "sl_lookback": 10, "sl_atr_buf": 1.0,
    "min_sl_dist_pct": 0.005, "max_sl_dist_pct": 0.15,
    "exit_mode": "trail", "raw_rr_target": 1.618, "atr_mult": 5.0,
    "max_hold_bars": 0,
    "min_signal_idx": 1,
    "sig_min_gap_bars": 0,
    "sig_min_height_pct": 0.0,
    "sig_min_rally_pct": 0.02,
    "rs_top_frac": 0.0,
    "min_price": 0.0,
    "clean_lk": 0,
    "min_sep_pct": 0.0,
    "corr_touch_lk": 0,
    "slow_slope_lk": 0,
    "allow_long": True, "allow_short": False,
    "allow_ungated": True,   # msi=1 < canonical 2 — EXPLICIT opt-out, logged at import
}

XNN_TF_CONFIG: dict = {
    # gates(corr_touch_lk, slow_slope_lk, sig_min_gap_bars, sl_lookback)
    "1d": {**_XNN_BASE,
           "ema_adaptive": True, "ema_adaptive_mode": "union",
           "corr_touch_lk": 10, "slow_slope_lk": 20, "sig_min_gap_bars": 2, "sl_lookback": 10,
           "min_sep_pct": 0.004, "clean_lk": 0, "ema_min_slope_atr": 1.25,
           "ema_fit_window": 60,
           "max_sl_dist_pct": xnn_core.TF_MAX_SL["1d"],     # 0.15
           "allow_long": True, "allow_short": False},
    "8h": {**_XNN_BASE,
           "ema_adaptive": True, "ema_adaptive_mode": "union",
           "corr_touch_lk": 30, "slow_slope_lk": 60, "sig_min_gap_bars": 6, "sl_lookback": 30,
           "min_sep_pct": 0.008, "clean_lk": 0, "ema_min_slope_atr": 1.25,
           "ema_fit_window": 180,                            # fit window scaled 1d->8h (x3)
           "max_sl_dist_pct": xnn_core.TF_MAX_SL["8h"],     # 0.10
           "allow_long": True, "allow_short": False},
    "4h": {**_XNN_BASE,
           "ema_adaptive": False,                            # FIXED pair
           "emf": 89, "ems": 144,
           "corr_touch_lk": 60, "slow_slope_lk": 120, "sig_min_gap_bars": 12, "sl_lookback": 60,
           "min_sep_pct": 0.0, "clean_lk": 20, "ema_min_slope_atr": 0.0,   # slope gate off (inert in fixed mode anyway)
           "ema_fit_window": 360,                            # inert (fixed pair)
           "max_sl_dist_pct": xnn_core.TF_MAX_SL["4h"],     # 0.08
           "allow_long": True, "allow_short": True},
}

# Forcing-fn at import: prints the explicit "allow_ungated" warning per TF (msi=1 deploy).
for _tf, _cfg in XNN_TF_CONFIG.items():
    xnn_core.assert_canonical_gates(_cfg)


def _bar_ts_ms(df: pd.DataFrame, i: int) -> int:
    try:
        return int(df["time"].iloc[i].value // 10**6)
    except Exception:
        return 0


def _build_signal(df: pd.DataFrame, coin: str, tf: str, cfg: dict, res: dict) -> Signal:
    i = res["signal_idx"]
    k = int(cfg["sl_lookback"])
    entry = float(res["entry_price"])
    sl = float(res["sl_price"])
    side = res["side"]
    sl_dist = abs(entry - sl)
    rr = float(cfg.get("raw_rr_target", 1.618) or 1.618)
    tp1 = entry + rr * sl_dist if side == "long" else entry - rr * sl_dist  # FICTIVE (journal)
    meta = res.get("meta", {})
    return Signal(
        coin=coin,
        tf=tf,
        side=side,
        trigger_price=entry,          # continuation-gate trigger = signal close
        entry_price=entry,
        sl_price=sl,
        tp1_price=tp1,
        sl_dist_pct=sl_dist / entry if entry > 0 else 0.0,
        pivot_high=float(df["High"].iloc[max(0, i - k):i + 1].max()),
        pivot_low=float(df["Low"].iloc[max(0, i - k):i + 1].min()),
        bar_ts=_bar_ts_ms(df, i),
        atr14=float(meta.get("atr14", 0.0)),
        ema20=float(meta.get("ema_fast_i", 0.0)),   # fast EMA of firing pair (diagnostic)
        f1_dist=float(meta.get("count", 0)),        # series count (diagnostic)
    )


# ───────────────────────── scan entrypoints (framework contract) ───────────────────────
def scan_for_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,                  # IGNORED (uk knob) — XNN config embedded per-TF
    raw_rr_target: float,             # IGNORED (uk knob)
    require_ema50_up: bool,           # IGNORED (uk knob)
    f1_min_dist_ema20_atr: float,     # IGNORED (uk knob)
    tf_max_sl: dict,                  # IGNORED — XNN TF_MAX_SL embedded (xnn_core.TF_MAX_SL)
    min_sl_dist_pct: float,           # IGNORED — XNN min_sl_dist_pct embedded (0.005)
    f2_min_rsi14: float = 0.0,        # IGNORED
    f3_max_dollar_vol_usd: float = 0.0,  # IGNORED
) -> Optional[Signal]:
    """LONG scan на последнем ЗАКРЫТОМ баре df (forming bar уже отрезан клиентом —
    exchange_hl.candles, exchange_hl.py:493-495; scanner вызывает только на new_bar_closed).

    Signature == strategy_uk_v102.scan_for_signal (scanner.py:170-180 calls by kwargs).
    uk-параметры принимаются и игнорируются; решение целиком в xnn_core.scan_signal.
    """
    cfg = XNN_TF_CONFIG.get(tf)
    if cfg is None or not cfg.get("allow_long", True):
        return None
    if df is None or len(df) < int(cfg["ems"]) + 3:
        return None
    res = xnn_core.scan_signal(df, {**cfg, "allow_short": False})
    if res is None:
        return None
    sig = _build_signal(df, coin, tf, cfg, res)
    log.info("XNN LONG %s %s: close=%.6f sl=%.6f (%.2f%%) pair=%s count=%s",
             coin, tf, sig.entry_price, sig.sl_price, sig.sl_dist_pct * 100,
             res["meta"].get("pair"), res["meta"].get("count"))
    return sig


def scan_for_short_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,                  # IGNORED (uk knob)
    raw_rr_target: float,             # IGNORED
    require_ema50_down: bool,         # IGNORED
    f1_min_dist_ema20_atr: float,     # IGNORED
    tf_max_sl: dict,                  # IGNORED
    min_sl_dist_pct: float,           # IGNORED
    f2_max_rsi14: float = 0.0,        # IGNORED
    f3_max_dollar_vol_usd: float = 0.0,  # IGNORED
) -> Optional[Signal]:
    """SHORT scan — only fires on TFs with allow_short=true (deploy: 4h only).

    Signature == strategy_uk_v102.scan_for_short_signal (scanner.py:181-192).
    """
    cfg = XNN_TF_CONFIG.get(tf)
    if cfg is None or not cfg.get("allow_short", False):
        return None
    if df is None or len(df) < int(cfg["ems"]) + 3:
        return None
    res = xnn_core.scan_signal(df, {**cfg, "allow_long": False, "allow_short": True})
    if res is None:
        return None
    sig = _build_signal(df, coin, tf, cfg, res)
    log.info("XNN SHORT %s %s: close=%.6f sl=%.6f (%.2f%%) pair=%s count=%s",
             coin, tf, sig.entry_price, sig.sl_price, sig.sl_dist_pct * 100,
             res["meta"].get("pair"), res["meta"].get("count"))
    return sig


# ───────────────────────── PositionManager (framework contract) ────────────────────────
class PositionManager:
    """vstop trail (ratchet only): SL-hit / partial-BE shell verbatim from
    strategy_uk_v102.PM (uk:428-557), trail CANDIDATE delegated to xnn_core.trail_stop —
    which since 2026-06-10 is the VERBATIM bt-engine pivot formula
    (bt-1 harness/engine.py:2101-2135, called :1795-1797 with up_to_idx=i-1; the old
    uk-PM min-of-window swing formula DIVERGED from bt and was replaced).

    XNN deploy (.env): VSTOP_PIVOT_WINDOW=3 (bt engine default, engine.py:1207 — xnn
    configs do not override), VSTOP_BUFFER_PCT=0.15, TP1_PARTIAL_FRAC=0 (no partial —
    apply_partial_be dead code kept for interface), MAX_RUN_R=1000 (inert — no R-cap in bt).
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
        """bt-engine pivot vstop candidate (already ratcheted vs pos.sl_current inside)."""
        return xnn_core.trail_stop(df, i, self._vstop_window, self._vstop_buffer,
                                   side=pos.side, current_sl=pos.sl_current)

    def update_sl_on_new_bar(
        self,
        pos: Position,
        df: pd.DataFrame,
        enable_trail_after_tp: bool = True,
    ) -> tuple[Optional[float], Optional[str]]:
        """Verbatim logic of strategy_uk_v102.PM.update_sl_on_new_bar (uk:462-503)."""
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

        # max_run_r cap — inert at MAX_RUN_R=1000 but kept for interface parity
        cur_r = ((close_i - pos.entry_price) if pos.side == "long"
                 else (pos.entry_price - close_i)) / sl_dist
        if cur_r >= self._max_run_r:
            log.info("MAX_RUN_R %s %s %s @ %.6f", pos.coin, pos.tf, pos.side, close_i)
            return None, "max_run_cap"

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
        """Verbatim from uk PM (uk:505-522). Dead code at TP1_PARTIAL_FRAC=0 — kept because
        trader._detect_partial_fill can still call it on a manual partial close."""
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
        """Verbatim from uk PM (uk:524-557): gap-through fills at Open, wick at SL."""
        if df.empty:
            return None

        bar = df.iloc[-1]
        o = float(bar["Open"])
        h = float(bar["High"])
        l = float(bar["Low"])
        sl = pos.sl_current

        if pos.side == "long":
            if o < sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and l <= sl:
                return (sl, "wick_sl")
        else:  # short
            if o > sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and h >= sl:
                return (sl, "wick_sl")

        return None
