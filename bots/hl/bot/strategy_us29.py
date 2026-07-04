"""strategy_us29.py — US29 adapter for the HL bot framework.

Drop-in contract clone of strategy_xnn (same 7 exported names, same signatures):
  Signal, Position, compute_indicators, scan_for_signal, scan_for_short_signal,
  PositionManager, _estimate_tick.

SIGNAL/EXIT MATH lives in bot/us29_core.py (mirrors strategy_xnn -> bot.xnn_core).
This module only ADAPTS:
  * scanner kwargs (donchian_k / f1 / ...) are ACCEPTED but IGNORED — US29 per-TF
    config is EMBEDDED below (US29_TF_CONFIG).
  * US29 is LONG-ONLY: scan_for_short_signal ALWAYS returns None.
  * Entry semantics: signal-bar close. trigger_price=entry_price=close[j] mapped onto
    the framework stop-limit continuation pipeline (same as xnn). NOTE: the US29 SIM
    spec assumes next_open fill — the live framework executes a stop-limit gate instead.
    This fill-model mismatch is a documented pre-money human checkpoint (build_spec
    open_risks #1); it does NOT change the code contract, only realized fills vs bt.
  * tp1_price is FICTIVE (journal NOT-NULL): entry + 1.618*sl_dist. No TP order placed
    (TP1_PARTIAL_FRAC=0). US29 has no take-profit / no breakeven / no time-stop.
  * EXIT = chandelier RAISE-ONLY stop (us29_core.trail_stop, MULT=7) — PositionManager
    owns the per-position state (frozen ATR_e at entry + running hh). This is NOT the
    xnn fractal-pivot vstop; the PM below replaces that logic entirely.

The cross-universe causal TOP-K (top-30%) ranking gate is NOT in this per-coin scan —
it is implemented in main.py main_loop as an expanding-prior pooled-score threshold
(build_spec section E / open_risks #6).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from bot import us29_core

log = logging.getLogger(__name__)


# ───────────────────────── Signal / Position (framework contract) ──────────────────────
@dataclass
class Signal:
    # Field set identical to strategy_xnn.Signal (NO __slots__ — trader/main stash extras).
    coin: str
    tf: str
    side: str                    # always "long" for US29
    trigger_price: float         # = signal-bar close (continuation-gate trigger)
    entry_price: float           # = signal-bar close
    sl_price: float              # swing-low-10 floored 0.5%
    tp1_price: float             # FICTIVE journal value: entry + 1.618*sl_dist
    sl_dist_pct: float           # |entry − sl| / entry
    pivot_high: float            # diagnostics: max High over sl_lookback window
    pivot_low: float             # diagnostics: min Low over sl_lookback window (SL anchor)
    bar_ts: int                  # timestamp of signal bar (ms UTC)
    atr14: float                 # WilderATR14 at signal bar
    ema20: float                 # diagnostics: EMA20 at signal bar
    f1_dist: float               # diagnostics: rank score (close-EMA20)/ATR14


@dataclass
class Position:
    # Verbatim field set of strategy_xnn.Position. NO __slots__ — trader/main stash
    # _sl_order_id / _sl_placed_px / _trade_id / _orig_size / _tp_oid via pos.__dict__.
    coin: str
    tf: str
    entry_price: float
    sl_initial: float
    sl_current: float
    tp1_price: float
    size: float                  # base asset units (always positive)
    bar_entry_idx: int
    side: str = "long"
    tp1_hit: bool = False
    trail_sl: Optional[float] = None
    tp1_partial_done: bool = False


# ───────────────────────── indicators (framework contract) ─────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Adds ema20/ema50/atr14 (used by the manage-loop / resting_orders, NOT by signal).

    Verbatim port of strategy_xnn.compute_indicators (pandas ewm/atr). US29 signal math
    in us29_core computes its own causal-seeded EMAs + Wilder ATR from raw OHLC.
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
    """Verbatim copy of strategy_xnn._estimate_tick."""
    if price <= 0:
        return 0.0001
    magnitude = 10 ** math.floor(math.log10(price))
    tick = magnitude * 0.0001
    return max(0.0001, min(tick, 0.01 * price))


# ───────────────────────── US29 per-TF embedded config ─────────────────────────────────
# ems=100 (slowest non-regime EMA) drives the framework's len(df)>=ems+3 guard; the REAL
# warmup gate (j>=201) is enforced inside us29_core.scan_signal regardless.
_US29_BASE: dict = {
    "ems": us29_core.EMA_SLOW,          # 100 (for the len(df)>=ems+3 contract guard)
    "sl_lookback": us29_core.SWING_LB,  # 10
    "chandelier_mult": us29_core.CHANDELIER_ATR_MULT,  # 7
    "raw_rr_target": 1.618,
    "allow_long": True,
    "allow_short": False,
}

US29_TF_CONFIG: dict = {
    "1d": {**_US29_BASE},
    "4h": {**_US29_BASE},
}


def _bar_ts_ms(df: pd.DataFrame, i: int) -> int:
    try:
        return int(df["time"].iloc[i].value // 10**6)
    except Exception:
        try:
            return int(pd.Timestamp(df["time"].iloc[i]).value // 10**6)
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
    tp1 = entry + rr * sl_dist  # FICTIVE (journal NOT-NULL); long-only
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
        pivot_high=float(df["High"].iloc[max(0, i - (k - 1)):i + 1].max()),
        pivot_low=float(df["Low"].iloc[max(0, i - (k - 1)):i + 1].min()),
        bar_ts=_bar_ts_ms(df, i),
        atr14=float(meta.get("atr14", 0.0)),
        ema20=float(meta.get("ema_fast_i", 0.0)),
        f1_dist=float(meta.get("score", 0.0)),   # rank score (diagnostic; topK gate in main)
    )


# ───────────────────────── scan entrypoints (framework contract) ───────────────────────
def scan_for_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,                  # IGNORED (uk knob) — US29 config embedded per-TF
    raw_rr_target: float,             # IGNORED (uk knob)
    require_ema50_up: bool,           # IGNORED (uk knob)
    f1_min_dist_ema20_atr: float,     # IGNORED (uk knob)
    tf_max_sl: dict,                  # IGNORED
    min_sl_dist_pct: float,           # IGNORED — US29 RISK_FLOOR embedded (0.005)
    f2_min_rsi14: float = 0.0,        # IGNORED
    f3_max_dollar_vol_usd: float = 0.0,  # IGNORED
) -> Optional[Signal]:
    """LONG scan on the last CLOSED bar of df (forming bar already trimmed by client).

    Signature == strategy_xnn.scan_for_signal (scanner.py calls by kwargs). uk-params
    accepted and ignored; the decision lives entirely in us29_core.scan_signal.
    """
    cfg = US29_TF_CONFIG.get(tf)
    if cfg is None or not cfg.get("allow_long", True):
        return None
    if df is None or len(df) < int(cfg["ems"]) + 3:
        return None
    res = us29_core.scan_signal(df, {**cfg, "allow_short": False})
    if res is None:
        return None
    sig = _build_signal(df, coin, tf, cfg, res)
    log.info("US29 LONG %s %s: close=%.6f sl=%.6f (%.2f%%) score=%.4f atr=%.6f",
             coin, tf, sig.entry_price, sig.sl_price, sig.sl_dist_pct * 100,
             sig.f1_dist, sig.atr14)
    return sig


def scan_for_short_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,                  # IGNORED
    raw_rr_target: float,             # IGNORED
    require_ema50_down: bool,         # IGNORED
    f1_min_dist_ema20_atr: float,     # IGNORED
    tf_max_sl: dict,                  # IGNORED
    min_sl_dist_pct: float,           # IGNORED
    f2_max_rsi14: float = 0.0,        # IGNORED
    f3_max_dollar_vol_usd: float = 0.0,  # IGNORED
) -> Optional[Signal]:
    """US29 is LONG-ONLY — no short leg. Always returns None (contract parity only)."""
    return None


# ───────────────────────── PositionManager (framework contract) ────────────────────────
class PositionManager:
    """US29 EXIT = chandelier RAISE-ONLY stop (MULT=7), NOT the xnn fractal-pivot vstop.

    Per-position state is stashed on pos.__dict__ (slots are off):
      _us29_atr_e : WilderATR14 frozen at the ENTRY bar (fallback nanmedian over 21 bars,
                    final fallback = initial sl_dist).
      _us29_hh    : running max High since entry (init = entry_price).
    cand = hh - 7*ATR_e ; pos.sl_current ratchets UP only (never lowered).

    Ctor signature is FIXED by main.py:522-528 (do NOT reorder):
      PositionManager(be_buffer_pct, vstop_pivot_window, max_run_r, vstop_buffer_pct,
                      tp1_partial_frac=0.5)
    vstop_pivot_window / vstop_buffer_pct / max_run_r are INERT for US29 (chandelier
    trail) but must be accepted/parseable. SL-hit + partial-BE shells copy xnn verbatim.
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
        self._vstop_window = vstop_pivot_window     # INERT for US29
        self._vstop_buffer = vstop_buffer_pct       # INERT for US29
        self._max_run_r = max_run_r                 # INERT (MAX_RUN_R=1000)
        self._tp1_frac = tp1_partial_frac
        self._chandelier_mult = float(us29_core.CHANDELIER_ATR_MULT)  # 7

    @staticmethod
    def _bar_ts_ms(df: pd.DataFrame, i: int) -> int:
        try:
            return int(pd.Timestamp(df["time"].iloc[i]).value // 10**6)
        except Exception:
            return 0

    def _ensure_chandelier_state(self, pos: "Position", df: pd.DataFrame) -> None:
        """Init frozen ATR_e (fallback path only) and running hh.

        F4: ATR_e is normally FROZEN at entry by attempt_entry (pos._us29_atr_e = signal.atr14
        = Wilder ATR14 at the signal bar). This recompute path is a FALLBACK only — used when
        the stash is missing/<=0 (e.g. a restart-adopted position with no stash). It keeps the
        chandelier alive but is not the bit-exact signal-bar value.
        """
        if not hasattr(pos, "_us29_atr_e") or getattr(pos, "_us29_atr_e", None) is None \
                or float(getattr(pos, "_us29_atr_e", 0.0) or 0.0) <= 0:
            atr_e = us29_core.signal_bar_atr14(df) if df is not None and not df.empty else 0.0
            if not atr_e or atr_e <= 0:
                # final fallback: initial sl distance (always > 0 for a valid long)
                atr_e = max(pos.entry_price - pos.sl_initial, 0.0)
            pos._us29_atr_e = float(atr_e)
        if not hasattr(pos, "_us29_hh") or getattr(pos, "_us29_hh", None) is None:
            pos._us29_hh = float(pos.entry_price)

    def update_sl_on_new_bar(
        self,
        pos: Position,
        df: pd.DataFrame,
        enable_trail_after_tp: bool = True,
    ) -> tuple[Optional[float], Optional[str]]:
        """Chandelier raise-only ratchet. Returns (new_sl_or_None, exit_reason_or_None).

        F7: hh = max(entry, running_hh, nanmax(High) over ALL bars in the manage frame since
            entry EXCLUDING the current/last bar) — incorporates bars missed during downtime,
            never lost, restart-robust. Falls back to the running value if no entry_ts is known.
        F8: the chandelier SL ratcheted on bar N is computed from hh that EXCLUDES bar N's own
            high (the sim computes the stop from highs through bar N-1, fills bar_low<=stop,
            THEN folds bar N's high). This means the just-closed bar's high can NEVER raise the
            SL above that same bar's low and self-trigger a wick_sl on the very bar that made
            the high — the trader.py manage loop checks check_sl_hit against this SL. Bar N's
            high is folded into the persisted running hh AFTER the ratchet, for the NEXT bar.
            Each closed bar is folded at most once (last-processed bar_ts gate).
        """
        if df is None or df.empty:
            return None, None
        i = len(df) - 1
        self._ensure_chandelier_state(pos, df)

        # max_run_r cap kept for interface parity — inert at MAX_RUN_R=1000 (no R-cap in US29)
        sl_dist = pos.entry_price - pos.sl_initial
        if sl_dist > 0:
            close_i = float(df["Close"].iloc[i])
            cur_r = (close_i - pos.entry_price) / sl_dist
            if cur_r >= self._max_run_r:
                log.info("MAX_RUN_R %s %s long @ %.6f", pos.coin, pos.tf, close_i)
                return None, "max_run_cap"

        cur_bar_ts = self._bar_ts_ms(df, i)
        entry_ts = int(getattr(pos, "_us29_entry_ts", 0) or 0)
        running_hh = float(getattr(pos, "_us29_hh", pos.entry_price) or pos.entry_price)

        # --- hh used for THIS bar's ratchet: highs since entry EXCLUDING the current bar (F7+F8)
        hh_prior = running_hh
        if "High" in df and "time" in df and len(df) > 1:
            try:
                high = df["High"].to_numpy(dtype=float)
                # Robust datetime->ms (pandas 3.x: Series.astype('int64') on datetime64[ms]
                # returns 0; numpy datetime64[ns] view is correct).
                ts = df["time"].to_numpy().astype("datetime64[ns]").astype("int64") // 10**6
                # bars strictly BEFORE the current/last bar, and at/after entry if known
                mask = ts < cur_bar_ts
                if entry_ts > 0:
                    mask = mask & (ts >= entry_ts)
                prior_highs = high[mask]
                prior_highs = prior_highs[np.isfinite(prior_highs)]
                if prior_highs.size:
                    hh_prior = max(hh_prior, float(np.nanmax(prior_highs)))
            except Exception:
                pass  # fall back to running_hh (raise-only — can only stall, never lower)
        hh_prior = max(hh_prior, float(pos.entry_price))

        cand = us29_core.trail_stop(
            df, i, self._chandelier_mult, float(pos._us29_atr_e),
            side=pos.side, current_sl=pos.sl_current, hh=hh_prior,
        )
        new_sl = None
        if cand is not None and pos.side == "long" and cand > pos.sl_current:
            pos.sl_current = cand
            pos.trail_sl = cand
            new_sl = cand

        # --- Persist running hh = max high through the PREVIOUS (non-current) bar (= hh_prior).
        # The current bar's OWN high is DELIBERATELY NOT persisted while it is the latest bar:
        # it would otherwise leak back into hh_prior on a same-bar re-run (60s manage ticks vs a
        # 4h/1d bar) and let the bar's own high raise the SL above its own low -> self-stop on
        # the very bar that made the high (F8). The current bar's high is incorporated only on
        # the NEXT call, when `mask = ts < cur_bar_ts` includes it. This makes same-bar re-runs
        # idempotent (raise-only, never lowered) and keeps hh_prior current-bar-excluded.
        pos._us29_hh = max(running_hh, hh_prior)
        return new_sl, None

    def apply_partial_be(self, pos: "Position") -> Optional[float]:
        """Dead at TP1_PARTIAL_FRAC=0 — kept because trader._detect_partial_fill may call it."""
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
        """Verbatim from xnn PM: gap-through fills at Open, wick at SL."""
        if df is None or df.empty:
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
        else:  # short (unused in US29)
            if o > sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and h >= sl:
                return (sl, "wick_sl")

        return None
