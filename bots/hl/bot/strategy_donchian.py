"""strategy_donchian.py — Donchian-8h crypto-perps leg for the HL combo bot.

Drop-in contract clone of strategy_us29 / strategy_xnn (SAME 7 exported names,
SAME signatures) so scanner.py can route native-crypto coins here per-coin:
  Signal, Position, compute_indicators, scan_for_signal, scan_for_short_signal,
  PositionManager, _estimate_tick.

Logic ported verbatim from bt-1 strategies/uk_v10c_donchian_calmar.py
(uk_v10c_donchian_calmar) — pure Donchian channel breakout, long-only:

  ENTRY (last CLOSED bar j=len(df)-1):
    LONG when close[j] > max(High[j-N : j])           (PREVIOUS N highs, EXCL bar j)
    N = DONCHIAN_N = 20.
  SL:
    sl_raw = min(Low[j-N : j]) * (1 - SL_BUFFER_PCT)   (PREVIOUS N lows, EXCL bar j)
    reject if sl_raw >= entry.
    sl_dist_pct = (entry - sl_raw)/entry; reject if < MIN_SL_DIST_PCT (0.5%) or
    > MAX_SL_DIST_PCT (10%). (uk_v10c rejects out-of-band rather than clamping; we
    keep that exact semantics — clamping would change the realized risk unit.)
  TP:
    tp = entry + TP_R_MULTIPLE * (entry - sl)          TP_R_MULTIPLE = 4.0
    (informational/journal — like us29's tp1, no resting TP order placed; exit is the
    trail + time-stop. The framework trader manages the SL order; tp1_price is the
    journal NOT-NULL value AND the 4R take is realized via PositionManager.)
  EXIT (PositionManager) — order/mechanism match bt-1 maybe_exit + engine vstop:
    (1) time_stop at MAX_HOLD_BARS = 120 bars-held (checked FIRST),
    (2) TP take at 4R,
    (3) raise-only PIVOT-LOW vstop trail (engine.py _trail_long_sl, pivot_window=3,
        buffer=0.003) — ratchet SL up only. NO chandelier (Audit MED 2026-06-20).
  SHORT:
    long-only — scan_for_short_signal ALWAYS returns None (contract parity only).

The cross-universe selection (top-K / regime) for the crypto leg, if any, is a
main.py concern; this per-coin scan only decides the raw Donchian breakout signal.

NOTE on fill model: the bt sim enters at signal-bar close (close[j]); the live
framework executes a stop-limit continuation gate (trigger_price = entry_price =
close[j]) — same documented fill-model checkpoint as strategy_us29. The code
contract is unchanged; only realized fills vs bt differ.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ───────────────────────── Donchian params (ported from uk_v10d_combo) ───────
# CUTOVER 2026-06-20 v10c -> v10d: exhaustive all-fronts sweep + 4-lens adversarial panel
# (clustered-t / LOO-top2 / sub-period thirds) selected uk_v10d_combo as the ONLY config
# surviving every adversarial cut (OOSt2.1, coin-clustered t2.3, LOO-top2 t2.2, thirds all+);
# high-Calmar cells were concentration mirages (edge in <=2 coins). Pinned to bt-1
# strategies/uk_v10d_combo.py default_config (parity-confirmed by-name == swept winner).
DONCHIAN_N = 15            # v10d: 20 -> 15 (broad edge across coins; clustered+LOO significant)
SL_BUFFER_PCT = 0.010      # v10d: 0.005 -> 0.010 (N15 sl.010 DD30 vs sl.005 DD38; bt-1
                           # uk_v10d_combo sl_buffer_pct=0.010). Entry SL = swing-low*(1-this).
MIN_SL_DIST_PCT = 0.005    # 0.5% minimum SL distance (reject below)
MAX_SL_DIST_PCT = 0.10     # 10% maximum SL distance (reject above)
TP_R_MULTIPLE = 999.0      # v10d: 4.0 -> 999 = NO FIXED TP (trailing-only). TP caps the right
                           # tail; dropping it lifts avgR/CAGR/Calmar at every N. tp1 = entry +
                           # 999*sl_dist is unreachable so the 'tp' exit never fires; trail+time
                           # govern. TP1_PARTIAL_FRAC=0 already (no resting TP order placed).
MAX_HOLD_BARS = 120        # time-stop (bars held) — unchanged (120 optimal in sweep)
ATR_LEN = 14               # Wilder ATR (frozen at entry; kept for sl_dist fallback only)
CHANDELIER_ATR_MULT = 3.0  # INERT (Audit MED 2026-06-20): the validated bt-1 trail is pivot-
                           # low vstop, NOT chandelier — see TRAIL_PIVOT_WINDOW/_BUFFER below.
# ── Trailing SL = pivot-low vstop (Audit MED 2026-06-20, bt-1 parity) ────────────────────
# The validated uk_v10c_donchian_calmar run trails the SL via the ENGINE's pivot-low vstop
# (harness/engine.py:1804 `_trail_long_sl(lo, i-1, vstop_window, vstop_buffer, sl_current)`),
# NOT a chandelier. The validated run config pins vstop{pivot_window:3, buffer_pct:0.003,
# wick_check:true} (data/results/runs/uk_v10c_donchian_calmar_8h_.../config.json). Pin those
# here as module constants — NOT from the shared .env _pm_kwargs, which carries the 4h-XNN-
# tuned VSTOP_BUFFER_PCT=0.15 and would diverge the donchian leg's trail from the backtest.
TRAIL_PIVOT_WINDOW = 2     # v10d: 3 -> 2 (flat plateau, lower DD, better OOS-t); bt-1 vstop.pivot_window
TRAIL_VSTOP_BUFFER = 0.005 # v10d: 0.003 -> 0.005 (clean Pareto: Calmar up, DD down); bt-1 vstop.buffer_pct
# Warmup: need at least N+ a few bars for the channel; keep a safe floor so the
# len(df) >= ems+3 contract guard and the channel both have room.
WARMUP_BARS = DONCHIAN_N + 5

# ── TF gate (SCOPE fix 2026-06-19) ──────────────────────────────────────────────────
# GOAL: the crypto-perps leg is donchian-*8h-only*. scanner.py routes every native-crypto
# coin here for EVERY tf in WORKING_TFS (1d,4h,8h) — and WORKING_TFS must keep 1d/4h for
# the us29 (xyz_) leg — so without an internal gate donchian also fired on 1d AND 4h.
# Mirror strategy_us29.US29_TF_CONFIG: this module owns its own TF set and returns None
# for any tf outside it. Env-driven (DONCHIAN_TFS=8h default) so the operator can widen
# without code change; empty/unset -> {8h}. scan_for_short_signal is long-only None anyway.
import os as _os_tf
DONCHIAN_TFS: frozenset = frozenset(
    t.strip() for t in _os_tf.getenv("DONCHIAN_TFS", "8h").split(",") if t.strip()
) or frozenset({"8h"})


# ───────────────────────── Signal / Position (framework contract) ──────────────────────
@dataclass
class Signal:
    # Field set identical to strategy_us29.Signal / strategy_xnn.Signal
    # (NO __slots__ — trader/main stash extras via __dict__).
    coin: str
    tf: str
    side: str                    # always "long" for donchian
    trigger_price: float         # = signal-bar close (continuation-gate trigger)
    entry_price: float           # = signal-bar close
    sl_price: float              # N-bar swing-low * (1 - buffer), band-checked
    tp1_price: float             # entry + TP_R_MULTIPLE * sl_dist
    sl_dist_pct: float           # |entry - sl| / entry
    pivot_high: float            # diagnostics: Donchian upper (max High prev-N) — broken level
    pivot_low: float             # diagnostics: Donchian lower (min Low prev-N) — SL anchor
    bar_ts: int                  # timestamp of signal bar (ms UTC)
    atr14: float                 # WilderATR14 at signal bar (frozen ATR_e for chandelier)
    ema20: float                 # diagnostics: EMA20 at signal bar
    f1_dist: float               # diagnostics: rank score (close-EMA20)/ATR14


@dataclass
class Position:
    # Verbatim field set of strategy_us29.Position / strategy_xnn.Position.
    # NO __slots__ — trader/main stash _sl_order_id / _trade_id / etc via pos.__dict__.
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

    Verbatim shape of strategy_us29.compute_indicators. The Donchian channel + ATR_e used
    by the signal/trail are recomputed from raw OHLC inside this module (causal), so these
    pandas ewm columns are advisory only.
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
    """Verbatim copy of strategy_us29._estimate_tick / strategy_xnn._estimate_tick."""
    if price <= 0:
        return 0.0001
    magnitude = 10 ** math.floor(math.log10(price))
    tick = magnitude * 0.0001
    return max(0.0001, min(tick, 0.01 * price))


# ───────────────────────── Wilder ATR (causal, for chandelier ATR_e) ───────────────────
def _wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                length: int = ATR_LEN) -> np.ndarray:
    """Wilder ATR (RMA of True Range). ATR[length-1]=mean(TR[0:length]); recursive after.
    Indices < length-1 are NaN."""
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
    atr[length - 1] = float(np.mean(tr[0:length]))
    for i in range(length, n):
        atr[i] = (atr[i - 1] * (length - 1) + tr[i]) / length
    return atr


def _signal_bar_atr14(df: pd.DataFrame) -> float:
    """Wilder ATR14 at the last bar (frozen as ATR_e). Fallback nanmedian over 21 bars; 0.0
    if nothing usable (caller applies sl_dist fallback)."""
    if df is None or len(df) == 0:
        return 0.0
    h = df["High"].to_numpy(dtype=float)
    l = df["Low"].to_numpy(dtype=float)
    c = df["Close"].to_numpy(dtype=float)
    atr = _wilder_atr(h, l, c)
    last = float(atr[-1])
    if np.isfinite(last) and last > 0:
        return last
    tail = atr[-21:]
    med = float(np.nanmedian(tail)) if tail.size else np.nan
    if np.isfinite(med) and med > 0:
        return med
    return 0.0


def _bar_ts_ms(df: pd.DataFrame, i: int) -> int:
    try:
        return int(pd.Timestamp(df["time"].iloc[i]).value // 10**6)
    except Exception:
        return 0


def _trail_long_sl(
    lows: np.ndarray,
    up_to_idx: int,
    window: int,
    buffer_pct: float,
    current_sl: float,
) -> float:
    """Pivot-low vstop trail — VERBATIM port of bt-1 harness/engine.py:2110 `_trail_long_sl`.

    Scans backward from the most-recent CONFIRMABLE pivot (k <= up_to_idx - window) for a
    strict pivot-low: lows[k] strictly LESS than every neighbour within ±window. The proposed
    SL is pivot_low*(1-buffer_pct); raise-only via max(proposed, current_sl). Returns
    current_sl unchanged when no confirmed pivot exists (insufficient bars, or none strict).

    Engine call site passes up_to_idx = i-1 (the bar BEFORE the current/last bar) so the
    pivot's right-side confirmation bars never include bar i — the trailed SL therefore
    EXCLUDES the current bar's own low and cannot self-stop on the bar that set it (the F8
    self-trigger-safety the chandelier path enforced via `ts < cur_bar_ts`).
    """
    if up_to_idx < 2 * window:
        return current_sl
    max_pivot_idx = up_to_idx - window
    for k in range(max_pivot_idx, window - 1, -1):
        val = float(lows[k])
        is_piv = True
        for j in range(1, window + 1):
            if lows[k - j] <= val or lows[k + j] <= val:
                is_piv = False
                break
        if is_piv:
            proposed = val * (1.0 - buffer_pct)
            return max(proposed, current_sl)
    return current_sl


# ───────────────────────── scan entrypoints (framework contract) ───────────────────────
def scan_for_signal(
    df: pd.DataFrame,
    coin: str,
    tf: str,
    donchian_k: int,                  # uk knob — used as the Donchian N override if >0
    raw_rr_target: float,             # IGNORED (TP_R_MULTIPLE embedded = 4.0)
    require_ema50_up: bool,           # IGNORED (pure trend-follow, no extra filter)
    f1_min_dist_ema20_atr: float,     # IGNORED
    tf_max_sl: dict,                  # IGNORED (MAX_SL_DIST_PCT embedded = 0.10)
    min_sl_dist_pct: float,           # IGNORED (MIN_SL_DIST_PCT embedded = 0.005)
    f2_min_rsi14: float = 0.0,        # IGNORED
    f3_max_dollar_vol_usd: float = 0.0,  # IGNORED
) -> Optional[Signal]:
    """LONG Donchian breakout on the last CLOSED bar of df (forming bar trimmed by client).

    Signature == strategy_us29.scan_for_signal (scanner.py calls by kwargs). uk-params are
    accepted; only donchian_k is honoured (as an N override when > 0) — everything else
    embedded per the ported uk_v10c spec.
    """
    # TF gate (SCOPE fix): donchian crypto leg is 8h-only by intent. Mirror us29's
    # US29_TF_CONFIG.get(tf) is None -> return None. Keeps the router/WORKING_TFS
    # untouched (us29 still needs 1d/4h) while restricting THIS leg to DONCHIAN_TFS.
    if tf not in DONCHIAN_TFS:
        return None
    n = int(donchian_k) if donchian_k and int(donchian_k) > 0 else DONCHIAN_N
    if df is None or len(df) < max(WARMUP_BARS, n + 3):
        return None

    high = df["High"].to_numpy(dtype=float)
    low = df["Low"].to_numpy(dtype=float)
    close = df["Close"].to_numpy(dtype=float)
    nb = close.shape[0]
    j = nb - 1
    if j - n < 0:
        return None

    # Donchian channel over the PREVIOUS n bars, EXCLUDING the signal bar j
    # (mirrors uk_v10c: max(high[i-n:i]) / min(low[i-n:i])).
    h_window = high[j - n: j]
    l_window = low[j - n: j]
    if h_window.size == 0 or l_window.size == 0:
        return None
    h_n = float(np.nanmax(h_window))
    l_n = float(np.nanmin(l_window))
    entry = float(close[j])
    if not np.isfinite(h_n) or not np.isfinite(l_n) or not np.isfinite(entry) or entry <= 0:
        return None

    # ENTRY: close breaks above the upper channel
    if entry <= h_n:
        return None

    # SL: lower channel minus buffer; reject if degenerate or out-of-band [0.5%, 10%]
    sl_price = l_n * (1.0 - SL_BUFFER_PCT)
    if sl_price >= entry or sl_price <= 0:
        return None
    sl_dist_pct = (entry - sl_price) / entry
    if sl_dist_pct < MIN_SL_DIST_PCT:
        return None
    if sl_dist_pct > MAX_SL_DIST_PCT:
        return None

    sl_dist = entry - sl_price
    tp1 = entry + TP_R_MULTIPLE * sl_dist

    atr_e = _signal_bar_atr14(df)
    e20 = float(df["ema20"].iloc[j]) if "ema20" in df.columns else entry
    score = (entry - e20) / atr_e if atr_e and atr_e > 0 else 0.0

    sig = Signal(
        coin=coin,
        tf=tf,
        side="long",
        trigger_price=entry,          # continuation-gate trigger = signal close
        entry_price=entry,
        sl_price=float(sl_price),
        tp1_price=float(tp1),
        sl_dist_pct=float(sl_dist_pct),
        pivot_high=h_n,               # broken Donchian upper
        pivot_low=l_n,                # Donchian lower (SL anchor pre-buffer)
        bar_ts=_bar_ts_ms(df, j),
        atr14=float(atr_e),
        ema20=float(e20),
        f1_dist=float(score),
    )
    log.info(
        "DONCHIAN LONG %s %s: close=%.6f > upper=%.6f sl=%.6f (%.2f%%) tp=%.6f atr=%.6f",
        coin, tf, entry, h_n, sl_price, sl_dist_pct * 100, tp1, atr_e,
    )
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
    """Donchian is LONG-ONLY — no short leg. Always returns None (contract parity only)."""
    return None


# ───────────────────────── PositionManager (framework contract) ────────────────────────
class PositionManager:
    """Donchian EXIT = time_stop(120) FIRST -> 4R take -> raise-only pivot-low vstop trail.

    Mechanism + order match bt-1 uk_v10c_donchian_calmar.maybe_exit() + engine `_trail_long_sl`
    (Audit MED 2026-06-20): the trail is the pivot-low vstop (pivot_window=3, buffer=0.003 from
    the validated run config), NOT a chandelier; time_stop is checked before tp; time_stop
    fires at E+121 (bt-1 open_bars == bars_held-1).

    Per-position state stashed on pos.__dict__ (slots off):
      _donch_atr_e : WilderATR14 frozen at the ENTRY bar (sl_dist fallback only; trail no
                     longer uses ATR — kept for interface/journal parity).
      _donch_hh    : running max High since entry (LEGACY chandelier state — no longer read by
                     the trail; left set by _ensure_state for backward compat / debug only).
      _donch_bars  : bars-held counter (incremented once per NEW closed bar; in-memory
                     fast-path / fallback when no entry_ts is known).
      _donch_entry_ts : ms of the ENTRY/signal bar (stashed by trader.attempt_entry, F7).
                     The 120-bar time-stop is derived from CLOSED bars at/after this ts so
                     it is RESTART-PERSISTENT (Audit MED 2026-06-19): a restart resets the
                     in-memory _donch_bars to 0, which alone would re-arm the full 120-bar
                     clock and let a position overstay its time-stop across restarts.
      _donch_last_bar_ts : ms of the last bar folded (idempotency gate for 60s re-runs).
    cand = _trail_long_sl(lows, i-1, pivot_window=3, buffer=0.003, sl_current) ; ratchets UP only.

    Ctor signature is FIXED by main.py (do NOT reorder) — identical to strategy_us29:
      PositionManager(be_buffer_pct, vstop_pivot_window, max_run_r, vstop_buffer_pct,
                      tp1_partial_frac=0.5)
    vstop_pivot_window / vstop_buffer_pct / max_run_r are accepted for interface parity but the
    donchian trail uses the VALIDATED-config-pinned TRAIL_PIVOT_WINDOW / TRAIL_VSTOP_BUFFER
    (the shared .env _pm_kwargs carries the 4h-XNN VSTOP_BUFFER_PCT=0.15). update_sl_on_new_bar
    returns (new_sl_or_None, exit_reason_or_None) — exit_reason carries 'time_stop' / 'tp' so the
    trader manage loop can close the position (same channel strategy_us29 uses).
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
        self._vstop_window = vstop_pivot_window     # accepted (interface parity)
        self._vstop_buffer = vstop_buffer_pct       # accepted (interface parity)
        self._max_run_r = max_run_r                 # accepted (interface parity)
        self._tp1_frac = tp1_partial_frac
        self._chandelier_mult = float(CHANDELIER_ATR_MULT)  # INERT (Audit MED 2026-06-20)
        self._max_hold_bars = int(MAX_HOLD_BARS)
        self._tp_r_multiple = float(TP_R_MULTIPLE)
        # Pivot-low vstop trail params — pinned to the VALIDATED bt-1 uk_v10c run config
        # (vstop{pivot_window:3, buffer_pct:0.003}), NOT the shared .env _pm_kwargs
        # (vstop_pivot_window / vstop_buffer_pct carry the 4h-XNN VSTOP_BUFFER_PCT=0.15).
        self._trail_pivot_window = int(TRAIL_PIVOT_WINDOW)
        self._trail_vstop_buffer = float(TRAIL_VSTOP_BUFFER)

    @staticmethod
    def _bar_ts_ms(df: pd.DataFrame, i: int) -> int:
        try:
            return int(pd.Timestamp(df["time"].iloc[i]).value // 10**6)
        except Exception:
            return 0

    def _ensure_state(self, pos: "Position", df: pd.DataFrame) -> None:
        """Init frozen ATR_e (fallback path only), running hh, bars-held counter."""
        if not hasattr(pos, "_donch_atr_e") or getattr(pos, "_donch_atr_e", None) is None \
                or float(getattr(pos, "_donch_atr_e", 0.0) or 0.0) <= 0:
            atr_e = _signal_bar_atr14(df) if df is not None and not df.empty else 0.0
            if not atr_e or atr_e <= 0:
                atr_e = max(pos.entry_price - pos.sl_initial, 0.0)  # final fallback: sl dist
            pos._donch_atr_e = float(atr_e)
        if not hasattr(pos, "_donch_hh") or getattr(pos, "_donch_hh", None) is None:
            pos._donch_hh = float(pos.entry_price)
        if not hasattr(pos, "_donch_bars") or getattr(pos, "_donch_bars", None) is None:
            pos._donch_bars = 0
        if not hasattr(pos, "_donch_entry_ts") or getattr(pos, "_donch_entry_ts", None) is None:
            pos._donch_entry_ts = 0
        if not hasattr(pos, "_donch_last_bar_ts"):
            pos._donch_last_bar_ts = 0
        # Staged next-bar trail (trail_{i-1}); promoted to sl_current on the NEXT new bar so
        # the current bar resolves+rests at trail_{i-2} for all ~480 re-presentation ticks
        # (Audit MED 2026-06-20, Issue 1). None until the first trail is computed; raise-only.
        if not hasattr(pos, "_donch_sl_staged"):
            pos._donch_sl_staged = None

    def is_entry_bar(self, pos: "Position", df: pd.DataFrame) -> bool:
        """True iff the LATEST closed bar of df IS the position's entry/signal bar E.

        bt-1 (harness/engine.py) never evaluates an exit on bar E — its exit block runs while
        open_pos is still None on E and the position is created AFTERWARDS, so the earliest exit
        eval is bar E+1 (validated run open_bars min=1, open_bars==0 count=0 / 814 trades). The
        live manage loop is first invoked while the latest closed bar still IS E; the trader uses
        this predicate to SUPPRESS check_sl_hit on E (the strategy's update_sl_on_new_bar suppresses
        its own time-stop/TP/trail on E independently). Match key: latest closed bar ts == the
        frozen entry-bar ts stashed by attempt_entry / re-seeded on restore (_donch_entry_ts). When
        entry_ts is unknown (0) this returns False (fail-OPEN to the normal SL path — never blocks a
        protective exit), which is safe: a missing entry_ts only occurs post-restart where the latest
        bar is already past E anyway.
        """
        if df is None or df.empty:
            return False
        entry_ts = int(getattr(pos, "_donch_entry_ts", 0) or 0)
        if entry_ts <= 0:
            return False
        cur_bar_ts = self._bar_ts_ms(df, len(df) - 1)
        return cur_bar_ts == entry_ts

    def update_sl_on_new_bar(
        self,
        pos: Position,
        df: pd.DataFrame,
        enable_trail_after_tp: bool = True,
    ) -> tuple[Optional[float], Optional[str]]:
        """Raise-only pivot-low vstop trail + time-stop + 4R take (bt-1 uk_v10c parity).

        Returns (new_sl_or_None, exit_reason_or_None). exit_reason in {'time_stop','tp'}
        signals the trader manage loop to CLOSE the position. The SL itself is never lowered.

        Exit-check ORDER mirrors bt-1 uk_v10c_donchian_calmar.maybe_exit() (engine.py via
        strategies/uk_v10c_donchian_calmar.py:53-65): TIME-STOP is tested FIRST, then the 4R
        TP (Audit MED 2026-06-20, Issue 3 — was inverted: TP-then-time_stop). On the single
        bar where bars_held reaches the time-stop AND that same bar's high wicks to entry+4R,
        bt-1 records reason='time_stop' at close[i]; live now matches instead of emitting 'tp'.

        The trail is the ENGINE's pivot-low vstop (engine.py:1804 `_trail_long_sl(lo, i-1, ...)`),
        NOT a chandelier (Audit MED 2026-06-20, Issue 1 — the validated uk_v10c run has no
        chandelier; its SL is ratcheted purely by `_trail_long_sl` with vstop{pivot_window:3,
        buffer_pct:0.003}). `enable_trail_after_tp` accepted for interface parity (bt-1 trails
        every bar raise-only regardless).
        """
        if df is None or df.empty or pos.side != "long":
            return None, None
        i = len(df) - 1
        self._ensure_state(pos, df)

        cur_bar_ts = self._bar_ts_ms(df, i)
        last_folded = int(getattr(pos, "_donch_last_bar_ts", 0) or 0)
        new_bar = cur_bar_ts > last_folded  # only advance per NEW closed bar (60s re-run safe)

        # --- ENTRY-BAR EXIT SUPPRESSION (Audit MED 2026-06-20, Issue: entry-bar exit check) -----
        # bt-1 NEVER evaluates an exit on the ENTRY/SIGNAL bar E. In harness/engine.py the bar loop
        # runs the EXIT block first (only when open_pos is not None) and the ENTRY block AFTER it;
        # on bar E open_pos is still None when the exit block runs, so no maybe_exit / trailing-SL
        # resolution happens for E — the position is created at the END of bar E and its FIRST exit
        # eval is on bar E+1 (open_bars min = 1, open_bars==0 count = 0 across the validated run
        # 8f870f1280dc_..._4df027/trades.parquet, 814 trades). The live manage loop, by contrast,
        # is first invoked while the latest CLOSED bar still IS the entry bar E (_donch_last_bar_ts
        # inits to 0 -> new_bar=True), so without this guard it would run time-stop/4R-TP/trail AND
        # the trader's check_sl_hit on bar E — one bar EARLIER than bt-1's E+1 minimum (a wide-range
        # breakout bar whose own low <= sl_initial, or high >= entry+4R, would exit live on E).
        #
        # Guard: the latest closed bar IS the entry bar exactly when cur_bar_ts == _donch_entry_ts.
        # On that tick, mirror bt-1: NO time-stop, NO TP, NO trail, NO sl-resolution. We DO mark the
        # bar folded (idempotent across the ~480 re-presentation ticks of bar E) and seed the bars
        # counter, but emit no exit and stage no trail (bt-1 also doesn't trail on E). The trader's
        # check_sl_hit is separately gated via is_entry_bar() below.
        entry_ts_guard = int(getattr(pos, "_donch_entry_ts", 0) or 0)
        if entry_ts_guard > 0 and cur_bar_ts == entry_ts_guard:
            if new_bar:
                pos._donch_last_bar_ts = cur_bar_ts  # fold E once; no exit/trail evaluated on E
            return None, None

        close_i = float(df["Close"].iloc[i])
        high_i = float(df["High"].iloc[i])

        # --- (A) time-stop FIRST: close after MAX_HOLD_BARS bars held ---------------------
        # ORDER: bt-1 maybe_exit checks `open_bars >= max_hold` BEFORE the 4R tp (Issue 3).
        #
        # OFF-BY-ONE (Audit MED 2026-06-20, Issue 2): bt-1 creates the Position with
        # open_bars=0 on the ENTRY bar E (no exit check on E — the entry lifecycle runs AFTER
        # the exit block, when open_pos was still None), then increments open_bars by 1 AFTER
        # each surviving bar's maybe_exit (engine.py:1812). So on bar E+k maybe_exit sees
        # open_bars = k-1, and `open_bars >= 120` first fires on bar E+121. Our bars_held counts
        # CLOSED bars in (entry_ts, cur_bar_ts] = k on bar E+k -> bars_held is bt-1's open_bars
        # PLUS ONE. Comparing `bars_held - 1 >= max_hold` (== `open_bars >= max_hold`) realigns
        # the fire bar to E+121 (was E+120, one bar early).
        #
        # Restart-persistent: a pure in-memory counter resets to 0 on a bot restart and would
        # re-arm the full clock. Derive bars-held from CLOSED bars at/after the frozen entry_ts
        # (stashed by attempt_entry) and max() with the in-memory counter (raise-only). Identical
        # to the in-memory path when no restart occurs; recovers the true elapsed count after a
        # restart; falls back to the in-memory counter when entry_ts is unknown.
        if new_bar:
            pos._donch_bars = int(getattr(pos, "_donch_bars", 0) or 0) + 1
        bars_held = int(getattr(pos, "_donch_bars", 0) or 0)
        entry_ts = int(getattr(pos, "_donch_entry_ts", 0) or 0)
        if entry_ts > 0 and "time" in df and len(df) > 0:
            try:
                ts = df["time"].to_numpy().astype("datetime64[ns]").astype("int64") // 10**6
                # closed bars strictly AFTER the entry bar, up to and including the latest bar
                ts_held = int(((ts > entry_ts) & (ts <= cur_bar_ts)).sum())
                bars_held = max(bars_held, ts_held)
            except Exception:
                pass  # fall back to the in-memory counter (raise-only — never under-counts)
        # bt-1 open_bars == bars_held - 1 (see OFF-BY-ONE note); fire at E+121, not E+120.
        if bars_held - 1 >= self._max_hold_bars:
            return None, "time_stop"

        # --- (B) TP take at 4R (informational tp1_price mirrors entry + 4R) ----------------
        sl_dist = pos.entry_price - pos.sl_initial
        if sl_dist > 0:
            tp = pos.entry_price + self._tp_r_multiple * sl_dist
            if high_i >= tp:
                return None, "tp"

        # --- (C) pivot-low vstop raise-only trail (bt-1 _trail_long_sl, engine.py:1804) ----
        # bt-1 calls `_trail_long_sl(lo, i-1, vstop_window, vstop_buffer, sl_current)` every
        # surviving bar: scan back from the most-recent confirmable pivot (k <= (i-1)-window)
        # for a strict pivot-low, propose pivot_low*(1-buffer), ratchet UP only. Passing
        # up_to_idx = i-1 keeps the pivot's right-side confirmation bars <= i-1, so the trailed
        # SL EXCLUDES the current bar's own low and cannot self-stop on the bar that set it
        # (self-trigger-safety). The vstop is stateless over the df window — inherently
        # restart-robust as long as df spans the position's history (no _donch_hh high-water-mark
        # to lose across a restart).
        #
        # IMPORTANT (bt-1 ordering, Audit MED 2026-06-20 — Issue 1 ROOT FIX): bt-1 RESOLVES
        # bar i's stop against the sl_current set on the PREVIOUS iteration (which was
        # `_trail_long_sl(lo, i-2, …)` == trail_{i-2}), and only AFTER no exit fires does it
        # trail to `_trail_long_sl(lo, i-1, …)` == trail_{i-1} and store THAT as sl_current for
        # bar i+1. So bar i must resolve (check_sl_hit) AND rest its exchange SL at trail_{i-2}
        # for the WHOLE bar; trail_{i-1} is bt-1's resolve level for bar i+1 only.
        #
        # client.candles returns CLOSED bars only, so at LOOP_INTERVAL_SEC=60 the SAME closed
        # bar i is re-presented ~480× across an 8h bar. The previous design mutated pos.sl_current
        # in-place to trail_{i-1} on tick 1 (and trader.py persisted it on the exchange + tracked
        # it), so on tick 2+ pos.sl_current ALREADY == trail_{i-1}: the trader's pre/post-ratchet
        # swap (keyed on sl_current != _sl_pre_ratchet) became a no-op and check_sl_hit resolved
        # bar i against trail_{i-1} — one bar EARLY vs bt-1 (and the resting exchange SL led too).
        #
        # ROOT FIX: keep pos.sl_current == the BAR-ENTRY resolve level (trail_{i-2}) for every
        # tick of bar i; compute trail_{i-1} but STAGE it (pos._donch_sl_staged) WITHOUT mutating
        # pos.sl_current. Promote the staged value to pos.sl_current ONLY on the first tick of the
        # NEXT bar (new_bar). This is idempotent across the ~480 re-presentations of bar i: the
        # resolve level and the on-exchange reduce-only SL both stay at trail_{i-2} all bar, and
        # the bot adopts trail_{i-1} exactly when bar i+1 becomes the latest closed bar — bt-1
        # parity at BOTH the internal check AND the exchange trigger (kills the compounding
        # one-bar-early exchange exit too). raise-only: staged can only rise; never lowers.
        new_sl = None

        # (C1) On a NEW bar, FIRST promote last bar's staged trail (trail_{i-1}, computed while
        # bar i-1 was latest) into pos.sl_current — this is the bar-i resolve level == bt-1's
        # sl_current stored on its previous iteration. Raise-only.
        if new_bar:
            staged = pos.__dict__.get("_donch_sl_staged")
            if staged is not None and float(staged) > float(pos.sl_current):
                pos.sl_current = float(staged)
                pos.trail_sl = float(staged)
                new_sl = float(staged)   # resolve/placement level genuinely advanced this bar

        # (C2) Re-compute the trail for the CURRENT latest bar (= trail_{i-1}) and STAGE it for
        # next-bar promotion. Do NOT mutate pos.sl_current here — bar i must resolve+rest at the
        # value promoted in (C1) (trail_{i-2}) for all its ticks. Idempotent on re-presentation:
        # the candidate is a pure function of the closed df window, so recomputing each tick of
        # bar i yields the same staged value (no drift).
        try:
            lows = df["Low"].to_numpy(dtype=float)
            cand = _trail_long_sl(
                lows,
                i - 1,
                self._trail_pivot_window,
                self._trail_vstop_buffer,
                float(pos.sl_current),
            )
            if np.isfinite(cand):
                prev_staged = pos.__dict__.get("_donch_sl_staged")
                # stage the higher of (current candidate, any prior stage) — raise-only, and
                # never below pos.sl_current (a pivot can only tighten the next-bar stop up).
                stage_val = float(cand)
                if prev_staged is not None:
                    stage_val = max(stage_val, float(prev_staged))
                stage_val = max(stage_val, float(pos.sl_current))
                pos._donch_sl_staged = stage_val
        except Exception:
            pass  # leave staged unchanged (raise-only — can only stall, never lower)

        if new_bar:
            pos._donch_last_bar_ts = cur_bar_ts
        return new_sl, None

    def apply_partial_be(self, pos: "Position") -> Optional[float]:
        """Kept for interface parity (trader._detect_partial_fill may call it). Raise-only BE."""
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
        """Verbatim from us29/xnn PM: gap-through fills at Open, wick at SL."""
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
        else:  # short (unused in donchian)
            if o > sl:
                return (o, "gap_through_sl")
            if vstop_wick_check and h >= sl:
                return (sl, "wick_sl")

        return None
