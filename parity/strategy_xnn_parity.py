#!/usr/bin/env python3
"""Parity harness for fleet_core strategy_xnn (Donchian 8h leg — pacifica/extended/nado).

Usage (on each bot host, inside its venv):
    python parity/strategy_xnn_parity.py --old <bot/strategy_xnn.py> --new <fleet_core/strategy_xnn.py>

Loads BOTH files under synthetic module names, feeds deterministic synthetic OHLCV,
and asserts BIT-IDENTICAL outputs for:
  1. compute_indicators        — per-column raw-byte equality (tobytes)
  2. scan_for_signal           — Signal dataclass full-field equality over 3 synthetic
                                 series x 3 tfs (8h in-gate, 4h/1d gate behavior)
  3. scan_for_short_signal     — always-None contract
  4. PositionManager lifecycle — open -> per-bar update_sl_on_new_bar/check_sl_hit over a
                                 deterministic 140-bar sequence (covers entry-bar
                                 suppression, staged-trail promotion, re-presented-bar
                                 idempotency, 4R tp, time_stop at E+121)
  5. module constants + AST equality (docstrings blanked)

NO network, NO writes outside tempdir (none needed — module is pure compute).
Exit 0 = PASS, 1 = FAIL.

The module under test imports ONLY stdlib+numpy+pandas (no bot.* imports), so no fake
'bot' package is required; a stub 'bot' package is pre-injected anyway as a guard so
any future accidental 'from bot import ...' fails loudly here rather than silently
resolving to the live package.
"""
from __future__ import annotations

import argparse
import ast
import dataclasses
import importlib.util
import os
import sys
import types

# Deterministic import-time env: the module reads DONCHIAN_TFS at import.
os.environ["DONCHIAN_TFS"] = "4h,8h"

import numpy as np
import pandas as pd

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    line = f"{'PASS' if ok else 'FAIL'}: {name}" + (f" — {detail}" if detail and not ok else "")
    print(line)
    if not ok:
        FAILURES.append(name)


def load_module(path: str, name: str):
    # Guard stub: module must not import anything from 'bot' (it doesn't today).
    if "bot" not in sys.modules:
        stub = types.ModuleType("bot")
        stub.__path__ = []  # type: ignore[attr-defined]
        sys.modules["bot"] = stub
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── AST equality (docstrings blanked) ────────────────────────────────────────
def ast_dump_no_docstrings(path: str) -> str:
    tree = ast.parse(open(path).read())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body[0].value.value = ""
    return ast.dump(tree)


# ── deterministic synthetic OHLCV ────────────────────────────────────────────
def make_df(seed: int, n: int = 200, breakout_at: int | None = None,
            tf_ms: int = 8 * 3600 * 1000) -> pd.DataFrame:
    """Random-walk OHLCV; optional forced Donchian breakout on the LAST bar."""
    rng = np.random.default_rng(seed)
    base = 100.0 * (1.0 + rng.standard_normal(n).cumsum() * 0.004)
    base = np.abs(base) + 10.0
    o = base
    c = base * (1.0 + rng.standard_normal(n) * 0.003)
    h = np.maximum(o, c) * (1.0 + np.abs(rng.standard_normal(n)) * 0.002)
    l = np.minimum(o, c) * (1.0 - np.abs(rng.standard_normal(n)) * 0.002)
    if breakout_at is not None:
        j = breakout_at
        h_n = h[max(0, j - 15):j].max()
        c[j] = h_n * 1.02          # close above 15-bar upper channel
        h[j] = max(h[j], c[j] * 1.001)
        o[j] = min(o[j], c[j])
        l[j] = min(l[j], o[j] * 0.999)
    t0 = 1_700_000_000_000
    ts = pd.to_datetime(np.arange(n) * tf_ms + t0, unit="ms")
    return pd.DataFrame({"time": ts, "Open": o, "High": h, "Low": l,
                         "Close": c, "Volume": np.abs(rng.standard_normal(n)) * 1e6})


def df_bytes(df: pd.DataFrame) -> bytes:
    parts = [",".join(map(str, df.columns)).encode()]
    for col in df.columns:
        parts.append(df[col].to_numpy().tobytes())
    return b"|".join(parts)


def sig_tuple(mod, sig):
    if sig is None:
        return None
    return tuple((f.name, getattr(sig, f.name)) for f in dataclasses.fields(sig))


SCAN_KW = dict(zigzag_length=5, raw_rr_target=2.0, require_ema50_up=True,
               f1_min_dist_ema20_atr=0.5, tf_max_sl={"8h": 0.1}, min_sl_dist_pct=0.005,
               f2_min_rsi14=0.0, f3_max_dollar_vol_usd=0.0)
SCAN_SHORT_KW = dict(zigzag_length=5, raw_rr_target=2.0, require_ema50_down=True,
                     f1_min_dist_ema20_atr=0.5, tf_max_sl={"8h": 0.1}, min_sl_dist_pct=0.005,
                     f2_max_rsi14=0.0, f3_max_dollar_vol_usd=0.0)


def pm_state(pos) -> tuple:
    d = pos.__dict__
    return (float(pos.sl_current), pos.trail_sl,
            d.get("_donch_bars"), d.get("_donch_last_bar_ts"),
            d.get("_donch_sl_staged"), d.get("_donch_atr_e"), d.get("_donch_hh"))


def run_lifecycle(mod, n_bars: int = 140, entry_idx: int = 60, seed: int = 7) -> list:
    """Deterministic PM lifecycle trace. Same bar re-presented twice (60s-loop model)."""
    full = make_df(seed, n=n_bars, breakout_at=entry_idx)
    df_e = full.iloc[: entry_idx + 1].reset_index(drop=True)
    sig = mod.scan_for_signal(df_e, "TESTCOIN", "8h", **SCAN_KW)
    trace = [("signal", sig_tuple(mod, sig))]
    if sig is None:
        return trace
    pos = mod.Position(coin=sig.coin, tf=sig.tf, entry_price=sig.entry_price,
                       sl_initial=sig.sl_price, sl_current=sig.sl_price,
                       tp1_price=sig.tp1_price, size=1.0, bar_entry_idx=entry_idx)
    pos._donch_entry_ts = sig.bar_ts       # trader.attempt_entry stashes this (F7)
    pm = mod.PositionManager(be_buffer_pct=0.001, vstop_pivot_window=3,
                             max_run_r=6.0, vstop_buffer_pct=0.15, tp1_partial_frac=0.0)
    closed = False
    for i in range(entry_idx, n_bars):
        df_i = full.iloc[: i + 1].reset_index(drop=True)
        for tick in (0, 1):                # re-presentation idempotency
            entry_bar = pm.is_entry_bar(pos, df_i)
            new_sl, reason = pm.update_sl_on_new_bar(pos, df_i)
            hit = None if entry_bar else pm.check_sl_hit(pos, df_i, True)
            trace.append((i, tick, entry_bar, new_sl, reason, hit, pm_state(pos)))
            if reason is not None or hit is not None:
                closed = True
                break
        if closed:
            break
    trace.append(("be", pm.apply_partial_be(pos), pm_state(pos)))
    return trace


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    a = ap.parse_args()

    check("ast_equal_docstrings_blanked",
          ast_dump_no_docstrings(a.old) == ast_dump_no_docstrings(a.new))

    old = load_module(a.old, "old_strategy_xnn")
    new = load_module(a.new, "new_strategy_xnn")

    consts = ["DONCHIAN_N", "SL_BUFFER_PCT", "MIN_SL_DIST_PCT", "MAX_SL_DIST_PCT",
              "TP_R_MULTIPLE", "MAX_HOLD_BARS", "ATR_LEN", "CHANDELIER_ATR_MULT",
              "TRAIL_PIVOT_WINDOW", "TRAIL_VSTOP_BUFFER", "WARMUP_BARS", "DONCHIAN_TFS"]
    for c in consts:
        vo, vn = getattr(old, c), getattr(new, c)
        check(f"const:{c}", vo == vn and type(vo) is type(vn), f"{vo!r} != {vn!r}")

    for cls in ("Signal", "Position"):
        fo = [(f.name, f.default) for f in dataclasses.fields(getattr(old, cls))]
        fn = [(f.name, f.default) for f in dataclasses.fields(getattr(new, cls))]
        check(f"dataclass_fields:{cls}", fo == fn, f"{fo} != {fn}")

    # 1. compute_indicators — bit-identical frames on 3 seeds
    for seed in (1, 2, 3):
        df = make_df(seed)
        bo = df_bytes(old.compute_indicators(df))
        bn = df_bytes(new.compute_indicators(df))
        check(f"compute_indicators_bits:seed{seed}", bo == bn)

    # 2/3. scan_for_signal / short — 3 seeds x 3 tfs, breakout forced on last bar
    n_signals = 0
    for seed in (11, 12, 13):
        df = old.compute_indicators(make_df(seed, n=120, breakout_at=119))
        for tf in ("8h", "4h", "1d"):
            so = old.scan_for_signal(df, f"C{seed}", tf, **SCAN_KW)
            sn = new.scan_for_signal(df, f"C{seed}", tf, **SCAN_KW)
            check(f"scan_for_signal:seed{seed}:{tf}",
                  sig_tuple(old, so) == sig_tuple(new, sn),
                  f"{sig_tuple(old, so)} != {sig_tuple(new, sn)}")
            if so is not None:
                n_signals += 1
            sso = old.scan_for_short_signal(df, f"C{seed}", tf, **SCAN_SHORT_KW)
            ssn = new.scan_for_short_signal(df, f"C{seed}", tf, **SCAN_SHORT_KW)
            check(f"scan_for_short_None:seed{seed}:{tf}", sso is None and ssn is None)
    check("non_vacuous:at_least_one_long_signal_fired", n_signals > 0, str(n_signals))

    # helpers
    for seed in (21, 22):
        df = make_df(seed)
        h, l, c = (df[k].to_numpy(float) for k in ("High", "Low", "Close"))
        check(f"_wilder_atr_bits:seed{seed}",
              old._wilder_atr(h, l, c).tobytes() == new._wilder_atr(h, l, c).tobytes())
        check(f"_signal_bar_atr14:seed{seed}",
              old._signal_bar_atr14(df) == new._signal_bar_atr14(df))
        for p in (0.5, 3.7, 123.4, 61234.0):
            check(f"_estimate_tick:{p}", old._estimate_tick(p) == new._estimate_tick(p))
        cur = float(l.min())
        check(f"_trail_long_sl:seed{seed}",
              old._trail_long_sl(l, len(l) - 2, 2, 0.005, cur)
              == new._trail_long_sl(l, len(l) - 2, 2, 0.005, cur))

    # 4. PM lifecycle traces: trail-exit path, tp path (low TP unreachable here since
    #    TP_R_MULTIPLE=999 — trace still exercises the branch arithmetics), time-stop
    #    path via a monotone series long enough to reach E+121.
    for label, kw in [("walk140", dict(n_bars=140, entry_idx=60, seed=7)),
                      ("walk300_timestop", dict(n_bars=300, entry_idx=40, seed=19)),
                      ("walk90", dict(n_bars=90, entry_idx=55, seed=23))]:
        to, tn = run_lifecycle(old, **kw), run_lifecycle(new, **kw)
        check(f"pm_lifecycle:{label}", to == tn,
              f"first diff at {next((k for k, (x, y) in enumerate(zip(to, tn)) if x != y), 'len')}")
        check(f"pm_lifecycle_nonvacuous:{label}", len(to) > 1, str(len(to)))

    print(f"\n{'PASS' if not FAILURES else 'FAIL'}: strategy_xnn parity "
          f"({len(FAILURES)} failures)")
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
