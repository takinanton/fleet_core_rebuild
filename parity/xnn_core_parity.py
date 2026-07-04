#!/usr/bin/env python3
"""Parity check for xnn_core.py: old bot/xnn_core.py vs new fleet_core/xnn_core.py.

Loads both files under synthetic module names (no 'bot' package needed —
xnn_core imports only sys/numpy/pandas) and asserts bit-identical outputs of
every public entry point over deterministic synthetic OHLCV series.

Usage: python xnn_core_parity.py --old <path> --new <path>
Exit 0 = PASS, 1 = FAIL. No network, no writes.
"""
import argparse
import hashlib
import importlib.util
import sys

import numpy as np
import pandas as pd


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def make_series():
    """5 deterministic synthetic OHLCV DataFrames."""
    out = {}
    rng = np.random.RandomState(42)
    n = 400

    def mk(close):
        close = np.asarray(close, dtype=float)
        high = close * (1.0 + np.abs(np.random.RandomState(7).randn(len(close))) * 0.004 + 0.001)
        low = close * (1.0 - np.abs(np.random.RandomState(9).randn(len(close))) * 0.004 - 0.001)
        openp = np.roll(close, 1); openp[0] = close[0]
        return pd.DataFrame({"Open": openp, "High": high, "Low": low, "Close": close})

    # 1. trending up with pullbacks
    t = np.arange(n)
    out["trend_up"] = mk(100 * np.exp(0.002 * t) * (1 + 0.01 * np.sin(t / 9.0)) + rng.randn(n) * 0.2)
    # 2. trending down
    out["trend_down"] = mk(100 * np.exp(-0.002 * t) * (1 + 0.01 * np.sin(t / 7.0)) + rng.randn(n) * 0.1)
    # 3. flat series (constant price)
    out["flat"] = mk(np.full(n, 50.0))
    # 4. random walk
    out["random_walk"] = mk(100 + np.cumsum(rng.randn(n) * 0.5))
    # 5. NaN-leading warmup then trend
    c = 100 * np.exp(0.003 * t)
    c[:30] = np.nan
    out["nan_warmup"] = mk(c)
    # 6. short degenerate window (< warmup)
    out["short"] = mk(100 + np.cumsum(rng.randn(15)))
    # 7. staircase up with deep pullback wicks — fires GATED long signals
    r3 = np.random.RandomState(3)
    cs = [100.0]
    for k in range(1, n):
        cs.append(cs[-1] + (0.9 if k % 26 < 20 else -1.05) + r3.randn() * 0.05)
    cs = np.asarray(cs)
    o = np.roll(cs, 1); o[0] = cs[0]
    out["stair_up"] = pd.DataFrame({"Open": o, "High": cs + 0.5, "Low": cs - 2.5, "Close": cs})
    # 8. mirrored staircase down — fires GATED short signals
    cd = 200.0 - (cs - 100.0)
    od = np.roll(cd, 1); od[0] = cd[0]
    out["stair_down"] = pd.DataFrame({"Open": od, "High": cd + 2.5, "Low": cd - 0.5, "Close": cd})
    # 9. staircase with sharp dips — fires UNGATED long signals
    r4 = np.random.RandomState(3)
    cu = [100.0]
    for k in range(1, n):
        cu.append(cu[-1] + (0.9 if k % 30 < 24 else -1.6) + r4.randn() * 0.05)
    cu = np.asarray(cu)
    ou = np.roll(cu, 1); ou[0] = cu[0]
    out["stair_sharp"] = pd.DataFrame({"Open": ou, "High": cu + 0.8, "Low": cu - 0.8, "Close": cu})
    return out


CFG = {  # canonical-gates-satisfying config (values in deploy style)
    "emf": 10, "ems": 30, "ema_fit_window": 60,
    "min_signal_idx": 2, "corr_touch_lk": 6, "slow_slope_lk": 5,
    "sig_min_rally_pct": 0.02, "sig_min_gap_bars": 0, "sig_min_height_pct": 0.0,
    "clean_lk": 3, "min_sep_pct": 0.0, "min_price": 0.0,
    "sl_lookback": 5, "sl_atr_buf": 0.5,
    "min_sl_dist_pct": 0.0, "max_sl_dist_pct": 0.5,
    "allow_long": True, "allow_short": True,
}
CFG_ADAPTIVE = {**CFG, "ema_adaptive": True, "ema_adaptive_mode": "union",
                "ema_candidates": [[5, 15], [10, 30], [20, 50]]}
CFG_BEST = {**CFG_ADAPTIVE, "ema_adaptive_mode": "best"}
CFG_UNGATED = {**CFG, "allow_ungated": True, "min_signal_idx": 1,
               "corr_touch_lk": 0, "slow_slope_lk": 0, "sig_min_rally_pct": 0.0}


def canon(x):
    return repr(x)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    a = ap.parse_args()

    h_old = hashlib.md5(open(a.old, "rb").read()).hexdigest()
    h_new = hashlib.md5(open(a.new, "rb").read()).hexdigest()
    print(f"md5 old={h_old} new={h_new} {'IDENTICAL' if h_old == h_new else 'differ'}")

    old = load(a.old, "xnn_core_old")
    new = load(a.new, "xnn_core_new")

    fails = 0
    signals = 0

    def check(label, fo, fn):
        nonlocal fails, signals
        try:
            ro = canon(fo())
        except Exception as e:
            ro = f"RAISE {type(e).__name__}: {e}"
        try:
            rn = canon(fn())
        except Exception as e:
            rn = f"RAISE {type(e).__name__}: {e}"
        if ro == rn:
            if ro.startswith("{'side'"):
                signals += 1
            print(f"PASS {label}")
        else:
            print(f"FAIL {label}\n  old={ro[:300]}\n  new={rn[:300]}")
            fails += 1

    check("TF_MAX_SL", lambda: old.TF_MAX_SL, lambda: new.TF_MAX_SL)

    series = make_series()
    for name, df in series.items():
        check(f"atr14/{name}",
              lambda o=old, d=df: o.compute_xnn_atr14(d.High.values, d.Low.values, d.Close.values).tolist(),
              lambda n=new, d=df: n.compute_xnn_atr14(d.High.values, d.Low.values, d.Close.values).tolist())
        for cfgname, cfg in [("fixed", CFG), ("union", CFG_ADAPTIVE),
                             ("best", CFG_BEST), ("ungated", CFG_UNGATED)]:
            check(f"scan_signal/{name}/{cfgname}",
                  lambda o=old, d=df, c=cfg: o.scan_signal(d, dict(c)),
                  lambda n=new, d=df, c=cfg: n.scan_signal(d, dict(c)))
        # scan every bar suffix (decision at multiple i) on one cfg for depth;
        # dense step on staircase series so gated/ungated signals actually fire
        step = 1 if name.startswith("stair") else 37
        sweep_cfg = CFG_UNGATED if name == "stair_sharp" else CFG
        for i in range(60, len(df), step):
            sub = df.iloc[:i]
            check(f"scan_signal/{name}/sweep@i{i}",
                  lambda o=old, d=sub, c=sweep_cfg: o.scan_signal(d, dict(c)),
                  lambda n=new, d=sub, c=sweep_cfg: n.scan_signal(d, dict(c)))
        for side in ("long", "short"):
            for cur in (0.0, 42.0):
                check(f"trail_stop/{name}/{side}/cur={cur}",
                      lambda o=old, d=df, s=side, c=cur: o.trail_stop(d, len(d) - 1, 3, 0.15, s, c),
                      lambda n=new, d=df, s=side, c=cur: n.trail_stop(d, len(d) - 1, 3, 0.15, s, c))
        # internals: series counts + window path
        check(f"series_count/{name}",
              lambda o=old, d=df: (lambda w, m: (lambda p: o._series_count_at(w, p[0], p[1], len(w.close) - 1, CFG))(o._emas_pair(w, 10, 30, m)))(o.make_window(d), {}),
              lambda n=new, d=df: (lambda w, m: (lambda p: n._series_count_at(w, p[0], p[1], len(w.close) - 1, CFG))(n._emas_pair(w, 10, 30, m)))(n.make_window(d), {}))
        check(f"series_count_short/{name}",
              lambda o=old, d=df: (lambda w, m: (lambda p: o._series_count_short_at(w, p[0], p[1], len(w.close) - 1, CFG))(o._emas_pair(w, 10, 30, m)))(o.make_window(d), {}),
              lambda n=new, d=df: (lambda w, m: (lambda p: n._series_count_short_at(w, p[0], p[1], len(w.close) - 1, CFG))(n._emas_pair(w, 10, 30, m)))(n.make_window(d), {}))

    # assert_canonical_gates: pass / fail-loud cases
    check("gates/ok", lambda: old.assert_canonical_gates(dict(CFG)),
          lambda: new.assert_canonical_gates(dict(CFG)))
    check("gates/missing", lambda: old.assert_canonical_gates({"min_signal_idx": 1}),
          lambda: new.assert_canonical_gates({"min_signal_idx": 1}))
    check("gates/ungated", lambda: old.assert_canonical_gates({"allow_ungated": True}),
          lambda: new.assert_canonical_gates({"allow_ungated": True}))

    # non-vacuity gate: require real (non-None) signal comparisons on both sides
    if signals < 10:
        print(f"FAIL non-vacuity: only {signals} non-None signal comparisons (<10) — "
              "synthetic series did not exercise the entry path")
        fails += 1
    else:
        print(f"PASS non-vacuity: {signals} non-None matched signals")

    print(f"\n{'PASS' if fails == 0 else 'FAIL'}: {fails} mismatches")
    sys.exit(0 if fails == 0 else 1)


if __name__ == "__main__":
    main()
