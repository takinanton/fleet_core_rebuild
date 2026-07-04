#!/usr/bin/env python3
"""Parity harness for warmup_backfill.py (fleet_core P1).

Usage (run on each bot host inside its venv):
    python warmup_backfill_parity.py --old <bot/warmup_backfill.py> --new <fleet_core/warmup_backfill.py>

Loads BOTH files under synthetic module names and asserts bit-identical behavior on
deterministic synthetic inputs. warmup_backfill imports ONLY stdlib + pandas (no bot.*
imports), so no fake 'bot'/'bot.config' modules are needed in sys.modules — verified
by inspecting the module source (import json/logging/os/threading/urllib.request/pandas).

NO network (all HTTP adapters are monkeypatched to deterministic synthetic fetchers),
NO live exchange, NO writes outside a tempdir (WARMUP_LOCAL_DIR is pointed at a fresh
tempdir BEFORE import so the v6 module-level default can never touch real paths).

Checks:
  1. import side-effect free: importing neither file creates any file/dir
  2. _candidates parity for non-xyz coins (incl _OVERRIDE / _TRADFI / kSHIB-style)
  3. _candidates for xyz_ coins = [('local', coin)] prepended, rest unchanged (new only)
  4. backfill_warmup end-to-end old-vs-new bit-identical DataFrames on:
       gate off / short-circuit / accept-splice / same-asset REJECT / glue-guard cut /
       cadence REJECT / no-source / tail-empty
  5. source-router isolation: new._ADAPTERS['local'] replaced with a raiser; every
     non-xyz scenario re-run — must never raise and must stay identical to old
  6. new-only sanity: xyz_ coin with no local file -> adapter returns [] -> own df back

Exit 0 with all PASS lines, exit 1 on first FAIL.
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import tempfile

# ---------------------------------------------------------------------------
TMP = tempfile.mkdtemp(prefix="wb_parity_")
os.environ["WARMUP_LOCAL_DIR"] = os.path.join(TMP, "xyz_backfill")  # fence v6 default
os.environ["WARMUP_BACKFILL"] = "1"

import pandas as pd  # noqa: E402

FAILS = []


def check(name, ok, detail=""):
    line = f"{'PASS' if ok else 'FAIL'} {name}" + (f" — {detail}" if detail and not ok else "")
    print(line)
    if not ok:
        FAILS.append(name)


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def snapshot_tree(root):
    out = []
    for d, dirs, files in os.walk(root):
        for f in files:
            out.append(os.path.join(d, f))
    return sorted(out)


# --- deterministic synthetic candle universe --------------------------------
DAY = 86_400_000
T0 = 1_500_000_000_000  # deep-history anchor (ms)


def mk_rows(n, start_ms, step_ms, px, drift=0.0):
    """Oldest-first [[ts,o,h,l,c,v],...] with a deterministic price path."""
    rows = []
    p = px
    for i in range(n):
        t = start_ms + i * step_ms
        o = round(p, 8)
        c = round(p * (1.0 + drift), 8)
        rows.append([t, o, round(max(o, c) * 1.001, 8), round(min(o, c) * 0.999, 8), c, 100.0 + i])
        p = c
    return rows


def mk_own_df(n, start_ms, step_ms, px):
    rows = mk_rows(n, start_ms, step_ms, px)
    df = pd.DataFrame(rows, columns=["ms", "Open", "High", "Low", "Close", "Volume"])
    df["time"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
    return df[["time", "Open", "High", "Low", "Close", "Volume"]].reset_index(drop=True)


def frames_bitident(a, b):
    if a is None or b is None:
        return a is None and b is None
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=True)
    except AssertionError:
        return False
    return a.to_csv(index=True) == b.to_csv(index=True)


def make_synth_fetch(table):
    """table: {(source, symbol): rows}. Returns per-source fetcher factory."""
    def factory(source):
        def _fetch(symbol, tf, end_ms):
            return [list(r) for r in table.get((source, symbol), [])]
        return _fetch
    return factory


def raiser(symbol, tf, end_ms):
    raise AssertionError(f"LOCAL SOURCE TOUCHED for symbol={symbol!r} (must never fire off-xyz)")


def patch(mod, table, local_raises):
    fac = make_synth_fetch(table)
    for src in ("binance", "bybit", "gate", "yahoo"):
        if src in mod._ADAPTERS:
            mod._ADAPTERS[src] = fac(src)
    if "local" in mod._ADAPTERS and local_raises:
        mod._ADAPTERS["local"] = raiser
    mod._NO_SRC.clear()
    mod._ROWS_CACHE.clear()
    if hasattr(mod.backfill_warmup, "_logged"):
        mod.backfill_warmup._logged.clear()


def run_case(mod, table, coin, tf, min_bars, own_df, local_raises=True):
    patch(mod, table, local_raises)
    return mod.backfill_warmup(own_df.copy(deep=True), coin, tf, min_bars)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    args = ap.parse_args()

    pre = snapshot_tree(TMP)
    old = load(os.path.abspath(args.old), "wb_parity_old")
    new = load(os.path.abspath(args.new), "wb_parity_new")
    post = snapshot_tree(TMP)
    check("import_side_effect_free_tempdir", pre == post)
    check("import_no_local_dir_created", not os.path.exists(os.environ["WARMUP_LOCAL_DIR"]))
    check("new_has_local_bf_dir_str",
          isinstance(getattr(new, "_LOCAL_BF_DIR", None), str)
          and new._LOCAL_BF_DIR == os.environ["WARMUP_LOCAL_DIR"])

    # -- 2/3: _candidates parity ---------------------------------------------
    normal_coins = ["SOL", "BTC", "ETH-PERP", "TAO-USD", "kPEPE", "XAU", "GOLD",
                    "SPX", "EUR", "kSHIB", "sol", "LIT", "DOGE/USDT", "XYZABC",
                    "XYZ", "Xyz_AMD"]  # last three: near-miss prefixes, must NOT gate
    ok = True
    for c in normal_coins:
        a, b = old._candidates(c), new._candidates(c)
        if a != b:
            ok = False
            print(f"  candidates mismatch {c}: old={a} new={b}")
    check("candidates_identical_non_xyz", ok)
    for c in ["xyz_AMD", "xyz_GOLD", "xyz_VIX"]:
        a, b = old._candidates(c), new._candidates(c)
        # old may be v5 (no local) or already v6 (local prepended, e.g. HL host)
        rest = a[1:] if (a and a[0] == ("local", c)) else a
        check(f"candidates_xyz_prepends_local_only[{c}]",
              b[0] == ("local", c) and b[1:] == rest, f"old={a} new={b}")

    # -- 4/5: end-to-end scenarios (local adapter = raiser in new) -----------
    tf, tf_ms, min_bars = "1d", DAY, 610
    own_start = T0 + 700 * DAY
    own = mk_own_df(120, own_start, DAY, 100.0)          # short own history, px ~100

    deep_ok = mk_rows(900, own_start - 800 * DAY, DAY, 100.0)          # same asset, contiguous
    deep_wrongpx = mk_rows(900, own_start - 800 * DAY, DAY, 46.0)      # ratio ~0.46 -> REJECT
    glue = (mk_rows(400, own_start - 900 * DAY, DAY, 0.74)             # old asset segment
            + mk_rows(400, own_start - 450 * DAY, DAY, 100.0))         # 50-day gap -> cut
    # cadence-mismatch tail: same price level but every 3rd bar missing (~gapfrac .33)
    gappy = [r for i, r in enumerate(mk_rows(900, own_start - 800 * DAY, DAY, 100.0)) if i % 3]

    scenarios = [
        ("accept_splice",   {("binance", "SOLUSDT"): deep_ok},       "SOL"),
        ("same_asset_reject", {("binance", "SOLUSDT"): deep_wrongpx}, "SOL"),
        ("glue_guard_cut",  {("binance", "LITUSDT"): glue},          "LIT"),
        ("cadence_reject",  {("binance", "SOLUSDT"): gappy},         "SOL"),
        ("no_source",       {},                                       "NEWCOIN"),
        ("override_first_kpepe", {("binance", "1000PEPEUSDT"): deep_ok,
                                  ("binance", "KPEPEUSDT"): deep_wrongpx}, "kPEPE"),
        ("tradfi_yahoo",    {("yahoo", "SI=F"): gappy},               "XAG"),  # yahoo exempt from glue, cadence rejects
        ("tail_empty",      {("binance", "SOLUSDT"): mk_rows(50, own_start + 10 * DAY, DAY, 100.0)}, "SOL"),
    ]
    for name, table, coin in scenarios:
        oa = run_case(old, table, coin, tf, min_bars, own, local_raises=False)
        try:
            nb = run_case(new, table, coin, tf, min_bars, own, local_raises=True)
        except AssertionError as e:
            check(f"e2e_{name}", False, str(e))
            continue
        check(f"e2e_{name}", frames_bitident(oa, nb),
              f"shapes old={None if oa is None else oa.shape} new={None if nb is None else nb.shape}")

    # gate off / short-circuits (identity object semantics preserved)
    os.environ["WARMUP_BACKFILL"] = "0"
    o1, n1 = run_case(old, {}, "SOL", tf, min_bars, own, False), run_case(new, {}, "SOL", tf, min_bars, own, True)
    check("e2e_gate_off_passthrough", frames_bitident(o1, n1) and frames_bitident(o1, own))
    os.environ["WARMUP_BACKFILL"] = "1"
    long_own = mk_own_df(700, own_start, DAY, 100.0)
    check("e2e_already_long_passthrough",
          frames_bitident(run_case(old, {}, "SOL", tf, min_bars, long_own, False),
                          run_case(new, {}, "SOL", tf, min_bars, long_own, True)))
    check("e2e_none_df", run_case(old, {}, "SOL", tf, min_bars, pd.DataFrame(columns=["time", "Close"]), False).empty
          and run_case(new, {}, "SOL", tf, min_bars, pd.DataFrame(columns=["time", "Close"]), True).empty)

    # -- 6: new-only xyz sanity (no file -> [] -> own df unchanged; tempdir only) --
    patch(new, {}, local_raises=False)  # restore real _fetch_local? no — reload for real one
    new2 = load(os.path.abspath(args.new), "wb_parity_new2")
    fac = make_synth_fetch({})
    for src in ("binance", "bybit", "gate", "yahoo"):
        new2._ADAPTERS[src] = fac(src)
    out = new2.backfill_warmup(own.copy(deep=True), "xyz_AMD", tf, min_bars)
    check("new_xyz_no_file_failsafe", frames_bitident(out, own))
    check("no_writes_after_run", snapshot_tree(TMP) == pre)

    print(f"{'PASS' if not FAILS else 'FAIL'} TOTAL: {len(FAILS)} failures")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
