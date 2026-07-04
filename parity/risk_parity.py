#!/usr/bin/env python3
"""Parity harness for bot/risk.py -> fleet_core/risk.py (module: risk).

Run ON EACH BOT HOST inside its venv (stdlib-only, any Python >= 3.9):

    python risk_parity.py --old /path/to/bot/risk.py --new /path/to/fleet_core/risk.py

Loads BOTH files under synthetic module names with a fake 'bot'/'bot.config'
pre-injected into sys.modules (Settings attrs mimic every venue's bot/config.py
defaults: risk_per_trade, leverage, mm_cap_pct, max_concurrent — the only
Settings fields risk.py touches). Feeds deterministic synthetic inputs and
asserts bit-identical outputs.

NO network. NO file writes. NO live exchange.

Documented allowed delta (HL only): old check_concurrent_cap reject string
lacks the ' (long+short shared)' suffix present in the canonical (pac/ext/nado
majority) text. The comparator asserts strings equal after stripping that exact
suffix from both sides, and reports the raw delta as ALLOWED-STRING-DELTA.

nado note: old nado compute_size has no leverage_eff parameter. Cases that pass
leverage_eff are auto-skipped for such an old file (reported SKIP-old-no-lev);
instead we assert new(leverage_eff omitted) == new(leverage_eff=None) == old(),
proving the added parameter is inert when not supplied.

Exit 0 = PASS (all cases identical modulo the one documented suffix delta).
Exit 1 = FAIL.
"""
from __future__ import annotations

import argparse
import inspect
import sys
import types

SUFFIX = " (long+short shared)"


# ---------------------------------------------------------------- fake bot pkg
def install_fake_bot() -> None:
    """Pre-inject fake 'bot' + 'bot.config' so 'from bot.config import Settings' works.

    Settings mirrors the fields risk.py reads (bot/config.py defaults across venues:
    RISK_PER_TRADE=0.005 (nado .env default 0.01), LEVERAGE=5, MM_CAP_PCT=0.50,
    MAX_CONCURRENT=5). risk.py only ever reads settings.risk_per_trade and
    settings.leverage; check_* take plain scalars.
    """
    if "bot" in sys.modules and getattr(sys.modules["bot"], "__fake__", False):
        return

    class Settings:  # minimal stand-in; attrs set per-instance
        def __init__(self, risk_per_trade=0.005, leverage=5,
                     mm_cap_pct=0.50, max_concurrent=5):
            self.risk_per_trade = risk_per_trade
            self.leverage = leverage
            self.mm_cap_pct = mm_cap_pct
            self.max_concurrent = max_concurrent

    bot = types.ModuleType("bot")
    bot.__fake__ = True
    bot.__path__ = []  # mark as package
    cfg = types.ModuleType("bot.config")
    cfg.Settings = Settings
    bot.config = cfg
    sys.modules["bot"] = bot
    sys.modules["bot.config"] = cfg


def load_module(path: str, name: str) -> types.ModuleType:
    with open(path, "r", encoding="utf-8") as fh:
        src = fh.read()
    mod = types.ModuleType(name)
    mod.__file__ = path
    sys.modules[name] = mod
    exec(compile(src, path, "exec"), mod.__dict__)
    return mod


# ------------------------------------------------------------------ comparators
def sr_tuple(res):
    """SizeResult -> plain tuple (or None) for bit-identical comparison."""
    if res is None:
        return None
    return (res.size, res.risk_dollars, res.notional)


def norm_reason(s: str) -> str:
    return s[:-len(SUFFIX)] if s.endswith(SUFFIX) else s


def cmp_reason_pair(old_pair, new_pair):
    """(ok, reason) pairs: ok must be identical; reason identical OR differ only
    by the documented ' (long+short shared)' suffix. Returns (equal, note)."""
    if old_pair[0] != new_pair[0]:
        return False, "ok-flag mismatch"
    if old_pair[1] == new_pair[1]:
        return True, ""
    if norm_reason(old_pair[1]) == norm_reason(new_pair[1]):
        return True, "ALLOWED-STRING-DELTA (long+short shared suffix)"
    return False, f"reason mismatch: old={old_pair[1]!r} new={new_pair[1]!r}"


# ------------------------------------------------------------------- test table
def build_cases(Settings):
    s_def = Settings()                                   # 0.005 risk, 5x
    s_nado = Settings(risk_per_trade=0.01, leverage=5)   # nado .env default
    s_lev1 = Settings(leverage=1)

    # compute_size cases: (label, kwargs)  — leverage_eff key may be absent
    cs = [
        ("cs01_long_basic",        dict(entry_price=100.0, sl_price=95.0, account_value=10_000.0, settings=s_def)),
        ("cs02_short_basic",       dict(entry_price=100.0, sl_price=105.0, account_value=10_000.0, settings=s_def)),
        ("cs03_long_lev_none",     dict(entry_price=100.0, sl_price=99.9, account_value=10_000.0, settings=s_def, leverage_eff=None)),
        ("cs04_long_lev3",         dict(entry_price=100.0, sl_price=99.9, account_value=10_000.0, settings=s_def, leverage_eff=3)),
        ("cs05_long_lev0_fallback", dict(entry_price=100.0, sl_price=99.9, account_value=10_000.0, settings=s_def, leverage_eff=0)),
        ("cs06_long_levneg_fallback", dict(entry_price=100.0, sl_price=99.9, account_value=10_000.0, settings=s_def, leverage_eff=-2)),
        ("cs07_lev_binds",         dict(entry_price=50.0, sl_price=49.99, account_value=1_000.0, settings=s_def, leverage_eff=2)),
        ("cs08_tiny_sl_dist",      dict(entry_price=1.0, sl_price=1.0 - 1e-9, account_value=10_000.0, settings=s_def)),
        ("cs09_sl_eq_entry_none",  dict(entry_price=100.0, sl_price=100.0, account_value=10_000.0, settings=s_def)),
        ("cs10_entry_zero_none",   dict(entry_price=0.0, sl_price=1.0, account_value=10_000.0, settings=s_def)),
        ("cs11_liq_cap_binds",     dict(entry_price=100.0, sl_price=95.0, account_value=100_000.0, settings=s_def, liquidity_cap_notional=500.0)),
        ("cs12_liq_cap_zero_ignored", dict(entry_price=100.0, sl_price=95.0, account_value=10_000.0, settings=s_def, liquidity_cap_notional=0.0)),
        ("cs13_liq_cap_loose",     dict(entry_price=100.0, sl_price=95.0, account_value=10_000.0, settings=s_def, liquidity_cap_notional=1e9)),
        ("cs14_szdec0_floor",      dict(entry_price=3.0, sl_price=2.7, account_value=10_000.0, settings=s_def, sz_decimals=0)),
        ("cs15_rounds_to_zero",    dict(entry_price=100_000.0, sl_price=50_000.0, account_value=100.0, settings=s_def, sz_decimals=0)),
        ("cs16_nado_risk_default", dict(entry_price=2.5, sl_price=2.4, account_value=5_000.0, settings=s_nado, sz_decimals=1)),
        ("cs17_lev1_settings",     dict(entry_price=10.0, sl_price=9.0, account_value=1_000.0, settings=s_lev1)),
        ("cs18_short_lev4_szdec2", dict(entry_price=200.0, sl_price=210.0, account_value=25_000.0, settings=s_def, sz_decimals=2, leverage_eff=4)),
        ("cs19_neg_szdec_clamped", dict(entry_price=100.0, sl_price=90.0, account_value=10_000.0, settings=s_def, sz_decimals=-3)),
        ("cs20_irrational_floats", dict(entry_price=0.123456789, sl_price=0.111111111, account_value=7_777.77, settings=s_def, sz_decimals=6, leverage_eff=7)),
    ]

    # check_mm_cap: (label, args) args=(new_notional, eff_lev, existing_margin_usd, account_value, mm_cap_pct)
    mm = [
        ("mm01_exact_cap_ok",   (500.0, 5, 400.0, 1000.0, 0.5)),   # 400+100 = 50.0% == cap -> ok
        ("mm02_just_over",      (500.0, 5, 400.01, 1000.0, 0.5)),
        ("mm03_well_under",     (100.0, 5, 0.0, 1000.0, 0.5)),
        ("mm04_acct_zero",      (100.0, 5, 0.0, 0.0, 0.5)),
        ("mm05_acct_neg",       (100.0, 5, 0.0, -1.0, 0.5)),
        ("mm06_efflev_zero",    (100.0, 0, 0.0, 1000.0, 0.5)),
        ("mm07_efflev_neg",     (100.0, -3, 0.0, 1000.0, 0.5)),
        ("mm08_breach_big",     (10_000.0, 2, 900.0, 1000.0, 0.5)),
    ]

    # check_concurrent_cap: (label, (n_open, max_concurrent))
    cc = [
        ("cc01_under",    (3, 5)),
        ("cc02_boundary", (5, 5)),   # n == max -> blocked
        ("cc03_over",     (7, 5)),
        ("cc04_zero",     (0, 5)),
        ("cc05_max999",   (42, 999)),
    ]
    return cs, mm, cc


# ------------------------------------------------------------------------ main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True, help="path to original bot/risk.py")
    ap.add_argument("--new", required=True, help="path to fleet_core/risk.py")
    args = ap.parse_args()

    install_fake_bot()
    Settings = sys.modules["bot.config"].Settings
    old = load_module(args.old, "parity_old_risk")
    new = load_module(args.new, "parity_new_risk")

    old_has_lev = "leverage_eff" in inspect.signature(old.compute_size).parameters
    cs_cases, mm_cases, cc_cases = build_cases(Settings)

    n_pass = n_fail = n_skip = 0

    def report(label, ok, note=""):
        nonlocal n_pass, n_fail
        if ok:
            n_pass += 1
            print(f"PASS {label}" + (f"  [{note}]" if note else ""))
        else:
            n_fail += 1
            print(f"FAIL {label}: {note}")

    # --- compute_size ---
    for label, kw in cs_cases:
        if "leverage_eff" in kw and not old_has_lev:
            if kw["leverage_eff"] is None:
                # old lacks the param; prove new's None-default is inert:
                kw2 = {k: v for k, v in kw.items() if k != "leverage_eff"}
                r_old = sr_tuple(old.compute_size(**kw2))
                r_new_omit = sr_tuple(new.compute_size(**kw2))
                r_new_none = sr_tuple(new.compute_size(**kw))
                ok = r_old == r_new_omit == r_new_none
                report(label + "_nado_inert", ok,
                       "" if ok else f"old={r_old} new_omit={r_new_omit} new_none={r_new_none}")
            else:
                n_skip += 1
                print(f"SKIP {label}  [SKIP-old-no-lev: old compute_size has no leverage_eff param]")
            continue
        r_old = sr_tuple(old.compute_size(**kw))
        r_new = sr_tuple(new.compute_size(**kw))
        report(label, r_old == r_new, "" if r_old == r_new else f"old={r_old} new={r_new}")

    # --- check_mm_cap ---
    for label, a in mm_cases:
        p_old = old.check_mm_cap(*a)
        p_new = new.check_mm_cap(*a)
        ok = p_old == p_new  # no string delta permitted in mm_cap
        report(label, ok, "" if ok else f"old={p_old} new={p_new}")

    # --- check_concurrent_cap (documented HL suffix delta allowed) ---
    for label, a in cc_cases:
        p_old = old.check_concurrent_cap(*a)
        p_new = new.check_concurrent_cap(*a)
        ok, note = cmp_reason_pair(p_old, p_new)
        report(label, ok, note)

    # --- _floor_to_decimals direct spot-checks ---
    for label, (v, d) in [("fl01", (1.23456789, 4)), ("fl02", (0.0, 4)),
                          ("fl03", (-1.0, 4)), ("fl04", (9.999, 0)),
                          ("fl05", (123.456, -2))]:
        r_old = old._floor_to_decimals(v, d)
        r_new = new._floor_to_decimals(v, d)
        report(label, r_old == r_new and type(r_old) is type(r_new),
               "" if r_old == r_new else f"old={r_old!r} new={r_new!r}")

    print(f"---\ntotal: pass={n_pass} fail={n_fail} skip={n_skip} "
          f"(old_has_leverage_eff={old_has_lev})")
    if n_fail:
        print("RESULT: FAIL")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
