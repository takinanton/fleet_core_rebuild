#!/usr/bin/env python3
"""Parity check for bot/liquidity.py vs fleet_core/liquidity.py.

Run on each bot host inside its venv:
    python liquidity_parity.py --old /path/to/bot/liquidity.py --new /path/to/fleet_core/liquidity.py

No network, no live exchange. Writes only inside a tempdir.
liquidity.py has NO 'from bot...' imports and no config reads, so no fake
bot modules are needed; we still pre-inject a stub 'bot' package defensively.
"""
import argparse
import importlib.util
import json
import sys
import tempfile
import types
from pathlib import Path

FAILS = []


def check(name, ok, detail=""):
    print(("PASS" if ok else "FAIL") + f" {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILS.append(name)


def load(path, modname):
    # defensive stubs (liquidity.py imports nothing from bot, but be safe)
    if "bot" not in sys.modules:
        pkg = types.ModuleType("bot")
        pkg.__path__ = []
        sys.modules["bot"] = pkg
    spec = importlib.util.spec_from_file_location(modname, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


SNAP_WITH_KEY = {
    "generated_at_utc": "2026-07-01T00:05:00+00:00",
    "coins": {
        "BTC": {"avg_1h_vol_usd": 1234567.89, "spread_pct": 0.0002,
                "depth_top20_usd": 5000000.0, "depth_at_0.5pct_usd": 750000.5,
                "pct_traded_60min": 1.0},
        "DOGE": {"avg_1h_vol_usd": 4321.0, "spread_pct": 0.004,
                 "depth_top20_usd": 90000.0, "depth_at_0.5pct_usd": 0.0,
                 "pct_traded_60min": 0.55},
        "BAD": {"avg_1h_vol_usd": "not-a-number"},  # must be skipped by both
        "NULLS": {"avg_1h_vol_usd": None, "spread_pct": None,
                  "depth_top20_usd": None, "pct_traded_60min": None},
    },
}
SNAP_NO_KEY = {
    "generated_at_utc": "2026-07-01T00:05:00+00:00",
    "coins": {
        "BTC": {"avg_1h_vol_usd": 1234567.89, "spread_pct": 0.0002,
                "depth_top20_usd": 5000000.0, "pct_traded_60min": 0.9833},
        "ETH": {"avg_1h_vol_usd": 999999.0, "spread_pct": 0.0001,
                "depth_top20_usd": 3000000.0},
    },
}
SHARED = ("coin", "avg_1h_vol_usd", "spread_pct", "depth_top20_usd", "pct_traded_60min")


def snap_repr(snap):
    if snap is None:
        return None
    return {
        "generated_at_utc": snap.generated_at_utc,
        "coins": {c: tuple(getattr(p, f) for f in SHARED) for c, p in sorted(snap.coins.items())},
    }


def run_case(old, new, raw, label, tmp):
    p = Path(tmp) / f"snap_{label}.json"
    p.write_text(json.dumps(raw))
    so, sn = old.load_snapshot(p), new.load_snapshot(p)
    check(f"load_snapshot[{label}] shared-fields identical", snap_repr(so) == snap_repr(sn),
          f"old={snap_repr(so)} new={snap_repr(sn)}")
    # extra field on new must default 0.0 when key absent
    if sn is not None:
        for c, prof in sn.coins.items():
            v = getattr(prof, "depth_at_half_pct_usd", 0.0)
            exp = float((raw["coins"][c].get("depth_at_0.5pct_usd") or 0.0))
            check(f"new depth_at_half_pct_usd[{label}/{c}]", v == exp, f"{v} != {exp}")
    # .get() semantics on snapshot object
    if so is not None and sn is not None:
        check(f"get() present/absent [{label}]",
              (so.get("BTC") is not None) == (sn.get("BTC") is not None)
              and so.get("ZZZ") is None and sn.get("ZZZ") is None)
    return so, sn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    a = ap.parse_args()
    old = load(a.old, "parity_old_liquidity")
    new = load(a.new, "parity_new_liquidity")

    exported = ["LiquidityProfile", "LiquiditySnapshot", "load_snapshot", "SnapshotHolder"]
    for n in exported:
        check(f"export {n} exists", hasattr(old, n) and hasattr(new, n))

    # dataclass field-order parity for shared prefix + defaults
    import dataclasses
    fo = [f.name for f in dataclasses.fields(old.LiquidityProfile)]
    fn = [f.name for f in dataclasses.fields(new.LiquidityProfile)]
    check("shared fields subset of new, prefix order coin..depth_top20_usd",
          fo[:4] == fn[:4] == ["coin", "avg_1h_vol_usd", "spread_pct", "depth_top20_usd"]
          and set(fo) <= set(fn), f"old={fo} new={fn}")
    check("pct_traded_60min default 1.0 both",
          old.LiquidityProfile("X", 1, 1, 1).pct_traded_60min == 1.0
          == new.LiquidityProfile("X", 1, 1, 1).pct_traded_60min)

    with tempfile.TemporaryDirectory() as tmp:
        run_case(old, new, SNAP_WITH_KEY, "with_key", tmp)
        so, sn = run_case(old, new, SNAP_NO_KEY, "no_key", tmp)

        # missing file
        miss = Path(tmp) / "nope.json"
        check("load_snapshot missing file -> None both",
              old.load_snapshot(miss) is None and new.load_snapshot(miss) is None)
        # corrupt file
        bad = Path(tmp) / "bad.json"
        bad.write_text("{not json")
        check("load_snapshot corrupt -> None both",
              old.load_snapshot(bad) is None and new.load_snapshot(bad) is None)

        # SnapshotHolder: current/set/maybe_reload/fetch_inline
        for tag, mod, snap in (("old", old, so), ("new", new, sn)):
            h = mod.SnapshotHolder(snap)
            assert h.current() is snap
        p = Path(tmp) / "snap_no_key.json"
        ho, hn = old.SnapshotHolder(so), new.SnapshotHolder(sn)
        check("maybe_reload no-mtime-change False both",
              ho.maybe_reload(p) is False and hn.maybe_reload(p) is False)
        import os
        os.utime(p, (9999999999, 9999999999))
        check("maybe_reload after touch True both",
              ho.maybe_reload(p) is True and hn.maybe_reload(p) is True)
        check("reloaded shared fields identical",
              snap_repr(ho.current()) == snap_repr(hn.current()))

        # fetch_inline: existing coin returns cached; missing coin calls fetch; rate-limit
        for tag, mod, h in (("old", old, ho), ("new", new, hn)):
            got = h.fetch_inline("BTC", lambda: (_ for _ in ()).throw(RuntimeError("must not call")))
            check(f"fetch_inline cached [{tag}]", got is h.current().coins["BTC"])
            prof = mod.LiquidityProfile(coin="NEW", avg_1h_vol_usd=5.0, spread_pct=0.1,
                                        depth_top20_usd=6.0, pct_traded_60min=0.5)
            got = h.fetch_inline("NEW", lambda: prof)
            check(f"fetch_inline patch [{tag}]", got is prof and h.current().coins["NEW"] is prof)
            got2 = h.fetch_inline("COOLDOWN", lambda: None)
            got3 = h.fetch_inline("COOLDOWN", lambda: prof)  # within cooldown -> None
            check(f"fetch_inline cooldown [{tag}]", got2 is None and got3 is None)
            got4 = h.fetch_inline("ERR", lambda: (_ for _ in ()).throw(ValueError("boom")))
            check(f"fetch_inline exception->None [{tag}]", got4 is None)

    print("RESULT:", "PASS" if not FAILS else f"FAIL ({len(FAILS)})")
    sys.exit(0 if not FAILS else 1)


if __name__ == "__main__":
    main()
