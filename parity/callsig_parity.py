#!/usr/bin/env python3
"""Cross-module call-signature parity guard (drift class R1).

Instance 2026-07-02: main.py on ext/pac/nado called
_lookup_real_close_px(..., sl_oid=...) while those venues' trader.py def was still
2-arg -> TypeError, swallowed by the blanket restore-resolve except -> stale DB rows
kept open forever (delist-phantom class resurrected by the port of its own fix).

For every venue: parse all bot/*.py, collect function defs, and assert every call
targeting a venue-local function name only uses keywords some def of that name
accepts (or the def takes **kwargs). Exits 1 loud on any violation.

RUN AFTER ANY MANUAL PORT OF A FIX ACROSS VENUES, BEFORE DEPLOY:
    python3 fleet_core_rebuild/parity/callsig_parity.py [bots_dir]
"""
import ast
import sys
from pathlib import Path

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "bots"
VENUES = sorted(p.name for p in ROOT.iterdir() if (p / "bot").is_dir())
# P1 shims (2026-07-02): venue bot/*.py may be loader shims for <repo>/fleet_core/*.py —
# the canonical defs live there, so merge them into every venue's def-space.
CORE = ROOT.parent / "fleet_core"


def defs_in(tree):
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            names = {x.arg for x in a.posonlyargs + a.args + a.kwonlyargs}
            out.setdefault(node.name, []).append((names, a.kwarg is not None))
    return out


core_defs = {}
if CORE.is_dir():
    for f in sorted(CORE.glob("*.py")):
        try:
            for name, sigs in defs_in(ast.parse(f.read_text())).items():
                core_defs.setdefault(name, []).extend(sigs)
        except SyntaxError as e:
            print(f"FAIL fleet_core/{f.name}: syntax error {e}")
            sys.exit(1)

fail = 0
for venue in VENUES:
    files = sorted((ROOT / venue / "bot").glob("*.py"))
    trees = {}
    defs = {k: list(v) for k, v in core_defs.items()}
    for f in files:
        try:
            t = ast.parse(f.read_text())
        except SyntaxError as e:
            print(f"FAIL {venue}/{f.name}: syntax error {e}")
            fail = 1
            continue
        trees[f] = t
        for name, sigs in defs_in(t).items():
            defs.setdefault(name, []).extend(sigs)
    for f, t in trees.items():
        for node in ast.walk(t):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue
            name = node.func.id
            if name not in defs:
                continue
            for kw in node.keywords:
                if kw.arg is None:  # **expansion
                    continue
                if not any(kw.arg in names or has_kw for names, has_kw in defs[name]):
                    print(f"FAIL {venue}/{f.name}:{node.lineno}: {name}(... {kw.arg}=) "
                          f"— no def of {name} in this venue accepts '{kw.arg}'")
                    fail = 1

print("callsig-parity:", "FAIL" if fail else "PASS", f"({len(VENUES)} venues: {', '.join(VENUES)})")
sys.exit(fail)
