"""Regression guard — REMEDY-A add_isolated_margin float rounding (incident 2026-06-23).

xyz_DELL: REMEDY-A tried to add isolated margin $72.95652778947826 (unrounded) -> HL SDK
float_to_usd_int raised ('float_to_int causes rounding') -> REMEDY-A failed -> REMEDY-B
clamped SL 390.93->412.28 -> instant gap_through_sl, -$186.76 (-0.72R) in 4s.
Root: raw float -> SDK float_to_int without round() to 6dp.
Fix: exchange_hl.py add_isolated_margin -> update_isolated_margin(round(float(usd), 6), name).

Fails LOUD if (a) SDK precondition regresses, or (b) round() removed from the call site.
Usage: python tests/test_remedy_a_float_round.py /path/to/hl_repo
"""
import sys, re, pathlib
REPO = sys.argv[1] if len(sys.argv) > 1 else "."
sys.path.insert(0, REPO)
from hyperliquid.utils.signing import float_to_usd_int

PATHOLOGICAL = 72.95652778947826  # exact value from the DELL incident
fails = []

# (1) raw float MUST raise — the trap we fell into
try:
    float_to_usd_int(PATHOLOGICAL)
    fails.append("raw float did NOT raise — SDK precondition changed; revisit guard")
except ValueError:
    pass

# (2) rounded-to-6dp MUST be SDK-safe — the fix
try:
    float_to_usd_int(round(float(PATHOLOGICAL), 6))
except ValueError as e:
    fails.append("round(usd,6) still raised: %s" % (e,))

# (3) STATIC: the live call site must still round before the SDK call
src = pathlib.Path(REPO, "bot/exchange_hl.py").read_text()
m = re.search(r"def add_isolated_margin\b.*?(?:\n    def |\Z)", src, re.S)
body = m.group(0) if m else ""
if "update_isolated_margin(" not in body:
    fails.append("add_isolated_margin: update_isolated_margin call not found")
elif not re.search(r"update_isolated_margin\(\s*round\(", body):
    fails.append("add_isolated_margin: SDK call NOT wrapped in round() — float-round regression!")

if fails:
    print("FAILED:"); [print("  -", f) for f in fails]; sys.exit(1)
print("ALL REMEDY-A FLOAT-ROUND GUARD TESTS PASSED")
