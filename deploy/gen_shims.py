#!/usr/bin/env python3
"""Generate bot/<mod>.py loader shims for P1 fleet_core cutover.

The shim executes <bot_root>/fleet_core/<mod>.py under the module name bot.<mod>,
so logger names (%(name)s), class __module__, __name__ and every
'from bot.<mod> import ...' consumer behave byte-identically to the pre-P1 fork copy.
"""
import os
import sys

TEMPLATE = '''"""fleet_core loader shim (P1 2026-07-02) — canonical source: <bot_root>/fleet_core/{mod}.py.

Executes the canonical source under module name bot.{mod} so logger names,
class __module__ and all existing imports keep pre-P1 behavior exactly.
Edit fleet_core/{mod}.py — never this shim. Rollback: restore bot/{mod}.py.bak_p1_*.
"""
import os as _os
_src = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                     "fleet_core", "{mod}.py")
with open(_src, encoding="utf-8") as _f:
    exec(compile(_f.read(), _src, "exec"), globals())
'''

MODULES = sys.argv[1:] or [
    "xnn_core", "liquidity", "warmup_backfill", "strategy_xnn",
    "risk", "journal", "orphan_sweep",
]

out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shims")
os.makedirs(out_dir, exist_ok=True)
for mod in MODULES:
    p = os.path.join(out_dir, f"{mod}.py")
    with open(p, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(mod=mod))
    print(f"wrote {p}")
