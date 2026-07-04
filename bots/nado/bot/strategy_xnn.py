"""fleet_core loader shim (P1 2026-07-02) — canonical source: <bot_root>/fleet_core/strategy_xnn.py.

Executes the canonical source under module name bot.strategy_xnn so logger names,
class __module__ and all existing imports keep pre-P1 behavior exactly.
Edit fleet_core/strategy_xnn.py — never this shim. Rollback: restore bot/strategy_xnn.py.bak_p1_*.
"""
import os as _os
_src = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                     "fleet_core", "strategy_xnn.py")
with open(_src, encoding="utf-8") as _f:
    exec(compile(_f.read(), _src, "exec"), globals())
