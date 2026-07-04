"""fleet_core loader shim (P1 2026-07-02) — canonical source: <bot_root>/fleet_core/xnn_core.py.

Executes the canonical source under module name bot.xnn_core so logger names,
class __module__ and all existing imports keep pre-P1 behavior exactly.
Edit fleet_core/xnn_core.py — never this shim. Rollback: restore bot/xnn_core.py.bak_p1_*.
"""
import os as _os
_src = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                     "fleet_core", "xnn_core.py")
with open(_src, encoding="utf-8") as _f:
    exec(compile(_f.read(), _src, "exec"), globals())
