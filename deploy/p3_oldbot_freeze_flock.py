#!/usr/bin/env python3
"""P3 rollout §3 step 2 — the TWO permitted old-bot changes (extended first),
deployed during the shadow window so they soak before cutover:

1. ENTRY_FREEZE gate at the top of trader.attempt_entry — INERT until env
   ENTRY_FREEZE=1 is set (cutover step 2). Manage/exit loop unaffected.
2. Startup single-writer flock on data/trades.db.lock in main_loop — the same
   lock fleet-engine takes (locks.py), so old-bot and engine can NEVER both
   write trades.db (rollout §4 mutual exclusion). Non-blocking acquire; held
   lock => CRITICAL + exit 1 (systemd restart-loops LOUDLY rather than
   dual-writing).

Run ON the bot host: python3 p3_oldbot_freeze_flock.py <bot_root>
Idempotent (marker check); timestamped .bak per file.
"""
import shutil
import sys
import time
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

# ---- 1. trader.py: ENTRY_FREEZE gate --------------------------------------
tp = root / "bot" / "trader.py"
src = tp.read_text(encoding="utf-8")
if "ENTRY_FREEZE" in src:
    print("trader.py ALREADY_APPLIED")
else:
    anchor = ''') -> Optional[Position]:
    """Full entry pipeline: liquidity → risk → stop-limit order → fill check.'''
    assert src.count(anchor) == 1, f"attempt_entry anchor != 1 ({src.count(anchor)})"
    src = src.replace(anchor, ''') -> Optional[Position]:
    """Full entry pipeline: liquidity → risk → stop-limit order → fill check.

    P3 cutover freeze (rollout §3 step 2, one of the TWO permitted old-bot
    changes): ENTRY_FREEZE=1 env rejects every new entry loudly while the
    manage/exit loop keeps running. INERT while the env var is unset.''', 1)
    # gate goes right after the docstring — find the line after the docstring end
    import re
    m = re.search(r'(\) -> Optional\[Position\]:\n(?:    """(?:.|\n)*?"""\n))', src)
    assert m, "attempt_entry docstring block not found"
    gate = '''    import os as _os
    if (_os.getenv("ENTRY_FREEZE", "") or "").strip().lower() in ("1", "true", "yes", "on"):
        log.warning("ENTRY_FREEZE active — rejecting entry %s %s (cutover freeze)",
                    getattr(signal, "coin", "?"), getattr(signal, "tf", "?"))
        _reject_and_log(signal, "entry_freeze_cutover")
        return None
'''
    src = src[:m.end(1)] + gate + src[m.end(1):]
    shutil.copy2(tp, f"{tp}.bak_p3freeze_{ts}")
    tp.write_text(src, encoding="utf-8")
    print(f"trader.py APPLIED — backup {tp}.bak_p3freeze_{ts}")

# ---- 2. main.py: startup flock ---------------------------------------------
mp = root / "bot" / "main.py"
src = mp.read_text(encoding="utf-8")
if "trades.db.lock" in src:
    print("main.py ALREADY_APPLIED")
else:
    anchor = "    init_db()"
    assert src.count(anchor) == 1, f"init_db anchor != 1 ({src.count(anchor)})"
    src = src.replace(anchor, '''    # P3 single-writer law (rollout §4, one of the TWO permitted old-bot
    # changes): exclusive flock on data/trades.db.lock — the SAME lock the
    # fleet-engine takes. A held lock means another writer owns trades.db
    # (engine mid-cutover): refuse to start LOUDLY, never dual-write.
    import fcntl as _fcntl
    _lockf = open("data/trades.db.lock", "a+")
    try:
        _fcntl.flock(_lockf.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        _lockf.seek(0); _lockf.truncate()
        _lockf.write("extended-bot pid=%d\\n" % os.getpid()); _lockf.flush()
        globals()["_TRADES_DB_FLOCK"] = _lockf  # hold for process lifetime
    except OSError:
        log.critical("trades.db.lock HELD by another writer — refusing to "
                     "start (P3 single-writer law). Is fleet-engine running?")
        raise SystemExit(1)
    init_db()''', 1)
    shutil.copy2(mp, f"{mp}.bak_p3flock_{ts}")
    mp.write_text(src, encoding="utf-8")
    print(f"main.py APPLIED — backup {mp}.bak_p3flock_{ts}")

print("done")
