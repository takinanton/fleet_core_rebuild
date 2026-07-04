#!/usr/bin/env python3
"""P4-C (hl): WS candle feed liveness — reconnect counter for ops (goal c ONLY).

Audit R4 2026-07-02: goals (a) keepalive ping (app-level {"method":"ping"} every 20s,
failed send -> loud force-reconnect) and (b) staleness watchdog (3x ping_sec = 60s ->
ONE WARNING + force-reconnect, RECOVERED INFO, degraded flag in stats) are ALREADY
deployed on hl (Phase-0 patch 2026-07-02, bak_phase0_20260702T144544Z; local md5
54cef3f8 == live). Remaining gap = goal (c): reconnects are only visible as per-connect
INFO lines with no cumulative count — ops cannot see flap frequency at a glance.

Delta (logging/stats ONLY — zero behavior change):
  * __init__:  self._reconnect_count = 0
  * _on_open:  count += 1; connect INFO gains "(connect #N)"
  * stats():   gains "reconnects"

Run ON hl-bot host:
  python3 p4c_ws_liveness_hl.py /home/ubuntu/hl_combo_bot/bot/ws_candle_feed.py
Idempotent: refuses to run twice (marker check). Creates .bak_p4c_<ts>.
Then: venv/bin/python -m py_compile <path> && systemctl restart hl-bot.
"""
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/home/ubuntu/hl_combo_bot/bot/ws_candle_feed.py"
src = open(path, encoding="utf-8").read()

if "_reconnect_count" in src:
    print("ALREADY_APPLIED — marker found, nothing to do")
    sys.exit(0)

# 1. __init__: counter next to the existing Phase-0 watchdog state
a1 = """        self._stale_after = 3.0 * self._ping_sec
        self._degraded = False
"""
assert src.count(a1) == 1, f"__init__ anchor count != 1 ({src.count(a1)})"
src = src.replace(a1, a1 + "        self._reconnect_count = 0"
                  "   # P4-C 2026-07-03: cumulative (re)connects, ops flap signal\n", 1)

# 2. _on_open: cumulative count in the connect INFO line
a2 = """        self._sub_count = n
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs", n)
"""
assert src.count(a2) == 1, f"_on_open anchor count != 1 ({src.count(a2)})"
src = src.replace(a2, """        self._sub_count = n
        self._reconnect_count += 1
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs (connect #%d)",
                 n, self._reconnect_count)
""", 1)

# 3. stats(): expose the counter
a3 = """                "bars": total, "final": finals, "last_msg_age": age,
                "degraded": self._degraded}
"""
assert src.count(a3) == 1, f"stats anchor count != 1 ({src.count(a3)})"
src = src.replace(a3, """                "bars": total, "final": finals, "last_msg_age": age,
                "degraded": self._degraded, "reconnects": self._reconnect_count}
""", 1)

bak = f"{path}.bak_p4c_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
print("Next: venv/bin/python -m py_compile", path, "&& systemctl restart hl-bot && journalctl verify")
