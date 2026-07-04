#!/usr/bin/env python3
"""P4-C (pacifica): WS candle feed liveness — loud ping-send failure + reconnect counter.

Audit R4 2026-07-02: goals (a) app-level ping and (b) staleness watchdog (60s = max(60, 3x
ping_sec) -> ONE WARNING + force-close, RECOVERED INFO) are ALREADY deployed (audit patch
2026-07-02, bak_phase0_20260702T143957Z; local md5 354d58c6 == live). Remaining gaps:
  1. goal (c): no cumulative reconnect counter (only per-connect INFO).
  2. weakness vs HL canon: a FAILED ping send is swallowed silently ("except Exception:
     pass") — detection deferred up to 60s to the watchdog instead of immediate reconnect.
  3. stats() lacks the "degraded" field HL exposes.

Delta (liveness/logging/stats ONLY — bar store, finality gate, subscriptions untouched):
  * _ping_loop: failed ping send -> WARNING + force-close (mirror HL canon; the socket is
    almost certainly dead — a silent pass leaves _connected=True while scans quietly
    degrade to serial throttled REST until the watchdog fires).
  * __init__:  self._reconnect_count = 0
  * _on_open:  count += 1; connect INFO gains "(connect #N)"
  * stats():   gains "degraded" + "reconnects"

Run ON pacifica-bot host:
  python3 p4c_ws_liveness_pacifica.py /home/ubuntu/pacifica_xnn_bot/bot/ws_candle_feed.py
Idempotent: refuses to run twice (marker check). Creates .bak_p4c_<ts>.
Then: venv/bin/python -m py_compile <path> && systemctl restart pacifica-bot.
"""
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/home/ubuntu/pacifica_xnn_bot/bot/ws_candle_feed.py"
src = open(path, encoding="utf-8").read()

if "_reconnect_count" in src:
    print("ALREADY_APPLIED — marker found, nothing to do")
    sys.exit(0)

# 1. __init__: counter next to the existing watchdog state
a1 = "        self._stale_limit_sec = max(60.0, 3.0 * self._ping_sec)\n"
assert src.count(a1) == 1, f"__init__ anchor count != 1 ({src.count(a1)})"
src = src.replace(a1, a1 + "        self._reconnect_count = 0"
                  "   # P4-C 2026-07-03: cumulative (re)connects, ops flap signal\n", 1)

# 2. _ping_loop: failed ping send -> loud + immediate force-close (HL canon)
a2 = """                if self._ws and self._connected:
                    self._ws.send(json.dumps({"method": "ping"}))
            except Exception:
                pass
"""
assert src.count(a2) == 1, f"ping-send anchor count != 1 ({src.count(a2)})"
src = src.replace(a2, """                if self._ws and self._connected:
                    self._ws.send(json.dumps({"method": "ping"}))
            except Exception as e:
                # P4-C 2026-07-03 (mirror HL canon): a failed ping send = socket almost
                # certainly dead. Never swallow silently (zombie-WS: _connected stays True,
                # store goes stale, every scan quietly degrades to serial throttled REST
                # until the 60s watchdog fires) — force reconnect NOW.
                log.warning("ws-candle ping send failed: %s — forcing reconnect", e)
                self._connected = False
                try:
                    if self._ws:
                        self._ws.close()  # run_forever returns -> _run reconnects
                except Exception:
                    pass
                continue
""", 1)

# 3. _on_open: cumulative count in the connect INFO line
a3 = """        self._sub_count = n
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs", n)
"""
assert src.count(a3) == 1, f"_on_open anchor count != 1 ({src.count(a3)})"
src = src.replace(a3, """        self._sub_count = n
        self._reconnect_count += 1
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs (connect #%d)",
                 n, self._reconnect_count)
""", 1)

# 4. stats(): expose degraded (parity with HL) + reconnects
a4 = """        return {"connected": self._connected, "subs": self._sub_count, "pairs": pairs,
                "bars": total, "final": finals, "last_msg_age": age}
"""
assert src.count(a4) == 1, f"stats anchor count != 1 ({src.count(a4)})"
src = src.replace(a4, """        return {"connected": self._connected, "subs": self._sub_count, "pairs": pairs,
                "bars": total, "final": finals, "last_msg_age": age,
                "degraded": self._degraded, "reconnects": self._reconnect_count}
""", 1)

bak = f"{path}.bak_p4c_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
print("Next: venv/bin/python -m py_compile", path, "&& systemctl restart pacifica-bot && journalctl verify")
