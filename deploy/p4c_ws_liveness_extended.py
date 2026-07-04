#!/usr/bin/env python3
"""P4-C (extended): WS candle feed liveness — feed-wide staleness watchdog + loud sub
errors + reconnect counter + loop-thread restart.

Audit R4 2026-07-02 (local md5 3c23b538 == live /root/extended_xnn_bot/bot/ws_candle_feed.py):
  (a) protocol ping: ALREADY covered by websockets-lib DEFAULTS inside the x10 SDK stream
      (SDK perpetual_stream_connection.py:105 passes no ping args -> ping_interval=20/
      ping_timeout=20). Kills half-open SOCKETS. No code change — documented in a comment.
  (b) staleness: ABSENT. _last_msg_ts is written in _ingest and shown in stats() but NEVER
      checked. A server-side dropped SUBSCRIPTION on a pong-alive socket = zero messages,
      zero exceptions, zero logs -> every scan silently degrades to serial throttled REST
      forever (the exact zombie class bought back 2026-07-02). Also: per-sub reconnect
      errors are log.DEBUG only (crash-looping sub invisible at ops level), and if the bg
      event-loop thread dies the whole feed is dead with one WARNING and no restart.
  (c) reconnect counter: ABSENT ('connected, subscribed' INFO fires only when ALL subs
      connect the FIRST time; later reconnects fully silent).

Delta (liveness/logging/stats ONLY — bar store, finality gate, subscribe args untouched):
  * __init__: WS_STALE_MAX_S (env, default 180s = 3x the 60s recv-timeout scale used by the
    asyncio feeds; Extended's _last_msg_ts is refreshed only by candle pushes whose cadence
    is trade-dependent, so the tight 60s HL/Paci threshold — which rides on guaranteed
    20s ping/pong traffic — would false-positive on quiet tape) + degraded/counter state
    + task list.
  * _run_loop: keep task refs; spawn _watchdog(); restart run_forever if it ever exits
    while not stopped (was: single WARNING then permanently dead feed).
  * _watchdog (new async task, checks every 30s): no message across ALL subs for
    WS_STALE_MAX_S -> ONE WARNING 'ws-candle STALE age=..s — force-reconnect #N' + cancel/
    recreate every sub task (each re-subscribe then uses the existing per-sub backoff
    path); INFO RECOVERED when flow resumes.
  * _sub_task: first error per degrade promoted debug->WARNING (retries stay debug);
    explicit CancelledError re-raise (py<3.8-safe — watchdog cancel must not be swallowed
    by 'except Exception' and resurrect a duplicate loop); re-subscribe after error counts
    into _reconnect_count + INFO on recovery.
  * stats(): gains "degraded", "reconnects", "stale_restarts".

Fail-safe unchanged: stale store -> get_df None -> REST fallback; this patch changes WHEN
we notice and reconnect, never what bars are served.

Run ON extended-bot host:
  python3 p4c_ws_liveness_extended.py /root/extended_xnn_bot/bot/ws_candle_feed.py
Idempotent: refuses to run twice (marker check). Creates .bak_p4c_<ts>.
Then: venv/bin/python -m py_compile <path> && systemctl restart extended-bot.
"""
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/root/extended_xnn_bot/bot/ws_candle_feed.py"
src = open(path, encoding="utf-8").read()

if "_stale_max_s" in src or "_watchdog" in src:
    print("ALREADY_APPLIED — marker found, nothing to do")
    sys.exit(0)

# 1. imports: need os for WS_STALE_MAX_S
a1 = "import logging\nimport threading\n"
assert src.count(a1) == 1, f"import anchor count != 1 ({src.count(a1)})"
src = src.replace(a1, "import logging\nimport os\nimport threading\n", 1)

# 2. __init__: liveness state
a2 = """        self._last_msg_ts = 0.0
        self._sub_count = 0
"""
assert src.count(a2) == 1, f"__init__ anchor count != 1 ({src.count(a2)})"
src = src.replace(a2, a2 + """        # ── P4-C WS liveness (2026-07-03) ──────────────────────────────────
        # (a) protocol ping/pong: handled by websockets-lib DEFAULTS inside the x10
        #     SDK stream (SDK passes no ping args -> ping_interval=20/ping_timeout=20).
        #     Kills half-open SOCKETS. Documented dependency — do not "optimize" away.
        # (b) residual zombie class: a server-side DROPPED SUBSCRIPTION on a pong-alive
        #     socket delivers zero messages and zero exceptions -> every scan silently
        #     degrades to serial throttled REST forever. Feed-wide watchdog: no message
        #     across ALL subs for WS_STALE_MAX_S -> ONE WARNING + restart all sub tasks.
        #     Default 180s: pushes are trade-cadence (no guaranteed app-pong traffic like
        #     HL/Paci 20s ping -> their 60s threshold would false-positive on quiet tape).
        self._stale_max_s = float(os.getenv("WS_STALE_MAX_S", "180") or 180)
        self._degraded = False
        self._stale_count = 0         # watchdog-forced restart batches
        self._reconnect_count = 0     # per-sub re-subscribes after an error (feed-wide)
        self._last_reconnect_ts = 0.0
        self._tasks: list = []
""", 1)

# 3. _run_loop: keep task refs, spawn watchdog, restart a dead loop
a3 = """        for (coin, mkt, iv) in self._subs:
            loop.create_task(self._sub_task(coin, mkt, iv))
        try:
            loop.run_forever()
        except Exception as e:
            log.warning("ws-candle loop error: %s", e)
"""
assert src.count(a3) == 1, f"_run_loop anchor count != 1 ({src.count(a3)})"
src = src.replace(a3, """        self._tasks = [loop.create_task(self._sub_task(coin, mkt, iv))
                       for (coin, mkt, iv) in self._subs]
        loop.create_task(self._watchdog())
        # P4-C: keep the loop thread alive — a crashed run_forever previously killed the
        # WHOLE feed with a single WARNING and no restart (permanent silent REST degrade).
        while not self._stop:
            try:
                loop.run_forever()          # returns only via stop()'s loop.stop()
                if not self._stop:
                    log.warning("ws-candle loop stopped unexpectedly — restarting")
            except Exception as e:
                log.warning("ws-candle loop error: %s — restarting", e)
            if self._stop:
                break
            time.sleep(5.0)
""", 1)

# 4. new _watchdog task, inserted right before _sub_task
a4 = "    async def _sub_task(self, coin, market_name, interval):\n"
assert src.count(a4) == 1, f"_sub_task def anchor count != 1 ({src.count(a4)})"
src = src.replace(a4, '''    async def _watchdog(self):
        """P4-C feed-wide staleness watchdog (2026-07-03). Protocol pings (websockets/SDK
        defaults) surface dead SOCKETS, but a dropped server-side subscription on a
        pong-alive socket is push-dead and error-free. If NO message arrives across all
        subs for WS_STALE_MAX_S, warn (once per forced restart) and cancel/recreate every
        sub task — each re-subscribe runs the normal per-sub backoff. get_df fails safe
        meanwhile (stale store -> None -> REST), so this changes liveness only, never
        which bars are served."""
        while not self._stop:
            await asyncio.sleep(30.0)
            if self._stop:
                break
            ref = max(self._last_msg_ts, self._last_reconnect_ts)
            if ref <= 0:
                continue                      # nothing received / never connected yet
            age = time.time() - ref
            if age < self._stale_max_s:
                # recovery must be proven by an ACTUAL message after the restart —
                # a fresh _last_reconnect_ts alone is just us reconnecting, not the
                # server pushing again (else STALE/RECOVERED would flap spuriously).
                if self._degraded and self._last_msg_ts > self._last_reconnect_ts:
                    self._degraded = False
                    log.info("ws-candle RECOVERED: WS messages flowing again after stale period")
                continue
            self._stale_count += 1
            log.warning("ws-candle STALE age=%.0fs — force-reconnect #%d (feed degraded, "
                        "scans fall back to throttled REST; restarting %d sub tasks)",
                        age, self._stale_count, len(self._tasks))
            self._degraded = True
            for t in self._tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
            loop = asyncio.get_event_loop()
            self._tasks = [loop.create_task(self._sub_task(c, mkt, iv))
                           for (c, mkt, iv) in self._subs]
            self._last_reconnect_ts = time.time()   # restart the stale clock post-restart

''' + a4, 1)

# 5a. _sub_task: warned flag (one WARNING per degrade)
a5a = """        backoff = 1.0
        first = True
        while not self._stop:
"""
assert src.count(a5a) == 1, f"_sub_task head anchor count != 1 ({src.count(a5a)})"
src = src.replace(a5a, """        backoff = 1.0
        first = True
        warned = False   # P4-C: one WARNING per degrade, retries stay debug
        while not self._stop:
""", 1)

# 5b. _sub_task: count re-subscribes + recovery INFO
a5b = """                    first = False
                backoff = 1.0
"""
assert src.count(a5b) == 1, f"_sub_task first-flag anchor count != 1 ({src.count(a5b)})"
src = src.replace(a5b, """                    first = False
                else:
                    # P4-C: re-subscribe after an error — count it; close the degrade
                    # window with an INFO if this sub had warned.
                    self._reconnect_count += 1
                    if warned:
                        log.info("ws-candle sub %s %s reconnected (feed reconnects=%d)",
                                 coin, interval, self._reconnect_count)
                        warned = False
                backoff = 1.0
""", 1)

# 5c. _sub_task: promote first failure to WARNING; CancelledError must propagate
a5c = """            except Exception as e:
                log.debug("ws-candle sub %s %s err: %s", coin, interval, e)
"""
assert src.count(a5c) == 1, f"_sub_task except anchor count != 1 ({src.count(a5c)})"
src = src.replace(a5c, """            except asyncio.CancelledError:
                raise   # P4-C watchdog restart — die cleanly, never resurrect a duplicate
            except Exception as e:
                # P4-C: first failure per degrade WARNING (was debug-only — a crash-looping
                # sub was invisible at ops log level); subsequent retries stay debug.
                if not warned:
                    warned = True
                    log.warning("ws-candle sub %s %s err: %s — reconnecting with backoff",
                                coin, interval, e)
                else:
                    log.debug("ws-candle sub %s %s err: %s", coin, interval, e)
""", 1)

# 6. stats(): expose liveness state
a6 = """        return {"subs": self._sub_count, "pairs": pairs, "bars": total,
                "final": finals, "last_msg_age": age}
"""
assert src.count(a6) == 1, f"stats anchor count != 1 ({src.count(a6)})"
src = src.replace(a6, """        return {"subs": self._sub_count, "pairs": pairs, "bars": total,
                "final": finals, "last_msg_age": age,
                "degraded": self._degraded, "reconnects": self._reconnect_count,
                "stale_restarts": self._stale_count}
""", 1)

bak = f"{path}.bak_p4c_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
print("Next: venv/bin/python -m py_compile", path, "&& systemctl restart extended-bot && journalctl verify")
