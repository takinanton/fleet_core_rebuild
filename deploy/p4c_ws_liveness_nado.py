#!/usr/bin/env python3
"""P4-C (nado): WS candle feed liveness — data-level staleness gate + reconnect counter.

Audit R4 2026-07-02 (local md5 98ef9291 == live /root/nado_xnn_bot/bot/ws_candle_feed.py):
  (a) protocol ping: ALREADY explicit — websockets.connect(ping_interval=20, ping_timeout=10,
      open_timeout=15). Kills half-open SOCKETS. No change.
  (b) staleness: PARTIAL/ABSENT — the 60s recv timeout just `continue`s ("ping_interval
      keeps the conn alive"); _last_msg_ts is written and shown in stats() but NEVER
      checked. Residual zombie class: a server-side dropped subscription on a pong-alive
      socket pushes nothing and raises nothing -> every scan silently degrades to the
      serial REST indexer forever.
  (c) reconnect counter: ABSENT (per-connect INFO only, no cumulative count).

Delta (liveness/logging/stats ONLY — bar store, finality gate, subscribe messages untouched):
  * __init__: WS_STALE_MAX_S (env, default 180s = 3x the 60s recv timeout; Nado's
    _last_msg_ts is refreshed only by candle pushes whose cadence is trade-dependent, so a
    60s threshold would false-positive on quiet tape) + _connect_ts baseline + degraded/
    counter state.
  * _run recv loop, TimeoutError branch (fires ONLY when nothing arrived for 60s — the
    natural, zero-hot-path-cost place to check; detection latency <= stale_max + 60s):
    age(max(last_msg, connect)) >= WS_STALE_MAX_S -> ONE WARNING 'ws-candle STALE age=..s —
    force-reconnect #N' + break, so the EXISTING outer loop closes the socket, backs off
    (its existing 1s->30s expo path) and resubscribes everything via _on_open.
  * _on_open: connect baseline _connect_ts (a connect that never pushes must still trip
    the gate), reconnect counter in the connect INFO.
  * _on_message: RECOVERED INFO when a message ACTUALLY arrives after a degrade (mirror
    HL canon — logging it at reconnect would falsely claim recovery on a still-dead sub).
  * stats(): gains "degraded", "reconnects", "stale_reconnects".

Fail-safe unchanged: stale store -> get_df None -> REST indexer fallback; the patch changes
WHEN we notice and reconnect, never which bars are served.

Run ON nado-bot host:
  python3 p4c_ws_liveness_nado.py /root/nado_xnn_bot/bot/ws_candle_feed.py
Idempotent: refuses to run twice (marker check). Creates .bak_p4c_<ts>.
Then: venv/bin/python -m py_compile <path> && systemctl restart nado-bot.
"""
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/root/nado_xnn_bot/bot/ws_candle_feed.py"
src = open(path, encoding="utf-8").read()

if "_stale_max_s" in src:
    print("ALREADY_APPLIED — marker found, nothing to do")
    sys.exit(0)

# 1. imports: need os for WS_STALE_MAX_S
a1 = "import logging\nimport ssl\n"
assert src.count(a1) == 1, f"import anchor count != 1 ({src.count(a1)})"
src = src.replace(a1, "import logging\nimport os\nimport ssl\n", 1)

# 2. __init__: liveness state
a2 = """        self._connected = False
        self._last_msg_ts = 0.0
        self._sub_count = 0
"""
assert src.count(a2) == 1, f"__init__ anchor count != 1 ({src.count(a2)})"
src = src.replace(a2, a2 + """        # ── P4-C WS liveness (2026-07-03) ──────────────────────────────────
        # (a) protocol ping already explicit (ping_interval=20/ping_timeout=10 above in
        #     _run) — kills half-open SOCKETS. (b) residual zombie class: a server-side
        #     dropped subscription on a pong-alive socket pushes nothing and raises
        #     nothing -> silent permanent REST-indexer degrade. Gate in the recv loop's
        #     timeout branch: no push for WS_STALE_MAX_S -> ONE WARNING + reconnect +
        #     full resubscribe. Default 180s = 3x the 60s recv timeout (pushes are
        #     trade-cadence; no guaranteed app-pong traffic to ride a tighter limit on).
        self._stale_max_s = float(os.getenv("WS_STALE_MAX_S", "180") or 180)
        self._connect_ts = 0.0
        self._degraded = False
        self._stale_count = 0         # staleness-forced reconnects
        self._reconnect_count = 0     # cumulative successful (re)connects, ops flap signal
""", 1)

# 3. recv-loop TimeoutError branch: staleness gate (fires only when NOTHING arrived 60s)
a3 = """                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            continue  # ping_interval keeps the conn alive
"""
assert src.count(a3) == 1, f"recv-timeout anchor count != 1 ({src.count(a3)})"
src = src.replace(a3, """                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            # P4-C 2026-07-03: protocol ping keeps the SOCKET alive, but a
                            # dropped server-side subscription pushes nothing and never
                            # errors. No push since connect/last-msg for _stale_max_s ->
                            # force reconnect via the existing outer loop (close+backoff+
                            # full resubscribe in _on_open).
                            ref = max(self._last_msg_ts, self._connect_ts)
                            age = time.time() - ref
                            if ref > 0 and age >= self._stale_max_s:
                                self._stale_count += 1
                                self._degraded = True
                                log.warning(
                                    "ws-candle STALE age=%.0fs — force-reconnect #%d "
                                    "(feed degraded, scans fall back to REST indexer)",
                                    age, self._stale_count)
                                break  # leave recv loop -> ctx closes ws -> outer reconnects
                            continue  # ping_interval keeps the conn alive
""", 1)

# 4. _on_open: connect baseline + reconnect counter + RECOVERED
a4 = """        self._sub_count = n
        self._connected = True
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs", n)
"""
assert src.count(a4) == 1, f"_on_open anchor count != 1 ({src.count(a4)})"
src = src.replace(a4, """        self._sub_count = n
        self._connected = True
        self._connect_ts = time.time()   # P4-C: staleness baseline for a push-quiet connect
        self._reconnect_count += 1
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs (connect #%d)",
                 n, self._reconnect_count)
""", 1)

# 4b. _on_message: RECOVERED only when a REAL candle push arrives after a degrade
#     (review fix 2026-07-03: clearing at the top fired on subscribe-ACK frames too —
#     a still-dead subscription would flap STALE->RECOVERED every cycle; the clear now
#     sits AFTER the latest_candlestick type filter, so only an actual push proves life).
a4b = """        if o.get("type") != "latest_candlestick":
            return  # ignore subscribe acks / errors / other streams
"""
assert src.count(a4b) == 1, f"_on_message type-filter anchor count != 1 ({src.count(a4b)})"
src = src.replace(a4b, """        if o.get("type") != "latest_candlestick":
            return  # ignore subscribe acks / errors / other streams
        if self._degraded:
            self._degraded = False
            log.info("ws-candle RECOVERED: candle pushes flowing again after stale period")
""", 1)

# 5. stats(): expose liveness state
a5 = """        return {"connected": self._connected, "subs": self._sub_count,
                "pairs": pairs, "bars": total, "last_msg_age": age}
"""
assert src.count(a5) == 1, f"stats anchor count != 1 ({src.count(a5)})"
src = src.replace(a5, """        return {"connected": self._connected, "subs": self._sub_count,
                "pairs": pairs, "bars": total, "last_msg_age": age,
                "degraded": self._degraded, "reconnects": self._reconnect_count,
                "stale_reconnects": self._stale_count}
""", 1)

bak = f"{path}.bak_p4c_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
print("Next: venv/bin/python -m py_compile", path, "&& systemctl restart nado-bot && journalctl verify")
