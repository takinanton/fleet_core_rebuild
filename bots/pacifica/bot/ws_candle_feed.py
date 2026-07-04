"""Pacifica WS candle feed — push-based OHLC store (kills per-scan REST /kline polling).

WHY (root, 2026-06-23): every venue's kline API is per-symbol REST; tuning throttle/limit/
parallel only polishes polling. The architecturally-correct fix is WS PUSH — subscribe once
per (coin,interval), the exchange streams closed bars, we serve scans from memory: 0 fetch,
0 rate-limit, instant. This module is that layer for Pacifica.

DESIGN (additive · env-gated PACIFICA_WS_CANDLE · REST-fallback = zero correctness risk):
  * One background WebSocketApp -> wss://ws.pacifica.fi/ws.
  * subscribe {method:subscribe, params:{source:candle, symbol, interval}} per (coin,interval).
  * push {channel:candle, data:{t,T,s,i,o,c,h,l,v,n}} -> rolling store keyed by bar-start t.
  * A bar is CONFIRMED-FINAL iff (a closing push arrived: recv_walltime >= T_end) OR a newer
    bar exists in the store. get_df serves the closed-bar window ONLY when the latest closed
    bar (now//bar*bar - bar) is confirmed-final; else returns None -> caller uses REST.
  * seed(coin,iv,df): warm the store from the first REST df so cold-start history reuses the
    existing REST path (no extra burst). WS pushes never clobber a fresher value.
Any WS failure -> store goes stale -> get_df None -> REST. Flag off -> module never attached.
"""
from __future__ import annotations

import json
import logging
import threading
import time

import pandas as pd

log = logging.getLogger(__name__)

WS_URL = "wss://ws.pacifica.fi/ws"
_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
    "12h": 43_200_000, "1d": 86_400_000,
}


class WsCandleFeed:
    def __init__(self, coins, intervals, store_bars: int = 320, ping_sec: float = 20.0):
        self._coins = list(dict.fromkeys(coins))
        self._intervals = [iv for iv in dict.fromkeys(intervals) if iv in _TF_MS]
        self._subs = [(c, i) for c in self._coins for i in self._intervals]
        self._store_bars = int(store_bars)
        self._ping_sec = float(ping_sec)
        # (coin,iv) -> {t_ms: [t,o,h,l,c,v]}  and  (coin,iv) -> set(final t_ms)
        self._bars: dict[tuple, dict] = {}
        self._final: dict[tuple, set] = {}
        self._lock = threading.RLock()
        self._ws = None
        self._stop = False
        self._connected = False
        self._last_msg_ts = 0.0
        self._sub_count = 0
        # Staleness watchdog state (audit 2026-07-02, zombie-WS fix): run_forever(ping_interval=0)
        # + an exception-swallowing app-level ping loop means a half-dead socket is NEVER detected
        # — the feed silently degrades to throttled REST (entry-latency edge lost). The server
        # answers every {"method":"ping"} with a message (any message refreshes _last_msg_ts), so
        # 3 consecutive unanswered pings (= 3×ping_sec, floor 60s) proves a dead link → force-close
        # the socket so _run's reconnect loop re-establishes it. One WARNING per degrade, INFO on
        # recover.
        self._connect_ts = 0.0
        self._degraded = False
        self._stale_limit_sec = max(60.0, 3.0 * self._ping_sec)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self):
        threading.Thread(target=self._run, name="ws-candle", daemon=True).start()
        threading.Thread(target=self._ping_loop, name="ws-candle-ping", daemon=True).start()

    def stop(self):
        self._stop = True
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def _run(self):
        import websocket  # websocket-client
        backoff = 1.0
        while not self._stop:
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_close=self._on_close,
                    on_error=self._on_error,
                )
                self._ws.run_forever(ping_interval=0)
                backoff = 1.0
            except Exception as e:
                log.warning("ws-candle run_forever error: %s", e)
            self._connected = False
            if self._stop:
                break
            time.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2.0, 30.0)

    def _ping_loop(self):
        while not self._stop:
            time.sleep(self._ping_sec)
            try:
                if self._ws and self._connected:
                    self._ws.send(json.dumps({"method": "ping"}))
            except Exception:
                pass
            # Staleness watchdog (audit 2026-07-02): a zombie socket keeps _connected=True
            # but stops delivering messages (incl. pong replies to the ping above). If no
            # message has arrived for _stale_limit_sec since the later of last-message /
            # connect time, force-close so _run's loop reconnects. WARNING once on degrade.
            try:
                ref = max(self._last_msg_ts, self._connect_ts)
                if self._connected and ref > 0 and (time.time() - ref) > self._stale_limit_sec:
                    if not self._degraded:
                        self._degraded = True
                        log.warning(
                            "ws-candle STALE: no message for %.0fs (> %.0fs limit) — zombie "
                            "socket, forcing close/reconnect (scans fall back to REST meanwhile)",
                            time.time() - ref, self._stale_limit_sec,
                        )
                    self._connected = False
                    if self._ws:
                        self._ws.close()  # run_forever returns -> _run reconnects
            except Exception as e:
                log.warning("ws-candle staleness watchdog error: %s", e)

    # ── ws callbacks ─────────────────────────────────────────────────────────
    def _on_open(self, ws):
        self._connected = True
        self._connect_ts = time.time()
        if self._degraded:
            self._degraded = False
            log.info("ws-candle RECOVERED: reconnected after stale/zombie socket — push feed live again")
        n = 0
        for (c, i) in self._subs:
            try:
                ws.send(json.dumps({"method": "subscribe",
                                    "params": {"source": "candle", "symbol": c, "interval": i}}))
                n += 1
            except Exception as e:
                log.warning("ws-candle subscribe %s %s failed: %s", c, i, e)
        self._sub_count = n
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs", n)

    def _on_close(self, ws, *a):
        self._connected = False

    def _on_error(self, ws, err):
        log.warning("ws-candle error: %s", err)

    def _on_message(self, ws, msg):
        self._last_msg_ts = time.time()
        try:
            o = json.loads(msg)
        except Exception:
            return
        if o.get("channel") != "candle":
            return
        d = o.get("data") or {}
        try:
            s = d["s"]; i = d["i"]; t = int(d["t"]); t_end = int(d["T"])
            bar = [t, float(d["o"]), float(d["h"]), float(d["l"]), float(d["c"]), float(d["v"])]
        except (KeyError, ValueError, TypeError):
            return
        key = (s, i)
        now_ms = time.time() * 1000.0
        with self._lock:
            store = self._bars.setdefault(key, {})
            fin = self._final.setdefault(key, set())
            store[t] = bar
            # closing push: this update arrived at/after the bar's end -> values are final
            if now_ms >= t_end:
                fin.add(t)
            if len(store) > self._store_bars * 2:
                keep = set(sorted(store)[-self._store_bars:])
                for old in [k for k in store if k not in keep]:
                    store.pop(old, None)
                    fin.discard(old)

    # ── warm from REST ───────────────────────────────────────────────────────
    def seed(self, coin, interval, df_full):
        """Warm store from a REST df (cols time,Open,High,Low,Close,Volume). Closed rows are
        final by construction; the last row (possibly forming) is NOT marked final."""
        if interval not in self._intervals:
            return
        key = (coin, interval)
        try:
            rows = []
            for _, r in df_full.iterrows():
                t = int(pd.Timestamp(r["time"]).value // 10**6)
                rows.append((t, [t, float(r["Open"]), float(r["High"]), float(r["Low"]),
                                 float(r["Close"]), float(r["Volume"])]))
            if not rows:
                return
            last_t = rows[-1][0]
            with self._lock:
                store = self._bars.setdefault(key, {})
                fin = self._final.setdefault(key, set())
                for t, bar in rows:
                    store.setdefault(t, bar)        # never clobber a WS-fresher bar
                    if t != last_t:
                        fin.add(t)                  # all but the last REST row are closed/final
                if len(store) > self._store_bars * 2:
                    keep = set(sorted(store)[-self._store_bars:])
                    for old in [k for k in store if k not in keep]:
                        store.pop(old, None)
                        fin.discard(old)
        except Exception as e:
            log.debug("ws-candle seed %s %s failed: %s", coin, interval, e)

    # ── read API ─────────────────────────────────────────────────────────────
    def get_df(self, coin, interval, limit):
        """Closed-bar window IDENTICAL to PacificaClient.candles() REST output, or None if the
        latest closed bar is not confirmed-final / store stale / too short -> REST fallback."""
        bar_ms = _TF_MS.get(interval)
        if bar_ms is None:
            return None
        key = (coin, interval)
        now_ms = int(time.time() * 1000)
        last_closed = (now_ms // bar_ms) * bar_ms - bar_ms
        with self._lock:
            store = self._bars.get(key)
            fin = self._final.get(key)
            if not store:
                return None
            keys = sorted(store)
            if last_closed not in store:
                return None                              # missing the latest closed bar -> REST
            confirmed = (fin is not None and last_closed in fin) or (keys[-1] > last_closed)
            if not confirmed:
                return None                              # last closed bar not yet final -> REST
            closed = [t for t in keys if t <= last_closed]
            if not closed or closed[-1] != last_closed:
                return None
            if len(closed) < min(limit, 100):
                return None                              # window too short -> REST (will seed)
            bars = [store[t] for t in closed]
        df = pd.DataFrame(bars, columns=["t", "Open", "High", "Low", "Close", "Volume"])
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df[["time", "Open", "High", "Low", "Close", "Volume"]]
        if limit and len(df) > limit:
            df = df.iloc[-limit:]
        return df.reset_index(drop=True)

    def stats(self):
        with self._lock:
            pairs = len(self._bars)
            total = sum(len(v) for v in self._bars.values())
            finals = sum(len(v) for v in self._final.values())
        age = round(time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None
        return {"connected": self._connected, "subs": self._sub_count, "pairs": pairs,
                "bars": total, "final": finals, "last_msg_age": age}
