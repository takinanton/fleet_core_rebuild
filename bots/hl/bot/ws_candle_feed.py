"""Hyperliquid WS candle feed — push-based OHLC store (kills per-scan REST /kline polling).

Ported from Pacifica ws_candle_feed (2026-06-24). HL candle push data shape (t,T,s,i,o,c,h,l,v)
is IDENTICAL to Pacifica; only the WS URL + subscribe message format differ.

DESIGN (additive · env-gated HL_WS_CANDLE · REST-fallback = zero correctness risk):
  * One background WebSocketApp -> wss://api.hyperliquid.xyz/ws.
  * subscribe {method:subscribe, subscription:{type:candle, coin, interval}} per (coin,interval).
  * push {channel:candle, data:{t,T,s,i,o,c,h,l,v,n}} -> rolling store keyed by bar-start t.
  * A bar is CONFIRMED-FINAL iff (a closing push arrived: recv_walltime >= T_end) OR a newer
    bar exists in the store. get_df serves the closed-bar window ONLY when the latest closed
    bar is confirmed-final; else returns None -> caller uses REST.
  * seed(coin,iv,df): warm the store from the first REST df so cold-start history reuses the
    existing REST path (no extra burst). WS pushes never clobber a fresher value.
Any WS failure -> store goes stale -> get_df None -> REST. Flag off -> module never attached.
HIP-3 (xyz:) coins are NOT subscribed here (caller passes crypto api-names only).
"""
from __future__ import annotations

import json
import logging
import threading
import time

import pandas as pd

log = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
    "12h": 43_200_000, "1d": 86_400_000,
}

# Slow TFs whose just-closed bar can be aggregated bit-exact from finer 4h bars when HL's
# sparse 1d/8h (esp HIP-3) candle pushes leave it absent from store at the boundary
# (2026-06-29 self-heal; verified 0.00bp vs REST candles()). 4h itself is WS-served promptly.
_SLOW_TF_FROM_4H = {"8h", "1d"}


class WsCandleFeed:
    def __init__(self, coins, intervals, store_bars: int = 320, ping_sec: float = 20.0):
        self._coins = list(dict.fromkeys(coins))
        self._intervals = [iv for iv in dict.fromkeys(intervals) if iv in _TF_MS]
        self._subs = [(c, i) for c in self._coins for i in self._intervals]
        self._store_bars = int(store_bars)
        self._ping_sec = float(ping_sec)
        self._bars: dict[tuple, dict] = {}
        self._final: dict[tuple, set] = {}
        self._lock = threading.RLock()
        self._ws = None
        self._stop = False
        self._connected = False
        self._last_msg_ts = 0.0
        self._sub_count = 0
        # Zombie-WS staleness watchdog (Phase-0 2026-07-02): the server answers the
        # app-level {"method":"ping"} with a pong push, so ANY live socket refreshes
        # _last_msg_ts at least once per ping_sec. 3 consecutive missed pongs
        # (3 x ping_sec) = derived dead threshold -> force reconnect.
        self._stale_after = 3.0 * self._ping_sec
        self._degraded = False

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
            except Exception as e:
                # A failed ping send = socket almost certainly dead. Never swallow
                # silently (zombie-WS: _connected stays True, store goes stale, every
                # scan quietly degrades to serial throttled REST) — force reconnect.
                log.warning("ws-candle ping send failed: %s — forcing reconnect", e)
                self._force_reconnect()
                continue
            # Staleness watchdog (Phase-0 2026-07-02): a half-open socket delivers
            # nothing but raises nothing either. Any live socket refreshes
            # _last_msg_ts at least once per ping (server pongs app-level pings),
            # so silence for _stale_after (3 x ping_sec) == dead -> reconnect, loud.
            if self._connected and self._last_msg_ts > 0:
                age = time.time() - self._last_msg_ts
                if age >= self._stale_after:
                    self._degraded = True
                    log.warning(
                        "ws-candle STALE: no WS message for %.0fs (>= %.0fs) — feed "
                        "degraded, scans fall back to throttled REST; forcing reconnect",
                        age, self._stale_after,
                    )
                    self._force_reconnect()

    def _force_reconnect(self):
        """Drop the current socket so _run()'s loop reconnects (~1s backoff)."""
        self._connected = False
        try:
            if self._ws:
                self._ws.close()
        except Exception as e:
            log.warning("ws-candle force-reconnect close failed: %s", e)

    def _on_open(self, ws):
        self._connected = True
        # Baseline for the staleness watchdog: a connection that never delivers a
        # single message (not even our ping's pong) must still trip it.
        self._last_msg_ts = time.time()
        n = 0
        for (c, i) in self._subs:
            try:
                ws.send(json.dumps({"method": "subscribe",
                                    "subscription": {"type": "candle", "coin": c, "interval": i}}))
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
        if self._degraded:
            self._degraded = False
            log.info("ws-candle RECOVERED: WS messages flowing again after stale period")
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
            if now_ms >= t_end:
                fin.add(t)
            if len(store) > self._store_bars * 2:
                keep = set(sorted(store)[-self._store_bars:])
                for old in [k for k in store if k not in keep]:
                    store.pop(old, None)
                    fin.discard(old)

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
                        fin.add(t)
                if len(store) > self._store_bars * 2:
                    keep = set(sorted(store)[-self._store_bars:])
                    for old in [k for k in store if k not in keep]:
                        store.pop(old, None)
                        fin.discard(old)
        except Exception as e:
            log.debug("ws-candle seed %s %s failed: %s", coin, interval, e)

    def get_df(self, coin, interval, limit):
        """Closed-bar window IDENTICAL to HLClient.candles() REST output, or None if the
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
            # SLOW-TF SELF-HEAL (2026-06-29): HL pushes 1d/8h (esp HIP-3) candles SPARSELY, so
            # the just-closed slow-TF bar is often absent from store at the boundary -> the old
            # `last_closed not in store -> None` forced a full per-coin REST refetch under the
            # 1.5s throttle (~191s 1d-walk on 260 coins). The slow bar OHLCV is DETERMINISTIC from
            # the finer 4h bars this feed serves promptly (6x4h==1d, 2x4h==8h; bit-exact vs REST
            # candles() verified 2026-06-29). Synthesize it; require ALL constituent 4h bars
            # present+final else fall through to REST (degraded-safe). Served bars == REST bars.
            synth = None
            if last_closed not in store and interval in _SLOW_TF_FROM_4H:
                synth = self._agg_from_4h(coin, last_closed, bar_ms)
            if last_closed not in store and synth is None:
                return None
            keys = sorted(store) if synth is None else sorted(set(store) | {last_closed})
            # Finality (mirror Extended ws_candle_feed): the just-closed bar is FINAL once
            # its full window has elapsed (now_ms >= last_closed + bar_ms == current_bar_start,
            # always true by construction of last_closed) OR a newer bar exists (roll-forward).
            # The time-based branch serves the just-closed bar at the boundary WITHOUT waiting
            # for the next forming-bar push (the old roll-forward-only path stalled the boundary
            # scan -> REST fallback -> serial; xyz/HIP-3 hit hardest, sparse next-bar pushes).
            # Forming bar (current_bar_start) is never served: closed[] keeps only t<=last_closed.
            confirmed = (now_ms >= last_closed + bar_ms) \
                or (fin is not None and last_closed in fin) or (keys[-1] > last_closed)
            if not confirmed:
                return None
            closed = [t for t in keys if t <= last_closed]
            if not closed or closed[-1] != last_closed:
                return None
            if len(closed) < min(limit, 100):
                return None
            bars = [(synth if (synth is not None and t == last_closed and t not in store)
                     else store[t]) for t in closed]
        df = pd.DataFrame(bars, columns=["t", "Open", "High", "Low", "Close", "Volume"])
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df[["time", "Open", "High", "Low", "Close", "Volume"]]
        if limit and len(df) > limit:
            df = df.iloc[-limit:]
        return df.reset_index(drop=True)

    def _agg_from_4h(self, coin, slow_open_ts, bar_ms):
        """Aggregate a just-closed 8h/1d bar from the finer 4h bars already in store (caller
        holds self._lock). Returns [t,o,h,l,c,v] or None if any constituent 4h bar is missing/
        non-final (-> caller REST-falls-back, degraded-safe). Bit-exact vs HL REST (2026-06-29)."""
        step = _TF_MS.get("4h")
        if not step or bar_ms % step != 0:
            return None
        n = bar_ms // step
        store4 = self._bars.get((coin, "4h"))
        fin4 = self._final.get((coin, "4h"))
        if not store4 or not fin4:
            return None
        ts = [slow_open_ts + k * step for k in range(n)]
        for t in ts:
            if t not in store4 or t not in fin4:
                return None
        b = [store4[t] for t in ts]
        return [slow_open_ts, b[0][1], max(x[2] for x in b), min(x[3] for x in b),
                b[-1][4], sum(x[5] for x in b)]

    def stats(self):
        with self._lock:
            pairs = len(self._bars)
            total = sum(len(v) for v in self._bars.values())
            finals = sum(len(v) for v in self._final.values())
        age = round(time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None
        return {"connected": self._connected, "subs": self._sub_count, "pairs": pairs,
                "bars": total, "final": finals, "last_msg_age": age,
                "degraded": self._degraded}
