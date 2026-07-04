"""Nado (Vertex-protocol) WS candle feed — push-based OHLC store (kills per-scan REST
indexer get_candlesticks polling, which serially scans the universe and adds ~30s entry lag).

WHY (root, ported 2026-06-27 from Pacifica reference mem:project_fleet_ws_push_rlfix):
the Vertex indexer's get_candlesticks is per-product HTTP; tuning NADO_FETCH_SLEEP_SEC /
retry/backoff only polishes polling. The architecturally-correct fix is WS PUSH — subscribe
once per (product_id, granularity) to the gateway, the exchange streams the forming bar, we
serve scans from memory: 0 indexer fetch, 0 rate-limit, instant. Push < poll => peak indexer
request-rate FALLS (rate-safe). This module is that layer for Nado/Vertex.

VENUE PROTOCOL (probed read-only 2026-06-27, mainnet gateway):
  * endpoint:  wss://gateway.prod.nado.xyz/v1/subscribe  (== <gateway_http>/subscribe, ws scheme)
  * subscribe: {"method":"subscribe","stream":{"type":"latest_candlestick",
                "product_id":<int>,"granularity":<seconds>},"id":N}
               valid stream variants incl. latest_candlestick (server-listed); granularity
               in SECONDS, identical to IndexerCandlesticksGranularity enum values.
  * push:      {"type":"latest_candlestick","timestamp":"<bar_start_sec>","product_id":2,
                "granularity":14400,"open_x18":"..","high_x18":"..","low_x18":"..",
                "close_x18":"..","volume":".."}
               -> SAME _x18 fixed-point + second-precision bar-start timestamp as the REST
               IndexerCandlestick rows. The stream pushes ONLY the latest (forming) bar and
               updates it intra-bar; it does NOT replay history.

TRANSPORT: uses the `websockets` asyncio lib (present in this venv; NOT websocket-client which
the Pacifica reference uses). A dedicated background thread owns a private asyncio loop; the
public API (start/stop/seed/get_df/stats) is sync + thread-safe, identical contract to the
Pacifica WsCandleFeed so the exchange/main wiring matches.

DESIGN (additive · env-gated NADO_WS_CANDLE · REST-fallback = zero correctness risk):
  * subscribe per (coin,interval) -> (product_id, granularity_sec). NATIVE intervals only;
    resampled TFs (30m,8h) are built by candles() from their native base (15m,4h), which
    itself hits this fast-path -> no WS subscription needed for resampled TFs.
  * push -> rolling store keyed by bar-start ms (= timestamp_sec*1000).
  * CONFIRMED-FINAL: Vertex sends no explicit close flag, only the forming bar. A bar at
    start t is final IFF a strictly-newer bar exists in the store (venue rolled forward).
    get_df serves the closed window ONLY when the latest closed bar (now//bar*bar - bar) is
    present AND keys[-1] > last_closed; else returns None -> caller uses the REST indexer path.
  * seed(coin,iv,df): warm the store from the first REST df so cold-start history reuses the
    existing REST path (no extra burst). WS pushes never clobber a seeded bar for the same
    bar-start except the forming bar (which is expected to move).
Any WS failure -> store goes stale -> get_df None -> REST. Flag off -> module never attached.

get_df output is the closed-bar window candles() returns on the native path (after its
forming-bar drop): columns time,Open,High,Low,Close,Volume; sorted; forming bar excluded.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time

import pandas as pd

log = logging.getLogger(__name__)

# gateway http -> ws subscribe endpoint (mainnet). Built from MAINNET_GATEWAY:
#   https://gateway.prod.nado.xyz/v1  ->  wss://gateway.prod.nado.xyz/v1/subscribe
WS_URL = "wss://gateway.prod.nado.xyz/v1/subscribe"

X18 = 10 ** 18

# native TF -> granularity seconds (mirrors exchange_nado._TF_TO_GRANULARITY enum values).
# Only NATIVE TFs; resampled TFs (30m,8h) are served by candles() from their base.
_TF_SEC = {
    "1m": 60, "5m": 300, "15m": 900, "1h": 3600,
    "2h": 7200, "4h": 14400, "1d": 86400, "1w": 604800,
}
_TF_MS = {k: v * 1000 for k, v in _TF_SEC.items()}


def _x18(s) -> float:
    if s is None:
        return 0.0
    try:
        return int(s) / X18
    except (TypeError, ValueError):
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0


class WsCandleFeed:
    def __init__(self, pid_map: dict, intervals, store_bars: int = 320, ping_sec: float = 20.0):
        # pid_map: {coin_symbol: product_id}. We only subscribe coins we can resolve a pid for.
        self._pid_map = dict(pid_map)
        self._coins = list(dict.fromkeys(pid_map.keys()))
        self._intervals = [iv for iv in dict.fromkeys(intervals) if iv in _TF_SEC]
        self._subs = []           # list of (coin, iv, pid, gran_sec)
        self._key_to_coin = {}    # (pid, gran_sec) -> (coin, iv)
        for c in self._coins:
            pid = self._pid_map.get(c)
            if pid is None:
                continue
            for iv in self._intervals:
                gran = _TF_SEC[iv]
                self._subs.append((c, iv, int(pid), gran))
                self._key_to_coin[(int(pid), gran)] = (c, iv)
        self._store_bars = int(store_bars)
        self._ping_sec = float(ping_sec)
        # (coin,iv) -> {t_ms: [t_ms,o,h,l,c,v]}
        self._bars: dict[tuple, dict] = {}
        self._lock = threading.RLock()
        self._stop = False
        self._connected = False
        self._last_msg_ts = 0.0
        self._sub_count = 0

    # -- lifecycle ----------------------------------------------------------
    def start(self):
        threading.Thread(target=self._thread_main, name="ws-candle", daemon=True).start()

    def stop(self):
        self._stop = True

    def _thread_main(self):
        try:
            asyncio.run(self._run())
        except Exception as e:
            log.warning("ws-candle thread exited: %s", e)

    async def _run(self):
        import websockets
        ssl_ctx = ssl.create_default_context()
        backoff = 1.0
        while not self._stop:
            try:
                async with websockets.connect(
                    WS_URL, ssl=ssl_ctx, open_timeout=15,
                    ping_interval=self._ping_sec, ping_timeout=10,
                    max_size=2 ** 22,
                ) as ws:
                    await self._on_open(ws)
                    backoff = 1.0
                    while not self._stop:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            continue  # ping_interval keeps the conn alive
                        self._on_message(msg)
            except Exception as e:
                log.warning("ws-candle connect/recv error: %s", e)
            self._connected = False
            if self._stop:
                break
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2.0, 30.0)

    async def _on_open(self, ws):
        n = 0
        for (c, iv, pid, gran) in self._subs:
            try:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "stream": {"type": "latest_candlestick",
                               "product_id": pid, "granularity": gran},
                    "id": n + 1,
                }))
                n += 1
            except Exception as e:
                log.warning("ws-candle subscribe %s %s failed: %s", c, iv, e)
        self._sub_count = n
        self._connected = True
        log.info("ws-candle connected, subscribed %d (coin,interval) pairs", n)

    def _on_message(self, msg):
        self._last_msg_ts = time.time()
        try:
            o = json.loads(msg)
        except Exception:
            return
        if o.get("type") != "latest_candlestick":
            return  # ignore subscribe acks / errors / other streams
        try:
            pid = int(o["product_id"])
            gran = int(o["granularity"])
            t = int(o["timestamp"]) * 1000
            bar = [t, _x18(o["open_x18"]), _x18(o["high_x18"]),
                   _x18(o["low_x18"]), _x18(o["close_x18"]), _x18(o.get("volume"))]
        except (KeyError, ValueError, TypeError):
            return
        ci = self._key_to_coin.get((pid, gran))
        if ci is None:
            return
        with self._lock:
            store = self._bars.setdefault(ci, {})
            store[t] = bar  # forming bar: overwrite with latest values for this bar-start
            if len(store) > self._store_bars * 2:
                keep = set(sorted(store)[-self._store_bars:])
                for old in [k for k in store if k not in keep]:
                    store.pop(old, None)

    # -- warm from REST -----------------------------------------------------
    def seed(self, coin, interval, df_full):
        """Warm store from a REST df (cols time,Open,High,Low,Close,Volume). Bars keyed by
        bar-start ms. Never clobber a WS-fresher bar; seed only fills gaps so cold-start
        history exists before WS has rolled enough bars."""
        if interval not in self._intervals:
            return
        key = (coin, interval)
        try:
            rows = []
            for _, r in df_full.iterrows():
                t = int(pd.Timestamp(r["time"]).value // 10 ** 6)
                rows.append((t, [t, float(r["Open"]), float(r["High"]), float(r["Low"]),
                                 float(r["Close"]), float(r["Volume"])]))
            if not rows:
                return
            with self._lock:
                store = self._bars.setdefault(key, {})
                for t, bar in rows:
                    store.setdefault(t, bar)  # never clobber a WS-fresher bar
                if len(store) > self._store_bars * 2:
                    keep = set(sorted(store)[-self._store_bars:])
                    for old in [k for k in store if k not in keep]:
                        store.pop(old, None)
        except Exception as e:
            log.debug("ws-candle seed %s %s failed: %s", coin, interval, e)

    # -- read API -----------------------------------------------------------
    def get_df(self, coin, interval, limit):
        """Closed-bar window IDENTICAL to NadoClient.candles() native-path output, or None if
        the latest closed bar isn't confirmed-final / store stale / too short -> REST fallback.

        Confirmed-final (TIME-BASED OR roll-forward, mirror of Extended ws_candle_feed,
        2026-06-27): the bar at last_closed is trustworthy iff EITHER its full window has
        elapsed (now_ms >= last_closed + bar_ms — true by construction of last_closed, i.e.
        the just-closed bar is final the instant the boundary passes, NO wait for the next
        forming-bar push) OR a strictly-newer bar already exists in the store (venue rolled
        forward). The prior roll-forward-ONLY gate stalled the just-closed bar at the boundary
        scan (t+2s): the next forming push hadn't arrived, so get_df returned None and every
        coin fell through to the serial REST indexer (~24s/4h, ~95s/8h). The forming bar
        (current window, start = last_closed + bar_ms) is NEVER served — get_df only emits
        rows with t <= last_closed (the `closed` filter below)."""
        bar_ms = _TF_MS.get(interval)
        if bar_ms is None:
            return None  # resampled/unknown TF -> not served here
        key = (coin, interval)
        now_ms = int(time.time() * 1000)
        last_closed = (now_ms // bar_ms) * bar_ms - bar_ms
        with self._lock:
            store = self._bars.get(key)
            if not store:
                return None
            keys = sorted(store)
            if last_closed not in store:
                return None  # missing the latest closed bar -> REST
            # TIME-BASED OR roll-forward finality (mirror Extended): just-closed bar's window
            # has elapsed by construction (now_ms >= last_closed + bar_ms), so it is final
            # immediately at the boundary even before the next forming-bar push; roll-forward
            # (keys[-1] > last_closed) is kept as the OR-branch.
            confirmed = (now_ms >= last_closed + bar_ms) or (keys[-1] > last_closed)
            if not confirmed:
                return None  # last closed bar not yet final -> REST
            closed = [t for t in keys if t <= last_closed]
            if not closed or closed[-1] != last_closed:
                return None
            if len(closed) < min(limit, 100):
                return None  # window too short -> REST (will seed)
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
        age = round(time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None
        return {"connected": self._connected, "subs": self._sub_count,
                "pairs": pairs, "bars": total, "last_msg_age": age}
