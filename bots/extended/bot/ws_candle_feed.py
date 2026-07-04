"""Extended (x10 SDK) WS candle feed — push-based OHLC store (kills per-scan REST polling).

WHY (root, 2026-06-27, port of Pacifica ws_candle_feed): Extended's candle history is
per-market REST (exchange_extended.get_candles_history). Every scan loop fetches each
(coin,interval) serially -> entry-detect lag ~30s. The architecturally-correct fix is WS
PUSH: subscribe once per (coin,interval), the exchange streams the live bar, scans serve
the closed window from memory -> 0 REST fetch, 0 rate-limit, instant (~Pacifica ~1.5s).
Push < poll => peak request-rate FALLS -> rate-safe.

DESIGN (additive · env-gated EXTENDED_WS_CANDLE · REST-fallback = zero correctness risk):
  * Transport = x10 SDK PerpetualStreamClient.subscribe_to_candles (public, NO api_key ->
    public market-data stream, NOT the account/order stream). One asyncio loop in a bg
    thread; one subscription task per (market_name, interval), auto-reconnect w/ backoff.
  * Stream msg = WrappedStreamResponse{ data: List[CandleModel], ts, seq }. CandleModel:
    open/high/low/close/volume/timestamp(T=bar-START ms). The stream pushes the CURRENT
    (forming) bar repeatedly; T advances when a new bar opens.
  * store keyed by bar-start T. A bar T is CONFIRMED-FINAL iff (now_ms >= T + bar_ms,
    i.e. the bar's window has elapsed) OR a newer bar T' > T exists in the store.
  * get_df serves the closed-bar window (INCLUDING the forming bar's predecessors, mirror
    of REST candles() which returns ...,closed,forming and the scanner drops the forming
    row) ONLY when the latest CLOSED bar is confirmed-final; else None -> caller uses REST.
    NOTE: get_df returns rows up to AND INCLUDING last_closed only (no forming row) — the
    scanner's drop_forming_bars then leaves them untouched (already closed). Identical net
    window to the REST path post-drop.
  * seed(coin,iv,df): warm the store from the first REST df so cold-start history reuses
    the existing REST path (no extra burst). WS pushes never clobber a fresher value.
Any WS failure -> store goes stale -> get_df None -> REST. Flag off -> module never attached.

Market-name resolution: universe coins are bare ("BTC"); Extended markets are "BTC-USD".
The feed is given a coin->market_name map at construction (from client._market(coin).name)
and stores under the BARE coin so get_df/seed key identically to candles(coin,...).
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time

import pandas as pd

log = logging.getLogger(__name__)

# native intervals only (resample TFs like 8h/1w are derived by candles() from a native
# base TF, so subscribing to the base is sufficient). Maps bot-TF -> SDK ISO interval.
_ISO = {
    "1m": "PT1M", "5m": "PT5M", "15m": "PT15M", "30m": "PT30M",
    "1h": "PT1H", "2h": "PT2H", "4h": "PT4H", "1d": "P1D",
}
_TF_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000,
}


class WsCandleFeed:
    def __init__(self, coin_market_map, intervals, stream_url, store_bars: int = 320):
        # coin_market_map: {bare_coin: market_name}, e.g. {"BTC": "BTC-USD"}
        self._coin_market = dict(coin_market_map)
        self._intervals = [iv for iv in dict.fromkeys(intervals) if iv in _ISO]
        self._subs = [(c, mkt, i) for c, mkt in self._coin_market.items()
                      for i in self._intervals]
        self._stream_url = stream_url
        self._store_bars = int(store_bars)
        # (coin,iv) -> {t_ms: [t,o,h,l,c,v]}  and  (coin,iv) -> set(final t_ms)
        self._bars: dict[tuple, dict] = {}
        self._final: dict[tuple, set] = {}
        self._lock = threading.RLock()
        self._stop = False
        self._loop = None
        self._connected_subs = set()      # (coin,iv) with an open stream
        self._last_msg_ts = 0.0
        self._sub_count = 0

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self):
        threading.Thread(target=self._run_loop, name="ws-candle", daemon=True).start()

    def stop(self):
        self._stop = True
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except Exception:
                pass

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        for (coin, mkt, iv) in self._subs:
            loop.create_task(self._sub_task(coin, mkt, iv))
        try:
            loop.run_forever()
        except Exception as e:
            log.warning("ws-candle loop error: %s", e)

    async def _sub_task(self, coin, market_name, interval):
        from x10.perpetual.stream_client.stream_client import PerpetualStreamClient
        sc = PerpetualStreamClient(api_url=self._stream_url)
        iso = _ISO[interval]
        backoff = 1.0
        first = True
        while not self._stop:
            stream = None
            try:
                # public market-data stream (NO api_key) — candle_type="trades" mirrors REST
                stream = await sc.subscribe_to_candles(market_name, "trades", iso)
                if first:
                    self._connected_subs.add((coin, interval))
                    self._sub_count = len(self._connected_subs)
                    if self._sub_count == len(self._subs):
                        log.info("ws-candle connected, subscribed %d (coin,interval) pairs",
                                 self._sub_count)
                    first = False
                backoff = 1.0
                async for ev in stream:
                    if self._stop:
                        break
                    self._ingest(coin, interval, ev)
            except Exception as e:
                log.debug("ws-candle sub %s %s err: %s", coin, interval, e)
            finally:
                if stream is not None:
                    try:
                        await stream.close()
                    except Exception:
                        pass
            if self._stop:
                break
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2.0, 30.0)

    # ── ingest ───────────────────────────────────────────────────────────────
    def _ingest(self, coin, interval, ev):
        self._last_msg_ts = time.time()
        data = getattr(ev, "data", None)
        if not data:
            return
        bar_ms = _TF_MS[interval]
        key = (coin, interval)
        now_ms = time.time() * 1000.0
        with self._lock:
            store = self._bars.setdefault(key, {})
            fin = self._final.setdefault(key, set())
            for c in data:
                try:
                    t = int(c.timestamp)
                    bar = [t, float(c.open), float(c.high), float(c.low),
                           float(c.close), float(c.volume or 0.0)]
                except (AttributeError, ValueError, TypeError):
                    continue
                store[t] = bar
                # final iff the bar's full window has elapsed
                if now_ms >= t + bar_ms:
                    fin.add(t)
            # any bar strictly older than the newest stored bar is closed -> final
            if store:
                newest = max(store)
                for t in store:
                    if t < newest:
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
                        fin.add(t)                  # all but the last REST row are closed
                if len(store) > self._store_bars * 2:
                    keep = set(sorted(store)[-self._store_bars:])
                    for old in [k for k in store if k not in keep]:
                        store.pop(old, None)
                        fin.discard(old)
        except Exception as e:
            log.debug("ws-candle seed %s %s failed: %s", coin, interval, e)

    # ── read API ─────────────────────────────────────────────────────────────
    def get_df(self, coin, interval, limit):
        """Closed-bar window (rows up to & incl. last_closed) matching what candles()+
        drop_forming_bars yield, or None if the latest closed bar is not confirmed-final /
        store stale / too short -> REST fallback. Returns the FORMING bar too when present
        and confirmed, mirroring REST output shape (scanner drops forming downstream)."""
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
                return None                              # missing latest closed bar -> REST
            confirmed = (fin is not None and last_closed in fin) or (keys[-1] > last_closed)
            if not confirmed:
                return None                              # last closed bar not final -> REST
            # serve closed window + (optional) forming bar, mirror of REST candles() shape
            usable = [t for t in keys if t <= last_closed + bar_ms]  # incl forming if present
            closed = [t for t in usable if t <= last_closed]
            if not closed or closed[-1] != last_closed:
                return None
            if len(closed) < min(limit, 100):
                return None                              # window too short -> REST (will seed)
            bars = [store[t] for t in usable]
        df = pd.DataFrame(bars, columns=["t", "Open", "High", "Low", "Close", "Volume"])
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df[["time", "Open", "High", "Low", "Close", "Volume"]]
        # limit applies to the closed history (REST limit semantics); keep a little extra
        if limit and len(df) > limit + 1:
            df = df.iloc[-(limit + 1):]
        return df.reset_index(drop=True)

    def stats(self):
        with self._lock:
            pairs = len(self._bars)
            total = sum(len(v) for v in self._bars.values())
            finals = sum(len(v) for v in self._final.values())
        age = round(time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None
        return {"subs": self._sub_count, "pairs": pairs, "bars": total,
                "final": finals, "last_msg_age": age}
