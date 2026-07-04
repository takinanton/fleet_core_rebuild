"""Pacifica perpetual DEX adapter — mirror of ExtendedClient interface.

Solana-based perps; agent-wallet signing:
  - PACIFICA_AGENT_PRIVATE_KEY (or PACIFICA_PRIVATE_KEY) signs requests
  - PACIFICA_ACCOUNT_ADDRESS is main wallet pubkey (payload `account` field)

Signing flow (verbatim from legacy adapter — DO NOT change without protocol audit):
  1. timestamp = now_ms, expiry_window = 5000
  2. header = {timestamp, expiry_window, type: <op_name>}
  3. message = json.dumps(sort_keys({**header, "data": payload}), separators=(",", ":"))
  4. signature = base58(ed25519_sign(message_utf8, agent_keypair))
  5. body = {account, agent_wallet, signature, timestamp, expiry_window, **payload}

REST: https://api.pacifica.fi/api/v1
WS:   wss://ws.pacifica.fi/ws  (not yet wired — REST polling only, matches Extended)

Public-method parity with ExtendedClient (duck-typed callers in trader/scanner/
liquidity_snapshot expect identical signatures + return shapes — HL-style
response wrappers with `response.data.statuses`).

DRY_RUN env: when DRY_RUN=1, write-side methods (market_open/market_close/
trigger_sl/trigger_tp/update_leverage/cancel_sl_order) log + return a mock
success response without hitting REST. Read methods stay live so we can audit
universe / mark / candles paths against the live API.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests

from bot.config import Settings, TF_MS

log = logging.getLogger(__name__)

REST_URL = "https://api.pacifica.fi/api/v1"
WS_URL = "wss://ws.pacifica.fi/ws"


# ---------------------------------------------------------------------------
# AssetMeta — shape parity with ExtendedClient.AssetMeta
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AssetMeta:
    name: str
    sz_decimals: int
    max_leverage: int
    min_size: float = 0.0
    tick_size: float = 0.0
    lot_size: float = 0.0
    min_notional_usd: float = 10.0  # Pacifica enforces $10 floor (user spec)


# ---------------------------------------------------------------------------
# Signing helpers (VERBATIM from legacy /tmp/pacifica_legacy/bot/exchange_pacifica.py)
# ---------------------------------------------------------------------------
def _sort_keys(value):
    if isinstance(value, dict):
        return {k: _sort_keys(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_sort_keys(v) for v in value]
    return value


def _prepare_message(header: dict, payload: dict) -> str:
    """Match SDK: sort all nested keys, compact JSON encode."""
    if not all(k in header for k in ("type", "timestamp", "expiry_window")):
        raise ValueError("header missing type/timestamp/expiry_window")
    data = {**header, "data": payload}
    return json.dumps(_sort_keys(data), separators=(",", ":"))


def _sign(header: dict, payload: dict, keypair) -> tuple[str, str]:
    """Returns (message_str, base58_signature)."""
    import base58
    message = _prepare_message(header, payload)
    signature = keypair.sign_message(message.encode("utf-8"))
    return message, base58.b58encode(bytes(signature)).decode("ascii")


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class PacificaClient:
    """Pacifica perp DEX client — mirror of ExtendedClient public interface."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.dry_run = bool(int(os.getenv("DRY_RUN", "0") or "0"))

        # Lazy import solders — keeps non-Pacifica environments lightweight.
        try:
            from solders.keypair import Keypair  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                f"solders not installed (pip install solders base58 requests). {e}"
            ) from e

        agent_priv = (
            getattr(settings, "pacifica_agent_private_key", "")
            or getattr(settings, "pacifica_private_key", "")
        )
        if not agent_priv:
            raise RuntimeError(
                "PACIFICA_AGENT_PRIVATE_KEY (or PACIFICA_PRIVATE_KEY) not set in env"
            )
        self.agent_keypair = Keypair.from_base58_string(agent_priv)
        self.agent_pubkey = str(self.agent_keypair.pubkey())
        self.main_account = (
            getattr(settings, "pacifica_account_address", "") or self.agent_pubkey
        )

        # HTTP session — warm TCP/TLS for lower latency on hot path
        self.session = requests.Session()

        # Caches (mirror Extended): markets, candles, mark, funding, fees, positions
        self._cache_lock = threading.RLock()
        self._meta_cache: dict[str, AssetMeta] | None = None
        self._candles_cache: dict[tuple[str, str], tuple[int, pd.DataFrame, float]] = {}
        self._mark_cache: dict[str, tuple[float, float]] = {}
        self._funding_cache: dict[str, tuple[float, float]] = {}
        self._prices_cache: tuple[float, list] | None = None
        self._positions_cache: tuple[dict, float] | None = None
        self._fills_cache: tuple[float, list] | None = None

        # Rate-limit gate for /kline (Pacifica returns 429 above ~10 req/s)
        self._kline_lock = threading.Lock()
        self._kline_last_call_ms: float = 0.0
        self._kline_min_interval_ms: float = float(
            os.getenv("PACIFICA_KLINE_MIN_INTERVAL_MS", "150")
        )

        # Bootstrap markets cache
        self._load_meta()

        log.info(
            "Pacifica client init: dry_run=%s account=%s...%s agent=%s...%s markets=%d",
            self.dry_run,
            self.main_account[:6], self.main_account[-4:],
            self.agent_pubkey[:6], self.agent_pubkey[-4:],
            len(self._meta_cache or {}),
        )

    # ===================================================================
    # Internal helpers
    # ===================================================================
    def _load_meta(self) -> None:
        """Load Pacifica markets info (perp list with leverage/tick/lot)."""
        try:
            r = self.session.get(f"{REST_URL}/info", timeout=15)
            r.raise_for_status()
            data = r.json().get("data") or []
        except Exception as e:
            log.warning("Pacifica /info load failed: %s — meta cache empty", e)
            self._meta_cache = {}
            return

        out: dict[str, AssetMeta] = {}
        for m in data:
            try:
                lot_raw = str(m["lot_size"])
                # sz_decimals from lot_size string ("0.001" → 3)
                if "." in lot_raw:
                    sz_decimals = max(0, len(lot_raw.split(".")[-1].rstrip("0")))
                else:
                    sz_decimals = 0
                out[m["symbol"]] = AssetMeta(
                    name=m["symbol"],
                    sz_decimals=sz_decimals,
                    max_leverage=int(m["max_leverage"]),
                    min_size=float(m.get("min_order_size") or 0),
                    tick_size=float(m["tick_size"]),
                    lot_size=float(lot_raw),
                    # Pacifica enforces $10 notional floor (user spec 2026-05-25).
                    # No per-market override exposed; honour global.
                    min_notional_usd=float(m.get("min_notional") or 10.0),
                )
            except Exception as e:
                log.warning("meta parse failed for %s: %s", m.get("symbol"), e)
        self._meta_cache = out
        log.info("Pacifica meta loaded: %d perp markets", len(out))

    def _signed_request(
        self,
        method: str,
        path: str,
        op_type: str,
        payload: dict,
        timeout: float = 15.0,
    ) -> dict:
        """Sign payload with agent keypair, POST/GET to REST_URL+path.

        In DRY_RUN: returns mock success dict for write ops, but read ops still
        go through (so account_value/positions return real state).
        """
        if self.dry_run and method.upper() == "POST":
            log.info("[DRY] %s %s op=%s payload=%s",
                     method, path, op_type, json.dumps(payload)[:200])
            return {
                "success": True,
                "data": {"order_id": f"dry-{uuid.uuid4().hex[:10]}",
                         "avg_price": payload.get("amount") and 0,
                         "filled": payload.get("amount"),
                         "dry_run": True},
            }
        timestamp = int(time.time() * 1000)
        header = {"timestamp": timestamp, "expiry_window": 5000, "type": op_type}
        try:
            _, signature = _sign(header, payload, self.agent_keypair)
        except Exception as e:
            return {"success": False, "error": f"signing failed: {e}"}
        body = {
            "account": self.main_account,
            "agent_wallet": self.agent_pubkey,
            "signature": signature,
            "timestamp": timestamp,
            "expiry_window": 5000,
            **payload,
        }
        url = REST_URL + path
        # 429 retry: up to 2 backoffs honoring Retry-After header (max ~120s total)
        for attempt in range(3):
            try:
                if method.upper() == "POST":
                    r = self.session.post(
                        url, json=body, timeout=timeout,
                        headers={"Content-Type": "application/json"},
                    )
                else:
                    r = self.session.get(url, params=body, timeout=timeout)
            except requests.exceptions.RequestException as e:
                log.warning("signed_request %s network error: %s", path, e)
                return {"success": False, "error": str(e)}
            if r.status_code != 429:
                break
            wait_s = float(r.headers.get("Retry-After", 0) or 0)
            if wait_s <= 0 or wait_s > 60:
                wait_s = min(2 ** attempt, 30)  # exponential cap 30s
            log.warning("signed_request %s 429 — sleeping %.1fs (attempt %d/3)",
                        path, wait_s, attempt + 1)
            time.sleep(wait_s)
        try:
            return r.json()
        except Exception:
            return {"success": False, "error": f"http {r.status_code}: {r.text[:200]}"}

    @staticmethod
    def _to_filled_resp(avg_px: float, filled: float, oid) -> dict:
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"filled": {
                "avgPx": str(avg_px), "totalSz": str(filled), "oid": str(oid),
            }}],
        }}}

    @staticmethod
    def _to_resting_resp(oid) -> dict:
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"resting": {"oid": str(oid)}}],
        }}}

    @staticmethod
    def _to_error_resp(msg: str) -> dict:
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"error": str(msg)[:300]}],
        }}}

    def _market(self, coin: str) -> AssetMeta:
        """Mirror of ExtendedClient._market — returns AssetMeta or raises KeyError.

        Pacifica symbols are bare ("BTC", "SOL"), no "-USD" suffix — but accept
        suffix form for callers that pass either.
        """
        if not self._meta_cache:
            self._load_meta()
        meta = (self._meta_cache or {}).get(coin)
        if not meta and coin.endswith("-USD"):
            meta = (self._meta_cache or {}).get(coin[:-4])
        if not meta:
            raise KeyError(f"Pacifica market not found: {coin}")
        return meta

    # ===================================================================
    # Account state
    # ===================================================================
    def account_value(self) -> float:
        """Total account equity in USDC. Always live (read-only)."""
        resp = self._signed_request("GET", "/account", "get_account", {})
        if not resp.get("success"):
            log.warning("account_value failed: %s", resp.get("error", resp))
            return 0.0
        d = resp.get("data") or {}
        for k in ("equity", "total_equity", "balance"):
            if k in d:
                try:
                    return float(d[k])
                except (TypeError, ValueError):
                    continue
        log.warning("account_value: unexpected response shape: %s", d)
        return 0.0

    def open_positions(self) -> dict:
        """HL-shape positions dict. 5s TTL cache (mirror Extended)."""
        with self._cache_lock:
            if self._positions_cache:
                pos, t = self._positions_cache
                if time.time() - t < 5.0:
                    return pos

        resp = self._signed_request("GET", "/positions", "get_positions", {})
        if not resp.get("success"):
            log.warning("open_positions failed: %s", resp.get("error", resp))
            return {}

        out: dict = {}
        for p in resp.get("data") or []:
            sym = p.get("symbol")
            if not sym:
                continue
            try:
                amount = float(p.get("amount", 0) or 0)
            except (TypeError, ValueError):
                amount = 0.0
            side_raw = (p.get("side") or "").lower()
            # Pacifica side encoding: bid/long → +size; ask/short → -size
            # (root v2 fix from legacy — orderbook-side terms, NOT direction strings).
            if side_raw in ("bid", "long", "buy"):
                sz_signed = amount
            elif side_raw in ("ask", "short", "sell"):
                sz_signed = -amount
            else:
                sz_signed = amount  # preserve sign if exchange returns signed
            try:
                lev_val = int(p.get("leverage", 1) or 1)
            except (TypeError, ValueError):
                lev_val = 1
            entry = {
                "coin": sym,
                "szi": str(sz_signed),
                "entryPx": str(p.get("entry_price", p.get("entry", 0)) or 0),
                "leverage": {"value": lev_val, "type": "cross"},
                "marginMode": "cross",
                "liquidationPx": str(p.get("liquidation_price") or "") or None,
                "marginUsed": str(p.get("margin_used", p.get("value", 0)) or 0),
                "unrealizedPnl": str(p.get("unrealized_pnl", 0) or 0),
            }
            out[sym] = entry
        with self._cache_lock:
            self._positions_cache = (out, time.time())
        return out

    def invalidate_positions_cache(self) -> None:
        with self._cache_lock:
            self._positions_cache = None

    def spot_usdc(self) -> float:
        """Spot USDC balance — Pacifica unified margin (perp + USDC same account).

        Returns account_value() because Pacifica doesn't separate spot/perp wallets.
        """
        return self.account_value()

    # ===================================================================
    # Market data
    # ===================================================================
    def _prices(self, ttl: float = 5.0) -> list:
        """Pacifica /info/prices snapshot (mark, OI, funding, vol_24h per symbol)."""
        now = time.time()
        if self._prices_cache and (now - self._prices_cache[0]) < ttl:
            return self._prices_cache[1]
        try:
            r = self.session.get(f"{REST_URL}/info/prices", timeout=10)
            r.raise_for_status()
            data = r.json().get("data") or []
            self._prices_cache = (now, data)
            return data
        except Exception as e:
            log.warning("prices fetch failed: %s", e)
            return self._prices_cache[1] if self._prices_cache else []

    @staticmethod
    def _cache_offset_ms(coin: str, interval: str, bar_ms: int) -> int:
        """Per-(coin,interval) deterministic offset — anti-thundering-herd."""
        h = hashlib.md5(f"{coin}:{interval}".encode()).digest()
        raw = int.from_bytes(h[:4], "big")
        cap_ms = min(30_000, bar_ms // 2)
        return raw % max(1, cap_ms) if cap_ms > 0 else 0

    def candles(self, coin: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """OHLCV — bar-aligned cache + 429 backoff (mirror Extended/HL/KF pattern).

        Pacifica /kline returns: list of {t, o, h, l, c, v, n}.
        """
        bar_ms = TF_MS.get(interval)
        if bar_ms is None:
            log.warning("Pacifica unsupported interval: %s", interval)
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        # Early reject if coin not in markets (avoid wasted API call)
        try:
            self._market(coin)
        except KeyError:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        now_ms = int(time.time() * 1000)
        offset_ms = self._cache_offset_ms(coin, interval, bar_ms)
        current_bar_start = ((now_ms - offset_ms) // bar_ms) * bar_ms
        cache_key = (coin, interval)

        with self._cache_lock:
            cached = self._candles_cache.get(cache_key)
        if cached is not None and cached[0] >= current_bar_start:
            return cached[1].copy()
        # min-TTL re-fetch gate (prevents top-of-hour burst)
        if cached is not None:
            min_ttl = float(os.getenv("CANDLES_MIN_TTL_SEC", "30"))
            if min_ttl > 0 and (now_ms / 1000.0 - cached[2]) < min_ttl:
                return cached[1].copy()

        end = now_ms
        start = end - bar_ms * (limit + 2)

        # Throttle /kline (legacy gate)
        with self._kline_lock:
            now_t = time.time() * 1000.0
            wait_ms = self._kline_min_interval_ms - (now_t - self._kline_last_call_ms)
            if wait_ms > 0:
                time.sleep(wait_ms / 1000.0)
            self._kline_last_call_ms = time.time() * 1000.0

        params = {"symbol": coin, "interval": interval,
                  "start_time": start, "end_time": end}
        try:
            r = self.session.get(f"{REST_URL}/kline", params=params, timeout=15)
            if r.status_code == 429:
                rl = r.headers.get("ratelimit", "")
                wait_s = 5.0
                try:
                    for part in rl.split(";"):
                        if part.strip().startswith("t="):
                            wait_s = max(2.0, float(part.split("=")[1].strip().rstrip(","))) + 1.0
                            break
                except Exception:
                    pass
                log.info("Pacifica 429 — sleeping %.1fs before retry", wait_s)
                time.sleep(wait_s)
                r = self.session.get(f"{REST_URL}/kline", params=params, timeout=15)
            r.raise_for_status()
            data = r.json().get("data") or []
        except Exception as e:
            log.warning("Pacifica /kline %s %s: %s", coin, interval, e)
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        if not data:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
        for col in ("Open", "High", "Low", "Close", "Volume"):
            df[col] = df[col].astype(float)
        df = df[["time", "Open", "High", "Low", "Close", "Volume"]].sort_values("time").reset_index(drop=True)

        last_bar_t_ms = current_bar_start
        try:
            last_bar_t_ms = int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        except Exception:
            pass
        with self._cache_lock:
            self._candles_cache[cache_key] = (last_bar_t_ms, df, time.time())
        return df

    def funding_rate(self, coin: str) -> float:
        """60s TTL cache, source = /info/prices."""
        with self._cache_lock:
            cached = self._funding_cache.get(coin)
            if cached and time.time() - cached[1] < 60:
                return cached[0]
        rate = 0.0
        for m in self._prices(ttl=60):
            if m.get("symbol") == coin:
                try:
                    rate = float(m.get("funding", 0) or 0)
                except (TypeError, ValueError):
                    rate = 0.0
                break
        with self._cache_lock:
            self._funding_cache[coin] = (rate, time.time())
        return rate

    def asset(self, coin: str) -> AssetMeta:
        """Pacifica market meta — frozen dataclass (parity with Extended)."""
        return self._market(coin)

    def mark_price(self, coin: str, ttl: float = 5.0) -> float:
        with self._cache_lock:
            cached = self._mark_cache.get(coin)
            if cached and time.time() - cached[1] < ttl:
                return cached[0]
        px = 0.0
        for m in self._prices(ttl=ttl):
            if m.get("symbol") == coin:
                try:
                    px = float(m.get("mark", 0) or 0)
                except (TypeError, ValueError):
                    px = 0.0
                break
        with self._cache_lock:
            self._mark_cache[coin] = (px, time.time())
        return px

    def slip_per_side(self, coin: str, notional_usd: float) -> float:
        """Static tiered slip estimate (parity with legacy). Refine via fills later."""
        del notional_usd
        majors = {"BTC", "ETH", "SOL"}
        mids = {"XRP", "DOGE", "WIF", "PUMP", "PEPE", "BONK", "BNB", "AVAX",
                "ADA", "LINK", "UNI"}
        if coin in majors:
            return 0.0008
        if coin in mids:
            return 0.0020
        return 0.0050

    def round_price(self, coin: str, px: float) -> float:
        """Round price to market tick_size."""
        try:
            meta = self._market(coin)
        except KeyError:
            return px
        if meta.tick_size <= 0:
            return px
        return round(round(px / meta.tick_size) * meta.tick_size, 12)

    def round_qty(self, coin: str, sz: float) -> float:
        """Round size to lot_size (floor — never over-order)."""
        try:
            meta = self._market(coin)
        except KeyError:
            return sz
        if meta.lot_size > 0:
            sz = (int(sz / meta.lot_size)) * meta.lot_size
        return round(sz, meta.sz_decimals)

    # ===================================================================
    # Orderbook (public method — replaces ExtendedClient._client.markets_info.
    # get_orderbook_snapshot reach-through in liquidity_snapshot.py)
    # ===================================================================
    def orderbook_snapshot(self, coin: str) -> tuple[list, list]:
        """Top-of-book + depth — returns (bids, asks) each as [(px, qty), ...].

        Pacifica: GET /book?symbol=<sym> (no agg_level — that param triggers 500).
        Response: data.s = symbol, data.l = [bids_array, asks_array]
                  each level = {"p": price_str, "a": amount_str, "n": int}
        Caller (liquidity_snapshot) computes spread + depth_top20 from this.
        """
        try:
            r = self.session.get(
                f"{REST_URL}/book",
                params={"symbol": coin},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json().get("data") or {}
        except Exception as e:
            log.warning("orderbook %s fetch failed: %s", coin, e)
            return [], []

        def _parse(levels):
            out = []
            for lvl in levels or []:
                try:
                    if isinstance(lvl, dict):
                        px = float(lvl.get("p") or lvl.get("price"))
                        qty = float(lvl.get("a") or lvl.get("amount") or lvl.get("q"))
                    else:
                        px = float(lvl[0])
                        qty = float(lvl[1])
                    if px > 0 and qty > 0:
                        out.append((px, qty))
                except Exception:
                    continue
            return out

        # New format: data.l = [bids, asks]; legacy fallback to b/a or bids/asks keys
        l_arr = data.get("l")
        if isinstance(l_arr, list) and len(l_arr) >= 2:
            bids = _parse(l_arr[0])
            asks = _parse(l_arr[1])
        else:
            bids = _parse(data.get("b") or data.get("bids"))
            asks = _parse(data.get("a") or data.get("asks"))
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        return bids, asks

    # ===================================================================
    # Order placement
    # ===================================================================
    def _poll_order_fill(self, oid, client_oid: str, timeout: float = 5.0) -> dict | None:
        """Poll /orders/history for terminal status post-place (legacy logic).

        Returns: {"status":"filled", "avg_px":float, "filled":float, "oid":int}
                 {"status":<terminal>, "reason":str}
                 None on timeout
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(0.2)
            try:
                r = self.session.get(
                    f"{REST_URL}/orders/history",
                    params={"account": self.main_account, "limit": 20},
                    timeout=5,
                )
                data = (r.json() or {}).get("data") or []
            except Exception as e:
                log.warning("orders/history poll failed: %s", e)
                continue
            for o in data:
                if o.get("order_id") != oid and o.get("client_order_id") != client_oid:
                    continue
                st = o.get("order_status")
                try:
                    fa = float(o.get("filled_amount") or 0)
                except (TypeError, ValueError):
                    fa = 0.0
                try:
                    ap = float(o.get("average_filled_price") or 0)
                except (TypeError, ValueError):
                    ap = 0.0
                if st == "filled" or (fa > 0 and ap > 0):
                    return {"status": "filled", "avg_px": ap, "filled": fa, "oid": oid}
                if st in ("cancelled", "rejected", "expired", "partially_cancelled"):
                    return {"status": st, "reason": o.get("reason") or st}
        return None

    def market_open(self, coin: str, is_buy: bool, sz: float) -> dict:
        """Market entry via /orders/create_market.

        Slippage_percent is sent in % terms (legacy: settings.slippage = 0.0025 →
        Pacifica expects "0.25" string for 0.25%).
        """
        try:
            meta = self._market(coin)
        except KeyError as e:
            return self._to_error_resp(str(e))
        sz = self.round_qty(coin, sz)
        if sz <= 0:
            return self._to_error_resp(f"size <= 0 after lot rounding (coin={coin})")

        # Min-notional gate (Pacifica $10 floor)
        mark = self.mark_price(coin) or 0
        if mark > 0 and sz * mark < meta.min_notional_usd:
            return self._to_error_resp(
                f"notional ${sz * mark:.2f} < ${meta.min_notional_usd:.2f} floor"
            )

        slip_pct = float(self.settings.slippage or 0.0025) * 100.0  # 0.0025 → 0.25
        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "reduce_only": False,
            "amount": str(sz),
            "side": "bid" if is_buy else "ask",
            "slippage_percent": f"{slip_pct:.2f}",  # Pacifica max 2 decimal places
            "client_order_id": client_oid,
        }
        log.info("market_open %s side=%s sz=%s slip=%.4f%%",
                 coin, "buy" if is_buy else "sell", sz, slip_pct)

        resp = self._signed_request(
            "POST", "/orders/create_market", "create_market_order", payload,
        )
        if not resp.get("success"):
            return self._to_error_resp(resp.get("error") or str(resp))

        d = resp.get("data") or {}
        oid_raw = d.get("order_id")
        # DRY shortcut: legacy adapter returned a mock fill — skip poll
        if d.get("dry_run"):
            return self._to_filled_resp(
                mark or 0, sz, oid_raw or f"dry-{uuid.uuid4().hex[:8]}",
            )
        try:
            oid = int(oid_raw) if oid_raw is not None else None
        except (TypeError, ValueError):
            oid = None
        if oid is None:
            return self._to_error_resp(f"create_market success but no order_id: {d!r}")

        poll = self._poll_order_fill(oid, client_oid, timeout=5.0)
        if poll is None:
            return self._to_error_resp(
                f"poll timeout for order {oid} (ack but no terminal status in 5s)"
            )
        if poll["status"] == "filled":
            return self._to_filled_resp(poll["avg_px"], poll["filled"], oid)
        return self._to_error_resp(
            f"order {poll['status']} (oid {oid}): {poll.get('reason')}"
        )

    def market_close(self, coin: str) -> dict:
        """Reduce-only close — derives size+direction from live exchange position.

        Single-arg signature matches ExtendedClient.market_close(coin).
        """
        try:
            positions = self.open_positions()
        except Exception as e:
            return self._to_error_resp(f"positions fetch failed: {e}")
        pos = positions.get(coin)
        if not pos:
            log.warning("market_close: no position for %s", coin)
            return {}
        try:
            sz_signed = float(pos.get("szi", 0))
        except (TypeError, ValueError):
            sz_signed = 0.0
        if sz_signed == 0:
            return {}

        sz = abs(sz_signed)
        is_buy_to_close = sz_signed < 0  # closing short = BUY; closing long = SELL
        sz = self.round_qty(coin, sz)
        if sz <= 0:
            return self._to_error_resp("size <= 0 after rounding")

        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "reduce_only": True,
            "amount": str(sz),
            "side": "bid" if is_buy_to_close else "ask",
            "slippage_percent": "1.0",  # wider on close to ensure fill
            "client_order_id": client_oid,
        }
        log.info("market_close %s sz=%s is_buy_to_close=%s",
                 coin, sz, is_buy_to_close)

        resp = self._signed_request(
            "POST", "/orders/create_market", "create_market_order", payload,
        )
        if not resp.get("success"):
            return self._to_error_resp(resp.get("error") or str(resp))
        d = resp.get("data") or {}
        oid_raw = d.get("order_id")
        if d.get("dry_run"):
            return self._to_filled_resp(self.mark_price(coin) or 0, sz, oid_raw or "dry")
        try:
            oid = int(oid_raw) if oid_raw is not None else None
        except (TypeError, ValueError):
            oid = None
        if oid is None:
            return self._to_error_resp(f"close success but no order_id: {d!r}")

        # Bust positions cache so manage_open_position sees the close on next tick
        self.invalidate_positions_cache()

        poll = self._poll_order_fill(oid, client_oid, timeout=5.0)
        if poll is None:
            return self._to_error_resp(f"close poll timeout for order {oid}")
        if poll["status"] == "filled":
            return self._to_filled_resp(poll["avg_px"], poll["filled"], oid)
        return self._to_error_resp(
            f"close order {poll['status']} (oid {oid}): {poll.get('reason')}"
        )

    def trigger_sl(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Place reduce-only stop-market order.

        is_buy = CLOSE side (True for closing short, False for closing long).
        Endpoint: POST /orders/stop/create (op_type=create_stop_order).
        Returns HL-shape `resting` response.
        """
        try:
            meta = self._market(coin)
        except KeyError as e:
            return self._to_error_resp(str(e))
        sz = self.round_qty(coin, sz)
        if sz <= 0:
            return self._to_error_resp("size <= 0 after rounding")
        trigger_px = self.round_price(coin, trigger_px)

        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "side": "bid" if is_buy else "ask",
            "reduce_only": True,
            "stop_order": {
                "stop_price": str(trigger_px),
                "client_order_id": client_oid,
                "trigger_price_type": "last_trade_price",
                "amount": str(sz),
            },
        }
        log.info("trigger_sl %s side=%s sz=%s trigger=%s",
                 coin, "buy" if is_buy else "sell", sz, trigger_px)

        resp = self._signed_request(
            "POST", "/orders/stop/create", "create_stop_order", payload,
        )
        if not resp.get("success"):
            return self._to_error_resp(resp.get("error") or str(resp))
        d = resp.get("data") or {}
        oid = d.get("order_id") or d.get("id")
        if oid is None:
            return self._to_error_resp(f"create_stop_order success but no order_id: {d!r}")
        return self._to_resting_resp(oid)

    def trigger_tp(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Place reduce-only take-profit stop-market order.

        Pacifica /orders/stop/create supports trigger_price_type=last_trade_price
        for both SL and TP. To distinguish TP from SL semantically we just place
        on the take-profit side; Pacifica does not have a separate TP endpoint.
        Same signature as trigger_sl.
        """
        try:
            meta = self._market(coin)
        except KeyError as e:
            return self._to_error_resp(str(e))
        sz = self.round_qty(coin, sz)
        if sz <= 0:
            return self._to_error_resp("size <= 0 after rounding")
        trigger_px = self.round_price(coin, trigger_px)

        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "side": "bid" if is_buy else "ask",
            "reduce_only": True,
            "stop_order": {
                "stop_price": str(trigger_px),
                "client_order_id": client_oid,
                "trigger_price_type": "last_trade_price",
                "amount": str(sz),
            },
        }
        log.info("trigger_tp %s side=%s sz=%s trigger=%s",
                 coin, "buy" if is_buy else "sell", sz, trigger_px)

        resp = self._signed_request(
            "POST", "/orders/stop/create", "create_stop_order", payload,
        )
        if not resp.get("success"):
            return self._to_error_resp(resp.get("error") or str(resp))
        d = resp.get("data") or {}
        oid = d.get("order_id") or d.get("id")
        if oid is None:
            return self._to_error_resp(f"create_tp success but no order_id: {d!r}")
        return self._to_resting_resp(oid)

    def cancel_sl_order(self, coin: str, oid) -> dict:
        """Cancel a stop order. Tries stop endpoint first; falls back to regular."""
        try:
            oid_int = int(oid)
        except (TypeError, ValueError):
            oid_int = oid
        payload = {"symbol": coin, "order_id": oid_int}
        resp = self._signed_request(
            "POST", "/orders/stop/cancel", "cancel_stop_order", payload,
        )
        if not resp.get("success"):
            err = (resp.get("error") or "").lower()
            if "not found" in err or "deserialize" in err:
                # Fallback — maybe it was a regular limit order
                resp = self._signed_request(
                    "POST", "/orders/cancel", "cancel_order", payload,
                )
        if resp.get("success"):
            return {"status": "ok"}
        return {"status": "error", "error": str(resp.get("error", resp))[:200]}

    def list_open_sl_orders(self, coin: str) -> list:
        """Return list of open reduce-only stop-order ids for coin."""
        resp = self._signed_request("GET", "/orders", "get_orders", {})
        if not resp.get("success"):
            return []
        out = []
        for o in resp.get("data") or []:
            if o.get("symbol") != coin:
                continue
            if not o.get("reduce_only"):
                continue
            ot = (o.get("order_type") or "").lower()
            if "stop" not in ot:
                continue
            oid = o.get("order_id") or o.get("id")
            if oid is not None:
                out.append(str(oid))
        return out

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> Optional[dict]:
        """Update leverage for `coin`. Margin mode separate call (legacy)."""
        try:
            meta = self._market(coin)
            max_lev = meta.max_leverage
        except KeyError:
            max_lev = leverage
        leverage = min(int(leverage), max_lev)

        payload = {"symbol": coin, "leverage": leverage}
        resp = self._signed_request(
            "POST", "/account/leverage", "update_leverage", payload,
        )
        if not resp.get("success"):
            log.warning("update_leverage(%s, %dx) failed: %s",
                        coin, leverage, resp.get("error"))
            return None

        # Margin mode (best-effort)
        try:
            mm_payload = {"symbol": coin, "is_isolated": not is_cross}
            self._signed_request(
                "POST", "/account/margin", "update_margin_mode", mm_payload,
            )
        except Exception as e:
            log.warning("margin mode set fail %s: %s", coin, e)
        return {"status": "ok"}

    # ===================================================================
    # Fills + PnL
    # ===================================================================
    def user_fills(self, ttl_sec: float = 60.0) -> list:
        """Return executed fills in HL-shape (bot.trader greps for `dir` token).

        Pacifica `side` on fills: open_long/open_short/close_long/close_short.
        Map to HL "Open Long" / "Close Short" etc. — matches Extended adapter.
        """
        now = time.time()
        if self._fills_cache and ttl_sec > 0 and (now - self._fills_cache[0]) < ttl_sec:
            return self._fills_cache[1]
        try:
            r = self.session.get(
                f"{REST_URL}/trades/history",
                params={"account": self.main_account, "limit": 200},
                timeout=10,
            )
            data = r.json().get("data") or []
        except Exception as e:
            log.warning("user_fills failed: %s", e)
            return self._fills_cache[1] if self._fills_cache else []

        SIDE_MAP = {
            "open_long": "Open Long",
            "open_short": "Open Short",
            "close_long": "Close Long",
            "close_short": "Close Short",
        }
        out = []
        for f in data:
            try:
                side_raw = f.get("side") or ""
                dir_str = SIDE_MAP.get(side_raw, side_raw)
                pnl_v = float(f.get("pnl") or 0)
                t_ms = int(f.get("created_at") or 0)
                sz_v = float(f.get("amount") or 0)
                px_v = float(f.get("price") or 0)
                fee_v = float(f.get("fee") or 0)
                out.append({
                    "coin": f.get("symbol"),
                    "dir": dir_str,
                    "side": "B" if "long" in side_raw and "open" in side_raw or "short" in side_raw and "close" in side_raw else "A",
                    "time": t_ms,
                    "closedPnl": pnl_v,
                    "sz": sz_v,
                    "px": px_v,
                    "fee": fee_v,
                    "oid": f.get("order_id") or f.get("id"),
                })
            except Exception:
                continue
        self._fills_cache = (now, out)
        return out

    def compute_realized_pnl(
        self,
        fills: list,
        coin: str,
        direction: str,
        size: float,
        trade_open_iso: str | None = None,
    ):
        """Realized PnL from closing fills (mirror Extended adapter pattern).

        Pacifica fill `pnl` IS the realized PnL on close (verified legacy).
        Returns (pnl_dollars, avg_exit_px).
        """
        if not fills:
            return None, None
        open_ms = 0
        if trade_open_iso:
            try:
                from datetime import datetime as _dt, timezone as _tz
                _t = _dt.fromisoformat(str(trade_open_iso).replace("Z", "+00:00"))
                if _t.tzinfo is None:
                    _t = _t.replace(tzinfo=_tz.utc)
                open_ms = int(_t.timestamp() * 1000) - 60_000
            except Exception:
                open_ms = 0

        close_dir = "Close Long" if direction == "long" else "Close Short"
        matching = []
        for f in fills:
            if f.get("coin") != coin:
                continue
            if f.get("dir") != close_dir:
                continue
            try:
                t_ms = int(f.get("time", 0))
            except (TypeError, ValueError):
                t_ms = 0
            if open_ms and t_ms and t_ms < open_ms:
                continue
            matching.append(f)

        if not matching:
            return None, None

        total_sz = 0.0
        weighted = 0.0
        pnl_sum = 0.0
        for f in matching:
            try:
                sz = float(f.get("sz", 0))
                px = float(f.get("px", 0))
                pnl = float(f.get("closedPnl", 0))
            except (TypeError, ValueError):
                continue
            total_sz += sz
            weighted += px * sz
            pnl_sum += pnl
        avg_px = (weighted / total_sz) if total_sz > 0 else None
        return (pnl_sum if pnl_sum else None), avg_px

    # ===================================================================
    # HL-compat shims (parity with ExtendedClient.info / user_state)
    # ===================================================================
    @property
    def info(self):
        return self

    def user_state(self, address: str = "") -> dict:
        """HL-compat: marginSummary shape — mirror Extended adapter."""
        equity = self.account_value()
        positions = self.open_positions()
        # Aggregate marginUsed from positions
        margin_used = 0.0
        ntl = 0.0
        for p in positions.values():
            try:
                margin_used += float(p.get("marginUsed", 0) or 0)
                ntl += abs(float(p.get("szi", 0) or 0)) * float(p.get("entryPx", 0) or 0)
            except (TypeError, ValueError):
                continue
        ms = {
            "accountValue": str(equity),
            "totalMarginUsed": str(margin_used),
            "totalNtlPos": str(ntl),
        }
        asset_positions = [{"position": p} for p in positions.values()]
        return {
            "marginSummary": ms,
            "crossMarginSummary": ms,
            "withdrawable": str(max(0.0, equity - margin_used)),
            "crossMaintenanceMarginUsed": "0",
            "assetPositions": asset_positions,
        }


# ===========================================================================
# Pending TODOs (track in MEMORY when deployed)
# ===========================================================================
# 1. Verify Pacifica /book response field names against live mainnet snapshot
#    (legacy used inferred `b`/`a` or `bids`/`asks` — confirm vs current API).
# 2. WebSocket subscription for live position/order/fill events (REST polling
#    fine for >=1h TF; 1m/5m would benefit from WS — see ws_subscribe_kline stub).
# 3. Pacifica margin-mode endpoint may not accept is_isolated=false on perps —
#    confirm whether cross is the only supported mode (legacy assumed yes).
# 4. /info/prices `funding` field — verify per-bar vs hourly normalisation.
# 5. Compute_realized_pnl matches Extended PR pattern but Pacifica may return
#    one consolidated `pnl` per close OR per-fill — re-verify post-first trade.
