"""bindings.extended — Extended Exchange (x10/Starknet) binding.

Transport layer inspected (bots/extended/bot/exchange_extended.py):
  * the x10-python-trading-starknet SDK is aiohttp-based; the adapter drives
    it through a private background event loop (_AsyncBridge :275) that
    reuses one aiohttp session (:279 comment);
  * the WS-confirm root fix (2026-07-02) added REST-confirm-by-external_id —
    also aiohttp;
  * no requests usage in the adapter itself, but patch_requests is installed
    anyway (belt) since sibling libs may pull it in.
→ lowest interception layer = aiohttp.ClientSession._request (patch_aiohttp);
  the SDK's own retry/timeout wrappers and the adapter's _AsyncBridge hard
  timeouts (:145, :164 — 15s REST-place ceiling) run unmodified above it.

Constructor needs (:297): EXTENDED_API_KEY, EXTENDED_STARK_PUBLIC/PRIVATE
(hex ints for StarkPerpetualAccount), EXTENDED_VAULT_ID (int),
EXTENDED_ACCOUNT_ID, EXTENDED_ETH_ADDRESS, EXCHANGE=extended, DRY_RUN=0.
Fabricated inert values below; the SDK signs into the gate only.

Responder: substring-routed rendering of the x10 REST surface (markets,
market stats, orderbook, balance, positions, orders incl. query filters,
fees, candles, trades) over the shared FakeVenue. x10 payloads are enveloped
{"status": "OK", "data": ...}. Unknown endpoints → None →
UninterceptedRealCall (host-iteration loop — expected on first run; the
adapter constructor tolerates markets/fees fetch failures by design,
:363/:376, so even a partial map lets construction complete).

ID SHAPES: FakeVenue's canonical oids are "fk-<n>"; live Extended ids are
plain ints — so every id this responder renders is the NUMERIC SUFFIX
(int(oid.rsplit('-')[-1])), which the x10 pydantic models can parse and
FakeScenario._oid_matches maps back to venue truth by suffix.

TPSL note: the contract client places SLs through raw.trigger_sl — an x10
TPSL POSITION order (top-level qty=0, trigger inside the nested stopLoss
leg). The responder extracts the nested trigger and, for qty==0
position-wide orders, sizes the venue-side trigger to the LIVE position
(that is what the venue's min-size validation applies to).

Env also carries FC_EXT_* fast-confirm knobs consumed by
fleet_core.venues.extended (readback poll budgets shrink from live-grade
seconds to harness-grade milliseconds).
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from fleet_core.conformance.faults import GateResponse, json_response
from fleet_core.conformance.bindings import BindingUnavailable, BoundContext
from fleet_core.conformance.bindings.fake import FakeVenue, corrupt_one
from fleet_core.conformance.bindings._venue_base import (
    build_venue_context,
    parse_body,
    smoke_construct_venue,
)

NAME = "extended"
TRANSPORTS = ("aiohttp", "requests")

_FAKE_STARK_PRIV = "0x" + "3" * 63
_FAKE_STARK_PUB = "0x" + "4" * 63


def _path(url: str) -> str:
    return urlparse(url).path


def _query(url: str) -> Dict[str, List[str]]:
    return parse_qs(urlparse(url).query)


def classify(method: str, url: str, body: Any) -> str:
    p = _path(url).lower()
    m = method.upper()
    if "candle" in p or "kline" in p:
        return "candles"
    if "orderbook" in p or "depth" in p:
        return "mark"  # book snapshot is a price read
    if "markets" in p and "stats" not in p and "orderbook" not in p:
        return "meta" if m == "GET" else "other"
    if "balance" in p:
        return "account"
    if "positions" in p:
        return "positions"
    if "fees" in p:
        return "meta"
    if "trades" in p or "fills" in p:
        return "fills"
    if "leverage" in p:
        return "leverage"
    if "order" in p:
        if m == "GET":
            q = urlparse(url).query.lower()
            if "tpsl" in q or "conditional" in q or "trigger" in q:
                return "triggers"  # per-coin/account SL listings
            return "orders"
        if m == "DELETE" or "cancel" in p:
            return "cancel_order"
        b = parse_body(body)
        typ = str(b.get("type") or b.get("orderType") or "").upper()
        if "TPSL" in typ or "CONDITIONAL" in typ or b.get("trigger") \
                or b.get("takeProfit") or b.get("stopLoss") \
                or b.get("triggerPrice") or b.get("stopPrice"):
            return "place_trigger"
        return "place_order"
    if "funding" in p or "stats" in p:
        return "mark"
    return "other"


class _ExtendedResponder:
    def __init__(self, venue: FakeVenue) -> None:
        self.venue = venue

    @staticmethod
    def _ok(data: Any) -> GateResponse:
        return json_response({"status": "OK", "data": data})

    @staticmethod
    def _err(code: int, msg: str) -> GateResponse:
        return json_response({"status": "ERROR",
                              "error": {"code": code, "message": msg}})

    @staticmethod
    def _num(oid: Any) -> Any:
        """Numeric-suffix rendering of a FakeVenue oid ('fk-1000' -> 1000)."""
        s = str(oid)
        tail = s.rsplit("-", 1)[-1]
        return int(tail) if tail.isdigit() else s

    @staticmethod
    def _prec(step: Any) -> int:
        s = str(step)
        return max(0, len(s.split(".", 1)[1].rstrip("0"))) if "." in s else 0

    def _coins(self) -> List[str]:
        return sorted(c for c, m in self.venue.meta.items()
                      if m.get("scope", "main") == "main")

    def _market_from_path(self, url: str) -> str:
        for seg in _path(url).split("/"):
            if seg.endswith("-USD"):
                return seg
        return ""

    # reads ----------------------------------------------------------------
    def _market_stats(self, mark: float) -> Dict[str, Any]:
        """Full x10 MarketStatsModel rendering (field set from the host run's
        pydantic missing-field report — every key it named is here)."""
        return {"markPrice": str(mark), "lastPrice": str(mark),
                "indexPrice": str(mark),
                "dailyVolume": "0", "dailyVolumeBase": "0",
                "dailyPriceChange": "0", "dailyPriceChangePercentage": "0",
                "dailyLow": str(mark), "dailyHigh": str(mark),
                "askPrice": str(mark * 1.0001) if mark else "0",
                "bidPrice": str(mark * 0.9999) if mark else "0",
                "openInterest": "0", "openInterestBase": "0",
                "fundingRate": "0.0",
                "nextFundingRate": int(time.time() * 1000) + 3_600_000}

    def _markets(self) -> GateResponse:
        rows = []
        for c in self._coins():
            meta = self.venue.meta[c]
            mark = self.venue.marks.get(c, 0.0)
            qty_prec = self._prec(meta["min_size"])
            rows.append({
                "name": "%s-USD" % c, "assetName": c,
                "assetPrecision": qty_prec,
                "collateralAssetName": "USD",
                "collateralAssetPrecision": 6,
                "active": True,
                "status": "ACTIVE",
                "marketStats": self._market_stats(mark),
                "tradingConfig": {
                    "minOrderSize": str(meta["min_size"]),
                    "minOrderSizeChange": str(meta["min_size"]),
                    "minPriceChange": str(meta["tick"]),
                    "maxLeverage": str(meta["max_leverage"]),
                    "maxOrderSize": "1000000000",
                    "maxPositionValue": "100000000",
                    "maxMarketOrderValue": "10000000",
                    "maxLimitOrderValue": "10000000",
                    "maxNumOrders": "200",
                    "limitPriceCap": "0.05",
                    "limitPriceFloor": "0.05",
                    # one flat tier; riskFactor = 1/maxLev (the venue's own
                    # initial-margin identity)
                    "riskFactorConfig": [
                        {"upperBound": "100000000",
                         "riskFactor": str(1.0 / float(
                             meta["max_leverage"]))}],
                    "quantityPrecision": qty_prec,
                    "pricePrecision": self._prec(meta["tick"]),
                },
                "l2Config": {
                    "type": "STARKX",
                    "collateralId": "0x%064x" % 1,
                    "collateralResolution": 10**6,
                    "syntheticId": "0x%064x" % (2 + self._coins().index(c)),
                    "syntheticResolution": 10**qty_prec,
                },
            })
        return self._ok(rows)

    def _stats(self, url: str) -> GateResponse:
        market = self._market_from_path(url)
        coin = market.replace("-USD", "")
        if coin not in self.venue.meta:
            return self._err(1140, "unknown_symbol %s" % coin)
        mark = self.venue.marks.get(coin, 0.0)
        return self._ok(self._market_stats(mark))

    def _book(self, url: str) -> GateResponse:
        market = self._market_from_path(url)
        coin = market.replace("-USD", "")
        if coin not in self.venue.meta:
            return self._err(1140, "unknown_symbol %s" % coin)
        mark = self.venue.marks.get(coin, 0.0)
        if not mark:
            return self._ok({"market": market, "bid": [], "ask": []})
        return self._ok({
            "market": market,
            "bid": [{"price": str(mark * 0.9999), "qty": "1000"}],
            "ask": [{"price": str(mark * 1.0001), "qty": "1000"}],
        })

    def _balance(self) -> GateResponse:
        # Field mapping (see fleet_core.venues.extended): venue "equity" ==
        # FakeVenue.account_value (today's deployed sizing basis) and
        # balance + unrealisedPnl == FakeVenue.equity (the with-uPnL truth) —
        # on the real venue balance + uPnL == equity identically.
        av = self.venue.account_value
        return self._ok({"collateralName": "USD",
                         "balance": str(av),
                         "equity": str(av),
                         "availableForTrade": str(av),
                         "availableForWithdrawal": str(av),
                         "initialMargin": str(self.venue.margin_used),
                         "marginRatio": "0.0",
                         "exposure": "0",
                         "leverage": "0",
                         "updatedTime": int(time.time() * 1000),
                         "unrealisedPnl": str(
                             self.venue.equity - self.venue.account_value)})

    def _positions(self, directive: Optional[Dict[str, Any]] = None
                   ) -> GateResponse:
        rows = []
        seq = 0
        for scope in self.venue.positions:
            for coin, p in self.venue.positions[scope].items():
                mark = self.venue.marks.get(coin, 0.0)
                seq += 1
                now_ms = int(time.time() * 1000)
                rows.append({
                    # x10 PositionModel requires the venue-side identity
                    # fields (host run named id/account_id/status missing)
                    "id": seq, "accountId": 101,
                    "market": "%s-USD" % coin,
                    "status": "OPENED",
                    "side": "LONG" if p["size_signed"] > 0 else "SHORT",
                    "size": str(abs(p["size_signed"])),
                    "openPrice": str(p["entry_px"]),
                    "markPrice": str(mark),
                    "exitPrice": None,
                    "liquidationPrice": (None if p.get("liq_px") is None
                                         else str(p["liq_px"])),
                    "value": str(abs(p["size_signed"]) * mark),
                    "unrealisedPnl": "0.0", "realisedPnl": "0.0",
                    "leverage": "5", "margin": "0.0",
                    "adl": 1,
                    "maxPositionSize": "1000000000",
                    "tpPrice": None, "slPrice": None,
                    "createdAt": now_ms, "updatedAt": now_ms,
                    "createdTime": now_ms, "updatedTime": now_ms,
                })
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "size")
        return self._ok(rows)

    def _orders(self, url: str,
                directive: Optional[Dict[str, Any]] = None) -> GateResponse:
        # honor query filters the SDK sends (market names / order type /
        # external id) — generic key matching, values may be comma-joined
        markets: List[str] = []
        types: List[str] = []
        externals: List[str] = []
        for k, vals in _query(url).items():
            kl = k.lower()
            for v in vals:
                for piece in v.split(","):
                    piece = piece.strip()
                    if not piece:
                        continue
                    if "market" in kl:
                        markets.append(piece)
                    elif "type" in kl:
                        types.append(piece.upper())
                    elif "external" in kl:
                        externals.append(piece)
        rows = []
        for o in self.venue.orders.values():
            rendered_type = "TPSL" if o["is_trigger"] else "LIMIT"
            if markets and "%s-USD" % o["coin"] not in markets:
                continue
            if types and not any(t in rendered_type or rendered_type in t
                                 for t in types):
                continue
            if externals and str(o.get("client_oid")) not in externals:
                continue
            now_ms = int(time.time() * 1000)
            row = {
                "id": self._num(o["oid"]),
                "accountId": 101,
                # external_id is a REQUIRED string on OpenOrderModel — venue
                # echoes the client id or renders its own (never null)
                "externalId": (str(o["client_oid"]) if o.get("client_oid")
                               else "ext-%s" % self._num(o["oid"])),
                "market": "%s-USD" % o["coin"],
                "side": "BUY" if o["side"] == "buy" else "SELL",
                "qty": str(o["size"]),
                "filledQty": "0",
                "price": str(o["limit_px"] or 0),
                "type": rendered_type,
                "reduceOnly": bool(o["reduce_only"]),
                "postOnly": False,
                "timeInForce": "GTT",
                # OrderStatus enum: triggers rest as UNTRIGGERED, plain
                # orders as NEW ("PLACED" is not in the SDK vocabulary)
                "status": "UNTRIGGERED" if o["is_trigger"] else "NEW",
                "createdTime": now_ms,
                "updatedTime": now_ms,
            }
            if o["is_trigger"]:
                # nested TPSL leg — the ONLY place the SDK model carries the
                # trigger price (readers use stop_loss.trigger_price)
                row["tpSlType"] = "POSITION"
                row["stopLoss"] = {
                    "triggerPrice": str(o["trigger_px"] or 0),
                    "triggerPriceType": "MARK",
                    "price": str(o["limit_px"] or o["trigger_px"] or 0),
                    "priceType": "MARKET",
                }
            rows.append(row)
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "qty")
        return self._ok(rows)

    def _fills(self, directive: Optional[Dict[str, Any]] = None
               ) -> GateResponse:
        rows = [
            {"id": self._num(f["oid"]),
             "accountId": 101,
             "market": "%s-USD" % f["coin"],
             "orderId": self._num(f["oid"]),
             "externalId": f.get("client_oid"),
             "side": "BUY" if f["side"] == "buy" else "SELL",
             "price": str(f["px"]), "qty": str(f["size"]),
             "value": str(f["px"] * f["size"]),
             "fee": "0", "isTaker": True,
             "tradeType": "TRADE",
             "createdTime": int(f["time"] * 1000),
             "reduceOnly": bool(f.get("reduce_only")),
             "isReduceOnly": bool(f.get("reduce_only"))}
            for f in self.venue.fills[:100]]
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "qty")
        return self._ok(rows)

    def _fees(self) -> GateResponse:
        return self._ok([{"market": "%s-USD" % c,
                          "makerFeeRate": "0.0002",
                          "takerFeeRate": "0.00045",
                          "builderFeeRate": "0"}
                         for c in self._coins()])

    def _candles(self, url: str) -> GateResponse:
        qs = {k: v[0] for k, v in _query(url).items()}
        # market may be path-embedded: /info/candles/BTC-USD/trades?...
        market = qs.get("market", "") or self._market_from_path(url)
        coin = market.replace("-USD", "")
        tf_map = {"PT1M": "1m", "PT5M": "5m", "PT1H": "1h", "PT4H": "4h",
                  "P1D": "1d", "1m": "1m", "5m": "5m", "1h": "1h",
                  "4h": "4h", "1d": "1d"}
        tf = tf_map.get(qs.get("interval", ""), qs.get("interval", ""))
        neutral = self.venue._serve_candles(  # noqa: SLF001
            {"coin": coin, "tf": tf, "limit": qs.get("limit", "500")})
        if neutral.status != 200:
            return self._ok([])
        return self._ok([
            {"T": int(r[0]), "timestamp": int(r[0]), "openTime": int(r[0]),
             "o": str(r[1]), "open": str(r[1]),
             "h": str(r[2]), "high": str(r[2]),
             "l": str(r[3]), "low": str(r[3]),
             "c": str(r[4]), "close": str(r[4]),
             "v": str(r[5]), "volume": str(r[5])}
            for r in neutral.json().get("bars", [])])

    # writes --------------------------------------------------------------
    def _place(self, b: Dict[str, Any],
               directive: Optional[Dict[str, Any]]) -> GateResponse:
        market = str(b.get("market", ""))
        coin = market.replace("-USD", "")
        is_buy = str(b.get("side", "")).upper() == "BUY"
        qty = float(b.get("qty", 0) or b.get("size", 0) or 0)
        typ = str(b.get("type") or "").upper()
        client_oid = b.get("externalId") or b.get("id")
        # nested TPSL/CONDITIONAL trigger legs (x10 NewOrderModel shape)
        sl_leg = b.get("stopLoss") if isinstance(b.get("stopLoss"), dict) \
            else {}
        tp_leg = b.get("takeProfit") if isinstance(b.get("takeProfit"),
                                                   dict) else {}
        cond = b.get("trigger") if isinstance(b.get("trigger"), dict) else {}
        trig_raw = (b.get("triggerPrice") or b.get("stopPrice")
                    or (sl_leg or {}).get("triggerPrice")
                    or (tp_leg or {}).get("triggerPrice")
                    or (cond or {}).get("triggerPrice") or 0)
        try:
            trig = float(trig_raw or 0)
        except (TypeError, ValueError):
            trig = 0.0
        is_trigger = ("TPSL" in typ or "CONDITIONAL" in typ or trig > 0)
        if is_trigger:
            if qty <= 0:
                # position-wide TPSL (amount_of_synthetic=0 by protocol):
                # the venue sizes the trigger to the live position
                pos = self.venue.find_position(coin)
                qty = abs(pos["size_signed"]) if pos else 0.0
            neutral = self.venue._handle_place_trigger(  # noqa: SLF001
                {"coin": coin, "is_buy": is_buy, "sz": qty,
                 "trigger_px": trig, "client_oid": client_oid}, directive)
            nd = neutral.json()
            if not nd.get("ok"):
                return self._err(1121, str(nd.get("reject")))
            return self._ok({"id": self._num(nd["oid"]),
                             "externalId": (str(client_oid) if client_oid
                                            else "ext-%s"
                                            % self._num(nd["oid"])),
                             "status": "PLACED"})
        tif = str(b.get("timeInForce", "GTT")).upper()
        neutral = self.venue._handle_place_order(  # noqa: SLF001
            {"type": "market" if tif == "IOC" else "limit",
             "coin": coin, "is_buy": is_buy, "sz": qty,
             "px": float(b.get("price", 0) or 0),
             "reduce_only": bool(b.get("reduceOnly")),
             "client_oid": client_oid}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(1121, str(nd.get("reject")))
        data = {"id": self._num(nd["oid"]),
                "externalId": (str(client_oid) if client_oid
                               else "ext-%s" % self._num(nd["oid"])),
                "status": "PLACED"}
        if nd.get("filled_size"):
            data.update({"status": "FILLED",
                         "filledQty": str(nd["filled_size"]),
                         "averagePrice": str(nd["avg_px"])})
        return self._ok(data)

    def _cancel(self, url: str, b: Dict[str, Any],
                directive: Optional[Dict[str, Any]]) -> GateResponse:
        oid = str(b.get("orderId") or b.get("id") or "")
        if not oid:
            segs = [s for s in _path(url).split("/") if s]
            oid = segs[-1] if segs else ""
        hit = None
        for full in list(self.venue.orders):
            if full == oid or full.endswith("-%s" % oid) or \
                    str(self.venue.orders[full].get("client_oid")) == oid:
                hit = full
                break
        if hit is None:
            return self._err(1142, "order not found")
        neutral = self.venue._handle_cancel({"oid": hit}, directive)  # noqa: SLF001
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(1142, str(nd.get("reject")))
        return self._ok({"cancelledOrders": [self._num(hit)]})

    def _leverage(self, b: Dict[str, Any]) -> GateResponse:
        # NOTE rejections go out as HTTP 400: the x10 account.update_leverage
        # SDK path does not inspect the {"status":"ERROR"} envelope on a 200
        # (host run: above-max returned None instead of raising) — the venue
        # itself signals leverage rejects at the HTTP layer.
        market = str(b.get("market") or b.get("marketName") or "")
        coin = market.replace("-USD", "")
        if coin not in self.venue.meta:
            return json_response(
                {"status": "ERROR",
                 "error": {"code": 1140,
                           "message": "unknown_symbol %s" % coin}},
                status=400)
        try:
            lev = int(float(b.get("leverage", 0) or 0))
        except (TypeError, ValueError):
            return json_response(
                {"status": "ERROR",
                 "error": {"code": 1137, "message":
                           "bad leverage value %r" % b.get("leverage")}},
                status=400)
        if lev > int(self.venue.meta[coin]["max_leverage"]):
            return json_response(
                {"status": "ERROR",
                 "error": {"code": 1137, "message":
                           "leverage_above_max: %d > %s"
                           % (lev, self.venue.meta[coin]["max_leverage"])}},
                status=400)
        return self._ok({"market": market, "leverage": str(lev)})

    # entry point ------------------------------------------------------------
    def __call__(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        url = ctx["url"]
        p = _path(url).lower()
        m = ctx["method"]
        b = parse_body(ctx.get("body"))
        if "candle" in p or "kline" in p:
            return self._candles(url)
        if m == "GET":
            if "orderbook" in p or "depth" in p:
                return self._book(url)
            if "stats" in p:
                return self._stats(url)
            if "markets" in p:
                return self._markets()
            if "balance" in p:
                return self._balance()
            if "positions" in p:
                return self._positions(directive)
            if "fees" in p:
                return self._fees()
            if "trades" in p or "fills" in p:
                return self._fills(directive)
            if "funding" in p:
                return self._stats(url)
            if "order" in p:
                return self._orders(url, directive)
            return None
        if m in ("POST", "PUT", "PATCH"):
            if "leverage" in p:
                return self._leverage(b)
            if "cancel" in p:
                return self._cancel(url, b, directive)
            if "order" in p:
                return self._place(b, directive)
            return None
        if m == "DELETE":
            if "order" in p:
                return self._cancel(url, b, directive)
            return None
        return None


def _env(dry_run: bool = False) -> Dict[str, str]:
    return {
        "EXCHANGE": "extended",
        "NETWORK": "mainnet",
        # live-shaped by default; dry_run=True = the F2 write-isolation lane
        # (the WRAPPER enforces DRY on extended — the raw has no gate)
        "DRY_RUN": "1" if dry_run else "0",
        "EXTENDED_API_KEY": "harness-inert-key",
        "EXTENDED_STARK_PUBLIC": _FAKE_STARK_PUB,
        "EXTENDED_STARK_PRIVATE": _FAKE_STARK_PRIV,
        "EXTENDED_VAULT_ID": "101",
        "EXTENDED_ACCOUNT_ID": "101",
        "EXTENDED_ETH_ADDRESS": "0x" + "56" * 20,
        "EXTENDED_WS_CANDLE": "0",
        # fast readback budgets for the contract client under the harness
        # (live defaults are seconds-grade; the fake venue answers instantly)
        "FC_EXT_CONFIRM_ATTEMPTS": "3",
        "FC_EXT_CONFIRM_SLEEP_SEC": "0.02",
        "FC_EXT_RETRY_429_SLEEP": "0.02",
        "FC_EXT_CLOSE_ATTEMPTS": "2",
        "FC_EXT_POS_TTL_SEC": "0.5",
        "FC_EXT_CANDLE_TTL_SEC": "0.5",
    }


def _construct_raw() -> Any:
    try:
        from bot.config import Settings
        from bot.exchange_extended import ExtendedClient
    except ImportError as e:
        raise BindingUnavailable(
            "extended: bot package / x10 SDK not importable here (run on the "
            "extended host from the bot root): %s" % e)
    return ExtendedClient(Settings.from_env())


def _teardown_raw(raw: Any) -> None:
    """Shut the adapter's aiohttp machinery down cleanly at context close:
    close the (lazy) BlockingTradingClient, __aexit__ the manually-entered
    PerpetualTradingClient session, then stop the _AsyncBridge loop — kills
    the 'Unclosed client session' GC noise the first host run showed.
    Best-effort at every step: teardown must never fail a run."""
    bridge = getattr(raw, "_bridge", None)
    if bridge is None:
        return

    async def _close() -> None:
        for obj, closers in (
                (getattr(raw, "_trader", None),
                 ("close", "aclose", "stop")),
                (getattr(raw, "_client", None),
                 ("__aexit__", "close", "aclose"))):
            if obj is None:
                continue
            for name in closers:
                fn = getattr(obj, name, None)
                if fn is None:
                    continue
                try:
                    res = fn(None, None, None) if name == "__aexit__" else fn()
                    if hasattr(res, "__await__"):
                        await res
                    break
                except Exception:
                    continue

    try:
        bridge.run(_close(), timeout=10)
    except Exception:
        pass
    try:
        bridge.stop()
    except Exception:
        pass


def partial_outage_match(tctx: Dict[str, Any]) -> bool:
    """Partial-outage selector: extended's positions snapshot is ONE GET
    /positions — no multi-scope subset exists, so the selector is that single
    leg (the law degenerates to a full-read outage; still must be UNKNOWN)."""
    return tctx.get("op") == "positions"


def build_context(dry_run: bool = False) -> BoundContext:
    ctx = build_venue_context(
        name=NAME, env=_env(dry_run=dry_run), classify=classify,
        make_responder=lambda venue: _ExtendedResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw,
        partial_outage_match=partial_outage_match,
        teardown_raw=_teardown_raw)
    # extended's positions snapshot costs exactly ONE transport call —
    # sequenced faults (fire_after) must target the readback correctly
    ctx.scenario.positions_read_transport_calls = 1
    return ctx


def smoke_construct() -> str:
    return smoke_construct_venue(
        name=NAME, env=_env(), classify=classify,
        make_responder=lambda venue: _ExtendedResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw,
        teardown_raw=_teardown_raw)
