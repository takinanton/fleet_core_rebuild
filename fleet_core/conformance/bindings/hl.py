"""bindings.hl — Hyperliquid venue binding (lazy; SDK only on the hl host).

Transport layer inspected (bots/hl/bot/exchange_hl.py):
  * module-level `requests.post(url + "/info", ...)` (preflight, perpDexs);
  * hyperliquid-python-sdk Info/Exchange each hold a requests.Session
    (adapter installs default timeouts on them at :157/:264/:298);
  * NO aiohttp, NO websockets (skip_ws=True).
→ lowest interception layer = requests.Session.send (patch_requests), which
  also covers requests.post/get module functions (they build a Session).

Constructor needs (exchange_hl.py:235): NETWORK, EXCHANGE=hyperliquid,
HYPERLIQUID_AGENT_PRIVATE_KEY (throwaway secp256k1 hex — signs only into the
gate), HYPERLIQUID_ACCOUNT_ADDRESS, DRY_RUN=0 (live-shaped paths; see
_venue_base posture note). ENV_FILE is pointed at an empty temp file so the
host's real .env is never loaded.

Responder: renders the shared FakeVenue truth store into HL /info and
/exchange shapes. Scopes: FakeVenue "main" → HL main dex (""), "xyz" → HIP-3
dex "xyz" (the multi-scope snapshot law of exchange_hl.py:806 is testable).
Unknown /info types or /exchange actions return None → UninterceptedRealCall
(host-iteration loop).
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Callable, Dict, List, Optional

from fleet_core.conformance.faults import GateResponse, json_response
from fleet_core.conformance.bindings import BindingUnavailable, BoundContext
from fleet_core.conformance.bindings.fake import FakeVenue, corrupt_one
from fleet_core.conformance.bindings._venue_base import (
    build_venue_context,
    parse_body,
    smoke_construct_venue,
)

NAME = "hl"
TRANSPORTS = ("requests",)

# throwaway, never-funded creds (valid-shaped secp256k1 scalar / EVM address)
_FAKE_AGENT_KEY = "0x" + "ab" * 32
_FAKE_ACCOUNT = "0x" + "12" * 20

_INFO_OP = {
    "meta": "meta", "spotMeta": "meta", "perpDexs": "meta",
    "metaAndAssetCtxs": "mark", "allMids": "mark",
    "clearinghouseState": "positions", "spotClearinghouseState": "positions",
    "candleSnapshot": "candles",
    "userFills": "fills", "userFillsByTime": "fills",
    "frontendOpenOrders": "orders", "openOrders": "orders",
    # orderStatus is the wrapper's per-order SL placement-confirm readback —
    # HL has no separate per-coin trigger-list endpoint (frontendOpenOrders
    # serves both roles), so orderStatus carries the canonical "triggers" op
    # (checks fault it to model the trigger-confirm read going down).
    "orderStatus": "triggers",
}


def classify(method: str, url: str, body: Any) -> str:
    b = parse_body(body)
    if url.rstrip("/").endswith("/info"):
        return _INFO_OP.get(str(b.get("type")), "other")
    if url.rstrip("/").endswith("/exchange"):
        action = b.get("action") or {}
        at = action.get("type")
        if at == "order":
            for o in action.get("orders", []):
                if "trigger" in (o.get("t") or {}):
                    return "place_trigger"
            return "place_order"
        if at in ("cancel", "cancelByCloid"):
            return "cancel_order"
        if at == "updateLeverage":
            return "leverage"
        return "other"
    return "other"


class _HLResponder:
    """Renders FakeVenue truth into HL wire shapes.

    Asset-index mapping: the SDK addresses assets by integer index into the
    meta universe we serve. Main dex: index = position in the main universe.
    HIP-3 ("xyz"): the SDK offsets per-dex (dex_idx*10000 + i) — covered for
    the single "xyz" dex.
    """

    def __init__(self, venue: FakeVenue) -> None:
        self.venue = venue

    # deterministic universes ------------------------------------------------
    def _universe(self, scope: str) -> List[str]:
        return sorted(c for c, m in self.venue.meta.items()
                      if m.get("scope", "main") == scope)

    def _api_name(self, coin: str) -> str:
        # HL wire format for HIP-3 coins is "xyz:GOLD" (internal "xyz_GOLD")
        # — the adapter's coin_to_api/api_to_coin translation is exercised.
        return coin.replace("_", ":", 1) if coin.startswith("xyz_") else coin

    @staticmethod
    def _internal_name(api: str) -> str:
        return api.replace(":", "_", 1) if ":" in api else api

    def _coin_by_asset(self, asset_idx: int) -> Optional[str]:
        if asset_idx >= 10000:
            uni = self._universe("xyz")
            i = asset_idx % 10000
        else:
            uni = self._universe("main")
            i = asset_idx
        return uni[i] if 0 <= i < len(uni) else None

    # renderers ---------------------------------------------------------------
    def _meta(self, dex: str) -> Dict[str, Any]:
        scope = "xyz" if dex == "xyz" else "main"
        return {"universe": [
            {"name": self._api_name(c),
             "szDecimals": 3,
             "maxLeverage": int(self.venue.meta[c]["max_leverage"])}
            for c in self._universe(scope)]}

    def _positions_state(self, dex: str,
                         directive: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        scope = "xyz" if dex == "xyz" else "main"
        rows = []
        for coin, p in self.venue.positions[scope].items():
            rows.append({"position": {
                "coin": self._api_name(coin),
                "szi": str(p["size_signed"]),
                "entryPx": str(p["entry_px"]),
                "leverage": {"type": "cross", "value": 5},
                "liquidationPx": (None if p.get("liq_px") is None
                                  else str(p["liq_px"])),
                "marginUsed": "0.0",
                "unrealizedPnl": "0.0",
                "positionValue": str(abs(p["size_signed"]) * p["entry_px"]),
            }, "type": "oneWay"})
        if directive is not None and directive.get("mode") == "corrupt_item" \
                and rows:
            rows[-1]["position"]["szi"] = "corrupt-item-0xDEAD"
        # Account fields live on the MAIN clearinghouse only; a HIP-3 dex
        # clearinghouse carries its own (zero here) accountValue — rendering
        # the same number on every dex double-counted equity/margin for any
        # adapter that sums across dexes (exchange_hl.py:752 does).
        if dex == "xyz":
            av, mu = "0.0", "0.0"
        else:
            av = str(self.venue.account_value)
            mu = str(self.venue.margin_used)
        return {
            "assetPositions": rows,
            "marginSummary": {
                "accountValue": av,
                "totalMarginUsed": mu,
                "totalNtlPos": "0.0", "totalRawUsd": "0.0",
            },
            "crossMarginSummary": {
                "accountValue": av,
                "totalMarginUsed": mu,
            },
            "withdrawable": av,
        }

    def _all_mids(self, dex: str) -> Dict[str, str]:
        scope = "xyz" if dex == "xyz" else "main"
        return {self._api_name(c): str(self.venue.marks.get(c, 0.0))
                for c in self._universe(scope)}

    def _candles(self, b: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        req = b.get("req") or b
        api = str(req.get("coin", ""))
        tf = str(req.get("interval", ""))
        coin = self._internal_name(api)
        if coin not in self.venue.meta:
            return []
        neutral = self.venue._serve_candles(  # noqa: SLF001
            {"coin": coin, "tf": tf, "limit": "500"})
        if neutral.status != 200:
            return []
        return [{"t": int(r[0]), "T": int(r[0]),  # close-time approximated
                 "s": api, "i": tf, "o": str(r[1]), "h": str(r[2]),
                 "l": str(r[3]), "c": str(r[4]), "v": str(r[5]), "n": 1}
                for r in neutral.json().get("bars", [])]

    def _open_orders(self, directive: Optional[Dict[str, Any]] = None,
                     dex: str = "") -> List[Dict[str, Any]]:
        # Per-dex listing (real HL: frontendOpenOrders without dex= misses
        # xyz:* and vice versa — exchange_hl.py:1536 bugfix note); serving
        # every order on every dex leg double-listed each order to adapters
        # that merge across dexes.
        scope = "xyz" if dex == "xyz" else "main"
        out = []
        for o in self.venue.orders.values():
            if self.venue.coin_scope(o["coin"]) != scope:
                continue
            out.append({
                "coin": self._api_name(o["coin"]),
                "oid": int(o["oid"].split("-")[-1]),
                "side": "B" if o["side"] == "buy" else "A",
                "sz": str(o["size"]),
                "limitPx": str(o["limit_px"] or 0),
                "triggerPx": str(o["trigger_px"] or 0),
                "isTrigger": bool(o["is_trigger"]),
                "reduceOnly": bool(o["reduce_only"]),
                "orderType": ("Stop Market" if o["is_trigger"]
                              else "Limit"),
                "isPositionTpsl": False,
                "cloid": o.get("client_oid"),
            })
        if directive is not None and directive.get("mode") == "corrupt_item":
            out = corrupt_one(out, "sz")
        return out

    def _order_status(self, b: Dict[str, Any]) -> GateResponse:
        """orderStatus — the wrapper's trigger placement-confirm readback.
        Real shape: {"status":"order","order":{"order":{...},"status":"open",
        ...}} for a live order, {"status":"unknownOid"} otherwise (an acked-
        but-never-landed trigger reads as unknownOid → the naked-success
        class is detectable)."""
        oid = b.get("oid")
        for full_oid, o in self.venue.orders.items():
            if not (str(full_oid) == str(oid)
                    or str(full_oid).endswith("-%s" % oid)):
                continue
            return json_response({"status": "order", "order": {
                "order": {
                    "coin": self._api_name(o["coin"]),
                    "side": "B" if o["side"] == "buy" else "A",
                    "limitPx": str(o["limit_px"] or 0),
                    "sz": str(o["size"]),
                    "oid": int(str(full_oid).split("-")[-1]),
                    "timestamp": 0,
                    "triggerCondition": "",
                    "isTrigger": bool(o["is_trigger"]),
                    "triggerPx": str(o["trigger_px"] or 0),
                    "isPositionTpsl": False,
                    "reduceOnly": bool(o["reduce_only"]),
                    "orderType": ("Stop Market" if o["is_trigger"]
                                  else "Limit"),
                    "origSz": str(o["size"]),
                    "cloid": o.get("client_oid"),
                },
                "status": "open",
                "statusTimestamp": 0,
            }})
        return json_response({"status": "unknownOid"})

    def _user_fills(self, directive: Optional[Dict[str, Any]] = None
                    ) -> List[Dict[str, Any]]:
        rows = [{
            "coin": self._api_name(f["coin"]),
            "px": str(f["px"]), "sz": str(f["size"]),
            "side": "B" if f["side"] == "buy" else "A",
            "time": int(f["time"] * 1000),
            "oid": int(f["oid"].split("-")[-1]),
            "cloid": f.get("client_oid"),
            "dir": ("Close" if f.get("reduce_only") else "Open"),
            "closedPnl": "0.0", "fee": "0.0", "crossed": True,
        } for f in self.venue.fills[:100]]
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "sz")
        return rows

    # /exchange ---------------------------------------------------------------
    def _hl_statuses(self, statuses: List[Dict[str, Any]]) -> GateResponse:
        return json_response({"status": "ok", "response": {
            "type": "order", "data": {"statuses": statuses}}})

    def _handle_order_action(self, action: Dict[str, Any],
                             directive: Optional[Dict[str, Any]]) -> GateResponse:
        statuses: List[Dict[str, Any]] = []
        for o in action.get("orders", []):
            coin = self._coin_by_asset(int(o.get("a", -1)))
            if coin is None:
                statuses.append({"error": "invalid asset"})
                continue
            t = o.get("t") or {}
            is_buy = bool(o.get("b"))
            sz = float(o.get("s", 0))
            if "trigger" in t:
                trig = t["trigger"]
                neutral = self.venue._handle_place_trigger(  # noqa: SLF001
                    {"coin": coin, "is_buy": is_buy, "sz": sz,
                     "trigger_px": float(trig.get("triggerPx", 0)),
                     "client_oid": o.get("c")}, directive)
                nd = neutral.json()
                if not nd.get("ok"):
                    statuses.append({"error": str(nd.get("reject"))})
                else:
                    statuses.append({"resting": {
                        "oid": int(str(nd["oid"]).split("-")[-1])}})
                continue
            # limit order; HL market entries are IOC limits with slippage px
            tif = (t.get("limit") or {}).get("tif", "Gtc")
            neutral = self.venue._handle_place_order(  # noqa: SLF001
                {"type": "market" if tif == "Ioc" else "limit",
                 "coin": coin, "is_buy": is_buy, "sz": sz,
                 "px": float(o.get("p", 0) or 0),
                 "reduce_only": bool(o.get("r")),
                 "client_oid": o.get("c")}, directive)
            nd = neutral.json()
            if not nd.get("ok"):
                statuses.append({"error": str(nd.get("reject"))})
            elif nd.get("filled_size"):
                statuses.append({"filled": {
                    "avgPx": str(nd["avg_px"]), "totalSz": str(nd["filled_size"]),
                    "oid": int(str(nd["oid"]).split("-")[-1])}})
            else:
                statuses.append({"resting": {
                    "oid": int(str(nd["oid"]).split("-")[-1])}})
        return self._hl_statuses(statuses)

    def _handle_cancel(self, action: Dict[str, Any],
                       directive: Optional[Dict[str, Any]]) -> GateResponse:
        statuses: List[Any] = []
        for c in action.get("cancels", []):
            oid = c.get("o") or c.get("oid")
            hit = None
            for full_oid in list(self.venue.orders):
                if str(full_oid).endswith("-%s" % oid) or full_oid == str(oid):
                    hit = full_oid
                    break
            if hit is None:
                statuses.append({"error": "Order was never placed, already "
                                          "canceled, or filled."})
                continue
            neutral = self.venue._handle_cancel(  # noqa: SLF001
                {"oid": hit}, directive)
            statuses.append("success" if neutral.json().get("ok")
                            else {"error": str(neutral.json().get("reject"))})
        return json_response({"status": "ok", "response": {
            "type": "cancel", "data": {"statuses": statuses}}})

    # entry point ---------------------------------------------------------------
    def __call__(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        url, b = ctx["url"], parse_body(ctx.get("body"))
        if url.rstrip("/").endswith("/info"):
            t = str(b.get("type"))
            dex = str(b.get("dex") or "")
            if t == "perpDexs":
                return json_response([None, {"name": "xyz",
                                             "full_name": "xyz HIP-3"}])
            if t == "meta":
                return json_response(self._meta(dex))
            if t == "spotMeta":
                return json_response({"universe": [], "tokens": []})
            if t == "metaAndAssetCtxs":
                metas = self._meta(dex)
                ctxs = [{"markPx": str(self.venue.marks.get(
                            self._internal_name(c), 0.0)),
                         "midPx": str(self.venue.marks.get(
                            self._internal_name(c), 0.0)),
                         "funding": "0.0", "openInterest": "0.0",
                         "dayNtlVlm": "0.0"}
                        for c in [u["name"] for u in metas["universe"]]]
                return json_response([metas, ctxs])
            if t == "allMids":
                return json_response(self._all_mids(dex))
            if t == "clearinghouseState":
                return json_response(self._positions_state(dex, directive))
            if t == "spotClearinghouseState":
                return json_response({"balances": [
                    {"coin": "USDC", "token": 0, "total": "0.0",
                     "hold": "0.0", "entryNtl": "0.0"}]})
            if t == "candleSnapshot":
                bars = self._candles(b)
                return json_response(bars if bars is not None else [])
            if t in ("userFills", "userFillsByTime"):
                return json_response(self._user_fills(directive))
            if t in ("frontendOpenOrders", "openOrders"):
                return json_response(self._open_orders(directive, dex))
            if t == "orderStatus":
                return self._order_status(b)
            return None  # unsimulated /info type → fail loud
        if url.rstrip("/").endswith("/exchange"):
            action = b.get("action") or {}
            at = action.get("type")
            if at == "order":
                return self._handle_order_action(action, directive)
            if at in ("cancel", "cancelByCloid"):
                return self._handle_cancel(action, directive)
            if at == "updateLeverage":
                coin = self._coin_by_asset(int(action.get("asset", -1)))
                if coin is None:
                    return json_response({"status": "err",
                                          "response": "Invalid asset"})
                max_lev = int(self.venue.meta[coin]["max_leverage"])
                if int(action.get("leverage", 0)) > max_lev:
                    return json_response({
                        "status": "err",
                        "response": "Cannot set leverage greater than max "
                                    "leverage %d for %s" % (max_lev, coin)})
                return json_response({"status": "ok",
                                      "response": {"type": "default"}})
            if at == "approveBuilderFee":
                return json_response({"status": "ok",
                                      "response": {"type": "default"}})
            return None
        return None


def _env(dry_run: bool = False) -> Dict[str, str]:
    empty_env = os.path.join(tempfile.mkdtemp(prefix="p2harness-hl-"),
                             "empty.env")
    with open(empty_env, "w"):
        pass
    return {
        "ENV_FILE": empty_env,           # never load the host's real .env
        "EXCHANGE": "hyperliquid",
        "NETWORK": "mainnet",
        # live-shaped paths by default (gate intercepts all); dry_run=True is
        # the F2 write-isolation lane: DRY_RUN=1 must keep EVERY write off
        # the transport.
        "DRY_RUN": "1" if dry_run else "0",
        "HL_LIVE_SIGNOFF": "1",          # config.py live-arm gate; safe: the
                                         # gate intercepts 100% of transport
                                         # and creds are fabricated throwaways
        "HYPERLIQUID_AGENT_PRIVATE_KEY": _FAKE_AGENT_KEY,
        "HYPERLIQUID_ACCOUNT_ADDRESS": _FAKE_ACCOUNT,
        "HL_BUILDER_ADDRESS": "",        # builder path off
        "HL_PREFLIGHT_DIRTY_SLEEP_SEC": "0.01",
        # neutralize the adapter's live rate-limit pacing under the harness
        # (cold-start ramp + per-fetch throttle sleeps are real-API defenses;
        # they would only slow the simulated matrix down)
        "HL_COLD_START_SEC": "0",
        "HL_MAIN_FETCH_SLEEP_SEC": "0",
        "HL_HIP3_FETCH_SLEEP_SEC": "0",
        "HL_HIP3_COLD_SLEEP_SEC": "0",
        "CANDLES_MIN_TTL_SEC": "0",
        "FLEET_CORE_HL_429_SLEEP": "0.02",
    }


def _construct_raw() -> Any:
    try:
        from bot.config import Settings  # bot root is cwd on the host
        from bot.exchange_hl import HLClient
    except ImportError:
        # Local rebuild tree: the bot package mirror lives under bots/hl/
        # (on the host cwd IS the bot root, so this fallback never fires).
        import sys
        root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        cand = os.path.join(root, "bots", "hl")
        if os.path.isdir(os.path.join(cand, "bot")) and cand not in sys.path:
            sys.path.insert(0, cand)
        try:
            from bot.config import Settings
            from bot.exchange_hl import HLClient
        except ImportError as e:
            raise BindingUnavailable(
                "hl: bot package / hyperliquid SDK not importable here (run "
                "on the hl host from the bot root): %s" % e)
    return HLClient(Settings.from_env())


def partial_outage_match(tctx: Dict[str, Any]) -> bool:
    """Partial-outage selector: HL's snapshot spans main dex + HIP-3 dex(es);
    downing ONLY the xyz-dex clearinghouseState leg models one-dex-of-several
    down (the exchange_hl.py:806 multi-scope snapshot law)."""
    return parse_body(tctx.get("body")).get("dex") == "xyz"


def build_context(dry_run: bool = False) -> BoundContext:
    ctx = build_venue_context(
        name=NAME, env=_env(dry_run=dry_run), classify=classify,
        make_responder=lambda venue: _HLResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw,
        partial_outage_match=partial_outage_match)
    # HL is a UNIFIED-margin account: spot USDC IS the perp collateral and
    # there is ONE account-equity number — equity_with_upnl() is the census-
    # H11 documented alias of account_value(). Two different seeded bases are
    # unrepresentable on this venue's wire; the equity/sizing check asserts
    # the alias identity instead (see checks._r_equity_and_sizing_bases).
    ctx.scenario.unified_equity_basis = True
    # HL resolves an unknown candles coin via a LIVE meta refresh — the
    # refresh-transport-down check (F7e) applies to this binding.
    ctx.scenario.meta_refresh_on_unknown = True
    return ctx


def smoke_construct() -> str:
    return smoke_construct_venue(
        name=NAME, env=_env(), classify=classify,
        make_responder=lambda venue: _HLResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw)
