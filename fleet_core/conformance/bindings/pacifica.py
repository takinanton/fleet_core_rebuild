"""bindings.pacifica — Pacifica venue binding (lazy; solders only on host).

Transport layer inspected (bots/pacifica/bot/exchange_pacifica.py):
  * ONE requests.Session (constructor :190) carrying the global RL pacer
    (:198) and the default-timeout installer (:210); every REST call —
    signed and unsigned — goes through it. No SDK, no aiohttp.
→ lowest interception layer = requests.Session.send (patch_requests); the
  adapter's RL pacer + timeout wrapper run unmodified above it.

Constructor needs (:163): PACIFICA_AGENT_PRIVATE_KEY (base58, 64-byte solders
keypair — an EPHEMERAL throwaway is generated at bind time, never persisted),
optional PACIFICA_ACCOUNT_ADDRESS (defaults to agent pubkey), DRY_RUN=0
(live-shaped write paths — the gate intercepts everything), plus bot.config
required envs (EXCHANGE=pacifica). Constructor bootstraps meta via
GET {REST_URL}/info.

LOCAL-RUN SUPPORT (this machine has no venue SDKs): when `solders`/`base58`
are not importable, inert in-module STUBS are installed (random-free, sign()
returns zero bytes) — the responder never verifies signatures, so contract
conformance is identical; on hosts the real libs import first and the stubs
never engage. Likewise, when `bot.*` is not importable (running from the
rebuild root instead of a bot root), the local mirror bots/pacifica is
appended to sys.path — on hosts the first import succeeds and the fallback
never engages.

Endpoints simulated (grep of the adapter + wrapper): GET /info, /info/prices,
/kline, /book, /orders (signed get_orders → op `triggers`; unsigned
account-param listing → op `orders`), /orders/history, /trades/history;
signed GET /account, /positions; POST /orders/create_market,
/orders/stop/create, /orders/create, /orders/(stop/)cancel,
/account/leverage, /account/margin. Anything else → None →
UninterceptedRealCall (host-iteration loop).

OID SHAPE: live Pacifica order ids are NUMERIC — the responder renders the
FakeVenue's canonical "fk-<n>" ids as their numeric suffix everywhere
(create acks, listings, history, fills). FakeScenario._oid_matches is
suffix-tolerant by design (same rule as HL int oids), and cancel requests
are canonicalized back ("1001" → "fk-1001").
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
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

NAME = "pacifica"
TRANSPORTS = ("requests",)


def _path(url: str) -> str:
    return urlparse(url).path


def _num_oid(oid: Any) -> str:
    """Render a FakeVenue canonical oid ("fk-1001") as the numeric string a
    live Pacifica response would carry ("1001")."""
    return str(oid).rsplit("-", 1)[-1]


def classify(method: str, url: str, body: Any) -> str:
    p = _path(url)
    m = method.upper()
    if m == "GET":
        if p.endswith("/info"):
            return "meta"
        if "/info/prices" in p:
            return "mark"
        if "/kline" in p:
            return "candles"
        if "/positions" in p:
            return "positions"
        if p.endswith("/account"):
            return "account"
        if "/trades/history" in p:
            return "fills"
        if "/orders/history" in p:
            return "orders"
        if p.endswith("/orders"):
            # ONE live endpoint, two live auth shapes: the signed get_orders
            # read is the per-coin SL-liveness/trigger-confirm path (op
            # `triggers`); the unsigned account-param listing is the
            # account-wide order-book read (op `orders`). Distinguishing by
            # the signature query param keeps both fault-targetable.
            qs = parse_qs(urlparse(url).query)
            return "triggers" if "signature" in qs else "orders"
        if "/book" in p:
            return "other"
        return "other"
    if m == "POST":
        if "cancel" in p:
            # MUST precede the /orders/stop check: /orders/stop/cancel is a
            # cancel, not a trigger placement.
            return "cancel_order"
        if "/orders/create_market" in p:
            return "place_order"
        if "/orders/stop" in p:
            return "place_trigger"
        if "/orders/create" in p:
            return "place_order"
        if "leverage" in p or "/account/margin" in p:
            return "leverage"
        return "other"
    return "other"


class _PacificaResponder:
    """Renders FakeVenue truth into Pacifica REST shapes (data-enveloped:
    {"success": true, "data": ...})."""

    def __init__(self, venue: FakeVenue) -> None:
        self.venue = venue

    @staticmethod
    def _ok(data: Any) -> GateResponse:
        return json_response({"success": True, "data": data})

    @staticmethod
    def _err(msg: str) -> GateResponse:
        return json_response({"success": False, "error": msg})

    def _coins(self) -> List[str]:
        # pacifica has no HIP-3 analogue — serve main-scope coins only
        return sorted(c for c, m in self.venue.meta.items()
                      if m.get("scope", "main") == "main")

    def _info(self) -> GateResponse:
        return self._ok([
            {"symbol": c,
             "lot_size": str(self.venue.meta[c]["min_size"]),
             "min_order_size": str(self.venue.meta[c]["min_size"]),
             "tick_size": str(self.venue.meta[c]["tick"]),
             "max_leverage": int(self.venue.meta[c]["max_leverage"]),
             "min_notional": 10.0,
             "isolated_only": False}
            for c in self._coins()])

    def _prices(self) -> GateResponse:
        return self._ok([
            {"symbol": c, "mark": str(self.venue.marks.get(c, 0.0)),
             "mid": str(self.venue.marks.get(c, 0.0)),
             "oracle": str(self.venue.marks.get(c, 0.0)),
             "funding": "0.0", "timestamp": 0}
            for c in self._coins()])

    def _kline(self, qs: Dict[str, str]) -> GateResponse:
        coin = qs.get("symbol", "")
        tf = qs.get("interval", "")
        neutral = self.venue._serve_candles(  # noqa: SLF001
            {"coin": coin, "tf": tf, "limit": qs.get("limit", "500")})
        if neutral.status != 200:
            return self._ok([])  # adapter maps empty → its own handling
        return self._ok([
            {"t": int(r[0]), "o": str(r[1]), "h": str(r[2]), "l": str(r[3]),
             "c": str(r[4]), "v": str(r[5]), "T": int(r[0])}
            for r in neutral.json().get("bars", [])])

    def _positions(self, directive: Optional[Dict[str, Any]] = None
                   ) -> GateResponse:
        rows = []
        for scope in self.venue.positions:
            for coin, p in self.venue.positions[scope].items():
                side = "bid" if p["size_signed"] > 0 else "ask"
                rows.append({"symbol": coin, "amount": str(abs(p["size_signed"])),
                             "side": side,
                             "entry_price": str(p["entry_px"]),
                             "liquidation_price": (
                                 None if p.get("liq_px") is None
                                 else str(p["liq_px"])),
                             "margin": "0.0", "funding": "0.0",
                             "isolated": False})
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "amount")
        return self._ok(rows)

    def _account(self) -> GateResponse:
        # F5 (final): the deployed sizing basis is `account_equity` — the
        # wrapper reads it for BOTH account_value() and equity_with_upnl()
        # (unified-basis venue; the check seeds equity == account_value).
        # `balance` stays rendered because the live wire carries it, but no
        # contract read consumes it.
        return self._ok({
            "balance": str(self.venue.account_value),
            "account_equity": str(self.venue.equity),
            "total_margin_used": str(self.venue.margin_used),
            "available_to_spend": str(self.venue.account_value),
            "pending_balance": "0.0",
        })

    def _orders(self, directive: Optional[Dict[str, Any]] = None
                ) -> GateResponse:
        rows = []
        for o in self.venue.orders.values():
            rows.append({
                "order_id": _num_oid(o["oid"]), "symbol": o["coin"],
                "side": o["side"], "amount": str(o["size"]),
                "price": str(o["limit_px"] or 0),
                "stop_price": str(o["trigger_px"] or 0),
                "order_type": ("stop_market" if o["is_trigger"] else "limit"),
                "reduce_only": bool(o["reduce_only"]),
                "client_order_id": o.get("client_oid"),
            })
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "amount")
        return self._ok(rows)

    def _order_history(self) -> GateResponse:
        """Live shape the adapters' fill-confirm polls read: one terminal
        row per fill (order_status=filled + filled_amount/avg px from VENUE
        TRUTH) plus open rows for still-resting orders."""
        rows: List[Dict[str, Any]] = []
        for f in self.venue.fills:
            rows.append({
                "order_id": _num_oid(f["oid"]),
                "client_order_id": f.get("client_oid"),
                "symbol": f["coin"],
                "side": f["side"],
                "order_status": "filled",
                "filled_amount": str(f["size"]),
                "average_filled_price": str(f["px"]),
                "created_at": int(f["time"] * 1000),
            })
        for o in self.venue.orders.values():
            rows.append({
                "order_id": _num_oid(o["oid"]),
                "client_order_id": o.get("client_oid"),
                "symbol": o["coin"],
                "side": o["side"],
                "order_status": "open",
                "filled_amount": "0",
                "average_filled_price": "0",
                "created_at": 0,
            })
        return self._ok(rows[:20])

    def _fills(self, directive: Optional[Dict[str, Any]] = None
               ) -> GateResponse:
        rows = [
            {"symbol": f["coin"], "price": str(f["px"]),
             "amount": str(f["size"]), "side": f["side"],
             "event_type": ("close" if f.get("reduce_only") else "open"),
             "client_order_id": f.get("client_oid"),
             "order_id": _num_oid(f["oid"]),
             "pnl": "0", "fee": "0",
             "created_at": int(f["time"] * 1000),
             "timestamp": int(f["time"] * 1000)}
            for f in self.venue.fills[:100]]
        if directive is not None and directive.get("mode") == "corrupt_item":
            rows = corrupt_one(rows, "amount")
        return self._ok(rows)

    # writes ------------------------------------------------------------------
    def _create_market(self, b: Dict[str, Any],
                       directive: Optional[Dict[str, Any]]) -> GateResponse:
        # payload may be nested under signature envelope
        d = b.get("data") or b
        coin = str(d.get("symbol", ""))
        is_buy = str(d.get("side", "")).lower() in ("bid", "buy")
        sz = float(d.get("amount", 0) or 0)
        neutral = self.venue._handle_place_order(  # noqa: SLF001
            {"type": "market", "coin": coin, "is_buy": is_buy, "sz": sz,
             "reduce_only": bool(d.get("reduce_only")),
             "client_oid": d.get("client_order_id")}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(str(nd.get("reject")))
        return self._ok({"order_id": _num_oid(nd["oid"])})

    def _create_stop(self, b: Dict[str, Any],
                     directive: Optional[Dict[str, Any]]) -> GateResponse:
        d = b.get("data") or b
        coin = str(d.get("symbol", ""))
        stop = d.get("stop_order") or d
        neutral = self.venue._handle_place_trigger(  # noqa: SLF001
            {"coin": coin,
             "is_buy": str(d.get("side", "")).lower() in ("bid", "buy"),
             "sz": float(d.get("amount", 0) or stop.get("amount", 0) or 0),
             "trigger_px": float(stop.get("stop_price", 0) or 0),
             "client_oid": (stop.get("client_order_id")
                            or d.get("client_order_id"))}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(str(nd.get("reject")))
        return self._ok({"order_id": _num_oid(nd["oid"])})

    def _create_limit(self, b: Dict[str, Any],
                      directive: Optional[Dict[str, Any]]) -> GateResponse:
        d = b.get("data") or b
        coin = str(d.get("symbol", ""))
        neutral = self.venue._handle_place_order(  # noqa: SLF001
            {"type": "limit", "coin": coin,
             "is_buy": str(d.get("side", "")).lower() in ("bid", "buy"),
             "sz": float(d.get("amount", 0) or 0),
             "px": float(d.get("price", 0) or 0),
             "reduce_only": bool(d.get("reduce_only")),
             "client_oid": d.get("client_order_id")}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(str(nd.get("reject")))
        if nd.get("filled_size"):
            return self._ok({"order_id": _num_oid(nd["oid"]),
                             "filled_amount": str(nd["filled_size"]),
                             "average_price": str(nd["avg_px"])})
        return self._ok({"order_id": _num_oid(nd["oid"])})

    def _cancel(self, b: Dict[str, Any],
                directive: Optional[Dict[str, Any]]) -> GateResponse:
        d = b.get("data") or b
        oid = str(d.get("order_id", ""))
        # canonicalize the live numeric id back to the venue's "fk-<n>" key
        if oid not in self.venue.orders and ("fk-" + oid) in self.venue.orders:
            oid = "fk-" + oid
        neutral = self.venue._handle_cancel(  # noqa: SLF001
            {"oid": oid}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return self._err(str(nd.get("reject")))
        return self._ok({"cancelled": _num_oid(oid)})

    def _leverage(self, b: Dict[str, Any]) -> GateResponse:
        d = b.get("data") or b
        coin = str(d.get("symbol", ""))
        meta = self.venue.meta.get(coin)
        if meta is None:
            return self._err("unknown_symbol %s" % coin)
        try:
            lev = int(d.get("leverage", 0) or 0)
        except (TypeError, ValueError):
            return self._err("invalid leverage")
        if lev > int(meta["max_leverage"]):
            return self._err("leverage_above_max")
        return self._ok({"leverage_updated": True})

    # entry point ---------------------------------------------------------------
    def __call__(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        url = ctx["url"]
        p = _path(url)
        qs = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        b = parse_body(ctx.get("body"))
        m = ctx["method"]
        if m == "GET":
            if p.endswith("/info"):
                return self._info()
            if "/info/prices" in p:
                return self._prices()
            if "/kline" in p:
                return self._kline(qs)
            if "/positions" in p:
                return self._positions(directive)
            if p.endswith("/account"):
                return self._account()
            if "/orders/history" in p:
                return self._order_history()
            if p.endswith("/orders"):
                return self._orders(directive)
            if "/trades/history" in p:
                return self._fills(directive)
            if "/book" in p:
                coin = qs.get("symbol", "BTC")
                mark = self.venue.marks.get(coin, 0.0)
                return self._ok({"l": [[{"p": str(mark * 0.999), "a": "10"}],
                                       [{"p": str(mark * 1.001), "a": "10"}]]})
            return None
        if m == "POST":
            if "cancel" in p:
                # must precede /orders/stop: /orders/stop/cancel is a cancel
                return self._cancel(b, directive)
            if "/orders/create_market" in p:
                return self._create_market(b, directive)
            if "/orders/stop" in p:
                return self._create_stop(b, directive)
            if "/orders/create" in p:
                return self._create_limit(b, directive)
            if "/account/margin" in p:
                return self._ok({"margin_mode_updated": True})
            if "leverage" in p:
                return self._leverage(b)
            return None
        return None


# ---------------------------------------------------------------------------
# local-run stubs: solders/base58 (signature bytes are NEVER verified by the
# responder; hosts import the real libs first and the stubs never engage)
# ---------------------------------------------------------------------------

class _StubKeypair:
    """Inert throwaway keypair for gate-intercepted local runs only."""

    _n = 0

    def __init__(self) -> None:
        _StubKeypair._n += 1
        self._pub = "FKSTUBPUBKEY%08d" % _StubKeypair._n

    @classmethod
    def from_base58_string(cls, s: str) -> "_StubKeypair":
        return cls()

    def pubkey(self) -> str:
        return self._pub

    def sign_message(self, message: bytes) -> bytes:
        return b"\x00" * 64

    def __str__(self) -> str:
        return "fk-stub-private-key"


def _ensure_signing_libs() -> None:
    try:
        import solders.keypair  # noqa: F401 — real lib present (host)
    except ImportError:
        solders_mod = types.ModuleType("solders")
        keypair_mod = types.ModuleType("solders.keypair")
        keypair_mod.Keypair = _StubKeypair  # type: ignore[attr-defined]
        solders_mod.keypair = keypair_mod  # type: ignore[attr-defined]
        sys.modules.setdefault("solders", solders_mod)
        sys.modules["solders.keypair"] = keypair_mod
    try:
        import base58  # noqa: F401
    except ImportError:
        import binascii

        b58_mod = types.ModuleType("base58")
        b58_mod.b58encode = (  # type: ignore[attr-defined]
            lambda b: binascii.hexlify(bytes(b)))
        sys.modules["base58"] = b58_mod


def _env(dry_run: bool = False) -> Dict[str, str]:
    _ensure_signing_libs()
    from solders.keypair import Keypair  # real on host, stub locally
    kp = Keypair()  # EPHEMERAL throwaway — never funded, never persisted
    return {
        "EXCHANGE": "pacifica",
        "NETWORK": "mainnet",
        # live-shaped paths by default; dry_run=True = the F2 write-isolation
        # lane (raw _signed_request gates POSTs before transport)
        "DRY_RUN": "1" if dry_run else "0",
        "PACIFICA_AGENT_PRIVATE_KEY": str(kp),
        "PACIFICA_PRIVATE_KEY": str(kp),
        "PACIFICA_ACCOUNT_ADDRESS": str(kp.pubkey()),
        "PACIFICA_REST_MIN_INTERVAL_MS": "0",   # no pacing sleeps in tests
        "PACIFICA_KLINE_MIN_INTERVAL_MS": "0",
    }


def _import_bot():
    try:
        from bot.config import Settings
        from bot.exchange_pacifica import PacificaClient
        return Settings, PacificaClient
    except ImportError as e:
        # local mirror fallback (rebuild root instead of a bot root)
        root = Path(__file__).resolve().parents[3]
        cand = root / "bots" / "pacifica"
        if (cand / "bot" / "exchange_pacifica.py").exists() and \
                str(cand) not in sys.path:
            sys.path.insert(0, str(cand))
            try:
                from bot.config import Settings
                from bot.exchange_pacifica import PacificaClient
                return Settings, PacificaClient
            except ImportError as e2:
                raise BindingUnavailable(
                    "pacifica: bot package not importable even from the "
                    "local mirror %s: %s" % (cand, e2))
        raise BindingUnavailable(
            "pacifica: bot package not importable here (run on the pacifica "
            "host from the bot root): %s" % e)


def _construct_raw() -> Any:
    Settings, PacificaClient = _import_bot()
    return PacificaClient(Settings.from_env())


def partial_outage_match(tctx: Dict[str, Any]) -> bool:
    """Partial-outage selector: pacifica's positions snapshot is ONE signed
    GET /positions — there is no multi-scope subset, so the selector is that
    single leg and the partial-outage law degenerates to a full-read outage
    (still must surface as ReadUnknown; no false alarm, nothing untested)."""
    return tctx.get("op") == "positions"


def build_context(dry_run: bool = False) -> BoundContext:
    ctx = build_venue_context(
        name=NAME, env=_env(dry_run=dry_run), classify=classify,
        make_responder=lambda venue: _PacificaResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw,
        partial_outage_match=partial_outage_match)
    # ONE transport call per open_positions() snapshot (single signed GET) —
    # lets sequenced faults (fire_after) target the FINAL readback precisely.
    ctx.scenario.positions_read_transport_calls = 1
    # F5 DECISION (final): the deployed sizing basis IS /account
    # account_equity (incl uPnL) — account_value() and equity_with_upnl()
    # read the SAME venue field, so two different seeded bases are
    # unrepresentable (unified-basis venue; see the wrapper SEMANTICS NOTE).
    ctx.scenario.unified_equity_basis = True
    # F5 DECISION (final): the deployed silent clamp-to-max is preserved —
    # an over-max update_leverage ask clamps to the venue max and succeeds
    # (the venue's own above-max reject proves the clamp happened).
    ctx.scenario.leverage_clamp_to_max = True
    return ctx


def smoke_construct() -> str:
    return smoke_construct_venue(
        name=NAME, env=_env(), classify=classify,
        make_responder=lambda venue: _PacificaResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw)
