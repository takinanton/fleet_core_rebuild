"""bindings.fake — pure-python reference venue + reference adapter.

Two jobs:

1. `FakeVenue` — an in-memory exchange (positions in two scopes to model
   HL's main+HIP-3 multi-scope snapshot law, orders, SL triggers, fills,
   marks, candles, account) serving a tiny REST-ish surface through the
   TransportGate responder protocol. It honors the injector's write
   directives (`no_land`, `partial`) — acks the order, suppresses/scales the
   side effect — which is what makes readback conformance testable — and the
   read directive (`corrupt_item`) — serves a valid envelope with exactly one
   unparseable position/order row, truth store untouched.
   FakeVenue is ALSO the shared truth store the real-venue bindings render
   into venue-shaped JSON, so every binding drives the same scenarios.

2. `FakeExchangeClient` — the reference CORRECT implementation of
   fleet_core.exchange_api.ExchangeClient: verified-or-raise reads, readback-
   confirmed writes, bounded timeouts on every call, bounded 429 retry, cache
   discipline with StaleData. The conformance suite runs green against it
   locally with zero third-party deps; it is the executable spec the venue
   bindings are held to.

Candles note: the contract annotates candles() -> pd.DataFrame; to keep this
module stdlib-pure it returns `MiniFrame`, a duck-typed stand-in exposing the
subset the suite (and scanner-style consumers) touch: .columns, len(), .empty,
.col(name). Real venue bindings return real DataFrames; the suite only uses
the common subset.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence
from urllib.parse import parse_qs, urlparse

from fleet_core.exchange_api import (
    ExchangeClient,
    FillResult,
    FlatResult,
    OpenOrderInfo,
    PositionInfo,
    RateLimited,
    ReadUnknown,
    SLOrderInfo,
    StaleData,
    VenueRejected,
    WriteUnconfirmed,
)
from fleet_core.conformance.faults import (
    GateResponse,
    TransportConnectError,
    TransportGate,
    TransportTimeout,
    block_sockets,
    json_response,
)
from fleet_core.conformance.bindings import BoundContext

VENUE = "fake"
BASE = "fake://venue"

TF_MS: Dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

SCOPES = ("main", "xyz")  # two scopes -> partial-snapshot law is testable

_oid_counter = itertools.count(1000)


def _new_oid() -> str:
    return "fk-%d" % next(_oid_counter)


def corrupt_one(rows: List[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
    """corrupt_item directive support (shared by all venue responders):
    mangle exactly ONE row — the last — of a list payload so the envelope
    stays valid but that item is unparseable. Copies the row so the venue
    truth store is never touched; the checks cross-check against truth."""
    if rows:
        rows[-1] = dict(rows[-1])
        rows[-1][field] = "corrupt-item-0xDEAD"
    return rows


# ===========================================================================
# The venue truth store + responder
# ===========================================================================

class FakeVenue:
    """In-memory exchange. All mutation goes through place/cancel handlers so
    the injector's no_land/partial directives have a single enforcement
    point."""

    DEFAULT_META = {
        "BTC": {"min_size": 0.001, "tick": 0.5, "max_leverage": 50, "scope": "main"},
        "ETH": {"min_size": 0.01, "tick": 0.05, "max_leverage": 50, "scope": "main"},
        "XRP": {"min_size": 1.0, "tick": 0.0001, "max_leverage": 20, "scope": "main"},
        "xyz_GOLD": {"min_size": 0.1, "tick": 0.1, "max_leverage": 10, "scope": "xyz"},
    }
    DEFAULT_MARKS = {"BTC": 50_000.0, "ETH": 2_500.0, "XRP": 0.5,
                     "xyz_GOLD": 2_400.0}
    SLIP = 0.0005  # taker slip the venue applies to market fills

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.meta: Dict[str, Dict[str, Any]] = json.loads(
            json.dumps(self.DEFAULT_META))
        self.marks: Dict[str, float] = dict(self.DEFAULT_MARKS)
        # positions: scope -> coin -> {size_signed, entry_px, liq_px}
        self.positions: Dict[str, Dict[str, Dict[str, Any]]] = {
            s: {} for s in SCOPES}
        # resting orders (limits + triggers): oid -> order dict
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.fills: List[Dict[str, Any]] = []
        self.equity: float = 10_000.0
        self.account_value: float = 10_000.0
        self.margin_used: float = 0.0
        # candles: (coin, tf) -> {"lag_bars": float, "n": int}
        self.candle_cfg: Dict[Any, Dict[str, float]] = {}

    # ------------------------------------------------------------- seeding

    def seed_position(self, coin: str, size_signed: float, entry_px: float,
                      liq_px: Optional[float] = None,
                      scope: Optional[str] = None) -> None:
        scope = scope or self.meta.get(coin, {}).get("scope", "main")
        self.positions[scope][coin] = {
            "size_signed": float(size_signed),
            "entry_px": float(entry_px),
            "liq_px": liq_px,
        }

    def coin_scope(self, coin: str) -> str:
        return self.meta.get(coin, {}).get("scope", "main")

    def find_position(self, coin: str) -> Optional[Dict[str, Any]]:
        for s in SCOPES:
            if coin in self.positions[s]:
                return self.positions[s][coin]
        return None

    # ------------------------------------------------------------ mutation

    def _apply_fill(self, coin: str, is_buy: bool, sz: float, px: float,
                    client_oid: Optional[str], reduce_only: bool) -> Dict[str, Any]:
        scope = self.coin_scope(coin)
        book = self.positions[scope]
        pos = book.get(coin)
        delta = sz if is_buy else -sz
        if pos is None:
            book[coin] = {"size_signed": delta, "entry_px": px, "liq_px": None}
        else:
            new_size = pos["size_signed"] + delta
            if abs(new_size) < 1e-12:
                del book[coin]
            else:
                if (pos["size_signed"] > 0) == (delta > 0):
                    tot = abs(pos["size_signed"]) + sz
                    pos["entry_px"] = (
                        pos["entry_px"] * abs(pos["size_signed"]) + px * sz) / tot
                pos["size_signed"] = new_size
        fill = {
            "oid": _new_oid(),
            "client_oid": client_oid,
            "coin": coin,
            "side": "buy" if is_buy else "sell",
            "px": px,
            "size": sz,
            "reduce_only": reduce_only,
            "time": time.time(),
        }
        self.fills.insert(0, fill)  # newest first
        return fill

    def _reject(self, reason: str) -> GateResponse:
        return json_response({"ok": False, "reject": reason})

    def _handle_place_order(self, body: Dict[str, Any],
                            directive: Optional[Dict[str, Any]]) -> GateResponse:
        coin = body.get("coin", "")
        if coin not in self.meta:
            return self._reject("unknown_symbol %s" % coin)
        sz = float(body.get("sz", 0.0))
        is_buy = bool(body.get("is_buy"))
        reduce_only = bool(body.get("reduce_only", False))
        typ = body.get("type", "market")
        client_oid = body.get("client_oid")
        if sz < float(self.meta[coin]["min_size"]):
            return self._reject(
                "below_min_size: %s < %s" % (sz, self.meta[coin]["min_size"]))
        if reduce_only:
            pos = self.find_position(coin)
            delta = sz if is_buy else -sz
            if pos is None or (pos["size_signed"] > 0) == (delta > 0):
                return self._reject("reduce_only_would_increase")
            sz = min(sz, abs(pos["size_signed"]))

        oid = _new_oid()
        if directive is not None and directive.get("mode") == "no_land":
            # acked, NO side effect, NO authoritative fill fields
            return json_response({"ok": True, "ack": True, "oid": oid})

        if typ == "limit":
            px = float(body.get("px", 0.0))
            if px <= 0:
                return self._reject("bad_limit_px")
            mark = self.marks.get(coin, 0.0)
            crosses = (is_buy and px >= mark) or ((not is_buy) and px <= mark)
            if crosses:
                fill = self._apply_fill(coin, is_buy, sz, px, client_oid,
                                        reduce_only)
                return json_response({"ok": True, "oid": oid,
                                      "filled_size": fill["size"],
                                      "avg_px": fill["px"]})
            self.orders[oid] = {
                "oid": oid, "coin": coin,
                "side": "buy" if is_buy else "sell", "size": sz,
                "limit_px": px, "trigger_px": None,
                "reduce_only": reduce_only, "is_trigger": False,
                "client_oid": client_oid,
            }
            # resting ack: order id only — listing readback is authoritative
            return json_response({"ok": True, "ack": True, "oid": oid})

        # market
        mark = self.marks.get(coin)
        if not mark:
            return self._reject("no_mark")
        px = mark * (1 + self.SLIP) if is_buy else mark * (1 - self.SLIP)
        if directive is not None and directive.get("mode") == "partial":
            landed = sz * float(directive.get("ratio", 0.5))
            self._apply_fill(coin, is_buy, landed, px, client_oid, reduce_only)
            # ack WITHOUT fill fields — truth only via readback
            return json_response({"ok": True, "ack": True, "oid": oid})
        fill = self._apply_fill(coin, is_buy, sz, px, client_oid, reduce_only)
        return json_response({"ok": True, "oid": oid,
                              "filled_size": fill["size"],
                              "avg_px": fill["px"]})

    def _handle_place_trigger(self, body: Dict[str, Any],
                              directive: Optional[Dict[str, Any]]) -> GateResponse:
        coin = body.get("coin", "")
        if coin not in self.meta:
            return self._reject("unknown_symbol %s" % coin)
        sz = float(body.get("sz", 0.0))
        trigger_px = float(body.get("trigger_px", 0.0))
        if trigger_px <= 0:
            return self._reject("bad_trigger_px")
        if sz < float(self.meta[coin]["min_size"]):
            return self._reject(
                "trigger_below_min_size: %s < %s"
                % (sz, self.meta[coin]["min_size"]))
        oid = _new_oid()
        if directive is not None and directive.get("mode") == "no_land":
            # THE naked-success class: acked with an oid, never listed live
            return json_response({"ok": True, "oid": oid})
        self.orders[oid] = {
            "oid": oid, "coin": coin,
            "side": "buy" if body.get("is_buy") else "sell", "size": sz,
            "limit_px": None, "trigger_px": trigger_px,
            "reduce_only": True, "is_trigger": True,
            "client_oid": body.get("client_oid"),
        }
        return json_response({"ok": True, "oid": oid})

    def _handle_cancel(self, body: Dict[str, Any],
                       directive: Optional[Dict[str, Any]]) -> GateResponse:
        oid = str(body.get("oid", ""))
        if oid not in self.orders:
            return self._reject("not_found")
        if directive is not None and directive.get("mode") == "no_land":
            return json_response({"ok": True})  # acked, order still live
        del self.orders[oid]
        return json_response({"ok": True})

    # ----------------------------------------------------------- responder

    def respond(self, ctx: Dict[str, Any],
                directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        url = ctx["url"]
        if not url.startswith(BASE):
            return None
        parsed = urlparse(url)
        path = parsed.path.lstrip("/")
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        body = ctx.get("body") or {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        method = ctx["method"]

        if method == "GET":
            if path == "meta":
                return json_response({"coins": self.meta,
                                      "tfs": sorted(TF_MS)})
            if path == "positions":
                scope = qs.get("scope", "main")
                if scope not in self.positions:
                    return json_response({"error": "bad scope"}, status=400)
                rows = [
                    {"coin": c, "size_signed": p["size_signed"],
                     "entry_px": p["entry_px"], "liq_px": p["liq_px"]}
                    for c, p in self.positions[scope].items()
                ]
                if directive is not None and \
                        directive.get("mode") == "corrupt_item":
                    rows = corrupt_one(rows, "size_signed")
                return json_response({"scope": scope, "positions": rows})
            if path == "mark":
                coin = qs.get("coin", "")
                if coin not in self.marks:
                    return json_response({"error": "unknown coin"}, status=400)
                return json_response({"coin": coin, "mark": self.marks[coin]})
            if path == "candles":
                return self._serve_candles(qs)
            if path == "account":
                return json_response({"equity": self.equity,
                                      "account_value": self.account_value,
                                      "margin_used": self.margin_used})
            if path == "orders":
                rows = [dict(o) for o in self.orders.values()]
                if directive is not None and \
                        directive.get("mode") == "corrupt_item":
                    rows = corrupt_one(rows, "size")
                return json_response({"orders": rows})
            if path == "triggers":
                coin = qs.get("coin")
                rows = [o for o in self.orders.values() if o["is_trigger"]
                        and (coin is None or o["coin"] == coin)]
                return json_response({"triggers": rows})
            if path == "fills":
                rows = [dict(f) for f in self.fills[:50]]
                if directive is not None and \
                        directive.get("mode") == "corrupt_item":
                    rows = corrupt_one(rows, "size")
                return json_response({"fills": rows})
            return None

        if method == "POST":
            if path == "order":
                return self._handle_place_order(body, directive)
            if path == "trigger":
                return self._handle_place_trigger(body, directive)
            if path == "cancel":
                return self._handle_cancel(body, directive)
            if path == "leverage":
                coin = body.get("coin", "")
                if coin not in self.meta:
                    return self._reject("unknown_symbol %s" % coin)
                if int(body.get("leverage", 0)) > int(
                        self.meta[coin]["max_leverage"]):
                    return self._reject("leverage_above_max")
                return json_response({"ok": True})
            return None
        return None

    def _serve_candles(self, qs: Dict[str, str]) -> GateResponse:
        coin = qs.get("coin", "")
        tf = qs.get("tf", "")
        limit = int(qs.get("limit", "200"))
        if tf not in TF_MS:
            return json_response({"error": "bad interval"}, status=400)
        if coin not in self.meta:
            return json_response({"error": "unknown coin"}, status=400)
        cfg = self.candle_cfg.get((coin, tf), {"lag_bars": 0.0, "n": 60})
        tf_ms = TF_MS[tf]
        now_ms = int(time.time() * 1000)
        boundary = (now_ms // tf_ms) * tf_ms
        # last CLOSED bar open-time, shifted back by the configured lag
        last_open = boundary - tf_ms - int(float(cfg["lag_bars"]) * tf_ms)
        n = min(int(cfg["n"]), limit)
        px = self.marks.get(coin, 100.0)
        bars = []
        for i in range(n):
            t = last_open - (n - 1 - i) * tf_ms
            o = px * (1 + 0.001 * ((i % 7) - 3))
            c = px * (1 + 0.001 * ((i % 5) - 2))
            bars.append([t, o, max(o, c) * 1.001, min(o, c) * 0.999, c, 10.0 + i])
        return json_response({"bars": bars})


# ===========================================================================
# Scenario control — the venue-truth API every binding must expose
# ===========================================================================

class FakeScenario:
    """Reference ScenarioControl. Real-venue bindings wrap the SAME FakeVenue
    truth store, so this class is reused by them as-is."""

    #: transport calls one open_positions() snapshot costs (fake: 2 scopes).
    #: Venue bindings override to their per-scope fan-out (HL: 1 + #HIP3 dexes)
    #: so sequenced faults (fire_after) can target the FINAL readback.
    positions_read_transport_calls = 2

    def __init__(self, venue: FakeVenue) -> None:
        self.venue = venue

    def reset(self) -> None:
        self.venue.reset()

    # seeding -------------------------------------------------------------
    def seed_mark(self, coin: str, px: float) -> None:
        self.venue.marks[coin] = float(px)

    def seed_position(self, coin: str, size_signed: float, entry_px: float,
                      liq_px: Optional[float] = None) -> None:
        self.venue.seed_position(coin, size_signed, entry_px, liq_px)

    def seed_candles(self, coin: str, tf: str, n: int = 60,
                     lag_bars: float = 0.0) -> None:
        self.venue.candle_cfg[(coin, tf)] = {"lag_bars": lag_bars, "n": n}

    def seed_account(self, equity: float, account_value: Optional[float] = None,
                     margin_used: float = 0.0) -> None:
        self.venue.equity = float(equity)
        self.venue.account_value = float(
            equity if account_value is None else account_value)
        self.venue.margin_used = float(margin_used)

    def set_min_size(self, coin: str, min_size: float) -> None:
        self.venue.meta[coin]["min_size"] = float(min_size)

    def seed_order(self, coin: str, side: str, size: float,
                   limit_px: Optional[float] = None,
                   trigger_px: Optional[float] = None,
                   reduce_only: bool = False,
                   is_trigger: bool = False) -> str:
        """Seed a RESTING order/trigger directly into the venue truth store
        (bypasses the place handlers — no directive interaction). Returns the
        canonical venue oid."""
        oid = _new_oid()
        self.venue.orders[oid] = {
            "oid": oid, "coin": coin, "side": side, "size": float(size),
            "limit_px": None if limit_px is None else float(limit_px),
            "trigger_px": None if trigger_px is None else float(trigger_px),
            "reduce_only": bool(reduce_only), "is_trigger": bool(is_trigger),
            "client_oid": None,
        }
        return oid

    # venue truth for cross-checks -----------------------------------------
    def position(self, coin: str) -> Optional[Dict[str, Any]]:
        return self.venue.find_position(coin)

    def trigger_oids(self, coin: Optional[str] = None) -> List[str]:
        return [o["oid"] for o in self.venue.orders.values()
                if o["is_trigger"] and (coin is None or o["coin"] == coin)]

    def trigger_px_of(self, oid: Any) -> Optional[float]:
        """Venue truth: the trigger price of the live trigger `oid` (as
        returned by the client under test), or None when no such live
        trigger exists. Used by the foreign-trigger-adoption check (F6)."""
        for o in self.venue.orders.values():
            if o["is_trigger"] and self._oid_matches(oid, o["oid"]):
                return o["trigger_px"]
        return None

    def order_oids(self) -> List[str]:
        return list(self.venue.orders.keys())

    # oid canonicalization: the venue's canonical ids are "fk-<n>"; venue
    # renderers may expose only the numeric suffix (e.g. HL int oids). This
    # matching rule lives HERE — on the mock's control surface — so contract
    # checks never encode mock id shapes themselves.
    @staticmethod
    def _oid_matches(client_oid: Any, venue_oid: Any) -> bool:
        a, b = str(client_oid), str(venue_oid)
        if not a or not b:
            return False
        if a == b:
            return True
        return a.endswith("-" + b) or b.endswith("-" + a)

    def has_live_trigger(self, coin: str, oid: Any) -> bool:
        """Venue truth: is `oid` (as returned by the client under test) among
        the live triggers for `coin`?"""
        return any(self._oid_matches(oid, v) for v in self.trigger_oids(coin))

    def has_order(self, oid: Any) -> bool:
        """Venue truth: is `oid` on the venue's live order book?"""
        return any(self._oid_matches(oid, v) for v in self.order_oids())

    def unknown_oid(self) -> str:
        """A venue-canonically-shaped oid that has NEVER existed."""
        return "fk-99999999"

    def fills(self) -> List[Dict[str, Any]]:
        return list(self.venue.fills)

    def last_fill(self) -> Optional[Dict[str, Any]]:
        return self.venue.fills[0] if self.venue.fills else None


# ===========================================================================
# MiniFrame — stdlib DataFrame stand-in (see module docstring)
# ===========================================================================

class MiniFrame:
    COLUMNS = ["time", "Open", "High", "Low", "Close", "Volume"]

    def __init__(self, rows: Sequence[Sequence[float]]) -> None:
        self._rows = [list(r) for r in rows]
        self.columns = list(self.COLUMNS)

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def empty(self) -> bool:
        return not self._rows

    def col(self, name: str) -> List[float]:
        i = self.columns.index(name)
        return [r[i] for r in self._rows]


# ===========================================================================
# The reference CORRECT adapter
# ===========================================================================

class FakeExchangeClient(ExchangeClient):
    """Executable spec of the contract. Every outbound call is time-bounded,
    every read is verified-or-raise, every write is readback-confirmed."""

    TIMEOUT = 0.15          # per-call transport bound (timeout law)
    RETRY_429 = 2           # extra attempts after a 429 (bounded budget)
    RETRY_429_SLEEP = 0.01
    CLOSE_ATTEMPTS = 3

    def __init__(self, gate: TransportGate, dry_run: bool = False) -> None:
        self.gate = gate
        #: DRY law (reference semantics for every binding): under DRY_RUN no
        #: write may reach the transport — writes fail CLOSED with
        #: WriteUnconfirmed(may_have_landed=False) before any send. Reads run
        #: normally.
        self.dry_run = bool(dry_run)
        self._mark_cache: Dict[str, Any] = {}  # coin -> (px, ts)
        meta = self._read("meta", "GET", BASE + "/meta")
        self._meta: Dict[str, Any] = meta["coins"]
        self._tfs = set(meta["tfs"])

    # ----------------------------------------------------------- transport

    def _raw(self, method: str, url: str, body: Any = None) -> GateResponse:
        """One bounded transport round-trip with an internal 429 budget.
        Raises TransportTimeout / TransportConnectError / RateLimited."""
        attempts = 1 + self.RETRY_429
        for i in range(attempts):
            g = self.gate.request(method, url, body=body,
                                  timeout=self.TIMEOUT, transport="fake")
            if g.status == 429:
                if i < attempts - 1:
                    time.sleep(self.RETRY_429_SLEEP)
                    continue
                raise RateLimited("429 retry budget exhausted on %s" % url,
                                  venue=VENUE, op=method)
            return g
        raise RateLimited("unreachable", venue=VENUE)  # pragma: no cover

    def _read(self, op: str, method: str, url: str,
              coin: str = "") -> Dict[str, Any]:
        """Verified read: parsed JSON dict or ReadUnknown/RateLimited."""
        try:
            g = self._raw(method, url)
        except RateLimited:
            raise
        except (TransportTimeout, TransportConnectError) as e:
            raise ReadUnknown("read failed (%s): %s" % (op, e),
                              venue=VENUE, op=op, coin=coin)
        if g.status >= 400:
            raise ReadUnknown("read failed (%s): HTTP %d" % (op, g.status),
                              venue=VENUE, op=op, coin=coin)
        if not g.body:
            raise ReadUnknown("read failed (%s): empty body" % op,
                              venue=VENUE, op=op, coin=coin)
        try:
            data = g.json()
        except Exception as e:
            raise ReadUnknown("read failed (%s): malformed body (%s)" % (op, e),
                              venue=VENUE, op=op, coin=coin)
        if not isinstance(data, dict):
            raise ReadUnknown("read failed (%s): non-object body" % op,
                              venue=VENUE, op=op, coin=coin)
        return data

    def _write(self, op: str, url: str, body: Dict[str, Any],
               coin: str = "") -> Dict[str, Any]:
        """Submit a write. Returns the ack/response dict. Raises:
        VenueRejected (definitive reject), WriteUnconfirmed (transport
        ambiguity — caller must readback), RateLimited."""
        if self.dry_run:
            raise WriteUnconfirmed(
                "[DRY] %s blocked at the exchange layer — nothing signed, "
                "nothing sent" % op, may_have_landed=False, venue=VENUE,
                op=op, coin=coin)
        try:
            g = self._raw("POST", url, body=body)
        except RateLimited:
            # a rate-limited WRITE may or may not have landed upstream — per
            # the contract, surface as WriteUnconfirmed for writes.
            raise WriteUnconfirmed("rate limited mid-write (%s)" % op,
                                   may_have_landed=False, venue=VENUE,
                                   op=op, coin=coin)
        except TransportTimeout as e:
            raise WriteUnconfirmed("write response lost (%s): %s" % (op, e),
                                   may_have_landed=None, venue=VENUE,
                                   op=op, coin=coin)
        except TransportConnectError as e:
            # F1 law: ONLY a connection-ESTABLISHMENT failure proves the
            # request never left this process. A connection error can also be
            # a MID-RESPONSE reset (the venue accepted and LANDED the order
            # before the socket died) — that is ambiguous and the caller must
            # readback before deciding.
            if "failed to establish" in str(e).lower():
                raise WriteUnconfirmed(
                    "write connect failed (%s): %s" % (op, e),
                    may_have_landed=False, venue=VENUE, op=op, coin=coin)
            raise WriteUnconfirmed(
                "write connection died mid-flight (%s): %s — the order MAY "
                "have landed" % (op, e),
                may_have_landed=None, venue=VENUE, op=op, coin=coin)
        if g.status >= 500:
            raise WriteUnconfirmed("write HTTP %d (%s)" % (g.status, op),
                                   may_have_landed=None, venue=VENUE,
                                   op=op, coin=coin)
        if g.status >= 400:
            raise WriteUnconfirmed("write HTTP %d (%s)" % (g.status, op),
                                   may_have_landed=False, venue=VENUE,
                                   op=op, coin=coin)
        if not g.body:
            raise WriteUnconfirmed("write ack unreadable (empty body, %s)" % op,
                                   may_have_landed=True, venue=VENUE,
                                   op=op, coin=coin)
        try:
            data = g.json()
        except Exception as e:
            raise WriteUnconfirmed("write ack unreadable (%s): %s" % (op, e),
                                   may_have_landed=True, venue=VENUE,
                                   op=op, coin=coin)
        if isinstance(data, dict) and data.get("ok") is False:
            raise VenueRejected("venue rejected %s: %s"
                                % (op, data.get("reject")),
                                reason=str(data.get("reject") or "unspecified"),
                                venue=VENUE, op=op, coin=coin)
        return data if isinstance(data, dict) else {}

    # --------------------------------------------------------------- writes

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        if coin not in self._meta:
            raise VenueRejected("unknown symbol %s" % coin,
                                reason="unknown_symbol", venue=VENUE,
                                op="market_open", coin=coin)
        if intended_px is not None and not allow_marketable:
            mark = self.mark_price(coin)  # ReadUnknown propagates: nothing sent
            if (is_buy and mark <= intended_px) or \
               ((not is_buy) and mark >= intended_px):
                raise VenueRejected(
                    "level %.8g already through the book (mark %.8g)"
                    % (intended_px, mark),
                    reason="immediately_marketable", venue=VENUE,
                    op="market_open", coin=coin)
        client_oid = uuid.uuid4().hex
        body = {"type": "market", "coin": coin, "is_buy": is_buy, "sz": sz,
                "reduce_only": False, "client_oid": client_oid}
        try:
            resp = self._write("place_order", BASE + "/order", body, coin=coin)
        except WriteUnconfirmed as e:
            if e.may_have_landed is False:
                raise
            # ambiguous send — the fill may exist: readback before deciding
            fill = self._readback_fill(coin, client_oid)
            if fill is None:
                raise WriteUnconfirmed(
                    "market_open ambiguous and fill readback shows nothing "
                    "(%s)" % e, may_have_landed=False, venue=VENUE,
                    op="market_open", coin=coin)
            return self._fill_result(coin, is_buy, sz, fill)

        if resp.get("filled_size") and resp.get("avg_px"):
            # venue-authoritative fill fields in the response ARE readback
            return FillResult(coin=coin, is_buy=is_buy,
                              avg_px=float(resp["avg_px"]),
                              size=float(resp["filled_size"]),
                              requested_size=float(sz),
                              oid=str(resp.get("oid") or "") or None)
        # ack without fill fields — truth lives in the fills feed
        fill = self._readback_fill(coin, client_oid)
        if fill is None:
            raise WriteUnconfirmed(
                "market_open acked but fill readback shows nothing",
                may_have_landed=True, venue=VENUE, op="market_open", coin=coin)
        return self._fill_result(coin, is_buy, sz, fill)

    def _readback_fill(self, coin: str,
                       client_oid: str) -> Optional[Dict[str, Any]]:
        """Find our fill(s) in the venue fills feed. None = positively absent.
        Raises WriteUnconfirmed if the readback itself cannot be trusted."""
        try:
            data = self._read("fills", "GET", BASE + "/fills", coin=coin)
        except ReadUnknown as e:
            raise WriteUnconfirmed(
                "fill readback failed — cannot prove either way: %s" % e,
                may_have_landed=True, venue=VENUE, op="fill_readback",
                coin=coin)
        mine = [f for f in data.get("fills", [])
                if f.get("client_oid") == client_oid]
        if not mine:
            return None
        tot = sum(float(f["size"]) for f in mine)
        vwap = sum(float(f["px"]) * float(f["size"]) for f in mine) / tot
        return {"size": tot, "px": vwap, "oid": mine[0].get("oid")}

    @staticmethod
    def _fill_result(coin: str, is_buy: bool, requested: float,
                     fill: Dict[str, Any]) -> FillResult:
        return FillResult(coin=coin, is_buy=is_buy, avg_px=float(fill["px"]),
                          size=float(fill["size"]),
                          requested_size=float(requested),
                          oid=str(fill.get("oid") or "") or None)

    def ensure_flat(self, coin: str) -> FlatResult:
        # initial read — failure here means NOTHING was sent
        pos_map = self.open_positions()
        if coin not in pos_map:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        pos = pos_map[coin]
        total_to_close = abs(pos.size_signed)
        is_buy = pos.size_signed < 0
        attempts = 0
        sent_any = False
        remaining = total_to_close
        for attempts in range(1, self.CLOSE_ATTEMPTS + 1):
            client_oid = uuid.uuid4().hex
            body = {"type": "market", "coin": coin, "is_buy": is_buy,
                    "sz": remaining, "reduce_only": True,
                    "client_oid": client_oid}
            try:
                self._write("place_order", BASE + "/order", body, coin=coin)
                sent_any = True
            except VenueRejected:
                # e.g. reduce-only-would-increase because it closed already —
                # the position readback below decides.
                pass
            except WriteUnconfirmed:
                sent_any = True  # may have landed — readback decides
            # position readback — the only source of "flat" truth
            try:
                self.invalidate_positions_cache()
                now_map = self.open_positions()
            except (ReadUnknown, RateLimited) as e:
                raise WriteUnconfirmed(
                    "ensure_flat(%s): close sent but final position readback "
                    "failed — flatness UNPROVEN: %s" % (coin, e),
                    may_have_landed=sent_any, venue=VENUE, op="ensure_flat",
                    coin=coin)
            if coin not in now_map:
                exit_px = self._exit_px_best_effort(coin)
                return FlatResult(coin=coin, already_flat=False,
                                  closed_size=total_to_close,
                                  exit_avg_px=exit_px, attempts=attempts)
            remaining = abs(now_map[coin].size_signed)
        raise WriteUnconfirmed(
            "ensure_flat(%s): %d close attempts exhausted, position still "
            "open (residual %.10g)" % (coin, attempts, remaining),
            may_have_landed=sent_any, venue=VENUE, op="ensure_flat", coin=coin)

    def _exit_px_best_effort(self, coin: str) -> Optional[float]:
        """Real exit VWAP from the fills feed; None when unattributable —
        NEVER a reference/SL price (wick_sl ×1330 class)."""
        try:
            data = self._read("fills", "GET", BASE + "/fills", coin=coin)
        except (ReadUnknown, RateLimited):
            return None
        rows = [f for f in data.get("fills", [])
                if f.get("coin") == coin and f.get("reduce_only")]
        if not rows:
            return None
        tot = sum(float(f["size"]) for f in rows)
        if tot <= 0:
            return None
        return sum(float(f["px"]) * float(f["size"]) for f in rows) / tot

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        body = {"coin": coin, "is_buy": is_buy, "sz": sz,
                "trigger_px": trigger_px, "client_oid": uuid.uuid4().hex}
        resp = self._write("place_trigger", BASE + "/trigger", body, coin=coin)
        oid = str(resp.get("oid") or "")
        if not oid:
            raise WriteUnconfirmed(
                "trigger_sl acked without an order id — cannot confirm",
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        # MANDATORY live-list readback (nado _confirm_trigger_live law)
        try:
            data = self._read("triggers", "GET",
                              BASE + "/triggers?coin=%s" % coin, coin=coin)
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "trigger_sl placed but live-trigger readback failed: %s" % e,
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        for t in data.get("triggers", []):
            if t.get("oid") == oid:
                return SLOrderInfo(coin=coin, oid=oid,
                                   trigger_px=float(t["trigger_px"]),
                                   size=float(t["size"]),
                                   is_buy_to_close=(t["side"] == "buy"))
        raise WriteUnconfirmed(
            "trigger_sl acked (oid=%s) but NOT in the live trigger list — "
            "naked-success class, refusing to claim placed" % oid,
            may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        already_gone = False
        try:
            self._write("cancel_order", BASE + "/cancel", {"oid": oid},
                        coin=coin)
        except VenueRejected as e:
            if "not_found" in (e.reason or ""):
                already_gone = True  # idempotent: goal state may already hold
            else:
                raise
        except WriteUnconfirmed:
            pass  # ambiguity — the gone-readback below decides
        # confirmed-gone readback
        try:
            data = self._read("orders", "GET", BASE + "/orders", coin=coin)
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s): cannot prove order gone — order-list "
                "readback failed: %s" % (oid, e),
                may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                coin=coin)
        live = {o.get("oid") for o in data.get("orders", [])}
        if oid in live:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s): cancel %s but order STILL LIVE"
                % (oid, "acked" if not already_gone else "not-found-claimed"),
                may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                coin=coin)
        return None

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        client_oid = uuid.uuid4().hex
        body = {"type": "limit", "coin": coin, "is_buy": is_buy, "sz": sz,
                "px": px, "reduce_only": True, "client_oid": client_oid}
        resp = self._write("place_order", BASE + "/order", body, coin=coin)
        oid = str(resp.get("oid") or "")
        if resp.get("filled_size") and resp.get("avg_px"):
            # immediately filled — return with the venue fill in raw
            return OpenOrderInfo(coin=coin, oid=oid or "filled",
                                 side="buy" if is_buy else "sell",
                                 size=float(sz), limit_px=float(px),
                                 reduce_only=True, is_trigger=False,
                                 raw={"filled": dict(resp)})
        # resting — confirm against the live order list
        try:
            data = self._read("orders", "GET", BASE + "/orders", coin=coin)
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "limit_reduce_only acked but order-list readback failed: %s"
                % e, may_have_landed=True, venue=VENUE,
                op="limit_reduce_only", coin=coin)
        for o in data.get("orders", []):
            if o.get("oid") == oid:
                return OpenOrderInfo(
                    coin=coin, oid=oid, side=str(o["side"]),
                    size=float(o["size"]), limit_px=float(o["limit_px"]),
                    trigger_px=None, reduce_only=True, is_trigger=False,
                    raw=o)
        # not resting, not filled in response — check fills before giving up
        fill = self._readback_fill(coin, client_oid)
        if fill is not None:
            return OpenOrderInfo(coin=coin, oid=oid or "filled",
                                 side="buy" if is_buy else "sell",
                                 size=float(fill["size"]),
                                 limit_px=float(px), reduce_only=True,
                                 is_trigger=False, raw={"filled": fill})
        raise WriteUnconfirmed(
            "limit_reduce_only acked (oid=%s) but neither resting nor filled "
            "on readback" % oid, may_have_landed=True, venue=VENUE,
            op="limit_reduce_only", coin=coin)

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        self._write("leverage", BASE + "/leverage",
                    {"coin": coin, "leverage": leverage,
                     "is_cross": is_cross}, coin=coin)
        return None

    # ---------------------------------------------------------------- reads

    def open_positions(self) -> Mapping[str, PositionInfo]:
        out: Dict[str, PositionInfo] = {}
        for scope in SCOPES:  # partial fetch = ReadUnknown for the WHOLE snapshot
            data = self._read("positions", "GET",
                              BASE + "/positions?scope=%s" % scope)
            for row in data.get("positions", []):
                try:
                    out[row["coin"]] = PositionInfo(
                        coin=row["coin"],
                        size_signed=float(row["size_signed"]),
                        entry_px=float(row["entry_px"]),
                        liquidation_px=(None if row.get("liq_px") is None
                                        else float(row["liq_px"])),
                        raw=row)
                except (KeyError, TypeError, ValueError) as e:
                    raise ReadUnknown(
                        "unparseable position row in scope %s: %s"
                        % (scope, e), venue=VENUE, op="open_positions")
        return out

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        data = self._read("orders", "GET", BASE + "/orders")
        out: List[OpenOrderInfo] = []
        for o in data.get("orders", []):
            try:
                out.append(self._order_info(o))
            except (KeyError, TypeError, ValueError) as e:
                # per-item parse failure aborts the WHOLE read — one silently
                # dropped order is the invisible-SL / phantom-guard class
                raise ReadUnknown("unparseable order row: %s" % e,
                                  venue=VENUE, op="open_orders")
        return out

    @staticmethod
    def _order_info(o: Dict[str, Any]) -> OpenOrderInfo:
        return OpenOrderInfo(
            coin=str(o["coin"]), oid=str(o["oid"]), side=str(o["side"]),
            size=float(o["size"]),
            limit_px=None if o.get("limit_px") is None else float(o["limit_px"]),
            trigger_px=(None if o.get("trigger_px") is None
                        else float(o["trigger_px"])),
            reduce_only=bool(o.get("reduce_only")),
            is_trigger=bool(o.get("is_trigger")), raw=o)

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        data = self._read("triggers", "GET",
                          BASE + "/triggers?coin=%s" % coin, coin=coin)
        try:
            return [str(t["oid"]) for t in data.get("triggers", [])]
        except (KeyError, TypeError) as e:
            raise ReadUnknown("unparseable trigger row: %s" % e,
                              venue=VENUE, op="list_open_sl_orders", coin=coin)

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        data = self._read("orders", "GET", BASE + "/orders")
        out: List[OpenOrderInfo] = []
        for o in data.get("orders", []):
            if not (o.get("is_trigger") and o.get("reduce_only")):
                continue
            try:
                out.append(self._order_info(o))
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown("unparseable trigger row: %s" % e,
                                  venue=VENUE, op="list_reduce_only_triggers")
        return out

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        try:
            data = self._read("mark", "GET", BASE + "/mark?coin=%s" % coin,
                              coin=coin)
            px = float(data.get("mark") or 0.0)
            if px <= 0.0:
                raise ReadUnknown("mark for %s is %r — insane/absent" %
                                  (coin, data.get("mark")),
                                  venue=VENUE, op="mark_price", coin=coin)
            self._mark_cache[coin] = (px, time.time())
            return px
        except RateLimited:
            raise
        except ReadUnknown:
            cached = self._mark_cache.get(coin)
            if cached is not None:
                px, ts = cached
                age = time.time() - ts
                if age <= max_age_sec:
                    return px  # cache INSIDE tolerance is legal + encouraged
                raise StaleData(
                    "mark for %s only available %.3fs old (tolerance %.3fs)"
                    % (coin, age, max_age_sec), age_sec=age,
                    tolerance_sec=max_age_sec, venue=VENUE, op="mark_price",
                    coin=coin)
            raise

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0):
        if interval not in TF_MS:
            raise ValueError("unsupported interval %r (caller bug)" % interval)
        if coin not in self._meta:
            raise ValueError("unknown coin %r (caller bug)" % coin)
        data = self._read(
            "candles", "GET",
            BASE + "/candles?coin=%s&tf=%s&limit=%d" % (coin, interval, limit),
            coin=coin)
        bars = data.get("bars") or []
        if not bars:
            raise ReadUnknown("candles(%s,%s): venue returned no bars"
                              % (coin, interval), venue=VENUE, op="candles",
                              coin=coin)
        tf_ms = TF_MS[interval]
        now_ms = int(time.time() * 1000)
        boundary = (now_ms // tf_ms) * tf_ms
        last_open = int(bars[-1][0])
        lag_bars = (boundary - (last_open + tf_ms)) / tf_ms
        if lag_bars > max_stale_bars:
            raise StaleData(
                "candles(%s,%s): last closed bar lags %.2f bars "
                "(tolerance %.2f)" % (coin, interval, lag_bars, max_stale_bars),
                age_sec=lag_bars * tf_ms / 1000.0,
                tolerance_sec=max_stale_bars * tf_ms / 1000.0,
                venue=VENUE, op="candles", coin=coin)
        return MiniFrame(bars)

    def equity_with_upnl(self) -> float:
        data = self._read("account", "GET", BASE + "/account")
        if "equity" not in data:
            raise ReadUnknown("account payload missing equity", venue=VENUE,
                              op="equity_with_upnl")
        return float(data["equity"])

    def account_value(self) -> float:
        data = self._read("account", "GET", BASE + "/account")
        if "account_value" not in data:
            raise ReadUnknown("account payload missing account_value",
                              venue=VENUE, op="account_value")
        return float(data["account_value"])

    def margin_used_usd(self) -> float:
        data = self._read("account", "GET", BASE + "/account")
        if "margin_used" not in data:
            raise ReadUnknown("account payload missing margin_used",
                              venue=VENUE, op="margin_used_usd")
        return float(data["margin_used"])

    def position_liquidation(self, coin: str) -> Optional[float]:
        pos_map = self.open_positions()  # ReadUnknown propagates
        pos = pos_map.get(coin)
        if pos is None:
            return None  # POSITIVELY no position
        return pos.liquidation_px

    def user_fills(self, max_age_sec: float = 60.0) -> Sequence[Mapping[str, Any]]:
        data = self._read("fills", "GET", BASE + "/fills")
        rows = data.get("fills", [])
        for f in rows:
            try:
                float(f["px"])
                float(f["size"])
            except (KeyError, TypeError, ValueError) as e:
                # per-item-corrupt law (P10/N11): one unparseable fill row
                # poisons the WHOLE read — a persistently-unparseable fill
                # must never be silently invisible to exit attribution
                raise ReadUnknown("user_fills: unparseable fill row (%s)" % e,
                                  venue=VENUE, op="user_fills")
        return list(rows)


# ===========================================================================
# Binding entry points
# ===========================================================================

def classify(method: str, url: str, body: Any) -> str:
    path = urlparse(url).path.lstrip("/")
    m = method.upper()
    if m == "GET":
        return {"meta": "meta", "positions": "positions", "mark": "mark",
                "candles": "candles", "account": "account",
                "orders": "orders", "triggers": "triggers",
                "fills": "fills"}.get(path, "other")
    if m == "POST":
        return {"order": "place_order", "trigger": "place_trigger",
                "cancel": "cancel_order", "leverage": "leverage"}.get(
                    path, "other")
    return "other"


def partial_outage_match(tctx: Dict[str, Any]) -> bool:
    """Partial-outage selector (see BoundContext): the fake snapshot spans two
    scopes (main + xyz); downing ONLY the xyz leg models one-scope-of-several
    down. The whole snapshot must then read as UNKNOWN."""
    return "scope=xyz" in str(tctx.get("url", ""))


def build_context(dry_run: bool = False) -> BoundContext:
    stack = contextlib.ExitStack()
    stack.enter_context(block_sockets())  # prove no real I/O even here
    venue = FakeVenue()
    gate = TransportGate(classify, venue.respond, name="fake-gate")
    scenario = FakeScenario(venue)

    def make_client() -> FakeExchangeClient:
        return FakeExchangeClient(gate, dry_run=dry_run)

    return BoundContext(venue=VENUE, client=make_client(), gate=gate,
                        scenario=scenario, make_client=make_client,
                        stack=stack, partial_outage_match=partial_outage_match)


def smoke_construct() -> str:
    """Offline-construction smoke: build the reference client under the
    socket guard. Returns a short proof string."""
    ctx = build_context()
    try:
        n_coins = len(ctx.client._meta)  # noqa: SLF001
        return "fake reference client constructed offline (%d coins)" % n_coins
    finally:
        ctx.close()
