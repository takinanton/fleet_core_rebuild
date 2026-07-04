"""bindings.nado — Nado (Vertex-descendant) binding.

Transport layer inspected (bots/nado/bot/exchange_nado.py):
  * nado_protocol SDK: EngineClient / TriggerClient / IndexerClient each hold
    a requests.Session (the adapter's :54/:203 timeout installers target
    exactly those sessions) — plus each execute-client's embedded _querier
    session (:386-388);
  * no aiohttp.
→ lowest interception layer = requests.Session.send (patch_requests); the
  adapter's ported session-timeout guards run unmodified above it — which is
  precisely what the harness must verify on this venue (audit defect #0:
  zero SDK timeouts was nado's root gap; slow_response faults raise
  UnboundedCallDetected on any call that still slips through untimed).

Constructor needs (:360): NADO_LINKED_SIGNER_PRIVATE_KEY (throwaway
secp256k1 hex), NADO_ACCOUNT_ADDRESS, NADO_SUBACCOUNT, DRY_RUN=0.

Responder: `create_nado_client(mode=MAINNET, signer=...)` performs contract
auto-discovery over HTTP at init. The exact discovery JSON is SDK-internal —
the first host run will fail loud with UninterceptedRealCall naming each
endpoint; extend `_NadoResponder.__call__` with those shapes (host-iteration
loop, see bindings/_venue_base.py docstring).

Simulated over the shared FakeVenue truth store (census 3b — Vertex-descendant
wire shapes, field spellings refined on-host): subaccount_info summary
(positions + healths/equity, per-item corruptible), indexer matches feed
(venue-truth fills at mark ± SLIP, submission_idx ordering), engine open-order
query, trigger-service list/execute, market_liquidity, engine execute
place/cancel with DIGEST-ONLY acks (no fill fields — an adapter that
fabricates avgPx/totalSz instead of reading the matches feed fails the
cross-check; no_land/partial directives honored).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from fleet_core.conformance.faults import GateResponse, json_response
from fleet_core.conformance.bindings import BindingUnavailable, BoundContext
from fleet_core.conformance.bindings.fake import FakeScenario, FakeVenue
from fleet_core.conformance.bindings._venue_base import (
    build_venue_context,
    parse_body,
    smoke_construct_venue,
)

NAME = "nado"
TRANSPORTS = ("requests",)

_FAKE_SIGNER_KEY = "0x" + "cd" * 32
# NON-UTF8 bytes on purpose (0xf4): the SDK RE-parses already-validated
# params (CancelTriggerOrdersParams.parse_obj(engine_params)) and pydantic
# v1 Union[str, bytes] coerces bytes32 -> str via utf-8 decode when the
# bytes happen to decode — the decoded ASCII then dies in hex_to_bytes32
# ("Value of `sender` (4444…default) is not encodable as bytes32", host runs
# it2-it5). Real wallets are never valid utf-8, so live code always stays on
# the bytes branch; the fabricated account must do the same.
_FAKE_ACCOUNT = "0x" + "f4" * 20


def _signed_amount(obj: Any) -> Optional[int]:
    """Recursive first 'amount'-keyed value, as signed int (x18)."""
    v = _find_key(obj, "amount")
    try:
        return int(str(v))
    except (TypeError, ValueError):
        return None


def _find_trigger_direction(obj: Any) -> Optional[str]:
    """'above' | 'below' from any key OR string value mentioning it
    (oracle_price_above / price_below spellings — refined on-host)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if "above" in kl:
                return "above"
            if "below" in kl:
                return "below"
            hit = _find_trigger_direction(v)
            if hit is not None:
                return hit
    elif isinstance(obj, list):
        for v in obj:
            hit = _find_trigger_direction(v)
            if hit is not None:
                return hit
    elif isinstance(obj, str):
        sl = obj.lower()
        if "above" in sl:
            return "above"
        if "below" in sl:
            return "below"
    return None


def classify(method: str, url: str, body: Any) -> str:
    p = urlparse(url).path.lower()
    b = parse_body(body)
    if "candlestick" in p or "candlestick" in str(b.get("type", "")) \
            or isinstance(b.get("candlesticks"), dict):
        return "candles"
    btype = str(b.get("type") or "")
    if "subaccount_info" in p or btype == "subaccount_info" \
            or "summary" in p:
        return "positions"  # nado summary serves positions+equity together
    if "perp_prices" in p or "prices" in p \
            or btype in ("perp_prices", "price", "prices",
                         "market_price", "market_prices") \
            or isinstance(b.get("price"), dict) \
            or isinstance(b.get("market_price"), dict):
        return "mark"
    if btype == "market_liquidity" or isinstance(b.get("market_liquidity"),
                                                 dict):
        return "mark"  # order-book top used to anchor entry prices
    if "matches" in p or btype == "matches" or isinstance(
            b.get("matches"), dict):
        # indexer matches feed: either {"type":"matches",...} or the
        # IndexerMatchesParams envelope {"matches": {...}}
        return "fills"
    if "orders" in p and "trigger" not in p and "trigger" not in url.lower():
        return "orders"
    if btype in ("open_orders", "orders", "subaccount_orders") or (
            "orders" in btype and "trigger" not in btype):
        return "orders"  # covers multi-products open-order query spellings
    if btype == "list_trigger_orders" or b.get("list_trigger_orders"):
        return "triggers"
    # trigger SERVICE lives on its own host (…trigger…/v1/execute) — match the
    # FULL url, not just the path, so trigger placements are not misread as
    # engine place_order
    if "trigger" in url.lower():
        if method.upper() == "POST" and (
                "execute" in p or b.get("place_order") or b.get("cancel")
                or b.get("cancel_orders")):
            if b.get("cancel") or b.get("cancel_orders"):
                return "cancel_order"
            if b.get("place_order") or b.get("order"):
                # TP vs SL split (canonical-op vocabulary: TP limits fault as
                # place_order, stops as place_trigger). Profit-side trigger =
                # close-side amount toward the trigger: sell(neg)+above or
                # buy(pos)+below is a TP; the loss-side pair is an SL.
                amt = _signed_amount(b)
                dirn = _find_trigger_direction(b)
                if amt is not None and dirn is not None and (
                        (amt < 0 and dirn == "above")
                        or (amt > 0 and dirn == "below")):
                    return "place_order"
                return "place_trigger"
            return "place_trigger"
        return "triggers"
    if "execute" in p:
        if b.get("cancel_orders") or b.get("cancel"):
            return "cancel_order"
        if b.get("place_order") or b.get("order"):
            return "place_order"
        if b.get("update_leverage"):
            return "leverage"
        return "other"
    if "symbols" in p or "contracts" in p or "query" in p:
        return "meta"
    return "other"


def _x18(v: Any) -> str:
    return str(int(round(float(v) * 1e18)))


def _oid_idx(oid: str) -> int:
    """Canonical FakeVenue oid 'fk-<n>' → n (also used as submission_idx)."""
    try:
        return int(str(oid).split("-")[-1])
    except (TypeError, ValueError):
        return 0


_DIGEST_PAD = "ff" * 24  # 24 leading 0xff bytes -> NEVER valid utf-8


def _digest_of(oid: str) -> str:
    """Deterministic bidirectional digest ↔ canonical oid mapping.

    BARE 64-hex (no 0x — the digest validator is bytes.fromhex) with a
    0xff…ff prefix: the SDK re-parses validated params (parse_obj) and
    pydantic v1 Union[str, bytes] utf-8-decodes any bytes32 that HAPPENS to
    decode — a low-numeric digest (b'\\x00…\\x04\\x01') decoded to a control
    string and then died in the re-run digest validator ('non-hexadecimal
    number found in fromhex() arg at position 0', host it2-it5). Real venue
    digests are keccak outputs — practically never valid utf-8 — so the
    binding's digests must be non-decodable too; the idx lives in the low
    16 hex digits."""
    return _DIGEST_PAD + "%016x" % _oid_idx(oid)


def _oid_of_digest(digest: str) -> str:
    try:
        return "fk-%d" % int(str(digest), 16)
    except (TypeError, ValueError):
        return str(digest)


def _resolve_digest_oid(digest: Any, orders: Dict[str, Any]) -> str:
    """Digest → canonical venue oid, robust to every wire spelling the SDK
    may serialize a bytes32 digest to (0x-hex string, bare-hex string,
    DECIMAL int/str after an int coercion). Ambiguity is settled against the
    LIVE order set — a candidate that exists wins; otherwise the canonical
    hex reading is returned (→ not_found reject, the venue's honest answer).
    """
    s = str(digest)
    if s.lower().startswith("0x"):
        s = s[2:]
    cands: List[int] = []
    try:
        if len(s) == 64:
            # canonical binding digest: idx in the LOW 16 hex digits
            cands.append(int(s[-16:], 16))
            cands.append(int(s, 16))
        elif s.isdigit():
            cands.append(int(s, 16))  # bare-hex reading first
            cands.append(int(s))      # decimal spelling (int-coerced digest)
        else:
            cands.append(int(s, 16))
    except (TypeError, ValueError):
        return str(digest)
    for n in cands:
        oid = "fk-%d" % n
        if oid in orders:
            return oid
    return "fk-%d" % cands[0]


def _trigger_px_value(obj: Any) -> Optional[Any]:
    """First SCALAR value under a *_above/*_below key (the trigger-price
    requirement leaf: oracle_price_above / last_price_below / ...)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if ("above" in kl or "below" in kl) \
                    and not isinstance(v, (dict, list)):
                return v
            hit = _trigger_px_value(v)
            if hit is not None:
                return hit
    elif isinstance(obj, list):
        for v in obj:
            hit = _trigger_px_value(v)
            if hit is not None:
                return hit
    return None


def _find_key(obj: Any, *needles: str) -> Optional[Any]:
    """Recursive first value whose key contains ALL needles (case-insens)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if all(n in kl for n in needles):
                return v
        for v in obj.values():
            hit = _find_key(v, *needles)
            if hit is not None:
                return hit
    elif isinstance(obj, list):
        for v in obj:
            hit = _find_key(v, *needles)
            if hit is not None:
                return hit
    return None


class _NadoResponder:
    """Venue-truth rendering over the shared FakeVenue (Vertex-descendant wire
    shapes, best-effort — refine field spellings on-host via the fail-loud
    loop).

    WRITE SEMANTICS (census 3b — the fabricated-fill defect class): engine
    place_order acks DIGEST-ONLY ({"status":"success","data":{"digest":..}} —
    NO fill fields). The fill itself lands in the venue truth store at the
    VENUE's price (mark ± FakeVenue.SLIP taker slip), NOT at the order's limit
    price, and is served back ONLY through the indexer matches feed — so an
    adapter that echoes the request size, the mark, or its own limit price
    instead of reading the matches feed fails the cross-check, and the
    no_land / partial directives make write_market_open_partial_visible /
    accept_no_land exercise real venue-truth divergence."""

    def __init__(self, venue: FakeVenue) -> None:
        self.venue = venue

    def _coins(self) -> List[str]:
        return sorted(c for c, m in self.venue.meta.items()
                      if m.get("scope", "main") == "main")

    def _pid(self, coin: str) -> int:
        return 2 + self._coins().index(coin)

    def _coin_of_pid(self, pid: int) -> Optional[str]:
        coins = self._coins()
        i = int(pid) - 2
        return coins[i] if 0 <= i < len(coins) else None

    # ------------------------------------------------------------- reads
    def _perp_products(self) -> List[Dict[str, Any]]:
        """Product-state rows (oracle px + book info) — the SDK's
        _get_subaccount_product_position / close_position paths read the
        product out of the subaccount_info payload. Field spellings are
        best-effort Vertex-descendant (host-iteration loop)."""
        out = []
        for c in self._coins():
            m = self.venue.meta[c]
            mark = self.venue.marks.get(c, 0.0) or 1.0
            out.append({
                "product_id": self._pid(c),
                "oracle_price_x18": _x18(mark),
                "risk": {
                    # nado ProductRisk (SDK schema dump): 4 weights +
                    # price_x18 — ALL required
                    "long_weight_initial_x18": _x18(0.95),
                    "short_weight_initial_x18": _x18(1.05),
                    "long_weight_maintenance_x18": _x18(0.97),
                    "short_weight_maintenance_x18": _x18(1.03),
                    "price_x18": _x18(mark),
                },
                "state": {
                    "cumulative_funding_long_x18": "0",
                    "cumulative_funding_short_x18": "0",
                    "available_settle": "0",
                    "open_interest": "0",
                },
                "lp_state": {
                    "supply": "0",
                    "quote": {"amount": "0",
                              "last_cumulative_funding_x18": "0"},
                    "base": {"amount": "0",
                             "last_cumulative_funding_x18": "0"},
                },
                "book_info": {
                    # size_increment raw wei; min_size = COUNT of increments
                    # ×1e18 (Vertex encoding, exchange_nado.py:488)
                    "size_increment": str(int(
                        float(m["min_size"]) * 1e18)),
                    "min_size": str(10**18),
                    "price_increment_x18": _x18(m["tick"]),
                    "collected_fees": "0",
                    "lp_spread_x18": "0",
                },
            })
        return out

    def _subaccount_info(self, directive: Optional[Dict[str, Any]],
                         b: Optional[Dict[str, Any]] = None) -> GateResponse:
        # SubaccountInfoData REQUIRES the subaccount echo (schema dump)
        sub = str((b or {}).get("subaccount") or "0x" + "34" * 32)
        # one balance row per PRODUCT, zero-amount rows included: the SDK's
        # _get_subaccount_product_position indexes the row for the requested
        # pid unconditionally ("Invalid product id provided N. Error: list
        # index out of range" on the it2 host run when only position-holding
        # coins were served) — the real venue serves the full product set.
        by_coin: Dict[str, Dict[str, Any]] = {}
        for scope in self.venue.positions:  # nado has no scope split: serve all
            for coin, p in self.venue.positions[scope].items():
                by_coin[coin] = p
        rows: List[Dict[str, Any]] = []
        for coin in self._coins():
            p = by_coin.get(coin)
            sz = p["size_signed"] if p else 0.0
            vq = -p["size_signed"] * p["entry_px"] if p else 0.0
            rows.append({
                "product_id": self._pid(coin),
                "lp_balance": {"amount": "0"},
                "balance": {
                    "amount": _x18(sz),
                    # v_quote is opposite-signed notional (entry basis)
                    "v_quote_balance": _x18(vq),
                    "last_cumulative_funding_x18": "0",
                },
            })
        if directive is not None and directive.get("mode") == "corrupt_item" \
                and rows:
            rows[-1] = dict(rows[-1])
            rows[-1]["balance"] = dict(rows[-1]["balance"])
            rows[-1]["balance"]["amount"] = "corrupt-item-0xDEAD"
        # healths — Vertex convention [initial, maintenance, unweighted]:
        #   h0 = initial  -> nado account_value() (funds available)
        #   h1 = maint    -> marginal-liq buffer: derived from the FIRST
        #                    seeded liq_px so position_liquidation() round-
        #                    trips venue truth (long: (entry-liq)*amt; the
        #                    signed expression covers shorts too)
        #   h2 = unweighted -> equity_with_upnl (portfolio value)
        # NOTE nado identity (F3): margin_used == h2 - h0 == equity -
        # account_value — an INDEPENDENT margin_used seed is unrepresentable
        # on this wire. The binding declares
        # scenario.margin_identity_basis=True and the equity/sizing check
        # seeds an identity-consistent triple (equity 12345 / account_value
        # 12000 / margin_used 345) so the venue-agnostic semantics still
        # assert three distinct venue-truth propagations.
        h1_val = self.venue.equity
        for scope in self.venue.positions:
            hit = False
            for coin, p in self.venue.positions[scope].items():
                if p.get("liq_px") is not None:
                    h1_val = (p["entry_px"] - p["liq_px"]) * p["size_signed"]
                    hit = True
                    break
            if hit:
                break
        h0, h1, h2 = (_x18(self.venue.account_value), _x18(h1_val),
                      _x18(self.venue.equity))
        healths = [{"assets": h0, "liabilities": "0", "health": h0},
                   {"assets": h1, "liabilities": "0", "health": h1},
                   {"assets": h2, "liabilities": "0", "health": h2}]
        return json_response({"status": "success", "data": {
            "subaccount": sub,
            "exists": True, "healths": healths, "health_contributions": [],
            "spot_count": 0, "perp_count": len(rows),
            "spot_balances": [], "perp_balances": rows,
            "spot_products": [], "perp_products": self._perp_products()}})

    def _matches(self, b: Dict[str, Any],
                 directive: Optional[Dict[str, Any]] = None) -> GateResponse:
        req = b.get("matches") if isinstance(b.get("matches"), dict) else b
        pids = set(int(x) for x in (req.get("product_ids") or []))
        limit = int(req.get("limit") or 100)
        out = []
        for f in self.venue.fills:  # newest first
            coin = f["coin"]
            try:
                pid = self._pid(coin)
            except ValueError:
                continue
            if pids and pid not in pids:
                continue
            sign = 1 if f["side"] == "buy" else -1
            idx = _oid_idx(f["oid"])
            base = _x18(sign * f["size"])
            quote = _x18(-sign * f["size"] * f["px"])
            out.append({
                "digest": _digest_of(f["oid"]),
                # IndexerBaseOrder — priceX18 spelling (SDK schema dump)
                "order": {"sender": "0x0", "priceX18": _x18(f["px"]),
                          "amount": _x18(sign * f["size"]),
                          "expiration": "0", "nonce": str(idx)},
                "base_filled": base, "quote_filled": quote, "fee": "0",
                "builder_fee": "0", "sequencer_fee": "0",
                "cumulative_base_filled": base,
                "cumulative_quote_filled": quote, "cumulative_fee": "0",
                "submission_idx": str(idx),
                "timestamp": str(int(f.get("time", 0))),
                "isolated": False,
            })
            if len(out) >= limit:
                break
        if directive is not None and directive.get("mode") == "corrupt_item" \
                and out:
            out[-1] = dict(out[-1])
            out[-1]["base_filled"] = "corrupt-item-0xDEAD"
        return json_response({"matches": out, "txs": []})

    def _open_orders(self, directive: Optional[Dict[str, Any]]
                     ) -> GateResponse:
        per_pid: Dict[int, List[Dict[str, Any]]] = {}
        for o in self.venue.orders.values():
            if o["is_trigger"]:
                continue  # triggers live on the trigger service
            try:
                pid = self._pid(o["coin"])
            except ValueError:
                continue
            sign = 1 if o["side"] == "buy" else -1
            per_pid.setdefault(pid, []).append({
                "product_id": pid, "sender": "0x0",
                "price_x18": _x18(o["limit_px"] or 0),
                "amount": _x18(sign * o["size"]),
                "expiration": "0", "nonce": str(_oid_idx(o["oid"])),
                "unfilled_amount": _x18(sign * o["size"]),
                "digest": _digest_of(o["oid"]), "placed_at": 0,
            })
        entries = [{"product_id": pid, "orders": rows}
                   for pid, rows in sorted(per_pid.items())]
        if directive is not None and directive.get("mode") == "corrupt_item" \
                and entries:
            entries[-1]["orders"][-1]["amount"] = "corrupt-item-0xDEAD"
        return json_response({"status": "success", "data": {
            "sender": "0x0", "product_orders": entries}})

    def _trigger_list(self, directive: Optional[Dict[str, Any]]
                      ) -> GateResponse:
        """TriggerOrdersData (SDK schema dump): orders[].order is a
        TriggerOrderData {product_id, order{sender,priceX18,amount,expiration,
        nonce}, signature, digest, trigger{price_trigger{price_requirement}}}
        with placed_at/updated_at REQUIRED ints and status a plain string."""
        rows = []
        for o in self.venue.orders.values():
            if not o["is_trigger"]:
                continue
            try:
                pid = self._pid(o["coin"])
            except ValueError:
                continue
            sign = 1 if o["side"] == "buy" else -1
            # SL semantics: buy-to-close triggers above, sell-to-close below
            req_key = "oracle_price_above" if o["side"] == "buy" \
                else "oracle_price_below"
            rows.append({
                "status": "waiting_price",
                "placed_at": 0,
                "updated_at": 0,
                "order": {
                    "product_id": pid,
                    "digest": _digest_of(o["oid"]),
                    "signature": "0x" + "00" * 65,
                    "order": {"sender": "0x0",
                              "priceX18": _x18(o["limit_px"]
                                               or o["trigger_px"] or 0),
                              "amount": _x18(sign * o["size"]),
                              "expiration": "0",
                              "nonce": str(_oid_idx(o["oid"]))},
                    "trigger": {"price_trigger": {"price_requirement": {
                        req_key: _x18(o["trigger_px"] or 0)}}},
                },
            })
        if directive is not None and directive.get("mode") == "corrupt_item" \
                and rows:
            rows[-1]["order"]["order"]["amount"] = "corrupt-item-0xDEAD"
        return json_response({"status": "success", "data": {"orders": rows}})

    def _market_liquidity(self, b: Dict[str, Any]) -> GateResponse:
        coin = self._coin_of_pid(int(b.get("product_id") or 0))
        mark = self.venue.marks.get(coin or "", 0.0)
        return json_response({"status": "success", "data": {
            "bids": [[_x18(mark * 0.999), _x18(100)]],
            "asks": [[_x18(mark * 1.001), _x18(100)]],
            "timestamp": "0"}})

    # ------------------------------------------------------------- writes
    def _execute_place(self, b: Dict[str, Any],
                       directive: Optional[Dict[str, Any]],
                       is_trigger_service: bool) -> GateResponse:
        po = b.get("place_order") or {}
        order = po.get("order") or {}
        try:
            amt_x18 = int(str(_find_key(order, "amount") or 0))
        except (TypeError, ValueError):
            amt_x18 = 0
        coin = self._coin_of_pid(int(po.get("product_id") or 0))
        if coin is None:
            return json_response({"status": "failure", "error_code": 2000,
                                  "error": "unknown product"})
        is_buy = amt_x18 > 0
        sz = abs(amt_x18) / 1e18
        if is_trigger_service:
            # trigger px lives at price_requirement.{oracle|last|mid}_price_
            # {above|below} (SDK schema dump) — take the first scalar value
            # under an above/below key
            trig = _trigger_px_value(b)
            try:
                trig_px = int(str(trig)) / 1e18 if trig is not None else 0.0
            except (TypeError, ValueError):
                trig_px = 0.0
            # venue-side reduce-only law (every nado trigger is reduce-only):
            # no position / same-direction -> error_code 2064 "Reduce only
            # order increases position" (the live-documented reject class,
            # exchange_nado.py:1449)
            pos = self.venue.find_position(coin)
            delta = sz if is_buy else -sz
            if pos is None or (pos["size_signed"] > 0) == (delta > 0):
                return json_response({
                    "status": "failure", "error_code": 2064,
                    "error": "Reduce only order increases position (%s)"
                             % coin})
            neutral = self.venue._handle_place_trigger(  # noqa: SLF001
                {"coin": coin, "is_buy": is_buy, "sz": sz,
                 "trigger_px": trig_px, "client_oid": None}, directive)
            nd = neutral.json()
            if not nd.get("ok"):
                return json_response({"status": "failure", "error_code": 2064,
                                      "error": str(nd.get("reject"))})
            return json_response({"status": "success", "data": {
                "digest": _digest_of(nd["oid"])},
                "request_type": "execute_place_order"})
        # engine order: adapter sends marketable FOK limits for entries/closes
        # → simulate as a MARKET so the venue applies its own taker slip vs
        # mark (fabricated-fill class stays testable); reduce_only cannot be
        # decoded from the packed appendix — venue-side reduce-only law is
        # exercised via the other venues + fake (documented gap, on-host loop)
        neutral = self.venue._handle_place_order(  # noqa: SLF001
            {"type": "market", "coin": coin, "is_buy": is_buy, "sz": sz,
             "reduce_only": False, "client_oid": None}, directive)
        nd = neutral.json()
        if not nd.get("ok"):
            return json_response({"status": "failure", "error_code": 2000,
                                  "error": str(nd.get("reject"))})
        # DIGEST-ONLY ack — no fill fields; truth only via the matches feed
        return json_response({"status": "success", "data": {
            "digest": _digest_of(nd["oid"])},
            "request_type": "execute_place_order"})

    def _order_data_row(self, o: Dict[str, Any]) -> Dict[str, Any]:
        """Engine OrderData rendering (SDK schema dump: 9 required fields)."""
        sign = 1 if o["side"] == "buy" else -1
        return {
            "product_id": self._pid(o["coin"]),
            "sender": "0x0",
            "price_x18": _x18(o["limit_px"] or o["trigger_px"] or 0),
            "amount": _x18(sign * o["size"]),
            "expiration": "0",
            "nonce": str(_oid_idx(o["oid"])),
            "unfilled_amount": _x18(sign * o["size"]),
            "digest": _digest_of(o["oid"]),
            "placed_at": 0,
        }

    def _execute_cancel(self, b: Dict[str, Any],
                        directive: Optional[Dict[str, Any]],
                        is_trigger_service: bool) -> GateResponse:
        co = b.get("cancel_orders") or b.get("cancel") or {}
        # digests may be nested under the signed tx envelope — search deep
        digests = co.get("digests") or co.get("digest") or \
            _find_key(co, "digests") or _find_key(co, "digest") or []
        if isinstance(digests, str):
            digests = [digests]
        cancelled_rows = []
        for d in digests:
            oid = _resolve_digest_oid(d, self.venue.orders)
            snapshot = self.venue.orders.get(oid)
            neutral = self.venue._handle_cancel(  # noqa: SLF001
                {"oid": oid}, directive)
            nd = neutral.json()
            if not nd.get("ok"):
                return json_response({"status": "failure", "error_code": 2020,
                                      "error": str(nd.get("reject"))})
            if snapshot is not None:
                try:
                    cancelled_rows.append(self._order_data_row(snapshot))
                except ValueError:
                    pass
        if is_trigger_service:
            # trigger-service cancel ack: status envelope only (the SDK's
            # ExecuteResponse.data union has no digest-list member)
            return json_response({"status": "success",
                                  "request_type": "execute_cancel_orders"})
        # engine cancel ack: CancelOrdersResponse carries the FULL OrderData
        # rows of what was cancelled (schema dump — bare digests fail parse)
        return json_response({"status": "success",
                              "data": {"cancelled_orders": cancelled_rows},
                              "request_type": "execute_cancel_orders"})

    # ------------------------------------------------------------- dispatch
    def __call__(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        op = ctx["op"]
        b = parse_body(ctx.get("body"))
        if op == "meta":
            # The gateway /v1/query serves SEVERAL meta query types — branch
            # on the body's "type" (first host run served one symbols blob to
            # ALL of them and every SDK model failed pydantic):
            #   contracts       — create_nado_client's verification step
            #                     (ContractsData: chain_id/endpoint_addr/
            #                     book_addrs); its failure left trigger_client
            #                     None for the whole process;
            #   product_symbols / symbols — adapter market map loads.
            btype = str(b.get("type") or "").lower()
            if btype == "contracts":
                n_books = 2 + len(self._coins())
                return json_response({"status": "success", "data": {
                    "chain_id": "1",
                    "endpoint_addr": "0x" + "11" * 20,
                    "book_addrs": ["0x" + ("%02x" % i) * 20
                                   for i in range(n_books)]}})
            if btype == "nonces":
                return json_response({"status": "success", "data": {
                    "tx_nonce": "1", "order_nonce": "1"}})
            if btype == "all_products":
                return json_response({"status": "success", "data": {
                    "spot_products": [],
                    "perp_products": self._perp_products()}})

            def _sym_row(c: str) -> Dict[str, Any]:
                # SymbolData (SDK schema dump): product_id is a STRING;
                # min_size is COUNT of increments ×1e18 (Vertex encoding):
                # 1 increment == min order. long_weight_initial drives the
                # adapter's per-product max-leverage derivation.
                return {
                    "symbol": c, "product_id": str(self._pid(c)),
                    "type": "perp",
                    "size_increment": str(int(
                        self.venue.meta[c]["min_size"] * 1e18)),
                    "min_size": str(10**18),
                    "price_increment_x18": str(int(
                        self.venue.meta[c]["tick"] * 1e18)),
                    "min_depth_x18": "0",
                    "max_spread_rate_x18": "0",
                    "maker_fee_rate_x18": "0",
                    "taker_fee_rate_x18": "0",
                    "long_weight_initial_x18": _x18(
                        1.0 - 1.0 / float(
                            self.venue.meta[c]["max_leverage"])),
                    "long_weight_maintenance_x18": _x18(0.97),
                    "max_open_interest_x18": "0",
                }
            if btype == "product_symbols":
                # engine product_symbols query: flat product_id↔symbol map
                return json_response({"status": "success", "data": [
                    {"product_id": self._pid(c), "symbol": c}
                    for c in self._coins()]})
            if str(ctx.get("method", "")).upper() == "GET":
                # v2-style GET symbols listing: RAW list, no engine envelope
                return json_response([_sym_row(c) for c in self._coins()])
            # engine symbols query — SymbolsData: dict KEYED BY SYMBOL
            return json_response({
                "status": "success",
                "data": {"symbols": {c: _sym_row(c)
                                     for c in self._coins()}},
            })
        if op == "mark":
            btype = str(b.get("type") or "")
            if btype == "market_liquidity" or \
                    isinstance(b.get("market_liquidity"), dict):
                req = b.get("market_liquidity") \
                    if isinstance(b.get("market_liquidity"), dict) else b
                return self._market_liquidity(req)
            if btype in ("market_price", "market_prices") or \
                    isinstance(b.get("market_price"), dict):
                # engine market_price (enveloped): top-of-book both sides
                req = b.get("market_price") \
                    if isinstance(b.get("market_price"), dict) else b
                pid = int(req.get("product_id") or 0)
                coin = self._coin_of_pid(pid)
                mark = self.venue.marks.get(coin or "", 0.0)
                return json_response({"status": "success", "data": {
                    "product_id": pid,
                    "bid_x18": _x18(mark * 0.999) if mark else "0",
                    "ask_x18": _x18(mark * 1.001) if mark else "0"}})
            # indexer perp/oracle prices (flat, no engine envelope)
            req = b.get("price") if isinstance(b.get("price"), dict) else b
            pid = int(req.get("product_id") or 0)
            coin = self._coin_of_pid(pid)
            mark = self.venue.marks.get(coin or "", 0.0)
            return json_response({
                "product_id": pid,
                "mark_price_x18": _x18(mark) if mark else "0",
                "index_price_x18": _x18(mark) if mark else "0",
                "update_time": "0"})
        if op == "candles":
            # IndexerCandlesticksParams envelope {"candlesticks": {...}} or
            # flat; product_id preferred, symbol fallback; granularity may be
            # the enum's seconds value or a tf string.
            req = b.get("candlesticks") \
                if isinstance(b.get("candlesticks"), dict) else b
            coin = None
            if req.get("product_id") is not None:
                try:
                    coin = self._coin_of_pid(int(req["product_id"]))
                except (TypeError, ValueError):
                    coin = None
            if coin is None:
                coin = str(req.get("symbol") or req.get("product_symbol")
                           or "BTC")
            g = req.get("granularity") or req.get("tf") or "4h"
            gran_to_tf = {60: "1m", 300: "5m", 900: "15m", 3600: "1h",
                          7200: "2h", 14400: "4h", 86400: "1d",
                          604800: "1w"}
            try:
                tf = gran_to_tf.get(int(str(g)), str(g))
            except (TypeError, ValueError):
                tf = str(g)
            neutral = self.venue._serve_candles(  # noqa: SLF001
                {"coin": coin, "tf": tf, "limit": str(req.get("limit", 500))})
            if neutral.status != 200:
                return json_response({"candlesticks": []})
            # indexer responses are FLAT (no engine status/data envelope) —
            # mirrors the matches feed shape below
            return json_response({"candlesticks": [
                {"product_id": self._pid(coin) if coin in self.venue.meta
                 else 0,
                 "granularity": str(g),
                 "submission_idx": "0",
                 "timestamp": int(r[0] // 1000),
                 "open_x18": str(int(float(r[1]) * 1e18)),
                 "high_x18": str(int(float(r[2]) * 1e18)),
                 "low_x18": str(int(float(r[3]) * 1e18)),
                 "close_x18": str(int(float(r[4]) * 1e18)),
                 "volume": str(int(float(r[5]) * 1e18))}
                for r in neutral.json().get("bars", [])]})
        if op == "positions":
            return self._subaccount_info(directive, b)
        if op == "fills":
            return self._matches(b, directive)
        if op == "orders":
            return self._open_orders(directive)
        if op == "triggers":
            return self._trigger_list(directive)
        if op in ("place_order", "place_trigger"):
            # route by HOST, not by op: the TP-vs-SL op split (classify) still
            # lands profit-side triggers on the trigger service
            is_trig = "trigger" in str(ctx.get("url", "")).lower()
            return self._execute_place(b, directive,
                                       is_trigger_service=is_trig)
        if op == "cancel_order":
            return self._execute_cancel(
                b, directive,
                is_trigger_service="trigger" in str(
                    ctx.get("url", "")).lower())
        # anything else: SDK-internal wire shapes still unknown — fail loud,
        # extend during the on-host iteration loop.
        return None


def _env(dry_run: bool = False) -> Dict[str, str]:
    return {
        "EXCHANGE": "nado",
        "NETWORK": "mainnet",
        # live-shaped by default; dry_run=True = the F2 write-isolation lane
        # (the WRAPPER enforces DRY on nado — the raw has no gate)
        "DRY_RUN": "1" if dry_run else "0",
        "NADO_LINKED_SIGNER_PRIVATE_KEY": _FAKE_SIGNER_KEY,
        "NADO_ACCOUNT_ADDRESS": _FAKE_ACCOUNT,
        "NADO_SUBACCOUNT": "default",
        "NADO_WS_CANDLE": "0",
        "NADO_FETCH_SLEEP_SEC": "0",  # no per-fetch pacing under the harness
    }


class _NadoScenario(FakeScenario):
    """Nado venue-truth control:

    * positions_read_transport_calls = 1 — one open_positions() snapshot is a
      single engine subaccount_info query (sequenced faults target the
      readback correctly);
    * oid canonicalization — the binding exposes the venue's canonical
      'fk-<n>' oids as deterministic 0x-64hex digests (_digest_of); truth
      cross-checks must match either spelling;
    * unknown_oid — a digest-shaped id (the SDK signs digests; 'fk-…' would
      die in request validation before reaching the venue).
    """

    positions_read_transport_calls = 1

    #: F3: nado's wire DERIVES margin_used from the two equity bases
    #: (healths identity h2-h0) — the equity/sizing check seeds an
    #: identity-consistent triple instead of an independent margin value.
    margin_identity_basis = True

    @staticmethod
    def _oid_matches(client_oid: Any, venue_oid: Any) -> bool:
        if FakeScenario._oid_matches(client_oid, venue_oid):
            return True

        def _num(x: Any) -> Optional[int]:
            s = str(x)
            if s.lower().startswith("0x"):
                s = s[2:]
            try:
                if len(s) == 64:  # 32-byte digest: idx in the low 16 hex
                    return int(s[-16:], 16)
                return int(s.rsplit("-", 1)[-1])
            except (TypeError, ValueError):
                return None

        na, nb = _num(client_oid), _num(venue_oid)
        return na is not None and na == nb

    def unknown_oid(self) -> str:
        return _DIGEST_PAD + "%016x" % 99999999


def _construct_raw() -> Any:
    try:
        from bot.config import Settings
        from bot.exchange_nado import NadoClient_
    except ImportError as e:
        raise BindingUnavailable(
            "nado: bot package / nado_protocol SDK not importable here (run "
            "on the nado host from the bot root): %s" % e)
    return NadoClient_(Settings.from_env())


def partial_outage_match(tctx: Dict[str, Any]) -> bool:
    """Partial-outage selector: nado's positions snapshot is ONE engine
    subaccount_info summary query — no multi-scope subset exists, so the
    selector is that single leg (the law degenerates to a full-read outage;
    still must surface as ReadUnknown)."""
    return tctx.get("op") == "positions"


def build_context(dry_run: bool = False) -> BoundContext:
    ctx = build_venue_context(
        name=NAME, env=_env(dry_run=dry_run), classify=classify,
        make_responder=lambda venue: _NadoResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw,
        partial_outage_match=partial_outage_match)
    # venue-specific scenario semantics over the SAME truth store
    ctx.scenario = _NadoScenario(ctx.scenario.venue)
    return ctx


def smoke_construct() -> str:
    return smoke_construct_venue(
        name=NAME, env=_env(), classify=classify,
        make_responder=lambda venue: _NadoResponder(venue),
        transports=TRANSPORTS, construct_raw=_construct_raw)
