"""fleet_core.venues.pacifica — THE conforming ExchangeClient for Pacifica.

Wraps the existing raw adapter (bot.exchange_pacifica.PacificaClient — left
UNTOUCHED; its in-place fixes ride the P3 cutover) and enforces the
fleet_core.exchange_api contract at this layer:

  * reads are verified-or-raise (ReadUnknown / StaleData / RateLimited /
    ValueError-for-caller-bugs) — never 0.0 / {} / [] / silently-stale cache;
  * writes return only readback-confirmed results (FillResult / SLOrderInfo /
    OpenOrderInfo / FlatResult are verified-by-construction);
  * ensure_flat() is the only close primitive (position readback loop, raise
    WriteUnconfirmed on residual/unprovable flatness);
  * every outbound call is time-bounded (raw adapter's session already
    carries the (connect, read) default-timeout installer; this wrapper only
    ever passes explicit timeouts on top).

CENSUS VIOLATIONS CLOSED HERE (proofs/p2_violation_census.md §2):
  P1  account_value 0.0-mask ................. verified-or-raise (_account_data)
  P2  mark_price 0.0 + negative-cache ........ >0-or-raise, own cache, never
                                               caches failures, StaleData
                                               beyond max_age_sec
  P3  candles empty-df masks ................. ValueError (bad interval /
                                               unknown coin = caller bug),
                                               ReadUnknown (fetch fail/empty),
                                               StaleData (last-closed-bar gate)
  P4  market_open ack-then-poll-timeout ...... fill readback via order-history
                                               + fills feed; ambiguity ->
                                               WriteUnconfirmed(may_have_landed)
  P5  market_close error-dict / no readback .. ensure_flat(): close + position
                                               readback loop, WriteUnconfirmed
                                               on residual / unprovable
  P6  trigger_sl no live-list readback ....... _confirm_trigger_live port:
                                               placed SL must appear in the
                                               live order list or
                                               WriteUnconfirmed; definitive
                                               reject -> VenueRejected(reason)
                                               ($10-notional class actionable)
  P7  cancel_sl_order error-dict mask ........ confirmed-GONE readback;
                                               not-found = idempotent success
  P8  position_liquidation None-on-fail ...... via verified open_positions();
                                               None only on positive no-position
  P9  update_leverage None-on-fail ........... VenueRejected(reason) /
                                               WriteUnconfirmed. The deployed
                                               silent clamp-to-max is KEPT
                                               (F5 decision, final): an
                                               over-max ask clamps to the
                                               venue max, the EFFECTIVE
                                               leverage is what gets sent /
                                               venue-confirmed, and the clamp
                                               is logged loudly.
  P10 user_fills stale-cache/[] + item drop .. verified-or-raise; a single
                                               unparseable fill row poisons
                                               the whole read
  P11 equity_with_upnl missing ............... /account account_equity
                                               verified-or-raise
  minor: limit_reduce_only probe-fail-nonfatal -> resting/filled confirmed by
         readback or WriteUnconfirmed.

SEMANTICS NOTE (account_value vs equity_with_upnl — F5 DECISION, FINAL):
deployed semantics are preserved EXACTLY. The deployed adapter's
account_value() key-priority chain resolves to /account `account_equity`
(equity INCL unrealized PnL) — so under this contract BOTH reads resolve to
the SAME verified `account_equity` field (pacifica is a unified-basis venue;
the harness binding declares scenario.unified_equity_basis accordingly).
/account `balance` (EXCL uPnL) is deliberately NOT used as a sizing basis —
that earlier draft was a risk-number delta and was rejected. P2 changes NO
risk numbers.

DRY_RUN: the raw adapter's DRY_RUN=1 layer fabricates ok-shaped write acks
with no venue state behind them — under this wrapper those cannot confirm and
every write surfaces WriteUnconfirmed (fail-closed). Run the wrapper with
DRY_RUN=0; dry-run belongs to the trader layer, not the exchange layer.

PURITY: importing this module pulls in NO bot code and no SDK — bot.*
imports happen lazily inside build_client()/get_client(). `requests` is
imported at module top only for its exception types (the raw adapter it
wraps is requests-based by definition).
"""

from __future__ import annotations

import importlib
import logging
import re
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

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

log = logging.getLogger(__name__)

VENUE = "pacifica"

__all__ = ["PacificaExchangeClient", "build_client", "get_client"]


# ---------------------------------------------------------------------------
# Venue error-string classification (Pacifica speaks strings, not codes)
# ---------------------------------------------------------------------------
# DEFINITIVE rejects: the venue (or a pre-send local gate) refused; state is
# unchanged; blind retry re-rejects. Includes the $10-min-notional class and
# the reduce-only / min-size families.
_REJECT_MARKERS = (
    "below_min_size",
    "trigger_below_min_size",
    "reduce_only_would_increase",
    "reduce only",
    "reduce-only",
    "unknown_symbol",
    "unknown symbol",
    "bad_trigger_px",
    "bad_limit_px",
    "no_mark",
    "not found",
    "not_found",
    "leverage_above_max",
    "insufficient",
    "notional",
    "minimum",
    "min_order",
    "invalid",          # "Invalid stop tick" / "Invalid amount" / bad payloads
    "market not found",
    "verification failed",   # signed-payload schema reject — nothing landed
    "exceeds max",
    "http 4",           # definitive 4xx (400/401/403) — request refused
)
_RATELIMIT_MARKERS = ("rate limit", "too many requests")
#: "429" must match as a NUMBER, never inside a longer digit run — raw error
#: messages embed request URLs whose ms timestamps (start_time=...742910...)
#: contain "429" wall-clock-dependently (flake root-caused 2026-07-03: a
#: transport error was mislabelled RateLimited whenever the epoch happened to
#: contain the digits).
_RE_429 = re.compile(r"(?<!\d)429(?!\d)")


def _is_rate_msg(msg: str) -> bool:
    low = (msg or "").lower()
    return any(m in low for m in _RATELIMIT_MARKERS) or \
        bool(_RE_429.search(low))
# Transport-shaped: the request MAY have reached the venue — ambiguous.
_TRANSPORT_MARKERS = (
    "timed out", "timeout", "read timed out",
    "connection", "connect",
    "network error",
    "max retries",
    "temporarily", "unavailable", "bad gateway",
    "internal server error", "http 5",
    "poll timeout",
)


#: harness-integrity fingerprints (F7a, nado _reraise_harness pattern): the
#: raw adapter funnels exceptions into error-string dicts, so a harness
#: violation (UninterceptedRealCall / UnboundedCallDetected) can arrive as a
#: MESSAGE — re-raise it as AssertionError, never re-type as a venue error.
_HARNESS_PAT = ("unintercepted", "timeout-law", "unboundedcalldetected",
                "blocked by conformance harness")


def _reraise_harness(exc: BaseException) -> None:
    if isinstance(exc, AssertionError):
        raise exc


def _raise_if_harness_msg(msg: str) -> None:
    m = (msg or "").lower()
    if any(p in m for p in _HARNESS_PAT):
        raise AssertionError(msg)


def _is_marked(msg: str, markers: Tuple[str, ...]) -> bool:
    low = (msg or "").lower()
    return any(m in low for m in markers)


def _oid_eq(a: Any, b: Any) -> bool:
    """Suffix-tolerant order-id equality (venues/mocks may prefix ids)."""
    sa, sb = str(a or ""), str(b or "")
    if not sa or not sb:
        return False
    return sa == sb or sa.endswith("-" + sb) or sb.endswith("-" + sa)


def _side_norm(side: Any) -> Optional[str]:
    s = str(side or "").strip().lower()
    if s in ("bid", "buy", "long"):
        return "buy"
    if s in ("ask", "sell", "short"):
        return "sell"
    return None


class PacificaExchangeClient(ExchangeClient):
    """Contract-conforming Pacifica client over the raw PacificaClient.

    The raw adapter contributes: its signed-request machinery (agent-wallet
    signing, 429 backoff, session default timeouts, global RL pacer), market
    meta / rounding, and the candles fetch path. Every masked-neutral return
    of the raw layer is bypassed or re-verified here.
    """

    READ_TIMEOUT: Tuple[float, float] = (5.0, 10.0)   # unsigned GETs
    RETRY_429 = 2                 # extra attempts on 429 for direct GETs
    RETRY_429_SLEEP = 0.15
    CLOSE_ATTEMPTS = 3            # ensure_flat close->readback cycles
    FILL_POLL_SEC = 2.0           # market order fill-confirm budget
    FILL_POLL_GAP = 0.2
    CONFIRM_TRIES = 4             # trigger/cancel/limit list-readback tries
    CONFIRM_GAP = 0.15

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self._mod = importlib.import_module(type(raw).__module__)
        self._rest = self._mod.REST_URL
        self._fmt = self._mod._fmt_decimal
        self._tf_ms: Mapping[str, int] = self._mod.TF_MS
        self._lock = threading.RLock()
        self._marks: Dict[str, Tuple[float, float]] = {}  # coin -> (px, ts)
        if getattr(raw, "dry_run", False):
            log.warning(
                "PacificaExchangeClient built over a DRY_RUN=1 raw adapter — "
                "fabricated write acks cannot be readback-confirmed; every "
                "write will raise WriteUnconfirmed (fail-closed)")
        # Cache hygiene: this wrapper enforces its own staleness bounds; a
        # pre-existing raw-cache snapshot from before this client existed
        # must not be servable through it.
        raw.invalidate_positions_cache()
        with raw._cache_lock:
            raw._candles_cache.clear()
            raw._mark_cache.clear()
            raw._funding_cache.clear()
            raw._prices_cache = None
            raw._fills_cache = None

    # ======================================================================
    # transport helpers
    # ======================================================================

    def _http_get(self, op: str, path: str,
                  params: Optional[Dict[str, Any]] = None,
                  coin: str = "") -> Any:
        """Unsigned GET through the raw adapter's session (RL pacer + default
        timeouts run underneath). Returns the venue `data` payload.
        Raises ReadUnknown / RateLimited — never returns a masked neutral."""
        sess = self._raw.session
        attempts = 1 + self.RETRY_429
        r = None
        for i in range(attempts):
            try:
                r = sess.get(self._rest + path, params=params or {},
                             timeout=self.READ_TIMEOUT)
            except requests.exceptions.RequestException as e:
                raise ReadUnknown("%s: GET %s failed: %s" % (op, path, e),
                                  venue=VENUE, op=op, coin=coin)
            if r.status_code == 429:
                if i < attempts - 1:
                    time.sleep(self.RETRY_429_SLEEP)
                    continue
                raise RateLimited("%s: GET %s 429 budget exhausted"
                                  % (op, path), venue=VENUE, op=op, coin=coin)
            break
        assert r is not None
        if r.status_code >= 400:
            raise ReadUnknown("%s: GET %s -> HTTP %d" % (op, path,
                                                         r.status_code),
                              venue=VENUE, op=op, coin=coin)
        try:
            js = r.json()
        except Exception as e:
            raise ReadUnknown("%s: GET %s unparseable body (%s)"
                              % (op, path, e), venue=VENUE, op=op, coin=coin)
        if isinstance(js, dict):
            if js.get("success") is False:
                msg = str(js.get("error") or js)
                _raise_if_harness_msg(msg)
                if _is_rate_msg(msg):
                    raise RateLimited("%s: %s" % (op, msg), venue=VENUE,
                                      op=op, coin=coin)
                raise ReadUnknown("%s: venue error: %s" % (op, msg),
                                  venue=VENUE, op=op, coin=coin)
            return js.get("data")
        return js

    def _signed_read(self, op: str, path: str, op_type: str,
                     coin: str = "") -> Any:
        """Signed GET via the raw adapter's _signed_request (its 429 backoff
        + session timeouts run underneath). Verified data or raise."""
        resp = self._raw._signed_request("GET", path, op_type, {})
        if not isinstance(resp, dict):
            raise ReadUnknown("%s: non-dict response %r" % (op, resp),
                              venue=VENUE, op=op, coin=coin)
        if not resp.get("success"):
            msg = str(resp.get("error") or resp)
            _raise_if_harness_msg(msg)
            if _is_rate_msg(msg):
                raise RateLimited("%s: %s" % (op, msg), venue=VENUE, op=op,
                                  coin=coin)
            raise ReadUnknown("%s: %s failed: %s" % (op, path, msg),
                              venue=VENUE, op=op, coin=coin)
        return resp.get("data")

    def _signed_write(self, op: str, path: str, op_type: str,
                      payload: Dict[str, Any], coin: str = "") -> Dict[str, Any]:
        """Signed POST. Returns the ack `data` dict on venue success.
        Raises VenueRejected (definitive, nothing landed) or WriteUnconfirmed
        (ambiguous — CALLER MUST READBACK before deciding)."""
        resp = self._raw._signed_request("POST", path, op_type, payload)
        if not isinstance(resp, dict):
            raise WriteUnconfirmed("%s: non-dict response %r" % (op, resp),
                                   may_have_landed=None, venue=VENUE, op=op,
                                   coin=coin)
        if resp.get("success"):
            d = resp.get("data")
            return d if isinstance(d, dict) else {}
        msg = str(resp.get("error") or resp)
        _raise_if_harness_msg(msg)
        if _is_rate_msg(msg):
            # 429 = the venue refused to execute; nothing landed
            raise WriteUnconfirmed("%s: rate limited: %s" % (op, msg),
                                   may_have_landed=False, venue=VENUE, op=op,
                                   coin=coin)
        if "signing failed" in msg.lower():
            # local pre-send failure — provably never left this process
            raise WriteUnconfirmed("%s: %s" % (op, msg),
                                   may_have_landed=False, venue=VENUE, op=op,
                                   coin=coin)
        if _is_marked(msg, _REJECT_MARKERS):
            raise VenueRejected("%s rejected: %s" % (op, msg), reason=msg,
                                venue=VENUE, op=op, coin=coin)
        # transport-shaped or unknown error text: ambiguous by law
        raise WriteUnconfirmed("%s: ambiguous send failure: %s" % (op, msg),
                               may_have_landed=None, venue=VENUE, op=op,
                               coin=coin)

    # ======================================================================
    # meta / rounding helpers (raw adapter machinery, KeyError -> typed)
    # ======================================================================

    def _meta_or_reject(self, coin: str, op: str) -> Any:
        try:
            return self._raw._market(coin)
        except KeyError:
            raise VenueRejected("%s: unknown symbol %s" % (op, coin),
                                reason="unknown_symbol", venue=VENUE, op=op,
                                coin=coin)

    # ======================================================================
    # order/fill listing readbacks
    # ======================================================================

    def _order_rows_unsigned(self, op: str, coin: str = "") -> List[Dict[str, Any]]:
        """Open orders via the UNSIGNED account-scoped listing (the live-proven
        readback shape the raw adapter's limit_reduce_only probe uses)."""
        data = self._http_get(op, "/orders",
                              {"account": self._raw.main_account,
                               "limit": 200}, coin=coin)
        if not isinstance(data, list):
            raise ReadUnknown("%s: /orders payload not a list: %r"
                              % (op, type(data).__name__), venue=VENUE, op=op,
                              coin=coin)
        return data

    def _trigger_rows_signed(self, op: str, coin: str = "") -> List[Dict[str, Any]]:
        """Open orders via the SIGNED get_orders listing (the raw adapter's
        SL-liveness path). Same endpoint, distinct auth shape."""
        data = self._signed_read(op, "/orders", "get_orders", coin=coin)
        if not isinstance(data, list):
            raise ReadUnknown("%s: /orders payload not a list: %r"
                              % (op, type(data).__name__), venue=VENUE, op=op,
                              coin=coin)
        return data

    def _order_info(self, row: Mapping[str, Any]) -> OpenOrderInfo:
        """Strict venue-row -> OpenOrderInfo. Raises KeyError/TypeError/
        ValueError on any unparseable field (caller converts to ReadUnknown —
        per-item drop-and-continue is banned)."""
        oid = row.get("order_id")
        if oid is None:
            oid = row.get("id")
        if oid is None:
            raise KeyError("order row without order_id: %r" % (dict(row),))
        coin = row["symbol"]
        if not coin:
            raise ValueError("order row without symbol")
        side = _side_norm(row.get("side"))
        if side is None:
            raise ValueError("order row with unmappable side %r"
                             % (row.get("side"),))
        size = float(row["amount"])
        limit_px = float(row.get("price") or 0) or None
        trigger_px = float(row.get("stop_price") or 0) or None
        ot = str(row.get("order_type") or "").lower()
        is_trigger = ("stop" in ot) or (trigger_px is not None and
                                        limit_px is None)
        return OpenOrderInfo(
            coin=str(coin), oid=str(oid), side=side, size=size,
            limit_px=limit_px, trigger_px=trigger_px,
            reduce_only=bool(row.get("reduce_only")),
            is_trigger=is_trigger, raw=dict(row))

    @staticmethod
    def _is_sl_row(row: Mapping[str, Any]) -> bool:
        ot = str(row.get("order_type") or "").lower()
        return bool(row.get("reduce_only")) and "stop" in ot

    def _history_rows(self, coin: str = "") -> List[Dict[str, Any]]:
        data = self._http_get("order_history", "/orders/history",
                              {"account": self._raw.main_account,
                               "limit": 20}, coin=coin)
        if not isinstance(data, list):
            raise ReadUnknown("order_history payload not a list",
                              venue=VENUE, op="order_history", coin=coin)
        return data

    def _fills_for_client_oid(self, client_oid: str,
                              coin: str = "") -> Optional[Dict[str, float]]:
        """Venue fills feed filtered to OUR client order id. None = positively
        absent. Raises ReadUnknown when the feed itself cannot be read."""
        data = self._http_get("fills", "/trades/history",
                              {"account": self._raw.main_account,
                               "limit": 200}, coin=coin)
        if not isinstance(data, list):
            raise ReadUnknown("fills payload not a list", venue=VENUE,
                              op="fills", coin=coin)
        mine = [f for f in data
                if str(f.get("client_order_id") or "") == client_oid]
        if not mine:
            return None
        tot = 0.0
        wsum = 0.0
        oid: Any = None
        for f in mine:
            sz = float(f.get("amount") or 0)
            px = float(f.get("price") or 0)
            if sz <= 0 or px <= 0:
                continue
            tot += sz
            wsum += px * sz
            oid = oid or f.get("order_id") or f.get("id")
        if tot <= 0:
            return None
        return {"size": tot, "px": wsum / tot,
                "oid": oid if oid is None else float("nan")} if False else \
            {"size": tot, "px": wsum / tot, "oid": oid}

    def _await_fill(self, coin: str, client_oid: str, oid: Any,
                    budget_sec: float,
                    requested: float) -> Optional[Dict[str, Any]]:
        """Bounded fill confirm: poll the order-history feed for a terminal
        status on OUR order, falling back to the fills feed by client oid.

        Returns {"size", "px", "oid"} for a confirmed (possibly partial)
        fill; None when the venue POSITIVELY shows nothing landed within the
        budget. Raises VenueRejected on a terminal cancelled/rejected/expired
        status with zero fill; raises WriteUnconfirmed when EVERY readback
        read failed (cannot prove either way)."""
        deadline = time.time() + budget_sec
        read_ok = False
        while True:
            try:
                rows = self._history_rows(coin=coin)
                read_ok = True
                for o in rows:
                    if not (_oid_eq(o.get("order_id"), oid) or
                            str(o.get("client_order_id") or "") == client_oid):
                        continue
                    st = str(o.get("order_status") or "").lower()
                    try:
                        fa = float(o.get("filled_amount") or 0)
                    except (TypeError, ValueError):
                        fa = 0.0
                    try:
                        ap = float(o.get("average_filled_price") or 0)
                    except (TypeError, ValueError):
                        ap = 0.0
                    if st == "filled" or (fa > 0 and ap > 0):
                        return {"size": fa, "px": ap,
                                "oid": o.get("order_id") or oid}
                    if st in ("cancelled", "rejected", "expired",
                              "partially_cancelled"):
                        raise VenueRejected(
                            "order %s terminal without fill: %s (%s)"
                            % (oid, st, o.get("reason") or ""),
                            reason=str(o.get("reason") or st), venue=VENUE,
                            op="fill_confirm", coin=coin)
            except (ReadUnknown, RateLimited):
                pass  # bounded loop; final verdict below
            if time.time() >= deadline:
                break
            time.sleep(self.FILL_POLL_GAP)
        # fills-feed fallback (authoritative attribution by client oid)
        try:
            fill = self._fills_for_client_oid(client_oid, coin=coin)
            read_ok = True
            if fill is not None:
                return fill
        except (ReadUnknown, RateLimited):
            pass
        if not read_ok:
            raise WriteUnconfirmed(
                "fill readback unavailable for %s (order %s) — cannot prove "
                "either way" % (coin, oid), may_have_landed=True, venue=VENUE,
                op="fill_confirm", coin=coin)
        return None

    # ======================================================================
    # WRITES
    # ======================================================================

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        meta = self._meta_or_reject(coin, "market_open")
        if intended_px is not None and not allow_marketable:
            # marketability guard (extended pattern): entering at a LEVEL that
            # is already through the book is a re-plan, not a market fill.
            mark = self.mark_price(coin)  # ReadUnknown propagates: nothing sent
            if (is_buy and mark <= intended_px) or \
               ((not is_buy) and mark >= intended_px):
                raise VenueRejected(
                    "market_open(%s): level %.10g already through the book "
                    "(mark %.10g)" % (coin, intended_px, mark),
                    reason="immediately_marketable", venue=VENUE,
                    op="market_open", coin=coin)
        sz_r = self._raw.round_qty(coin, sz)
        if sz_r <= 0:
            raise VenueRejected(
                "market_open(%s): size %.12g <= 0 after lot rounding"
                % (coin, sz), reason="size_zero_after_lot_rounding",
                venue=VENUE, op="market_open", coin=coin)
        # $10-min-notional pre-gate (census P-class; venue enforces it too —
        # this just rejects with an actionable reason without spending a send)
        try:
            mark_px: Optional[float] = self.mark_price(coin)
        except (ReadUnknown, RateLimited):
            mark_px = None  # venue-side gate still applies
        min_notional = float(getattr(meta, "min_notional_usd", 10.0) or 0.0)
        if mark_px and sz_r * mark_px < min_notional:
            raise VenueRejected(
                "market_open(%s): notional $%.2f < $%.2f min-notional floor"
                % (coin, sz_r * mark_px, min_notional),
                reason="below_min_notional", venue=VENUE, op="market_open",
                coin=coin)

        slip_pct = float(self._raw.settings.slippage or 0.0025) * 100.0
        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "reduce_only": False,
            "amount": self._fmt(sz_r),
            "side": "bid" if is_buy else "ask",
            "slippage_percent": "%.2f" % slip_pct,
            "client_order_id": client_oid,
        }
        oid: Any = None
        try:
            data = self._signed_write("market_open", "/orders/create_market",
                                      "create_market_order", payload,
                                      coin=coin)
            oid = data.get("order_id")
        except WriteUnconfirmed as e:
            if e.may_have_landed is False:
                raise  # provably nothing left / venue refused to execute
            # ambiguous send — the fill may exist; readback decides
            fill = self._await_fill(coin, client_oid, None,
                                    self.FILL_POLL_SEC, sz_r)
            if fill is None:
                raise WriteUnconfirmed(
                    "market_open(%s): ambiguous send and readback shows no "
                    "fill (%s)" % (coin, e),
                    may_have_landed=e.may_have_landed, venue=VENUE,
                    op="market_open", coin=coin)
            self.invalidate_positions_cache()
            return FillResult(coin=coin, is_buy=is_buy,
                              avg_px=float(fill["px"]),
                              size=float(fill["size"]),
                              requested_size=float(sz_r),
                              oid=str(fill.get("oid") or "") or None)

        fill = self._await_fill(coin, client_oid, oid, self.FILL_POLL_SEC,
                                sz_r)
        self.invalidate_positions_cache()
        if fill is None:
            raise WriteUnconfirmed(
                "market_open(%s): order acked (oid=%s) but no fill visible "
                "within %.1fs readback budget" % (coin, oid,
                                                  self.FILL_POLL_SEC),
                may_have_landed=True, venue=VENUE, op="market_open",
                coin=coin)
        return FillResult(coin=coin, is_buy=is_buy, avg_px=float(fill["px"]),
                          size=float(fill["size"]),
                          requested_size=float(sz_r),
                          oid=str(fill.get("oid") or oid or "") or None)

    def ensure_flat(self, coin: str) -> FlatResult:
        # initial read — failure here means NOTHING was sent (ReadUnknown up)
        self.invalidate_positions_cache()
        pos_map = self.open_positions()
        if coin not in pos_map:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        pos = pos_map[coin]
        total_to_close = abs(pos.size_signed)
        is_buy_to_close = pos.size_signed < 0
        remaining = total_to_close
        sent_any = False
        exit_px: Optional[float] = None
        attempts = 0
        for attempts in range(1, self.CLOSE_ATTEMPTS + 1):
            sz_r = self._raw.round_qty(coin, remaining)
            amount = self._fmt(sz_r if sz_r > 0 else remaining)
            client_oid = str(uuid.uuid4())
            payload = {
                "symbol": coin,
                "reduce_only": True,
                "amount": amount,
                "side": "bid" if is_buy_to_close else "ask",
                "slippage_percent": "1.0",  # wide on close to ensure fill
                "client_order_id": client_oid,
            }
            oid: Any = None
            try:
                data = self._signed_write("ensure_flat",
                                          "/orders/create_market",
                                          "create_market_order", payload,
                                          coin=coin)
                oid = data.get("order_id")
                sent_any = True
            except VenueRejected:
                # e.g. reduce-only-would-increase because it already closed —
                # the position readback below is the judge, not the reject.
                pass
            except WriteUnconfirmed as e:
                if e.may_have_landed is not False:
                    sent_any = True
            if oid is not None or sent_any:
                # best-effort exit-fill attribution (REAL fill VWAP or None —
                # never a reference price; wick_sl ×1330 class)
                try:
                    fill = self._await_fill(coin, client_oid, oid, 1.5,
                                            remaining)
                    if fill is not None and float(fill.get("px") or 0) > 0:
                        exit_px = float(fill["px"])
                except (VenueRejected, WriteUnconfirmed,
                        ReadUnknown, RateLimited):
                    pass
            # position readback — the ONLY source of "flat" truth
            try:
                self.invalidate_positions_cache()
                now_map = self.open_positions()
            except (ReadUnknown, RateLimited) as e:
                raise WriteUnconfirmed(
                    "ensure_flat(%s): close attempt %d done but the "
                    "confirming position readback FAILED — flatness UNPROVEN: "
                    "%s" % (coin, attempts, e),
                    may_have_landed=sent_any, venue=VENUE, op="ensure_flat",
                    coin=coin)
            if coin not in now_map:
                return FlatResult(coin=coin, already_flat=False,
                                  closed_size=total_to_close,
                                  exit_avg_px=exit_px, attempts=attempts)
            remaining = abs(now_map[coin].size_signed)
            is_buy_to_close = now_map[coin].size_signed < 0
        raise WriteUnconfirmed(
            "ensure_flat(%s): %d close attempts exhausted, position still "
            "open (residual %.12g)" % (coin, attempts, remaining),
            may_have_landed=sent_any, venue=VENUE, op="ensure_flat", coin=coin)

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        self._meta_or_reject(coin, "trigger_sl")
        sz_r = self._raw.round_qty(coin, sz)
        if sz_r <= 0:
            raise VenueRejected(
                "trigger_sl(%s): size %.12g <= 0 after lot rounding"
                % (coin, sz), reason="size_zero_after_lot_rounding",
                venue=VENUE, op="trigger_sl", coin=coin)
        trig_r = self._raw.round_price(coin, trigger_px)
        if trig_r <= 0:
            raise VenueRejected(
                "trigger_sl(%s): trigger px %.12g <= 0 after tick rounding"
                % (coin, trigger_px), reason="bad_trigger_px", venue=VENUE,
                op="trigger_sl", coin=coin)
        client_oid = str(uuid.uuid4())
        payload = {
            "symbol": coin,
            "side": "bid" if is_buy else "ask",
            "reduce_only": True,
            "stop_order": {
                "stop_price": self._fmt(trig_r),
                "client_order_id": client_oid,
                "trigger_price_type": "last_trade_price",
                "amount": self._fmt(sz_r),
            },
        }
        oid: Any = None
        try:
            data = self._signed_write("trigger_sl", "/orders/stop/create",
                                      "create_stop_order", payload, coin=coin)
            oid = data.get("order_id")
            if oid is None:
                oid = data.get("id")
        except WriteUnconfirmed as e:
            if e.may_have_landed is False:
                raise
            info = self._confirm_trigger_live(coin, None, client_oid, e)
            if info is not None:
                return info
            raise WriteUnconfirmed(
                "trigger_sl(%s): ambiguous send and no matching live trigger "
                "on readback (%s)" % (coin, e),
                may_have_landed=e.may_have_landed, venue=VENUE,
                op="trigger_sl", coin=coin)
        # MANDATORY live-list confirm (nado _confirm_trigger_live law):
        # an ok-shaped ack without a listed live trigger is the naked-success
        # class and must NOT be reported as placed.
        info = self._confirm_trigger_live(coin, oid, client_oid, None)
        if info is not None:
            return info
        raise WriteUnconfirmed(
            "trigger_sl(%s): acked (oid=%s) but NOT in the live trigger list "
            "after %d readback tries — refusing to claim placed"
            % (coin, oid, self.CONFIRM_TRIES),
            may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)

    def _confirm_trigger_live(self, coin: str, oid: Any,
                              client_oid: str,
                              send_err: Optional[Exception]
                              ) -> Optional[SLOrderInfo]:
        """Live-list readback for a just-placed trigger. Returns the confirmed
        SLOrderInfo, None for positively-absent, raises WriteUnconfirmed when
        the confirming read itself failed (read-error is NOT absence — the
        nado N9 duplicate-SL-stack lesson)."""
        last_read_err: Optional[Exception] = None
        for i in range(self.CONFIRM_TRIES):
            try:
                rows = self._trigger_rows_signed("trigger_confirm", coin=coin)
            except (ReadUnknown, RateLimited) as e:
                last_read_err = e
                if i < self.CONFIRM_TRIES - 1:
                    time.sleep(self.CONFIRM_GAP)
                continue
            for row in rows:
                if str(row.get("symbol") or "") != coin:
                    continue
                row_oid = row.get("order_id")
                if row_oid is None:
                    row_oid = row.get("id")
                matched = (oid is not None and _oid_eq(row_oid, oid)) or \
                    (str(row.get("client_order_id") or "") == client_oid)
                if not matched:
                    continue
                try:
                    return SLOrderInfo(
                        coin=coin, oid=str(row_oid),
                        trigger_px=float(row.get("stop_price") or 0),
                        size=float(row.get("amount") or 0),
                        is_buy_to_close=(_side_norm(row.get("side")) == "buy"))
                except (TypeError, ValueError) as e:
                    raise WriteUnconfirmed(
                        "trigger_sl(%s): live row for oid=%s unparseable "
                        "(%s) — cannot confirm" % (coin, row_oid, e),
                        may_have_landed=True, venue=VENUE, op="trigger_sl",
                        coin=coin)
            if i < self.CONFIRM_TRIES - 1:
                time.sleep(self.CONFIRM_GAP)
        if last_read_err is not None:
            raise WriteUnconfirmed(
                "trigger_sl(%s): placed%s but the live-trigger confirming "
                "read failed (%s) — read-error is not absence"
                % (coin, "" if send_err is None else " (ambiguous)",
                   last_read_err),
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        return None

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        # send the cancel through the raw adapter (stop endpoint + regular
        # fallback); its response dict NEVER decides — the gone-readback does.
        res = self._raw.cancel_sl_order(coin, oid)
        err = ""
        if not (isinstance(res, dict) and res.get("status") == "ok"):
            err = str((res or {}).get("error") or res)
            _raise_if_harness_msg(err)
        # confirmed-GONE readback (idempotent: absent = success, whether we
        # cancelled it, someone else did, or it never existed)
        last_read_err: Optional[Exception] = None
        still_live = False
        for i in range(self.CONFIRM_TRIES):
            try:
                rows = self._trigger_rows_signed("cancel_confirm", coin=coin)
            except (ReadUnknown, RateLimited) as e:
                last_read_err = e
                if i < self.CONFIRM_TRIES - 1:
                    time.sleep(self.CONFIRM_GAP)
                continue
            last_read_err = None
            still_live = any(
                _oid_eq(r.get("order_id") if r.get("order_id") is not None
                        else r.get("id"), oid)
                for r in rows)
            if not still_live:
                return None  # goal state proven: oid not live
            if i < self.CONFIRM_TRIES - 1:
                time.sleep(self.CONFIRM_GAP)
        if last_read_err is not None:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s, %s): cannot prove order gone — the "
                "confirming list read failed: %s" % (coin, oid, last_read_err),
                may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                coin=coin)
        # order is provably still live
        if err and _is_marked(err, _REJECT_MARKERS) and \
                not _is_marked(err, ("not found", "not_found")):
            raise VenueRejected(
                "cancel_sl_order(%s, %s): venue refused: %s"
                % (coin, oid, err), reason=err, venue=VENUE,
                op="cancel_sl_order", coin=coin)
        raise WriteUnconfirmed(
            "cancel_sl_order(%s, %s): cancel %s but the order is STILL LIVE "
            "on readback" % (coin, oid, "errored (%s)" % err if err
                             else "acked"),
            may_have_landed=True, venue=VENUE, op="cancel_sl_order", coin=coin)

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        resp = self._raw.limit_reduce_only(coin, is_buy, sz, px)
        status = self._parse_hl_statuses(resp)
        kind = status[0]
        if kind == "error":
            msg = status[1]
            _raise_if_harness_msg(msg)
            if _is_rate_msg(msg):
                raise WriteUnconfirmed(
                    "limit_reduce_only(%s): rate limited: %s" % (coin, msg),
                    may_have_landed=False, venue=VENUE,
                    op="limit_reduce_only", coin=coin)
            if _is_marked(msg, _REJECT_MARKERS):
                raise VenueRejected(
                    "limit_reduce_only(%s) rejected: %s" % (coin, msg),
                    reason=msg, venue=VENUE, op="limit_reduce_only", coin=coin)
            raise WriteUnconfirmed(
                "limit_reduce_only(%s): ambiguous failure: %s" % (coin, msg),
                may_have_landed=None, venue=VENUE, op="limit_reduce_only",
                coin=coin)
        if kind == "filled":
            _, avg_px, filled_sz, oid = status
            return OpenOrderInfo(
                coin=coin, oid=str(oid or "filled"),
                side="buy" if is_buy else "sell",
                size=float(filled_sz), limit_px=float(px), reduce_only=True,
                is_trigger=False,
                raw={"filled": {"avgPx": avg_px, "totalSz": filled_sz,
                                "oid": oid}})
        # resting claim — confirm against the live order list (the raw probe
        # is best-effort/non-fatal; here confirmation is mandatory)
        oid = status[1]
        last_read_err: Optional[Exception] = None
        for i in range(self.CONFIRM_TRIES):
            try:
                rows = self._order_rows_unsigned("limit_confirm", coin=coin)
                last_read_err = None
            except (ReadUnknown, RateLimited) as e:
                last_read_err = e
                rows = []
            for row in rows:
                row_oid = row.get("order_id")
                if row_oid is None:
                    row_oid = row.get("id")
                if _oid_eq(row_oid, oid):
                    try:
                        return self._order_info(row)
                    except (KeyError, TypeError, ValueError) as e:
                        raise WriteUnconfirmed(
                            "limit_reduce_only(%s): resting row unparseable "
                            "(%s)" % (coin, e), may_have_landed=True,
                            venue=VENUE, op="limit_reduce_only", coin=coin)
            # not resting — did it fill immediately?
            try:
                for h in self._history_rows(coin=coin):
                    if not _oid_eq(h.get("order_id"), oid):
                        continue
                    fa = float(h.get("filled_amount") or 0)
                    ap = float(h.get("average_filled_price") or 0)
                    if fa > 0 and ap > 0:
                        return OpenOrderInfo(
                            coin=coin, oid=str(oid),
                            side="buy" if is_buy else "sell",
                            size=fa, limit_px=float(px), reduce_only=True,
                            is_trigger=False,
                            raw={"filled": {"avgPx": ap, "totalSz": fa,
                                            "oid": oid}})
            except (ReadUnknown, RateLimited) as e:
                last_read_err = last_read_err or e
            if i < self.CONFIRM_TRIES - 1:
                time.sleep(self.CONFIRM_GAP)
        raise WriteUnconfirmed(
            "limit_reduce_only(%s): acked (oid=%s) but neither resting nor "
            "filled on readback%s" % (
                coin, oid,
                " (confirming reads failing: %s)" % last_read_err
                if last_read_err else ""),
            may_have_landed=True, venue=VENUE, op="limit_reduce_only",
            coin=coin)

    @staticmethod
    def _parse_hl_statuses(resp: Any) -> Tuple[Any, ...]:
        """Parse the raw adapter's HL-shaped response envelope into
        ('filled', avg_px, size, oid) | ('resting', oid) | ('error', msg)."""
        try:
            statuses = resp["response"]["data"]["statuses"]
            st = statuses[0]
        except Exception:
            return ("error", "unparseable adapter response: %r" % (resp,))
        if "error" in st:
            return ("error", str(st["error"]))
        if "filled" in st:
            f = st["filled"]
            return ("filled", float(f.get("avgPx") or 0),
                    float(f.get("totalSz") or 0), f.get("oid"))
        if "resting" in st:
            return ("resting", st["resting"].get("oid"))
        return ("error", "unrecognized status shape: %r" % (st,))

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        """F5 DECISION (final): deployed semantics preserved EXACTLY — the
        deployed adapter silently clamps an over-max ask to the venue max
        (exchange_pacifica.py:1294-1301) and proceeds; risk numbers are
        unchanged. The wrapper keeps the clamp, LOGS it loudly, and sends /
        venue-confirms the EFFECTIVE leverage actually set (the venue's own
        above-max reject would fire if the clamp ever regressed, so a clean
        return proves the effective value). Venue errors on the effective
        ask surface typed (VenueRejected / WriteUnconfirmed) — never the raw
        None-mask (census P9)."""
        meta = self._meta_or_reject(coin, "update_leverage")
        effective = int(leverage)
        max_lev = int(getattr(meta, "max_leverage", 0) or 0)
        if max_lev > 0 and effective > max_lev:
            log.warning(
                "update_leverage(%s): %dx requested > venue max %dx — "
                "clamped to the EFFECTIVE %dx (deployed silent-clamp "
                "semantics, F5)", coin, effective, max_lev, max_lev)
            effective = max_lev
        payload = {"symbol": coin, "leverage": effective}
        self._signed_write("update_leverage", "/account/leverage",
                           "update_leverage", payload, coin=coin)
        # margin-mode leg: deployed best-effort semantics preserved
        try:
            self._raw._signed_request("POST", "/account/margin",
                                      "update_margin_mode",
                                      {"symbol": coin,
                                       "is_isolated": not is_cross})
        except Exception as e:  # noqa: BLE001 — best-effort by design
            _reraise_harness(e)
            log.warning("update_leverage(%s): margin-mode leg failed: %s",
                        coin, e)
        return None

    # ======================================================================
    # READS
    # ======================================================================

    def open_positions(self) -> Mapping[str, PositionInfo]:
        data = self._signed_read("open_positions", "/positions",
                                 "get_positions")
        if not isinstance(data, list):
            raise ReadUnknown("open_positions: payload not a list: %r"
                              % (type(data).__name__,), venue=VENUE,
                              op="open_positions")
        out: Dict[str, PositionInfo] = {}
        for p in data:
            try:
                sym = p["symbol"]
                if not sym:
                    raise ValueError("empty symbol")
                amount = float(p["amount"])
                side = _side_norm(p.get("side"))
                if side == "buy":
                    sz_signed = abs(amount)
                elif side == "sell":
                    sz_signed = -abs(amount)
                else:
                    sz_signed = amount  # venue returned a signed amount
                if sz_signed == 0.0:
                    continue  # dust-flat row: positively no position
                entry_px = float(p.get("entry_price", p.get("entry")) or 0)
                liq_raw = p.get("liquidation_price")
                liq_px = (None if liq_raw in (None, "")
                          else float(liq_raw))
                lev: Optional[float]
                try:
                    lev = float(p.get("leverage")) if p.get("leverage") \
                        else None
                except (TypeError, ValueError):
                    lev = None
                upnl: Optional[float]
                try:
                    upnl = (float(p.get("unrealized_pnl"))
                            if p.get("unrealized_pnl") not in (None, "")
                            else None)
                except (TypeError, ValueError):
                    upnl = None
                out[str(sym)] = PositionInfo(
                    coin=str(sym), size_signed=sz_signed, entry_px=entry_px,
                    leverage=lev, liquidation_px=liq_px, unrealized_pnl=upnl,
                    raw=dict(p))
            except (KeyError, TypeError, ValueError) as e:
                # one unparseable row poisons the WHOLE snapshot — a silently
                # dropped live position is the phantom-guard mass-close class
                raise ReadUnknown(
                    "open_positions: unparseable position row (%s): %r"
                    % (e, p), venue=VENUE, op="open_positions")
        return out

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        rows = self._order_rows_unsigned("open_orders")
        out: List[OpenOrderInfo] = []
        for row in rows:
            try:
                out.append(self._order_info(row))
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "open_orders: unparseable order row (%s): %r — one bad "
                    "row poisons the whole listing" % (e, row),
                    venue=VENUE, op="open_orders")
        return out

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        rows = self._trigger_rows_signed("list_open_sl_orders", coin=coin)
        out: List[str] = []
        for row in rows:
            if str(row.get("symbol") or "") != coin:
                continue
            if not self._is_sl_row(row):
                continue
            oid = row.get("order_id")
            if oid is None:
                oid = row.get("id")
            if oid is None:
                # an id-less trigger cannot be cancelled/matched anyway;
                # census: harmless — log, keep
                log.warning("list_open_sl_orders(%s): id-less stop row %r",
                            coin, row)
                continue
            out.append(str(oid))
        return out

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        rows = self._order_rows_unsigned("list_reduce_only_triggers")
        out: List[OpenOrderInfo] = []
        for row in rows:
            ot = str(row.get("order_type") or "").lower()
            if not (bool(row.get("reduce_only")) and "stop" in ot):
                continue
            try:
                out.append(self._order_info(row))
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "list_reduce_only_triggers: unparseable trigger row "
                    "(%s): %r — the orphan sweep CANCELS from this list; a "
                    "masked partial listing is a gun" % (e, row),
                    venue=VENUE, op="list_reduce_only_triggers")
        return out

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        try:
            px = self._fetch_mark(coin)
        except (ReadUnknown, RateLimited) as e:
            with self._lock:
                cached = self._marks.get(coin)
            if cached is not None:
                cpx, ts = cached
                age = time.time() - ts
                if age <= max_age_sec:
                    return cpx  # cache INSIDE tolerance: legal + encouraged
                raise StaleData(
                    "mark_price(%s): only a %.3fs-old cached value available "
                    "(tolerance %.3fs); fresh fetch failed: %s"
                    % (coin, age, max_age_sec, e), age_sec=age,
                    tolerance_sec=max_age_sec, venue=VENUE, op="mark_price",
                    coin=coin)
            raise
        with self._lock:
            self._marks[coin] = (px, time.time())
        return px

    def _fetch_mark(self, coin: str) -> float:
        data = self._http_get("mark_price", "/info/prices", None, coin=coin)
        if not isinstance(data, list):
            raise ReadUnknown("mark_price: /info/prices payload not a list",
                              venue=VENUE, op="mark_price", coin=coin)
        for m in data:
            if m.get("symbol") != coin:
                continue
            try:
                px = float(m.get("mark") or 0)
            except (TypeError, ValueError):
                px = 0.0
            if px <= 0.0:
                raise ReadUnknown(
                    "mark_price(%s): venue mark %r absent/insane — refusing "
                    "to serve 0.0" % (coin, m.get("mark")), venue=VENUE,
                    op="mark_price", coin=coin)
            return px
        raise ReadUnknown(
            "mark_price(%s): symbol missing from the /info/prices snapshot"
            % coin, venue=VENUE, op="mark_price", coin=coin)

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> Any:
        tf_ms = self._tf_ms.get(interval)
        if tf_ms is None:
            raise ValueError("candles: unsupported interval %r (caller bug)"
                             % (interval,))
        try:
            self._raw._market(coin)
        except KeyError:
            raise ValueError("candles: unknown coin %r (caller bug)" % (coin,))

        df = self._fetch_candles(coin, interval, limit)
        lag = self._candle_lag_bars(df, tf_ms)
        if lag is None or lag > max_stale_bars:
            # one forced re-fetch past the raw adapter's bar-aligned cache
            self._raw.invalidate_candles_cache(coin, interval)
            df = self._fetch_candles(coin, interval, limit)
            lag = self._candle_lag_bars(df, tf_ms)
            if lag is None:
                raise ReadUnknown(
                    "candles(%s,%s): last-bar timestamp unreadable — "
                    "freshness unverifiable" % (coin, interval), venue=VENUE,
                    op="candles", coin=coin)
            if lag > max_stale_bars:
                raise StaleData(
                    "candles(%s,%s): last CLOSED bar lags %.2f bars "
                    "(tolerance %.2f)" % (coin, interval, lag,
                                          max_stale_bars),
                    age_sec=lag * tf_ms / 1000.0,
                    tolerance_sec=max_stale_bars * tf_ms / 1000.0,
                    venue=VENUE, op="candles", coin=coin)
        return df

    def _fetch_candles(self, coin: str, interval: str, limit: int) -> Any:
        try:
            df = self._raw.candles(coin, interval, limit)
        except Exception as e:  # raw raises RuntimeError on fetch failure
            _reraise_harness(e)
            msg = str(e)
            _raise_if_harness_msg(msg)
            if _is_rate_msg(msg):
                raise RateLimited("candles(%s,%s): %s" % (coin, interval,
                                                          msg),
                                  venue=VENUE, op="candles", coin=coin)
            raise ReadUnknown("candles(%s,%s): fetch failed: %s"
                              % (coin, interval, msg), venue=VENUE,
                              op="candles", coin=coin)
        if df is None or len(df) == 0:
            raise ReadUnknown(
                "candles(%s,%s): venue returned no bars — silent-empty is "
                "banned (signal-loss class)" % (coin, interval), venue=VENUE,
                op="candles", coin=coin)
        return df

    @staticmethod
    def _candle_lag_bars(df: Any, tf_ms: int) -> Optional[float]:
        try:
            ts = df["time"].iloc[-1]
            last_open_ms = int(getattr(ts, "value") // 10 ** 6)
        except Exception:
            return None
        now_ms = int(time.time() * 1000)
        boundary = (now_ms // tf_ms) * tf_ms
        return (boundary - (last_open_ms + tf_ms)) / float(tf_ms)

    def _account_data(self, op: str) -> Mapping[str, Any]:
        data = self._signed_read(op, "/account", "get_account")
        if not isinstance(data, dict):
            raise ReadUnknown("%s: /account payload not an object: %r"
                              % (op, type(data).__name__), venue=VENUE, op=op)
        return data

    def equity_with_upnl(self) -> float:
        d = self._account_data("equity_with_upnl")
        for k in ("account_equity", "cross_account_equity"):
            if k in d:
                try:
                    return float(d[k])
                except (TypeError, ValueError) as e:
                    raise ReadUnknown(
                        "equity_with_upnl: bad %s=%r: %s" % (k, d.get(k), e),
                        venue=VENUE, op="equity_with_upnl")
        raise ReadUnknown(
            "equity_with_upnl: account_equity missing from /account: %r"
            % (sorted(d),), venue=VENUE, op="equity_with_upnl")

    def account_value(self) -> float:
        """Sizing basis — EXACTLY the deployed trader's semantics (F5
        DECISION, final): the deployed adapter's account_value key-priority
        chain resolves to /account `account_equity` (equity INCL unrealized
        PnL), i.e. the SAME verified field equity_with_upnl() reads —
        pacifica is a unified-basis venue. Verified-or-raise (census P1:
        the raw path masked to 0.0). Risk numbers unchanged."""
        d = self._account_data("account_value")
        for k in ("account_equity", "cross_account_equity"):
            if k in d:
                try:
                    return float(d[k])
                except (TypeError, ValueError) as e:
                    raise ReadUnknown(
                        "account_value: bad %s=%r: %s" % (k, d.get(k), e),
                        venue=VENUE, op="account_value")
        raise ReadUnknown(
            "account_value: account_equity missing from /account: %r"
            % (sorted(d),), venue=VENUE, op="account_value")

    def margin_used_usd(self) -> float:
        d = self._account_data("margin_used_usd")
        if "total_margin_used" not in d:
            raise ReadUnknown(
                "margin_used_usd: total_margin_used missing from /account: %r"
                % (sorted(d),), venue=VENUE, op="margin_used_usd")
        try:
            return float(d["total_margin_used"] or 0)
        except (TypeError, ValueError) as e:
            raise ReadUnknown(
                "margin_used_usd: bad total_margin_used=%r: %s"
                % (d.get("total_margin_used"), e), venue=VENUE,
                op="margin_used_usd")

    def position_liquidation(self, coin: str) -> Optional[float]:
        pos_map = self.open_positions()  # ReadUnknown propagates (H3/P8 class)
        pos = pos_map.get(coin)
        if pos is None:
            return None  # POSITIVELY no position
        return pos.liquidation_px

    def user_fills(self, max_age_sec: float = 60.0
                   ) -> Sequence[Mapping[str, Any]]:
        data = self._http_get("user_fills", "/trades/history",
                              {"account": self._raw.main_account,
                               "limit": 200})
        if not isinstance(data, list):
            raise ReadUnknown("user_fills: payload not a list", venue=VENUE,
                              op="user_fills")
        side_map = {"open_long": "Open Long", "open_short": "Open Short",
                    "close_long": "Close Long", "close_short": "Close Short"}
        out: List[Mapping[str, Any]] = []
        for f in data:
            try:
                sym = f["symbol"]
                if not sym:
                    raise ValueError("empty symbol")
                px_v = float(f["price"])
                sz_v = float(f["amount"])
                side_raw = str(f.get("side") or "")
                out.append({
                    "coin": str(sym),
                    "dir": side_map.get(side_raw, side_raw),
                    "side": ("B" if ("long" in side_raw and
                                     "open" in side_raw) or
                                    ("short" in side_raw and
                                     "close" in side_raw) else "A"),
                    "time": int(f.get("created_at") or
                                f.get("timestamp") or 0),
                    "closedPnl": float(f.get("pnl") or 0),
                    "sz": sz_v,
                    "px": px_v,
                    "fee": float(f.get("fee") or 0),
                    "oid": f.get("order_id") or f.get("id"),
                    "client_order_id": f.get("client_order_id"),
                })
            except (KeyError, TypeError, ValueError) as e:
                # a persistently-unparseable fill must not be invisible
                # forever (census P10) — poison the whole read
                raise ReadUnknown(
                    "user_fills: unparseable fill row (%s): %r" % (e, f),
                    venue=VENUE, op="user_fills")
        return out

    # ---------------------------------------------------- CONTRACT-EXEMPT
    # Non-contract caller surface (F4 / mapping §6): VERBATIM delegation to
    # the raw adapter, semantics (incl. dict|None returns and the raw
    # _signed_request DRY gate) UNCHANGED — the P3 repoint cannot
    # AttributeError. Typed-error laws do NOT apply here.
    #   callers grepped 2026-07-03: trader.py (asset, round_price,
    #   add_isolated_margin), scanner.py (invalidate_candles_cache),
    #   liquidity_snapshot.py (orderbook_snapshot); the rest are raw-adapter
    #   surface kept for parity with mapping §6.

    # NO add_isolated_margin: the pacifica raw adapter does not define it and
    # the trader's REMEDY-A branch is hasattr-GATED (trader.py:683) — adding
    # a passthrough would flip that gate open and AttributeError at runtime.
    CONTRACT_EXEMPT_PASSTHROUGH: Tuple[str, ...] = (
        "asset", "round_price", "round_qty", "slip_per_side", "funding_rate",
        "spot_usdc", "orderbook_snapshot", "compute_realized_pnl",
        "trigger_tp", "invalidate_candles_cache",
    )

    def asset(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.asset."""
        return self._raw.asset(*a, **kw)

    def round_price(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.round_price."""
        return self._raw.round_price(*a, **kw)

    def round_qty(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.round_qty."""
        return self._raw.round_qty(*a, **kw)

    def slip_per_side(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.slip_per_side."""
        return self._raw.slip_per_side(*a, **kw)

    def funding_rate(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.funding_rate."""
        return self._raw.funding_rate(*a, **kw)

    def spot_usdc(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.spot_usdc."""
        return self._raw.spot_usdc(*a, **kw)

    def orderbook_snapshot(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.orderbook_snapshot."""
        return self._raw.orderbook_snapshot(*a, **kw)

    def compute_realized_pnl(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.compute_realized_pnl."""
        return self._raw.compute_realized_pnl(*a, **kw)

    def trigger_tp(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.trigger_tp (raw DRY
        gate applies)."""
        return self._raw.trigger_tp(*a, **kw)

    def invalidate_candles_cache(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.invalidate_candles_cache."""
        return self._raw.invalidate_candles_cache(*a, **kw)

    @property
    def raw(self) -> Any:
        """The underlying bot.exchange_pacifica.PacificaClient (forensics
        only)."""
        return self._raw

    # ------------------------------------------------------------ cache ctl

    def invalidate_positions_cache(self) -> None:
        self._raw.invalidate_positions_cache()
        return None


# ===========================================================================
# factories
# ===========================================================================

def build_client(raw: Any = None) -> PacificaExchangeClient:
    """Wrap an already-constructed bot.exchange_pacifica.PacificaClient
    (conformance-harness hook). raw=None -> construct from env (host)."""
    if raw is None:
        return get_client()
    return PacificaExchangeClient(raw)


def get_client() -> PacificaExchangeClient:
    """Production factory: build the raw adapter from the bot env, wrap it."""
    from bot.config import Settings  # lazy: host-only import
    from bot.exchange_pacifica import PacificaClient
    return PacificaExchangeClient(PacificaClient(Settings.from_env()))
