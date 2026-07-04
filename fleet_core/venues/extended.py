"""fleet_core.venues.extended — THE conforming ExchangeClient for Extended.

Wraps the existing raw adapter (bots/extended/bot/exchange_extended.py
ExtendedClient — untouched; its in-place fixes ride the P3 cutover) and
enforces the fleet_core.exchange_api contract AT THIS LAYER.

Census fixes implemented here (proofs/p2_violation_census.md §3, venue=extended):

  E1  account_value 0.0-on-failure ....... _api_get_balance verified-or-raise
                                           (ReadUnknown / RateLimited)
  E2  mark_price 0.0-on-failure .......... own cache + insanity check;
                                           StaleData beyond max_age_sec,
                                           ReadUnknown otherwise
  E3  candles 6× silent empty-df ......... unsupported interval / unknown coin
                                           -> ValueError (caller bug); fetch
                                           fail / empty payload -> ReadUnknown;
                                           last-closed-bar boundary gate ->
                                           StaleData
  E4  trigger_sl ok-shaped error dict .... definitive reject -> VenueRejected;
                                           ambiguous -> WriteUnconfirmed; and
                                           MANDATORY live-list confirm (port of
                                           nado _confirm_trigger_live) — an ack
                                           without a listed live trigger raises
  E5  market_close masks (ensure_flat) ... ensure_flat() is the only close
                                           primitive: reduce-only IOC close +
                                           fresh position readback loop; flat
                                           proven or WriteUnconfirmed
  E6  market_open terminal error dicts ... FillResult built from the venue
                                           fills-feed readback (never a request
                                           echo, never mark); ambiguous send ->
                                           readback; nothing attributable ->
                                           WriteUnconfirmed(may_have_landed)
  E7  cancel_sl_order error-dict mask .... cancel then confirmed-GONE readback
                                           (absence from the live order list);
                                           venue not-found == success
                                           (idempotent); still-live / listing
                                           down -> WriteUnconfirmed
  E8  update_leverage None-on-failure .... VenueRejected(reason) /
                                           WriteUnconfirmed
  E9  position_liquidation None-on-fail .. ReadUnknown propagates from the
                                           positions read; None only for a
                                           POSITIVELY absent position/liq
  E10 user_fills []-on-failure ........... ReadUnknown / RateLimited
  +   equity_with_upnl (was MISSING) ..... balance + unrealised_pnl, verified-
                                           or-raise. On the real venue
                                           balance + uPnL == equity identically
                                           (venue invariant), so this equals
                                           today's sizing basis — P2 changes NO
                                           risk numbers; the two reads are kept
                                           on distinct venue fields so the
                                           conformance harness can seed them
                                           apart.
  +   open_positions bounded staleness ... bias-to-present cache serve on
                                           failure bounded to <= one loop
                                           interval (census "near-conforms"
                                           ceiling), StaleData(age) beyond
  +   marketability guard ................ intended_px + allow_marketable=False
                                           through the book -> VenueRejected
                                           ('immediately_marketable'); guard
                                           unverifiable -> ReadUnknown (fail
                                           closed — the manual-BTC-short class)

DESIGN
------
* The wrapper talks to the venue ONLY through a narrow `_api_*` layer that
  runs x10-SDK coroutines on the raw adapter's own `_AsyncBridge` with an
  explicit result timeout (timeout law: a hang converts to
  ReadUnknown/WriteUnconfirmed, never a frozen loop). Everything above
  `_api_*` is pure python — offline-testable without the SDK.
* Battle-tested raw ORDER CONSTRUCTION is reused where it already goes
  through plain REST: raw.trigger_sl (TPSL POSITION + SDK signing
  monkey-patch + 28d expiry + limit buffer) and raw.limit_reduce_only. Their
  ok-shaped response dicts are parsed and re-typed here; their placement is
  then readback-confirmed here. raw.market_open / raw.market_close are NOT
  used (they ride the BlockingTradingClient WebSocket path and mask terminal
  failures — E5/E6); entries and closes are placed via plain REST
  create_order_object + orders.place_order and confirmed via the fills feed
  and position readback.
* No live I/O at import or construction: build_client(raw) only stores the
  raw adapter, reads env knobs and clears the raw adapter's caches.

Env knobs (read at construction; conformance binding sets fast values):
  FC_EXT_RETRY_429            extra read attempts after a 429 (default 2)
  FC_EXT_RETRY_429_SLEEP      base sleep between 429 retries (default 0.35s)
  FC_EXT_CONFIRM_ATTEMPTS     readback poll attempts for writes (default 6)
  FC_EXT_CONFIRM_SLEEP_SEC    sleep between readback polls (default 1.5s)
  FC_EXT_CLOSE_ATTEMPTS       ensure_flat close attempts (default 3)
  FC_EXT_POS_TTL_SEC          open_positions serve-cache TTL (default 4s)
  FC_EXT_CANDLE_TTL_SEC       candles cache TTL (default 30s)
  FC_EXT_READ_TIMEOUT_SEC     bridge timeout for reads (default 12s)
  FC_EXT_WRITE_TIMEOUT_SEC    bridge timeout for writes (default 25s)
"""

from __future__ import annotations

import logging
import math
import os
import re
import threading
import time
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from fleet_core.exchange_api import (
    ExchangeClient,
    ExchangeError,
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

log = logging.getLogger("fleet_core.venues.extended")

VENUE = "extended"

# Static protocol data (mirrors bot/exchange_extended.py _INTERVAL_MAP /
# bot/config.py TF_MS — duplicated here so this module needs no bot import
# until an adapter is actually wrapped).
_TF_MS: Dict[str, int] = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
    "1d": 86_400_000, "1w": 604_800_000,
}
_INTERVAL_ISO: Dict[str, str] = {
    "1m": "PT1M", "5m": "PT5M", "15m": "PT15M", "30m": "PT30M",
    "1h": "PT1H", "2h": "PT2H", "4h": "PT4H", "1d": "P1D",
}
# Extended has no native 8h/1w — client-side resample (feedback_extended_no_8h_interval)
_RESAMPLE_FROM: Dict[str, Tuple[str, int]] = {"8h": ("4h", 2), "1w": ("1d", 7)}

_RATE_MARKERS = ("rate limit", "ratelimit", "too many request")
# Venue-definitive reject fingerprints: Extended/x10 error codes + FakeVenue
# reject strings + SDK-side definitive validation phrases. Kept NARROW so a
# transport/parse failure can never classify as a reject (a mis-typed reject
# would skip the readback that decides may_have_landed).
_REJECT_MARKERS = (
    "below_min", "min_size", "min order", "minimum order",
    "reduce_only", "reduce-only", "would_increase",
    "bad_trigger", "bad_limit", "unknown_symbol", "unknown symbol",
    "no_mark", "leverage_above_max", "leverage above",
    "not_found", "not found", "order does not exist",
    "duplicate", "wrong size increment", "immediately marketable",
    "invalid starkex signature",
)
# Numeric markers ("429" rate; x10 reject codes 1101/1121/…/1142) must match
# as STANDALONE numbers — error messages embed request URLs and ms-timestamps
# whose digit runs contain them wall-clock-dependently (pacifica flake class,
# root-caused 2026-07-03; a timestamp containing "1121" mis-typing a
# transport error as a definitive reject would SKIP the readback that decides
# may_have_landed).
_RE_429 = re.compile(r"(?<!\d)429(?!\d)")
_RE_REJECT_CODES = re.compile(
    r"(?<!\d)(?:1101|1121|1122|1123|1137|1140|1141|1142)(?!\d)")
_RE_NOT_FOUND_CODE = re.compile(r"(?<!\d)1142(?!\d)")
_NOT_FOUND_MARKERS = ("not_found", "not found", "order does not exist")


#: harness-integrity fingerprints (F7a, nado _reraise_harness pattern): the
#: raw adapter funnels exceptions into HL-shaped error dicts, so a harness
#: violation (UninterceptedRealCall / UnboundedCallDetected) can arrive as a
#: MESSAGE — re-raise as AssertionError, never re-type as a venue error.
_HARNESS_PAT = ("unintercepted", "timeout-law", "unboundedcalldetected",
                "blocked by conformance harness")


def _reraise_harness(exc: BaseException) -> None:
    if isinstance(exc, AssertionError):
        raise exc


def _raise_if_harness_msg(msg: str) -> None:
    m = (msg or "").lower()
    if any(p in m for p in _HARNESS_PAT):
        raise AssertionError(msg)


def _exc_kind(e: BaseException) -> str:
    """'rate' | 'reject' | 'transport' from an SDK/bridge exception."""
    s = ("%s %s" % (type(e).__name__, e)).lower()
    for mk in _RATE_MARKERS:
        if mk in s:
            return "rate"
    if _RE_429.search(s):
        return "rate"
    for mk in _REJECT_MARKERS:
        if mk in s:
            return "reject"
    if _RE_REJECT_CODES.search(s):
        return "reject"
    return "transport"


def _oid_eq(a: Any, b: Any) -> bool:
    """Tolerant oid equality: exact, or one side is the numeric suffix of the
    other (live Extended oids are plain ints; harness truth ids are
    prefixed)."""
    sa, sb = str(a), str(b)
    if not sa or not sb:
        return False
    return sa == sb or sa.endswith("-" + sb) or sb.endswith("-" + sa)


def _bare(market: str) -> str:
    m = str(market)
    return m.rsplit("-USD", 1)[0] if m.endswith("-USD") else m


class ExtendedExchangeClient(ExchangeClient):
    """Contract implementation over a live bot.exchange_extended.ExtendedClient."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self._lock = threading.RLock()
        self._mark_cache: Dict[str, Tuple[float, float]] = {}
        self._pos_cache: Optional[Tuple[Dict[str, PositionInfo], float]] = None
        self._candles_cache: Dict[Tuple[str, str], Tuple[Any, float]] = {}

        # F2 DRY law: the extended raw adapter has NO exchange-layer DRY gate
        # (trader-level guards only), so the wrapper enforces it — under
        # DRY_RUN=1 no write leaves this process (fail-closed
        # WriteUnconfirmed(may_have_landed=False); update_leverage returns a
        # benign None, the HL _dry_guard caller-proceeds semantics).
        _dry = getattr(getattr(raw, "settings", None), "dry_run", None)
        if _dry is None:
            _dry = os.environ.get("DRY_RUN", "0")
        try:
            self._dry = bool(int(str(_dry)))
        except (TypeError, ValueError):
            self._dry = bool(_dry)
        if self._dry:
            log.warning("ExtendedExchangeClient in DRY_RUN=1 — every write "
                        "is blocked at the wrapper (fail-closed)")

        env = os.environ.get
        self._retry429 = max(0, int(env("FC_EXT_RETRY_429", "2")))
        self._retry_sleep = float(env("FC_EXT_RETRY_429_SLEEP", "0.35"))
        self._confirm_attempts = max(1, int(env("FC_EXT_CONFIRM_ATTEMPTS", "6")))
        self._confirm_sleep = float(env("FC_EXT_CONFIRM_SLEEP_SEC", "1.5"))
        self._close_attempts = max(1, int(env("FC_EXT_CLOSE_ATTEMPTS", "3")))
        self._pos_ttl = float(env("FC_EXT_POS_TTL_SEC", "4.0"))
        self._candle_ttl = float(env("FC_EXT_CANDLE_TTL_SEC", "30.0"))
        self._read_t = float(env("FC_EXT_READ_TIMEOUT_SEC", "12"))
        self._write_t = float(env("FC_EXT_WRITE_TIMEOUT_SEC", "25"))
        # bias-to-present ceiling for open_positions == one main-loop interval
        # (the census-approved extended pattern, formalized with StaleData)
        self._pos_stale_bound = float(
            getattr(getattr(raw, "settings", None), "loop_interval_sec", 60)
            or 60)

        # Fresh-truth discipline: never inherit the raw adapter's caches
        # (they may be stale/poisoned relative to this client's lifetime).
        for meth in ("invalidate_positions_cache", "invalidate_candles_cache"):
            try:
                getattr(raw, meth)()
            except Exception:  # pragma: no cover — defensive
                pass
        try:
            with raw._cache_lock:  # noqa: SLF001
                raw._mark_cache.clear()  # noqa: SLF001
        except Exception:  # pragma: no cover — defensive
            pass

    # =====================================================================
    # SDK bridge layer (_api_*) — the ONLY code that touches x10 objects.
    # Each call runs on the raw adapter's bridge with an explicit result
    # timeout and returns/accepts plain python; every exception escapes raw
    # for _exc_kind classification above this layer.
    # =====================================================================

    def _run(self, coro: Any, timeout: float) -> Any:
        return self._raw._bridge.run(coro, timeout=timeout)  # noqa: SLF001

    @staticmethod
    def _sdk():  # lazy: only resolved when an _api_* actually fires
        import bot.exchange_extended as X  # noqa: PLC0415
        return X

    def _api_get_balance(self) -> Dict[str, float]:
        async def _go():
            r = await self._raw._client.account.get_balance()  # noqa: SLF001
            d = r.data
            return {
                "equity": float(d.equity),
                "balance": float(d.balance),
                "upnl": float(d.unrealised_pnl),
                "margin": float(d.initial_margin),
            }
        return self._run(_go(), self._read_t)

    def _api_get_positions(self) -> List[Dict[str, Any]]:
        async def _go():
            r = await self._raw._client.account.get_positions()  # noqa: SLF001
            rows: List[Dict[str, Any]] = []
            for p in (r.data or []):
                rows.append({
                    "market": str(p.market),
                    "side": str(p.side),
                    "size": p.size,
                    "open_price": p.open_price,
                    "liq": getattr(p, "liquidation_price", None),
                    "upnl": getattr(p, "unrealised_pnl", None),
                    "leverage": getattr(p, "leverage", None),
                    "value": getattr(p, "value", None),
                })
            return rows
        return self._run(_go(), self._read_t)

    def _api_get_stats_mark(self, market_name: str) -> float:
        async def _go():
            r = await self._raw._client.markets_info.get_market_statistics(  # noqa: SLF001
                market_name=market_name)
            return float(r.data.mark_price)
        return self._run(_go(), self._read_t)

    def _api_get_book(self, market_name: str) -> Tuple[float, float]:
        async def _go():
            r = await self._raw._client.markets_info.get_orderbook_snapshot(  # noqa: SLF001
                market_name=market_name)
            d = r.data
            bid = d.bid[0] if getattr(d, "bid", None) else None
            ask = d.ask[0] if getattr(d, "ask", None) else None
            b = float(getattr(bid, "price", 0) or 0) if bid is not None else 0.0
            a = float(getattr(ask, "price", 0) or 0) if ask is not None else 0.0
            return b, a
        return self._run(_go(), self._read_t)

    def _api_get_candles(self, market_name: str, iso_interval: str,
                         limit: int) -> List[Dict[str, float]]:
        async def _go():
            r = await self._raw._client.markets_info.get_candles_history(  # noqa: SLF001
                market_name=market_name, candle_type="trades",
                interval=iso_interval, limit=limit)
            rows: List[Dict[str, float]] = []
            for c in (getattr(r, "data", None) or []):
                rows.append({
                    "t": int(c.timestamp),
                    "o": float(c.open), "h": float(c.high),
                    "l": float(c.low), "c": float(c.close),
                    "v": float(c.volume),
                })
            return rows
        return self._run(_go(), self._read_t)

    def _api_get_open_orders(self, market_name: Optional[str] = None,
                             tpsl_only: bool = False) -> List[Dict[str, Any]]:
        X = self._sdk()

        async def _go():
            kw: Dict[str, Any] = {}
            if market_name:
                kw["market_names"] = [market_name]
            if tpsl_only:
                kw["order_type"] = X.OrderType.TPSL
            r = await self._raw._client.account.get_open_orders(**kw)  # noqa: SLF001
            rows: List[Dict[str, Any]] = []
            for o in (r.data or []):
                typ = str(getattr(o, "type", "") or "").upper()
                sl_leg = getattr(o, "stop_loss", None)
                trig = (getattr(o, "trigger_price", None)
                        or (getattr(sl_leg, "trigger_price", None)
                            if sl_leg is not None else None))
                rows.append({
                    "oid": str(getattr(o, "id", "") or ""),
                    "market": str(getattr(o, "market", "") or ""),
                    "side": ("buy" if "BUY" in str(getattr(o, "side", "")).upper()
                             else "sell"),
                    "qty": getattr(o, "qty", None),
                    "price": getattr(o, "price", None),
                    "trigger_px": trig,
                    "reduce_only": bool(getattr(o, "reduce_only", False)),
                    "is_trigger": ("TPSL" in typ or "CONDITIONAL" in typ
                                   or bool(tpsl_only)),
                    "client_oid": getattr(o, "external_id", None),
                })
            return rows
        return self._run(_go(), self._read_t)

    def _api_get_trades(self) -> List[Dict[str, Any]]:
        async def _go():
            r = await self._raw._client.account.get_trades()  # noqa: SLF001
            rows: List[Dict[str, Any]] = []
            for f in (r.data or []):
                ro = getattr(f, "reduce_only",
                             getattr(f, "is_reduce_only", None))
                rows.append({
                    "market": str(f.market),
                    "coin": _bare(f.market),
                    "side": ("buy" if "BUY" in str(f.side).upper() else "sell"),
                    "px": float(f.price),
                    "sz": float(f.qty),
                    "fee": float(getattr(f, "fee", 0) or 0),
                    "time_ms": int(f.created_time),
                    "oid": str(getattr(f, "id", "") or ""),
                    "order_id": getattr(f, "order_id", None),
                    "trade_type": str(getattr(f, "trade_type", "") or ""),
                    "reduce_only": (None if ro is None else bool(ro)),
                })
            return rows
        return self._run(_go(), self._read_t)

    def _api_place_market(self, m: Any, is_buy: bool, qty: Decimal,
                          px: float, reduce_only: bool, ext_id: str
                          ) -> Dict[str, Any]:
        """Plain-REST IOC market-shaped order (NOT the BlockingTradingClient
        WebSocket path — see module docstring). Returns the ack {'oid': ...};
        raises the raw SDK exception on any failure."""
        X = self._sdk()
        from decimal import ROUND_DOWN, ROUND_UP  # noqa: PLC0415
        side = X.OrderSide.BUY if is_buy else X.OrderSide.SELL
        tick = m.trading_config.min_price_change
        px_dec = Decimal(str(px)).quantize(
            tick, rounding=ROUND_UP if is_buy else ROUND_DOWN)
        taker_fee = self._raw._market_taker_fee(m.name)  # noqa: SLF001

        async def _go():
            order = X.create_order_object(
                account=self._raw._stark_acc, market=m,  # noqa: SLF001
                starknet_domain=self._raw._cfg.signing.starknet_domain,  # noqa: SLF001
                order_type=X.OrderType.MARKET,
                time_in_force=X.TimeInForce.IOC,
                side=side, amount_of_synthetic=qty, price=px_dec,
                reduce_only=reduce_only, taker_fee=taker_fee,
                order_external_id=ext_id)
            r = await self._raw._client.orders.place_order(order)  # noqa: SLF001
            d = getattr(r, "data", None)
            oid = str(getattr(d, "id", "") or "") if d is not None else ""
            return {"oid": oid or None}
        return self._run(_go(), self._write_t)

    def _api_cancel(self, oid: Any) -> None:
        tail = str(oid).rsplit("-", 1)[-1]
        oid_arg: Any = int(tail) if tail.isdigit() else oid

        async def _go():
            return await self._raw._client.orders.cancel_order(oid_arg)  # noqa: SLF001
        self._run(_go(), self._write_t)

    def _api_update_leverage(self, market_name: str, leverage: int) -> None:
        async def _go():
            return await self._raw._client.account.update_leverage(  # noqa: SLF001
                market_name=market_name, leverage=Decimal(int(leverage)))
        self._run(_go(), self._write_t)

    # =====================================================================
    # shared plumbing above the SDK layer
    # =====================================================================

    def _dry_block(self, op: str, coin: str) -> None:
        """F2: under DRY_RUN=1 a write must never reach the transport."""
        if self._dry:
            raise WriteUnconfirmed(
                "[DRY] %s(%s) blocked at the exchange wrapper — nothing "
                "signed, nothing sent" % (op, coin),
                may_have_landed=False, venue=VENUE, op=op, coin=coin)

    def _read_op(self, op: str, coin: str, fn: Any) -> Any:
        """Verified read: fn() result, or RateLimited (bounded internal 429
        budget exhausted) / ReadUnknown. Never a masked neutral."""
        for attempt in range(1 + self._retry429):
            try:
                return fn()
            except ExchangeError:
                raise
            except Exception as e:
                _reraise_harness(e)
                if _exc_kind(e) == "rate":
                    if attempt < self._retry429:
                        time.sleep(self._retry_sleep * (attempt + 1))
                        continue
                    raise RateLimited(
                        "%s: 429 retry budget exhausted: %s"
                        % (op, str(e)[:200]),
                        venue=VENUE, op=op, coin=coin) from e
                raise ReadUnknown(
                    "%s failed: %s: %s" % (op, type(e).__name__, str(e)[:300]),
                    venue=VENUE, op=op, coin=coin) from e
        raise ReadUnknown("%s: unreachable" % op, venue=VENUE, op=op,
                          coin=coin)  # pragma: no cover

    def _meta_or_bug(self, coin: str, op: str) -> Any:
        """MarketModel for `coin`; unknown coin -> ValueError (caller bug),
        markets meta unavailable -> ReadUnknown (venue unknown)."""
        try:
            return self._raw._market(coin)  # noqa: SLF001
        except KeyError:
            if getattr(self._raw, "_markets_cache", None):
                raise ValueError(
                    "unknown coin %r on extended (caller bug)" % coin)
            raise ReadUnknown(
                "markets meta unavailable — cannot resolve %r" % coin,
                venue=VENUE, op=op, coin=coin)
        except Exception as e:
            _reraise_harness(e)
            raise ReadUnknown(
                "markets meta lookup failed for %r: %s" % (coin, e),
                venue=VENUE, op=op, coin=coin) from e

    def _meta_or_reject(self, coin: str, op: str) -> Any:
        """Write-side meta lookup: unknown coin is a definitive reject."""
        try:
            return self._meta_or_bug(coin, op)
        except ValueError as e:
            raise VenueRejected(str(e), reason="unknown_symbol", venue=VENUE,
                                op=op, coin=coin) from e

    def _slip(self) -> float:
        return float(getattr(getattr(self._raw, "settings", None),
                             "slippage", 0.01) or 0.01)

    # ------------------------------------------------------- positions truth

    def _fresh_positions(self) -> Dict[str, PositionInfo]:
        """One verified venue snapshot, NO cache serve — the flatness-proof
        primitive. Raises ReadUnknown/RateLimited. Per-item-corrupt law: one
        unparseable row poisons the WHOLE read."""
        rows = self._read_op("open_positions", "", self._api_get_positions)
        out: Dict[str, PositionInfo] = {}
        for row in rows:
            try:
                coin = _bare(row["market"])
                sz = float(row["size"])
                if sz == 0.0:
                    continue  # venue-confirmed flat leftover row
                signed = sz if "LONG" in str(row["side"]).upper() else -sz
                liq = row.get("liq")
                liq_f = None
                if liq is not None:
                    liq_f = float(liq)
                    if liq_f <= 0:
                        liq_f = None
                lev = row.get("leverage")
                mu = row.get("value")
                upnl = row.get("upnl")
                out[coin] = PositionInfo(
                    coin=coin,
                    size_signed=signed,
                    entry_px=float(row["open_price"]),
                    leverage=None if lev is None else float(lev),
                    margin_used_usd=None if mu is None else float(mu),
                    liquidation_px=liq_f,
                    unrealized_pnl=None if upnl is None else float(upnl),
                    raw=row)
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "unparseable position row %r: %s — aborting the whole "
                    "snapshot (never drop an item)" % (row, e),
                    venue=VENUE, op="open_positions") from e
        with self._lock:
            self._pos_cache = (out, time.time())
        return out

    # ------------------------------------------------------ fills readback

    def _find_fills(self, coin: str, is_buy: bool, since_ts: float,
                    want_ro: Optional[bool], oid: Optional[str]
                    ) -> Optional[Tuple[float, float, Optional[str]]]:
        """(size, vwap, fill_oid) attributable to our order, or None when the
        feed POSITIVELY shows nothing. Raises on an unreadable feed."""
        rows = self._api_get_trades()
        since_ms = int((since_ts - 2.0) * 1000)
        side = "buy" if is_buy else "sell"
        mine = []
        for r in rows:
            if r.get("coin") != coin or r.get("side") != side:
                continue
            if int(r.get("time_ms") or 0) < since_ms:
                continue
            ro = r.get("reduce_only")
            if want_ro is True and ro is False:
                continue
            if want_ro is False and ro is True:
                continue
            mine.append(r)
        if oid:
            sub = [r for r in mine if r.get("order_id") is not None
                   and _oid_eq(r["order_id"], oid)]
            if sub:
                mine = sub
        if not mine:
            return None
        tot = sum(float(r["sz"]) for r in mine)
        if tot <= 0:
            return None
        vwap = sum(float(r["px"]) * float(r["sz"]) for r in mine) / tot
        return tot, vwap, (str(mine[0].get("oid") or "") or None)

    def _confirm_fill(self, coin: str, is_buy: bool, since_ts: float,
                      oid: Optional[str], want_ro: bool
                      ) -> Optional[Tuple[float, float, Optional[str]]]:
        """Poll the fills feed for our fill (bounded). None = nothing landed
        within budget AND the feed stayed readable; raises WriteUnconfirmed
        when the feed itself could not be read (cannot prove either way)."""
        feed_err: Optional[Exception] = None
        for attempt in range(self._confirm_attempts):
            if attempt:
                time.sleep(self._confirm_sleep)
            try:
                hit = self._find_fills(coin, is_buy, since_ts, want_ro, oid)
            except Exception as e:
                _reraise_harness(e)
                feed_err = e
                continue
            feed_err = None
            if hit is not None:
                return hit
        if feed_err is not None:
            raise WriteUnconfirmed(
                "fill readback unreadable for %s — cannot prove either way: "
                "%s" % (coin, str(feed_err)[:200]),
                may_have_landed=True, venue=VENUE, op="fill_readback",
                coin=coin)
        return None

    @staticmethod
    def _parse_hl(resp: Any) -> Tuple[str, Any]:
        """Parse the raw adapter's HL-shaped response dict into
        ('resting', oid) | ('filled', payload) | ('error', message)."""
        try:
            sts = resp["response"]["data"]["statuses"]
        except Exception:
            return "error", "unparseable adapter response: %r" % (resp,)
        if not sts:
            return "error", "empty statuses"
        st = sts[0]
        if isinstance(st, dict):
            if "error" in st:
                return "error", str(st["error"])
            if "resting" in st:
                return "resting", str(st["resting"].get("oid") or "")
            if "filled" in st:
                return "filled", st["filled"]
        return "error", "unknown status shape: %r" % (st,)

    # =====================================================================
    # writes
    # =====================================================================

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        m = self._meta_or_reject(coin, "market_open")
        self._dry_block("market_open", coin)
        if intended_px and intended_px > 0 and not allow_marketable:
            self._guard_marketability(coin, m, is_buy, float(intended_px))
        # pre-send verified reads — any failure here means NOTHING was sent
        mark = self.mark_price(coin)
        slip = self._slip()
        agg = mark * (1 + slip) if is_buy else mark * (1 - slip)
        qty = self._raw._round_qty(m, float(sz))  # noqa: SLF001
        if float(qty) <= 0:
            raise VenueRejected(
                "market_open(%s): size %.10g rounds to zero at the venue step"
                % (coin, sz), reason="size_rounds_to_zero", venue=VENUE,
                op="market_open", coin=coin)
        ext_id = "fcmo-%s-%d" % (m.name, time.time_ns())
        t0 = time.time()
        acked_oid: Optional[str] = None
        may_have_landed: Optional[bool] = None
        try:
            ack = self._api_place_market(m, is_buy, qty, agg, False, ext_id)
            acked_oid = ack.get("oid")
            may_have_landed = True
        except Exception as e:
            _reraise_harness(e)
            kind = _exc_kind(e)
            if kind == "reject":
                raise VenueRejected(
                    "market_open(%s) rejected by venue: %s"
                    % (coin, str(e)[:300]), reason=str(e)[:300], venue=VENUE,
                    op="market_open", coin=coin) from e
            may_have_landed = None  # ambiguous — readback decides
            log.warning("market_open(%s) send ambiguous (%s: %s) — "
                        "readback decides", coin, type(e).__name__,
                        str(e)[:200])
        self.invalidate_positions_cache()
        hit = self._confirm_fill(coin, is_buy, t0, acked_oid, want_ro=False)
        if hit is None:
            raise WriteUnconfirmed(
                "market_open(%s): %s and fills readback shows nothing landed"
                % (coin, "acked" if acked_oid else "send ambiguous"),
                may_have_landed=may_have_landed, venue=VENUE,
                op="market_open", coin=coin)
        size, vwap, fill_oid = hit
        return FillResult(coin=coin, is_buy=is_buy, avg_px=vwap, size=size,
                          requested_size=float(qty),
                          oid=acked_oid or fill_oid)

    def _guard_marketability(self, coin: str, m: Any, is_buy: bool,
                             intended_px: float) -> None:
        """allow_marketable=False law: a level already through the book must
        NOT market-fill (manual-BTC-short class). Fail CLOSED: unverifiable
        book AND mark -> ReadUnknown (nothing sent)."""
        bid = ask = 0.0
        try:
            bid, ask = self._api_get_book(m.name)
        except Exception as e:
            _reraise_harness(e)
            log.warning("marketability book read failed for %s: %s", coin, e)
        if bid > 0 or ask > 0:
            marketable = (is_buy and ask > 0 and intended_px >= ask) or \
                         ((not is_buy) and bid > 0 and intended_px <= bid)
        else:
            mark = self.mark_price(coin)  # ReadUnknown propagates: fail closed
            marketable = (is_buy and mark <= intended_px) or \
                         ((not is_buy) and mark >= intended_px)
        if marketable:
            raise VenueRejected(
                "%s entry for %s at %.10g is immediately marketable "
                "(bid=%.10g ask=%.10g) — use a trigger entry"
                % ("BUY" if is_buy else "SELL", coin, intended_px, bid, ask),
                reason="immediately_marketable", venue=VENUE,
                op="market_open", coin=coin)

    def ensure_flat(self, coin: str) -> FlatResult:
        self.invalidate_positions_cache()
        pos_map = self._fresh_positions()  # initial read fail -> nothing sent
        p = pos_map.get(coin)
        if p is None:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        self._dry_block("ensure_flat", coin)
        m = self._meta_or_reject(coin, "ensure_flat")
        mark = self.mark_price(coin)  # pre-send read; failure -> nothing sent
        slip = self._slip()
        total = abs(p.size_signed)
        remaining = total
        is_buy = p.size_signed < 0
        sent_any = False
        t0 = time.time()
        attempt = 0
        for attempt in range(1, self._close_attempts + 1):
            qty = self._raw._round_qty(m, remaining)  # noqa: SLF001
            if float(qty) > 0:
                agg = mark * (1 + slip) if is_buy else mark * (1 - slip)
                ext_id = "fcef-%s-%d" % (m.name, time.time_ns())
                try:
                    self._api_place_market(m, is_buy, qty, agg, True, ext_id)
                    sent_any = True
                except Exception as e:
                    _reraise_harness(e)
                    if _exc_kind(e) == "reject":
                        # e.g. reduce-only-would-increase because it already
                        # closed — the position readback below decides.
                        log.warning("ensure_flat(%s) close rejected (%s) — "
                                    "readback decides", coin, str(e)[:200])
                    else:
                        sent_any = True  # may have landed — readback decides
                        log.warning("ensure_flat(%s) close send ambiguous: %s",
                                    coin, str(e)[:200])
            # fresh position readback — the ONLY source of "flat" truth
            try:
                self.invalidate_positions_cache()
                now_map = self._fresh_positions()
            except ExchangeError as e:
                raise WriteUnconfirmed(
                    "ensure_flat(%s): close sent but the confirming position "
                    "readback failed — flatness UNPROVEN: %s" % (coin, e),
                    may_have_landed=(True if sent_any else None), venue=VENUE,
                    op="ensure_flat", coin=coin) from e
            if coin not in now_map:
                return FlatResult(
                    coin=coin, already_flat=False, closed_size=total,
                    exit_avg_px=self._exit_px_best_effort(coin, is_buy, t0),
                    attempts=attempt)
            remaining = abs(now_map[coin].size_signed)
            if attempt < self._close_attempts:
                time.sleep(self._confirm_sleep)
        raise WriteUnconfirmed(
            "ensure_flat(%s): %d close attempts exhausted, position still "
            "open (residual %.10g of %.10g)" % (coin, attempt, remaining,
                                                total),
            may_have_landed=sent_any, venue=VENUE, op="ensure_flat", coin=coin)

    def _exit_px_best_effort(self, coin: str, is_buy: bool,
                             since_ts: float) -> Optional[float]:
        """Real exit fill VWAP from the fills feed; None when unattributable —
        NEVER a reference price (wick_sl ×1330 class)."""
        try:
            hit = self._find_fills(coin, is_buy, since_ts, want_ro=True,
                                   oid=None)
        except Exception as e:
            _reraise_harness(e)
            return None
        return None if hit is None else hit[1]

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        m = self._meta_or_reject(coin, "trigger_sl")
        if not trigger_px or trigger_px <= 0:
            raise VenueRejected(
                "trigger_sl(%s): non-positive trigger %r" % (coin, trigger_px),
                reason="bad_trigger_px", venue=VENUE, op="trigger_sl",
                coin=coin)
        self._dry_block("trigger_sl", coin)
        # pre-placement live-trigger snapshot: lets an oid-less ambiguous ack
        # still be resolved against venue truth (N9 duplicate-SL class killer)
        pre_oids: Optional[set] = None
        try:
            pre_oids = {r["oid"] for r in self._list_tpsl_rows(m)}
        except Exception as e:
            _reraise_harness(e)
            pre_oids = None
        # battle-tested TPSL POSITION construction lives in the raw adapter
        try:
            resp = self._raw.trigger_sl(coin, is_buy, sz, trigger_px)
        except Exception as e:  # raw wraps everything, but stay defensive
            _reraise_harness(e)
            resp = {"response": {"data": {"statuses": [{"error": str(e)}]}}}
        status, payload = self._parse_hl(resp)
        oid: Optional[str] = None
        if status == "resting":
            oid = payload or None
        elif status == "filled":
            oid = str((payload or {}).get("oid") or "") or None
        else:
            err = str(payload)
            _raise_if_harness_msg(err)
            if _exc_kind(Exception(err)) == "reject":
                raise VenueRejected(
                    "trigger_sl(%s) rejected by venue: %s" % (coin, err[:300]),
                    reason=err[:300], venue=VENUE, op="trigger_sl", coin=coin)
            log.warning("trigger_sl(%s) send ambiguous (%s) — live-list "
                        "confirm decides", coin, err[:200])
        # MANDATORY live-list confirm (nado _confirm_trigger_live law)
        close_side = "buy" if is_buy else "sell"
        for attempt in range(self._confirm_attempts):
            if attempt:
                time.sleep(self._confirm_sleep)
            try:
                rows = self._list_tpsl_rows(m)
            except Exception as e:
                _reraise_harness(e)
                # a failed CONFIRM read is NOT absence (N9): may_have_landed
                raise WriteUnconfirmed(
                    "trigger_sl(%s): placed (oid=%s) but the live-trigger "
                    "confirm read failed — do NOT blind-retry: %s"
                    % (coin, oid, str(e)[:200]),
                    may_have_landed=True, venue=VENUE, op="trigger_sl",
                    coin=coin) from e
            hit: Optional[Dict[str, Any]] = None
            if oid:
                for r in rows:
                    if _oid_eq(r["oid"], oid):
                        hit = r
                        break
            elif pre_oids is not None:
                new = [r for r in rows if r["oid"] not in pre_oids
                       and r.get("side") == close_side]
                if len(new) == 1:
                    hit = new[0]
                elif len(new) > 1:
                    raise WriteUnconfirmed(
                        "trigger_sl(%s): ambiguous ack and %d new live "
                        "triggers — refusing to guess ownership"
                        % (coin, len(new)),
                        may_have_landed=True, venue=VENUE, op="trigger_sl",
                        coin=coin)
            if hit is not None:
                try:
                    tpx = float(hit.get("trigger_px") or 0)
                except (TypeError, ValueError):
                    tpx = 0.0
                try:
                    szv = float(hit.get("qty") or 0)
                except (TypeError, ValueError):
                    szv = 0.0
                return SLOrderInfo(
                    coin=coin, oid=str(hit["oid"]),
                    trigger_px=tpx if tpx > 0 else float(trigger_px),
                    size=szv, is_buy_to_close=is_buy,
                    position_wide=True)  # extended TPSL POSITION semantics
        raise WriteUnconfirmed(
            "trigger_sl(%s): acked (oid=%s) but NOT in the live trigger list "
            "after %d polls — naked-success class, refusing to claim placed"
            % (coin, oid, self._confirm_attempts),
            may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)

    def _list_tpsl_rows(self, m: Any) -> List[Dict[str, Any]]:
        rows = self._api_get_open_orders(m.name, tpsl_only=True)
        return [r for r in rows
                if _bare(r.get("market", "")) == _bare(m.name)
                and r.get("is_trigger")]

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        m = self._meta_or_reject(coin, "cancel_sl_order")
        self._dry_block("cancel_sl_order", coin)
        cancel_err: Optional[Exception] = None
        try:
            self._api_cancel(oid)
        except Exception as e:
            _reraise_harness(e)
            cancel_err = e  # not-found == goal state; readback decides all
        # confirmed-GONE readback against the live order list (all types —
        # this primitive also cancels TP limits on some paths)
        for attempt in range(self._confirm_attempts):
            if attempt:
                time.sleep(self._confirm_sleep)
            try:
                rows = self._api_get_open_orders(m.name, tpsl_only=False)
            except Exception as e:
                _reraise_harness(e)
                raise WriteUnconfirmed(
                    "cancel_sl_order(%s/%s): cannot prove order gone — live "
                    "order listing failed: %s" % (coin, oid, str(e)[:200]),
                    may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                    coin=coin) from e
            if not any(_oid_eq(r.get("oid"), oid) for r in rows):
                if cancel_err is not None:
                    s = str(cancel_err).lower()
                    if not any(mk in s for mk in _NOT_FOUND_MARKERS) and \
                            not _RE_NOT_FOUND_CODE.search(s):
                        log.warning(
                            "cancel_sl_order(%s/%s): cancel call errored (%s) "
                            "but the order is confirmed GONE — goal state "
                            "holds", coin, oid, str(cancel_err)[:150])
                return None
        raise WriteUnconfirmed(
            "cancel_sl_order(%s/%s): cancel %s but order STILL LIVE after %d "
            "polls" % (coin, oid,
                       "errored (%s)" % str(cancel_err)[:100] if cancel_err
                       else "acked", self._confirm_attempts),
            may_have_landed=True, venue=VENUE, op="cancel_sl_order", coin=coin)

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        m = self._meta_or_reject(coin, "limit_reduce_only")
        if not px or px <= 0:
            raise VenueRejected(
                "limit_reduce_only(%s): non-positive px %r" % (coin, px),
                reason="bad_limit_px", venue=VENUE, op="limit_reduce_only",
                coin=coin)
        self._dry_block("limit_reduce_only", coin)
        t0 = time.time()
        try:
            resp = self._raw.limit_reduce_only(coin, is_buy, sz, px)
        except Exception as e:
            _reraise_harness(e)
            resp = {"response": {"data": {"statuses": [{"error": str(e)}]}}}
        status, payload = self._parse_hl(resp)
        oid: Optional[str] = None
        if status == "resting":
            oid = payload or None
        elif status == "filled":
            oid = str((payload or {}).get("oid") or "") or None
        else:
            err = str(payload)
            _raise_if_harness_msg(err)
            if _exc_kind(Exception(err)) == "reject":
                raise VenueRejected(
                    "limit_reduce_only(%s) rejected by venue: %s"
                    % (coin, err[:300]), reason=err[:300], venue=VENUE,
                    op="limit_reduce_only", coin=coin)
            log.warning("limit_reduce_only(%s) send ambiguous (%s) — "
                        "readback decides", coin, err[:200])
        side = "buy" if is_buy else "sell"
        for attempt in range(self._confirm_attempts):
            if attempt:
                time.sleep(self._confirm_sleep)
            # RESTING? — live order list is authoritative
            try:
                rows = self._api_get_open_orders(m.name, tpsl_only=False)
            except Exception as e:
                _reraise_harness(e)
                raise WriteUnconfirmed(
                    "limit_reduce_only(%s): acked (oid=%s) but the resting-"
                    "confirm listing failed: %s" % (coin, oid, str(e)[:200]),
                    may_have_landed=True, venue=VENUE,
                    op="limit_reduce_only", coin=coin) from e
            if oid:
                for r in rows:
                    if _oid_eq(r.get("oid"), oid):
                        try:
                            szv = float(r.get("qty") or 0) or float(sz)
                            lpx = float(r.get("price") or 0) or float(px)
                        except (TypeError, ValueError):
                            szv, lpx = float(sz), float(px)
                        return OpenOrderInfo(
                            coin=coin, oid=str(r["oid"]), side=r["side"],
                            size=szv, limit_px=lpx,
                            trigger_px=None, reduce_only=True,
                            is_trigger=False, raw=r)
            # already FILLED? — fills feed
            try:
                hit = self._find_fills(coin, is_buy, t0, want_ro=True,
                                       oid=oid)
            except Exception as e:
                _reraise_harness(e)
                raise WriteUnconfirmed(
                    "limit_reduce_only(%s): fills readback failed while "
                    "confirming: %s" % (coin, str(e)[:200]),
                    may_have_landed=True, venue=VENUE,
                    op="limit_reduce_only", coin=coin) from e
            if hit is not None:
                size_f, vwap, fill_oid = hit
                return OpenOrderInfo(
                    coin=coin, oid=str(oid or fill_oid or "filled"),
                    side=side, size=size_f, limit_px=float(px),
                    reduce_only=True, is_trigger=False,
                    raw={"filled": {"size": size_f, "avg_px": vwap,
                                    "oid": fill_oid}})
        raise WriteUnconfirmed(
            "limit_reduce_only(%s): acked (oid=%s) but neither resting nor "
            "filled on readback" % (coin, oid),
            may_have_landed=True, venue=VENUE, op="limit_reduce_only",
            coin=coin)

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        m = self._meta_or_reject(coin, "update_leverage")
        if self._dry:
            # benign success under DRY (HL _dry_guard caller-proceeds
            # semantics): nothing is signed or sent
            log.info("[DRY] update_leverage(%s, %d) blocked at the wrapper",
                     coin, leverage)
            return None
        if not is_cross:
            log.warning("extended is cross-only (SDK 1.4.x) — setting cross "
                        "leverage for %s", coin)
        try:
            self._api_update_leverage(m.name, int(leverage))
            return None
        except Exception as e:
            _reraise_harness(e)
            kind = _exc_kind(e)
            if kind == "reject":
                raise VenueRejected(
                    "update_leverage(%s, %dx) rejected: %s"
                    % (coin, leverage, str(e)[:300]), reason=str(e)[:300],
                    venue=VENUE, op="update_leverage", coin=coin) from e
            raise WriteUnconfirmed(
                "update_leverage(%s, %dx): confirmation unreadable: %s"
                % (coin, leverage, str(e)[:200]),
                may_have_landed=(False if kind == "rate" else None),
                venue=VENUE, op="update_leverage", coin=coin) from e

    # =====================================================================
    # reads
    # =====================================================================

    def open_positions(self) -> Mapping[str, PositionInfo]:
        now = time.time()
        with self._lock:
            cached = self._pos_cache
        if cached is not None and now - cached[1] <= self._pos_ttl:
            return cached[0]
        try:
            return self._fresh_positions()
        except (ReadUnknown, RateLimited) as e:
            if isinstance(e, StaleData):  # pragma: no cover — not produced here
                raise
            with self._lock:
                cached = self._pos_cache
            if cached is not None:
                age = now - cached[1]
                if age <= self._pos_stale_bound:
                    # census-approved extended ceiling: bias-to-present,
                    # bounded to one loop interval, age surfaced in the log
                    log.warning(
                        "open_positions failed (%s) — serving %.1fs-old "
                        "snapshot (bias-to-present, bound %.0fs)",
                        str(e)[:150], age, self._pos_stale_bound)
                    return cached[0]
                raise StaleData(
                    "open_positions only available %.1fs old (bound %.0fs)"
                    % (age, self._pos_stale_bound), age_sec=age,
                    tolerance_sec=self._pos_stale_bound, venue=VENUE,
                    op="open_positions") from e
            raise

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        rows = self._read_op("open_orders", "",
                             lambda: self._api_get_open_orders(None, False))
        return self._rows_to_order_infos(rows, "open_orders")

    def _rows_to_order_infos(self, rows: List[Dict[str, Any]],
                             op: str) -> List[OpenOrderInfo]:
        out: List[OpenOrderInfo] = []
        for r in rows:
            try:
                lpx = r.get("price")
                lpx_f = None if lpx in (None, "") else float(lpx)
                if lpx_f is not None and lpx_f <= 0:
                    lpx_f = None
                tpx = r.get("trigger_px")
                tpx_f = None if tpx in (None, "") else float(tpx)
                if tpx_f is not None and tpx_f <= 0:
                    tpx_f = None
                out.append(OpenOrderInfo(
                    coin=_bare(r["market"]), oid=str(r["oid"]),
                    side=str(r["side"]), size=float(r["qty"]),
                    limit_px=lpx_f, trigger_px=tpx_f,
                    reduce_only=bool(r.get("reduce_only")),
                    is_trigger=bool(r.get("is_trigger")), raw=r))
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "unparseable order row %r: %s — aborting the whole "
                    "listing (never drop an item)" % (r, e),
                    venue=VENUE, op=op) from e
        return out

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        m = self._meta_or_bug(coin, "list_open_sl_orders")
        rows = self._read_op(
            "list_open_sl_orders", coin,
            lambda: self._api_get_open_orders(m.name, tpsl_only=True))
        try:
            return [str(r["oid"]) for r in rows
                    if _bare(r.get("market", "")) == _bare(m.name)]
        except (KeyError, TypeError) as e:
            raise ReadUnknown("unparseable trigger row: %s" % e, venue=VENUE,
                              op="list_open_sl_orders", coin=coin) from e

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        rows = self._read_op(
            "list_reduce_only_triggers", "",
            lambda: self._api_get_open_orders(None, tpsl_only=True))
        infos = self._rows_to_order_infos(rows, "list_reduce_only_triggers")
        return [o for o in infos if o.is_trigger and o.reduce_only]

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        m = self._meta_or_bug(coin, "mark_price")
        now = time.time()
        with self._lock:
            cached = self._mark_cache.get(coin)
        if cached is not None and now - cached[1] <= max_age_sec:
            return cached[0]  # cache INSIDE tolerance: legal + encouraged
        try:
            px = self._read_op("mark_price", coin,
                               lambda: self._api_get_stats_mark(m.name))
        except (ReadUnknown, RateLimited) as e:
            if cached is not None:
                age = now - cached[1]
                raise StaleData(
                    "mark for %s only available %.3fs old (tolerance %.3fs)"
                    % (coin, age, max_age_sec), age_sec=age,
                    tolerance_sec=max_age_sec, venue=VENUE, op="mark_price",
                    coin=coin) from e
            raise
        if px is None or not math.isfinite(px) or px <= 0.0:
            raise ReadUnknown(
                "mark for %s is %r — insane/absent (never 0.0)" % (coin, px),
                venue=VENUE, op="mark_price", coin=coin)
        with self._lock:
            self._mark_cache[coin] = (px, time.time())
        return px

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> Any:
        if interval in _RESAMPLE_FROM:
            base_tf, factor = _RESAMPLE_FROM[interval]
            base = self.candles(coin, base_tf, limit=(limit + 2) * factor,
                                max_stale_bars=max_stale_bars * factor)
            return self._resample_checked(base, coin, interval,
                                          max_stale_bars)
        if interval not in _INTERVAL_ISO or interval not in _TF_MS:
            raise ValueError(
                "unsupported interval %r on extended (caller bug)" % interval)
        m = self._meta_or_bug(coin, "candles")  # unknown coin -> ValueError
        key = (coin, interval)
        now = time.time()
        with self._lock:
            cached = self._candles_cache.get(key)
        if cached is not None and now - cached[1] <= self._candle_ttl:
            try:
                df = self._staleness_gate(cached[0], coin, interval,
                                          max_stale_bars)
                return df.copy()
            except (StaleData, ReadUnknown):
                pass  # bar boundary crossed since the cached fetch — refetch
        try:
            rows = self._read_op(
                "candles", coin,
                lambda: self._api_get_candles(m.name,
                                              _INTERVAL_ISO[interval], limit))
        except (ReadUnknown, RateLimited):
            if cached is not None:
                # serve the cache ONLY if it still passes the freshness gate
                # (fetch is down here — a stale cache raises StaleData)
                df = self._staleness_gate(cached[0], coin, interval,
                                          max_stale_bars)
                return df.copy()
            raise
        if not rows:
            raise ReadUnknown(
                "candles(%s,%s): venue returned no bars" % (coin, interval),
                venue=VENUE, op="candles", coin=coin)
        df = self._rows_to_df(rows, coin, interval)
        df = self._staleness_gate(df, coin, interval, max_stale_bars)
        with self._lock:
            self._candles_cache[key] = (df, time.time())
        return df.copy()

    def _rows_to_df(self, rows: List[Dict[str, float]], coin: str,
                    interval: str) -> Any:
        import pandas as pd  # noqa: PLC0415 — lazy; module stays stdlib at import
        try:
            recs = [{"t": int(r["t"]), "Open": float(r["o"]),
                     "High": float(r["h"]), "Low": float(r["l"]),
                     "Close": float(r["c"]), "Volume": float(r["v"])}
                    for r in rows]
        except (KeyError, TypeError, ValueError) as e:
            raise ReadUnknown(
                "candles(%s,%s): unparseable bar row: %s" % (coin, interval,
                                                             e),
                venue=VENUE, op="candles", coin=coin) from e
        recs.sort(key=lambda r: r["t"])
        # drop the in-progress bar: contract serves CLOSED bars only
        bar_ms = _TF_MS[interval]
        boundary = (int(time.time() * 1000) // bar_ms) * bar_ms
        recs = [r for r in recs if r["t"] < boundary]
        if not recs:
            raise ReadUnknown(
                "candles(%s,%s): no CLOSED bars in payload" % (coin, interval),
                venue=VENUE, op="candles", coin=coin)
        df = pd.DataFrame(recs)
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        return df[["time", "Open", "High", "Low", "Close", "Volume"]]

    def _staleness_gate(self, df: Any, coin: str, interval: str,
                        max_stale_bars: float) -> Any:
        import pandas as pd  # noqa: PLC0415
        bar_ms = _TF_MS[interval]
        boundary = (int(time.time() * 1000) // bar_ms) * bar_ms
        try:
            last_open = int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        except Exception as e:
            raise ReadUnknown(
                "candles(%s,%s): cannot date the last bar: %s"
                % (coin, interval, e), venue=VENUE, op="candles", coin=coin)
        lag_bars = (boundary - (last_open + bar_ms)) / bar_ms
        if lag_bars > max_stale_bars:
            raise StaleData(
                "candles(%s,%s): last closed bar lags %.2f bars (tolerance "
                "%.2f)" % (coin, interval, lag_bars, max_stale_bars),
                age_sec=lag_bars * bar_ms / 1000.0,
                tolerance_sec=max_stale_bars * bar_ms / 1000.0,
                venue=VENUE, op="candles", coin=coin)
        return df

    def _resample_checked(self, base_df: Any, coin: str, interval: str,
                          max_stale_bars: float) -> Any:
        """Aggregate a native-TF df to `interval` (epoch-aligned, label/closed
        left — mirrors the raw adapter), then drop the in-progress aggregate
        bar and re-run the staleness gate at the TARGET interval."""
        import pandas as pd  # noqa: PLC0415
        bar_ms = _TF_MS[interval]
        rule = "%ds" % (bar_ms // 1000)
        df = (base_df.set_index("time")
              .resample(rule, label="left", closed="left", origin="epoch")
              .agg({"Open": "first", "High": "max", "Low": "min",
                    "Close": "last", "Volume": "sum"})
              .dropna(subset=["Open", "Close"])
              .reset_index())
        boundary = (int(time.time() * 1000) // bar_ms) * bar_ms
        opens_ms = (df["time"].astype("int64") // 10**6)
        df = df[opens_ms < boundary].reset_index(drop=True)
        if df.empty:
            raise ReadUnknown(
                "candles(%s,%s): no closed bars after resample"
                % (coin, interval), venue=VENUE, op="candles", coin=coin)
        return self._staleness_gate(df, coin, interval, max_stale_bars)

    def equity_with_upnl(self) -> float:
        bal = self._read_op("equity_with_upnl", "", self._api_get_balance)
        v = float(bal["balance"]) + float(bal["upnl"])
        if not math.isfinite(v):
            raise ReadUnknown("equity_with_upnl non-finite: %r" % v,
                              venue=VENUE, op="equity_with_upnl")
        return v

    def account_value(self) -> float:
        # EXACT deployed sizing semantics: get_balance().equity (E1 fixed:
        # verified-or-raise, never 0.0-on-failure)
        bal = self._read_op("account_value", "", self._api_get_balance)
        v = float(bal["equity"])
        if not math.isfinite(v):
            raise ReadUnknown("account_value non-finite: %r" % v,
                              venue=VENUE, op="account_value")
        return v

    def margin_used_usd(self) -> float:
        bal = self._read_op("margin_used_usd", "", self._api_get_balance)
        v = float(bal["margin"])
        if not math.isfinite(v):
            raise ReadUnknown("margin_used_usd non-finite: %r" % v,
                              venue=VENUE, op="margin_used_usd")
        return v

    def position_liquidation(self, coin: str) -> Optional[float]:
        pos_map = self.open_positions()  # typed raise propagates (E9)
        p = pos_map.get(coin)
        if p is None:
            return None  # POSITIVELY no position
        return p.liquidation_px  # None == venue exposes no liq px (cross)

    def user_fills(self, max_age_sec: float = 60.0
                   ) -> Sequence[Mapping[str, Any]]:
        rows = self._read_op("user_fills", "", self._api_get_trades)
        # raw-adapter-compatible shape (trader/journal exit attribution)
        out: List[Dict[str, Any]] = []
        for f in rows:
            out.append({
                "coin": f["coin"], "market": f["market"],
                "side": "B" if f["side"] == "buy" else "A",
                "px": str(f["px"]), "sz": str(f["sz"]),
                "fee": str(f["fee"]), "time": int(f["time_ms"]),
                "oid": f["oid"], "order_id": f.get("order_id"),
                "trade_type": f.get("trade_type"),
            })
        out.sort(key=lambda r: r["time"], reverse=True)  # newest first
        return out

    # ---------------------------------------------------- CONTRACT-EXEMPT
    # Non-contract caller surface (F4 / mapping §6): VERBATIM delegation to
    # the raw adapter, semantics UNCHANGED (dict|None returns and the raw
    # trigger_entry/assert_entry_marketability extended-only extensions ride
    # P3 as-is) — so the P3 repoint cannot AttributeError. Typed-error laws
    # do NOT apply here.
    #   callers grepped 2026-07-03: trader.py (asset, round_price,
    #   add_isolated_margin), scanner.py (invalidate_candles_cache); the
    #   rest are raw-adapter surface kept for parity with mapping §6.

    CONTRACT_EXEMPT_PASSTHROUGH: Tuple[str, ...] = (
        "asset", "round_price", "slip_per_side", "funding_rate", "spot_usdc",
        "compute_realized_pnl", "trigger_tp", "trigger_entry",
        "assert_entry_marketability", "add_isolated_margin",
        "invalidate_candles_cache",
    )

    def asset(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.asset."""
        return self._raw.asset(*a, **kw)

    def round_price(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.round_price."""
        return self._raw.round_price(*a, **kw)

    def slip_per_side(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.slip_per_side."""
        return self._raw.slip_per_side(*a, **kw)

    def funding_rate(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.funding_rate."""
        return self._raw.funding_rate(*a, **kw)

    def spot_usdc(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.spot_usdc."""
        return self._raw.spot_usdc(*a, **kw)

    def compute_realized_pnl(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.compute_realized_pnl."""
        return self._raw.compute_realized_pnl(*a, **kw)

    def trigger_tp(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.trigger_tp."""
        return self._raw.trigger_tp(*a, **kw)

    def trigger_entry(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.trigger_entry
        (extended-only entry extension)."""
        return self._raw.trigger_entry(*a, **kw)

    def assert_entry_marketability(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation →
        raw.assert_entry_marketability (extended-only)."""
        return self._raw.assert_entry_marketability(*a, **kw)

    def add_isolated_margin(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.add_isolated_margin."""
        return self._raw.add_isolated_margin(*a, **kw)

    def invalidate_candles_cache(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.invalidate_candles_cache."""
        return self._raw.invalidate_candles_cache(*a, **kw)

    @property
    def raw(self) -> Any:
        """The underlying bot.exchange_extended.ExtendedClient (forensics
        only)."""
        return self._raw

    # ------------------------------------------------------------ cache ctl

    def invalidate_positions_cache(self) -> None:
        with self._lock:
            self._pos_cache = None
        try:
            self._raw.invalidate_positions_cache()
        except Exception:  # pragma: no cover — defensive
            pass


# ===========================================================================
# factories
# ===========================================================================

def build_client(raw: Any) -> ExtendedExchangeClient:
    """Wrap an already-constructed bot.exchange_extended.ExtendedClient
    (conformance-harness entry point; no I/O)."""
    return ExtendedExchangeClient(raw)


def get_client() -> ExtendedExchangeClient:
    """Production factory: build the raw adapter from the bot env, wrap it."""
    from bot.config import Settings  # noqa: PLC0415 — lazy bot import
    from bot.exchange_extended import ExtendedClient  # noqa: PLC0415
    return build_client(ExtendedClient(Settings.from_env()))
