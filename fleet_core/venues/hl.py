"""fleet_core.venues.hl — THE conforming ExchangeClient for Hyperliquid.

Wraps the existing raw adapter (bot.exchange_hl.HLClient) WITHOUT modifying
it: every read is verified-or-raise, every write is readback-confirmed, and
the raw adapter's masked-neutral conventions (0.0 mark, empty df, None-liq,
[] fills, raw SDK dicts) are translated into the typed contract of
fleet_core.exchange_api at this layer. The raw adapter keeps doing transport,
signing, rounding, 429 backoff, WS candle feed and caching underneath.

CENSUS CLOSURES (proofs/p2_violation_census.md §1, venue=hl):

  H1  mark_price          — own verified allMids read (main + per-HIP-3-dex)
                            with a per-coin (px, ts) cache: value > 0 within
                            max_age_sec, else StaleData(age)/ReadUnknown.
                            Missing symbol on a SUCCESSFUL fetch = ValueError
                            (caller bug), never 0.0. Raw's 0.0-mask and
                            unbounded-age mids fallback are bypassed entirely.
  H2  candles             — raw.candles() is kept for its WS fast-path, bar
                            cache and 429 throttle, but its silent empty-df
                            and negative-fail-cache returns become
                            ReadUnknown (raw.candles_in_fail_cache tells a
                            fetch failure apart from a genuinely-empty
                            response — both raise, an empty df is never
                            data); last-closed-bar freshness beyond
                            max_stale_bars raises StaleData. Unknown coin /
                            unsupported interval raise ValueError BEFORE any
                            transport.
  H3  position_liquidation— derived from THIS class's verified positions
                            snapshot: a failed dex/user_state read raises
                            ReadUnknown; None is returned ONLY on a
                            positively-confirmed no-position / no-liq-price.
  H4  user_fills          — own verified userFills read: non-list payload or
                            fetch failure raises ReadUnknown; [] only when
                            the venue positively answered an empty list.
                            Raw's any-age fills cache is not consulted.
  H5  market_open         — SDK response statuses are parsed HERE: statuses
                            error -> VenueRejected(reason); filled ->
                            FillResult from the venue's own fill fields;
                            ack-without-fill / lost response -> fills-feed
                            readback attribution (confirm-via-readback,
                            the trader _confirm_fill_via_positions pattern
                            moved below the API line) -> FillResult with the
                            REAL landed size (partials visible) or
                            WriteUnconfirmed(may_have_landed=...).
  H6  market_close        — ABSENT from this surface. ensure_flat() is the
                            only close primitive: reduce-only close via
                            raw.market_close + invalidate + position-readback
                            loop; flat proven -> FlatResult (exit px from the
                            venue fill fields / fills feed, never an SL
                            reference); readback failed or residual after
                            bounded attempts -> WriteUnconfirmed.
  H7  trigger_sl          — placement is confirmed LIVE via an orderStatus
                            readback of the acked oid (nado
                            _confirm_trigger_live pattern): acked-but-not-
                            live raises WriteUnconfirmed (naked-success
                            class); definitive statuses error (min-size,
                            reduce-only violation) -> VenueRejected(reason).
  H8  cancel_sl_order     — cancel + confirmed-GONE readback against the
                            live order listing; venue not-found answer maps
                            to idempotent success; acked-but-still-live or
                            unreadable listing -> WriteUnconfirmed.
  H9  update_leverage     — SDK called directly (bypassing raw's None-on-
                            failure mask): {"status":"err"} ->
                            VenueRejected(reason=venue words); transport
                            ambiguity -> WriteUnconfirmed.
  H10 limit_reduce_only   — resting confirmed against the live order
                            listing; immediate fill returned with the venue
                            fill payload in .raw; acked-but-neither-resting-
                            nor-filled -> WriteUnconfirmed.
  H11 equity_with_upnl    — implemented as a strict verified read of the
                            deployable-equity components (spot USDC + perp
                            accountValue incl uPnL, main + every HIP-3 dex —
                            exchange_hl.py:752 semantics). On HL's unified
                            account this is BY DEFINITION the same number as
                            account_value(): the contract's documented alias
                            (census H11, mapping §1). account_value() keeps
                            the deployed sizing basis exactly (P2 changes no
                            risk numbers); both fail CLOSED (ReadUnknown) —
                            never the raw adapter's silent-0.0-on-unparseable
                            path (its `or 0` parses mask a malformed payload
                            into a zero equity).

Reads already conforming in the raw adapter (open_positions:806,
margin_used_usd:893, list_open_sl_orders:1531, list_reduce_only_triggers:1484)
are still re-implemented here as strict wire reads because the raw paths
consume the SDK's parse quirk (a non-JSON 200 becomes {"error": ...} which
raw code reads as an EMPTY payload — a residual mask the census fault matrix
exposes); the retry/fail-loud semantics are preserved.

margin_used_usd note: read from marginSummary.totalMarginUsed of the main
clearinghouse + every HIP-3 dex. On HL this equals the census-canon
per-position marginUsed sum (marginSummary spans cross AND isolated positions
of its clearinghouse) while staying meaningful on a flat account.

TRANSPORT: all wrapper-level reads go through the raw adapter's own SDK
requests.Session (timeout-guarded by exchange_hl._install_session_timeout)
with an explicit (connect, read) timeout per call — timeout law L6. Writes go
through the raw adapter's signed SDK methods unchanged. 429s carry a bounded
internal retry budget (RateLimited when spent).

OFFLINE-CONSTRUCTIBLE: this module imports ONLY the stdlib +
fleet_core.exchange_api at import time; bot.* / the hyperliquid SDK are
touched lazily inside build_client()/get_client() — the conformance harness
builds the raw adapter under its transport gate and hands it in.
"""

from __future__ import annotations

import importlib
import logging
import os
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

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

VENUE = "hl"

__all__ = ["HLExchangeClient", "build_client", "get_client"]


def _f(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, "") or default)
    except (TypeError, ValueError):
        return default


#: harness-integrity fingerprints — an AssertionError (UninterceptedRealCall /
#: UnboundedCallDetected) or a message carrying one of these is a HARNESS
#: violation, never a venue error: always propagate, never re-type.
_HARNESS_PAT = ("unintercepted", "timeout-law", "unboundedcalldetected",
                "blocked by conformance harness")


def _reraise_harness(exc: BaseException) -> None:
    """Propagate harness/contract violations out of broad excepts (nado
    _reraise_harness pattern, ported fleet-wide per F7a)."""
    if isinstance(exc, AssertionError):
        raise exc


def _raise_if_harness_msg(msg: str) -> None:
    m = (msg or "").lower()
    if any(p in m for p in _HARNESS_PAT):
        raise AssertionError(msg)


class HLExchangeClient(ExchangeClient):
    """The one conforming HL binding. See module docstring for the census map."""

    #: bounded 429 retry budget for wrapper-level reads (attempt count incl.
    #: the first try; sleeps FLEET_CORE_HL_429_SLEEP * 2^i between attempts)
    RETRY_429_ATTEMPTS = 4
    #: ensure_flat close+readback attempts before WriteUnconfirmed
    CLOSE_ATTEMPTS = 3

    def __init__(self, raw: Any) -> None:
        for attr in ("info", "exchange", "settings", "url"):
            if not hasattr(raw, attr):
                raise TypeError(
                    "hl build_client expects a bot.exchange_hl.HLClient-shaped "
                    "adapter; %r lacks .%s" % (type(raw).__name__, attr))
        self._raw = raw
        mod = importlib.import_module(type(raw).__module__)
        self._coin_to_api: Callable[[str], str] = mod.coin_to_api
        self._api_to_coin: Callable[[str], str] = mod.api_to_coin
        self._tf_ms: Dict[str, int] = dict(mod.TF_MS)

        # Fresh verified-layer state; the raw adapter's own neutral-value
        # caches are purged so no pre-wrapper stale value can ever be served
        # through the contract surface (unbounded-stale class, H1/H4).
        self._mark_cache: Dict[str, Tuple[float, float]] = {}
        self._pos_cache: Optional[Tuple[float, Dict[str, PositionInfo]]] = None
        self._pos_ttl = _f("FLEET_CORE_HL_POS_TTL_SEC", 5.0)
        self._purge_raw_caches()

        meta = raw.get_meta()
        if not meta:
            meta = raw.get_meta(force=True)
        if not meta:
            # get_meta swallows fetch errors into a possibly-empty dict; an
            # empty universe means we could not verify the venue's meta.
            raise ReadUnknown("hl meta unavailable at client build",
                              venue=VENUE, op="get_meta")
        self._meta: Dict[str, Any] = dict(meta)

    # ------------------------------------------------------------------ infra

    def _purge_raw_caches(self) -> None:
        """Drop every raw-adapter cache that could serve a pre-wrapper masked
        or stale value. Attribute access is deliberately direct — if the raw
        adapter renames a cache this must fail LOUD at build, not silently
        leave a stale-serving path alive."""
        r = self._raw
        r.invalidate_positions_cache()
        r.invalidate_user_state()
        with r._cache_lock:  # noqa: SLF001 — wrapper owns its adapter
            r._candles_cache.clear()
            r._candles_fail_cache.clear()
            r._mids_cache = None
            r._mids_cache_hip3 = None
            r._fills_cache = None
            r._av_cache = None
            r._funding_cache = None

    @property
    def _addr(self) -> str:
        return self._raw.settings.account_address

    @staticmethod
    def _timeouts() -> Tuple[float, float]:
        return (5.0, _f("HL_SDK_READ_TIMEOUT", 20.0))

    def _post_info(self, op: str, payload: Dict[str, Any],
                   coin: str = "") -> Any:
        """One verified POST /info through the raw adapter's SDK session.

        Bypasses the SDK's response-parse layer (which turns a non-JSON 200
        into {"error": ...} that legacy code silently reads as empty — the
        residual mask) and its 4xx parser (which KeyErrors on bodies without
        code/msg). Status/JSON handling is done here, typed:
          transport error / 5xx / empty / malformed -> ReadUnknown
          429 beyond the bounded budget            -> RateLimited
        """
        sess = self._raw.info.session
        url = self._raw.url + "/info"
        attempts = max(1, int(_f("FLEET_CORE_HL_429_ATTEMPTS",
                                 self.RETRY_429_ATTEMPTS)))
        for i in range(attempts):
            try:
                r = sess.post(url, json=payload, timeout=self._timeouts())
            except Exception as e:  # ReadTimeout/ConnectionError/anything
                _reraise_harness(e)
                raise ReadUnknown("info %s transport failed: %s" % (op, e),
                                  venue=VENUE, op=op, coin=coin)
            if r.status_code == 429:
                if i < attempts - 1:
                    time.sleep(_f("FLEET_CORE_HL_429_SLEEP", 1.0) * (2 ** i))
                    continue
                raise RateLimited("info %s: 429 retry budget exhausted" % op,
                                  venue=VENUE, op=op, coin=coin)
            if r.status_code >= 400:
                raise ReadUnknown("info %s: HTTP %d" % (op, r.status_code),
                                  venue=VENUE, op=op, coin=coin)
            try:
                data = r.json()
            except Exception as e:
                raise ReadUnknown(
                    "info %s: unparseable body (%s)" % (op, e),
                    venue=VENUE, op=op, coin=coin)
            if data is None:
                raise ReadUnknown("info %s: null body" % op,
                                  venue=VENUE, op=op, coin=coin)
            return data
        raise RateLimited("info %s: unreachable" % op,
                          venue=VENUE, op=op, coin=coin)  # pragma: no cover

    # ------------------------------------------------------------------ reads

    def _parse_positions(self, state: Any, leg: str,
                         out: Dict[str, PositionInfo]) -> None:
        """Strict per-leg (main / HIP-3 dex) snapshot parse. One unparseable
        row poisons the WHOLE snapshot (per-item-corrupt law): a silently
        dropped LIVE position is the phantom-guard mass-close class."""
        if not isinstance(state, dict) or \
                not isinstance(state.get("assetPositions"), list) or \
                not isinstance(state.get("marginSummary"), dict):
            raise ReadUnknown(
                "open_positions: %s leg returned a non-snapshot payload %r"
                % (leg, type(state).__name__), venue=VENUE,
                op="open_positions")
        for ap in state["assetPositions"]:
            try:
                pos = ap.get("position") or {}
                api_name = pos["coin"]
                szi = float(pos["szi"])
                if szi == 0.0:
                    continue
                coin = self._api_to_coin(str(api_name))
                lev_raw = pos.get("leverage") or {}
                liq_raw = pos.get("liquidationPx")
                mu_raw = pos.get("marginUsed")
                up_raw = pos.get("unrealizedPnl")
                out[coin] = PositionInfo(
                    coin=coin,
                    size_signed=szi,
                    entry_px=float(pos["entryPx"]),
                    leverage=(float(lev_raw["value"])
                              if lev_raw.get("value") is not None else None),
                    margin_used_usd=(float(mu_raw)
                                     if mu_raw is not None else None),
                    liquidation_px=(float(liq_raw)
                                    if liq_raw is not None else None),
                    unrealized_pnl=(float(up_raw)
                                    if up_raw is not None else None),
                    raw=pos,
                )
            except (KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "open_positions: unparseable position row on %s leg: %s "
                    "— aborting the whole snapshot (never drop-and-continue)"
                    % (leg, e), venue=VENUE, op="open_positions")

    def _fetch_positions(self) -> Dict[str, PositionInfo]:
        out: Dict[str, PositionInfo] = {}
        state = self._post_info(
            "open_positions",
            {"type": "clearinghouseState", "user": self._addr})
        self._parse_positions(state, "main", out)
        for dex in self._raw.HIP3_USDC_DEXES:
            dex_state = self._post_info(
                "open_positions",
                {"type": "clearinghouseState", "user": self._addr,
                 "dex": dex})
            self._parse_positions(dex_state, "dex=%s" % dex, out)
        return out

    def open_positions(self) -> Mapping[str, PositionInfo]:
        now = time.time()
        if self._pos_cache is not None and \
                (now - self._pos_cache[0]) <= self._pos_ttl:
            return dict(self._pos_cache[1])
        out = self._fetch_positions()
        self._pos_cache = (now, out)
        return dict(out)

    def position_liquidation(self, coin: str) -> Optional[float]:
        pos = self.open_positions().get(coin)  # ReadUnknown propagates (H3)
        if pos is None:
            return None  # POSITIVELY no position
        return pos.liquidation_px

    # -- account bases --------------------------------------------------------

    def _margin_summary(self, dex: str = "") -> Tuple[float, float]:
        """(accountValue, totalMarginUsed) of one clearinghouse, verified."""
        payload: Dict[str, Any] = {"type": "clearinghouseState",
                                   "user": self._addr}
        if dex:
            payload["dex"] = dex
        state = self._post_info("account", payload)
        ms = state.get("marginSummary") if isinstance(state, dict) else None
        if not isinstance(ms, dict):
            raise ReadUnknown(
                "account: clearinghouseState(dex=%r) has no marginSummary"
                % dex, venue=VENUE, op="account")
        try:
            return (float(ms["accountValue"]), float(ms["totalMarginUsed"]))
        except (KeyError, TypeError, ValueError) as e:
            raise ReadUnknown(
                "account: unparseable marginSummary(dex=%r): %s" % (dex, e),
                venue=VENUE, op="account")

    def _spot_usdc(self) -> float:
        spot = self._post_info(
            "account", {"type": "spotClearinghouseState", "user": self._addr})
        bals = spot.get("balances") if isinstance(spot, dict) else None
        if not isinstance(bals, list):
            raise ReadUnknown("account: spotClearinghouseState has no "
                              "balances list", venue=VENUE, op="account")
        total = 0.0
        for b in bals:
            try:
                if b.get("coin") == "USDC":
                    total += float(b["total"])
            except (AttributeError, KeyError, TypeError, ValueError) as e:
                raise ReadUnknown(
                    "account: unparseable spot balance row: %s" % e,
                    venue=VENUE, op="account")
        return total

    def account_value(self) -> float:
        """Deployed sizing basis, unchanged semantics (exchange_hl.py:752):
        spot USDC + perp accountValue (incl uPnL) over main + HIP-3 dexes —
        verified-or-raise (the raw path's `or 0` parses masked a malformed
        payload into 0.0)."""
        total = self._spot_usdc()
        av, _ = self._margin_summary("")
        total += av
        for dex in self._raw.HIP3_USDC_DEXES:
            av, _ = self._margin_summary(dex)
            total += av
        return total

    def equity_with_upnl(self) -> float:
        """H11: on HL's unified account the deployable equity (spot USDC +
        perp accountValue incl uPnL) IS the account equity — documented alias
        of account_value() (census H11 / mapping §1: 'no separate uPnL-free
        basis in use'). Fail-closed: ReadUnknown propagates."""
        return self.account_value()

    def margin_used_usd(self) -> float:
        _, mu = self._margin_summary("")
        total = mu
        for dex in self._raw.HIP3_USDC_DEXES:
            _, mu = self._margin_summary(dex)
            total += mu
        return total

    # -- marks -----------------------------------------------------------------

    def _fetch_mids(self, dex: str, coin: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"type": "allMids"}
        if dex:
            payload["dex"] = dex
        mids = self._post_info("mark_price", payload, coin=coin)
        if not isinstance(mids, dict) or "error" in mids:
            raise ReadUnknown(
                "mark_price(%s): allMids returned non-map payload" % coin,
                venue=VENUE, op="mark_price", coin=coin)
        return mids

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        api = self._coin_to_api(coin)
        dex = api.split(":", 1)[0] if ":" in api else ""
        try:
            mids = self._fetch_mids(dex, coin)
            raw_val = mids.get(api)
            if raw_val is None:
                # SUCCESSFUL fetch, symbol absent from the venue's own map —
                # caller bug / delisted coin, never 0.0 (H1 detail).
                raise ValueError(
                    "mark_price(%s): symbol %r absent from a successful "
                    "allMids(dex=%r) response" % (coin, api, dex))
            px = float(raw_val)
            if px <= 0.0:
                raise ReadUnknown(
                    "mark_price(%s): venue served %r — insane/absent"
                    % (coin, raw_val), venue=VENUE, op="mark_price", coin=coin)
        except RateLimited:
            return self._mark_from_cache(coin, max_age_sec, reraise=True)
        except ReadUnknown:
            return self._mark_from_cache(coin, max_age_sec, reraise=True)
        self._mark_cache[coin] = (px, time.time())
        return px

    def _mark_from_cache(self, coin: str, max_age_sec: float,
                         reraise: bool) -> float:
        cached = self._mark_cache.get(coin)
        if cached is not None:
            px, ts = cached
            age = time.time() - ts
            if age <= max_age_sec:
                return px  # cache INSIDE tolerance: legal and encouraged (L5)
            raise StaleData(
                "mark_price(%s): freshest verifiable value is %.3fs old "
                "(tolerance %.3fs)" % (coin, age, max_age_sec),
                age_sec=age, tolerance_sec=max_age_sec,
                venue=VENUE, op="mark_price", coin=coin)
        raise  # re-raise the in-flight ReadUnknown/RateLimited

    # -- candles ---------------------------------------------------------------

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> Any:
        if interval not in self._tf_ms:
            raise ValueError(
                "candles: unsupported interval %r (caller bug)" % interval)
        if coin not in self._meta:
            # one refresh — newly listed coins. F7e: ValueError (caller bug)
            # is legal ONLY off a VERIFIED meta; a refresh that dies on
            # transport leaves coin liveness UNKNOWN (raw.get_meta swallows
            # fetch errors into a possibly-EMPTY dict — empty means the
            # refresh itself failed, not that the coin is unknown).
            try:
                fresh = self._raw.get_meta(force=True)
            except Exception as e:
                _reraise_harness(e)
                raise ReadUnknown(
                    "candles(%s,%s): coin absent from cached meta and the "
                    "meta refresh failed (%s) — coin liveness UNVERIFIABLE, "
                    "not a caller bug" % (coin, interval, e),
                    venue=VENUE, op="candles", coin=coin)
            if not fresh:
                raise ReadUnknown(
                    "candles(%s,%s): coin absent from cached meta and the "
                    "meta refresh returned EMPTY (transport failure masked "
                    "by the raw adapter) — coin liveness UNVERIFIABLE"
                    % (coin, interval), venue=VENUE, op="candles", coin=coin)
            self._meta = dict(fresh)
            if coin not in self._meta:
                raise ValueError(
                    "candles: unknown coin %r (caller bug)" % coin)
        try:
            df = self._raw.candles(coin, interval, limit=limit)
        except Exception as e:
            _reraise_harness(e)
            raise ReadUnknown("candles(%s,%s): fetch failed: %s"
                              % (coin, interval, e),
                              venue=VENUE, op="candles", coin=coin)
        if df is None or len(df) == 0:
            if self._raw.candles_in_fail_cache(coin, interval):
                raise ReadUnknown(
                    "candles(%s,%s): fetch failed (negative-cached) — the "
                    "raw adapter's silent empty-df is an error here (H2)"
                    % (coin, interval), venue=VENUE, op="candles", coin=coin)
            raise ReadUnknown(
                "candles(%s,%s): venue returned no bars — emptiness is not "
                "verifiable data" % (coin, interval),
                venue=VENUE, op="candles", coin=coin)
        tf_ms = int(self._tf_ms[interval])
        try:
            last_open_ms = int(df.iloc[-1]["time"].timestamp() * 1000)
        except Exception as e:
            _reraise_harness(e)
            raise ReadUnknown(
                "candles(%s,%s): unparseable last-bar timestamp: %s"
                % (coin, interval, e), venue=VENUE, op="candles", coin=coin)
        now_ms = int(time.time() * 1000)
        boundary = (now_ms // tf_ms) * tf_ms
        lag_bars = (boundary - (last_open_ms + tf_ms)) / float(tf_ms)
        if lag_bars > max_stale_bars:
            raise StaleData(
                "candles(%s,%s): last closed bar lags %.2f bars "
                "(tolerance %.2f)" % (coin, interval, lag_bars,
                                      max_stale_bars),
                age_sec=lag_bars * tf_ms / 1000.0,
                tolerance_sec=max_stale_bars * tf_ms / 1000.0,
                venue=VENUE, op="candles", coin=coin)
        return df

    # -- fills -----------------------------------------------------------------

    def user_fills(self, max_age_sec: float = 60.0
                   ) -> Sequence[Mapping[str, Any]]:
        fills = self._post_info("user_fills",
                                {"type": "userFills", "user": self._addr})
        if not isinstance(fills, list):
            raise ReadUnknown(
                "user_fills: non-list payload %r — [] is only legal on a "
                "positive venue answer (H4)" % type(fills).__name__,
                venue=VENUE, op="user_fills")
        for f in fills:
            try:
                float(f["px"])
                float(f["sz"])
                int(f.get("time", 0) or 0)
            except (AttributeError, KeyError, TypeError, ValueError) as e:
                # per-item-corrupt law (H4 + P10): ONE unparseable fill row
                # poisons the WHOLE read — a fill the consumers' float()
                # parses would silently skip must never be invisible data
                raise ReadUnknown(
                    "user_fills: unparseable fill row (%s): %r — aborting "
                    "the whole read" % (e, f), venue=VENUE, op="user_fills")
        return list(fills)

    # -- order listings --------------------------------------------------------

    def _frontend_orders(self) -> List[Dict[str, Any]]:
        """frontendOpenOrders across main + every HIP-3 dex; ANY leg failing
        raises (a partial listing must never read as the whole truth —
        exchange_hl.py:1531 canon)."""
        rows: List[Dict[str, Any]] = []
        for dex in [""] + list(self._raw.HIP3_USDC_DEXES):
            payload: Dict[str, Any] = {"type": "frontendOpenOrders",
                                       "user": self._addr}
            if dex:
                payload["dex"] = dex
            chunk = self._post_info("open_orders", payload)
            if not isinstance(chunk, list):
                raise ReadUnknown(
                    "open_orders: dex=%r leg returned non-list payload"
                    % dex, venue=VENUE, op="open_orders")
            rows.extend(c for c in chunk if isinstance(c, dict))
            if len([c for c in chunk if not isinstance(c, dict)]):
                raise ReadUnknown(
                    "open_orders: dex=%r leg holds a non-object row" % dex,
                    venue=VENUE, op="open_orders")
        return rows

    def _order_info(self, o: Dict[str, Any]) -> OpenOrderInfo:
        try:
            side_raw = str(o["side"])
            side = {"B": "buy", "A": "sell",
                    "buy": "buy", "sell": "sell"}[side_raw]
            limit_px = float(o.get("limitPx") or 0.0)
            trigger_px = float(o.get("triggerPx") or 0.0)
            return OpenOrderInfo(
                coin=self._api_to_coin(str(o["coin"])),
                oid=str(o["oid"]),
                side=side,
                size=float(o["sz"]),
                limit_px=limit_px if limit_px > 0 else None,
                trigger_px=trigger_px if trigger_px > 0 else None,
                reduce_only=bool(o.get("reduceOnly")),
                is_trigger=bool(o.get("isTrigger")),
                raw=o,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ReadUnknown(
                "open_orders: unparseable order row (%s) — aborting the "
                "whole listing (an invisible live order is the false-naked/"
                "orphan-blindness class)" % e, venue=VENUE, op="open_orders")

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        out: Dict[str, OpenOrderInfo] = {}
        for o in self._frontend_orders():
            info = self._order_info(o)
            out[info.oid] = info  # dedupe across dex legs by oid
        return list(out.values())

    @staticmethod
    def _is_protective_stop(o: Dict[str, Any]) -> bool:
        """reduce-only + genuine stop-trigger (mirrors exchange_hl.py:1592
        filter: a reduce-only TP limit must NOT count as an SL)."""
        if not o.get("reduceOnly"):
            return False
        return bool(o.get("isTrigger")) and \
            "stop" in str(o.get("orderType", "")).lower()

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        api = self._coin_to_api(coin)
        out: List[str] = []
        for o in self._frontend_orders():
            try:
                if o.get("coin") != api or not self._is_protective_stop(o):
                    continue
                oid = o["oid"]
            except (KeyError, TypeError) as e:
                raise ReadUnknown(
                    "list_open_sl_orders(%s): unparseable row: %s" % (coin, e),
                    venue=VENUE, op="list_open_sl_orders", coin=coin)
            out.append(str(oid))
        return out

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        out: Dict[str, OpenOrderInfo] = {}
        for o in self._frontend_orders():
            if not (o.get("isTrigger") and o.get("reduceOnly")):
                continue
            info = self._order_info(o)
            out[info.oid] = info
        return list(out.values())

    # ------------------------------------------------------------------ writes

    @staticmethod
    def _classify_write_exc(e: Exception) -> Optional[bool]:
        """may_have_landed for a raised write (F1 root-fix).

        False is reserved for connection-ESTABLISHMENT failures ONLY —
        requests.ConnectTimeout / urllib3.NewConnectionError (their message
        carries 'Failed to establish a new connection') provably never put
        bytes on the wire. A plain requests.ConnectionError ALSO covers
        MID-RESPONSE failures (reset-by-peer AFTER the venue accepted and
        landed the order), so it is AMBIGUOUS: None — the caller must
        readback before deciding. Pre-fix, any 'Connect*'-named exception was
        classified False, which skipped the fills readback and hid a landed
        fill (the untracked-position class this layer exists to kill)."""
        cur: Optional[BaseException] = e
        seen = 0
        while cur is not None and seen < 8:  # walk the cause/context chain
            name = type(cur).__name__
            if name in ("ConnectTimeout", "NewConnectionError"):
                return False
            cur = cur.__cause__ or cur.__context__
            seen += 1
        if "failed to establish" in str(e).lower():
            return False  # requests wraps NewConnectionError into its message
        return None

    @staticmethod
    def _parse_statuses(resp: Any) -> Tuple[str, Any]:
        """SDK /exchange response -> one of:
        ("rejected", reason) ("error", msg) ("filled", d) ("resting", oid)
        ("cancel_statuses", list) ("unknown", None)."""
        if not isinstance(resp, dict):
            return ("unknown", None)
        if resp.get("status") == "err":
            return ("rejected", str(resp.get("response")))
        if resp.get("status") != "ok":
            return ("unknown", None)  # e.g. SDK's {"error": ...} parse echo
        data = (resp.get("response") or {}).get("data") or {}
        statuses = data.get("statuses")
        if not isinstance(statuses, list) or not statuses:
            return ("unknown", None)
        st = statuses[0]
        if isinstance(st, str):
            return ("cancel_statuses", statuses)
        if not isinstance(st, dict):
            return ("unknown", None)
        if "error" in st:
            return ("error", str(st["error"]))
        if isinstance(st.get("filled"), dict):
            return ("filled", st["filled"])
        if isinstance(st.get("resting"), dict):
            return ("resting", st["resting"].get("oid"))
        return ("unknown", None)

    def _my_fills_since(self, coin: str, is_buy: bool,
                        t0_ms: int) -> List[Dict[str, Any]]:
        api = self._coin_to_api(coin)
        want_side = "B" if is_buy else "A"
        out = []
        for f in self.user_fills():
            try:
                if f.get("coin") != api or f.get("side") != want_side:
                    continue
                if int(f.get("time", 0) or 0) < t0_ms:
                    continue
                out.append({"px": float(f["px"]), "sz": float(f["sz"]),
                            "oid": f.get("oid")})
            except (KeyError, TypeError, ValueError):
                continue  # attribution readback: unmatchable row is skipped
        return out

    def _fill_from_readback(self, coin: str, is_buy: bool, requested: float,
                            t0_ms: int, acked: Optional[bool]) -> FillResult:
        """Fills-feed readback attribution (H5). Raises WriteUnconfirmed when
        the feed is unreadable or holds nothing attributable."""
        try:
            mine = self._my_fills_since(coin, is_buy, t0_ms)
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "market_open(%s): ack ambiguous and the fills readback "
                "failed — cannot prove either way: %s" % (coin, e),
                may_have_landed=True, venue=VENUE, op="market_open",
                coin=coin)
        if not mine:
            raise WriteUnconfirmed(
                "market_open(%s): %s and the fills readback shows NOTHING "
                "landed" % (coin, "acked without fill fields"
                            if acked else "response lost"),
                may_have_landed=acked, venue=VENUE, op="market_open",
                coin=coin)
        tot = sum(f["sz"] for f in mine)
        vwap = sum(f["px"] * f["sz"] for f in mine) / tot
        return FillResult(coin=coin, is_buy=is_buy, avg_px=vwap, size=tot,
                          requested_size=float(requested),
                          oid=(str(mine[0]["oid"])
                               if mine[0].get("oid") is not None else None))

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        if coin not in self._meta:
            raise VenueRejected(
                "market_open(%s): unknown symbol" % coin,
                reason="unknown_symbol", venue=VENUE, op="market_open",
                coin=coin)
        if intended_px is not None and not allow_marketable:
            # marketability guard (extended pattern, opt-in per contract) —
            # mark read failing here raises ReadUnknown: NOTHING was sent.
            mark = self.mark_price(coin)
            if (is_buy and mark <= intended_px) or \
                    ((not is_buy) and mark >= intended_px):
                raise VenueRejected(
                    "market_open(%s): level %.10g already through the book "
                    "(mark %.10g)" % (coin, intended_px, mark),
                    reason="immediately_marketable", venue=VENUE,
                    op="market_open", coin=coin)
        t0_ms = int(time.time() * 1000) - 2000
        try:
            resp = self._raw.market_open(coin, is_buy, sz)
        except Exception as e:
            _reraise_harness(e)
            self.invalidate_positions_cache()
            landed = self._classify_write_exc(e)
            if landed is False:
                raise WriteUnconfirmed(
                    "market_open(%s): send failed before the wire: %s"
                    % (coin, e), may_have_landed=False, venue=VENUE,
                    op="market_open", coin=coin)
            # ambiguous (timeout / lost response): the fill may exist —
            # readback BEFORE deciding (HL confirm-via-readback pattern).
            return self._fill_from_readback(coin, is_buy, sz, t0_ms,
                                            acked=None)
        self.invalidate_positions_cache()
        kind, val = self._parse_statuses(resp)
        if kind in ("error", "rejected"):
            raise VenueRejected(
                "market_open(%s): venue rejected: %s" % (coin, val),
                reason=str(val), venue=VENUE, op="market_open", coin=coin)
        if kind == "filled":
            try:
                return FillResult(
                    coin=coin, is_buy=is_buy,
                    avg_px=float(val["avgPx"]), size=float(val["totalSz"]),
                    requested_size=float(sz),
                    oid=(str(val["oid"]) if val.get("oid") is not None
                         else None))
            except (KeyError, TypeError, ValueError):
                pass  # unparseable fill fields -> readback decides
        # resting-acked IOC / unparseable ack: truth lives in the fills feed
        return self._fill_from_readback(coin, is_buy, sz, t0_ms, acked=True)

    def ensure_flat(self, coin: str) -> FlatResult:
        # initial read — failure here means NOTHING was sent (fresh, not cache)
        self.invalidate_positions_cache()
        pos_map = self.open_positions()
        if coin not in pos_map:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        pos = pos_map[coin]
        total = abs(pos.size_signed)
        close_is_buy = pos.size_signed < 0
        t0_ms = int(time.time() * 1000) - 2000
        sent_any = False
        exit_px: Optional[float] = None
        attempts = 0
        for attempts in range(1, self.CLOSE_ATTEMPTS + 1):
            try:
                resp = self._raw.market_close(coin)
            except Exception as e:
                _reraise_harness(e)
                resp = None
                if self._classify_write_exc(e) is not False:
                    sent_any = True  # may have reached the venue
            if resp is not None:
                kind, val = self._parse_statuses(resp)
                if kind == "filled":
                    sent_any = True
                    try:
                        exit_px = float(val["avgPx"])
                    except (KeyError, TypeError, ValueError):
                        exit_px = None
                elif kind in ("resting", "unknown"):
                    sent_any = True
                # "error"/"rejected" (e.g. already flat -> SDK no-op/None,
                # reduce-only-would-increase): the position readback decides.
            # position readback — the ONLY source of "flat" truth (N7 lesson:
            # a failed confirming read is NEVER success)
            self.invalidate_positions_cache()
            try:
                now_map = self.open_positions()
            except (ReadUnknown, RateLimited) as e:
                raise WriteUnconfirmed(
                    "ensure_flat(%s): close %s but the confirming position "
                    "readback failed — flatness UNPROVEN: %s"
                    % (coin, "sent" if sent_any else "maybe-sent", e),
                    may_have_landed=(True if sent_any else None),
                    venue=VENUE, op="ensure_flat", coin=coin)
            if coin not in now_map:
                if exit_px is None:
                    exit_px = self._exit_px_best_effort(coin, close_is_buy,
                                                        t0_ms)
                return FlatResult(coin=coin, already_flat=False,
                                  closed_size=total, exit_avg_px=exit_px,
                                  attempts=attempts)
        raise WriteUnconfirmed(
            "ensure_flat(%s): %d close attempts exhausted, position still "
            "open on readback" % (coin, attempts),
            may_have_landed=(True if sent_any else None),
            venue=VENUE, op="ensure_flat", coin=coin)

    def _exit_px_best_effort(self, coin: str, close_is_buy: bool,
                             t0_ms: int) -> Optional[float]:
        """Real exit VWAP from the fills feed; None when unattributable —
        NEVER an SL/reference price (wick_sl ×1330 class)."""
        try:
            mine = self._my_fills_since(coin, close_is_buy, t0_ms)
        except Exception as e:
            _reraise_harness(e)
            return None
        tot = sum(f["sz"] for f in mine)
        if tot <= 0:
            return None
        return sum(f["px"] * f["sz"] for f in mine) / tot

    # -- SL triggers ------------------------------------------------------------

    def _order_status(self, oid: int, coin: str) -> Tuple[str, Dict[str, Any]]:
        """orderStatus readback: ("open"|"gone"|"other", order-dict)."""
        st = self._post_info(
            "trigger_confirm",
            {"type": "orderStatus", "user": self._addr, "oid": int(oid)},
            coin=coin)
        if not isinstance(st, dict):
            raise ReadUnknown("orderStatus(%s): non-object payload" % oid,
                              venue=VENUE, op="trigger_confirm", coin=coin)
        if st.get("status") == "unknownOid":
            return ("gone", {})
        inner = st.get("order") or {}
        state = str(inner.get("status", ""))
        o = inner.get("order") or {}
        if st.get("status") == "order" and state == "open":
            return ("open", o if isinstance(o, dict) else {})
        return ("other", o if isinstance(o, dict) else {})

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        try:
            resp = self._raw.trigger_sl(coin, is_buy, sz, trigger_px)
        except Exception as e:
            _reraise_harness(e)
            landed = self._classify_write_exc(e)
            if landed is False:
                raise WriteUnconfirmed(
                    "trigger_sl(%s): send failed before the wire: %s"
                    % (coin, e), may_have_landed=False, venue=VENUE,
                    op="trigger_sl", coin=coin)
            # response lost with no oid: try to identify OUR trigger in the
            # live listing by shape (prevents the blind-retry duplicate-SL
            # stack, N9 lesson) — else UNCONFIRMED.
            return self._trigger_from_listing_match(coin, is_buy, sz,
                                                    trigger_px, cause=e)
        kind, val = self._parse_statuses(resp)
        if kind in ("error", "rejected"):
            raise VenueRejected(
                "trigger_sl(%s): venue rejected: %s" % (coin, val),
                reason=str(val), venue=VENUE, op="trigger_sl", coin=coin)
        if kind == "resting" and val is not None:
            return self._confirm_trigger_live(coin, is_buy, int(val))
        raise WriteUnconfirmed(
            "trigger_sl(%s): ok-shaped response without a resting oid "
            "(kind=%s) — cannot confirm placement" % (coin, kind),
            may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)

    def _confirm_trigger_live(self, coin: str, is_buy: bool,
                              oid: int) -> SLOrderInfo:
        """MANDATORY live readback of an acked trigger (H7): an ok-shaped ack
        that never appears live was the naked-success class."""
        try:
            state, o = self._order_status(oid, coin)
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "trigger_sl(%s): acked oid=%s but the live-confirm readback "
                "failed — refusing to claim placed: %s" % (coin, oid, e),
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        if state != "open":
            raise WriteUnconfirmed(
                "trigger_sl(%s): acked oid=%s is NOT live on the venue "
                "(status=%s) — naked-success class" % (coin, oid, state),
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        try:
            return SLOrderInfo(
                coin=coin, oid=str(oid),
                trigger_px=float(o["triggerPx"]),
                size=float(o["sz"]),
                is_buy_to_close=(str(o.get("side", "B" if is_buy else "A"))
                                 == "B"))
        except (KeyError, TypeError, ValueError) as e:
            raise WriteUnconfirmed(
                "trigger_sl(%s): live order %s unparseable (%s) — cannot "
                "hand back a verified SLOrderInfo" % (coin, oid, e),
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)

    def _trigger_from_listing_match(self, coin: str, is_buy: bool, sz: float,
                                    trigger_px: float,
                                    cause: Exception) -> SLOrderInfo:
        want_px = self._raw.round_price(coin, trigger_px)
        want_sz = self._raw.round_size(coin, sz)
        api = self._coin_to_api(coin)
        want_side = "B" if is_buy else "A"
        try:
            rows = self._frontend_orders()
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "trigger_sl(%s): response lost (%s) and the live listing is "
                "unreadable (%s) — placement UNKNOWN" % (coin, cause, e),
                may_have_landed=True, venue=VENUE, op="trigger_sl", coin=coin)
        matches = []
        for o in rows:
            try:
                if o.get("coin") != api or not self._is_protective_stop(o):
                    continue
                if str(o.get("side")) != want_side:
                    continue
                if abs(float(o.get("triggerPx") or 0.0) - want_px) > \
                        max(1e-9, 1e-6 * want_px):
                    continue
                if abs(float(o.get("sz") or 0.0) - want_sz) > \
                        max(1e-12, 1e-6 * max(want_sz, 1.0)):
                    continue
                matches.append(o)
            except (TypeError, ValueError):
                continue
        if len(matches) == 1:
            o = matches[0]
            return SLOrderInfo(coin=coin, oid=str(o["oid"]),
                               trigger_px=float(o["triggerPx"]),
                               size=float(o["sz"]),
                               is_buy_to_close=is_buy)
        raise WriteUnconfirmed(
            "trigger_sl(%s): response lost (%s); live listing shows %d "
            "shape-matching trigger(s) — cannot uniquely confirm"
            % (coin, cause, len(matches)),
            may_have_landed=None, venue=VENUE, op="trigger_sl", coin=coin)

    # -- cancel ------------------------------------------------------------------

    _GONE_MARKERS = ("never placed", "already canceled", "already cancelled",
                     "or filled", "unknown oid", "order not found",
                     "not_found")

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        try:
            oid_i = int(str(oid).split("-")[-1])
        except (TypeError, ValueError):
            raise VenueRejected(
                "cancel_sl_order(%s): malformed oid %r — HL cancels by "
                "integer oid" % (coin, oid),
                reason="malformed_oid", venue=VENUE, op="cancel_sl_order",
                coin=coin)
        venue_reject: Optional[str] = None
        try:
            resp = self._raw.cancel_sl_order(coin, oid_i)
        except Exception as e:
            _reraise_harness(e)
            resp = None  # ambiguity — the gone-readback below decides
        if resp is not None:
            kind, val = self._parse_statuses(resp)
            if kind == "cancel_statuses":
                pass  # ["success", ...] — verified by readback anyway
            elif kind in ("error", "rejected"):
                low = str(val).lower()
                if not any(m in low for m in self._GONE_MARKERS):
                    venue_reject = str(val)
        # confirmed-GONE readback (H8) — cancel-then-assume was half of the
        # trail place/cancel races; not-found maps to idempotent success.
        try:
            rows = self._frontend_orders()
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s): cannot prove oid=%s gone — the "
                "confirming listing failed: %s" % (coin, oid_i, e),
                may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                coin=coin)
        still_live = any(str(o.get("oid")) == str(oid_i) for o in rows)
        if still_live:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s): cancel %s but oid=%s is STILL LIVE on "
                "the venue" % (coin, "answered %r" % venue_reject
                               if venue_reject else "acked", oid_i),
                may_have_landed=True, venue=VENUE, op="cancel_sl_order",
                coin=coin)
        if venue_reject is not None:
            # definitive non-not-found refuse AND the order is gone: goal
            # state holds — treat as success (idempotence beats the refuse).
            log.info("cancel_sl_order(%s): venue answered %r but oid=%s is "
                     "confirmed gone — goal state holds", coin, venue_reject,
                     oid_i)
        return None

    # -- TP1 resting limit --------------------------------------------------------

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        t0_ms = int(time.time() * 1000) - 2000
        try:
            resp = self._raw.limit_reduce_only(coin, is_buy, sz, px)
        except Exception as e:
            _reraise_harness(e)
            landed = self._classify_write_exc(e)
            if landed is False:
                raise WriteUnconfirmed(
                    "limit_reduce_only(%s): send failed before the wire: %s"
                    % (coin, e), may_have_landed=False, venue=VENUE,
                    op="limit_reduce_only", coin=coin)
            return self._limit_ro_readback(coin, is_buy, sz, px, oid=None,
                                           t0_ms=t0_ms)
        kind, val = self._parse_statuses(resp)
        if kind in ("error", "rejected"):
            raise VenueRejected(
                "limit_reduce_only(%s): venue rejected: %s" % (coin, val),
                reason=str(val), venue=VENUE, op="limit_reduce_only",
                coin=coin)
        if kind == "filled":
            self.invalidate_positions_cache()
            return OpenOrderInfo(
                coin=coin,
                oid=str(val.get("oid") if val.get("oid") is not None
                        else "filled"),
                side="buy" if is_buy else "sell",
                size=float(val.get("totalSz") or sz),
                limit_px=float(px), reduce_only=True, is_trigger=False,
                raw={"filled": dict(val)})
        oid = int(val) if kind == "resting" and val is not None else None
        return self._limit_ro_readback(coin, is_buy, sz, px, oid=oid,
                                       t0_ms=t0_ms)

    def _limit_ro_readback(self, coin: str, is_buy: bool, sz: float,
                           px: float, oid: Optional[int],
                           t0_ms: int) -> OpenOrderInfo:
        try:
            rows = self._frontend_orders()
        except (ReadUnknown, RateLimited) as e:
            raise WriteUnconfirmed(
                "limit_reduce_only(%s): acked (oid=%s) but the resting-"
                "confirm listing failed: %s" % (coin, oid, e),
                may_have_landed=True, venue=VENUE, op="limit_reduce_only",
                coin=coin)
        if oid is not None:
            for o in rows:
                if str(o.get("oid")) == str(oid):
                    return self._order_info(o)
        # not resting under its oid — an immediate fill may have raced the ack
        try:
            mine = self._my_fills_since(coin, is_buy, t0_ms)
        except (ReadUnknown, RateLimited):
            mine = []
        if mine:
            tot = sum(f["sz"] for f in mine)
            vwap = sum(f["px"] * f["sz"] for f in mine) / tot
            self.invalidate_positions_cache()
            return OpenOrderInfo(
                coin=coin,
                oid=str(oid if oid is not None else
                        (mine[0].get("oid") or "filled")),
                side="buy" if is_buy else "sell", size=tot,
                limit_px=float(px), reduce_only=True, is_trigger=False,
                raw={"filled": {"avgPx": vwap, "totalSz": tot}})
        raise WriteUnconfirmed(
            "limit_reduce_only(%s): acked (oid=%s) but neither resting nor "
            "filled on readback" % (coin, oid),
            may_have_landed=True, venue=VENUE, op="limit_reduce_only",
            coin=coin)

    # -- leverage ------------------------------------------------------------------

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        """H9: the raw adapter's dict|None mask is bypassed — the SDK is
        called directly so a definitive venue refuse carries its reason.

        DRY guard (F2 root-fix): calling the SDK directly also bypassed the
        raw adapter's _dry_guard (exchange_hl.py:1216-1227 route / :1305
        helper — audit defense-in-depth 2026-06-19), so DRY_RUN=1 signed and
        SENT a real leverage change. Replicated here: under DRY nothing is
        signed and the call is a benign success (None), exactly the raw
        guard's caller-proceeds semantics."""
        if bool(getattr(self._raw.settings, "dry_run", False)):
            log.info("[DRY] update_leverage(%s, %d, cross=%s) blocked at the "
                     "wrapper — no signed action leaves the process",
                     coin, leverage, is_cross)
            return None
        api = self._coin_to_api(coin)
        try:
            resp = self._raw.exchange.update_leverage(int(leverage), api,
                                                      bool(is_cross))
        except Exception as e:
            _reraise_harness(e)
            landed = self._classify_write_exc(e)
            raise WriteUnconfirmed(
                "update_leverage(%s, %d): confirmation unreadable: %s"
                % (coin, leverage, e), may_have_landed=landed, venue=VENUE,
                op="update_leverage", coin=coin)
        if isinstance(resp, dict) and resp.get("status") == "ok":
            return None
        if isinstance(resp, dict) and resp.get("status") == "err":
            raise VenueRejected(
                "update_leverage(%s, %d): venue refused: %s"
                % (coin, leverage, resp.get("response")),
                reason=str(resp.get("response")), venue=VENUE,
                op="update_leverage", coin=coin)
        raise WriteUnconfirmed(
            "update_leverage(%s, %d): unrecognized response %r"
            % (coin, leverage, resp), may_have_landed=True, venue=VENUE,
            op="update_leverage", coin=coin)

    # ---------------------------------------------------- CONTRACT-EXEMPT
    # Non-contract caller surface (F4 / mapping §6): methods the deployed
    # trader/scanner/resting_orders/liquidity_snapshot callers use that stay
    # OUTSIDE the typed error contract — VERBATIM delegation to the raw
    # adapter, semantics (incl. dict|None returns and the raw DRY guards)
    # UNCHANGED, so the P3 repoint cannot AttributeError. Typed-error laws do
    # NOT apply here; these ride P3 as-is.
    #   callers grepped 2026-07-03: trader.py (asset, round_price,
    #   add_isolated_margin), resting_orders.py (asset, round_price,
    #   resting_stop_limit), scanner.py (invalidate_candles_cache,
    #   candles_in_fail_cache), liquidity_snapshot.py (orderbook_snapshot);
    #   the rest are raw-adapter surface kept for parity with mapping §6.

    CONTRACT_EXEMPT_PASSTHROUGH: Tuple[str, ...] = (
        # NO round_qty: the HL raw adapter does not define it (round_size is
        # the HL spelling) — delegating to a nonexistent method would turn a
        # clean AttributeError into a runtime one.
        "asset", "round_price", "round_size", "slip_per_side",
        "funding_rate", "spot_usdc", "orderbook_snapshot",
        "compute_realized_pnl", "trigger_tp", "resting_stop_limit",
        "add_isolated_margin", "invalidate_candles_cache",
        "candles_in_fail_cache",
    )

    def asset(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.asset."""
        return self._raw.asset(*a, **kw)

    def round_price(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.round_price."""
        return self._raw.round_price(*a, **kw)

    def round_size(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.round_size."""
        return self._raw.round_size(*a, **kw)

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
        guard applies)."""
        return self._raw.trigger_tp(*a, **kw)

    def resting_stop_limit(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.resting_stop_limit (raw
        DRY guard applies)."""
        return self._raw.resting_stop_limit(*a, **kw)

    def add_isolated_margin(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.add_isolated_margin
        (raw DRY guard applies)."""
        return self._raw.add_isolated_margin(*a, **kw)

    def invalidate_candles_cache(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.invalidate_candles_cache."""
        return self._raw.invalidate_candles_cache(*a, **kw)

    def candles_in_fail_cache(self, *a: Any, **kw: Any) -> Any:
        """CONTRACT-EXEMPT verbatim delegation → raw.candles_in_fail_cache."""
        return self._raw.candles_in_fail_cache(*a, **kw)

    @property
    def raw(self) -> Any:
        """The underlying bot.exchange_hl.HLClient (forensics only)."""
        return self._raw

    # ------------------------------------------------------------- cache control

    def invalidate_positions_cache(self) -> None:
        self._pos_cache = None
        self._raw.invalidate_positions_cache()
        self._raw.invalidate_user_state()


# ============================================================================
# Factories
# ============================================================================

def build_client(raw: Any = None) -> HLExchangeClient:
    """Wrap an already-built bot.exchange_hl.HLClient (conformance-harness
    hook); raw=None builds the adapter from the environment (production)."""
    if raw is None:
        from bot.config import Settings  # lazy: bot root must be on sys.path
        from bot.exchange_hl import HLClient
        raw = HLClient(Settings.from_env())
    return HLExchangeClient(raw)


def get_client() -> HLExchangeClient:
    """Production factory: build the raw adapter from the bot env, wrap it."""
    return build_client(None)
