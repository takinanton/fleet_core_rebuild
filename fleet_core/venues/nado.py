"""fleet_core.venues.nado — the ONE conforming ExchangeClient for Nado.

Wraps the existing raw adapter (bot.exchange_nado.NadoClient_) WITHOUT
modifying it: every fleet_core.exchange_api law is enforced HERE, at the
wrapper layer. The raw adapter stays byte-identical on the host until the P3
cutover; this module imports bot.* / nado_protocol.* LAZILY so it is
importable (and its factory constructible) on machines with no venue SDK.

Census violations closed at this layer (proofs/p2_violation_census.md §4):

  N0  timeouts        — the raw adapter's 2026-07-02 session-timeout guards
                        (_install_session_timeout + _sweep_untimed_sessions)
                        already bound every SDK session at construction; the
                        wrapper re-runs the gc sweep whenever it detects the
                        SDK object was rebuilt in-place (trigger-client heal
                        path creates FRESH unguarded sessions).
  N1  open_positions  — wrapper-owned summary fetch; stale serve is BOUNDED
                        (<= NADO_POSITIONS_STALE_BOUND_SEC, default 30s =
                        one main-loop interval ceiling); beyond -> StaleData;
                        no cache -> ReadUnknown. Never the raw unbounded
                        stale-cache fallback (exchange_nado.py:807).
  N2  account_value   — healths[0] (initial health, the deployed sizing
                        basis — semantics unchanged) verified-or-ReadUnknown,
                        never 0.0.
  N3  mark_price      — verified mid (book) with oracle fallback, sanity-
                        gated (MARK_SANITY_MAX); ReadUnknown on fail/insane,
                        StaleData beyond max_age_sec, ValueError on unknown
                        symbol. Never 0.0, never negative-cached.
  N4  candles         — empty frame -> ReadUnknown; last-closed-bar boundary
                        gate -> StaleData(max_stale_bars); unsupported
                        interval / unknown coin -> ValueError.
  N5  open_orders     — engine open-order listing MERGED with the trigger-
                        service listing; either leg failing -> ReadUnknown;
                        per-item parse failure poisons the WHOLE read.
  N6  market_open     — raw ok-shaped error dicts translated: definitive
                        venue rejects (below_min_size / error_code bodies /
                        empty book) -> VenueRejected(reason); transport
                        ambiguity -> venue-truth readback (matches feed
                        watermark first, position delta second) -> real
                        FillResult or WriteUnconfirmed. No fabrication.
  N7  ensure_flat     — wrapper-owned close loop: close send + FORCED
                        position readback; a FAILED confirming read is
                        WriteUnconfirmed(may_have_landed=True), NEVER the
                        raw sentinel-(-1.0)-falls-to-success path
                        (exchange_nado.py:1416-1418).
  N8  exit fill truth — FlatResult.exit_avg_px = matches-feed VWAP of our
                        close fills newer than the pre-close watermark, or
                        None. Never mark_price (raw market_close avgPx=mark).
  N9  trigger_sl      — read-error during live-confirm is WriteUnconfirmed
                        (may_have_landed=True), DISTINCT from positive
                        absence; definitive rejects (2064 reduce-only, min
                        size x10 NAKED class) -> VenueRejected(reason).
                        F6: the confirm matches trigger PX within one tick
                        and requires a UNIQUE match (HL exactly-1 law) — a
                        same-side same-size foreign trigger at another px
                        (manual SL) can never be adopted as ours.
  N10 cancel_sl_order — cancel + re-list until the digest is ABSENT;
                        venue 2020 not-found = idempotent success; list
                        read failure or still-live -> WriteUnconfirmed.
  N11 user_fills      — composed from the per-product matches feed over the
                        open-position pids + session-touched pids,
                        verified-or-raise (never the hardcoded []).
  N12 position_liq    — summary read failure / missing healths / parse fail
                        -> ReadUnknown; None ONLY for a confirmed-flat coin.
  N13 limit_reduce_only — same error split as N9 -> OpenOrderInfo confirmed
                        live (nado TP = reduce-only price trigger, live
                        mechanics unchanged).
  update_leverage     — documented no-op (no venue knob, census conforming);
                        requests above the product max raise VenueRejected
                        (definitive: the venue cannot grant it).

Import purity: stdlib + fleet_core.exchange_api only at module import;
pandas under TYPE_CHECKING; bot/SDK inside functions.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

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

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

log = logging.getLogger(__name__)

VENUE = "nado"

__all__ = ["NadoExchangeClient", "build_client", "get_client"]


# ---------------------------------------------------------------------------
# error-string classification (the raw adapter funnels EVERYTHING into
# ok-shaped dicts whose only signal is the message text — census N6/N9/N13)
# ---------------------------------------------------------------------------

# harness integrity markers — never swallow into venue-error types
_HARNESS_PAT = ("unintercepted", "timeout-law", "unboundedcalldetected",
                "blocked by conformance harness")

_RATE_PAT = ("rate limit", "ratelimit", "too many request")
#: "429" only as a standalone NUMBER — raw/SDK error messages embed request
#: URLs/timestamps whose digit runs contain "429" wall-clock-dependently
#: (pacifica flake class, root-caused 2026-07-03; fixed on ALL venues).
_RE_429 = re.compile(r"(?<!\d)429(?!\d)")

# definitive venue rejects: the raw adapter's own local reject strings plus
# any venue failure BODY (the SDK raises with the JSON body text, which
# always carries "error_code") — the venue ANSWERED, state is unchanged.
_REJECT_PAT = ("below_min_size", "trigger_below_min_size",
               "reduce only order increases", "reduce_only_would_increase",
               "immediately_marketable", "empty asks", "empty bids",
               "unknown product", "unknown nado symbol", "insane price guard",
               "error_code")


def _is_harness_err(msg: str) -> bool:
    m = msg.lower()
    return any(p in m for p in _HARNESS_PAT)


def _is_rate_err(msg: str) -> bool:
    m = msg.lower()
    return any(p in m for p in _RATE_PAT) or bool(_RE_429.search(m))


def _classify_write_err(msg: str) -> str:
    """'harness' | 'reject' (definitive, nothing landed) | 'ambiguous'."""
    m = (msg or "").lower()
    if _is_harness_err(m):
        return "harness"
    for p in _REJECT_PAT:
        if p in m:
            return "reject"
    return "ambiguous"


def _mod(raw: Any):
    """The raw adapter's defining module (bot.exchange_nado) — reuse its
    canonical helpers (_from_x18, MARK_SANITY_MAX, _valid_x18_side, X18,
    TF_MS) without importing bot.* at module import time."""
    return sys.modules[type(raw).__module__]


class NadoExchangeClient(ExchangeClient):
    """Conforming Nado binding over the untouched raw adapter."""

    SUMMARY_TTL = 2.0            # serve-fresh window for the account summary
    MARK_TTL = 5.0               # default mark cache window (== raw ttl)
    POS_STALE_BOUND = float(
        os.getenv("NADO_POSITIONS_STALE_BOUND_SEC", "30") or 30.0)
    READ_ATTEMPTS = 3            # bounded retry budget for verified reads
    READ_RETRY_SLEEP = 0.05
    CLOSE_ATTEMPTS = 3           # ensure_flat close->readback loop
    CONFIRM_ATTEMPTS = 3         # gone/live listing confirm loop
    FILL_READBACK_ATTEMPTS = 4   # ambiguous market_open venue-truth readback

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        # never inherit the raw adapter's possibly-stale caches (they mask by
        # age; the wrapper owns freshness): drop them once at construction.
        try:
            raw.invalidate_positions_cache()
            raw._mark_cache = None
            raw._fills_cache = None
            raw._candles_cache.clear()
        except Exception:
            pass
        self._summary_cache: Optional[Tuple[float, Any]] = None
        self._mark_cache: Dict[str, Tuple[float, float]] = {}
        self._touched_pids: set = set()   # pids traded this session (N11)
        self._sdk_id = id(getattr(raw, "_sdk", None))
        # F2 DRY law: the nado raw adapter has NO exchange-layer DRY gate
        # (trader-level guards only) — the wrapper enforces it: under
        # DRY_RUN=1 no write leaves this process (fail-closed
        # WriteUnconfirmed(may_have_landed=False)).
        _dry = getattr(getattr(raw, "settings", None), "dry_run", None)
        if _dry is None:
            _dry = os.getenv("DRY_RUN", "0")
        try:
            self._dry = bool(int(str(_dry)))
        except (TypeError, ValueError):
            self._dry = bool(_dry)
        if self._dry:
            log.warning("NadoExchangeClient in DRY_RUN=1 — every write is "
                        "blocked at the wrapper (fail-closed)")
        # timeout-law floor at construction (see _sweep_untimed_sessions_fixed)
        self._sweep_untimed_sessions_fixed()

    @staticmethod
    def _sweep_untimed_sessions_fixed() -> None:
        """WRAPPER BUG FIX (P2 harness, timeout-law check): the adapter's
        _sweep_untimed_sessions class-guard is a NO-OP — it passes each found
        requests.Session INTO _install_session_timeout(sdk_obj=...), which
        then does getattr(sdk_obj, 'session', None) → a Session has no
        .session → 'SDK gc-sweep has no .session' WARNING and nothing gets
        guarded (the '%d untimed found+guarded' count tallies no-ops). Net
        effect on live: EVERY embedded SDK session (indexer candles/matches
        on archive.*, engine _querier reads) runs with NO timeout — audit
        defect #0, the exact class the conformance timeout law exists for
        (proven by harness_timeout_law_every_call_bounded: 8 unbounded
        candles calls). _guard_sessions delegated to that broken sweep, so
        the wrapper's own 'no session runs unbounded' contract was broken.
        This sweep does the same walk but installs the timeout ON THE
        SESSION, correctly. Env knob mirrors the adapter's
        (NADO_SDK_READ_TIMEOUT; connect fixed at 5s, read default 20s)."""
        import gc  # noqa: PLC0415

        try:
            import requests as _rq  # noqa: PLC0415
        except ImportError:  # pragma: no cover
            return
        try:
            read_to = float(os.getenv("NADO_SDK_READ_TIMEOUT", "20"))
        except (TypeError, ValueError):
            read_to = 20.0
        guarded = 0
        for obj in gc.get_objects():
            try:
                if not isinstance(obj, _rq.Session):
                    continue
                if getattr(obj, "_nado_timeout_installed", False) or \
                        getattr(obj, "_fc_timeout_installed", False):
                    continue
                _orig = obj.request

                def _request(method, url, *a, __orig=_orig, **kw):
                    if kw.get("timeout") is None:
                        kw["timeout"] = (5.0, read_to)
                    return __orig(method, url, *a, **kw)

                obj.request = _request
                obj._fc_timeout_installed = True
                guarded += 1
            except Exception:  # noqa: BLE001
                continue
        if guarded:
            log.warning(
                "wrapper session-sweep: %d untimed requests.Session "
                "guarded with (5, %.0fs) default timeout (adapter gc-sweep "
                "is a no-op — see _sweep_untimed_sessions_fixed)",
                guarded, read_to)

    # ------------------------------------------------------------------ util

    @property
    def raw(self) -> Any:
        """The underlying bot.exchange_nado.NadoClient_ (forensics only)."""
        return self._raw

    def _guard_sessions(self) -> None:
        """N0: the trigger-heal path rebuilds the SDK in-place with FRESH
        requests.Sessions; re-run the adapter's gc timeout sweep when the SDK
        object identity changed so no session runs unbounded."""
        sdk = getattr(self._raw, "_sdk", None)
        if id(sdk) != self._sdk_id:
            try:
                _mod(self._raw)._sweep_untimed_sessions()
                # the adapter sweep is a proven no-op (double .session
                # getattr) — run the CORRECT wrapper sweep as well
                self._sweep_untimed_sessions_fixed()
            except Exception as e:  # noqa: BLE001
                # F7c: the sweep failing must never be SILENT — an unguarded
                # fresh session runs unbounded (audit defect #0 class).
                # Log ERROR and continue (belt-and-suspenders layer; the
                # adapter's constructor-time installers remain in force).
                self._reraise_harness(e)
                log.error(
                    "N0 session-timeout sweep FAILED after in-place SDK "
                    "rebuild — fresh sessions may run UNBOUNDED until the "
                    "next successful sweep: %s", e)
            self._sdk_id = id(sdk)

    def _reraise_harness(self, exc: BaseException) -> None:
        """AssertionError = harness/contract violation (UninterceptedRealCall,
        UnboundedCallDetected) — never a venue error; always propagate."""
        if isinstance(exc, AssertionError):
            raise exc

    def _dry_block(self, op: str, coin: str) -> None:
        """F2: under DRY_RUN=1 a write must never reach the transport."""
        if self._dry:
            raise WriteUnconfirmed(
                "[DRY] %s(%s) blocked at the exchange wrapper — nothing "
                "signed, nothing sent" % (op, coin),
                may_have_landed=False, venue=VENUE, op=op, coin=coin)

    def _typed_read_error(self, op: str, exc: Optional[BaseException],
                          coin: str = "") -> ExchangeError:
        msg = str(exc) if exc is not None else "unknown failure"
        if _is_rate_err(msg):
            return RateLimited("%s: 429 retry budget exhausted: %s"
                               % (op, msg), venue=VENUE, op=op, coin=coin)
        return ReadUnknown("%s failed: %s" % (op, msg),
                           venue=VENUE, op=op, coin=coin)

    def _read_retry(self, op: str, fn, coin: str = "") -> Any:
        """Bounded verified-read attempt loop. Any exception is retried
        (idempotent reads) up to READ_ATTEMPTS, then surfaced TYPED."""
        last: Optional[BaseException] = None
        for i in range(self.READ_ATTEMPTS):
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 — typed at the boundary
                self._reraise_harness(e)
                last = e
                if i < self.READ_ATTEMPTS - 1:
                    time.sleep(self.READ_RETRY_SLEEP * (i + 1))
        raise self._typed_read_error(op, last, coin=coin)

    def _pid(self, coin: str, op: str) -> int:
        try:
            return self._raw._pid(coin)  # noqa: SLF001
        except KeyError:
            raise ValueError("%s: unknown Nado symbol %r (caller bug)"
                             % (op, coin))

    @staticmethod
    def _first_status(resp: Any) -> Dict[str, Any]:
        """First HL-shaped status dict out of a raw-adapter response."""
        try:
            sts = resp["response"]["data"]["statuses"]
            if sts and isinstance(sts[0], dict):
                return sts[0]
        except Exception:
            pass
        return {"error": "unparseable adapter response: %.200r" % (resp,)}

    # ---------------------------------------------------------- summary reads

    def _fetch_summary(self) -> Any:
        raw = self._raw

        def _do():
            s = raw._sdk.subaccount.get_engine_subaccount_summary(  # noqa: SLF001
                subaccount=raw.subaccount_hex)
            now = time.time()
            self._summary_cache = (now, s)
            # keep the raw adapter's cache coherent (its own methods stay
            # truthful if anything else reads them between wrapper calls)
            try:
                raw._summary_cache = (now, s)  # noqa: SLF001
            except Exception:
                pass
            return s

        return self._read_retry("summary", _do)

    def _summary_verified(self, op: str,
                          stale_bound: Optional[float] = None) -> Any:
        """Fresh-or-typed summary. stale_bound (positions only, N1) permits a
        BOUNDED bias-to-present serve of the last good summary on fetch
        failure; beyond the bound raises StaleData, never a silent serve."""
        now = time.time()
        c = self._summary_cache
        if c is not None and now - c[0] <= self.SUMMARY_TTL:
            return c[1]
        try:
            return self._fetch_summary()
        except ExchangeError as e:
            if c is not None and stale_bound is not None:
                age = time.time() - c[0]
                if age <= stale_bound:
                    return c[1]
                raise StaleData(
                    "%s: fetch failed and cache is %.1fs old (bound %.1fs): %s"
                    % (op, age, stale_bound, e), age_sec=age,
                    tolerance_sec=stale_bound, venue=VENUE, op=op)
            raise

    def _parse_positions(self, s: Any) -> Dict[str, PositionInfo]:
        """Per-item-corrupt law: ONE unparseable row poisons the WHOLE read."""
        raw = self._raw
        from_x18 = _mod(raw)._from_x18
        out: Dict[str, PositionInfo] = {}
        for b in (getattr(s, "perp_balances", None) or []):
            try:
                amount = from_x18(b.balance.amount)
                if abs(amount) < 1e-12:
                    continue
                pid = int(b.product_id)
                sym = raw._pid_to_symbol.get(pid, "PID_%d" % pid)  # noqa: SLF001
                v_quote = from_x18(b.balance.v_quote_balance)
                entry_px = abs(v_quote / amount)
                if not entry_px > 0.0:
                    raise ValueError("entry_px %r not > 0" % entry_px)
                out[sym] = PositionInfo(
                    coin=sym, size_signed=float(amount),
                    entry_px=float(entry_px),
                    raw={"product_id": pid, "amount": str(b.balance.amount),
                         "v_quote_balance": str(b.balance.v_quote_balance)})
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "open_positions: perp balance row unparseable — aborting "
                    "the WHOLE read (positions UNKNOWN, item NOT dropped): %s"
                    % e, venue=VENUE, op="open_positions")
        return out

    # ---------------------------------------------------------------- reads

    def open_positions(self) -> Mapping[str, PositionInfo]:
        s = self._summary_verified("open_positions",
                                   stale_bound=self.POS_STALE_BOUND)
        return self._parse_positions(s)

    def _open_positions_force(self) -> Dict[str, PositionInfo]:
        """Cache-bypassing snapshot (write readbacks): fresh fetch or raise."""
        self.invalidate_positions_cache()
        return self._parse_positions(self._fetch_summary())

    def account_value(self) -> float:
        """Nado sizing basis = healths[0] (INITIAL health / funds available)
        — deliberately != equity_with_upnl; P2 changes NO risk numbers."""
        s = self._summary_verified("account_value")
        try:
            return float(_mod(self._raw)._from_x18(s.healths[0].health))
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            raise ReadUnknown("account_value: healths[0] unparseable: %s" % e,
                              venue=VENUE, op="account_value")

    def _equity_and_margin(self, op: str) -> Tuple[float, float]:
        """(equity_with_upnl, margin_used) from the verified summary.
        Canonical live formulas (== SDK MarginManager, live-verified
        2026-06-11: portfolio_value == unweighted health = healths[2];
        initial_margin_used = unweighted - initial = healths[2]-healths[0])."""
        s = self._summary_verified(op)
        try:
            from_x18 = _mod(self._raw)._from_x18
            h = s.healths
            if h is None or len(h) < 3:
                raise ValueError("healths missing/short: %r" % (h,))
            h0 = float(from_x18(h[0].health))
            h2 = float(from_x18(h[2].health))
            return h2, h2 - h0
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            raise ReadUnknown("%s: healths unparseable: %s" % (op, e),
                              venue=VENUE, op=op)

    def equity_with_upnl(self) -> float:
        return self._equity_and_margin("equity_with_upnl")[0]

    def margin_used_usd(self) -> float:
        return self._equity_and_margin("margin_used_usd")[1]

    def position_liquidation(self, coin: str) -> Optional[float]:
        """Marginal liq px (cross health model, same math the raw adapter
        uses) — but a FAILED read is ReadUnknown, never None (N12)."""
        s = self._summary_verified("position_liquidation")
        raw = self._raw
        from_x18 = _mod(raw)._from_x18
        try:
            healths = s.healths
            if healths is None or len(healths) < 2:
                raise ValueError("healths missing/short")
            h1 = float(from_x18(healths[1].health))
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            raise ReadUnknown(
                "position_liquidation(%s): maintenance health unparseable: %s"
                % (coin, e), venue=VENUE, op="position_liquidation", coin=coin)
        for b in (getattr(s, "perp_balances", None) or []):
            try:
                pid = int(b.product_id)
                sym = raw._pid_to_symbol.get(pid, "PID_%d" % pid)  # noqa: SLF001
                if sym != coin:
                    continue
                amt = from_x18(b.balance.amount)
                if abs(amt) < 1e-12:
                    return None  # zero-size row = positively flat
                v_quote = from_x18(b.balance.v_quote_balance)
                entry_px = abs(v_quote / amt)
                if amt > 0:
                    liq = entry_px - h1 / amt
                else:
                    liq = entry_px + h1 / abs(amt)
                return float(max(liq, 0.0))
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "position_liquidation(%s): balance row unparseable: %s"
                    % (coin, e), venue=VENUE, op="position_liquidation",
                    coin=coin)
        return None  # verified summary, coin absent -> POSITIVELY no position

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        pid = self._pid(coin, "mark_price")
        raw = self._raw
        m = _mod(raw)
        now = time.time()
        cached = self._mark_cache.get(coin)
        if cached is not None and now - cached[1] <= min(max_age_sec,
                                                         self.MARK_TTL):
            return cached[0]

        def _do() -> float:
            mp = raw._sdk.context.engine_client.get_market_price(pid)  # noqa: SLF001
            bid = m._valid_x18_side(getattr(mp, "bid_x18", None))
            ask = m._valid_x18_side(getattr(mp, "ask_x18", None))
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
            else:
                # empty/one-sided book -> indexer oracle mark (never 0.0)
                pp = raw._sdk.context.indexer_client.get_perp_prices(pid)  # noqa: SLF001
                mid = None
                for attr in ("mark_price_x18", "index_price_x18"):
                    v = m._valid_x18_side(getattr(pp, attr, None))
                    if v is not None:
                        mid = v
                        break
                if mid is None:
                    raise ValueError("no valid book side and no oracle mark")
            if not (0.0 < mid < m.MARK_SANITY_MAX):
                raise ValueError(
                    "insane mark %.6g (empty-book sentinel / scaling bug)"
                    % mid)
            return float(mid)

        try:
            px = self._read_retry("mark_price", _do, coin=coin)
        except ExchangeError:
            if cached is not None:
                age = time.time() - cached[1]
                if age <= max_age_sec:
                    return cached[0]
                raise StaleData(
                    "mark_price(%s): only a %.3fs-old cached mark available "
                    "(tolerance %.3fs)" % (coin, age, max_age_sec),
                    age_sec=age, tolerance_sec=max_age_sec,
                    venue=VENUE, op="mark_price", coin=coin)
            raise
        self._mark_cache[coin] = (px, time.time())
        return px

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> "pd.DataFrame":
        raw = self._raw
        tf_ms_map = _mod(raw).TF_MS
        try:
            df = raw.candles(coin, interval, limit=limit)
        except ValueError:
            raise  # unsupported interval — caller bug, propagate as-is
        except KeyError as e:
            raise ValueError("candles: unknown Nado symbol %s (caller bug)"
                             % e)
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            raise self._typed_read_error("candles", e, coin=coin)
        if df is None or len(df) == 0:
            # the raw adapter masks fetch failure AND venue-empty into an
            # empty frame — both are UNKNOWN here (N4)
            raise ReadUnknown(
                "candles(%s,%s): no verified bars (fetch failed or venue "
                "returned empty)" % (coin, interval),
                venue=VENUE, op="candles", coin=coin)
        tf_ms = tf_ms_map.get(interval)
        if tf_ms is None:
            raise ValueError("candles: interval %r has no TF_MS entry "
                             "(caller bug)" % interval)
        now_ms = int(time.time() * 1000)
        boundary = (now_ms // tf_ms) * tf_ms
        try:
            last_open_ms = int(df["time"].iloc[-1].value // 10**6)
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            raise ReadUnknown("candles(%s,%s): time column unparseable: %s"
                              % (coin, interval, e), venue=VENUE,
                              op="candles", coin=coin)
        lag_bars = (boundary - (last_open_ms + tf_ms)) / float(tf_ms)
        if lag_bars > max_stale_bars:
            raise StaleData(
                "candles(%s,%s): last closed bar lags %.2f bars "
                "(tolerance %.2f) — 09:00-missing-bar class" %
                (coin, interval, lag_bars, max_stale_bars),
                age_sec=lag_bars * tf_ms / 1000.0,
                tolerance_sec=max_stale_bars * tf_ms / 1000.0,
                venue=VENUE, op="candles", coin=coin)
        return df

    # ------------------------------------------------------- order-book reads

    def _fetch_triggers_ccxt(self, op: str) -> List[Dict[str, Any]]:
        """Verified trigger-service listing (raises typed). The raw method
        already raises on query failure (XNN-canon); the heal path may
        rebuild the SDK -> re-run the timeout sweep after (N0)."""
        raw = self._raw
        try:
            out = self._read_retry(op, raw.fetch_open_orders_ccxt_shape)
        finally:
            self._guard_sessions()
        return out

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        """Engine open orders MERGED with trigger-service orders (N5)."""
        raw = self._raw
        from_x18 = _mod(raw)._from_x18

        def _engine_leg():
            perp_pids = sorted({m.product_id
                                for m in raw._meta_cache.values()})  # noqa: SLF001
            if not perp_pids:
                perp_pids = list(raw._pid_to_symbol.keys())  # noqa: SLF001
            return raw._sdk.context.engine_client.\
                get_subaccount_multi_products_open_orders(  # noqa: SLF001
                    product_ids=perp_pids, sender=raw.subaccount_hex)

        resp = self._read_retry("open_orders", _engine_leg)
        out: List[OpenOrderInfo] = []
        entries = getattr(resp, "product_orders", None) or []
        for entry in entries:
            pid = int(entry.product_id)
            sym = raw._pid_to_symbol.get(pid, "PID_%d" % pid)  # noqa: SLF001
            for o in (entry.orders or []):
                try:
                    amt = from_x18(o.amount)
                    oid = str(getattr(o, "digest", "") or "")
                    if not oid:
                        raise ValueError("order row without digest")
                    out.append(OpenOrderInfo(
                        coin=sym, oid=oid,
                        side="buy" if amt > 0 else "sell",
                        size=abs(float(amt)),
                        limit_px=float(from_x18(o.price_x18))
                        if getattr(o, "price_x18", None) is not None else None,
                        trigger_px=None, reduce_only=False, is_trigger=False,
                        raw={"product_id": pid}))
                except Exception as e:  # noqa: BLE001
                    self._reraise_harness(e)
                    raise ReadUnknown(
                        "open_orders: engine order row unparseable — aborting "
                        "the WHOLE read (one invisible order is the "
                        "false-naked class): %s" % e,
                        venue=VENUE, op="open_orders")
        # trigger leg (raw omits it entirely — census N5)
        for t in self._fetch_triggers_ccxt("open_orders"):
            dg = (t.get("info") or {}).get("digest")
            if not dg:
                continue  # id-less trigger cannot be cancelled — harmless
            try:
                out.append(OpenOrderInfo(
                    coin=str(t.get("coin") or t.get("symbol")),
                    oid=str(dg),
                    side=str(t.get("side") or "").lower(),
                    size=abs(float(t.get("amount", 0.0) or 0.0)),
                    limit_px=None, trigger_px=None,  # not exposed by feed
                    reduce_only=bool(t.get("reduceOnly", True)),
                    is_trigger=True, raw=dict(t.get("info") or {})))
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "open_orders: trigger row unparseable — aborting the "
                    "WHOLE read: %s" % e, venue=VENUE, op="open_orders")
        return out

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        try:
            orders = self._fetch_triggers_ccxt("list_open_sl_orders")
        except ExchangeError as e:
            raise ReadUnknown(
                "list_open_sl_orders(%s): trigger-service read-back FAILED — "
                "SL liveness INDETERMINATE (not 'no SL'): %s" % (coin, e),
                venue=VENUE, op="list_open_sl_orders", coin=coin)
        out: List[str] = []
        for o in orders:
            if o.get("symbol") != coin and o.get("coin") != coin:
                continue
            t = str(o.get("type", "")).lower()
            info = o.get("info") or {}
            info_t = str(info.get("orderType", "")).lower()
            info_status = str(info.get("status", "")).lower()
            if ("trigger" in t or "stop" in t or "trigger" in info_t
                    or "stop" in info_t or "waiting" in info_status):
                oid = info.get("digest") or o.get("id")
                if oid:
                    out.append(str(oid))
        return out

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        out: List[OpenOrderInfo] = []
        for t in self._fetch_triggers_ccxt("list_reduce_only_triggers"):
            dg = (t.get("info") or {}).get("digest")
            if dg is None:
                continue  # id-less: cannot be cancelled anyway (census note)
            try:
                out.append(OpenOrderInfo(
                    coin=str(t.get("coin") or t.get("symbol")),
                    oid=str(dg),
                    side=str(t.get("side") or "").lower(),
                    size=abs(float(t.get("amount", 0.0) or 0.0)),
                    limit_px=None, trigger_px=None,
                    reduce_only=bool(t.get("reduceOnly", True)),
                    is_trigger=True, raw=dict(t.get("info") or {})))
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "list_reduce_only_triggers: row unparseable — aborting "
                    "(the sweep CANCELS on this listing): %s" % e,
                    venue=VENUE, op="list_reduce_only_triggers")
        return out

    # ------------------------------------------------------------ fills reads

    def _matches_verified(self, pid: int, limit: int = 100,
                          op: str = "user_fills") -> List[Dict[str, Any]]:
        """Verified per-product matches feed; per-item parse failure aborts
        the read (N11 + P10 law). ccxt-ish dicts, newest first."""
        raw = self._raw
        from_x18 = _mod(raw)._from_x18

        def _do():
            from nado_protocol.indexer_client.types.query import (
                IndexerMatchesParams,
            )
            return raw._sdk.context.indexer_client.get_matches(  # noqa: SLF001
                IndexerMatchesParams(subaccounts=[raw.subaccount_hex],
                                     product_ids=[pid], limit=limit))

        data = self._read_retry(op, _do)
        matches = getattr(data, "matches", None) or []
        sym = raw._pid_to_symbol.get(pid, "PID_%d" % pid)  # noqa: SLF001
        out: List[Dict[str, Any]] = []
        for m in matches:
            try:
                amount_x18 = int(m.order.amount)
                base_filled = from_x18(m.base_filled)
                quote_filled = from_x18(m.quote_filled)
                price = (abs(quote_filled / base_filled)
                         if abs(base_filled) > 1e-12 else 0.0)
                fee = from_x18(getattr(m, "fee", "0") or "0")
                sub_idx = int(getattr(m, "submission_idx", 0) or 0)
                out.append({
                    "symbol": sym, "coin": sym,
                    "side": "buy" if amount_x18 > 0 else "sell",
                    "amount": abs(base_filled), "price": price,
                    "timestamp": 0, "submission_idx": sub_idx,
                    "fee": {"cost": fee}, "info": {"product_id": pid},
                })
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "%s: match row unparseable (pid=%d) — aborting the read "
                    "(a persistently-unparseable fill must never be silently "
                    "invisible): %s" % (op, pid, e),
                    venue=VENUE, op=op)
        out.sort(key=lambda f: f["submission_idx"], reverse=True)
        return out

    def user_fills(self, max_age_sec: float = 60.0
                   ) -> Sequence[Mapping[str, Any]]:
        """Composed from the per-product matches feed over the open-position
        pids plus every pid this client traded this session (N11: the raw
        hardcoded [] was a permanent fake 'no fills' claim)."""
        pos = self.open_positions()  # verified — raises typed on failure
        pids = set(self._touched_pids)
        for p in pos.values():
            try:
                pids.add(int(p.raw["product_id"]))
            except Exception:
                sym = p.coin
                try:
                    pids.add(self._raw._pid(sym))  # noqa: SLF001
                except Exception:
                    continue
        fills: List[Dict[str, Any]] = []
        for pid in sorted(pids):
            fills.extend(self._matches_verified(pid, limit=100,
                                                op="user_fills"))
        fills.sort(key=lambda f: f["submission_idx"], reverse=True)
        return fills

    def _matches_watermark(self, pid: int) -> Optional[int]:
        """Highest submission_idx currently visible (pre-send watermark).
        -1 = feed POSITIVELY empty; None = unreadable (no attribution)."""
        try:
            rows = self._matches_verified(pid, limit=5, op="fill_watermark")
        except ExchangeError:
            return None
        if not rows:
            return -1
        return max(r["submission_idx"] for r in rows)

    # ---------------------------------------------------------------- writes

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        raw = self._raw
        try:
            pid = raw._pid(coin)  # noqa: SLF001
        except KeyError:
            raise VenueRejected("market_open: unknown Nado symbol %r" % coin,
                                reason="unknown_symbol", venue=VENUE,
                                op="market_open", coin=coin)
        if intended_px is not None and not allow_marketable:
            # marketability guard (wrapper-level; the raw adapter has none):
            # verified mark — ReadUnknown propagates, nothing sent.
            mark = self.mark_price(coin)
            if (is_buy and mark <= float(intended_px)) or \
               ((not is_buy) and mark >= float(intended_px)):
                raise VenueRejected(
                    "market_open(%s): level %.8g already through the book "
                    "(mark %.8g)" % (coin, float(intended_px), mark),
                    reason="immediately_marketable", venue=VENUE,
                    op="market_open", coin=coin)

        self._dry_block("market_open", coin)
        # soft pre-state for ambiguous-path attribution (never blocks entry)
        wm = self._matches_watermark(pid)
        try:
            baseline: Optional[Mapping[str, PositionInfo]] = \
                self.open_positions()
        except ExchangeError:
            baseline = None

        self._touched_pids.add(pid)
        try:
            resp = raw.market_open(coin, is_buy, sz)
        except Exception as e:  # raw catches internally; belt-and-suspenders
            self._reraise_harness(e)
            resp = {"response": {"data": {"statuses": [{"error": str(e)}]}}}

        st = self._first_status(resp)
        err = ""
        if "filled" in st:
            f = st["filled"] or {}
            try:
                avg = float(f.get("avgPx") or 0.0)
                size = float(f.get("totalSz") or 0.0)
            except (TypeError, ValueError):
                avg = size = 0.0
            if avg > 0.0 and size > 0.0:
                self.invalidate_positions_cache()
                return FillResult(coin=coin, is_buy=is_buy, avg_px=avg,
                                  size=size, requested_size=float(sz),
                                  oid=str(f.get("oid") or "") or None)
            err = "fill fields unparseable: %r" % (f,)
        elif "error" in st:
            err = str(st.get("error"))
            kind = _classify_write_err(err)
            if kind == "harness":
                raise AssertionError(err)
            if kind == "reject":
                raise VenueRejected(
                    "market_open(%s) rejected by venue: %s" % (coin, err),
                    reason=err, venue=VENUE, op="market_open", coin=coin)
        else:
            err = str(st)  # 'unconfirmed' path — venue-truth readback below

        # ambiguous: the order MAY have landed — venue-truth readback
        self.invalidate_positions_cache()
        fill, positive, landed = self._attribute_fill(
            pid, coin, is_buy, float(sz), wm, baseline)
        if fill is not None:
            return FillResult(coin=coin, is_buy=is_buy,
                              avg_px=float(fill["px"]),
                              size=float(fill["size"]),
                              requested_size=float(sz), oid=None)
        raise WriteUnconfirmed(
            "market_open(%s) ambiguous (%s) and venue-truth readback %s"
            % (coin, err,
               "shows NOTHING landed" if positive and not landed
               else ("shows a landed position delta but the fill px is "
                     "unattributable — reconcile before re-acting"
                     if landed else "itself FAILED")),
            may_have_landed=(True if landed else (False if positive else None)),
            venue=VENUE, op="market_open", coin=coin)

    def _attribute_fill(self, pid: int, coin: str, is_buy: bool, sz: float,
                        wm: Optional[int],
                        baseline: Optional[Mapping[str, PositionInfo]],
                        ) -> Tuple[Optional[Dict[str, float]], bool, bool]:
        """Venue-truth fill attribution: (fill|None, readback_positive,
        landed_but_unattributable). Matches feed (real VWAP) first, position
        delta second. Never echoes the request, never uses mark."""
        want_side = "buy" if is_buy else "sell"
        positive = False
        landed = False
        for i in range(self.FILL_READBACK_ATTEMPTS):
            if wm is not None:
                try:
                    rows = self._matches_verified(pid, limit=25,
                                                  op="fill_readback")
                    positive = True
                    cum = num = 0.0
                    for f in rows:
                        if f["side"] != want_side:
                            continue
                        if f["submission_idx"] <= wm:
                            continue
                        if f["amount"] <= 0 or f["price"] <= 0:
                            continue
                        cum += f["amount"]
                        num += f["amount"] * f["price"]
                    if cum >= sz * 0.95 or \
                            (i == self.FILL_READBACK_ATTEMPTS - 1 and cum > 0):
                        return ({"px": num / cum, "size": cum}, True, True)
                    if cum > 0:
                        landed = True  # partial visible; keep polling
                except ExchangeError:
                    pass
            if baseline is not None:
                try:
                    now_map = self._open_positions_force()
                    positive = True
                    pre = baseline.get(coin)
                    post = now_map.get(coin)
                    pre_sz = pre.size_signed if pre is not None else 0.0
                    post_sz = post.size_signed if post is not None else 0.0
                    delta = post_sz - pre_sz
                    good = delta > 1e-12 if is_buy else delta < -1e-12
                    if good:
                        landed = True
                        px = self._derive_fill_px(pre, post)
                        if px is not None and px > 0.0:
                            return ({"px": px, "size": abs(delta)},
                                    True, True)
                except ExchangeError:
                    pass
            if i < self.FILL_READBACK_ATTEMPTS - 1:
                time.sleep(0.15 * (i + 1))
        return (None, positive, landed)

    @staticmethod
    def _derive_fill_px(pre: Optional[PositionInfo],
                        post: Optional[PositionInfo]) -> Optional[float]:
        """Fill VWAP derivable from venue entry-price bookkeeping: a fresh
        position's entry IS the fill; a same-direction add back-solves the
        weighted average. Reduces don't move entry -> unattributable (None)."""
        if post is None:
            return None
        if pre is None or abs(pre.size_signed) < 1e-12:
            return post.entry_px
        same_dir = (pre.size_signed > 0) == (post.size_signed > 0)
        grew = abs(post.size_signed) > abs(pre.size_signed) + 1e-12
        if same_dir and grew:
            s1, s2 = abs(pre.size_signed), abs(post.size_signed)
            px = (post.entry_px * s2 - pre.entry_px * s1) / (s2 - s1)
            return px if px > 0.0 else None
        return None

    def ensure_flat(self, coin: str) -> FlatResult:
        """THE close primitive: close send + FORCED position readback loop.
        A failed confirming read is WriteUnconfirmed — never the raw
        market_close sentinel-(-1.0)-reads-as-success path (N7); exit px is
        matches-feed truth or None — never mark (N8)."""
        raw = self._raw
        try:
            pid = raw._pid(coin)  # noqa: SLF001
        except KeyError:
            raise ValueError("ensure_flat: unknown Nado symbol %r (caller "
                             "bug)" % coin)
        # initial read — failure here means NOTHING was sent (typed raise)
        pos_map = self.open_positions()
        pos = pos_map.get(coin)
        if pos is None:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        self._dry_block("ensure_flat", coin)
        total_to_close = abs(pos.size_signed)
        wm = self._matches_watermark(pid)  # exit-px attribution (soft)
        self._touched_pids.add(pid)
        sent_any = False
        attempts = 0
        for attempts in range(1, self.CLOSE_ATTEMPTS + 1):
            try:
                raw._sdk.market.close_position(raw.subaccount_hex, pid)  # noqa: SLF001
                sent_any = True
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                if _classify_write_err(str(e)) != "reject":
                    sent_any = True  # transport ambiguity: may have landed
            # FORCED position readback — the only source of flat truth
            try:
                now_map = self._open_positions_force()
            except ExchangeError as e:
                raise WriteUnconfirmed(
                    "ensure_flat(%s): close attempt %d %s but the confirming "
                    "position readback FAILED — flatness UNPROVEN (never "
                    "reported as closed): %s"
                    % (coin, attempts,
                       "sent" if sent_any else "not-proven-sent", e),
                    may_have_landed=sent_any, venue=VENUE,
                    op="ensure_flat", coin=coin)
            if coin not in now_map:
                return FlatResult(
                    coin=coin, already_flat=False,
                    closed_size=total_to_close,
                    exit_avg_px=self._exit_px_from_matches(
                        pid, wm, close_is_buy=(pos.size_signed < 0)),
                    attempts=attempts)
            if attempts < self.CLOSE_ATTEMPTS:
                time.sleep(0.2 * attempts)
        residual = None
        try:
            residual = abs(self._open_positions_force()[coin].size_signed)
        except Exception as e:  # noqa: BLE001 — diagnostic only
            self._reraise_harness(e)
        raise WriteUnconfirmed(
            "ensure_flat(%s): %d close attempts exhausted, position still "
            "open (residual %s) — NAKED-position branch, escalate"
            % (coin, attempts, residual),
            may_have_landed=sent_any, venue=VENUE, op="ensure_flat",
            coin=coin)

    def _exit_px_from_matches(self, pid: int, wm: Optional[int],
                              close_is_buy: bool) -> Optional[float]:
        """Real close-fill VWAP (fills strictly newer than the pre-close
        watermark, our close side) or None — NEVER mark/SL reference."""
        if wm is None:
            return None
        want = "buy" if close_is_buy else "sell"
        try:
            rows = self._matches_verified(pid, limit=25, op="exit_readback")
        except ExchangeError:
            return None
        cum = num = 0.0
        for f in rows:
            if f["side"] != want or f["submission_idx"] <= wm:
                continue
            if f["amount"] <= 0 or f["price"] <= 0:
                continue
            cum += f["amount"]
            num += f["amount"] * f["price"]
        return (num / cum) if cum > 0 else None

    # ------------------------------------------------------------ SL triggers

    def _snap_size(self, coin: str, sz: float) -> float:
        raw = self._raw
        X18 = _mod(raw).X18
        meta = raw.asset(coin)
        inc_x18 = meta.size_increment_x18 or \
            int(round(raw._size_increment_for(coin) * X18))  # noqa: SLF001
        return ((int(round(sz * X18)) // inc_x18) * inc_x18) / X18

    def _align_px(self, coin: str, px: float) -> float:
        raw = self._raw
        X18 = _mod(raw).X18
        tick = raw.asset(coin).tick_size or 0.0001
        tick_x18 = max(int(round(tick * X18)), 1)
        return ((int(round(px * X18)) // tick_x18) * tick_x18) / X18

    #: raw trigger_sl / limit_reduce_only hardcoded worse-than-trigger limit
    #: buffer (exchange_nado.py:1511/:1572) — used to derive the expected
    #: inner-order limit px for px matching against the live listing.
    _RAW_TRIGGER_LIMIT_SLIP = 0.005

    def _fetch_trigger_rows_px(self, op: str) -> List[Dict[str, Any]]:
        """Wrapper-owned ACTIVE trigger listing WITH the price field (F6).

        raw.fetch_open_orders_ccxt_shape DROPS every price, which is exactly
        why the old shape-only confirm could adopt a same-side same-size
        FOREIGN trigger. Same trigger-service query the raw performs; rows:
        {coin, side, reduce_only, amount, digest, px} where px is the row's
        order price (the harness wire serves the trigger px; the live wire
        serves the slip-adjusted inner limit px — the caller matches BOTH
        targets). Per-item parse failure aborts the read (per-item law)."""
        raw = self._raw

        def _do():
            from nado_protocol.trigger_client.types.query import (
                ListTriggerOrdersParams,
                ListTriggerOrdersTx,
            )
            tc = raw._ensure_trigger_client()  # noqa: SLF001
            tx = ListTriggerOrdersTx(
                sender=raw.subaccount_hex,
                recvTime=int(time.time() * 1000) + 60_000)
            return tc.list_trigger_orders(ListTriggerOrdersParams(
                tx=tx, limit=100,
                status_types=["waiting_price", "waiting_dependency",
                              "triggering"]))

        try:
            resp = self._read_retry(op, _do)
        finally:
            self._guard_sessions()
        data = resp.data if hasattr(resp, "data") else resp
        from_x18 = _mod(raw)._from_x18
        out: List[Dict[str, Any]] = []
        for t in (getattr(data, "orders", None) or []):
            try:
                status = str(getattr(t, "status", "")).lower()
                if any(b in status for b in ("cancelled", "triggered",
                                             "error", "executing",
                                             "completed")):
                    continue
                od = t.order
                pid = int(od.product_id)
                sym = raw._pid_to_symbol.get(pid, "PID_%d" % pid)  # noqa: SLF001
                amt = from_x18(od.order.amount)
                # WRAPPER BUG FIX (P2 host run, SDK schema proof): the
                # trigger-client OrderData spells the price field priceX18
                # (EIP-712 camelCase — sender/priceX18/amount/expiration/
                # nonce), unlike the ENGINE OrderData's snake price_x18
                # (:593). Reading price_x18 here returned None for EVERY
                # live trigger row, so the F6 px-match could never succeed
                # and each trigger_sl/limit_reduce_only placement raised
                # WriteUnconfirmed on a correctly-placed trigger.
                px_raw = getattr(od.order, "priceX18",
                                 getattr(od.order, "price_x18", None))
                px = float(from_x18(px_raw)) if px_raw is not None else None
                out.append({
                    "coin": sym,
                    "side": "buy" if amt > 0 else "sell",
                    "reduce_only": True,
                    "amount": abs(float(amt)),
                    "digest": getattr(od, "digest", None),
                    "px": px,
                })
            except Exception as e:  # noqa: BLE001
                self._reraise_harness(e)
                raise ReadUnknown(
                    "%s: trigger row unparseable — aborting the WHOLE read "
                    "(per-item law): %s" % (op, e), venue=VENUE, op=op)
        return out

    def _trigger_px_targets(self, coin: str, is_buy: bool,
                            ref_px: float) -> Tuple[float, float]:
        """(aligned trigger px, aligned raw-derived inner limit px) — the two
        prices a live row belonging to OUR placement can legally carry."""
        slip = self._RAW_TRIGGER_LIMIT_SLIP
        limit = ref_px * (1 + slip) if is_buy else ref_px * (1 - slip)
        return (self._align_px(coin, ref_px), self._align_px(coin, limit))

    def _confirm_trigger(self, coin: str, is_buy: bool, sz: float,
                         ref_px: float) -> Optional[str]:
        """N9 split + F6 exactly-1 law (mirrors HL _trigger_from_listing_match):
        a candidate must match side + reduce-only + size (increment tol) AND
        price WITHIN ONE TICK of our placement (trigger px or the raw-derived
        limit px) — and the match must be UNIQUE. A same-side same-size
        foreign trigger at a different px (manual SL / stale trail) must
        never be adopted: cancelling it later strips the foreign protection
        (naked class). >1 px-matching candidates -> WriteUnconfirmed (refuse
        to guess ownership). Read-error during confirm -> WriteUnconfirmed
        (may_have_landed=True, no blind retry); positive absence -> None."""
        want_side = "buy" if is_buy else "sell"
        raw = self._raw
        try:
            meta = raw.asset(coin)
            inc = (meta.size_increment_x18 / _mod(raw).X18) \
                if meta.size_increment_x18 else \
                raw._size_increment_for(coin)  # noqa: SLF001
            sz_tol = max(inc, abs(sz) * 1e-6)
            tick = float(meta.tick_size or 0.0001)
        except Exception as e:  # noqa: BLE001
            self._reraise_harness(e)
            sz_tol = max(abs(sz) * 1e-3, 1e-9)
            tick = max(abs(ref_px) * 1e-4, 1e-9)
        px_tol = tick * (1.0 + 1e-9) + 1e-12  # ONE tick (float-safe)
        targets = self._trigger_px_targets(coin, is_buy, ref_px)
        read_err: Optional[ExchangeError] = None
        for i in range(self.CONFIRM_ATTEMPTS):
            try:
                live = self._fetch_trigger_rows_px("trigger_confirm")
                read_err = None
            except ExchangeError as e:
                read_err = e
                if i < self.CONFIRM_ATTEMPTS - 1:
                    time.sleep(0.15 * (i + 1))
                continue
            matches: List[str] = []
            for o in live:
                if o["coin"] != coin or o["side"] != want_side:
                    continue
                if not o["reduce_only"]:
                    continue
                if abs(o["amount"] - abs(sz)) > sz_tol:
                    continue
                px = o.get("px")
                if px is None or \
                        not any(abs(px - t) <= px_tol for t in targets):
                    continue  # F6: px must match within ONE tick
                if o.get("digest"):
                    matches.append(str(o["digest"]))
            uniq = sorted(set(matches))
            if len(uniq) == 1:
                return uniq[0]
            if len(uniq) > 1:
                raise WriteUnconfirmed(
                    "trigger confirm for %s: %d px-matching live triggers — "
                    "cannot uniquely identify OURS (exactly-1 law; adopting "
                    "a foreign trigger is the cancel-the-manual-SL naked "
                    "class)" % (coin, len(uniq)),
                    may_have_landed=True, venue=VENUE, op="trigger_confirm",
                    coin=coin)
            if i < self.CONFIRM_ATTEMPTS - 1:
                time.sleep(0.15 * (i + 1))
        if read_err is not None:
            raise WriteUnconfirmed(
                "trigger confirm read-back FAILED for %s — the trigger MAY be "
                "live (read error != absence; blind re-place is the "
                "duplicate-SL-stack class): %s" % (coin, read_err),
                may_have_landed=True, venue=VENUE, op="trigger_confirm",
                coin=coin)
        return None

    def _place_trigger_common(self, op: str, coin: str, is_buy: bool,
                              sz: float, ref_px: float, raw_call) -> str:
        """Shared trigger_sl / limit_reduce_only placement: raw call ->
        error-shape split (N9/N13) -> px-verified UNIQUE live digest or typed
        raise (F6 exactly-1 law).

        The raw adapter's own _confirm_trigger_live is px-BLIND (the ccxt
        listing drops prices), so even its 'resting' digest could name a
        same-side same-size FOREIGN trigger — the wrapper therefore ALWAYS
        re-confirms via the px-aware listing and only hands back a digest
        whose price matches our placement within one tick, uniquely."""
        self._dry_block(op, coin)
        try:
            resp = raw_call()
        except Exception as e:  # raw catches internally; defensive
            self._reraise_harness(e)
            resp = {"response": {"data": {"statuses": [{"error": str(e)}]}}}
        st = self._first_status(resp)
        raw_oid: Optional[str] = None
        err = ""
        if "resting" in st:
            raw_oid = str((st.get("resting") or {}).get("oid") or "") or None
            if raw_oid is None:
                err = "resting status without oid"
        else:
            err = str(st.get("error") or st)
            kind = _classify_write_err(err)
            if kind == "harness":
                raise AssertionError(err)
            if kind == "reject":
                raise VenueRejected(
                    "%s(%s) rejected by venue: %s" % (op, coin, err),
                    reason=err, venue=VENUE, op=op, coin=coin)
        # px-verified unique confirm (F6) — runs on the happy path too: the
        # raw 'resting' digest is trusted only when it IS the px-matching
        # live trigger.
        snapped = self._snap_size(coin, sz) if sz > 0 else sz
        found = self._confirm_trigger(coin, is_buy, snapped or sz, ref_px)
        if found:
            if raw_oid is not None and found.lower() != raw_oid.lower():
                log.warning(
                    "%s(%s): raw-confirmed digest %s is NOT the px-matching "
                    "live trigger %s — the raw px-blind shape-match adopted "
                    "a foreign trigger; using the px-verified digest",
                    op, coin, raw_oid, found)
            self.invalidate_positions_cache()
            return found
        raise WriteUnconfirmed(
            "%s(%s): no px-matching live trigger after read-back (%s) — "
            "NOT placed (naked-success class killed)"
            % (op, coin, err or "raw acked %s" % raw_oid),
            may_have_landed=True, venue=VENUE, op=op, coin=coin)

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        raw = self._raw
        oid = self._place_trigger_common(
            "trigger_sl", coin, is_buy, sz, trigger_px,
            lambda: raw.trigger_sl(coin, is_buy, sz, trigger_px))
        return SLOrderInfo(
            coin=coin, oid=oid,
            trigger_px=self._align_px(coin, trigger_px),
            size=self._snap_size(coin, sz),
            is_buy_to_close=bool(is_buy))

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        """Nado TP1 = reduce-only price trigger on the profit side (the raw
        adapter's live mechanics, unchanged) — confirmed live or raise."""
        raw = self._raw
        oid = self._place_trigger_common(
            "limit_reduce_only", coin, is_buy, sz, px,
            lambda: raw.limit_reduce_only(coin, is_buy, sz, px))
        return OpenOrderInfo(
            coin=coin, oid=oid,
            side="buy" if is_buy else "sell",
            size=self._snap_size(coin, sz),
            limit_px=None,
            trigger_px=self._align_px(coin, px),
            reduce_only=True, is_trigger=True,
            raw={"venue": VENUE, "mechanics": "reduce-only price trigger "
                                              "(nado has no native TP limit)"})

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        """Cancel + re-list until ABSENT (N10). 2020 not-found = idempotent
        success (still readback-verified); still-live or unlistable ->
        WriteUnconfirmed."""
        raw = self._raw
        oid = str(oid)
        self._dry_block("cancel_sl_order", coin)
        try:
            r = raw.cancel_sl_order(coin, oid)
        except Exception as e:  # raw catches internally; defensive
            self._reraise_harness(e)
            r = {}
        already_gone = bool(isinstance(r, dict) and r.get("already_gone"))
        # confirmed-GONE readback — the only proof (the sibling cancel API
        # returns success with cancelled_orders=[], the 2026-05-06 shape)
        for i in range(self.CONFIRM_ATTEMPTS):
            try:
                live = self._fetch_triggers_ccxt("cancel_confirm")
            except ExchangeError as e:
                raise WriteUnconfirmed(
                    "cancel_sl_order(%s, %s): cannot prove the order gone — "
                    "trigger listing read FAILED: %s" % (coin, oid, e),
                    may_have_landed=True, venue=VENUE,
                    op="cancel_sl_order", coin=coin)
            live_oids = set()
            for o in live:
                dg = (o.get("info") or {}).get("digest") or o.get("oid")
                if dg:
                    live_oids.add(str(dg).lower())
            if oid.lower() not in live_oids:
                return None  # goal state holds (incl. the not-found path)
            if i < self.CONFIRM_ATTEMPTS - 1:
                time.sleep(0.15 * (i + 1))
        raise WriteUnconfirmed(
            "cancel_sl_order(%s, %s): cancel %s but the order is STILL LIVE "
            "on the trigger service — duplicate-SL race half, not swallowed"
            % (coin, oid,
               "reported-not-found" if already_gone else "sent"),
            may_have_landed=True, venue=VENUE, op="cancel_sl_order",
            coin=coin)

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        """Nado cross-margin subaccounts have NO per-product leverage knob —
        documented no-op (census: conforming). A request above the product's
        max is definitively un-grantable -> VenueRejected."""
        raw = self._raw
        try:
            meta = raw.asset(coin)
        except KeyError:
            raise VenueRejected(
                "update_leverage: unknown Nado symbol %r" % coin,
                reason="unknown_symbol", venue=VENUE,
                op="update_leverage", coin=coin)
        max_lev = int(getattr(meta, "max_leverage", 0) or 20)
        if int(leverage) > max_lev:
            raise VenueRejected(
                "update_leverage(%s): %dx requested but the product max is "
                "%dx and nado has no leverage knob to change it"
                % (coin, int(leverage), max_lev),
                reason="leverage_above_product_max (no venue knob)",
                venue=VENUE, op="update_leverage", coin=coin)
        return None

    # ---------------------------------------------------- CONTRACT-EXEMPT
    # Non-contract caller surface (F4 / mapping §6): VERBATIM delegation to
    # the raw adapter, semantics UNCHANGED — the P3 repoint cannot
    # AttributeError. Typed-error laws do NOT apply here.
    #   callers grepped 2026-07-03: trader.py (asset, round_price); the rest
    #   are raw-adapter surface kept for parity with mapping §6 (nado's raw
    #   exposes no round_size/round_qty/orderbook_snapshot/trigger_tp/
    #   add_isolated_margin/candles-cache knobs — nothing to delegate).

    CONTRACT_EXEMPT_PASSTHROUGH: Tuple[str, ...] = (
        "asset", "round_price", "slip_per_side", "funding_rate", "spot_usdc",
        "compute_realized_pnl",
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

    # --------------------------------------------------------- cache control

    def invalidate_positions_cache(self) -> None:
        self._summary_cache = None
        try:
            self._raw.invalidate_positions_cache()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# factories
# ---------------------------------------------------------------------------

def build_client(raw: Any) -> NadoExchangeClient:
    """Wrap an already-constructed raw adapter (conformance-harness hook)."""
    return NadoExchangeClient(raw)


def get_client() -> NadoExchangeClient:
    """Production factory: build the raw adapter from the bot env (lazy
    bot/SDK import — run from the nado bot root), then wrap."""
    from bot.config import Settings  # noqa: PLC0415 — lazy by design
    from bot.exchange_nado import NadoClient_  # noqa: PLC0415

    return NadoExchangeClient(NadoClient_(Settings.from_env()))
