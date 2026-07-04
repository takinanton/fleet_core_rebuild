"""fleet_core.exchange_api — typed ExchangeClient contract (rebuild Phase 2).

THE DEFECT CLASS THIS MODULE KILLS (audit verdict R3, adapter error-contract
inversion, architecture_audit_full.json 2026-07-02):

    * reads that mask failure into neutral values — mark_price -> 0.0,
      open_positions -> {} / unbounded-stale cache, open_orders -> [],
      account_value -> 0.0, candles -> silent empty DataFrame;
    * writes that return ok-shaped dicts without readback — market_close
      "succeeds" while the position is still open, cancel_sl_order returns {}
      on failure, trigger_sl returns {"status": "ok", ... "error": ...};
    * callers whose guards assume raise-on-failure, so the masked value is
      consumed as DATA: 0.0 mark skips freshness gates, {} positions fires the
      phantom-guard mass-close, [] SL-list reads as "naked -> heal storm".

CONTRACT LAW (every method below states its own raise clause; these are the
fleet-wide invariants):

  1. A read returns VERIFIED data or raises ReadUnknown. Never {} / 0.0 /
     [] / None-as-data / silently-stale cache. "I could not find out" is an
     exception, not a value. Empty results are legal ONLY when the venue
     positively confirmed emptiness (flat account -> {}, no orders -> []).

  2. A write returns only after the exchange CONFIRMED the resulting state by
     READBACK (position re-read, live-order listing, fill feed) — otherwise it
     raises WriteUnconfirmed. An accepted-but-unverifiable write is UNCONFIRMED,
     not ok. Result dataclasses carry readback_verified=True enforced BY
     CONSTRUCTION (__post_init__ raises), so an unverified result object cannot
     exist in a caller's hands.

  3. A definitive venue rejection (min-size, reduce-only-increases, insufficient
     margin, bad symbol) raises VenueRejected(reason) — state is unchanged and
     the caller may re-plan. Rejection is not failure-to-communicate: it must
     NOT be retried blindly.

  4. There is NO market_close in this API. ensure_flat(coin) is the ONLY close
     primitive: it returns only after position readback confirms flat (or that
     the coin was already flat), else raises WriteUnconfirmed. This encodes the
     2026-05-30/31 HL orphan root-fix at the type level — a bare fire-and-forget
     close cannot be expressed. (Audit confirmed defects #1-#3: bare
     market_close with discarded error-dict on nado/pacifica/extended
     SL-fail-3x paths; Phase-0 already routed those through trader-level
     _ensure_flat — this contract makes the safe path the only path.)

  5. Staleness is an error, not a discount: candles and mark_price take an
     explicit tolerance and raise StaleData when the freshest verifiable value
     is older than it. Serving a cached value INSIDE tolerance is fine and
     encouraged (rate-limit discipline); serving one outside it is lying.

  6. Every outbound call is time-bounded (HL _install_session_timeout pattern,
     exchange_hl.py:157). A binding MUST NOT expose an unbounded SDK call
     through this interface; a hang converts to ReadUnknown/WriteUnconfirmed
     via timeout, never a frozen loop (audit defect #0: nado zero timeouts,
     300s watchdog-suicide as the only defense).

WHY RAISING IS SAFE FOR TODAY'S CALLERS (grepped 2026-07-02, all four bots'
trader.py / main.py / scanner.py / orphan_sweep.py): every adapter call site is
wrapped in a broad `except Exception` layer that already implements
UNKNOWN-semantics (skip tick / defer / bias-to-protect) — see
proofs/p2_contract_mapping.md for the per-site table. The Phase-0 fail-loud
reads (open_positions raise, margin_used_usd raise, equity_with_upnl raise)
raise plain RuntimeError today; therefore ExchangeError roots at RuntimeError,
so `except RuntimeError` and `except Exception` sites are both preserved.

PURITY: this module imports ONLY the standard library. pandas appears solely
under TYPE_CHECKING for annotations. Venue SDKs live in per-venue bindings
(fleet_core/venues/*) that import bot.exchange_* lazily — this file must be
importable and testable on any machine.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

if TYPE_CHECKING:  # pragma: no cover — annotation-only, no runtime pandas dep
    import pandas as pd

__all__ = [
    "ExchangeError",
    "ReadUnknown",
    "StaleData",
    "WriteUnconfirmed",
    "VenueRejected",
    "RateLimited",
    "PositionInfo",
    "FillResult",
    "SLOrderInfo",
    "OpenOrderInfo",
    "FlatResult",
    "ExchangeClient",
]


# ============================================================================
# Exceptions
# ============================================================================

class ExchangeError(RuntimeError):
    """Base for every contract exception.

    Roots at RuntimeError deliberately: the Phase-0 fail-loud adapters already
    raise RuntimeError on read failure and every live caller catch-site is a
    broad `except Exception` (verified by grep — no narrower venue-error
    catches exist in trader/main/scanner/orphan_sweep on any of the 4 bots).
    Migrating an adapter to these types therefore changes NO caller behavior;
    it only lets new code discriminate.

    Attributes are diagnostic, never control-flow-load-bearing (control flow
    discriminates on TYPE): venue, op, coin.
    """

    def __init__(self, msg: str, *, venue: str = "", op: str = "",
                 coin: str = "") -> None:
        super().__init__(msg)
        self.venue = venue
        self.op = op
        self.coin = coin


class ReadUnknown(ExchangeError):
    """A read could not produce VERIFIED data — state is UNKNOWN.

    Raised instead of the masked neutrals this fleet has been burned by:
    {} (phantom-guard mass-close class), 0.0 mark (freshness-gate silent skip),
    [] SL list (duplicate-SL storm / spurious emergency close), stale-silent
    cache (hid real closes for the outage duration).

    Caller semantics: treat the tick as indeterminate — defer / skip / keep
    protecting (bias-to-protect). NEVER interpret as "absent" or "zero".
    """


class StaleData(ReadUnknown):
    """Data exists but its freshest verifiable value exceeds the declared
    tolerance. A special case of UNKNOWN: we know what the world looked like
    `age_sec` ago, which is not the same as knowing what it looks like now.

    age_sec / tolerance_sec are diagnostic (both may be None when the venue
    cannot even date its data — that is still StaleData, not a value).
    """

    def __init__(self, msg: str, *, age_sec: Optional[float] = None,
                 tolerance_sec: Optional[float] = None, **kw: Any) -> None:
        super().__init__(msg, **kw)
        self.age_sec = age_sec
        self.tolerance_sec = tolerance_sec


class WriteUnconfirmed(ExchangeError):
    """A write was sent (or MAY have been sent) and readback could not confirm
    the intended resulting state. The exchange may or may not hold the order /
    position — the caller must reconcile before acting again.

    `may_have_landed`:
      True  -> the request reached the venue (accepted / ambiguous response);
               assume side effects exist until a readback proves otherwise.
      False -> we can prove the request never left (e.g. local pre-send
               failure); state is clean.
      None  -> genuinely unknown (timeout mid-flight). Treat as True.

    Caller semantics: this is the money-dangerous branch — never swallow.
    Typical handling: invalidate caches, re-read positions/orders, re-drive to
    the intended goal state (ensure_flat retries, SL re-place), escalate loud
    if convergence fails.
    """

    def __init__(self, msg: str, *, may_have_landed: Optional[bool] = None,
                 **kw: Any) -> None:
        super().__init__(msg, **kw)
        self.may_have_landed = may_have_landed


class VenueRejected(ExchangeError):
    """The venue DEFINITIVELY rejected the request; state is unchanged.

    reason: the venue's own words (error string / code text) — mandatory,
    because "rejected" without a reason is unactionable (min-size vs
    reduce-only-increases vs insufficient-margin demand different replans:
    nado trigger-SL min-size x10 NAKED incident, pacifica $10-notional
    min_order_size class).

    Caller semantics: do NOT blind-retry (the same request rejects again);
    re-plan (resize, skip coin, abort entry) or surface. State needs no
    reconciliation — nothing landed.
    """

    def __init__(self, msg: str, *, reason: str, code: Any = None,
                 **kw: Any) -> None:
        super().__init__(msg, **kw)
        self.reason = reason
        self.code = code


class RateLimited(ExchangeError):
    """Retry budget for 429/rate-limit exhausted INSIDE the binding.

    Bindings retry 429s internally (existing per-adapter discipline: HL
    _retry_429, pacifica global pacer, extended bridge, nado backoff); this
    surfaces only when the budget is spent. Distinct from ReadUnknown so a
    supervisor can back off globally instead of treating it as venue flakiness.
    Caller semantics: same as ReadUnknown/WriteUnconfirmed for safety purposes
    (state unknown for reads; check may_have_landed analog n/a — a rate-limited
    WRITE that may have landed must be raised as WriteUnconfirmed instead).
    """


# ============================================================================
# Result dataclasses — verified-by-construction
# ============================================================================

def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)


@dataclass(frozen=True)
class PositionInfo:
    """One VERIFIED open position (present on the exchange at read time).

    size_signed: base units, sign = direction (+long / -short); never 0 — a
    zero-size position must be omitted from open_positions(), not emitted.
    entry_px: venue's average entry price (> 0; from the venue, never
    synthesized from mark — nado fill-truth class).
    Optional fields are None when the venue does not expose them — None means
    "venue has no such field", NEVER "read failed" (a failed read raises).
    raw: the venue's original position payload for forensic/journal use;
    excluded from equality.
    """
    coin: str
    size_signed: float
    entry_px: float
    leverage: Optional[float] = None
    margin_used_usd: Optional[float] = None
    liquidation_px: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False,
                                   compare=False)

    def __post_init__(self) -> None:
        _require(bool(self.coin), "PositionInfo: coin required")
        _require(self.size_signed != 0.0,
                 f"PositionInfo({self.coin}): size_signed must be non-zero")
        _require(self.entry_px > 0.0,
                 f"PositionInfo({self.coin}): entry_px must be > 0")

    @property
    def is_long(self) -> bool:
        return self.size_signed > 0


@dataclass(frozen=True)
class FillResult:
    """A CONFIRMED fill. Cannot be constructed unverified.

    readback_verified MUST be True — __post_init__ raises otherwise. This is
    the type-level kill of the fabricated-fill class (nado pre-fix: avgPx :=
    mark, totalSz := requested size for ANY non-raising response -> inert
    partial-fill guard + fake slip journal). A binding that cannot verify the
    fill (order response unparseable AND position/fill-feed readback failed)
    must raise WriteUnconfirmed — it has nothing of this type to return.

    avg_px  : real fill VWAP from the venue (response fill fields or fills-feed
              readback), > 0. Never a reference/mark price.
    size    : actually-filled base size, > 0. May be < requested_size (partial
              fill) — the CALLER decides accept vs ensure_flat+abort; the
              binding must not hide a partial by echoing the request.
    oid     : venue order id when the venue returns one (None where the venue
              genuinely has no id for market fills).
    """
    coin: str
    is_buy: bool
    avg_px: float
    size: float
    requested_size: float
    oid: Optional[str] = None
    readback_verified: bool = True

    def __post_init__(self) -> None:
        _require(self.readback_verified is True,
                 f"FillResult({self.coin}): readback_verified must be True by "
                 "construction — raise WriteUnconfirmed instead of building an "
                 "unverified FillResult")
        _require(self.avg_px > 0.0,
                 f"FillResult({self.coin}): avg_px must be > 0 (real fill VWAP,"
                 " never mark/reference)")
        _require(self.size > 0.0,
                 f"FillResult({self.coin}): size must be > 0 (an unfilled order"
                 " is VenueRejected/WriteUnconfirmed, not a zero-size fill)")
        _require(self.requested_size > 0.0,
                 f"FillResult({self.coin}): requested_size must be > 0")

    @property
    def fill_ratio(self) -> float:
        return self.size / self.requested_size


@dataclass(frozen=True)
class SLOrderInfo:
    """A stop-loss trigger CONFIRMED LIVE on the exchange by readback.

    Constructed only after the binding re-read the venue's live trigger/order
    list and matched this order (nado _confirm_trigger_live pattern — the
    strongest in the fleet; the contract makes it mandatory everywhere).
    readback_verified must be True by construction.

    oid: the id/digest under which the order can later be found by
    list_open_sl_orders() and cancelled by cancel_sl_order(). For venues with
    position-attached SLs (extended TPSL POSITION) the binding must return the
    handle its own list/cancel methods understand.
    trigger_px: price as ACCEPTED by the venue (post-rounding), > 0.
    size: venue-accepted size; 0.0 is allowed ONLY for position-wide SLs
    (extended tp_sl_type=POSITION has amount_of_synthetic=0 by protocol) —
    flagged by position_wide=True.
    """
    coin: str
    oid: str
    trigger_px: float
    size: float
    is_buy_to_close: bool
    position_wide: bool = False
    readback_verified: bool = True

    def __post_init__(self) -> None:
        _require(self.readback_verified is True,
                 f"SLOrderInfo({self.coin}): readback_verified must be True by "
                 "construction — raise WriteUnconfirmed if the trigger cannot "
                 "be confirmed live")
        _require(bool(self.oid),
                 f"SLOrderInfo({self.coin}): oid required (a fabricated or "
                 "missing oid is the naked-success class)")
        _require(self.trigger_px > 0.0,
                 f"SLOrderInfo({self.coin}): trigger_px must be > 0")
        _require(self.size > 0.0 or self.position_wide,
                 f"SLOrderInfo({self.coin}): size must be > 0 unless "
                 "position_wide")


@dataclass(frozen=True)
class OpenOrderInfo:
    """One VERIFIED open order as listed by the venue (open_orders /
    list_reduce_only_triggers / limit_reduce_only readback).

    side: 'buy' | 'sell'. size in base units (>0). limit_px may be None for
    pure-trigger orders; trigger_px None for plain limits. raw keeps the venue
    payload (excluded from equality).
    """
    coin: str
    oid: str
    side: str
    size: float
    limit_px: Optional[float] = None
    trigger_px: Optional[float] = None
    reduce_only: bool = False
    is_trigger: bool = False
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False,
                                   compare=False)

    def __post_init__(self) -> None:
        _require(bool(self.coin) and bool(self.oid),
                 "OpenOrderInfo: coin and oid required")
        _require(self.side in ("buy", "sell"),
                 f"OpenOrderInfo({self.coin}): side must be 'buy'|'sell', got "
                 f"{self.side!r}")


@dataclass(frozen=True)
class FlatResult:
    """PROOF that `coin` is flat on the exchange — the only legal return of
    ensure_flat(). verified_flat must be True by construction: a close whose
    final position readback failed, or that left residual size, raises
    WriteUnconfirmed and this object never exists.

    already_flat  : True when no position existed (idempotent call) — then
                    closed_size == 0.0 and exit_avg_px is None.
    closed_size   : base size actually closed by this call (0.0 if already
                    flat).
    exit_avg_px   : real exit fill VWAP when the venue exposes it; None when
                    the venue confirmed flat but could not attribute a fill
                    price (journal then records px from user_fills readback,
                    never from the SL reference — wick_sl x1330 class).
    attempts      : close attempts consumed (diagnostic).
    """
    coin: str
    already_flat: bool
    closed_size: float
    exit_avg_px: Optional[float] = None
    attempts: int = 1
    verified_flat: bool = True

    def __post_init__(self) -> None:
        _require(self.verified_flat is True,
                 f"FlatResult({self.coin}): verified_flat must be True by "
                 "construction — raise WriteUnconfirmed instead")
        _require(self.closed_size >= 0.0,
                 f"FlatResult({self.coin}): closed_size must be >= 0")
        if self.already_flat:
            _require(self.closed_size == 0.0,
                     f"FlatResult({self.coin}): already_flat implies "
                     "closed_size == 0")
        if self.exit_avg_px is not None:
            _require(self.exit_avg_px > 0.0,
                     f"FlatResult({self.coin}): exit_avg_px must be > 0 or "
                     "None")


# ============================================================================
# The contract
# ============================================================================

class ExchangeClient(abc.ABC):
    """Typed venue adapter contract. One implementation per venue
    (fleet_core/venues/<venue>.py), each importing bot.exchange_* lazily and
    translating that adapter's dict/neutral-value conventions into this
    raise-or-verified-value contract.

    GLOBAL RULES (bind every method):
      * verified-or-raise: see module docstring, laws 1-6.
      * time-bounded: every underlying call carries an explicit timeout.
      * cache discipline: internal caches are legal ONLY inside each method's
        declared tolerance; a cache older than tolerance is StaleData, not a
        return value.
      * idempotence where stated (ensure_flat, cancel_sl_order): calling twice
        is safe and the second call cheaply confirms.
      * NO strategy/risk logic here: sizing, SL levels, entry gating stay in
        trader/risk code — this layer only moves orders and tells the truth.
    """

    # ------------------------------------------------------------------ writes

    @abc.abstractmethod
    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        """Market/IOC entry for `sz` base units. Returns a CONFIRMED FillResult.

        intended_px / allow_marketable (extended marketability guard,
        exchange_extended.py:877): when the caller means to enter at a LEVEL
        and the level is already through the book, allow_marketable=False must
        raise VenueRejected(reason='immediately_marketable') instead of filling
        at market. Venues without the guard ignore both args (documented
        per-binding).

        Raises:
          VenueRejected     — definitive reject (min-size/notional, margin,
                              bad symbol, marketability guard). Nothing landed.
          WriteUnconfirmed  — order sent (or possibly sent) and neither the
                              response nor fill/position readback could confirm
                              a fill (may_have_landed set accordingly). The
                              binding MUST attempt readback before raising
                              (HL _confirm_fill_via_positions pattern: a
                              session read-timeout on a slow accept does NOT
                              mean unfilled).
          ReadUnknown       — pre-send verified reads failed (e.g. meta/price
                              needed to build the order). Nothing sent.
        Never returns a zero/partial-masked fill: partial fills return the REAL
        filled size (caller applies its min_fill_ratio policy and calls
        ensure_flat to unwind if unacceptable).

        KNOWN ATTRIBUTION LIMITATION (documented, P2): bindings without venue
        client-order-id attribution recover ambiguous fills from a TIME/
        WATERMARK-windowed fills feed (HL: since t0-2s; extended: since-2s;
        nado: matches-feed watermark) — a CONCURRENT same-coin same-side fill
        inside that window (e.g. an SL firing mid-entry) can co-mingle into
        FillResult.size/avg_px. Pacifica's exact client_order_id attribution
        is the model; the fleet-wide fix rides the P3 entry state machine,
        which brings client-order-id attribution to every venue.
        """

    @abc.abstractmethod
    def ensure_flat(self, coin: str) -> FlatResult:
        """THE ONLY CLOSE PRIMITIVE — market_close is intentionally absent.

        Drive `coin` to flat and PROVE it: send reduce-only close(s), then
        re-read positions (cache invalidated) until the venue confirms size 0;
        bounded internal retries. Idempotent — already-flat returns
        FlatResult(already_flat=True) after a verified read.

        Raises:
          WriteUnconfirmed — close attempts exhausted with position still (or
                             possibly still) open, OR the final position
                             readback failed so flatness cannot be proven
                             (may_have_landed=True: some close orders were
                             sent). Caller escalates (emergency SL protect /
                             loud alert) — this is the naked-position branch
                             and must never be swallowed.
          ReadUnknown      — the INITIAL position read failed; nothing sent.

        Encodes: HL orphan root-fix 2026-05-31 (verified close), audit defects
        #1-#3 (discarded market_close error-dicts), nado market_close internal
        readback (exchange_nado.py:1403) promoted from error-dict to raise.
        """

    @abc.abstractmethod
    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        """Place a reduce-only stop-loss trigger and CONFIRM IT LIVE.

        is_buy is the CLOSE side: True closes a short, False closes a long.
        Returns only after the venue's live trigger/order list shows a matching
        active order (nado _confirm_trigger_live, exchange_nado.py:1437 —
        mandatory on all venues under this contract: an ok-shaped response
        without a listed live trigger was the naked-success class,
        error_code=2064 reduce-only-increases reported as placed).

        Raises:
          VenueRejected    — definitive reject (min trigger size — nado trigSL
                             min x10 NAKED class — reduce-only violation, bad
                             px). reason mandatory so the caller can resize vs
                             abort.
          WriteUnconfirmed — sent but no matching live trigger found within the
                             readback budget (may_have_landed as known).
        Caller contract stays what trader._place_sl_with_retry enforces today:
        no confirmed SL -> position is NAKED -> ensure_flat + escalate.
        """

    @abc.abstractmethod
    def cancel_sl_order(self, coin: str, oid: str) -> None:
        """Cancel order `oid` and return only after readback confirms it is no
        longer live (absent from the venue's open trigger/order list) —
        cancel-then-assume was half of the trail place/cancel races.

        IDEMPOTENT: already-gone (venue 'order not found', nado code 2020) is
        SUCCESS — the goal state 'oid not live' holds. Bindings must map the
        venue's not-found signal to a clean return, not an error.

        Raises:
          WriteUnconfirmed — cancel sent but the order still appears live (or
                             the confirming list read failed: cannot prove
                             gone).
          VenueRejected    — venue refused the cancel for a definitive,
                             non-not-found reason.
        """

    @abc.abstractmethod
    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        """Place a reduce-only resting limit (TP1 partial-exit primitive used
        by all four traders) and confirm it is either RESTING (returned
        OpenOrderInfo matched against the live order list) or already FILLED —
        an immediate fill is returned as an OpenOrderInfo whose raw payload
        carries the venue fill fields (caller inspects, as today).

        Raises: VenueRejected / WriteUnconfirmed with the same semantics as
        trigger_sl.
        """

    @abc.abstractmethod
    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        """Set leverage/margin mode for `coin`. Returns on venue confirmation.

        Venues with no per-product leverage knob (nado cross-margin
        subaccounts, exchange_nado.py:1193) implement this as a documented
        no-op returning None — that is a CONFORMING implementation, not a
        masked failure, because there is nothing to fail.

        Raises:
          VenueRejected    — venue refused (e.g. leverage above max, open
                             position blocks mode change). Callers today log
                             and proceed with venue-default leverage; the raise
                             lets them keep doing exactly that in one except
                             clause instead of parsing Optional[dict].
          WriteUnconfirmed — request sent, confirmation unreadable.
        """

    # ------------------------------------------------------------------- reads

    @abc.abstractmethod
    def open_positions(self) -> Mapping[str, PositionInfo]:
        """All open positions, keyed by INTERNAL coin symbol, VERIFIED at read
        time (cache allowed within the binding's declared TTL, <= one main-loop
        interval).

        {} means the venue POSITIVELY reported an empty account. A failed or
        partial fetch (one HIP-3 dex down, one perp balance unparseable) raises
        ReadUnknown for the WHOLE snapshot — a dict silently missing one
        position is how the phantom-guard closed live xyz_* legs. Canon:
        exchange_hl.py:806 (Phase-0 raise-on-unknown, per-dex + per-item).

        Bindings must NOT serve stale cache beyond TTL on failure (nado's
        unbounded stale-cache fallback, exchange_nado.py:795, is the residual
        masked path this contract removes; extended's <= loop-interval bounded
        bias-to-present is the maximum allowed and must surface age via
        StaleData beyond it).

        Raises: ReadUnknown (incl. StaleData), RateLimited.
        """

    @abc.abstractmethod
    def open_orders(self) -> Sequence[OpenOrderInfo]:
        """All open orders (resting limits AND trigger orders) for the account,
        verified. [] only on positive venue confirmation of no orders.

        nado's current open_orders (exchange_nado.py:1069) returns [] on fetch
        failure AND omits trigger orders — both are contract violations the
        binding must fix (merge engine open orders + trigger service, raise on
        either leg failing).

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        """Oids/digests of live stop/trigger/reduce-only orders for `coin` —
        the per-tick SL-liveness primitive behind trader._sl_confirmed_live on
        all four bots ("never naked >1 tick").

        [] is a POSITIVE claim ("this coin has NO live SL") and triggers a
        heal; it is only legal after a fully-successful listing across every
        venue scope (all HIP-3 dexes — exchange_hl.py:1531 raise-on-any-dex-
        failure is canon). On any failure raise ReadUnknown so the caller's
        assume-live bias-to-protect branch engages instead of a heal storm.

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        """Account-wide reduce-only trigger orders (orphan_sweep primitive on
        all four bots). Same []-is-positive / raise-on-unknown law as
        list_open_sl_orders: the sweep CANCELS what it decides is orphaned, so
        a masked partial listing turns the safety sweep into a gun.

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        """Current mark/mid for `coin`, > 0, no older than max_age_sec.

        NEVER returns 0.0 or a beyond-tolerance cached value (today all four
        adapters mask to 0.0 and/or unbounded stale cache — mark feeds SL-trail
        distance, liq-guard clamps, exit px references and the signal freshness
        gate; a silent 0.0 disabled the freshness gate on HIP-3, bugfix
        2026-05-06). Values failing sanity (nado MARK_SANITY_MAX) are
        ReadUnknown, not 0.0.

        Raises: ReadUnknown (fetch failed / insane value),
                StaleData (only cache older than max_age_sec available),
                RateLimited.
        """

    @abc.abstractmethod
    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> "pd.DataFrame":
        """CLOSED OHLCV bars, columns [time, Open, High, Low, Close, Volume],
        in-progress bar dropped, ascending time.

        Freshness contract: the last returned CLOSED bar must be within
        max_stale_bars * interval of the most recent boundary — else StaleData
        (nado 09:00-bar-missing -> 1.7h-stale-signal class). A fetch failure
        with no within-tolerance cache raises ReadUnknown; the silent
        empty-DataFrame return (all four adapters today, incl. HL's negative
        fail-cache serving stale/empty for CANDLES_FAIL_TTL) is banned — the
        scanner's per-coin except/skip + all-items-skipped fail-loud gate
        already handles the raise correctly.

        Unsupported interval / unknown coin is a CALLER BUG: raise ValueError
        (not ReadUnknown) so it surfaces in tests, never in a quiet skip.

        Raises: ReadUnknown, StaleData, RateLimited, ValueError.
        """

    @abc.abstractmethod
    def equity_with_upnl(self) -> float:
        """Account equity INCLUDING unrealized PnL, USD — the MM-cap
        denominator (canon: exchange_nado.py:772, raises; live-verified vs UI
        2026-06-11). For unified-margin venues this is the deployable-equity
        basis (HL: spot USDC + perp accountValue incl uPnL,
        exchange_hl.py:752).

        0.0 is only a genuinely-empty account. Raises ReadUnknown on any fetch/
        parse failure — the MM-cap gate must fail CLOSED (refuse entry), never
        size against a masked 0.0/model fallback.

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def account_value(self) -> float:
        """Venue's SIZING basis (risk-per-trade denominator) with EXACTLY the
        semantics the deployed trader uses today per venue (HL deployable =
        spot USDC + perp AV; nado = initial_health, deliberately !=
        equity_with_upnl). Kept as a separate method so P2 changes NO risk
        numbers; venues where both coincide may return equity_with_upnl().

        Same fail-closed law: verified value or ReadUnknown (pacifica/extended
        currently mask to 0.0 — under-sizing to zero silently kills entries,
        a masked liveness failure, not safety).

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def margin_used_usd(self) -> float:
        """REAL exchange initial margin used ($), account-wide, all positions
        incl. manual/foreign at their OWN leverage (MM-cap numerator). Already
        fail-loud on all four adapters (canon docstrings ported 2026-06-11) —
        this contract just fixes that behavior in the type.

        0.0 only when the venue positively reports no margin in use.
        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def position_liquidation(self, coin: str) -> Optional[float]:
        """Liquidation price for `coin`'s open position, or None when the
        venue POSITIVELY reports no position / no liq price for it (cross
        account far from liq). None never encodes a failed read.

        Feeds the SL-inside-liquidation invariant (SL must sit inside liq
        x1.01) — a masked None weakens a money-safety clamp.

        Raises: ReadUnknown, RateLimited.
        """

    @abc.abstractmethod
    def user_fills(self, max_age_sec: float = 60.0) -> Sequence[Mapping[str, Any]]:
        """Recent account fills (venue-shaped mappings, newest first) — the
        exit-attribution / realized-PnL readback source (fill-truth class:
        exit rows must record REAL fill VWAP, never the SL reference px).

        [] only when the venue positively reports no recent fills.
        Raises: ReadUnknown, StaleData, RateLimited.

        KNOWN LIMITATION (documented, P2): consumers that window this feed by
        TIME to attribute a specific order's fills can co-mingle concurrent
        same-coin same-side fills (see market_open note) — venue-native
        client-order-id attribution lands fleet-wide with the P3 entry state
        machine.
        """

    # ------------------------------------------------------------ cache control

    def invalidate_positions_cache(self) -> None:
        """Force the next open_positions()/position_liquidation() to re-fetch.
        MUST be called by bindings internally after every write; exposed
        because callers (trader tick handlers) also invalidate around
        adopt/heal decisions today. Default no-op for bindings without caches.
        """
        return None
