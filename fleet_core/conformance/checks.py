"""fleet_core.conformance.checks — the contract assertions.

Pytest-free on purpose: `suite.py` parameterizes these into pytest tests;
`runner.py` drives the same list with plain python on hosts whose venvs lack
pytest. Every check takes a fresh BoundContext (faults cleared, venue truth
reset, client rebuilt) and raises AssertionError on contract violation or
SkipCheck when it does not apply.

WHAT IS ASSERTED (audit verdict R3 — error-contract inversion — killed):
  * every read under every transport fault raises a TYPED error and NEVER
    returns the neutral value the legacy adapters masked to ({}/0.0/[]/df);
  * every write returns only venue-verified state: fills cross-checked
    against what the simulated venue ACTUALLY recorded (no fabrication, no
    request-echo), ensure_flat returns only with proven flatness, trigger_sl
    only with the trigger live-listed, cancel only confirmed-gone;
  * market_close is absent from the API surface;
  * StaleData/tolerance semantics for mark_price and candles;
  * ValueError (caller bug) vs ReadUnknown (venue unknown) split for candles;
  * bounded 429 discipline (transient survives, exhausted raises RateLimited);
  * per-item-corrupt law: one unparseable position/order row poisons the
    WHOLE read (ReadUnknown) or is venue-truth-resolved — never silently
    dropped (nado per-item drop-and-continue class);
  * intercept-all guard integrity.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Tuple

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
from fleet_core.conformance.faults import Fault, UninterceptedRealCall

__all__ = ["SkipCheck", "CONSTRUCTION_CHECKS", "CONFORMANCE_CHECKS",
           "DRY_WRITE_CHECKS", "P3_TRANSITIONS"]


class SkipCheck(Exception):
    """Raise inside a check to skip it (suite → pytest.skip)."""


def _expect(exc_types, fn, what: str):
    """Assert fn() raises one of exc_types; returns the exception."""
    try:
        result = fn()
    except exc_types as e:
        return e
    except Exception as e:  # wrong type — the defect class itself
        raise AssertionError(
            "%s: raised %s(%s) — expected %s"
            % (what, type(e).__name__, e,
               "/".join(t.__name__ for t in (
                   exc_types if isinstance(exc_types, tuple)
                   else (exc_types,)))))
    raise AssertionError(
        "%s: returned %r instead of raising %s — the masked-neutral defect "
        "class" % (what, result,
                   "/".join(t.__name__ for t in (
                       exc_types if isinstance(exc_types, tuple)
                       else (exc_types,)))))


def _approx(a: float, b: float, rel: float = 1e-6) -> bool:
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


# ===========================================================================
# 1. Construction invariants (no venue needed — always run)
# ===========================================================================

def _c_fillresult_requires_verified() -> None:
    for kw in (
        dict(readback_verified=False),
        dict(avg_px=0.0), dict(avg_px=-5.0),
        dict(size=0.0), dict(size=-1.0),
        dict(requested_size=0.0),
    ):
        base = dict(coin="BTC", is_buy=True, avg_px=50_000.0, size=1.0,
                    requested_size=1.0)
        base.update(kw)
        try:
            FillResult(**base)
        except ValueError:
            continue
        raise AssertionError(
            "FillResult constructed with %r — verified-by-construction "
            "violated" % (kw,))


def _c_slorder_requires_confirmed() -> None:
    for kw in (
        dict(readback_verified=False), dict(oid=""),
        dict(trigger_px=0.0), dict(size=0.0),
    ):
        base = dict(coin="BTC", oid="o1", trigger_px=49_000.0, size=1.0,
                    is_buy_to_close=False)
        base.update(kw)
        try:
            SLOrderInfo(**base)
        except ValueError:
            continue
        raise AssertionError("SLOrderInfo constructed with %r" % (kw,))
    # position-wide SLs legally carry size 0
    SLOrderInfo(coin="BTC", oid="o1", trigger_px=49_000.0, size=0.0,
                is_buy_to_close=False, position_wide=True)


def _c_flatresult_requires_verified() -> None:
    for kw in (
        dict(verified_flat=False),
        dict(closed_size=-1.0),
        dict(already_flat=True, closed_size=2.0),
        dict(exit_avg_px=0.0),
    ):
        base = dict(coin="BTC", already_flat=False, closed_size=1.0,
                    exit_avg_px=50_000.0)
        base.update(kw)
        try:
            FlatResult(**base)
        except ValueError:
            continue
        raise AssertionError("FlatResult constructed with %r" % (kw,))
    FlatResult(coin="BTC", already_flat=True, closed_size=0.0)  # legal


def _c_positioninfo_invariants() -> None:
    for kw in (dict(size_signed=0.0), dict(entry_px=0.0), dict(coin="")):
        base = dict(coin="BTC", size_signed=1.0, entry_px=50_000.0)
        base.update(kw)
        try:
            PositionInfo(**base)
        except ValueError:
            continue
        raise AssertionError("PositionInfo constructed with %r" % (kw,))
    assert PositionInfo(coin="BTC", size_signed=-1.0, entry_px=1.0).is_long \
        is False


def _c_openorderinfo_invariants() -> None:
    for kw in (dict(side="LONG"), dict(oid=""), dict(coin="")):
        base = dict(coin="BTC", oid="o1", side="buy", size=1.0)
        base.update(kw)
        try:
            OpenOrderInfo(**base)
        except ValueError:
            continue
        raise AssertionError("OpenOrderInfo constructed with %r" % (kw,))


def _c_no_market_close_in_api() -> None:
    assert "market_close" not in getattr(ExchangeClient,
                                         "__abstractmethods__", set()), \
        "market_close must NOT be an abstract contract method"
    assert not hasattr(ExchangeClient, "market_close"), \
        "market_close must be absent from the ExchangeClient surface — " \
        "ensure_flat is the only close primitive"


def _c_abstract_method_set() -> None:
    expected = {
        "market_open", "ensure_flat", "trigger_sl", "cancel_sl_order",
        "limit_reduce_only", "update_leverage", "open_positions",
        "open_orders", "list_open_sl_orders", "list_reduce_only_triggers",
        "mark_price", "candles", "equity_with_upnl", "account_value",
        "margin_used_usd", "position_liquidation", "user_fills",
    }
    actual = set(ExchangeClient.__abstractmethods__)
    assert actual == expected, (
        "contract abstract-method drift: missing=%s extra=%s"
        % (sorted(expected - actual), sorted(actual - expected)))
    assert len(expected) == 17


def _c_exception_hierarchy() -> None:
    assert issubclass(ExchangeError, RuntimeError), \
        "ExchangeError must root at RuntimeError (Phase-0 caller compat)"
    for t in (ReadUnknown, StaleData, WriteUnconfirmed, VenueRejected,
              RateLimited):
        assert issubclass(t, ExchangeError)
    assert issubclass(StaleData, ReadUnknown), \
        "StaleData is a special case of UNKNOWN"
    # VenueRejected demands a reason
    try:
        VenueRejected("x")  # type: ignore[call-arg]
    except TypeError:
        pass
    else:
        raise AssertionError("VenueRejected must require reason=")
    e = WriteUnconfirmed("x", may_have_landed=None)
    assert e.may_have_landed is None


CONSTRUCTION_CHECKS: List[Tuple[str, Callable[[], None]]] = [
    ("fillresult_verified_by_construction", _c_fillresult_requires_verified),
    ("slorderinfo_confirmed_by_construction", _c_slorder_requires_confirmed),
    ("flatresult_verified_by_construction", _c_flatresult_requires_verified),
    ("positioninfo_invariants", _c_positioninfo_invariants),
    ("openorderinfo_invariants", _c_openorderinfo_invariants),
    ("no_market_close_in_api", _c_no_market_close_in_api),
    ("abstract_method_set_is_the_17", _c_abstract_method_set),
    ("exception_hierarchy", _c_exception_hierarchy),
]


# ===========================================================================
# 2. Read conformance: method × fault matrix
# ===========================================================================

# read entry points; the lambda gets the client
_READS: List[Tuple[str, Callable[[Any], Any]]] = [
    ("open_positions", lambda c: c.open_positions()),
    ("mark_price", lambda c: c.mark_price("BTC", max_age_sec=5.0)),
    ("candles", lambda c: c.candles("BTC", "4h", limit=50)),
    ("equity_with_upnl", lambda c: c.equity_with_upnl()),
    ("account_value", lambda c: c.account_value()),
    ("margin_used_usd", lambda c: c.margin_used_usd()),
    ("open_orders", lambda c: c.open_orders()),
    ("list_open_sl_orders", lambda c: c.list_open_sl_orders("BTC")),
    ("list_reduce_only_triggers", lambda c: c.list_reduce_only_triggers()),
    ("user_fills", lambda c: c.user_fills()),
    ("position_liquidation", lambda c: c.position_liquidation("BTC")),
]

# fault kind -> (fault kwargs, expected exception types)
_READ_FAULTS: List[Tuple[str, Dict[str, Any], tuple]] = [
    ("timeout", dict(kind="timeout"), (ReadUnknown,)),
    ("connect_error", dict(kind="connect_error"), (ReadUnknown,)),
    ("http_5xx", dict(kind="http_5xx"), (ReadUnknown,)),
    ("empty_body_200", dict(kind="empty_body_200"), (ReadUnknown,)),
    ("malformed_json_200", dict(kind="malformed_json_200"), (ReadUnknown,)),
    ("http_429_exhausted", dict(kind="http_429"), (RateLimited, ReadUnknown)),
    ("slow_beyond_timeout", dict(kind="slow_response", delay_sec=1e9),
     (ReadUnknown,)),
]


def _mk_read_fault_check(read_name: str, read_fn, fault_kw, expected):
    def check(ctx) -> None:
        ctx.scenario.seed_position("BTC", 1.0, 50_000.0)  # so reads are non-trivial
        ctx.gate.add_fault(Fault(op="*", **fault_kw))
        _expect(expected, lambda: read_fn(ctx.client),
                "%s under %s" % (read_name, fault_kw["kind"]))
    check.__name__ = "read_%s_under_%s" % (read_name, fault_kw["kind"])
    return check


# ===========================================================================
# 3. Read happy paths + staleness/positivity semantics
# ===========================================================================

def _r_open_positions_truth(ctx) -> None:
    ctx.scenario.seed_position("BTC", 1.5, 48_000.0, liq_px=30_000.0)
    ctx.scenario.seed_position("ETH", -10.0, 2_600.0)
    pos = ctx.client.open_positions()
    assert set(pos) == {"BTC", "ETH"}, "positions %r" % (set(pos),)
    assert isinstance(pos["BTC"], PositionInfo)
    assert _approx(pos["BTC"].size_signed, 1.5)
    assert _approx(pos["BTC"].entry_px, 48_000.0)
    assert pos["BTC"].is_long and not pos["ETH"].is_long
    assert _approx(pos["ETH"].size_signed, -10.0)


def _r_open_positions_empty_is_positive(ctx) -> None:
    pos = ctx.client.open_positions()
    assert dict(pos) == {}, \
        "flat account must read as {} (positively-empty): %r" % (pos,)


def _r_open_positions_partial_outage_is_unknown(ctx) -> None:
    """One scope down → the WHOLE snapshot is UNKNOWN (HL per-dex canon).
    A dict silently missing one scope is the phantom-guard mass-close class.

    Venue-agnostic: the binding declares WHICH transport subset models a
    partial positions outage (fake: the xyz scope; HL: the HIP-3 dex leg;
    single-endpoint venues: their one snapshot leg — the law degenerates to a
    full-read outage there). No venue-specific substrings live here."""
    sel = ctx.partial_outage_match
    assert sel is not None, (
        "binding %r declares no partial_outage_match selector — every binding "
        "MUST declare the transport subset that models a partial positions "
        "outage (see BoundContext docstring); refusing to silently pass"
        % ctx.venue)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    ctx.gate.add_fault(Fault(kind="http_5xx", op="positions", match=sel))
    _expect((ReadUnknown,), lambda: ctx.client.open_positions(),
            "open_positions with one snapshot leg down")


def _r_mark_price_happy(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 51_234.5)
    px = ctx.client.mark_price("BTC")
    assert _approx(px, 51_234.5), "mark %r != seeded 51234.5" % px
    assert px > 0


def _r_mark_price_cache_inside_tolerance(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 51_000.0)
    first = ctx.client.mark_price("BTC", max_age_sec=5.0)
    ctx.gate.add_fault(Fault(kind="timeout", op="*"))
    # fetch fails now, but the cached value is INSIDE tolerance → legal serve
    second = ctx.client.mark_price("BTC", max_age_sec=5.0)
    assert _approx(first, second)


def _r_mark_price_stale_beyond_tolerance(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 51_000.0)
    ctx.client.mark_price("BTC", max_age_sec=5.0)  # warm cache
    ctx.gate.add_fault(Fault(kind="timeout", op="*"))
    time.sleep(0.06)
    e = _expect((StaleData, ReadUnknown),
                lambda: ctx.client.mark_price("BTC", max_age_sec=0.02),
                "mark_price with only a stale cache")
    if isinstance(e, StaleData) and e.age_sec is not None:
        assert e.age_sec > 0.02


def _r_candles_happy_shape(ctx) -> None:
    ctx.scenario.seed_candles("BTC", "4h", n=50, lag_bars=0.0)
    df = ctx.client.candles("BTC", "4h", limit=50)
    cols = list(df.columns)
    assert cols[:6] == ["time", "Open", "High", "Low", "Close", "Volume"], \
        "candle columns %r" % (cols,)
    assert len(df) > 0 and not df.empty


def _r_candles_stale_beyond_tolerance(ctx) -> None:
    ctx.scenario.seed_candles("BTC", "4h", n=50, lag_bars=3.0)
    _expect((StaleData,),
            lambda: ctx.client.candles("BTC", "4h", max_stale_bars=1.0),
            "candles 3 bars stale with tolerance 1")


def _r_candles_bad_interval_is_caller_bug(ctx) -> None:
    try:
        ctx.client.candles("BTC", "7m")
    except ValueError:
        return
    except ExchangeError as e:
        raise AssertionError(
            "unsupported interval must raise ValueError (caller bug), got %s"
            % type(e).__name__)
    raise AssertionError("unsupported interval returned instead of raising")


def _r_candles_unknown_coin_is_caller_bug(ctx) -> None:
    try:
        ctx.client.candles("NOPECOIN", "4h")
    except ValueError:
        return
    except ExchangeError as e:
        raise AssertionError(
            "unknown coin must raise ValueError (caller bug), got %s"
            % type(e).__name__)
    raise AssertionError("unknown coin returned instead of raising")


def _r_equity_and_sizing_bases(ctx) -> None:
    """Each account method must track ITS OWN venue truth (no cross-
    contamination between the MM-cap denominator and the sizing basis).

    UNIFIED-MARGIN venues (HL: spot USDC IS the perp collateral — ONE account
    equity number, census H11 alias; pacifica: the deployed sizing basis IS
    account_equity, F5 decision) declare `scenario.unified_equity_basis =
    True` in their binding: seeding two DIFFERENT truths is unrepresentable
    on such a venue's wire, so both methods are held to the ONE seeded number
    instead — truth propagation is still fully asserted, only the two-bases
    split degenerates by design.

    MARGIN-IDENTITY venues (nado: the wire DERIVES margin_used == h2 - h0 ==
    equity - account_value, F3) declare `scenario.margin_identity_basis =
    True`: an INDEPENDENT margin_used seed is unrepresentable there, so the
    check seeds an identity-consistent triple — three DISTINCT values, all
    still venue-truth-propagated."""
    if getattr(ctx.scenario, "unified_equity_basis", False):
        ctx.scenario.seed_account(equity=12_345.0, account_value=12_345.0,
                                  margin_used=678.0)
        assert _approx(ctx.client.equity_with_upnl(), 12_345.0)
        assert _approx(ctx.client.account_value(), 12_345.0)
        assert _approx(ctx.client.margin_used_usd(), 678.0)
        return
    if getattr(ctx.scenario, "margin_identity_basis", False):
        # margin_used = equity - account_value (the venue identity):
        # 12_345 - 12_000 = 345 — NOT an arbitrary number
        ctx.scenario.seed_account(equity=12_345.0, account_value=12_000.0,
                                  margin_used=345.0)
        assert _approx(ctx.client.equity_with_upnl(), 12_345.0)
        assert _approx(ctx.client.account_value(), 12_000.0)
        assert _approx(ctx.client.margin_used_usd(), 345.0)
        return
    ctx.scenario.seed_account(equity=12_345.0, account_value=12_000.0,
                              margin_used=678.0)
    assert _approx(ctx.client.equity_with_upnl(), 12_345.0)
    assert _approx(ctx.client.account_value(), 12_000.0)
    assert _approx(ctx.client.margin_used_usd(), 678.0)


def _r_position_liquidation_semantics(ctx) -> None:
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0, liq_px=41_000.0)
    liq = ctx.client.position_liquidation("BTC")
    assert liq is not None and _approx(liq, 41_000.0)
    assert ctx.client.position_liquidation("ETH") is None, \
        "no position must read as POSITIVE None"


def _r_transient_429_survives(ctx) -> None:
    """One 429 then clean — the binding's internal retry budget absorbs it."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="http_429", op="*", count=1))
    px = ctx.client.mark_price("BTC")
    assert px > 0


def _r_list_sl_orders_positive_empty(ctx) -> None:
    assert list(ctx.client.list_open_sl_orders("BTC")) == []
    assert list(ctx.client.list_reduce_only_triggers()) == []
    assert list(ctx.client.open_orders()) == []
    assert list(ctx.client.user_fills()) == []


def _r_open_positions_corrupt_item_never_dropped(ctx) -> None:
    """Per-item-corrupt law (census 3c — exchange_nado per-item
    drop-and-continue class): a snapshot whose envelope is valid but holds ONE
    unparseable position row must raise ReadUnknown (abort the WHOLE read) or
    return with the item VERIFIED-resolved against venue truth — NEVER
    silently drop the item (a dropped LIVE position is the phantom-guard
    auto-close class)."""
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    ctx.scenario.seed_position("ETH", -2.0, 2_500.0)
    ctx.gate.add_fault(Fault(kind="corrupt_item", op="positions"))
    try:
        pos = ctx.client.open_positions()
    except ReadUnknown:
        return  # correct: one bad row poisons the whole snapshot
    except Exception as e:
        raise AssertionError(
            "open_positions with a corrupt row raised %s(%s) — must be "
            "ReadUnknown (typed) or a fully-resolved snapshot"
            % (type(e).__name__, e))
    # returned a snapshot: acceptable ONLY if nothing was dropped and every
    # value matches venue truth (i.e. the corrupt item was truly RESOLVED)
    assert set(pos) == {"BTC", "ETH"}, (
        "corrupt position row silently DROPPED: snapshot %r but the venue "
        "truly holds {'BTC','ETH'} — the per-item drop-and-continue class"
        % (set(pos),))
    for coin in ("BTC", "ETH"):
        truth = ctx.scenario.position(coin)
        assert truth is not None and \
            _approx(pos[coin].size_signed, float(truth["size_signed"])), \
            "corrupt row %s returned UNVERIFIED size %r (venue truth %r)" \
            % (coin, pos[coin].size_signed, truth and truth["size_signed"])


def _r_open_orders_corrupt_item_never_dropped(ctx) -> None:
    """Same law for the order book: one unparseable order row must never
    silently vanish from open_orders — an invisible live SL/TP is the
    false-naked / orphan-sweep-blindness class."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_order("BTC", "sell", 1.0, limit_px=55_000.0,
                            reduce_only=True)
    ctx.scenario.seed_order("ETH", "sell", 3.0, trigger_px=2_000.0,
                            reduce_only=True, is_trigger=True)
    ctx.gate.add_fault(Fault(kind="corrupt_item", op="orders"))
    try:
        orders = list(ctx.client.open_orders())
    except ReadUnknown:
        return
    except Exception as e:
        raise AssertionError(
            "open_orders with a corrupt row raised %s(%s) — must be "
            "ReadUnknown (typed) or a fully-resolved listing"
            % (type(e).__name__, e))
    assert len(orders) == 2, (
        "corrupt order row silently DROPPED: %d of 2 venue-live orders "
        "returned" % len(orders))
    for o in orders:
        assert ctx.scenario.has_order(o.oid), \
            "open_orders returned oid %r that venue truth does not hold" \
            % (o.oid,)


def _r_user_fills_corrupt_item_never_dropped(ctx) -> None:
    """Per-item-corrupt law extended to the fills feed (F7d, P10/N11 class):
    one unparseable fill row must poison the WHOLE read (ReadUnknown) or the
    listing must come back complete and fully parseable — never a silently
    dropped row AND never the corrupted row served as data (consumers'
    float() parses would then drop it silently at attribution time — a
    persistently-invisible fill)."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    truth_n = len(ctx.scenario.fills())
    assert truth_n >= 2, "venue truth should hold the two entry fills"
    ctx.gate.add_fault(Fault(kind="corrupt_item", op="fills"))
    try:
        rows = list(ctx.client.user_fills())
    except ReadUnknown:
        return  # correct: one bad row poisons the whole read
    except Exception as e:
        raise AssertionError(
            "user_fills with a corrupt row raised %s(%s) — must be "
            "ReadUnknown (typed) or a complete verified listing"
            % (type(e).__name__, e))
    assert len(rows) >= truth_n, (
        "corrupt fill row silently DROPPED: %d rows returned, venue truly "
        "holds %d fills" % (len(rows), truth_n))
    for r in rows:
        assert "corrupt-item-0xDEAD" not in str(dict(r)), (
            "corrupt fill row served AS DATA %r — consumers will silently "
            "skip it at parse time (invisible-fill class)" % (dict(r),))


def _r_candles_unknown_coin_meta_refresh_down(ctx) -> None:
    """F7e: unknown coin -> ValueError (caller bug) is legal ONLY off a
    VERIFIED meta. When the binding resolves unknown coins via a LIVE meta
    refresh and that refresh dies on transport, coin liveness is UNKNOWN —
    ReadUnknown, never ValueError (a newly-listed coin would be mislabelled
    a caller bug for the outage duration)."""
    if not getattr(ctx.scenario, "meta_refresh_on_unknown", False):
        raise SkipCheck(
            "binding resolves unknown coins from construction-verified meta "
            "(no live refresh path) — ValueError is correct there")
    ctx.gate.add_fault(Fault(kind="timeout", op="meta"))
    _expect((ReadUnknown,),
            lambda: ctx.client.candles("NEWLYLISTED", "4h"),
            "candles(unknown coin) with the meta refresh transport down")


# ===========================================================================
# 4. Write conformance
# ===========================================================================

def _w_market_open_happy_cross_checked(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    fr = ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    assert isinstance(fr, FillResult) and fr.readback_verified
    truth = ctx.scenario.last_fill()
    assert truth is not None, "venue recorded no fill but adapter returned one"
    assert _approx(fr.size, float(truth["size"])), \
        "FillResult.size %r != venue truth %r (fabrication?)" \
        % (fr.size, truth["size"])
    assert _approx(fr.avg_px, float(truth["px"])), \
        "FillResult.avg_px %r != venue fill px %r — mark/reference echo is " \
        "the fabricated-fill class" % (fr.avg_px, truth["px"])
    pos = ctx.scenario.position("BTC")
    assert pos is not None and _approx(pos["size_signed"], 0.5)
    assert _approx(fr.fill_ratio, 1.0)


def _w_market_open_partial_visible(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="accept_partial_land", op="place_order",
                             partial_ratio=0.5))
    try:
        fr = ctx.client.market_open("BTC", is_buy=True, sz=1.0)
    except WriteUnconfirmed:
        # acceptable ONLY if the venue truly has nothing attributable —
        # here it does, so this is a conformance failure
        truth = ctx.scenario.last_fill()
        raise AssertionError(
            "market_open raised WriteUnconfirmed but the venue holds an "
            "attributable partial fill %r — readback must find it" % (truth,))
    assert _approx(fr.size, 0.5), \
        "partial fill hidden: FillResult.size=%r, venue landed 0.5 " \
        "(request-echo is the defect)" % fr.size
    assert _approx(fr.requested_size, 1.0)
    assert fr.fill_ratio < 0.75


def _w_market_open_accepted_but_nothing_landed(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="accept_no_land", op="place_order"))
    e = _expect((WriteUnconfirmed,),
                lambda: ctx.client.market_open("BTC", is_buy=True, sz=0.5),
                "market_open acked-but-nothing-landed")
    assert ctx.scenario.position("BTC") is None
    assert isinstance(e, WriteUnconfirmed)


def _w_market_open_timeout_after_land_recovers_truth(ctx) -> None:
    """Response lost AFTER the fill landed — the HL confirm-via-positions
    pattern must recover the REAL fill via readback, not raise blindly and
    not fabricate."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="timeout_after_land", op="place_order"))
    fr = ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    truth = ctx.scenario.last_fill()
    assert truth is not None
    assert _approx(fr.size, float(truth["size"]))
    assert _approx(fr.avg_px, float(truth["px"]))


def _w_market_open_reset_after_land_recovers_truth(ctx) -> None:
    """F1: the connection dies AFTER the venue accepted and LANDED the fill
    (reset-by-peer — the same transport exception TYPE as a connect failure,
    but NOT establishment-shaped). Classifying every connection error as
    may_have_landed=False ('provably never left') skips the fills readback
    and hides the landed fill — the untracked-position class. The client
    must readback and return the REAL fill."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="reset_after_land", op="place_order"))
    fr = ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    truth = ctx.scenario.last_fill()
    assert truth is not None, \
        "venue landed no fill — reset_after_land responder path broken"
    assert _approx(fr.size, float(truth["size"])), \
        "FillResult.size %r != venue truth %r" % (fr.size, truth["size"])
    assert _approx(fr.avg_px, float(truth["px"])), \
        "FillResult.avg_px %r != venue fill px %r" % (fr.avg_px, truth["px"])


def _w_market_open_plain_timeout_unconfirmed(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.gate.add_fault(Fault(kind="timeout", op="place_order"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.market_open("BTC", is_buy=True, sz=0.5),
            "market_open under send timeout with empty readback")
    assert ctx.scenario.position("BTC") is None


def _w_market_open_definitive_reject(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.set_min_size("BTC", 1.0)
    e = _expect((VenueRejected,),
                lambda: ctx.client.market_open("BTC", is_buy=True, sz=0.5),
                "market_open below min size")
    assert getattr(e, "reason", ""), "VenueRejected without a reason is " \
                                     "unactionable"
    assert ctx.scenario.position("BTC") is None, "reject must leave state " \
                                                 "unchanged"


def _w_market_open_marketability_guard(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    try:
        ctx.client.market_open("BTC", is_buy=True, sz=0.5,
                               intended_px=51_000.0, allow_marketable=False)
    except VenueRejected as e:
        assert "market" in (e.reason or "").lower() or e.reason, e.reason
        assert ctx.scenario.position("BTC") is None
        return
    except NotImplementedError:
        raise SkipCheck("venue has no marketability guard (documented)")
    raise AssertionError(
        "allow_marketable=False with level through the book must raise "
        "VenueRejected('immediately_marketable')")


def _w_ensure_flat_happy(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 2.0, 48_000.0)
    res = ctx.client.ensure_flat("BTC")
    assert isinstance(res, FlatResult) and res.verified_flat
    assert not res.already_flat
    assert _approx(res.closed_size, 2.0)
    assert ctx.scenario.position("BTC") is None, \
        "FlatResult returned but venue still holds the position"
    # exit-fill truth (F7b, wick_sl ×1330 class): the venue applied its own
    # taker SLIP to the close fill — exit_avg_px must be THAT fill px, never
    # a mark/SL-reference echo (SLIP makes an echo numerically detectable),
    # and never None while the venue holds an attributable close fill.
    truth = ctx.scenario.last_fill()
    assert truth is not None, "venue recorded no close fill"
    assert res.exit_avg_px is not None, (
        "exit_avg_px is None but the venue holds an attributable close fill "
        "%r — exit-fill truth lost" % (truth,))
    assert _approx(res.exit_avg_px, float(truth["px"])), (
        "exit_avg_px %r != venue close-fill px %r — mark/reference echo "
        "(fill-truth class)" % (res.exit_avg_px, truth["px"]))


def _w_ensure_flat_idempotent(ctx) -> None:
    res = ctx.client.ensure_flat("BTC")
    assert res.already_flat and res.closed_size == 0.0


def _w_ensure_flat_close_noop_unconfirmed(ctx) -> None:
    """Close acked but the position never shrinks → WriteUnconfirmed, never
    an ok-shaped return (audit defects #1-#3 class)."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 2.0, 48_000.0)
    ctx.gate.add_fault(Fault(kind="accept_no_land", op="place_order"))
    e = _expect((WriteUnconfirmed,), lambda: ctx.client.ensure_flat("BTC"),
                "ensure_flat with close-acked-but-noop")
    assert ctx.scenario.position("BTC") is not None
    assert e.may_have_landed is not False


def _w_ensure_flat_final_readback_fail_unproven(ctx) -> None:
    """Initial position read succeeds, close goes out, the CONFIRMING read
    fails → flatness unproven → WriteUnconfirmed (never a blind FlatResult)."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 2.0, 48_000.0)
    per_read = getattr(ctx.scenario, "positions_read_transport_calls", 1)
    ctx.gate.add_fault(Fault(kind="timeout", op="positions",
                             fire_after=per_read))
    _expect((WriteUnconfirmed,), lambda: ctx.client.ensure_flat("BTC"),
            "ensure_flat with final position readback down")


def _w_ensure_flat_initial_read_fail_sends_nothing(ctx) -> None:
    ctx.scenario.seed_position("BTC", 2.0, 48_000.0)
    ctx.gate.add_fault(Fault(kind="timeout", op="positions"))
    _expect((ReadUnknown,), lambda: ctx.client.ensure_flat("BTC"),
            "ensure_flat with initial read down")
    sent = [o for o in ctx.gate.ops_called()
            if o in ("place_order", "place_trigger")]
    assert not sent, ("ensure_flat sent %r despite UNKNOWN initial state — "
                      "must send nothing" % sent)
    assert ctx.scenario.position("BTC") is not None


def _w_trigger_sl_happy_live_listed(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                               trigger_px=48_000.0)
    assert isinstance(sl, SLOrderInfo) and sl.readback_verified
    assert sl.oid, "SLOrderInfo without oid"
    # venue-truth cross-check via the scenario's canonical oid matcher (mock
    # id shapes live on the mock's control surface, never in this check)
    assert ctx.scenario.has_live_trigger("BTC", sl.oid), \
        "returned SL oid %r not among venue live triggers %r" \
        % (sl.oid, ctx.scenario.trigger_oids("BTC"))
    listed = ctx.client.list_open_sl_orders("BTC")
    assert any(str(sl.oid) == str(x) for x in listed), \
        "trigger_sl oid %r not visible via list_open_sl_orders %r " \
        "(place/list handle mismatch)" % (sl.oid, list(listed))


def _w_trigger_sl_naked_success_killed(ctx) -> None:
    """Venue acks with an oid but the trigger never appears in the live list
    — THE naked-success class. Must raise WriteUnconfirmed."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    ctx.gate.add_fault(Fault(kind="accept_no_land", op="place_trigger"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                                          trigger_px=48_000.0),
            "trigger_sl acked-but-never-listed")
    assert ctx.scenario.trigger_oids("BTC") == []


def _w_trigger_sl_min_size_reject(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    ctx.scenario.set_min_size("BTC", 10.0)
    e = _expect((VenueRejected,),
                lambda: ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                                              trigger_px=48_000.0),
                "trigger_sl below min trigger size (nado ×10 NAKED class)")
    assert getattr(e, "reason", ""), "reject must carry the venue's reason"


def _w_trigger_sl_foreign_shape_not_adopted(ctx) -> None:
    """F6: our trigger LANDS but the response is lost, while a FOREIGN
    same-side same-size trigger (manual SL) rests at a DIFFERENT px.
    Ambiguity recovery must match on PX (within a tick) and require a UNIQUE
    match — adopting the foreign trigger as 'ours' lets the trail later
    cancel the manual SL (naked class). Legal outcomes: OUR trigger
    confirmed, or WriteUnconfirmed (refusing to guess). Never the foreign
    one."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    foreign = ctx.scenario.seed_order("BTC", "sell", 1.0,
                                      trigger_px=47_000.0, reduce_only=True,
                                      is_trigger=True)
    ctx.gate.add_fault(Fault(kind="timeout_after_land", op="place_trigger"))
    try:
        sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                                   trigger_px=48_000.0)
    except WriteUnconfirmed:
        return  # refusing to claim/guess ownership is contract-legal
    px = ctx.scenario.trigger_px_of(sl.oid)
    assert px is not None, (
        "trigger_sl returned oid %r that is NOT live on the venue" % (sl.oid,))
    assert _approx(px, 48_000.0), (
        "trigger_sl adopted a FOREIGN trigger: returned oid %r has venue "
        "trigger px %r (ours is 48000; foreign %r rests at 47000) — px-blind "
        "shape matching, the cancel-the-manual-SL naked class"
        % (sl.oid, px, foreign))


def _w_trigger_sl_confirm_read_down_unconfirmed(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    ctx.gate.add_fault(Fault(kind="timeout", op="triggers"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                                          trigger_px=48_000.0),
            "trigger_sl with live-list confirm read down")


def _w_cancel_sl_happy_confirmed_gone(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                               trigger_px=48_000.0)
    ctx.client.cancel_sl_order("BTC", sl.oid)
    assert ctx.scenario.trigger_oids("BTC") == [], \
        "cancel returned but venue still lists the trigger"


def _w_cancel_sl_idempotent_already_gone(ctx) -> None:
    # cancelling an oid that never existed = goal state already holds
    # (the scenario mints a venue-canonically-shaped never-existed oid)
    ctx.client.cancel_sl_order("BTC", ctx.scenario.unknown_oid())


def _w_cancel_sl_acked_but_still_live(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                               trigger_px=48_000.0)
    ctx.gate.add_fault(Fault(kind="accept_no_land", op="cancel_order"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.cancel_sl_order("BTC", sl.oid),
            "cancel acked but order still live")
    assert ctx.scenario.trigger_oids("BTC") != []


def _w_cancel_sl_gone_readback_down(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
    sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                               trigger_px=48_000.0)
    ctx.gate.add_fault(Fault(kind="timeout", op="orders"))
    ctx.gate.add_fault(Fault(kind="timeout", op="triggers"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.cancel_sl_order("BTC", sl.oid),
            "cancel with gone-confirm listing down")


def _w_limit_reduce_only_happy(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 48_000.0)
    o = ctx.client.limit_reduce_only("BTC", is_buy=False, sz=0.5,
                                     px=55_000.0)
    assert isinstance(o, OpenOrderInfo)
    assert o.reduce_only and o.oid
    assert ctx.scenario.has_order(o.oid), \
        "TP1 limit oid %r not on the venue's book %r after return" \
        % (o.oid, ctx.scenario.order_oids())


def _w_limit_reduce_only_acked_not_resting(ctx) -> None:
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("BTC", 1.0, 48_000.0)
    ctx.gate.add_fault(Fault(kind="accept_no_land", op="place_order"))
    _expect((WriteUnconfirmed,),
            lambda: ctx.client.limit_reduce_only("BTC", is_buy=False,
                                                 sz=0.5, px=55_000.0),
            "limit_reduce_only acked but neither resting nor filled")


def _w_limit_reduce_only_violation_reject(ctx) -> None:
    # no position → reduce-only cannot rest/fill
    ctx.scenario.seed_mark("BTC", 50_000.0)
    _expect((VenueRejected,),
            lambda: ctx.client.limit_reduce_only("BTC", is_buy=True,
                                                 sz=0.5, px=45_000.0),
            "reduce-only limit with no position")


def _w_update_leverage_semantics(ctx) -> None:
    assert ctx.client.update_leverage("BTC", 5) is None
    if getattr(ctx.scenario, "leverage_clamp_to_max", False):
        # F5 deployed-semantics venues (pacifica): an over-max ask silently
        # clamps to the venue max and SUCCEEDS. The venue itself rejects
        # anything above max, so a clean None return PROVES the EFFECTIVE
        # (clamped) value is what actually got sent.
        assert ctx.client.update_leverage("BTC", 999) is None, \
            "clamp venue: over-max ask must clamp-to-max and succeed"
        return
    _expect((VenueRejected, WriteUnconfirmed),
            lambda: ctx.client.update_leverage("BTC", 999),
            "update_leverage above venue max")


def _w_writes_under_transport_faults_typed(ctx) -> None:
    """No write path may leak a raw transport exception or return a dict."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_position("ETH", 1.0, 2_500.0)
    ctx.gate.add_fault(Fault(kind="connect_error", op="place_trigger"))
    _expect((WriteUnconfirmed, VenueRejected, ReadUnknown),
            lambda: ctx.client.trigger_sl("ETH", is_buy=False, sz=1.0,
                                          trigger_px=2_000.0),
            "trigger_sl under connect error")
    ctx.gate.clear_faults()
    ctx.gate.add_fault(Fault(kind="malformed_json_200", op="place_order"))
    _expect((WriteUnconfirmed, VenueRejected, ReadUnknown),
            lambda: ctx.client.market_open("BTC", is_buy=True, sz=0.5),
            "market_open with unreadable ack")


# ===========================================================================
# 5. Harness integrity
# ===========================================================================

def _h_unintercepted_call_fails_loud(ctx) -> None:
    try:
        ctx.gate.request("GET", "fake://venue/never-simulated-endpoint",
                         timeout=1.0)
    except UninterceptedRealCall:
        return
    raise AssertionError(
        "gate served an endpoint no responder simulates — intercept-all "
        "guard broken")


def _h_client_never_calls_unbounded(ctx) -> None:
    """After a full happy-path workout, every transport call must have carried
    a timeout (the contract's timeout law, audit defect #0 class)."""
    ctx.scenario.seed_mark("BTC", 50_000.0)
    ctx.scenario.seed_candles("BTC", "4h", n=30)
    ctx.client.open_positions()
    ctx.client.mark_price("BTC")
    ctx.client.candles("BTC", "4h", limit=30)
    ctx.client.equity_with_upnl()
    fr = ctx.client.market_open("BTC", is_buy=True, sz=0.5)
    sl = ctx.client.trigger_sl("BTC", is_buy=False, sz=fr.size,
                               trigger_px=45_000.0)
    ctx.client.cancel_sl_order("BTC", sl.oid)
    ctx.client.ensure_flat("BTC")
    unbounded = [c for c in ctx.gate.calls if c.get("timeout") is None]
    assert not unbounded, (
        "%d transport call(s) carried NO timeout (first: %s %s op=%s) — "
        "timeout-law violation"
        % (len(unbounded), unbounded[0]["method"], unbounded[0]["url"],
           unbounded[0]["op"]))


def _h_passthrough_surface_verbatim(ctx) -> None:
    """F4 / mapping §6: every venue wrapper must re-export the non-contract
    caller surface (asset / round_* / slip_per_side / ... — whatever ITS raw
    adapter exposes and its callers use) as CONTRACT-EXEMPT VERBATIM
    delegations, declared in CONTRACT_EXEMPT_PASSTHROUGH — otherwise the P3
    repoint AttributeErrors on the first trader/scanner tick. Verbatim-ness
    is proven with a sentinel probe on the raw adapter instance."""
    if ctx.raw_adapter is None:
        raise SkipCheck("reference binding has no raw adapter behind it")
    names = tuple(getattr(type(ctx.client),
                          "CONTRACT_EXEMPT_PASSTHROUGH", ()))
    assert names, (
        "venue wrapper declares no CONTRACT_EXEMPT_PASSTHROUGH surface — "
        "P3 repoint would AttributeError on the non-contract callers "
        "(mapping §6)")
    sentinel = object()
    for n in names:
        fn = getattr(ctx.client, n, None)
        assert callable(fn), "wrapper passthrough %r missing/not callable" % n
        assert hasattr(ctx.raw_adapter, n), (
            "wrapper declares passthrough %r but the raw adapter has no such "
            "attribute" % n)
        setattr(ctx.raw_adapter, n, lambda *a, **kw: sentinel)
        try:
            got = getattr(ctx.client, n)()
        finally:
            delattr(ctx.raw_adapter, n)  # restore the class-level original
        assert got is sentinel, (
            "passthrough %r did not delegate verbatim to the raw adapter "
            "(returned %r)" % (n, got))


CONFORMANCE_CHECKS: List[Tuple[str, Callable[[Any], None]]] = []

# read × fault matrix
for _rn, _rf in _READS:
    for _fn, _fkw, _exp in _READ_FAULTS:
        CONFORMANCE_CHECKS.append((
            "read_%s__%s" % (_rn, _fn),
            _mk_read_fault_check(_rn, _rf, dict(_fkw), _exp)))

CONFORMANCE_CHECKS += [
    # read semantics
    ("read_open_positions_truth", _r_open_positions_truth),
    ("read_open_positions_empty_is_positive",
     _r_open_positions_empty_is_positive),
    ("read_open_positions_partial_outage_unknown",
     _r_open_positions_partial_outage_is_unknown),
    ("read_mark_price_happy", _r_mark_price_happy),
    ("read_mark_price_cache_inside_tolerance",
     _r_mark_price_cache_inside_tolerance),
    ("read_mark_price_stale_beyond_tolerance",
     _r_mark_price_stale_beyond_tolerance),
    ("read_candles_happy_shape", _r_candles_happy_shape),
    ("read_candles_stale_beyond_tolerance",
     _r_candles_stale_beyond_tolerance),
    ("read_candles_bad_interval_caller_bug",
     _r_candles_bad_interval_is_caller_bug),
    ("read_candles_unknown_coin_caller_bug",
     _r_candles_unknown_coin_is_caller_bug),
    ("read_equity_and_sizing_bases", _r_equity_and_sizing_bases),
    ("read_position_liquidation_semantics",
     _r_position_liquidation_semantics),
    ("read_transient_429_survives", _r_transient_429_survives),
    ("read_lists_positive_empty", _r_list_sl_orders_positive_empty),
    ("read_open_positions_corrupt_item_never_dropped",
     _r_open_positions_corrupt_item_never_dropped),
    ("read_open_orders_corrupt_item_never_dropped",
     _r_open_orders_corrupt_item_never_dropped),
    ("read_user_fills_corrupt_item_never_dropped",
     _r_user_fills_corrupt_item_never_dropped),
    ("read_candles_unknown_coin_meta_refresh_down",
     _r_candles_unknown_coin_meta_refresh_down),
    # writes
    ("write_market_open_happy_cross_checked",
     _w_market_open_happy_cross_checked),
    ("write_market_open_partial_visible", _w_market_open_partial_visible),
    ("write_market_open_accept_no_land",
     _w_market_open_accepted_but_nothing_landed),
    ("write_market_open_timeout_after_land_recovers",
     _w_market_open_timeout_after_land_recovers_truth),
    ("write_market_open_reset_after_land_recovers",
     _w_market_open_reset_after_land_recovers_truth),
    ("write_market_open_plain_timeout_unconfirmed",
     _w_market_open_plain_timeout_unconfirmed),
    ("write_market_open_definitive_reject", _w_market_open_definitive_reject),
    ("write_market_open_marketability_guard",
     _w_market_open_marketability_guard),
    ("write_ensure_flat_happy", _w_ensure_flat_happy),
    ("write_ensure_flat_idempotent", _w_ensure_flat_idempotent),
    ("write_ensure_flat_close_noop_unconfirmed",
     _w_ensure_flat_close_noop_unconfirmed),
    ("write_ensure_flat_final_readback_fail_unproven",
     _w_ensure_flat_final_readback_fail_unproven),
    ("write_ensure_flat_initial_read_fail_sends_nothing",
     _w_ensure_flat_initial_read_fail_sends_nothing),
    ("write_trigger_sl_happy_live_listed", _w_trigger_sl_happy_live_listed),
    ("write_trigger_sl_naked_success_killed",
     _w_trigger_sl_naked_success_killed),
    ("write_trigger_sl_min_size_reject", _w_trigger_sl_min_size_reject),
    ("write_trigger_sl_foreign_shape_not_adopted",
     _w_trigger_sl_foreign_shape_not_adopted),
    ("write_trigger_sl_confirm_read_down_unconfirmed",
     _w_trigger_sl_confirm_read_down_unconfirmed),
    ("write_cancel_sl_happy_confirmed_gone",
     _w_cancel_sl_happy_confirmed_gone),
    ("write_cancel_sl_idempotent_already_gone",
     _w_cancel_sl_idempotent_already_gone),
    ("write_cancel_sl_acked_but_still_live",
     _w_cancel_sl_acked_but_still_live),
    ("write_cancel_sl_gone_readback_down", _w_cancel_sl_gone_readback_down),
    ("write_limit_reduce_only_happy", _w_limit_reduce_only_happy),
    ("write_limit_reduce_only_acked_not_resting",
     _w_limit_reduce_only_acked_not_resting),
    ("write_limit_reduce_only_violation_reject",
     _w_limit_reduce_only_violation_reject),
    ("write_update_leverage_semantics", _w_update_leverage_semantics),
    ("write_transport_faults_always_typed",
     _w_writes_under_transport_faults_typed),
    # harness integrity
    ("harness_unintercepted_call_fails_loud",
     _h_unintercepted_call_fails_loud),
    ("harness_timeout_law_every_call_bounded",
     _h_client_never_calls_unbounded),
    ("venue_passthrough_surface_verbatim",
     _h_passthrough_surface_verbatim),
]


# ===========================================================================
# 5b. DRY-mode write-isolation lane (F2 regression guard for the WHOLE class)
# ===========================================================================
# Run against a context built with DRY_RUN=1 (binding.build_context(
# dry_run=True)): under DRY, NO write op may ever reach the transport —
# updateLeverage bypassing the raw _dry_guard signed and SENT a real leverage
# change (F2). Typed fail-closed refusals (WriteUnconfirmed(False)) or benign
# successes are both legal; a write op on the gate is not.

_WRITE_OPS = frozenset({"place_order", "place_trigger", "cancel_order",
                        "leverage"})


def _mk_dry_write_check(method_name: str, do: Callable[[Any, str], Any]):
    def check(ctx) -> None:
        ctx.scenario.seed_mark("BTC", 50_000.0)
        ctx.scenario.seed_position("BTC", 1.0, 50_000.0)
        oid = ctx.scenario.seed_order("BTC", "sell", 1.0,
                                      trigger_px=48_000.0, reduce_only=True,
                                      is_trigger=True)
        pre_fills = len(ctx.scenario.fills())
        pre_orders = set(ctx.scenario.order_oids())
        try:
            do(ctx, oid)
        except ExchangeError:
            pass  # typed fail-closed refusal under DRY is legal
        sent = [o for o in ctx.gate.ops_called() if o in _WRITE_OPS]
        assert not sent, (
            "DRY_RUN=1 but %s pushed write op(s) %r onto the transport — "
            "DRY guard bypassed (F2 class)" % (method_name, sent))
        assert len(ctx.scenario.fills()) == pre_fills, \
            "DRY_RUN=1 but %s changed the venue fills" % method_name
        assert set(ctx.scenario.order_oids()) == pre_orders, \
            "DRY_RUN=1 but %s changed the venue order book" % method_name
    check.__name__ = "dry_%s_sends_nothing" % method_name
    return check


DRY_WRITE_CHECKS: List[Tuple[str, Callable[[Any], None]]] = [
    ("dry_market_open_sends_nothing", _mk_dry_write_check(
        "market_open",
        lambda c, o: c.client.market_open("BTC", is_buy=True, sz=0.5))),
    ("dry_ensure_flat_sends_nothing", _mk_dry_write_check(
        "ensure_flat", lambda c, o: c.client.ensure_flat("BTC"))),
    ("dry_trigger_sl_sends_nothing", _mk_dry_write_check(
        "trigger_sl",
        lambda c, o: c.client.trigger_sl("BTC", is_buy=False, sz=1.0,
                                         trigger_px=48_500.0))),
    ("dry_cancel_sl_sends_nothing", _mk_dry_write_check(
        "cancel_sl_order", lambda c, o: c.client.cancel_sl_order("BTC", o))),
    ("dry_limit_reduce_only_sends_nothing", _mk_dry_write_check(
        "limit_reduce_only",
        lambda c, o: c.client.limit_reduce_only("BTC", is_buy=False, sz=0.5,
                                                px=55_000.0))),
    ("dry_update_leverage_sends_nothing", _mk_dry_write_check(
        "update_leverage", lambda c, o: c.client.update_leverage("BTC", 5))),
]


# ===========================================================================
# 6. P3 scaffolding — crash-at-each-transition (entry state machine)
# ===========================================================================
# These land with the P3 entry state machine (pre-order pending journal row +
# startup reconcile + untracked-protect sweep, HL canon fleet-wide). The
# transition points are fixed NOW so P3 implements against a stable matrix.

P3_TRANSITIONS: List[str] = [
    "pre_send",               # crash before the entry order leaves
    "post_send_pre_confirm",  # crash with the order in flight, no readback yet
    "post_fill_pre_sl",       # crash after fill, before SL placement
    "post_sl_pre_journal",    # crash after SL live, before trade row commit
    "post_journal",           # crash after commit (steady state)
]
