"""fleet_core.conformance.mutation_sensitivity — harness sensitivity proof.

Re-introduces the audited defect classes into the REFERENCE client
(bindings/fake.FakeExchangeClient) one at a time and proves the conformance
checks CATCH each one (≥1 check fails under the mutant, and the unmutated
baseline is fully green). A mutation the suite cannot catch means the harness
has a blind spot for that whole defect class — the run exits non-zero.

The 17 mutations (audit verdict R3 classes + census 3c + F1/F6/F7d):

     1 read_mask_empty          — open_positions masks ReadUnknown to {}
     2 mark_zero_on_fail        — mark_price masks failure to 0.0
     3 mark_stale_serve         — mark_price serves ANY-age cache (no StaleData)
     4 candles_empty_on_fail    — candles masks ReadUnknown to an empty frame
     5 candles_no_staleness     — candles never raises StaleData on lag
     6 fabricated_fill          — market_open fabricates FillResult at mark
     7 request_echo_partial     — market_open echoes the REQUESTED size
     8 blind_flatresult         — ensure_flat returns without final readback
     9 flat_masks_initial_read  — ensure_flat masks initial ReadUnknown to
                                  already_flat
    10 naked_success_sl         — trigger_sl trusts the ack, no live-list
                                  confirm
    11 cancel_no_confirm        — cancel_sl_order trusts the ack, no gone-
                                  readback
    12 unbounded_calls          — every transport call sent with timeout=None
    13 limit_ro_trust_ack       — limit_reduce_only trusts the ack, no
                                  resting/filled readback
    14 per_item_drop            — open_positions/open_orders silently DROP an
                                  unparseable row (nado exchange_nado
                                  drop-and-continue class, census 3c)

    15 connect_reset_false      — EVERY connection error classified
                                  may_have_landed=False (skips the readback
                                  that would find a fill landed before a
                                  mid-flight reset — F1 misclassification)
    16 fills_no_validation      — user_fills serves a corrupt row AS DATA
                                  (no per-item parse validation, F7d)
    17 sl_adopt_foreign_shape   — trigger_sl ambiguity recovery adopts the
                                  first same-side live trigger WITHOUT a px
                                  match (F6 foreign-trigger adoption)

Run:  /usr/bin/python3 fleet_core/conformance/mutation_sensitivity.py
Exit: 0 = baseline green AND all 17 mutations caught; 1 otherwise.
"""

from __future__ import annotations

import sys
from typing import Any, Callable, Dict, List, Optional, Tuple


def _ensure_root_on_path() -> None:
    import os
    here = os.path.abspath(os.path.dirname(__file__))
    root = os.path.dirname(os.path.dirname(here))
    if root not in sys.path:
        sys.path.insert(0, root)


_ensure_root_on_path()

from fleet_core.exchange_api import (  # noqa: E402
    ExchangeError,
    FillResult,
    FlatResult,
    OpenOrderInfo,
    PositionInfo,
    ReadUnknown,
    SLOrderInfo,
    VenueRejected,
    WriteUnconfirmed,
)
from fleet_core.conformance.checks import (  # noqa: E402
    CONFORMANCE_CHECKS,
    SkipCheck,
)
from fleet_core.conformance.bindings import fake as fake_binding  # noqa: E402
from fleet_core.conformance.bindings.fake import (  # noqa: E402
    BASE,
    FakeExchangeClient,
    MiniFrame,
    SCOPES,
)

C = FakeExchangeClient


# ===========================================================================
# The mutations: each is fn() -> dict of {attr_name: mutant_callable} to be
# set on FakeExchangeClient. Originals are captured/restored by the runner.
# ===========================================================================

def _m_read_mask_empty() -> Dict[str, Any]:
    orig = C.open_positions

    def open_positions(self):
        try:
            return orig(self)
        except ExchangeError:
            return {}  # the {}-mask defect

    return {"open_positions": open_positions}


def _m_mark_zero_on_fail() -> Dict[str, Any]:
    orig = C.mark_price

    def mark_price(self, coin, max_age_sec=5.0):
        try:
            return orig(self, coin, max_age_sec)
        except ExchangeError:
            return 0.0  # the 0.0-mark defect

    return {"mark_price": mark_price}


def _m_mark_stale_serve() -> Dict[str, Any]:
    orig = C.mark_price

    def mark_price(self, coin, max_age_sec=5.0):
        return orig(self, coin, max_age_sec=1e12)  # ignore caller tolerance

    return {"mark_price": mark_price}


def _m_candles_empty_on_fail() -> Dict[str, Any]:
    orig = C.candles

    def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
        try:
            return orig(self, coin, interval, limit, max_stale_bars)
        except ExchangeError:
            return MiniFrame([])  # empty-df mask

    return {"candles": candles}


def _m_candles_no_staleness() -> Dict[str, Any]:
    orig = C.candles

    def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
        return orig(self, coin, interval, limit, max_stale_bars=1e12)

    return {"candles": candles}


def _m_fabricated_fill() -> Dict[str, Any]:
    def market_open(self, coin, is_buy, sz, intended_px=None,
                    allow_marketable=True):
        import uuid
        body = {"type": "market", "coin": coin, "is_buy": is_buy, "sz": sz,
                "reduce_only": False, "client_oid": uuid.uuid4().hex}
        try:
            self._write("place_order", BASE + "/order", body, coin=coin)
        except WriteUnconfirmed:
            pass  # the defect: proceed to fabricate anyway
        px = self.mark_price(coin)  # mark echo, NOT the venue fill px
        return FillResult(coin=coin, is_buy=is_buy, avg_px=float(px),
                          size=float(sz), requested_size=float(sz))

    return {"market_open": market_open}


def _m_request_echo_partial() -> Dict[str, Any]:
    orig = C.market_open

    def market_open(self, coin, is_buy, sz, intended_px=None,
                    allow_marketable=True):
        fr = orig(self, coin, is_buy, sz, intended_px, allow_marketable)
        # the defect: echo the REQUESTED size over the venue-read fill size
        return FillResult(coin=fr.coin, is_buy=fr.is_buy, avg_px=fr.avg_px,
                          size=float(sz), requested_size=float(sz),
                          oid=fr.oid)

    return {"market_open": market_open}


def _m_blind_flatresult() -> Dict[str, Any]:
    def ensure_flat(self, coin):
        import uuid
        pos_map = self.open_positions()
        if coin not in pos_map:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        pos = pos_map[coin]
        body = {"type": "market", "coin": coin,
                "is_buy": pos.size_signed < 0,
                "sz": abs(pos.size_signed), "reduce_only": True,
                "client_oid": uuid.uuid4().hex}
        try:
            self._write("place_order", BASE + "/order", body, coin=coin)
        except (VenueRejected, WriteUnconfirmed):
            pass
        # the defect: no position readback — blind "flat" claim
        return FlatResult(coin=coin, already_flat=False,
                          closed_size=abs(pos.size_signed))

    return {"ensure_flat": ensure_flat}


def _m_flat_masks_initial_read() -> Dict[str, Any]:
    orig = C.ensure_flat

    def ensure_flat(self, coin):
        try:
            return orig(self, coin)
        except ReadUnknown:
            # the defect: UNKNOWN initial state read as "already flat"
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)

    return {"ensure_flat": ensure_flat}


def _m_naked_success_sl() -> Dict[str, Any]:
    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        import uuid
        body = {"coin": coin, "is_buy": is_buy, "sz": sz,
                "trigger_px": trigger_px, "client_oid": uuid.uuid4().hex}
        resp = self._write("place_trigger", BASE + "/trigger", body,
                           coin=coin)
        # the defect: ack trusted, NO live-list confirm
        return SLOrderInfo(coin=coin, oid=str(resp.get("oid") or "fab-1"),
                           trigger_px=float(trigger_px), size=float(sz),
                           is_buy_to_close=bool(is_buy))

    return {"trigger_sl": trigger_sl}


def _m_cancel_no_confirm() -> Dict[str, Any]:
    def cancel_sl_order(self, coin, oid):
        try:
            self._write("cancel_order", BASE + "/cancel", {"oid": oid},
                        coin=coin)
        except (VenueRejected, WriteUnconfirmed):
            pass
        return None  # the defect: no confirmed-gone readback

    return {"cancel_sl_order": cancel_sl_order}


def _m_unbounded_calls() -> Dict[str, Any]:
    import time as _time

    from fleet_core.exchange_api import RateLimited

    def _raw(self, method, url, body=None):
        attempts = 1 + self.RETRY_429
        for i in range(attempts):
            g = self.gate.request(method, url, body=body,
                                  timeout=None,  # the defect
                                  transport="fake")
            if g.status == 429:
                if i < attempts - 1:
                    _time.sleep(self.RETRY_429_SLEEP)
                    continue
                raise RateLimited("429 retry budget exhausted on %s" % url,
                                  venue="fake", op=method)
            return g
        raise RateLimited("unreachable", venue="fake")  # pragma: no cover

    return {"_raw": _raw}


def _m_limit_ro_trust_ack() -> Dict[str, Any]:
    def limit_reduce_only(self, coin, is_buy, sz, px):
        import uuid
        body = {"type": "limit", "coin": coin, "is_buy": is_buy, "sz": sz,
                "px": px, "reduce_only": True,
                "client_oid": uuid.uuid4().hex}
        resp = self._write("place_order", BASE + "/order", body, coin=coin)
        # the defect: ack trusted, neither resting- nor filled-readback
        return OpenOrderInfo(coin=coin, oid=str(resp.get("oid") or "fab-2"),
                             side="buy" if is_buy else "sell",
                             size=float(sz), limit_px=float(px),
                             reduce_only=True, is_trigger=False)

    return {"limit_reduce_only": limit_reduce_only}


def _m_per_item_drop() -> Dict[str, Any]:
    """Census 3c: the exchange_nado drop-and-continue class — one unparseable
    row silently vanishes from the snapshot / order listing."""

    def open_positions(self):
        out = {}
        for scope in SCOPES:
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
                except (KeyError, TypeError, ValueError):
                    continue  # the defect: drop-and-continue
        return out

    def open_orders(self):
        data = self._read("orders", "GET", BASE + "/orders")
        out = []
        for o in data.get("orders", []):
            try:
                out.append(self._order_info(o))
            except (KeyError, TypeError, ValueError):
                continue  # the defect: drop-and-continue
        return out

    return {"open_positions": open_positions, "open_orders": open_orders}


def _m_connect_reset_false() -> Dict[str, Any]:
    """F1 misclassification: EVERY connection-shaped write failure re-typed
    as may_have_landed=False ('provably never left') — the caller then skips
    the fills readback and a fill landed before a mid-flight reset is
    hidden (untracked-position class)."""
    orig = C._write

    def _write(self, op, url, body, coin=""):
        try:
            return orig(self, op, url, body, coin)
        except WriteUnconfirmed as e:
            low = str(e).lower()
            if "connect" in low or "connection" in low:
                raise WriteUnconfirmed(
                    "%s (misclassified as never-left)" % e,
                    may_have_landed=False, venue="fake", op=op, coin=coin)
            raise

    return {"_write": _write}


def _m_fills_no_validation() -> Dict[str, Any]:
    """F7d: user_fills serves rows WITHOUT per-item parse validation — a
    corrupt row rides through as data and consumers silently drop it at
    their own float() parse (persistently-invisible fill)."""

    def user_fills(self, max_age_sec=60.0):
        data = self._read("fills", "GET", BASE + "/fills")
        return list(data.get("fills", []))  # the defect: no validation

    return {"user_fills": user_fills}


def _m_sl_adopt_foreign_shape() -> Dict[str, Any]:
    """F6: on an ambiguous trigger placement, adopt the FIRST same-side live
    trigger by SHAPE (no px match, no uniqueness) — a same-side same-size
    foreign trigger (manual SL) at another px is claimed as ours."""

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        import uuid
        body = {"coin": coin, "is_buy": is_buy, "sz": sz,
                "trigger_px": trigger_px, "client_oid": uuid.uuid4().hex}
        oid = ""
        try:
            resp = self._write("place_trigger", BASE + "/trigger", body,
                               coin=coin)
            oid = str(resp.get("oid") or "")
        except WriteUnconfirmed:
            pass  # the defect: ambiguity -> shape-match adoption below
        data = self._read("triggers", "GET",
                          BASE + "/triggers?coin=%s" % coin, coin=coin)
        want = "buy" if is_buy else "sell"
        for t in data.get("triggers", []):
            if oid and t.get("oid") != oid:
                continue
            if not oid and t.get("side") != want:
                continue
            return SLOrderInfo(coin=coin, oid=str(t["oid"]),
                               trigger_px=float(t["trigger_px"]),
                               size=float(t["size"]),
                               is_buy_to_close=(t["side"] == "buy"))
        raise WriteUnconfirmed(
            "trigger_sl: nothing to adopt", may_have_landed=True,
            venue="fake", op="trigger_sl", coin=coin)

    return {"trigger_sl": trigger_sl}


MUTATIONS: List[Tuple[str, Callable[[], Dict[str, Any]]]] = [
    ("read_mask_empty", _m_read_mask_empty),
    ("mark_zero_on_fail", _m_mark_zero_on_fail),
    ("mark_stale_serve", _m_mark_stale_serve),
    ("candles_empty_on_fail", _m_candles_empty_on_fail),
    ("candles_no_staleness", _m_candles_no_staleness),
    ("fabricated_fill", _m_fabricated_fill),
    ("request_echo_partial", _m_request_echo_partial),
    ("blind_flatresult", _m_blind_flatresult),
    ("flat_masks_initial_read", _m_flat_masks_initial_read),
    ("naked_success_sl", _m_naked_success_sl),
    ("cancel_no_confirm", _m_cancel_no_confirm),
    ("unbounded_calls", _m_unbounded_calls),
    ("limit_ro_trust_ack", _m_limit_ro_trust_ack),
    ("per_item_drop", _m_per_item_drop),
    ("connect_reset_false", _m_connect_reset_false),
    ("fills_no_validation", _m_fills_no_validation),
    ("sl_adopt_foreign_shape", _m_sl_adopt_foreign_shape),
]


# ===========================================================================
# Runner
# ===========================================================================

def _run_all_checks(ctx) -> List[Tuple[str, str]]:
    """Run every conformance check on a fresh ctx; return [(name, err)]."""
    failures: List[Tuple[str, str]] = []
    for name, fn in CONFORMANCE_CHECKS:
        try:
            ctx.reset()
            fn(ctx)
        except SkipCheck:
            continue
        except Exception as e:  # noqa: BLE001 — any failure counts
            failures.append((name, "%s: %s" % (type(e).__name__, e)))
    return failures


def main() -> int:
    ctx = fake_binding.build_context()
    try:
        # baseline must be green — otherwise "caught" is meaningless
        base_failures = _run_all_checks(ctx)
        if base_failures:
            print("BASELINE RED — %d check(s) fail unmutated; aborting:"
                  % len(base_failures))
            for n, err in base_failures[:10]:
                print("  %s\n    %s" % (n, err))
            return 1
        print("baseline: %d checks GREEN (unmutated reference client)"
              % len(CONFORMANCE_CHECKS))

        caught = 0
        missed: List[str] = []
        for mname, build in MUTATIONS:
            patch = build()
            saved = {k: C.__dict__.get(k) for k in patch}
            for k, v in patch.items():
                setattr(C, k, v)
            try:
                failures = _run_all_checks(ctx)
            finally:
                for k, old in saved.items():
                    if old is None:
                        try:
                            delattr(C, k)
                        except AttributeError:
                            pass
                    else:
                        setattr(C, k, old)
            if failures:
                caught += 1
                catchers = ", ".join(n for n, _ in failures[:3])
                more = "" if len(failures) <= 3 else " (+%d more)" % (
                    len(failures) - 3)
                print("CAUGHT  %-24s by %d check(s): %s%s"
                      % (mname, len(failures), catchers, more))
            else:
                missed.append(mname)
                print("MISSED  %-24s — NO check failed (harness blind spot)"
                      % mname)

        print("\n%d/%d mutations caught" % (caught, len(MUTATIONS)))
        if missed:
            print("BLIND SPOTS: %s" % ", ".join(missed))
            return 1
        return 0
    finally:
        ctx.close()


if __name__ == "__main__":
    sys.exit(main())
