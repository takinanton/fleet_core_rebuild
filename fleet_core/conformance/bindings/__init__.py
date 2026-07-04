"""fleet_core.conformance.bindings — per-venue harness bindings.

A binding knows how to stand up ONE venue's adapter completely offline:

  * fabricate inert env/keys the adapter's constructor demands (never real
    credentials — throwaway keypairs / zero-value hex);
  * install the transport patchers (fleet_core.conformance.faults) so that
    EVERY outbound call is intercepted — the intercept-all + socket-block
    guard makes a live API hit impossible by construction;
  * provide a venue-shaped Responder that renders a shared neutral truth
    store (the FakeVenue state model from bindings/fake.py) into the JSON the
    real adapter expects, so the SAME conformance checks drive every venue;
  * classify(method, url, body) → canonical op names so checks can target
    faults venue-agnostically.

CANONICAL OPS (the vocabulary `classify` must emit — checks fault on these):
    meta         — market/instrument metadata, universe listings
    positions    — open-positions / clearinghouse state reads
    mark         — mark/mid price reads
    candles      — OHLCV reads
    account      — equity / balance / margin reads
    orders       — open-order listings (incl. account-wide trigger listings)
    triggers     — per-coin SL/trigger listings
    fills        — user fill/trade-history reads
    place_order  — market/limit order submission (entries, closes, TP limits)
    place_trigger— SL/TP trigger submission
    cancel_order — order/trigger cancellation
    leverage     — leverage/margin-mode updates
    other        — anything else (unknown endpoints → responder returns None
                   → UninterceptedRealCall, by law)

Bindings are imported lazily by name — importing this package pulls in NO
venue SDK and no bot code.
"""

from __future__ import annotations

import contextlib
import importlib
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from fleet_core.conformance.faults import TransportGate

__all__ = [
    "BindingUnavailable",
    "BoundContext",
    "set_env",
    "get_binding",
    "VENUES",
]

VENUES = ("fake", "hl", "pacifica", "extended", "nado")


class BindingUnavailable(RuntimeError):
    """This venue binding cannot run HERE (SDK not installed / bot package not
    importable / venue contract implementation not landed yet). The suite
    converts this to a pytest skip with the reason text."""


@dataclass
class BoundContext:
    """Everything a conformance check needs for one venue.

    client    — object implementing fleet_core.exchange_api.ExchangeClient.
    gate      — the TransportGate all of the client's I/O flows through.
    scenario  — venue-truth control (seed positions/marks/candles, read the
                venue's authoritative state for cross-checks). All bindings
                expose the SAME scenario API (see bindings/fake.py
                FakeScenario — the reference).
    make_client — build a FRESH client over the same gate/venue state
                (reset() uses it so per-check cache state starts clean).
    stack     — ExitStack holding the installed patchers; close() unwinds.
    partial_outage_match — REQUIRED per-binding selector: predicate over the
                transport ctx matching the venue-appropriate SUBSET of the
                positions-snapshot calls that models a PARTIAL outage (one
                scope/dex/endpoint of several). Venues whose snapshot is a
                single call declare the whole call (the law degenerates to a
                full-read outage there — still must read as UNKNOWN). The
                partial-outage conformance check refuses to run without it
                (no silent pass, no venue-specific substring hacks).
    """

    venue: str
    client: Any
    gate: TransportGate
    scenario: Any
    make_client: Callable[[], Any]
    stack: contextlib.ExitStack = field(default_factory=contextlib.ExitStack)
    raw_adapter: Any = None  # the underlying bot.exchange_* instance, if any
    partial_outage_match: Optional[Callable[[Dict[str, Any]], bool]] = None

    def reset(self) -> None:
        """Fresh faults, fresh call log, fresh venue truth, fresh client."""
        self.gate.clear_faults()
        self.gate.clear_calls()
        self.scenario.reset()
        self.client = self.make_client()

    def close(self) -> None:
        self.stack.close()


@contextlib.contextmanager
def set_env(env: Dict[str, str]):
    """Temporarily set (and on exit restore) os.environ entries."""
    saved: Dict[str, Optional[str]] = {}
    for k, v in env.items():
        saved[k] = os.environ.get(k)
        os.environ[k] = v
    try:
        yield
    finally:
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


def get_binding(venue: str):
    """Import and return the binding module for `venue` (lazy)."""
    venue = (venue or "").strip().lower()
    if venue not in VENUES:
        raise BindingUnavailable(
            "unknown venue %r — expected one of %s" % (venue, list(VENUES)))
    return importlib.import_module(
        "fleet_core.conformance.bindings.%s" % venue)
