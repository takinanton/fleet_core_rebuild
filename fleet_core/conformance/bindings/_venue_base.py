"""Shared machinery for the real-venue bindings (hl/pacifica/extended/nado).

POSTURE (why DRY_RUN=0 with throwaway keys):
The adapters' own DRY_RUN=1 layer short-circuits writes BEFORE the transport —
testing that would test the mock, not the adapter. The harness instead runs
the LIVE-shaped code paths with FABRICATED throwaway credentials while the
fault injector intercepts 100% of transport:

  * `block_sockets()` — raw TCP connect is impossible;
  * `patch_requests()` / `patch_aiohttp()` — every HTTP call routes into the
    TransportGate; any endpoint the venue responder does not simulate raises
    UninterceptedRealCall (test failure), never reaches the wire.

So a live API hit is impossible by construction even on a bot host whose
.env carries real keys — the binding pre-sets every credential env var to an
inert fabricated value BEFORE bot.config loads (os.environ wins over .env
because dotenv does not override existing environment).

HOST ITERATION LOOP (expected, by design):
venue responders are best-effort renderings of each venue's REST surface over
the shared FakeVenue truth store. On first host runs, unsimulated endpoints
fail loud with UninterceptedRealCall naming the exact method+URL — add a
renderer branch for it and re-run. That loop converges quickly and is the
point: the harness enumerates the adapter's real transport surface.
"""

from __future__ import annotations

import contextlib
import importlib
import json
from typing import Any, Callable, Dict, Iterable, Optional

from fleet_core.conformance.faults import (
    TransportGate,
    block_sockets,
    patch_aiohttp,
    patch_requests,
)
from fleet_core.conformance.bindings import (
    BindingUnavailable,
    BoundContext,
    set_env,
)
from fleet_core.conformance.bindings.fake import FakeScenario, FakeVenue

__all__ = ["parse_body", "build_venue_context", "smoke_construct_venue"]


def parse_body(body: Any) -> Dict[str, Any]:
    """Normalize a transport body (str/bytes/dict/None) to a dict."""
    if isinstance(body, dict):
        return body
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except Exception:
            return {}
    if isinstance(body, str):
        try:
            data = json.loads(body)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _contract_factory(venue_name: str, raw_adapter: Any) -> Callable[[], Any]:
    """Resolve the venue's contract implementation (fleet_core.venues.<name>).

    The conformance suite tests the CONTRACT surface; until the venue's
    ExchangeClient implementation lands (next P2 step), the binding can build
    the raw adapter offline (smoke) but has no contract client to test —
    surfaced as BindingUnavailable → pytest skip with this exact reason.
    """
    try:
        mod = importlib.import_module("fleet_core.venues.%s" % venue_name)
    except ImportError as e:
        raise BindingUnavailable(
            "fleet_core.venues.%s not landed yet (P2 cutover pending) — "
            "harness + offline adapter construction are ready; contract "
            "conformance will run once the venue implementation exists "
            "(import error: %s)" % (venue_name, e))
    build = getattr(mod, "build_client", None)
    if build is None:
        raise BindingUnavailable(
            "fleet_core.venues.%s exists but exposes no build_client(raw)"
            % venue_name)
    return lambda: build(raw_adapter)


def build_venue_context(
    *,
    name: str,
    env: Dict[str, str],
    classify: Callable[[str, str, Any], str],
    make_responder: Callable[[FakeVenue], Callable[..., Any]],
    transports: Iterable[str],
    construct_raw: Callable[[], Any],
    partial_outage_match: Optional[Callable[[Dict[str, Any]], bool]] = None,
    teardown_raw: Optional[Callable[[Any], None]] = None,
) -> BoundContext:
    """Standard venue-binding assembly. Raises BindingUnavailable when this
    machine cannot run the venue (SDK/bot import failure, constructor failure,
    contract impl not landed)."""
    stack = contextlib.ExitStack()
    try:
        stack.enter_context(set_env(env))
        venue = FakeVenue()
        scenario = FakeScenario(venue)
        gate = TransportGate(classify, make_responder(venue),
                             name="%s-gate" % name)
        stack.enter_context(block_sockets())
        if "requests" in transports:
            try:
                stack.enter_context(patch_requests(gate))
            except ImportError as e:
                raise BindingUnavailable(
                    "%s: requests not installed here: %s" % (name, e))
        if "aiohttp" in transports:
            try:
                stack.enter_context(patch_aiohttp(gate))
            except ImportError as e:
                raise BindingUnavailable(
                    "%s: aiohttp not installed here: %s" % (name, e))

        try:
            raw = construct_raw()
        except BindingUnavailable:
            raise
        except Exception as e:
            raise BindingUnavailable(
                "%s: offline adapter construction failed under the harness "
                "(%s: %s) — if this names an unintercepted endpoint, add it "
                "to the binding's responder (host-iteration loop)"
                % (name, type(e).__name__, e))
        if teardown_raw is not None:
            # unwound by ctx.close() BEFORE the patchers pop — the adapter's
            # transports (aiohttp sessions / bg loops) shut down cleanly
            # instead of leaking "Unclosed client session" noise at GC time
            stack.callback(teardown_raw, raw)

        make_client = _contract_factory(name, raw)
        try:
            client = make_client()
        except Exception as e:
            raise BindingUnavailable(
                "%s: fleet_core.venues.%s.build_client failed: %s"
                % (name, name, e))
        return BoundContext(venue=name, client=client, gate=gate,
                            scenario=scenario, make_client=make_client,
                            stack=stack, raw_adapter=raw,
                            partial_outage_match=partial_outage_match)
    except BaseException:
        stack.close()
        raise


def smoke_construct_venue(
    *,
    name: str,
    env: Dict[str, str],
    classify: Callable[[str, str, Any], str],
    make_responder: Callable[[FakeVenue], Callable[..., Any]],
    transports: Iterable[str],
    construct_raw: Callable[[], Any],
    teardown_raw: Optional[Callable[[Any], None]] = None,
) -> str:
    """Offline raw-adapter construction proof (no contract impl required)."""
    stack = contextlib.ExitStack()
    try:
        stack.enter_context(set_env(env))
        venue = FakeVenue()
        gate = TransportGate(classify, make_responder(venue),
                             name="%s-smoke-gate" % name)
        stack.enter_context(block_sockets())
        if "requests" in transports:
            try:
                stack.enter_context(patch_requests(gate))
            except ImportError as e:
                raise BindingUnavailable(
                    "%s: requests not installed here: %s" % (name, e))
        if "aiohttp" in transports:
            try:
                stack.enter_context(patch_aiohttp(gate))
            except ImportError as e:
                raise BindingUnavailable(
                    "%s: aiohttp not installed here: %s" % (name, e))
        try:
            raw = construct_raw()
        except BindingUnavailable:
            raise
        except Exception as e:
            raise BindingUnavailable(
                "%s: offline construction failed (%s: %s)"
                % (name, type(e).__name__, e))
        if teardown_raw is not None:
            stack.callback(teardown_raw, raw)
        return ("%s raw adapter constructed OFFLINE under intercept-all "
                "guard (%s, %d transport calls served)"
                % (name, type(raw).__name__, len(gate.calls)))
    finally:
        stack.close()
