"""fleet_core.conformance.faults — transport-level fault injector (P2 harness).

WHAT THIS IS
------------
A single chokepoint (`TransportGate`) through which EVERY outbound call of an
adapter-under-test is routed, plus context-manager patchers that install the
gate at the LOWEST transport layer each venue adapter actually uses:

  * requests-based adapters (hl / pacifica / nado SDK sessions) —
    `patch_requests()` monkeypatches `requests.Session.send`, which sits BELOW
    the adapters' own `session.request` wrappers (HL `_install_session_timeout`,
    pacifica RL pacer, nado order-path guards) — so the adapter's OWN error
    handling, retry and timeout discipline runs unmodified over injected faults.
  * aiohttp-based adapters (extended / x10 SDK) — `patch_aiohttp()`
    monkeypatches `aiohttp.ClientSession._request`.
  * the pure-python reference binding calls `gate.request(...)` directly
    (transport="fake") so the whole harness runs locally without any SDK.

INTERCEPT-ALL LAW
-----------------
While a gate is installed the process MUST NOT reach a real network:
  * every request the responder does not recognize raises
    `UninterceptedRealCall` (an AssertionError — the test fails loud);
  * `block_sockets()` additionally patches `socket.socket.connect` /
    `socket.create_connection` so anything that slips past the HTTP-level
    patchers (a new SDK transport, a raw socket) dies before the wire.

TIMEOUT LAW ENFORCEMENT
-----------------------
`slow_response` faults compare the injected delay against the timeout the
ADAPTER passed down. If the adapter passed NO timeout at all the gate raises
`UnboundedCallDetected` — surfacing the exact defect class of audit #0
(nado zero-timeout SDK calls) as a test failure instead of a hang.

FAULT TYPES (`Fault.kind`)
--------------------------
  timeout              — transport raises a read-timeout; NOTHING landed.
  connect_error        — transport raises a connection-ESTABLISHMENT error
                         ('Failed to establish new connection' shape); nothing
                         landed — the one connection failure a write may
                         classify as may_have_landed=False.
  http_429             — venue answers 429 (rate limit). `count=` bounds how
                         many times it fires (count=None → persistent).
  http_5xx             — venue answers 503.
  empty_body_200       — 200 OK with a zero-byte body.
  malformed_json_200   — 200 OK with a non-JSON body.
  slow_response        — response slower than the adapter's own timeout →
                         transport timeout (see timeout law above).
  accept_no_land       — WRITE fault: venue answers "accepted" (ack-shaped,
                         no fill/data fields) but the side effect NEVER lands:
                         readback shows nothing. The naked-success class.
  accept_partial_land  — WRITE fault: venue acks, but only `partial_ratio`
                         of the requested size actually lands; readback shows
                         the partial. Adapter must NOT echo the request.
  timeout_after_land   — WRITE fault: the side effect LANDS, then the
                         transport times out before the response is read.
                         Correct adapters recover the truth via readback
                         (HL confirm-via-positions pattern).
  reset_after_land     — WRITE fault: the side effect LANDS, then the
                         connection dies (reset-by-peer) before the response
                         is read. Raises the same transport connection-error
                         TYPE as connect_error but with a mid-flight (NOT
                         'failed to establish') shape: an adapter that
                         classifies EVERY connection error as
                         may_have_landed=False skips the readback and hides
                         the landed fill (F1 untracked-position class).
  corrupt_item         — READ fault: venue answers 200 with a VALID envelope
                         in which exactly ONE list item (position/order row)
                         is corrupted (unparseable). Honored by responders on
                         list-shaped reads (positions / orders / fills). The adapter
                         must raise ReadUnknown (or venue-truth-resolve the
                         item) — NEVER silently drop it (nado exchange_nado
                         per-item drop-and-continue class, census 3c).

Fault targeting: `op` is a canonical operation name produced by the binding's
`classify(method, url, body)` (see bindings/__init__.py for the canonical op
vocabulary), `op="*"` matches everything, `match=` is an optional predicate
over the full request context, `fire_after=N` lets the first N matching calls
through untouched (sequencing: e.g. fail only the FINAL readback of
ensure_flat), `count=N` limits how many times the fault fires.

Pure stdlib; `requests` / `aiohttp` are imported lazily inside their patchers.
"""

from __future__ import annotations

import contextlib
import json
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

__all__ = [
    "FAULT_KINDS",
    "InjectorError",
    "UninterceptedRealCall",
    "UnboundedCallDetected",
    "TransportTimeout",
    "TransportConnectError",
    "GateResponse",
    "json_response",
    "Fault",
    "TransportGate",
    "block_sockets",
    "patch_requests",
    "patch_aiohttp",
]

FAULT_KINDS = frozenset({
    "timeout",
    "connect_error",
    "http_429",
    "http_5xx",
    "empty_body_200",
    "malformed_json_200",
    "slow_response",
    "accept_no_land",
    "accept_partial_land",
    "timeout_after_land",
    "reset_after_land",
    "corrupt_item",
})

#: Faults whose directive the RESPONDER must honor (write-side no_land/partial
#: + the read-side per-item corruption).
_DIRECTIVE_FAULTS = frozenset({"accept_no_land", "accept_partial_land",
                               "corrupt_item"})


class InjectorError(AssertionError):
    """Harness-level failure (NOT a simulated venue error) — fails the test."""


class UninterceptedRealCall(InjectorError):
    """A call reached the gate that no responder recognizes, or a raw socket
    connect was attempted. Under the harness this means the adapter tried to
    hit something the simulation does not cover — by law that must fail loud,
    never fall through to the real network."""


class UnboundedCallDetected(InjectorError):
    """A slow_response fault found the adapter passed NO timeout down to the
    transport — the timeout-law violation (audit defect #0 class)."""


class TransportTimeout(Exception):
    """Neutral transport timeout; patchers map it onto the transport's own
    exception type (requests.ReadTimeout / asyncio.TimeoutError)."""


class TransportConnectError(Exception):
    """Neutral connection failure; mapped per transport."""


@dataclass
class GateResponse:
    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))


def json_response(obj: Any, status: int = 200) -> GateResponse:
    return GateResponse(
        status=status,
        body=json.dumps(obj).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


@dataclass
class Fault:
    kind: str
    op: str = "*"
    match: Optional[Callable[[Dict[str, Any]], bool]] = None
    fire_after: int = 0          # let this many matching calls pass first
    count: Optional[int] = None  # fire at most this many times (None = always)
    delay_sec: float = 0.0       # slow_response: injected latency
    partial_ratio: float = 0.5   # accept_partial_land: landed fraction
    _seen: int = field(default=0, repr=False)
    _fired: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        if self.kind not in FAULT_KINDS:
            raise ValueError(
                "unknown fault kind %r — must be one of %s"
                % (self.kind, sorted(FAULT_KINDS))
            )

    def _matches(self, ctx: Dict[str, Any]) -> bool:
        if self.op != "*" and ctx.get("op") != self.op:
            return False
        if self.match is not None and not self.match(ctx):
            return False
        return True

    def should_fire(self, ctx: Dict[str, Any]) -> bool:
        """Stateful: counts matching calls for fire_after/count semantics."""
        if not self._matches(ctx):
            return False
        self._seen += 1
        if self._seen <= self.fire_after:
            return False
        if self.count is not None and self._fired >= self.count:
            return False
        self._fired += 1
        return True


# Responder signature:
#   respond(ctx: dict, directive: Optional[dict]) -> Optional[GateResponse]
# ctx keys: method, url, body, op, timeout, transport.
# directive is None (normal) or {"mode": "no_land"} /
# {"mode": "partial", "ratio": float} — the responder MUST honor it by
# suppressing / scaling the write's side effect while still answering with an
# ack-shaped body (no authoritative fill fields), forcing the adapter down its
# readback path — or {"mode": "corrupt_item"} on list-shaped READS
# (positions / orders / fills): serve a valid envelope with exactly ONE item
# corrupted (unparseable), leaving the venue truth store untouched. Returning None = endpoint not simulated →
# UninterceptedRealCall.
Responder = Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Optional[GateResponse]]


class TransportGate:
    """The single interception chokepoint. Thread-safe."""

    #: hard cap on how long a slow_response fault may actually sleep locally
    MAX_REAL_SLEEP_SEC = 0.5

    def __init__(self, classify: Callable[[str, str, Any], str],
                 responder: Responder, name: str = "gate") -> None:
        self._classify = classify
        self._responder = responder
        self.name = name
        self._faults: List[Fault] = []
        self.calls: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    # -------------------------------------------------------------- fault API

    def add_fault(self, fault: Optional[Fault] = None, **kw: Any) -> Fault:
        if fault is None:
            fault = Fault(**kw)
        with self._lock:
            self._faults.append(fault)
        return fault

    def fail(self, op: str, kind: str, **kw: Any) -> Fault:
        """Convenience: add_fault(kind=kind, op=op, ...)."""
        return self.add_fault(Fault(kind=kind, op=op, **kw))

    def clear_faults(self) -> None:
        with self._lock:
            self._faults = []

    def clear_calls(self) -> None:
        with self._lock:
            self.calls = []

    def ops_called(self) -> List[str]:
        with self._lock:
            return [c["op"] for c in self.calls]

    # ------------------------------------------------------------ the request

    def request(self, method: str, url: str, body: Any = None,
                timeout: Optional[float] = None,
                transport: str = "fake") -> GateResponse:
        op = self._classify(method, url, body)
        ctx = {
            "method": method.upper(),
            "url": url,
            "body": body,
            "op": op,
            "timeout": timeout,
            "transport": transport,
            "ts": time.time(),
        }
        with self._lock:
            self.calls.append(ctx)
            fault = None
            for f in self._faults:
                if f.should_fire(ctx):
                    fault = f
                    break

        directive: Optional[Dict[str, Any]] = None
        if fault is not None:
            k = fault.kind
            if k == "timeout":
                raise TransportTimeout(
                    "injected timeout on %s %s (op=%s)" % (method, url, op))
            if k == "connect_error":
                # establishment shape: the request provably never left
                raise TransportConnectError(
                    "injected connect error on %s %s (op=%s): failed to "
                    "establish new connection" % (method, url, op))
            if k == "http_429":
                return json_response({"error": "rate limit exceeded"}, status=429)
            if k == "http_5xx":
                return json_response({"error": "internal server error"}, status=503)
            if k == "empty_body_200":
                return GateResponse(status=200, body=b"")
            if k == "malformed_json_200":
                return GateResponse(status=200, body=b'{"broken": [not json',
                                    headers={"Content-Type": "application/json"})
            if k == "slow_response":
                if timeout is None:
                    raise UnboundedCallDetected(
                        "slow_response fault hit a call with NO timeout: "
                        "%s %s (op=%s) — the adapter exposes an unbounded "
                        "transport call (timeout-law violation, audit #0 class)"
                        % (method, url, op))
                if fault.delay_sec >= float(timeout):
                    raise TransportTimeout(
                        "injected slow response %.3fs exceeds adapter timeout "
                        "%.3fs on %s (op=%s)"
                        % (fault.delay_sec, float(timeout), url, op))
                time.sleep(min(fault.delay_sec, self.MAX_REAL_SLEEP_SEC))
            elif k == "accept_no_land":
                directive = {"mode": "no_land"}
            elif k == "accept_partial_land":
                directive = {"mode": "partial", "ratio": fault.partial_ratio}
            elif k == "corrupt_item":
                directive = {"mode": "corrupt_item"}
            elif k == "timeout_after_land":
                # side effect LANDS (normal responder invocation), then the
                # response is lost to a transport timeout.
                self._respond(ctx, None)
                raise TransportTimeout(
                    "injected timeout AFTER side effect landed on %s %s (op=%s)"
                    % (method, url, op))
            elif k == "reset_after_land":
                # side effect LANDS, then the connection dies mid-flight —
                # deliberately NOT establishment-shaped ('failed to
                # establish' absent): may_have_landed must NOT be False.
                self._respond(ctx, None)
                raise TransportConnectError(
                    "injected connection reset by peer AFTER side effect "
                    "landed on %s %s (op=%s)" % (method, url, op))

        return self._respond(ctx, directive)

    def _respond(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> GateResponse:
        resp = self._responder(ctx, directive)
        if resp is None:
            raise UninterceptedRealCall(
                "unintercepted call: %s %s (op=%s, transport=%s) — responder "
                "does not simulate this endpoint; refusing to let it reach a "
                "real network" % (ctx["method"], ctx["url"], ctx["op"],
                                  ctx["transport"]))
        return resp


# ===========================================================================
# Patchers
# ===========================================================================

@contextlib.contextmanager
def block_sockets():
    """Belt-and-suspenders: any raw TCP connect while active raises
    UninterceptedRealCall. Covers transports the HTTP patchers don't."""
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create = socket.create_connection

    def _blocked_connect(self, addr, *a, **kw):  # noqa: ANN001
        raise UninterceptedRealCall(
            "raw socket connect to %r blocked by conformance harness" % (addr,))

    def _blocked_connect_ex(self, addr, *a, **kw):  # noqa: ANN001
        raise UninterceptedRealCall(
            "raw socket connect_ex to %r blocked by conformance harness" % (addr,))

    def _blocked_create(*a, **kw):
        raise UninterceptedRealCall(
            "socket.create_connection blocked by conformance harness "
            "(args=%r)" % (a[:1],))

    socket.socket.connect = _blocked_connect          # type: ignore[method-assign]
    socket.socket.connect_ex = _blocked_connect_ex    # type: ignore[method-assign]
    socket.create_connection = _blocked_create        # type: ignore[assignment]
    try:
        yield
    finally:
        socket.socket.connect = real_connect          # type: ignore[method-assign]
        socket.socket.connect_ex = real_connect_ex    # type: ignore[method-assign]
        socket.create_connection = real_create        # type: ignore[assignment]


def _flatten_requests_timeout(timeout: Any) -> Optional[float]:
    """requests timeout may be None, float, or (connect, read) tuple."""
    if timeout is None:
        return None
    if isinstance(timeout, (tuple, list)):
        try:
            return float(timeout[-1])
        except Exception:
            return None
    try:
        return float(timeout)
    except Exception:
        return None


@contextlib.contextmanager
def patch_requests(gate: TransportGate):
    """Route every `requests` call in the process through the gate by patching
    `requests.Session.send` (below the adapters' own session.request wrappers,
    so their timeout installers / RL pacers / retry loops run unmodified)."""
    import requests  # lazy: only needed where requests-based adapters run
    from requests.structures import CaseInsensitiveDict

    real_send = requests.Session.send

    def _fake_send(self, request, **kw):  # noqa: ANN001
        timeout = _flatten_requests_timeout(kw.get("timeout"))
        body = request.body
        if isinstance(body, bytes):
            try:
                body = body.decode("utf-8")
            except Exception:
                pass
        try:
            g = gate.request(request.method or "GET", request.url or "",
                             body=body, timeout=timeout, transport="requests")
        except TransportTimeout as e:
            raise requests.exceptions.ReadTimeout(str(e))
        except TransportConnectError as e:
            raise requests.exceptions.ConnectionError(str(e))

        resp = requests.models.Response()
        resp.status_code = g.status
        resp._content = g.body  # noqa: SLF001
        resp.headers = CaseInsensitiveDict(
            dict(g.headers) or {"Content-Type": "application/json"})
        resp.url = request.url or ""
        resp.request = request
        resp.reason = "OK" if g.status < 400 else "SIMULATED-ERROR"
        resp.encoding = "utf-8"
        return resp

    requests.Session.send = _fake_send  # type: ignore[method-assign]
    try:
        yield
    finally:
        requests.Session.send = real_send  # type: ignore[method-assign]


class _FakeAioResponse:
    """Minimal duck-typed aiohttp.ClientResponse stand-in."""

    def __init__(self, g: GateResponse, method: str, url: str) -> None:
        self.status = g.status
        self._body = g.body
        self.headers = dict(g.headers) or {"Content-Type": "application/json"}
        self.method = method
        self._url = url
        self.reason = "OK" if g.status < 400 else "SIMULATED-ERROR"

    @property
    def ok(self) -> bool:
        return self.status < 400

    async def json(self, **kw: Any) -> Any:
        return json.loads(self._body.decode("utf-8"))

    async def text(self, **kw: Any) -> str:
        return self._body.decode("utf-8", errors="replace")

    async def read(self) -> bytes:
        return self._body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            import aiohttp
            try:
                from multidict import CIMultiDict, CIMultiDictProxy
                from yarl import URL
                req_info = aiohttp.RequestInfo(
                    url=URL(self._url), method=self.method,
                    headers=CIMultiDictProxy(CIMultiDict()), real_url=URL(self._url))
            except Exception:  # pragma: no cover — defensive
                req_info = None  # type: ignore[assignment]
            raise aiohttp.ClientResponseError(
                request_info=req_info, history=(), status=self.status,
                message="simulated HTTP %d" % self.status)

    def release(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def __aenter__(self) -> "_FakeAioResponse":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None


@contextlib.contextmanager
def patch_aiohttp(gate: TransportGate):
    """Route every aiohttp request through the gate by patching
    `aiohttp.ClientSession._request` (lowest public-ish layer; the SDK's own
    wrappers — x10 PerpetualTradingClient et al. — run unmodified above it)."""
    import asyncio

    import aiohttp  # lazy

    real_request = aiohttp.ClientSession._request  # noqa: SLF001

    def _bound_of(to) -> Optional[float]:  # noqa: ANN001
        """Effective read bound of an aiohttp timeout value. `total` bounds the
        whole op; with total=None a per-read socket bound (sock_read) STILL
        bounds a slow response — honor it. connect/sock_connect only bound
        connection establishment, never a slow body, so they deliberately do
        NOT count (a connect-only timeout is still unbounded for reads)."""
        if to is None or to is aiohttp.helpers.sentinel:
            return None
        if isinstance(to, (int, float)):
            return float(to)
        total = getattr(to, "total", None)
        if total:
            return float(total)
        sock_read = getattr(to, "sock_read", None)
        if sock_read:
            return float(sock_read)
        return None

    async def _fake_request(self, method, str_or_url, **kw):  # noqa: ANN001
        timeout: Optional[float] = _bound_of(kw.get("timeout"))
        if timeout is None:
            timeout = _bound_of(getattr(self, "_timeout", None))
        body = kw.get("json")
        if body is None:
            body = kw.get("data")
        try:
            g = gate.request(str(method), str(str_or_url), body=body,
                             timeout=timeout, transport="aiohttp")
        except TransportTimeout as e:
            raise asyncio.TimeoutError(str(e))
        except TransportConnectError as e:
            raise aiohttp.ClientConnectionError(str(e))
        return _FakeAioResponse(g, str(method), str(str_or_url))

    aiohttp.ClientSession._request = _fake_request  # type: ignore  # noqa: SLF001
    try:
        yield
    finally:
        aiohttp.ClientSession._request = real_request  # type: ignore  # noqa: SLF001
