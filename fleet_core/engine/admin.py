"""fleet_core.engine.admin — engine admin unix socket (server + client).

Spec sources:
  - p3_rollout.md §3b (write-canary is a thin CLIENT of this socket; the
    engine must ack ``state=CANARY_HELD, tick_loop=parked`` before C1),
  - p3_rollout.md §4 (--drain routes THROUGH this socket: the drain CLI is
    a client of the already-running engine process — never a second writer),
  - entry-SM §2.1 ``management_class='manual_claimed'`` (claim tool RPC).

Protocol: newline-delimited JSON over a unix stream socket.  One request
object per line, one response object per line, multiple requests per
connection allowed.  Every response carries ``"ok": true|false``.

Commands (dispatch table `_COMMANDS`):
  ping           -> {"ok":true,"pong":<ts>}
  state          -> engine.admin_state()
  park           -> park the tick loop           (payload: reason)
  resume         -> resume the tick loop
  entry_freeze   -> {"on": true|false}
  canary_hold    -> transition to CANARY_HELD (entries frozen + tick loop
                    parked, watchdog-bounded); ack per rollout §3b
  canary_release -> leave CANARY_HELD, unpark
  canary_exec    -> run canary ladder steps IN-PROCESS (delegated to the
                    handler the canary glue registered on the engine);
                    refused unless state == CANARY_HELD
  drain          -> rollout §4 DRAIN: entries frozen + bounded reconciler
                    passes until non-terminal pre-OPEN rows converge
  claim          -> {"coin": ...}: set management_class='manual_claimed'
                    on the coin's live rows (operator escape hatch;
                    manual_claimed keeps ONLY the phantom-K3 DB-row-resolve,
                    zero venue actions — see runner.Engine.claim)

The server is deliberately thin: it validates & dispatches to duck-typed
engine methods and serializes results.  All policy lives in the engine.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union

log = logging.getLogger("fleet_core.engine.admin")

__all__ = ["AdminError", "AdminServer", "request"]

_MAX_LINE = 1 << 20  # 1 MiB per request line — ample for any admin payload
_ACCEPT_POLL_SEC = 1.0


class AdminError(RuntimeError):
    """Client-side error talking to the engine admin socket."""


# --------------------------------------------------------------------------- server


def _dispatch(engine: Any, obj: Dict[str, Any]) -> Dict[str, Any]:
    cmd = obj.get("cmd")
    if not isinstance(cmd, str):
        return {"ok": False, "error": "missing 'cmd'"}
    if cmd == "ping":
        return {"ok": True, "pong": time.time()}
    if cmd == "state":
        return engine.admin_state()
    if cmd == "park":
        return engine.park_ticks(reason=str(obj.get("reason", "admin")))
    if cmd == "resume":
        return engine.resume_ticks()
    if cmd == "entry_freeze":
        if "on" not in obj:
            return {"ok": False, "error": "entry_freeze requires 'on': true|false"}
        return engine.set_entry_freeze(bool(obj["on"]))
    if cmd == "canary_hold":
        return engine.canary_hold()
    if cmd == "canary_release":
        return engine.canary_release(reason=str(obj.get("reason", "operator")))
    if cmd == "canary_exec":
        return engine.canary_exec(obj.get("payload") or {})
    if cmd == "drain":
        return engine.drain(obj.get("payload") or {})
    if cmd == "claim":
        coin = obj.get("coin")
        if not coin or not isinstance(coin, str):
            return {"ok": False, "error": "claim requires 'coin'"}
        return engine.claim(coin, note=str(obj.get("note", "")))
    return {"ok": False, "error": "unknown cmd %r" % cmd}


class AdminServer:
    """Threaded unix-socket admin endpoint bound to one engine instance."""

    def __init__(self, sock_path: Union[str, Path], engine: Any):
        self.sock_path = Path(sock_path)
        self.engine = engine
        self._sock = None  # type: Optional[socket.socket]
        self._stop = threading.Event()
        self._thread = None  # type: Optional[threading.Thread]
        self._conn_threads = []  # type: list

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        self._bind()
        t = threading.Thread(target=self._accept_loop, name="engine-admin", daemon=True)
        self._thread = t
        t.start()
        log.info("admin socket listening at %s", self.sock_path)

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        try:
            self.sock_path.unlink()
        except OSError:
            pass

    def _bind(self) -> None:
        self.sock_path.parent.mkdir(parents=True, exist_ok=True)
        if self.sock_path.exists():
            # Stale socket vs live twin: probe.  A live twin should be
            # impossible (locks.py flock is the authoritative guard) — if we
            # can connect, refuse rather than steal the endpoint.
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.settimeout(1.0)
            try:
                probe.connect(str(self.sock_path))
            except OSError:
                self.sock_path.unlink()  # stale — previous process died
            else:
                probe.close()
                raise RuntimeError(
                    "admin socket %s is alive — another engine process is "
                    "serving it (dual-writer guard should have prevented this)"
                    % self.sock_path
                )
            finally:
                try:
                    probe.close()
                except OSError:
                    pass
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(str(self.sock_path))
        os.chmod(str(self.sock_path), 0o600)  # operator/root only
        s.listen(8)
        s.settimeout(_ACCEPT_POLL_SEC)
        self._sock = s

    # -- serving -----------------------------------------------------------
    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            sock = self._sock
            if sock is None:
                return
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return  # socket closed under us during stop()
            t = threading.Thread(
                target=self._serve_conn, args=(conn,), daemon=True,
                name="engine-admin-conn",
            )
            t.start()
            self._conn_threads = [x for x in self._conn_threads if x.is_alive()]
            self._conn_threads.append(t)

    def _serve_conn(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(600.0)  # drain can legitimately take minutes
            buf = b""
            while not self._stop.is_set():
                nl = buf.find(b"\n")
                while nl < 0:
                    if len(buf) > _MAX_LINE:
                        conn.sendall(b'{"ok": false, "error": "request too large"}\n')
                        return
                    chunk = conn.recv(65536)
                    if not chunk:
                        return  # client closed
                    buf += chunk
                    nl = buf.find(b"\n")
                line, buf = buf[:nl], buf[nl + 1:]
                if not line.strip():
                    continue
                try:
                    req = json.loads(line.decode("utf-8"))
                    if not isinstance(req, dict):
                        raise ValueError("request must be a JSON object")
                except (ValueError, UnicodeDecodeError) as e:
                    resp = {"ok": False, "error": "bad request: %s" % e}
                else:
                    try:
                        resp = _dispatch(self.engine, req)
                    except Exception as e:  # noqa: BLE001 — admin must not die
                        log.exception("admin command %r failed", req.get("cmd"))
                        resp = {
                            "ok": False,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        }
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


# --------------------------------------------------------------------------- client


def request(sock_path: Union[str, Path], obj: Dict[str, Any],
            timeout: float = 10.0) -> Dict[str, Any]:
    """Send one admin request and return the parsed response.

    Used by ``runner --drain`` / ``runner --state`` and by the write-canary
    tool (rollout §3b: the canary is a thin client of this socket).
    """
    sock_path = str(sock_path)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        try:
            s.connect(sock_path)
        except OSError as e:
            raise AdminError(
                "cannot connect to engine admin socket %s: %s "
                "(is the engine running?)" % (sock_path, e)
            )
        s.sendall((json.dumps(obj) + "\n").encode("utf-8"))
        buf = b""
        while b"\n" not in buf:
            if len(buf) > _MAX_LINE:
                raise AdminError("oversized admin response")
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                raise AdminError("timed out waiting for admin response (cmd=%r)"
                                 % obj.get("cmd"))
            if not chunk:
                raise AdminError("engine closed admin connection mid-response")
            buf += chunk
        line = buf.split(b"\n", 1)[0]
        try:
            resp = json.loads(line.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise AdminError("bad admin response: %s" % e)
        if not isinstance(resp, dict):
            raise AdminError("bad admin response: not an object")
        return resp
    finally:
        try:
            s.close()
        except OSError:
            pass
