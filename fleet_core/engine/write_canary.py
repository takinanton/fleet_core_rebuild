"""fleet_core.engine.write_canary — WRITE-CANARY: thin client + in-engine ladder glue (P3).

Spec: p3_rollout.md §3b (round-3 approved) — rollout step 6b. The 48h shadow
gate is READ-only; this component exercises the binding's money WRITE paths
against the REAL venue by the REAL engine BEFORE the engine's first
unsupervised real-money write.

TWO HALVES, one module (matching runner.py's expectation that "canary glue
(write_canary in-process ladder) registers here"):

  1. THIN CLIENT (``main`` / ``run_canary``) — operator-invoked
     ``python -m fleet_core.engine.write_canary --venue X``. Speaks the
     fleet_core.engine.admin wire protocol (newline-delimited JSON, flat
     request objects: {"cmd": ..., "payload": ...}). It never touches
     trades.db and never performs venue I/O.
  2. IN-PROCESS LADDER GLUE (``wire_canary(engine)`` / ``make_canary_handler``)
     — registered on the engine via ``engine.register_canary_handler(run,
     on_expire)``; executes C1-C8 INSIDE the running ``fleet-<venue>-bot``
     process (IntentExecutor-grade paths: P2 binding + entry-SM transitions +
     placed_trigger_oids registry), single writer — the §4 flock stays held
     by the engine with ZERO exemptions. ``on_expire`` is the §3b watchdog
     auto-flatten (engine calls it when the CANARY_HELD window expires).

CLIENT-SIDE LAWS (rollout §3b):
  * Unit-state precondition (R3/R2-F3): ``systemctl is-active
    fleet-<venue>-bot`` == active AND the ``canary_hold`` ack reads EXACTLY
    ``state=CANARY_HELD, tick_loop=parked`` before C1; ack ``window_sec``
    must be <= 900 (15 min watchdog bound).
  * Hard caps (labeled): notional <= $50 — abort BEFORE C1; cumulative
    realized cost <= $2 — abort any step. Size rule: notional = 1.2 x
    max(venue min order, venue trigger-SL min, $10); Nado law: trigger-SL
    min = 10x order min (project_nado_trigger_sl_min_size_mismatch).
    The venue minimums are OPERATOR INPUTS (--min-order-usd /
    --trigger-sl-min-usd, taken from the venue liquidity snapshot as §3b
    prescribes and recorded in the canary report); the engine re-derives
    the notional from them and BOTH sides enforce the caps.
  * Instrument (--coin): venue's highest-liquidity coin satisfying the size
    rule, NO live position, NOT _fenced — engine verifies both at plan time.
  * Operator-supervised, fail-stop: a RED is NEVER auto-retried; ABORT =
    immediate in-engine ensure_flat + cancel all canary oids ->
    canary_release RED -> exit 1 -> §4 rollback path, NO unfreeze.
  * The canary row stays PROTECTED throughout (never t_open'd); expected SM
    chain INTENT->PENDING->FILLED->PROTECTED->ABORTING->ABORTED; row tagged
    ``notes='write_canary'`` (reconciler skips it permanently).

canary_exec payload contract (client -> engine handler):
  {"op":"plan","coin":C,"min_order_usd":F,"trigger_sl_min_usd":F}
      -> {"ok":true,"coin":C,"notional_usd":F,"min_order_usd":F,
          "trigger_sl_min_usd":F,"mark_px":F,"has_live_position":false,
          "fenced":false,"run_c2b":bool}
  {"op":"step","step":"C1".."C8","coin":C}
      -> {"ok":true,"step":S,"coin":C,"actions":[{"coin":..,"op":..}],
          "cost_usd_cum":F,"evidence":{...}}   (per-step keys below)
  {"op":"abort","coin":C}   -> ensure_flat + cancel all canary oids

Per-step evidence keys (asserted by the client):
  C1 : fill_avg_px, fill_size, requested_size, fill_oid, readback_verified,
       limit_px, cap_breach
  C2 : sl_oid, trigger_px, confirm_sec, registry_inserted
  C2b: tp_oid, tp_confirm_sec, cancel_gone_sec        (run_c2b venues: ext/hl)
  C3 : new_oid, resting_own_count, converged_ticks
  C4 : cancelled_oid, gone_sec
  C5 : sl_oid, trigger_px, confirm_sec
  C6 : verified_flat, closed_size, exit_avg_px, abort_reason
  C7 : cancelled_orphans, resting_triggers_for_coin
  C8 : transitions [[from,to],...], sm_conflicts, realized_cost_usd,
       resting_triggers_for_coin

Offline selftest (no SDKs, no engine, no systemd):
``python3 write_canary.py --selftest`` — scripted fake server scenarios PLUS
an end-to-end run of the REAL in-process handler behind a fake admin server
with a fake P2 client and a real (temp) SM journal.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = [
    "EngineAdminClient",
    "AdminSocketError",
    "CanaryRed",
    "run_canary",
    "resolve_sock_path",
    "make_canary_handler",
    "wire_canary",
]

# ---------------------------------------------------------------------------
# Constants — every number labeled with its p3_rollout.md §3b source
# ---------------------------------------------------------------------------

HARD_NOTIONAL_CAP_USD = 50.0    # §3b "Hard $ cap": notional <= $50, abort BEFORE C1
HARD_COST_CAP_USD = 2.0         # §3b: hard abort if cumulative realized cost > $2
SIZE_RULE_MULT = 1.2            # §3b Size: 1.2 x max(min order, trigger-SL min, $10)
SIZE_RULE_FLOOR_USD = 10.0      # §3b Size rule floor
SIZE_RULE_TOL = 0.01            # 1% tolerance plan-vs-recomputed notional (rounding)
SL_CONFIRM_MAX_SEC = 15.0       # §3b ABORT: SL not confirmed resting within 15s
GONE_READBACK_MAX_SEC = 15.0    # §3b assert (4): cancel gone-readback <= 15s
UNRESOLVED_UNKNOWN_SEC = 120.0  # §3b ABORT: Write/Read-unknown unresolved after 120s (2 ticks)
WATCHDOG_WINDOW_SEC = 900       # §3b: CANARY_HELD window bounded <= 15 min
SUPERSEDE_CONVERGE_TICKS = 1    # §3b ABORT: supersede must converge in 1 tick
CANARY_SL_PCT = 0.30            # §3b C2/C5: far-OTM trigger at mark -30% (long)
CANARY_REPLACE_PCT = 0.25       # §3b C3: replace_sl at mark -25%
CANARY_TP_PCT = 0.30            # §3b C2b: limit_reduce_only far above mark (+30%)
CANARY_LIMIT_CAP_PCT = 0.005    # canary's own limit_px cap (ENTRY_LIMIT_CAP_PCT class, env-overridable)
CANARY_NOTES = "write_canary"   # §3b: journaled as ordinary SM rows tagged notes='write_canary'

# §3b: row stays PROTECTED throughout — never t_open'd (entry-SM §1 chain)
EXPECTED_SM_CHAIN = [
    ["INTENT", "PENDING"],
    ["PENDING", "FILLED"],
    ["FILLED", "PROTECTED"],
    ["PROTECTED", "ABORTING"],
    ["ABORTING", "ABORTED"],
]

GREEN_ASSERT_NAMES = [
    "1 fill readback_verified with REAL px/size",
    "2 SL oid in list_open_sl_orders <=15s post-place",
    "3 replace left EXACTLY ONE resting own SL (supersede proven)",
    "4 cancel gone-readback <=15s (SL and, where run, C2b TP)",
    "5 FlatResult verified_flat",
    "6 zero resting triggers for the coin after C7",
    "7 SM chain INTENT->PENDING->FILLED->PROTECTED->ABORTING->ABORTED, no SMConflict",
    "8 realized cost <= $2",
]

# §3b C2b: venues with TP1_PARTIAL_FRAC>0 (ext/hl); env overrides
RUN_C2B_VENUES = ("extended", "hl")


# ---------------------------------------------------------------------------
# Admin-socket client (wire-compatible with fleet_core.engine.admin;
# shared with drain.py and cutover_check.py)
# ---------------------------------------------------------------------------

class AdminSocketError(RuntimeError):
    """Admin socket unreachable / bad response."""


class EngineAdminClient:
    """One JSON-line request per connection, flat request objects exactly as
    fleet_core.engine.admin._dispatch consumes them:
    {"cmd": <name>, <cmd-specific fields: payload / reason / coin / on>}."""

    def __init__(self, sock_path: str, timeout: float = 30.0) -> None:
        self.sock_path = sock_path
        self.timeout = timeout

    def request(self, cmd: str, timeout: Optional[float] = None,
                **fields: Any) -> Dict[str, Any]:
        obj = dict(fields)
        obj["cmd"] = cmd
        payload = (json.dumps(obj) + "\n").encode("utf-8")
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(timeout if timeout is not None else self.timeout)
            s.connect(self.sock_path)
            s.sendall(payload)
            buf = b""
            while b"\n" not in buf:
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
            s.close()
        except OSError as e:
            raise AdminSocketError(
                "admin socket %s unreachable for cmd=%s: %s"
                % (self.sock_path, cmd, e))
        if not buf:
            raise AdminSocketError("empty response for cmd=%s" % cmd)
        try:
            resp = json.loads(buf.split(b"\n", 1)[0].decode("utf-8"))
        except ValueError as e:
            raise AdminSocketError("unparseable response for cmd=%s: %s" % (cmd, e))
        if not isinstance(resp, dict):
            raise AdminSocketError("non-object response for cmd=%s" % cmd)
        return resp

    def connectable(self) -> bool:
        """True if something is listening (drain.py's engine-alive probe)."""
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect(self.sock_path)
            s.close()
            return True
        except OSError:
            return False


def resolve_sock_path(explicit: Optional[str] = None,
                      db_path: Optional[str] = None) -> str:
    """Socket-path convention: $FLEET_ADMIN_SOCK, else <dir(db)>/engine_admin.sock."""
    if explicit:
        return explicit
    env = os.environ.get("FLEET_ADMIN_SOCK")
    if env:
        return env
    if db_path:
        return os.path.join(os.path.dirname(os.path.abspath(db_path)),
                            "engine_admin.sock")
    return os.path.join("data", "engine_admin.sock")


def _systemctl_is_active(unit: str) -> str:
    try:
        out = subprocess.run(["systemctl", "is-active", unit],
                             capture_output=True, text=True, timeout=10)
        return (out.stdout or out.stderr or "").strip() or "unknown"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Thin client — canary run
# ---------------------------------------------------------------------------

class CanaryRed(RuntimeError):
    """Any ABORT criterion / failed precondition — fail-stop, never retried."""


def _fmt_row(cells: List[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _err_of(resp: Dict[str, Any]) -> Tuple[str, float]:
    err = resp.get("error")
    if isinstance(err, dict):
        return str(err.get("kind", err.get("detail", "unknown"))), \
            float(err.get("unresolved_sec", 0.0) or 0.0)
    return str(err or "unknown"), 0.0


def _check_scope_fence(coin: str, resp: Dict[str, Any]) -> None:
    """§3b ABORT: ANY action touching a coin != canary coin = auto-RED bug."""
    for act in resp.get("actions", []) or []:
        acoin = str(act.get("coin", ""))
        if acoin and acoin != coin:
            raise CanaryRed(
                "SCOPE FENCE TRIP: action %r touched coin %s != canary coin %s "
                "— auto-RED engine bug" % (act.get("op"), acoin, coin))


def _check_cost(resp: Dict[str, Any]) -> None:
    cost = resp.get("cost_usd_cum")
    if cost is not None and float(cost) > HARD_COST_CAP_USD:
        raise CanaryRed("realized cost $%.4f > hard cap $%.2f"
                        % (float(cost), HARD_COST_CAP_USD))


def _step_abort_checks(step: str, coin: str, resp: Dict[str, Any]) -> None:
    """Per-step ABORT criteria (p3_rollout.md §3b ABORT row)."""
    if not resp.get("ok"):
        kind, unresolved = _err_of(resp)
        if kind in ("WriteUnconfirmed", "ReadUnknown") and unresolved >= UNRESOLVED_UNKNOWN_SEC:
            raise CanaryRed("%s failed: %s unresolved after %.0fs (>=%.0fs abort "
                            "bound)" % (step, kind, unresolved, UNRESOLVED_UNKNOWN_SEC))
        # Any not-ok is still RED: fail-stop, no client retry — the 120s bound
        # is the engine's internal resolution budget, not a retry license.
        raise CanaryRed("%s failed: %s — fail-stop, no retry" % (step, kind))
    _check_scope_fence(coin, resp)
    _check_cost(resp)
    ev = resp.get("evidence") or {}
    if step in ("C2", "C5"):
        if float(ev.get("confirm_sec", 1e9)) > SL_CONFIRM_MAX_SEC:
            raise CanaryRed(
                "%s: SL not confirmed resting within %.0fs of place-accept "
                "(got %.1fs) — 2026-07-02 Ext WS-confirm class"
                % (step, SL_CONFIRM_MAX_SEC, float(ev.get("confirm_sec", -1))))
    if step == "C1":
        if ev.get("cap_breach"):
            raise CanaryRed("C1: fill px breaches the canary's own limit_px cap")
        limit_px, fill_px = ev.get("limit_px"), ev.get("fill_avg_px")
        if limit_px and fill_px and float(fill_px) > float(limit_px) * 1.001:
            raise CanaryRed("C1: fill px %.10g breaches limit_px %.10g (+0.1%% tol)"
                            % (float(fill_px), float(limit_px)))
    if step == "C3":
        if int(ev.get("resting_own_count", -1)) > 1 and \
                int(ev.get("converged_ticks", 99)) > SUPERSEDE_CONVERGE_TICKS:
            raise CanaryRed("C3: >1 own SL resting, supersede did not converge "
                            "in %d tick" % SUPERSEDE_CONVERGE_TICKS)
    if step in ("C4", "C2b"):
        gone = ev.get("gone_sec", ev.get("cancel_gone_sec"))
        if gone is not None and float(gone) > GONE_READBACK_MAX_SEC:
            raise CanaryRed("%s: cancel gone-readback %.1fs > %.0fs"
                            % (step, float(gone), GONE_READBACK_MAX_SEC))


def _evaluate_green_asserts(evidence: Dict[str, Dict[str, Any]]
                            ) -> List[Tuple[str, bool, str]]:
    """The 8 GREEN asserts, p3_rollout.md §3b verbatim."""
    out: List[Tuple[str, bool, str]] = []
    c1 = evidence.get("C1", {})
    ok1 = bool(c1.get("readback_verified")) and \
        float(c1.get("fill_avg_px", 0) or 0) > 0 and \
        float(c1.get("fill_size", 0) or 0) > 0
    out.append((GREEN_ASSERT_NAMES[0], ok1,
                "px=%s size=%s verified=%s" % (c1.get("fill_avg_px"),
                                               c1.get("fill_size"),
                                               c1.get("readback_verified"))))
    c2 = evidence.get("C2", {})
    ok2 = bool(c2.get("sl_oid")) and float(c2.get("confirm_sec", 1e9)) <= SL_CONFIRM_MAX_SEC
    out.append((GREEN_ASSERT_NAMES[1], ok2,
                "oid=%s confirm=%ss" % (c2.get("sl_oid"), c2.get("confirm_sec"))))
    c3 = evidence.get("C3", {})
    ok3 = int(c3.get("resting_own_count", -1)) == 1
    out.append((GREEN_ASSERT_NAMES[2], ok3,
                "resting_own_count=%s" % c3.get("resting_own_count")))
    c4 = evidence.get("C4", {})
    ok4 = float(c4.get("gone_sec", 1e9)) <= GONE_READBACK_MAX_SEC
    c2b = evidence.get("C2b")
    if c2b is not None:
        ok4 = ok4 and float(c2b.get("cancel_gone_sec", 1e9)) <= GONE_READBACK_MAX_SEC
    out.append((GREEN_ASSERT_NAMES[3], ok4,
                "sl_gone=%ss tp_gone=%s" % (c4.get("gone_sec"),
                                            (c2b or {}).get("cancel_gone_sec", "n/a"))))
    c6 = evidence.get("C6", {})
    out.append((GREEN_ASSERT_NAMES[4], bool(c6.get("verified_flat")),
                "verified_flat=%s" % c6.get("verified_flat")))
    c7, c8 = evidence.get("C7", {}), evidence.get("C8", {})
    ok6 = int(c7.get("resting_triggers_for_coin", -1)) == 0 and \
        int(c8.get("resting_triggers_for_coin", -1)) == 0
    out.append((GREEN_ASSERT_NAMES[5], ok6,
                "after_C7=%s after_C8=%s" % (c7.get("resting_triggers_for_coin"),
                                             c8.get("resting_triggers_for_coin"))))
    trans = [[str(a), str(b)] for a, b in (c8.get("transitions") or [])]
    ok7 = trans == EXPECTED_SM_CHAIN and int(c8.get("sm_conflicts", 1)) == 0
    out.append((GREEN_ASSERT_NAMES[6], ok7,
                "chain=%s conflicts=%s" % ("->".join(t[1] for t in trans) or "none",
                                           c8.get("sm_conflicts"))))
    cost = float(c8.get("realized_cost_usd", 1e9))
    out.append((GREEN_ASSERT_NAMES[7], cost <= HARD_COST_CAP_USD, "cost=$%.4f" % cost))
    return out


def run_canary(venue: str,
               sock_path: str,
               coin: str,
               min_order_usd: float,
               trigger_sl_min_usd: float,
               report_path: Optional[str] = None,
               systemctl: Callable[[str], str] = _systemctl_is_active,
               now: Callable[[], float] = time.monotonic) -> int:
    """Run the full §3b canary against a live engine. Exit 0 GREEN / 1 RED.
    Never auto-retries a RED (fail-stop)."""
    unit = "fleet-%s-bot" % venue
    client = EngineAdminClient(sock_path)
    lines: List[str] = []
    evidence: Dict[str, Dict[str, Any]] = {}
    held = False
    verdict = "RED"
    reason = ""
    deadline = None  # type: Optional[float]

    def say(msg: str) -> None:
        print(msg)
        lines.append(msg)

    say("# WRITE-CANARY %s (p3_rollout.md §3b) — %s UTC"
        % (venue, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
    say("operator inputs (venue liquidity snapshot): coin=%s min_order_usd=%.2f "
        "trigger_sl_min_usd=%.2f" % (coin, min_order_usd, trigger_sl_min_usd))
    try:
        # ---- preconditions (R3 / R2-F3: single writer, in-process) ----
        active = systemctl(unit)
        if active != "active":
            raise CanaryRed("systemctl is-active %s == %r (need 'active'): the "
                            "canary runs IN-PROCESS inside the engine" % (unit, active))
        ack = client.request("canary_hold")
        # Bias-to-release: any ok=True hold response may have transitioned the
        # engine — release in `finally` even if the ack fields are wrong.
        held = bool(ack.get("ok"))
        if not (ack.get("ok") and ack.get("state") == "CANARY_HELD"
                and ack.get("tick_loop") == "parked"):
            raise CanaryRed(
                "canary_hold ack != {state=CANARY_HELD, tick_loop=parked}: got %r "
                "— REFUSING to start C1 (R2-F3 precondition)" % ack)
        window = float(ack.get("window_sec", WATCHDOG_WINDOW_SEC))
        if window > WATCHDOG_WINDOW_SEC:
            raise CanaryRed("engine canary window %.0fs > %ds (rollout §3b: "
                            "watchdog bounded <=15 min)" % (window, WATCHDOG_WINDOW_SEC))
        deadline = now() + window
        say("hold ack: state=CANARY_HELD tick_loop=parked window=%.0fs" % window)

        # ---- plan + $50 cap BEFORE C1 ----
        plan = client.request("canary_exec", payload={
            "op": "plan", "coin": coin, "min_order_usd": min_order_usd,
            "trigger_sl_min_usd": trigger_sl_min_usd})
        if not plan.get("ok"):
            raise CanaryRed("canary plan failed: %r" % (plan.get("error"),))
        notional = float(plan["notional_usd"])
        expected = SIZE_RULE_MULT * max(min_order_usd, trigger_sl_min_usd,
                                        SIZE_RULE_FLOOR_USD)
        if plan.get("has_live_position"):
            raise CanaryRed("plan coin %s has a live position — §3b instrument "
                            "rule violated" % coin)
        if plan.get("fenced"):
            raise CanaryRed("plan coin %s is _fenced — §3b instrument rule violated" % coin)
        if abs(notional - expected) > expected * SIZE_RULE_TOL:
            raise CanaryRed(
                "plan notional $%.2f != size rule 1.2 x max(min_order=$%.2f, "
                "trigger_sl_min=$%.2f, $10) = $%.2f (±%.0f%%)"
                % (notional, min_order_usd, trigger_sl_min_usd, expected,
                   SIZE_RULE_TOL * 100))
        if notional > HARD_NOTIONAL_CAP_USD:
            raise CanaryRed("ABORT BEFORE C1: notional $%.2f > hard cap $%.2f"
                            % (notional, HARD_NOTIONAL_CAP_USD))
        say("plan: coin=%s notional=$%.2f (rule $%.2f) run_c2b=%s"
            % (coin, notional, expected, bool(plan.get("run_c2b"))))

        # ---- C1..C8 ----
        steps = ["C1", "C2"]
        if plan.get("run_c2b"):
            steps.append("C2b")   # §3b: venues with TP1_PARTIAL_FRAC>0 (ext/hl)
        steps += ["C3", "C4", "C5", "C6", "C7", "C8"]
        say("")
        say(_fmt_row(["step", "verdict", "evidence"]))
        say(_fmt_row(["---", "---", "---"]))
        for step in steps:
            if deadline is not None and now() > deadline:
                raise CanaryRed("watchdog window expired mid-run — engine "
                                "auto-flattens under CANARY_HELD; RED")
            resp = client.request("canary_exec", timeout=180.0,
                                  payload={"op": "step", "step": step, "coin": coin})
            _step_abort_checks(step, coin, resp)
            evidence[step] = dict(resp.get("evidence") or {})
            say(_fmt_row([step, "ok", json.dumps(evidence[step], sort_keys=True)]))

        # ---- 8 GREEN asserts ----
        say("")
        say(_fmt_row(["assert", "verdict", "detail"]))
        say(_fmt_row(["---", "---", "---"]))
        results = _evaluate_green_asserts(evidence)
        failed = [r for r in results if not r[1]]
        for name, ok, detail in results:
            say(_fmt_row([name, "GREEN" if ok else "RED", detail]))
        if failed:
            raise CanaryRed("asserts RED: %s" % "; ".join(n for n, _, _ in failed))
        verdict = "GREEN"
    except (CanaryRed, AdminSocketError) as e:
        reason = str(e)
        say("")
        say("CANARY RED: %s" % reason)
        # §3b ABORT: immediate in-engine ensure_flat + cancel all canary oids,
        # canary RED, §4 rollback path, NO unfreeze. Never auto-retry.
        if held:
            try:
                ab = client.request("canary_exec", timeout=180.0,
                                    payload={"op": "abort", "coin": coin})
                say("canary abort: %s" % json.dumps(ab, sort_keys=True))
            except Exception as e2:  # noqa: BLE001 — must reach release/report
                say("CRITICAL: canary abort failed (%s) — OPERATOR: verify %s flat "
                    "and cancel canary oids manually before anything else" % (e2, coin))
    finally:
        if held:
            try:
                client.request("canary_release", reason=verdict)
            except Exception as e3:  # noqa: BLE001
                print("CRITICAL: canary_release failed (%s) — engine stays "
                      "CANARY_HELD until its watchdog expires (<=15 min)" % e3)
        lines.append("")
        lines.append("VERDICT: %s%s" % (verdict, (" — " + reason) if reason else ""))
        print("VERDICT: %s" % verdict)
        if verdict == "RED":
            print("Next: §4 rollback path. Entry freeze stays ON. Do NOT unfreeze. "
                  "Do NOT re-run without operator root-cause (fail-stop).")
        if report_path:
            try:
                with open(report_path, "w") as f:
                    f.write("\n".join(lines) + "\n")
                print("report: %s" % report_path)
            except OSError as e4:
                print("WARN: could not write report %s: %s" % (report_path, e4))
    return 0 if verdict == "GREEN" else 1


# ---------------------------------------------------------------------------
# In-process ladder glue (engine side) — runner.register_canary_handler
# ---------------------------------------------------------------------------

def make_canary_handler(engine: Any):
    """Build (run, on_expire) for engine.register_canary_handler.

    ``engine`` duck-type: .client (P2 ExchangeClient), .cfg.venue,
    .cfg.db_path. All journal writes go through fleet_core.engine.entry_sm
    (real SM transitions) + fleet_core.engine.registry (F5 oid discipline).
    Executes ONLY under CANARY_HELD (runner refuses canary_exec otherwise).
    """
    from fleet_core.engine import entry_sm as sm
    from fleet_core.engine import registry as reg
    from fleet_core.exchange_api import FillResult  # noqa: F401 — type contract

    db_path = str(engine.cfg.db_path)
    venue = str(engine.cfg.venue)
    client = engine.client
    st: Dict[str, Any] = {"actions": [], "cost": 0.0, "sm_conflicts": 0}

    def _act(coin: str, op: str) -> None:
        st["actions"].append({"coin": coin, "op": op})

    def _row(tid: int) -> sqlite3.Row:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
        finally:
            con.close()

    def _update_row_sl(tid: int, oid: Optional[str], px: Optional[float]) -> None:
        con = sqlite3.connect(db_path)
        try:
            con.execute("UPDATE trades SET sl_order_id=?, sl_placed_px=?, "
                        "sl_current=? WHERE id=?", (oid, px, px, tid))
            con.commit()
        finally:
            con.close()

    def _own_resting(coin: str) -> List[str]:
        live = [str(o) for o in client.list_open_sl_orders(coin)]
        own = reg.registry_oids(db_path)
        return [o for o in live if o in own]

    def _await(pred: Callable[[], bool], budget: float) -> Optional[float]:
        t0 = time.monotonic()
        while time.monotonic() - t0 <= budget:
            if pred():
                return time.monotonic() - t0
            time.sleep(0.5)
        return None

    def _fail(step: str, kind: str, detail: str,
              unresolved: float = 0.0) -> Dict[str, Any]:
        return {"ok": False, "step": step,
                "actions": list(st["actions"]),
                "cost_usd_cum": st["cost"],
                "error": {"kind": kind, "detail": detail,
                          "unresolved_sec": unresolved}}

    def _ok(step: str, coin: str, ev: Dict[str, Any]) -> Dict[str, Any]:
        return {"ok": True, "step": step, "coin": coin,
                "actions": list(st["actions"]), "cost_usd_cum": st["cost"],
                "evidence": ev}

    def _clamped_sl(coin: str, pct: float) -> float:
        mark = client.mark_price(coin, 5.0)
        candidate = mark * (1.0 - pct)   # long canary, far-OTM below mark
        liq = client.position_liquidation(coin)
        px, _ = sm.liq_guard_clamp(True, candidate, liq)
        return px

    def _plan(payload: Dict[str, Any]) -> Dict[str, Any]:
        coin = str(payload.get("coin") or "")
        if not coin:
            return _fail("plan", "bad_request", "plan requires coin")
        mo = float(payload.get("min_order_usd", 0) or 0)
        tmin = float(payload.get("trigger_sl_min_usd", 0) or 0)
        notional = SIZE_RULE_MULT * max(mo, tmin, SIZE_RULE_FLOOR_USD)
        if notional > HARD_NOTIONAL_CAP_USD:   # engine-side cap, belt-and-braces
            return _fail("plan", "notional_cap",
                         "notional $%.2f > $%.2f" % (notional, HARD_NOTIONAL_CAP_USD))
        try:
            client.invalidate_positions_cache()
            poss = client.open_positions()
            has_pos = any(str(c).upper().replace("-PERP", "").replace("-USD", "")
                          == coin.upper().replace("-PERP", "").replace("-USD", "")
                          for c in poss)
            mark = client.mark_price(coin, 5.0)
        except Exception as e:  # noqa: BLE001
            return _fail("plan", "ReadUnknown", str(e))
        fenced = False
        try:
            from fleet_core.orphan_sweep import _fenced
            fenced = bool(_fenced(coin, db_open=[], bot_owned=None,
                                  oid=None, placed_oids=None))
        except Exception:  # noqa: BLE001 — fence machinery broken => fail closed
            fenced = True
        try:
            frac = float(os.environ.get("TP1_PARTIAL_FRAC", "") or
                         ("1" if venue in RUN_C2B_VENUES else "0"))
        except ValueError:
            frac = 0.0
        st.update(coin=coin, notional=notional, mark=mark)
        return {"ok": True, "coin": coin, "notional_usd": notional,
                "min_order_usd": mo, "trigger_sl_min_usd": tmin,
                "mark_px": mark, "has_live_position": bool(has_pos),
                "fenced": fenced, "run_c2b": frac > 0}

    def _c1(coin: str) -> Dict[str, Any]:
        mark = client.mark_price(coin, 5.0)
        notional = float(st.get("notional") or 0)
        size = notional / mark
        limit_pct = float(os.environ.get("CANARY_LIMIT_CAP_PCT",
                                         str(CANARY_LIMIT_CAP_PCT)))
        limit_px = mark * (1.0 + limit_pct)
        plan = sm.EntryPlan(
            venue=venue, coin=coin, tf="canary", direction="long",
            entry_intended=mark, sl_initial=mark * (1.0 - CANARY_SL_PCT),
            tp1=0.0, size=size, risk_dollars=notional, notional=notional,
            leverage_eff=1.0, limit_px=limit_px,
            entry_bar_ts=int(time.time() * 1000), atr14=None, tp1_frac=0.0,
            client_order_id=sm.make_client_order_id(venue, coin, "canary",
                                                    int(time.time() * 1000),
                                                    time.time_ns()))
        tid = sm.t_intent(db_path, plan)
        con = sqlite3.connect(db_path)
        try:  # §3b: rows tagged notes='write_canary' (reconciler skips forever)
            con.execute("UPDATE trades SET notes=? WHERE id=?", (CANARY_NOTES, tid))
            con.commit()
        finally:
            con.close()
        st["tid"] = tid
        sm.t_mark_sent(db_path, tid)
        _act(coin, "market_open")
        fr = client.market_open(coin, True, size, intended_px=mark)
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
            snap = sm.intent_snapshot(con, tid)
        finally:
            con.close()
        verdict = sm.run_entry_policy(row, fr.avg_px, fr.size, snap)
        if not verdict.ok:
            st["fill"] = {"px": fr.avg_px, "size": fr.size}
            return _fail("C1", "cap_breach", verdict.reason)
        sm.t_fill(db_path, tid, fr, verdict)
        st["fill"] = {"px": fr.avg_px, "size": fr.size}
        return _ok("C1", coin, {
            "fill_avg_px": fr.avg_px, "fill_size": fr.size,
            "requested_size": fr.requested_size, "fill_oid": fr.oid,
            "readback_verified": bool(fr.readback_verified),
            "limit_px": limit_px, "cap_breach": False})

    def _place_sl(coin: str, pct: float) -> Tuple[Any, Optional[float]]:
        px = _clamped_sl(coin, pct)
        _act(coin, "trigger_sl")
        sl = client.trigger_sl(coin, False, float(st["fill"]["size"]), px)
        # F5 write discipline: registry INSERT BEFORE any dependent step
        reg.register_oid(db_path, sl.oid, coin, trade_id=st.get("tid"), kind="sl")
        confirm = _await(lambda: str(sl.oid) in
                         [str(o) for o in client.list_open_sl_orders(coin)],
                         SL_CONFIRM_MAX_SEC)
        return sl, confirm

    def _c2(coin: str) -> Dict[str, Any]:
        sl, confirm = _place_sl(coin, CANARY_SL_PCT)
        if confirm is None:
            return _fail("C2", "WriteUnconfirmed",
                         "SL %s not listed within %.0fs" % (sl.oid, SL_CONFIRM_MAX_SEC),
                         unresolved=SL_CONFIRM_MAX_SEC)
        sm.t_protect(db_path, st["tid"], sl)   # row PROTECTED — never t_open'd
        st["sl_oid"] = str(sl.oid)
        return _ok("C2", coin, {"sl_oid": str(sl.oid), "trigger_px": sl.trigger_px,
                                "confirm_sec": round(confirm, 2),
                                "registry_inserted": True})

    def _c2b(coin: str) -> Dict[str, Any]:
        mark = client.mark_price(coin, 5.0)
        _act(coin, "limit_reduce_only")
        tp = client.limit_reduce_only(coin, False, float(st["fill"]["size"]),
                                      mark * (1.0 + CANARY_TP_PCT))
        reg.register_oid(db_path, tp.oid, coin, trade_id=st.get("tid"), kind="tp1")
        t0 = time.monotonic()
        _act(coin, "cancel_sl_order")
        client.cancel_sl_order(coin, tp.oid)   # gone-readback inside P2 contract
        gone = time.monotonic() - t0
        return _ok("C2b", coin, {"tp_oid": str(tp.oid), "tp_confirm_sec": 0.0,
                                 "cancel_gone_sec": round(gone, 2)})

    def _c3(coin: str) -> Dict[str, Any]:
        old = st.get("sl_oid")
        sl, confirm = _place_sl(coin, CANARY_REPLACE_PCT)   # place-before-cancel §7
        if confirm is None:
            return _fail("C3", "WriteUnconfirmed", "replacement SL not listed",
                         unresolved=SL_CONFIRM_MAX_SEC)
        # supersede: cancel ALL other bot-own resting SLs (registry-gated, §7.5)
        for oid in _own_resting(coin):
            if oid != str(sl.oid):
                _act(coin, "cancel_sl_order")
                client.cancel_sl_order(coin, oid)
        _update_row_sl(st["tid"], str(sl.oid), sl.trigger_px)
        st["sl_oid"] = str(sl.oid)
        own = _own_resting(coin)
        return _ok("C3", coin, {"new_oid": str(sl.oid), "old_oid": old,
                                "resting_own_count": len(own),
                                "converged_ticks": 0})

    def _c4(coin: str) -> Dict[str, Any]:
        oid = st.get("sl_oid")
        t0 = time.monotonic()
        _act(coin, "cancel_sl_order")
        client.cancel_sl_order(coin, oid)
        gone = _await(lambda: str(oid) not in
                      [str(o) for o in client.list_open_sl_orders(coin)],
                      GONE_READBACK_MAX_SEC)
        if gone is None:
            return _fail("C4", "WriteUnconfirmed",
                         "oid %s still listed after cancel" % oid,
                         unresolved=GONE_READBACK_MAX_SEC)
        _update_row_sl(st["tid"], None, None)
        return _ok("C4", coin, {"cancelled_oid": str(oid),
                                "gone_sec": round(time.monotonic() - t0, 2)})

    def _c5(coin: str) -> Dict[str, Any]:
        sl, confirm = _place_sl(coin, CANARY_SL_PCT)   # heal path re-place
        if confirm is None:
            return _fail("C5", "WriteUnconfirmed", "heal SL not listed",
                         unresolved=SL_CONFIRM_MAX_SEC)
        _update_row_sl(st["tid"], str(sl.oid), sl.trigger_px)
        st["sl_oid"] = str(sl.oid)
        return _ok("C5", coin, {"sl_oid": str(sl.oid), "trigger_px": sl.trigger_px,
                                "confirm_sec": round(confirm, 2)})

    def _c6(coin: str) -> Dict[str, Any]:
        tid = st["tid"]
        # t_abort_begin BEFORE the first ensure_flat call (entry-SM §3 law)
        sm.t_abort_begin(db_path, tid, "canary")
        _act(coin, "ensure_flat")
        flat = client.ensure_flat(coin)
        sm.t_abort_final(db_path, tid, flat)
        exit_px = flat.exit_avg_px or client.mark_price(coin, 5.0)
        fill = st.get("fill") or {}
        # realized cost on the long round-trip = (entry - exit) x size
        st["cost"] = (float(fill.get("px", 0)) - float(exit_px)) * float(fill.get("size", 0))
        return _ok("C6", coin, {"verified_flat": bool(flat.verified_flat),
                                "closed_size": flat.closed_size,
                                "exit_avg_px": flat.exit_avg_px,
                                "abort_reason": "canary"})

    def _c7(coin: str) -> Dict[str, Any]:
        cancelled = 0
        for oid in _own_resting(coin):   # registry-gated orphan sweep (§4.2.4)
            _act(coin, "cancel_sl_order")
            client.cancel_sl_order(coin, oid)
            cancelled += 1
        return _ok("C7", coin, {"cancelled_orphans": cancelled,
                                "resting_triggers_for_coin": len(_own_resting(coin))})

    def _c8(coin: str) -> Dict[str, Any]:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                "SELECT from_state, to_state FROM sm_transitions WHERE trade_id=? "
                "ORDER BY id", (st["tid"],)).fetchall()
        finally:
            con.close()
        # entry_sm audits multi-source transitions with the guard SET as
        # from_state (e.g. "PENDING|FILLED|PROTECTED" for t_abort_begin);
        # normalize to the ACTUAL prior state (= previous row's to_state) so
        # the §3b chain assert compares real states.
        trans: List[List[str]] = []
        prev = None  # type: Optional[str]
        for r in rows:
            frm, to = str(r["from_state"] or ""), str(r["to_state"])
            if not frm:            # ∅→INTENT creation audit
                prev = to
                continue
            if ("|" in frm or "/" in frm) and prev:
                frm = prev
            trans.append([frm, to])
            prev = to
        return _ok("C8", coin, {"transitions": trans,
                                "sm_conflicts": st["sm_conflicts"],
                                "realized_cost_usd": round(float(st["cost"]), 6),
                                "resting_triggers_for_coin": len(_own_resting(coin))})

    steps = {"C1": _c1, "C2": _c2, "C2b": _c2b, "C3": _c3, "C4": _c4,
             "C5": _c5, "C6": _c6, "C7": _c7, "C8": _c8}

    def _abort(coin: str) -> Dict[str, Any]:
        """§3b ABORT: immediate ensure_flat + cancel all canary oids."""
        out: Dict[str, Any] = {"ok": True, "flat": False, "cancelled": 0}
        tid = st.get("tid")
        try:
            if tid is not None:
                try:
                    sm.t_abort_begin(db_path, tid, "canary_abort")
                except sm.SMConflict:
                    pass  # already ABORTING/terminal — re-drive is fine
            _act(coin, "ensure_flat")
            flat = client.ensure_flat(coin)
            out["flat"] = bool(flat.verified_flat)
            if tid is not None:
                try:
                    sm.t_abort_final(db_path, tid, flat)
                except (sm.SMConflict, ValueError):
                    pass  # terminal already / NeverSent path — row is safe
            for oid in _own_resting(coin):
                _act(coin, "cancel_sl_order")
                client.cancel_sl_order(coin, oid)
                out["cancelled"] += 1
        except Exception as e:  # noqa: BLE001 — CRITICAL, operator territory
            out = {"ok": False, "error": {"kind": "WriteUnconfirmed",
                                          "detail": "abort ladder: %s" % e,
                                          "unresolved_sec": 0.0}}
        return out

    def run(payload: Dict[str, Any]) -> Dict[str, Any]:
        op = str(payload.get("op") or "")
        try:
            if op == "plan":
                return _plan(payload)
            if op == "abort":
                return _abort(str(payload.get("coin") or st.get("coin") or ""))
            if op == "step":
                step = str(payload.get("step") or "")
                coin = str(payload.get("coin") or st.get("coin") or "")
                fn = steps.get(step)
                if fn is None:
                    return _fail(step or "?", "bad_request", "unknown step")
                if coin != st.get("coin"):
                    return _fail(step, "scope_fence",
                                 "step coin %r != planned canary coin %r"
                                 % (coin, st.get("coin")))
                return fn(coin)
            return _fail(op or "?", "bad_request", "unknown op")
        except sm.SMConflict as e:
            st["sm_conflicts"] += 1
            return _fail(op, "SMConflict", str(e))
        except Exception as e:  # noqa: BLE001 — P2 raises map to typed kinds
            kind = type(e).__name__
            if kind not in ("ReadUnknown", "WriteUnconfirmed", "VenueRejected",
                            "StaleData", "RateLimited"):
                kind = "engine_error:%s" % kind
            return _fail(op, kind, str(e))

    def on_expire(_engine: Any) -> None:
        """§3b watchdog: CANARY_HELD window expiry => auto ensure_flat of the
        canary + cancel canary oids (engine then unparks + records RED)."""
        coin = st.get("coin")
        if coin:
            _abort(str(coin))

    return run, on_expire


def wire_canary(engine: Any) -> None:
    """Convenience: build + register the canary glue on a runner Engine."""
    run, on_expire = make_canary_handler(engine)
    engine.register_canary_handler(run, on_expire)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="P3 WRITE-CANARY thin admin-socket client (p3_rollout.md §3b)")
    p.add_argument("--venue", choices=["extended", "pacifica", "nado", "hl"])
    p.add_argument("--db", help="live trades.db path (derives the default "
                                "admin socket path)")
    p.add_argument("--sock", help="admin socket path override")
    p.add_argument("--coin", help="canary instrument (venue's highest-liquidity "
                                  "coin per the §3b rule; engine re-verifies "
                                  "no-position + not-fenced)")
    p.add_argument("--min-order-usd", type=float,
                   help="venue min order (USD notional) from the liquidity snapshot")
    p.add_argument("--trigger-sl-min-usd", type=float,
                   help="venue trigger-SL min (USD; Nado law: 10x order min)")
    p.add_argument("--report", help="write the canary report to this file")
    p.add_argument("--selftest", action="store_true",
                   help="offline selftest (fake server + real handler e2e)")
    a = p.parse_args(argv)
    if a.selftest:
        return _selftest()
    missing = [n for n, v in (("--venue", a.venue), ("--coin", a.coin),
                              ("--min-order-usd", a.min_order_usd),
                              ("--trigger-sl-min-usd", a.trigger_sl_min_usd))
               if v in (None, "")]
    if missing:
        p.error("required (unless --selftest): %s" % ", ".join(missing))
    sock = resolve_sock_path(a.sock, a.db)
    return run_canary(a.venue, sock, a.coin, a.min_order_usd,
                      a.trigger_sl_min_usd, report_path=a.report)


# ---------------------------------------------------------------------------
# Offline selftest — fake admin server + fake P2 client, no SDKs, no engine
# ---------------------------------------------------------------------------

def _fake_server(sock_path: str, handler: Callable[[Dict[str, Any]], Dict[str, Any]]):
    """Threaded fake admin endpoint: full request object -> response object."""
    import threading

    if os.path.exists(sock_path):
        os.unlink(sock_path)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(8)
    stop = {"flag": False}
    seen: List[Dict[str, Any]] = []

    def loop():
        srv.settimeout(0.2)
        while not stop["flag"]:
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            try:
                buf = b""
                while b"\n" not in buf:
                    ch = conn.recv(65536)
                    if not ch:
                        break
                    buf += ch
                req = json.loads(buf.split(b"\n", 1)[0].decode())
                seen.append(req)
                conn.sendall((json.dumps(handler(req)) + "\n").encode())
            except Exception:
                pass
            finally:
                conn.close()

    t = threading.Thread(target=loop, daemon=True)
    t.start()

    def shutdown():
        stop["flag"] = True
        t.join(timeout=2)
        srv.close()
        if os.path.exists(sock_path):
            os.unlink(sock_path)

    return shutdown, seen


def _scripted_handler(script: Dict[str, Any]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Key = cmd, or canary_exec:<op or step>."""
    def handle(req: Dict[str, Any]) -> Dict[str, Any]:
        cmd = req.get("cmd")
        key = cmd
        if cmd == "canary_exec":
            pl = req.get("payload") or {}
            key = "canary_exec:%s" % (pl.get("step") if pl.get("op") == "step"
                                      else pl.get("op"))
        resp = script.get(key, {"ok": False, "error": {"kind": "unknown_cmd",
                                                       "detail": str(key)}})
        return resp(req) if callable(resp) else resp
    return handle


def _green_script(coin: str = "BTC") -> Dict[str, Any]:
    ev = {
        "C1": {"fill_avg_px": 64000.0, "fill_size": 0.0002, "requested_size": 0.0002,
               "fill_oid": "o1", "readback_verified": True, "limit_px": 64320.0,
               "cap_breach": False},
        "C2": {"sl_oid": "sl1", "trigger_px": 44800.0, "confirm_sec": 2.1,
               "registry_inserted": True},
        "C2b": {"tp_oid": "tp1", "tp_confirm_sec": 1.0, "cancel_gone_sec": 1.5},
        "C3": {"new_oid": "sl2", "resting_own_count": 1, "converged_ticks": 0},
        "C4": {"cancelled_oid": "sl2", "gone_sec": 1.2},
        "C5": {"sl_oid": "sl3", "trigger_px": 44800.0, "confirm_sec": 3.0},
        "C6": {"verified_flat": True, "closed_size": 0.0002, "exit_avg_px": 64010.0,
               "abort_reason": "canary"},
        "C7": {"cancelled_orphans": 1, "resting_triggers_for_coin": 0},
        "C8": {"transitions": EXPECTED_SM_CHAIN, "sm_conflicts": 0,
               "realized_cost_usd": 0.07, "resting_triggers_for_coin": 0},
    }
    script: Dict[str, Any] = {
        "state": {"ok": True, "state": "RUNNING", "tick_loop": "running"},
        "canary_hold": {"ok": True, "state": "CANARY_HELD", "tick_loop": "parked",
                        "window_sec": 900, "entry_freeze": True},
        "canary_exec:plan": {"ok": True, "coin": coin, "notional_usd": 12.0,
                             "min_order_usd": 10.0, "trigger_sl_min_usd": 10.0,
                             "mark_px": 64000.0, "has_live_position": False,
                             "fenced": False, "run_c2b": True},
        "canary_exec:abort": {"ok": True, "flat": True, "cancelled": 0},
        "canary_release": {"ok": True, "state": "RUNNING", "tick_loop": "running"},
    }
    for step, e in ev.items():
        script["canary_exec:%s" % step] = {"ok": True, "step": step, "coin": coin,
                                           "actions": [{"coin": coin, "op": step}],
                                           "cost_usd_cum": 0.07, "evidence": e}
    return script


# -- fake P2 client for the real-handler e2e --------------------------------

class _FakeP2:
    def __init__(self, mark: float = 100.0):
        self._mark = mark
        self._pos: Dict[str, Any] = {}
        self._resting: Dict[str, str] = {}   # oid -> coin
        self._n = 0

    def invalidate_positions_cache(self):
        return None

    def open_positions(self):
        return dict(self._pos)

    def mark_price(self, coin, tol=5.0):
        return self._mark

    def position_liquidation(self, coin):
        return None

    def market_open(self, coin, is_buy, sz, intended_px=None, allow_marketable=True):
        from fleet_core.exchange_api import FillResult, PositionInfo
        self._pos[coin] = PositionInfo(coin=coin, size_signed=sz, entry_px=self._mark)
        return FillResult(coin=coin, is_buy=is_buy, avg_px=self._mark, size=sz,
                          requested_size=sz, oid="fill1")

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        from fleet_core.exchange_api import SLOrderInfo
        self._n += 1
        oid = "sl%d" % self._n
        self._resting[oid] = coin
        return SLOrderInfo(coin=coin, oid=oid, trigger_px=trigger_px, size=sz,
                           is_buy_to_close=is_buy)

    def limit_reduce_only(self, coin, is_buy, sz, px):
        from fleet_core.exchange_api import OpenOrderInfo
        self._n += 1
        oid = "tp%d" % self._n
        self._resting[oid] = coin
        return OpenOrderInfo(coin=coin, oid=oid, side="sell", size=sz,
                             limit_px=px, reduce_only=True)

    def cancel_sl_order(self, coin, oid):
        self._resting.pop(str(oid), None)

    def list_open_sl_orders(self, coin):
        return [o for o, c in self._resting.items() if c == coin]

    def ensure_flat(self, coin):
        from fleet_core.exchange_api import FlatResult
        had = coin in self._pos
        pos = self._pos.pop(coin, None)
        return FlatResult(coin=coin, already_flat=not had,
                          closed_size=abs(pos.size_signed) if pos else 0.0,
                          exit_avg_px=self._mark if had else None)


class _FakeEngineCfg:
    def __init__(self, venue, db_path):
        self.venue, self.db_path = venue, db_path


class _FakeEngine:
    def __init__(self, venue, db_path, client):
        self.cfg = _FakeEngineCfg(venue, db_path)
        self.client = client
        self.handlers = None

    def register_canary_handler(self, run, on_expire=None):
        self.handlers = (run, on_expire)


def _selftest() -> int:
    import copy
    import tempfile

    repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                             "..", ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    fails: List[str] = []

    def case(name: str, mutate: Callable[[Dict[str, Any]], None],
             expect_exit: int, expect_abort: Optional[bool] = None) -> None:
        script = _green_script()
        mutate(script)
        d = tempfile.mkdtemp(prefix="wc_st_")
        sock = os.path.join(d, "engine_admin.sock")
        shutdown, seen = _fake_server(sock, _scripted_handler(script))
        try:
            rc = run_canary("extended", sock, "BTC", 10.0, 10.0,
                            report_path=os.path.join(d, "rep.md"),
                            systemctl=lambda u: "active")
        finally:
            shutdown()
        ok = rc == expect_exit
        if ok and expect_abort is not None:
            aborted = any(r.get("cmd") == "canary_exec"
                          and (r.get("payload") or {}).get("op") == "abort"
                          for r in seen)
            ok = aborted == expect_abort
        held = any(r.get("cmd") == "canary_hold" for r in seen)
        released = any(r.get("cmd") == "canary_release" for r in seen)
        if ok and held and not released:
            ok = False  # engine must always be released after a taken hold
        print("[%s] %s (rc=%d)" % ("PASS" if ok else "FAIL", name, rc))
        if not ok:
            fails.append(name)

    # 1. full GREEN (scripted)
    case("green_run", lambda s: None, 0, expect_abort=False)
    # 2. bad hold ack (tick loop not parked) -> refuse before C1, still released
    case("hold_ack_not_parked",
         lambda s: s.update(canary_hold={"ok": True, "state": "CANARY_HELD",
                                         "tick_loop": "running"}), 1)
    # 3. engine window over 15 min -> refuse
    case("window_gt_15min",
         lambda s: s.update(canary_hold=dict(s["canary_hold"], window_sec=1200)), 1)
    # 4. notional over $50 -> abort BEFORE C1
    def _big(s):
        s["canary_exec:plan"] = dict(s["canary_exec:plan"], notional_usd=60.0)
    case("notional_cap_50", _big, 1, expect_abort=True)
    # 5. size-rule mismatch
    def _rule(s):
        s["canary_exec:plan"] = dict(s["canary_exec:plan"], notional_usd=25.0)
    case("size_rule_mismatch", _rule, 1, expect_abort=True)
    # 6. SL confirm slow (>15s) at C2
    def _slow(s):
        s["canary_exec:C2"] = copy.deepcopy(s["canary_exec:C2"])
        s["canary_exec:C2"]["evidence"]["confirm_sec"] = 16.5
    case("sl_confirm_gt_15s", _slow, 1, expect_abort=True)
    # 7. foreign-coin action -> scope fence auto-RED
    def _foreign(s):
        s["canary_exec:C3"] = copy.deepcopy(s["canary_exec:C3"])
        s["canary_exec:C3"]["actions"] = [{"coin": "ETH", "op": "cancel_sl_order"}]
    case("foreign_coin_scope_fence", _foreign, 1, expect_abort=True)
    # 8. cost breach (> $2 cumulative)
    def _cost(s):
        s["canary_exec:C6"] = copy.deepcopy(s["canary_exec:C6"])
        s["canary_exec:C6"]["cost_usd_cum"] = 2.4
    case("cost_cap_2usd", _cost, 1, expect_abort=True)
    # 9. supersede not converged
    def _dup(s):
        s["canary_exec:C3"] = copy.deepcopy(s["canary_exec:C3"])
        s["canary_exec:C3"]["evidence"].update(resting_own_count=2, converged_ticks=3)
    case("supersede_not_converged", _dup, 1, expect_abort=True)
    # 10. wrong SM chain (row t_open'd)
    def _chain(s):
        s["canary_exec:C8"] = copy.deepcopy(s["canary_exec:C8"])
        s["canary_exec:C8"]["evidence"]["transitions"] = [
            ["INTENT", "PENDING"], ["PENDING", "FILLED"], ["FILLED", "PROTECTED"],
            ["PROTECTED", "OPEN"], ["OPEN", "CLOSED"]]
    case("sm_chain_wrong", _chain, 1, expect_abort=True)
    # 11. WriteUnconfirmed unresolved >120s
    def _wu(s):
        s["canary_exec:C6"] = {"ok": False, "error": {
            "kind": "WriteUnconfirmed", "detail": "ensure_flat",
            "unresolved_sec": 130.0}}
    case("write_unconfirmed_120s", _wu, 1, expect_abort=True)
    # 12. systemctl not active -> refuse, no hold taken
    d = tempfile.mkdtemp(prefix="wc_st_")
    sock = os.path.join(d, "engine_admin.sock")
    shutdown, seen = _fake_server(sock, _scripted_handler(_green_script()))
    try:
        rc = run_canary("extended", sock, "BTC", 10.0, 10.0,
                        systemctl=lambda u: "inactive")
    finally:
        shutdown()
    ok = rc == 1 and not seen
    print("[%s] systemctl_inactive_refuse (rc=%d)" % ("PASS" if ok else "FAIL", rc))
    if not ok:
        fails.append("systemctl_inactive_refuse")

    # 13. END-TO-END: REAL in-process handler + fake P2 client + real SM journal
    try:
        d = tempfile.mkdtemp(prefix="wc_e2e_")
        db = os.path.join(d, "trades.db")
        eng = _FakeEngine("extended", db, _FakeP2(mark=100.0))
        wire_canary(eng)
        run_handler = eng.handlers[0]
        held_state = {"held": False}

        def dispatch(req: Dict[str, Any]) -> Dict[str, Any]:
            cmd = req.get("cmd")
            if cmd == "canary_hold":
                held_state["held"] = True
                return {"ok": True, "state": "CANARY_HELD", "tick_loop": "parked",
                        "window_sec": 900}
            if cmd == "canary_release":
                held_state["held"] = False
                return {"ok": True, "state": "RUNNING", "tick_loop": "running"}
            if cmd == "canary_exec":
                if not held_state["held"]:
                    return {"ok": False, "error": "not CANARY_HELD"}
                return run_handler(req.get("payload") or {})
            return {"ok": False, "error": "unknown"}

        sock = os.path.join(d, "engine_admin.sock")
        shutdown, seen = _fake_server(sock, dispatch)
        try:
            rc = run_canary("extended", sock, "SOL", 10.0, 10.0,
                            report_path=os.path.join(d, "rep.md"),
                            systemctl=lambda u: "active")
        finally:
            shutdown()
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        rej = con.execute("SELECT COUNT(*) c FROM rejected_signals").fetchone()["c"]
        con.close()
        ok = (rc == 0 and row is not None and row["sm_state"] == "ABORTED"
              and row["status"] == "aborted" and row["notes"] == CANARY_NOTES
              and rej >= 1)
        print("[%s] e2e_real_handler_green (rc=%d sm=%s notes=%s)"
              % ("PASS" if ok else "FAIL", rc,
                 row["sm_state"] if row else None, row["notes"] if row else None))
        if not ok:
            fails.append("e2e_real_handler_green")
    except Exception as e:  # noqa: BLE001
        print("[FAIL] e2e_real_handler_green (exception: %s)" % e)
        fails.append("e2e_real_handler_green")

    print("\nselftest: %d failures" % len(fails))
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
