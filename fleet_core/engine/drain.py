"""fleet_core.engine.drain — DRAIN procedure tools (P3 rollout §4).

Spec: p3_rollout.md §4 "DRAIN procedure (in-flight ladders)" + review nits:

  (1) ``--drain`` (engine ALIVE) ROUTES THROUGH THE ENGINE ADMIN SOCKET —
      the engine itself drives every non-terminal pre-OPEN row to convergence
      per entry-SM §4.1 (PENDING -> policy re-run -> FILLED ladder or abort;
      FILLED -> protect -> OPEN; ABORTING -> re-drive ensure_flat -> ABORTED),
      bounded 5 reconciler ticks (~5 min). This tool sends one ``drain``
      command and prints the engine's row-by-row report. It never writes
      trades.db itself in this mode (single writer: the engine holds the §4
      flock throughout).

  (2) MANUAL tools (engine DEAD/crash-looping) — the standalone drain runbook:
      ``--protect``, ``--resolve-pending``, ``--resolve-aborting``, and
      ``--resolve-phantom``. These REFUSE to run unless BOTH hold:
        a. engine dead: ``systemctl is-active fleet-<venue>-bot`` != active
           AND the admin socket is not connectable;
        b. flock free: this process takes the EXCLUSIVE flock on
           ``<db_dir>/trades.db.lock`` (the same file both units flock, §4
           mutual exclusion) and holds it for the whole operation — if held
           by anyone, refuse (exit 3). There are ZERO exemptions: while a
           manual tool runs, IT is the single writer.

MANUAL TOOL SEMANTICS (entry-SM §3/§4.1 implemented verbatim; every state
transition = one UPDATE guarded ``WHERE sm_state = :from`` + an append-only
``sm_transitions`` row + the §2.3 legacy-status mapping so the OLD bot sees
exactly today's semantics after rollback):

  --protect (per FILLED/naked row, or a named --coin):
      PROTECTIVE-PLACEMENT LAW (entry-SM §4, R3/R2-F1c) runs FIRST: candidate
      px (row sl_initial) through or at current mark -> re-anchor to CURRENT
      mark -/+6% (_RESTORE_REANCHOR_PCT), liq-guarded (SL inside liq x1.01),
      NEVER close, labeled deviation logged. Then trigger_sl -> readback-
      verified SLOrderInfo -> registry INSERT (placed_trigger_oids, BEFORE any
      dependent persist — F5 write discipline) -> row FILLED->PROTECTED->OPEN.
      REFUSED for management_class in {protect_only, manual_claimed}: the
      standalone protect has strategy-row authority only (protect_only heal is
      engine cover-check semantics; manual_claimed = zero placement authority).

  --resolve-pending (per PENDING row): gather venue evidence (position /
      user_fills matched by client_order_id / resting remnant), then RE-RUN
      CALLER POLICY against the row's PERSISTED limit_px (+-0.1% tick tol) and
      the INTENT-snapshot min_fill_ratio (sm_transitions detail, fallback env
      MIN_FILL_RATIO=0.10). Pass -> t_fill with REAL px/size -> protect ladder
      -> OPEN. Reject -> t_abort_begin (abort_reason + entry_cooldowns durable
      BEFORE unwind I/O) -> ensure_flat -> t_abort_final. No evidence ->
      VerifiedAbsent abort (cache-invalidated re-read + empty fills lookup).
      The reconciler policy-re-run law (F3) binds here identically: a rejected
      fill is NEVER resurrected as a managed position.

  --resolve-aborting (per ABORTING row): abort_reason is durable -> decision
      unambiguous: RE-DRIVE the unwind, never resurrect. Position live ->
      ensure_flat -> t_abort_final(FlatResult). Already flat -> t_abort_final.
      WriteUnconfirmed -> protective SL on the residual (+-2.5% mark,
      liq-guarded, exit-engine §9 step 5) + row STAYS ABORTING + CRITICAL +
      exit non-zero.

  --resolve-phantom (per OPEN row whose position is verifiably ABSENT) —
      REVIEW NIT 2, explicit spec: rows with management_class 'protect_only'
      AND 'manual_claimed' get the SAME phantom-K3 DB-ONLY row-resolve
      (entry-SM §4.2.3 authority whitelist (ii): position gone -> close the
      ROW with attribution; a row-close touches the DB, not the venue — it is
      therefore legal for manual_claimed too, whose VENUE intents are all
      refused). Attribution: oid-first against the registry corpus via
      user_fills, then 1% px proximity, else reason
      'phantom_no_exchange_position'. Absence must be PROVEN: cache-
      invalidated open_positions() re-read; ReadUnknown -> refuse (bias-to-
      protect). STRATEGY rows are REFUSED here (their OPEN-absent resolution
      belongs to the reconciler's evidence path, entry-SM §4.1).

After ANY manual mutation the tool prints the mandatory next step: re-run
``cutover_check --pre-rollback`` -> GREEN -> only then start the old unit
(rollout §4: NEVER start the old unit while the check is RED).

Offline selftest: ``python3 drain.py --selftest`` (fakes; no SDKs, no engine).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import fcntl  # POSIX only — both live hosts and dev Macs have it
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

__all__ = ["main", "run_socket_drain", "manual_protect", "manual_resolve_pending",
           "manual_resolve_aborting", "manual_resolve_phantom"]

# ---------------------------------------------------------------------------
# Labeled constants
# ---------------------------------------------------------------------------

RESTORE_REANCHOR_PCT = 0.06   # entry-SM §4 protective-placement law: CURRENT mark -/+6%
RESIDUAL_SL_PCT = 0.025       # exit-engine §9 step 5: residual protective SL +-2.5% mark
LIQ_BUFFER = 1.01             # SL must sit INSIDE liquidation x1.01 (fleet law)
CAP_TICK_TOL = 0.001          # entry-SM §4.1: cap-breach re-run vs persisted limit_px +-0.1%
DEFAULT_MIN_FILL_RATIO = 0.10  # exit-engine §8 trader values: MIN_FILL_RATIO=10%
ENTRY_ABORT_COOLDOWN_SEC = 900.0  # ext/nado/hl 15 min == pac 900s (exit-engine §8 — same value)
PROXIMITY_PCT = 0.01          # exit-engine §6: 1% px proximity fallback

# entry-SM §2.3 sm_state -> legacy status (old bot must see today's semantics)
LEGACY_STATUS = {"INTENT": "pending", "PENDING": "pending", "FILLED": "pending",
                 "PROTECTED": "pending", "ABORTING": "pending",
                 "OPEN": "open", "CLOSED": "closed", "ABORTED": "aborted"}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# Engine-dead + flock gate (manual tools)
# ---------------------------------------------------------------------------

def _systemctl_is_active(unit: str) -> str:
    try:
        out = subprocess.run(["systemctl", "is-active", unit],
                             capture_output=True, text=True, timeout=10)
        return (out.stdout or out.stderr or "").strip() or "unknown"
    except Exception:
        return "unknown"


def _load_admin_client_cls():
    """Shared admin client lives in write_canary.py (same package, both mine).
    Robust to direct-run without a package __init__."""
    try:
        from fleet_core.engine.write_canary import EngineAdminClient
        return EngineAdminClient
    except Exception:
        import importlib.util
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "write_canary.py")
        spec = importlib.util.spec_from_file_location("_wc_drain", p)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod.EngineAdminClient


def _resolve_sock(sock: Optional[str], db_path: str) -> str:
    if sock:
        return sock
    env = os.environ.get("FLEET_ADMIN_SOCK")
    if env:
        return env
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), "engine_admin.sock")


class ManualGate:
    """Refuse-unless-(engine dead + flock free); holds the flock while open."""

    def __init__(self, venue: str, db_path: str, sock: Optional[str] = None,
                 systemctl: Callable[[str], str] = _systemctl_is_active) -> None:
        self.venue, self.db_path = venue, db_path
        self.sock = _resolve_sock(sock, db_path)
        self.systemctl = systemctl
        self.lock_path = os.path.join(os.path.dirname(os.path.abspath(db_path)),
                                      "trades.db.lock")
        self._fh = None

    def acquire(self) -> Optional[str]:
        """Returns refusal reason, or None when the gate is passed (flock HELD)."""
        unit = "fleet-%s-bot" % self.venue
        state = self.systemctl(unit)
        if state == "active":
            return ("engine ALIVE (systemctl is-active %s == active) — use "
                    "`--drain` (admin-socket route), not the manual tools" % unit)
        cls = _load_admin_client_cls()
        if cls(self.sock).connectable():
            return ("admin socket %s is connectable — an engine process is "
                    "alive; use `--drain`" % self.sock)
        if fcntl is None:
            return "fcntl unavailable on this platform — cannot prove single-writer"
        self._fh = open(self.lock_path, "a+")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fh.close()
            self._fh = None
            return ("flock on %s is HELD by another process (F10 dual-writer "
                    "guard) — refusing" % self.lock_path)
        return None

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None


# ---------------------------------------------------------------------------
# Journal plumbing (entry-SM §1/§2 semantics, standalone)
# ---------------------------------------------------------------------------

def _conn(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def _ensure_sm_tables(con: sqlite3.Connection) -> None:
    """ADDITIVE, idempotent (entry-SM §2.2 DDL)."""
    con.executescript("""
        CREATE TABLE IF NOT EXISTS sm_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER NOT NULL,
            at TEXT NOT NULL,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            detail TEXT
        );
        CREATE TABLE IF NOT EXISTS entry_cooldowns (
            coin TEXT PRIMARY KEY,
            until_ts REAL NOT NULL,
            reason TEXT
        );
        CREATE TABLE IF NOT EXISTS placed_trigger_oids (
            oid TEXT PRIMARY KEY,
            coin TEXT NOT NULL,
            trade_id INTEGER,
            placed_at TEXT NOT NULL,
            kind TEXT NOT NULL
        );
    """)


class SMConflict(RuntimeError):
    pass


def _transition(con: sqlite3.Connection, tid: int, frm: Tuple[str, ...], to: str,
                detail: Dict[str, Any], extra_sets: str = "",
                extra_vals: Tuple = ()) -> None:
    """One guarded UPDATE (WHERE sm_state IN frm) + sm_transitions append +
    §2.3 legacy status maintained. 0 rows + already-at-target = idempotent OK;
    0 rows + elsewhere = SMConflict (entry-SM §1 rules)."""
    sets = "sm_state=?, sm_updated_at=?, status=?" + \
        ((", " + extra_sets) if extra_sets else "")
    vals = (to, _now_iso(), LEGACY_STATUS[to]) + tuple(extra_vals) + (tid,)
    cur = con.execute(
        "UPDATE trades SET %s WHERE id=? AND sm_state IN (%s)"
        % (sets, ",".join("?" * len(frm))), vals + frm)
    if cur.rowcount == 0:
        row = con.execute("SELECT sm_state FROM trades WHERE id=?", (tid,)).fetchone()
        cur_state = row["sm_state"] if row else None
        if cur_state == to:
            return  # replay-safe no-op
        raise SMConflict("trade %d: cannot %s->%s (row at %r)"
                         % (tid, "/".join(frm), to, cur_state))
    con.execute("INSERT INTO sm_transitions (trade_id, at, from_state, to_state, detail) "
                "VALUES (?,?,?,?,?)",
                (tid, _now_iso(), "/".join(frm), to, json.dumps(detail, sort_keys=True)))


def _register_oid(con: sqlite3.Connection, oid: str, coin: str, tid: Optional[int],
                  kind: str = "sl") -> None:
    """F5 write discipline: INSERT the oid BEFORE any dependent persist."""
    con.execute("INSERT OR IGNORE INTO placed_trigger_oids "
                "(oid, coin, trade_id, placed_at, kind) VALUES (?,?,?,?,?)",
                (str(oid), coin, tid, _now_iso(), kind))
    con.commit()


def _abort_begin(con: sqlite3.Connection, row: sqlite3.Row, reason: str) -> None:
    """t_abort_begin: abort_reason + cooldown durable BEFORE any unwind I/O
    (entry-SM §3; a crash 1ms later still leaves the cooldown armed)."""
    _transition(con, row["id"], ("PENDING", "FILLED", "PROTECTED"), "ABORTING",
                {"abort_reason": reason},
                extra_sets="abort_reason=?", extra_vals=(reason,))
    con.execute("INSERT OR REPLACE INTO entry_cooldowns (coin, until_ts, reason) "
                "VALUES (?,?,?)",
                (row["coin"], time.time() + ENTRY_ABORT_COOLDOWN_SEC, reason))
    con.commit()


def _abort_final(con: sqlite3.Connection, tid: int, proof: str,
                 detail: Dict[str, Any]) -> None:
    _transition(con, tid, ("ABORTING", "INTENT"), "ABORTED",
                dict(detail, proof=proof))
    con.commit()


# ---------------------------------------------------------------------------
# Protective-placement law + liq guard (entry-SM §4, exit-engine §7)
# ---------------------------------------------------------------------------

def _liq_guard(direction: str, px: float, liq: Optional[float]) -> Tuple[float, bool]:
    """Clamp SL inside liquidation x1.01. Returns (px, clamped)."""
    if liq is None or liq <= 0:
        return px, False
    if direction == "long":
        floor = liq * LIQ_BUFFER
        if px < floor:
            return floor, True
    else:
        ceil = liq * (2.0 - LIQ_BUFFER)  # liq x0.99 mirror for shorts
        if px > ceil:
            return ceil, True
    return px, False


def _protective_px(direction: str, candidate: Optional[float], mark: float,
                   liq: Optional[float], pct: float,
                   log: Callable[[str], None]) -> float:
    """PROTECTIVE-PLACEMENT LAW (entry-SM §4, R3/R2-F1c): through-px re-anchor
    check runs FIRST, BEFORE any protective placement. Re-anchor target is
    always CURRENT mark -/+pct — never a remembered/historic/user px."""
    through = candidate is None or \
        (direction == "long" and candidate >= mark) or \
        (direction != "long" and candidate <= mark)
    if through:
        px = mark * (1.0 - pct) if direction == "long" else mark * (1.0 + pct)
        if candidate is not None:
            log("LABELED DEVIATION: candidate SL %.10g through/at mark %.10g -> "
                "re-anchored to CURRENT mark -/+%.1f%% = %.10g (never close, "
                "never the remembered px)" % (candidate, mark, pct * 100, px))
    else:
        px = float(candidate)
    px, clamped = _liq_guard(direction, px,
                             liq)
    if clamped:
        log("LABELED DEVIATION: SL clamped inside liquidation x%.2f -> %.10g"
            % (LIQ_BUFFER, px))
    return px


def _find_position(client: Any, coin: str) -> Optional[Any]:
    client.invalidate_positions_cache()
    poss = client.open_positions()  # raises => caller refuses (bias-to-protect)
    for c, p in poss.items():
        base = str(c).upper().replace("-PERP", "").replace("-USD", "")
        want = str(coin).upper().replace("-PERP", "").replace("-USD", "")
        if base == want:
            return p
    return None


# ---------------------------------------------------------------------------
# Manual tools
# ---------------------------------------------------------------------------

def manual_protect(con: sqlite3.Connection, client: Any, row: sqlite3.Row,
                   log: Callable[[str], None] = print) -> bool:
    """FILLED/naked row -> place SL -> registry INSERT -> PROTECTED -> OPEN."""
    mclass = row["management_class"] if "management_class" in row.keys() else None
    if (mclass or "strategy") != "strategy":
        log("REFUSE protect row#%d %s: management_class=%r — standalone protect "
            "has strategy-row authority only (protect_only = engine cover-check "
            "heal; manual_claimed = zero placement authority)"
            % (row["id"], row["coin"], mclass))
        return False
    coin, direction = row["coin"], (row["direction"] or "long")
    pos = _find_position(client, coin)
    if pos is None:
        log("REFUSE protect %s: no live position (use --resolve-phantom / "
            "reconciler evidence path)" % coin)
        return False
    mark = client.mark_price(coin, 5.0)
    liq = client.position_liquidation(coin)
    target = _protective_px(direction, row["sl_initial"], mark, liq,
                            RESTORE_REANCHOR_PCT, log)
    sl = client.trigger_sl(coin, direction != "long", abs(pos.size_signed), target)
    _register_oid(con, sl.oid, coin, row["id"], "protective")  # BEFORE persist (F5)
    _transition(con, row["id"], ("FILLED",), "PROTECTED",
                {"sl_oid": sl.oid, "sl_px": sl.trigger_px, "via": "drain --protect"},
                extra_sets="sl_order_id=?, sl_placed_px=?, sl_current=?, sl_confirmed_at=?",
                extra_vals=(str(sl.oid), sl.trigger_px, sl.trigger_px, _now_iso()))
    _transition(con, row["id"], ("PROTECTED",), "OPEN",
                {"via": "drain --protect"})
    con.commit()
    log("protected row#%d %s: SL %s @ %.10g (venue-accepted) -> OPEN"
        % (row["id"], coin, sl.oid, sl.trigger_px))
    return True


def _intent_snapshot(con: sqlite3.Connection, tid: int) -> Dict[str, Any]:
    r = con.execute("SELECT detail FROM sm_transitions WHERE trade_id=? AND "
                    "to_state='INTENT' ORDER BY id LIMIT 1", (tid,)).fetchone()
    if r and r["detail"]:
        try:
            return json.loads(r["detail"])
        except ValueError:
            pass
    return {}


def manual_resolve_pending(con: sqlite3.Connection, client: Any, row: sqlite3.Row,
                           env: Dict[str, str],
                           log: Callable[[str], None] = print) -> bool:
    """PENDING row: evidence -> policy re-run vs PERSISTED limit_px (F3) ->
    t_fill+protect ladder or abort ladder."""
    coin, direction, tid = row["coin"], (row["direction"] or "long"), row["id"]
    snap = _intent_snapshot(con, tid)
    min_ratio = float(snap.get("min_fill_ratio",
                               env.get("MIN_FILL_RATIO", DEFAULT_MIN_FILL_RATIO)))
    tick_tol = float(snap.get("cap_tick_tol", CAP_TICK_TOL))
    limit_px = row["limit_px"] if "limit_px" in row.keys() else None

    pos = _find_position(client, coin)
    fill_px = fill_size = None
    fill_oid = None
    if pos is not None:
        fill_px, fill_size = float(pos.entry_px), abs(float(pos.size_signed))
    else:
        cid = row["client_order_id"] if "client_order_id" in row.keys() else None
        for f in client.user_fills(3600.0):
            if cid and str(f.get("cloid", f.get("client_order_id", ""))) == str(cid):
                fill_px, fill_size = float(f["px"]), float(f["sz"])
                fill_oid = str(f.get("oid", ""))
                break
    if fill_px is None:
        # resting remnant? (shouldn't exist on market-at-close venues)
        for o in client.open_orders():
            if str(getattr(o, "coin", "")).upper() == str(coin).upper() and \
                    not getattr(o, "reduce_only", False):
                log("resting unfilled remnant %s on %s -> cancel + abort" % (o.oid, coin))
                client.cancel_sl_order(coin, o.oid)
                break
        # VerifiedAbsent: cache-invalidated re-read + fills lookup empty
        _abort_begin(con, row, "crash_unfilled")
        _abort_final(con, tid, "VerifiedAbsent",
                     {"via": "drain --resolve-pending", "coin": coin})
        log("row#%d %s: no fill evidence -> ABORTED (VerifiedAbsent)" % (tid, coin))
        return True

    # F3: RE-RUN CALLER POLICY vs the row's PERSISTED limit_px — never a
    # recomputed/env-drifted one. Rejected fills are NEVER resurrected.
    reject = None
    if limit_px:
        lp = float(limit_px)
        if direction == "long" and fill_px > lp * (1.0 + tick_tol):
            reject = "cap_breach fill %.10g > limit_px %.10g (+%.1f bps tol)" \
                     % (fill_px, lp, tick_tol * 1e4)
        if direction != "long" and fill_px < lp * (1.0 - tick_tol):
            reject = "cap_breach fill %.10g < limit_px %.10g" % (fill_px, lp)
    req = float(row["size"] or 0)
    if not reject and req > 0 and fill_size / req < min_ratio:
        reject = "min_fill_ratio %.3f < %.3f (partial/residual)" \
                 % (fill_size / req, min_ratio)
    if reject:
        log("row#%d %s policy REJECT: %s -> abort ladder" % (tid, coin, reject))
        _abort_begin(con, row, reject)
        flat = client.ensure_flat(coin)
        _abort_final(con, tid, "FlatResult",
                     {"closed_size": flat.closed_size, "exit_px": flat.exit_avg_px,
                      "via": "drain --resolve-pending"})
        return True

    # policy passed -> t_fill with REAL px/size (fixes D11), then protect ladder
    _transition(con, tid, ("PENDING",), "FILLED",
                {"fill_px": fill_px, "fill_size": fill_size, "oid": fill_oid,
                 "policy": "re-run vs persisted limit_px, passed"},
                extra_sets="entry=?, size=?, fill_confirmed_at=?" +
                           (", fill_oid=?" if "fill_oid" in row.keys() else ""),
                extra_vals=(fill_px, fill_size, _now_iso()) +
                           ((fill_oid,) if "fill_oid" in row.keys() else ()))
    con.commit()
    fresh = con.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
    return manual_protect(con, client, fresh, log)


def manual_resolve_aborting(con: sqlite3.Connection, client: Any, row: sqlite3.Row,
                            log: Callable[[str], None] = print) -> bool:
    """ABORTING row: re-drive per persisted abort_reason, never resurrect."""
    coin, tid = row["coin"], row["id"]
    reason = (row["abort_reason"] if "abort_reason" in row.keys() else None) or "unknown"
    try:
        flat = client.ensure_flat(coin)
    except Exception as e:  # noqa: BLE001 — WriteUnconfirmed branch
        log("CRITICAL row#%d %s: ensure_flat unconfirmed (%s) — placing residual "
            "protective SL, row STAYS ABORTING" % (tid, coin, e))
        pos = None
        try:
            pos = _find_position(client, coin)
        except Exception as e2:  # noqa: BLE001
            log("CRITICAL: residual read also unknown (%s) — hands off, operator" % e2)
            return False
        if pos is not None:
            direction = "long" if pos.size_signed > 0 else "short"
            mark = client.mark_price(coin, 5.0)
            liq = client.position_liquidation(coin)
            px = _protective_px(direction, None, mark, liq, RESIDUAL_SL_PCT, log)
            sl = client.trigger_sl(coin, direction != "long", abs(pos.size_signed), px)
            _register_oid(con, sl.oid, coin, tid, "protective")
            log("residual SL %s @ %.10g placed (+-2.5%% mark, exit-engine §9.5)"
                % (sl.oid, sl.trigger_px))
        return False
    _abort_final(con, tid, "FlatResult",
                 {"abort_reason": reason, "already_flat": flat.already_flat,
                  "closed_size": flat.closed_size, "via": "drain --resolve-aborting"})
    log("row#%d %s: unwind re-driven per persisted abort_reason=%r -> ABORTED"
        % (tid, coin, reason))
    return True


def manual_resolve_phantom(con: sqlite3.Connection, client: Any, row: sqlite3.Row,
                           log: Callable[[str], None] = print) -> bool:
    """REVIEW NIT 2: DB-only phantom row-resolve for protect_only AND
    manual_claimed rows (same phantom-K3 semantics). Never touches the venue."""
    coin, tid = row["coin"], row["id"]
    mclass = (row["management_class"] if "management_class" in row.keys() else None) \
        or "strategy"
    if mclass not in ("protect_only", "manual_claimed"):
        log("REFUSE resolve-phantom row#%d %s: management_class=%r — strategy "
            "rows resolve through the reconciler evidence path (entry-SM §4.1)"
            % (tid, coin, mclass))
        return False
    try:
        pos = _find_position(client, coin)
    except Exception as e:  # noqa: BLE001 — ReadUnknown => cannot prove absent
        log("REFUSE resolve-phantom %s: position read UNKNOWN (%s) — bias-to-protect"
            % (coin, e))
        return False
    if pos is not None:
        log("REFUSE resolve-phantom %s: position still PRESENT" % coin)
        return False
    # attribution: oid-first (registry corpus), then 1% proximity, else phantom
    reg = {str(r["oid"]) for r in con.execute(
        "SELECT oid FROM placed_trigger_oids WHERE coin=?", (coin,))}
    for v in (row["sl_order_id"], row["tp1_order_id"]):
        if v:
            reg.add(str(v))
    reason, exit_px = "phantom_no_exchange_position", None
    try:
        fills = list(client.user_fills(86400.0))
    except Exception:  # noqa: BLE001 — attribution is best-effort, DB-only close stands
        fills = []
    for f in fills:
        if str(f.get("coin", "")).upper() != str(coin).upper():
            continue
        if str(f.get("oid", "")) in reg:
            reason, exit_px = "sl", float(f.get("px", 0) or 0) or None
            break
        for ref in (row["sl_current"] if "sl_current" in row.keys() else None,
                    row["sl_initial"]):
            if ref and abs(float(f.get("px", 0)) - float(ref)) <= float(ref) * PROXIMITY_PCT:
                reason, exit_px = "sl", float(f.get("px", 0))
                break
        if exit_px:
            break
    _transition(con, tid, ("OPEN",), "CLOSED",
                {"via": "drain --resolve-phantom (DB-only, nit-2: protect_only + "
                        "manual_claimed same resolve)", "attribution": reason},
                extra_sets="closed_at=?, exit_price=?, exit_reason=?",
                extra_vals=(_now_iso(), exit_px, reason))
    con.commit()
    log("row#%d %s (%s): position verifiably ABSENT -> DB-only row close, "
        "exit_reason=%s (zero venue actions)" % (tid, coin, mclass, reason))
    return True


# ---------------------------------------------------------------------------
# Socket drain (engine alive) — review nit 1
# ---------------------------------------------------------------------------

def run_socket_drain(venue: str, db_path: str, sock: Optional[str] = None,
                     log: Callable[[str], None] = print) -> int:
    """NIT 1: --drain routes through the engine admin socket. The ENGINE
    drives convergence (entry-SM §4.1, bounded 5 reconciler ticks); this
    client only requests and reports."""
    cls = _load_admin_client_cls()
    client = cls(_resolve_sock(sock, db_path))
    try:
        resp = client.request("drain", timeout=360.0)  # 5 ticks ~5 min + margin
    except Exception as e:  # noqa: BLE001
        log("drain FAILED: admin socket unreachable (%s). Engine dead/crash-"
            "looping -> use the manual drain runbook (--protect / "
            "--resolve-pending / --resolve-aborting / --resolve-phantom)" % e)
        return 2
    log("engine drain report: %s" % json.dumps(resp, sort_keys=True))
    if resp.get("ok") and resp.get("converged"):
        log("drain converged. Next: cutover_check --pre-rollback must be GREEN "
            "before any unit flip (rollout §4 GATE)")
        return 0
    log("drain NOT converged (bounded 5 reconciler ticks) — do NOT proceed; "
        "root-cause per rollout §4")
    return 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _rows(con: sqlite3.Connection, states: Tuple[str, ...],
          coin: Optional[str]) -> List[sqlite3.Row]:
    q = "SELECT * FROM trades WHERE sm_state IN (%s)" % ",".join("?" * len(states))
    args: List[Any] = list(states)
    if coin:
        q += " AND coin=?"
        args.append(coin)
    return con.execute(q + " ORDER BY id", args).fetchall()


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="P3 DRAIN tools (p3_rollout.md §4). --drain = engine-alive "
                    "route via admin socket; manual tools refuse unless engine "
                    "dead + flock free.")
    p.add_argument("--venue", choices=["extended", "pacifica", "nado", "hl"])
    p.add_argument("--db", help="live trades.db path")
    p.add_argument("--sock", help="admin socket path override")
    p.add_argument("--coin", help="restrict manual tools to one coin")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--drain", action="store_true",
                   help="engine ALIVE: request the in-engine drain via the admin socket")
    g.add_argument("--protect", action="store_true",
                   help="engine DEAD: SL-protect FILLED/naked rows (strategy authority only)")
    g.add_argument("--resolve-pending", action="store_true",
                   help="engine DEAD: policy re-run vs persisted limit_px per PENDING row")
    g.add_argument("--resolve-aborting", action="store_true",
                   help="engine DEAD: re-drive ensure_flat per ABORTING row")
    g.add_argument("--resolve-phantom", action="store_true",
                   help="engine DEAD: DB-only row-resolve for protect_only/"
                        "manual_claimed rows whose position is verifiably absent (nit 2)")
    p.add_argument("--env", help=".env path (MIN_FILL_RATIO fallback)")
    p.add_argument("--selftest", action="store_true")
    a = p.parse_args(argv)
    if a.selftest:
        return _selftest()
    if not a.venue or not a.db:
        p.error("--venue and --db are required (unless --selftest)")
    if a.drain:
        return run_socket_drain(a.venue, a.db, a.sock)
    if not (a.protect or a.resolve_pending or a.resolve_aborting or a.resolve_phantom):
        p.error("pick one of --drain / --protect / --resolve-pending / "
                "--resolve-aborting / --resolve-phantom")

    gate = ManualGate(a.venue, a.db, a.sock)
    refusal = gate.acquire()
    if refusal:
        print("REFUSE: %s" % refusal)
        return 3
    env: Dict[str, str] = {}
    if a.env and os.path.exists(a.env):
        with open(a.env) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    try:
        from fleet_core.venues import get_client  # lazy — live host only
        client = get_client(a.venue)
        con = _conn(a.db)
        _ensure_sm_tables(con)
        ok_all = True
        try:
            if a.protect:
                targets = _rows(con, ("FILLED",), a.coin)
            elif a.resolve_pending:
                targets = _rows(con, ("PENDING",), a.coin)
            elif a.resolve_aborting:
                targets = _rows(con, ("ABORTING",), a.coin)
            else:
                targets = _rows(con, ("OPEN",), a.coin)
            if not targets:
                print("no matching rows")
            for row in targets:
                if a.protect:
                    ok_all &= manual_protect(con, client, row)
                elif a.resolve_pending:
                    ok_all &= manual_resolve_pending(con, client, row, env)
                elif a.resolve_aborting:
                    ok_all &= manual_resolve_aborting(con, client, row)
                else:
                    ok_all &= manual_resolve_phantom(con, client, row)
        finally:
            con.close()
        print("NEXT: re-run cutover_check --pre-rollback -> GREEN before starting "
              "the old unit (rollout §4: NEVER start the old unit while RED)")
        return 0 if ok_all else 1
    finally:
        gate.release()


# ---------------------------------------------------------------------------
# Offline selftest — fakes only (no SDKs, no engine, no systemd)
# ---------------------------------------------------------------------------

class _FakePos:
    def __init__(self, coin, size_signed, entry_px):
        self.coin, self.size_signed, self.entry_px = coin, size_signed, entry_px


class _FakeSL:
    def __init__(self, coin, oid, trigger_px):
        self.coin, self.oid, self.trigger_px = coin, oid, trigger_px


class _FakeFlat:
    def __init__(self, already_flat, closed_size, exit_avg_px=None):
        self.already_flat, self.closed_size = already_flat, closed_size
        self.exit_avg_px = exit_avg_px
        self.verified_flat = True


class _FakeClient:
    def __init__(self, positions=None, mark=100.0, liq=None, fills=None,
                 flat_raises=False, pos_raises=False):
        self._pos = dict(positions or {})
        self._mark, self._liq = mark, liq
        self._fills = fills or []
        self._flat_raises, self._pos_raises = flat_raises, pos_raises
        self.sl_placed: List[Tuple[str, float]] = []

    def invalidate_positions_cache(self):
        return None

    def open_positions(self):
        if self._pos_raises:
            raise RuntimeError("ReadUnknown")
        return dict(self._pos)

    def mark_price(self, coin, tol=5.0):
        return self._mark

    def position_liquidation(self, coin):
        return self._liq

    def trigger_sl(self, coin, is_buy, sz, px):
        oid = "slx%d" % (len(self.sl_placed) + 1)
        self.sl_placed.append((oid, px))
        return _FakeSL(coin, oid, px)

    def ensure_flat(self, coin):
        if self._flat_raises:
            raise RuntimeError("WriteUnconfirmed")
        had = coin in self._pos
        self._pos.pop(coin, None)
        return _FakeFlat(not had, 1.0 if had else 0.0, self._mark if had else None)

    def user_fills(self, max_age_sec=60.0):
        return list(self._fills)

    def open_orders(self):
        return []

    def cancel_sl_order(self, coin, oid):
        return None


def _mkdb(path: str) -> sqlite3.Connection:
    con = _conn(path)
    con.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, coin TEXT, tf TEXT, direction TEXT,
        entry REAL, sl_initial REAL, sl_current REAL, size REAL,
        status TEXT, sl_order_id TEXT, tp1_order_id TEXT,
        closed_at TEXT, exit_price REAL, exit_reason TEXT,
        sm_state TEXT, sm_updated_at TEXT, management_class TEXT,
        client_order_id TEXT, limit_px REAL, abort_reason TEXT,
        fill_oid TEXT, fill_confirmed_at TEXT, sl_placed_px REAL,
        sl_confirmed_at TEXT, opened_at TEXT)""")
    _ensure_sm_tables(con)
    con.commit()
    return con


def _ins(con, **kw) -> sqlite3.Row:
    keys = sorted(kw)
    con.execute("INSERT INTO trades (%s) VALUES (%s)"
                % (",".join(keys), ",".join("?" * len(keys))),
                [kw[k] for k in keys])
    con.commit()
    return con.execute("SELECT * FROM trades WHERE id=(SELECT MAX(id) FROM trades)"
                       ).fetchone()


def _selftest() -> int:
    import tempfile
    fails: List[str] = []
    sink = lambda s: None  # noqa: E731

    def check(name, ok):
        print("[%s] %s" % ("PASS" if ok else "FAIL", name))
        if not ok:
            fails.append(name)

    d = tempfile.mkdtemp(prefix="drain_st_")

    # 1. protect: FILLED naked row -> SL placed -> OPEN; registry BEFORE persist
    con = _mkdb(os.path.join(d, "a.db"))
    row = _ins(con, coin="BTC", direction="long", sl_initial=90.0, size=1.0,
               status="pending", sm_state="FILLED", management_class="strategy")
    cl = _FakeClient({"BTC": _FakePos("BTC", 1.0, 100.0)}, mark=100.0, liq=85.0)
    ok = manual_protect(con, cl, row, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    reg = con.execute("SELECT COUNT(*) c FROM placed_trigger_oids").fetchone()["c"]
    check("protect_filled_to_open",
          ok and r2["sm_state"] == "OPEN" and r2["status"] == "open"
          and r2["sl_order_id"] and reg == 1 and abs(r2["sl_placed_px"] - 90.0) < 1e-9)

    # 2. protect: through-px -> re-anchor at CURRENT mark -6% (law), liq clamp
    row = _ins(con, coin="ETH", direction="long", sl_initial=105.0, size=1.0,
               status="pending", sm_state="FILLED", management_class="strategy")
    cl = _FakeClient({"ETH": _FakePos("ETH", 1.0, 100.0)}, mark=100.0, liq=None)
    ok = manual_protect(con, cl, row, sink)
    px = cl.sl_placed[0][1]
    check("protect_through_px_reanchor", ok and abs(px - 94.0) < 1e-9)

    # 3. protect refuses protect_only and manual_claimed
    row = _ins(con, coin="OP", direction="long", sl_initial=1.0, size=1.0,
               status="open", sm_state="OPEN", management_class="protect_only")
    check("protect_refuses_protect_only",
          manual_protect(con, _FakeClient({"OP": _FakePos("OP", 1.0, 2.0)}), row, sink) is False)
    row = _ins(con, coin="xyz_GOLD", direction="long", sl_initial=1.0, size=1.0,
               status="open", sm_state="OPEN", management_class="manual_claimed")
    check("protect_refuses_manual_claimed",
          manual_protect(con, _FakeClient({"xyz_GOLD": _FakePos("xyz_GOLD", 1.0, 2.0)}), row, sink) is False)

    # 4. resolve-pending: policy pass -> FILLED with REAL px -> OPEN
    row = _ins(con, coin="SOL", direction="long", sl_initial=90.0, size=1.0,
               status="pending", sm_state="PENDING", management_class="strategy",
               limit_px=101.0)
    cl = _FakeClient({"SOL": _FakePos("SOL", 1.0, 100.5)}, mark=100.5, liq=None)
    ok = manual_resolve_pending(con, cl, row, {}, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    check("resolve_pending_pass",
          ok and r2["sm_state"] == "OPEN" and abs(r2["entry"] - 100.5) < 1e-9)

    # 5. resolve-pending: cap-breach vs PERSISTED limit_px -> abort + cooldown
    row = _ins(con, coin="XRP", direction="long", sl_initial=0.9, size=100.0,
               status="pending", sm_state="PENDING", management_class="strategy",
               limit_px=1.0)
    cl = _FakeClient({"XRP": _FakePos("XRP", 100.0, 1.05)}, mark=1.05)
    ok = manual_resolve_pending(con, cl, row, {}, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    cd = con.execute("SELECT COUNT(*) c FROM entry_cooldowns WHERE coin='XRP'"
                     ).fetchone()["c"]
    check("resolve_pending_cap_breach_abort",
          ok and r2["sm_state"] == "ABORTED" and r2["status"] == "aborted"
          and r2["abort_reason"] and cd == 1)

    # 6. resolve-pending: no evidence -> VerifiedAbsent ABORTED
    row = _ins(con, coin="ADA", direction="long", sl_initial=0.5, size=10.0,
               status="pending", sm_state="PENDING", management_class="strategy")
    ok = manual_resolve_pending(con, _FakeClient(), row, {}, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    check("resolve_pending_verified_absent", ok and r2["sm_state"] == "ABORTED")

    # 7. resolve-aborting: re-drive -> ABORTED
    row = _ins(con, coin="DOT", direction="long", sl_initial=5.0, size=1.0,
               status="pending", sm_state="ABORTING", abort_reason="cap_breach",
               management_class="strategy")
    ok = manual_resolve_aborting(con, _FakeClient({"DOT": _FakePos("DOT", 1.0, 6.0)}),
                                 row, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    check("resolve_aborting_redrive", ok and r2["sm_state"] == "ABORTED")

    # 8. resolve-aborting: WriteUnconfirmed -> residual SL +-2.5%, STAYS ABORTING
    row = _ins(con, coin="AVAX", direction="long", sl_initial=5.0, size=1.0,
               status="pending", sm_state="ABORTING", abort_reason="partial",
               management_class="strategy")
    cl = _FakeClient({"AVAX": _FakePos("AVAX", 1.0, 10.0)}, mark=10.0, flat_raises=True)
    ok = manual_resolve_aborting(con, cl, row, sink)
    r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
    check("resolve_aborting_unconfirmed_residual_sl",
          ok is False and r2["sm_state"] == "ABORTING"
          and cl.sl_placed and abs(cl.sl_placed[0][1] - 9.75) < 1e-9)

    # 9. resolve-phantom: protect_only AND manual_claimed -> DB-only close (nit 2)
    for mclass in ("protect_only", "manual_claimed"):
        row = _ins(con, coin="LINK", direction="long", sl_initial=5.0, size=1.0,
                   status="open", sm_state="OPEN", management_class=mclass)
        cl = _FakeClient({})  # position absent
        ok = manual_resolve_phantom(con, cl, row, sink)
        r2 = con.execute("SELECT * FROM trades WHERE id=?", (row["id"],)).fetchone()
        check("resolve_phantom_%s" % mclass,
              ok and r2["sm_state"] == "CLOSED" and r2["status"] == "closed"
              and r2["exit_reason"] == "phantom_no_exchange_position"
              and not cl.sl_placed)
    # 10. resolve-phantom refuses strategy rows / present position / ReadUnknown
    row = _ins(con, coin="UNI", direction="long", sl_initial=5.0, size=1.0,
               status="open", sm_state="OPEN", management_class="strategy")
    check("resolve_phantom_refuses_strategy",
          manual_resolve_phantom(con, _FakeClient({}), row, sink) is False)
    row = _ins(con, coin="ARB", direction="long", sl_initial=5.0, size=1.0,
               status="open", sm_state="OPEN", management_class="protect_only")
    check("resolve_phantom_refuses_present",
          manual_resolve_phantom(con, _FakeClient({"ARB": _FakePos("ARB", 1.0, 2.0)}),
                                 row, sink) is False)
    check("resolve_phantom_refuses_readunknown",
          manual_resolve_phantom(con, _FakeClient(pos_raises=True), row, sink) is False)

    # 11. SM guard: protect on a row already OPEN -> SMConflict refused
    row = _ins(con, coin="NEAR", direction="long", sl_initial=1.0, size=1.0,
               status="open", sm_state="OPEN", management_class="strategy")
    try:
        manual_protect(con, _FakeClient({"NEAR": _FakePos("NEAR", 1.0, 2.0)}), row, sink)
        conflict = False
    except SMConflict:
        conflict = True
    check("sm_guard_conflict", conflict)
    con.close()

    # 12. ManualGate: refuses when unit active; refuses when flock held; passes dead+free
    db2 = os.path.join(d, "b.db")
    open(db2, "w").close()
    g = ManualGate("extended", db2, sock=os.path.join(d, "no.sock"),
                   systemctl=lambda u: "active")
    check("gate_refuses_active_unit", g.acquire() is not None)
    g = ManualGate("extended", db2, sock=os.path.join(d, "no.sock"),
                   systemctl=lambda u: "inactive")
    check("gate_passes_dead_engine", g.acquire() is None)
    g2 = ManualGate("extended", db2, sock=os.path.join(d, "no.sock"),
                    systemctl=lambda u: "inactive")
    check("gate_refuses_held_flock", g2.acquire() is not None)
    g.release()
    # 13. gate refuses when a live admin socket is connectable (engine alive)
    import socket as _sk
    import threading as _th
    sockp = os.path.join(d, "live.sock")
    srv = _sk.socket(_sk.AF_UNIX, _sk.SOCK_STREAM)
    srv.bind(sockp)
    srv.listen(1)
    t = _th.Thread(target=lambda: srv.accept(), daemon=True)
    t.start()
    g3 = ManualGate("extended", db2, sock=sockp, systemctl=lambda u: "inactive")
    check("gate_refuses_live_socket", g3.acquire() is not None)
    srv.close()

    print("\nselftest: %d failures" % len(fails))
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
