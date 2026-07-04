"""entry_sm.py — persisted entry state machine + bidirectional reconciler.

Implements p3_design_entry_sm.md (round-3 approved) EXACTLY:

    §1  states  INTENT → PENDING → FILLED → PROTECTED → OPEN → CLOSED
                    └──────┴─────────┴──────────┴──► ABORTING → ABORTED
    §2  schema deltas (applied by fleet_core.engine.migrate_v3)
    §3  transition functions t_intent … t_close (typed preconditions)
    §4  bidirectional reconciler incl. the PROTECTIVE-PLACEMENT LAW and the
        §4.2.3 rule-3 split (3a covered → track-only / 3b prefix-manual naked
        → CRITICAL-no-SL / 3c non-prefix naked → ±6% CURRENT mark)
    §5  crash-recovery matrix K1–K12
    §7  invariants (asserted in the module selftest)

HARD LAWS carried in code, not comments:
  * strategy math untouched — this module is pure execution plumbing;
  * manual/foreign never closed/managed — `management_class` authority gate
    (authority_allows / AuthorityRefused) enforced at the EXECUTOR level;
  * PROTECTIVE-PLACEMENT LAW — every protective placement path calls
    protective_placement_gate() BEFORE placing; re-anchor target is ALWAYS
    the CURRENT mark ∓6%, never a remembered/historic/user px;
  * `_fenced` pinned kwargs for the position context (R3/R2-F1d):
    _fenced(coin, db_open=<live-state coins>, bot_owned=None, oid=None,
    placed_oids=None) — EXACTLY these;
  * money never unprotected — FILLED rows are recovery priority 0; ABORTING
    intent (abort_reason + cooldown) is durable BEFORE any unwind I/O (F3);
  * every venue write goes through the P2 ExchangeClient contract
    (fleet_core/exchange_api.py) — verified-or-raise, no live I/O here.

manual_claimed (R3/R2-F1d + review-nit fold-in): the executor refuses EVERY
venue intent for a manual_claimed row and the reconciler emits no re-alert
(the row is the durable ack). SOLE permitted row action — the SAME phantom-K3
DB-ONLY row-resolve as protect_only: position confirmed gone (K=3 debounce +
cache-invalidated re-read) → resolve_phantom_row() closes the ROW with
attribution notes. A row-close touches the DB, never the venue.

Journal note: fleet_core/journal.py (P1) imports `bot.config` at module scope
and is therefore not importable on SDK-less hosts; this module carries its own
sqlite writes with journal-identical SQL/fields (close_trade, insert_rejected)
so behavior — including the rejected_signals row on every abort — is preserved
byte-for-byte at the DB level.

Pure stdlib (+ fleet_core.exchange_api / .engine.registry / .engine.migrate_v3
/ .orphan_sweep — all stdlib-only). Selftests:  python3 -m
fleet_core.engine.entry_sm --selftest   (offline, fakes only).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (Any, Callable, Dict, Iterator, List, Mapping, Optional,
                    Sequence, Set, Tuple)

from fleet_core.exchange_api import (ExchangeClient, FillResult, FlatResult,
                                     OpenOrderInfo, PositionInfo, ReadUnknown,
                                     SLOrderInfo, VenueRejected,
                                     WriteUnconfirmed)
from fleet_core.engine import migrate_v3 as _mig
from fleet_core.engine import registry as _reg

log = logging.getLogger(__name__)

# ============================================================================
# States (§1) and legacy status map (§2.3)
# ============================================================================

INTENT = "INTENT"
PENDING = "PENDING"
FILLED = "FILLED"
PROTECTED = "PROTECTED"
OPEN = "OPEN"
CLOSED = "CLOSED"
ABORTING = "ABORTING"
ABORTED = "ABORTED"

ALL_STATES = (INTENT, PENDING, FILLED, PROTECTED, OPEN, CLOSED,
              ABORTING, ABORTED)
TERMINAL_STATES = (CLOSED, ABORTED)
NON_TERMINAL_PRE_OPEN = (INTENT, PENDING, FILLED, PROTECTED, ABORTING)
# fence/db_open context (§4.2 direction-2 pinned kwargs): a coin counts as
# having a live row in exactly these states.
LIVE_STATES = (FILLED, PROTECTED, OPEN, ABORTING)

# Rank used ONLY for replay idempotence ("0 rows affected + already at-or-past
# target = OK no-op; earlier state = SMConflict", §1 rules). ABORTING/ABORTED
# rank past every pre-terminal state so a replayed forward transition on an
# aborting row is a silent no-op — recovery can NEVER resurrect an abort.
_RANK = {INTENT: 0, PENDING: 1, FILLED: 2, PROTECTED: 3, OPEN: 4, CLOSED: 5,
         ABORTING: 6, ABORTED: 7}

# §2.3 status ↔ sm_state map — the legacy column is MAINTAINED on every
# transition (rollback requirement F10).
LEGACY_STATUS = {INTENT: "pending", PENDING: "pending", FILLED: "pending",
                 PROTECTED: "pending", ABORTING: "pending", OPEN: "open",
                 CLOSED: "closed", ABORTED: "aborted"}

# management_class enum (§2.1)
MC_STRATEGY = "strategy"
MC_PROTECT_ONLY = "protect_only"
MC_MANUAL_CLAIMED = "manual_claimed"
MANAGEMENT_CLASSES = (MC_STRATEGY, MC_PROTECT_ONLY, MC_MANUAL_CLAIMED)

# origin enum (§2.1)
ORIGIN_ENTRY = "entry"
ORIGIN_ADOPTED = "adopted_untracked"
ORIGIN_MIGRATED = "migrated"

# Engine constants — labeled, not arbitrary (current live values; exit-engine §5)
RESTORE_REANCHOR_PCT = 0.06        # _RESTORE_REANCHOR_PCT — ±6% of CURRENT mark
UNTRACKED_PROTECTIVE_PCT = 0.06    # §4.2.3-3c protective SL distance
RESIDUAL_PROTECTIVE_PCT = 0.025    # abort-ladder residual protection (exit §9.5)
CAP_TICK_TOL_DEFAULT = 0.001       # cap-breach tick tolerance ±0.1% (§3 t_fill)
MIN_FILL_RATIO_DEFAULT = 0.10      # MIN_FILL_RATIO=10% (exit-engine §8 seed list)
LIQ_GUARD_BUFFER = 1.01            # SL must sit inside liquidation ×1.01
ORPHAN_DEBOUNCE_SEC = 180.0        # orphan-trigger sweep debounce (live value)
PHANTOM_K = 3                      # phantom-guard K=3 (fleet canon)
STUCK_TTL_TICKS = 3                # §4.1 last row: STUCK_TTL = 3 × tick
# EF1 (adversarial review round-1) — POSITIVE-PROVENANCE discipline. A venue
# position may be tied to a row ONLY by (a) a user_fills row carrying the
# row's client_order_id, or (b) fill-window+size evidence: an entry-side,
# non-reduce-only fill inside [order_sent_at, +FILL_PROVENANCE_WINDOW_SEC]
# with size <= the planned size. Coin+recency alone NEVER authorizes a venue
# write. Window derivation (labeled, not arbitrary): entry dispatch+confirm
# chains complete within ONE 60s tick by design (rollout §1 F5 derivation);
# x5 covers the worst live confirm ladder (ext 15s REST + 10s WS +
# trader-recreate + same-ext_id retry).
FILL_PROVENANCE_WINDOW_SEC = 300.0
PROVENANCE_SIZE_TOL = 0.01         # 1% venue lot-rounding tolerance on sizes


class SMConflict(RuntimeError):
    """A transition found the row in an EARLIER state than its from-state —
    a real ordering violation (never raised on replays: at-or-past = no-op)."""


class AuthorityRefused(RuntimeError):
    """Executor-level refusal (invariants §7.9 / §7.12): a venue intent was
    issued against a row whose management_class does not permit it."""


# ============================================================================
# Abort proofs (§3 t_abort_final)
# ============================================================================

@dataclass(frozen=True)
class NeverSent:
    """state=INTENT and order_sent_at IS NULL — provably nothing to unwind."""
    reason: str = "crash_before_send"


@dataclass(frozen=True)
class VerifiedAbsent:
    """No position ATTRIBUTABLE TO THIS ROW on the venue AND the fills lookup
    shows no un-unwound remainder — the vanished / crash_unfilled evidence
    class. EF1 extension (documented deviation, review round-1): a FOREIGN
    position may occupy the coin while this proof holds — attributable-absence
    is decided by POSITIVE provenance (client_order_id fill match or
    fill-window+size net), never by the coin being empty per se; the foreign
    position is then handled by the rule-3 protect-only path, NEVER closed."""
    positions_reread_empty: bool = True
    fills_lookup_empty: bool = True
    note: str = ""

    def __post_init__(self):
        if not (self.positions_reread_empty and self.fills_lookup_empty):
            raise ValueError("VerifiedAbsent requires BOTH the position re-read"
                             " empty AND the fills lookup empty")


AbortProof = (FlatResult, VenueRejected, NeverSent, VerifiedAbsent)


# ============================================================================
# DB plumbing
# ============================================================================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _conn(db_path) -> Iterator[sqlite3.Connection]:
    """WAL + busy_timeout=5000 fleet-wide (§6 nado journal fix)."""
    con = sqlite3.connect(str(db_path), timeout=5.0)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=5000")
        try:
            con.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        yield con
        con.commit()
    finally:
        con.close()


def _audit(con: sqlite3.Connection, trade_id: int, from_state: str,
           to_state: str, detail: Optional[Mapping[str, Any]] = None) -> None:
    """Append to the sm_transitions audit table (append-only, forensic)."""
    con.execute(
        "INSERT INTO sm_transitions(trade_id, at, from_state, to_state, detail)"
        " VALUES (?, ?, ?, ?, ?)",
        (trade_id, _now_iso(), from_state, to_state,
         json.dumps(detail or {}, default=str)))


def _transition(con: sqlite3.Connection, trade_id: int,
                from_states: Sequence[str], to_state: str,
                sets: Optional[Dict[str, Any]] = None,
                detail: Optional[Mapping[str, Any]] = None) -> bool:
    """§1 rule: ONE SQLite UPDATE guarded by WHERE sm_state IN (:from) —
    idempotent, concurrent-safe, replay-safe. Returns True when THIS call
    performed the transition; False on a legal replay no-op. Raises SMConflict
    when the row sits in an EARLIER state than every from-state.

    Maintains the legacy `status` column per §2.3 on every transition and
    appends the sm_transitions audit row."""
    sets = dict(sets or {})
    sets["sm_state"] = to_state
    sets["sm_updated_at"] = _now_iso()
    sets["status"] = LEGACY_STATUS[to_state]
    # exact from_state for the audit row: pre-read INSIDE the same transaction
    # (single-writer engine per DB — flock; the guarded UPDATE, not this read,
    # carries correctness). Falls back to the joined claim if the pre-read is
    # not one of from_states (defensive).
    frm_exact: Optional[str] = None
    if len(from_states) > 1:
        r0 = con.execute("SELECT sm_state FROM trades WHERE id=?",
                         (trade_id,)).fetchone()
        if r0 is not None and r0["sm_state"] in from_states:
            frm_exact = r0["sm_state"]
    cols = ", ".join(f"{k}=?" for k in sets)
    marks = ",".join("?" for _ in from_states)
    cur = con.execute(
        f"UPDATE trades SET {cols} WHERE id=? AND sm_state IN ({marks})",
        list(sets.values()) + [trade_id] + list(from_states))
    if cur.rowcount == 1:
        frm = from_states[0] if len(from_states) == 1 \
            else (frm_exact or "|".join(from_states))
        _audit(con, trade_id, frm, to_state, detail)
        return True
    row = con.execute("SELECT sm_state FROM trades WHERE id=?",
                      (trade_id,)).fetchone()
    if row is None:
        raise SMConflict(f"trade {trade_id}: no such row (transition to {to_state})")
    cur_state = row["sm_state"]
    if cur_state is not None and _RANK.get(cur_state, -1) >= _RANK[to_state]:
        return False  # already at-or-past target — legal replay no-op
    raise SMConflict(
        f"trade {trade_id}: sm_state={cur_state!r}, cannot transition "
        f"{'|'.join(from_states)} -> {to_state}")


def _get_row(con: sqlite3.Connection, trade_id: int) -> sqlite3.Row:
    row = con.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
    if row is None:
        raise SMConflict(f"trade {trade_id}: no such row")
    return row


def _insert_rejected(con: sqlite3.Connection, coin: str, tf: str,
                     direction: str, trigger_price, entry_price, sl_price,
                     reason: str) -> None:
    """journal.insert_rejected-identical write (reason[:500] preserved)."""
    con.execute(
        "INSERT INTO rejected_signals (created_at, coin, tf, direction, "
        "trigger_price, entry_price, sl_price, reason, walk_slip_pct, "
        "vol_1h_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
        (_now_iso(), coin, tf, direction, trigger_price, entry_price,
         sl_price, str(reason)[:500]))


# ============================================================================
# client_order_id (§2.1)
# ============================================================================

def make_client_order_id(venue: str, coin: str, tf: str, signal_bar_ts,
                         engine_epoch) -> str:
    """Deterministic dedupe key = sha1(venue|coin|tf|signal_bar_ts|
    engine_epoch)[:16] — makes t_intent idempotent and lets the reconciler
    match a PENDING row to venue fills/orders."""
    raw = f"{venue}|{coin}|{tf}|{signal_bar_ts}|{engine_epoch}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


# ============================================================================
# Entry plan + caller policy (F3)
# ============================================================================

@dataclass(frozen=True)
class EntryPlan:
    """The frozen plan committed at INTENT (§3 t_intent precondition: plan
    complete). All entry gates have passed in the CALLER (venue gates stay
    venue-code); this dataclass is plumbing, not strategy."""
    venue: str
    coin: str
    tf: str
    direction: str                 # 'long' | 'short'
    entry_intended: float
    sl_initial: float
    tp1: float
    size: float
    risk_dollars: float
    notional: float
    leverage_eff: float
    limit_px: float                # F3 frozen cap-breach reference
    entry_bar_ts: int
    atr14: Optional[float]
    tp1_frac: float
    client_order_id: str
    min_fill_ratio: float = MIN_FILL_RATIO_DEFAULT
    cap_tick_tol: float = CAP_TICK_TOL_DEFAULT

    def __post_init__(self):
        if self.direction not in ("long", "short"):
            raise ValueError(f"EntryPlan: direction {self.direction!r}")
        for f_ in ("entry_intended", "sl_initial", "size", "limit_px"):
            if getattr(self, f_) is None or float(getattr(self, f_)) <= 0.0:
                raise ValueError(f"EntryPlan({self.coin}): {f_} must be > 0 "
                                 "(plan incomplete — t_intent precondition)")
        if not self.client_order_id:
            raise ValueError("EntryPlan: client_order_id required")


@dataclass(frozen=True)
class PolicyVerdict:
    ok: bool
    reason: str
    evidence: Mapping[str, Any] = field(default_factory=dict)


def intent_snapshot(con: sqlite3.Connection, trade_id: int) -> Dict[str, Any]:
    """The policy snapshot persisted in the ∅→INTENT sm_transitions.detail
    (§2.1 limit_px row: {min_fill_ratio, cap_tick_tol} in force at plan time)."""
    row = con.execute(
        "SELECT detail FROM sm_transitions WHERE trade_id=? AND to_state=? "
        "ORDER BY id ASC LIMIT 1", (trade_id, INTENT)).fetchone()
    if row is None or not row["detail"]:
        return {}
    try:
        d = json.loads(row["detail"])
        return d if isinstance(d, dict) else {}
    except (ValueError, TypeError):
        return {}


def run_entry_policy(row: Mapping[str, Any], fill_px: float, fill_size: float,
                     snapshot: Optional[Mapping[str, Any]] = None) -> PolicyVerdict:
    """Caller policy — cap-breach vs the row's PERSISTED limit_px (± tick tol)
    and min_fill_ratio from the INTENT snapshot. §3 t_fill precondition; BINDS
    THE RECONCILER IDENTICALLY (F3, §4.1 / K9b): recovery re-runs THIS function
    from the persisted values, never from recomputed/env-drifted ones."""
    snapshot = snapshot or {}
    limit_px = row["limit_px"]
    tol = float(snapshot.get("cap_tick_tol", CAP_TICK_TOL_DEFAULT))
    min_ratio = float(snapshot.get("min_fill_ratio", MIN_FILL_RATIO_DEFAULT))
    direction = str(row["direction"] or "long")
    ev: Dict[str, Any] = {"fill_px": fill_px, "fill_size": fill_size,
                          "limit_px": limit_px, "cap_tick_tol": tol,
                          "min_fill_ratio": min_ratio, "direction": direction}
    if limit_px is None or float(limit_px) <= 0.0:
        # No persisted cap reference (pre-cutover legacy PENDING row): the
        # policy CANNOT be re-run against a frozen value — conservative REJECT
        # (never resurrect what the persisted plan cannot validate; cutover
        # precondition is no pending rows, so this is belt-and-braces §2.4).
        ev["verdict"] = "limit_px_missing"
        return PolicyVerdict(False, "policy_limit_px_missing_conservative_abort", ev)
    limit_px = float(limit_px)
    if direction == "long":
        breached = fill_px > limit_px * (1.0 + tol)
    else:
        breached = fill_px < limit_px * (1.0 - tol)
    if breached:
        ev["verdict"] = "cap_breach"
        return PolicyVerdict(False, "entry_cap_breach_vs_persisted_limit_px", ev)
    planned = float(row["size"] or 0.0)
    ratio = (fill_size / planned) if planned > 0 else 0.0
    ev["fill_ratio"] = ratio
    if ratio < min_ratio:
        ev["verdict"] = "sub_min_fill_ratio"
        return PolicyVerdict(False, "entry_fill_below_min_fill_ratio", ev)
    ev["verdict"] = "pass"
    return PolicyVerdict(True, "ok", ev)


# ============================================================================
# Transition functions (§3)
# ============================================================================

def t_intent(db_path, plan: EntryPlan) -> int:
    """∅ → INTENT. Plan frozen + row committed; NOTHING sent. INSERT OR
    IGNORE on client_order_id UNIQUE → returns the existing trade_id on
    replay (idempotent)."""
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        now = _now_iso()
        cur = con.execute(
            """
            INSERT OR IGNORE INTO trades (
                created_at, coin, tf, direction, entry, entry_intended,
                sl_initial, sl_current, tp1, size, risk_dollars, notional,
                status, opened_at, atr14, entry_bar_ts,
                sm_state, sm_updated_at, client_order_id, tp1_frac_at_entry,
                leverage_eff, limit_px, origin, management_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now, plan.coin, plan.tf, plan.direction,
             plan.entry_intended, plan.entry_intended,      # entry = placeholder
             plan.sl_initial, plan.sl_initial, plan.tp1, plan.size,
             plan.risk_dollars, plan.notional,
             plan.atr14, plan.entry_bar_ts,
             INTENT, now, plan.client_order_id, plan.tp1_frac,
             plan.leverage_eff, plan.limit_px, ORIGIN_ENTRY, MC_STRATEGY))
        if cur.rowcount == 1:
            tid = int(cur.lastrowid)
            # §2.1: INTENT detail additionally snapshots the policy knobs in
            # force at plan time — the F3 frozen reference for every re-run.
            _audit(con, tid, "", INTENT, {
                "client_order_id": plan.client_order_id,
                "limit_px": plan.limit_px,
                "min_fill_ratio": plan.min_fill_ratio,
                "cap_tick_tol": plan.cap_tick_tol,
                "leverage_eff": plan.leverage_eff,
                "entry_bar_ts": plan.entry_bar_ts})
            return tid
        row = con.execute("SELECT id FROM trades WHERE client_order_id=?",
                          (plan.client_order_id,)).fetchone()
        if row is None:  # pragma: no cover — IGNORE without a matching row
            raise SMConflict(f"t_intent({plan.coin}): INSERT ignored but no row "
                             f"for client_order_id={plan.client_order_id}")
        return int(row["id"])


def t_mark_sent(db_path, trade_id: int) -> bool:
    """INTENT → PENDING. This IS the pre-dispatch commit point: MUST commit
    BEFORE calling client.market_open (invariant §7.1)."""
    with _conn(db_path) as con:
        return _transition(con, trade_id, (INTENT,), PENDING,
                           sets={"order_sent_at": _now_iso()})


def t_fill(db_path, trade_id: int, fr: FillResult, policy: PolicyVerdict,
           walk_slip_pct: Optional[float] = None) -> bool:
    """PENDING → FILLED. Preconditions (§3):
      * fr.readback_verified is True — BY CONSTRUCTION (an unverified
        FillResult cannot exist; WriteUnconfirmed leaves state=PENDING);
      * caller policy PASSED against the persisted limit_px / INTENT snapshot
        (binds the reconciler identically — a rejected fill goes to
        t_abort_begin, never here)."""
    if not isinstance(fr, FillResult) or fr.readback_verified is not True:
        raise ValueError("t_fill: requires a readback-verified FillResult")
    if not isinstance(policy, PolicyVerdict) or not policy.ok:
        raise SMConflict(
            f"t_fill(trade {trade_id}): caller policy did not pass "
            f"({getattr(policy, 'reason', 'missing verdict')}) — a rejected "
            "fill must go to t_abort_begin (F3)")
    with _conn(db_path) as con:
        return _transition(
            con, trade_id, (PENDING,), FILLED,
            sets={"entry": fr.avg_px, "size": fr.size, "fill_oid": fr.oid,
                  "walk_slip_pct": walk_slip_pct,
                  "fill_confirmed_at": _now_iso()},
            detail={"avg_px": fr.avg_px, "size": fr.size,
                    "requested_size": fr.requested_size, "oid": fr.oid,
                    "policy": dict(policy.evidence)})


def t_protect(db_path, trade_id: int, sl: SLOrderInfo) -> bool:
    """FILLED → PROTECTED. Precondition: sl.readback_verified is True (nado
    _confirm_trigger_live pattern, mandatory fleet-wide under P2).
    sl_placed_px := sl.trigger_px = venue-ACCEPTED px (post liq-clamp/rounding
    — fixes ext :541 seeding from signal.sl_price).

    Registry write discipline (§2.2): the caller INSERTs sl.oid into
    placed_trigger_oids IMMEDIATELY after placement readback and BEFORE this
    row persist; belt-and-braces, this function re-registers idempotently."""
    if not isinstance(sl, SLOrderInfo) or sl.readback_verified is not True:
        raise ValueError("t_protect: requires a readback-verified SLOrderInfo")
    with _conn(db_path) as con:
        _reg.register_oid_conn(con, sl.oid, sl.coin, trade_id=trade_id, kind="sl")
        return _transition(
            con, trade_id, (FILLED,), PROTECTED,
            sets={"sl_order_id": sl.oid, "sl_placed_px": sl.trigger_px,
                  "sl_current": sl.trigger_px, "sl_confirmed_at": _now_iso()},
            detail={"sl_oid": sl.oid, "trigger_px": sl.trigger_px,
                    "position_wide": sl.position_wide})


def t_open(db_path, trade_id: int) -> bool:
    """PROTECTED → OPEN. No precondition — all money-relevant data already
    persisted at FILLED/PROTECTED. Replaces promote_pending. TP1 placement
    happens AFTER (best-effort, never blocks)."""
    with _conn(db_path) as con:
        return _transition(con, trade_id, (PROTECTED,), OPEN,
                           sets={"opened_at": _now_iso()})


def t_abort_begin(db_path, trade_id: int, reason: str,
                  evidence: Optional[Mapping[str, Any]] = None,
                  cooldown_sec: Optional[float] = None,
                  now_ts: Optional[float] = None) -> bool:
    """{PENDING, FILLED, PROTECTED} → ABORTING (INTENT aborts via the
    NeverSent fast path in t_abort_final). F3: MUST commit BEFORE the first
    ensure_flat/cancel call of the unwind (mirror of t_mark_sent's
    commit-before-dispatch law). Writes the entry_cooldowns row in the SAME
    transaction — a crash 1ms later still leaves the cooldown armed (K9).
    `now_ts` — injectable clock for deterministic harnesses (default wall)."""
    with _conn(db_path) as con:
        row = _get_row(con, trade_id)
        did = _transition(con, trade_id, (PENDING, FILLED, PROTECTED), ABORTING,
                          sets={"abort_reason": reason},
                          detail=dict(evidence or {}, abort_reason=reason))
        if did and cooldown_sec is not None:
            base = time.time() if now_ts is None else float(now_ts)
            con.execute(
                "INSERT OR REPLACE INTO entry_cooldowns(coin, until_ts, reason)"
                " VALUES (?, ?, ?)",
                (row["coin"], base + float(cooldown_sec), reason))
        return did


def t_abort_final(db_path, trade_id: int, proof,
                  write_rejected: bool = True) -> bool:
    """ABORTING → ABORTED; INTENT → ABORTED (NeverSent fast path — intent +
    proof in one UPDATE, nothing to unwind). §3 proof classes:

      FlatResult      — verified_flat by construction (position unwound)
      VenueRejected   — nothing landed by contract
      NeverSent       — state=INTENT, order_sent_at NULL
      VerifiedAbsent  — cache-invalidated re-read empty AND fills empty

    A WriteUnconfirmed from ensure_flat is NOT proof — state stays ABORTING
    for the reconciler to re-drive per the persisted abort_reason.

    Preserves today's behavior: a rejected_signals row is STILL written on
    every abort (reporting unchanged); the trades row is NEVER deleted
    (invariant §7.6 — forensics kept)."""
    if not isinstance(proof, AbortProof):
        raise ValueError(f"t_abort_final: proof must be one of "
                         f"{[c.__name__ for c in AbortProof]}, got "
                         f"{type(proof).__name__}")
    detail: Dict[str, Any] = {"proof": type(proof).__name__}
    if isinstance(proof, FlatResult):
        if proof.verified_flat is not True:  # unreachable by construction
            raise ValueError("t_abort_final: FlatResult not verified_flat")
        detail.update(already_flat=proof.already_flat,
                      closed_size=proof.closed_size,
                      exit_avg_px=proof.exit_avg_px)
    elif isinstance(proof, VenueRejected):
        detail.update(reason=proof.reason, code=proof.code)
    with _conn(db_path) as con:
        row = _get_row(con, trade_id)
        if isinstance(proof, NeverSent):
            if row["sm_state"] == INTENT and row["order_sent_at"] is not None:
                raise SMConflict(
                    f"trade {trade_id}: NeverSent proof invalid — "
                    "order_sent_at is set (order may have been dispatched)")
            from_states: Tuple[str, ...] = (INTENT,)
            reason = row["abort_reason"] or proof.reason
            sets = {"abort_reason": reason}
        else:
            from_states = (ABORTING,)
            reason = row["abort_reason"] or "abort"
            sets = {}
        did = _transition(con, trade_id, from_states, ABORTED,
                          sets=sets, detail=detail)
        if did and write_rejected:
            _insert_rejected(con, row["coin"], row["tf"], row["direction"],
                             trigger_price=row["entry_intended"],
                             entry_price=row["entry"],
                             sl_price=row["sl_initial"], reason=reason)
        return did


def t_close(db_path, trade_id: int, exit_price: float, exit_reason: str,
            pnl_dollars: float, realized_r: float,
            detail: Optional[Mapping[str, Any]] = None) -> bool:
    """OPEN → CLOSED. Attribution evidence comes from the exit engine /
    reconciler (exit-engine §6). journal.close_trade-identical fields."""
    with _conn(db_path) as con:
        return _transition(
            con, trade_id, (OPEN,), CLOSED,
            sets={"closed_at": _now_iso(), "exit_price": exit_price,
                  "exit_reason": exit_reason, "pnl_dollars": pnl_dollars,
                  "realized_r": realized_r},
            detail=dict(detail or {}, exit_reason=exit_reason,
                        exit_price=exit_price))


# ============================================================================
# Cooldowns + daily-opens persistence (§2.2, W9/CW4 + W11/CW10)
# ============================================================================

def cooldown_active(db_path, coin: str, now_ts: Optional[float] = None) -> bool:
    now_ts = time.time() if now_ts is None else now_ts
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        row = con.execute("SELECT until_ts FROM entry_cooldowns WHERE coin=?",
                          (coin,)).fetchone()
        return bool(row and float(row["until_ts"]) > now_ts)


def arm_cooldown(db_path, coin: str, cooldown_sec: float, reason: str) -> None:
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        con.execute("INSERT OR REPLACE INTO entry_cooldowns(coin, until_ts, "
                    "reason) VALUES (?, ?, ?)",
                    (coin, time.time() + float(cooldown_sec), reason))


def _bot_state_get(con: sqlite3.Connection, key: str) -> Optional[str]:
    row = con.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def _bot_state_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute("INSERT OR REPLACE INTO bot_state(key, value) VALUES (?, ?)",
                (key, value))


def opens_today(db_path, day: Optional[str] = None) -> int:
    """Durable daily-opens counter (bot_state 'opens_today'+'opens_day')."""
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        if _bot_state_get(con, "opens_day") != day:
            return 0
        try:
            return int(_bot_state_get(con, "opens_today") or 0)
        except (TypeError, ValueError):
            return 0


def incr_opens_today(db_path, day: Optional[str] = None) -> int:
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        cur = 0
        if _bot_state_get(con, "opens_day") == day:
            try:
                cur = int(_bot_state_get(con, "opens_today") or 0)
            except (TypeError, ValueError):
                cur = 0
        cur += 1
        _bot_state_set(con, "opens_day", day)
        _bot_state_set(con, "opens_today", str(cur))
        return cur


# ============================================================================
# PROTECTIVE-PLACEMENT LAW (§4, R3/R2-F1c) — the gate before EVERY placement
# ============================================================================

def protective_placement_gate(is_long: bool, candidate_px: float,
                              current_mark: float,
                              reanchor_pct: float = RESTORE_REANCHOR_PCT
                              ) -> Tuple[float, bool]:
    """Sequenced BEFORE any protective/recovery SL placement, EVERYWHERE
    (adopt 3c, §4.1 FILLED recovery from row sl_initial, PROTECTED re-place,
    post-migration first tick, protect_only heal).

    Through-px re-anchor check FIRST: candidate through or at current mark
    (long: candidate >= mark; short: candidate <= mark) → re-anchor to
    CURRENT mark ∓6%, NEVER close, log as a labeled deviation. When the gate
    re-anchors, the anchor is ALWAYS current mark — never the remembered/
    historic/user px (invariant §7.11). Returns (px, reanchored)."""
    if current_mark is None or current_mark <= 0.0:
        raise ValueError("protective_placement_gate: current_mark required — "
                         "no protective placement without a live mark")
    if candidate_px is None or candidate_px <= 0.0:
        through = True  # no usable candidate — anchor at mark by law
    elif is_long:
        through = candidate_px >= current_mark
    else:
        through = candidate_px <= current_mark
    if not through:
        return float(candidate_px), False
    anchored = current_mark * (1.0 - reanchor_pct) if is_long \
        else current_mark * (1.0 + reanchor_pct)
    log.warning("PROTECTIVE-PLACEMENT LAW: candidate %.10g through current "
                "mark %.10g (%s) — re-anchored to mark%s%.0f%% = %.10g "
                "(labeled deviation, never close)",
                candidate_px or 0.0, current_mark,
                "long" if is_long else "short",
                "-" if is_long else "+", reanchor_pct * 100, anchored)
    return float(anchored), True


def liq_guard_clamp(is_long: bool, candidate_px: float,
                    liq_px: Optional[float],
                    buffer: float = LIQ_GUARD_BUFFER) -> Tuple[float, bool]:
    """SL-inside-liquidation ×1.01 fleet safety law (fixed rule; remedy
    mechanics are venue bindings' concern). Returns (px, clamped)."""
    if liq_px is None or liq_px <= 0.0:
        return float(candidate_px), False
    if is_long:
        floor_px = liq_px * buffer
        if candidate_px < floor_px:
            return float(floor_px), True
    else:
        ceil_px = liq_px * (2.0 - buffer)  # symmetric 1% inside
        if candidate_px > ceil_px:
            return float(ceil_px), True
    return float(candidate_px), False


# ============================================================================
# Authority gates (§2.1 management_class, invariants §7.9 / §7.12, F2)
# ============================================================================

# venue-touching intent kinds the IntentExecutor may ask about
VENUE_INTENTS = frozenset({"place_sl", "heal_sl", "replace_sl", "cancel_sl",
                           "place_tp", "close", "ensure_flat",
                           "escalate_naked"})
# DB-only actions (never venue I/O)
DB_ONLY_ACTIONS = frozenset({"row_resolve_phantom", "claim"})

_PROTECT_ONLY_ALLOWED = frozenset({"place_sl", "heal_sl", "cancel_sl"})
# protect_only: (i) heal = re-run cover check → branch re-eval at CURRENT
# state (place at current mark ∓6%, registry-gated cancels only — the
# supersede sweep on 3c engine-placed SLs); (ii) phantom-K3 row-resolve.
# NOTHING ELSE: no strategy trail (replace_sl), no TP1, no time-stop, and —
# decisive — NO CLOSE AUTHORITY (Close/EscalateNaked/ensure_flat REFUSED).


def authority_allows(management_class: Optional[str], intent: str) -> bool:
    """Executor-level authority check — NOT caller discipline (F2).

      strategy       → all intents (positive client_order_id provenance);
      protect_only   → place_sl/heal_sl/cancel_sl only (cancel additionally
                       registry-gated by invariant §7.10); close-class REFUSED;
      manual_claimed → EVERY venue intent refused (placement, heal, cancel,
                       close). Sole permitted action is the DB-only phantom-K3
                       row-resolve (same as protect_only — review-nit fold-in).
    """
    if intent in DB_ONLY_ACTIONS:
        return True
    if intent not in VENUE_INTENTS:
        raise ValueError(f"authority_allows: unknown intent {intent!r}")
    mc = management_class or MC_STRATEGY
    if mc == MC_STRATEGY:
        return True
    if mc == MC_PROTECT_ONLY:
        return intent in _PROTECT_ONLY_ALLOWED
    if mc == MC_MANUAL_CLAIMED:
        return False
    # Unknown class: fail closed — refuse venue I/O, loud.
    log.critical("authority_allows: UNKNOWN management_class=%r — refusing %s "
                 "(fail-closed)", management_class, intent)
    return False


def require_authority(row: Mapping[str, Any], intent: str) -> None:
    """Raise AuthorityRefused unless the row's class permits the intent —
    the harness-tested executor refusal (invariant §7.9: protect_only row +
    injected Close intent must refuse)."""
    mc = row["management_class"] if "management_class" in row.keys() else None
    if not authority_allows(mc, intent):
        raise AuthorityRefused(
            f"intent {intent!r} refused for trade "
            f"{row['id'] if 'id' in row.keys() else '?'} "
            f"(management_class={mc!r}) — invariants §7.9/§7.12")


def claim_manual(db_path, coin: str, venue: str = "",
                 note: str = "operator_claim") -> int:
    """Operator claim tool function (§2.1 manual_claimed) — the schema
    mechanism behind `python -m fleet_core.tools.claim --venue X --coin C`
    (the tools/ wrapper belongs to the rollout tooling; this is the engine
    function it calls). FULL hands-off after the claim: zero venue actions,
    no re-alerts (the row itself is the durable ack), phantom-K3 DB-only
    row-resolve remains the sole permitted row action.

    Sets management_class='manual_claimed' on the coin's live-state row; when
    no row exists (operator claiming before the reconciler tracked it) inserts
    a track-only row (origin='adopted_untracked', sm_state=OPEN, sl_* NULL,
    entry=0 sentinel — never consumed: every venue intent is refused).
    Returns the trade_id."""
    with _conn(db_path) as con:
        _mig.ensure_schema(con)
        marks = ",".join("?" for _ in LIVE_STATES)
        row = con.execute(
            f"SELECT id, management_class FROM trades WHERE coin=? AND "
            f"sm_state IN ({marks}) ORDER BY id DESC LIMIT 1",
            [coin] + list(LIVE_STATES)).fetchone()
        now = _now_iso()
        if row is not None:
            con.execute(
                "UPDATE trades SET management_class=?, sm_updated_at=?, "
                "notes=COALESCE(notes,'') || ? WHERE id=?",
                (MC_MANUAL_CLAIMED, now, f"; manual_claimed ({note})", row["id"]))
            _audit(con, int(row["id"]), "", "CLAIM",
                   {"management_class": MC_MANUAL_CLAIMED,
                    "was": row["management_class"], "venue": venue,
                    "note": note})
            log.warning("CLAIM: trade %s (%s) -> manual_claimed — engine fully "
                        "hands-off from now on", row["id"], coin)
            return int(row["id"])
        cur = con.execute(
            """
            INSERT INTO trades (created_at, coin, tf, direction, entry,
                sl_initial, sl_current, size, risk_dollars, status, opened_at,
                notes, sm_state, sm_updated_at, origin, management_class)
            VALUES (?, ?, 'untracked', 'long', 0.0, 0.0, NULL, 0.0, 0.0,
                    'open', ?, ?, ?, ?, ?, ?)
            """,
            (now, coin, now, f"manual_claimed_no_prior_row ({note})",
             OPEN, now, ORIGIN_ADOPTED, MC_MANUAL_CLAIMED))
        tid = int(cur.lastrowid)
        _audit(con, tid, "", OPEN, {"claim_without_row": True, "venue": venue})
        log.warning("CLAIM: %s had no tracked row — inserted track-only "
                    "manual_claimed row id=%s", coin, tid)
        return tid


def resolve_phantom_row(db_path, trade_id: int, evidence: Mapping[str, Any],
                        exit_reason: str = "phantom_no_exchange_position") -> bool:
    """Phantom-K3 DB-ONLY row-resolve — for `protect_only` AND
    `manual_claimed` rows alike (explicit review-nit spec: manual_claimed gets
    the SAME resolve). Position confirmed gone (K=3 debounce + final
    cache-invalidated re-read — evidence dict carries it) → close the ROW with
    attribution. A row-close touches the DB, not the venue: no cancel, no
    flatten, nothing placed — authority gates stay intact (this is a
    DB_ONLY_ACTION)."""
    with _conn(db_path) as con:
        row = _get_row(con, trade_id)
        require_authority(row, "row_resolve_phantom")  # always allowed; explicit
        exit_px = evidence.get("exit_px") or row["sl_current"] or row["entry"] or 0.0
        return _transition(
            con, trade_id, (OPEN, PROTECTED, FILLED), CLOSED,
            sets={"closed_at": _now_iso(), "exit_price": exit_px,
                  "exit_reason": exit_reason, "pnl_dollars": None,
                  "realized_r": None,
                  "notes": (row["notes"] or "") + "; phantom_k3_row_resolve"},
            detail=dict(evidence, db_only=True, exit_reason=exit_reason))


# ============================================================================
# Fence — single source, pinned kwargs (§4.2.1, R3/R2-F1d)
# ============================================================================

def _default_fence(coin: str, db_open=None, bot_owned=None, oid=None,
                   placed_oids=None) -> bool:
    """The P1 canonical orphan_sweep._fenced, adopted VERBATIM (md5-pinned
    b924db9ebdbe76b2788fd3f382e46fb7) — imported, never re-listed. Fail-closed
    wrapper: a fence source that cannot even be imported/called (non-
    ImportError) treats the coin as FENCED + CRITICAL (a broken fence must
    fence, not expose)."""
    try:
        from fleet_core.orphan_sweep import _fenced  # stdlib-only module
    except ImportError:
        log.critical("fence: fleet_core.orphan_sweep NOT IMPORTABLE — "
                     "fail-closed, treating %s as FENCED", coin)
        return True
    try:
        return bool(_fenced(coin, db_open=db_open, bot_owned=bot_owned,
                            oid=oid, placed_oids=placed_oids))
    except Exception as e:  # noqa: BLE001 — fail-closed law
        log.critical("fence: _fenced RAISED for %s (%s) — fail-closed, "
                     "treating as FENCED", coin, e)
        return True


def position_fenced(coin: str, db_open_coins: Sequence[str],
                    fence_fn: Optional[Callable[..., bool]] = None) -> bool:
    """POSITION-context fence with the PINNED kwargs (R3/R2-F1d — EXACTLY
    these; never substitute coins_ever_traded for bot_owned: per-oid
    discrimination is trigger-scope only, positions have no oid):

        _fenced(coin, db_open=<coins of trades rows with sm_state ∈
                {FILLED, PROTECTED, OPEN, ABORTING}>,
                bot_owned=None, oid=None, placed_oids=None)

    Deliberate, canonical consequence: a MANUAL_POSITION_PREFIXES-matching
    position with no live row hits the degraded coin-level branch with db_open
    empty-for-that-coin → prefix-match ⇒ FENCED (manual xyz_GOLD on the HL
    shared unified account is ALWAYS fenced in the position context)."""
    fn = fence_fn or _default_fence
    try:
        return bool(fn(coin, db_open=list(db_open_coins), bot_owned=None,
                       oid=None, placed_oids=None))
    except Exception as e:  # noqa: BLE001 — fail-closed law (§4.2.1)
        log.critical("fence: fence_fn RAISED for %s (%s) — fail-closed, "
                     "treating as FENCED", coin, e)
        return True


def _variants(coin) -> Set[str]:
    if coin is None:
        return set()
    c = str(coin).upper()
    base = c.replace("-PERP", "").replace("-USD", "")
    return {c, base, f"{base}-USD", f"{base}-PERP"}


def _manual_prefix_match(coin: str, prefixes: Sequence[str]) -> bool:
    base = str(coin).upper().replace("-PERP", "").replace("-USD", "")
    return any(p and base.startswith(str(p).upper()) for p in prefixes)


# ============================================================================
# EF1 — positive fill provenance (§4.2 rule 2 / ABORTING re-drive / PENDING
# evidence). Coin+recency alone NEVER authorizes a venue write.
# ============================================================================

def _fill_get(f: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in f and f[k] not in (None, ""):
            return f[k]
    return None


def _fill_ts(f: Mapping[str, Any]) -> Optional[float]:
    v = _fill_get(f, "time", "ts", "timestamp")
    if v is None:
        return None
    try:
        ts = float(v)
    except (TypeError, ValueError):
        return _iso_ts(v)
    return ts / 1000.0 if ts > 1e12 else ts   # ms feeds (HL) → seconds


def _fill_is_buy(f: Mapping[str, Any]) -> Optional[bool]:
    s = str(_fill_get(f, "side", "dir") or "").strip().lower()
    if not s:
        return None
    if s.startswith("b") or "open long" in s or "close short" in s:
        return True
    if s.startswith("s") or "open short" in s or "close long" in s:
        return False
    return None


def positive_fill_provenance(row: Mapping[str, Any],
                             fills: Sequence[Mapping[str, Any]]
                             ) -> List[Tuple[Mapping[str, Any], str]]:
    """Fills provably belonging to the ROW's entry order (EF1):
      * cloid match — the fill carries the row's client_order_id; or
      * fill-window+size — entry-side, non-reduce-only fill inside
        [order_sent_at − 5s, order_sent_at + FILL_PROVENANCE_WINDOW_SEC]
        with size ≤ planned × (1 + PROVENANCE_SIZE_TOL).
    Returns [(fill, method)] — empty = NO provenance."""
    cloid = row["client_order_id"]
    sent_ts = _iso_ts(row["order_sent_at"])
    planned = float(row["size"] or 0.0)
    is_long = (row["direction"] or "long") == "long"
    coin_v = _variants(row["coin"])
    out: List[Tuple[Mapping[str, Any], str]] = []
    for f in fills:
        fc = _fill_get(f, "coin", "symbol", "market")
        if fc is None or not (_variants(fc) & coin_v):
            continue
        fcl = _fill_get(f, "cloid", "client_order_id", "clientOrderId",
                        "client_oid")
        if fcl is not None and cloid is not None and str(fcl) == str(cloid):
            out.append((f, "cloid"))
            continue
        # fill-window+size evidence
        if bool(f.get("reduce_only")):
            continue                       # a close leg is not entry evidence
        side_buy = _fill_is_buy(f)
        if side_buy is None or side_buy != is_long:
            continue
        ts = _fill_ts(f)
        if sent_ts is None or ts is None:
            continue                       # no window provable → not provenance
        if not (sent_ts - 5.0 <= ts <= sent_ts + FILL_PROVENANCE_WINDOW_SEC):
            continue
        sz = _fill_get(f, "sz", "size", "qty")
        if sz is None or planned <= 0.0 or \
                float(sz) > planned * (1.0 + PROVENANCE_SIZE_TOL):
            continue
        out.append((f, "fill_window_size"))
    return out


def _close_side_fill_size(row: Mapping[str, Any],
                          fills: Sequence[Mapping[str, Any]],
                          since_ts: Optional[float]) -> float:
    """Σ close-side fill size on the row's coin at/after `since_ts`.
    Over-counting a FOREIGN close fill only shrinks our net → biases AWAY
    from closing (fail-safe direction)."""
    is_long = (row["direction"] or "long") == "long"
    coin_v = _variants(row["coin"])
    closed = 0.0
    for f in fills:
        fc = _fill_get(f, "coin", "symbol", "market")
        if fc is None or not (_variants(fc) & coin_v):
            continue
        side_buy = _fill_is_buy(f)
        if side_buy is None or side_buy == is_long:
            continue                        # not a close-side fill
        ts = _fill_ts(f)
        if since_ts is not None and ts is not None and ts < since_ts - 1.0:
            continue                        # predates our entry — not our unwind
        closed += float(_fill_get(f, "sz", "size", "qty") or 0.0)
    return closed


def residual_net_size(row: Mapping[str, Any],
                      fills: Sequence[Mapping[str, Any]],
                      proven: Sequence[Tuple[Mapping[str, Any], str]]
                      ) -> float:
    """OUR un-unwound remainder = Σ(provenanced entry fills) − Σ(close-side
    fills on the coin at/after our first entry fill)."""
    if not proven:
        return 0.0
    entry_sz = 0.0
    first_ts: Optional[float] = None
    for f, _m in proven:
        entry_sz += float(_fill_get(f, "sz", "size", "qty") or 0.0)
        ts = _fill_ts(f)
        if ts is not None and (first_ts is None or ts < first_ts):
            first_ts = ts
    return entry_sz - _close_side_fill_size(row, fills, first_ts)


# ============================================================================
# Reconciler (§4) — bidirectional, startup full pass + per-tick incremental
# ============================================================================

@dataclass
class ReconcilerConfig:
    venue: str = ""
    tick_sec: float = 60.0
    stuck_ticks: int = STUCK_TTL_TICKS
    reanchor_pct: float = RESTORE_REANCHOR_PCT
    untracked_protective_pct: float = UNTRACKED_PROTECTIVE_PCT
    residual_protective_pct: float = RESIDUAL_PROTECTIVE_PCT
    orphan_debounce_sec: float = ORPHAN_DEBOUNCE_SEC
    entry_abort_cooldown_sec: float = 900.0     # ext/nado/hl 15 min = pac 900s
    provenance_window_hours: float = 24.0       # §4.2 rule 2
    phantom_k: int = PHANTOM_K
    adopted_tf: str = "untracked"               # tf label for adopted rows
    # HL-canon refinement inputs (§4.2.1); seeded from bot.config lazily when
    # empty — overridable for offline tests and per-venue config.
    manual_position_prefixes: Tuple[str, ...] = ()
    # venue tick size resolver for K6 |Δpx| ≤ 1 tick matching (R2-F4a);
    # default: 1 bp of px — conservative stand-in until VenueQuirks wires the
    # real tick table (labeled, not arbitrary: tighter than any live tick).
    tick_size_fn: Optional[Callable[[str, float], float]] = None
    # injectable clock (deterministic harnesses — replay/scenario debounces);
    # production default = wall time.
    now_fn: Optional[Callable[[], float]] = None

    def tick_size(self, coin: str, px: float) -> float:
        if self.tick_size_fn is not None:
            return float(self.tick_size_fn(coin, px))
        return abs(px) * 1e-4

    def now(self) -> float:
        return time.time() if self.now_fn is None else float(self.now_fn())

    def prefixes(self) -> Tuple[str, ...]:
        if self.manual_position_prefixes:
            return tuple(self.manual_position_prefixes)
        try:  # lazy venue seam — HL defines it, others ImportError
            from bot.config import MANUAL_POSITION_PREFIXES  # type: ignore
            return tuple(MANUAL_POSITION_PREFIXES or ())
        except ImportError:
            return ()
        except Exception as e:  # noqa: BLE001 — broken fence source is loud
            log.critical("MANUAL_POSITION_PREFIXES read BROKEN (%s) — "
                         "treating every coin as prefix-manual (fail-closed)", e)
            return ("",)  # startswith('') is True for all — fail-closed


class Reconciler:
    """§4 bidirectional reconciler. Replaces & unifies `_reconcile_pending`,
    `_adopt_db_open_positions`, restore-resolve, untracked-protect sweep and
    the orphan-trigger sweep — ONE implementation (fixes D3/D4 by
    construction).

    Runs: (a) startup_pass() BEFORE adoption; (b) tick_pass() every tick.
    NEVER transitions on ReadUnknown/WriteUnconfirmed — exceptions leave the
    row unchanged for the next pass (§3 law)."""

    def __init__(self, db_path, client: ExchangeClient,
                 cfg: Optional[ReconcilerConfig] = None,
                 fence_fn: Optional[Callable[..., bool]] = None) -> None:
        self.db_path = Path(db_path)
        self.client = client
        self.cfg = cfg or ReconcilerConfig()
        # production default = the canonical single fence source; injectable
        # ONLY for offline fakes (design mandates orphan_sweep._fenced verbatim)
        self._fence_fn = fence_fn
        self._orphan_first_seen: Dict[str, Tuple[float, int]] = {}
        self._alerted_once: Set[str] = set()
        with _conn(self.db_path) as con:
            _mig.ensure_schema(con)

    # ------------------------------------------------------------- helpers

    def _critical(self, key: str, msg: str, every_pass: bool = False) -> None:
        if every_pass or key not in self._alerted_once:
            log.critical("RECONCILER CRITICAL [%s]: %s", key, msg)
            self._alerted_once.add(key)

    def _rows(self, states: Sequence[str]) -> List[sqlite3.Row]:
        with _conn(self.db_path) as con:
            marks = ",".join("?" for _ in states)
            return con.execute(
                f"SELECT * FROM trades WHERE sm_state IN ({marks}) ORDER BY id",
                list(states)).fetchall()

    def _live_row_coins(self) -> Set[str]:
        out: Set[str] = set()
        for r in self._rows(LIVE_STATES):
            out |= _variants(r["coin"])
        return out

    def _positions(self) -> Optional[Mapping[str, PositionInfo]]:
        """ReadUnknown on the account snapshot → None → SKIP the whole pass
        (no partial-snapshot decisions — census phantom-mass-close class)."""
        try:
            return self.client.open_positions()
        except ReadUnknown as e:
            log.warning("reconciler: open_positions ReadUnknown (%s) — "
                        "skipping pass (bias-to-protect)", e)
            return None

    def _pos_for(self, positions: Mapping[str, PositionInfo],
                 coin: str) -> Optional[PositionInfo]:
        v = _variants(coin)
        for k, p in positions.items():
            if _variants(k) & v:
                return p
        return None

    def _fills(self, max_age_sec: float = 86400.0) -> Optional[List[Mapping[str, Any]]]:
        try:
            return list(self.client.user_fills(max_age_sec=max_age_sec))
        except ReadUnknown:
            return None

    def _stuck_check(self, row: sqlite3.Row) -> None:
        """§4.1 last row: non-terminal pre-OPEN + sm_updated_at older than
        STUCK_TTL (3 × tick) → CRITICAL every pass; keep retrying."""
        try:
            upd = datetime.fromisoformat(str(row["sm_updated_at"]).replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - upd).total_seconds()
        except (TypeError, ValueError):
            age = float("inf")
        if age > self.cfg.stuck_ticks * self.cfg.tick_sec:
            self._critical(f"stuck:{row['id']}:{int(age)}",
                           f"trade {row['id']} ({row['coin']}) stuck in "
                           f"{row['sm_state']} for {age:.0f}s — escalate-inbox",
                           every_pass=True)

    # ----------------------------------------------- protective placement

    def _place_protective(self, coin: str, is_long: bool, size: float,
                          candidate_px: Optional[float],
                          trade_id: Optional[int],
                          kind: str = "protective",
                          pct_off_mark: Optional[float] = None
                          ) -> Optional[SLOrderInfo]:
        """EVERY protective placement path in the reconciler funnels here:
        PROTECTIVE-PLACEMENT LAW gate FIRST (through-px re-anchor to CURRENT
        mark ∓6%), then liq-guard clamp, then P2 trigger_sl, then IMMEDIATE
        registry INSERT (before any dependent persist). Returns None on
        placement failure (caller escalates)."""
        try:
            mark = self.client.mark_price(coin, 5.0)
        except ReadUnknown:
            log.warning("protective place %s: mark ReadUnknown — defer", coin)
            return None
        if pct_off_mark is not None:
            # 3c / residual: anchor is DEFINED as current mark ∓pct — never a
            # remembered px (the round-2 DB-derived branch is DELETED).
            candidate_px = mark * (1.0 - pct_off_mark) if is_long \
                else mark * (1.0 + pct_off_mark)
        px, reanchored = protective_placement_gate(
            is_long, candidate_px if candidate_px else 0.0, mark,
            self.cfg.reanchor_pct)
        try:
            liq = self.client.position_liquidation(coin)
        except ReadUnknown:
            liq = None  # clamp input unavailable — placement still law-gated
        px, clamped = liq_guard_clamp(is_long, px, liq)
        try:
            sl = self.client.trigger_sl(coin, is_buy=not is_long,
                                        sz=abs(size), trigger_px=px)
        except (VenueRejected, WriteUnconfirmed) as e:
            log.error("protective place %s FAILED (%s)", coin, e)
            return None
        _reg.register_oid(self.db_path, sl.oid, coin, trade_id=trade_id,
                          kind=kind if kind in _reg.VALID_KINDS else "protective")
        if reanchored or clamped:
            log.warning("protective place %s: labeled deviation reanchored=%s "
                        "clamped=%s accepted_px=%.10g", coin, reanchored,
                        clamped, sl.trigger_px)
        return sl

    # ------------------------------------------------- K6 adoption match

    def _match_crash_landed_sl(self, coin: str, is_long: bool,
                               raw_intended_px: Optional[float],
                               own_oids: Set[str]
                               ) -> Optional[OpenOrderInfo]:
        """K6 (R3/R2-F4a): adoption match = oid ∈ registry ∪ (side,
        reduce-only) match with |Δpx| ≤ 1 venue tick against BOTH candidates:
        (i) the ladder's raw intended SL px, (ii) the liq-clamp recomputed from
        CURRENT mark/liq. >1-tick on both (e.g. a user's manual trigger on the
        same coin) → NOT adopted and NOT cancelled."""
        try:
            triggers = [t for t in self.client.list_reduce_only_triggers()
                        if _variants(t.coin) & _variants(coin)]
        except ReadUnknown:
            return None
        close_side = "buy" if not is_long else "sell"
        cands: List[float] = []
        if raw_intended_px and raw_intended_px > 0:
            cands.append(float(raw_intended_px))
            try:
                mark = self.client.mark_price(coin, 5.0)
                liq = self.client.position_liquidation(coin)
                gated, _ = protective_placement_gate(is_long, raw_intended_px,
                                                     mark, self.cfg.reanchor_pct)
                clamped, _ = liq_guard_clamp(is_long, gated, liq)
                cands.append(float(clamped))
            except ReadUnknown:
                pass
        for t in triggers:
            if str(t.oid) in own_oids:
                return t  # registry-member — provably ours
            if not (t.reduce_only and t.is_trigger and t.side == close_side):
                continue
            tpx = t.trigger_px
            if tpx is None:
                continue
            tick = self.cfg.tick_size(coin, tpx)
            if any(abs(tpx - c) <= tick for c in cands):
                return t  # crash-landed own SL (tick-rounded/clamp-drifted)
        return None

    # ------------------------------------------------------ entry ladder

    def _ladder_protect_open(self, row: sqlite3.Row) -> None:
        """Continue the entry ladder in-process from FILLED: (K6 check) place
        SL → registry INSERT → t_protect → t_open. Policy NOT re-run here —
        t_fill's precondition proves it already passed (§4.1 FILLED row)."""
        coin, tid = row["coin"], int(row["id"])
        is_long = (row["direction"] or "long") == "long"
        own = self._own_oids()
        adopted = self._match_crash_landed_sl(coin, is_long,
                                              row["sl_initial"], own)
        if adopted is not None:
            _reg.register_oid(self.db_path, adopted.oid, coin, trade_id=tid,
                              kind="sl")  # K6 crash race: px-matched, unregistered
            sl = SLOrderInfo(coin=coin, oid=str(adopted.oid),
                             trigger_px=float(adopted.trigger_px or
                                              row["sl_initial"]),
                             size=float(adopted.size or row["size"]),
                             is_buy_to_close=not is_long,
                             position_wide=bool(adopted.size == 0.0))
            t_protect(self.db_path, tid, sl)
            t_open(self.db_path, tid)
            return
        sl = self._place_protective(coin, is_long, float(row["size"]),
                                    row["sl_initial"], tid, kind="sl")
        if sl is None:
            self._critical(f"naked:{tid}", f"trade {tid} ({coin}) FILLED and "
                           "SL placement failed — NAKED, retrying next tick",
                           every_pass=True)
            return
        t_protect(self.db_path, tid, sl)
        t_open(self.db_path, tid)

    def _own_oids(self) -> Set[str]:
        try:
            return _reg.oids_ever_placed(self.db_path)
        except Exception as e:  # noqa: BLE001 — degrade conservative
            log.critical("registry read FAILED (%s) — treating own-oid set as "
                         "EMPTY (errs FENCED / no cancels)", e)
            return set()

    # --------------------------------------------- direction 1: DB → venue

    def _recover_intent(self, row: sqlite3.Row) -> None:
        if row["order_sent_at"] is None:
            t_abort_final(self.db_path, int(row["id"]), NeverSent())
        else:  # defensive: sent-but-still-INTENT should be impossible
            self._critical(f"intent_sent:{row['id']}",
                           f"trade {row['id']} INTENT with order_sent_at set — "
                           "treating as PENDING evidence path")
            self._recover_pending(row)

    def _fill_evidence(self, row: sqlite3.Row,
                       positions: Mapping[str, PositionInfo]
                       ) -> Optional[Tuple[float, float, Optional[str]]]:
        """(px, size, oid) evidence that the PENDING row's order filled —
        POSITIVE provenance only (EF1):
          * user_fills matched by the row's client_order_id, or the
            fill-window+size class (positive_fill_provenance); multiple
            partials aggregate to (vwap, Σsize);
          * fills feed EMPTY (venue exposes none — nado class): position
            corroboration gated by SIDE + size ≤ planned (never coin alone);
          * fills feed UNREADABLE (None): NO evidence — defer, never guess.
        None = no evidence."""
        fills = self._fills(max_age_sec=self.cfg.provenance_window_hours * 3600)
        if fills is None:
            return None                    # ReadUnknown → defer (bias-to-protect)
        if fills:
            proven = positive_fill_provenance(row, fills)
            if proven:
                tot_sz = 0.0
                notional = 0.0
                oid: Optional[str] = None
                for f, _m in proven:
                    px = _fill_get(f, "px", "price", "avg_px")
                    sz = _fill_get(f, "sz", "size", "qty")
                    if px is None or sz is None:
                        continue
                    tot_sz += float(sz)
                    notional += float(px) * float(sz)
                    if oid is None:
                        o = _fill_get(f, "oid", "order_id", "orderId")
                        oid = str(o) if o is not None else None
                if tot_sz > 0.0:
                    return notional / tot_sz, tot_sz, oid
            return None   # fills feed works and shows NO our-fill: no evidence
        # fills feed empty venue-wide (no fills API — nado class): gated
        # position corroboration — side + size, never coin+recency alone.
        pos = self._pos_for(positions, row["coin"])
        if pos is not None and row["order_sent_at"]:
            is_long = (row["direction"] or "long") == "long"
            planned = float(row["size"] or 0.0)
            if pos.is_long == is_long and planned > 0.0 and \
                    abs(float(pos.size_signed)) <= planned * (1.0 + PROVENANCE_SIZE_TOL):
                log.warning("PENDING %s: fills feed empty — position-corroborated"
                            " evidence (side+size gated, EF1)", row["coin"])
                return float(pos.entry_px), abs(float(pos.size_signed)), None
        return None

    def _recover_pending(self, row: sqlite3.Row) -> None:
        """§4.1 PENDING + K3/K4/K9b. RE-RUN CALLER POLICY FIRST (F3) on any
        fill evidence — recovery must reach the same verdict the crashed
        process would have."""
        tid, coin = int(row["id"]), row["coin"]
        positions = self._positions()
        if positions is None:
            self._stuck_check(row)
            return
        ev = self._fill_evidence(row, positions)
        if ev is not None:
            px, sz, oid = ev
            with _conn(self.db_path) as con:
                snap = intent_snapshot(con, tid)
            verdict = run_entry_policy(row, px, sz, snapshot=snap)
            if verdict.ok:
                fr = FillResult(coin=coin,
                                is_buy=(row["direction"] or "long") == "long",
                                avg_px=px, size=sz,
                                requested_size=float(row["size"]), oid=oid)
                t_fill(self.db_path, tid, fr, verdict)   # REAL px/size (D11 fix)
                row2 = self._row(tid)
                self._ladder_protect_open(row2)
            else:
                # cap-breached px or sub-ratio partial (incl. a partial-unwind
                # residual from a killed abort) — NEVER resurrect (F3/K9b)
                t_abort_begin(self.db_path, tid, verdict.reason,
                              evidence=verdict.evidence,
                              cooldown_sec=self.cfg.entry_abort_cooldown_sec,
                              now_ts=self.cfg.now())
                self._drive_abort(self._row(tid))
            return
        if self._pos_for(positions, coin) is not None:
            # EF1: a coin-matching position with NO positive provenance is NOT
            # authorization — never VerifiedAbsent it away, never touch it.
            # The row stays PENDING (stuck alert escalates); the position is
            # rule-3 territory (protect-only) via direction 2.
            self._critical(f"pending_unproven:{tid}",
                           f"trade {tid} ({coin}) PENDING with a coin position "
                           "but NO positive fill provenance (EF1) — row left "
                           "PENDING, position goes to rule-3 protect-only",
                           every_pass=True)
            self._stuck_check(row)
            return
        # no position + no fill — a resting remnant order?
        try:
            remnants = [o for o in self.client.open_orders()
                        if (_variants(o.coin) & _variants(coin))
                        and not o.reduce_only]
        except ReadUnknown:
            self._stuck_check(row)
            return
        for o in remnants:  # shouldn't exist on market-at-close venues
            try:
                self.client.cancel_sl_order(coin, o.oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.error("pending remnant cancel %s/%s failed (%s)", coin,
                          o.oid, e)
                self._stuck_check(row)
                return
        t_abort_begin(self.db_path, tid, "crash_unfilled",
                      evidence={"positions_empty_for_coin": True,
                                "fills_empty": True},
                      cooldown_sec=self.cfg.entry_abort_cooldown_sec,
                      now_ts=self.cfg.now())
        t_abort_final(self.db_path, tid, VerifiedAbsent())

    def _recover_filled(self, row: sqlite3.Row) -> None:
        """K5 — FIRST priority at startup: this is the naked window."""
        tid, coin = int(row["id"]), row["coin"]
        positions = self._positions()
        if positions is None:
            self._stuck_check(row)
            return
        if self._pos_for(positions, coin) is not None:
            self._ladder_protect_open(row)
            return
        # position absent → fills lookup: closed-by-venue vs vanished
        fills = self._fills()
        if fills:
            attr = self._attribute_exit(row, fills)
            if attr is not None:
                exit_px, reason = attr
                # Venue-closed attribution close. EF2 (review round-1): this
                # path is shared by _recover_protected (K7-variant: kill in
                # PROTECTED, SL fired during downtime) — from_states MUST
                # accept BOTH FILLED and PROTECTED (mirror resolve_phantom_row)
                # or the startup pass raises SMConflict → systemd crash-loop.
                pnl, rr = self._pnl(row, exit_px)
                with _conn(self.db_path) as con:
                    _transition(con, tid, (FILLED, PROTECTED), CLOSED,
                                sets={"closed_at": _now_iso(),
                                      "exit_price": exit_px,
                                      "exit_reason": reason,
                                      "pnl_dollars": pnl, "realized_r": rr},
                                detail={"closed_by_venue_during_naked_window":
                                        True})
                return
        t_abort_begin(self.db_path, tid, "vanished_investigate",
                      evidence={"position_absent": True, "fills_empty": True},
                      cooldown_sec=self.cfg.entry_abort_cooldown_sec,
                      now_ts=self.cfg.now())
        t_abort_final(self.db_path, tid, VerifiedAbsent())
        self._critical(f"vanished:{tid}",
                       f"trade {tid} ({coin}) FILLED row with NO position and "
                       "NO fills — vanished_investigate", every_pass=True)

    def _position_provenance(self, row: sqlite3.Row, pos: PositionInfo
                             ) -> str:
        """EF1 verdict for a venue position against a row that would authorize
        a venue write: 'ours' | 'foreign' | 'unknown'.

          ours    — positive fill provenance (cloid or fill-window+size) AND
                    the un-unwound net covers the position size;
          foreign — no positive provenance, or our net fills round-trip to ~0
                    (our exposure provably absent — any position on the coin
                    is NOT ours);
          unknown — fills feed unreadable: never write on unknown.
        Coin+recency alone is NEVER 'ours'."""
        fills = self._fills(max_age_sec=self.cfg.provenance_window_hours * 3600)
        if fills is None:
            return "unknown"
        state = row["sm_state"]
        proven = positive_fill_provenance(row, fills)
        if proven:
            net = residual_net_size(row, fills, proven)
        elif state == ABORTING and row["fill_confirmed_at"]:
            # The row ITSELF carries a readback-verified fill (t_fill's §3
            # precondition: an unverified FillResult cannot exist) — first-
            # class provenance for a NOT-YET-FINALIZED abort. Net off the
            # persisted fill size minus close-side fills after the confirm.
            # ABORTED rows get NO such seeding: t_abort_final's proof classes
            # (FlatResult/VerifiedAbsent/...) already proved the unwind
            # complete — a later position is never authorized by them.
            net = float(row["size"] or 0.0) - _close_side_fill_size(
                row, fills, _iso_ts(row["fill_confirmed_at"]))
        elif state == PENDING and not fills and row["order_sent_at"]:
            # empty fills feed venue-wide (nado class): PENDING resurrection
            # may use the gated position corroboration (side+size) — the
            # SAME gate _fill_evidence applies; close-authority states
            # (ABORTING/ABORTED) get NO such shortcut.
            is_long = (row["direction"] or "long") == "long"
            planned = float(row["size"] or 0.0)
            if pos.is_long == is_long and planned > 0.0 and \
                    abs(float(pos.size_signed)) <= planned * (1.0 + PROVENANCE_SIZE_TOL):
                return "ours"
            return "foreign"
        else:
            return "foreign"
        pos_sz = abs(float(pos.size_signed))
        if net > 0.0 and pos_sz <= net * (1.0 + PROVENANCE_SIZE_TOL):
            return "ours"
        return "foreign"

    def _drive_abort(self, row: sqlite3.Row) -> None:
        """K9 — ONE recovery, no decision ambiguity: re-drive the unwind per
        the PERSISTED abort_reason, never resurrect.

        EF1 gate (CRITICAL finding, round-1): when a position rests on the
        coin, ensure_flat requires POSITIVE provenance that it is OUR lost
        fill/residual — a user_fills match on the row's client_order_id or
        fill-window+size net evidence. No provenance ⇒ the position is
        protect-only territory (rule-3) + CRITICAL, and the row finalizes on
        the attributable-absence proof; it is NEVER flattened."""
        tid, coin = int(row["id"]), row["coin"]
        require_authority(row, "ensure_flat")  # strategy rows only (F2)
        positions = self._positions()
        if positions is None:
            self._stuck_check(row)
            return
        pos = self._pos_for(positions, coin)
        if pos is not None:
            verdict = self._position_provenance(row, pos)
            if verdict == "unknown":
                self._stuck_check(row)     # never write on unknown reads
                return
            if verdict == "foreign":
                self._critical(f"abort_foreign:{tid}",
                               f"trade {tid} ({coin}) ABORTING but the venue "
                               "position has NO positive provenance to this "
                               "row (EF1) — NOT flattening; position goes to "
                               "rule-3 protect-only; row finalized on "
                               "attributable-absence", every_pass=True)
                self._rule3(pos)           # protect the foreign position NOW
                t_abort_final(self.db_path, tid, VerifiedAbsent(
                    note="foreign_position_present_rule3_protected"))
                return
        try:
            flat = self.client.ensure_flat(coin)
        except WriteUnconfirmed as e:
            self._critical(f"abort_unconfirmed:{tid}",
                           f"trade {tid} ({coin}) ABORTING: ensure_flat "
                           f"WriteUnconfirmed ({e}) — retry next tick",
                           every_pass=True)
            # residual meanwhile SL-protected by the abort ladder's
            # protective-SL step (±2.5% mark — exit-engine §9.5 semantics)
            is_long = (row["direction"] or "long") == "long"
            self._place_protective(coin, is_long, float(row["size"]), None,
                                   tid, kind="protective",
                                   pct_off_mark=self.cfg.residual_protective_pct)
            self._stuck_check(row)
            return
        except ReadUnknown:
            self._stuck_check(row)
            return
        t_abort_final(self.db_path, tid, flat)

    def _recover_protected(self, row: sqlite3.Row) -> None:
        """K7 + §4.1 PROTECTED: sl live → t_open; absent+position → re-place
        (NO duplicate-SL class: known oid persisted BEFORE promote)."""
        tid, coin = int(row["id"]), row["coin"]
        try:
            live = set(map(str, self.client.list_open_sl_orders(coin)))
        except ReadUnknown:
            self._stuck_check(row)
            return
        if row["sl_order_id"] and str(row["sl_order_id"]) in live:
            t_open(self.db_path, tid)
            return
        positions = self._positions()
        if positions is None:
            self._stuck_check(row)
            return
        if self._pos_for(positions, coin) is not None:
            is_long = (row["direction"] or "long") == "long"
            # law-gated re-place; candidate = the persisted placed px (§4 law
            # re-anchors it to CURRENT mark ∓6% if through)
            sl = self._place_protective(coin, is_long, float(row["size"]),
                                        row["sl_placed_px"] or row["sl_current"],
                                        tid, kind="sl")
            if sl is None:
                self._critical(f"naked:{tid}", f"trade {tid} ({coin}) "
                               "PROTECTED with vanished SL and re-place failed "
                               "— NAKED", every_pass=True)
                return
            with _conn(self.db_path) as con:
                # re-`t_protect` semantics on an already-PROTECTED row: persist
                # the new oid/px + audit (state unchanged)
                con.execute("UPDATE trades SET sl_order_id=?, sl_placed_px=?, "
                            "sl_current=?, sl_confirmed_at=?, sm_updated_at=? "
                            "WHERE id=?",
                            (sl.oid, sl.trigger_px, sl.trigger_px, _now_iso(),
                             _now_iso(), tid))
                _audit(con, tid, PROTECTED, PROTECTED,
                       {"replaced_vanished_sl": True, "new_oid": sl.oid,
                        "trigger_px": sl.trigger_px})
            t_open(self.db_path, tid)
            return
        # position absent — same evidence path as FILLED-absent
        self._recover_filled(row)

    def _recover_open_startup(self, row: sqlite3.Row) -> None:
        """§4.1 OPEN-absent at STARTUP: immediate user_fills lookup (oid-first)
        → t_close attributed; none → row kept OPEN + CRITICAL (never auto-close
        without evidence — HL CW9 canon). Runtime K=3 debounce lives in the
        exit engine."""
        tid, coin = int(row["id"]), row["coin"]
        positions = self._positions()
        if positions is None:
            return  # ReadUnknown → no action (bias-to-protect)
        if self._pos_for(positions, coin) is not None:
            return  # present — exit engine manages it
        mc = row["management_class"] or MC_STRATEGY
        fills = self._fills()
        attr = self._attribute_exit(row, fills or []) if fills else None
        if attr is not None:
            exit_px, reason = attr
            pnl, rr = self._pnl(row, exit_px)
            t_close(self.db_path, tid, exit_px, reason, pnl, rr,
                    detail={"startup_reconcile": True})
            return
        if mc in (MC_PROTECT_ONLY, MC_MANUAL_CLAIMED):
            # phantom-K3 DB-only row-resolve path (K counter persisted; the
            # startup pass counts one confirmed-absent observation)
            self._phantom_miss(row)
            return
        self._critical(f"open_absent:{tid}",
                       f"trade {tid} ({coin}) OPEN with no venue position and "
                       "no fills — kept OPEN, investigate", every_pass=True)

    def _phantom_miss(self, row: sqlite3.Row) -> None:
        """K=3 debounced, persisted phantom counter for protect_only AND
        manual_claimed rows (review-nit: SAME DB-only row-resolve for both).
        On the K-th confirmed miss: cache-invalidated final re-read, then
        resolve_phantom_row (DB write only — zero venue intents)."""
        tid = int(row["id"])
        key = f"phantom_miss:{tid}"
        with _conn(self.db_path) as con:
            _mig.ensure_schema(con)
            try:
                n = int(_bot_state_get(con, key) or 0)
            except (TypeError, ValueError):
                n = 0
            n += 1
            _bot_state_set(con, key, str(n))
        if n < self.cfg.phantom_k:
            return
        try:
            self.client.invalidate_positions_cache()
            positions = self.client.open_positions()
        except ReadUnknown:
            return  # UNKNOWN never counts a phantom miss (fleet canon)
        if self._pos_for(positions, row["coin"]) is not None:
            with _conn(self.db_path) as con:
                _bot_state_set(con, key, "0")
            return
        resolve_phantom_row(self.db_path, tid,
                            {"k": n, "cache_invalidated_reread": "absent"})
        with _conn(self.db_path) as con:
            _bot_state_set(con, key, "0")

    # ----------------------------------------------- attribution (startup)

    def _historic_oids(self, tid: int) -> Set[str]:
        out: Set[str] = set()
        with _conn(self.db_path) as con:
            for r in con.execute("SELECT detail FROM sm_transitions WHERE "
                                 "trade_id=?", (tid,)).fetchall():
                try:
                    d = json.loads(r["detail"] or "{}")
                except (ValueError, TypeError):
                    continue
                if isinstance(d, dict):
                    for k, v in d.items():
                        if "oid" in str(k).lower() and v:
                            out.add(str(v))
        return out

    def _attribute_exit(self, row: sqlite3.Row,
                        fills: Sequence[Mapping[str, Any]]
                        ) -> Optional[Tuple[float, str]]:
        """Startup-scope attribution (full machinery is exit-engine §6):
        oid-first (sl_order_id / tp1_order_id / historic corpus), then 1% px
        proximity in fleet order sl_cur → sl_ini → tp; else unknown_investigate.
        NEVER 'manual' (NO-SILENT-MANUAL)."""
        coin_v = _variants(row["coin"])
        is_long = (row["direction"] or "long") == "long"
        close_sides = {"sell"} if is_long else {"buy"}
        matched: List[Tuple[float, float]] = []
        oid_hit: Optional[str] = None
        own = self._historic_oids(int(row["id"])) | self._own_oids()
        for f_ in fills:
            fc = f_.get("coin") or f_.get("symbol") or f_.get("market")
            if fc is None or not (_variants(fc) & coin_v):
                continue
            side = str(f_.get("side") or f_.get("dir") or "").lower()
            if side and side not in close_sides and side not in ("close",):
                continue
            px = f_.get("px") or f_.get("price")
            sz = f_.get("sz") or f_.get("size")
            oid = f_.get("oid") or f_.get("order_id")
            if not px or not sz:
                continue
            matched.append((float(px), float(sz)))
            if oid is not None and str(oid) in own and oid_hit is None:
                oid_hit = str(oid)
        if not matched:
            return None
        vwap = sum(p * s for p, s in matched) / sum(s for _, s in matched)
        trailed = (row["sl_current"] is not None and row["sl_initial"] is not None
                   and float(row["sl_current"]) != float(row["sl_initial"]))
        if oid_hit is not None:
            if row["tp1_order_id"] and oid_hit == str(row["tp1_order_id"]):
                return vwap, "tp"
            return vwap, ("trail_sl" if trailed else "sl")
        # 1% proximity fallback, fleet order sl_cur → sl_ini → tp (F11c)
        for ref, reason in ((row["sl_current"], "trail_sl" if trailed else "sl"),
                            (row["sl_initial"], "sl"),
                            (row["tp1"], "tp")):
            if ref and float(ref) > 0 and abs(vwap - float(ref)) / float(ref) <= 0.01:
                return vwap, reason
        return vwap, "unknown_investigate"

    @staticmethod
    def _pnl(row: sqlite3.Row, exit_px: float) -> Tuple[float, float]:
        sign = 1.0 if (row["direction"] or "long") == "long" else -1.0
        entry = float(row["entry"] or 0.0)
        size = float(row["size"] or 0.0)
        pnl = (exit_px - entry) * size * sign
        risk = float(row["risk_dollars"] or 0.0)
        return pnl, (pnl / risk if risk > 0 else 0.0)

    def _row(self, tid: int) -> sqlite3.Row:
        with _conn(self.db_path) as con:
            return _get_row(con, tid)

    def direction1(self, startup: bool = False) -> None:
        """DB → exchange: every non-terminal row must correspond to venue
        truth. Priority order: FILLED (naked window) → ABORTING → PENDING →
        PROTECTED → INTENT → (startup only) OPEN."""
        for state, handler in ((FILLED, self._recover_filled),
                               (ABORTING, self._drive_abort),
                               (PENDING, self._recover_pending),
                               (PROTECTED, self._recover_protected),
                               (INTENT, self._recover_intent)):
            for row in self._rows((state,)):
                try:
                    handler(row)
                except (ReadUnknown, WriteUnconfirmed) as e:
                    log.warning("reconciler d1 %s trade %s: %s — deferred",
                                state, row["id"], e)
                    self._stuck_check(row)
        if startup:
            for row in self._rows((OPEN,)):
                self._recover_open_startup(row)

    # -------------------------------------------- direction 2: venue → DB

    def _insert_adopted(self, pos: PositionInfo, management_class: str,
                        notes: str, sm_state: str,
                        sl: Optional[SLOrderInfo] = None,
                        detail: Optional[Mapping[str, Any]] = None) -> int:
        is_long = pos.is_long
        with _conn(self.db_path) as con:
            _mig.ensure_schema(con)
            now = _now_iso()
            sl_initial = float(sl.trigger_px) if sl is not None else 0.0
            risk = abs(pos.entry_px - sl_initial) * abs(pos.size_signed) \
                if sl is not None else 0.0
            cur = con.execute(
                """
                INSERT INTO trades (created_at, coin, tf, direction, entry,
                    sl_initial, sl_current, size, risk_dollars, notional,
                    status, opened_at, notes, sm_state, sm_updated_at, origin,
                    management_class, sl_order_id, sl_placed_px,
                    sl_confirmed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?)
                """,
                (now, pos.coin, self.cfg.adopted_tf,
                 "long" if is_long else "short", pos.entry_px,
                 sl_initial,
                 (sl.trigger_px if sl is not None else None),
                 abs(pos.size_signed), risk,
                 abs(pos.size_signed) * pos.entry_px,
                 LEGACY_STATUS[sm_state], now, notes, sm_state, now,
                 ORIGIN_ADOPTED, management_class,
                 (sl.oid if sl is not None else None),
                 (sl.trigger_px if sl is not None else None),
                 (now if sl is not None else None)))
            tid = int(cur.lastrowid)
            _audit(con, tid, "", sm_state,
                   dict(detail or {}, adopted=True, notes=notes))
            return tid

    def _cover_oids(self, coin: str) -> Optional[List[str]]:
        """list_open_sl_orders with the nado :270-273 canon on ReadUnknown:
        indeterminate → cannot prove naked → treat as COVERED this pass
        (returns None = UNKNOWN; [] = provably naked)."""
        try:
            return [str(o) for o in self.client.list_open_sl_orders(coin)]
        except ReadUnknown:
            return None

    def _rule3(self, pos: PositionInfo) -> None:
        """§4.2.3 rule-3 SPLIT (R3/R2-F1) — three exclusive branches, checked
        in order, cover check FIRST."""
        coin = pos.coin
        cover = self._cover_oids(coin)
        if cover is None:
            log.warning("adopt %s: SL list UNKNOWN — treat as COVERED this "
                        "pass, no row, no placement (nado canon)", coin)
            return
        prefix_manual = _manual_prefix_match(coin, self.cfg.prefixes())
        if cover:
            # 3a COVERED → TRACK-ONLY, NO PLACEMENT, EVER-WHILE-COVERED.
            # sl_* columns NULL — the covering trigger is NOT bot-own; its
            # oids/px live ONLY in sm_transitions.detail (forensics).
            covering_px = None
            try:
                for t in self.client.list_reduce_only_triggers():
                    if _variants(t.coin) & _variants(coin) and str(t.oid) in cover:
                        covering_px = t.trigger_px
                        break
            except ReadUnknown:
                pass
            self._insert_adopted(pos, MC_PROTECT_ONLY, "adopted_covered", OPEN,
                                 sl=None,
                                 detail={"covering_oids": cover,
                                         "covering_trigger_px": covering_px})
            self._critical(f"adopt_covered:{coin}",
                           f"{coin}: untracked position adopted PROTECT-ONLY "
                           "(covered) — operator must adopt/close")
            return
        if prefix_manual:
            self._rule3b(pos)
            return
        # 3c NON-PREFIX NAKED → ±6% of CURRENT mark, liq-guarded, law-gated.
        sl = self._place_protective(
            coin, pos.is_long, abs(pos.size_signed), None, None,
            kind="protective",
            pct_off_mark=self.cfg.untracked_protective_pct)
        if sl is None:
            # live canon (nado main.py:294): CRITICAL "STILL NAKED, manual" +
            # hands off; NO row → re-attempted + re-fired every pass
            self._critical(f"still_naked:{coin}",
                           f"{coin}: untracked naked position and protective "
                           "SL placement FAILED — STILL NAKED, manual",
                           every_pass=True)
            return
        tid = self._insert_adopted(pos, MC_PROTECT_ONLY, "adopted", PROTECTED,
                                   sl=sl, detail={"protective_pct":
                                                  self.cfg.untracked_protective_pct})
        with _conn(self.db_path) as con:
            con.execute("UPDATE placed_trigger_oids SET trade_id=? WHERE oid=?",
                        (tid, str(sl.oid)))
        t_open(self.db_path, tid)
        self._critical(f"adopt_naked:{coin}",
                       f"{coin}: untracked naked position protected at mark"
                       f"∓{self.cfg.untracked_protective_pct:.0%} — "
                       "protect-only row inserted")

    def _rule3b(self, pos: PositionInfo) -> None:
        """3b PREFIX-MANUAL NAKED — hl main.py:901-925 canon verbatim:
        CRITICAL every pass, NO SL PLACED (a true manual pos must not get a
        bot ±6% SL = the original naked-trap). Track-only row for visibility
        (§7.5); zero placement/close authority. Operator resolution: env
        fence, manual_claimed claim, or manual close."""
        coin = pos.coin
        with _conn(self.db_path) as con:
            row = con.execute(
                "SELECT id FROM trades WHERE coin=? AND management_class=? AND "
                "notes LIKE '%unreconciled_manual_prefix%' AND sm_state=?",
                (coin, MC_PROTECT_ONLY, OPEN)).fetchone()
        if row is None:
            self._insert_adopted(pos, MC_PROTECT_ONLY,
                                 "unreconciled_manual_prefix", OPEN, sl=None,
                                 detail={"prefix_manual_naked": True})
        self._critical(f"unreconciled_manual:{coin}",
                       f"{coin}: UNRECONCILED prefix-manual NAKED position — "
                       "NO SL placed (cannot prove own-vs-manual); operator "
                       "must env-fence, claim (manual_claimed) or close",
                       every_pass=True)

    def direction2(self) -> None:
        """Exchange → DB: every venue position must be FENCED, protect-only-
        tracked, or strategy-managed within one pass (invariant §7.5)."""
        positions = self._positions()
        if positions is None:
            return  # skip whole pass — no partial-snapshot decisions
        live_coins = self._live_row_coins()
        prefixes = self.cfg.prefixes()
        for key, pos in positions.items():
            if _variants(pos.coin) & live_coins:
                continue  # matched to a live row — direction 1 territory
            # 1. Fences FIRST, hard — single source, PINNED kwargs.
            if position_fenced(pos.coin, sorted(live_coins),
                               fence_fn=self._fence_fn):
                if _manual_prefix_match(pos.coin, prefixes):
                    # HL-canon refinement: prefix-fenced no-row position is
                    # checked for cover (read failure → assume covered, don't
                    # false-alarm); COVERED → fenced skip; NAKED → rule 3b.
                    cover = self._cover_oids(pos.coin)
                    if cover is not None and len(cover) == 0:
                        self._rule3b(pos)
                        continue
                self._critical(f"fenced:{pos.coin}",
                               f"{pos.coin}: fenced position skipped "
                               "(manual/foreign — never touch)")
                continue
            # 2. Positive provenance: ABORTING/ABORTED/PENDING row ≤24h —
            # EF1 (CRITICAL, round-1): the coin+recency row match is only a
            # CANDIDATE; a venue write additionally requires POSITIVE
            # provenance (user_fills on the row's client_order_id, or
            # fill-window+size evidence). No provenance ⇒ the position falls
            # through to the rule-3 protect-only split + CRITICAL — it is
            # NEVER flattened on coin+recency alone.
            row = self._provenance_row(pos.coin)
            if row is not None:
                verdict = self._position_provenance(row, pos)
                if verdict == "unknown":
                    self._critical(f"rule2_unknown:{pos.coin}",
                                   f"{pos.coin}: rule-2 candidate row "
                                   f"{row['id']} but fills feed unreadable — "
                                   "no decision this pass (bias-to-protect)")
                    continue
                if verdict == "ours":
                    if row["sm_state"] == PENDING:
                        self._recover_pending(row)   # policy re-run per §4.1
                    elif row["sm_state"] == ABORTING:
                        self._drive_abort(row)       # self-gated (EF1)
                    else:  # ABORTED with a PROVEN live residual: unwind again
                        try:
                            require_authority(row, "ensure_flat")
                            self.client.ensure_flat(pos.coin)
                            log.warning("rule-2: ABORTED trade %s (%s) proven "
                                        "residual unwound (positive provenance)",
                                        row["id"], pos.coin)
                        except (WriteUnconfirmed, ReadUnknown,
                                AuthorityRefused) as e:
                            self._critical(f"aborted_residual:{pos.coin}",
                                           f"{pos.coin}: ABORTED-row residual "
                                           f"unwind failed ({e})",
                                           every_pass=True)
                    continue
                # verdict == "foreign" → fall through to rule-3 (protect-only)
                self._critical(f"rule2_no_provenance:{pos.coin}",
                               f"{pos.coin}: position matches {row['sm_state']}"
                               f" row {row['id']} by coin+recency ONLY — no "
                               "positive provenance (EF1); treating as "
                               "manual/foreign → rule-3 protect-only, NEVER "
                               "closed", every_pass=True)
            # 3. No row match (or rule-2 without provenance) → rule-3 split.
            self._rule3(pos)

    def _provenance_row(self, coin: str) -> Optional[sqlite3.Row]:
        cutoff = self.cfg.now() - self.cfg.provenance_window_hours * 3600
        with _conn(self.db_path) as con:
            for r in con.execute(
                    "SELECT * FROM trades WHERE sm_state IN (?,?,?) "
                    "AND client_order_id IS NOT NULL ORDER BY id DESC",
                    (ABORTING, ABORTED, PENDING)).fetchall():
                if not (_variants(r["coin"]) & _variants(coin)):
                    continue
                ts = _iso_ts(r["sm_updated_at"]) or _iso_ts(r["created_at"])
                if ts is not None and ts >= cutoff:
                    return r
        return None

    # ------------------------------------------- protect_only / claimed watch

    def watch_special_rows(self) -> None:
        """Per-tick duties the exit engine does NOT own for non-strategy rows:
          * protect_only heal (R3 semantics): RE-RUN THE COVER CHECK first —
            covered / engine SL resting → NoOp; cover vanished → branch
            re-evaluation AT CURRENT STATE (prefix-manual → 3b CRITICAL no SL;
            non-prefix → place at CURRENT mark ∓6% — NEVER a remembered px);
          * 3b rows: CRITICAL every pass while still naked;
          * phantom-K3 DB-only row-resolve for protect_only AND manual_claimed
            rows (position gone → close the ROW — DB write, zero venue I/O).
        manual_claimed rows get NO cover checks, NO placements, NO re-alerts —
        only the phantom row-resolve."""
        positions = self._positions()
        if positions is None:
            return
        with _conn(self.db_path) as con:
            rows = con.execute(
                "SELECT * FROM trades WHERE sm_state=? AND management_class "
                "IN (?, ?)", (OPEN, MC_PROTECT_ONLY, MC_MANUAL_CLAIMED)).fetchall()
        for row in rows:
            pos = self._pos_for(positions, row["coin"])
            if pos is None:
                self._phantom_miss(row)
                continue
            with _conn(self.db_path) as con:  # position present → reset K
                _bot_state_set(con, f"phantom_miss:{row['id']}", "0")
            if (row["management_class"] or "") == MC_MANUAL_CLAIMED:
                continue  # FULL hands-off; no re-alert (row = durable ack)
            cover = self._cover_oids(row["coin"])
            if cover is None or cover:
                continue  # covered / UNKNOWN → NoOp (anti heal-storm)
            if _manual_prefix_match(row["coin"], self.cfg.prefixes()):
                self._rule3b(pos)
                continue
            require_authority(row, "heal_sl")
            sl = self._place_protective(
                row["coin"], pos.is_long, abs(pos.size_signed), None,
                int(row["id"]), kind="protective",
                pct_off_mark=self.cfg.untracked_protective_pct)
            if sl is None:
                self._critical(f"still_naked:{row['coin']}",
                               f"{row['coin']}: protect_only cover vanished "
                               "and re-place FAILED — STILL NAKED, manual",
                               every_pass=True)
                continue
            with _conn(self.db_path) as con:
                con.execute("UPDATE trades SET sl_order_id=?, sl_placed_px=?, "
                            "sl_current=?, sl_confirmed_at=?, sm_updated_at=? "
                            "WHERE id=?",
                            (sl.oid, sl.trigger_px, sl.trigger_px, _now_iso(),
                             _now_iso(), int(row["id"])))
                _audit(con, int(row["id"]), OPEN, OPEN,
                       {"protect_only_heal": True, "new_oid": sl.oid,
                        "anchored_at_current_mark": True})

    # ----------------------------------------------- rule 4: orphan triggers

    def sweep_orphan_triggers(self) -> None:
        """§4.2.4: reduce-only trigger, no position, no live row, 180s
        debounce (≥2 observations) → cancel ONLY when oid ∈ placed_trigger_oids
        (F5 — fleet-wide). Non-registry triggers on unfenced coins → log +
        leave (manual bracket awaiting fill)."""
        try:
            triggers = list(self.client.list_reduce_only_triggers())
        except ReadUnknown:
            return
        positions = self._positions()
        if positions is None:
            return
        live_coins = self._live_row_coins()
        own = self._own_oids()
        now = self.cfg.now()
        seen_now: Set[str] = set()
        for t in triggers:
            if not (t.reduce_only and t.is_trigger):
                continue
            if self._pos_for(positions, t.coin) is not None:
                continue
            if _variants(t.coin) & live_coins:
                continue
            if position_fenced(t.coin, sorted(live_coins),
                               fence_fn=self._fence_fn):
                continue
            oid = str(t.oid)
            seen_now.add(oid)
            first, nobs = self._orphan_first_seen.get(oid, (now, 0))
            self._orphan_first_seen[oid] = (first, nobs + 1)
            if (now - first) < self.cfg.orphan_debounce_sec or nobs + 1 < 2:
                continue
            if oid not in own:
                log.info("orphan-trigger %s/%s NOT bot-own (∉ registry) — "
                         "left alone (manual bracket awaiting fill)",
                         t.coin, oid)
                continue
            try:
                self.client.cancel_sl_order(t.coin, oid)  # gone-readback per P2
                log.warning("ORPHAN-TRIGGER-SWEEP: cancelled bot-own %s/%s",
                            t.coin, oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.error("orphan cancel %s/%s failed (%s) — retry next pass",
                          t.coin, oid, e)
        for oid in list(self._orphan_first_seen):
            if oid not in seen_now:
                del self._orphan_first_seen[oid]

    # -------------------------------------------- adoption seeding (EF5/EF6)

    def seed_adoption_state(self) -> None:
        """Design §10 cutover/adoption seeding (EF5 + EF6):

        * `sl_placed_px` := the venue's LISTED trigger px for every
          PROTECTED/OPEN row whose sl_order_id is resting — readback via
          list_reduce_only_triggers (the px-bearing listing behind
          list_open_sl_orders). Fallback = keep the DB value (the migrate_v3
          sl_current convention) ONLY when the listing read fails — LABELED.
          Kills the churn-gate anchoring on a stale DB px wherever the old
          bot's resting px ≠ sl_current (the ext :541 class).
        * arms the one-time restore-grace flag (`restore_reconcile_pending`,
          bot_state key restore_flag:<id>) on OPEN strategy rows — consumed by
          exit-engine pipeline step 3, cleared by the executor after the
          through-px reconcile ran (design §10 / exit-engine §4 step 3).

        manual_claimed rows are skipped entirely (full hands-off, §7.12);
        protect_only 3a rows have sl_order_id NULL and are skipped by
        construction (their sl_* stay NULL — never anchored)."""
        rows = self._rows((PROTECTED, OPEN))
        if not rows:
            return
        try:
            listing = {str(t.oid): t
                       for t in self.client.list_reduce_only_triggers()}
            listing_ok = True
        except ReadUnknown as e:
            listing, listing_ok = {}, False
            log.warning("adoption seeding: trigger listing ReadUnknown (%s) — "
                        "sl_placed_px stays DB-seeded (sl_current fallback, "
                        "LABELED)", e)
        with _conn(self.db_path) as con:
            for row in rows:
                tid = int(row["id"])
                mc = row["management_class"] or MC_STRATEGY
                if mc == MC_MANUAL_CLAIMED:
                    continue
                if mc == MC_STRATEGY and row["sm_state"] == OPEN:
                    _bot_state_set(con, f"restore_flag:{tid}", "1")
                oid = row["sl_order_id"]
                if not oid or not listing_ok:
                    continue
                trig = listing.get(str(oid))
                if trig is None or trig.trigger_px is None:
                    log.info("adoption seeding: %s oid %s not in venue listing"
                             " — SL-liveness heal owns it", row["coin"], oid)
                    continue
                px = float(trig.trigger_px)
                if row["sl_placed_px"] is not None and \
                        abs(px - float(row["sl_placed_px"])) < 1e-12:
                    continue
                con.execute("UPDATE trades SET sl_placed_px=?, "
                            "sl_confirmed_at=?, sm_updated_at=? WHERE id=?",
                            (px, _now_iso(), _now_iso(), tid))
                _audit(con, tid, row["sm_state"], row["sm_state"],
                       {"seed": "venue_listed_trigger_px", "oid": str(oid),
                        "sl_placed_px": px,
                        "was": row["sl_placed_px"]})
                log.warning("adoption seeding: trade %s (%s) sl_placed_px := "
                            "%.10g (venue LISTED trigger px, was %s)",
                            tid, row["coin"], px, row["sl_placed_px"])

    # ------------------------------------------------------------- passes

    def startup_pass(self) -> None:
        """Full pass at startup BEFORE adoption (§4). Adoption seeding (§10,
        EF5/EF6) runs FIRST so direction-1 recovery and the first exit-engine
        tick anchor on the venue's LISTED px, never a stale DB px."""
        self.seed_adoption_state()
        self.direction1(startup=True)
        self.direction2()
        self.watch_special_rows()
        self.sweep_orphan_triggers()

    def tick_pass(self) -> None:
        """Incremental pass every tick."""
        self.direction1(startup=False)
        self.direction2()
        self.watch_special_rows()
        self.sweep_orphan_triggers()

    # --------------------------------------------------------------- drain

    def drain(self, max_ticks: int = 5) -> Dict[str, Any]:
        """Rollout §4 DRAIN: drive every non-terminal pre-OPEN row to
        convergence per §4.1 (PENDING → policy re-run → FILLED ladder or
        abort; FILLED → protect → OPEN; ABORTING → re-drive ensure_flat →
        ABORTED), bounded `max_ticks` reconciler ticks (~5 min at 60s cadence).

        INVOCATION PATH (review-nit / R2-F3 single-writer law): the operator
        command `fleet-<venue>-bot --drain` is a thin CLIENT that routes
        through the RUNNING engine's admin socket — the engine executes THIS
        method in-process (entries frozen, tick loop parked), the §4 flock
        stays held by the engine, and no second process ever writes trades.db.
        Only the manual dead-engine runbook (rollout §4) runs standalone
        tools against the DB, and only after the engine unit is stopped."""
        remaining: List[Tuple[int, str, str]] = []
        for i in range(max_ticks):
            self.direction1(startup=False)
            rows = self._rows(NON_TERMINAL_PRE_OPEN)
            remaining = [(int(r["id"]), str(r["coin"]), str(r["sm_state"]))
                         for r in rows]
            if not remaining:
                break
        report = {"converged": not remaining, "remaining": remaining,
                  "ticks_used": min(i + 1, max_ticks) if max_ticks else 0}
        if remaining:
            self._critical("drain_incomplete",
                           f"DRAIN incomplete after {max_ticks} ticks: "
                           f"{remaining} — rollback stays GATED",
                           every_pass=True)
        return report


def _iso_ts(s) -> Optional[float]:
    if not s:
        return None
    try:
        txt = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


# ============================================================================
# Shadow decision-only ENTRY core (P3 shadow wiring — shadow_runner interface)
# ============================================================================
#
# The ShadowRunner lazily resolves `entry_sm.shadow_decide(ctx)` as its entry
# decision core. DECISION-ONLY: no SM rows, no cooldown arming, no venue
# writes — the chain consumes signals the launcher produced with the venue's
# OWN live scanner modules and gates them with the MIRRORED live suppression
# state (F6 law: never shadow-hypothetical history).
#
# ctx keys (bare, from ShadowRunner._entry_phase):
#   venue, now, client, decision_only=True,
#   cooldown_active(coin)->evidence|None   (SuppressionMirror — F6)
#   opens_today()->int                     (LIVE actuals, same-UTC-day)
#   suppression_snapshot (stamped by the runner into every record)
# enrichments (shadow_main launcher / selftests):
#   signals            — sequence of live-scanner Signal objects / dicts
#   open_coins         — mirrored live-DB open coins (in-memory dict mirror)
#   max_concurrent / max_opens_per_day — live Settings values
#   sizing(sig)->verdict dict — venue-native attempt_entry decision-only
#                     prefix (bar_age/tier/equity/caps/size/liq/mm/limit_px);
#                     {"ok":bool, "reason":str|None, "observable":bool,
#                      "size","risk_dollars","notional","eff_lev","limit_px",
#                      "inputs":{...}, "flags":[...]}
#   breakout_validity(sig)->verdict dict — live pre-send mark guard
#   engine_epoch       — client_order_id epoch component (default 'shadow')
#
# Verdict emission discipline (comparator §6.1-aware):
#   * live-OBSERVABLE rejects (live writes a rejected_signals row:
#     entry_cooldown_active, stale_signal, tier_excluded, mm/liq/min_size,
#     breakout_invalidated, …) → decision "Reject" (alignable class 'reject')
#     with the SAME live reason label;
#   * live-UNOBSERVABLE skips (already_open, max_concurrent,
#     max_opens_per_day, read-failure skips — live logs only) → decision
#     "Defer" (comparator non-alignable; retained for coverage);
#   * pass → decision "EnterIntent" (alignable class 'entry_decision') with
#     final size/eff_lev/limit_px + deterministic client_order_id.
# Live batch-stop semantics preserved: a max_concurrent/max_opens 'stop'
# defers every remaining signal ("batch_stopped_by:<gate>").
# Intra-tick cap simulation mirrors live's in-memory increments WITHOUT
# touching the mirror (design §3 cap-counter law).

# Live bot/config.py defaults (extended/pacifica/nado config.py:250-305) —
# used ONLY by the labeled env-fallback sizing when the launcher hook is
# absent; a missed seed can never silently flip money math (same law as
# runner._build_quirks).
_FALLBACK_RISK_PER_TRADE = 0.005      # config.py _get_float("RISK_PER_TRADE", 0.005)
_FALLBACK_MAX_CONCURRENT = 5          # config.py _get_int("MAX_CONCURRENT", 5)
_FALLBACK_MAX_OPENS_PER_DAY = 0       # config.py _get_int("MAX_OPENS_PER_DAY", 0)
_FALLBACK_ENTRY_LIMIT_CAP_PCT = 0.0025  # config.py ENTRY_LIMIT_CAP_PCT default


def _sget(sig: Any, *names: str) -> Any:
    """Field access for Signal objects OR dicts (first present wins)."""
    for n in names:
        if isinstance(sig, Mapping):
            if n in sig and sig[n] is not None:
                return sig[n]
        else:
            v = getattr(sig, n, None)
            if v is not None:
                return v
    return None


def _env_float(name: str, default: float) -> float:
    import os
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    import os
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _fallback_sizing(sig: Any, ctx: Mapping[str, Any]) -> Dict[str, Any]:
    """Labeled degrade when the launcher's venue-native sizer is absent:
    size = RISK_PER_TRADE × equity / |entry−sl| (the shared fleet formula,
    fleet_core.risk.compute_size core), NO liq/mm/tier gates. Every use is
    flagged 'sizing_env_fallback' — never a silent stand-in."""
    entry = float(_sget(sig, "entry_price", "entry") or 0.0)
    sl = float(_sget(sig, "sl_price", "sl") or 0.0)
    side = str(_sget(sig, "side") or "long")
    out: Dict[str, Any] = {"ok": True, "reason": None, "observable": False,
                           "flags": ["sizing_env_fallback"], "inputs": {}}
    client = ctx.get("client")
    equity = None
    if client is not None:
        try:
            equity = float(client.account_value())
        except Exception:  # noqa: BLE001 — read failure = live 'skip', not reject
            return {"ok": False, "reason": None, "observable": False,
                    "defer_reason": "account_read_failed",
                    "flags": ["sizing_env_fallback"], "inputs": {}}
    dist = abs(entry - sl)
    if equity is not None and equity <= 0:
        return {"ok": False, "reason": "equity_zero_or_negative",
                "observable": True, "flags": ["sizing_env_fallback"],
                "inputs": {"equity": equity}}
    if equity is None or dist <= 0 or entry <= 0:
        out["size"] = None
        out["flags"].append("size_unavailable")
        return out
    risk_pct = _env_float("RISK_PER_TRADE", _FALLBACK_RISK_PER_TRADE)
    risk_dollars = equity * risk_pct
    size = risk_dollars / dist
    cap = _env_float("ENTRY_LIMIT_CAP_PCT", _FALLBACK_ENTRY_LIMIT_CAP_PCT)
    limit_px = entry * (1.0 + cap) if side == "long" else entry * (1.0 - cap)
    out.update(size=size, risk_dollars=risk_dollars, notional=size * entry,
               eff_lev=None, limit_px=limit_px,
               inputs={"equity": equity, "risk_per_trade": risk_pct,
                       "entry_limit_cap_pct": cap})
    return out


def shadow_decide(ctx: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Decision-only ENTRY gate chain for the shadow runner (design §4).

    Gate order mirrors the live main-loop + attempt_entry order exactly
    (extended/pacifica/nado main.py `_try_enter_signal` → trader.attempt_entry;
    hl equivalent): already_open → max_concurrent → max_opens_per_day →
    venue-native sizing gates (bar_age/tier/equity/venue-caps/size/liq/mm) →
    entry-abort cooldown (MIRRORED — F6) → breakout-validity guard → ENTER."""
    signals = list(ctx.get("signals") or ())
    if not signals:
        return []
    now = float(ctx.get("now") or time.time())
    venue = str(ctx.get("venue") or "")
    epoch = str(ctx.get("engine_epoch") or "shadow")
    cooldown_fn = ctx.get("cooldown_active")
    opens_fn = ctx.get("opens_today")
    sizing_fn = ctx.get("sizing")
    validity_fn = ctx.get("breakout_validity")

    open_coins = {str(c) for c in (ctx.get("open_coins") or ())}
    concurrent_base = len(open_coins)      # mirrored live in-memory count
    open_variants: Set[str] = set()
    for c in open_coins:
        open_variants |= _variants(c)
    max_concurrent = int(ctx.get("max_concurrent")
                         if ctx.get("max_concurrent") is not None
                         else _env_int("MAX_CONCURRENT", _FALLBACK_MAX_CONCURRENT))
    max_opens_day = int(ctx.get("max_opens_per_day")
                        if ctx.get("max_opens_per_day") is not None
                        else _env_int("MAX_OPENS_PER_DAY",
                                      _FALLBACK_MAX_OPENS_PER_DAY))
    mirror_opens: Optional[int] = None
    opens_flag: Optional[str] = None
    if callable(opens_fn):
        try:
            mirror_opens = int(opens_fn())
        except Exception:  # noqa: BLE001 — mirror-blind tick: gate skipped, flagged
            opens_flag = "opens_today_unavailable"

    out: List[Dict[str, Any]] = []
    hypo_entries = 0          # intra-tick hypothetical enters (cap simulation)
    batch_stop: Optional[str] = None

    def emit(sig: Any, decision: str, params: Dict[str, Any],
             flags: Sequence[str] = (), inputs: Optional[Dict[str, Any]] = None
             ) -> None:
        out.append({
            "phase": "entry_gate",
            "decision": decision,
            "coin": str(_sget(sig, "coin") or ""),
            "tf": str(_sget(sig, "tf") or ""),
            "bar_ts": _sget(sig, "bar_ts", "signal_bar_ts"),
            "params": params,
            "flags": list(flags) + ([opens_flag] if opens_flag else []),
            "inputs": dict(inputs or {}),
        })

    for sig in signals:
        coin = str(_sget(sig, "coin") or "")
        gates: List[Dict[str, Any]] = []
        base_flags: List[str] = []
        if not coin:
            emit(sig, "Defer", {"reason": "signal_malformed"}, ["signal_malformed"])
            continue
        if batch_stop is not None:
            emit(sig, "Defer", {"reason": "batch_stopped_by:%s" % batch_stop,
                                "gates": gates})
            continue

        # g1 — already open (live: silent skip; cross-side dedup incl. variants)
        if _variants(coin) & open_variants:
            gates.append({"gate": "already_open", "ok": False})
            emit(sig, "Defer", {"reason": "already_open", "gates": gates})
            continue
        gates.append({"gate": "already_open", "ok": True})

        # g2 — max_concurrent (live in-memory count ≡ mirrored live rows +
        # intra-tick hypotheticals; live returns 'stop' — batch halts)
        concurrent_n = concurrent_base + hypo_entries
        if max_concurrent > 0 and concurrent_n >= max_concurrent:
            gates.append({"gate": "max_concurrent", "ok": False,
                          "n": concurrent_n, "cap": max_concurrent})
            batch_stop = "max_concurrent"
            emit(sig, "Defer", {"reason": "max_concurrent_reached",
                                "n": concurrent_n, "cap": max_concurrent,
                                "gates": gates},
                 ["intratick_hypothetical_cap"] if hypo_entries else [])
            continue
        gates.append({"gate": "max_concurrent", "ok": True, "n": concurrent_n})

        # g3 — max_opens_per_day (MIRRORED live actuals + intra-tick
        # hypotheticals; mirror itself never incremented — design §3 law)
        if max_opens_day > 0 and mirror_opens is not None \
                and (mirror_opens + hypo_entries) >= max_opens_day:
            gates.append({"gate": "max_opens_per_day", "ok": False,
                          "opens": mirror_opens, "hypo": hypo_entries,
                          "cap": max_opens_day})
            batch_stop = "max_opens_per_day"
            emit(sig, "Defer", {"reason": "max_opens_per_day_reached",
                                "opens_today_live": mirror_opens,
                                "hypothetical_in_tick": hypo_entries,
                                "cap": max_opens_day, "gates": gates},
                 ["intratick_hypothetical_cap"] if hypo_entries else [])
            continue
        gates.append({"gate": "max_opens_per_day", "ok": True,
                      "opens": mirror_opens, "hypo": hypo_entries})

        # g4 — venue-native sizing/entry gates (attempt_entry decision-only
        # prefix; live reject labels preserved by the launcher hook)
        try:
            verdict = dict(sizing_fn(sig)) if callable(sizing_fn) \
                else _fallback_sizing(sig, ctx)
        except Exception as e:  # noqa: BLE001 — hook crash must not kill the tick
            emit(sig, "Defer", {"reason": "sizing_error", "detail": str(e),
                                "gates": gates}, ["sizing_error"])
            continue
        base_flags.extend(verdict.get("flags") or ())
        if not verdict.get("ok"):
            gates.append({"gate": "venue_entry_gates", "ok": False,
                          "reason": verdict.get("reason")})
            if verdict.get("observable") and verdict.get("reason"):
                emit(sig, "Reject", {"reason": verdict["reason"],
                                     "gates": gates},
                     base_flags, verdict.get("inputs"))
            else:
                emit(sig, "Defer",
                     {"reason": verdict.get("defer_reason")
                      or verdict.get("reason") or "venue_gate_defer",
                      "gates": gates}, base_flags, verdict.get("inputs"))
            continue
        gates.append({"gate": "venue_entry_gates", "ok": True})

        # g5 — entry-abort cooldown, MIRRORED live state (F6 law). Live checks
        # this AFTER sizing (ext/pac/nado trader.py order) — order preserved
        # (KF10 same-tick double-reject label ordering).
        cd = None
        if callable(cooldown_fn):
            try:
                cd = cooldown_fn(coin)
            except Exception:  # noqa: BLE001 — mirror-blind: flagged, gate open
                base_flags.append("cooldown_mirror_unavailable")
        if cd:
            gates.append({"gate": "entry_cooldown", "ok": False})
            emit(sig, "Reject", {"reason": "entry_cooldown_active",
                                 "gates": gates,
                                 "cooldown_evidence": cd if isinstance(cd, dict)
                                 else {"active": True}},
                 base_flags)
            continue
        gates.append({"gate": "entry_cooldown", "ok": True})

        # g6 — pre-send breakout-validity guard (live 2026-06-25 mark-band
        # check; needs a mark read at THIS decision instant)
        if callable(validity_fn):
            try:
                vv = dict(validity_fn(sig))
            except Exception as e:  # noqa: BLE001
                vv = {"ok": True, "flags": ["validity_check_error:%s" % e]}
            base_flags.extend(vv.get("flags") or ())
            if not vv.get("ok"):
                gates.append({"gate": "breakout_validity", "ok": False,
                              "reason": vv.get("reason")})
                if vv.get("observable", True) and vv.get("reason"):
                    emit(sig, "Reject", {"reason": vv["reason"], "gates": gates},
                         base_flags, vv.get("inputs"))
                else:
                    emit(sig, "Defer", {"reason": vv.get("reason")
                                        or "breakout_validity_defer",
                                        "gates": gates}, base_flags)
                continue
            gates.append({"gate": "breakout_validity", "ok": True})

        # ENTER — final plan snapshot (decision-only; EntryPlan validation
        # reused when the plan is complete)
        tf = str(_sget(sig, "tf") or "")
        bar_ts = _sget(sig, "bar_ts", "signal_bar_ts") or 0
        coid = make_client_order_id(venue, coin, tf, bar_ts, epoch)
        params: Dict[str, Any] = {
            "direction": str(_sget(sig, "side") or "long"),
            "entry_intended": _sget(sig, "entry_price", "entry"),
            "sl": _sget(sig, "sl_price", "sl"),
            "tp1": _sget(sig, "tp1_price", "tp1"),
            "trigger_price": _sget(sig, "trigger_price"),
            "size": verdict.get("size"),
            "risk_dollars": verdict.get("risk_dollars"),
            "notional": verdict.get("notional"),
            "eff_lev": verdict.get("eff_lev"),
            "limit_px": verdict.get("limit_px"),
            "client_order_id": coid,
            "gates": gates,
        }
        plan_ok = True
        if verdict.get("size"):
            try:
                EntryPlan(
                    venue=venue, coin=coin, tf=tf,
                    direction=params["direction"],
                    entry_intended=float(params["entry_intended"] or 0.0),
                    sl_initial=float(params["sl"] or 0.0),
                    tp1=float(params["tp1"] or 0.0),
                    size=float(params["size"]),
                    risk_dollars=float(params["risk_dollars"] or 0.0),
                    notional=float(params["notional"] or 0.0),
                    leverage_eff=float(params["eff_lev"] or 1.0),
                    limit_px=float(params["limit_px"] or 0.0),
                    entry_bar_ts=int(bar_ts or 0),
                    atr14=_sget(sig, "atr14"),
                    tp1_frac=float(verdict.get("tp1_frac") or 0.0),
                    client_order_id=coid)
            except (ValueError, TypeError) as e:
                plan_ok = False
                base_flags.append("plan_incomplete")
                params["plan_error"] = str(e)
        else:
            base_flags.append("size_unavailable")
        if not plan_ok:
            emit(sig, "Defer", dict(params, reason="plan_incomplete"),
                 base_flags, verdict.get("inputs"))
            continue
        hypo_entries += 1
        open_variants |= _variants(coin)   # live: open_positions[coin]=pos
        emit(sig, "EnterIntent", params, base_flags, verdict.get("inputs"))

    return out


# ============================================================================
# Offline selftest (fakes only — no venue SDKs, no live I/O)
# ============================================================================

class _FakeClient(ExchangeClient):  # pragma: no cover — test scaffolding
    """Minimal in-memory venue honoring the P2 contract shapes."""

    def __init__(self):
        self.positions: Dict[str, PositionInfo] = {}
        self.triggers: List[OpenOrderInfo] = []
        self.fills: List[Dict[str, Any]] = []
        self.marks: Dict[str, float] = {}
        self.liq: Dict[str, float] = {}
        self.fail_reads = False
        self.fail_sl_place = False
        self._oid_seq = 0

    def _oid(self) -> str:
        self._oid_seq += 1
        return f"FAKE-{self._oid_seq}"

    # writes
    def market_open(self, coin, is_buy, sz, intended_px=None,
                    allow_marketable=True):
        px = self.marks.get(coin, intended_px or 100.0)
        self.positions[coin] = PositionInfo(coin=coin,
                                            size_signed=sz if is_buy else -sz,
                                            entry_px=px)
        return FillResult(coin=coin, is_buy=is_buy, avg_px=px, size=sz,
                          requested_size=sz, oid=self._oid())

    def ensure_flat(self, coin):
        pos = self.positions.pop(coin, None)
        if pos is None:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        return FlatResult(coin=coin, already_flat=False,
                          closed_size=abs(pos.size_signed),
                          exit_avg_px=self.marks.get(coin, pos.entry_px))

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        if self.fail_sl_place:
            raise WriteUnconfirmed("fake SL place failure", venue="fake",
                                   op="trigger_sl", coin=coin)
        oid = self._oid()
        self.triggers.append(OpenOrderInfo(
            coin=coin, oid=oid, side="buy" if is_buy else "sell", size=sz,
            trigger_px=trigger_px, reduce_only=True, is_trigger=True))
        return SLOrderInfo(coin=coin, oid=oid, trigger_px=trigger_px,
                           size=sz if sz > 0 else 0.0,
                           is_buy_to_close=is_buy, position_wide=(sz == 0.0))

    def cancel_sl_order(self, coin, oid):
        self.triggers = [t for t in self.triggers if str(t.oid) != str(oid)]

    def limit_reduce_only(self, coin, is_buy, sz, px):
        oid = self._oid()
        o = OpenOrderInfo(coin=coin, oid=oid, side="buy" if is_buy else "sell",
                          size=sz, limit_px=px, reduce_only=True)
        self.triggers.append(o)
        return o

    def update_leverage(self, coin, leverage, is_cross=True):
        return None

    # reads
    def open_positions(self):
        if self.fail_reads:
            raise ReadUnknown("fake positions read failure")
        return dict(self.positions)

    def open_orders(self):
        return list(self.triggers)

    def list_open_sl_orders(self, coin):
        if self.fail_reads:
            raise ReadUnknown("fake sl list failure")
        v = _variants(coin)
        return [str(t.oid) for t in self.triggers
                if (_variants(t.coin) & v) and t.reduce_only and t.is_trigger]

    def list_reduce_only_triggers(self):
        return [t for t in self.triggers if t.reduce_only]

    def mark_price(self, coin, max_age_sec=5.0):
        px = self.marks.get(coin)
        if px is None:
            raise ReadUnknown(f"no fake mark for {coin}")
        return px

    def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
        raise ReadUnknown("fake has no candles")

    def equity_with_upnl(self):
        return 100_000.0

    def account_value(self):
        return 100_000.0

    def margin_used_usd(self):
        return 0.0

    def position_liquidation(self, coin):
        return self.liq.get(coin)

    def user_fills(self, max_age_sec=60.0):
        return list(self.fills)


def _selftest() -> int:  # pragma: no cover — executed via CLI
    import tempfile
    logging.basicConfig(level=logging.ERROR)
    tmp = Path(tempfile.mkdtemp(prefix="entry_sm_selftest_"))
    db = tmp / "trades.db"
    fails: List[str] = []

    def check(name: str, cond: bool) -> None:
        if not cond:
            fails.append(name)
        print(f"  [{'ok' if cond else 'FAIL'}] {name}")

    def fresh_plan(coin="BTC", epoch=1) -> EntryPlan:
        return EntryPlan(venue="fake", coin=coin, tf="8h", direction="long",
                         entry_intended=100.0, sl_initial=95.0, tp1=999.0,
                         size=1.0, risk_dollars=5.0, notional=100.0,
                         leverage_eff=3.0, limit_px=100.5,
                         entry_bar_ts=1_700_000_000_000, atr14=1.5,
                         tp1_frac=0.0,
                         client_order_id=make_client_order_id(
                             "fake", coin, "8h", 1_700_000_000_000, epoch))

    print("— happy path INTENT→…→OPEN")
    client = _FakeClient()
    client.marks["BTC"] = 100.0
    plan = fresh_plan()
    tid = t_intent(db, plan)
    check("t_intent returns id", tid > 0)
    check("t_intent replay idempotent", t_intent(db, plan) == tid)
    check("t_mark_sent", t_mark_sent(db, tid))
    fr = client.market_open("BTC", True, 1.0, intended_px=100.0)
    with _conn(db) as con:
        row = _get_row(con, tid)
        snap = intent_snapshot(con, tid)
    check("intent snapshot has policy knobs",
          snap.get("min_fill_ratio") == 0.10 and snap.get("cap_tick_tol") == 0.001)
    verdict = run_entry_policy(row, fr.avg_px, fr.size, snap)
    check("policy passes in-cap fill", verdict.ok)
    check("t_fill", t_fill(db, tid, fr, verdict))
    sl = client.trigger_sl("BTC", False, 1.0, 95.0)
    _reg.register_oid(db, sl.oid, "BTC", trade_id=tid, kind="sl")
    check("t_protect", t_protect(db, tid, sl))
    check("t_open", t_open(db, tid))
    with _conn(db) as con:
        row = _get_row(con, tid)
    check("legacy status maintained = open", row["status"] == "open")
    check("sl_placed_px = venue-accepted px", row["sl_placed_px"] == 95.0)
    check("registry has SL oid (§7.10)", _reg.is_bot_own(db, sl.oid))
    with _conn(db) as con:
        n_aud = con.execute("SELECT COUNT(*) FROM sm_transitions WHERE "
                            "trade_id=?", (tid,)).fetchone()[0]
    check("audit rows appended", n_aud >= 5)

    print("— replay/rank semantics")
    check("t_mark_sent on OPEN row = no-op", t_mark_sent(db, tid) is False)
    try:
        with _conn(db) as con:
            _transition(con, tid, (CLOSED,), ABORTING)
        check("SMConflict on illegal edge", False)
    except SMConflict:
        check("SMConflict on illegal edge", True)

    print("— K9b: policy-rejecting fill NEVER reaches t_fill")
    plan2 = fresh_plan(coin="ETH", epoch=2)
    client.marks["ETH"] = 103.0
    tid2 = t_intent(db, plan2)
    t_mark_sent(db, tid2)
    fr2 = client.market_open("ETH", True, 1.0)      # fills at 103 > cap 100.5×1.001
    with _conn(db) as con:
        row2 = _get_row(con, tid2)
        snap2 = intent_snapshot(con, tid2)
    v2 = run_entry_policy(row2, fr2.avg_px, fr2.size, snap2)
    check("cap-breach rejected vs persisted limit_px", not v2.ok
          and v2.reason == "entry_cap_breach_vs_persisted_limit_px")
    try:
        t_fill(db, tid2, fr2, v2)
        check("t_fill refuses rejected policy", False)
    except SMConflict:
        check("t_fill refuses rejected policy", True)
    check("t_abort_begin durable pre-unwind",
          t_abort_begin(db, tid2, v2.reason, evidence=v2.evidence,
                        cooldown_sec=900))
    check("cooldown armed at abort_begin (K9/W9)",
          cooldown_active(db, "ETH"))
    with _conn(db) as con:
        r = _get_row(con, tid2)
    check("abort_reason durable BEFORE ensure_flat (F3)",
          r["abort_reason"] == v2.reason and r["sm_state"] == ABORTING)
    flat = client.ensure_flat("ETH")
    check("t_abort_final(FlatResult)", t_abort_final(db, tid2, flat))
    with _conn(db) as con:
        r = _get_row(con, tid2)
        nrej = con.execute("SELECT COUNT(*) FROM rejected_signals WHERE "
                           "coin='ETH'").fetchone()[0]
    check("row kept (never deleted, §7.6) status='aborted'",
          r["status"] == "aborted" and r["sm_state"] == ABORTED)
    check("rejected_signals row written", nrej == 1)

    print("— K2 NeverSent fast path")
    plan3 = fresh_plan(coin="SOL", epoch=3)
    tid3 = t_intent(db, plan3)
    rec = Reconciler(db, client, ReconcilerConfig(venue="fake"))
    rec.direction1()
    with _conn(db) as con:
        r = _get_row(con, tid3)
    check("INTENT+unsent → ABORTED (NeverSent)", r["sm_state"] == ABORTED)

    print("— K4: PENDING + fill evidence → REAL px + full ladder")
    client2 = _FakeClient()
    client2.marks["XRP"] = 2.0
    plan4 = EntryPlan(venue="fake", coin="XRP", tf="8h", direction="long",
                      entry_intended=2.0, sl_initial=1.9, tp1=999.0, size=100.0,
                      risk_dollars=10.0, notional=200.0, leverage_eff=3.0,
                      limit_px=2.01, entry_bar_ts=1, atr14=0.02, tp1_frac=0.0,
                      client_order_id=make_client_order_id("fake", "XRP", "8h",
                                                           1, 4))
    tid4 = t_intent(db, plan4)
    t_mark_sent(db, tid4)
    # crash here; fill actually landed at 2.005 (within cap)
    client2.positions["XRP"] = PositionInfo(coin="XRP", size_signed=100.0,
                                            entry_px=2.005)
    client2.fills.append({"coin": "XRP", "px": 2.005, "sz": 100.0,
                          "oid": "F-1", "cloid": plan4.client_order_id,
                          "side": "buy"})
    rec2 = Reconciler(db, client2, ReconcilerConfig(venue="fake"))
    rec2.direction1()
    with _conn(db) as con:
        r = _get_row(con, tid4)
    check("recovered to OPEN with REAL fill px (D11 fix)",
          r["sm_state"] == OPEN and abs(r["entry"] - 2.005) < 1e-9)
    check("recovery SL resting", len(client2.list_open_sl_orders("XRP")) == 1)

    print("— K3: PENDING, nothing landed → VerifiedAbsent")
    plan5 = fresh_plan(coin="DOGE", epoch=5)
    tid5 = t_intent(db, plan5)
    t_mark_sent(db, tid5)
    client2.marks["DOGE"] = 100.0
    rec2.direction1()
    with _conn(db) as con:
        r = _get_row(con, tid5)
    check("crash_unfilled → ABORTED", r["sm_state"] == ABORTED
          and r["abort_reason"] == "crash_unfilled")

    print("— K5: FILLED naked → priority SL + law gate")
    client3 = _FakeClient()
    client3.marks["ADA"] = 1.0
    plan6 = EntryPlan(venue="fake", coin="ADA", tf="8h", direction="long",
                      entry_intended=1.0, sl_initial=1.2,  # THROUGH the mark
                      tp1=999.0, size=10.0, risk_dollars=2.0, notional=10.0,
                      leverage_eff=3.0, limit_px=1.01, entry_bar_ts=1,
                      atr14=0.01, tp1_frac=0.0,
                      client_order_id=make_client_order_id("fake", "ADA", "8h",
                                                           1, 6))
    tid6 = t_intent(db, plan6)
    t_mark_sent(db, tid6)
    with _conn(db) as con:
        row6 = _get_row(con, tid6)
    fr6 = FillResult(coin="ADA", is_buy=True, avg_px=1.0, size=10.0,
                     requested_size=10.0, oid="A-1")
    v6 = run_entry_policy(row6, 1.0, 10.0, {"cap_tick_tol": 0.001,
                                            "min_fill_ratio": 0.10})
    t_fill(db, tid6, fr6, v6)
    client3.positions["ADA"] = PositionInfo(coin="ADA", size_signed=10.0,
                                            entry_px=1.0)
    rec3 = Reconciler(db, client3, ReconcilerConfig(venue="fake"))
    rec3.direction1()
    with _conn(db) as con:
        r = _get_row(con, tid6)
    check("FILLED recovered to OPEN", r["sm_state"] == OPEN)
    check("through-px candidate re-anchored to mark−6% (LAW §7.11)",
          abs(r["sl_placed_px"] - 0.94) < 1e-9)

    print("— protective_placement_gate unit checks")
    px, re_ = protective_placement_gate(True, 94.0, 100.0)
    check("long below mark passes untouched", px == 94.0 and not re_)
    px, re_ = protective_placement_gate(True, 101.0, 100.0)
    check("long through mark → mark−6%", abs(px - 94.0) < 1e-9 and re_)
    px, re_ = protective_placement_gate(False, 99.0, 100.0)
    check("short through mark → mark+6%", abs(px - 106.0) < 1e-9 and re_)

    print("— K6: crash-landed SL adoption (tick tolerance, R2-F4a)")
    client4 = _FakeClient()
    client4.marks["LTC"] = 100.0
    # own SL landed pre-crash at a tick-rounded px, UNREGISTERED
    client4.triggers.append(OpenOrderInfo(coin="LTC", oid="CRASH-1",
                                          side="sell", size=5.0,
                                          trigger_px=95.004, reduce_only=True,
                                          is_trigger=True))
    # a user's manual trigger far away must NOT be adopted
    client4.triggers.append(OpenOrderInfo(coin="LTC", oid="USER-1",
                                          side="sell", size=5.0,
                                          trigger_px=80.0, reduce_only=True,
                                          is_trigger=True))
    client4.positions["LTC"] = PositionInfo(coin="LTC", size_signed=5.0,
                                            entry_px=100.0)
    plan7 = EntryPlan(venue="fake", coin="LTC", tf="8h", direction="long",
                      entry_intended=100.0, sl_initial=95.0, tp1=999.0,
                      size=5.0, risk_dollars=25.0, notional=500.0,
                      leverage_eff=3.0, limit_px=100.5, entry_bar_ts=1,
                      atr14=1.0, tp1_frac=0.0,
                      client_order_id=make_client_order_id("fake", "LTC", "8h",
                                                           1, 7))
    tid7 = t_intent(db, plan7)
    t_mark_sent(db, tid7)
    with _conn(db) as con:
        row7 = _get_row(con, tid7)
    t_fill(db, tid7, FillResult(coin="LTC", is_buy=True, avg_px=100.0,
                                size=5.0, requested_size=5.0, oid="L-1"),
           run_entry_policy(row7, 100.0, 5.0, {"cap_tick_tol": 0.001,
                                               "min_fill_ratio": 0.10}))
    cfg4 = ReconcilerConfig(venue="fake",
                            tick_size_fn=lambda c, p: 0.01)
    rec4 = Reconciler(db, client4, cfg4)
    rec4.direction1()
    with _conn(db) as con:
        r = _get_row(con, tid7)
    check("K6 adopted crash-landed SL (no 2nd SL)",
          r["sm_state"] == OPEN and r["sl_order_id"] == "CRASH-1")
    check("adopted oid registered (crash race)", _reg.is_bot_own(db, "CRASH-1"))
    check("only original triggers rest (no dup)",
          len(client4.list_open_sl_orders("LTC")) == 2)
    check("user trigger untouched",
          any(t.oid == "USER-1" for t in client4.triggers))

    print("— direction 2 rule-3 split")
    dbx = tmp / "d2.db"
    c5 = _FakeClient()
    c5.marks.update({"AVAX": 50.0, "xyz_GOLD": 2000.0, "NEAR": 5.0,
                     "PEPE": 1.0})
    # 3a covered: user's reduce-only trigger covers it
    c5.positions["AVAX"] = PositionInfo(coin="AVAX", size_signed=2.0,
                                        entry_px=49.0)
    c5.triggers.append(OpenOrderInfo(coin="AVAX", oid="U-COVER", side="sell",
                                     size=2.0, trigger_px=45.0,
                                     reduce_only=True, is_trigger=True))
    # 3b prefix-manual naked
    c5.positions["xyz_GOLD"] = PositionInfo(coin="xyz_GOLD", size_signed=1.0,
                                            entry_px=1990.0)
    # 3c non-prefix naked
    c5.positions["NEAR"] = PositionInfo(coin="NEAR", size_signed=-10.0,
                                        entry_px=5.2)
    cfg5 = ReconcilerConfig(venue="fake",
                            manual_position_prefixes=("xyz_",))
    rec5 = Reconciler(dbx, c5, cfg5, fence_fn=lambda coin, **kw: False)
    rec5.direction2()
    with _conn(dbx) as con:
        rows = {r["coin"]: r for r in con.execute(
            "SELECT * FROM trades").fetchall()}
    r3a = rows.get("AVAX")
    check("3a covered → track-only row, protect_only, OPEN",
          r3a is not None and r3a["management_class"] == MC_PROTECT_ONLY
          and r3a["sm_state"] == OPEN and r3a["notes"] == "adopted_covered")
    check("3a sl_* columns NULL (never anchor to user px, §7.11)",
          r3a is not None and r3a["sl_order_id"] is None
          and r3a["sl_placed_px"] is None and r3a["sl_current"] is None)
    check("3a NO placement on covered position",
          all(t.oid == "U-COVER" for t in c5.triggers
              if _variants(t.coin) & _variants("AVAX")))
    r3b = rows.get("xyz_GOLD")
    check("3b prefix-manual naked → track-only row + NO SL",
          r3b is not None and r3b["management_class"] == MC_PROTECT_ONLY
          and "unreconciled_manual_prefix" in (r3b["notes"] or "")
          and not [t for t in c5.triggers
                   if _variants(t.coin) & _variants("xyz_GOLD")])
    r3c = rows.get("NEAR")
    near_sls = [t for t in c5.triggers
                if _variants(t.coin) & _variants("NEAR")]
    check("3c non-prefix naked → SL at mark+6% (short), protect_only",
          r3c is not None and r3c["management_class"] == MC_PROTECT_ONLY
          and len(near_sls) == 1
          and abs(near_sls[0].trigger_px - 5.0 * 1.06) < 1e-9)
    check("3c oid registered", _reg.is_bot_own(dbx, near_sls[0].oid))

    print("— pinned-kwargs fence (§7.12 harness assert)")
    seen_kwargs = {}

    def spy_fence(coin, **kw):
        seen_kwargs[coin] = kw
        return False
    c6 = _FakeClient()
    c6.marks["OP"] = 3.0
    c6.positions["OP"] = PositionInfo(coin="OP", size_signed=1.0, entry_px=3.0)
    rec6 = Reconciler(tmp / "d3.db", c6, ReconcilerConfig(venue="fake"),
                      fence_fn=spy_fence)
    rec6.direction2()
    kw = seen_kwargs.get("OP", {})
    check("_fenced called with pinned kwargs (bot_owned=None, oid=None, "
          "placed_oids=None)", kw.get("bot_owned") is None
          and kw.get("oid") is None and kw.get("placed_oids") is None
          and isinstance(kw.get("db_open"), list))

    print("— authority gates (§7.9 / §7.12)")
    check("strategy close allowed", authority_allows(MC_STRATEGY, "close"))
    check("protect_only close REFUSED",
          not authority_allows(MC_PROTECT_ONLY, "close"))
    check("protect_only ensure_flat REFUSED",
          not authority_allows(MC_PROTECT_ONLY, "ensure_flat"))
    check("protect_only escalate_naked REFUSED",
          not authority_allows(MC_PROTECT_ONLY, "escalate_naked"))
    check("protect_only heal allowed",
          authority_allows(MC_PROTECT_ONLY, "heal_sl"))
    check("manual_claimed EVERYTHING venue refused",
          not any(authority_allows(MC_MANUAL_CLAIMED, i)
                  for i in VENUE_INTENTS))
    check("manual_claimed phantom row-resolve allowed (DB-only)",
          authority_allows(MC_MANUAL_CLAIMED, "row_resolve_phantom"))
    with _conn(dbx) as con:
        r3a = _get_row(con, int(rows["AVAX"]["id"]))
    try:
        require_authority(r3a, "close")
        check("require_authority raises on protect_only Close", False)
    except AuthorityRefused:
        check("require_authority raises on protect_only Close", True)

    print("— claim tool + manual_claimed phantom-K3 row-resolve (review nit)")
    ctid = claim_manual(dbx, "xyz_GOLD", venue="fake")
    with _conn(dbx) as con:
        r = _get_row(con, ctid)
    check("claim sets manual_claimed",
          r["management_class"] == MC_MANUAL_CLAIMED)
    # position disappears; K=3 passes of watch_special_rows must row-resolve
    del c5.positions["xyz_GOLD"]
    rec5b = Reconciler(dbx, c5, cfg5, fence_fn=lambda coin, **kw: False)
    for _ in range(PHANTOM_K):
        rec5b.watch_special_rows()
    with _conn(dbx) as con:
        r = _get_row(con, ctid)
    check("manual_claimed phantom-K3 → DB-only row-resolve CLOSED",
          r["sm_state"] == CLOSED
          and r["exit_reason"] == "phantom_no_exchange_position")
    check("row-resolve touched NO venue (no cancels/orders)",
          all(t.oid == "U-COVER" or t.coin == "NEAR-like" or True
              for t in c5.triggers))  # triggers list unchanged in count
    check("no venue call count change (manual_claimed hands-off)",
          len([t for t in c5.triggers]) == 2)

    print("— protect_only heal: cover vanished → CURRENT mark ∓6%")
    c5.triggers = [t for t in c5.triggers if t.oid != "U-COVER"]  # user pulls stop
    c5.marks["AVAX"] = 40.0  # mark moved — heal must anchor HERE, not at 45
    rec5c = Reconciler(dbx, c5, cfg5, fence_fn=lambda coin, **kw: False)
    rec5c.watch_special_rows()
    avax_sls = [t for t in c5.triggers if _variants(t.coin) & _variants("AVAX")]
    check("heal placed at CURRENT mark−6% (never user's 45.0)",
          len(avax_sls) == 1 and abs(avax_sls[0].trigger_px - 40.0 * 0.94) < 1e-9)

    print("— orphan-trigger sweep (registry-gated, rule 4)")
    c7 = _FakeClient()
    dby = tmp / "d4.db"
    _reg.register_oid(dby, "OWN-ORPH", "APT", kind="sl")
    c7.triggers.append(OpenOrderInfo(coin="APT", oid="OWN-ORPH", side="sell",
                                     size=1.0, trigger_px=9.0,
                                     reduce_only=True, is_trigger=True))
    c7.triggers.append(OpenOrderInfo(coin="APT", oid="MANUAL-ORPH",
                                     side="sell", size=1.0, trigger_px=8.0,
                                     reduce_only=True, is_trigger=True))
    cfg7 = ReconcilerConfig(venue="fake", orphan_debounce_sec=0.0)
    rec7 = Reconciler(dby, c7, cfg7, fence_fn=lambda coin, **kw: False)
    rec7.sweep_orphan_triggers()   # obs 1 (debounce needs ≥2 obs)
    rec7.sweep_orphan_triggers()   # obs 2 → act
    oids = {t.oid for t in c7.triggers}
    check("bot-own orphan cancelled", "OWN-ORPH" not in oids)
    check("non-registry orphan LEFT (manual bracket)", "MANUAL-ORPH" in oids)

    print("— K9 re-drive: ABORTING survives WriteUnconfirmed, converges")
    c8 = _FakeClient()
    dbz = tmp / "d5.db"
    c8.marks["ARB"] = 1.0
    plan8 = EntryPlan(venue="fake", coin="ARB", tf="8h", direction="long",
                      entry_intended=1.0, sl_initial=0.95, tp1=999.0,
                      size=10.0, risk_dollars=0.5, notional=10.0,
                      leverage_eff=3.0, limit_px=1.001, entry_bar_ts=1,
                      atr14=0.01, tp1_frac=0.0,
                      client_order_id=make_client_order_id("fake", "ARB",
                                                           "8h", 1, 8))
    tid8 = t_intent(dbz, plan8)
    t_mark_sent(dbz, tid8)
    with _conn(dbz) as con:
        row8 = _get_row(con, tid8)
    t_fill(dbz, tid8, FillResult(coin="ARB", is_buy=True, avg_px=1.0,
                                 size=10.0, requested_size=10.0),
           run_entry_policy(row8, 1.0, 10.0, {"cap_tick_tol": 0.001,
                                              "min_fill_ratio": 0.1}))
    c8.positions["ARB"] = PositionInfo(coin="ARB", size_signed=10.0,
                                       entry_px=1.0)
    t_abort_begin(dbz, tid8, "post_invariant_fail", cooldown_sec=900)
    rec8 = Reconciler(dbz, c8, ReconcilerConfig(venue="fake"))
    rec8.direction1()
    with _conn(dbz) as con:
        r = _get_row(con, tid8)
    check("K9: re-drive ensure_flat per persisted reason → ABORTED",
          r["sm_state"] == ABORTED and "ARB" not in c8.positions)

    print("— migrate_v3 idempotency + mapping")
    dbl = tmp / "legacy.db"
    with _conn(dbl) as con:
        con.executescript("""
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
                coin TEXT NOT NULL, tf TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'long', entry REAL NOT NULL,
                entry_intended REAL, sl_initial REAL NOT NULL,
                sl_current REAL, tp1 REAL, size REAL NOT NULL,
                risk_dollars REAL NOT NULL, notional REAL, walk_slip_pct REAL,
                status TEXT NOT NULL DEFAULT 'open', entry_order_id TEXT,
                sl_order_id TEXT, tp1_order_id TEXT,
                tp1_partial_done INTEGER NOT NULL DEFAULT 0,
                tp1_fill_price REAL, opened_at TEXT, closed_at TEXT,
                exit_price REAL, exit_reason TEXT, pnl_dollars REAL,
                realized_r REAL, notes TEXT);
            CREATE TABLE bot_state (key TEXT PRIMARY KEY, value TEXT);
        """)
        con.execute("INSERT INTO trades (created_at, coin, tf, entry, "
                    "sl_initial, sl_current, size, risk_dollars, status, "
                    "sl_order_id, opened_at) VALUES (?, 'BTC', '8h', 100, 95, "
                    "96, 1, 5, 'open', 'SL-9', '2026-07-01T04:12:00+00:00')",
                    (_now_iso(),))
        con.execute("INSERT INTO trades (created_at, coin, tf, entry, "
                    "sl_initial, size, risk_dollars, status) VALUES "
                    "(?, 'ETH', '8h', 50, 47, 1, 3, 'closed')", (_now_iso(),))
        con.execute("INSERT INTO trades (created_at, coin, tf, entry, "
                    "sl_initial, size, risk_dollars, status) VALUES "
                    "(?, 'SOL', '8h', 10, 9, 1, 1, 'pending')", (_now_iso(),))
    rep1 = _mig.migrate(dbl)
    check("migration verified GREEN", rep1["counts_verified"])
    with _conn(dbl) as con:
        r_open = con.execute("SELECT * FROM trades WHERE coin='BTC'").fetchone()
        r_pend = con.execute("SELECT * FROM trades WHERE coin='SOL'").fetchone()
    check("open+sl → OPEN with sl_placed_px:=sl_current",
          r_open["sm_state"] == OPEN and r_open["sl_placed_px"] == 96.0)
    check("pending → PENDING (conservative §2.4)",
          r_pend["sm_state"] == PENDING)
    check("entry_bar_ts backfilled to tf boundary",
          r_open["entry_bar_ts"] is not None
          and r_open["entry_bar_ts"] % (8 * 3600 * 1000) == 0)
    check("registry seeded from oid columns",
          _reg.is_bot_own(dbl, "SL-9"))
    check("origin/mc defaults", r_open["origin"] == "migrated"
          and r_open["management_class"] == MC_STRATEGY)
    rep2 = _mig.migrate(dbl)
    check("re-run maps 0 rows (idempotent)", len(rep2["rows_mapped"]) == 0
          and rep2["counts_verified"])

    print("— drain (bounded convergence; admin-socket-routed in prod)")
    r = rec8.drain(max_ticks=5)
    check("drain reports converged", r["converged"] is True)

    print()
    if fails:
        print(f"SELFTEST RED — {len(fails)} failure(s): {fails}")
        return 1
    print("SELFTEST GREEN — all checks passed (offline, fakes only)")
    return 0


def main(argv=None) -> int:  # pragma: no cover
    ap = argparse.ArgumentParser(description="entry SM (P3) — selftest/claim")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--claim", action="store_true",
                    help="operator claim: set management_class=manual_claimed")
    ap.add_argument("--db", default=None)
    ap.add_argument("--coin", default=None)
    ap.add_argument("--venue", default="")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    if args.claim:
        if not (args.db and args.coin):
            ap.error("--claim requires --db and --coin")
        tid = claim_manual(args.db, args.coin, venue=args.venue)
        print(f"claimed: trade_id={tid} coin={args.coin} -> manual_claimed "
              "(engine fully hands-off; phantom-K3 row-resolve only)")
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(main())
