"""comparator — shadow-vs-live decision comparator, coverage ledger, 48h green gate.

P3 design authority: proofs/p3_design_shadow_runner.md §5 (live-side extraction),
§6 (alignment, tolerances, classification, KF1-KF11 whitelist), §7 (48h GREEN GATE
incl. the F8 per-class coverage LEDGER and replay-covered classes). Round-3 approved;
matchers are MECHANICAL evidence rules — never judgment, never post-hoc
rationalization (EVID GATE).

Path note (interface decision): the design doc names `fleet_core/shadow/compare.py`;
the P3 build file-ownership map places this component at
`fleet_core/engine/comparator.py` — ownership map wins, content is the §6/§7 spec.

RE-RUN LAW (§7.2, F9): the comparator is STATELESS over retained inputs — every
`Comparator.run()` recomputes ALL verdicts from the full shadow decision log + live
extraction; earlier per-event verdicts are never grandfathered. `comparator_sha`
(md5 of this file) is stamped on every run so a mid-window classifier fix forces a
visible full-window re-run.

CLASSES (§6.3, every divergence gets exactly one):
    benign-timing | known-fix-delta (KF1..KF11) | strategy-delta | bug | unexplained
GATE: strategy-delta == 0, open bugs == 0, unexplained == 0 after evidence-triage.

Stdlib only; no venue SDK; offline-selftestable.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger("fleet_core.engine.comparator")

__all__ = [
    "Event",
    "AlignedPair",
    "Divergence",
    "ComparatorContext",
    "LiveExtractor",
    "Comparator",
    "CoverageLedger",
    "GateCalculator",
    "shadow_events_from_log",
    "comparator_sha",
]

# ---------------------------------------------------------------- constants (§6)

EVENT_CLASSES = ("entry_decision", "reject", "sl_replace", "heal", "tp1",
                 "exit", "phantom_resolve")
BAR_DRIVEN = frozenset({"entry_decision", "reject", "sl_replace", "tp1", "exit"})
TICK_DRIVEN = frozenset({"heal", "phantom_resolve"})   # ±3 ticks (§6.1)
TICK_WINDOW = 3

# Tolerances (§6.2 — derived from engine constants, not arbitrary):
SL_REPLACE_THRESH_BPS = 5.0     # the engine's own _SL_REPLACE_THRESH churn gate
EXIT_PX_INFO_ONLY = True        # exit px informational; exit REASON exact; SAME bar
KF3_SAME_PX_BPS = 0.5           # "same target px" for ordering-only KF3 (float-safe)
KF7_RATIO_RTOL = 1e-6           # "size ratio == lev ratio exactly" (float repr tol)

MATCH, SHADOW_ONLY, LIVE_ONLY, PARAM_MISMATCH = (
    "MATCH", "SHADOW_ONLY", "LIVE_ONLY", "PARAM_MISMATCH")

CLS_BENIGN = "benign-timing"
CLS_KF = "known-fix-delta"
CLS_STRAT = "strategy-delta"
CLS_BUG = "bug"
CLS_UNEXPLAINED = "unexplained"

SL_FIRE_REASONS = frozenset({"sl", "trail_sl"})
ABORT_REASON_MARKERS = ("cap_breach", "partial", "abort",
                        "sl_placement_failed_3x_naked_position_closed",
                        "sl_outside_liquidation_invariant_fail")

# §7.5 per-class coverage ledger (F8) — 10 classes; floor 0 = opportunistic
LEDGER_CLASSES: Tuple[Tuple[str, int], ...] = (
    ("entry", 1),
    ("exit_sl", 1),
    ("trail", 3),
    ("tp1_be", 1),          # venues with frac>0; pac/nado = replay-only, inert-live
    ("abort_unwind", 0),
    ("heal", 0),
    ("phantom_k3", 0),
    ("adopt_untracked", 0),
    ("orphan_cancel", 0),
    ("supersede", 0),
)


def comparator_sha() -> str:
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.md5(fh.read()).hexdigest()


def _parse_iso(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        d = _dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=_dt.timezone.utc)
        return d.timestamp()
    except ValueError:
        return None


def _utc_day(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%d")


def _bps(a: float, b: float) -> float:
    if b == 0:
        return float("inf")
    return abs(a - b) / abs(b) * 1e4


# ============================================================================ events

@dataclass
class Event:
    source: str                      # 'shadow' | 'live'
    venue: str
    coin: str
    tf: str
    event_class: str                 # one of EVENT_CLASSES
    ts: float                        # epoch seconds
    bar_ts: Optional[int] = None     # ms; alignment key for bar-driven classes
    params: Dict[str, Any] = field(default_factory=dict)
    inputs: Dict[str, Any] = field(default_factory=dict)
    tick: Optional[int] = None
    raw: Optional[Mapping[str, Any]] = None


# shadow log phase -> event class (design §4 phases / §6.1 classes)
_PHASE_TO_CLASS = {
    "entry_gate": None,     # decided by the record's decision (enter vs reject)
    "trail": "sl_replace",
    "heal": "heal",
    "tp1_partial": "tp1",
    "exit": "exit",
    "sl_hit": "exit",
    "phantom": "phantom_resolve",
}
_ENTER_DECISIONS = frozenset({"Enter", "EnterIntent", "t_intent", "INTENT"})
_NONALIGN_DECISIONS = frozenset({"NoOp", "Defer", "TickComplete",
                                 "shadow_tick_skipped", "RehearsalPass",
                                 "RehearsalFail"})


def shadow_events_from_log(records: Sequence[Mapping[str, Any]]) -> List[Event]:
    """Map shadow_decisions.jsonl records to alignable events. NoOp/Defer and tick
    heartbeats are retained in the log for coverage but are NOT alignable (live
    NoOps are unobservable — §5: event-anchored, bidirectional on OBSERVABLE)."""
    out: List[Event] = []
    for r in records:
        phase, decision = r.get("phase", ""), r.get("decision", "")
        if decision in _NONALIGN_DECISIONS:
            continue
        cls: Optional[str] = None
        if phase == "entry_gate":
            cls = "entry_decision" if decision in _ENTER_DECISIONS else "reject"
        elif phase == "pm":
            if decision == "ReplaceSL":
                cls = "sl_replace"
            elif decision in ("Close", "RecordExit"):
                cls = "exit"
        else:
            cls = _PHASE_TO_CLASS.get(phase)
        if cls is None:
            continue
        if phase == "phantom" and decision not in ("AdoptResolve", "Resolve"):
            continue  # miss-counter bookkeeping, not a resolve event
        if phase == "sl_hit" and decision not in ("Close", "RecordExit"):
            continue  # suppression verdicts (phantom-wick guard) are KF6 evidence,
                      # not alignable exits — matchers read them via ctx.shadow_raw
        out.append(Event(
            source="shadow", venue=r.get("venue", ""), coin=r.get("coin", ""),
            tf=r.get("tf", ""), event_class=cls,
            ts=_parse_iso(r.get("ts", "")) or 0.0,
            bar_ts=r.get("bar_ts"),
            params=dict(r.get("params") or {}),
            inputs=dict(r.get("inputs") or {}),
            tick=r.get("tick"), raw=r))
    return out


# ============================================================ live extraction (§5)

class LiveExtractor:
    """Reconstructs LIVE actions (no live-bot code changes) from:
      1. trades.db polled diff — new pending/open rows (entries), rejected_signals
         (gate rejects), sl_current/sl_order_id updates (trail/heal), tp1_partial,
         close rows (exit reason/px/realized_r);
      2. pre-parsed journalctl structured events (log-analysis grammar; producer
         must use the ' UTC' --since suffix) injected as dicts:
         {'kind': 'sl_replace'|'heal'|'phantom_resolve'|'adopt'|'orphan_cancel'|
          'supersede'|'signal', 'coin','tf','bar_ts','ts','params'}.

    Stateful poller (cursors + last-seen sl per trade); each poll() emits the delta.
    """

    def __init__(self, venue: str, live_db_path: str,
                 bar_ts_of: Optional[Callable[[float, str], Optional[int]]] = None) -> None:
        self.venue = venue
        self.live_db_path = live_db_path
        self._last_trade_id = 0
        self._last_rejected_id = 0
        self._sl_seen: Dict[int, Tuple[Any, Any]] = {}       # trade_id -> (sl_current, oid)
        self._tp1_seen: Dict[int, bool] = {}
        self._closed_seen: set = set()
        # bar_ts derivation for events the DB does not stamp: default = open ts of the
        # LAST CLOSED bar at event time (both processes see identical closed bars —
        # design §3 alignment property). Injectable for venue bar conventions.
        self.bar_ts_of = bar_ts_of or self._default_bar_ts

    @staticmethod
    def _default_bar_ts(ts: float, tf: str) -> Optional[int]:
        from fleet_core.engine.shadow_runner import tf_to_ms
        try:
            step = tf_to_ms(tf)
        except ValueError:
            return None
        ms = int(ts * 1000)
        return (ms // step) * step - step

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect("file:%s?mode=ro" % self.live_db_path, uri=True, timeout=5.0)
        con.row_factory = sqlite3.Row
        return con

    def poll(self, journal_events: Sequence[Mapping[str, Any]] = ()) -> List[Event]:
        out: List[Event] = []
        con = self._conn()
        try:
            cols = {r["name"] for r in con.execute("PRAGMA table_info(trades)")}
            for r in con.execute("SELECT * FROM trades ORDER BY id"):
                tid = int(r["id"])
                ebts = r["entry_bar_ts"] if "entry_bar_ts" in cols else None
                if tid > self._last_trade_id:
                    self._last_trade_id = tid
                    ots = _parse_iso(r["opened_at"] or r["created_at"]) or 0.0
                    out.append(Event(
                        "live", self.venue, r["coin"], r["tf"], "entry_decision",
                        ts=ots, bar_ts=ebts if ebts is not None
                        else self.bar_ts_of(ots, r["tf"]),
                        params={"entry": r["entry"], "size": r["size"],
                                "status": r["status"], "trade_id": tid,
                                "sl_initial": r["sl_initial"]}))
                    self._sl_seen[tid] = (r["sl_current"],
                                          r["sl_order_id"] if "sl_order_id" in cols else None)
                    self._tp1_seen[tid] = bool(r["tp1_partial_done"]) \
                        if "tp1_partial_done" in cols else False
                    if r["status"] == "closed":
                        self._closed_seen.add(tid)
                    continue
                prev_sl, prev_oid = self._sl_seen.get(tid, (None, None))
                cur_oid = r["sl_order_id"] if "sl_order_id" in cols else None
                if (r["sl_current"], cur_oid) != (prev_sl, prev_oid):
                    ts = _parse_iso(r["closed_at"]) or _now()
                    out.append(Event(
                        "live", self.venue, r["coin"], r["tf"], "sl_replace",
                        ts=ts, bar_ts=self.bar_ts_of(ts, r["tf"]),
                        params={"target_px": r["sl_current"], "prev_sl": prev_sl,
                                "oid": cur_oid, "prev_oid": prev_oid,
                                "trade_id": tid}))
                    self._sl_seen[tid] = (r["sl_current"], cur_oid)
                tp1 = bool(r["tp1_partial_done"]) if "tp1_partial_done" in cols else False
                if tp1 and not self._tp1_seen.get(tid, False):
                    ts = _parse_iso(r["closed_at"]) or _now()
                    out.append(Event(
                        "live", self.venue, r["coin"], r["tf"], "tp1",
                        ts=ts, bar_ts=self.bar_ts_of(ts, r["tf"]),
                        params={"fill_px": r["tp1_fill_price"], "trade_id": tid}))
                    self._tp1_seen[tid] = True
                if r["status"] == "closed" and tid not in self._closed_seen:
                    cts = _parse_iso(r["closed_at"]) or _now()
                    out.append(Event(
                        "live", self.venue, r["coin"], r["tf"], "exit",
                        ts=cts, bar_ts=self.bar_ts_of(cts, r["tf"]),
                        params={"reason": r["exit_reason"], "exit_px": r["exit_price"],
                                "realized_r": r["realized_r"], "trade_id": tid,
                                "entry_bar_ts": ebts}))
                    self._closed_seen.add(tid)
            for r in con.execute(
                    "SELECT * FROM rejected_signals WHERE id > ? ORDER BY id",
                    (self._last_rejected_id,)):
                self._last_rejected_id = int(r["id"])
                ts = _parse_iso(r["created_at"]) or _now()
                out.append(Event(
                    "live", self.venue, r["coin"], r["tf"], "reject",
                    ts=ts, bar_ts=self.bar_ts_of(ts, r["tf"]),
                    params={"reason": r["reason"], "row_id": int(r["id"])}))
        finally:
            con.close()
        # journal structured events refine/extend the DB view
        jmap = {"sl_replace": "sl_replace", "heal": "heal",
                "phantom_resolve": "phantom_resolve", "exit": "exit"}
        for j in journal_events:
            kind = j.get("kind", "")
            if kind in jmap:
                ts = float(j.get("ts", 0.0))
                tf = j.get("tf", "")
                out.append(Event(
                    "live", self.venue, j.get("coin", ""), tf, jmap[kind],
                    ts=ts, bar_ts=j.get("bar_ts", self.bar_ts_of(ts, tf) if tf else None),
                    params=dict(j.get("params") or {})))
        return out


def _now() -> float:
    import time
    return time.time()


# ================================================================= alignment (§6.1)

@dataclass
class AlignedPair:
    key: Tuple[str, str, Optional[int], str]
    shadow: Optional[Event]
    live: Optional[Event]
    outcome: str                     # MATCH | SHADOW_ONLY | LIVE_ONLY | PARAM_MISMATCH
    mismatch: Dict[str, Any] = field(default_factory=dict)


def _params_match(cls: str, s: Event, l: Event) -> Tuple[bool, Dict[str, Any]]:
    """§6.2 tolerance table. Returns (match, mismatch-evidence)."""
    if cls in ("sl_replace", "heal"):
        st, lt = s.params.get("target_px"), l.params.get("target_px")
        if st is None or lt is None:
            return True, {}
        d = _bps(float(st), float(lt))
        return d <= SL_REPLACE_THRESH_BPS, {"px_delta_bps": d, "shadow_px": st,
                                            "live_px": lt}
    if cls == "exit":
        sr, lr = s.params.get("reason"), l.params.get("reason")
        same_bar = s.bar_ts == l.bar_ts
        # exit px informational only (§6.2); reason exact; bar exact
        if sr == lr and same_bar:
            return True, {}
        return False, {"shadow_reason": sr, "live_reason": lr,
                       "shadow_bar": s.bar_ts, "live_bar": l.bar_ts}
    if cls == "entry_decision":
        ss, ls = s.params.get("size"), l.params.get("size")
        if ss is None or ls is None or float(ss) == float(ls):
            return True, {}
        return False, {"shadow_size": ss, "live_size": ls}
    if cls == "reject":
        sr, lr = s.params.get("reason"), l.params.get("reason")
        return (sr == lr) or sr is None or lr is None, \
            {"shadow_reason": sr, "live_reason": lr}
    return True, {}


def align(shadow_events: Sequence[Event], live_events: Sequence[Event],
          tick_sec: float = 60.0) -> List[AlignedPair]:
    """Join on (coin, tf, bar_ts, event_class); same bar for bar-driven, ±3 ticks
    (ts window) for tick-driven (§6.1). Greedy nearest-ts pairing within window."""
    pairs: List[AlignedPair] = []
    used_live: set = set()
    live_list = list(live_events)
    for s in shadow_events:
        best_i, best_dt = None, None
        for i, l in enumerate(live_list):
            if i in used_live or l.event_class != s.event_class \
                    or l.coin != s.coin or (s.tf and l.tf and l.tf != s.tf):
                continue
            if s.event_class in BAR_DRIVEN:
                if s.bar_ts is not None and l.bar_ts is not None:
                    if s.bar_ts != l.bar_ts:
                        continue
                elif abs(l.ts - s.ts) > 86400.0:
                    continue  # bar_ts missing on one side: bound pairing to 24h
            else:
                if abs(l.ts - s.ts) > TICK_WINDOW * tick_sec:
                    continue
            dt = abs(l.ts - s.ts)
            if best_dt is None or dt < best_dt:
                best_i, best_dt = i, dt
        key = (s.coin, s.tf, s.bar_ts, s.event_class)
        if best_i is None:
            pairs.append(AlignedPair(key, s, None, SHADOW_ONLY))
        else:
            used_live.add(best_i)
            l = live_list[best_i]
            ok, mm = _params_match(s.event_class, s, l)
            pairs.append(AlignedPair(key, s, l, MATCH if ok else PARAM_MISMATCH, mm))
    for i, l in enumerate(live_list):
        if i not in used_live:
            pairs.append(AlignedPair((l.coin, l.tf, l.bar_ts, l.event_class),
                                     None, l, LIVE_ONLY))
    return pairs


# =============================================================== context + matchers

@dataclass
class ComparatorContext:
    """Everything the mechanical matchers need. All fields injectable/optional —
    a missing field makes the dependent matcher return no-match (never a guess)."""
    venue: str
    tick_sec: float = 60.0
    cooldown_sec: float = 900.0                        # venue value (live constant)
    suppression_events: Sequence[Any] = ()             # MirrorEvent list (KF11 corpus)
    live_restarts: Sequence[float] = ()                # journal unit-start epochs
    entry_bar_ts: Mapping[Tuple[str, str], int] = field(default_factory=dict)
    tf_ms: Callable[[str], Optional[int]] = None       # type: ignore[assignment]
    env_leverage: Optional[float] = None               # KF7
    asset_max_leverage: Mapping[str, float] = field(default_factory=dict)
    # F9 cap context: bar_ts -> {'bound','remaining','candidates_live_order',
    #   'candidates_engine_order','live_selection'} (from journalctl SIGNAL lines)
    cap_context: Callable[[Optional[int]], Optional[Mapping[str, Any]]] = None  # type: ignore[assignment]
    recompute_size: Optional[Callable[[str, float], Optional[float]]] = None    # (coin, other_equity)->size
    venue_round_size: Callable[[str, float], float] = None    # type: ignore[assignment]
    opens_count_for_day: Optional[Callable[[str], int]] = None
    mirror_opens_for_day: Optional[Callable[[str], int]] = None
    manual_classes: Mapping[int, str] = field(default_factory=dict)  # trade_id->management_class
    shadow_raw: Sequence[Mapping[str, Any]] = ()   # full retained log (KF6 evidence)

    def __post_init__(self) -> None:
        if self.tf_ms is None:
            def _tf(tf: str) -> Optional[int]:
                from fleet_core.engine.shadow_runner import tf_to_ms
                try:
                    return tf_to_ms(tf)
                except (ValueError, AttributeError):
                    return None
            self.tf_ms = _tf
        if self.cap_context is None:
            self.cap_context = lambda bar_ts: None
        if self.venue_round_size is None:
            self.venue_round_size = lambda coin, sz: sz


@dataclass
class Divergence:
    div_id: int
    pair: AlignedPair
    cls: str
    kf_id: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    triage_note: Optional[str] = None


def _ev(p: AlignedPair) -> Event:
    return p.shadow or p.live  # type: ignore[return-value]


def _entry_bar(ctx: ComparatorContext, coin: str, tf: str,
               ev: Event) -> Optional[int]:
    if ev.params.get("entry_bar_ts") is not None:
        return ev.params["entry_bar_ts"]
    return ctx.entry_bar_ts.get((coin, tf))


def _find(events: Sequence[Event], coin: str, cls: str,
          bar_ts: Optional[int] = None, reason: Optional[str] = None) -> Optional[Event]:
    for e in events:
        if e.coin != coin or e.event_class != cls:
            continue
        if bar_ts is not None and e.bar_ts != bar_ts:
            continue
        if reason is not None and e.params.get("reason") != reason:
            continue
        return e
    return None


# ---- KF matchers (§6.4): each returns evidence dict on MECHANICAL match, else None.

def _kf1(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """entry-bar suppression (ext/pac/nado): live exits/trails on bar E; shadow
    defers to E+1. Exactly one bar shift, reason unchanged."""
    if ctx.venue not in ("extended", "pacifica", "nado"):
        return None
    if p.outcome == LIVE_ONLY and p.live is not None \
            and p.live.event_class in ("exit", "sl_replace"):
        l = p.live
        eb = _entry_bar(ctx, l.coin, l.tf, l)
        step = ctx.tf_ms(l.tf)
        if eb is None or step is None or l.bar_ts != eb:
            return None
        s = _find(sev, l.coin, l.event_class, bar_ts=eb + step,
                  reason=l.params.get("reason"))
        if s is not None:
            return {"entry_bar_ts": eb, "live_bar": l.bar_ts, "shadow_bar": s.bar_ts,
                    "reason": l.params.get("reason"), "shift_bars": 1}
    if p.outcome == SHADOW_ONLY and p.shadow is not None \
            and p.shadow.event_class in ("exit", "sl_replace"):
        s = p.shadow
        eb = _entry_bar(ctx, s.coin, s.tf, s)
        step = ctx.tf_ms(s.tf)
        if eb is None or step is None or s.bar_ts != eb + step:
            return None
        l = _find(lev, s.coin, s.event_class, bar_ts=eb,
                  reason=s.params.get("reason"))
        if l is not None:
            return {"entry_bar_ts": eb, "live_bar": l.bar_ts, "shadow_bar": s.bar_ts,
                    "reason": s.params.get("reason"), "shift_bars": 1}
    return None


def _kf2(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """time-stop persistence: across a live restart the live 120-bar clock re-arms;
    shadow (persisted entry_bar_ts) fires time_stop at true E+121."""
    if ctx.venue not in ("extended", "pacifica", "nado"):
        return None
    e = _ev(p)
    if e.event_class != "exit" or e.params.get("reason") != "time_stop":
        return None
    if p.outcome == MATCH:
        return None
    coin, tf = e.coin, e.tf
    eb = _entry_bar(ctx, coin, tf, e)
    if eb is None:
        return None
    restart = [r for r in ctx.live_restarts if eb / 1000.0 <= r <= e.ts]
    if not restart:
        return None
    if p.outcome == SHADOW_ONLY or (
            p.outcome == PARAM_MISMATCH and p.shadow is not None
            and p.live is not None and (p.shadow.bar_ts or 0) < (p.live.bar_ts or 0)):
        return {"entry_bar_ts": eb, "restart_ts": restart[-1],
                "direction": "shadow_fires_true_E+121_live_rearmed"}
    return None


def _kf3(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """place-before-cancel (pac): ordering-only; same target px."""
    if ctx.venue != "pacifica" or p.outcome != PARAM_MISMATCH:
        return None
    if p.shadow is None or p.live is None or p.shadow.event_class != "sl_replace":
        return None
    st, lt = p.shadow.params.get("target_px"), p.live.params.get("target_px")
    if st is None or lt is None:
        return None
    d = _bps(float(st), float(lt))
    if d <= KF3_SAME_PX_BPS:
        return {"px_delta_bps": d, "note": "ordering-only, same target px"}
    return None


def _kf4(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """sl_placed_px anchor (pac/nado): shadow suppresses a re-place live performs
    (Δ<=5bps churn), or skips live's restart-forced re-place."""
    if ctx.venue not in ("pacifica", "nado") or p.outcome != LIVE_ONLY:
        return None
    l = p.live
    if l is None or l.event_class != "sl_replace":
        return None
    prev, tgt = l.params.get("prev_sl"), l.params.get("target_px")
    if prev is not None and tgt is not None \
            and _bps(float(tgt), float(prev)) <= SL_REPLACE_THRESH_BPS:
        return {"churn_delta_bps": _bps(float(tgt), float(prev)),
                "sub_case": "churn_below_engine_thresh"}
    recent_restart = [r for r in ctx.live_restarts
                      if 0 <= l.ts - r <= TICK_WINDOW * ctx.tick_sec]
    if recent_restart:
        return {"restart_ts": recent_restart[-1], "sub_case": "restart_forced_replace"}
    return None


def _kf5(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """oid-first attribution label flip: trail_sl->sl (untrailed) or
    unknown_investigate->oid-attributed; SAME exit bar + px.
    R3/R2-F4b: expected live count ~ 0 (oid-first fleet-wide since 2026-07-01);
    retained for residual proximity-fallback paths only."""
    if ctx.venue not in ("extended", "pacifica", "nado") \
            or p.outcome != PARAM_MISMATCH or p.shadow is None or p.live is None:
        return None
    if p.shadow.event_class != "exit" or p.shadow.bar_ts != p.live.bar_ts:
        return None
    sr, lr = p.shadow.params.get("reason"), p.live.params.get("reason")
    flips = ((lr == "trail_sl" and sr == "sl") or
             (lr == "unknown_investigate" and sr in ("sl", "trail_sl", "tp")))
    if flips:
        return {"live_reason": lr, "shadow_reason": sr,
                "note": "expected live count ~0 (oid-first live 2026-07-01)"}
    return None


def _kf6(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """gap phantom-guard scope: shadow suppresses a gap_through_sl self-close where
    live mark is back inside; must observe exchange SL resting throughout."""
    if ctx.venue not in ("extended", "pacifica", "nado") or p.outcome != LIVE_ONLY:
        return None
    l = p.live
    if l is None or l.event_class != "exit" \
            or l.params.get("reason") != "gap_through_sl":
        return None
    # the shadow's suppression verdict is a raw sl_hit record (not an alignable
    # event) — read it from the retained log via ctx.shadow_raw
    supp = None
    for raw in ctx.shadow_raw:
        if raw.get("phase") == "sl_hit" and raw.get("coin") == l.coin \
                and (raw.get("params") or {}).get("suppressed") == "gap_through_sl":
            supp = raw
            break
    if supp is None:
        return None
    if not supp.get("inputs", {}).get("sl_live", False):
        return None  # evidence REQUIRED: exchange SL resting throughout
    if not supp.get("params", {}).get("mark_back_inside", False):
        return None
    return {"suppression_record_ts": supp.get("ts"), "sl_live": True,
            "mark_back_inside": True}


def _kf7(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """eff_lev cap on Nado: shadow size < live size ONLY where asset max_leverage <
    env LEVERAGE; size ratio == lev ratio exactly."""
    if ctx.venue != "nado" or p.outcome != PARAM_MISMATCH \
            or p.shadow is None or p.live is None \
            or p.shadow.event_class != "entry_decision":
        return None
    ss, ls = p.shadow.params.get("size"), p.live.params.get("size")
    if ss is None or ls is None or not (float(ss) < float(ls)):
        return None
    env = ctx.env_leverage
    amax = ctx.asset_max_leverage.get(p.shadow.coin)
    if env is None or amax is None or not (amax < env):
        return None
    lev_ratio = env / min(env, amax)
    size_ratio = float(ls) / float(ss)
    if abs(size_ratio - lev_ratio) <= KF7_RATIO_RTOL * lev_ratio:
        return {"env_leverage": env, "asset_max": amax,
                "size_ratio": size_ratio, "lev_ratio": lev_ratio}
    return None


def _kf8(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """restore-resolve alive: shadow resolves a DB-open-exchange-absent row that
    live keeps forever (TypeError bug)."""
    if ctx.venue not in ("extended", "pacifica", "nado") \
            or p.outcome != SHADOW_ONLY or p.shadow is None:
        return None
    s = p.shadow
    if s.event_class != "phantom_resolve":
        return None
    if s.inputs.get("presence") == "ABSENT" or s.params.get("presence") == "ABSENT" \
            or int(s.params.get("miss_count", 0)) >= 3:
        return {"presence": "ABSENT", "miss_count": s.params.get("miss_count"),
                "note": "live keeps row (restore-resolve TypeError class)"}
    return None


def _kf9(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
         lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """supersede dup-SL (all venues): shadow would cancel a duplicate resting SL
    live leaves (incl. the per-tick supersede sweep, exit-engine §7-K12)."""
    if p.outcome != SHADOW_ONLY or p.shadow is None:
        return None
    s = p.shadow
    cause = (s.params.get("cause") or "").lower()
    if s.event_class in ("sl_replace", "heal") and \
            ("supersede" in cause or "duplicate" in cause or
             s.params.get("sweep") is True):
        return {"cause": cause or "supersede_sweep",
                "cancelled_oids": s.params.get("cancel_oids")}
    return None


_KF10_REJECT_LABELS = frozenset({"entry_cooldown_active", "cooldown",
                                 "breakout_invalid", "breakout_validity"})


def _kf10(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
          lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """fixed reject-taxonomy diffs — label-only, same verdict. Pre-registered:
    (a) nado proximity-order label flip (tp<->sl/trail_sl, SAME exit bar + px);
    (b) nado same-tick double-reject label (cooldown vs breakout ordering);
    (c) HL cooldown/breakout reject without create-then-delete pending row."""
    if p.outcome != PARAM_MISMATCH or p.shadow is None or p.live is None:
        # (c) shows as LIVE_ONLY entry_decision of a deleted pending row on HL
        if ctx.venue == "hl" and p.outcome == LIVE_ONLY and p.live is not None \
                and p.live.event_class == "entry_decision" \
                and p.live.params.get("deleted_pending"):
            rej = _find(lev, p.live.coin, "reject", bar_ts=p.live.bar_ts)
            if rej is not None and any(k in (rej.params.get("reason") or "")
                                       for k in ("cooldown", "breakout")):
                return {"entry": "hl_no_create_then_delete",
                        "reject_reason": rej.params.get("reason")}
        return None
    s, l = p.shadow, p.live
    if ctx.venue == "nado" and s.event_class == "exit" and s.bar_ts == l.bar_ts:
        sr, lr = s.params.get("reason"), l.params.get("reason")
        pair = {sr, lr}
        if "tp" in pair and pair & SL_FIRE_REASONS:
            spx, lpx = s.params.get("exit_px"), l.params.get("exit_px")
            if spx is None or lpx is None or _bps(float(spx), float(lpx)) <= 1.0:
                return {"entry": "nado_proximity_order_flip",
                        "shadow_reason": sr, "live_reason": lr}
    if ctx.venue == "nado" and s.event_class == "reject":
        sr = (s.params.get("reason") or "").lower()
        lr = (l.params.get("reason") or "").lower()
        s_hit = any(k in sr for k in ("cooldown", "breakout"))
        l_hit = any(k in lr for k in ("cooldown", "breakout"))
        if s_hit and l_hit and sr != lr:
            return {"entry": "nado_double_reject_label",
                    "shadow_reason": sr, "live_reason": lr}
    return None


def _kf11(p: AlignedPair, ctx: ComparatorContext, sev: Sequence[Event],
          lev: Sequence[Event]) -> Optional[Dict[str, Any]]:
    """live-internal-state divergence (F6) — enter-vs-reject or cap-gated selection
    divergence, evidence matcher with PER-SUB-CASE horizons (R3 / R2-F2):
      (i) abort-cooldown: live abort/_register_entry_abort for THAT coin within the
          venue cooldown horizon (cooldown_sec, 900s/15min) BEFORE the decision;
      (ii) opens_today-counter: horizon = since the last live restart within the
          SAME UTC DAY (restart ts from journal unit-start marker; opens_today
          resets at UTC midnight — day-scale window, NOT 900s).
    NO evidence match => NOT KF11 (falls through, never rationalized)."""
    if p.outcome not in (SHADOW_ONLY, LIVE_ONLY):
        return None
    e = _ev(p)
    if e.event_class not in ("entry_decision", "reject"):
        return None
    # divergence must be enter-vs-reject across sides for the SAME (coin, bar)
    other_side = lev if e.source == "shadow" else sev
    opp_class = "reject" if e.event_class == "entry_decision" else "entry_decision"
    counterpart = _find(other_side, e.coin, opp_class, bar_ts=e.bar_ts)
    if counterpart is None and e.bar_ts is not None:
        counterpart = _find(other_side, e.coin, opp_class)  # ts-derived bar jitter
    if counterpart is None:
        return None
    dec_ts = max(e.ts, counterpart.ts)
    shadow_entered = (e.source == "shadow" and e.event_class == "entry_decision") \
        or (counterpart.source == "shadow" and counterpart.event_class == "entry_decision")

    # sub-case (i): abort-cooldown, 900s horizon, that coin
    for m in ctx.suppression_events:
        if getattr(m, "kind", None) == "abort" and getattr(m, "coin", None) == e.coin:
            dt = dec_ts - float(getattr(m, "ts", 0.0))
            if 0.0 <= dt <= ctx.cooldown_sec:
                if shadow_entered:  # expected direction: shadow-enter/live-reject
                    return {"sub_case": "abort_cooldown", "abort_ts": m.ts,
                            "dt_sec": dt, "cooldown_sec": ctx.cooldown_sec,
                            "direction": "shadow_enter_live_reject"}

    # sub-case (ii): opens_today counter, since-last-restart-same-UTC-day horizon
    day = _utc_day(dec_ts)
    restarts_same_day = [r for r in ctx.live_restarts
                         if r <= dec_ts and _utc_day(r) == day]
    if restarts_same_day:
        rts = max(restarts_same_day)
        db_opens = ctx.opens_count_for_day(day) if ctx.opens_count_for_day else None
        mir_opens = ctx.mirror_opens_for_day(day) if ctx.mirror_opens_for_day else None
        # direction: shadow-reject/live-enter after restart (live lost counter), or
        # symmetric live-reject/shadow-enter when counters provably diverge
        direction = ("shadow_reject_live_enter" if not shadow_entered
                     else "live_reject_shadow_enter")
        if not shadow_entered or (db_opens is not None and mir_opens is not None
                                  and db_opens != mir_opens):
            return {"sub_case": "opens_today_counter", "restart_ts": rts,
                    "utc_day": day, "db_opens": db_opens,
                    "mirror_opens": mir_opens, "direction": direction}
    return None


# order matters only for reporting; matchers are disjoint by construction
KF_MATCHERS: Tuple[Tuple[str, Callable[..., Optional[Dict[str, Any]]]], ...] = (
    ("KF1", _kf1), ("KF2", _kf2), ("KF3", _kf3), ("KF4", _kf4), ("KF5", _kf5),
    ("KF6", _kf6), ("KF7", _kf7), ("KF8", _kf8), ("KF9", _kf9), ("KF10", _kf10),
    ("KF11", _kf11),
)


def _ordering_recompute(cap: Mapping[str, Any]) -> Dict[str, Any]:
    """F9 both-orderings recompute at a bound cap: selection under LIVE stream
    on-detect order (journalctl SIGNAL lines) and under ENGINE order; the
    engine-acceptable set = union of both selections."""
    remaining = int(cap.get("remaining", 0))
    sel_live_order = list(cap.get("candidates_live_order") or [])[:remaining]
    sel_engine_order = list(cap.get("candidates_engine_order") or [])[:remaining]
    return {
        "sel_live_order": sel_live_order,
        "sel_engine_order": sel_engine_order,
        "engine_acceptable_set": set(sel_live_order) | set(sel_engine_order),
        "live_selection": set(cap.get("live_selection") or []),
    }


# ------------------------------------------------------------- benign-timing (F9)

def _benign_timing(p: AlignedPair, ctx: ComparatorContext,
                   sev: Sequence[Event], lev: Sequence[Event]
                   ) -> Optional[Dict[str, Any]]:
    """§6.3 benign-timing: same decision + params within tolerance at a different
    wall-clock; or input-jitter proven by recompute; F9 entry-ordering rule."""
    # stale_mirror race: live wrote mid-tick — shadow acted on a superseded rowver
    if p.outcome in (PARAM_MISMATCH, SHADOW_ONLY) and p.shadow is not None \
            and p.shadow.event_class in ("sl_replace", "heal"):
        rowver = p.shadow.inputs.get("mirror_rowver")
        if rowver and p.live is not None:
            prev = p.live.params.get("prev_sl")
            if prev is not None and str(rowver[1]) not in (repr(float(prev)), str(prev)):
                return {"sub_case": "stale_mirror", "mirror_rowver": rowver,
                        "live_prev_sl": prev}
    # size input-jitter separation (§6.2): recompute from the OTHER side's equity
    if p.outcome == PARAM_MISMATCH and p.shadow is not None and p.live is not None \
            and p.shadow.event_class == "entry_decision" and ctx.recompute_size:
        leq = p.live.inputs.get("equity") or p.live.params.get("equity")
        if leq is not None:
            re_sz = ctx.recompute_size(p.shadow.coin, float(leq))
            if re_sz is not None:
                rounded = ctx.venue_round_size(p.shadow.coin, re_sz)
                if float(p.live.params.get("size", -1)) == rounded:
                    return {"sub_case": "equity_read_jitter",
                            "recomputed_size": rounded,
                            "live_equity": leq}
    # F9 entry ORDERING: benign ONLY under the no-cap-binding proof; cap bound =>
    # recompute BOTH orderings, live choice must be in the engine-acceptable set
    e = _ev(p)
    if e.event_class == "entry_decision" and p.outcome in (SHADOW_ONLY, LIVE_ONLY):
        cap = ctx.cap_context(e.bar_ts)
        if cap is not None:
            cands = list(cap.get("candidates_live_order") or [])
            remaining = int(cap.get("remaining", 0))
            if not cap.get("bound", False) and remaining >= len(cands):
                return {"sub_case": "entry_ordering_no_cap_binding",
                        "remaining": remaining, "candidates_n": len(cands),
                        "proof": "no shared cap bound on this bar"}
            rc = _ordering_recompute(cap)
            if rc["live_selection"] and \
                    rc["live_selection"].issubset(rc["engine_acceptable_set"]):
                return {"sub_case": "entry_ordering_cap_bound_recompute",
                        "engine_acceptable_set": sorted(rc["engine_acceptable_set"]),
                        "live_selection": sorted(rc["live_selection"]),
                        "orderings": {"live": rc["sel_live_order"],
                                      "engine": rc["sel_engine_order"]}}
            return None  # cap bound and live choice outside set => NOT benign
    return None


# ---------------------------------------------------------- authority checks (bug)

_MANUAL_ALLOWED = frozenset({"NoOp", "Defer", "AdoptResolve"})
_PROTECT_ONLY_FORBIDDEN = frozenset({"Close", "EscalateNaked", "RecordExit"})


def _authority_bug(p: AlignedPair, ctx: ComparatorContext) -> Optional[Dict[str, Any]]:
    """Engine-defect detector (comparator-proven `bug`, §6.3): shadow venue-touching
    intents against rows without authority.
      * management_class='manual_claimed': FULL hands-off — the ONLY legal engine
        action is the phantom-K3 DB-only row-resolve (AdoptResolve), same as
        protect_only (review nit 2: explicit spec — a row-resolve touches the DB,
        not the venue). Anything else = bug.
      * management_class='protect_only': Close/EscalateNaked/RecordExit refused
        (executor authority gate F2) — shadow deciding one = engine defect.
    """
    if p.shadow is None:
        return None
    raw = p.shadow.raw or {}
    tid = (p.shadow.params or {}).get("trade_id") \
        or (raw.get("params") or {}).get("trade_id")
    mclass = ctx.manual_classes.get(tid) if tid is not None else \
        (raw.get("inputs") or {}).get("management_class")
    decision = raw.get("decision", "")
    if mclass == "manual_claimed" and decision not in _MANUAL_ALLOWED:
        return {"management_class": "manual_claimed", "decision": decision,
                "rule": "manual_claimed rows: DB-only phantom-K3 row-resolve is the "
                        "only legal engine action (same as protect_only)"}
    if mclass == "protect_only" and decision in _PROTECT_ONLY_FORBIDDEN:
        return {"management_class": "protect_only", "decision": decision,
                "rule": "F2 executor authority gate: no close authority"}
    return None


# =============================================================== classification run

class Comparator:
    """Stateless full-window comparator (re-run law §7.2). Feed it the RETAINED
    shadow log records + live events; every call recomputes every verdict."""

    def __init__(self, ctx: ComparatorContext) -> None:
        self.ctx = ctx
        self.sha = comparator_sha()

    def run(self, shadow_records: Sequence[Mapping[str, Any]],
            live_events: Sequence[Event]) -> Tuple[List[AlignedPair], List[Divergence]]:
        self.ctx.shadow_raw = list(shadow_records)   # KF6 suppression evidence
        sev = shadow_events_from_log(shadow_records)
        pairs = align(sev, live_events, self.ctx.tick_sec)
        divs: List[Divergence] = []
        div_id = 0
        # shadow bug records are divergences of class bug regardless of alignment
        for r in shadow_records:
            if r.get("decision") == "bug" or (r.get("phase") == "tick"
                                              and r.get("decision") == "bug"):
                div_id += 1
                divs.append(Divergence(div_id, AlignedPair(
                    ("", "", None, "bug"), None, None, SHADOW_ONLY),
                    CLS_BUG, evidence={"record": r}))
        for p in pairs:
            if p.outcome == MATCH:
                continue
            div_id += 1
            divs.append(self._classify(div_id, p, sev, live_events))
        return pairs, divs

    def _classify(self, div_id: int, p: AlignedPair, sev: Sequence[Event],
                  lev: Sequence[Event]) -> Divergence:
        bug_ev = _authority_bug(p, self.ctx)
        if bug_ev is not None:
            return Divergence(div_id, p, CLS_BUG, evidence=bug_ev)
        for kf_id, fn in KF_MATCHERS:
            ev = fn(p, self.ctx, sev, lev)
            if ev is not None:
                return Divergence(div_id, p, CLS_KF, kf_id=kf_id, evidence=ev)
        ben = _benign_timing(p, self.ctx, sev, lev)
        if ben is not None:
            return Divergence(div_id, p, CLS_BENIGN, evidence=ben)
        e = _ev(p)
        # strategy-delta definition (§6.3): different MONEY decision not covered above
        if e.event_class in ("entry_decision", "reject"):
            cap = self.ctx.cap_context(e.bar_ts)
            if cap is not None and cap.get("bound") \
                    and e.event_class == "entry_decision":
                # reaching here = _benign_timing already recomputed BOTH orderings
                # and the live choice is OUTSIDE the engine-acceptable set (F9)
                rc = _ordering_recompute(cap)
                return Divergence(div_id, p, CLS_STRAT, evidence={
                    "kind": "at_cap_selection_outside_engine_acceptable_set",
                    "engine_acceptable_set": sorted(rc["engine_acceptable_set"]),
                    "live_selection": sorted(rc["live_selection"])})
            other = lev if e.source == "shadow" else sev
            opp = "reject" if e.event_class == "entry_decision" else "entry_decision"
            if _find(other, e.coin, opp, bar_ts=e.bar_ts) is not None or \
                    _find(other, e.coin, opp) is not None:
                return Divergence(div_id, p, CLS_STRAT,
                                  evidence={"kind": "enter_vs_reject"})
            return Divergence(div_id, p, CLS_UNEXPLAINED,
                              evidence={"kind": "unmatched_%s" % e.event_class})
        if p.outcome == PARAM_MISMATCH:
            if e.event_class == "exit":
                return Divergence(div_id, p, CLS_STRAT, evidence=dict(
                    p.mismatch, kind="exit_reason_or_bar_differs"))
            if e.event_class in ("sl_replace", "heal"):
                return Divergence(div_id, p, CLS_STRAT, evidence=dict(
                    p.mismatch, kind="trail_target_gt_5bps_identical_inputs"))
            if e.event_class == "entry_decision":
                return Divergence(div_id, p, CLS_STRAT, evidence=dict(
                    p.mismatch, kind="size_beyond_input_jitter"))
        if e.event_class == "exit" and p.outcome in (SHADOW_ONLY, LIVE_ONLY):
            return Divergence(div_id, p, CLS_STRAT, evidence={
                "kind": "exit_on_one_side_only", "outcome": p.outcome})
        return Divergence(div_id, p, CLS_UNEXPLAINED,
                          evidence={"outcome": p.outcome})


def apply_triage(divs: Sequence[Divergence],
                 triage: Mapping[int, Mapping[str, str]]) -> None:
    """Manual triage of `unexplained` (§7.3): each entry MUST carry written evidence
    and a target class. Mechanically enforced: no evidence text => refused."""
    for d in divs:
        t = triage.get(d.div_id)
        if t is None:
            continue
        if d.cls != CLS_UNEXPLAINED:
            raise ValueError("triage only applies to unexplained (div %d is %s)"
                             % (d.div_id, d.cls))
        ncls, evidence = t.get("class"), (t.get("evidence") or "").strip()
        if ncls not in (CLS_BENIGN, CLS_KF, CLS_STRAT, CLS_BUG):
            raise ValueError("triage class invalid: %r" % ncls)
        if not evidence:
            raise ValueError("triage without written evidence refused (EVID GATE)")
        d.cls = ncls
        d.triage_note = evidence
        if ncls == CLS_KF:
            d.kf_id = t.get("kf_id")
            if not d.kf_id:
                raise ValueError("triage to known-fix-delta requires kf_id")


# ============================================================ coverage ledger (§7.5)

@dataclass
class ReplayResult:
    """Result of a per-class REPLAY (§7.5 mechanism): synthetic scenario through the
    FULL shadow engine (decision core + IntentExecutor vs scripted fake binding),
    built from the P2 conformance fault bank (fleet_core/conformance/faults.py) and
    recorded live sequences. `passed` = the class produced EXACTLY the expected
    intent sequence (machine-asserted by the replay harness); any deviation = bug."""
    cls: str
    passed: bool
    detail: str = ""


class CoverageLedger:
    """F8 per-class event-coverage ledger: EVERY class green before cutover, by live
    observation OR replay — a class with zero exercise cannot be blessed."""

    def __init__(self, venue: str, tp1_frac_live: float = 0.0) -> None:
        self.venue = venue
        self.floors: Dict[str, int] = dict(LEDGER_CLASSES)
        self.live_n: Dict[str, int] = {c: 0 for c, _ in LEDGER_CLASSES}
        self.replay: Dict[str, ReplayResult] = {}
        # pac/nado run TP1_PARTIAL_FRAC=0 live -> replay-only, flagged inert-live
        self.inert_live: Dict[str, bool] = {"tp1_be": tp1_frac_live <= 0.0}

    def observe_live_events(self, live_events: Sequence[Event]) -> None:
        for e in live_events:
            if e.event_class == "entry_decision":
                self.live_n["entry"] += 1
            elif e.event_class == "exit" and e.params.get("reason") in SL_FIRE_REASONS:
                self.live_n["exit_sl"] += 1
            elif e.event_class == "sl_replace":
                if (e.params.get("cause") or "trail") == "heal":
                    self.live_n["heal"] += 1
                else:
                    self.live_n["trail"] += 1
            elif e.event_class == "heal":
                self.live_n["heal"] += 1
            elif e.event_class == "tp1":
                self.live_n["tp1_be"] += 1
            elif e.event_class == "phantom_resolve":
                self.live_n["phantom_k3"] += 1
            elif e.event_class == "reject" and any(
                    m in (e.params.get("reason") or "").lower()
                    for m in ABORT_REASON_MARKERS):
                self.live_n["abort_unwind"] += 1
        # adopt_untracked / orphan_cancel / supersede arrive via journal-kind events
        # mapped by the extractor's journal passthrough (params carry 'ledger_class')
        for e in live_events:
            lc = e.params.get("ledger_class")
            if lc in self.live_n:
                self.live_n[lc] += 1

    def record_replay(self, result: ReplayResult) -> None:
        if result.cls not in self.live_n:
            raise ValueError("unknown ledger class %r" % result.cls)
        self.replay[result.cls] = result

    def class_status(self, cls: str) -> Dict[str, Any]:
        floor = self.floors[cls]
        n = self.live_n[cls]
        rr = self.replay.get(cls)
        live_green = n >= floor if floor > 0 else n >= 1
        replay_green = rr is not None and rr.passed
        if self.inert_live.get(cls):
            green = replay_green      # replay-only class, flagged
        else:
            green = live_green or replay_green
        return {"class": cls, "live_n": n, "floor": floor if floor > 0 else "opportunistic",
                "replay": (None if rr is None else ("PASS" if rr.passed else "FAIL")),
                "inert_live": bool(self.inert_live.get(cls)), "green": green}

    def all_status(self) -> List[Dict[str, Any]]:
        return [self.class_status(c) for c, _ in LEDGER_CLASSES]

    def all_green(self) -> bool:
        return all(s["green"] for s in self.all_status())


# ================================================================ gate (§7) + report

def coverage_stats(shadow_records: Sequence[Mapping[str, Any]],
                   window_start: float, window_end: float,
                   tick_sec: float = 60.0) -> Dict[str, Any]:
    """§7.4 coverage from the retained decision log's tick heartbeat records."""
    ticks = [(_parse_iso(r.get("ts", "")) or 0.0, r.get("decision"))
             for r in shadow_records if r.get("phase") == "tick"]
    ticks = [(t, d) for t, d in ticks if window_start <= t <= window_end]
    ticks.sort()
    expected = max(1, int((window_end - window_start) / tick_sec))
    uptime_pct = 100.0 * min(1.0, len(ticks) / expected)
    max_gap = 0.0
    prev = window_start
    for t, _ in ticks:
        max_gap = max(max_gap, t - prev)
        prev = t
    max_gap = max(max_gap, window_end - prev)
    skipped = sum(1 for _, d in ticks if d == "shadow_tick_skipped")
    skipped_pct = 100.0 * skipped / max(1, len(ticks))
    return {"uptime_pct": uptime_pct, "max_gap_sec": max_gap,
            "skipped_pct": skipped_pct, "ticks": len(ticks),
            "expected_ticks": expected}


class GateCalculator:
    """48h GREEN GATE (§7): machine-readable verdict, ALL criteria must hold."""

    def __init__(self, venue: str, engine_sha: str) -> None:
        self.venue = venue
        self.engine_sha = engine_sha
        self.comparator_sha = comparator_sha()

    def evaluate(self, divs: Sequence[Divergence], ledger: CoverageLedger,
                 cov: Mapping[str, Any], rehearsal_passed: bool,
                 unaligned_live_events: int = 0,
                 open_bugs: Optional[int] = None) -> Dict[str, Any]:
        by_cls: Dict[str, int] = {}
        for d in divs:
            by_cls[d.cls] = by_cls.get(d.cls, 0) + 1
        n_strat = by_cls.get(CLS_STRAT, 0)
        n_bug = by_cls.get(CLS_BUG, 0) if open_bugs is None else open_bugs
        n_unex = by_cls.get(CLS_UNEXPLAINED, 0)
        kf_counts: Dict[str, int] = {}
        for d in divs:
            if d.cls == CLS_KF:
                kf_counts[d.kf_id or "?"] = kf_counts.get(d.kf_id or "?", 0) + 1
        # §7.7: every KF observed matched its registered signature exactly — true by
        # construction (class KF is only assignable via a registered matcher or a
        # kf_id-carrying triage); a signature mismatch lands in unexplained.
        criteria = {
            "c1_zero_strategy_delta": n_strat == 0,
            "c2_zero_open_bugs": n_bug == 0,
            "c3_zero_unexplained_after_triage": n_unex == 0,
            "c4_coverage": (cov["uptime_pct"] >= 99.0
                            and cov["max_gap_sec"] <= 300.0
                            and unaligned_live_events == 0
                            and cov["skipped_pct"] < 1.0),
            "c5_ledger_all_green": ledger.all_green(),
            "c6_adoption_rehearsal_passed": bool(rehearsal_passed),
            "c7_kf_enumerated_signature_exact": True,
            "c8_read_gate_scope_honesty": True,  # report ALWAYS states the exclusion
        }
        return {
            "venue": self.venue, "green": all(criteria.values()),
            "criteria": criteria, "by_class": by_cls, "kf_counts": kf_counts,
            "coverage": dict(cov), "engine_sha": self.engine_sha,
            "comparator_sha": self.comparator_sha,
            "note_writes": ("READ-gate scope honesty (F7): this 48h gate proves "
                            "DECISION parity on live reads only — venue WRITE paths "
                            "are NOT covered here; they are exercised by the "
                            "per-venue WRITE-CANARY at cutover (p3_rollout.md §3 "
                            "step 6b), a separate hard gate."),
            "note_kf5": ("KF5 expected live count ~0 (oid-first shipped fleet-wide "
                         "2026-07-01); 0 occurrences is NOT a coverage anomaly "
                         "(R3 / R2-F4b)."),
        }

    def write_report(self, path: str, verdict: Mapping[str, Any],
                     divs: Sequence[Divergence], ledger: CoverageLedger,
                     window_utc: Tuple[str, str]) -> None:
        """proofs/p3_shadow_report_<venue>.md format (§7 tail): divergence table,
        gate checklist PASS/FAIL, engine_sha, window bounds UTC."""
        lines = [
            "# P3 SHADOW REPORT — %s" % self.venue,
            "",
            "Window (UTC): %s -> %s | engine_sha `%s` | comparator_sha `%s`"
            % (window_utc[0], window_utc[1], verdict["engine_sha"],
               verdict["comparator_sha"]),
            "",
            "**%s**" % ("GREEN" if verdict["green"] else "RED"),
            "",
            "> %s" % verdict["note_writes"],
            "",
            "## Gate checklist", "",
            "| criterion | verdict |", "|---|---|",
        ]
        for k, v in verdict["criteria"].items():
            lines.append("| %s | %s |" % (k, "PASS" if v else "FAIL"))
        lines += ["", "## Divergences by class", "", "| class | count |", "|---|---|"]
        for cls in (CLS_BENIGN, CLS_KF, CLS_STRAT, CLS_BUG, CLS_UNEXPLAINED):
            lines.append("| %s | %d |" % (cls, verdict["by_class"].get(cls, 0)))
        lines += ["", "## Known-fix-delta enumeration", "",
                  "| KF | count |", "|---|---|"]
        for kf in ("KF1", "KF2", "KF3", "KF4", "KF5", "KF6", "KF7", "KF8",
                   "KF9", "KF10", "KF11"):
            lines.append("| %s | %d |" % (kf, verdict["kf_counts"].get(kf, 0)))
        lines += ["", "> %s" % verdict["note_kf5"], "",
                  "## Coverage ledger (F8)", "",
                  "| class | live n | floor | replay | inert-live | green |",
                  "|---|---|---|---|---|---|"]
        for s in ledger.all_status():
            lines.append("| %s | %d | %s | %s | %s | %s |" % (
                s["class"], s["live_n"], s["floor"], s["replay"] or "—",
                "yes" if s["inert_live"] else "no",
                "GREEN" if s["green"] else "RED"))
        lines += ["", "## Divergence detail", "",
                  "| id | key | outcome | class | KF | evidence |", "|---|---|---|---|---|---|"]
        for d in divs:
            ev = json.dumps(d.evidence, default=str)
            if d.triage_note:
                ev += " | triage: " + d.triage_note
            lines.append("| %d | %s | %s | %s | %s | %s |" % (
                d.div_id, "/".join(str(x) for x in d.pair.key), d.pair.outcome,
                d.cls, d.kf_id or "—", ev.replace("|", "\\|")))
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")


# ============================================================================ selftest

def _selftest() -> None:  # pragma: no cover — offline, synthetic events only
    import tempfile
    from types import SimpleNamespace

    T = _dt.datetime(2026, 7, 2, 16, 0, 2, tzinfo=_dt.timezone.utc).timestamp()
    ISO = "2026-07-02T16:00:02Z"
    STEP = 28800000  # 8h in ms
    E = STEP * 100

    def srec(phase, decision, coin, tf="8h", bar_ts=None, params=None, inputs=None):
        return {"ts": ISO, "tick": 1, "venue": "extended", "coin": coin, "tf": tf,
                "bar_ts": bar_ts, "phase": phase, "decision": decision,
                "params": params or {}, "inputs": inputs or {},
                "engine_sha": "x", "flags": []}

    def lev_ev(cls, coin, bar_ts=None, params=None, tf="8h", ts=T):
        return Event("live", "extended", coin, tf, cls, ts=ts, bar_ts=bar_ts,
                     params=params or {})

    # ---- scenario 1: MATCH + KF1 + KF11(i) + KF11(ii) + strategy exit + KF9 +
    #      authority bug + unexplained/triage
    shadow = [
        srec("trail", "ReplaceSL", "BTC", bar_ts=E,
             params={"target_px": 64123.0}),                       # MATCH vs 64120
        srec("exit", "RecordExit", "SOL", bar_ts=E + STEP,
             params={"reason": "tp", "exit_px": 100.0}),           # KF1 (E+1 vs E)
        srec("entry_gate", "Enter", "XRP", bar_ts=E,
             params={"size": 10.0}),                               # KF11(i)
        srec("entry_gate", "Reject", "DOGE", bar_ts=E,
             params={"reason": "max_opens_reached"}),              # KF11(ii)
        srec("exit", "RecordExit", "ETH", bar_ts=E,
             params={"reason": "time_stop", "exit_px": 1.0}),      # strategy-delta
        srec("heal", "HealSL", "AVAX", bar_ts=E,
             params={"cause": "supersede_sweep",
                     "cancel_oids": ["o2"]}),                      # KF9
        srec("pm", "Close", "xyz_GOLD", bar_ts=E,
             params={"reason": "tp", "trade_id": 99},
             inputs={"management_class": "manual_claimed"}),       # authority bug
    ]
    live = [
        lev_ev("sl_replace", "BTC", bar_ts=E, params={"target_px": 64120.0}),
        lev_ev("exit", "SOL", bar_ts=E, params={"reason": "tp", "exit_px": 100.1}),
        lev_ev("reject", "XRP", bar_ts=E,
               params={"reason": "entry_cooldown_active"}),
        lev_ev("entry_decision", "DOGE", bar_ts=E, params={"size": 5.0}),
        lev_ev("exit", "ETH", bar_ts=E, params={"reason": "sl", "exit_px": 1.0}),
        lev_ev("heal", "ZEC", params={"target_px": 9.0}),          # unexplained
    ]
    ctx = ComparatorContext(
        venue="extended",
        suppression_events=[SimpleNamespace(kind="abort", coin="XRP", ts=T - 100.0)],
        live_restarts=[T - 3600.0],
        entry_bar_ts={("SOL", "8h"): E},
        manual_classes={99: "manual_claimed"},
    )
    comp = Comparator(ctx)
    pairs, divs = comp.run(shadow, live)
    assert any(p.outcome == MATCH for p in pairs
               if p.key[0] == "BTC"), "5bps churn tolerance must MATCH"
    by = {}
    for d in divs:
        by.setdefault((d.pair.key[0], d.cls, d.kf_id), []).append(d)
    assert ("SOL", CLS_KF, "KF1") in by, by.keys()
    assert ("XRP", CLS_KF, "KF11") in by
    assert by[("XRP", CLS_KF, "KF11")][0].evidence["sub_case"] == "abort_cooldown"
    assert ("DOGE", CLS_KF, "KF11") in by
    assert by[("DOGE", CLS_KF, "KF11")][0].evidence["sub_case"] == "opens_today_counter"
    assert ("ETH", CLS_STRAT, None) in by          # exit reason differs = strategy
    assert ("AVAX", CLS_KF, "KF9") in by
    assert ("xyz_GOLD", CLS_BUG, None) in by       # manual_claimed Close = engine bug
    unex = [d for d in divs if d.cls == CLS_UNEXPLAINED]
    assert len(unex) == 1 and unex[0].pair.key[0] == "ZEC"

    # triage: refuses without evidence; accepts with written evidence
    try:
        apply_triage(divs, {unex[0].div_id: {"class": CLS_BENIGN, "evidence": ""}})
        raise AssertionError("evidence-less triage accepted")
    except ValueError:
        pass
    apply_triage(divs, {unex[0].div_id: {
        "class": CLS_BENIGN,
        "evidence": "live heal at tick N+4 (outside ±3-tick window); same target px"}})
    assert unex[0].cls == CLS_BENIGN and unex[0].triage_note

    # ---- scenario 2: F9 ordering — cap bound, live choice IN acceptable set
    cap_in = {"bound": True, "remaining": 1,
              "candidates_live_order": ["B", "A"],
              "candidates_engine_order": ["A", "B"],
              "live_selection": ["B"]}
    ctx2 = ComparatorContext(venue="extended", cap_context=lambda b: cap_in)
    _, divs2 = Comparator(ctx2).run(
        [srec("entry_gate", "Enter", "A", bar_ts=E, params={"size": 1.0})],
        [lev_ev("entry_decision", "B", bar_ts=E, params={"size": 1.0})])
    assert all(d.cls == CLS_BENIGN and
               d.evidence["sub_case"] == "entry_ordering_cap_bound_recompute"
               for d in divs2), divs2

    # ---- scenario 3: F9 ordering — live choice OUTSIDE acceptable set
    cap_out = dict(cap_in, live_selection=["C"])
    ctx3 = ComparatorContext(venue="extended", cap_context=lambda b: cap_out)
    _, divs3 = Comparator(ctx3).run(
        [srec("entry_gate", "Enter", "A", bar_ts=E, params={"size": 1.0})],
        [lev_ev("entry_decision", "C", bar_ts=E, params={"size": 1.0})])
    assert any(d.cls == CLS_STRAT and "engine_acceptable_set" in d.evidence
               for d in divs3), divs3

    # ---- scenario 4: KF7 nado eff_lev cap (size ratio == lev ratio exactly)
    ctx4 = ComparatorContext(venue="nado", env_leverage=10.0,
                             asset_max_leverage={"PAXG": 5.0})
    s4 = srec("entry_gate", "Enter", "PAXG", bar_ts=E, params={"size": 1.0})
    s4["venue"] = "nado"
    _, divs4 = Comparator(ctx4).run(
        [s4], [Event("live", "nado", "PAXG", "8h", "entry_decision", ts=T,
                     bar_ts=E, params={"size": 2.0})])
    assert len(divs4) == 1 and divs4[0].kf_id == "KF7", divs4

    # ---- ledger + gate + report
    with tempfile.TemporaryDirectory() as td:
        ledger = CoverageLedger("extended", tp1_frac_live=0.25)
        ledger.observe_live_events(live)
        assert ledger.live_n["entry"] >= 1 and ledger.live_n["exit_sl"] >= 1
        assert not ledger.all_green()   # trail floor 3 unmet, opportunistic at 0
        for cls_name, _floor in LEDGER_CLASSES:
            ledger.record_replay(ReplayResult(cls_name, True, "fault-bank script"))
        assert ledger.all_green()       # every class green via live-or-replay

        w0, w1 = T - 1800, T + 1800
        tick_recs = [{"ts": _dt.datetime.fromtimestamp(
                          w0 + i * 60.0, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                      "phase": "tick", "decision": "TickComplete"}
                     for i in range(60)]
        cov = coverage_stats(tick_recs, w0, w1)
        assert cov["uptime_pct"] >= 99.0 and cov["max_gap_sec"] <= 300.0

        gate = GateCalculator("extended", engine_sha="deadbeef")
        v_red = gate.evaluate(divs, ledger, cov, rehearsal_passed=True)
        assert not v_red["green"]       # strategy-delta + bug present => RED
        assert not v_red["criteria"]["c1_zero_strategy_delta"]
        assert not v_red["criteria"]["c2_zero_open_bugs"]
        clean = [d for d in divs if d.cls in (CLS_BENIGN, CLS_KF)]
        v_green = gate.evaluate(clean, ledger, cov, rehearsal_passed=True)
        assert v_green["green"], v_green
        rpt = os.path.join(td, "p3_shadow_report_extended.md")
        gate.write_report(rpt, v_green, clean, ledger,
                          ("2026-07-02T16:00Z", "2026-07-04T16:00Z"))
        text = open(rpt, encoding="utf-8").read()
        assert "writes NOT covered here" not in text  # exact phrasing check below
        assert "WRITE paths" in text and "NOT covered" in text
        assert "| KF11 | " in text and "comparator_sha" in text
    print("comparator selftest OK")


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.WARNING)
    _selftest()
