"""replay_binding — the REAL-engine adapter behind
``fleet_core.engine.exit_engine.build_replay_engine`` (integrator gap #1,
review round-1).

Binds the PRODUCTION components — entry_sm transition machinery
(t_intent … t_abort_final, run_entry_policy) + entry_sm.Reconciler
(direction-2 adoption, watch_special_rows, orphan sweep) + exit_engine's pure
``tick()`` + ``IntentExecutor`` — to replay's scenario surface
(ReferenceEngine-compatible: ``entry`` / ``tick_row`` / ``executor_try`` /
``reconcile_untracked`` / ``orphan_pass``), so ``replay --engine real`` runs
the 10-class coverage ledger against the REAL engine (a GATE ledger requires
engine=real, replay module contract).

Mechanics
---------
The real entry_sm functions write sqlite by *path*; replay's FakeJournal is an
in-memory DB. The adapter REBINDS the FakeJournal onto a temp-file DB whose
schema is the SUPERSET of both shapes (replay DDL ∪ entry_sm/P1 columns), with
sqlite triggers keeping the twin columns coherent (side↔direction,
exit_px↔exit_price, orig_size seeded at INSERT). The journal connection runs
autocommit (isolation_level=None) + WAL so the real components (own
connections, commit-per-transition) and the scenario asserts (journal.conn)
always see one truth — this is also what lets scenario op-hooks PROVE the
commit-before-dispatch laws (t_mark_sent/t_abort_begin durable before venue
I/O) against the real code.

Label bridges (adapter-level, documented — decisions are identical):
  * NoOp reason 'steady' → 'nothing_due' (replay vocabulary);
  * executed AdoptResolve reason ← the executor's attribution reason;
  * MarkTP1Partial executed → reason 'executed';
  * any refused intent → reason 'REFUSED'.

Zero strategy math here (ScriptedStrategy outputs are data — the byte-pinned
math is exercised by exit_engine's own selftest over strategy_math).
"""
from __future__ import annotations

import logging
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from fleet_core.exchange_api import (RateLimited, ReadUnknown, VenueRejected,
                                     WriteUnconfirmed)
from fleet_core.engine import entry_sm as esm
from fleet_core.engine import exit_engine as xng

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Superset DDL: replay FakeJournal shape ∪ entry_sm/P1 columns.
#   * side/venue nullable (real t_intent writes direction; triggers sync);
#   * sm_transitions.seq nullable (real _audit writes none; ORDER BY id);
#   * rejected_signals carries BOTH shapes' columns.
# --------------------------------------------------------------------------
_SUPERSET_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    venue TEXT DEFAULT 'replay',
    created_at TEXT,
    coin TEXT NOT NULL,
    tf TEXT NOT NULL DEFAULT '8h',
    side TEXT,
    direction TEXT,
    status TEXT NOT NULL,
    sm_state TEXT NOT NULL,
    sm_updated_at TEXT,
    client_order_id TEXT UNIQUE,
    order_sent_at TEXT,
    fill_confirmed_at TEXT,
    fill_oid TEXT,
    entry REAL,
    entry_intended REAL,
    size REAL,
    orig_size REAL,
    sl_initial REAL,
    sl_current REAL,
    sl_order_id TEXT,
    sl_placed_px REAL,
    sl_confirmed_at TEXT,
    tp1 REAL,
    tp1_order_id TEXT,
    tp1_partial_done INTEGER NOT NULL DEFAULT 0,
    tp1_fill_price REAL,
    tp1_frac_at_entry REAL,
    entry_bar_ts INTEGER,
    atr14 REAL,
    leverage_eff REAL,
    limit_px REAL,
    risk_dollars REAL,
    notional REAL,
    walk_slip_pct REAL,
    abort_reason TEXT,
    origin TEXT NOT NULL DEFAULT 'entry',
    management_class TEXT NOT NULL DEFAULT 'strategy',
    notes TEXT,
    opened_at TEXT,
    closed_at TEXT,
    exit_reason TEXT,
    exit_px REAL,
    exit_price REAL,
    pnl_dollars REAL,
    realized_r REAL,
    entry_order_id TEXT
);
CREATE TABLE IF NOT EXISTS sm_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    at TEXT NOT NULL,
    seq INTEGER,
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
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS rejected_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT,
    venue TEXT,
    coin TEXT,
    tf TEXT,
    reason TEXT,
    created_at TEXT,
    direction TEXT,
    trigger_price REAL,
    entry_price REAL,
    sl_price REAL,
    walk_slip_pct REAL,
    vol_1h_usd REAL
);
CREATE TRIGGER IF NOT EXISTS trg_trades_ins_sync AFTER INSERT ON trades
BEGIN
    UPDATE trades SET
        side       = COALESCE(NEW.side, NEW.direction, 'long'),
        direction  = COALESCE(NEW.direction, NEW.side, 'long'),
        orig_size  = COALESCE(NEW.orig_size, NEW.size),
        created_at = COALESCE(NEW.created_at, NEW.sm_updated_at),
        exit_px    = COALESCE(NEW.exit_px, NEW.exit_price),
        exit_price = COALESCE(NEW.exit_price, NEW.exit_px)
    WHERE id = NEW.id;
END;
CREATE TRIGGER IF NOT EXISTS trg_trades_exit_price_sync
AFTER UPDATE OF exit_price ON trades
WHEN NEW.exit_price IS NOT NULL
BEGIN
    UPDATE trades SET exit_px = NEW.exit_price WHERE id = NEW.id;
END;
CREATE TRIGGER IF NOT EXISTS trg_trades_exit_px_sync
AFTER UPDATE OF exit_px ON trades
WHEN NEW.exit_px IS NOT NULL
BEGIN
    UPDATE trades SET exit_price = NEW.exit_px WHERE id = NEW.id;
END;
"""


def _bind_journal_to_file(journal: Any) -> str:
    """Rebind replay's FakeJournal onto a temp-file superset DB (idempotent —
    scenario 7 constructs a second engine over the SAME journal)."""
    existing = getattr(journal, "_real_db_path", None)
    if existing:
        return str(existing)
    tmp = Path(tempfile.mkdtemp(prefix="replay_real_engine_"))
    db = tmp / "trades.db"
    con = sqlite3.connect(str(db))
    try:
        con.executescript(_SUPERSET_DDL)
        con.commit()
    finally:
        con.close()
    new_conn = sqlite3.connect(str(db), isolation_level=None)  # autocommit
    new_conn.row_factory = sqlite3.Row
    new_conn.execute("PRAGMA busy_timeout=5000")
    try:
        new_conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.Error:
        pass
    try:
        journal.conn.close()
    except Exception:  # noqa: BLE001 — old :memory: conn
        pass
    journal.conn = new_conn
    journal._real_db_path = str(db)
    return str(db)


class _JournalShim:
    """exit-engine JournalPort over replay's FakeJournal (DB writes only)."""

    def __init__(self, journal: Any, clock: Any) -> None:
        self.j = journal
        self.clock = clock

    def update_trade_sl(self, tid: int, px: float) -> None:
        self.j.update(tid, sl_current=px)

    def update_trade_sl_order(self, tid: int, oid: str) -> None:
        self.j.update(tid, sl_order_id=oid)

    def update_trade_sl_placed(self, tid: int, px: float) -> None:
        self.j.update(tid, sl_placed_px=px, sl_confirmed_at=self.clock.iso())

    def mark_tp1_partial(self, tid: int, fill_px: float, remaining: float,
                         new_sl: float) -> None:
        self.j.update(tid, tp1_partial_done=1, tp1_fill_price=fill_px,
                      size=remaining, sl_current=new_sl, tp1_order_id=None)
        self.j._audit(tid, "OPEN", "OPEN",
                      {"op": "mark_tp1_partial", "remainder": remaining,
                       "be_px": new_sl})

    def close_trade(self, tid: int, exit_px, reason: str, pnl, rr) -> None:
        row = self.j.row(tid)
        self.j.transition(tid, row["sm_state"], "CLOSED",
                          {"reason": reason, "exit_px": exit_px},
                          exit_reason=reason, exit_px=exit_px,
                          pnl_dollars=pnl, realized_r=rr,
                          closed_at=self.clock.iso())

    def register_placed_trigger_oid(self, oid, coin: str = "",
                                    trade_id: Optional[int] = None,
                                    kind: str = "sl") -> None:
        self.j.registry_insert(str(oid), coin, kind or "sl", trade_id)

    def get_state(self, key: str, default: Any = None) -> Any:
        return self.j.kv_get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        self.j.kv_set(key, value)

    def clear_restore_flag(self, tid: int) -> None:
        self.j.kv_set("restore_flag:%d" % tid, 0)


class _ScriptedAdapter:
    """StrategyAdapter-shaped wrapper over replay's ScriptedStrategy
    (scenario-scripted OUTPUTS — zero math, exit-engine §2 seam)."""

    def __init__(self, st: Any, journal: Any, tid: int) -> None:
        self.st = st
        self.j = journal
        self.tid = tid

    def _row(self):
        return self.j.row(self.tid)

    def update_sl_on_new_bar(self, pos, df):
        new_sl = self.st.new_sl(self._row()) if self.st.new_sl else None
        reason = self.st.exit_reason(self._row()) if self.st.exit_reason else None
        return new_sl, reason

    def is_entry_bar(self, pos, df) -> bool:
        return bool(getattr(self.st, "is_entry_bar", False))

    def check_sl_hit(self, pos, df, vstop_wick_check: bool = True):
        if not self.st.sl_hit:
            return None
        hit = self.st.sl_hit(self._row())
        if hit is None:
            return None
        if isinstance(hit, tuple):
            return hit
        row = self._row()
        ref = row["sl_current"] if row["sl_current"] is not None \
            else row["sl_initial"]
        return (float(ref or 0.0), str(hit))

    def sl_current_view(self, pos) -> float:
        return float(pos.sl_current if pos.sl_current is not None
                     else pos.sl_initial)

    def forget(self, trade_id: int) -> None:
        pass


class _StubBars:
    """Duck-typed non-empty CLOSED-bars stand-in (the ScriptedStrategy seam
    needs no real bars; tick()'s df gate needs len()>0 and _bar_close reads
    ['Close'].iloc[-1])."""

    def __init__(self, close: float = 0.0) -> None:
        self._close = float(close)

    def __len__(self) -> int:
        return 1

    def __getitem__(self, col: str):
        if col != "Close":
            raise KeyError(col)
        outer = self

        class _I:
            @property
            def iloc(self):
                return [outer._close]
        return _I()


class _CapturingReconciler(esm.Reconciler):
    """Real Reconciler with CRITICALs mirrored into the scenario sink
    (same once-per-process vs every-pass cadence as the real _critical)."""

    def __init__(self, *a: Any, sink: Optional[List[str]] = None,
                 **k: Any) -> None:
        super().__init__(*a, **k)
        self._sink = sink if sink is not None else []

    def _critical(self, key: str, msg: str, every_pass: bool = False) -> None:
        if every_pass or key not in self._alerted_once:
            self._sink.append(msg)
        super()._critical(key, msg, every_pass)


class RealReplayEngine:
    """ReferenceEngine-compatible surface over the REAL engine components."""

    name = "real"

    def __init__(self, journal: Any, client: Any, clock: Any,
                 criticals: Optional[List[str]] = None) -> None:
        self.j = journal
        self.c = client
        self.clock = clock
        self.criticals = criticals if criticals is not None else []
        self.db = _bind_journal_to_file(journal)
        # EF3 seam: BE-after-TP1 buffer (VenueQuirks.trail_after_tp_buffer_pct)
        # — scenario-settable, mirrors the live .env seed.
        self.be_buffer_pct = 0.003
        self._jshim = _JournalShim(journal, clock)
        self._rec: Optional[_CapturingReconciler] = None

    # ------------------------------------------------------------ plumbing
    def _reconciler(self) -> _CapturingReconciler:
        if self._rec is None:
            self._rec = _CapturingReconciler(
                self.db, self.c,
                cfg=esm.ReconcilerConfig(
                    venue="replay",
                    entry_abort_cooldown_sec=900.0,
                    now_fn=self.clock.now),
                sink=self.criticals)
        return self._rec

    def _prefixes(self) -> Tuple[str, ...]:
        try:
            from bot.config import MANUAL_POSITION_PREFIXES  # type: ignore
            return tuple(MANUAL_POSITION_PREFIXES or ())
        except Exception:  # noqa: BLE001 — fence config absent outside fake ctx
            return ()

    def _quirks(self) -> xng.VenueQuirks:
        return xng.VenueQuirks(venue="replay",
                               manual_position_prefixes=self._prefixes(),
                               trail_after_tp_buffer_pct=float(self.be_buffer_pct))

    def _executor(self) -> xng.IntentExecutor:
        return xng.IntentExecutor(self.c, self._jshim, self._quirks(),
                                  alert=self.criticals.append)

    # ------------------------------------------------------ entry pipeline
    def entry(self, plan: Any) -> Any:
        """The §3 fleet pipeline over the REAL SM transitions: t_intent →
        t_mark_sent (commit BEFORE dispatch) → market_open → caller policy
        from the PERSISTED limit_px → t_fill | abort ladder (t_abort_begin
        durable + cooldown armed BEFORE any unwind I/O) → SL ladder via the
        real Reconciler (K6 adoption match → place → registry → t_protect →
        t_open)."""
        from fleet_core.engine.replay import EntryOutcome
        is_long = plan.side == "long"
        eplan = esm.EntryPlan(
            venue=plan.venue, coin=plan.coin, tf=plan.tf, direction=plan.side,
            entry_intended=float(plan.limit_px), sl_initial=float(plan.sl_px),
            tp1=0.0, size=float(plan.size),
            risk_dollars=abs(float(plan.limit_px) - float(plan.sl_px)) * float(plan.size),
            notional=float(plan.limit_px) * float(plan.size),
            leverage_eff=float(plan.leverage_eff), limit_px=float(plan.limit_px),
            entry_bar_ts=int(plan.entry_bar_ts), atr14=plan.atr14 or None,
            tp1_frac=float(plan.tp1_frac),
            client_order_id=plan.client_order_id,
            min_fill_ratio=float(plan.min_fill_ratio),
            cap_tick_tol=float(plan.cap_tick_tol))
        tid = esm.t_intent(self.db, eplan)
        esm.t_mark_sent(self.db, tid)          # invariant §7.1: commit precedes
        try:
            fr = self.c.market_open(plan.coin, is_long, float(plan.size))
        except VenueRejected as e:
            esm.t_abort_begin(self.db, tid, "venue_rejected:%s" % e.reason,
                              cooldown_sec=float(plan.cooldown_sec),
                              now_ts=self.clock.now())
            esm.t_abort_final(self.db, tid, e)
            return EntryOutcome(tid, "ABORTED", "venue_rejected")
        except WriteUnconfirmed:
            return EntryOutcome(tid, "PENDING", "write_unconfirmed_reconcile")
        row = self.j.row(tid)
        with esm._conn(self.db) as con:
            snap = esm.intent_snapshot(con, tid)
        verdict = esm.run_entry_policy(row, fr.avg_px, fr.size, snapshot=snap)
        if not verdict.ok:
            esm.t_abort_begin(self.db, tid, verdict.reason,
                              evidence=verdict.evidence,
                              cooldown_sec=float(plan.cooldown_sec),
                              now_ts=self.clock.now())
            try:
                flat = self.c.ensure_flat(plan.coin)
            except WriteUnconfirmed:
                return EntryOutcome(tid, "ABORTING", "unwind_unconfirmed")
            esm.t_abort_final(self.db, tid, flat)
            return EntryOutcome(tid, "ABORTED", verdict.reason)
        esm.t_fill(self.db, tid, fr, verdict)
        try:
            self._reconciler()._ladder_protect_open(self.j.row(tid))
        except (ReadUnknown, WriteUnconfirmed) as e:
            log.warning("entry %s: SL ladder deferred (%s)", plan.coin, e)
        row = self.j.row(tid)
        state = str(row["sm_state"])
        return EntryOutcome(tid, state,
                            "" if state == "OPEN" else "ladder_incomplete")

    # ------------------------------------------------------ exit-tick path
    def _build_tick(self, tid: int, strategy: Any) -> xng.TickInput:
        row = self.j.row(tid)
        coin = str(row["coin"])
        pos = xng.PosView(
            trade_id=tid, coin=coin, tf=str(row["tf"] or "8h"),
            side=str(row["side"] or row["direction"] or "long"),
            entry=float(row["entry"] or 0.0), size=float(row["size"] or 0.0),
            sl_initial=float(row["sl_initial"] or 0.0),
            sl_current=row["sl_current"], sl_placed_px=row["sl_placed_px"],
            sl_order_id=row["sl_order_id"], tp1_order_id=row["tp1_order_id"],
            tp1_price=row["tp1"], tp1_partial_done=bool(row["tp1_partial_done"]),
            tp1_frac_at_entry=float(row["tp1_frac_at_entry"] or 0.0),
            risk_dollars=float(row["risk_dollars"] or 0.0),
            entry_bar_ts=int(row["entry_bar_ts"] or 0), atr14=row["atr14"],
            origin=str(row["origin"] or "entry"),
            management_class=str(row["management_class"] or "strategy"),
            client_order_id=row["client_order_id"], notes=row["notes"],
            restore_reconcile_pending=(
                str(self.j.kv_get("restore_flag:%d" % tid, 0)) == "1"))
        try:
            positions = self.c.open_positions()
            info = None
            for k, p in positions.items():
                if esm._variants(k) & esm._variants(coin):
                    info = p
                    break
            presence = (xng.Presence(xng.PRESENT, info) if info is not None
                        else xng.Presence(xng.ABSENT))
        except (ReadUnknown, RateLimited):
            presence = xng.Presence(xng.UNKNOWN)
        try:
            sl_live = frozenset(str(o)
                                for o in self.c.list_open_sl_orders(coin))
        except (ReadUnknown, RateLimited):
            sl_live = None
        try:
            mark = self.c.mark_price(coin)
        except (ReadUnknown, RateLimited):
            mark = None
        try:
            liq = self.c.position_liquidation(coin)
        except (ReadUnknown, RateLimited):
            liq = None
        registry = frozenset(self.j.registry_oids()) | \
            frozenset(self.j.historic_oid_corpus(tid))
        prior = int(self.j.kv_get("phantom_miss:%d" % tid, 0) or 0)
        return xng.TickInput(
            pos=pos, presence=presence,
            bars=_StubBars(mark if mark is not None else 0.0),
            mark=mark, sl_live_oids=sl_live, liq_px=liq,
            phantom_misses=prior, registry_oids=registry,
            venue_cfg=self._quirks(),
            strategy=_ScriptedAdapter(strategy, self.j, tid),
            fence_verdict=None, now_ts=self.clock.now())

    @staticmethod
    def _map_result(it: xng.Intent, res: Mapping[str, Any]) -> Any:
        from fleet_core.engine.replay import Intent
        kind = it.kind
        reason = it.reason
        status = res.get("status")
        if status == "refused":
            reason = "REFUSED"
        elif kind == "AdoptResolve" and status == "closed":
            reason = str(res.get("reason", reason))
        elif kind == "MarkTP1Partial" and status == "tp1_marked":
            reason = "executed"
        elif kind == "NoOp" and reason == "steady":
            reason = "nothing_due"        # label bridge (replay vocabulary)
        return Intent(kind, reason, dict(res))

    def tick_row(self, tid: int, strategy: Any = None) -> List[Any]:
        from fleet_core.engine.replay import ScriptedStrategy
        st = strategy or ScriptedStrategy()
        t = self._build_tick(tid, st)
        intents = xng.tick(t)
        results = self._executor().execute(t, intents)
        closed = any(r.get("status") == "closed" for r in results)
        reread_present = any(r.get("why") == "present_on_final_reread"
                             for r in results)
        if closed or reread_present:
            self.j.kv_set("phantom_miss:%d" % tid, 0)
        else:
            self.j.kv_set("phantom_miss:%d" % tid,
                          xng.next_phantom_misses(t.presence,
                                                  t.phantom_misses))
        return [self._map_result(it, res)
                for it, res in zip(intents, results)]

    def executor_try(self, tid: int, intent: Any) -> Any:
        """Direct executor injection (authority-gate tests §7.9/§7.12).
        Maps replay's normalized Intent records onto the typed exit-engine
        intents, runs the REAL IntentExecutor, returns the verdict."""
        from fleet_core.engine.replay import ScriptedStrategy
        t = self._build_tick(tid, ScriptedStrategy())
        p = dict(intent.params or {})
        kind = intent.kind
        row_sz = float(self.j.row(tid)["size"] or 0.0)
        if kind == "Close":
            it: xng.Intent = xng.Close(reason=intent.reason,
                                       ref_px=p.get("ref_px"))
        elif kind == "EscalateNaked":
            it = xng.EscalateNaked(reason=intent.reason,
                                   context=str(p.get("context", "")))
        elif kind == "HealSL":
            it = xng.HealSL(reason=intent.reason,
                            target_px=float(p.get("target_px") or 0.0),
                            size_from_live=float(p.get("size", row_sz)),
                            mode=str(p.get("mode", "strategy")))
        elif kind == "ReplaceSL":
            it = xng.ReplaceSL(reason=intent.reason,
                               target_px=float(p.get("target_px") or 0.0),
                               current_placed_px=p.get("current_placed_px"),
                               cause=str(p.get("cause", "trail")))
        elif kind == "MarkTP1Partial":
            it = xng.MarkTP1Partial(reason=intent.reason,
                                    fill_px=float(p.get("fill_px", 0.0)),
                                    remainder=float(p.get("remainder", row_sz)),
                                    be_px=float(p.get("be_px", 0.0)))
        elif kind == "AdoptResolve":
            it = xng.AdoptResolve(reason=intent.reason, evidence=p)
        elif kind == "Defer":
            it = xng.Defer(reason=intent.reason)
        else:
            it = xng.NoOp(reason=intent.reason)
        results = self._executor().execute(t, [it])
        return self._map_result(it, results[0] if results
                                else {"status": "refused",
                                      "why": "not_executed"})

    # ------------------------------------------- reconciler passes / sweep
    def reconcile_untracked(self) -> None:
        """Direction 2 (entry-SM §4.2) via the REAL Reconciler: fences →
        rule-2 positive provenance (EF1) → rule-3 split, plus the per-tick
        protect_only/manual_claimed watch (3b every-pass CRITICAL cadence)."""
        rec = self._reconciler()
        rec.direction2()
        rec.watch_special_rows()

    def orphan_pass(self) -> List[Tuple[str, str]]:
        """entry-SM §4.2.4 via the REAL registry-gated sweep; returns the
        cancelled (coin, oid) pairs (truth diff — the sweep itself logs)."""
        before = {(t.coin, str(t.oid)) for t in self.c.truth.triggers}
        self._reconciler().sweep_orphan_triggers()
        after = {(t.coin, str(t.oid)) for t in self.c.truth.triggers}
        return sorted(before - after)
