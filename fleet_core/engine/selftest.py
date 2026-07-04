"""fleet_core.engine.selftest — P3 INTEGRATION selftest (offline, fakes only).

Wires the REAL engine components together — entry_sm (SM + Reconciler),
exit_engine (tick + IntentExecutor), registry, migrate_v3 — against the
CONFORMANCE FAKE BINDING (fleet_core/conformance/bindings/fake.py: a
P2-contract-conforming ExchangeClient over the TransportGate fault bank, with
block_sockets() active — zero live I/O by construction) and a TEMP trades.db.

Coverage (task spec):
  1. lifecycle: full entry INTENT→PENDING→FILLED→PROTECTED→OPEN; trail
     replaces (place-before-cancel + 5bps churn gate); TP1→BE; exits with
     attribution (strategy close via ensure_flat + real VWAP; SL-fire
     phantom-K3 oid-first attribution); abort-unwind with durable ABORTING
     (F3: reason + cooldown committed BEFORE the first unwind I/O);
     SL-place-fail-3x → position closed, never naked.
  2. crash matrix K1–K12 (p3_design_entry_sm.md §5): exception-inject a kill
     at EVERY transition boundary, "restart" (fresh client + Reconciler
     startup pass over the same DB/venue truth), assert the designed
     recovery — incl. policy re-run from the PERSISTED limit_px on K4/K9b,
     re-drive-never-resurrect on K9, K12 supersede convergence ≤1 tick.
  3. management-class law: covered-adoption 3a track-only (sl_* NULL, no
     placement while covered, heal at CURRENT mark after cover vanishes),
     3b prefix-manual naked CRITICAL-no-SL every pass, 3c non-prefix naked
     mark∓6%, fenced coins untouchable (REAL orphan_sweep._fenced chain via
     scripted bot.config), manual_claimed FULL hands-off with the phantom-K3
     DB-only row-resolve (review nit #2), executor authority refusals.
  4. shadow-mode zero-write proof: shadow_runner.ReadOnlyExchangeClient
     write fence (every write raises locally + intent recorded),
     runner.ReadOnlyClientProxy, RecorderExecutor jsonl capture, raw-socket
     block proof — zero venue write ops across the whole shadow section.
  5. replay: `python -m fleet_core.engine.replay --all` green (subprocess).

Run:  /usr/bin/python3 -m fleet_core.engine.selftest        (from repo root)
Exit: 0 all green, 1 failures.

Scripted fences: the REAL P1 fence (orphan_sweep._fenced, md5-pinned) reads
bot.config/bot.universe lazily (ImportError = fence undefined on the venue).
This selftest injects a SCRIPTED `bot` module (FX_EXCLUDE={'BNB'},
MANUAL_POSITION_PREFIXES=('xyz_',), UNIVERSE_SYMBOL_EXCLUDE={'DOGE'},
_is_fx=EUR*) so the canonical fence CHAIN is exercised offline — injection
happens inside main(), never at import time.

Pure stdlib + fleet_core (no venue SDKs, no pandas, no `bot` package beyond
the scripted stub above).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from fleet_core.exchange_api import (ExchangeClient, FillResult, FlatResult,
                                     ReadUnknown, SLOrderInfo, VenueRejected,
                                     WriteUnconfirmed)
from fleet_core.engine import entry_sm as esm
from fleet_core.engine import exit_engine as xng
from fleet_core.engine import migrate_v3 as mig
from fleet_core.engine import registry as reg
from fleet_core.conformance.faults import UninterceptedRealCall

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]

# venue WRITE ops on the fake TransportGate (classify() POST map) — the
# zero-venue-write proofs count these.
VENUE_WRITE_OPS = frozenset({"place_order", "place_trigger", "cancel_order",
                             "leverage"})

ENTRY_ABORT_COOLDOWN_SEC = 900.0   # pac 900s == ext/nado/hl 15 min (design §8)


# ============================================================================
# check plumbing
# ============================================================================

_FAILURES: List[str] = []
_N_CHECKS = [0]
_QUIET_OK = os.environ.get("SELFTEST_QUIET_OK", "").strip() in ("1", "true")


def check(name: str, cond: bool, detail: str = "") -> bool:
    _N_CHECKS[0] += 1
    if cond:
        if not _QUIET_OK:
            print("  [ok] %s" % name)
    else:
        print("  [FAIL] %s %s" % (name, detail))
        _FAILURES.append(name)
    return bool(cond)


class LogSpy(logging.Handler):
    """Capture CRITICAL records from the engine loggers (Reconciler alerts)."""

    def __init__(self) -> None:
        super().__init__(level=logging.CRITICAL)
        self.records: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.records.append(record.getMessage())
        except Exception:
            self.records.append(record.msg if isinstance(record.msg, str) else "?")

    def count(self, needle: str) -> int:
        return sum(1 for m in self.records if needle in m)


# ============================================================================
# scripted fence config (REAL orphan_sweep._fenced chain, offline)
# ============================================================================

def install_scripted_fences() -> None:
    bot = types.ModuleType("bot")
    bot.__path__ = []  # mark as package
    cfg = types.ModuleType("bot.config")
    cfg.FX_EXCLUDE = {"BNB"}
    cfg.MANUAL_POSITION_PREFIXES = ("xyz_",)
    uni = types.ModuleType("bot.universe")
    uni.UNIVERSE_SYMBOL_EXCLUDE = {"DOGE"}
    uni._is_fx = lambda coin: str(coin).upper().startswith("EUR")
    bot.config = cfg
    bot.universe = uni
    sys.modules["bot"] = bot
    sys.modules["bot.config"] = cfg
    sys.modules["bot.universe"] = uni


# ============================================================================
# tiny bar-frame + scripted strategy (strategy OUTPUTS as data — zero math)
# ============================================================================

class _Col:
    def __init__(self, vals: Sequence[float]) -> None:
        self._v = list(vals)

    @property
    def iloc(self) -> "_Col":
        return self

    def __getitem__(self, i: int) -> float:
        return self._v[i]


class TinyDF:
    """Duck-typed CLOSED-bars stand-in: len() + df['Close'].iloc[-1]."""

    def __init__(self, closes: Sequence[float]) -> None:
        self._closes = list(closes)

    def __len__(self) -> int:
        return len(self._closes)

    def __getitem__(self, col: str):
        if col == "Close":
            return _Col(self._closes)
        raise KeyError(col)


class ScriptedStrategy:
    """StrategyAdapter-shaped scriptable stand-in. The engine byte-copies the
    real math (md5-pinned in strategy_math); scenarios here script strategy
    OUTPUTS as data, exactly like replay's ScriptedStrategy."""

    def __init__(self, new_sl: Optional[float] = None,
                 exit_reason: Optional[str] = None,
                 sl_hit: Optional[Tuple[float, str]] = None,
                 entry_bar: bool = False) -> None:
        self.new_sl = new_sl
        self.exit_reason = exit_reason
        self.sl_hit = sl_hit
        self.entry_bar = entry_bar

    def update_sl_on_new_bar(self, pos, df):
        return self.new_sl, self.exit_reason

    def is_entry_bar(self, pos, df) -> bool:
        return self.entry_bar

    def check_sl_hit(self, pos, df, vstop_wick_check: bool = True):
        return self.sl_hit

    def sl_current_view(self, pos) -> float:
        return float(pos.sl_current if pos.sl_current is not None
                     else pos.sl_initial)

    def forget(self, trade_id: int) -> None:
        pass


# ============================================================================
# DB-backed JournalPort (exit-engine documented surface over the temp trades.db)
# ============================================================================

class DBJournal:
    """JournalPort implementation over the SM trades.db — P1 journal-identical
    SQL for the sl/tp1 fields, entry_sm.t_close for OPEN→CLOSED, registry for
    the F5 oid INSERT-before-dependents discipline."""

    def __init__(self, db_path) -> None:
        self.db = str(db_path)

    def _exec(self, sql: str, args: Sequence[Any]) -> None:
        with esm._conn(self.db) as con:
            con.execute(sql, list(args))

    def update_trade_sl(self, trade_id: int, new_sl: float) -> None:
        self._exec("UPDATE trades SET sl_current=? WHERE id=?", (new_sl, trade_id))

    def update_trade_sl_order(self, trade_id: int, sl_order_id: str) -> None:
        self._exec("UPDATE trades SET sl_order_id=? WHERE id=?",
                   (sl_order_id, trade_id))

    def update_trade_sl_placed(self, trade_id: int, sl_placed_px: float) -> None:
        self._exec("UPDATE trades SET sl_placed_px=?, sl_confirmed_at=? WHERE id=?",
                   (sl_placed_px, esm._now_iso(), trade_id))

    def mark_tp1_partial(self, trade_id: int, tp1_fill_price: float,
                         remaining_size: float, new_sl: float) -> None:
        # journal.mark_tp1_partial-identical fields
        self._exec("UPDATE trades SET tp1_partial_done=1, tp1_fill_price=?, "
                   "size=?, sl_current=?, tp1_order_id=NULL WHERE id=?",
                   (tp1_fill_price, remaining_size, new_sl, trade_id))

    def close_trade(self, trade_id: int, exit_price: float, exit_reason: str,
                    pnl_dollars: float, realized_r: float) -> None:
        esm.t_close(self.db, trade_id, exit_price, exit_reason,
                    pnl_dollars, realized_r)

    def register_placed_trigger_oid(self, oid, coin: str = "",
                                    trade_id: Optional[int] = None,
                                    kind: str = "sl") -> None:
        reg.register_oid(self.db, oid, coin or "", trade_id=trade_id,
                         kind=kind if kind in reg.VALID_KINDS else "sl")

    def get_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            v = esm._bot_state_get(con, key)
        return default if v is None else v

    def set_state(self, key: str, value: str) -> None:
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            esm._bot_state_set(con, key, value)

    def clear_restore_flag(self, trade_id: int) -> None:
        self.set_state("restore_flag:%d" % trade_id, "0")


# ============================================================================
# kill-point injection
# ============================================================================

class KillPoint(RuntimeError):
    """Simulated process death (kill -9 / OOM / watchdog os._exit) at a named
    transition boundary. Everything already COMMITTED stays on disk; everything
    in-flight is lost — exactly the §5 crash model."""


class RejectTriggersProxy:
    """Thin P2 proxy: the venue rejects trigger placements N times (nado
    trigger-min class), everything else passes through — drives the
    sl_placement_failed_3x abort ladder against the real fake venue."""

    def __init__(self, inner: ExchangeClient, n: int = 999) -> None:
        self._inner = inner
        self.left = n

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        if self.left > 0:
            self.left -= 1
            raise VenueRejected("selftest: trigger rejected", reason="min trigger size")
        return self._inner.trigger_sl(coin, is_buy, sz, trigger_px)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# ============================================================================
# World — one isolated integration universe per scenario
# ============================================================================

class World:
    """Conformance fake venue (TransportGate + block_sockets) + temp trades.db
    + real engine components. STRICTLY one world at a time (socket patcher is
    global state; LIFO close)."""

    _seq = [0]

    def __init__(self, name: str) -> None:
        from fleet_core.conformance.bindings import fake as fake_binding
        self.name = name
        self.tmp = Path(tempfile.mkdtemp(prefix="fleet_selftest_%s_" % name))
        self.db = self.tmp / "trades.db"
        con = mig._connect(self.db)
        try:
            mig.ensure_schema(con)
            con.commit()
        finally:
            con.close()
        self.ctx = fake_binding.build_context()
        self.client: ExchangeClient = self.ctx.client
        self.gate = self.ctx.gate
        self.scenario = self.ctx.scenario
        self.venue = self.scenario.venue
        self.alerts: List[str] = []
        self.cfg = esm.ReconcilerConfig(
            venue="fake",
            manual_position_prefixes=("xyz_",),
            orphan_debounce_sec=0.0,           # test cadence (2-obs law kept)
            entry_abort_cooldown_sec=ENTRY_ABORT_COOLDOWN_SEC)
        self.quirks = xng.VenueQuirks(venue="fake",
                                      manual_position_prefixes=("xyz_",))

    # ------------------------------------------------------------ lifecycle
    def close(self) -> None:
        try:
            self.ctx.close()
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)

    def __enter__(self) -> "World":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -------------------------------------------------------------- helpers
    def row(self, tid: int) -> sqlite3.Row:
        with esm._conn(self.db) as con:
            return esm._get_row(con, tid)

    def rows(self) -> List[sqlite3.Row]:
        with esm._conn(self.db) as con:
            return con.execute("SELECT * FROM trades ORDER BY id").fetchall()

    def states(self, tid: int) -> List[str]:
        """sm_transitions to_state path for the trade (SM states only)."""
        with esm._conn(self.db) as con:
            rows = con.execute(
                "SELECT to_state FROM sm_transitions WHERE trade_id=? ORDER BY id",
                (tid,)).fetchall()
        return [r["to_state"] for r in rows if r["to_state"] in esm.ALL_STATES]

    def transition_details(self, tid: int) -> List[Dict[str, Any]]:
        with esm._conn(self.db) as con:
            rows = con.execute(
                "SELECT to_state, detail FROM sm_transitions WHERE trade_id=? "
                "ORDER BY id", (tid,)).fetchall()
        out = []
        for r in rows:
            try:
                d = json.loads(r["detail"] or "{}")
            except (ValueError, TypeError):
                d = {}
            d["__to"] = r["to_state"]
            out.append(d)
        return out

    def rejected(self) -> List[sqlite3.Row]:
        with esm._conn(self.db) as con:
            return con.execute("SELECT * FROM rejected_signals ORDER BY id").fetchall()

    def kv_get(self, key: str) -> Optional[str]:
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            return esm._bot_state_get(con, key)

    def kv_set(self, key: str, value: str) -> None:
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            esm._bot_state_set(con, key, value)

    def write_ops(self, since: int = 0, coin: Optional[str] = None
                  ) -> List[Dict[str, Any]]:
        out = []
        for c in self.gate.calls[since:]:
            if c["op"] not in VENUE_WRITE_OPS:
                continue
            if coin is not None:
                body = c.get("body") or {}
                if isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except Exception:
                        body = {}
                if body.get("coin") != coin:
                    continue
            out.append(c)
        return out

    def call_mark(self) -> int:
        return len(self.gate.calls)

    def triggers(self, coin: str) -> List[str]:
        return self.scenario.trigger_oids(coin)

    def trigger_px(self, oid: str) -> Optional[float]:
        return self.scenario.trigger_px_of(oid)

    def position(self, coin: str) -> Optional[Dict[str, Any]]:
        return self.scenario.position(coin)

    def drop_position(self, coin: str) -> None:
        for scope in self.venue.positions:
            self.venue.positions[scope].pop(coin, None)

    def resize_position(self, coin: str, size_signed: float) -> None:
        p = self.venue.find_position(coin)
        assert p is not None, "resize of absent position"
        p["size_signed"] = float(size_signed)

    def drop_trigger(self, oid: str) -> None:
        self.venue.orders.pop(str(oid), None)

    def add_fill(self, coin: str, side: str, px: float, size: float,
                 oid: Optional[str] = None, reduce_only: bool = True) -> None:
        self.venue.fills.insert(0, {
            "oid": oid or ("fill-%d" % int(time.time() * 1e6)),
            "client_oid": None, "coin": coin, "side": side, "px": float(px),
            "size": float(size), "reduce_only": bool(reduce_only),
            "time": time.time()})

    def mark(self, coin: str) -> float:
        return float(self.venue.marks[coin])

    def set_mark(self, coin: str, px: float) -> None:
        self.scenario.seed_mark(coin, px)

    # --------------------------------------------------------------- engine
    def reconciler(self, client: Optional[ExchangeClient] = None) -> esm.Reconciler:
        return esm.Reconciler(self.db, client or self.client, cfg=self.cfg)

    def restart(self) -> esm.Reconciler:
        """Process restart: fresh client (fresh caches — the venue does not
        die with the bot), fresh Reconciler, startup full pass (§4)."""
        self.client = self.ctx.make_client()
        rec = self.reconciler()
        rec.startup_pass()
        return rec

    def journal(self) -> DBJournal:
        return DBJournal(self.db)

    # ----------------------------------------------------------- entry plan
    def plan(self, coin: str = "BTC", direction: str = "long",
             size: float = 1.0, sl_pct: float = 0.05,
             limit_cap_pct: float = 0.005, tp_pct: float = 0.08,
             tp1_frac: float = 0.0,
             limit_px: Optional[float] = None) -> esm.EntryPlan:
        World._seq[0] += 1
        mark = self.mark(coin)
        is_long = direction == "long"
        sl = mark * (1.0 - sl_pct) if is_long else mark * (1.0 + sl_pct)
        tp = mark * (1.0 + tp_pct) if is_long else mark * (1.0 - tp_pct)
        cap = limit_px if limit_px is not None else (
            mark * (1.0 + limit_cap_pct) if is_long
            else mark * (1.0 - limit_cap_pct))
        bar_ms = 8 * 3600 * 1000
        bar_ts = (int(time.time() * 1000) // bar_ms) * bar_ms
        return esm.EntryPlan(
            venue="fake", coin=coin, tf="8h", direction=direction,
            entry_intended=mark, sl_initial=sl, tp1=tp, size=size,
            risk_dollars=abs(mark - sl) * size, notional=mark * size,
            leverage_eff=3.0, limit_px=cap, entry_bar_ts=bar_ts,
            atr14=mark * 0.01, tp1_frac=tp1_frac,
            client_order_id=esm.make_client_order_id(
                "fake", coin, "8h", bar_ts, World._seq[0]))

    # -------------------------------------------- entry pipeline (§3 order)
    def run_entry(self, plan: esm.EntryPlan, kill_at: Optional[str] = None,
                  client: Optional[ExchangeClient] = None
                  ) -> Tuple[str, Optional[int]]:
        """The §3 fleet entry pipeline over the REAL SM functions + P2 client,
        with named kill boundaries (crash matrix §5). Returns (outcome, tid)."""
        cl = client or self.client
        is_long = plan.direction == "long"

        def kill(point: str) -> None:
            if kill_at == point:
                raise KillPoint(point)

        kill("K1")
        # cooldown re-check (pre-INTENT fleet order, F11a)
        if esm.cooldown_active(self.db, plan.coin):
            return ("rejected_cooldown", None)
        tid = esm.t_intent(self.db, plan)
        kill("K2")
        esm.t_mark_sent(self.db, tid)
        kill("K3")
        try:
            fr = cl.market_open(plan.coin, is_long, plan.size,
                                intended_px=plan.entry_intended)
        except VenueRejected as e:
            esm.t_abort_begin(self.db, tid, "venue_rejected:%s" % e.reason,
                              cooldown_sec=ENTRY_ABORT_COOLDOWN_SEC)
            esm.t_abort_final(self.db, tid, e)
            return ("aborted", tid)
        except WriteUnconfirmed:
            # §3: WriteUnconfirmed from market_open leaves PENDING for the
            # reconciler — never guess a fill.
            return ("pending_reconcile", tid)
        kill("K4")  # response lost: fill landed, never processed
        row = self.row(tid)
        with esm._conn(self.db) as con:
            snap = esm.intent_snapshot(con, tid)
        verdict = esm.run_entry_policy(row, fr.avg_px, fr.size, snapshot=snap)
        if not verdict.ok:
            kill("K9b")  # reject decided, ABORTING not yet durable
            esm.t_abort_begin(self.db, tid, verdict.reason,
                              evidence=verdict.evidence,
                              cooldown_sec=ENTRY_ABORT_COOLDOWN_SEC)
            self.f3_probe = self._f3_probe(tid, plan.coin)  # F3 evidence
            kill("K9")   # ABORTING durable; killed inside the unwind
            try:
                flat = cl.ensure_flat(plan.coin)
            except WriteUnconfirmed:
                return ("aborting_unconfirmed", tid)  # reconciler re-drives
            esm.t_abort_final(self.db, tid, flat)
            return ("aborted", tid)
        esm.t_fill(self.db, tid, fr, verdict)
        kill("K5")
        # liq-guard the fresh strategy SL (protective-placement LAW gates
        # protective/recovery placements; the fresh entry SL is liq-clamped only)
        try:
            liq = cl.position_liquidation(plan.coin)
        except ReadUnknown:
            liq = None
        sl_target, _ = esm.liq_guard_clamp(is_long, plan.sl_initial, liq)
        sl_info: Optional[SLOrderInfo] = None
        for _attempt in range(3):
            try:
                sl_info = cl.trigger_sl(plan.coin, not is_long, fr.size, sl_target)
                break
            except VenueRejected:
                continue
            except WriteUnconfirmed as e:
                if e.may_have_landed is False:
                    continue
                # ambiguous land — the DESIGNED resolution is the reconciler's
                # K6 adoption match, never a blind second place.
                return ("filled_sl_unconfirmed", tid)
        if sl_info is None:
            esm.t_abort_begin(self.db, tid,
                              "sl_placement_failed_3x_naked_position_closed",
                              cooldown_sec=ENTRY_ABORT_COOLDOWN_SEC)
            try:
                flat = cl.ensure_flat(plan.coin)
            except WriteUnconfirmed:
                return ("aborting_unconfirmed", tid)
            esm.t_abort_final(self.db, tid, flat)
            return ("aborted", tid)
        kill("K6")  # SL resting on venue; registry INSERT + t_protect lost
        reg.register_oid(self.db, sl_info.oid, plan.coin, trade_id=tid, kind="sl")
        esm.t_protect(self.db, tid, sl_info)
        kill("K7")
        esm.t_open(self.db, tid)
        esm.incr_opens_today(self.db)
        kill("K8")
        return ("open", tid)

    def _f3_probe(self, tid: int, coin: str) -> Dict[str, Any]:
        """State snapshot AT the unwind boundary (before any unwind I/O)."""
        r = self.row(tid)
        return {"sm_state": r["sm_state"], "abort_reason": r["abort_reason"],
                "cooldown_armed": esm.cooldown_active(self.db, coin)}

    # ------------------------------------------------------ exit-engine tick
    def build_tick(self, tid: int, strategy: Optional[Any] = None,
                   bars: Optional[TinyDF] = None,
                   client: Optional[ExchangeClient] = None,
                   fence_verdict: Optional[str] = None) -> xng.TickInput:
        cl = client or self.client
        r = self.row(tid)
        pos = xng.PosView(
            trade_id=tid, coin=r["coin"], tf=r["tf"],
            side=r["direction"] or "long", entry=float(r["entry"] or 0.0),
            size=float(r["size"] or 0.0),
            sl_initial=float(r["sl_initial"] or 0.0),
            sl_current=r["sl_current"], sl_placed_px=r["sl_placed_px"],
            sl_order_id=r["sl_order_id"], tp1_order_id=r["tp1_order_id"],
            tp1_price=r["tp1"], tp1_partial_done=bool(r["tp1_partial_done"]),
            tp1_frac_at_entry=float(r["tp1_frac_at_entry"] or 0.0),
            risk_dollars=float(r["risk_dollars"] or 0.0),
            entry_bar_ts=int(r["entry_bar_ts"] or 0), atr14=r["atr14"],
            origin=r["origin"] or "entry",
            management_class=r["management_class"] or "strategy",
            client_order_id=r["client_order_id"], notes=r["notes"])
        try:
            pmap = cl.open_positions()
            info = None
            for k, p in pmap.items():
                if esm._variants(k) & esm._variants(pos.coin):
                    info = p
                    break
            presence = (xng.Presence(xng.PRESENT, info) if info is not None
                        else xng.Presence(xng.ABSENT))
        except ReadUnknown:
            presence = xng.Presence(xng.UNKNOWN)
        try:
            sl_live: Optional[frozenset] = frozenset(
                str(o) for o in cl.list_open_sl_orders(pos.coin))
        except ReadUnknown:
            sl_live = None
        try:
            mark = cl.mark_price(pos.coin, 5.0)
        except (ReadUnknown, Exception) as e:  # noqa: BLE001 — mark optional
            mark = None if isinstance(e, ReadUnknown) else None
        try:
            liq = cl.position_liquidation(pos.coin)
        except ReadUnknown:
            liq = None
        prior = int(self.kv_get("phantom_miss:%d" % tid) or 0)
        return xng.TickInput(
            pos=pos, presence=presence, bars=bars, mark=mark,
            sl_live_oids=sl_live, liq_px=liq, phantom_misses=prior,
            registry_oids=frozenset(reg.oids_ever_placed(self.db)),
            venue_cfg=self.quirks, strategy=strategy,
            fence_verdict=fence_verdict, now_ts=time.time())

    def executor(self, client: Optional[ExchangeClient] = None
                 ) -> xng.IntentExecutor:
        return xng.IntentExecutor(client or self.client, self.journal(),
                                  self.quirks, alert=self.alerts.append)

    def engine_tick(self, tid: int, strategy: Optional[Any] = None,
                    bars: Optional[TinyDF] = None
                    ) -> Tuple[xng.TickInput, List[Any], List[Mapping[str, Any]]]:
        """One real engine tick for one row: build inputs from live venue
        reads → pure tick() → IntentExecutor → persist the phantom counter
        (runner duty, next_phantom_misses rule)."""
        t = self.build_tick(tid, strategy=strategy, bars=bars)
        intents = xng.tick(t)
        results = self.executor().execute(t, intents)
        closed = any(res.get("status") == "closed" for res in results)
        nxt = 0 if closed else xng.next_phantom_misses(t.presence, t.phantom_misses)
        self.kv_set("phantom_miss:%d" % tid, str(nxt))
        return t, intents, results


def legacy_consistent(w: World) -> bool:
    """Invariant §7.7: legacy status column always matches the §2.3 map."""
    for r in w.rows():
        sm = r["sm_state"]
        if sm and esm.LEGACY_STATUS.get(sm) != r["status"]:
            return False
    return True


def money_protected(w: World) -> bool:
    """No STRATEGY-row venue position may sit without a resting trigger after
    recovery (money never unprotected; 3b prefix-manual rows are exempt BY
    design — CRITICAL-no-SL)."""
    live_coins = {r["coin"] for r in w.rows()
                  if r["sm_state"] in esm.LIVE_STATES + (esm.OPEN,)
                  and (r["management_class"] or "strategy") == "strategy"}
    for scope in w.venue.positions:
        for coin in w.venue.positions[scope]:
            if coin in live_coins and not w.triggers(coin):
                return False
    return True


def op_index(w: World, op: str, pred: Callable[[Dict[str, Any]], bool],
             since: int = 0) -> int:
    """Index (in gate.calls) of the first call with `op` matching pred."""
    for i, c in enumerate(w.gate.calls[since:], start=since):
        if c["op"] != op:
            continue
        body = c.get("body") or {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        if pred(body):
            return i
    return -1


# ============================================================================
# SECTION 1 — lifecycle
# ============================================================================

def s_entry_lifecycle() -> None:
    print("— lifecycle: full entry INTENT→OPEN")
    with World("entry") as w:
        p = w.plan("BTC")
        out, tid = w.run_entry(p)
        r = w.row(tid)
        fill_px = w.mark("BTC") * 1.0005  # fake taker slip, buy side
        check("entry reaches OPEN", out == "open" and r["sm_state"] == esm.OPEN)
        check("SM path INTENT→PENDING→FILLED→PROTECTED→OPEN",
              w.states(tid) == [esm.INTENT, esm.PENDING, esm.FILLED,
                                esm.PROTECTED, esm.OPEN], str(w.states(tid)))
        check("entry px = REAL venue fill, not intended",
              abs(float(r["entry"]) - fill_px) < 1e-6,
              "entry=%s fill=%s" % (r["entry"], fill_px))
        trigs = w.triggers("BTC")
        check("SL resting on venue + oid persisted",
              len(trigs) == 1 and str(r["sl_order_id"]) == trigs[0])
        check("sl_placed_px == sl_current == venue-ACCEPTED trigger px",
              r["sl_placed_px"] == r["sl_current"] ==
              w.trigger_px(r["sl_order_id"]))
        check("registry INSERT before promote (F5)",
              str(r["sl_order_id"]) in reg.oids_ever_placed(w.db))
        check("legacy status mapping ('open')", r["status"] == "open")
        check("client_order_id idempotent replay (t_intent)",
              esm.t_intent(w.db, p) == tid)
        check("opens_today persisted", esm.opens_today(w.db) == 1)
        check("legacy column consistent", legacy_consistent(w))
        # first tick after open: steady NoOp/Defer (cutover invariant shape)
        _, intents, _ = w.engine_tick(tid, strategy=ScriptedStrategy(),
                                      bars=TinyDF([w.mark("BTC")] * 3))
        check("first tick steady (NoOp/Defer only)",
              all(i.kind in ("NoOp", "Defer") for i in intents),
              str([i.kind for i in intents]))


def s_trail_and_churn() -> None:
    print("— lifecycle: trail replace (place-before-cancel) + churn gate")
    with World("trail") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        old_oid = str(w.row(tid)["sl_order_id"])
        w.set_mark("BTC", 54_000.0)           # price ran up; trail follows
        mark0 = w.call_mark()
        target = 49_000.0
        _, intents, results = w.engine_tick(
            tid, strategy=ScriptedStrategy(new_sl=target),
            bars=TinyDF([54_000.0] * 3))
        r = w.row(tid)
        check("trail → ReplaceSL(trail) executed",
              any(i.kind == "ReplaceSL" and i.cause == "trail" for i in intents)
              and any(res.get("status") == "replaced" for res in results))
        new_oid = str(r["sl_order_id"])
        i_place = op_index(w, "place_trigger",
                           lambda b: abs(float(b.get("trigger_px") or 0) - target) < 1e-6,
                           since=mark0)
        i_cancel = op_index(w, "cancel_order",
                            lambda b: str(b.get("oid")) == old_oid, since=mark0)
        check("place-BEFORE-cancel on the venue wire",
              0 <= i_place < i_cancel, "place=%d cancel=%d" % (i_place, i_cancel))
        check("old SL superseded, exactly one resting",
              w.triggers("BTC") == [new_oid] and new_oid != old_oid)
        check("persisted sl_current == sl_placed_px == accepted px",
              r["sl_current"] == r["sl_placed_px"] == w.trigger_px(new_oid))
        check("new oid in registry (step 3a)",
              new_oid in reg.oids_ever_placed(w.db))
        # churn gate: ≤5bps → NO ReplaceSL intent
        _, intents2, _ = w.engine_tick(
            tid, strategy=ScriptedStrategy(new_sl=target * 1.0002),
            bars=TinyDF([54_000.0] * 3))
        check("5bps churn gate → no replace",
              not any(i.kind == "ReplaceSL" for i in intents2),
              str([i.kind for i in intents2]))


def s_tp1_be() -> None:
    print("— lifecycle: TP1 partial detect → BE replace (EF3 buffered math)")
    with World("tp1") as w:
        # EF3: buffer explicitly seeded to the ext-config live .env value
        # (bots/extended/.env TRAIL_AFTER_TP_BUFFER_PCT=0.003 — design §8).
        w.quirks = xng.VenueQuirks(venue="extended",
                                   manual_position_prefixes=("xyz_",),
                                   trail_after_tp_buffer_pct=0.003)
        _, tid = w.run_entry(w.plan("BTC", tp1_frac=0.5))
        r0 = w.row(tid)
        old_oid = str(r0["sl_order_id"])
        entry = float(r0["entry"])
        # venue: price reached TP1, half the position was taken off
        w.set_mark("BTC", 54_000.0)
        w.resize_position("BTC", 0.5)
        w.add_fill("BTC", "sell", float(r0["tp1"]), 0.5, reduce_only=True)
        _, intents, results = w.engine_tick(
            tid, strategy=ScriptedStrategy(), bars=TinyDF([54_000.0] * 3))
        r = w.row(tid)
        check("TP1 partial detected (live < 0.9×journal)",
              any(i.kind == "MarkTP1Partial" for i in intents))
        check("journal: tp1_partial_done=1, size=remainder",
              r["tp1_partial_done"] == 1 and float(r["size"]) == 0.5)
        be_expected = entry * (1.0 - 0.003)   # live: entry×(1−buf), raise-only
        check("EF3: SL moved to the BUFFERED BE entry×(1−buf) via the replace "
              "primitive (accepted px persisted)",
              r["sl_current"] == r["sl_placed_px"] == w.trigger_px(r["sl_order_id"])
              and abs(float(r["sl_current"]) - be_expected) < 1.0,
              "sl=%s expected=%s" % (r["sl_current"], be_expected))
        check("EF3: raw entry NEVER the BE target when buffer>0",
              abs(float(r["sl_current"]) - entry) > entry * 0.002,
              "sl=%s entry=%s" % (r["sl_current"], entry))
        check("old SL cancelled after BE place",
              old_oid not in w.triggers("BTC") and len(w.triggers("BTC")) == 1)
        check("tp1 executed status", any(res.get("status") == "tp1_marked"
                                         for res in results))


def s_exit_strategy_close() -> None:
    print("— lifecycle: strategy exit (time_stop) — §9 ordering + real VWAP")
    with World("xclose") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        sl_oid = str(w.row(tid)["sl_order_id"])
        mark0 = w.call_mark()
        _, intents, results = w.engine_tick(
            tid, strategy=ScriptedStrategy(exit_reason="time_stop"),
            bars=TinyDF([50_000.0] * 3))
        r = w.row(tid)
        exp_px = 50_000.0 * (1 - 0.0005)  # sell-side slip on the venue close
        check("Close(time_stop) emitted + executed",
              any(i.kind == "Close" and i.reason == "time_stop" for i in intents)
              and any(res.get("status") == "closed" for res in results))
        check("row CLOSED with exit_reason=time_stop",
              r["sm_state"] == esm.CLOSED and r["exit_reason"] == "time_stop")
        check("exit px = REAL close VWAP (never the ref)",
              abs(float(r["exit_price"]) - exp_px) < 1e-6,
              "exit=%s exp=%s" % (r["exit_price"], exp_px))
        check("venue flat + SL cancelled AFTER flat",
              w.position("BTC") is None and w.triggers("BTC") == [])
        i_close = op_index(w, "place_order",
                           lambda b: b.get("reduce_only") is True, since=mark0)
        i_cancel = op_index(w, "cancel_order",
                            lambda b: str(b.get("oid")) == sl_oid, since=mark0)
        check("§9 order: ensure_flat close precedes SL cancel",
              0 <= i_close < i_cancel, "close=%d cancel=%d" % (i_close, i_cancel))
        check("pnl/realized_r journaled",
              r["pnl_dollars"] is not None and r["realized_r"] is not None)


def s_exit_sl_fire_attribution() -> None:
    print("— lifecycle: venue SL fire → phantom K=3 → oid-first attribution")
    with World("slfire") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        # trail once so trailed=True (sl_current != sl_initial)
        w.set_mark("BTC", 54_000.0)
        w.engine_tick(tid, strategy=ScriptedStrategy(new_sl=49_000.0),
                      bars=TinyDF([54_000.0] * 3))
        r = w.row(tid)
        sl_oid = str(r["sl_order_id"])
        # UNKNOWN read never counts a miss
        w.gate.fail("positions", "timeout", count=1)
        _, intents_u, _ = w.engine_tick(tid, strategy=ScriptedStrategy())
        check("presence UNKNOWN → Defer, counter frozen",
              intents_u[0].kind == "Defer" and
              intents_u[0].reason == "presence_unknown" and
              w.kv_get("phantom_miss:%d" % tid) == "0")
        w.gate.clear_faults()
        # venue: SL fired — position gone, trigger consumed, close fill exists
        w.drop_position("BTC")
        w.drop_trigger(sl_oid)
        w.add_fill("BTC", "sell", 48_990.0, 1.0, oid=sl_oid, reduce_only=True)
        wmark = w.call_mark()
        kinds: List[str] = []
        for _ in range(3):
            _, intents, results = w.engine_tick(tid, strategy=ScriptedStrategy())
            kinds.append(intents[0].kind)
        r = w.row(tid)
        check("K=3 debounce: Defer, Defer, AdoptResolve",
              kinds == ["Defer", "Defer", "AdoptResolve"], str(kinds))
        check("closed with oid-first attribution 'trail_sl' (trailed row)",
              r["sm_state"] == esm.CLOSED and r["exit_reason"] == "trail_sl")
        check("exit px = the SL fill px", float(r["exit_price"]) == 48_990.0)
        check("attribution never 'manual'", r["exit_reason"] != "manual")
        check("phantom resolve was DB-only (zero venue writes)",
              len(w.write_ops(since=wmark)) == 0)
        check("phantom counter reset after close",
              w.kv_get("phantom_miss:%d" % tid) == "0")


def s_abort_unwind_lifecycle() -> None:
    print("— lifecycle: cap-breach abort — ABORTING durable BEFORE unwind (F3)")
    with World("abort") as w:
        p = w.plan("BTC", limit_px=49_000.0)  # fill ≈50_025 breaches the cap
        out, tid = w.run_entry(p)
        r = w.row(tid)
        probe = getattr(w, "f3_probe", {})
        check("policy reject → ABORTED (unwound)",
              out == "aborted" and r["sm_state"] == esm.ABORTED)
        check("F3: ABORTING + abort_reason durable BEFORE first unwind I/O",
              probe.get("sm_state") == esm.ABORTING and
              probe.get("abort_reason") == "entry_cap_breach_vs_persisted_limit_px",
              str(probe))
        check("F3: cooldown armed in the SAME commit (pre-unwind)",
              probe.get("cooldown_armed") is True)
        check("venue flat after unwind", w.position("BTC") is None)
        check("rejected_signals row written (reporting preserved)",
              any(row["reason"] == "entry_cap_breach_vs_persisted_limit_px"
                  for row in w.rejected()))
        check("row kept (never deleted) with full path",
              w.states(tid) == [esm.INTENT, esm.PENDING, esm.ABORTING,
                                esm.ABORTED], str(w.states(tid)))
        out2, tid2 = w.run_entry(w.plan("BTC"))
        check("cooldown blocks the refire (pre-INTENT gate)",
              out2 == "rejected_cooldown" and tid2 is None)
        check("legacy column consistent", legacy_consistent(w))


def s_sl_fail_3x() -> None:
    print("— lifecycle: SL placement fails 3× → position closed, never naked")
    with World("slfail") as w:
        rej = RejectTriggersProxy(w.client, n=999)
        out, tid = w.run_entry(w.plan("BTC"), client=rej)
        r = w.row(tid)
        check("3× reject → abort ladder → ABORTED",
              out == "aborted" and r["sm_state"] == esm.ABORTED)
        check("abort_reason = sl_placement_failed_3x_naked_position_closed",
              r["abort_reason"] == "sl_placement_failed_3x_naked_position_closed")
        check("venue flat (money never left naked)",
              w.position("BTC") is None and w.triggers("BTC") == [])
        check("cooldown armed", esm.cooldown_active(w.db, "BTC"))


# ============================================================================
# SECTION 2 — crash matrix K1–K12
# ============================================================================

CRASH_MATRIX_COVERED: List[str] = []


def _covered(entry: str) -> None:
    CRASH_MATRIX_COVERED.append(entry)


def _run_kill(w: World, plan: esm.EntryPlan, kill_at: str) -> Optional[int]:
    """Run the pipeline expecting a KillPoint; return tid found on disk."""
    try:
        w.run_entry(plan, kill_at=kill_at)
        raise AssertionError("kill %s did not fire" % kill_at)
    except KillPoint:
        pass
    rows = w.rows()
    return int(rows[-1]["id"]) if rows else None


def k1_before_intent() -> None:
    print("— K1: kill before t_intent — no row, signal refires")
    with World("k1") as w:
        tid = _run_kill(w, w.plan("BTC"), "K1")
        check("K1: no row on disk", tid is None and len(w.rows()) == 0)
        w.restart()
        check("K1: recovery is a no-op (nothing to recover)", len(w.rows()) == 0)
        out, tid2 = w.run_entry(w.plan("BTC"))
        check("K1: signal refires clean → OPEN", out == "open" and
              w.row(tid2)["sm_state"] == esm.OPEN)
        _covered("K1 before t_intent → no row; refire clean")


def k2_after_intent() -> None:
    print("— K2: kill after t_intent — INTENT/no-sent → NeverSent, no venue calls")
    with World("k2") as w:
        tid = _run_kill(w, w.plan("BTC"), "K2")
        r = w.row(tid)
        check("K2: on-disk INTENT with order_sent_at NULL",
              r["sm_state"] == esm.INTENT and r["order_sent_at"] is None)
        wmark = w.call_mark()
        w.restart()
        r = w.row(tid)
        check("K2: ABORTED via NeverSent fast path",
              r["sm_state"] == esm.ABORTED and
              r["abort_reason"] == "crash_before_send")
        check("K2: proof=NeverSent in the audit trail",
              any(d.get("proof") == "NeverSent" for d in w.transition_details(tid)))
        check("K2: zero venue WRITES during recovery",
              len(w.write_ops(since=wmark)) == 0)
        check("K2: rejected_signals written", len(w.rejected()) == 1)
        _covered("K2 after t_intent → NeverSent fast path, provably clean")


def k3_sent_not_dispatched() -> None:
    print("— K3: kill after t_mark_sent, before dispatch — VerifiedAbsent abort")
    with World("k3") as w:
        tid = _run_kill(w, w.plan("BTC"), "K3")
        check("K3: on-disk PENDING, venue clean",
              w.row(tid)["sm_state"] == esm.PENDING and
              w.position("BTC") is None)
        w.restart()
        r = w.row(tid)
        check("K3: no fill/order/position evidence → ABORTED",
              r["sm_state"] == esm.ABORTED and
              r["abort_reason"] == "crash_unfilled")
        check("K3: proof=VerifiedAbsent",
              any(d.get("proof") == "VerifiedAbsent"
                  for d in w.transition_details(tid)))
        check("K3: legacy consistent", legacy_consistent(w))
        _covered("K3 sent-marker only → t_abort_begin→t_abort_final(VerifiedAbsent)")


def k4_response_lost() -> None:
    print("— K4: kill mid market_open (fill landed, response lost) — REAL px")
    with World("k4") as w:
        p = w.plan("BTC")
        tid = _run_kill(w, p, "K4")
        check("K4: on-disk PENDING + venue position live (naked)",
              w.row(tid)["sm_state"] == esm.PENDING and
              w.position("BTC") is not None)
        w.restart()
        r = w.row(tid)
        fill_px = 50_000.0 * 1.0005
        check("K4: recovered to OPEN via policy re-run + full ladder",
              r["sm_state"] == esm.OPEN,
              "state=%s" % r["sm_state"])
        check("K4: journal entry = ACTUAL fill px, not intended (D11 fix)",
              abs(float(r["entry"]) - fill_px) < 1e-6,
              "entry=%s" % r["entry"])
        check("K4: policy re-run evidence in t_fill audit (F3)",
              any(d.get("__to") == esm.FILLED and
                  (d.get("policy") or {}).get("verdict") == "pass"
                  for d in w.transition_details(tid)))
        check("K4: SL placed + registered + persisted",
              len(w.triggers("BTC")) == 1 and
              str(r["sl_order_id"]) in reg.oids_ever_placed(w.db) and
              r["sl_placed_px"] == w.trigger_px(r["sl_order_id"]))
        check("K4: money protected", money_protected(w))
        _covered("K4 mid market_open → reconciler fill-evidence + policy re-run "
                 "from persisted limit_px → t_fill REAL px → ladder → OPEN")
    # K4b — the same window driven through the P2 fault bank (WriteUnconfirmed)
    with World("k4b") as w:
        w.gate.fail("place_order", "timeout_after_land", count=1)
        w.gate.fail("fills", "timeout", count=1)
        out, tid = w.run_entry(w.plan("BTC"))
        w.gate.clear_faults()
        check("K4b: WriteUnconfirmed leaves PENDING (no guessed fill)",
              out == "pending_reconcile" and
              w.row(tid)["sm_state"] == esm.PENDING)
        w.restart()
        r = w.row(tid)
        check("K4b: reconciler recovers to OPEN with real px",
              r["sm_state"] == esm.OPEN and
              abs(float(r["entry"]) - 50_000.0 * 1.0005) < 1e-6)
        _covered("K4b market_open WriteUnconfirmed (fault bank) → PENDING → "
                 "same recovery")


def k5_filled_naked() -> None:
    print("— K5: kill after t_fill, before trigger_sl — priority-0 naked window")
    with World("k5") as w:
        tid = _run_kill(w, w.plan("BTC"), "K5")
        check("K5: on-disk FILLED, position live, NO SL",
              w.row(tid)["sm_state"] == esm.FILLED and
              w.position("BTC") is not None and w.triggers("BTC") == [])
        w.restart()
        r = w.row(tid)
        check("K5: recovered FILLED→PROTECTED→OPEN with SL resting",
              r["sm_state"] == esm.OPEN and len(w.triggers("BTC")) == 1)
        check("K5: sl_placed_px = venue-ACCEPTED px",
              r["sl_placed_px"] == w.trigger_px(r["sl_order_id"]))
        check("K5: explicit state (no pending+position inference)",
              w.states(tid)[:3] == [esm.INTENT, esm.PENDING, esm.FILLED])
        check("K5: money protected", money_protected(w))
        _covered("K5 FILLED naked window → reconciler priority-0 SL place → "
                 "t_protect → t_open")


def k6_sl_landed_unpersisted() -> None:
    print("— K6: kill mid trigger_sl — crash-landed SL adopted, NO dup")
    with World("k6") as w:
        tid = _run_kill(w, w.plan("BTC"), "K6")
        trigs = w.triggers("BTC")
        check("K6: SL resting on venue, DB still FILLED (unregistered oid)",
              w.row(tid)["sm_state"] == esm.FILLED and len(trigs) == 1 and
              trigs[0] not in reg.oids_ever_placed(w.db))
        crash_oid = trigs[0]
        w.restart()
        r = w.row(tid)
        check("K6: crash-landed SL ADOPTED (same oid), NO second SL",
              r["sm_state"] == esm.OPEN and
              str(r["sl_order_id"]) == crash_oid and
              w.triggers("BTC") == [crash_oid])
        check("K6: adopted oid registered (crash race healed)",
              crash_oid in reg.oids_ever_placed(w.db))
        _covered("K6 SL landed+unpersisted → |Δpx|≤1-tick adoption match, "
                 "registry INSERT, zero dup-SL")
    # K6-manual: a user trigger at a DIFFERENT px must be neither adopted nor
    # cancelled; the engine places its own SL.
    with World("k6m") as w:
        tid = _run_kill(w, w.plan("BTC"), "K5")  # crash pre-SL (nothing landed)
        manual_oid = w.scenario.seed_order("BTC", "sell", 0.4,
                                           trigger_px=46_000.0,
                                           reduce_only=True, is_trigger=True)
        w.restart()
        r = w.row(tid)
        check("K6-manual: >1-tick user trigger NOT adopted, NOT cancelled",
              manual_oid in w.triggers("BTC") and
              str(r["sl_order_id"]) != manual_oid)
        check("K6-manual: engine placed its OWN SL alongside",
              r["sm_state"] == esm.OPEN and len(w.triggers("BTC")) == 2 and
              abs(float(w.trigger_px(r["sl_order_id"])) - 47_500.0) < 1e-6)
        _covered("K6-manual user trigger at different px → not adopted, not "
                 "cancelled, own SL placed")


def k7_protected() -> None:
    print("— K7: kill after t_protect — trivial t_open, no venue writes")
    with World("k7") as w:
        tid = _run_kill(w, w.plan("BTC"), "K7")
        check("K7: on-disk PROTECTED with live SL",
              w.row(tid)["sm_state"] == esm.PROTECTED and
              len(w.triggers("BTC")) == 1)
        wmark = w.call_mark()
        w.restart()
        check("K7: recovered to OPEN with ZERO venue writes (no dup-SL)",
              w.row(tid)["sm_state"] == esm.OPEN and
              len(w.write_ops(since=wmark)) == 0 and
              len(w.triggers("BTC")) == 1)
        _covered("K7 PROTECTED → t_open only (sl_order_id known pre-promote, "
                 "dup-SL class dead)")


def k8_open_pre_tp1() -> None:
    print("— K8: kill after t_open, before TP1 — nothing to recover")
    with World("k8") as w:
        tid = _run_kill(w, w.plan("BTC", tp1_frac=0.5), "K8")
        wmark = w.call_mark()
        w.restart()
        r = w.row(tid)
        check("K8: OPEN unchanged, tp1_order_id NULL, zero venue writes",
              r["sm_state"] == esm.OPEN and r["tp1_order_id"] is None and
              len(w.write_ops(since=wmark)) == 0)
        _, intents, _ = w.engine_tick(tid, strategy=ScriptedStrategy(),
                                      bars=TinyDF([50_000.0] * 3))
        check("K8: next tick steady (TP1 best-effort by design)",
              all(i.kind in ("NoOp", "Defer") for i in intents))
        _covered("K8 OPEN pre-TP1 → no action (TP1 best-effort, frac live-inert)")


def k9_mid_abort_unwind() -> None:
    print("— K9: kill inside ensure_flat — re-drive per persisted abort_reason")
    with World("k9") as w:
        p = w.plan("BTC", limit_px=49_000.0)   # policy WILL reject
        tid = _run_kill(w, p, "K9")
        r = w.row(tid)
        check("K9: ABORTING durable with reason + position still live",
              r["sm_state"] == esm.ABORTING and
              r["abort_reason"] == "entry_cap_breach_vs_persisted_limit_px" and
              w.position("BTC") is not None)
        check("K9: cooldown was armed BEFORE the kill (same commit)",
              esm.cooldown_active(w.db, "BTC"))
        w.restart()
        r = w.row(tid)
        check("K9: re-driven to ABORTED, venue flat",
              r["sm_state"] == esm.ABORTED and w.position("BTC") is None)
        check("K9: NEVER resurrected (no FILLED/OPEN after ABORTING)",
              w.states(tid) == [esm.INTENT, esm.PENDING, esm.ABORTING,
                                esm.ABORTED], str(w.states(tid)))
        check("K9: cooldown survived restart (no double-fill)",
              esm.cooldown_active(w.db, "BTC"))
        check("K9: rejected_signals written on final", len(w.rejected()) >= 1)
        _covered("K9 mid-unwind → ONE recovery: re-drive ensure_flat per "
                 "persisted abort_reason, never resurrect; cooldown durable")


def k9b_reject_uncommitted() -> None:
    print("— K9b: kill after reject decision, BEFORE t_abort_begin — policy re-run")
    with World("k9b") as w:
        p = w.plan("BTC", limit_px=49_000.0)
        tid = _run_kill(w, p, "K9b")
        r = w.row(tid)
        check("K9b: on-disk PENDING (reject decision lost), fill landed",
              r["sm_state"] == esm.PENDING and w.position("BTC") is not None)
        w.restart()
        r = w.row(tid)
        check("K9b: policy re-run from PERSISTED limit_px → same reject verdict",
              r["sm_state"] == esm.ABORTED and
              r["abort_reason"] == "entry_cap_breach_vs_persisted_limit_px")
        check("K9b: reject evidence in the ABORTING audit row",
              any(d.get("__to") == esm.ABORTING and
                  d.get("verdict") == "cap_breach"
                  for d in w.transition_details(tid)))
        check("K9b: venue flat, never promoted",
              w.position("BTC") is None and
              esm.FILLED not in w.states(tid) and
              esm.OPEN not in w.states(tid))
        check("K9b: cooldown armed by recovery", esm.cooldown_active(w.db, "BTC"))
        _covered("K9b reject-decided/uncommitted → PENDING recovery re-runs the "
                 "policy from persisted limit_px → same verdict → abort ladder")


def k10_exit_windows() -> None:
    print("— K10: mid-exit crash windows (venue-closed / no-evidence / residual)")
    # (a) venue closed the position during downtime → startup attribution
    with World("k10a") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        r0 = w.row(tid)
        sl_oid = str(r0["sl_order_id"])
        w.drop_position("BTC")
        w.drop_trigger(sl_oid)
        w.add_fill("BTC", "sell", 47_500.0, 1.0, oid=sl_oid, reduce_only=True)
        w.restart()
        r = w.row(tid)
        check("K10a: startup fills lookup → t_close attributed 'sl'",
              r["sm_state"] == esm.CLOSED and r["exit_reason"] == "sl" and
              float(r["exit_price"]) == 47_500.0)
        _covered("K10a OPEN + venue-closed during downtime → startup oid-first "
                 "attribution → t_close")
    # (b) position absent, NO fills evidence → row KEPT OPEN + CRITICAL,
    #     then the runtime phantom K=3 resolves it.
    with World("k10b") as w:
        spy = LogSpy()
        logging.getLogger("fleet_core.engine.entry_sm").addHandler(spy)
        try:
            _, tid = w.run_entry(w.plan("BTC"))
            w.drop_position("BTC")
            w.venue.fills.clear()          # no evidence at all
            w.restart()
            r = w.row(tid)
            check("K10b: no evidence → row KEPT OPEN (never auto-close) + CRITICAL",
                  r["sm_state"] == esm.OPEN and spy.count("kept OPEN") >= 1)
            for _ in range(3):
                _, intents, results = w.engine_tick(tid, strategy=ScriptedStrategy())
            r = w.row(tid)
            check("K10b: runtime phantom K=3 → resolved "
                  "'phantom_no_exchange_position'",
                  r["sm_state"] == esm.CLOSED and
                  r["exit_reason"] == "phantom_no_exchange_position")
        finally:
            logging.getLogger("fleet_core.engine.entry_sm").removeHandler(spy)
        _covered("K10b OPEN-absent no evidence → kept OPEN + CRITICAL; runtime "
                 "phantom K=3 → AdoptResolve DB row-close")
    # (c) ensure_flat unconfirmed → §9.5 residual protect, row kept OPEN
    with World("k10c") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        w.gate.fail("place_order", "accept_no_land", count=3)  # close never lands
        t = w.build_tick(tid)
        results = w.executor().execute(
            t, [xng.Close(reason="time_stop", ref_px=w.mark("BTC"))])
        r = w.row(tid)
        resid_px = w.mark("BTC") * (1.0 - xng.PROTECTIVE_RESIDUAL_PCT)
        check("K10c: unconfirmed close → row KEPT OPEN",
              results[0].get("status") == "unconfirmed_kept_open" and
              r["sm_state"] == esm.OPEN)
        check("K10c: residual protected at mark∓2.5% (liq-guarded)",
              any(abs(float(w.trigger_px(o)) - resid_px) < 1e-6
                  for o in w.triggers("BTC")),
              str([w.trigger_px(o) for o in w.triggers("BTC")]))
        check("K10c: CRITICAL escalated", any("UNCONFIRMED" in a for a in w.alerts))
        w.gate.clear_faults()
        t2 = w.build_tick(tid)
        results2 = w.executor().execute(
            t2, [xng.Close(reason="time_stop", ref_px=w.mark("BTC"))])
        r = w.row(tid)
        check("K10c: next tick converges — closed, venue flat",
              results2[0].get("status") == "closed" and
              r["sm_state"] == esm.CLOSED and w.position("BTC") is None)
        _covered("K10c ensure_flat WriteUnconfirmed → residual SL ±2.5% mark + "
                 "row kept OPEN + CRITICAL; converges next tick")


def k11_durability() -> None:
    print("— K11: cooldowns / opens_today / sl_placed_px durable across restart")
    with World("k11") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        esm.arm_cooldown(w.db, "ETH", 900.0, "test")
        placed_before = w.row(tid)["sl_placed_px"]
        opens_before = esm.opens_today(w.db)
        w.restart()
        check("K11: entry_cooldowns durable", esm.cooldown_active(w.db, "ETH"))
        check("K11: opens_today durable",
              esm.opens_today(w.db) == opens_before == 1)
        check("K11: sl_placed_px durable (no restart SL churn)",
              w.row(tid)["sl_placed_px"] == placed_before is not None)
        _covered("K11 entry_cooldowns + opens_today + sl_placed_px persisted "
                 "(W9/W11/ext-§e classes)")


def k12_supersede() -> None:
    print("— K12: kill between new-SL-confirm+registry and persist — sweep")
    with World("k12") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        old_oid = str(w.row(tid)["sl_order_id"])
        manual_oid = w.scenario.seed_order("BTC", "sell", 0.3,
                                           trigger_px=42_000.0,
                                           reduce_only=True, is_trigger=True)
        # the K12 crash: new SL confirmed live + registry INSERT done, killed
        # before supersede-cancel/persist (steps 5-6)
        w.set_mark("BTC", 54_000.0)
        new = w.client.trigger_sl("BTC", False, 1.0, 49_000.0)
        reg.register_oid(w.db, new.oid, "BTC", trade_id=tid, kind="sl")
        check("K12: TWO bot-own SLs resting, DB oid = OLD",
              set(w.triggers("BTC")) == {old_oid, str(new.oid), manual_oid} and
              str(w.row(tid)["sl_order_id"]) == old_oid)
        w.client = w.ctx.make_client()   # restart
        _, intents, results = w.engine_tick(tid, strategy=ScriptedStrategy(),
                                            bars=TinyDF([54_000.0] * 3))
        sweeps = [i for i in intents if i.kind == "SupersedeSweep"]
        check("K12: SupersedeSweep emitted (cancel own ≠ DB-current)",
              len(sweeps) == 1 and sweeps[0].keep_oid == old_oid and
              sweeps[0].cancel_oids == (str(new.oid),))
        check("K12: converged ≤1 tick — DB-current survives, litter cancelled",
              old_oid in w.triggers("BTC") and
              str(new.oid) not in w.triggers("BTC"))
        check("K12: manual (non-registry) trigger untouched",
              manual_oid in w.triggers("BTC"))
        # fire-before-sweep: a fill from the superseded oid still attributes
        reason, px, _ev = xng.attribute_exit(
            w.build_tick(tid).pos,
            [{"coin": "BTC", "oid": str(new.oid), "px": 49_000.0, "size": 1.0}],
            frozenset(reg.oids_ever_placed(w.db)))
        check("K12: fire-before-sweep attributes via historic corpus (never "
              "unknown)", reason in ("sl", "trail_sl"), reason)
        _covered("K12 dup-SL litter → per-tick supersede sweep cancels own "
                 "non-DB-current, manual untouched; historic-oid attribution")


# ============================================================================
# SECTION 3 — management classes / fences / adoption
# ============================================================================

def s_covered_adoption_3a() -> None:
    print("— adopt 3a: covered untracked → TRACK-ONLY; heal at CURRENT mark")
    with World("adopt3a") as w:
        w.scenario.seed_position("ETH", 1.0, 2500.0)
        user_oid = w.scenario.seed_order("ETH", "sell", 1.0, trigger_px=2300.0,
                                         reduce_only=True, is_trigger=True)
        rec = w.reconciler()
        wmark = w.call_mark()
        rec.tick_pass()
        rows = [r for r in w.rows() if r["coin"] == "ETH"]
        check("3a: track-only row inserted (protect_only, adopted_covered)",
              len(rows) == 1 and rows[0]["management_class"] == "protect_only"
              and rows[0]["notes"] == "adopted_covered" and
              rows[0]["sm_state"] == esm.OPEN)
        r = rows[0]
        tid = int(r["id"])
        check("3a: sl_* columns NULL (user px never anchored)",
              r["sl_order_id"] is None and r["sl_placed_px"] is None and
              r["sl_current"] is None)
        check("3a: covering px ONLY in sm_transitions.detail (forensics)",
              any(d.get("covering_trigger_px") == 2300.0
                  for d in w.transition_details(tid)))
        check("3a: ZERO placements while covered",
              len(w.write_ops(since=wmark)) == 0)
        rec.tick_pass()
        check("3a: still zero placements on the next pass (ever-while-covered)",
              len(w.write_ops(since=wmark)) == 0)
        # exit-engine view: protect_only pipeline → NoOp, no trail/TP1 ever
        _, intents, _ = w.engine_tick(tid,
                                      strategy=ScriptedStrategy(new_sl=2400.0),
                                      bars=TinyDF([2500.0] * 3))
        check("3a: exit-engine tick = NoOp(protect_only_covered); strategy "
              "pressure ignored",
              all(i.kind in ("NoOp",) for i in intents) and
              any(i.reason == "protect_only_covered" for i in intents),
              str([(i.kind, i.reason) for i in intents]))
        # executor F2 gate: close authority refused
        res = w.executor().execute(w.build_tick(tid),
                                   [xng.Close(reason="tp", ref_px=2500.0)])
        check("3a: Close REFUSED (no authority without provenance)",
              res[0].get("status") == "refused" and
              w.position("ETH") is not None)
        # user pulls his stop → heal re-evaluates at CURRENT state/mark
        w.drop_trigger(user_oid)
        rec.watch_special_rows()
        r = w.row(tid)
        exp = 2500.0 * (1.0 - 0.06)
        check("3a-heal: cover vanished → placed at CURRENT mark ∓6% "
              "(2350, NEVER the user's 2300)",
              r["sl_order_id"] is not None and
              abs(float(w.trigger_px(r["sl_order_id"])) - exp) < 1e-6,
              str(w.trigger_px(r["sl_order_id"])))
        _covered("covered-adoption 3a track-only (sl_* NULL) + heal re-anchor "
                 "at CURRENT mark after cover vanish")


def s_adopt_3c_naked() -> None:
    print("— adopt 3c: non-prefix naked untracked → mark∓6% protect-only")
    with World("adopt3c") as w:
        w.scenario.seed_position("ETH", -2.0, 2500.0)   # SHORT — upper anchor
        rec = w.reconciler()
        rec.tick_pass()
        rows = [r for r in w.rows() if r["coin"] == "ETH"]
        check("3c: protect-only row OPEN with engine SL",
              len(rows) == 1 and rows[0]["management_class"] == "protect_only"
              and rows[0]["sm_state"] == esm.OPEN and rows[0]["notes"] == "adopted")
        r = rows[0]
        exp = 2500.0 * 1.06
        check("3c: protective SL at CURRENT mark +6% (short)",
              abs(float(w.trigger_px(r["sl_order_id"])) - exp) < 1e-6,
              str(w.trigger_px(r["sl_order_id"])))
        check("3c: oid registered + row linked",
              str(r["sl_order_id"]) in reg.oids_ever_placed(w.db))
        _, intents, _ = w.engine_tick(int(r["id"]),
                                      strategy=ScriptedStrategy(new_sl=2600.0),
                                      bars=TinyDF([2500.0] * 3))
        check("3c: covered thereafter → NoOp (no strategy management)",
              all(i.kind == "NoOp" for i in intents))
        _covered("3c non-prefix naked → ±6% CURRENT mark, registry, "
                 "protect_only row (no close authority)")


def s_prefix_manual_3b() -> None:
    print("— adopt 3b: prefix-manual naked → CRITICAL every pass, NO SL")
    with World("adopt3b") as w:
        spy = LogSpy()
        logging.getLogger("fleet_core.engine.entry_sm").addHandler(spy)
        try:
            w.scenario.seed_position("xyz_GOLD", 1.0, 2400.0)
            rec = w.reconciler()
            wmark = w.call_mark()
            rec.tick_pass()
            rows = [r for r in w.rows() if r["coin"] == "xyz_GOLD"]
            check("3b: track-only visibility row (unreconciled_manual_prefix)",
                  len(rows) == 1 and
                  rows[0]["notes"] == "unreconciled_manual_prefix" and
                  rows[0]["management_class"] == "protect_only")
            check("3b: NO SL placed (hl canon — no bot ±6% on a manual pos)",
                  w.triggers("xyz_GOLD") == [] and
                  rows[0]["sl_order_id"] is None)
            c1 = spy.count("UNRECONCILED")
            rec.tick_pass()
            c2 = spy.count("UNRECONCILED")
            check("3b: CRITICAL re-fired EVERY pass", c1 >= 1 and c2 > c1,
                  "c1=%d c2=%d" % (c1, c2))
            check("3b: zero venue writes for the prefix-manual coin",
                  len(w.write_ops(since=wmark, coin="xyz_GOLD")) == 0)
            # operator resolves via claim → alerts stop, still zero writes
            tid = int(rows[0]["id"])
            esm.claim_manual(w.db, "xyz_GOLD", venue="fake", note="selftest")
            c3 = spy.count("UNRECONCILED")
            rec.tick_pass()
            rec.tick_pass()
            check("3b→claim: manual_claimed silences the re-alert",
                  spy.count("UNRECONCILED") == c3 and
                  w.row(tid)["management_class"] == "manual_claimed")
        finally:
            logging.getLogger("fleet_core.engine.entry_sm").removeHandler(spy)
        _covered("prefix-manual naked (3b) → CRITICAL every pass, NO SL; claim "
                 "silences (operator escape hatch)")


def s_fenced_untouchable() -> None:
    print("— fences: FX_EXCLUDE / UNIVERSE / _is_fx / prefix — never touched")
    with World("fence") as w:
        w.scenario.seed_position("BNB", 3.0, 600.0)      # FX_EXCLUDE
        w.scenario.seed_position("DOGE", 100.0, 0.2)     # UNIVERSE_SYMBOL_EXCLUDE
        w.scenario.seed_position("EURUSD", 5.0, 1.1)     # _is_fx heuristic
        rec = w.reconciler()
        wmark = w.call_mark()
        rec.tick_pass()
        rec.tick_pass()
        check("fence: ZERO venue writes for fenced coins",
              len(w.write_ops(since=wmark)) == 0)
        check("fence: no rows adopted for fenced coins",
              all(r["coin"] not in ("BNB", "DOGE", "EURUSD") for r in w.rows()))
        check("fence: positions untouched",
              all(w.position(c) is not None for c in ("BNB", "DOGE", "EURUSD")))
        # pinned position-context kwargs (R3/R2-F1d): prefix ⇒ FENCED via the
        # REAL P1 _fenced (bot_owned=None, db_open without the coin)
        check("fence: pinned kwargs — xyz_ prefix positionally FENCED",
              esm.position_fenced("xyz_GOLD", []) is True)
        check("fence: BNB fenced via FX_EXCLUDE (real _fenced chain)",
              esm.position_fenced("BNB", []) is True)
        # fail-closed law: a THROWING fence must fence
        check("fence: fail-closed — broken fence_fn ⇒ FENCED",
              esm.position_fenced("SOL", [], fence_fn=lambda *a, **k: 1 / 0) is True)
        # exit-engine step-2 mirror: fenced verdict → NoOp only
        _, tid = w.run_entry(w.plan("BTC"))
        t = w.build_tick(tid, fence_verdict="fx_exclude")
        intents = xng.tick(t)
        check("fence: exit-engine fenced tick = NoOp only",
              [i.kind for i in intents] == ["NoOp"] and
              intents[0].reason.startswith("fenced:"))
        _covered("fenced coins (FX_EXCLUDE/UNIVERSE/_is_fx/prefix) untouchable "
                 "in both reconciler and exit-engine; fence fail-closed")


def s_manual_claimed() -> None:
    print("— manual_claimed: FULL hands-off + phantom-K3 DB-only row-resolve")
    with World("claimed") as w:
        _, tid = w.run_entry(w.plan("ETH", size=1.0))
        esm.claim_manual(w.db, "ETH", venue="fake", note="selftest")
        r = w.row(tid)
        check("claim: live row flipped to manual_claimed",
              r["management_class"] == "manual_claimed")
        wmark = w.call_mark()
        # engine tick under live strategy pressure → hands-off
        _, intents, _ = w.engine_tick(tid,
                                      strategy=ScriptedStrategy(new_sl=2600.0,
                                                                exit_reason="tp"),
                                      bars=TinyDF([2500.0] * 3))
        check("claimed: tick = NoOp hands-off (strategy pressure ignored)",
              [i.kind for i in intents] == ["NoOp"] and
              "hands_off" in intents[0].reason)
        # executor refuses EVERY venue intent
        t = w.build_tick(tid)
        res = w.executor().execute(t, [
            xng.ReplaceSL(reason="trail_advance", target_px=2400.0, cause="trail"),
            xng.HealSL(reason="sl_not_live", target_px=2400.0, size_from_live=1.0),
            xng.Close(reason="tp", ref_px=2500.0),
            xng.EscalateNaked(reason="sl_replace_failed_naked")])
        check("claimed: executor refuses EVERY venue intent (§7.12)",
              all(x.get("status") == "refused" for x in res), str(res))
        # reconciler watch: present → nothing; no venue writes at all
        rec = w.reconciler()
        rec.watch_special_rows()
        rec.watch_special_rows()
        check("claimed: zero venue writes across ticks/watches",
              len(w.write_ops(since=wmark)) == 0)
        # position vanishes → phantom-K3 DB-only row-resolve (review nit #2)
        w.drop_position("ETH")
        w.client = w.ctx.make_client()
        wmark2 = w.call_mark()
        for _ in range(3):
            rec.watch_special_rows()
        r = w.row(tid)
        check("claimed: K=3 → row CLOSED in DB",
              r["sm_state"] == esm.CLOSED and
              "phantom_k3_row_resolve" in (r["notes"] or ""))
        check("claimed: resolve was DB-ONLY (zero venue writes, SL left alone)",
              len(w.write_ops(since=wmark2)) == 0 and
              len(w.triggers("ETH")) == 1)
        # claim with no prior row → track-only row
        tid2 = esm.claim_manual(w.db, "XRP", venue="fake", note="no-row")
        r2 = w.row(tid2)
        check("claim(no row): track-only manual_claimed row inserted",
              r2["management_class"] == "manual_claimed" and
              r2["origin"] == "adopted_untracked" and r2["sm_state"] == esm.OPEN)
        _covered("manual_claimed hands-off (executor refuses all venue intents; "
                 "no re-alerts) + phantom-K3 DB-only row-resolve — same as "
                 "protect_only (review nit #2)")


def s_orphan_sweep() -> None:
    print("— orphan triggers: registry-gated cancel; manual + fenced survive")
    with World("orphan") as w:
        own_oid = w.scenario.seed_order("ETH", "sell", 1.0, trigger_px=2000.0,
                                        reduce_only=True, is_trigger=True)
        reg.register_oid(w.db, own_oid, "ETH", trade_id=None, kind="protective")
        manual_oid = w.scenario.seed_order("XRP", "buy", 10.0, trigger_px=0.6,
                                           reduce_only=True, is_trigger=True)
        fenced_oid = w.scenario.seed_order("BNB", "sell", 1.0, trigger_px=550.0,
                                           reduce_only=True, is_trigger=True)
        reg.register_oid(w.db, fenced_oid, "BNB", trade_id=None, kind="protective")
        rec = w.reconciler()
        rec.tick_pass()     # observation 1 (debounce: ≥2 obs)
        check("orphan: first observation only — nothing cancelled",
              own_oid in w.triggers("ETH"))
        rec.tick_pass()     # observation 2 → sweep
        check("orphan: bot-own registry orphan cancelled",
              own_oid not in w.triggers("ETH"))
        check("orphan: manual (non-registry) trigger survives",
              manual_oid in w.triggers("XRP"))
        check("orphan: FENCED coin trigger survives even though registry-own",
              fenced_oid in w.triggers("BNB"))
        _covered("orphan-trigger sweep registry-gated (F5): own cancelled after "
                 "debounce, manual + fenced untouched")


# ============================================================================
# SECTION 3b — round-2 review fixes (EF1/EF2/EF4/EF5/EF6)
# ============================================================================

def s_ef1_manual_after_abort() -> None:
    print("— EF1: bot abort SOL, manual SOL long 3h later → untouched + CRITICAL")
    from datetime import datetime, timedelta, timezone as _tz
    with World("ef1") as w:
        spy = LogSpy()
        logging.getLogger("fleet_core.engine.entry_sm").addHandler(spy)
        try:
            w.venue.meta["SOL"] = {"min_size": 0.1, "tick": 0.01,
                                   "max_leverage": 20, "scope": "main"}
            w.set_mark("SOL", 100.0)
            out, tid = w.run_entry(w.plan("SOL", limit_px=99.0))  # cap breach
            check("EF1: bot abort completed (ABORTED, venue flat)",
                  out == "aborted" and
                  w.row(tid)["sm_state"] == esm.ABORTED and
                  w.position("SOL") is None)
            # 3h pass — backdate the row's dispatch/activity stamps (the fill
            # window is anchored on order_sent_at; the row stays a rule-2
            # coin+recency candidate inside the 24h window BY DESIGN — the
            # provenance gate, not the row match, must protect the manual).
            ago = (datetime.now(_tz.utc) - timedelta(hours=3)).isoformat()
            with esm._conn(w.db) as con:
                con.execute("UPDATE trades SET order_sent_at=?, sm_updated_at=?"
                            " WHERE id=?", (ago, ago, tid))
            # manual SOL long (unfenced, non-prefix coin), user's own stop
            w.scenario.seed_position("SOL", 2.0, 101.0)
            manual_sl = w.scenario.seed_order("SOL", "sell", 2.0,
                                              trigger_px=95.0,
                                              reduce_only=True,
                                              is_trigger=True)
            wmark = w.call_mark()
            rec = w.reconciler()
            rec.tick_pass()
            r = w.row(tid)
            check("EF1: manual position UNTOUCHED — zero venue writes, "
                  "position + user stop intact",
                  len(w.write_ops(since=wmark, coin="SOL")) == 0 and
                  w.position("SOL") is not None and
                  manual_sl in w.triggers("SOL"))
            check("EF1: ABORTED row stays terminal (no resurrect, no re-drive)",
                  r["sm_state"] == esm.ABORTED)
            check("EF1: CRITICAL fired — coin+recency is NOT provenance",
                  spy.count("coin+recency") >= 1)
            sol_rows = [x for x in w.rows()
                        if x["coin"] == "SOL" and int(x["id"]) != tid]
            check("EF1: rule-3 protect-only path taken (3a covered track-only,"
                  " sl_* NULL)",
                  len(sol_rows) == 1 and
                  sol_rows[0]["management_class"] == "protect_only" and
                  sol_rows[0]["notes"] == "adopted_covered" and
                  sol_rows[0]["sl_order_id"] is None)
            rec.tick_pass()
            check("EF1: second pass still writes NOTHING for SOL",
                  len(w.write_ops(since=wmark, coin="SOL")) == 0)
            # user pulls his stop → live canon protects (mark∓6% SL), NEVER
            # closes: the ONLY permitted venue write class is place_trigger.
            w.drop_trigger(manual_sl)
            rec.tick_pass()
            writes = w.write_ops(since=wmark, coin="SOL")
            check("EF1: naked manual → protective SL only (place_trigger), "
                  "NEVER ensure_flat/close",
                  w.position("SOL") is not None and
                  all(c["op"] == "place_trigger" for c in writes) and
                  len(w.triggers("SOL")) == 1)
        finally:
            logging.getLogger("fleet_core.engine.entry_sm").removeHandler(spy)
        _covered("EF1 ABORTED row + later manual position → positive-provenance"
                 " gate refuses ensure_flat; rule-3 protect-only + CRITICAL")


def s_ef2_k7_sl_fired_downtime() -> None:
    print("— EF2 (K7-variant): kill in PROTECTED, SL fired during downtime")
    with World("ef2k7") as w:
        tid = _run_kill(w, w.plan("BTC"), "K7")
        r0 = w.row(tid)
        sl_oid = str(r0["sl_order_id"])
        check("EF2: on-disk PROTECTED with resting SL",
              r0["sm_state"] == esm.PROTECTED and sl_oid in w.triggers("BTC"))
        # downtime (host outage): the venue SL FIRES — position gone, trigger
        # consumed, close fill on the tape at the SL px.
        w.drop_position("BTC")
        w.drop_trigger(sl_oid)
        w.add_fill("BTC", "sell", 47_500.0, 1.0, oid=sl_oid, reduce_only=True)
        try:
            rec = w.restart()   # pre-fix: SMConflict FILLED->CLOSED wedge →
            startup_ok = True   # systemd crash-loop (EF2)
        except Exception as e:  # noqa: BLE001
            startup_ok = False
            rec = None
            print("    startup raised: %r" % e)
        r = w.row(tid)
        check("EF2: startup pass survives (no SMConflict crash-loop)",
              startup_ok)
        check("EF2: PROTECTED row attributed t_close 'sl' @ the fill px",
              r["sm_state"] == esm.CLOSED and r["exit_reason"] == "sl" and
              float(r["exit_price"]) == 47_500.0,
              "state=%s reason=%s px=%s" % (r["sm_state"], r["exit_reason"],
                                            r["exit_price"]))
        if rec is not None:
            rec.tick_pass()     # engine CONTINUES — later rows/passes run
        check("EF2: engine continues (tick pass clean post-recovery)",
              rec is not None and legacy_consistent(w))
        _covered("EF2/K7-variant PROTECTED + SL fired in downtime → attributed "
                 "t_close (from_states FILLED|PROTECTED), engine continues")


def s_ef5_ef6_adoption_seeding() -> None:
    print("— EF5/EF6: adoption seeds sl_placed_px from the venue LISTED px + "
          "restore grace")
    with World("seed") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        listed_px = float(w.trigger_px(w.row(tid)["sl_order_id"]))
        # old-bot drift: DB lost the placed px and remembers a DIFFERENT stop
        with esm._conn(w.db) as con:
            con.execute("UPDATE trades SET sl_placed_px=NULL, sl_current=? "
                        "WHERE id=?", (listed_px - 500.0, tid))
        w.restart()
        r = w.row(tid)
        check("EF5: sl_placed_px seeded FROM the venue's LISTED trigger px "
              "(never DB sl_current)",
              r["sl_placed_px"] is not None and
              float(r["sl_placed_px"]) == listed_px,
              "placed=%s listed=%s" % (r["sl_placed_px"], listed_px))
        check("EF6: one-time restore grace armed at adoption "
              "(restore_flag=1)", w.kv_get("restore_flag:%d" % tid) == "1")
        # LABELED fallback: listing read fails → DB value kept
        with esm._conn(w.db) as con:
            con.execute("UPDATE trades SET sl_placed_px=? WHERE id=?",
                        (123.0, tid))
        # the fake binding reads triggers via the 'orders' endpoint
        w.gate.fail("orders", "timeout", count=1)
        rec2 = w.reconciler()
        rec2.seed_adoption_state()
        check("EF5: listing ReadUnknown → LABELED fallback (DB value kept)",
              float(w.row(tid)["sl_placed_px"]) == 123.0)
        w.gate.clear_faults()
        rec2.seed_adoption_state()
        check("EF5: next pass re-seeds from the venue listing",
              float(w.row(tid)["sl_placed_px"]) == listed_px)
        # migrate_v3 at cutover WITH the venue reachable (EF5) — fresh legacy DB
        legacy = w.tmp / "legacy_seed.db"
        con = mig._connect(legacy)
        try:
            mig.ensure_schema(con)
            con.execute(
                "INSERT INTO trades (created_at, coin, tf, direction, entry, "
                "sl_initial, sl_current, size, risk_dollars, status, "
                "sl_order_id, opened_at) VALUES (?, 'BTC', '8h', 'long', "
                "50000, 47000, ?, 1.0, 3000, 'open', ?, ?)",
                (esm._now_iso(), listed_px - 500.0,
                 w.row(tid)["sl_order_id"], esm._now_iso()))
            con.commit()
        finally:
            con.close()
        rep = mig.migrate(legacy, client=w.client)
        lrow = mig._connect(legacy).execute(
            "SELECT * FROM trades ORDER BY id").fetchone()
        check("EF5: migrate_v3(client=…) seed mode = venue_listed_trigger_px",
              rep["venue_seed_mode"] == "venue_listed_trigger_px" and
              float(lrow["sl_placed_px"]) == listed_px and
              any("venue LISTED trigger px" in str(x["note"])
                  for x in rep["rows_mapped"]),
              str(rep["venue_seed_mode"]))
        lflag = mig._connect(legacy).execute(
            "SELECT value FROM bot_state WHERE key=?",
            ("restore_flag:%d" % int(lrow["id"]),)).fetchone()
        check("EF6: migrate_v3 arms the restore grace flag on OPEN rows",
              lflag is not None and str(lflag[0]) == "1")
        rep2 = mig.migrate(legacy)   # idempotent re-run, no client
        check("EF5: migrate re-run idempotent (0 rows remapped, counts GREEN)",
              rep2["counts_verified"] and not rep2["rows_mapped"])
        _covered("EF5 sl_placed_px := venue LISTED trigger px at adoption + "
                 "migrate_v3(client) readback seed; EF6 restore grace armed in "
                 "both production paths, labeled fallback on read failure")


def s_ef4_runner_wiring() -> None:
    print("— EF4: runner resolves the REAL modules; partial mode is loud")
    from fleet_core.engine import runner as rnr
    import importlib
    with World("wiring") as w:
        _, tid = w.run_entry(w.plan("BTC"))
        eng = rnr.Engine(rnr.EngineConfig(venue="extended", mode="live",
                                          data_dir=w.tmp, db_path=w.db))
        eng.client = w.client
        handle = eng._resolve_reconciler()
        check("EF4: reconciler = REAL entry_sm.Reconciler (startup/tick pass)",
              handle.full_pass.__name__ == "startup_pass" and
              handle.incremental_pass.__name__ == "tick_pass" and
              type(handle.full_pass.__self__).__module__ ==
              "fleet_core.engine.entry_sm")
        ex = eng._resolve_executor()
        eng.executor = ex
        check("EF4: executor = REAL exit_engine.IntentExecutor over "
              "SqliteJournal",
              type(ex).__module__ == "fleet_core.engine.exit_engine" and
              type(ex.journal).__name__ == "SqliteJournal")
        et = eng._resolve_exit_tick()
        check("EF4: exit tick resolves to the REAL exit_engine.run_tick",
              callable(et))
        check("EF4: unfreeze REFUSED while the entry pass is unwired "
              "(no silent entry disable)",
              eng._resolve_entry_pass() is None and
              eng.set_entry_freeze(False).get("ok") is False)
        ghost_gone = False
        try:
            importlib.import_module("fleet_core.reconciler")
        except ImportError:
            ghost_gone = True
        check("EF4: no fleet_core.reconciler ghost on the wiring path",
              ghost_gone)
        # end-to-end REAL run_tick over the world DB — proves EF6 consumption:
        # the through-px restore reconcile fires ONCE, re-anchors mark−6%,
        # clears the flag, NEVER closes.
        w.kv_set("restore_flag:%d" % tid, "1")
        mark = w.mark("BTC")
        with esm._conn(w.db) as con:
            con.execute("UPDATE trades SET sl_current=? WHERE id=?",
                        (mark * 1.02, tid))   # DB stop THROUGH the mark
        res = xng.run_tick(
            db_path=w.db, client=w.client, executor=w.executor(),
            venue="fake", quirks=w.quirks,
            strategy_factory=lambda row: ScriptedStrategy(),
            bars_fn=lambda c, tf: TinyDF([w.mark(c)] * 3))
        r = w.row(tid)
        check("EF4: run_tick drove the row (one result, ReplaceSL executed)",
              len(res) == 1 and "ReplaceSL" in res[0]["intents"],
              str(res))
        check("EF6: restore grace consumed — through DB stop re-anchored to "
              "mark−6%, flag cleared, row NEVER closed",
              r["sm_state"] == esm.OPEN and
              abs(float(r["sl_placed_px"]) - mark * 0.94) < 1.0 and
              w.kv_get("restore_flag:%d" % tid) == "0",
              "placed=%s expect≈%s flag=%s" % (r["sl_placed_px"], mark * 0.94,
                                               w.kv_get("restore_flag:%d" % tid)))
        _covered("EF4 runner wiring = REAL modules (entry_sm.Reconciler, "
                 "IntentExecutor+SqliteJournal, exit_engine.run_tick), loud "
                 "refusals; EF6 restore-grace consumed through run_tick")


def s_r21_production_strategy_wiring() -> None:
    print("— R2-1: un-injected production exit tick drives the REAL pinned "
          "strategy (steps 9-14 live)")
    from fleet_core.engine import runner as rnr
    import numpy as np
    import pandas as pd
    prev_dry = os.environ.pop("DRY_RUN", None)
    prev_allow = os.environ.pop("ENGINE_ALLOW_PARTIAL", None)
    try:
        with World("r21wire") as w:
            _, tid = w.run_entry(w.plan("BTC"))
            r0 = w.row(tid)
            entry = float(r0["entry"])
            # scripted CLOSED-bar sequence ending at the latest closed 8h bar:
            # +3% uptrend with a pivot low every 5th bar so the PINNED pivot-low
            # vstop trail (TRAIL_PIVOT_WINDOW=2 / TRAIL_VSTOP_BUFFER=0.005 —
            # strategy_xnn code-pinned constants) has pivots to ratchet to.
            # Entry bar = idx 10 (bars 0-9 are pre-entry history).
            tf_ms = 8 * 3600 * 1000
            last_open_ms = ((int(time.time() * 1000) // tf_ms) - 1) * tf_ms
            n = 40
            opens_ms = np.array([last_open_ms - (n - 1 - k) * tf_ms
                                 for k in range(n)], dtype="int64")
            trend = np.array([entry * (1.0 + 0.03 * k / (n - 1))
                              for k in range(n)])
            dip = np.where(np.arange(n) % 5 == 2, entry * 0.012, 0.0)
            df = pd.DataFrame({
                "time": pd.to_datetime(opens_ms, unit="ms"),
                "Open": trend - entry * 0.001,
                "High": trend + entry * 0.003,
                "Low": trend - entry * 0.002 - dip,
                "Close": trend, "Volume": np.ones(n)})
            with esm._conn(w.db) as con:
                con.execute("UPDATE trades SET entry_bar_ts=? WHERE id=?",
                            (int(opens_ms[10]), tid))
            w.set_mark("BTC", float(trend[-1]))   # mark rides the trend top

            class CandleClient:
                """Fake venue client + real-pandas candles. The BARS are
                scripted DATA; everything under test — default factory,
                pinned adapter, PM math, executor — is 100% production."""

                def __init__(self, inner, frames):
                    self._inner, self._frames = inner, frames

                def candles(self, coin, tf, *a, **k):
                    return self._frames[-1]

                def __getattr__(self, name):
                    return getattr(self._inner, name)

            frames = [df]
            eng = rnr.Engine(rnr.EngineConfig(venue="extended", mode="live",
                                              data_dir=w.tmp, db_path=w.db))
            eng.client = CandleClient(w.client, frames)
            eng.executor = eng._resolve_executor()
            et = eng._resolve_exit_tick()   # PRODUCTION path — nothing injected
            check("R2-1: production resolve wires a REAL default strategy "
                  "factory (no injection)", eng._strategy_factory is not None)
            ad = eng._strategy_factory({"coin": "BTC"})
            check("R2-1: default factory = pinned canonical adapter "
                  "(extended -> strategy_xnn, DONCHIAN_N=15)",
                  type(ad).__name__ == "StrategyAdapter" and
                  int(ad.module.DONCHIAN_N) == 15)
            check("R2-1: pm_kwargs seeded per §8 (live ext config.py defaults: "
                  "be_buf=0.003, tp1_frac=0.5, max_run_r=5.0)",
                  abs(float(ad.pm._be_buffer) - 0.003) < 1e-12 and
                  abs(float(ad.pm._tp1_frac) - 0.5) < 1e-12 and
                  abs(float(ad.pm._max_run_r) - 5.0) < 1e-12)
            check("R2-1: ONE adapter per leg for the process lifetime "
                  "(live main.py:978-979 parity)",
                  eng._strategy_factory({"coin": "ETH"}) is ad)
            check("R2-1: leg router mirrors the COMBO dual-router "
                  "(scanner._is_xyz): hl/xyz_->us29, hl/BTC->crypto, "
                  "non-hl always crypto",
                  rnr._leg_for_coin("hl", "xyz_GOLD") == "us29" and
                  rnr._leg_for_coin("hl", "BTC") == "crypto" and
                  rnr._leg_for_coin("extended", "xyz_GOLD") == "crypto")
            # tick 1: the pinned PM STAGES trail_{i-1} (staged-trail law — no
            # sl_current mutation, no intent yet); steps 9-14 must actually RUN
            # (no Defer(strategy_adapter_missing) anywhere).
            res1 = et(eng)
            check("R2-1: tick 1 through the un-injected production assembly — "
                  "steps 9-14 RAN (no strategy Defer)",
                  len(res1) == 1 and "Defer" not in res1[0]["intents"],
                  str(res1))
            # tick 2 on the NEXT closed bar: staged trail promotes to
            # sl_current -> ReplaceSL(cause='trail') through the executor.
            nxt = pd.DataFrame({
                "time": pd.to_datetime(
                    np.array([last_open_ms + tf_ms], dtype="int64"), unit="ms"),
                "Open": [float(trend[-1])],
                "High": [float(trend[-1]) + entry * 0.003],
                "Low": [float(trend[-1]) - entry * 0.002],
                "Close": [float(trend[-1]) + entry * 0.001],
                "Volume": [1.0]})
            frames.append(pd.concat([df.iloc[1:], nxt], ignore_index=True))
            res2 = et(eng)
            r = w.row(tid)
            check("R2-1: REAL pinned trail EMITTED + EXECUTED via production "
                  "wiring (ReplaceSL, SL ratcheted above the entry stop)",
                  "ReplaceSL" in res2[0]["intents"] and
                  any(x.get("status") == "replaced"
                      for x in res2[0]["results"]) and
                  float(r["sl_current"]) > float(r["sl_initial"]),
                  "intents=%s sl=%s init=%s" % (res2[0]["intents"],
                                                r["sl_current"],
                                                r["sl_initial"]))
            check("R2-1: exchange SL follows the trail (venue trigger px == "
                  "journal sl_placed_px == sl_current)",
                  w.trigger_px(r["sl_order_id"]) == r["sl_placed_px"] ==
                  r["sl_current"])
            _covered("R2-1 production exit-tick wiring: default "
                     "make_strategy_adapter factory (per-leg, §8 env "
                     "pm_kwargs) resolves in the un-injected assembly; the "
                     "pinned PM's staged pivot-low trail fires end-to-end")
    finally:
        if prev_dry is not None:
            os.environ["DRY_RUN"] = prev_dry
        if prev_allow is not None:
            os.environ["ENGINE_ALLOW_PARTIAL"] = prev_allow


def s_r21_missing_adapter_fail_loud() -> None:
    print("— R2-1: Defer(strategy_adapter_missing) is NEVER a silent ok")
    from fleet_core.engine import runner as rnr
    prev_allow = os.environ.pop("ENGINE_ALLOW_PARTIAL", None)
    try:
        with World("r21loud") as w:
            _, tid = w.run_entry(w.plan("BTC"))
            t = w.build_tick(tid, strategy=None,
                             bars=TinyDF([w.mark("BTC")] * 3))
            intents = xng.tick(t)
            check("R2-1: strategy row + missing adapter -> "
                  "Defer(strategy_adapter_missing) emitted",
                  any(i.kind == "Defer" and
                      i.reason == "strategy_adapter_missing"
                      for i in intents),
                  str([(i.kind, i.reason) for i in intents]))
            raised = False
            try:
                w.executor().execute(t, intents)
            except rnr.EngineWiringError:
                raised = True
            check("R2-1: ALLOW_PARTIAL unset -> executor RAISES "
                  "EngineWiringError (silent ok forbidden)", raised)
            os.environ["ENGINE_ALLOW_PARTIAL"] = "1"
            ex = w.executor()
            n0 = len(w.alerts)
            r1 = ex.execute(t, intents)
            ex.execute(t, intents)   # second pass, same executor
            d = [x for x in r1 if x.get("intent") == "Defer" and
                 x.get("reason") == "strategy_adapter_missing"]
            check("R2-1: ENGINE_ALLOW_PARTIAL=1 -> executed with labeled why "
                  "+ CRITICAL logged ONCE",
                  bool(d) and
                  d[0].get("why") == "allow_partial_strategy_mgmt_off" and
                  sum(1 for a in w.alerts[n0:]
                      if "strategy adapter MISSING" in a) == 1,
                  "d=%s alerts=%s" % (d, w.alerts[n0:]))
            _covered("R2-1 fail-loud backstop: missing strategy adapter on a "
                     "strategy row raises EngineWiringError; explicit "
                     "ALLOW_PARTIAL=1 degrades to ONE CRITICAL + labeled ok")
    finally:
        if prev_allow is not None:
            os.environ["ENGINE_ALLOW_PARTIAL"] = prev_allow
        else:
            os.environ.pop("ENGINE_ALLOW_PARTIAL", None)


# ============================================================================
# SECTION 4 — shadow-mode zero-write proof
# ============================================================================

def s_shadow_zero_write() -> None:
    print("— shadow mode: structural zero-write proof")
    from fleet_core.engine import shadow_runner as shr
    from fleet_core.engine import runner as rnr
    with World("shadow") as w:
        # a real position + row exist; shadow must decide but never write
        _, tid = w.run_entry(w.plan("BTC"))
        w.set_mark("BTC", 54_000.0)
        wmark = w.call_mark()

        shadow = shr.ReadOnlyExchangeClient(w.ctx.make_client(),
                                            shr.ReadBudget(600.0), venue="fake")
        try:
            shr.write_fence_selftest(shadow)
            fence_ok = True
        except shr.FenceSelfTestFailed:
            fence_ok = False
        check("shadow: write_fence_selftest — all 6 writes raise locally",
              fence_ok)
        check("shadow: fence probes dropped from the intent log",
              len(shadow.recorded_intents) == 0)

        # decisions still computed over shadow READS
        t = w.build_tick(tid, strategy=ScriptedStrategy(new_sl=49_000.0),
                         bars=TinyDF([54_000.0] * 3), client=shadow)
        intents = xng.tick(t)
        check("shadow: decision core still decides (ReplaceSL computed)",
              any(i.kind == "ReplaceSL" for i in intents))

        # recorder executor catches every intent, holds no client
        jsonl = w.tmp / "shadow_decisions.jsonl"
        recorder = rnr.RecorderExecutor(jsonl, venue="fake")
        recorder.execute(intents, context={"trade_id": tid})
        lines = jsonl.read_text().strip().splitlines()
        check("shadow: recorder captured the intents (jsonl)",
              recorder.records == 1 and len(lines) == 1 and
              "ReplaceSL" in lines[0])
        check("shadow: recorder holds NO exchange client (structural)",
              not any("client" in k.lower() for k in vars(recorder)))

        # a REAL executor pointed at the shadow client is stopped at the fence
        ex = xng.IntentExecutor(shadow, w.journal(), w.quirks,
                                alert=w.alerts.append)
        blocked = False
        n_rec_before = len(shadow.recorded_intents)
        try:
            ex.execute(t, [i for i in intents if i.kind == "ReplaceSL"])
        except shr.ShadowWriteBlocked:
            blocked = True
        check("shadow: executor write blocked BEFORE any venue I/O + intent "
              "recorded",
              blocked and len(shadow.recorded_intents) == n_rec_before + 1 and
              shadow.recorded_intents[-1].op == "trigger_sl")

        # runner's proxy blocks the same P2 write surface
        proxy = rnr.ReadOnlyClientProxy(w.ctx.make_client())
        proxy_blocked = 0
        for m, args in (("market_open", ("BTC", True, 1.0)),
                        ("ensure_flat", ("BTC",)),
                        ("trigger_sl", ("BTC", False, 1.0, 45_000.0)),
                        ("cancel_sl_order", ("BTC", "oid-x")),
                        ("limit_reduce_only", ("BTC", False, 1.0, 60_000.0)),
                        ("update_leverage", ("BTC", 3))):
            try:
                getattr(proxy, m)(*args)
            except Exception:
                proxy_blocked += 1
        check("shadow: ReadOnlyClientProxy blocks all 6 writes; reads pass",
              proxy_blocked == 6 and "BTC" in proxy.open_positions())

        # socket-level block (conformance guard active for the whole world)
        sock_blocked = False
        try:
            socket.create_connection(("127.0.0.1", 9), timeout=0.05)
        except UninterceptedRealCall:
            sock_blocked = True
        except Exception:
            sock_blocked = False
        check("shadow: raw sockets blocked (UninterceptedRealCall)", sock_blocked)

        check("shadow: ZERO venue write ops across the entire shadow section",
              len(w.write_ops(since=wmark)) == 0)
        check("shadow: venue truth untouched (position + single SL intact)",
              w.position("BTC") is not None and len(w.triggers("BTC")) == 1)
        _covered("shadow-mode zero-write: fence selftest, recorder capture, "
                 "executor blocked pre-I/O, proxy block, raw sockets blocked")


# ============================================================================
# SECTION 5 — replay --all
# ============================================================================

def s_replay_all() -> None:
    print("— replay: python -m fleet_core.engine.replay --all")
    proc = subprocess.run(
        [sys.executable, "-m", "fleet_core.engine.replay", "--all"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=600)
    tail = (proc.stdout or "").strip().splitlines()[-3:]
    check("replay --all exit 0", proc.returncode == 0,
          "rc=%d tail=%s" % (proc.returncode, tail))
    check("replay --all: 10 classes green, 0 failures",
          "failures: 0" in (proc.stdout or "") and
          "classes green: 10" in (proc.stdout or ""), str(tail))


# ============================================================================
# main
# ============================================================================

SCENARIOS: List[Tuple[str, Callable[[], None]]] = [
    ("entry_lifecycle", s_entry_lifecycle),
    ("trail_and_churn", s_trail_and_churn),
    ("tp1_be", s_tp1_be),
    ("exit_strategy_close", s_exit_strategy_close),
    ("exit_sl_fire_attribution", s_exit_sl_fire_attribution),
    ("abort_unwind_lifecycle", s_abort_unwind_lifecycle),
    ("sl_fail_3x", s_sl_fail_3x),
    ("K1", k1_before_intent),
    ("K2", k2_after_intent),
    ("K3", k3_sent_not_dispatched),
    ("K4", k4_response_lost),
    ("K5", k5_filled_naked),
    ("K6", k6_sl_landed_unpersisted),
    ("K7", k7_protected),
    ("K8", k8_open_pre_tp1),
    ("K9", k9_mid_abort_unwind),
    ("K9b", k9b_reject_uncommitted),
    ("K10", k10_exit_windows),
    ("K11", k11_durability),
    ("K12", k12_supersede),
    ("covered_adoption_3a", s_covered_adoption_3a),
    ("adopt_3c_naked", s_adopt_3c_naked),
    ("prefix_manual_3b", s_prefix_manual_3b),
    ("fenced_untouchable", s_fenced_untouchable),
    ("manual_claimed", s_manual_claimed),
    ("orphan_sweep", s_orphan_sweep),
    ("EF1_manual_after_abort", s_ef1_manual_after_abort),
    ("EF2_k7_sl_fired_downtime", s_ef2_k7_sl_fired_downtime),
    ("EF5_EF6_adoption_seeding", s_ef5_ef6_adoption_seeding),
    ("EF4_runner_wiring", s_ef4_runner_wiring),
    ("R21_production_strategy_wiring", s_r21_production_strategy_wiring),
    ("R21_missing_adapter_fail_loud", s_r21_missing_adapter_fail_loud),
    ("shadow_zero_write", s_shadow_zero_write),
    ("replay_all", s_replay_all),
]


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="fleet_core.engine.selftest")
    ap.add_argument("--only", help="run a single scenario by name")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args(argv)
    if args.list:
        for name, _fn in SCENARIOS:
            print(name)
        return 0
    logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
    install_scripted_fences()
    t0 = time.time()
    print("== fleet_core.engine INTEGRATION selftest (offline, fakes only) ==")
    for name, fn in SCENARIOS:
        if args.only and name != args.only:
            continue
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — a scenario crash is a failure
            import traceback
            traceback.print_exc()
            check("scenario %s completed" % name, False, repr(e))
    print()
    print("— crash-matrix coverage —")
    for line in CRASH_MATRIX_COVERED:
        print("  * %s" % line)
    print()
    n, nf = _N_CHECKS[0], len(_FAILURES)
    print("== %d checks, %d failures (%.1fs) ==" % (n, nf, time.time() - t0))
    if nf:
        for f in _FAILURES:
            print("  FAILED: %s" % f)
        print("INTEGRATION SELFTEST RED")
        return 1
    print("INTEGRATION SELFTEST GREEN — all checks passed (offline, fakes only)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
