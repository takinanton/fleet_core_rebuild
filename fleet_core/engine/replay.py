"""fleet_core.engine.replay — P3 replay harness (shadow-runner design §7.5).

WHAT THIS IS
------------
The REPLAY mechanism of the 48h shadow gate's per-class coverage LEDGER
(p3_design_shadow_runner.md §7.5): synthetic scripted scenarios for the 10
ledger event classes, fed through the engine (decision core + IntentExecutor)
against a SCRIPTED FAKE venue + FAKE journal, each scenario carrying EXACT
expected-intent assertions. A class with zero live occurrences in a shadow
window can only go green through this harness — the gate never blesses an
unexercised class (round-2 finding F8).

The fault side is built on the P2 conformance fault bank
(fleet_core.conformance.faults): every fake-venue call routes through a
TransportGate, so scenarios compose the SAME fault vocabulary the P2 harness
uses (accept_no_land, accept_partial_land, timeout, ...) — e.g. the
abort-unwind class = accept_partial_land(ratio < min_fill_ratio) on
market_open, exactly as §7.5 specifies.

LEDGER CLASSES COVERED (shadow §7.5 table, one or more scenarios each):
  entry, exit-SL/trail attribution, trail re-place, TP1->BE, abort-unwind,
  SL-liveness heal, phantom-K3 resolve, adopt-untracked (protect-only),
  orphan-trigger cancel (registry-gated), supersede sweep (K12).

REVIEW NIT (2) FOLDED IN — manual_claimed phantom-K3:
  `management_class='manual_claimed'` rows get the SAME phantom-K3 DB-only
  row-resolve as `protect_only` rows (position provably gone at K=3 final
  re-read -> close the ROW with attribution; a row-close touches the DB,
  never the venue — zero venue WRITE calls). This is explicit spec here and
  asserted in the phantom_k3 scenario (replay_scenarios.py). All OTHER venue
  intents for manual_claimed rows are refused by the executor (entry-SM
  §2.1 / invariant §7.12), also asserted.

ENGINE SELECTION (the EngineAPI seam)
-------------------------------------
  --engine reference  : built-in reference engine — an executable encoding of
                        the round-3 design docs' expected behavior for exactly
                        the ledger classes. Runs fully offline, no other P3
                        component required.
  --engine real       : binds fleet_core.engine.entry_sm / exit_engine /
                        reconciler per their DOCUMENTED interfaces (entry-SM
                        §3 t_* functions; exit-engine §1 tick + IntentExecutor).
                        Bind failure = hard error in this mode.
  --engine auto       : real if fully bindable, else reference (default; the
                        report header states which engine produced the verdict
                        — a gate ledger MUST be produced with engine=real).

STRATEGY MATH IS NOT REIMPLEMENTED HERE: scenarios use scripted strategy
stubs (prescribed trail targets / exit verdicts as data). Strategy math stays
byte-pinned in the exit-engine build (exit-engine §8 md5 pins); the replay
asserts PLUMBING intents, never Donchian arithmetic.

Runnable offline:
    /usr/bin/python3 -m fleet_core.engine.replay --all
Pure stdlib + fleet_core.exchange_api + fleet_core.conformance.faults (both
stdlib-pure). No venue SDK is ever imported. Python 3.9+.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from fleet_core.exchange_api import (
    ExchangeClient,
    FillResult,
    FlatResult,
    OpenOrderInfo,
    PositionInfo,
    RateLimited,
    ReadUnknown,
    SLOrderInfo,
    VenueRejected,
    WriteUnconfirmed,
)
from fleet_core.conformance.faults import (
    Fault,
    GateResponse,
    TransportConnectError,
    TransportGate,
    TransportTimeout,
    json_response,
)

__all__ = [
    "Clock",
    "EntryPlan",
    "EntryOutcome",
    "FakeJournal",
    "SMConflict",
    "VenueTruth",
    "FakeVenueClient",
    "ScriptedStrategy",
    "ReferenceEngine",
    "RealEngineBindError",
    "bind_real_engine",
    "ReplayEnv",
    "ScenarioFailure",
    "run_scenario",
    "main",
    # engine constants (labeled: current live values — exit-engine §5)
    "K_PHANTOM",
    "SL_REPLACE_THRESH_BPS",
    "RESTORE_REANCHOR_PCT",
    "RESIDUAL_PROTECT_PCT",
    "ORPHAN_DEBOUNCE_SEC",
    "LIQ_BUFFER",
    "TP1_DETECT_FRAC",
    "WRITE_OPS",
]

# ---------------------------------------------------------------------------
# Engine constants — labeled, NOT arbitrary: these are the current live fleet
# values (exit-engine design §5 last row: "protective-SL % (untracked ±6%,
# residual ±2.5%), K=3, 5bps thresh, 180s orphan debounce").
# ---------------------------------------------------------------------------
K_PHANTOM = 3                    # phantom-guard debounce (all 4 venues)
SL_REPLACE_THRESH_BPS = 5.0      # churn gate, exit-engine §7 step 2
RESTORE_REANCHOR_PCT = 0.06      # _RESTORE_REANCHOR_PCT (protective ±6%)
RESIDUAL_PROTECT_PCT = 0.025     # abort-residual protective SL ±2.5%
ORPHAN_DEBOUNCE_SEC = 180.0      # orphan-trigger sweep debounce
LIQ_BUFFER = 1.01                # SL must sit inside liquidation ×1.01
TP1_DETECT_FRAC = 0.9            # live sz < 0.9×orig => TP1 partial detected

#: venue WRITE ops (contract writes) — used by the "DB-only / zero venue
#: writes" asserts (protect_only + manual_claimed phantom-K3 row-resolve).
WRITE_OPS = frozenset({
    "market_open", "ensure_flat", "trigger_sl", "cancel_sl_order",
    "limit_reduce_only", "update_leverage",
})
READ_OPS = frozenset({
    "open_positions", "open_orders", "list_open_sl_orders",
    "list_reduce_only_triggers", "mark_price", "candles",
    "equity_with_upnl", "account_value", "margin_used_usd",
    "position_liquidation", "user_fills",
})


class Clock:
    """Deterministic scenario clock (never time.time — debounces must be
    scriptable without real sleeps)."""

    def __init__(self, start: float = 1_760_000_000.0) -> None:
        self.t = float(start)

    def now(self) -> float:
        return self.t

    def advance(self, sec: float) -> float:
        self.t += float(sec)
        return self.t

    def iso(self) -> str:
        import datetime
        return datetime.datetime.utcfromtimestamp(self.t).strftime(
            "%Y-%m-%dT%H:%M:%SZ")


# ===========================================================================
# Fake journal — entry-SM §2 schema (trades + sm_transitions + entry_cooldowns
# + placed_trigger_oids + bot_state kv + rejected_signals)
# ===========================================================================

class SMConflict(RuntimeError):
    """Guarded transition found the row in an EARLIER state than :from
    (entry-SM §1 rules: 0 rows affected + earlier state = raise)."""


_SM_ORDER = {
    "INTENT": 0, "PENDING": 1, "FILLED": 2, "PROTECTED": 3, "OPEN": 4,
    "CLOSED": 5, "ABORTING": 6, "ABORTED": 7,
}
#: entry-SM §2.3 legacy status mapping (maintained by the engine)
_LEGACY_STATUS = {
    "INTENT": "pending", "PENDING": "pending", "FILLED": "pending",
    "PROTECTED": "pending", "ABORTING": "pending",
    "OPEN": "open", "CLOSED": "closed", "ABORTED": "aborted",
}
#: live-position states for db_open fence kwarg (entry-SM §4.2 pinned kwargs)
LIVE_STATES = ("FILLED", "PROTECTED", "OPEN", "ABORTING")

_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    venue TEXT NOT NULL,
    coin TEXT NOT NULL,
    tf TEXT NOT NULL DEFAULT '8h',
    side TEXT NOT NULL DEFAULT 'long',
    status TEXT NOT NULL,
    sm_state TEXT NOT NULL,
    sm_updated_at TEXT,
    client_order_id TEXT UNIQUE,
    order_sent_at TEXT,
    fill_confirmed_at TEXT,
    fill_oid TEXT,
    entry REAL,
    size REAL,
    orig_size REAL,
    sl_initial REAL,
    sl_current REAL,
    sl_order_id TEXT,
    sl_placed_px REAL,
    sl_confirmed_at TEXT,
    tp1_order_id TEXT,
    tp1_partial_done INTEGER NOT NULL DEFAULT 0,
    tp1_frac_at_entry REAL,
    entry_bar_ts INTEGER,
    atr14 REAL,
    leverage_eff REAL,
    limit_px REAL,
    abort_reason TEXT,
    origin TEXT NOT NULL DEFAULT 'entry',
    management_class TEXT NOT NULL DEFAULT 'strategy',
    notes TEXT,
    opened_at TEXT,
    closed_at TEXT,
    exit_reason TEXT,
    exit_px REAL
);
CREATE TABLE IF NOT EXISTS sm_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    at TEXT NOT NULL,
    seq INTEGER NOT NULL,
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
    at TEXT NOT NULL,
    venue TEXT,
    coin TEXT,
    tf TEXT,
    reason TEXT
);
"""


class FakeJournal:
    """In-memory trades.db implementing the entry-SM §2 schema, with the §1
    guarded-transition law (single UPDATE ... WHERE sm_state=:from, append-only
    sm_transitions audit)."""

    def __init__(self, clock: Clock, venue: str = "replay") -> None:
        self.clock = clock
        self.venue = venue
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_TRADES_DDL)
        self._seq = 0

    # -- global monotone sequence shared with the venue gate (before/after
    #    ordering asserts: cooldown-durable-before-ensure_flat etc.)
    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # ------------------------------------------------------------ row helpers
    def insert_intent(self, plan: "EntryPlan") -> int:
        """t_intent INSERT (idempotent on client_order_id — entry-SM §3)."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO trades (venue, coin, tf, side, status,"
            " sm_state, sm_updated_at, client_order_id, sl_initial, sl_current,"
            " size, orig_size, entry_bar_ts, atr14, tp1_frac_at_entry,"
            " leverage_eff, limit_px, origin, management_class)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (plan.venue, plan.coin, plan.tf, plan.side, "pending", "INTENT",
             self.clock.iso(), plan.client_order_id, plan.sl_px, plan.sl_px,
             plan.size, plan.size, plan.entry_bar_ts, plan.atr14,
             plan.tp1_frac, plan.leverage_eff, plan.limit_px, "entry",
             "strategy"))
        if cur.rowcount == 0:  # replay — return existing trade id
            row = self.conn.execute(
                "SELECT id FROM trades WHERE client_order_id=?",
                (plan.client_order_id,)).fetchone()
            return int(row["id"])
        tid = int(cur.lastrowid)
        self._audit(tid, "-", "INTENT", {"plan": plan.coin})
        return tid

    def insert_row(self, **cols: Any) -> int:
        """Direct row INSERT for scenario setup / adopt paths."""
        cols.setdefault("venue", self.venue)
        cols.setdefault("tf", "8h")
        cols.setdefault("side", "long")
        sm = cols.get("sm_state", "OPEN")
        cols["sm_state"] = sm
        cols.setdefault("status", _LEGACY_STATUS[sm])
        cols.setdefault("sm_updated_at", self.clock.iso())
        keys = list(cols.keys())
        cur = self.conn.execute(
            "INSERT INTO trades (%s) VALUES (%s)"
            % (",".join(keys), ",".join("?" for _ in keys)),
            [cols[k] for k in keys])
        return int(cur.lastrowid)

    def row(self, tid: int) -> sqlite3.Row:
        r = self.conn.execute("SELECT * FROM trades WHERE id=?",
                              (tid,)).fetchone()
        if r is None:
            raise KeyError("no trade id %s" % tid)
        return r

    def rows_live(self) -> List[sqlite3.Row]:
        q = "SELECT * FROM trades WHERE sm_state IN (%s)" % ",".join(
            "?" for _ in LIVE_STATES)
        return list(self.conn.execute(q, LIVE_STATES))

    def db_open_coins(self) -> List[str]:
        """Pinned `_fenced` kwarg source: coins of rows with sm_state in
        {FILLED, PROTECTED, OPEN, ABORTING} (entry-SM §4.2 R3/R2-F1d)."""
        return [r["coin"] for r in self.rows_live()]

    def update(self, tid: int, **cols: Any) -> None:
        keys = list(cols.keys())
        self.conn.execute(
            "UPDATE trades SET %s WHERE id=?"
            % ",".join("%s=?" % k for k in keys),
            [cols[k] for k in keys] + [tid])

    # ------------------------------------------------- guarded SM transition
    def transition(self, tid: int, frm: str, to: str,
                   detail: Optional[Mapping[str, Any]] = None,
                   **cols: Any) -> bool:
        """entry-SM §1: one UPDATE guarded WHERE sm_state=:from. Returns True
        on transition, False on already-at-or-past no-op, raises SMConflict on
        earlier state."""
        cols = dict(cols)
        cols["sm_state"] = to
        cols["status"] = _LEGACY_STATUS[to]
        cols["sm_updated_at"] = self.clock.iso()
        keys = list(cols.keys())
        cur = self.conn.execute(
            "UPDATE trades SET %s WHERE id=? AND sm_state=?"
            % ",".join("%s=?" % k for k in keys),
            [cols[k] for k in keys] + [tid, frm])
        if cur.rowcount == 1:
            self._audit(tid, frm, to, detail)
            return True
        cur_state = self.row(tid)["sm_state"]
        if _SM_ORDER.get(cur_state, -1) >= _SM_ORDER.get(to, 99) \
                or cur_state == to:
            return False  # replay-safe no-op
        raise SMConflict("trade %s: cannot %s->%s from %s"
                         % (tid, frm, to, cur_state))

    def _audit(self, tid: int, frm: str, to: str,
               detail: Optional[Mapping[str, Any]]) -> None:
        self.conn.execute(
            "INSERT INTO sm_transitions (trade_id, at, seq, from_state,"
            " to_state, detail) VALUES (?,?,?,?,?,?)",
            (tid, self.clock.iso(), self.next_seq(), frm, to,
             json.dumps(detail or {})))

    def transitions(self, tid: int) -> List[Tuple[str, str]]:
        # ORDER BY id (insertion order): the REAL entry_sm audit rows carry no
        # seq (real-engine binding); id preserves both engines' ordering. The
        # real t_intent audits from_state '' — normalized to this harness's
        # '-' so path asserts are engine-agnostic.
        return [(r["from_state"] or "-", r["to_state"]) for r in
                self.conn.execute(
                    "SELECT from_state, to_state FROM sm_transitions "
                    "WHERE trade_id=? ORDER BY id", (tid,))]

    def transition_details(self, tid: int) -> List[Dict[str, Any]]:
        return [json.loads(r["detail"] or "{}") for r in self.conn.execute(
            "SELECT detail FROM sm_transitions WHERE trade_id=? ORDER BY id",
            (tid,))]

    # ------------------------------------------------------------- registry
    def registry_insert(self, oid: str, coin: str, kind: str,
                        trade_id: Optional[int] = None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO placed_trigger_oids"
            " (oid, coin, trade_id, placed_at, kind) VALUES (?,?,?,?,?)",
            (str(oid), coin, trade_id, self.clock.iso(), kind))

    def registry_has(self, oid: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM placed_trigger_oids WHERE oid=?",
            (str(oid),)).fetchone() is not None

    def registry_oids(self) -> List[str]:
        return [r["oid"] for r in self.conn.execute(
            "SELECT oid FROM placed_trigger_oids")]

    def historic_oid_corpus(self, tid: int) -> List[str]:
        """Exit-attribution corpus (exit-engine §6 / F4): current sl/tp oids ∪
        registry ∪ every oid in sm_transitions.detail for the trade."""
        row = self.row(tid)
        corpus = set()
        for k in ("sl_order_id", "tp1_order_id", "fill_oid"):
            if row[k]:
                corpus.add(str(row[k]))
        for r in self.conn.execute(
                "SELECT oid FROM placed_trigger_oids WHERE trade_id=? OR coin=?",
                (tid, row["coin"])):
            corpus.add(str(r["oid"]))
        for d in self.transition_details(tid):
            for key in ("oid", "sl_oid", "tp_oid", "new_oid", "old_oid"):
                if d.get(key):
                    corpus.add(str(d[key]))
        return sorted(corpus)

    # ------------------------------------------------------------- cooldowns
    def cooldown_set(self, coin: str, until_ts: float, reason: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO entry_cooldowns (coin, until_ts, reason)"
            " VALUES (?,?,?)", (coin, until_ts, reason))

    def cooldown_get(self, coin: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM entry_cooldowns WHERE coin=?", (coin,)).fetchone()

    # ------------------------------------------------------------ bot_state
    def kv_get(self, key: str, default: Any = None) -> Any:
        r = self.conn.execute("SELECT value FROM bot_state WHERE key=?",
                              (key,)).fetchone()
        return json.loads(r["value"]) if r is not None else default

    def kv_set(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?,?)",
            (key, json.dumps(value)))

    # ------------------------------------------------------ rejected_signals
    def insert_rejected(self, coin: str, reason: str, tf: str = "8h") -> None:
        self.conn.execute(
            "INSERT INTO rejected_signals (at, venue, coin, tf, reason)"
            " VALUES (?,?,?,?,?)",
            (self.clock.iso(), self.venue, coin, tf, reason))

    def rejected(self, coin: str) -> List[str]:
        return [r["reason"] for r in self.conn.execute(
            "SELECT reason FROM rejected_signals WHERE coin=?", (coin,))]


# ===========================================================================
# Fake venue — a P2-conforming ExchangeClient over an in-memory truth store,
# every call routed through the conformance TransportGate (fault bank).
# ===========================================================================

@dataclass
class _TruthPos:
    coin: str
    size_signed: float
    entry_px: float
    liquidation_px: Optional[float] = None


@dataclass
class _TruthTrig:
    coin: str
    oid: str
    trigger_px: float
    size: float
    is_buy_to_close: bool
    reduce_only: bool = True
    created_ts: float = 0.0


class VenueTruth:
    """The scripted exchange ground truth mutated only by the responder."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.positions: Dict[str, _TruthPos] = {}
        self.triggers: List[_TruthTrig] = []
        self.resting_limits: List[Dict[str, Any]] = []
        self.fills: List[Dict[str, Any]] = []
        self.mark: Dict[str, float] = {}
        self.tick_size: Dict[str, float] = {}
        self.equity: float = 100_000.0
        self.margin_used: float = 0.0
        self.slip_pct: float = 0.0002  # market fill slip vs mark (scripted)
        self._oid_n = 0

    def next_oid(self, prefix: str = "T") -> str:
        self._oid_n += 1
        return "%s%04d" % (prefix, self._oid_n)

    def round_px(self, coin: str, px: float) -> float:
        tick = self.tick_size.get(coin, 0.0)
        if tick <= 0:
            return float(px)
        n = round(px / tick)
        return round(n * tick, 12)

    def set_position(self, coin: str, size_signed: float, entry_px: float,
                     liq: Optional[float] = None) -> None:
        self.positions[coin] = _TruthPos(coin, size_signed, entry_px, liq)

    def add_trigger(self, coin: str, trigger_px: float, size: float,
                    is_buy_to_close: bool, oid: Optional[str] = None,
                    created_ts: Optional[float] = None) -> str:
        oid = oid or self.next_oid()
        self.triggers.append(_TruthTrig(
            coin, oid, trigger_px, size, is_buy_to_close,
            created_ts=self.clock.now() if created_ts is None else created_ts))
        return oid

    def triggers_for(self, coin: str) -> List[_TruthTrig]:
        return [t for t in self.triggers if t.coin == coin]

    def add_fill(self, coin: str, px: float, size: float, is_buy: bool,
                 oid: Optional[str] = None) -> None:
        self.fills.insert(0, {"coin": coin, "px": px, "sz": size,
                              "side": "B" if is_buy else "S",
                              "oid": oid, "time": self.clock.now()})


def _classify(method: str, url: str, body: Any) -> str:
    # url convention: fake://venue/<op>
    return url.rsplit("/", 1)[-1]


class FakeVenueClient(ExchangeClient):
    """Contract-conforming binding over VenueTruth through a TransportGate.

    Responder honors fault directives per the P2 fault-bank contract:
      accept_no_land      -> ack body, truth untouched -> readback empty ->
                             the binding raises WriteUnconfirmed;
      accept_partial_land -> only ratio×size lands -> binding returns the REAL
                             partial FillResult (never echoes the request);
      corrupt_item        -> read raises inside parse -> ReadUnknown.
    Transport faults (timeout/connect/429/5xx/empty/malformed) map to
    ReadUnknown / WriteUnconfirmed / RateLimited per contract law.
    """

    def __init__(self, truth: VenueTruth, journal_seq: Callable[[], int],
                 on_op: Optional[Callable[[str, Dict[str, Any]], None]] = None
                 ) -> None:
        self.truth = truth
        self.gate = TransportGate(_classify, self._respond, name="replay")
        self._on_op = on_op
        self._journal_seq = journal_seq
        self.op_seq: List[Tuple[int, str]] = []  # (global seq, op)

    # ------------------------------------------------------------ transport
    def _call(self, op: str, body: Optional[Dict[str, Any]] = None,
              write: bool = False) -> Any:
        try:
            g = self.gate.request("POST" if write else "GET",
                                  "fake://venue/%s" % op, body=body,
                                  timeout=5.0, transport="fake")
        except TransportTimeout as e:
            if write:
                raise WriteUnconfirmed(str(e), may_have_landed=None,
                                       venue="replay", op=op)
            raise ReadUnknown(str(e), venue="replay", op=op)
        except TransportConnectError as e:
            if write:
                raise WriteUnconfirmed(str(e), may_have_landed=False,
                                       venue="replay", op=op)
            raise ReadUnknown(str(e), venue="replay", op=op)
        if g.status == 429:
            raise RateLimited("replay venue 429 (op=%s)" % op, op=op)
        if g.status >= 400:
            if write:
                raise WriteUnconfirmed("HTTP %d (op=%s)" % (g.status, op),
                                       may_have_landed=None, op=op)
            raise ReadUnknown("HTTP %d (op=%s)" % (g.status, op), op=op)
        try:
            payload = g.json()
        except Exception as e:
            if write:
                raise WriteUnconfirmed("unparseable body (op=%s): %s"
                                       % (op, e), may_have_landed=None, op=op)
            raise ReadUnknown("unparseable body (op=%s): %s" % (op, e), op=op)
        if isinstance(payload, dict) and payload.get("__corrupt__"):
            raise ReadUnknown("corrupt item in %s listing" % op, op=op)
        return payload

    def _respond(self, ctx: Dict[str, Any],
                 directive: Optional[Dict[str, Any]]) -> Optional[GateResponse]:
        op = ctx["op"]
        body = ctx.get("body") or {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        self.op_seq.append((self._journal_seq(), op))
        if self._on_op is not None:
            self._on_op(op, ctx)
        t = self.truth
        # ---------------- writes (mutate truth unless directive suppresses)
        if op == "market_open":
            coin, is_buy, sz = body["coin"], body["is_buy"], float(body["sz"])
            if directive and directive.get("mode") == "no_land":
                return json_response({"status": "accepted"})
            if directive and directive.get("mode") == "partial":
                sz = sz * float(directive["ratio"])
            px = t.round_px(coin, t.mark[coin] *
                            (1 + t.slip_pct * (1 if is_buy else -1)))
            signed = sz if is_buy else -sz
            pos = t.positions.get(coin)
            if pos is None:
                t.set_position(coin, signed, px)
            else:
                pos.size_signed += signed
                if pos.size_signed == 0:
                    del t.positions[coin]
            t.add_fill(coin, px, sz, is_buy)
            return json_response({"status": "accepted"})  # ack-shaped: no fill
        if op == "ensure_flat":
            coin = body["coin"]
            pos = t.positions.pop(coin, None)
            if pos is not None:
                px = t.round_px(coin, t.mark[coin])
                t.add_fill(coin, px, abs(pos.size_signed),
                           is_buy=pos.size_signed < 0)
            return json_response({"status": "accepted"})
        if op == "trigger_sl":
            coin = body["coin"]
            if directive and directive.get("mode") == "no_land":
                return json_response({"status": "accepted"})
            accepted = t.round_px(coin, float(body["trigger_px"]))
            oid = t.add_trigger(coin, accepted, float(body["sz"]),
                                bool(body["is_buy"]))
            return json_response({"status": "accepted", "oid": oid})
        if op == "cancel_sl_order":
            coin, oid = body["coin"], str(body["oid"])
            before = len(t.triggers)
            t.triggers = [x for x in t.triggers
                          if not (x.coin == coin and x.oid == oid)]
            return json_response(
                {"status": "ok" if len(t.triggers) < before else "not_found"})
        if op == "limit_reduce_only":
            coin = body["coin"]
            oid = t.next_oid("L")
            t.resting_limits.append({"coin": coin, "oid": oid,
                                     "px": float(body["px"]),
                                     "sz": float(body["sz"]),
                                     "is_buy": bool(body["is_buy"])})
            return json_response({"status": "accepted", "oid": oid})
        if op == "update_leverage":
            return json_response({"status": "ok"})
        # ---------------------------------------------------------- reads
        if op == "open_positions":
            items = [{"coin": p.coin, "szi": p.size_signed,
                      "entry": p.entry_px, "liq": p.liquidation_px}
                     for p in t.positions.values()]
            if directive and directive.get("mode") == "corrupt_item" and items:
                items[0] = {"coin": None, "szi": "garbage"}
                return json_response({"positions": items, "__corrupt__": True})
            return json_response({"positions": items})
        if op == "list_open_sl_orders":
            coin = body.get("coin")
            return json_response(
                {"oids": [x.oid for x in t.triggers_for(coin)]})
        if op == "list_reduce_only_triggers":
            return json_response({"triggers": [
                {"coin": x.coin, "oid": x.oid, "trigger_px": x.trigger_px,
                 "sz": x.size, "is_buy": x.is_buy_to_close,
                 "created_ts": x.created_ts} for x in t.triggers]})
        if op == "open_orders":
            return json_response({"orders": [
                {"coin": x["coin"], "oid": x["oid"], "px": x["px"],
                 "sz": x["sz"], "is_buy": x["is_buy"]}
                for x in t.resting_limits]})
        if op == "mark_price":
            coin = body.get("coin")
            if coin not in t.mark:
                return json_response({"error": "unknown coin"}, status=422)
            return json_response({"mark": t.mark[coin]})
        if op == "position_liquidation":
            pos = t.positions.get(body.get("coin"))
            return json_response(
                {"liq": None if pos is None else pos.liquidation_px})
        if op == "user_fills":
            return json_response({"fills": t.fills})
        if op == "equity_with_upnl":
            return json_response({"equity": t.equity})
        if op == "account_value":
            return json_response({"av": t.equity})
        if op == "margin_used_usd":
            return json_response({"mm": t.margin_used})
        if op == "candles":
            return json_response({"bars": []})
        return None  # UninterceptedRealCall — endpoint not simulated

    # ---------------------------------------------------- contract methods
    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: Optional[float] = None,
                    allow_marketable: bool = True) -> FillResult:
        self._call("market_open", {"coin": coin, "is_buy": is_buy, "sz": sz},
                   write=True)
        # ack-shaped response by design -> MANDATORY readback (contract law 2)
        fills = self._call("user_fills")["fills"]
        landed = [f for f in fills if f["coin"] == coin
                  and f["side"] == ("B" if is_buy else "S")]
        if not landed:
            raise WriteUnconfirmed(
                "market_open(%s): accepted but readback shows no fill"
                % coin, may_have_landed=True, coin=coin, op="market_open")
        f = landed[0]
        return FillResult(coin=coin, is_buy=is_buy, avg_px=float(f["px"]),
                          size=float(f["sz"]), requested_size=float(sz),
                          oid=f.get("oid"))

    def ensure_flat(self, coin: str) -> FlatResult:
        pre = self._call("open_positions")["positions"]
        had = [p for p in pre if p["coin"] == coin]
        if not had:
            return FlatResult(coin=coin, already_flat=True, closed_size=0.0)
        self._call("ensure_flat", {"coin": coin}, write=True)
        post = self._call("open_positions")["positions"]
        if any(p["coin"] == coin for p in post):
            raise WriteUnconfirmed("ensure_flat(%s): residual remains" % coin,
                                   may_have_landed=True, coin=coin,
                                   op="ensure_flat")
        fills = self._call("user_fills")["fills"]
        px = next((float(f["px"]) for f in fills if f["coin"] == coin), None)
        return FlatResult(coin=coin, already_flat=False,
                          closed_size=abs(float(had[0]["szi"])),
                          exit_avg_px=px)

    def trigger_sl(self, coin: str, is_buy: bool, sz: float,
                   trigger_px: float) -> SLOrderInfo:
        resp = self._call("trigger_sl", {"coin": coin, "is_buy": is_buy,
                                         "sz": sz, "trigger_px": trigger_px},
                          write=True)
        oid = resp.get("oid")
        live = self._call("list_open_sl_orders", {"coin": coin})["oids"]
        if not oid or oid not in live:
            raise WriteUnconfirmed(
                "trigger_sl(%s): no matching live trigger on readback" % coin,
                may_have_landed=oid is not None, coin=coin, op="trigger_sl")
        trig = next(x for x in self.truth.triggers_for(coin) if x.oid == oid)
        return SLOrderInfo(coin=coin, oid=oid, trigger_px=trig.trigger_px,
                           size=trig.size, is_buy_to_close=is_buy)

    def cancel_sl_order(self, coin: str, oid: str) -> None:
        self._call("cancel_sl_order", {"coin": coin, "oid": oid}, write=True)
        live = self._call("list_open_sl_orders", {"coin": coin})["oids"]
        if oid in live:
            raise WriteUnconfirmed(
                "cancel_sl_order(%s, %s): still live after cancel"
                % (coin, oid), may_have_landed=True, coin=coin,
                op="cancel_sl_order")

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float,
                          px: float) -> OpenOrderInfo:
        resp = self._call("limit_reduce_only",
                          {"coin": coin, "is_buy": is_buy, "sz": sz, "px": px},
                          write=True)
        return OpenOrderInfo(coin=coin, oid=str(resp["oid"]),
                             side="buy" if is_buy else "sell", size=sz,
                             limit_px=px, reduce_only=True)

    def update_leverage(self, coin: str, leverage: int,
                        is_cross: bool = True) -> None:
        self._call("update_leverage", {"coin": coin, "leverage": leverage},
                   write=True)

    def open_positions(self) -> Mapping[str, PositionInfo]:
        items = self._call("open_positions")["positions"]
        out: Dict[str, PositionInfo] = {}
        for p in items:
            out[p["coin"]] = PositionInfo(
                coin=p["coin"], size_signed=float(p["szi"]),
                entry_px=float(p["entry"]), liquidation_px=p.get("liq"))
        return out

    def open_orders(self) -> Sequence[OpenOrderInfo]:
        return [OpenOrderInfo(coin=o["coin"], oid=str(o["oid"]),
                              side="buy" if o["is_buy"] else "sell",
                              size=float(o["sz"]), limit_px=float(o["px"]),
                              reduce_only=True)
                for o in self._call("open_orders")["orders"]]

    def list_open_sl_orders(self, coin: str) -> Sequence[str]:
        return list(self._call("list_open_sl_orders", {"coin": coin})["oids"])

    def list_reduce_only_triggers(self) -> Sequence[OpenOrderInfo]:
        out = []
        for x in self._call("list_reduce_only_triggers")["triggers"]:
            info = OpenOrderInfo(coin=x["coin"], oid=str(x["oid"]),
                                 side="buy" if x["is_buy"] else "sell",
                                 size=float(x["sz"]),
                                 trigger_px=float(x["trigger_px"]),
                                 reduce_only=True, is_trigger=True,
                                 raw={"created_ts": x["created_ts"]})
            out.append(info)
        return out

    def mark_price(self, coin: str, max_age_sec: float = 5.0) -> float:
        return float(self._call("mark_price", {"coin": coin})["mark"])

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> Any:
        return self._call("candles", {"coin": coin})["bars"]

    def equity_with_upnl(self) -> float:
        return float(self._call("equity_with_upnl")["equity"])

    def account_value(self) -> float:
        return float(self._call("account_value")["av"])

    def margin_used_usd(self) -> float:
        return float(self._call("margin_used_usd")["mm"])

    def position_liquidation(self, coin: str) -> Optional[float]:
        v = self._call("position_liquidation", {"coin": coin})["liq"]
        return None if v is None else float(v)

    def user_fills(self, max_age_sec: float = 60.0
                   ) -> Sequence[Mapping[str, Any]]:
        return list(self._call("user_fills")["fills"])

    def invalidate_positions_cache(self) -> None:  # no cache in the fake
        return None

    # ---------------------------------------------------------- assert help
    def ops(self) -> List[str]:
        return [op for _, op in self.op_seq]

    def write_ops_for_coin(self, coin: str) -> List[str]:
        out = []
        for c in self.gate.calls:
            if c["op"] not in WRITE_OPS:
                continue
            b = c.get("body") or {}
            if isinstance(b, str):
                try:
                    b = json.loads(b)
                except Exception:
                    b = {}
            if b.get("coin") == coin:
                out.append(c["op"])
        return out


# ===========================================================================
# Plans / intents / scripted strategy
# ===========================================================================

@dataclass
class EntryPlan:
    """Frozen entry plan (entry-SM §3 t_intent precondition: plan complete)."""
    coin: str
    size: float
    sl_px: float
    limit_px: float
    venue: str = "replay"
    tf: str = "8h"
    side: str = "long"
    min_fill_ratio: float = 0.10   # MIN_FILL_RATIO=10% (live value)
    cap_tick_tol: float = 0.001    # ±0.1% cap-breach tick tolerance (live)
    leverage_eff: float = 5.0
    entry_bar_ts: int = 0
    atr14: float = 0.0
    tp1_frac: float = 0.0
    cooldown_sec: float = 900.0    # entry-abort cooldown (900s/15min, live)
    client_order_id: str = ""

    def __post_init__(self) -> None:
        if not self.client_order_id:
            self.client_order_id = "coid-%s-%s-%d" % (
                self.venue, self.coin, self.entry_bar_ts)


@dataclass
class EntryOutcome:
    trade_id: int
    final_state: str
    detail: str = ""


@dataclass
class Intent:
    """Normalized intent record for asserts. kind ∈ exit-engine §3 vocabulary:
    NoOp, Defer, HealSL, ReplaceSL, Close, RecordExit, MarkTP1Partial,
    EscalateNaked, AdoptResolve (+ 'Refused' executor verdicts)."""
    kind: str
    reason: str = ""
    params: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:  # compact in failure messages
        return "%s(%s)" % (self.kind, self.reason or
                           ",".join("%s=%s" % kv for kv in
                                    sorted(self.params.items())))


@dataclass
class ScriptedStrategy:
    """Scenario-scripted strategy adapter — prescribed OUTPUTS, no math.

    exit-engine §2 'strategy' seam. The replay asserts plumbing; strategy
    math itself is byte-pinned elsewhere (exit-engine §8). NOTE (EF3): the
    BE-after-TP1 px is ENGINE math (entry×(1∓TRAIL_AFTER_TP_BUFFER_PCT),
    VenueQuirks-seeded) — never a scripted strategy output."""
    new_sl: Optional[Callable[[sqlite3.Row], Optional[float]]] = None
    exit_reason: Optional[Callable[[sqlite3.Row], Optional[str]]] = None
    sl_hit: Optional[Callable[[sqlite3.Row], Optional[str]]] = None
    is_entry_bar: bool = False


# ===========================================================================
# Reference engine — executable encoding of the round-3 design docs for the
# ledger classes (entry-SM §3/§4/§5, exit-engine §3/§4/§7).
# ===========================================================================

class ReferenceEngine:
    name = "reference"

    def __init__(self, journal: FakeJournal, client: FakeVenueClient,
                 clock: Clock,
                 criticals: Optional[List[str]] = None) -> None:
        self.j = journal
        self.c = client
        self.clock = clock
        self.criticals = criticals if criticals is not None else []
        self.orphan_seen: Dict[Tuple[str, str], float] = {}
        # EF3: BE-after-TP1 buffer (mirrors VenueQuirks.trail_after_tp_buffer_pct
        # — live .env TRAIL_AFTER_TP_BUFFER_PCT, ext/hl = 0.003); scenario-set.
        self.be_buffer_pct = 0.003

    # ------------------------------------------------------------- helpers
    def _critical(self, msg: str) -> None:
        self.criticals.append(msg)

    def _liq_guard(self, coin: str, is_long: bool, px: float,
                   liq: Optional[float]) -> float:
        if liq is None:
            return px
        if is_long:
            floor = liq * LIQ_BUFFER
            return max(px, floor)
        ceil = liq / LIQ_BUFFER
        return min(px, ceil)

    def _protective_gate(self, coin: str, is_long: bool,
                         candidate: float) -> Tuple[float, bool]:
        """PROTECTIVE-PLACEMENT LAW (entry-SM §4, R3/R2-F1c): candidate
        through/at current mark -> re-anchor CURRENT mark ∓6%, never close,
        never a remembered px. Gates EVERY protective placement."""
        mark = self.c.mark_price(coin)
        if (is_long and candidate >= mark) or \
                (not is_long and candidate <= mark):
            re = mark * (1 - RESTORE_REANCHOR_PCT) if is_long \
                else mark * (1 + RESTORE_REANCHOR_PCT)
            return re, True
        return candidate, False

    # ------------------------------------------------------ entry pipeline
    def entry(self, plan: EntryPlan) -> EntryOutcome:
        """entry-SM §3 fleet pipeline: t_intent -> t_mark_sent -> market_open
        -> caller policy -> t_fill | abort ladder -> trigger_sl -> registry
        INSERT -> t_protect -> post-invariant -> t_open."""
        j, c = self.j, self.c
        tid = j.insert_intent(plan)
        # t_mark_sent MUST commit BEFORE dispatch (invariant §7.1)
        j.transition(tid, "INTENT", "PENDING", {"op": "t_mark_sent"},
                     order_sent_at=self.clock.iso())
        try:
            fr = c.market_open(plan.coin, plan.side == "long", plan.size)
        except VenueRejected as e:
            self._abort(tid, plan, "venue_rejected:%s" % e.reason,
                        proof="VenueRejected", unwind=False)
            return EntryOutcome(tid, "ABORTED", "venue_rejected")
        except WriteUnconfirmed:
            return EntryOutcome(tid, "PENDING", "write_unconfirmed_reconcile")
        # caller policy vs the PERSISTED limit_px + INTENT min_fill snapshot
        row = j.row(tid)
        verdict = self._entry_policy(row, fr.avg_px, fr.fill_ratio)
        if verdict is not None:
            self._abort(tid, plan, verdict, proof="FlatResult", unwind=True)
            return EntryOutcome(tid, "ABORTED", verdict)
        j.transition(tid, "PENDING", "FILLED",
                     {"fill_px": fr.avg_px, "fill_sz": fr.size,
                      "oid": fr.oid},
                     entry=fr.avg_px, size=fr.size, orig_size=fr.size,
                     fill_oid=fr.oid, fill_confirmed_at=self.clock.iso())
        # SL ladder: liq-guard + protective-placement law -> place -> registry
        liq = c.position_liquidation(plan.coin)
        target, _re = self._protective_gate(plan.coin, plan.side == "long",
                                            plan.sl_px)
        target = self._liq_guard(plan.coin, plan.side == "long", target, liq)
        try:
            sl = c.trigger_sl(plan.coin, plan.side != "long", fr.size, target)
        except (VenueRejected, WriteUnconfirmed) as e:
            self._critical("entry %s: SL placement failed (%s) — abort"
                           % (plan.coin, e))
            self._abort(tid, plan, "sl_placement_failed", proof="FlatResult",
                        unwind=True)
            return EntryOutcome(tid, "ABORTED", "sl_placement_failed")
        j.registry_insert(sl.oid, plan.coin, "sl", tid)  # F5: durable FIRST
        j.transition(tid, "FILLED", "PROTECTED",
                     {"sl_oid": sl.oid, "sl_placed_px": sl.trigger_px},
                     sl_order_id=sl.oid, sl_placed_px=sl.trigger_px,
                     sl_current=sl.trigger_px,
                     sl_confirmed_at=self.clock.iso())
        j.transition(tid, "PROTECTED", "OPEN", {"op": "t_open"},
                     opened_at=self.clock.iso())
        return EntryOutcome(tid, "OPEN", "")

    def _entry_policy(self, row: sqlite3.Row, fill_px: float,
                      fill_ratio: float) -> Optional[str]:
        """Caller policy re-run from PERSISTED values (F3): cap-breach vs
        row.limit_px (±0.1% tick tol) + min_fill_ratio snapshot. Reason
        strings == the PRODUCTION entry_sm.run_entry_policy strings (these
        land in abort_reason/rejected_signals — one vocabulary, both engines)."""
        if fill_ratio < 0.10:  # MIN_FILL_RATIO snapshot (live 10%)
            return "entry_fill_below_min_fill_ratio"
        lim = row["limit_px"]
        if lim is not None:
            tol = float(lim) * 0.001
            if (row["side"] == "long" and fill_px > float(lim) + tol) or \
                    (row["side"] != "long" and fill_px < float(lim) - tol):
                return "entry_cap_breach_vs_persisted_limit_px"
        return None

    def _abort(self, tid: int, plan: EntryPlan, reason: str, proof: str,
               unwind: bool) -> None:
        """t_abort_begin (durable BEFORE any unwind I/O, cooldown armed HERE)
        -> optional ensure_flat -> t_abort_final (entry-SM §3 F3)."""
        j = self.j
        row = j.row(tid)
        j.transition(tid, row["sm_state"], "ABORTING",
                     {"abort_reason": reason}, abort_reason=reason)
        j.cooldown_set(plan.coin, self.clock.now() + plan.cooldown_sec, reason)
        if unwind:
            self.c.ensure_flat(plan.coin)  # FlatResult or raises (stays ABORTING)
        j.transition(tid, "ABORTING", "ABORTED", {"proof": proof})
        j.insert_rejected(plan.coin, reason, plan.tf)

    # -------------------------------------------------------- exit tick
    def tick_row(self, tid: int,
                 strategy: Optional[ScriptedStrategy] = None) -> List[Intent]:
        """One exit-engine tick for one row: pure decide -> execute.
        Returns the EXECUTED intent list (executor verdicts included)."""
        intents = self._decide(tid, strategy or ScriptedStrategy())
        return self._execute(tid, intents)

    def _decide(self, tid: int, st: ScriptedStrategy) -> List[Intent]:
        j, c = self.j, self.c
        row = j.row(tid)
        coin = row["coin"]
        mgmt = row["management_class"]
        is_long = row["side"] == "long"
        # -- presence (ONE read up-front, tri-state; UNKNOWN never a miss)
        try:
            positions = c.open_positions()
            presence = "PRESENT" if coin in positions else "ABSENT"
        except (ReadUnknown, RateLimited):
            return [Intent("Defer", "presence_unknown")]
        miss_key = "phantom_miss:%s:%s" % (row["venue"], tid)
        if presence == "ABSENT":
            miss = int(j.kv_get(miss_key, 0)) + 1
            j.kv_set(miss_key, miss)  # persisted counter (survives restart)
            if miss < K_PHANTOM:
                return [Intent("Defer", "phantom_miss_n<K",
                               {"miss": miss})]
            # K=3: final cache-invalidated re-read
            c.invalidate_positions_cache()
            try:
                if coin in c.open_positions():
                    j.kv_set(miss_key, 0)
                    return [Intent("Defer", "phantom_final_reread_present")]
            except (ReadUnknown, RateLimited):
                return [Intent("Defer", "presence_unknown")]
            return [Intent("AdoptResolve", "phantom_k3_confirmed_absent")]
        j.kv_set(miss_key, 0)
        pos = positions[coin]

        # -- manual_claimed: FULL hands-off while position present (§7.12) —
        #    the ONLY row action it ever gets is the phantom-K3 row-resolve
        #    above (review nit 2).
        if mgmt == "manual_claimed":
            return [Intent("NoOp", "manual_claimed_hands_off")]

        # -- protect_only: cover-check heal semantics ONLY (exit-engine §3 R3)
        if mgmt == "protect_only":
            return self._decide_protect_only(row, pos)

        # -- SL-liveness (step 7) + supersede sweep (step 7b), same read
        try:
            live_oids = list(c.list_open_sl_orders(coin))
            sl_live_known = True
        except (ReadUnknown, RateLimited):
            live_oids, sl_live_known = [], False  # assume-live (anti heal-storm)
        intents: List[Intent] = []
        if sl_live_known:
            own = [o for o in live_oids if j.registry_has(o)]
            if row["sl_order_id"] and row["sl_order_id"] not in live_oids:
                target = float(row["sl_current"] or row["sl_initial"])
                intents.append(Intent("HealSL", "sl_not_live",
                                      {"target_px": target}))
                return intents  # heal first; sweep re-runs next tick
            if len(own) > 1:  # K12 litter -> converge (exit-engine §7-sweep)
                for o in own:
                    if o != row["sl_order_id"]:
                        intents.append(Intent("SupersedeSweep",
                                              "supersede_sweep", {"oid": o}))
        else:
            intents.append(Intent("NoOp", "sl_liveness_unknown_assume_live"))

        # -- TP1 partial detect (step 8). EF3: BE = entry×(1∓buf) raise-only
        # vs sl_current (live _handle_tp1_partial math — ext trader.py:826-830;
        # buffer = VenueQuirks TRAIL_AFTER_TP_BUFFER_PCT seam). Raw entry is
        # NEVER the BE target when buf > 0.
        if not row["tp1_partial_done"] and (row["tp1_frac_at_entry"] or 0) > 0 \
                and row["orig_size"] and \
                abs(pos.size_signed) < TP1_DETECT_FRAC * float(row["orig_size"]):
            buf = float(self.be_buffer_pct or 0.0)
            entry = float(row["entry"])
            be = entry * (1.0 - buf) if is_long else entry * (1.0 + buf)
            cur = row["sl_current"]
            if cur is not None:
                be = max(be, float(cur)) if is_long else min(be, float(cur))
            intents.append(Intent("MarkTP1Partial", "tp1_fill_detected",
                                  {"remainder": abs(pos.size_signed),
                                   "be_px": be}))
            return intents
        # -- strategy exits (step 10; entry-bar suppression step 11)
        if not st.is_entry_bar:
            reason = st.exit_reason(j.row(tid)) if st.exit_reason else None
            if reason:
                intents.append(Intent("Close", reason))
                return intents
            hit = st.sl_hit(j.row(tid)) if st.sl_hit else None
            if hit:
                intents.append(Intent("Close", hit))
                return intents
            # -- trail (step 14)
            new_sl = st.new_sl(j.row(tid)) if st.new_sl else None
            if new_sl is not None:
                placed = row["sl_placed_px"]
                if placed is not None and placed > 0 and \
                        abs(new_sl - float(placed)) / float(placed) * 1e4 \
                        <= SL_REPLACE_THRESH_BPS:
                    intents.append(Intent("NoOp", "churn_gate_5bps"))
                elif is_long and row["sl_current"] is not None and \
                        new_sl <= float(row["sl_current"]):
                    intents.append(Intent("NoOp", "trail_raise_only"))
                else:
                    intents.append(Intent("ReplaceSL", "trail",
                                          {"target_px": new_sl}))
        if not intents:
            intents.append(Intent("NoOp", "nothing_due"))
        return intents

    def _decide_protect_only(self, row: sqlite3.Row,
                             pos: PositionInfo) -> List[Intent]:
        """protect_only authority whitelist (entry-SM §4.2.3): (i) cover-check
        heal; (ii) phantom-K3 row-resolve (handled in _decide). NOTHING else."""
        coin = row["coin"]
        try:
            live = list(self.c.list_open_sl_orders(coin))
        except (ReadUnknown, RateLimited):
            return [Intent("NoOp", "cover_check_unknown_assume_covered")]
        if row["sl_order_id"]:  # 3c: engine-own protective SL
            if row["sl_order_id"] in live:
                return [Intent("NoOp", "protect_only_engine_sl_resting")]
        elif live:  # 3a: any resting trigger covers -> NoOp
            return [Intent("NoOp", "protect_only_covered")]
        # cover vanished -> re-evaluate the 4.2.3 branch AT CURRENT STATE
        if self._prefix_manual(coin):
            return [Intent("EscalateNaked", "unreconciled_manual_prefix")]
        return [Intent("HealSL", "protect_only_cover_vanished",
                       {"target_px": None})]  # px derived at CURRENT mark ∓6%

    # --------------------------------------------------------- executor
    def _execute(self, tid: int, intents: List[Intent]) -> List[Intent]:
        out: List[Intent] = []
        for it in intents:
            done = self._execute_one(tid, it)
            out.append(done)
            if done.kind in ("Close", "RecordExit", "EscalateNaked",
                             "AdoptResolve") and done.reason != "REFUSED":
                break  # stop on first Close-class intent (exit-engine §3)
        return out

    def executor_try(self, tid: int, intent: Intent) -> Intent:
        """Direct executor injection (harness authority-gate tests,
        entry-SM invariant §7.9/§7.12)."""
        return self._execute_one(tid, intent)

    def _execute_one(self, tid: int, it: Intent) -> Intent:
        j, c = self.j, self.c
        row = j.row(tid)
        coin = row["coin"]
        mgmt = row["management_class"]
        # ---- AUTHORITY GATES (F2 / R2-F1d), executor-level not caller
        if mgmt == "manual_claimed" and it.kind not in (
                "NoOp", "Defer", "AdoptResolve"):
            self._critical("executor REFUSED %s for manual_claimed row %s"
                           % (it.kind, tid))
            return Intent(it.kind, "REFUSED",
                          {"why": "manual_claimed_full_hands_off"})
        if it.kind in ("Close", "EscalateNaked") and mgmt != "strategy":
            self._critical("executor REFUSED %s for %s row %s"
                           % (it.kind, mgmt, tid))
            return Intent(it.kind, "REFUSED",
                          {"why": "no_close_authority_%s" % mgmt})
        if it.kind in ("NoOp", "Defer"):
            return it
        if it.kind == "SupersedeSweep":
            c.cancel_sl_order(coin, it.params["oid"])
            return it
        if it.kind == "HealSL":
            return self._exec_heal(tid, it)
        if it.kind == "ReplaceSL":
            return self._replace_sl(tid, float(it.params["target_px"]),
                                    cause="trail")
        if it.kind == "MarkTP1Partial":
            j.update(tid, tp1_partial_done=1,
                     size=float(it.params["remainder"]))
            j._audit(tid, "OPEN", "OPEN",
                     {"op": "mark_tp1_partial",
                      "remainder": it.params["remainder"]})
            rep = self._replace_sl(tid, float(it.params["be_px"]),
                                   cause="be_after_tp1")
            return Intent("MarkTP1Partial", "executed",
                          {"be_replace": rep.reason, "be_px": it.params["be_px"]})
        if it.kind == "Close":
            return self._exec_close(tid, it.reason)
        if it.kind == "AdoptResolve":
            return self._exec_adopt_resolve(tid)
        if it.kind == "EscalateNaked":
            self._critical("row %s coin %s: STILL NAKED, manual" % (tid, coin))
            return it
        return Intent(it.kind, "unhandled_intent_kind")

    def _replace_sl(self, tid: int, target_px: float, cause: str,
                    churn_gate: bool = True) -> Intent:
        """THE replace primitive (exit-engine §7): liq-guard -> churn gate ->
        place -> 3a registry INSERT -> invariant -> supersede-cancel ->
        persist. Place-before-cancel ALWAYS."""
        j, c = self.j, self.c
        row = j.row(tid)
        coin, is_long = row["coin"], row["side"] == "long"
        liq = c.position_liquidation(coin)
        target = self._liq_guard(coin, is_long, target_px, liq)
        placed = row["sl_placed_px"]
        if churn_gate and placed is not None and placed > 0 and \
                abs(target - float(placed)) / float(placed) * 1e4 \
                <= SL_REPLACE_THRESH_BPS:
            return Intent("NoOp", "churn_gate_5bps")
        sz = abs(float(row["size"] or row["orig_size"] or 0)) or None
        pos = c.open_positions().get(coin)
        live_sz = abs(pos.size_signed) if pos is not None else sz
        new = c.trigger_sl(coin, not is_long, float(live_sz), target)
        j.registry_insert(new.oid, coin, "sl", tid)  # step 3a: durable FIRST
        j._audit(tid, row["sm_state"], row["sm_state"],
                 {"op": "replace_sl", "cause": cause, "new_oid": new.oid,
                  "old_oid": row["sl_order_id"]})
        # step 5: cancel ALL other BOT-OWN resting SLs (registry-gated)
        for oid in c.list_open_sl_orders(coin):
            if oid != new.oid and j.registry_has(oid):
                try:
                    c.cancel_sl_order(coin, oid)
                except WriteUnconfirmed:
                    pass  # keep both (reduce-only, harmless), retry next tick
        # step 6: persist venue-ACCEPTED px
        j.update(tid, sl_order_id=new.oid, sl_placed_px=new.trigger_px,
                 sl_current=new.trigger_px, sl_confirmed_at=self.clock.iso())
        return Intent("ReplaceSL", cause, {"new_oid": new.oid,
                                           "accepted_px": new.trigger_px})

    def _exec_heal(self, tid: int, it: Intent) -> Intent:
        """HealSL: strategy rows re-place at target (no churn gate — nothing
        is resting); protect_only cover-vanished heal anchors at CURRENT mark
        ∓6% — never a remembered px (exit-engine §3 R3/R2-F1)."""
        j = self.j
        row = j.row(tid)
        coin, is_long = row["coin"], row["side"] == "long"
        target = it.params.get("target_px")
        if target is None:  # protect_only heal: CURRENT mark ∓6%, always
            mark = self.c.mark_price(coin)
            target = mark * (1 - RESTORE_REANCHOR_PCT) if is_long \
                else mark * (1 + RESTORE_REANCHOR_PCT)
        else:
            target, _ = self._protective_gate(coin, is_long, float(target))
        rep = self._replace_sl(tid, float(target), cause="heal",
                               churn_gate=False)
        return Intent("HealSL", rep.reason, rep.params)

    def _exec_close(self, tid: int, reason: str) -> Intent:
        """Close ladder (exit-engine §3/§9): cancel TP -> ensure_flat ->
        cancel SL AFTER flat -> real-fill attribution -> RecordExit."""
        j, c = self.j, self.c
        row = j.row(tid)
        coin = row["coin"]
        if row["tp1_order_id"]:
            try:
                c.cancel_sl_order(coin, row["tp1_order_id"])
            except (WriteUnconfirmed, VenueRejected):
                pass  # best-effort
        flat = c.ensure_flat(coin)  # verified or raises
        if row["sl_order_id"]:
            try:
                c.cancel_sl_order(coin, row["sl_order_id"])
            except (WriteUnconfirmed, VenueRejected):
                pass
        px = flat.exit_avg_px
        if px is None:
            px = next((float(f["px"]) for f in c.user_fills()
                       if f["coin"] == coin), None)
        j.transition(tid, row["sm_state"], "CLOSED",
                     {"reason": reason, "exit_px": px},
                     exit_reason=reason, exit_px=px,
                     closed_at=self.clock.iso())
        return Intent("Close", reason, {"exit_px": px})

    def _exec_adopt_resolve(self, tid: int) -> Intent:
        """Phantom-K3 row-resolve: position provably absent -> close the ROW
        with attribution. DB-only — touches the DB, never the venue (no venue
        WRITE). Applies to strategy AND protect_only AND manual_claimed rows
        alike (review nit 2: manual_claimed gets the SAME DB-only resolve)."""
        j, c = self.j, self.c
        row = j.row(tid)
        coin = row["coin"]
        try:
            fills = [f for f in c.user_fills() if f["coin"] == coin]
        except (ReadUnknown, RateLimited):
            fills = []
        reason, px = "phantom_no_exchange_position", None
        corpus = set(j.historic_oid_corpus(tid))
        for f in fills:
            foid = str(f.get("oid")) if f.get("oid") else None
            if foid and foid in corpus:
                px = float(f["px"])
                if foid == str(row["tp1_order_id"] or ""):
                    reason = "tp"
                else:
                    trailed = row["sl_current"] is not None and \
                        row["sl_initial"] is not None and \
                        float(row["sl_current"]) != float(row["sl_initial"])
                    reason = "trail_sl" if trailed else "sl"
                break
        else:
            # 1% proximity fallback, order sl_cur -> sl_ini -> tp (HL canon)
            for f in fills:
                fpx = float(f["px"])
                for level, rn in ((row["sl_current"], None),
                                  (row["sl_initial"], None)):
                    if level and abs(fpx - float(level)) / float(level) <= 0.01:
                        trailed = row["sl_current"] is not None and \
                            row["sl_initial"] is not None and \
                            float(row["sl_current"]) != float(row["sl_initial"])
                        reason, px = ("trail_sl" if trailed else "sl"), fpx
                        break
                if px is not None:
                    break
        j.transition(tid, row["sm_state"], "CLOSED",
                     {"reason": reason, "exit_px": px, "resolve": "db_only",
                      "management_class": row["management_class"]},
                     exit_reason=reason, exit_px=px,
                     closed_at=self.clock.iso())
        return Intent("AdoptResolve", reason, {"exit_px": px})

    # -------------------------------------------- reconciler direction 2
    def _prefix_manual(self, coin: str) -> bool:
        try:
            import bot.config as _bc  # replay-injected fake, or absent
            prefixes = getattr(_bc, "MANUAL_POSITION_PREFIXES", ()) or ()
        except ImportError:
            return False
        base = str(coin).upper().replace("-PERP", "").replace("-USD", "")
        return any(p and base.startswith(str(p).upper()) for p in prefixes)

    def _fenced(self, coin: str) -> bool:
        """Position-context fence — REAL P1 orphan_sweep._fenced with the
        PINNED kwargs (entry-SM §4.2 R3/R2-F1d): db_open = live-state coins,
        bot_owned=None, oid=None, placed_oids=None. Import failure ->
        fail-closed FENCED + CRITICAL (a broken fence must fence)."""
        try:
            from fleet_core.orphan_sweep import _fenced as _p1_fenced
        except Exception as e:  # pragma: no cover — P1 module is present
            self._critical("fence source import BROKEN (%s) — fail-closed"
                           % e)
            return True
        return bool(_p1_fenced(coin, db_open=self.j.db_open_coins(),
                               bot_owned=None, oid=None, placed_oids=None))

    def reconcile_untracked(self) -> None:
        """Direction 2 (entry-SM §4.2): every venue position -> fenced /
        protect-only-tracked / strategy-managed. Rule-3 split 3a/3b/3c."""
        j, c = self.j, self.c
        try:
            positions = c.open_positions()
        except (ReadUnknown, RateLimited):
            return  # no partial-snapshot decisions
        row_coins = set(j.db_open_coins())
        for coin, pos in positions.items():
            if coin in row_coins:
                # 3b track-only rows: CRITICAL re-fires EVERY pass while the
                # prefix-manual position is still present and still naked
                # (entry-SM §4.2.3: never a silent fence, never a placement)
                r3b = j.conn.execute(
                    "SELECT id FROM trades WHERE coin=? AND notes=?"
                    " AND sm_state='OPEN'",
                    (coin, "unreconciled_manual_prefix")).fetchone()
                if r3b is not None:
                    try:
                        covered = bool(c.list_open_sl_orders(coin))
                    except (ReadUnknown, RateLimited):
                        covered = True
                    if not covered:
                        self._adopt_3b(coin)
                continue
            fenced = self._fenced(coin)
            if fenced:
                if not self._prefix_manual(coin):
                    continue  # env/FX fence: log-once skip
                # HL-canon refinement: prefix-fenced no-row -> cover check
                try:
                    covered = bool(c.list_open_sl_orders(coin))
                except (ReadUnknown, RateLimited):
                    covered = True  # read failure -> assume covered
                if covered:
                    continue  # fenced skip, log once
                self._adopt_3b(coin)  # NAKED prefix-manual
                continue
            # unfenced, no row -> rule-3 split; cover check FIRST
            try:
                live = list(c.list_open_sl_orders(coin))
            except (ReadUnknown, RateLimited):
                live = ["__assume_covered__"]  # indeterminate -> COVERED pass
            if live:
                self._adopt_3a(coin, pos, live)
            elif self._prefix_manual(coin):
                self._adopt_3b(coin)
            else:
                self._adopt_3c(coin, pos)

    def _adopt_3a(self, coin: str, pos: PositionInfo,
                  cover_oids: List[str]) -> None:
        """3a COVERED -> TRACK-ONLY: sl_* NULL by design; cover px only in
        sm_transitions.detail; never anchored to."""
        cover_px = None
        for t in self.c.truth.triggers_for(coin):
            if t.oid in cover_oids:
                cover_px = t.trigger_px
                break
        tid = self.j.insert_row(
            coin=coin, sm_state="OPEN", origin="adopted_untracked",
            management_class="protect_only", notes="adopted_covered",
            entry=pos.entry_px, size=abs(pos.size_signed),
            orig_size=abs(pos.size_signed),
            side="long" if pos.is_long else "short")
        # detail key == the production Reconciler's (covering_trigger_px) —
        # one forensic vocabulary for both engines.
        self.j._audit(tid, "-", "OPEN",
                      {"op": "adopt_covered", "covering_oids": cover_oids,
                       "covering_trigger_px": cover_px})
        self._critical("adopted COVERED untracked %s (protect_only, "
                       "track-only)" % coin)

    def _adopt_3b(self, coin: str) -> None:
        """3b PREFIX-MANUAL NAKED -> CRITICAL every pass, NO SL EVER (hl
        main.py:901-925 canon). Track-only row for visibility."""
        exists = self.j.conn.execute(
            "SELECT id FROM trades WHERE coin=? AND notes=?",
            (coin, "unreconciled_manual_prefix")).fetchone()
        if exists is None:
            pos = self.c.truth.positions.get(coin)
            self.j.insert_row(
                coin=coin, sm_state="OPEN", origin="adopted_untracked",
                management_class="protect_only",
                notes="unreconciled_manual_prefix",
                entry=pos.entry_px if pos else None,
                size=abs(pos.size_signed) if pos else None,
                side="long" if (pos and pos.size_signed > 0) else "short")
        self._critical("UNRECONCILED manual-prefix position %s: NAKED, no "
                       "bot SL will be placed — operator must fence/claim"
                       % coin)

    def _adopt_3c(self, coin: str, pos: PositionInfo) -> None:
        """3c NON-PREFIX NAKED -> protective SL at CURRENT mark ∓6%,
        liq-guarded, protective-placement law gated; registry INSERT."""
        is_long = pos.is_long
        mark = self.c.mark_price(coin)
        candidate = mark * (1 - RESTORE_REANCHOR_PCT) if is_long \
            else mark * (1 + RESTORE_REANCHOR_PCT)
        candidate, _ = self._protective_gate(coin, is_long, candidate)
        liq = self.c.position_liquidation(coin)
        target = self._liq_guard(coin, is_long, candidate, liq)
        try:
            sl = self.c.trigger_sl(coin, not is_long, abs(pos.size_signed),
                                   target)
        except (VenueRejected, WriteUnconfirmed):
            self._critical("adopt %s: SL placement FAILED — STILL NAKED, "
                           "manual" % coin)
            return
        tid = self.j.insert_row(
            coin=coin, sm_state="PROTECTED", origin="adopted_untracked",
            management_class="protect_only", notes="adopted",
            entry=pos.entry_px, size=abs(pos.size_signed),
            orig_size=abs(pos.size_signed),
            side="long" if is_long else "short",
            sl_order_id=sl.oid, sl_placed_px=sl.trigger_px,
            sl_current=sl.trigger_px)
        self.j.registry_insert(sl.oid, coin, "protective", tid)
        self.j.transition(tid, "PROTECTED", "OPEN", {"op": "adopt_3c"})
        self._critical("adopted NAKED untracked %s: protective SL @ %.6g "
                       "(protect_only)" % (coin, sl.trigger_px))

    # ------------------------------------------------- orphan-trigger pass
    def orphan_pass(self) -> List[Tuple[str, str]]:
        """entry-SM §4.2.4: reduce-only trigger, no position, no live row,
        180s debounce -> cancel ONLY when oid ∈ placed_trigger_oids."""
        j, c = self.j, self.c
        try:
            triggers = c.list_reduce_only_triggers()
            positions = c.open_positions()
        except (ReadUnknown, RateLimited):
            return []
        row_coins = set(j.db_open_coins())
        now = self.clock.now()
        cancelled: List[Tuple[str, str]] = []
        seen_now = set()
        for tr in triggers:
            key = (tr.coin.upper(), str(tr.oid))
            seen_now.add(key)
            if tr.coin in positions or tr.coin in row_coins:
                self.orphan_seen.pop(key, None)
                continue
            first = self.orphan_seen.setdefault(key, now)
            if now - first < ORPHAN_DEBOUNCE_SEC:
                continue
            if not j.registry_has(tr.oid):
                continue  # non-registry -> manual bracket: log + leave
            c.cancel_sl_order(tr.coin, tr.oid)
            cancelled.append((tr.coin, str(tr.oid)))
            self.orphan_seen.pop(key, None)
        for key in list(self.orphan_seen):
            if key not in seen_now:
                self.orphan_seen.pop(key, None)
        return cancelled


# ===========================================================================
# Real-engine binding (documented interfaces; integration seam)
# ===========================================================================

class RealEngineBindError(RuntimeError):
    pass


def bind_real_engine(journal: FakeJournal, client: FakeVenueClient,
                     clock: Clock, criticals: List[str]) -> Any:
    """Bind fleet_core.engine.{entry_sm,exit_engine} per their DOCUMENTED
    interfaces (p3_design_entry_sm.md §3, p3_design_exit_engine.md §1/§3).

    The real components are built in parallel; this binding codes against the
    design-doc surface and raises RealEngineBindError with the exact gap when
    the surface is absent/different — `--engine auto` then falls back to the
    reference engine and the report says so (a GATE ledger must be produced
    with engine=real).

    REPLAY-FACING HOOK (integration contract, documented here): the real
    engine side exposes

        fleet_core.engine.exit_engine.build_replay_engine(
            journal, client, clock, criticals) -> engine

    returning an object with the scenario-facing surface of ReferenceEngine:
        entry(plan) -> EntryOutcome
        tick_row(trade_id, strategy=None) -> list[Intent-like]   (obj.kind/.reason/.params)
        executor_try(trade_id, intent) -> Intent-like
        reconcile_untracked() -> None
        orphan_pass() -> list[(coin, oid)]
    `journal`/`client`/`clock` are this module's fakes (client is a P2-
    conforming ExchangeClient — the engine needs nothing else by design:
    exit-engine §1 pure core + IntentExecutor over the P2 contract). Until
    that hook exists this raises RealEngineBindError; `--engine auto` then
    runs the reference engine and the report says so."""
    import importlib
    try:
        esm = importlib.import_module("fleet_core.engine.entry_sm")
        xng = importlib.import_module("fleet_core.engine.exit_engine")
    except Exception as e:
        raise RealEngineBindError("import failed: %s" % e)
    missing = [n for n in ("t_intent", "t_mark_sent", "t_fill", "t_protect",
                           "t_open", "t_abort_begin", "t_abort_final")
               if not hasattr(esm, n)]
    for n in ("tick", "IntentExecutor"):
        if not hasattr(xng, n):
            missing.append("exit_engine.%s" % n)
    if missing:
        raise RealEngineBindError("documented surface missing: %s"
                                  % ", ".join(missing))
    builder = getattr(xng, "build_replay_engine", None)
    if builder is None:
        raise RealEngineBindError(
            "exit_engine.build_replay_engine hook absent — wire it at "
            "integration (surface documented in replay.bind_real_engine)")
    return builder(journal, client, clock, criticals)


# ===========================================================================
# Environment + runner
# ===========================================================================

class ScenarioFailure(AssertionError):
    pass


class ReplayEnv:
    """Per-scenario isolated world: fake journal + fake venue (fault-bank
    gated) + engine + deterministic clock + assert helpers."""

    def __init__(self, engine_mode: str = "reference") -> None:
        self.clock = Clock()
        self.journal = FakeJournal(self.clock)
        self.truth = VenueTruth(self.clock)
        self.criticals: List[str] = []
        self.hook_failures: List[str] = []
        self._op_hooks: List[Callable[[str, Dict[str, Any]], None]] = []
        self.client = FakeVenueClient(self.truth, self.journal.next_seq,
                                      on_op=self._dispatch_hooks)
        if engine_mode == "real":
            self.engine = bind_real_engine(self.journal, self.client,
                                           self.clock, self.criticals)
        else:
            self.engine = ReferenceEngine(self.journal, self.client,
                                          self.clock, self.criticals)

    # --------------------------------------------------------------- hooks
    def _dispatch_hooks(self, op: str, ctx: Dict[str, Any]) -> None:
        for h in self._op_hooks:
            try:
                h(op, ctx)
            except AssertionError as e:
                self.hook_failures.append(str(e))

    def on_op(self, hook: Callable[[str, Dict[str, Any]], None]) -> None:
        self._op_hooks.append(hook)

    # ------------------------------------------------------------- asserts
    def check(self, cond: bool, msg: str) -> None:
        if not cond:
            raise ScenarioFailure(msg)

    def check_intents(self, got: List[Intent], expected: List[Tuple[str, str]],
                      label: str) -> None:
        """EXACT expected-intent assert: (kind, reason-prefix) pairs."""
        norm = [(i.kind, i.reason) for i in got]
        ok = len(norm) == len(expected) and all(
            g[0] == e[0] and (e[1] == "" or g[1].startswith(e[1]))
            for g, e in zip(norm, expected))
        if not ok:
            raise ScenarioFailure(
                "%s: expected intents %s, got %s" % (label, expected, norm))

    def check_ops_in_order(self, ops_subseq: List[str], coin: Optional[str]
                           = None, label: str = "") -> None:
        """Assert ops_subseq occurs as an ordered subsequence of venue ops."""
        hay = []
        for c in self.client.gate.calls:
            b = c.get("body") or {}
            if isinstance(b, str):
                try:
                    b = json.loads(b)
                except Exception:
                    b = {}
            if coin is None or b.get("coin") == coin:
                hay.append(c["op"])
        i = 0
        for op in hay:
            if i < len(ops_subseq) and op == ops_subseq[i]:
                i += 1
        if i != len(ops_subseq):
            raise ScenarioFailure(
                "%s: expected op subsequence %s within %s"
                % (label or "ops", ops_subseq, hay))

    def check_no_write_ops(self, coin: str, label: str = "") -> None:
        w = self.client.write_ops_for_coin(coin)
        if w:
            raise ScenarioFailure(
                "%s: expected ZERO venue write ops for %s, got %s"
                % (label or "db-only", coin, w))

    def flush_hook_failures(self) -> None:
        if self.hook_failures:
            raise ScenarioFailure("op-hook asserts failed: "
                                  + " | ".join(self.hook_failures))

    # ----------------------------------------------------------- fault API
    def add_fault(self, **kw: Any) -> Fault:
        return self.client.gate.add_fault(Fault(**kw))


# --------------------------------------------------------------------------
# fake `bot` module injection (fence scenarios exercise the REAL P1
# orphan_sweep._fenced against scripted venue config)
# --------------------------------------------------------------------------

class fake_bot_config:
    """Context manager: install a fake `bot` package in sys.modules so the
    P1 `_fenced` chain (bot.config / bot.universe imports) runs offline with
    scripted fence lists. Restores sys.modules on exit."""

    def __init__(self, manual_prefixes: Sequence[str] = (),
                 fx_exclude: Sequence[str] = (),
                 universe_exclude: Sequence[str] = (),
                 foreign_skip: Sequence[str] = ()) -> None:
        import types
        bot = types.ModuleType("bot")
        cfg = types.ModuleType("bot.config")
        cfg.FX_EXCLUDE = tuple(fx_exclude)
        cfg.MANUAL_POSITION_PREFIXES = tuple(manual_prefixes)
        cfg.FOREIGN_SKIP_PREFIXES = tuple(foreign_skip)
        uni = types.ModuleType("bot.universe")
        uni.UNIVERSE_SYMBOL_EXCLUDE = set(universe_exclude)
        uni._is_fx = lambda coin: False
        bot.config = cfg
        bot.universe = uni
        self._mods = {"bot": bot, "bot.config": cfg, "bot.universe": uni}
        self._saved: Dict[str, Any] = {}

    def __enter__(self) -> "fake_bot_config":
        for name, mod in self._mods.items():
            self._saved[name] = sys.modules.get(name)
            sys.modules[name] = mod
        return self

    def __exit__(self, *exc: Any) -> None:
        for name in self._mods:
            old = self._saved.get(name)
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


# ===========================================================================
# CLI
# ===========================================================================

def run_scenario(scn: "Any", engine_mode: str) -> Tuple[str, str]:
    """Run one scenario. Returns (result, detail): PASS / FAIL / ERROR /
    BIND-FAIL."""
    try:
        env = ReplayEnv(engine_mode="real" if engine_mode == "real"
                        else "reference")
    except RealEngineBindError as e:
        return "BIND-FAIL", str(e)
    try:
        scn.run(env)
        env.flush_hook_failures()
        return "PASS", ""
    except ScenarioFailure as e:
        return "FAIL", str(e)
    except Exception as e:
        return "ERROR", "%s: %s\n%s" % (type(e).__name__, e,
                                        traceback.format_exc(limit=6))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="fleet_core.engine.replay",
        description="P3 replay harness — 10 coverage-ledger classes "
                    "(shadow design §7.5), exact expected-intent asserts, "
                    "offline (fake venue + fake journal + P2 fault bank).")
    ap.add_argument("--all", action="store_true",
                    help="run every scenario (the ledger)")
    ap.add_argument("--scenario", action="append", default=[],
                    help="run selected scenario(s) by name")
    ap.add_argument("--list", action="store_true", help="list scenarios")
    ap.add_argument("--engine", choices=("auto", "reference", "real"),
                    default="auto",
                    help="auto = real engine if bindable else reference "
                         "(default); the report states which produced the "
                         "verdict")
    args = ap.parse_args(argv)

    from fleet_core.engine import replay_scenarios as rs
    scenarios = rs.SCENARIOS

    if args.list:
        for s in scenarios:
            print("%-28s %s" % (s.name, s.ledger_class))
        return 0

    selected = scenarios
    if args.scenario:
        wanted = set(args.scenario)
        selected = [s for s in scenarios if s.name in wanted]
        unknown = wanted - {s.name for s in selected}
        if unknown:
            print("unknown scenario(s): %s" % ", ".join(sorted(unknown)),
                  file=sys.stderr)
            return 2
    elif not args.all:
        ap.print_help()
        return 2

    # engine mode resolution
    engine_mode = args.engine
    bind_note = ""
    if engine_mode in ("auto", "real"):
        try:
            probe = ReplayEnv.__new__(ReplayEnv)  # cheap probe env
            probe.clock = Clock()
            probe.journal = FakeJournal(probe.clock)
            probe.truth = VenueTruth(probe.clock)
            probe.criticals = []
            probe.hook_failures = []
            probe._op_hooks = []
            probe.client = FakeVenueClient(probe.truth,
                                           probe.journal.next_seq)
            bind_real_engine(probe.journal, probe.client, probe.clock, [])
            engine_mode = "real"
        except RealEngineBindError as e:
            if args.engine == "real":
                print("ENGINE BIND FAILED (--engine real): %s" % e,
                      file=sys.stderr)
                return 1
            engine_mode = "reference"
            bind_note = ("real engine not bindable (%s) — reference engine "
                         "used; a GATE ledger requires engine=real" % e)

    rows = []
    failed = 0
    for s in selected:
        result, detail = run_scenario(s, engine_mode)
        if result != "PASS":
            failed += 1
        rows.append((s.ledger_class, s.name, result, detail))

    print("REPLAY LEDGER — engine=%s" % engine_mode)
    if bind_note:
        print("NOTE: %s" % bind_note)
    print("| # | ledger class | scenario | result |")
    print("|---|---|---|---|")
    for i, (cls, name, result, _d) in enumerate(rows, 1):
        print("| %d | %s | %s | %s |" % (i, cls, name, result))
    for cls, name, result, detail in rows:
        if result != "PASS":
            print("\n[%s] %s: %s\n%s" % (result, name, cls, detail))
    covered = {r[0] for r in rows if r[2] == "PASS"}
    print("\nclasses green: %d | scenarios: %d | failures: %d"
          % (len(covered), len(rows), failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
