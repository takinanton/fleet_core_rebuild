"""shadow_runner — 48h DRY parallel shadow harness (one process per venue).

P3 design authority: proofs/p3_design_shadow_runner.md (round-3 approved). This module
implements §1 topology, §2 ReadOnlyExchangeClient write fence, §3 mirrored inputs,
§4 tick + decision log, §8 cutover-adoption rehearsal, §9 operational notes.
Comparator/gate live in fleet_core/engine/comparator.py; the mirrored live suppression
state lives in fleet_core/engine/suppression_mirror.py.

HARD LAWS (design §2/§3):
  * READ-ONLY: every P2 write raises ShadowWriteBlocked BEFORE any I/O and the intent
    the engine WOULD have executed is recorded. Zero authenticated write endpoints are
    reachable — verified at startup by `write_fence_selftest` (attempts every write,
    must raise locally).
  * Rate-limit budget: shadow reads throttled to <= 50% of the venue budget; on
    RateLimited the shadow SKIPS the tick and logs `shadow_tick_skipped` (coverage gap,
    never a live-bot risk).
  * The shadow does NOT own positions: PM state is re-derived per tick from the
    mirrored live trades.db row (read-only, sqlite URI mode=ro) + closed bars. Where
    live lacks entry_bar_ts (pac/ext/nado legacy rows) the mirror backfills from
    opened_at floored to the tf boundary and flags `approx_entry_ts` on decisions.
  * Decisions are stamped with the mirror row-version (trade_id, sl_current,
    sl_order_id) so the comparator can detect mid-tick live writes (`stale_mirror`).
  * The entry pipeline consults the MIRRORED suppression state (suppression_mirror),
    never shadow-hypothetical history (F6).

CROSS-COMPONENT INTERFACE (built against the design docs, not sibling internals):
the decision cores are INJECTED callables:
    exit_decider(ctx: dict)  -> sequence of decision dicts
    entry_decider(ctx: dict) -> sequence of decision dicts
each decision dict: {"phase": <§4 phase>, "decision": <intent name>, "coin", "tf",
"bar_ts", "params": {...}, "flags": [...]}. When not injected, the runner lazily
imports fleet_core.engine.exit_engine / fleet_core.engine.entry_sm and uses their
documented `shadow_decide(ctx)` hook if present (interface note: the design docs
define the decision core as `tick(TickInput) -> [Decision]`; the dict adapter is the
shadow-side reading — recorded as an interface note for the entry/exit builders).
Everything selftests offline with fakes; no venue SDK import, stdlib only.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from fleet_core.exchange_api import (
    ExchangeClient,
    ExchangeError,
    RateLimited,
    ReadUnknown,
)
from fleet_core.engine.suppression_mirror import SuppressionMirror

log = logging.getLogger("fleet_core.engine.shadow_runner")

__all__ = [
    "ShadowWriteBlocked",
    "FenceSelfTestFailed",
    "RecordedIntent",
    "ReadBudget",
    "ReadOnlyExchangeClient",
    "write_fence_selftest",
    "DecisionLog",
    "LiveMirror",
    "MirrorRow",
    "ShadowRunner",
    "tf_to_ms",
]

TICK_SEC = 60.0          # live cadence (design §4: 60s boundary-aligned wake)
SHADOW_BUDGET_FRACTION = 0.5   # design §1: <= 50% of the venue budget — LAW, not knob


# ============================================================================
# Write fence
# ============================================================================

class ShadowWriteBlocked(ExchangeError):
    """A write was attempted inside the shadow process. ALWAYS raised locally,
    before any HTTP — the recorded intent is the shadow's 'would have executed'."""


class FenceSelfTestFailed(RuntimeError):
    """Startup self-test could not prove the write fence (design §2 hard invariant)."""


@dataclass(frozen=True)
class RecordedIntent:
    """The write the engine WOULD have executed (design §2)."""
    op: str
    coin: str
    ts: float
    kwargs: Mapping[str, Any] = field(default_factory=dict)


class ReadBudget:
    """Token bucket capped at SHADOW_BUDGET_FRACTION of the venue read budget.

    venue_budget_per_min: the venue's own request budget (per-venue constant, from
    the binding's rate discipline). Shadow capacity = 50% of it by LAW (design §1).
    Denial raises nothing here — callers get False and the wrapper raises a LOCAL
    RateLimited (never sent to the venue).
    """

    def __init__(self, venue_budget_per_min: float, now: Optional[float] = None) -> None:
        if venue_budget_per_min <= 0:
            raise ValueError("venue_budget_per_min must be > 0")
        self.capacity = venue_budget_per_min * SHADOW_BUDGET_FRACTION
        self.refill_per_sec = self.capacity / 60.0
        self._tokens = self.capacity
        self._last = time.time() if now is None else now
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        # Monotonic bucket: negative elapsed (mixed logical/wall clocks — the
        # runner passes boundary-aligned tick instants while per-read acquire
        # uses wall time) must never DRAIN tokens; _last only moves forward.
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity,
                               self._tokens + elapsed * self.refill_per_sec)
            self._last = now

    def try_acquire(self, n: float = 1.0, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            self._refill(now)
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    def available(self, now: Optional[float] = None) -> float:
        """Current token level WITHOUT consuming (additive helper for the
        shadow_main scan-budget preflight: a closed-bar scan burst must not be
        started when it would starve the same tick's exit reads)."""
        now = time.time() if now is None else now
        with self._lock:
            self._refill(now)
            return self._tokens


class ReadOnlyExchangeClient(ExchangeClient):
    """P2 wrapper: reads pass through (budget-throttled); ALL writes raise
    ShadowWriteBlocked and record the intent (design §2). The inner client is
    NEVER touched on the write path — structurally, not by discipline."""

    _WRITE_OPS = ("market_open", "ensure_flat", "trigger_sl", "cancel_sl_order",
                  "limit_reduce_only", "update_leverage")

    def __init__(self, inner: ExchangeClient, budget: ReadBudget,
                 venue: str = "") -> None:
        self._inner = inner
        self._budget = budget
        self.venue = venue
        self.recorded_intents: List[RecordedIntent] = []
        self.local_rate_denials = 0

    # ---- write fence (no inner call on ANY path) ----------------------------

    def _block(self, op: str, coin: str, **kwargs: Any) -> None:
        self.recorded_intents.append(
            RecordedIntent(op=op, coin=coin, ts=time.time(), kwargs=kwargs))
        raise ShadowWriteBlocked(
            "shadow write fence: %s(%s) blocked (intent recorded)" % (op, coin),
            venue=self.venue, op=op, coin=coin)

    def market_open(self, coin, is_buy, sz, intended_px=None, allow_marketable=True):
        self._block("market_open", coin, is_buy=is_buy, sz=sz,
                    intended_px=intended_px, allow_marketable=allow_marketable)

    def ensure_flat(self, coin):
        self._block("ensure_flat", coin)

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        self._block("trigger_sl", coin, is_buy=is_buy, sz=sz, trigger_px=trigger_px)

    def cancel_sl_order(self, coin, oid):
        self._block("cancel_sl_order", coin, oid=oid)

    def limit_reduce_only(self, coin, is_buy, sz, px):
        self._block("limit_reduce_only", coin, is_buy=is_buy, sz=sz, px=px)

    def update_leverage(self, coin, leverage, is_cross=True):
        self._block("update_leverage", coin, leverage=leverage, is_cross=is_cross)

    # ---- reads: budget-throttled passthrough --------------------------------

    def _acquire(self, op: str, coin: str = "") -> None:
        if not self._budget.try_acquire(1.0):
            self.local_rate_denials += 1
            raise RateLimited("shadow local read budget exhausted (<=50%% venue "
                              "budget law)", venue=self.venue, op=op, coin=coin)

    def open_positions(self):
        self._acquire("open_positions")
        return self._inner.open_positions()

    def open_orders(self):
        self._acquire("open_orders")
        return self._inner.open_orders()

    def list_open_sl_orders(self, coin):
        self._acquire("list_open_sl_orders", coin)
        return self._inner.list_open_sl_orders(coin)

    def list_reduce_only_triggers(self):
        self._acquire("list_reduce_only_triggers")
        return self._inner.list_reduce_only_triggers()

    def mark_price(self, coin, max_age_sec=5.0):
        self._acquire("mark_price", coin)
        return self._inner.mark_price(coin, max_age_sec)

    def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
        self._acquire("candles", coin)
        return self._inner.candles(coin, interval, limit, max_stale_bars)

    def equity_with_upnl(self):
        self._acquire("equity_with_upnl")
        return self._inner.equity_with_upnl()

    def account_value(self):
        self._acquire("account_value")
        return self._inner.account_value()

    def margin_used_usd(self):
        self._acquire("margin_used_usd")
        return self._inner.margin_used_usd()

    def position_liquidation(self, coin):
        self._acquire("position_liquidation", coin)
        return self._inner.position_liquidation(coin)

    def user_fills(self, max_age_sec=60.0):
        self._acquire("user_fills")
        return self._inner.user_fills(max_age_sec)

    def invalidate_positions_cache(self):
        return self._inner.invalidate_positions_cache()


def write_fence_selftest(client: ReadOnlyExchangeClient) -> None:
    """Startup hard invariant (design §2): attempt every write; each MUST raise
    ShadowWriteBlocked locally. Any other outcome (return, venue error, timeout)
    means the fence is not structural -> FenceSelfTestFailed, process must exit."""
    attempts = [
        ("market_open", lambda: client.market_open("__FENCE__", True, 1.0)),
        ("ensure_flat", lambda: client.ensure_flat("__FENCE__")),
        ("trigger_sl", lambda: client.trigger_sl("__FENCE__", False, 1.0, 1.0)),
        ("cancel_sl_order", lambda: client.cancel_sl_order("__FENCE__", "oid")),
        ("limit_reduce_only", lambda: client.limit_reduce_only("__FENCE__", False, 1.0, 1.0)),
        ("update_leverage", lambda: client.update_leverage("__FENCE__", 1)),
    ]
    for op, fn in attempts:
        try:
            fn()
        except ShadowWriteBlocked:
            continue
        except Exception as exc:
            raise FenceSelfTestFailed(
                "write fence self-test: %s raised %r instead of ShadowWriteBlocked "
                "(possible pre-raise I/O)" % (op, exc))
        raise FenceSelfTestFailed(
            "write fence self-test: %s RETURNED — writes are reachable" % op)
    # drop the probe intents — they are self-test artifacts, not decisions
    client.recorded_intents = [i for i in client.recorded_intents if i.coin != "__FENCE__"]
    log.info("write fence self-test OK (all %d write ops raise locally)", len(attempts))


# ============================================================================
# Decision log (design §4)
# ============================================================================

def _utc_iso(ts: Optional[float] = None) -> str:
    return _dt.datetime.fromtimestamp(
        time.time() if ts is None else ts, _dt.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


class DecisionLog:
    """Append-only JSONL, one object per decision (§4 schema). Tick heartbeat
    records (`phase='tick'`) make coverage-gap detection mechanical (§7.4)."""

    def __init__(self, path: str, venue: str, engine_sha: str) -> None:
        self.path = path
        self.venue = venue
        self.engine_sha = engine_sha
        d = os.path.dirname(os.path.abspath(path))
        if d:
            os.makedirs(d, exist_ok=True)

    def emit(self, tick: int, phase: str, decision: str, *,
             coin: str = "", tf: str = "", bar_ts: Optional[int] = None,
             params: Optional[Mapping[str, Any]] = None,
             inputs: Optional[Mapping[str, Any]] = None,
             flags: Optional[Sequence[str]] = None,
             ts: Optional[float] = None) -> Dict[str, Any]:
        rec = {
            "ts": _utc_iso(ts),
            "tick": tick,
            "venue": self.venue,
            "coin": coin,
            "tf": tf,
            "bar_ts": bar_ts,
            "phase": phase,
            "decision": decision,
            "params": dict(params or {}),
            "inputs": dict(inputs or {}),
            "engine_sha": self.engine_sha,
            "flags": list(flags or []),
        }
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, separators=(",", ":"), default=str) + "\n")
        return rec

    def read_all(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.path):
            return []
        out: List[Dict[str, Any]] = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out


# ============================================================================
# Live trades.db mirror (design §3)
# ============================================================================

_TF_UNITS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}


def tf_to_ms(tf: str) -> int:
    """'8h' -> 28800000. ValueError on garbage (caller bug, surface loud)."""
    tf = tf.strip().lower()
    if not tf or tf[-1] not in _TF_UNITS:
        raise ValueError("unparseable tf: %r" % tf)
    return int(tf[:-1]) * _TF_UNITS[tf[-1]]


@dataclass(frozen=True)
class MirrorRow:
    """One mirrored live position row (design §3 mirror source columns)."""
    trade_id: int
    coin: str
    tf: str
    direction: str
    entry: float
    size: float
    sl_initial: Optional[float]
    sl_current: Optional[float]
    sl_order_id: Optional[str]
    tp1_partial_done: bool
    entry_bar_ts: Optional[int]
    atr14: Optional[float]
    opened_at: Optional[str]
    status: str
    management_class: Optional[str]
    approx_entry_ts: bool          # entry_bar_ts backfilled from opened_at (flag §3)

    @property
    def rowver(self) -> List[Any]:
        """Mirror row-version stamp (trade_id, sl_current, sl_order_id) — design §3
        mirroring rule; lets the comparator detect mid-tick live writes."""
        return [self.trade_id,
                None if self.sl_current is None else repr(float(self.sl_current)),
                self.sl_order_id]


class LiveMirror:
    """Read-only per-tick mirror of the LIVE bot's trades.db (URI mode=ro)."""

    def __init__(self, live_db_path: str) -> None:
        self.live_db_path = live_db_path

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect("file:%s?mode=ro" % self.live_db_path, uri=True, timeout=5.0)
        con.row_factory = sqlite3.Row
        return con

    def open_rows(self) -> List[MirrorRow]:
        con = self._conn()
        try:
            cols = {r["name"] for r in con.execute("PRAGMA table_info(trades)")}
            rows = con.execute("SELECT * FROM trades WHERE status='open'").fetchall()
        finally:
            con.close()
        out: List[MirrorRow] = []
        for r in rows:
            entry_bar_ts = r["entry_bar_ts"] if "entry_bar_ts" in cols else None
            approx = False
            if entry_bar_ts is None and r["opened_at"]:
                # backfill: opened_at floored to tf boundary (design §3 / entry-SM §2.4)
                try:
                    opened = _dt.datetime.fromisoformat(
                        str(r["opened_at"]).replace("Z", "+00:00"))
                    if opened.tzinfo is None:
                        opened = opened.replace(tzinfo=_dt.timezone.utc)
                    ms = int(opened.timestamp() * 1000)
                    step = tf_to_ms(r["tf"])
                    entry_bar_ts = (ms // step) * step
                    approx = True
                except (ValueError, KeyError):
                    entry_bar_ts = None
            out.append(MirrorRow(
                trade_id=int(r["id"]),
                coin=r["coin"], tf=r["tf"],
                direction=r["direction"] if "direction" in cols else "long",
                entry=float(r["entry"]), size=float(r["size"]),
                sl_initial=r["sl_initial"],
                sl_current=r["sl_current"],
                sl_order_id=r["sl_order_id"] if "sl_order_id" in cols else None,
                tp1_partial_done=bool(r["tp1_partial_done"]) if "tp1_partial_done" in cols else False,
                entry_bar_ts=entry_bar_ts,
                atr14=r["atr14"] if "atr14" in cols else None,
                opened_at=r["opened_at"],
                status=r["status"],
                management_class=r["management_class"] if "management_class" in cols else None,
                approx_entry_ts=approx,
            ))
        return out


# ============================================================================
# Shadow state (own separate DB — phantom counters, tick counter)
# ============================================================================

class ShadowState:
    """shadow_state.db — the shadow's OWN persistence (design §1 topology).
    Kv only: phantom miss counters (K=3 debounce, §4) + tick counter."""

    def __init__(self, path: str) -> None:
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        if d:
            os.makedirs(d, exist_ok=True)
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
        con.commit(); con.close()

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        con = sqlite3.connect(self.path)
        try:
            row = con.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
            return row[0] if row else default
        finally:
            con.close()

    def put(self, key: str, value: str) -> None:
        con = sqlite3.connect(self.path)
        try:
            con.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?,?)", (key, value))
            con.commit()
        finally:
            con.close()

    def phantom_miss(self, coin: str) -> int:
        return int(self.get("phantom_miss:%s" % coin, "0") or 0)

    def set_phantom_miss(self, coin: str, n: int) -> None:
        self.put("phantom_miss:%s" % coin, str(int(n)))


# ============================================================================
# Runner
# ============================================================================

def _default_engine_sha() -> str:
    """md5 over this package's .py files — decision-log provenance stamp."""
    h = hashlib.md5()
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    for name in sorted(os.listdir(pkg_dir)):
        if name.endswith(".py"):
            with open(os.path.join(pkg_dir, name), "rb") as fh:
                h.update(name.encode()); h.update(fh.read())
    return h.hexdigest()


def _lazy_decider(module: str, attr: str = "shadow_decide"
                  ) -> Optional[Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]]:
    try:
        import importlib
        mod = importlib.import_module(module)
    except Exception:
        return None
    return getattr(mod, attr, None)


class ShadowRunner:
    """One shadow process per venue (design §1). DRY, read-only, 60s boundary-aligned.

    Constructor args:
      venue            : venue name
      client           : P2 ExchangeClient (raw binding) — wrapped in the fence here,
                         or an already-wrapped ReadOnlyExchangeClient
      live_db_path     : the LIVE bot's trades.db (read-only mirror source)
      suppression      : SuppressionMirror (F6 mirrored live suppression state)
      decisions_path   : data/shadow_decisions.jsonl
      state_path       : shadow_state.db (shadow's own SM/journal — separate file)
      venue_budget_per_min : venue read budget; shadow uses <= 50% (LAW)
      exit_decider / entry_decider : injected decision cores (see module docstring);
                         default = lazy import of the engine modules' shadow hooks
    """

    def __init__(
        self,
        venue: str,
        client: ExchangeClient,
        live_db_path: str,
        suppression: SuppressionMirror,
        decisions_path: str,
        state_path: str,
        venue_budget_per_min: float,
        exit_decider: Optional[Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]] = None,
        entry_decider: Optional[Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]] = None,
        engine_sha: Optional[str] = None,
        tick_sec: float = TICK_SEC,
        clock: Callable[[], float] = time.time,
    ) -> None:
        os.environ["DRY_RUN"] = "1"  # belt-and-braces (design §2)
        if isinstance(client, ReadOnlyExchangeClient):
            self.client = client
        else:
            self.client = ReadOnlyExchangeClient(
                client, ReadBudget(venue_budget_per_min), venue=venue)
        self.venue = venue
        self.mirror = LiveMirror(live_db_path)
        self.suppression = suppression
        self.state = ShadowState(state_path)
        self.log = DecisionLog(decisions_path, venue, engine_sha or _default_engine_sha())
        self.tick_sec = float(tick_sec)
        self.clock = clock
        self.exit_decider = exit_decider or _lazy_decider("fleet_core.engine.exit_engine")
        self.entry_decider = entry_decider or _lazy_decider("fleet_core.engine.entry_sm")
        self.tick_no = int(self.state.get("tick_no", "0") or 0)
        self._stop = threading.Event()

    # ------------------------------------------------------------------ startup

    def startup_selftest(self) -> None:
        """Hard invariant (design §2): fence proven before the first tick."""
        write_fence_selftest(self.client)

    # --------------------------------------------------------------------- tick

    def run_tick(self, now: Optional[float] = None) -> List[Dict[str, Any]]:
        """One full shadow tick. Returns the emitted decision records.

        RateLimited (local <=50% budget) or ReadUnknown on the account snapshot =>
        tick skipped and logged (`shadow_tick_skipped`) — a coverage gap, never a
        live-bot risk (design §1). Any unexpected exception is logged as a `bug`
        record (comparator class `bug`, gate-blocking) and re-raised."""
        now = self.clock() if now is None else now
        self.tick_no += 1
        self.state.put("tick_no", str(self.tick_no))
        emitted: List[Dict[str, Any]] = []
        try:
            # 1. mirrors first (design §3: mirror refreshed at tick start)
            self.suppression.refresh(now)
            rows = self.mirror.open_rows()

            # 2. ONE presence read up-front (exit-engine §2 canon)
            try:
                positions = self.client.open_positions()
                presence_unknown = False
            except ReadUnknown:
                positions, presence_unknown = {}, True

            # 3. exit pipeline per mirrored row (decision-only)
            for row in rows:
                emitted.extend(self._exit_phase(row, positions, presence_unknown, now))

            # 4. entry pipeline (decision-only, mirrored suppression state — F6)
            emitted.extend(self._entry_phase(now))

            emitted.append(self.log.emit(self.tick_no, "tick", "TickComplete", ts=now,
                                         inputs={"open_rows": len(rows),
                                                 "presence_unknown": presence_unknown}))
        except RateLimited as exc:
            emitted.append(self.log.emit(
                self.tick_no, "tick", "shadow_tick_skipped", ts=now,
                params={"cause": "rate_limited", "detail": str(exc)}))
        except ReadUnknown as exc:
            emitted.append(self.log.emit(
                self.tick_no, "tick", "shadow_tick_skipped", ts=now,
                params={"cause": "read_unknown", "detail": str(exc)}))
        except ShadowWriteBlocked:
            raise  # never swallowed: a write attempt reaching tick level is a bug
        except Exception as exc:
            self.log.emit(self.tick_no, "tick", "bug", ts=now,
                          params={"cause": "shadow_exception",
                                  "detail": "%s: %s" % (type(exc).__name__, exc)})
            raise
        return emitted

    def _exit_phase(self, row: MirrorRow, positions: Mapping[str, Any],
                    presence_unknown: bool, now: float) -> List[Dict[str, Any]]:
        flags = ["approx_entry_ts"] if row.approx_entry_ts else []
        base_inputs: Dict[str, Any] = {"mirror_rowver": row.rowver,
                                       "sl_current": row.sl_current,
                                       "size": row.size}
        if presence_unknown:
            presence = "UNKNOWN"
        elif row.coin in positions:
            presence = "PRESENT"
        else:
            presence = "ABSENT"
        base_inputs["presence"] = presence

        # gather per-coin reads defensively — a per-coin ReadUnknown defers that coin
        mark = sl_live = None
        try:
            mark = self.client.mark_price(row.coin, 5.0)
        except ReadUnknown:
            pass
        try:
            sl_live = list(self.client.list_open_sl_orders(row.coin))
        except ReadUnknown:
            sl_live = None  # UNKNOWN -> assume-live (anti heal-storm canon)
        base_inputs["mark"] = mark
        base_inputs["sl_live"] = (sl_live is None) or bool(
            row.sl_order_id and row.sl_order_id in sl_live) or bool(sl_live)

        if self.exit_decider is None:
            return [self.log.emit(self.tick_no, "pm", "Defer", coin=row.coin, tf=row.tf,
                                  ts=now, params={"reason": "exit_decider_missing"},
                                  inputs=base_inputs, flags=flags + ["decider_missing"])]
        ctx = {
            "venue": self.venue, "now": now, "row": row, "presence": presence,
            "mark": mark, "sl_live_oids": sl_live, "client": self.client,
            "phantom_miss": self.state.phantom_miss(row.coin),
            "set_phantom_miss": lambda n, c=row.coin: self.state.set_phantom_miss(c, n),
            "decision_only": True,
        }
        out: List[Dict[str, Any]] = []
        for dec in self.exit_decider(ctx):
            inputs = dict(base_inputs); inputs.update(dec.get("inputs", {}))
            out.append(self.log.emit(
                self.tick_no, dec.get("phase", "pm"), dec.get("decision", "NoOp"),
                coin=dec.get("coin", row.coin), tf=dec.get("tf", row.tf),
                bar_ts=dec.get("bar_ts"), ts=now, params=dec.get("params"),
                inputs=inputs, flags=flags + list(dec.get("flags", []))))
        return out

    def _entry_phase(self, now: float) -> List[Dict[str, Any]]:
        if self.entry_decider is None:
            return [self.log.emit(self.tick_no, "entry_gate", "Defer", ts=now,
                                  params={"reason": "entry_decider_missing"},
                                  flags=["decider_missing"])]
        sup = self.suppression
        ctx = {
            "venue": self.venue, "now": now, "client": self.client,
            "decision_only": True,
            # F6 LAW: mirrored live suppression state, not shadow history
            "cooldown_active": lambda coin: sup.cooldown_active(coin, now),
            "opens_today": lambda: sup.opens_today(now),
            "suppression_snapshot": sup.snapshot(now),
        }
        out: List[Dict[str, Any]] = []
        for dec in self.entry_decider(ctx):
            inputs = dict(dec.get("inputs", {}))
            inputs.setdefault("suppression", ctx["suppression_snapshot"])
            out.append(self.log.emit(
                self.tick_no, dec.get("phase", "entry_gate"),
                dec.get("decision", "Reject"),
                coin=dec.get("coin", ""), tf=dec.get("tf", ""),
                bar_ts=dec.get("bar_ts"), ts=now, params=dec.get("params"),
                inputs=inputs, flags=dec.get("flags", [])))
        return out

    # ------------------------------------------------------------------ cadence

    def run_forever(self) -> None:  # pragma: no cover — live loop, not selftested
        self.startup_selftest()
        while not self._stop.is_set():
            now = self.clock()
            next_boundary = (int(now // self.tick_sec) + 1) * self.tick_sec
            self._stop.wait(max(0.0, next_boundary - now))
            if self._stop.is_set():
                break
            try:
                self.run_tick(self.clock())
            except Exception:
                log.exception("shadow tick raised (logged as bug record)")

    def stop(self) -> None:
        self._stop.set()

    # ---------------------------------------------------- adoption rehearsal §8

    def adoption_rehearsal(self, now: Optional[float] = None,
                           trail_due: bool = False) -> Dict[str, Any]:
        """Cutover-adoption rehearsal (design §8), run once per shadow window.

        DRY adoption pass AS IF cutting over now: migrate-map the live trades.db rows
        (read-only, in-memory) per entry-SM §2.4 -> adopt every position -> assert
        per exit-engine §10:
          * every live OPEN row maps to OPEN with sl_order_id found in
            list_open_sl_orders readback,
          * sl_placed_px seeded from the VENUE's listed trigger px (via
            list_reduce_only_triggers), never DB sl_current,
          * the first simulated tick yields NoOp/Defer for every protected position
            (no ReplaceSL/Close) unless a genuine new-bar trail is due (`trail_due`
            passed by the caller who owns bar-boundary knowledge).
        Any other intent = `bug` (gate-blocking). Result logged phase='reconcile'.
        """
        now = self.clock() if now is None else now
        result: Dict[str, Any] = {"passed": True, "positions": [], "bugs": []}
        rows = self.mirror.open_rows()
        try:
            triggers = {t.oid: t for t in self.client.list_reduce_only_triggers()}
        except ReadUnknown:
            result["passed"] = False
            result["bugs"].append("list_reduce_only_triggers ReadUnknown — rehearsal "
                                  "cannot verify (unknown != ok)")
            self.log.emit(self.tick_no, "reconcile", "RehearsalFail", ts=now,
                          params=result)
            return result

        for row in rows:
            entry: Dict[str, Any] = {"trade_id": row.trade_id, "coin": row.coin}
            try:
                live_oids = list(self.client.list_open_sl_orders(row.coin))
            except ReadUnknown:
                entry["verdict"] = "unknown_read"
                result["passed"] = False
                result["bugs"].append("%s: SL list ReadUnknown" % row.coin)
                result["positions"].append(entry)
                continue
            sl_ok = bool(row.sl_order_id) and row.sl_order_id in live_oids
            entry["sl_confirmed_live"] = sl_ok
            if not sl_ok:
                result["passed"] = False
                result["bugs"].append(
                    "%s: sl_order_id %r not confirmed live" % (row.coin, row.sl_order_id))
            # seed sl_placed_px from the VENUE listed trigger px (exit-engine §10)
            t = triggers.get(row.sl_order_id or "")
            entry["sl_placed_px_seed"] = t.trigger_px if t is not None else None
            if t is None and sl_ok:
                result["passed"] = False
                result["bugs"].append(
                    "%s: live SL oid not in trigger listing — cannot seed sl_placed_px "
                    "from venue px" % row.coin)
            result["positions"].append(entry)

        # first simulated tick: NoOp/Defer only (unless a genuine trail is due)
        allowed = {"NoOp", "Defer"}
        for rec in self.run_tick(now):
            if rec["phase"] in ("tick", "entry_gate", "reject"):
                continue
            dec = rec["decision"]
            if dec in allowed:
                continue
            if trail_due and dec == "ReplaceSL" and \
                    rec.get("params", {}).get("cause") == "trail":
                continue
            result["passed"] = False
            result["bugs"].append("first-tick intent %s on %s violates adoption "
                                  "invariant (expected NoOp/Defer)" % (dec, rec["coin"]))
        self.log.emit(self.tick_no, "reconcile",
                      "RehearsalPass" if result["passed"] else "RehearsalFail",
                      ts=now, params={"bugs": result["bugs"],
                                      "positions_n": len(result["positions"])})
        return result


# ============================================================================ selftest

def _selftest() -> None:  # pragma: no cover — offline, fakes only, no venue SDK
    import tempfile
    from fleet_core.exchange_api import OpenOrderInfo, PositionInfo

    class FakeClient(ExchangeClient):
        """Offline P2 fake: verified reads, canned data."""

        def market_open(self, coin, is_buy, sz, intended_px=None, allow_marketable=True):
            raise AssertionError("inner write reached — fence broken")

        def ensure_flat(self, coin):
            raise AssertionError("inner write reached — fence broken")

        def trigger_sl(self, coin, is_buy, sz, trigger_px):
            raise AssertionError("inner write reached — fence broken")

        def cancel_sl_order(self, coin, oid):
            raise AssertionError("inner write reached — fence broken")

        def limit_reduce_only(self, coin, is_buy, sz, px):
            raise AssertionError("inner write reached — fence broken")

        def update_leverage(self, coin, leverage, is_cross=True):
            raise AssertionError("inner write reached — fence broken")

        def open_positions(self):
            return {"BTC": PositionInfo(coin="BTC", size_signed=0.1, entry_px=60000.0)}

        def open_orders(self):
            return []

        def list_open_sl_orders(self, coin):
            return ["oid1"] if coin == "BTC" else []

        def list_reduce_only_triggers(self):
            return [OpenOrderInfo(coin="BTC", oid="oid1", side="sell", size=0.1,
                                  trigger_px=58000.0, reduce_only=True, is_trigger=True)]

        def mark_price(self, coin, max_age_sec=5.0):
            return 65000.0

        def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
            raise ReadUnknown("no candles in offline fake")

        def equity_with_upnl(self):
            return 50000.0

        def account_value(self):
            return 50000.0

        def margin_used_usd(self):
            return 1000.0

        def position_liquidation(self, coin):
            return 30000.0

        def user_fills(self, max_age_sec=60.0):
            return []

    with tempfile.TemporaryDirectory() as td:
        live_db = os.path.join(td, "trades.db")
        con = sqlite3.connect(live_db)
        con.executescript(
            """
            CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, coin TEXT, tf TEXT, direction TEXT,
                entry REAL, size REAL, sl_initial REAL, sl_current REAL,
                sl_order_id TEXT, tp1_partial_done INTEGER DEFAULT 0,
                opened_at TEXT, closed_at TEXT, exit_price REAL, exit_reason TEXT,
                realized_r REAL, tp1_fill_price REAL, status TEXT);
            CREATE TABLE rejected_signals (id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, coin TEXT, tf TEXT, reason TEXT);
            """)
        con.execute(
            "INSERT INTO trades (created_at, coin, tf, direction, entry, size, "
            "sl_initial, sl_current, sl_order_id, opened_at, status) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?)",
            ("2026-07-02T08:00:00+00:00", "BTC", "8h", "long", 60000.0, 0.1,
             58000.0, 61000.0, "oid1", "2026-07-02T08:00:03+00:00", "open"))
        con.commit(); con.close()

        # 1. write fence: every write raises ShadowWriteBlocked BEFORE inner I/O
        ro = ReadOnlyExchangeClient(FakeClient(), ReadBudget(1000.0), venue="extended")
        write_fence_selftest(ro)
        assert ro.recorded_intents == []  # probes dropped

        # leaky wrapper must FAIL the self-test
        class Leaky(ReadOnlyExchangeClient):
            def ensure_flat(self, coin):
                return None
        try:
            write_fence_selftest(Leaky(FakeClient(), ReadBudget(1000.0)))
            raise AssertionError("leaky fence not detected")
        except FenceSelfTestFailed:
            pass

        # 2. mirror row: entry_bar_ts backfilled from opened_at (approx flag),
        #    rowver stamped
        mirror = LiveMirror(live_db)
        rows = mirror.open_rows()
        assert len(rows) == 1 and rows[0].approx_entry_ts
        assert rows[0].entry_bar_ts == (rows[0].entry_bar_ts // tf_to_ms("8h")) * tf_to_ms("8h")
        assert rows[0].rowver[0] == rows[0].trade_id

        sup = SuppressionMirror("extended", live_db, lambda since: [])

        def exit_decider(ctx):
            row = ctx["row"]
            return [{"phase": "trail", "decision": "ReplaceSL", "coin": row.coin,
                     "tf": row.tf, "bar_ts": row.entry_bar_ts,
                     "params": {"target_px": 64123.0, "cause": "trail"}}]

        def entry_decider(ctx):
            assert ctx["cooldown_active"]("BTC") is None  # F6: mirrored state consulted
            assert ctx["opens_today"]() == 1              # live actuals
            return [{"phase": "entry_gate", "decision": "Reject", "coin": "ETH",
                     "tf": "8h", "params": {"reason": "tier_gate"}}]

        runner = ShadowRunner(
            "extended", FakeClient(), live_db, sup,
            decisions_path=os.path.join(td, "shadow_decisions.jsonl"),
            state_path=os.path.join(td, "shadow_state.db"),
            venue_budget_per_min=1000.0,
            exit_decider=exit_decider, entry_decider=entry_decider)
        runner.startup_selftest()
        now0 = _dt.datetime(2026, 7, 2, 16, 0, tzinfo=_dt.timezone.utc).timestamp()
        recs = runner.run_tick(now=now0)
        by_phase = {}
        for r in recs:
            by_phase.setdefault(r["phase"], []).append(r)
        assert "trail" in by_phase and "entry_gate" in by_phase and "tick" in by_phase
        trail = by_phase["trail"][0]
        assert trail["inputs"]["mirror_rowver"][0] == rows[0].trade_id
        assert "approx_entry_ts" in trail["flags"]
        assert by_phase["entry_gate"][0]["inputs"]["suppression"]["opens_today"] == 1
        assert os.environ.get("DRY_RUN") == "1"

        # 3. budget exhaustion -> shadow_tick_skipped (coverage gap, not a crash)
        starved = ShadowRunner(
            "extended", FakeClient(), live_db, sup,
            decisions_path=os.path.join(td, "starved.jsonl"),
            state_path=os.path.join(td, "starved_state.db"),
            venue_budget_per_min=2.0,   # capacity 1 read — open_positions ok, mark denied
            exit_decider=exit_decider, entry_decider=entry_decider)
        skipped = [r for r in starved.run_tick(now=now0 + 60.0)
                   if r["decision"] == "shadow_tick_skipped"]
        assert skipped and skipped[0]["params"]["cause"] == "rate_limited"

        # 4. adoption rehearsal: NoOp decider passes; Close decider fails
        runner.exit_decider = lambda ctx: [{"phase": "pm", "decision": "NoOp",
                                            "coin": ctx["row"].coin,
                                            "tf": ctx["row"].tf, "params": {}}]
        res = runner.adoption_rehearsal(now=now0 + 120.0)
        assert res["passed"], res
        assert res["positions"][0]["sl_confirmed_live"]
        assert res["positions"][0]["sl_placed_px_seed"] == 58000.0  # VENUE px, not DB
        runner.exit_decider = lambda ctx: [{"phase": "pm", "decision": "Close",
                                            "coin": ctx["row"].coin,
                                            "tf": ctx["row"].tf,
                                            "params": {"reason": "tp"}}]
        res = runner.adoption_rehearsal(now=now0 + 180.0)
        assert not res["passed"] and any("Close" in b for b in res["bugs"])
    print("shadow_runner selftest OK")


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.WARNING)
    _selftest()
