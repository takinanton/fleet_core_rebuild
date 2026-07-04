"""fleet_core.engine.runner — engine process scaffold (P3 engine_main).

One process per venue, replacing ``<venue>-bot.service`` at cutover
(p3_rollout.md §3; units in deploy/engine_units/ — install at cutover only,
NOTHING deploys now).

Modes
-----
live
    Takes the exclusive ``data/trades.db.lock`` flock (locks.py — ZERO
    exemptions, rollout §4 F10), writes trades.db, executes venue writes
    through the P2 binding via the exit-engine IntentExecutor.
shadow
    ZERO WRITES BY CONSTRUCTION (my spec + p3_design_shadow_runner.md §2):
      * the write-executor is swapped for a :class:`RecorderExecutor`
        (append-only jsonl, holds NO client reference);
      * the venue client is wrapped in a read-only proxy that raises
        ``ShadowWriteBlocked`` on any of the six P2 write methods BEFORE any
        venue I/O (startup self-test asserts this, shadow doc §2);
      * the journal is a SEPARATE file (``shadow_state.db``); the live
        trades.db.lock is NEVER taken (shadow is not a trades.db writer —
        it locks its own ``shadow_state.db.lock`` against a twin shadow);
      * belt-and-braces ``DRY_RUN=1`` exported to this process env.

Startup order (rollout §3 step 5): flock -> journal init -> admin socket ->
reconciler FULL pass (both directions incl. adoption, entry-SM §4) -> tick
loop (60s boundary-aligned).

Admin socket (rollout §3b / §4, this module's Engine methods):
  * ``state`` queries; ``park``/``resume`` of the tick loop;
  * ``canary_hold``/``canary_release``/``canary_exec`` — CANARY_HELD unit
    state: entries frozen + tick loop parked, persisted, watchdog-bounded
    ``<= 15 min`` with auto-flatten-hook + unpark + RED on expiry;
  * ``drain`` — rollout §4 DRAIN routed through the socket (``--drain`` CLI
    below is a thin client of the RUNNING engine, never a second process);
  * ``claim`` — claim tool RPC (entry-SM §2.1 ``manual_claimed``).

manual_claimed spec (review fold-in #2, explicit)
-------------------------------------------------
A row with ``management_class='manual_claimed'`` is FULL hands-off for
venue I/O: the executor refuses every venue intent for it (placement, heal,
cancel, close — entry-SM §7.12) and the reconciler emits no re-alert.  The
ONE thing it keeps, exactly like ``protect_only`` (entry-SM §4.2.3
authority whitelist item ii), is the phantom-K3 DB-ONLY ROW-RESOLVE: when
the venue position is confirmed absent (K=3 debounce + final
cache-invalidated re-read), the engine closes the DB ROW with attribution —
a DB write, zero venue actions.  Without it a manually-closed claimed
position would leave a stale OPEN row forever.  ``Engine.claim`` stamps
this policy in its response and audit detail so the exit-engine/reconciler
builders code against the same reading.

Cross-component interfaces (EF4 — the REAL modules, no ghosts; resolved
lazily so this module imports with NO venue SDKs installed):
  * ``fleet_core.engine.entry_sm.Reconciler(db_path, client, cfg)`` with
    ``startup_pass()`` / ``tick_pass()`` — entry-SM §4 (mapped to the
    engine's full/incremental pass slots).
  * ``fleet_core.engine.exit_engine`` — ``IntentExecutor(client, journal,
    quirks)`` over ``exit_engine.SqliteJournal(db_path)`` + the per-tick
    driver ``exit_engine.run_tick(db_path, client, executor, venue=…,
    strategy_factory=…)``. The runner seeds the DEFAULT strategy factory
    (R2-1): ``make_strategy_adapter`` over the md5-pinned modules, per-row
    leg resolution via the live COMBO router, pm_kwargs from the live env
    per design §8 — without it steps 9-14 are inert and the exit-engine
    backstop raises ``EngineWiringError``.
  * ``fleet_core.venues.<venue>`` — ``build_client(env)`` (or ``Client``).

EF4 assembly law: an un-injected engine either builds the REAL components or
raises ``EngineWiringError`` LOUDLY at setup — the silent reconciler-only
fallback (one-time WARNING) is the silent-disable class and is DELETED. The
ONE sanctioned partial mode is ``ENGINE_ALLOW_PARTIAL=1`` (explicit env, for
staged bring-up) which logs CRITICAL every setup. The entry pass is venue
scanner glue (P4): its absence is legal ONLY while ``ENTRY_FREEZE=1`` (the
rollout §3 step-2..8 state) or under ``ENGINE_ALLOW_PARTIAL=1``; unfreezing
without a wired entry pass raises.
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from fleet_core.engine import admin as admin_mod
from fleet_core.engine import locks

log = logging.getLogger("fleet_core.engine.runner")

__all__ = [
    "TICK_SECONDS_DEFAULT", "CANARY_WINDOW_SEC", "DRAIN_MAX_PASSES",
    "NON_TERMINAL_PRE_OPEN", "WRITE_METHODS", "VENUES",
    "ShadowWriteBlocked", "EngineWiringError",
    "ReadOnlyClientProxy", "RecorderExecutor",
    "EngineConfig", "Engine", "main",
]

# --- labeled constants (derivations cited; never arbitrary) -----------------
TICK_SECONDS_DEFAULT = 60          # fleet tick period (rollout §1 F5 derivation base: N=120s = 2×60s)
CANARY_WINDOW_SEC = 15 * 60        # rollout §3b: CANARY_HELD watchdog bound <= 15 min
DRAIN_MAX_PASSES = 5               # rollout §4 DRAIN: "bounded 5 reconciler ticks (~5 min)"
NON_TERMINAL_PRE_OPEN = ("INTENT", "PENDING", "FILLED", "ABORTING")  # entry-SM §1 / rollout §4 gate set
WRITE_METHODS = (                  # P2 write surface, shadow doc §2
    "market_open", "ensure_flat", "trigger_sl",
    "cancel_sl_order", "limit_reduce_only", "update_leverage",
)
VENUES = ("hl", "pacifica", "extended", "nado")
# --drain client timeout: DRAIN_MAX_PASSES ticks + 2 ticks slack (derived, not arbitrary)
DRAIN_CLIENT_TIMEOUT_SEC = (DRAIN_MAX_PASSES + 2) * TICK_SECONDS_DEFAULT


class EngineWiringError(RuntimeError):
    """A documented cross-component interface could not be resolved."""


try:  # shadow component may define the canonical exception; ours is drop-in
    from fleet_core.shadow import ShadowWriteBlocked  # type: ignore
except Exception:  # noqa: BLE001 — module optional during parallel build
    class ShadowWriteBlocked(RuntimeError):  # type: ignore
        """A write method was invoked on a shadow (read-only) client."""


# --------------------------------------------------------------------- shadow fences


class ReadOnlyClientProxy:
    """Wraps a P2 ExchangeClient: reads pass through, the six write methods
    raise :class:`ShadowWriteBlocked` locally, BEFORE any venue I/O
    (p3_design_shadow_runner.md §2)."""

    def __init__(self, inner: Any):
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        if name in WRITE_METHODS:
            def _blocked(*_a: Any, **_k: Any) -> Any:
                raise ShadowWriteBlocked(
                    "shadow mode: write %r blocked by construction" % name)
            return _blocked
        return getattr(self._inner, name)


class RecorderExecutor:
    """Shadow-mode stand-in for the exit-engine IntentExecutor.

    Assumed executor interface (exit-engine §1, design-doc reading):
    ``execute(intents, context=None)``.  This recorder appends every call to
    ``shadow_decisions.jsonl`` and holds NO exchange client — a venue write
    from this object is impossible by construction, not by discipline.
    """

    def __init__(self, jsonl_path: Union[str, Path], venue: str = ""):
        self.path = Path(jsonl_path)
        self.venue = venue
        self._lock = threading.Lock()
        self.records = 0

    @staticmethod
    def _ser(obj: Any) -> Any:
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            pass
        if hasattr(obj, "__dataclass_fields__"):
            try:
                import dataclasses
                out = {k: RecorderExecutor._ser(v)
                       for k, v in dataclasses.asdict(obj).items()}
                # Integration fix (P3 selftest): keep the intent CLASS NAME.
                # Typed exit-engine intents (ReplaceSL/HealSL/...) carry their
                # kind as the dataclass TYPE, not a field — asdict() alone made
                # shadow_decisions.jsonl records kind-less (ReplaceSL and
                # HealSL both serialize to {reason, target_px, ...}), which the
                # shadow comparator cannot match. 'kind' mirrors Intent.kind.
                out.setdefault("kind", type(obj).__name__)
                return out
            except (TypeError, ValueError):
                pass
        if isinstance(obj, Mapping):
            return {str(k): RecorderExecutor._ser(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [RecorderExecutor._ser(v) for v in obj]
        return repr(obj)

    def record(self, kind: str, payload: Any) -> None:
        row = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "venue": self.venue,
            "mode": "shadow",
            "kind": kind,
            "payload": self._ser(payload),
        }
        line = json.dumps(row)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            self.records += 1

    def execute(self, intents: Any, context: Any = None) -> list:
        # Accept BOTH call shapes: the real IntentExecutor's execute(t, intents)
        # (exit-engine §1 — run_tick calls this) and the recorder-native
        # execute(intents, context). A TickInput first arg is detected by its
        # 'pos' attribute and swapped into the context slot.
        if hasattr(intents, "pos") and context is not None:
            intents, context = context, intents
        self.record("intents", {"intents": intents, "context": context})
        return []

    __call__ = execute


# --------------------------------------------------------------------------- config


def _env_flag(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


# HL COMBO dual-router prefix — hl/bot/scanner.py `_is_xyz` (:39-52): xyz_*
# HIP-3 tokenized-stock coins route to the us29 leg, everything else to the
# donchian crypto leg. A single literal prefix test, VERBATIM from live.
HL_US29_COIN_PREFIX = "xyz_"


def _leg_for_coin(venue: str, coin: str) -> str:
    """Per-row strategy-leg resolution (R2-1): mirrors the live COMBO router
    exactly. Non-HL venues have ONE leg (canonical strategy_xnn)."""
    if venue in ("hl", "hyperliquid") and \
            str(coin or "").startswith(HL_US29_COIN_PREFIX):
        return "us29"
    return "crypto"


@dataclass
class EngineConfig:
    venue: str
    mode: str = "live"                      # 'live' | 'shadow'
    data_dir: Path = Path("data")
    db_path: Optional[Path] = None          # default: <data_dir>/trades.db (live) / shadow_state.db (shadow)
    tick_seconds: float = TICK_SECONDS_DEFAULT
    admin_sock: Optional[Path] = None       # default: <data_dir>/engine_admin.sock
    entry_freeze: bool = field(default_factory=lambda: _env_flag("ENTRY_FREEZE"))
    drain_pass_interval: Optional[float] = None  # default: tick_seconds (test hook)
    canary_window_sec: float = CANARY_WINDOW_SEC

    def __post_init__(self) -> None:
        if self.venue not in VENUES:
            raise ValueError("unknown venue %r (expected one of %s)" % (self.venue, ", ".join(VENUES)))
        if self.mode not in ("live", "shadow"):
            raise ValueError("mode must be 'live' or 'shadow'")
        self.data_dir = Path(self.data_dir)
        if self.db_path is None:
            self.db_path = self.data_dir / ("trades.db" if self.mode == "live" else "shadow_state.db")
        self.db_path = Path(self.db_path)
        if self.admin_sock is None:
            self.admin_sock = self.data_dir / "engine_admin.sock"
        self.admin_sock = Path(self.admin_sock)
        if self.drain_pass_interval is None:
            self.drain_pass_interval = float(self.tick_seconds)

    @property
    def state_path(self) -> Path:
        return self.data_dir / "engine_state.json"

    @property
    def shadow_decisions_path(self) -> Path:
        return self.data_dir / "shadow_decisions.jsonl"


# --------------------------------------------------------------------- db plumbing


def _db_connect(path: Union[str, Path]) -> sqlite3.Connection:
    """Engine journal db discipline: WAL + busy_timeout=5000 fleet-wide
    (p3_design_entry_sm.md §6 Nado row)."""
    conn = sqlite3.connect(str(path), timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.DatabaseError:
        pass
    return conn


# --------------------------------------------------------------------------- engine


class _ReconcilerHandle:
    def __init__(self, full: Callable[[], Any], incremental: Callable[[], Any]):
        self.full_pass = full
        self.incremental_pass = incremental


class Engine:
    """Per-venue engine process: lock -> wire -> reconcile -> tick loop.

    Cross-component pieces (client / executor / reconciler / exit tick /
    entry pass) are injectable for offline selftests and resolved lazily
    from their documented module paths in production.
    """

    def __init__(self, cfg: EngineConfig, *,
                 client: Any = None,
                 executor: Any = None,
                 reconciler: Any = None,
                 exit_tick: Optional[Callable[["Engine"], Any]] = None,
                 entry_pass: Optional[Callable[["Engine"], Any]] = None):
        self.cfg = cfg
        self.client = client
        self.executor = executor
        self.reconciler = reconciler
        self._exit_tick = exit_tick
        self._entry_pass = entry_pass

        self.db_lock = None  # type: Optional[locks.DBLock]
        self.admin_server = None  # type: Optional[admin_mod.AdminServer]

        self._state_mtx = threading.RLock()
        self._pass_lock = threading.Lock()   # serializes reconciler/exit passes vs drain
        self._stop = threading.Event()
        self._parked = threading.Event()     # set => tick loop parked
        self._park_reason = ""
        self._unit_state = "RUNNING"         # RUNNING | CANARY_HELD
        self._entry_freeze = bool(cfg.entry_freeze)
        self._draining = False
        self._canary_deadline = None  # type: Optional[float]
        self._canary_handler = None   # type: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]
        self._canary_expire_handler = None  # type: Optional[Callable[["Engine"], Any]]
        self._last_canary_result = None  # type: Optional[str]
        self._started_at = None  # type: Optional[float]
        self._last_tick_at = None  # type: Optional[float]
        self._tick_count = 0
        # R2-1: the resolved default strategy factory (None until
        # _resolve_exit_tick runs, or under ENGINE_ALLOW_PARTIAL fallback).
        self._strategy_factory = None  # type: Optional[Callable[[Mapping[str, Any]], Any]]

    # ---------------------------------------------------------------- wiring
    def _resolve_client(self) -> Any:
        if self.client is not None:
            return self.client
        modname = "fleet_core.venues.%s" % self.cfg.venue
        try:
            mod = importlib.import_module(modname)
        except Exception as e:  # noqa: BLE001 — venue SDK may be absent locally
            raise EngineWiringError("cannot import venue binding %s: %s" % (modname, e))
        for name in ("build_client", "make_client", "create_client"):
            fn = getattr(mod, name, None)
            if callable(fn):
                return fn(os.environ)
        for name in ("Client", "ExchangeClientImpl"):
            cls = getattr(mod, name, None)
            if cls is not None:
                return cls()
        raise EngineWiringError(
            "%s exposes no client factory (tried build_client/make_client/"
            "create_client/Client) — code against the P2 contract surface" % modname)

    @staticmethod
    def _allow_partial() -> bool:
        """EF4: the ONE sanctioned partial-assembly mode — explicit env for
        staged bring-up. Anything else that cannot build the REAL engine is a
        loud EngineWiringError, never a warning."""
        return _env_flag("ENGINE_ALLOW_PARTIAL")

    def _build_quirks(self) -> Any:
        """VenueQuirks seeded from the process env (design §8 'SEED from live
        .env, never memory'; code defaults mirror the live bot/config.py
        defaults so a missed seed cannot silently flip money math)."""
        xng = importlib.import_module("fleet_core.engine.exit_engine")
        env = os.environ

        def _f(name: str, default: float) -> float:
            try:
                return float(env.get(name, default))
            except (TypeError, ValueError):
                return default

        prefixes = tuple(p.strip() for p in
                         env.get("MANUAL_POSITION_PREFIXES", "").split(",")
                         if p.strip())
        return xng.VenueQuirks(
            venue=self.cfg.venue,
            liq_sl_buffer_pct=_f("LIQ_SL_BUFFER_PCT", xng.LIQ_SL_BUFFER_PCT_DEFAULT),
            trail_after_tp_buffer_pct=_f("TRAIL_AFTER_TP_BUFFER_PCT", 0.003),
            manual_position_prefixes=prefixes,
            regime_leg_prefix=env.get("REGIME_LEG_PREFIX", ""),
            dry_run=_env_flag("DRY_RUN"),
        )

    def _pm_kwargs_from_env(self) -> Dict[str, Any]:
        """Design §8: the trader-level PM ctor values SEEDED FROM THE LIVE
        process env — the exact live `_pm_kwargs` mapping (hl bot/main.py:
        971-977 reads settings.* which config.py seeds from these keys).
        Code defaults mirror the live bot/config.py defaults PER VENUE
        (hl config.py:445-451: MAX_RUN_R=1000.0 / TP1_PARTIAL_FRAC=0.0 —
        S4-1 + donch-6 fail-safe defaults; ext/pac/nado config.py:
        MAX_RUN_R=5.0 / TP1_PARTIAL_FRAC=0.5) so a missed seed cannot
        silently flip money math — same law as _build_quirks."""
        env = os.environ

        def _f(name: str, default: float) -> float:
            try:
                return float(env.get(name, default))
            except (TypeError, ValueError):
                return default

        def _i(name: str, default: int) -> int:
            try:
                return int(env.get(name, default))
            except (TypeError, ValueError):
                return default

        hl = self.cfg.venue in ("hl", "hyperliquid")
        return dict(
            be_buffer_pct=_f("TRAIL_AFTER_TP_BUFFER_PCT", 0.003),
            vstop_pivot_window=_i("VSTOP_PIVOT_WINDOW", 8),
            max_run_r=_f("MAX_RUN_R", 1000.0 if hl else 5.0),
            vstop_buffer_pct=_f("VSTOP_BUFFER_PCT", 0.008),
            tp1_partial_frac=_f("TP1_PARTIAL_FRAC", 0.0 if hl else 0.5),
        )

    def _resolve_strategy_factory(self) -> Callable[[Mapping[str, Any]], Any]:
        """R2-1 ROOT FIX: the production exit tick MUST drive the REAL pinned
        strategy adapter — run_tick with strategy_factory=None sets
        strategy=None on every row, turning steps 9-14 (PM trail, time_stop/
        tp, check_sl_hit, entry-bar suppression) INERT with zero CRITICAL
        (the silent-disable class). Default factory:
          * exit_engine.make_strategy_adapter(venue, leg, pm_kwargs) over the
            md5-pinned modules (strategy_math.get_module);
          * per-row leg resolution via the live COMBO router (_leg_for_coin);
          * pm_kwargs seeded from the live env per design §8
            (_pm_kwargs_from_env); DONCHIAN_K / TF_<TF>_K / DONCHIAN_TFS ride
            the process env directly (the pinned HL module reads them at
            exec, same as live) — guarded by the startup CONFIG-ASSERT;
          * ONE adapter (= ONE PositionManager) per leg for the process
            lifetime — mirrors live main.py:978-979 and keeps the PM's
            restart-persistent per-trade stashes across ticks."""
        xng = importlib.import_module("fleet_core.engine.exit_engine")
        venue = self.cfg.venue
        if venue in ("hl", "hyperliquid"):
            # live main.py:660 startup CONFIG-ASSERT port (strategy_math):
            # refuse to run the crypto leg on an unvalidated DONCHIAN_K/TFS
            # env — ConfigAssertError propagates (live equivalent: exit(1)).
            sm = importlib.import_module("fleet_core.engine.strategy_math")
            sm.assert_hl_effective_config()
        pm_kwargs = self._pm_kwargs_from_env()
        adapters: Dict[str, Any] = {}

        def factory(row: Mapping[str, Any]) -> Any:
            leg = _leg_for_coin(venue, str(row.get("coin") or ""))
            ad = adapters.get(leg)
            if ad is None:
                ad = xng.make_strategy_adapter(venue, leg, pm_kwargs=pm_kwargs)
                adapters[leg] = ad
            return ad

        return factory

    def _resolve_reconciler(self) -> Any:
        if self.reconciler is not None:
            rec = self.reconciler
        else:
            # EF4: the REAL module — fleet_core.engine.entry_sm.Reconciler
            # (startup_pass/tick_pass). No fleet_core.reconciler ghost.
            try:
                esm = importlib.import_module("fleet_core.engine.entry_sm")
            except Exception as e:  # noqa: BLE001
                raise EngineWiringError(
                    "fleet_core.engine.entry_sm unavailable (%s) — the startup "
                    "reconciler full pass is mandatory (entry-SM §4)" % e)
            rec = esm.Reconciler(self.cfg.db_path, self.client,
                                 cfg=esm.ReconcilerConfig(
                                     venue=self.cfg.venue,
                                     tick_sec=float(self.cfg.tick_seconds)))
        # map to the engine's pass slots: full=startup_pass, incremental=tick_pass
        full = getattr(rec, "startup_pass", None) or getattr(rec, "full_pass", None)
        inc = getattr(rec, "tick_pass", None) or getattr(rec, "incremental_pass", None)
        if not (callable(full) and callable(inc)):
            raise EngineWiringError(
                "reconciler exposes neither startup_pass/tick_pass (real "
                "entry_sm.Reconciler) nor full_pass/incremental_pass (injected)")
        return _ReconcilerHandle(full, inc)

    def _resolve_executor(self) -> Any:
        if self.executor is not None:
            return self.executor
        if self.cfg.mode == "shadow":
            # zero writes by construction — recorder, never the write executor
            return RecorderExecutor(self.cfg.shadow_decisions_path, venue=self.cfg.venue)
        # EF4: the REAL module — fleet_core.engine.exit_engine.IntentExecutor
        # over exit_engine.SqliteJournal (documented JournalPort surface).
        try:
            xng = importlib.import_module("fleet_core.engine.exit_engine")
        except Exception as e:  # noqa: BLE001
            raise EngineWiringError(
                "fleet_core.engine.exit_engine unavailable (%s) — live mode "
                "needs the IntentExecutor (exit-engine §1)" % e)
        return xng.IntentExecutor(self.client,
                                  xng.SqliteJournal(self.cfg.db_path),
                                  self._build_quirks())

    def _resolve_exit_tick(self) -> Callable[["Engine"], Any]:
        if self._exit_tick is not None:
            return self._exit_tick
        # EF4: the REAL per-tick entry — exit_engine.run_tick. Absence is a
        # HARD EngineWiringError unless ENGINE_ALLOW_PARTIAL=1 (explicit).
        try:
            xng = importlib.import_module("fleet_core.engine.exit_engine")
            fn = xng.run_tick
        except Exception as e:  # noqa: BLE001
            if self._allow_partial():
                log.critical(
                    "CRITICAL: exit-engine tick NOT wired (%s) — running "
                    "reconciler-only ticks under EXPLICIT ENGINE_ALLOW_PARTIAL=1 "
                    "(staged bring-up). Strategy exit management is OFF.", e)
                return None  # type: ignore[return-value]
            raise EngineWiringError(
                "fleet_core.engine.exit_engine.run_tick unavailable (%s) — "
                "strategy exit management cannot be silently disabled (EF4); "
                "set ENGINE_ALLOW_PARTIAL=1 explicitly for staged bring-up" % e)
        quirks = getattr(self.executor, "quirks", None)
        # R2-1 ROOT FIX: seed the DEFAULT strategy factory — run_tick without
        # one silently disarms steps 9-14 on every strategy row (Defer
        # executed as ok). Factory resolution failure is LOUD: EngineWiringError
        # unless ENGINE_ALLOW_PARTIAL=1 (CRITICAL, factory=None — the exit-
        # engine backstop then labels every such tick, never a silent ok).
        try:
            factory = self._resolve_strategy_factory()  # type: Optional[Callable]
        except Exception as e:  # noqa: BLE001 — pin/config/import failures
            if self._allow_partial():
                log.critical(
                    "CRITICAL: strategy adapter factory NOT wired (%s) — exit "
                    "ticks run WITHOUT strategy exit management (steps 9-14) "
                    "under EXPLICIT ENGINE_ALLOW_PARTIAL=1 (staged bring-up).",
                    e)
                factory = None
            else:
                raise EngineWiringError(
                    "strategy adapter factory unavailable (%s) — strategy "
                    "exit management cannot be silently disabled (R2-1/EF4); "
                    "set ENGINE_ALLOW_PARTIAL=1 explicitly for staged "
                    "bring-up" % e)
        self._strategy_factory = factory
        return lambda eng: fn(db_path=eng.cfg.db_path, client=eng.client,
                              executor=eng.executor, venue=eng.cfg.venue,
                              quirks=quirks, strategy_factory=factory)

    def _resolve_entry_pass(self) -> Optional[Callable[["Engine"], Any]]:
        if self._entry_pass is not None:
            return self._entry_pass
        # The entry pass is the venue scanner glue (P4) — there is no
        # fleet_core.entry_sm.run_entry_pass ghost to import (EF4). Its
        # absence is legal ONLY while entries are frozen (declared state,
        # rollout §3) or under explicit ENGINE_ALLOW_PARTIAL=1 — enforced in
        # setup() and again at unfreeze/set_entry_freeze + _tick.
        return None

    # ---------------------------------------------------------------- setup
    def setup(self) -> None:
        cfg = self.cfg
        cfg.data_dir.mkdir(parents=True, exist_ok=True)

        if cfg.mode == "shadow":
            # Belt-and-braces per shadow doc §2 — set BEFORE any wiring.
            os.environ["DRY_RUN"] = "1"

        # 1. single-writer flock. live -> data/trades.db.lock (rollout §4,
        #    zero exemptions). shadow -> its OWN shadow_state.db.lock (it is
        #    never a trades.db writer; the lock guards against a twin shadow).
        self.db_lock = locks.acquire_or_die(cfg.db_path, role="fleet-engine-%s-%s" % (cfg.venue, cfg.mode))

        # 2. journal init (P1 canonical; sm_* columns arrive via migrate_v3
        #    at cutover — engine queries are guarded for pre-migration DBs).
        try:
            journal = importlib.import_module("fleet_core.journal")
            journal.init_db(cfg.db_path)
        except Exception as e:  # noqa: BLE001
            log.warning("journal init skipped/failed on %s: %s", cfg.db_path, e)

        # 3. client (+ shadow read-only wrap and write-fence self-test)
        self.client = self._resolve_client()
        if cfg.mode == "shadow":
            self.client = ReadOnlyClientProxy(self.client)
            self._shadow_write_fence_selftest()

        # 4. executor (live: IntentExecutor; shadow: recorder)
        self.executor = self._resolve_executor()

        # 5. reconciler
        self.reconciler = self._resolve_reconciler()

        # 6. per-tick pipelines — REAL or loud (EF4; ENGINE_ALLOW_PARTIAL=1 is
        #    the only sanctioned partial mode, logged CRITICAL at resolve time)
        self._exit_tick = self._resolve_exit_tick()
        self._entry_pass = self._resolve_entry_pass()
        if self._entry_pass is None and not self._entry_freeze \
                and not self._allow_partial():
            raise EngineWiringError(
                "entry pass not wired (venue scanner glue, P4) and entries "
                "are NOT frozen — refusing to run with entries silently "
                "disabled (EF4). Run with ENTRY_FREEZE=1 (rollout §3) or set "
                "ENGINE_ALLOW_PARTIAL=1 explicitly for staged bring-up.")

        # 7. persisted unit state (crash during CANARY_HELD -> CRITICAL + parked;
        #    the expired watchdog then auto-releases per rollout §3b)
        self._load_persisted_state()

        # 8. admin socket
        self.admin_server = admin_mod.AdminServer(cfg.admin_sock, self)
        self.admin_server.start()

        # 9. startup reconciler FULL pass (before any tick; rollout §3 step 5
        #    "startup order: reconciler full pass -> adopt" — adoption is the
        #    full pass's direction-2 per entry-SM §4.2)
        with self._pass_lock:
            self.reconciler.full_pass()

        self._started_at = time.time()
        log.info("engine up: venue=%s mode=%s db=%s tick=%.0fs entry_freeze=%s unit_state=%s",
                 cfg.venue, cfg.mode, cfg.db_path, cfg.tick_seconds,
                 self._entry_freeze, self._unit_state)

    def _shadow_write_fence_selftest(self) -> None:
        """Shadow doc §2: attempt a no-op write at startup; it must raise
        locally before any HTTP.  Failure -> CRITICAL + exit(1)."""
        try:
            self.client.update_leverage("__shadow_selftest__", 1)
        except ShadowWriteBlocked:
            log.info("shadow write-fence selftest OK (ShadowWriteBlocked raised locally)")
            return
        except Exception as e:  # noqa: BLE001 — anything else means the fence is NOT ours
            log.critical("CRITICAL: shadow write-fence selftest raised %s instead of "
                         "ShadowWriteBlocked — fence not proven, refusing to run", type(e).__name__)
            sys.exit(1)
        log.critical("CRITICAL: shadow write-fence selftest DID NOT raise — "
                     "a write path is reachable from shadow. Refusing to run.")
        sys.exit(1)

    # ------------------------------------------------------------- state io
    def _load_persisted_state(self) -> None:
        p = self.cfg.state_path
        try:
            st = json.loads(p.read_text())
        except (OSError, ValueError):
            return
        if st.get("unit_state") == "CANARY_HELD":
            log.critical(
                "CRITICAL: persisted unit state is CANARY_HELD — process died "
                "inside a canary window. Starting PARKED; a non-terminal "
                "write_canary row is operator territory (rollout §3b: never "
                "silent adoption; reconciler skips notes='write_canary').")
            with self._state_mtx:
                self._unit_state = "CANARY_HELD"
                self._parked.set()
                self._park_reason = "canary_held_recovered"
                # keep the ORIGINAL deadline: an expired one fires the
                # watchdog (auto-release + RED) on the first loop iteration
                self._canary_deadline = st.get("canary_deadline") or 0.0
        if "entry_freeze" in st:
            self._entry_freeze = bool(st["entry_freeze"])

    def _persist_state(self) -> None:
        st = {
            "venue": self.cfg.venue,
            "mode": self.cfg.mode,
            "unit_state": self._unit_state,
            "entry_freeze": self._entry_freeze,
            "canary_deadline": self._canary_deadline,
            "last_canary_result": self._last_canary_result,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = self.cfg.state_path.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(st, indent=1))
            tmp.replace(self.cfg.state_path)
        except OSError as e:
            log.error("could not persist engine state: %s", e)

    def _audit(self, from_state: str, to_state: str, detail: Dict[str, Any]) -> None:
        """rollout §3b: canary/unit transitions logged sm_transitions-style
        (trade_id=0 rows).  Best-effort: table exists post-migration only."""
        try:
            conn = _db_connect(self.cfg.db_path)
            try:
                conn.execute(
                    "INSERT INTO sm_transitions (trade_id, at, from_state, to_state, detail) "
                    "VALUES (0, ?, ?, ?, ?)",
                    (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     from_state, to_state, json.dumps(detail)),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as e:
            log.info("unit-state audit not journaled (%s): %s -> %s %s",
                     e, from_state, to_state, detail)

    # ----------------------------------------------------------- admin api
    def admin_state(self) -> Dict[str, Any]:
        with self._state_mtx:
            st = {
                "ok": True,
                "venue": self.cfg.venue,
                "mode": self.cfg.mode,
                "pid": os.getpid(),
                "state": self._unit_state,
                "tick_loop": "parked" if self._parked.is_set() else "running",
                "park_reason": self._park_reason if self._parked.is_set() else None,
                "entry_freeze": self._entry_freeze,
                "draining": self._draining,
                "canary_deadline": self._canary_deadline,
                "last_canary_result": self._last_canary_result,
                "started_at": self._started_at,
                "last_tick_at": self._last_tick_at,
                "tick_count": self._tick_count,
                "db_path": str(self.cfg.db_path),
                "engine_sha": os.environ.get("ENGINE_SHA", "unknown"),
            }
        st["open_rows_by_sm_state"] = self._rows_by_sm_state()
        return st

    def _rows_by_sm_state(self) -> Any:
        try:
            conn = _db_connect(self.cfg.db_path)
            try:
                cur = conn.execute("SELECT sm_state, COUNT(*) FROM trades GROUP BY sm_state")
                return {str(r[0]): r[1] for r in cur.fetchall()}
            finally:
                conn.close()
        except sqlite3.Error as e:
            return "unavailable: %s" % e

    def park_ticks(self, reason: str = "admin") -> Dict[str, Any]:
        with self._state_mtx:
            self._parked.set()
            self._park_reason = reason
        log.warning("tick loop PARKED (reason=%s)", reason)
        return {"ok": True, "tick_loop": "parked", "reason": reason}

    def resume_ticks(self) -> Dict[str, Any]:
        with self._state_mtx:
            if self._unit_state == "CANARY_HELD":
                return {"ok": False,
                        "error": "cannot resume ticks while CANARY_HELD — "
                                 "canary_release first (rollout §3b)"}
            self._parked.clear()
            self._park_reason = ""
        log.warning("tick loop RESUMED")
        return {"ok": True, "tick_loop": "running"}

    def set_entry_freeze(self, on: bool) -> Dict[str, Any]:
        if not on and self._entry_pass is None and not self._allow_partial():
            # EF4: unfreezing without a wired entry pass would be the silent-
            # disable class — refuse loudly instead.
            return {"ok": False,
                    "error": "cannot unfreeze: entry pass not wired (venue "
                             "scanner glue, P4) — EF4 refuses silent entry "
                             "disable; set ENGINE_ALLOW_PARTIAL=1 explicitly "
                             "for staged bring-up"}
        with self._state_mtx:
            self._entry_freeze = bool(on)
        self._persist_state()
        log.warning("entry_freeze set to %s", on)
        return {"ok": True, "entry_freeze": bool(on)}

    # -- canary ------------------------------------------------------------
    def register_canary_handler(self,
                                run: Callable[[Dict[str, Any]], Dict[str, Any]],
                                on_expire: Optional[Callable[["Engine"], Any]] = None) -> None:
        """Canary glue (write_canary in-process ladder) registers here."""
        self._canary_handler = run
        self._canary_expire_handler = on_expire

    def canary_hold(self) -> Dict[str, Any]:
        with self._state_mtx:
            if self._draining:
                return {"ok": False, "error": "drain in progress — canary refused"}
            if self._unit_state == "CANARY_HELD":
                # idempotent re-ack with the SAME window (no deadline extension)
                return {"ok": True, "state": "CANARY_HELD", "tick_loop": "parked",
                        "deadline_ts": self._canary_deadline,
                        "window_sec": self.cfg.canary_window_sec,
                        "entry_freeze": self._entry_freeze}
            prev = self._unit_state
            self._unit_state = "CANARY_HELD"
            self._parked.set()
            self._park_reason = "canary_held"
            self._entry_freeze = True   # step-2 freeze stays ON throughout (§3b)
            self._canary_deadline = time.time() + float(self.cfg.canary_window_sec)
            deadline = self._canary_deadline
        self._persist_state()
        self._audit(prev, "CANARY_HELD", {"canary_held_at": time.time(),
                                          "window_sec": self.cfg.canary_window_sec})
        log.warning("unit state -> CANARY_HELD (tick loop parked, entries frozen, "
                    "watchdog %ds)", int(self.cfg.canary_window_sec))
        # exact ack fields the write_canary tool refuses to start without (§3b)
        return {"ok": True, "state": "CANARY_HELD", "tick_loop": "parked",
                "deadline_ts": deadline, "window_sec": self.cfg.canary_window_sec,
                "entry_freeze": True}

    def canary_release(self, reason: str = "operator") -> Dict[str, Any]:
        with self._state_mtx:
            if self._unit_state != "CANARY_HELD":
                return {"ok": False, "error": "not in CANARY_HELD"}
            self._unit_state = "RUNNING"
            self._canary_deadline = None
            self._parked.clear()
            self._park_reason = ""
            if reason != "operator":
                self._last_canary_result = reason
        self._persist_state()
        self._audit("CANARY_HELD", "RUNNING", {"canary_released_at": time.time(),
                                               "reason": reason})
        log.warning("unit state -> RUNNING (canary released, reason=%s); "
                    "tick loop resumed; entry_freeze stays %s until explicitly "
                    "lifted (rollout §3 step 8)", reason, self._entry_freeze)
        return {"ok": True, "state": "RUNNING", "tick_loop": "running",
                "entry_freeze": self._entry_freeze}

    def canary_exec(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._state_mtx:
            if self._unit_state != "CANARY_HELD":
                return {"ok": False,
                        "error": "canary_exec refused: engine state is %r, "
                                 "need CANARY_HELD (rollout §3b precondition)"
                                 % self._unit_state}
            handler = self._canary_handler
        if handler is None:
            return {"ok": False,
                    "error": "no canary handler registered in this engine build "
                             "(write_canary glue not wired)"}
        return handler(payload)

    def _check_canary_watchdog(self, now: float) -> None:
        with self._state_mtx:
            expired = (self._unit_state == "CANARY_HELD"
                       and self._canary_deadline is not None
                       and now > self._canary_deadline)
            expire_handler = self._canary_expire_handler
        if not expired:
            return
        log.critical("CRITICAL: CANARY_HELD watchdog expired (>%.0fs) — "
                     "auto-flatten canary + unpark + RED (rollout §3b)",
                     self.cfg.canary_window_sec)
        if expire_handler is not None:
            try:
                expire_handler(self)   # canary glue's ensure_flat of the canary coin
            except Exception:  # noqa: BLE001
                log.exception("canary expire handler failed — canary residue is "
                              "operator territory (fail-stop, never auto-retry)")
        self.canary_release(reason="RED_watchdog_expired")

    # -- drain (rollout §4, routed through the admin socket) ----------------
    def _non_terminal_rows(self) -> Union[List[Dict[str, Any]], None]:
        try:
            conn = _db_connect(self.cfg.db_path)
            try:
                q = ("SELECT id, coin, sm_state FROM trades WHERE sm_state IN (%s)"
                     % ",".join("?" for _ in NON_TERMINAL_PRE_OPEN))
                cur = conn.execute(q, NON_TERMINAL_PRE_OPEN)
                return [{"id": r[0], "coin": r[1], "sm_state": r[2]} for r in cur.fetchall()]
            finally:
                conn.close()
        except sqlite3.Error as e:
            log.error("drain: cannot enumerate non-terminal rows: %s", e)
            return None

    def drain(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Rollout §4 DRAIN: entries frozen; reconciler drives every
        non-terminal pre-OPEN row to convergence, bounded DRAIN_MAX_PASSES
        reconciler ticks; report convergence.  Runs in the admin thread with
        the tick loop parked (single in-process pass at a time)."""
        with self._state_mtx:
            if self._unit_state == "CANARY_HELD":
                return {"ok": False, "error": "drain refused during CANARY_HELD"}
            if self._draining:
                return {"ok": False, "error": "drain already in progress"}
            self._draining = True
            was_parked = self._parked.is_set()
            self._parked.set()
            self._park_reason = "draining"
            self._entry_freeze = True   # drain implies frozen entries, permanently
        self._persist_state()
        log.warning("DRAIN started (max %d reconciler passes)", DRAIN_MAX_PASSES)
        passes = 0
        remaining = self._non_terminal_rows()
        db_error = remaining is None
        try:
            for i in range(DRAIN_MAX_PASSES):
                remaining = self._non_terminal_rows()
                if remaining is None:
                    db_error = True
                    break
                if not remaining:
                    break
                passes = i + 1
                with self._pass_lock:
                    try:
                        self.reconciler.incremental_pass()
                    except Exception:  # noqa: BLE001 — a drain pass failure is loud, not fatal
                        log.exception("drain: reconciler pass %d raised", passes)
                remaining = self._non_terminal_rows()
                if remaining is None:
                    db_error = True
                    break
                if not remaining:
                    break
                if i < DRAIN_MAX_PASSES - 1:
                    self._stop.wait(float(self.cfg.drain_pass_interval))
        finally:
            with self._state_mtx:
                self._draining = False
                if not was_parked:
                    self._parked.clear()
                    self._park_reason = ""
        converged = (not db_error) and (not remaining)
        result = {
            "ok": not db_error,
            "converged": bool(converged),
            "passes": passes,
            "remaining": remaining if remaining is not None else "db_error",
            "entry_freeze": True,
        }
        if db_error:
            result["error"] = "could not read sm_state rows (pre-migration DB?) — NOT converged"
        log.warning("DRAIN finished: converged=%s passes=%d remaining=%s",
                    converged, passes,
                    len(remaining) if isinstance(remaining, list) else remaining)
        return result

    # -- claim tool RPC ------------------------------------------------------
    # explicit spec (review fold-in #2): manual_claimed == FULL hands-off for
    # venue I/O, but KEEPS the phantom-K3 DB-ONLY row-resolve exactly like
    # protect_only (entry-SM §4.2.3 authority whitelist item ii): position
    # confirmed absent (K=3 + final cache-invalidated re-read) -> close the
    # DB ROW with attribution. DB write only; zero venue actions.
    CLAIM_RESOLVE_POLICY = "phantom_k3_db_row_resolve_only"
    _CLAIMABLE_STATES = ("FILLED", "PROTECTED", "OPEN", "ABORTING")

    def claim(self, coin: str, note: str = "") -> Dict[str, Any]:
        try:
            conn = _db_connect(self.cfg.db_path)
        except sqlite3.Error as e:
            return {"ok": False, "error": "db open failed: %s" % e}
        try:
            q_states = ",".join("?" for _ in self._CLAIMABLE_STATES)
            try:
                cur = conn.execute(
                    "SELECT id, sm_state, management_class FROM trades "
                    "WHERE coin = ? AND sm_state IN (%s)" % q_states,
                    (coin,) + self._CLAIMABLE_STATES)
                rows = cur.fetchall()
            except sqlite3.Error as e:
                return {"ok": False,
                        "error": "sm columns unavailable (%s) — run migrate_v3 first" % e}
            if not rows:
                return {"ok": False,
                        "error": "no claimable row for %r — the engine claims "
                                 "EXISTING rows only (reconciler 3a/3b creates "
                                 "track-only rows first, entry-SM §4.2.3)" % coin}
            ids = [r["id"] for r in rows]
            detail = {
                "claimed_at": time.time(),
                "claimed_via": "admin_rpc",
                "coin": coin,
                "trade_ids": ids,
                "note": note,
                "resolve_policy": self.CLAIM_RESOLVE_POLICY,
                "spec": "manual_claimed: zero venue actions ever (entry-SM §7.12); "
                        "phantom-K3 DB-only row-resolve retained, same as protect_only",
            }
            conn.execute(
                "UPDATE trades SET management_class = 'manual_claimed' "
                "WHERE id IN (%s)" % ",".join("?" for _ in ids), ids)
            try:
                for tid in ids:
                    conn.execute(
                        "INSERT INTO sm_transitions (trade_id, at, from_state, to_state, detail) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (tid, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                         "management_class", "manual_claimed", json.dumps(detail)))
            except sqlite3.Error:
                log.info("claim audit rows skipped (sm_transitions absent)")
            conn.commit()
            log.warning("CLAIMED %s rows %s as manual_claimed — full hands-off; "
                        "%s retained", coin, ids, self.CLAIM_RESOLVE_POLICY)
            return {"ok": True, "coin": coin, "claimed_trade_ids": ids,
                    "management_class": "manual_claimed",
                    "resolve_policy": self.CLAIM_RESOLVE_POLICY}
        finally:
            conn.close()

    # ------------------------------------------------------------- tick loop
    @staticmethod
    def _next_boundary(now: float, period: float) -> float:
        return (int(now // period) + 1) * period

    def _tick(self) -> None:
        with self._pass_lock:
            self._tick_count += 1
            self._last_tick_at = time.time()
            # 1. incremental reconciler pass (entry-SM §4 (b))
            self.reconciler.incremental_pass()
            # 2. exit-engine tick over open rows (exit-engine §4). EF4: the
            #    silent reconciler-only fallback is DELETED — a missing exit
            #    tick either failed setup loudly or was explicitly sanctioned
            #    via ENGINE_ALLOW_PARTIAL=1 (CRITICAL-logged at resolve).
            if self._exit_tick is not None:
                self._exit_tick(self)
            elif not self._allow_partial():
                raise EngineWiringError(
                    "exit-engine tick vanished at runtime without "
                    "ENGINE_ALLOW_PARTIAL=1 — failing loud (EF4)")
            # 3. entry pass — gated by entry_freeze (rollout §3 steps 2/8)
            if not self._entry_freeze:
                if self._entry_pass is not None:
                    self._entry_pass(self)
                elif not self._allow_partial():
                    raise EngineWiringError(
                        "entries UNFROZEN with no entry pass wired — failing "
                        "loud (EF4; silent entry disable forbidden)")

    def run(self) -> None:
        """Main loop.  A tick exception is CRITICAL and PROPAGATES (fail
        loud, no masking — arch-audit verdict): systemd restarts us and the
        startup reconciler full pass is the designed recovery (entry-SM §5)."""
        period = float(self.cfg.tick_seconds)
        next_wake = self._next_boundary(time.time(), period)
        while not self._stop.is_set():
            now = time.time()
            self._check_canary_watchdog(now)
            if now >= next_wake:
                next_wake = self._next_boundary(now, period)
                if self._parked.is_set():
                    log.debug("tick skipped: parked (%s)", self._park_reason)
                else:
                    try:
                        self._tick()
                    except Exception:
                        log.critical("CRITICAL: tick raised — failing loud "
                                     "(systemd restarts; startup reconciler "
                                     "recovers per entry-SM §5)", exc_info=True)
                        raise
            self._stop.wait(min(1.0, max(0.05, next_wake - time.time())))

    def start(self) -> None:
        self.setup()
        try:
            self.run()
        finally:
            self.shutdown()

    def stop(self) -> None:
        self._stop.set()

    def shutdown(self) -> None:
        self._stop.set()
        if self.admin_server is not None:
            self.admin_server.stop()
            self.admin_server = None
        self._persist_state()
        if self.db_lock is not None:
            self.db_lock.release()
            self.db_lock = None
        log.info("engine shut down cleanly")


# ----------------------------------------------------------------------------- CLI


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def _client_cmd(args: argparse.Namespace, obj: Dict[str, Any], timeout: float) -> int:
    sock = Path(args.admin_socket) if args.admin_socket else Path(args.data_dir) / "engine_admin.sock"
    try:
        resp = admin_mod.request(sock, obj, timeout=timeout)
    except admin_mod.AdminError as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        return 2
    print(json.dumps(resp, indent=1))
    if obj.get("cmd") == "drain":
        return 0 if resp.get("converged") else 1
    return 0 if resp.get("ok") else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="fleet_core.engine.runner",
        description="P3 fleet engine (live|shadow) + admin-socket client verbs")
    p.add_argument("--venue", choices=VENUES, help="venue (required to run the engine)")
    p.add_argument("--mode", choices=("live", "shadow"), default="live")
    p.add_argument("--data-dir", default="data", help="data dir (trades.db / socket / state)")
    p.add_argument("--db", default=None, help="override db path")
    p.add_argument("--tick-seconds", type=float, default=TICK_SECONDS_DEFAULT,
                   help="tick period (default %d — fleet tick)" % TICK_SECONDS_DEFAULT)
    p.add_argument("--admin-socket", default=None, help="override admin socket path")
    p.add_argument("--drain", action="store_true",
                   help="CLIENT verb: request DRAIN from the RUNNING engine via "
                        "its admin socket (rollout §4) and exit 0 iff converged")
    p.add_argument("--state", action="store_true",
                   help="CLIENT verb: query engine state via the admin socket")
    args = p.parse_args(argv)
    _setup_logging()

    # client verbs — thin admin-socket clients, NEVER a second engine process
    if args.drain:
        return _client_cmd(args, {"cmd": "drain"}, timeout=DRAIN_CLIENT_TIMEOUT_SEC)
    if args.state:
        return _client_cmd(args, {"cmd": "state"}, timeout=10.0)

    if not args.venue:
        p.error("--venue is required to run the engine")
    cfg = EngineConfig(
        venue=args.venue,
        mode=args.mode,
        data_dir=Path(args.data_dir),
        db_path=Path(args.db) if args.db else None,
        tick_seconds=args.tick_seconds,
        admin_sock=Path(args.admin_socket) if args.admin_socket else None,
    )
    engine = Engine(cfg)

    def _sig(_signum: int, _frame: Any) -> None:
        engine.stop()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    engine.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
