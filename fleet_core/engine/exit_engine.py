"""exit_engine — shared fleet Exit/SL engine (P3, per p3_design_exit_engine.md, round-3).

ARCHITECTURE (design §1): pure decision core + intent executor.

    tick(TickInput) ── pure, no I/O ──► [Intent, ...]   (typed intents, ordered)
                                             │
                                       IntentExecutor ── P2 ExchangeClient ──► venue
                                             │
                                       journal writes (sl updates, tp1, close_trade)

The decision core is a pure function of its inputs — this is what makes the
shadow runner possible (same inputs → decisions comparable to live actions) and
what makes bt-parity testable. ALL venue I/O and DB writes live in the
IntentExecutor, every venue write through the P2 ExchangeClient contract
(fleet_core/exchange_api.py) — verified-or-raise, no masked neutrals.

LAWS carried by this module (do not weaken):
  * STRATEGY MATH BYTE-PRESERVED — signal/trail/exit math comes exclusively from
    fleet_core.engine.strategy_math (md5-pinned byte-copies, design §8).
  * manual/foreign NEVER closed/managed — management_class authority gates in the
    executor (design §3 F2, entry-SM §7.9/§7.12): Close/EscalateNaked/ensure_flat
    REFUSED unless management_class == 'strategy'; 'manual_claimed' rows get ZERO
    venue intents of any kind. Review-nit fold-in: manual_claimed rows DO get the
    same phantom-K3 DB-ONLY row-resolve as protect_only (a row-close touches the
    DB, not the venue) — see tick() step 2 and IntentExecutor._exec_adopt_resolve.
  * PROTECTIVE-PLACEMENT LAW (entry-SM §4, R3/R2-F1c) — before ANY protective SL
    placement, the candidate px runs the through-px re-anchor check FIRST:
    candidate through/at current mark → re-anchor CURRENT mark ∓6%, liq-guarded,
    NEVER close; never place from a remembered/historic/user px without the gate.
  * money never unprotected — place-before-cancel is THE replace primitive (§7);
    UNKNOWN reads bias-to-protect (assume-live SL, no phantom count, defer).

Import-safe anywhere: stdlib-only at import time; pandas appears only under
TYPE_CHECKING; strategy modules load lazily via strategy_math. py_compile 3.9+.

Selftest (offline, fakes only — no venue SDKs, no network):
    python3 -m fleet_core.engine.exit_engine --selftest
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import (TYPE_CHECKING, Any, Callable, FrozenSet, List, Mapping,
                    Optional, Sequence, Tuple)

from fleet_core.exchange_api import (ExchangeClient, FlatResult, PositionInfo,
                                     ReadUnknown, SLOrderInfo, VenueRejected,
                                     WriteUnconfirmed)

if TYPE_CHECKING:  # pragma: no cover — annotation only
    import pandas as pd

log = logging.getLogger(__name__)

__all__ = [
    "PHANTOM_MISS_CLOSE_K", "SL_REPLACE_THRESH", "RESTORE_REANCHOR_PCT",
    "PROTECTIVE_UNTRACKED_PCT", "PROTECTIVE_RESIDUAL_PCT",
    "ORPHAN_TRIGGER_DEBOUNCE_SEC", "LIQ_SL_BUFFER_PCT_DEFAULT",
    "TP1_PARTIAL_DETECT_RATIO",
    "PRESENT", "ABSENT", "UNKNOWN", "Presence",
    "PosView", "VenueQuirks", "StrategyAdapter", "TickInput",
    "Intent", "NoOp", "Defer", "HealSL", "ReplaceSL", "SupersedeSweep",
    "Close", "RecordExit", "MarkTP1Partial", "EscalateNaked", "AdoptResolve",
    "tick", "next_phantom_misses", "protective_placement_gate",
    "attribute_exit", "IntentExecutor", "JournalPort",
    "SqliteJournal", "run_tick", "build_replay_engine",
    "shadow_decide",
]

# ── Engine constants — LABELED, not arbitrary: current live values (design §5 last
#    row "protective-SL % (untracked ±6%, residual ±2.5%), K=3, 5bps thresh, 180s
#    orphan debounce | fleet | engine constants, labeled") ────────────────────────
PHANTOM_MISS_CLOSE_K = 3          # hl trader.py:77 PHANTOM_MISS_CLOSE_K (fleet class-fix)
SL_REPLACE_THRESH = 0.0005        # 5 bps churn gate — hl trader.py:97 _SL_REPLACE_THRESH
RESTORE_REANCHOR_PCT = 0.06       # hl trader.py:103 _RESTORE_REANCHOR_PCT (restore re-anchor)
PROTECTIVE_UNTRACKED_PCT = 0.06   # untracked adopt-protect SL: mark ∓6% (live all-4 canon)
PROTECTIVE_RESIDUAL_PCT = 0.025   # emergency-close residual SL: mark ∓2.5% (design §9.5)
ORPHAN_TRIGGER_DEBOUNCE_SEC = 180  # orphan-trigger sweep debounce (live value)
LIQ_SL_BUFFER_PCT_DEFAULT = 0.02  # hl settings.liq_sl_buffer_pct default (ensure_sl_inside_liq)
TP1_PARTIAL_DETECT_RATIO = 0.9    # pipeline step 8: live sz < 0.9 × journal size ⇒ TP1 fill


# ── Presence tri-state (design §2: ONE open_positions() read up-front per tick) ──
PRESENT = "PRESENT"
ABSENT = "ABSENT"
UNKNOWN = "UNKNOWN"   # ReadUnknown — NEVER counts a phantom miss (bias-to-protect)


@dataclass(frozen=True)
class Presence:
    state: str                                # PRESENT | ABSENT | UNKNOWN
    info: Optional[PositionInfo] = None       # PositionInfo when PRESENT

    def __post_init__(self) -> None:
        if self.state not in (PRESENT, ABSENT, UNKNOWN):
            raise ValueError("Presence.state must be PRESENT|ABSENT|UNKNOWN")
        if self.state == PRESENT and self.info is None:
            raise ValueError("Presence(PRESENT) requires PositionInfo")


# ── Row view the decision core reads (trades row + SM columns, entry-SM §2.1) ────
@dataclass
class PosView:
    trade_id: int
    coin: str
    tf: str
    side: str                     # 'long' | 'short'
    entry: float
    size: float                   # journal size (post-TP1 = remainder)
    sl_initial: float
    sl_current: Optional[float]
    sl_placed_px: Optional[float]  # venue-ACCEPTED resting px (persisted, entry-SM §2.1)
    sl_order_id: Optional[str]
    tp1_order_id: Optional[str] = None
    tp1_price: Optional[float] = None
    tp1_partial_done: bool = False
    tp1_frac_at_entry: float = 0.0
    risk_dollars: float = 0.0
    entry_bar_ts: int = 0
    atr14: Optional[float] = None
    origin: str = "entry"          # 'entry' | 'adopted_untracked' | 'migrated'
    management_class: str = "strategy"  # 'strategy' | 'protect_only' | 'manual_claimed'
    client_order_id: Optional[str] = None
    notes: Optional[str] = None
    # One-time post-adopt restore-grace flag (pipeline step 3) — set by the
    # reconciler/adoption on rows whose SL px came from the DB, cleared by the
    # executor after the reconcile ran.
    restore_reconcile_pending: bool = False

    @property
    def is_long(self) -> bool:
        return self.side == "long"


# ── Venue-config seams (design §5 — what stays venue-specific) ───────────────────
@dataclass
class VenueQuirks:
    venue: str
    # liq-guard remedies: ('B',) clamp-only (pacifica/nado) vs ('A','B') (HL xyz).
    liq_guard_remedies: Tuple[str, ...] = ("B",)
    liq_sl_buffer_pct: float = LIQ_SL_BUFFER_PCT_DEFAULT
    # REMEDY-A venue hook (add isolated margin) — NOT on the P2 contract surface;
    # supplied by the venue binding where it exists (HL), else None ⇒ clamp-only.
    add_isolated_margin: Optional[Callable[[str, float], Any]] = None
    # venue price rounding hook (binding-supplied); None ⇒ identity.
    round_price: Optional[Callable[[str, float], float]] = None
    # exit-reason venue aliases (design §6: nado maps the engine's post-place
    # invariant-fail close to its historical string).
    reason_alias: Mapping[str, str] = field(default_factory=dict)
    # MANUAL_POSITION_PREFIXES (fence semantics; used by protect_only branch re-eval
    # 3b: prefix-manual naked ⇒ CRITICAL, NO SL — hl main.py:901–925 canon).
    manual_position_prefixes: Tuple[str, ...] = ()
    # HL xyz leg marker for regime_flat routing (step 4) — empty on other venues.
    regime_leg_prefix: str = ""
    # nado: SL trigger executes as LIMIT @ trigger±0.5% (quirk flag; the engine's
    # check_sl_hit backstop is unchanged) — carried for shadow/report visibility.
    sl_limit_slip_pct: float = 0.0
    # EF3 — BE-after-TP1 buffer: live math is entry×(1∓TRAIL_AFTER_TP_BUFFER_PCT)
    # raise-only (bots/extended/bot/trader.py:826-830 _handle_tp1_partial;
    # canonical fleet_core/strategy_xnn.py:654-660 apply_partial_be). SEEDED
    # from the live .env per design §8 (TRAIL_AFTER_TP_BUFFER_PCT=0.003 on
    # ext/hl .env today); the code default mirrors the live config default
    # (bot/config.py _get_float("TRAIL_AFTER_TP_BUFFER_PCT", 0.003)) so a
    # missed seed can never silently flip the money math to raw-entry BE.
    trail_after_tp_buffer_pct: float = 0.003
    dry_run: bool = False

    def alias_reason(self, reason: str) -> str:
        return dict(self.reason_alias).get(reason, reason)


# ── Strategy adapter (byte-copied math behind one seam; design §4 steps 9–13) ────
class StrategyAdapter:
    """Thin, math-free wrapper around a pinned strategy module's PositionManager.

    ALL math is the byte-copied module's (strategy_math.get_module); this class
    only adapts the engine PosView to the module's Position dataclass and stashes
    the PM state the live traders stash (maps §(e)): _donch_atr_e/_donch_entry_ts.
    Construct via make_strategy_adapter()."""

    def __init__(self, module: Any, pm: Any) -> None:
        self.module = module
        self.pm = pm
        self._pos_cache: dict = {}

    def _strategy_pos(self, pos: PosView) -> Any:
        sp = self._pos_cache.get(pos.trade_id)
        if sp is None:
            sp = self.module.Position(
                coin=pos.coin, tf=pos.tf, entry_price=float(pos.entry),
                sl_initial=float(pos.sl_initial),
                sl_current=float(pos.sl_current if pos.sl_current is not None
                                 else pos.sl_initial),
                tp1_price=float(pos.tp1_price or 0.0), size=float(pos.size),
                bar_entry_idx=0, side=pos.side,
                tp1_partial_done=bool(pos.tp1_partial_done),
            )
            # Restart-persistent PM stashes (journal-frozen; maps §(e)).
            sp._donch_entry_ts = int(pos.entry_bar_ts or 0)
            if pos.atr14:
                sp._donch_atr_e = float(pos.atr14)
            self._pos_cache[pos.trade_id] = sp
        else:
            # journal is authoritative for sl_current between ticks (heals/BE).
            if pos.sl_current is not None and float(pos.sl_current) > float(sp.sl_current):
                sp.sl_current = float(pos.sl_current)
        return sp

    def is_entry_bar(self, pos: PosView, df: "pd.DataFrame") -> bool:
        sp = self._strategy_pos(pos)
        fn = getattr(self.pm, "is_entry_bar", None)
        if fn is None:
            return False
        return bool(fn(sp, df))

    def update_sl_on_new_bar(self, pos: PosView, df: "pd.DataFrame"
                             ) -> Tuple[Optional[float], Optional[str]]:
        sp = self._strategy_pos(pos)
        return self.pm.update_sl_on_new_bar(sp, df)

    def check_sl_hit(self, pos: PosView, df: "pd.DataFrame",
                     vstop_wick_check: bool = True
                     ) -> Optional[Tuple[float, str]]:
        sp = self._strategy_pos(pos)
        return self.pm.check_sl_hit(sp, df, vstop_wick_check)

    def sl_current_view(self, pos: PosView) -> float:
        """The PM-resolved stop for THIS bar (staged-trail semantics live in the
        byte-copied module; this just reads its Position.sl_current)."""
        return float(self._strategy_pos(pos).sl_current)

    def forget(self, trade_id: int) -> None:
        self._pos_cache.pop(trade_id, None)


def make_strategy_adapter(venue: str, leg: str = "crypto",
                          pm_kwargs: Optional[Mapping[str, Any]] = None
                          ) -> StrategyAdapter:
    """Build a StrategyAdapter over the pinned module for (venue, leg).
    pm_kwargs = the trader-level PM ctor values SEEDED FROM THE LIVE .env
    (design §8 'SEED from live .env per venue, never memory')."""
    from fleet_core.engine import strategy_math
    module = strategy_math.get_module(venue, leg)
    kw = dict(pm_kwargs or {})
    pm = module.PositionManager(
        be_buffer_pct=float(kw.get("be_buffer_pct", 0.0)),
        vstop_pivot_window=int(kw.get("vstop_pivot_window", 2)),
        max_run_r=float(kw.get("max_run_r", 1000.0)),
        vstop_buffer_pct=float(kw.get("vstop_buffer_pct", 0.005)),
        tp1_partial_frac=float(kw.get("tp1_partial_frac", 0.0)),
    )
    return StrategyAdapter(module, pm)


# ── Tick inputs (design §2) ──────────────────────────────────────────────────────
@dataclass
class TickInput:
    pos: PosView
    presence: Presence
    # CLOSED bars only (forming bar dropped). None on StaleData/ReadUnknown —
    # the df gate runs AFTER the phantom guard (delist fix 07-02, all 4).
    bars: Optional["pd.DataFrame"]
    mark: Optional[float]                 # None on ReadUnknown
    # list_open_sl_orders(coin): frozenset of oids, or None == UNKNOWN read
    # (assume-live, anti heal-storm — fleet canon).
    sl_live_oids: Optional[FrozenSet[str]]
    liq_px: Optional[float]               # position_liquidation (diagnostic here)
    phantom_misses: int                   # persisted counter (bot_state kv)
    registry_oids: FrozenSet[str]         # placed_trigger_oids (bot-own, entry-SM §2.2)
    venue_cfg: VenueQuirks
    strategy: Optional[StrategyAdapter]
    # Fence verdict computed by the CALLER via the single fence source
    # orphan_sweep._fenced VERBATIM (+ FOREIGN_SKIP_PREFIXES + direction-guard +
    # _pending_entry), pipeline step 2. Non-None ⇒ fenced (value = reason).
    # Fail-closed law: a fence check that THROWS must be reported here as
    # 'fence_error_fail_closed' by the caller (a broken fence must fence).
    fence_verdict: Optional[str] = None
    regime_flat_due: bool = False         # step 4 (hl xyz leg venue-cfg gate verdict)
    now_ts: float = 0.0


# ── Typed intents (design §3) ────────────────────────────────────────────────────
@dataclass
class Intent:
    reason: str = ""
    critical: Optional[str] = None        # CRITICAL alert text (executor logs/escalates)

    @property
    def kind(self) -> str:
        return type(self).__name__

    # venue-touching? (authority gates key off this)
    VENUE_TOUCHING = True
    CLOSE_CLASS = False                    # executor stops after the first Close-class


@dataclass
class NoOp(Intent):
    VENUE_TOUCHING = False


@dataclass
class Defer(Intent):
    # reasons: presence_unknown | phantom_miss_n<K | liq_no_position | df_missing
    VENUE_TOUCHING = False


@dataclass
class HealSL(Intent):
    target_px: float = 0.0
    size_from_live: float = 0.0
    # 'strategy' — supersede-aware re-place of the row's stop.
    # 'protect_only_cover_check' — §3 R3 semantics: RE-RUN THE COVER CHECK first;
    #   covered ⇒ NoOp; cover vanished ⇒ re-evaluate entry-SM §4.2.3 branch at
    #   CURRENT state (prefix-manual ⇒ CRITICAL + NO placement; non-prefix ⇒ place
    #   at CURRENT mark ∓6% liq-guarded — never a remembered/user/previous px).
    mode: str = "strategy"


@dataclass
class ReplaceSL(Intent):
    target_px: float = 0.0
    current_placed_px: Optional[float] = None
    cause: str = "trail"                  # 'trail' | 'be_after_tp1' | 'restore_reanchor'


@dataclass
class SupersedeSweep(Intent):
    """Pipeline step 7b (F4/K12): >1 registry-own SL resting for a coin with a live
    row ⇒ cancel every OWN oid ≠ DB-current (DB-current survives; the trail
    re-raises next bar). Non-registry oids untouched by construction."""
    cancel_oids: Tuple[str, ...] = ()
    keep_oid: Optional[str] = None


@dataclass
class Close(Intent):
    ref_px: Optional[float] = None
    precedence_class: str = "strategy"
    CLOSE_CLASS = True


@dataclass
class RecordExit(Intent):
    exit_px: float = 0.0
    attribution_evidence: Mapping[str, Any] = field(default_factory=dict)
    VENUE_TOUCHING = False                # journal-only (t_close)
    CLOSE_CLASS = True


@dataclass
class MarkTP1Partial(Intent):
    fill_px: float = 0.0
    remainder: float = 0.0
    be_px: float = 0.0


@dataclass
class EscalateNaked(Intent):
    context: str = ""
    CLOSE_CLASS = True


@dataclass
class AdoptResolve(Intent):
    """Phantom K=3 confirmed absent → reconciler hand-off: final cache-invalidated
    re-read, then DB-ONLY row resolve with fills attribution. VENUE_TOUCHING is
    False: the executor performs venue READS (cache-invalidate re-read,
    user_fills) and one DB row-close — never a venue WRITE. Allowed for
    protect_only AND manual_claimed rows (review nit #2: a row-close touches the
    DB, not the venue — same K3 semantics as protect_only, explicit)."""
    evidence: Mapping[str, Any] = field(default_factory=dict)
    VENUE_TOUCHING = False
    CLOSE_CLASS = True


# ── Pure helpers ─────────────────────────────────────────────────────────────────

def next_phantom_misses(presence: Presence, prior: int) -> int:
    """Pure counter rule (design §2 phantom_misses → bot_state kv): ABSENT
    increments, PRESENT resets, UNKNOWN never counts (bias-to-protect)."""
    if presence.state == ABSENT:
        return prior + 1
    if presence.state == PRESENT:
        return 0
    return prior


def protective_placement_gate(side: str, candidate_px: float, mark: float,
                              reanchor_pct: float = RESTORE_REANCHOR_PCT
                              ) -> Tuple[float, bool]:
    """PROTECTIVE-PLACEMENT LAW (entry-SM §4, R3/R2-F1c) — gates EVERY protective
    placement BEFORE it executes. Candidate through or AT current mark
    (long: candidate >= mark; short: candidate <= mark) → re-anchor to CURRENT
    mark ∓ reanchor_pct, NEVER close. Returns (px, reanchored). The re-anchor
    target is always CURRENT mark — never the remembered/historic/user px."""
    if side == "long":
        if candidate_px >= mark:
            return mark * (1.0 - reanchor_pct), True
    else:
        if candidate_px <= mark:
            return mark * (1.0 + reanchor_pct), True
    return candidate_px, False


def _own_resting(t: TickInput) -> FrozenSet[str]:
    """Registry-own oids currently resting for the coin. 'bot-own' is DEFINED as
    oid ∈ placed_trigger_oids (F5, fleet-wide); the DB-current sl_order_id is
    included defensively (it is registry-seeded at migration)."""
    if t.sl_live_oids is None:
        return frozenset()
    own = set(t.sl_live_oids & t.registry_oids)
    if t.pos.sl_order_id and t.pos.sl_order_id in t.sl_live_oids:
        own.add(t.pos.sl_order_id)
    return frozenset(own)


def _is_manual_prefix(coin: str, quirks: VenueQuirks) -> bool:
    return any(coin.startswith(p) for p in quirks.manual_position_prefixes if p)


# ── The pure decision core (design §4 canonical tick pipeline) ───────────────────

def tick(t: TickInput) -> List[Intent]:
    """One position, one tick → ORDERED intent list. Pure: no I/O, no mutation of
    journal state (StrategyAdapter PM stashes are the byte-copied module's own
    in-memory semantics, identical to the live traders'). The executor stops on
    the first Close-class intent (precedence preserved, design §3)."""
    pos, quirks = t.pos, t.venue_cfg
    out: List[Intent] = []

    # ── step 1: DRY gate — decisions are still computed (shadow runner needs
    # them); the EXECUTOR suppresses venue writes when quirks.dry_run.
    # ── step 2: fence skip (single source orphan_sweep._fenced verbatim, computed
    # by the caller — see TickInput.fence_verdict contract) + management_class.
    if t.fence_verdict is not None:
        crit = ("FENCE FAIL-CLOSED: fence check threw — coin treated as FENCED"
                if t.fence_verdict == "fence_error_fail_closed" else None)
        return [NoOp(reason="fenced:%s" % t.fence_verdict, critical=crit)]

    mc = pos.management_class or "strategy"
    if mc == "manual_claimed":
        # FULL hands-off (entry-SM §2.1/§7.12): zero venue intents, no re-alert.
        # Review-nit fold-in: the phantom-K3 DB-ONLY row-resolve still applies —
        # identical semantics to protect_only (row-close touches DB, not venue).
        return _phantom_only_pipeline(t, label="manual_claimed")
    if mc == "protect_only":
        return _protect_only_tick(t)
    if mc != "strategy":
        return [NoOp(reason="unknown_management_class:%s" % mc,
                     critical="unknown management_class %r on trade %d — hands off"
                              % (mc, pos.trade_id))]

    # ── step 3: restore-grace reconcile (one-time post-adopt). The through-px
    # check IS the entry-SM §4 protective-placement law (R3/R2-F1c) — it also
    # gates every later placement inside the executor's replace primitive.
    if pos.restore_reconcile_pending:
        if t.mark is None:
            return [Defer(reason="presence_unknown")]  # cannot gate w/o mark: defer
        anchor = float(pos.sl_current if pos.sl_current is not None else pos.sl_initial)
        gated, reanchored = protective_placement_gate(pos.side, anchor, t.mark)
        if reanchored:
            # 'through' DB SL → re-anchor mark∓6%, NEVER close (hl:1331 canon).
            return [ReplaceSL(reason="restore_reanchor_through_px",
                              target_px=gated, current_placed_px=pos.sl_placed_px,
                              cause="restore_reanchor")]
        # not through — reconcile done; executor clears the flag, pipeline continues.

    # ── step 4: regime_flat (hl xyz leg only — venue cfg routes the verdict in).
    if t.regime_flat_due and quirks.regime_leg_prefix and \
            pos.coin.startswith(quirks.regime_leg_prefix):
        return out + [Close(reason="regime_flat", ref_px=t.mark,
                            precedence_class="regime")]

    # ── step 5: phantom-guard K=3 (class-fix propagated; runs BEFORE df gate).
    ph = _phantom_step(t)
    if ph is not None:
        return out + [ph]

    # ── step 6: df gate (AFTER phantom guard — delist fix 07-02, all 4;
    # BEFORE heal/supersede/forced-anchor — EF7: design §4 table order, which
    # matches live: hl trader.py df early-return at :1522 precedes the
    # SL-liveness heal at :1533. A df-missing tick defers everything except
    # fence/restore-grace/regime/phantom — exactly today's behavior; the
    # round-1 heal-before-df inversion was an UNREGISTERED deviation and is
    # reverted here).
    if t.bars is None or len(t.bars) == 0:
        out.append(Defer(reason="df_missing"))
        return out

    # ── step 7: SL-liveness heal + step 7b: supersede sweep (same listing
    # read); UNKNOWN listing ⇒ labeled assume-live NoOp (anti heal-storm).
    heal = _heal_step(t)
    sweep = _supersede_step(t)
    if heal is not None:
        out.append(heal)
    if sweep is not None:
        out.append(sweep)

    # `sl_placed_px is None` → ONE forced re-place, then anchored (§7 :144 rule)
    # — RESTRICTED to management_class='strategy' rows (R3/R2-F1a; protect_only/
    # manual_claimed never reach this pipeline). Skipped when a heal already
    # (re)places. Ordered with the step-7 anchoring block per EF7.
    if pos.sl_placed_px is None and not isinstance(heal, HealSL):
        target = float(pos.sl_current if pos.sl_current is not None
                       else pos.sl_initial)
        out.append(ReplaceSL(reason="forced_anchor_replace", target_px=target,
                             current_placed_px=None, cause="restore_reanchor"))

    # ── step 8: TP1 partial detect (live sz < 0.9×journal size) — needs no
    # strategy adapter (pure size compare + the VenueQuirks BE buffer, EF3).
    tp1 = _tp1_detect_step(t)
    if tp1 is not None:
        out.append(tp1)

    if t.strategy is None:
        out.append(Defer(reason="strategy_adapter_missing"))
        return out

    # ── step 9: PM update_sl_on_new_bar (byte-copied strategy callback).
    new_sl, exit_reason = t.strategy.update_sl_on_new_bar(pos, t.bars)

    # ── step 10: strategy-exit precedence (time_stop/tp pre-empt SL-hit).
    if exit_reason:
        ref = _bar_close(t.bars) if exit_reason in ("time_stop", "tp") else t.mark
        out.append(Close(reason=exit_reason, ref_px=ref if ref is not None else t.mark,
                         precedence_class="strategy"))
        return out

    # ── step 11: entry-bar suppression (is_entry_bar — no exit eval on bar E;
    # bt-1 contract, becomes LIVE on ext/pac/nado — registered fix KF1).
    on_entry_bar = t.strategy.is_entry_bar(pos, t.bars)

    # ── step 12: check_sl_hit (gap@Open / wick@SL) + phantom-wick guard.
    if not on_entry_bar:
        hit = t.strategy.check_sl_hit(pos, t.bars, True)
        if hit is not None:
            px, hit_reason = float(hit[0]), str(hit[1])
            guard = _phantom_wick_guard(t, hit_reason)
            if guard is not None:
                out.append(guard)          # suppressed self-close (exchange SL rests)
            else:
                out.append(Close(reason=hit_reason, ref_px=px,
                                 precedence_class="sl_hit"))
                return out

    # ── step 13: max_run_cap — inert live (MAX_RUN_R asserted 1000; the byte-
    # copied strategy would emit it via exit_reason — nothing to do here).

    # ── step 14: trail re-place (liq-guard → 5bps churn vs sl_placed_px →
    # place-before-cancel; all inside the executor's replace primitive).
    trail = _trail_step(t, new_sl)
    if trail is not None:
        out.append(trail)

    if not out:
        out.append(NoOp(reason="steady"))
    return out


# ── pipeline step helpers (pure) ─────────────────────────────────────────────────

def _phantom_step(t: TickInput) -> Optional[Intent]:
    if t.presence.state == UNKNOWN:
        return Defer(reason="presence_unknown")
    if t.presence.state == ABSENT:
        n = next_phantom_misses(t.presence, t.phantom_misses)
        if n < PHANTOM_MISS_CLOSE_K:
            return Defer(reason="phantom_miss_n<K")
        return AdoptResolve(reason="phantom_k3_confirmed_absent",
                            evidence={"misses": n, "k": PHANTOM_MISS_CLOSE_K})
    return None


def _phantom_only_pipeline(t: TickInput, label: str) -> List[Intent]:
    """manual_claimed rows: ONLY the phantom-K3 DB-only row-resolve is active
    (review nit #2 — explicit spec + code); everything else is a silent NoOp
    (the row itself is the durable ack — no re-alert, entry-SM §2.1)."""
    ph = _phantom_step(t)
    if isinstance(ph, AdoptResolve):
        ev = dict(ph.evidence)
        ev["management_class"] = label
        ev["db_only"] = True
        return [AdoptResolve(reason=ph.reason, evidence=ev)]
    if isinstance(ph, Defer):
        return [ph]
    return [NoOp(reason="%s_hands_off" % label)]


def _heal_step(t: TickInput) -> Optional[Intent]:
    """Step 7: per-tick SL-liveness heal (`_sl_confirmed_live` readback semantics).
    UNKNOWN listing ⇒ assume-live (anti heal-storm) — emitted as a LABELED
    NoOp so shadow/replay see WHY nothing was healed (never a silent skip)."""
    pos = t.pos
    if t.sl_live_oids is None:
        return NoOp(reason="sl_liveness_unknown_assume_live")
    own = _own_resting(t)
    if pos.sl_order_id and pos.sl_order_id in t.sl_live_oids:
        return None                       # DB-current confirmed live
    if own:
        return None                       # a bot-own SL rests (K12 litter) — sweep
                                          # converges it; position is protected.
    # naked — heal at the row's stop, through-px law applied in the executor's
    # replace primitive (never place from a remembered px without the gate).
    target = float(pos.sl_current if pos.sl_current is not None else pos.sl_initial)
    live_sz = abs(t.presence.info.size_signed) if (
        t.presence.state == PRESENT and t.presence.info is not None) else pos.size
    return HealSL(reason="sl_not_live", target_px=target,
                  size_from_live=float(live_sz), mode="strategy")


def _supersede_step(t: TickInput) -> Optional[Intent]:
    """Step 7b (F4): registry-own-only supersede sweep, same listing read as step 7."""
    if t.sl_live_oids is None:
        return None
    own = _own_resting(t)
    if len(own) <= 1:
        return None
    keep = t.pos.sl_order_id if (t.pos.sl_order_id in own) else None
    cancels = tuple(sorted(o for o in own if o != keep))
    if keep is None:
        # No DB-current among the own litter: deterministic rule still needs ONE
        # survivor — keep the row protected; cancel all but one (sorted-first
        # survivor is deterministic), heal/trail re-anchors next tick.
        cancels = cancels[1:]
        keep = tuple(sorted(own))[0]
    if not cancels:
        return None
    return SupersedeSweep(reason="dup_own_sl", cancel_oids=cancels, keep_oid=keep)


def _tp1_detect_step(t: TickInput) -> Optional[Intent]:
    pos = t.pos
    if pos.tp1_partial_done or (pos.tp1_frac_at_entry or 0.0) <= 0.0:
        return None
    if t.presence.state != PRESENT or t.presence.info is None:
        return None
    live_sz = abs(t.presence.info.size_signed)
    if live_sz >= TP1_PARTIAL_DETECT_RATIO * pos.size:
        return None
    fill_px = float(pos.tp1_price or (t.mark or pos.entry))
    # EF3 — BE math EXACTLY as live (ext trader.py:826-830 / canonical
    # strategy_xnn.py:654-660): be = entry×(1∓TRAIL_AFTER_TP_BUFFER_PCT),
    # buffer wired through VenueQuirks (SEEDED from the live .env, §8). The
    # raw entry is NEVER the BE target when the buffer > 0; _exec_tp1 applies
    # the raise-only clamp vs sl_current (live max()/min() semantics) and the
    # replace primitive applies liq-guard.
    buf = float(t.venue_cfg.trail_after_tp_buffer_pct or 0.0)
    be = pos.entry * (1.0 - buf) if pos.is_long else pos.entry * (1.0 + buf)
    return MarkTP1Partial(reason="tp1_partial_detected", fill_px=fill_px,
                          remainder=float(live_sz), be_px=float(be))


def _phantom_wick_guard(t: TickInput, hit_reason: str) -> Optional[Intent]:
    """Design §4 'Phantom-wick guard scope decision': HL scope (wick+gap)
    fleet-wide, behind the venue seam. Suppresses ONLY the bot's own market-close
    while the exchange reduce-only SL keeps resting; if live mark is genuinely
    through the stop, the venue SL fires anyway. mark-read failure → fail-safe
    close (unchanged). Registered delta KF6."""
    if hit_reason not in ("wick_sl", "gap_through_sl"):
        return None
    if t.mark is None:
        return None                        # fail-safe: proceed with the close
    pos = t.pos
    sl = float(pos.sl_current if pos.sl_current is not None else pos.sl_initial)
    inside = (t.mark > sl) if pos.is_long else (t.mark < sl)
    if not inside:
        return None                        # mark genuinely through → close
    if t.sl_live_oids is None or not _own_resting(t):
        return None                        # can't PROVE the exchange SL rests → close
    return NoOp(reason="phantom_%s_suppressed" % hit_reason)


def _trail_step(t: TickInput, new_sl: Optional[float]) -> Optional[Intent]:
    pos = t.pos
    if pos.sl_placed_px is None:
        return None   # forced-anchor re-place already emitted (tick() step 7)
    if new_sl is None:
        return None
    target = float(new_sl)
    # raise-only law: the byte-copied PM ratchets sl_current and never lowers
    # it (live traders re-place at the POST-ratchet pos.sl_current — hl/ext
    # trader canon); a lower target (possible only from a scripted/degraded
    # adapter) must NEVER lower a resting stop — labeled NoOp.
    sl_cur = float(pos.sl_current if pos.sl_current is not None
                   else pos.sl_initial)
    if (pos.is_long and target < sl_cur) or \
            (not pos.is_long and target > sl_cur):
        return NoOp(reason="trail_raise_only")
    placed = float(pos.sl_placed_px)
    # churn gate (§7 step 2): |target − sl_placed_px| ≤ 5 bps → labeled NoOp.
    if abs(target - placed) / max(placed, 1e-9) <= SL_REPLACE_THRESH:
        return NoOp(reason="churn_gate_5bps")
    return ReplaceSL(reason="trail_advance", target_px=target,
                     current_placed_px=placed, cause="trail")


def _bar_close(df: "pd.DataFrame") -> Optional[float]:
    try:
        return float(df["Close"].iloc[-1])
    except Exception:
        return None


# ── protect_only tick (design §3 HealSL row + §7 exemptions, R3) ─────────────────

def _protect_only_tick(t: TickInput) -> List[Intent]:
    """protect_only authority whitelist (entry-SM §4.2.3, exhaustive):
      (i)  heal = RE-RUN THE COVER CHECK first — any resting reduce-only trigger
           covering the position (or the engine's own 3c SL resting) → NoOp;
           cover vanished → re-evaluate the branch at CURRENT state:
           prefix-manual → CRITICAL + NO placement; non-prefix → place at CURRENT
           mark ∓6% liq-guarded — never a remembered/user/previous px.
      (ii) phantom-K3 DB-only row-resolve.
    NOTHING ELSE: no strategy trail, no TP1, no time-stop, no forced re-place
    (exempt from the :144 rule), and NO close authority (executor-enforced)."""
    pos = t.pos
    # (ii) phantom guard first (same before-df ordering as strategy rows).
    ph = _phantom_step(t)
    if ph is not None:
        if isinstance(ph, AdoptResolve):
            ev = dict(ph.evidence)
            ev["management_class"] = "protect_only"
            ev["db_only"] = True
            return [AdoptResolve(reason=ph.reason, evidence=ev)]
        return [ph]
    # (i) cover check — the ONLY SL machinery for protect_only rows.
    if t.sl_live_oids is None:
        return [NoOp(reason="protect_only_cover_unknown_assume_live")]
    if len(t.sl_live_oids) > 0:
        # covered (user trigger or engine 3c SL). Registry-own supersede sweep
        # applies ONLY where the engine itself placed the SL (3c — registry
        # membership); covered 3a rows have no registry SL by construction.
        sweep = _supersede_step(t)
        return ([sweep] if sweep is not None else []) + \
            [NoOp(reason="protect_only_covered")]
    # cover vanished → branch re-eval at CURRENT state.
    if _is_manual_prefix(pos.coin, t.venue_cfg):
        return [NoOp(
            reason="protect_only_prefix_manual_naked",
            critical=("UNRECONCILED prefix-manual position %s NAKED — NO bot SL "
                      "placed (hl main.py:901–925 canon: a true manual pos must "
                      "not get a bot ±6%% SL). Operator: env fence, claim "
                      "(manual_claimed), or manual close." % pos.coin))]
    if t.mark is None:
        return [Defer(reason="presence_unknown")]
    anchor = t.mark * (1.0 - PROTECTIVE_UNTRACKED_PCT) if pos.is_long \
        else t.mark * (1.0 + PROTECTIVE_UNTRACKED_PCT)
    live_sz = abs(t.presence.info.size_signed) if t.presence.info else pos.size
    return [HealSL(reason="protect_only_cover_vanished", target_px=float(anchor),
                   size_from_live=float(live_sz), mode="protect_only_cover_check")]


# ── Exit attribution (design §6 — oid-first vs ALL historic oids) ────────────────

_PROXIMITY_PCT = 0.01  # 1% proximity fallback (live all-4 value)


def _fill_get(f: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in f and f[k] not in (None, ""):
            return f[k]
    return None


def attribute_exit(pos: PosView, fills: Sequence[Mapping[str, Any]],
                   historic_oids: FrozenSet[str],
                   quirks: Optional[VenueQuirks] = None
                   ) -> Tuple[str, Optional[float], Mapping[str, Any]]:
    """→ (reason, exit_px_or_None, evidence). Fleet attribution law (§6):

    1. oid-first vs the FULL historic bot-oid corpus (F4) = current sl/tp1 oid ∪
       placed_trigger_oids registry ∪ every oid ever recorded in sm_transitions
       (caller passes the union as historic_oids): sl fill → 'sl'/'trail_sl'
       (trailed iff sl_current ≠ sl_initial — fixes the untrailed→trail_sl
       mislabel, ext (c)12); tp1 oid → 'tp'; venue liquidation flag →
       'liquidation'.
    2. 1% proximity fallback, ONE fleet order = sl_cur → sl_ini → tp (HL canon,
       3-venue majority; nado's legacy tp-first order flip = pre-registered KF10).
    3. else 'unknown_investigate'. NEVER 'manual' (NO-SILENT-MANUAL, all 4).

    Exit px law: REAL close VWAP from fills; the caller falls back to a trigger
    ref (>1% divergence logged) only when no fill px exists (wick_sl ×1330 fix)."""
    sl_cur = float(pos.sl_current if pos.sl_current is not None else pos.sl_initial)
    sl_ini = float(pos.sl_initial)
    trailed = abs(sl_cur - sl_ini) > 1e-12
    tp_px = float(pos.tp1_price) if pos.tp1_price else None
    coin_fills = []
    for f in fills:
        c = _fill_get(f, "coin", "symbol", "market")
        if c is not None and str(c) != pos.coin:
            continue
        coin_fills.append(f)

    # 1) oid-first vs the full corpus
    for f in coin_fills:
        oid = _fill_get(f, "oid", "order_id", "orderId", "oid_str")
        px = _fill_get(f, "px", "price", "avg_px", "avgPx")
        px_f = float(px) if px is not None else None
        liq_flag = bool(_fill_get(f, "liquidation", "is_liquidation")) or \
            "liquidation" in str(_fill_get(f, "dir", "type") or "").lower()
        if liq_flag:
            return ("liquidation", px_f, {"method": "venue_flag", "fill": dict(f)})
        if oid is None:
            continue
        oid_s = str(oid)
        if pos.tp1_order_id and oid_s == str(pos.tp1_order_id):
            return ("tp", px_f, {"method": "oid_first", "oid": oid_s})
        if oid_s == str(pos.sl_order_id or "") or oid_s in historic_oids:
            reason = "trail_sl" if trailed else "sl"
            return (reason, px_f, {"method": "oid_first", "oid": oid_s,
                                   "trailed": trailed})

    # 2) proximity fallback — fleet order sl_cur → sl_ini → tp (F11c)
    for f in coin_fills:
        px = _fill_get(f, "px", "price", "avg_px", "avgPx")
        if px is None:
            continue
        px_f = float(px)
        for level, reason in ((sl_cur, "trail_sl" if trailed else "sl"),
                              (sl_ini, "sl"),
                              (tp_px, "tp")):
            if level is None or level <= 0:
                continue
            if abs(px_f - level) / level <= _PROXIMITY_PCT:
                return (reason, px_f, {"method": "proximity", "level": level,
                                       "order": "sl_cur->sl_ini->tp"})

    return ("unknown_investigate", None, {"method": "none",
                                          "fills_seen": len(coin_fills)})


# ── Journal port (duck-typed; wired by the runner over P1 journal + entry-SM) ────

class JournalPort:
    """Documented write surface the executor needs. The runner supplies an object
    with these methods (P1 fleet_core/journal.py names + entry-SM §2.2 registry +
    t_close). Every method is a DB write only — no venue I/O.

      update_trade_sl(trade_id, new_sl)
      update_trade_sl_order(trade_id, sl_order_id)
      update_trade_sl_placed(trade_id, sl_placed_px)      # entry-SM §2.1 column
      mark_tp1_partial(trade_id, tp1_fill_price, remaining_size, new_sl)
      close_trade(trade_id, exit_price, exit_reason, pnl_dollars, realized_r)
          # == entry-SM t_close (OPEN → CLOSED)
      register_placed_trigger_oid(oid, coin=..., trade_id=..., kind=...)
          # F5 durable registry INSERT — MUST be called BEFORE any dependent
          # cancel/persist (write discipline, entry-SM §2.2)
      set_state(key, value) / get_state(key, default)     # bot_state kv (phantom ctr)
      clear_restore_flag(trade_id)                        # step-3 one-time flag
      sm_transition(trade_id, detail)                     # append-only audit (optional)
    """


# ── The intent executor (design §3 executor column; §7 replace primitive; §9) ────

def _allow_partial() -> bool:
    """The ONE sanctioned partial-assembly mode (runner._allow_partial twin —
    same env key, same truthy set; duplicated here so the executor's R2-1
    backstop works with zero runner import at module load)."""
    v = os.environ.get("ENGINE_ALLOW_PARTIAL")
    return v is not None and v.strip().lower() in ("1", "true", "yes", "on")


class IntentExecutor:
    def __init__(self, client: ExchangeClient, journal: Any,
                 quirks: VenueQuirks,
                 alert: Optional[Callable[[str], None]] = None) -> None:
        self.client = client
        self.journal = journal
        self.quirks = quirks
        self._alert = alert or (lambda msg: log.critical("%s", msg))
        self._forced_anchor_done: set = set()
        self._partial_mgmt_warned = False   # R2-1: CRITICAL once per executor

    # ---------------------------------------------------------------- gates
    def _critical(self, msg: str) -> None:
        try:
            self._alert(msg)
        except Exception:
            log.critical("%s", msg)

    @staticmethod
    def _close_authority_ok(pos: PosView) -> bool:
        """F2 hard authority boundary: Close/EscalateNaked/ensure_flat REFUSED
        unless management_class='strategy' (client_order_id provenance class —
        origin 'entry'/'migrated' rows carry it per entry-SM §2.1)."""
        return (pos.management_class or "strategy") == "strategy"

    def _dry(self, what: str) -> bool:
        if self.quirks.dry_run:
            log.info("DRY: would %s", what)
            return True
        return False

    # ---------------------------------------------------------------- execute
    def execute(self, t: TickInput, intents: Sequence[Intent]) -> List[Mapping[str, Any]]:
        """Run the ordered intent list; stop after the first Close-class intent
        (design §3). Returns per-intent result dicts (shadow/forensics)."""
        results: List[Mapping[str, Any]] = []
        pos = t.pos
        mc = pos.management_class or "strategy"
        for it in intents:
            res: dict = {"intent": it.kind, "reason": it.reason}
            if it.critical:
                self._critical("[%s %s] %s" % (self.quirks.venue, pos.coin, it.critical))
            try:
                # manual_claimed: refuse EVERY venue intent (entry-SM §7.12);
                # DB-only intents (AdoptResolve/RecordExit) pass — review nit #2.
                if mc == "manual_claimed" and it.VENUE_TOUCHING:
                    res.update(status="refused", why="manual_claimed_full_hands_off")
                    self._critical(
                        "[%s %s] executor REFUSED venue intent %s for "
                        "manual_claimed row %d" % (self.quirks.venue, pos.coin,
                                                   it.kind, pos.trade_id))
                elif isinstance(it, (NoOp, Defer)):
                    # R2-1 fail-loud backstop: Defer(strategy_adapter_missing)
                    # on a 'strategy' row means the assembly runs WITHOUT
                    # strategy exit management (PM trail / time_stop / tp /
                    # check_sl_hit / entry-bar suppression, steps 9-14, all
                    # INERT) — executing it as a silent ok IS the silent-
                    # disable class (fleet law: never disable anything
                    # silently). protect_only / manual_claimed rows never
                    # emit this reason (their pipelines return before step 9)
                    # and are unaffected by construction + the mc guard.
                    if (isinstance(it, Defer)
                            and it.reason == "strategy_adapter_missing"
                            and mc == "strategy"):
                        if _allow_partial():
                            if not self._partial_mgmt_warned:
                                self._partial_mgmt_warned = True
                                self._critical(
                                    "[%s %s] CRITICAL: strategy adapter "
                                    "MISSING on strategy row %d — strategy "
                                    "exit management (steps 9-14) is OFF "
                                    "under EXPLICIT ENGINE_ALLOW_PARTIAL=1 "
                                    "(staged bring-up). Heals/phantom/TP1 "
                                    "guards still run; PM trail/time_stop/"
                                    "tp/check_sl_hit do NOT."
                                    % (self.quirks.venue, pos.coin,
                                       pos.trade_id))
                            res.update(status="ok",
                                       why="allow_partial_strategy_mgmt_off")
                        else:
                            from fleet_core.engine.runner import \
                                EngineWiringError
                            raise EngineWiringError(
                                "strategy adapter MISSING for strategy row "
                                "%d (%s %s) — strategy exit management "
                                "cannot be silently disabled (R2-1/EF4). "
                                "Wire a strategy_factory (runner default: "
                                "make_strategy_adapter per leg) or set "
                                "ENGINE_ALLOW_PARTIAL=1 explicitly for "
                                "staged bring-up."
                                % (pos.trade_id, self.quirks.venue, pos.coin))
                    else:
                        res.update(status="ok")
                elif isinstance(it, (Close, EscalateNaked)):
                    if not self._close_authority_ok(pos):
                        res.update(status="refused",
                                   why="close_refused_management_class=%s" % mc)
                        self._critical(
                            "[%s %s] executor REFUSED %s for %s row %d — close "
                            "authority requires management_class='strategy' "
                            "(F2). %s" % (self.quirks.venue, pos.coin, it.kind, mc,
                                          pos.trade_id,
                                          "STILL NAKED, manual" if
                                          isinstance(it, EscalateNaked) else ""))
                    elif isinstance(it, Close):
                        res.update(self._exec_close(t, it))
                    else:
                        res.update(self._exec_escalate_naked(t, it))
                elif isinstance(it, ReplaceSL):
                    res.update(self._exec_replace(t, it))
                elif isinstance(it, HealSL):
                    res.update(self._exec_heal(t, it))
                elif isinstance(it, SupersedeSweep):
                    res.update(self._exec_supersede(t, it))
                elif isinstance(it, MarkTP1Partial):
                    res.update(self._exec_tp1(t, it))
                elif isinstance(it, AdoptResolve):
                    res.update(self._exec_adopt_resolve(t, it))
                elif isinstance(it, RecordExit):
                    res.update(self._exec_record_exit(t, it))
                else:
                    res.update(status="refused", why="unknown_intent")
            except (ReadUnknown, WriteUnconfirmed) as e:
                res.update(status="deferred", error=type(e).__name__, msg=str(e))
            results.append(res)
            if it.CLOSE_CLASS:
                break
        return results

    # -------------------------------------------------------------- liq-guard
    def _liq_guard(self, coin: str, side: str, target: float, size: float
                   ) -> Tuple[float, str]:
        """SL-inside-liquidation clamp (fleet safety LAW — the rule itself is
        fixed; mechanics restructured). REMEDY-A (add isolated margin) runs only
        where the venue seam supplies the hook; REMEDY-B clamp is always-correct.
        Clamp is a LABELED deviation (log.warning)."""
        buf = self.quirks.liq_sl_buffer_pct
        is_long = side == "long"

        def _inside(sl: float, liq: float) -> bool:
            return (sl > liq * (1.0 + buf)) if is_long else (sl < liq * (1.0 - buf))

        def _edge(liq: float) -> float:
            return liq * (1.0 + buf) if is_long else liq * (1.0 - buf)

        try:
            liq = self.client.position_liquidation(coin)
        except ReadUnknown as e:
            log.warning("liq_guard %s: position_liquidation unknown (%s) — "
                        "post-invariant will re-check", coin, e)
            return target, "liq_unknown"
        if liq is None or liq <= 0:
            return target, "cross_account_safe"
        if _inside(target, liq):
            return target, "already_safe"
        actions = []
        if "A" in self.quirks.liq_guard_remedies and self.quirks.add_isolated_margin:
            for _ in range(3):
                need = abs(_edge(liq) - liq)
                add_usd = max(1.0, size * need * 1.25)
                try:
                    self.quirks.add_isolated_margin(coin, add_usd)
                    actions.append("add_margin")
                except Exception as e:
                    log.warning("liq_guard REMEDY-A %s failed (%s) → clamp", coin, e)
                    break
                try:
                    self.client.invalidate_positions_cache()
                    liq2 = self.client.position_liquidation(coin)
                except ReadUnknown:
                    break
                if liq2 is None or liq2 <= 0:
                    return target, "+".join(actions) or "cross_account_safe"
                liq = liq2
                if _inside(target, liq):
                    return target, "+".join(actions)
        edge = _edge(liq)
        new_sl = max(target, edge) if is_long else min(target, edge)
        if self.quirks.round_price:
            try:
                new_sl = self.quirks.round_price(coin, new_sl)
            except Exception:
                pass
        if not _inside(new_sl, liq):
            bump = liq * buf * 0.5
            new_sl = new_sl + bump if is_long else new_sl - bump
        actions.append("clamp")
        log.warning("liq_guard REMEDY-B CLAMP %s %s: liq=%.6f old_sl=%.6f → %.6f "
                    "(LABELED deviation)", coin, side, liq, target, new_sl)
        return new_sl, "+".join(actions)

    def _sl_inside_liq_ok(self, coin: str, side: str, sl_px: float) -> bool:
        buf = self.quirks.liq_sl_buffer_pct
        try:
            liq = self.client.position_liquidation(coin)
        except ReadUnknown:
            return True   # cannot prove unsafe (live fail-open semantics preserved)
        if liq is None or liq <= 0:
            return True
        return (sl_px > liq * (1.0 + buf)) if side == "long" \
            else (sl_px < liq * (1.0 - buf))

    # ------------------------------------------------- §7 THE replace primitive
    def replace_sl(self, t: TickInput, target_px: float, cause: str,
                   size: Optional[float] = None,
                   churn_gate: bool = True) -> Mapping[str, Any]:
        """Design §7 verbatim — the ONLY replace path (trail / BE-after-TP1 /
        heal / restore-reanchor all route here). PLACE-BEFORE-CANCEL always.

        churn_gate=False — HEAL semantics: when NOTHING rests on the venue the
        5-bps gate must not compare against the remembered sl_placed_px (the
        vanished stop's px == the heal target in the common case ⇒ the gate
        would leave the position naked forever)."""
        pos = t.pos
        coin, side = pos.coin, pos.side
        sz = float(size if size is not None else pos.size)
        # §7 pre-gate (R3/R2-F1c): any placement whose target derives from a
        # stored px passes the protective-placement law FIRST.
        if t.mark is not None:
            target_px, reanchored = protective_placement_gate(side, target_px, t.mark)
            if reanchored:
                log.warning("replace_sl %s: through-px re-anchor → %.6f (LABELED)",
                            coin, target_px)
        # step 1: liq-guard
        target, liq_action = self._liq_guard(coin, side, float(target_px), sz)
        # step 2: churn gate vs sl_placed_px (≤5 bps → NoOp)
        if churn_gate and pos.sl_placed_px is not None and \
                abs(target - pos.sl_placed_px) / max(pos.sl_placed_px, 1e-9) \
                <= SL_REPLACE_THRESH:
            return {"status": "noop", "why": "churn_gate_5bps"}
        if self._dry("replace_sl %s → %.6f (%s)" % (coin, target, cause)):
            return {"status": "dry", "target": target}
        # step 3: place NEW first — SLOrderInfo confirmed live, or raises → KEEP
        # OLD (never naked).
        is_buy_to_close = not pos.is_long
        new = self.client.trigger_sl(coin, is_buy_to_close, sz, target)
        # step 3a: DURABLE registry INSERT before ANY dependent step (F4/F5) — a
        # kill anywhere past here leaves the new oid provably OURS.
        self.journal.register_placed_trigger_oid(new.oid, coin=coin,
                                                 trade_id=pos.trade_id, kind="sl")
        # step 4: post-invariant — fail → cancel new (registry-legal, registered
        # @3a) → Close('sl_outside_liquidation_trail').
        if not self._sl_inside_liq_ok(coin, side, new.trigger_px):
            try:
                self.client.cancel_sl_order(coin, new.oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.warning("replace_sl %s: cancel of invariant-failing oid %s "
                            "unconfirmed (%s)", coin, new.oid, e)
            reason = self.quirks.alias_reason("sl_outside_liquidation_trail")
            close_res = self._exec_close(
                t, Close(reason=reason, ref_px=t.mark, precedence_class="safety"))
            return {"status": "invariant_fail_closed", "close": close_res}
        # step 5: cancel ALL other BOT-OWN resting SLs (bot-own = oid ∈
        # placed_trigger_oids; a non-registry oid — e.g. a user's manual trigger —
        # is NEVER cancelled). Supersede, not just the remembered oid (D12).
        own_before = set()
        if t.sl_live_oids is not None:
            own_before = set(t.sl_live_oids & t.registry_oids)
            if pos.sl_order_id and pos.sl_order_id in t.sl_live_oids:
                own_before.add(pos.sl_order_id)
        for old_oid in sorted(own_before - {new.oid}):
            try:
                self.client.cancel_sl_order(coin, old_oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                # keep both (reduce-only, harmless), retry next tick (§7 step 5).
                log.warning("replace_sl %s: cancel old oid %s failed (%s) — "
                            "supersede sweep converges next tick", coin, old_oid, e)
        # step 6: persist — sl_placed_px := venue-ACCEPTED trigger px; the §7
        # invariant `sl_current == sl_placed_px == exchange resting px` (and
        # entry-SM t_protect's `sl_current := sl.trigger_px`) resolves the step-6
        # wording: both columns take the ACCEPTED px.
        self.journal.update_trade_sl_order(pos.trade_id, new.oid)
        self.journal.update_trade_sl_placed(pos.trade_id, new.trigger_px)
        self.journal.update_trade_sl(pos.trade_id, new.trigger_px)
        pos.sl_order_id = new.oid
        pos.sl_placed_px = new.trigger_px
        pos.sl_current = new.trigger_px
        return {"status": "replaced", "oid": new.oid,
                "accepted_px": new.trigger_px, "liq_action": liq_action,
                "cause": cause}

    # ------------------------------------------------------------ intent bodies
    def _exec_replace(self, t: TickInput, it: ReplaceSL) -> Mapping[str, Any]:
        pos = t.pos
        if it.cause == "restore_reanchor":
            key = (pos.trade_id, "forced_anchor")
            if it.current_placed_px is None and key in self._forced_anchor_done:
                return {"status": "noop", "why": "forced_anchor_already_done"}
            res = dict(self.replace_sl(t, it.target_px, it.cause))
            if res.get("status") in ("replaced", "dry", "noop"):
                self._forced_anchor_done.add(key)
                if pos.restore_reconcile_pending:
                    self._clear_restore_flag(pos)
            return res
        res = dict(self.replace_sl(t, it.target_px, it.cause))
        if pos.restore_reconcile_pending and res.get("status") in ("replaced", "noop"):
            self._clear_restore_flag(pos)
        return res

    def _clear_restore_flag(self, pos: PosView) -> None:
        pos.restore_reconcile_pending = False
        fn = getattr(self.journal, "clear_restore_flag", None)
        if fn is not None:
            try:
                fn(pos.trade_id)
            except Exception as e:
                log.warning("clear_restore_flag(%d) failed: %s", pos.trade_id, e)

    def _exec_heal(self, t: TickInput, it: HealSL) -> Mapping[str, Any]:
        pos = t.pos
        if it.mode == "protect_only_cover_check":
            # RE-RUN THE COVER CHECK (executor-side, fresh read — §3 R3): any
            # resting reduce-only trigger covering the position → NoOp.
            try:
                live = self.client.list_open_sl_orders(pos.coin)
            except ReadUnknown:
                return {"status": "noop", "why": "cover_unknown_assume_live"}
            if len(live) > 0:
                return {"status": "noop", "why": "covered"}
            if _is_manual_prefix(pos.coin, self.quirks):
                self._critical(
                    "[%s %s] protect_only prefix-manual NAKED — NO SL placed "
                    "(hl canon), operator must resolve" % (self.quirks.venue,
                                                           pos.coin))
                return {"status": "refused", "why": "prefix_manual_no_placement"}
            # non-prefix → CURRENT mark ∓6% liq-guarded; never a remembered px.
            if t.mark is None:
                return {"status": "deferred", "why": "no_mark"}
            anchor = t.mark * (1.0 - PROTECTIVE_UNTRACKED_PCT) if pos.is_long \
                else t.mark * (1.0 + PROTECTIVE_UNTRACKED_PCT)
            try:
                return dict(self.replace_sl(t, anchor, "protect_only_heal",
                                            size=it.size_from_live,
                                            churn_gate=False))
            except VenueRejected as e:
                # live canon nado main.py:294: CRITICAL 'STILL NAKED, manual' +
                # hands off, re-fired every pass. NEVER ensure_flat (no authority).
                self._critical("[%s %s] protect_only SL place REJECTED (%s) — "
                               "STILL NAKED, manual" % (self.quirks.venue,
                                                        pos.coin, e.reason))
                return {"status": "refused", "why": "venue_rejected_still_naked"}
        # strategy heal: supersede-aware — replace primitive handles place-before-
        # cancel + registry + supersede of own litter. churn_gate=False: the
        # heal fires only when NOTHING of ours rests, and the vanished stop's
        # remembered px equals the target in the common case — the 5bps gate
        # would otherwise leave the position naked forever (caught by the
        # replay real-engine wiring, round-2).
        try:
            return dict(self.replace_sl(t, it.target_px, "heal",
                                        size=it.size_from_live,
                                        churn_gate=False))
        except VenueRejected as e:
            # cannot place a stop → position is naked → today's
            # sl_replace_failed_naked semantics (authority-gated close).
            self._critical("[%s %s] heal SL REJECTED (%s) — escalating naked"
                           % (self.quirks.venue, pos.coin, e.reason))
            return dict(self._exec_escalate_naked(
                t, EscalateNaked(reason="sl_replace_failed_naked",
                                 context="heal_venue_rejected")))

    def _exec_supersede(self, t: TickInput, it: SupersedeSweep) -> Mapping[str, Any]:
        pos = t.pos
        cancelled, kept = [], it.keep_oid
        if self._dry("supersede sweep %s cancel %s" % (pos.coin, list(it.cancel_oids))):
            return {"status": "dry"}
        for oid in it.cancel_oids:
            if oid not in t.registry_oids and oid != pos.sl_order_id:
                # §7 step 5 law: never cancel a non-registry oid.
                log.warning("supersede %s: oid %s not registry-own — SKIP", pos.coin, oid)
                continue
            try:
                self.client.cancel_sl_order(pos.coin, oid)
                cancelled.append(oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.warning("supersede %s: cancel %s failed (%s) — retry next tick",
                            pos.coin, oid, e)
        return {"status": "swept", "cancelled": cancelled, "kept": kept}

    def _exec_tp1(self, t: TickInput, it: MarkTP1Partial) -> Mapping[str, Any]:
        pos = t.pos
        # journal first (fill already happened on-venue), then SL→BE via THE
        # replace primitive (place-before-cancel — kills ext/nado cancel-first).
        be_px = max(it.be_px, float(pos.sl_current or pos.sl_initial)) \
            if pos.is_long else min(it.be_px, float(pos.sl_current or pos.sl_initial))
        self.journal.mark_tp1_partial(pos.trade_id, it.fill_px, it.remainder, be_px)
        pos.tp1_partial_done = True
        pos.size = it.remainder
        try:
            res = dict(self.replace_sl(t, be_px, "be_after_tp1", size=it.remainder))
        except VenueRejected as e:
            self._critical("[%s %s] post-partial BE SL REJECTED (%s)"
                           % (self.quirks.venue, pos.coin, e.reason))
            res = {"status": "refused", "why": "post_partial_sl_failed"}
        return {"status": "tp1_marked", "be": res}

    # --------------------------------------------------- §9 close / emergency
    def _exec_close(self, t: TickInput, it: Close) -> Mapping[str, Any]:
        """§3 Close executor + §9 canonical ordering: cancel TP (best-effort) →
        ensure_flat (verified or raises) → cancel SL AFTER flat → real-fill
        attribution → RecordExit. Authority was gated in execute()."""
        pos = t.pos
        reason = self.quirks.alias_reason(it.reason)
        if self._dry("close %s (%s)" % (pos.coin, reason)):
            return {"status": "dry", "reason": reason}
        # 1. cancel TP best-effort
        if pos.tp1_order_id:
            try:
                self.client.cancel_sl_order(pos.coin, pos.tp1_order_id)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.warning("close %s: TP cancel failed (%s) — proceeding", pos.coin, e)
        # 2. ensure_flat — reduce-only SL KEPT RESTING during close (never over-
        # closes); WriteUnconfirmed → §9 step 5 residual protection.
        try:
            flat = self.client.ensure_flat(pos.coin)
        except WriteUnconfirmed as e:
            return self._residual_protect(t, reason, e)
        except ReadUnknown as e:
            return {"status": "deferred", "why": "ensure_flat_read_unknown",
                    "msg": str(e)}
        # 3. cancel SL only after confirmed flat
        for oid in filter(None, {pos.sl_order_id} |
                          (set(t.sl_live_oids & t.registry_oids)
                           if t.sl_live_oids is not None else set())):
            try:
                self.client.cancel_sl_order(pos.coin, oid)
            except (WriteUnconfirmed, VenueRejected) as e:
                log.warning("close %s: post-flat SL cancel %s failed (%s)",
                            pos.coin, oid, e)
        # 4. real-fill attribution → RecordExit
        exit_px = flat.exit_avg_px
        evidence: Mapping[str, Any] = {"flat": True, "reason_pre_attr": reason}
        if exit_px is None:
            try:
                fills = self.client.user_fills()
            except ReadUnknown:
                fills = []
            attr_reason, attr_px, evidence = attribute_exit(
                pos, fills, t.registry_oids, self.quirks)
            if attr_px is not None:
                exit_px = attr_px
        if exit_px is None:
            # fallback trigger ref with divergence logged (wick_sl ×1330 class:
            # NEVER silently journal the SL reference as the fill).
            exit_px = float(it.ref_px if it.ref_px is not None else
                            (t.mark or pos.sl_current or pos.sl_initial))
            log.warning("close %s: no fill px readback — journaling ref %.6f "
                        "(divergence unverifiable, flagged)", pos.coin, exit_px)
            evidence = dict(evidence)
            evidence["px_source"] = "ref_fallback"
        elif it.ref_px and it.ref_px > 0 and \
                abs(exit_px - it.ref_px) / it.ref_px > 0.01:
            log.warning("close %s: real fill %.6f diverges >1%% from ref %.6f",
                        pos.coin, exit_px, it.ref_px)
        return self._exec_record_exit(
            t, RecordExit(reason=reason, exit_px=float(exit_px),
                          attribution_evidence=evidence))

    def _residual_protect(self, t: TickInput, reason: str,
                          err: WriteUnconfirmed) -> Mapping[str, Any]:
        """§9 step 5: WriteUnconfirmed from ensure_flat → protective SL on the
        residual (±2.5% mark, liq-guarded) + row KEPT OPEN + CRITICAL."""
        pos = t.pos
        self._critical("[%s %s] ensure_flat UNCONFIRMED during close(%s): %s — "
                       "row KEPT OPEN, protecting residual"
                       % (self.quirks.venue, pos.coin, reason, err))
        if t.mark is not None:
            anchor = t.mark * (1.0 - PROTECTIVE_RESIDUAL_PCT) if pos.is_long \
                else t.mark * (1.0 + PROTECTIVE_RESIDUAL_PCT)
            try:
                self.replace_sl(t, anchor, "residual_protect")
            except (WriteUnconfirmed, VenueRejected) as e:
                self._critical("[%s %s] residual protective SL FAILED (%s) — "
                               "STILL NAKED" % (self.quirks.venue, pos.coin, e))
        return {"status": "unconfirmed_kept_open", "reason": reason}

    def _exec_escalate_naked(self, t: TickInput, it: EscalateNaked
                             ) -> Mapping[str, Any]:
        """§3 EscalateNaked (authority-gated in execute()): ensure_flat + CRITICAL
        — today's sl_replace_failed_naked semantics."""
        pos = t.pos
        reason = self.quirks.alias_reason(it.reason or "sl_replace_failed_naked")
        self._critical("[%s %s] ESCALATE NAKED (%s / %s) — emergency close"
                       % (self.quirks.venue, pos.coin, reason, it.context))
        return self._exec_close(t, Close(reason=reason, ref_px=t.mark,
                                         precedence_class="safety"))

    def _exec_adopt_resolve(self, t: TickInput, it: AdoptResolve
                            ) -> Mapping[str, Any]:
        """Phantom K=3: final cache-invalidated re-read → still absent → DB-ONLY
        row resolve with fills attribution (never a venue write). Same semantics
        for strategy, protect_only AND manual_claimed rows (review nit #2)."""
        pos = t.pos
        try:
            self.client.invalidate_positions_cache()
            live = self.client.open_positions()
        except ReadUnknown:
            return {"status": "deferred", "why": "final_reread_unknown"}
        for key in (pos.coin, pos.coin + "-USD", pos.coin + "-PERP"):
            if key in live:
                return {"status": "noop", "why": "present_on_final_reread"}
        try:
            fills = self.client.user_fills()
        except ReadUnknown:
            fills = []
        reason, px, evidence = attribute_exit(pos, fills, t.registry_oids,
                                              self.quirks)
        if reason == "unknown_investigate" and not fills:
            reason = "phantom_no_exchange_position"
        if px is None:
            px = float(t.mark or pos.sl_current or pos.sl_initial)
            evidence = dict(evidence)
            evidence["px_source"] = "ref_fallback"
        evidence = dict(evidence)
        evidence.update(it.evidence)
        return self._exec_record_exit(
            t, RecordExit(reason=self.quirks.alias_reason(reason),
                          exit_px=float(px), attribution_evidence=evidence))

    def _exec_record_exit(self, t: TickInput, it: RecordExit) -> Mapping[str, Any]:
        pos = t.pos
        exit_px = float(it.exit_px)
        sign = 1.0 if pos.is_long else -1.0
        pnl = (exit_px - pos.entry) * pos.size * sign
        risk = pos.risk_dollars if pos.risk_dollars > 0 else \
            abs(pos.entry - pos.sl_initial) * pos.size
        realized_r = (pnl / risk) if risk > 0 else 0.0
        self.journal.close_trade(pos.trade_id, exit_px, it.reason, pnl, realized_r)
        fn = getattr(self.journal, "sm_transition", None)
        if fn is not None:
            try:
                fn(pos.trade_id, {"to": "CLOSED", "reason": it.reason,
                                  "evidence": dict(it.attribution_evidence)})
            except Exception as e:
                log.warning("sm_transition audit failed: %s", e)
        return {"status": "closed", "reason": it.reason, "exit_px": exit_px,
                "pnl": pnl, "realized_r": realized_r}


# ── Production journal port (documented JournalPort surface over trades.db) ──────

class SqliteJournal:
    """The JournalPort implementation the runner wires in production (EF4):
    P1 journal-identical SQL for the sl/tp1 fields, entry_sm.t_close for
    OPEN→CLOSED, the F5 registry INSERT discipline, bot_state kv for the
    phantom counter + restore flag. Every method is a DB write only — no
    venue I/O. Lazy imports keep this module import-safe on SDK-less hosts."""

    def __init__(self, db_path) -> None:
        self.db = str(db_path)

    def _esm(self):
        from fleet_core.engine import entry_sm as esm
        return esm

    def _exec(self, sql: str, args: Sequence[Any]) -> None:
        esm = self._esm()
        with esm._conn(self.db) as con:
            con.execute(sql, list(args))

    def update_trade_sl(self, trade_id: int, new_sl: float) -> None:
        self._exec("UPDATE trades SET sl_current=? WHERE id=?",
                   (new_sl, trade_id))

    def update_trade_sl_order(self, trade_id: int, sl_order_id: str) -> None:
        self._exec("UPDATE trades SET sl_order_id=? WHERE id=?",
                   (sl_order_id, trade_id))

    def update_trade_sl_placed(self, trade_id: int, sl_placed_px: float) -> None:
        esm = self._esm()
        self._exec("UPDATE trades SET sl_placed_px=?, sl_confirmed_at=? "
                   "WHERE id=?", (sl_placed_px, esm._now_iso(), trade_id))

    def mark_tp1_partial(self, trade_id: int, tp1_fill_price: float,
                         remaining_size: float, new_sl: float) -> None:
        self._exec("UPDATE trades SET tp1_partial_done=1, tp1_fill_price=?, "
                   "size=?, sl_current=?, tp1_order_id=NULL WHERE id=?",
                   (tp1_fill_price, remaining_size, new_sl, trade_id))

    def close_trade(self, trade_id: int, exit_price: float, exit_reason: str,
                    pnl_dollars: float, realized_r: float) -> None:
        self._esm().t_close(self.db, trade_id, exit_price, exit_reason,
                            pnl_dollars, realized_r)

    def register_placed_trigger_oid(self, oid, coin: str = "",
                                    trade_id: Optional[int] = None,
                                    kind: str = "sl") -> None:
        from fleet_core.engine import registry as reg
        reg.register_oid(self.db, oid, coin or "", trade_id=trade_id,
                         kind=kind if kind in reg.VALID_KINDS else "sl")

    def get_state(self, key: str, default: Optional[str] = None
                  ) -> Optional[str]:
        esm = self._esm()
        from fleet_core.engine import migrate_v3 as mig
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            v = esm._bot_state_get(con, key)
        return default if v is None else v

    def set_state(self, key: str, value: str) -> None:
        esm = self._esm()
        from fleet_core.engine import migrate_v3 as mig
        with esm._conn(self.db) as con:
            mig.ensure_schema(con)
            esm._bot_state_set(con, key, str(value))

    def clear_restore_flag(self, trade_id: int) -> None:
        self.set_state("restore_flag:%d" % trade_id, "0")


# ── Production per-tick driver (EF4: the runner's REAL exit tick entry) ──────────

def run_tick(db_path, client: ExchangeClient, executor: "IntentExecutor",
             venue: str = "", quirks: Optional[VenueQuirks] = None,
             strategy_factory: Optional[Callable[[Mapping[str, Any]],
                                                 Optional[StrategyAdapter]]] = None,
             bars_fn: Optional[Callable[[str, str], Any]] = None
             ) -> List[Mapping[str, Any]]:
    """One exit-engine pass over the OPEN **strategy** rows of trades.db —
    the production tick entry the runner resolves (EF4). protect_only /
    manual_claimed per-tick duties are the Reconciler's
    (entry_sm.Reconciler.watch_special_rows — single owner, no dual
    management).

    Per row: build TickInput from P2 reads (ONE open_positions read up-front
    per design §2), run the pure tick(), execute via the IntentExecutor,
    persist the phantom counter (next_phantom_misses rule) and the one-time
    restore flag (design §10 / EF6). bars via `bars_fn(coin, tf)` (default
    client.candles) — Read failures ⇒ bars None ⇒ Defer(df_missing), exactly
    the live df gate. Returns per-row result dicts (forensics).

    strategy_factory: row-dict -> StrategyAdapter. The PRODUCTION runner
    seeds the default (make_strategy_adapter per leg + §8 env pm_kwargs —
    runner._resolve_strategy_factory, R2-1). Passing None here leaves every
    strategy row's steps 9-14 inert — the IntentExecutor backstop makes
    that LOUD (EngineWiringError / CRITICAL under ENGINE_ALLOW_PARTIAL=1),
    never a silent ok."""
    import sqlite3 as _sql
    from fleet_core.engine import entry_sm as esm
    from fleet_core.engine import registry as reg

    quirks = quirks or getattr(executor, "quirks", None) or VenueQuirks(venue=venue)
    journal = getattr(executor, "journal", None) or SqliteJournal(db_path)
    con = _sql.connect(str(db_path), timeout=5.0)
    con.row_factory = _sql.Row
    try:
        con.execute("PRAGMA busy_timeout=5000")
        rows = con.execute(
            "SELECT * FROM trades WHERE sm_state='OPEN' AND "
            "COALESCE(management_class,'strategy')='strategy' ORDER BY id"
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return []

    try:
        positions = client.open_positions()
        presence_known = True
    except ReadUnknown:
        positions, presence_known = {}, False

    registry_oids = frozenset(reg.oids_ever_placed(db_path))
    live_coins = sorted({str(r["coin"]) for r in rows})
    out: List[Mapping[str, Any]] = []
    for r in rows:
        tid, coin = int(r["id"]), str(r["coin"])
        pos = PosView(
            trade_id=tid, coin=coin, tf=str(r["tf"] or "8h"),
            side=str(r["direction"] or "long"),
            entry=float(r["entry"] or 0.0), size=float(r["size"] or 0.0),
            sl_initial=float(r["sl_initial"] or 0.0),
            sl_current=r["sl_current"], sl_placed_px=r["sl_placed_px"],
            sl_order_id=r["sl_order_id"], tp1_order_id=r["tp1_order_id"],
            tp1_price=r["tp1"] if "tp1" in r.keys() else None,
            tp1_partial_done=bool(r["tp1_partial_done"]),
            tp1_frac_at_entry=float(r["tp1_frac_at_entry"] or 0.0),
            risk_dollars=float(r["risk_dollars"] or 0.0),
            entry_bar_ts=int(r["entry_bar_ts"] or 0), atr14=r["atr14"],
            origin=str(r["origin"] or "entry"),
            management_class=str(r["management_class"] or "strategy"),
            client_order_id=r["client_order_id"], notes=r["notes"],
            restore_reconcile_pending=(
                str(journal.get_state("restore_flag:%d" % tid, "0")) == "1"),
        )
        if presence_known:
            info = None
            for k, p in positions.items():
                if esm._variants(k) & esm._variants(coin):
                    info = p
                    break
            presence = (Presence(PRESENT, info) if info is not None
                        else Presence(ABSENT))
        else:
            presence = Presence(UNKNOWN)
        try:
            sl_live: Optional[FrozenSet[str]] = frozenset(
                str(o) for o in client.list_open_sl_orders(coin))
        except ReadUnknown:
            sl_live = None
        try:
            mark = client.mark_price(coin, 5.0)
        except ReadUnknown:
            mark = None
        try:
            liq = client.position_liquidation(coin)
        except ReadUnknown:
            liq = None
        bars = None
        try:
            bars = bars_fn(coin, pos.tf) if bars_fn is not None \
                else client.candles(coin, pos.tf)
        except Exception as e:  # noqa: BLE001 — StaleData/ReadUnknown/no-pandas
            log.info("run_tick %s: bars unavailable (%s) — df gate defers",
                     coin, e)
        strategy = None
        if strategy_factory is not None:
            try:
                strategy = strategy_factory(dict(r))
            except Exception as e:  # noqa: BLE001 — adapter build is loud, not fatal
                log.critical("run_tick %s: strategy adapter build FAILED (%s)"
                             " — bar-driven steps defer", coin, e)
        fence_verdict = None
        if esm.position_fenced(coin, live_coins):
            fence_verdict = "position_fence"
        try:
            prior = int(journal.get_state("phantom_miss:%d" % tid, "0") or 0)
        except (TypeError, ValueError):
            prior = 0
        t = TickInput(pos=pos, presence=presence, bars=bars, mark=mark,
                      sl_live_oids=sl_live, liq_px=liq, phantom_misses=prior,
                      registry_oids=registry_oids, venue_cfg=quirks,
                      strategy=strategy, fence_verdict=fence_verdict,
                      now_ts=time.time())
        intents = tick(t)
        results = executor.execute(t, intents) or []   # recorder returns []
        closed = any(res.get("status") == "closed" for res in results)
        nxt = 0 if closed else next_phantom_misses(presence, prior)
        journal.set_state("phantom_miss:%d" % tid, str(nxt))
        out.append({"trade_id": tid, "coin": coin,
                    "intents": [i.kind for i in intents],
                    "results": results})
    return out


# ── Replay factory hook (integration contract — replay.bind_real_engine) ─────────

def build_replay_engine(journal: Any, client: Any, clock: Any,
                        criticals: Optional[List[str]] = None) -> Any:
    """REPLAY-FACING HOOK documented in fleet_core.engine.replay
    (bind_real_engine): returns the REAL-engine adapter — entry_sm transition
    machinery + Reconciler + the pure tick() + IntentExecutor — over replay's
    FakeJournal/FakeVenueClient/Clock. Implementation lives in
    fleet_core.engine.replay_binding (lazy import: replay-only dependency)."""
    from fleet_core.engine.replay_binding import RealReplayEngine
    return RealReplayEngine(journal, client, clock, criticals)


# ── Shadow decision-only adapter (P3 shadow wiring — shadow_runner interface) ────
#
# The ShadowRunner (fleet_core/engine/shadow_runner.py) lazily resolves
# `exit_engine.shadow_decide(ctx)` as its exit decision core (module docstring
# interface note). This adapter is a THIN ctx→TickInput bridge: the decisions
# come from the SAME pure tick() pipeline live runs, driven by the SAME r3
# wiring (runner._build_quirks / runner._resolve_strategy_factory — reused, not
# forked) over the md5-pinned strategy modules. NOTHING here executes: no
# journal writes, no venue writes (client is the runner's fenced
# ReadOnlyExchangeClient; reads only, budget-throttled).
#
# ctx keys (bare, from ShadowRunner._exit_phase):
#   venue, now, row (MirrorRow), presence ('PRESENT'|'ABSENT'|'UNKNOWN'),
#   mark, sl_live_oids (list|None), client, phantom_miss (int),
#   set_phantom_miss (fn), decision_only=True
# optional enrichments (shadow_main launcher / selftests):
#   row_full (full live trades row dict — tp1/tp1_frac_at_entry/pattern/…),
#   db_open_coins, manual_coins, registry_oids, bars, position_info,
#   sl_placed_px_seed, quirks, strategy_factory, live_db_path (unused here,
#   forensics stamp only)
#
# Returns §4 decision dicts: {"phase","decision","coin","tf","bar_ts",
# "params","flags","inputs"} — phases map onto the comparator's event classes
# (comparator._PHASE_TO_CLASS): trail→sl_replace, heal→heal, exit/sl_hit→exit,
# tp1_partial→tp1, phantom+AdoptResolve→phantom_resolve; NoOp/Defer stay
# non-alignable coverage records.

# Process-lifetime wiring cache: ONE quirks + ONE strategy factory (= ONE
# PositionManager per leg) per venue — mirrors live main.py:978-979 so the
# PM's restart-persistent per-trade stashes survive across shadow ticks.
_SHADOW_WIRING: dict = {}


def _shadow_wiring(venue: str) -> Mapping[str, Any]:
    w = _SHADOW_WIRING.get(venue)
    if w is None:
        # Lazy import — no module-level cycle (runner imports exit_engine
        # lazily too). Engine construction is pure attribute setup; the two
        # resolve methods ARE the r3 production wiring (R2-1).
        from fleet_core.engine.runner import Engine, EngineConfig
        eng = Engine(EngineConfig(venue=venue, mode="shadow"))
        w = {"quirks": eng._build_quirks(),
             "factory": eng._resolve_strategy_factory()}
        _SHADOW_WIRING[venue] = w
    return w


def _shadow_last_bar_ts_ms(df: Any) -> Optional[int]:
    """Open-ts (ms) of the LAST CLOSED bar in the decision df — the §6.1
    alignment key for bar-driven events (both processes see identical closed
    bars for a given (coin, tf, bar_ts))."""
    if df is None:
        return None
    try:
        import pandas as pd  # noqa: PLC0415 — decision dfs imply pandas
        col = "time" if "time" in df.columns else ("Time" if "Time" in df.columns else None)
        if col is None or len(df) == 0:
            return None
        return int(pd.Timestamp(df[col].iloc[-1]).value // 10**6)
    except Exception:  # noqa: BLE001 — bar_ts is an alignment stamp, never fatal
        return None


def _shadow_intent_record(it: Intent, quirks: VenueQuirks) -> Tuple[str, str, dict, list]:
    """Map a typed intent to (phase, decision, params, extra_flags) per design
    §4 / comparator expectations."""
    params: dict = {}
    flags: list = []
    kind = it.kind
    if isinstance(it, ReplaceSL):
        phase = "trail" if it.cause == "trail" else "pm"
        params = {"target_px": it.target_px, "placed_px": it.current_placed_px,
                  "cause": it.cause, "reason": it.reason}
    elif isinstance(it, HealSL):
        phase = "heal"
        params = {"target_px": it.target_px, "mode": it.mode,
                  "size_from_live": it.size_from_live, "reason": it.reason}
    elif isinstance(it, SupersedeSweep):
        phase = "pm"   # KF9 evidence record; not an alignable §6.1 class
        params = {"cancel_oids": list(it.cancel_oids), "keep_oid": it.keep_oid,
                  "reason": it.reason}
    elif isinstance(it, Close):
        phase = "sl_hit" if it.precedence_class == "sl_hit" else "exit"
        aliased = quirks.alias_reason(it.reason)
        params = {"reason": aliased, "ref_px": it.ref_px,
                  "precedence_class": it.precedence_class}
        if aliased != it.reason:
            params["engine_reason"] = it.reason
            flags.append("reason_venue_aliased")
    elif isinstance(it, RecordExit):
        phase = "exit"
        aliased = quirks.alias_reason(it.reason)
        params = {"reason": aliased, "exit_px": it.exit_px,
                  "attribution_evidence": dict(it.attribution_evidence)}
        if aliased != it.reason:
            params["engine_reason"] = it.reason
    elif isinstance(it, MarkTP1Partial):
        phase = "tp1_partial"
        params = {"fill_px": it.fill_px, "remainder": it.remainder,
                  "be_px": it.be_px, "reason": it.reason}
    elif isinstance(it, AdoptResolve):
        phase = "phantom"
        params = {"reason": it.reason, "evidence": dict(it.evidence)}
    elif isinstance(it, EscalateNaked):
        phase = "pm"   # live-unobservable escalation — coverage record only
        params = {"reason": it.reason, "context": it.context}
    elif isinstance(it, Defer):
        phase = "phantom" if it.reason.startswith("phantom_miss") else "pm"
        params = {"reason": it.reason}
    else:  # NoOp and any future venue-silent intent
        phase = "pm"
        params = {"reason": it.reason}
    if it.critical:
        params["critical"] = it.critical
        flags.append("critical")
    return phase, kind, params, flags


def shadow_decide(ctx: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    """Decision-only exit core for the shadow runner (design §4).

    Builds TickInput from the mirrored ctx and runs the SAME pure tick()
    pipeline the live engine executes, with the production strategy factory
    and env-seeded quirks (runner r3 wiring, reused). Persists the shadow's
    own phantom counter via ctx['set_phantom_miss'] (pure
    next_phantom_misses rule — reset on Close-class decisions, exactly
    run_tick's rule). ZERO writes anywhere else."""
    from fleet_core.engine import entry_sm as esm

    row = ctx["row"]
    venue = str(ctx.get("venue") or "")
    client = ctx.get("client")
    now = float(ctx.get("now") or time.time())
    coin = str(getattr(row, "coin", "") or "")
    tf = str(getattr(row, "tf", "") or "8h")
    row_full: Mapping[str, Any] = dict(ctx.get("row_full") or {})
    flags: List[str] = []
    if getattr(row, "approx_entry_ts", False):
        flags.append("approx_entry_ts")

    # -- wiring (quirks + pinned strategy factory) — injectable for selftests
    try:
        wiring = _shadow_wiring(venue) if (
            "quirks" not in ctx or "strategy_factory" not in ctx) else {}
    except Exception as e:  # noqa: BLE001 — wiring failure must not kill the tick
        wiring = {}
        flags.append("wiring_unavailable")
        log.critical("shadow_decide %s: r3 wiring unavailable (%s) — "
                     "bar-driven steps will defer", coin, e)
    quirks: VenueQuirks = ctx.get("quirks") or wiring.get("quirks") \
        or VenueQuirks(venue=venue, dry_run=True)
    factory = ctx.get("strategy_factory") if "strategy_factory" in ctx \
        else wiring.get("factory")

    # -- management_class: mirrored column when present, else legacy pattern /
    #    launcher-provided manual set (manual/foreign never strategy-managed)
    mc = row_full.get("management_class") or getattr(row, "management_class", None)
    if not mc:
        pattern = str(row_full.get("pattern") or "")
        manual_coins = ctx.get("manual_coins") or ()
        if pattern.lower().startswith("manual") or coin in manual_coins:
            mc = "manual_claimed"
            flags.append("manual_pattern_row")
        else:
            mc = "strategy"

    # -- presence: canon variant-match re-derivation (run_tick semantics);
    #    the runner's exact-key string is kept as forensics input.
    presence_src = str(ctx.get("presence") or UNKNOWN)
    pos_info = ctx.get("position_info")
    presence: Presence
    if pos_info is not None:
        presence = Presence(PRESENT, pos_info)
    else:
        positions = None
        if client is not None:
            try:
                positions = client.open_positions()
            except Exception:  # noqa: BLE001 — ReadUnknown/RateLimited/local denial
                positions = None
        if positions is None:
            if presence_src == PRESENT:
                sz = abs(float(getattr(row, "size", 0.0) or 0.0))
                signed = sz if str(getattr(row, "direction", "long")) == "long" else -sz
                presence = Presence(PRESENT, PositionInfo(
                    coin=coin, size_signed=signed,
                    entry_px=float(getattr(row, "entry", 0.0) or 0.0)))
                flags.append("presence_info_from_mirror")
            elif presence_src == ABSENT:
                presence = Presence(ABSENT)
            else:
                presence = Presence(UNKNOWN)
        else:
            info = None
            for k, p in positions.items():
                if esm._variants(k) & esm._variants(coin):
                    info = p
                    break
            presence = Presence(PRESENT, info) if info is not None else Presence(ABSENT)
            if presence.state != presence_src:
                flags.append("presence_reclassified")

    # -- listing / mark / bars / liq reads (defensive; per-coin degrade)
    sl_live_raw = ctx.get("sl_live_oids")
    sl_live: Optional[FrozenSet[str]] = None if sl_live_raw is None \
        else frozenset(str(o) for o in sl_live_raw)
    mark = ctx.get("mark")
    if "bars" in ctx:
        bars = ctx.get("bars")
    else:
        bars = None
        if client is not None:
            try:
                bars = client.candles(coin, tf)
            except Exception as e:  # noqa: BLE001 — StaleData/ReadUnknown/RateLimited
                flags.append("bars_unavailable")
                log.info("shadow_decide %s: bars unavailable (%s) — df gate defers",
                         coin, e)
    liq = None
    if client is not None:
        try:
            liq = client.position_liquidation(coin)
        except Exception:  # noqa: BLE001 — diagnostic input only (executor-side use)
            liq = None

    # -- sl_placed_px: venue-ACCEPTED resting px. Resolution order (design §10
    #    adoption seeding): migrated column → launcher seed → venue-listed
    #    trigger px for the row's oid → DB sl_current (only while the SL is
    #    CONFIRMED resting) → None (naked ⇒ heal path owns it).
    sl_placed = row_full.get("sl_placed_px") if "sl_placed_px" in row_full else None
    if sl_placed is None:
        sl_placed = ctx.get("sl_placed_px_seed")
    sl_oid = getattr(row, "sl_order_id", None)
    if sl_placed is None and sl_oid and client is not None:
        try:
            for t_ in client.list_reduce_only_triggers():
                if str(getattr(t_, "oid", "")) == str(sl_oid):
                    sl_placed = float(t_.trigger_px)
                    flags.append("sl_placed_px_venue_seed")
                    break
        except Exception:  # noqa: BLE001 — fall through to DB fallback
            pass
    if sl_placed is None and sl_oid and sl_live is not None \
            and str(sl_oid) in sl_live and getattr(row, "sl_current", None) is not None:
        sl_placed = float(row.sl_current)
        flags.append("sl_placed_px_db_fallback")

    # -- strategy adapter via the production factory (per-leg cache inside)
    strategy = None
    if factory is not None:
        try:
            strategy = factory({"coin": coin, "tf": tf})
        except Exception as e:  # noqa: BLE001 — loud, bar-driven steps defer
            flags.append("strategy_adapter_unavailable")
            log.critical("shadow_decide %s: strategy adapter build FAILED (%s)",
                         coin, e)

    # -- fence (single source, pinned kwargs — fail-closed inside)
    db_open = list(ctx.get("db_open_coins") or [coin])
    fence_verdict = "position_fence" if esm.position_fenced(coin, db_open) else None

    sl_initial = getattr(row, "sl_initial", None)
    if sl_initial is None:
        sl_initial = getattr(row, "sl_current", None)
        flags.append("sl_initial_missing")

    prior = int(ctx.get("phantom_miss") or 0)
    pos = PosView(
        trade_id=int(getattr(row, "trade_id", 0) or 0),
        coin=coin, tf=tf,
        side=str(getattr(row, "direction", "long") or "long"),
        entry=float(getattr(row, "entry", 0.0) or 0.0),
        size=float(getattr(row, "size", 0.0) or 0.0),
        sl_initial=float(sl_initial or 0.0),
        sl_current=getattr(row, "sl_current", None),
        sl_placed_px=sl_placed,
        sl_order_id=sl_oid,
        tp1_order_id=row_full.get("tp1_order_id"),
        tp1_price=row_full.get("tp1"),
        tp1_partial_done=bool(getattr(row, "tp1_partial_done", False)),
        tp1_frac_at_entry=float(row_full.get("tp1_frac_at_entry") or 0.0),
        risk_dollars=float(row_full.get("risk_dollars") or 0.0),
        entry_bar_ts=int(getattr(row, "entry_bar_ts", 0) or 0),
        atr14=getattr(row, "atr14", None) or row_full.get("atr14"),
        origin=str(row_full.get("origin") or "entry"),
        management_class=str(mc),
        client_order_id=row_full.get("client_order_id"),
        notes=row_full.get("notes"),
        restore_reconcile_pending=False,   # pre-cutover live rows carry no flag
    )
    t = TickInput(pos=pos, presence=presence, bars=bars, mark=mark,
                  sl_live_oids=sl_live, liq_px=liq, phantom_misses=prior,
                  registry_oids=frozenset(str(o) for o in
                                          (ctx.get("registry_oids") or ())),
                  venue_cfg=quirks, strategy=strategy,
                  fence_verdict=fence_verdict, now_ts=now)

    intents = tick(t)

    # phantom counter persistence — run_tick's exact rule, decision-only form
    closed = any(getattr(i, "CLOSE_CLASS", False) for i in intents)
    nxt = 0 if closed else next_phantom_misses(presence, prior)
    setter = ctx.get("set_phantom_miss")
    if callable(setter):
        try:
            setter(nxt)
        except Exception:  # noqa: BLE001 — shadow-state persistence best-effort
            log.warning("shadow_decide %s: phantom counter persist failed", coin)

    bar_ts = _shadow_last_bar_ts_ms(bars)
    base_inputs = {"sl_placed_px": sl_placed, "management_class": mc,
                   "phantom_misses_prior": prior, "phantom_misses_next": nxt,
                   "fence": fence_verdict, "presence_derived": presence.state,
                   "liq_px": liq}
    out: List[Mapping[str, Any]] = []
    for it in intents:
        phase, decision, params, extra = _shadow_intent_record(it, quirks)
        out.append({
            "phase": phase, "decision": decision, "coin": coin, "tf": tf,
            "bar_ts": bar_ts, "params": params,
            "inputs": dict(base_inputs),
            "flags": flags + extra,
        })
    return out


# ── Offline selftest (fakes only — no venue SDKs, no network, no live I/O) ───────

def _selftest() -> int:  # pragma: no cover — exercised via CLI
    import json

    failures: List[str] = []
    n_checks = [0]

    def check(name: str, cond: bool, detail: str = "") -> None:
        n_checks[0] += 1
        status = "ok" if cond else "FAIL"
        print("  [%s] %s %s" % (status, name, detail))
        if not cond:
            failures.append(name)

    # ---- fakes -----------------------------------------------------------
    class FakeJournal:
        def __init__(self) -> None:
            self.registry: List[Tuple[str, str]] = []
            self.sl_updates: List[Tuple[int, float]] = []
            self.sl_orders: List[Tuple[int, str]] = []
            self.sl_placed: List[Tuple[int, float]] = []
            self.closed: List[Tuple[int, float, str]] = []
            self.tp1: List[Tuple] = []
            self.events: List[str] = []

        def register_placed_trigger_oid(self, oid, coin=None, trade_id=None,
                                        kind=None):
            self.registry.append((str(oid), str(coin)))
            self.events.append("registry:%s" % oid)

        def update_trade_sl(self, tid, px):
            self.sl_updates.append((tid, px)); self.events.append("sl:%s" % px)

        def update_trade_sl_order(self, tid, oid):
            self.sl_orders.append((tid, oid)); self.events.append("sl_oid:%s" % oid)

        def update_trade_sl_placed(self, tid, px):
            self.sl_placed.append((tid, px)); self.events.append("sl_placed:%s" % px)

        def mark_tp1_partial(self, tid, px, rem, be):
            self.tp1.append((tid, px, rem, be))

        def close_trade(self, tid, px, reason, pnl, r):
            self.closed.append((tid, px, reason))

        def clear_restore_flag(self, tid):
            self.events.append("clear_restore:%d" % tid)

    class FakeClient(ExchangeClient):
        """Scriptable in-memory ExchangeClient (P2-conforming shapes)."""

        def __init__(self) -> None:
            self.resting: dict = {}          # oid -> (coin, px)
            self.positions: dict = {}        # coin -> PositionInfo
            self.fills: list = []
            self.liq: dict = {}
            self.next_oid = 100
            self.calls: List[str] = []
            self.reject_trigger = False
            self.flat_unconfirmed = False

        # writes
        def market_open(self, coin, is_buy, sz, intended_px=None,
                        allow_marketable=True):
            raise NotImplementedError

        def ensure_flat(self, coin):
            self.calls.append("ensure_flat:%s" % coin)
            if self.flat_unconfirmed:
                raise WriteUnconfirmed("close unconfirmed", may_have_landed=True)
            info = self.positions.pop(coin, None)
            sz = abs(info.size_signed) if info else 0.0
            return FlatResult(coin=coin, already_flat=info is None,
                              closed_size=sz,
                              exit_avg_px=(info.entry_px if info else None))

        def trigger_sl(self, coin, is_buy, sz, trigger_px):
            self.calls.append("trigger_sl:%s@%.6f" % (coin, trigger_px))
            if self.reject_trigger:
                raise VenueRejected("rejected", reason="min trigger size")
            oid = "oid%d" % self.next_oid
            self.next_oid += 1
            accepted = round(trigger_px, 6)
            self.resting[oid] = (coin, accepted)
            return SLOrderInfo(coin=coin, oid=oid, trigger_px=accepted, size=sz,
                               is_buy_to_close=is_buy)

        def cancel_sl_order(self, coin, oid):
            self.calls.append("cancel:%s" % oid)
            self.resting.pop(oid, None)

        def limit_reduce_only(self, coin, is_buy, sz, px):
            raise NotImplementedError

        def update_leverage(self, coin, leverage, is_cross=True):
            return None

        # reads
        def open_positions(self):
            return dict(self.positions)

        def open_orders(self):
            return []

        def list_open_sl_orders(self, coin):
            return [o for o, (c, _) in self.resting.items() if c == coin]

        def list_reduce_only_triggers(self):
            return []

        def mark_price(self, coin, max_age_sec=5.0):
            return 100.0

        def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
            raise NotImplementedError

        def equity_with_upnl(self):
            return 1000.0

        def account_value(self):
            return 1000.0

        def margin_used_usd(self):
            return 0.0

        def position_liquidation(self, coin):
            return self.liq.get(coin)

        def user_fills(self, max_age_sec=60.0):
            return list(self.fills)

    class MiniBars:
        """Duck-typed CLOSED-bars stand-in for pre-pandas checks: len() +
        ['Close'].iloc[-1] (the only surface tick() touches without a
        strategy adapter)."""

        def __init__(self, closes=(100.0,)):
            self._c = list(closes)

        def __len__(self):
            return len(self._c)

        def __getitem__(self, col):
            if col != "Close":
                raise KeyError(col)
            outer = self

            class _I:
                @property
                def iloc(self):
                    return outer._c
            return _I()

    def mkpos(**kw) -> PosView:
        base = dict(trade_id=1, coin="BTC", tf="8h", side="long", entry=100.0,
                    size=1.0, sl_initial=95.0, sl_current=95.0,
                    sl_placed_px=95.0, sl_order_id="oid1", risk_dollars=5.0)
        base.update(kw)
        return PosView(**base)

    def mkt(pos, **kw) -> TickInput:
        base = dict(
            pos=pos,
            presence=Presence(PRESENT, PositionInfo(coin=pos.coin,
                                                    size_signed=pos.size if pos.is_long
                                                    else -pos.size,
                                                    entry_px=pos.entry)),
            bars=None, mark=100.0,
            sl_live_oids=frozenset({pos.sl_order_id} if pos.sl_order_id else set()),
            liq_px=None, phantom_misses=0,
            registry_oids=frozenset({pos.sl_order_id} if pos.sl_order_id else set()),
            venue_cfg=VenueQuirks(venue="test",
                                  manual_position_prefixes=("xyz_",)),
            strategy=None)
        base.update(kw)
        return TickInput(**base)

    print("== exit_engine selftest ==")

    # 1) replace primitive: place-before-cancel, registry INSERT before cancel,
    #    venue-accepted px persisted, churn gate.
    cl, jn = FakeClient(), FakeJournal()
    cl.resting["oid1"] = ("BTC", 95.0)
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    pos = mkpos()
    t = mkt(pos)
    res = ex.replace_sl(t, 97.0, "trail")
    order_ok = False
    if res["status"] == "replaced":
        seq = [e for e in jn.events if e.startswith("registry")] and True
        # trigger placed BEFORE cancel of oid1:
        ci = cl.calls.index("cancel:oid1") if "cancel:oid1" in cl.calls else -1
        ti = [i for i, c in enumerate(cl.calls) if c.startswith("trigger_sl")][0]
        order_ok = ci > ti and bool(seq)
    check("replace place-before-cancel + registry-first",
          res["status"] == "replaced" and order_ok, json.dumps(res))
    check("replace persists ACCEPTED px to sl_placed/sl_current",
          jn.sl_placed and jn.sl_placed[-1][1] == res["accepted_px"] and
          pos.sl_current == res["accepted_px"] == pos.sl_placed_px)
    res2 = ex.replace_sl(mkt(pos, sl_live_oids=frozenset({pos.sl_order_id}),
                             registry_oids=frozenset({pos.sl_order_id})),
                         pos.sl_placed_px * 1.0002, "trail")
    check("churn gate ≤5bps → noop", res2["status"] == "noop")

    # 2) K12 supersede sweep: 2 registry-own → cancel non-DB-current; foreign
    #    (non-registry) oid untouched. (bars supplied — EF7: heal/sweep run
    #    AFTER the step-6 df gate, per design §4 table == live hl order.)
    cl, jn = FakeClient(), FakeJournal()
    cl.resting.update({"oidA": ("BTC", 95.0), "oidB": ("BTC", 96.0),
                       "manual1": ("BTC", 90.0)})
    pos = mkpos(sl_order_id="oidA")
    t = mkt(pos, bars=MiniBars(),
            sl_live_oids=frozenset({"oidA", "oidB", "manual1"}),
            registry_oids=frozenset({"oidA", "oidB"}))
    intents = tick(t)
    sweeps = [i for i in intents if isinstance(i, SupersedeSweep)]
    check("supersede sweep emitted, cancels only own non-current",
          sweeps and sweeps[0].cancel_oids == ("oidB",) and
          sweeps[0].keep_oid == "oidA",
          str([i.kind for i in intents]))
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    ex.execute(t, sweeps)
    check("sweep cancelled own dup, manual trigger untouched",
          "oidB" not in cl.resting and "manual1" in cl.resting and
          "oidA" in cl.resting)

    # 3) authority gates: Close/EscalateNaked refused for protect_only +
    #    manual_claimed; allowed for strategy.
    for mc in ("protect_only", "manual_claimed"):
        cl, jn = FakeClient(), FakeJournal()
        cl.positions["BTC"] = PositionInfo(coin="BTC", size_signed=1.0,
                                           entry_px=100.0)
        pos = mkpos(management_class=mc)
        ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
        r = ex.execute(mkt(pos), [Close(reason="tp", ref_px=100.0)])
        check("Close REFUSED for %s" % mc,
              r[0]["status"] == "refused" and not jn.closed and
              "ensure_flat:BTC" not in cl.calls)
        r = ex.execute(mkt(pos), [EscalateNaked(reason="sl_replace_failed_naked")])
        check("EscalateNaked REFUSED for %s" % mc,
              r[0]["status"] == "refused" and "ensure_flat:BTC" not in cl.calls)
    cl, jn = FakeClient(), FakeJournal()
    cl.positions["BTC"] = PositionInfo(coin="BTC", size_signed=1.0, entry_px=100.0)
    pos = mkpos()
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    r = ex.execute(mkt(pos), [Close(reason="time_stop", ref_px=101.0)])
    check("Close allowed for strategy row (flat + journaled)",
          r[0]["status"] == "closed" and jn.closed and
          jn.closed[0][2] == "time_stop")

    # 4) phantom K=3: UNKNOWN never counts; ABSENT×3 → AdoptResolve; DB-only
    #    resolve applies to manual_claimed too (review nit #2).
    pos = mkpos()
    t_unknown = mkt(pos, presence=Presence(UNKNOWN))
    check("UNKNOWN presence → Defer, counter frozen",
          isinstance(tick(t_unknown)[0], Defer) and
          next_phantom_misses(Presence(UNKNOWN), 2) == 2)
    t_abs1 = mkt(pos, presence=Presence(ABSENT), phantom_misses=0)
    t_abs3 = mkt(pos, presence=Presence(ABSENT), phantom_misses=2)
    check("ABSENT n<K → Defer; n==K → AdoptResolve",
          isinstance(tick(t_abs1)[0], Defer) and
          isinstance(tick(t_abs3)[0], AdoptResolve))
    posm = mkpos(management_class="manual_claimed")
    im = tick(mkt(posm, presence=Presence(ABSENT), phantom_misses=2))
    check("manual_claimed gets phantom-K3 DB-only AdoptResolve (nit #2)",
          isinstance(im[0], AdoptResolve) and
          im[0].evidence.get("db_only") is True)
    cl, jn = FakeClient(), FakeJournal()   # venue: no position, no fills
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    r = ex.execute(mkt(posm, presence=Presence(ABSENT), phantom_misses=2), im)
    check("manual_claimed K3 resolve = row closed, ZERO venue writes",
          r[0]["status"] == "closed" and
          jn.closed and jn.closed[0][2] == "phantom_no_exchange_position" and
          not [c for c in cl.calls if c.startswith(("trigger", "cancel",
                                                    "ensure_flat"))])
    # manual_claimed otherwise: zero venue intents
    im2 = tick(mkt(posm))
    check("manual_claimed steady tick = NoOp only",
          len(im2) == 1 and isinstance(im2[0], NoOp))

    # 5) protect_only heal semantics (R3): covered → NoOp; vanished non-prefix →
    #    place at CURRENT mark ∓6%, never remembered px; prefix-manual → CRITICAL
    #    no placement.
    posp = mkpos(management_class="protect_only", sl_order_id=None,
                 sl_placed_px=None, sl_current=None)
    t_cov = mkt(posp, sl_live_oids=frozenset({"user_trig"}),
                registry_oids=frozenset())
    iv = tick(t_cov)
    check("protect_only covered → track-only NoOp (no placement)",
          all(isinstance(i, NoOp) for i in iv), str([i.kind for i in iv]))
    t_van = mkt(posp, sl_live_oids=frozenset(), registry_oids=frozenset())
    iv = tick(t_van)
    heal = iv[0]
    check("protect_only cover vanished → HealSL @ CURRENT mark −6%",
          isinstance(heal, HealSL) and
          abs(heal.target_px - 100.0 * 0.94) < 1e-9 and
          heal.mode == "protect_only_cover_check")
    cl, jn = FakeClient(), FakeJournal()
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    r = ex.execute(t_van, iv)
    check("protect_only heal placed engine SL at mark−6% + registry INSERT",
          r[0]["status"] == "replaced" and jn.registry and
          abs(r[0]["accepted_px"] - 94.0) < 1e-6)
    posx = mkpos(coin="xyz_GOLD", management_class="protect_only",
                 sl_order_id=None, sl_placed_px=None, sl_current=None)
    t_pfx = mkt(posx, sl_live_oids=frozenset(), registry_oids=frozenset())
    iv = tick(t_pfx)
    check("protect_only prefix-manual naked → CRITICAL, NO placement",
          isinstance(iv[0], NoOp) and iv[0].critical is not None)
    cl, jn = FakeClient(), FakeJournal()
    crits: List[str] = []
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test",
                                            manual_position_prefixes=("xyz_",)),
                        alert=crits.append)
    ex.execute(t_pfx, iv)
    check("prefix-manual naked executor: CRITICAL fired, zero venue calls",
          crits and not cl.calls)

    # 6) forced re-place (:144 rule) — strategy rows only; protect_only exempt.
    #    Runs with the step-7 anchoring block, AFTER the df gate (EF7).
    poss = mkpos(sl_placed_px=None)
    iv = tick(mkt(poss, bars=MiniBars()))
    forced = [i for i in iv if isinstance(i, ReplaceSL) and
              i.cause == "restore_reanchor"]
    check("sl_placed_px None → forced re-place for strategy row", bool(forced))
    iv = tick(mkt(poss))   # df missing → the WHOLE anchoring block defers
    check("EF7: df gate precedes heal/sweep/forced-anchor (design §4 order)",
          [i.kind for i in iv][-1] == "Defer" and
          not [i for i in iv if isinstance(i, (ReplaceSL, HealSL,
                                               SupersedeSweep))],
          str([(i.kind, i.reason) for i in iv]))
    # protect_only with NULL sl_* and covered → NO ReplaceSL anywhere:
    iv = tick(t_cov)
    check("protect_only exempt from :144 forced re-place",
          not [i for i in iv if isinstance(i, ReplaceSL)])
    # EF3) BE-after-TP1 math: entry×(1∓TRAIL_AFTER_TP_BUFFER_PCT) exactly as
    # live (ext trader.py:826-830 / canonical strategy_xnn.py:654-660); the
    # buffer rides VenueQuirks (SEEDED from live .env — ext .env = 0.003);
    # raw entry is NEVER the BE target when buffer > 0.
    ext_q = VenueQuirks(venue="extended", trail_after_tp_buffer_pct=0.003)
    post1 = mkpos(tp1_frac_at_entry=0.5, size=1.0, entry=100.0)
    t_tp1 = mkt(post1, bars=MiniBars(), venue_cfg=ext_q,
                presence=Presence(PRESENT, PositionInfo(
                    coin="BTC", size_signed=0.5, entry_px=100.0)))
    iv_tp1 = tick(t_tp1)
    tp1s = [i for i in iv_tp1 if isinstance(i, MarkTP1Partial)]
    check("EF3: BE px = entry×(1−buf) on ext config (never raw entry)",
          len(tp1s) == 1 and abs(tp1s[0].be_px - 100.0 * 0.997) < 1e-9 and
          abs(tp1s[0].be_px - 100.0) > 1e-6,
          str([(i.kind, getattr(i, "be_px", None)) for i in iv_tp1]))
    post2 = mkpos(tp1_frac_at_entry=0.5, size=1.0, entry=100.0, side="short",
                  sl_initial=105.0, sl_current=105.0, sl_placed_px=105.0)
    t_tp2 = mkt(post2, bars=MiniBars(), venue_cfg=ext_q,
                presence=Presence(PRESENT, PositionInfo(
                    coin="BTC", size_signed=-0.5, entry_px=100.0)))
    iv = tick(t_tp2)
    tp1s = [i for i in iv if isinstance(i, MarkTP1Partial)]
    check("EF3: short side BE px = entry×(1+buf)",
          len(tp1s) == 1 and abs(tp1s[0].be_px - 100.0 * 1.003) < 1e-9)
    # executor applies the live raise-only clamp vs sl_current (max()/min())
    cl, jn = FakeClient(), FakeJournal()
    ex = IntentExecutor(cl, jn, ext_q)
    ex.execute(t_tp1, [i for i in iv_tp1 if isinstance(i, MarkTP1Partial)])
    check("EF3: executor placed the BUFFERED BE (99.7), raise-only held",
          jn.tp1 and abs(jn.tp1[-1][3] - 99.7) < 1e-9 and
          jn.sl_placed and abs(jn.sl_placed[-1][1] - 99.7) < 1e-6,
          str(jn.tp1))

    # 7) wick/gap phantom guard (KF6, wick+gap fleet-wide): mark inside + own SL
    #    resting → suppress; mark through → close; mark None → fail-safe close.
    import pandas as pd  # local import — selftest only

    def mkbars(o, h, l, c, ts=1000000):
        return pd.DataFrame({"time": [pd.Timestamp(ts, unit="ms")],
                             "Open": [o], "High": [h], "Low": [l], "Close": [c]})

    class StubStrategy:
        def __init__(self, hit=None, new_sl=None, exit_reason=None,
                     entry_bar=False):
            self._hit, self._new_sl = hit, new_sl
            self._exit, self._entry_bar = exit_reason, entry_bar

        def is_entry_bar(self, pos, df):
            return self._entry_bar

        def update_sl_on_new_bar(self, pos, df):
            return self._new_sl, self._exit

        def check_sl_hit(self, pos, df, wick):
            return self._hit

        def sl_current_view(self, pos):
            return float(pos.sl_current)

    bars = mkbars(99, 101, 94, 100)
    pos = mkpos()
    t = mkt(pos, bars=bars, mark=100.0,
            strategy=StubStrategy(hit=(95.0, "wick_sl")))
    iv = tick(t)
    check("phantom wick guard suppresses (mark inside, SL resting)",
          any(isinstance(i, NoOp) and "suppressed" in i.reason for i in iv) and
          not any(isinstance(i, Close) for i in iv))
    t = mkt(pos, bars=bars, mark=90.0,
            strategy=StubStrategy(hit=(95.0, "wick_sl")))
    iv = tick(t)
    check("mark through stop → Close(wick_sl)",
          any(isinstance(i, Close) and i.reason == "wick_sl" for i in iv))
    t = mkt(pos, bars=bars, mark=None,
            strategy=StubStrategy(hit=(95.0, "gap_through_sl")))
    iv = tick(t)
    check("mark-read failure → fail-safe close (guard bypassed)",
          any(isinstance(i, Close) and i.reason == "gap_through_sl" for i in iv))

    # 8) strategy precedence + entry-bar suppression.
    t = mkt(pos, bars=bars, strategy=StubStrategy(hit=(95.0, "wick_sl"),
                                                  exit_reason="time_stop"))
    iv = tick(t)
    check("time_stop pre-empts SL-hit (precedence §4.10)",
          isinstance(iv[-1], Close) and iv[-1].reason == "time_stop")
    t = mkt(pos, bars=bars, strategy=StubStrategy(hit=(95.0, "wick_sl"),
                                                  entry_bar=True))
    iv = tick(t)
    check("entry-bar suppression: no exit eval on bar E (KF1)",
          not any(isinstance(i, Close) for i in iv))

    # 9) trail path: churn-gated ReplaceSL from PM new_sl.
    t = mkt(pos, bars=bars, strategy=StubStrategy(new_sl=97.0))
    iv = tick(t)
    check("trail advance → ReplaceSL(cause=trail)",
          any(isinstance(i, ReplaceSL) and i.cause == "trail" and
              i.target_px == 97.0 for i in iv))
    t = mkt(pos, bars=bars, strategy=StubStrategy(new_sl=95.0 * 1.0003))
    iv = tick(t)
    check("trail ≤5bps churn → no ReplaceSL",
          not any(isinstance(i, ReplaceSL) for i in iv))

    # 10) through-px re-anchor law (restore-grace step 3 + placement gate).
    posr = mkpos(sl_current=105.0, restore_reconcile_pending=True)
    iv = tick(mkt(posr, mark=100.0))
    check("restore 'through' DB SL → re-anchor mark−6%, NEVER close",
          isinstance(iv[0], ReplaceSL) and iv[0].cause == "restore_reanchor" and
          abs(iv[0].target_px - 94.0) < 1e-9 and
          not any(isinstance(i, Close) for i in iv))
    px, re_a = protective_placement_gate("short", 95.0, 100.0)
    check("gate (short, candidate below mark) → re-anchor mark+6%",
          re_a and abs(px - 106.0) < 1e-9)

    # 11) attribution: oid-first historic corpus; untrailed → 'sl'; proximity
    #     order sl_cur→sl_ini→tp; never 'manual'; unknown_investigate.
    pos = mkpos(sl_current=95.0, sl_initial=95.0, tp1_price=110.0)
    fills = [{"coin": "BTC", "oid": "histK12", "px": 96.1}]
    r = attribute_exit(pos, fills, frozenset({"histK12"}))
    check("oid-first vs historic corpus (K12 fired oid) → 'sl' (untrailed)",
          r[0] == "sl" and r[1] == 96.1)
    post = mkpos(sl_current=97.0, sl_initial=95.0)
    r = attribute_exit(post, fills, frozenset({"histK12"}))
    check("trailed row (sl_cur≠sl_ini) → 'trail_sl'", r[0] == "trail_sl")
    r = attribute_exit(post, [{"coin": "BTC", "oid": "someone", "px": 96.9}],
                       frozenset())
    check("proximity fallback order sl_cur first", r[0] == "trail_sl")
    r = attribute_exit(post, [{"coin": "BTC", "oid": "x", "px": 300.0}],
                       frozenset())
    check("no match → unknown_investigate (NEVER 'manual')",
          r[0] == "unknown_investigate")
    r = attribute_exit(post, [{"coin": "BTC", "px": 96.9,
                               "dir": "Liquidation"}], frozenset())
    check("venue liquidation flag → 'liquidation'", r[0] == "liquidation")

    # 12) heal: UNKNOWN SL listing → assume-live (LABELED NoOp, no heal);
    #     naked → HealSL. Heal placement bypasses the churn gate (a vanished
    #     stop's remembered px == the target — the gate would leave the row
    #     naked forever; caught by the real-engine replay wiring, round-2).
    pos = mkpos()
    iv = tick(mkt(pos, bars=MiniBars(), sl_live_oids=None))
    check("UNKNOWN SL listing → labeled assume-live NoOp, no heal",
          not any(isinstance(i, HealSL) for i in iv) and
          any(i.reason == "sl_liveness_unknown_assume_live" for i in iv),
          str([(i.kind, i.reason) for i in iv]))
    iv = tick(mkt(pos, bars=MiniBars(), sl_live_oids=frozenset(),
                  registry_oids=frozenset()))
    check("naked strategy row → HealSL",
          any(isinstance(i, HealSL) and i.mode == "strategy" for i in iv))
    cl, jn = FakeClient(), FakeJournal()
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"))
    r = ex.execute(mkt(pos, bars=MiniBars(), sl_live_oids=frozenset(),
                       registry_oids=frozenset()),
                   [i for i in iv if isinstance(i, HealSL)])
    check("heal re-places at the remembered target DESPITE the 5bps gate "
          "(churn_gate=False on heal)",
          r[0].get("status") == "replaced" and
          abs(r[0].get("accepted_px", 0) - 95.0) < 1e-6, str(r))

    # 13) §9 residual protection: ensure_flat unconfirmed → row kept open +
    #     protective SL ±2.5% + CRITICAL.
    cl, jn = FakeClient(), FakeJournal()
    cl.positions["BTC"] = PositionInfo(coin="BTC", size_signed=1.0, entry_px=100.0)
    cl.flat_unconfirmed = True
    crits = []
    ex = IntentExecutor(cl, jn, VenueQuirks(venue="test"), alert=crits.append)
    pos = mkpos()
    r = ex.execute(mkt(pos), [Close(reason="tp", ref_px=100.0)])
    placed = [c for c in cl.calls if c.startswith("trigger_sl")]
    check("ensure_flat UNCONFIRMED → kept open + residual SL 2.5% + CRITICAL",
          r[0]["status"] == "unconfirmed_kept_open" and not jn.closed and
          crits and placed and "97.5" in placed[-1])

    # 14) nado reason alias seam.
    q = VenueQuirks(venue="nado", reason_alias={
        "sl_outside_liquidation_trail": "sl_outside_liq_unfixable"})
    check("VenueQuirks.reason_alias maps invariant-fail close string",
          q.alias_reason("sl_outside_liquidation_trail") ==
          "sl_outside_liq_unfixable" and q.alias_reason("tp") == "tp")

    # 15) strategy_math integration: pinned module drives a real trail on
    #     synthetic bars (byte-copied math end-to-end).
    from fleet_core.engine import strategy_math
    strategy_math.verify_pins()
    adapter = make_strategy_adapter("extended", pm_kwargs={
        "be_buffer_pct": 0.002, "vstop_pivot_window": 2, "max_run_r": 1000.0,
        "vstop_buffer_pct": 0.005, "tp1_partial_frac": 0.0})
    import numpy as np
    n = 40
    ts = pd.date_range("2026-01-01", periods=n, freq="8h")
    base = np.linspace(100, 120, n)
    df = pd.DataFrame({"time": ts, "Open": base, "High": base + 1.0,
                       "Low": base - 1.0, "Close": base + 0.5,
                       "Volume": np.ones(n)})
    pos = mkpos(entry=105.0, sl_initial=100.0, sl_current=100.0,
                sl_placed_px=100.0,
                entry_bar_ts=int(ts[10].value // 10 ** 6))
    t = mkt(pos, bars=df, strategy=adapter)
    iv = tick(t)
    kinds = [i.kind for i in iv]
    check("pinned strategy module ticks clean (no exception)", True,
          str(kinds))
    check("byte-copied module is the canonical (DONCHIAN_N=15)",
          adapter.module.DONCHIAN_N == 15)

    # 16) fence: fenced verdict → single NoOp; fail-closed carries CRITICAL.
    iv = tick(mkt(mkpos(), fence_verdict="FX_EXCLUDE"))
    check("fenced → NoOp only", len(iv) == 1 and isinstance(iv[0], NoOp))
    iv = tick(mkt(mkpos(), fence_verdict="fence_error_fail_closed"))
    check("fence error → fail-closed FENCED + CRITICAL",
          isinstance(iv[0], NoOp) and iv[0].critical is not None)

    # 17) R2-1 backstop: Defer(strategy_adapter_missing) on a strategy row is
    #     NEVER a silent ok — raises EngineWiringError; under EXPLICIT
    #     ENGINE_ALLOW_PARTIAL=1 it executes with a labeled why + CRITICAL
    #     logged ONCE; protect_only/manual_claimed rows unaffected.
    prev_allow = os.environ.pop("ENGINE_ALLOW_PARTIAL", None)
    try:
        pos = mkpos()
        iv = tick(mkt(pos, bars=MiniBars(), strategy=None))
        check("R2-1: strategy row + no adapter → Defer(strategy_adapter_missing)",
              any(isinstance(i, Defer) and i.reason == "strategy_adapter_missing"
                  for i in iv), str([(i.kind, i.reason) for i in iv]))
        ex = IntentExecutor(FakeClient(), FakeJournal(), VenueQuirks(venue="test"))
        raised = False
        try:
            ex.execute(mkt(pos, bars=MiniBars(), strategy=None), iv)
        except Exception as e:  # noqa: BLE001 — asserting the exact type below
            raised = type(e).__name__ == "EngineWiringError"
        check("R2-1: ALLOW_PARTIAL unset → executor RAISES EngineWiringError",
              raised)
        os.environ["ENGINE_ALLOW_PARTIAL"] = "1"
        crits: List[str] = []
        ex = IntentExecutor(FakeClient(), FakeJournal(), VenueQuirks(venue="test"),
                            alert=crits.append)
        r1 = ex.execute(mkt(pos, bars=MiniBars(), strategy=None), iv)
        ex.execute(mkt(pos, bars=MiniBars(), strategy=None), iv)  # 2nd pass
        d = [x for x in r1 if x.get("intent") == "Defer"
             and x.get("reason") == "strategy_adapter_missing"]
        check("R2-1: ALLOW_PARTIAL=1 → labeled ok + CRITICAL logged ONCE",
              d and d[0].get("status") == "ok" and
              d[0].get("why") == "allow_partial_strategy_mgmt_off" and
              sum(1 for c in crits if "strategy adapter MISSING" in c) == 1,
              "d=%s crits=%d" % (d, len(crits)))
        os.environ.pop("ENGINE_ALLOW_PARTIAL", None)
        # protect_only: pipeline never emits the reason; a synthetic Defer with
        # this reason on a protect_only row must NOT trip the backstop.
        posp = mkpos(management_class="protect_only")
        ex = IntentExecutor(FakeClient(), FakeJournal(), VenueQuirks(venue="test"))
        rp = ex.execute(mkt(posp), [Defer(reason="strategy_adapter_missing")])
        check("R2-1: protect_only row unaffected by the backstop (plain ok)",
              rp[0].get("status") == "ok" and "why" not in rp[0], str(rp))
    finally:
        if prev_allow is not None:
            os.environ["ENGINE_ALLOW_PARTIAL"] = prev_allow
        else:
            os.environ.pop("ENGINE_ALLOW_PARTIAL", None)

    print("== %d checks, %d failures ==" % (n_checks[0], len(failures)))
    if failures:
        print("FAILED:", failures)
        return 1
    print("exit_engine selftest: ALL GREEN")
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys as _sys
    logging.basicConfig(level=logging.WARNING)
    if "--selftest" in _sys.argv:
        raise SystemExit(_selftest())
    print(__doc__)
