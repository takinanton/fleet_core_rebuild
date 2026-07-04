"""fleet_core.engine.replay_scenarios — scripted scenarios for the 10
coverage-ledger classes (p3_design_shadow_runner.md §7.5).

Each scenario is a self-contained scripted world (fake journal + fake venue
through the P2 fault bank) with EXACT expected-intent assertions. Any
deviation = `bug` class, gate blocked (shadow §7.5: "Each class's replay must
produce EXACTLY the expected intent sequence (asserted, machine-checked)").

| # | ledger class (shadow §7.5)      | scenario                        |
|---|---------------------------------|---------------------------------|
| 1 | entry (signal→entry→protect)    | entry_full_cycle                |
| 2 | exit — SL/trail_sl attribution  | exit_sl_fire_attribution        |
| 3 | trail re-place                  | trail_replace                   |
| 4 | TP1 partial → BE replace        | tp1_partial_be                  |
| 5 | abort-unwind                    | abort_unwind                    |
| 6 | SL-liveness heal                | sl_liveness_heal                |
| 7 | phantom K=3 resolve             | phantom_k3_resolve  (incl. the |
|   |                                 |  protect_only + manual_claimed |
|   |                                 |  DB-only row-resolve — nit 2)  |
| 8 | adopt-untracked (protect-only)  | adopt_untracked_protect_only    |
| 9 | orphan-trigger cancel           | orphan_trigger_cancel           |
|10 | supersede sweep (K12 litter)    | supersede_sweep_k12             |

Fault-bank composition follows §7.5 verbatim where specified: abort-unwind =
accept_partial_land(ratio < min_fill) then FlatResult; supersede = K12
kill-between-confirm-and-persist replayed from a checkpointed SM db.

Strategy outputs are SCRIPTED data (ScriptedStrategy) — no strategy math is
reimplemented here (byte-pins live in the exit-engine build, §8).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from fleet_core.engine.replay import (
    EntryPlan,
    Intent,
    ReplayEnv,
    ScenarioFailure,
    ScriptedStrategy,
    fake_bot_config,
)

__all__ = ["SCENARIOS"]


class Scenario:
    def __init__(self, name: str, ledger_class: str, fn) -> None:
        self.name = name
        self.ledger_class = ledger_class
        self._fn = fn

    def run(self, env: ReplayEnv) -> None:
        self._fn(env)


def _approx(a: Optional[float], b: float, tol: float = 1e-9) -> bool:
    return a is not None and abs(float(a) - b) <= tol


# ===========================================================================
# 1. entry — signal → entry → protect full cycle
# ===========================================================================

def scn_entry_full_cycle(env: ReplayEnv) -> None:
    t = env.truth
    t.mark["BTC"] = 50_000.0
    t.tick_size["BTC"] = 0.5
    plan = EntryPlan(coin="BTC", size=0.1, sl_px=48_500.3,
                     limit_px=50_250.0, entry_bar_ts=1_751_558_400_000)

    # op-hook: invariant §7.1 — no order ever sent without a durable PENDING
    # row (t_mark_sent commit PRECEDES dispatch)
    def hook(op: str, ctx: Dict[str, Any]) -> None:
        if op != "market_open":
            return
        row = env.journal.conn.execute(
            "SELECT * FROM trades WHERE client_order_id=?",
            (plan.client_order_id,)).fetchone()
        assert row is not None, "market_open dispatched with NO trades row"
        assert row["sm_state"] == "PENDING", (
            "market_open dispatched from sm_state=%s (must be PENDING)"
            % row["sm_state"])
        assert row["order_sent_at"], "PENDING row without order_sent_at"
    env.on_op(hook)

    out = env.engine.entry(plan)
    env.check(out.final_state == "OPEN",
              "entry did not reach OPEN: %s (%s)" % (out.final_state,
                                                     out.detail))
    tid = out.trade_id
    env.check(env.journal.transitions(tid) == [
        ("-", "INTENT"), ("INTENT", "PENDING"), ("PENDING", "FILLED"),
        ("FILLED", "PROTECTED"), ("PROTECTED", "OPEN")],
        "SM path wrong: %s" % env.journal.transitions(tid))
    row = env.journal.row(tid)
    # invariant §7.2: entry == venue readback fill px, never intended
    env.check(_approx(row["entry"], 50_010.0),
              "entry px must be the REAL fill (50010.0 = mark+slip), got %s"
              % row["entry"])
    # sl_placed_px = venue-ACCEPTED px (post tick rounding), not the target
    env.check(_approx(row["sl_placed_px"], 48_500.5),
              "sl_placed_px must be venue-ACCEPTED (48500.5 after 0.5-tick "
              "rounding of 48500.3), got %s" % row["sl_placed_px"])
    env.check(_approx(row["sl_current"], 48_500.5),
              "invariant sl_current == sl_placed_px violated: %s"
              % row["sl_current"])
    env.check(row["sl_order_id"] is not None and
              env.journal.registry_has(row["sl_order_id"]),
              "SL oid missing from placed_trigger_oids registry (F5)")
    # place order: market_open strictly before trigger_sl
    env.check_ops_in_order(["market_open", "trigger_sl"], coin="BTC",
                           label="entry ladder order")
    env.check(len(t.triggers_for("BTC")) == 1,
              "exactly one resting SL expected after entry")
    # t_intent idempotency (entry-SM §3: INSERT OR IGNORE on client_order_id)
    env.check(env.journal.insert_intent(plan) == tid,
              "t_intent replay must return the SAME trade_id (idempotent)")


# ===========================================================================
# 2. exit — SL / trail_sl fire attribution (oid-first, historic corpus)
# ===========================================================================

def scn_exit_sl_fire_attribution(env: ReplayEnv) -> None:
    j = env.journal
    # r1: TRAILED stop fired — fill oid == DB-current sl_order_id
    r1 = j.insert_row(coin="ETH", sm_state="OPEN", entry=3000.0, size=1.0,
                      orig_size=1.0, sl_initial=2900.0, sl_current=2950.0,
                      sl_order_id="SL-CUR")
    j.registry_insert("SL-CUR", "ETH", "sl", r1)
    env.truth.add_fill("ETH", 2949.2, 1.0, is_buy=False, oid="SL-CUR")
    # r2: UNTRAILED stop fired via a K12-superseded HISTORIC oid (∉ DB-current)
    # — attribution must match the FULL historic bot-oid corpus (F4)
    r2 = j.insert_row(coin="SOL", sm_state="OPEN", entry=100.0, size=10.0,
                      orig_size=10.0, sl_initial=95.0, sl_current=95.0,
                      sl_order_id="SL-B-CUR")
    j.registry_insert("SL-B-CUR", "SOL", "sl", r2)
    j.registry_insert("SL-B-OLD", "SOL", "sl", r2)   # superseded, fired
    env.truth.add_fill("SOL", 94.9, 10.0, is_buy=False, oid="SL-B-OLD")
    # positions absent (the stop DID fire) — no truth positions at all

    for tick in (1, 2):
        env.check_intents(env.engine.tick_row(r1),
                          [("Defer", "phantom_miss_n<K")],
                          "r1 tick%d" % tick)
        env.check_intents(env.engine.tick_row(r2),
                          [("Defer", "phantom_miss_n<K")],
                          "r2 tick%d" % tick)
    # K=3: resolve with EXACT attribution in the executed intent
    env.check_intents(env.engine.tick_row(r1), [("AdoptResolve", "trail_sl")],
                      "r1 K3 resolve (trailed: sl_current != sl_initial)")
    env.check_intents(env.engine.tick_row(r2), [("AdoptResolve", "sl")],
                      "r2 K3 resolve (untrailed + historic-corpus oid)")
    row1, row2 = j.row(r1), j.row(r2)
    env.check(row1["sm_state"] == "CLOSED" and
              row1["exit_reason"] == "trail_sl" and
              _approx(row1["exit_px"], 2949.2),
              "r1 must close trail_sl @ REAL fill 2949.2 (never the 2950 "
              "reference), got %s @ %s"
              % (row1["exit_reason"], row1["exit_px"]))
    env.check(row2["sm_state"] == "CLOSED" and row2["exit_reason"] == "sl" and
              _approx(row2["exit_px"], 94.9),
              "r2 must close sl @ 94.9 via historic oid SL-B-OLD, got %s @ %s"
              % (row2["exit_reason"], row2["exit_px"]))
    # SL-fire resolve is a ROW close: the venue already flattened us
    env.check_no_write_ops("ETH", "sl-fire resolve must not write venue")
    env.check_no_write_ops("SOL", "sl-fire resolve must not write venue")


# ===========================================================================
# 3. trail re-place (place-before-cancel + 5bps churn gate + raise-only)
# ===========================================================================

def scn_trail_replace(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    t.mark["SOL"] = 110.0
    t.tick_size["SOL"] = 0.01
    t.set_position("SOL", 10.0, 100.0)
    old_oid = t.add_trigger("SOL", 100.0, 10.0, is_buy_to_close=False,
                            oid="SL-OLD")
    tid = j.insert_row(coin="SOL", sm_state="OPEN", entry=100.0, size=10.0,
                       orig_size=10.0, sl_initial=98.0, sl_current=100.0,
                       sl_order_id=old_oid, sl_placed_px=100.0)
    j.registry_insert(old_oid, "SOL", "sl", tid)

    # tick 1: scripted trail target 102.0 (>5bps) -> ReplaceSL
    got = env.engine.tick_row(tid, ScriptedStrategy(new_sl=lambda r: 102.0))
    env.check_intents(got, [("ReplaceSL", "trail")], "trail tick1")
    env.check_ops_in_order(["trigger_sl", "cancel_sl_order"], coin="SOL",
                           label="place-BEFORE-cancel (exit-engine §7)")
    row = j.row(tid)
    env.check(row["sl_order_id"] != old_oid,
              "sl_order_id must rotate to the NEW oid")
    env.check(_approx(row["sl_placed_px"], 102.0) and
              _approx(row["sl_current"], 102.0),
              "persisted px must be venue-ACCEPTED 102.0, got placed=%s "
              "cur=%s" % (row["sl_placed_px"], row["sl_current"]))
    live = t.triggers_for("SOL")
    env.check(len(live) == 1 and live[0].oid == row["sl_order_id"],
              "exactly ONE own SL must rest after the replace, got %s"
              % [x.oid for x in live])
    env.check(j.registry_has(row["sl_order_id"]),
              "new oid must be in placed_trigger_oids (step 3a)")

    # tick 2: target within 5bps of sl_placed_px -> churn-gated NoOp
    writes_before = len([1 for c in env.client.gate.calls
                         if c["op"] == "trigger_sl"])
    got = env.check_intents(
        env.engine.tick_row(tid, ScriptedStrategy(new_sl=lambda r: 102.03)),
        [("NoOp", "churn_gate_5bps")], "trail tick2 churn gate")
    writes_after = len([1 for c in env.client.gate.calls
                        if c["op"] == "trigger_sl"])
    env.check(writes_before == writes_after,
              "churn-gated tick must place NOTHING")
    # tick 3: lower target -> raise-only NoOp
    env.check_intents(
        env.engine.tick_row(tid, ScriptedStrategy(new_sl=lambda r: 101.0)),
        [("NoOp", "trail_raise_only")], "trail tick3 raise-only")


# ===========================================================================
# 4. TP1 partial detected → BE replace (place-before-cancel)
# ===========================================================================

def scn_tp1_partial_be(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    t.mark["LTC"] = 210.0
    t.tick_size["LTC"] = 0.01
    t.set_position("LTC", 0.8, 200.0)   # venue already shows the TP1 partial
    sl_oid = t.add_trigger("LTC", 190.0, 2.0, is_buy_to_close=False,
                           oid="SL-L")
    tid = j.insert_row(coin="LTC", sm_state="OPEN", entry=200.0, size=2.0,
                       orig_size=2.0, sl_initial=190.0, sl_current=190.0,
                       sl_order_id=sl_oid, sl_placed_px=190.0,
                       tp1_frac_at_entry=0.6)
    j.registry_insert(sl_oid, "LTC", "sl", tid)

    # EF3: BE is ENGINE math — entry×(1−TRAIL_AFTER_TP_BUFFER_PCT) raise-only
    # (live ext trader.py:826-830 / canonical strategy_xnn.py:654-660), buffer
    # SEEDED from the live .env (ext config = 0.003) through the venue-quirks
    # seam. Raw entry (200.0) must NEVER be the placed BE when buffer > 0.
    env.engine.be_buffer_pct = 0.003
    be_expected = 200.0 * (1.0 - 0.003)          # 199.4
    got = env.engine.tick_row(tid, ScriptedStrategy())
    env.check_intents(got, [("MarkTP1Partial", "executed")], "tp1 tick")
    env.check_ops_in_order(["trigger_sl", "cancel_sl_order"], coin="LTC",
                           label="BE replace place-BEFORE-cancel")
    row = j.row(tid)
    env.check(row["tp1_partial_done"] == 1, "tp1_partial_done must persist")
    env.check(_approx(row["size"], 0.8),
              "remainder size must persist (0.8), got %s" % row["size"])
    env.check(_approx(row["sl_placed_px"], be_expected, tol=1e-6),
              "SL must sit at BUFFERED BE entry×(1−buf)=199.4 — got %s"
              % row["sl_placed_px"])
    env.check(abs(float(row["sl_placed_px"]) - 200.0) > 1e-6,
              "raw entry (200.0) must NEVER be the BE target when buffer>0 "
              "(EF3), got %s" % row["sl_placed_px"])
    live = t.triggers_for("LTC")
    env.check(len(live) == 1 and _approx(live[0].trigger_px, be_expected,
                                         tol=1e-6) and
              _approx(live[0].size, 0.8),
              "one resting BE SL sized to the REMAINDER expected, got %s"
              % [(x.oid, x.trigger_px, x.size) for x in live])


# ===========================================================================
# 5. abort-unwind — accept_partial_land(ratio < min_fill) → ABORTING → flat
#    (§7.5 verbatim composition of the P2 fault bank)
# ===========================================================================

def scn_abort_unwind(env: ReplayEnv) -> None:
    t = env.truth
    t.mark["DOGE"] = 0.10
    t.tick_size["DOGE"] = 0.00001
    plan = EntryPlan(coin="DOGE", size=100.0, sl_px=0.09, limit_px=0.1006)
    env.add_fault(kind="accept_partial_land", op="market_open",
                  partial_ratio=0.05)   # 5% < MIN_FILL_RATIO 10%

    # F3 op-hook: at the instant ensure_flat is dispatched the abort intent
    # AND the entry cooldown must already be DURABLE (t_abort_begin law)
    def hook(op: str, ctx: Dict[str, Any]) -> None:
        if op != "ensure_flat":
            return
        body = ctx.get("body") or {}
        if isinstance(body, str):
            body = json.loads(body)
        if body.get("coin") != "DOGE":
            return
        row = env.journal.conn.execute(
            "SELECT * FROM trades WHERE client_order_id=?",
            (plan.client_order_id,)).fetchone()
        assert row is not None and row["sm_state"] == "ABORTING", (
            "ensure_flat dispatched with row NOT in ABORTING (F3): %s"
            % (row["sm_state"] if row else None))
        assert row["abort_reason"], "ABORTING row without durable abort_reason"
        assert env.journal.cooldown_get("DOGE") is not None, (
            "entry cooldown must be durable BEFORE unwind I/O (K9 fix)")
    env.on_op(hook)

    out = env.engine.entry(plan)
    env.check(out.final_state == "ABORTED",
              "partial below min_fill must ABORT, got %s" % out.final_state)
    tid = out.trade_id
    env.check(env.journal.transitions(tid) == [
        ("-", "INTENT"), ("INTENT", "PENDING"),
        ("PENDING", "ABORTING"), ("ABORTING", "ABORTED")],
        "abort SM path wrong (a policy-rejected fill must NEVER pass "
        "t_fill): %s" % env.journal.transitions(tid))
    row = env.journal.row(tid)
    env.check(row["abort_reason"] == "entry_fill_below_min_fill_ratio",
              "abort_reason wrong (must be the production "
              "entry_sm.run_entry_policy string): %s" % row["abort_reason"])
    env.check(row["status"] == "aborted",
              "legacy status mapping (§2.3) must be 'aborted'")
    env.check("DOGE" not in env.truth.positions,
              "unwind must leave the venue FLAT")
    cd = env.journal.cooldown_get("DOGE")
    env.check(cd is not None and
              _approx(cd["until_ts"], env.clock.now() + 900.0, tol=1.0),
              "900s entry-abort cooldown must be armed")
    env.check("entry_fill_below_min_fill_ratio" in env.journal.rejected("DOGE"),
              "rejected_signals row must be written on abort (reporting "
              "unchanged)")
    env.check_ops_in_order(["market_open", "ensure_flat"], coin="DOGE",
                           label="abort unwind order")


# ===========================================================================
# 6. SL-liveness heal — ReadUnknown ⇒ assume-live; verified [] ⇒ heal ≤1 tick
# ===========================================================================

def scn_sl_liveness_heal(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    t.mark["AVAX"] = 35.0
    t.tick_size["AVAX"] = 0.01
    t.set_position("AVAX", 5.0, 30.0)
    tid = j.insert_row(coin="AVAX", sm_state="OPEN", entry=30.0, size=5.0,
                       orig_size=5.0, sl_initial=28.0, sl_current=30.0,
                       sl_order_id="SL-A", sl_placed_px=30.0)
    j.registry_insert("SL-A", "AVAX", "sl", tid)
    # SL VANISHED from the venue (truth has no trigger for AVAX)

    # tick A: liveness read fails -> assume-live (anti heal-storm), NO heal
    env.add_fault(kind="timeout", op="list_open_sl_orders", count=1)
    env.check_intents(env.engine.tick_row(tid),
                      [("NoOp", "sl_liveness_unknown_assume_live")],
                      "heal tickA (ReadUnknown must NOT heal)")
    env.check(len(t.triggers_for("AVAX")) == 0 and
              not env.client.write_ops_for_coin("AVAX"),
              "assume-live tick must place NOTHING")

    # tick B: verified [] -> HealSL places + confirms within the tick
    got = env.engine.tick_row(tid)
    env.check(len(got) == 1 and got[0].kind == "HealSL",
              "heal tickB expected HealSL, got %s" % got)
    live = t.triggers_for("AVAX")
    env.check(len(live) == 1 and _approx(live[0].trigger_px, 30.0),
              "healed SL must rest at sl_current 30.0 (one trigger), got %s"
              % [(x.oid, x.trigger_px) for x in live])
    row = j.row(tid)
    env.check(row["sl_order_id"] == live[0].oid and
              _approx(row["sl_placed_px"], 30.0),
              "healed oid/px must persist (never naked >1 tick)")
    env.check(j.registry_has(live[0].oid),
              "healed oid must be registry-inserted (F5)")


# ===========================================================================
# 7. phantom K=3 resolve — strategy row + protect_only + manual_claimed
#    (review nit 2: manual_claimed gets the SAME DB-only row-resolve) +
#    UNKNOWN-never-counts + counter persistence across restart +
#    executor authority-gate refusals (§7.9/§7.12)
# ===========================================================================

def scn_phantom_k3_resolve(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    for coin, mark in (("INJ", 20.0), ("PEPE", 0.001), ("BNB", 600.0)):
        t.mark[coin] = mark
    rS = j.insert_row(coin="INJ", sm_state="OPEN", entry=20.0, size=1.0,
                      orig_size=1.0, sl_initial=18.0, sl_current=18.0)
    rP = j.insert_row(coin="PEPE", sm_state="OPEN", entry=0.001, size=1e6,
                      orig_size=1e6, origin="adopted_untracked",
                      management_class="protect_only",
                      notes="adopted_covered")
    rM = j.insert_row(coin="BNB", sm_state="OPEN", entry=600.0, size=2.0,
                      orig_size=2.0, management_class="manual_claimed",
                      notes="operator_claimed")

    # ---- executor authority gates FIRST (rows still OPEN) — invariants
    # §7.9 (protect_only: no close authority) / §7.12 (manual_claimed: every
    # venue intent refused)
    for tid_, kind, params in ((rP, "Close", {}), (rP, "EscalateNaked", {}),
                               (rM, "Close", {}),
                               (rM, "HealSL", {"target_px": 590.0}),
                               (rM, "ReplaceSL", {"target_px": 590.0})):
        verdict = env.engine.executor_try(tid_, Intent(kind, "injected",
                                                       dict(params)))
        env.check(verdict.reason == "REFUSED",
                  "executor must REFUSE %s for row %s (%s), got %s"
                  % (kind, tid_, j.row(tid_)["management_class"], verdict))
    env.check_no_write_ops("PEPE", "refused intents must write nothing")
    env.check_no_write_ops("BNB", "refused intents must write nothing")

    # ---- manual_claimed while PRESENT: full hands-off NoOp
    t.set_position("BNB", 2.0, 600.0)
    env.check_intents(env.engine.tick_row(rM),
                      [("NoOp", "manual_claimed_hands_off")],
                      "manual_claimed present tick")
    del t.positions["BNB"]  # position vanishes (manual close elsewhere)

    # ---- strategy row: miss1 -> UNKNOWN (no count) -> miss2 -> miss3 resolve
    env.check_intents(env.engine.tick_row(rS),
                      [("Defer", "phantom_miss_n<K")], "rS miss1")
    env.add_fault(kind="timeout", op="open_positions", count=1)
    env.check_intents(env.engine.tick_row(rS),
                      [("Defer", "presence_unknown")],
                      "rS UNKNOWN tick (must NOT count a miss — "
                      "bias-to-protect)")
    # restart simulation: fresh engine over the same journal — the K counter
    # is persisted state (exit-engine §2 phantom_misses -> bot_state kv)
    engine2 = type(env.engine)(env.journal, env.client, env.clock,
                               env.criticals)
    env.check_intents(engine2.tick_row(rS),
                      [("Defer", "phantom_miss_n<K")], "rS miss2 (restart)")
    env.check_intents(engine2.tick_row(rS),
                      [("AdoptResolve", "phantom_no_exchange_position")],
                      "rS K3 resolve")
    env.check(j.row(rS)["sm_state"] == "CLOSED" and
              j.row(rS)["exit_reason"] == "phantom_no_exchange_position",
              "rS must close phantom_no_exchange_position")

    # ---- protect_only row: same K cadence, DB-only row-resolve
    for lbl in ("miss1", "miss2"):
        env.check_intents(env.engine.tick_row(rP),
                          [("Defer", "phantom_miss_n<K")], "rP %s" % lbl)
    env.check_intents(env.engine.tick_row(rP),
                      [("AdoptResolve", "phantom_no_exchange_position")],
                      "rP K3 resolve")
    env.check(j.row(rP)["sm_state"] == "CLOSED",
              "protect_only row must be row-resolved at K3")
    env.check_no_write_ops("PEPE", "protect_only K3 resolve must be DB-only")

    # ---- manual_claimed row: THE SAME DB-only row-resolve (review nit 2)
    for lbl in ("miss1", "miss2"):
        env.check_intents(env.engine.tick_row(rM),
                          [("Defer", "phantom_miss_n<K")], "rM %s" % lbl)
    env.check_intents(env.engine.tick_row(rM),
                      [("AdoptResolve", "phantom_no_exchange_position")],
                      "rM K3 resolve (manual_claimed — nit 2)")
    env.check(j.row(rM)["sm_state"] == "CLOSED",
              "manual_claimed row must get the SAME phantom-K3 row-resolve "
              "as protect_only (review nit 2)")
    env.check_no_write_ops("BNB", "manual_claimed K3 resolve must be DB-only "
                                  "(zero venue writes)")


# ===========================================================================
# 8. adopt-untracked — rule-3 split 3a/3b/3c (entry-SM §4.2.3 R3) + pinned
#    _fenced kwargs (real P1 fence) + protect_only heal at CURRENT mark
# ===========================================================================

def scn_adopt_untracked(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    # anchor row so db_open is non-empty (pinned-kwarg realism)
    t.mark["ETH"] = 3000.0
    j.insert_row(coin="ETH", sm_state="OPEN", entry=3000.0, size=1.0,
                 orig_size=1.0)
    # 3c candidate: non-prefix NAKED
    t.mark["LINK"] = 15.0
    t.tick_size["LINK"] = 0.001
    t.set_position("LINK", 100.0, 14.0)
    # 3a candidate: covered by a USER (non-registry) trigger
    t.mark["UNI"] = 8.0
    t.tick_size["UNI"] = 0.0001
    t.set_position("UNI", 50.0, 7.8)
    t.add_trigger("UNI", 7.5, 50.0, is_buy_to_close=False, oid="USER-1")
    # 3b candidate: prefix-manual NAKED (HL shared-account xyz_ class)
    t.mark["xyz_GOLD"] = 2400.0
    t.set_position("xyz_GOLD", 1.0, 2380.0)

    with fake_bot_config(manual_prefixes=("xyz_",)):
        env.engine.reconcile_untracked()
        env.engine.reconcile_untracked()  # second pass: 3b CRITICAL re-fires

        # ---- 3c: protective SL at CURRENT mark −6%, registry, protect_only
        rc = j.conn.execute("SELECT * FROM trades WHERE coin='LINK'").fetchone()
        env.check(rc is not None and rc["management_class"] == "protect_only"
                  and rc["origin"] == "adopted_untracked"
                  and rc["notes"] == "adopted",
                  "LINK 3c row wrong: %s" % (dict(rc) if rc else None))
        env.check(_approx(rc["sl_placed_px"], 14.1, tol=1e-6),
                  "3c protective SL must anchor CURRENT mark −6%% = 14.1, "
                  "got %s" % rc["sl_placed_px"])
        env.check(rc["sl_order_id"] and j.registry_has(rc["sl_order_id"]),
                  "3c protective oid must be registry-inserted")
        env.check(len(t.triggers_for("LINK")) == 1,
                  "exactly one protective SL must rest for LINK")

        # ---- 3a: COVERED -> track-only, sl_* NULL, ZERO placement
        ra = j.conn.execute("SELECT * FROM trades WHERE coin='UNI'").fetchone()
        env.check(ra is not None and ra["management_class"] == "protect_only"
                  and ra["notes"] == "adopted_covered",
                  "UNI 3a row wrong: %s" % (dict(ra) if ra else None))
        env.check(ra["sl_order_id"] is None and ra["sl_placed_px"] is None
                  and ra["sl_current"] is None,
                  "3a covered adoption must keep sl_* NULL BY DESIGN "
                  "(no engine machinery may anchor to the user's px)")
        env.check_no_write_ops("UNI", "3a covered adoption")
        details = j.transition_details(ra["id"])
        env.check(any(_approx(d.get("covering_trigger_px"), 7.5)
                      for d in details),
                  "cover px must be recorded ONLY in sm_transitions.detail "
                  "(covering_trigger_px — production key)")

        # ---- 3b: prefix-manual NAKED -> CRITICAL every pass, NO SL, 1 row
        rb = j.conn.execute(
            "SELECT * FROM trades WHERE coin='xyz_GOLD'").fetchall()
        env.check(len(rb) == 1 and
                  rb[0]["notes"] == "unreconciled_manual_prefix" and
                  rb[0]["management_class"] == "protect_only",
                  "xyz_GOLD 3b must be ONE track-only row, got %s"
                  % [dict(r) for r in rb])
        env.check_no_write_ops("xyz_GOLD",
                               "3b prefix-manual naked: NO SL EVER (hl "
                               "main.py:901-925 canon)")
        crits = [c for c in env.criticals if "xyz_GOLD" in c]
        env.check(len(crits) >= 2,
                  "3b CRITICAL must re-fire EVERY pass, got %d" % len(crits))

        # ---- executor: no close authority over adopted rows (§7.9)
        env.check(env.engine.executor_try(
            rc["id"], Intent("Close", "injected")).reason == "REFUSED",
            "Close must be REFUSED for the 3c protect_only row")

        # ---- protect_only heal after cover vanished: CURRENT mark ∓6%,
        #      NEVER the user's/remembered px (exit-engine §3 R3/R2-F1)
        t.triggers = [x for x in t.triggers if x.oid != "USER-1"]
        t.mark["UNI"] = 9.0  # user's old px 7.5 is now stale/vacated
        got = env.engine.tick_row(ra["id"])
        env.check(len(got) == 1 and got[0].kind == "HealSL",
                  "cover-vanished tick expected HealSL, got %s" % got)
        live = t.triggers_for("UNI")
        env.check(len(live) == 1 and _approx(live[0].trigger_px, 8.46,
                                             tol=1e-6),
                  "heal must anchor CURRENT mark −6%% = 8.46 — NEVER the "
                  "user's 7.5 — got %s"
                  % [(x.oid, x.trigger_px) for x in live])
        # while covered (before vanish) UNI got zero placements — recheck: the
        # only UNI write op is the heal placement just performed
        uni_writes = env.client.write_ops_for_coin("UNI")
        env.check(uni_writes == ["trigger_sl"],
                  "UNI writes must be exactly the post-vanish heal placement,"
                  " got %s" % uni_writes)


# ===========================================================================
# 9. orphan-trigger cancel — registry-gated + 180s debounce
# ===========================================================================

def scn_orphan_trigger_cancel(env: ReplayEnv) -> None:
    t = env.truth
    t.mark["OP"] = 2.0
    t.mark["ARB"] = 1.0
    bot_oid = t.add_trigger("OP", 1.8, 100.0, is_buy_to_close=False,
                            oid="ORPH-1")
    env.journal.registry_insert(bot_oid, "OP", "sl", None)
    t.add_trigger("ARB", 0.9, 50.0, is_buy_to_close=False, oid="MAN-1")
    # no positions, no live rows for either coin

    # pass 1 (t0): debounce arms, NOTHING cancelled
    env.check(env.engine.orphan_pass() == [],
              "first sighting must only ARM the 180s debounce")
    env.check(len(t.triggers) == 2, "no trigger may be touched pre-debounce")
    # pass 2 inside the debounce window: still nothing
    env.clock.advance(60.0)
    env.check(env.engine.orphan_pass() == [],
              "60s < 180s debounce: still no cancel")
    # pass 3 past the debounce: ONLY the registry-owned oid is cancelled
    env.clock.advance(125.0)
    cancelled = env.engine.orphan_pass()
    env.check(cancelled == [("OP", "ORPH-1")],
              "expected exactly [('OP','ORPH-1')], got %s" % cancelled)
    left = [(x.coin, x.oid) for x in t.triggers]
    env.check(left == [("ARB", "MAN-1")],
              "non-registry (manual bracket) trigger must SURVIVE, got %s"
              % left)
    env.check(env.client.write_ops_for_coin("ARB") == [],
              "zero write ops for the manual-bracket coin")


# ===========================================================================
# 10. supersede sweep — K12 litter replayed from a checkpointed SM db
# ===========================================================================

def scn_supersede_sweep_k12(env: ReplayEnv) -> None:
    t, j = env.truth, env.journal
    t.mark["MATIC"] = 2.2
    t.tick_size["MATIC"] = 0.0001
    t.set_position("MATIC", 20.0, 2.0)
    # CHECKPOINT: kill between replace_sl step 3a (registry INSERT of NEW,
    # confirmed live) and steps 5-6 (supersede-cancel + persist) — DB still
    # points at OLD, TWO registry SLs rest, plus a MANUAL non-registry trigger
    t.add_trigger("MATIC", 1.90, 20.0, is_buy_to_close=False, oid="K12-OLD")
    t.add_trigger("MATIC", 1.95, 20.0, is_buy_to_close=False, oid="K12-NEW")
    t.add_trigger("MATIC", 1.50, 20.0, is_buy_to_close=False, oid="K12-MAN")
    tid = j.insert_row(coin="MATIC", sm_state="OPEN", entry=2.0, size=20.0,
                       orig_size=20.0, sl_initial=1.85, sl_current=1.90,
                       sl_order_id="K12-OLD", sl_placed_px=1.90)
    j.registry_insert("K12-OLD", "MATIC", "sl", tid)
    j.registry_insert("K12-NEW", "MATIC", "sl", tid)

    got = env.engine.tick_row(tid)
    env.check_intents(got, [("SupersedeSweep", "")], "K12 sweep tick")
    left = sorted(x.oid for x in t.triggers_for("MATIC"))
    env.check(left == ["K12-MAN", "K12-OLD"],
              "sweep must cancel ONLY the own non-DB-current oid (K12-NEW); "
              "DB-current survives, manual untouched — got %s" % left)
    row = j.row(tid)
    env.check(row["sl_order_id"] == "K12-OLD" and
              _approx(row["sl_placed_px"], 1.90),
              "DB-current oid/px must be unchanged by the sweep")
    cancels = [c for c in env.client.gate.calls
               if c["op"] == "cancel_sl_order"]
    oids = []
    for c in cancels:
        b = c.get("body") or {}
        if isinstance(b, str):
            b = json.loads(b)
        oids.append(b.get("oid"))
    env.check(oids == ["K12-NEW"],
              "exactly one cancel, targeting K12-NEW, got %s" % oids)
    # convergence: next tick is quiet
    env.check_intents(env.engine.tick_row(tid), [("NoOp", "nothing_due")],
                      "post-sweep tick must be NoOp (converged ≤1 tick)")


# ===========================================================================
# registry
# ===========================================================================

SCENARIOS: List[Scenario] = [
    Scenario("entry_full_cycle",
             "entry (signal→entry→protect full cycle)", scn_entry_full_cycle),
    Scenario("exit_sl_fire_attribution",
             "exit — SL/trail_sl fire attribution",
             scn_exit_sl_fire_attribution),
    Scenario("trail_replace", "trail re-place", scn_trail_replace),
    Scenario("tp1_partial_be", "TP1 partial → BE replace", scn_tp1_partial_be),
    Scenario("abort_unwind",
             "abort-unwind (cap-breach/partial → ABORTING → flat)",
             scn_abort_unwind),
    Scenario("sl_liveness_heal", "SL-liveness heal", scn_sl_liveness_heal),
    Scenario("phantom_k3_resolve",
             "phantom K=3 resolve (strategy + protect_only + manual_claimed "
             "DB-only)", scn_phantom_k3_resolve),
    Scenario("adopt_untracked_protect_only",
             "adopt-untracked (protect-only path)", scn_adopt_untracked),
    Scenario("orphan_trigger_cancel",
             "orphan-trigger cancel (registry-gated)",
             scn_orphan_trigger_cancel),
    Scenario("supersede_sweep_k12", "supersede sweep (K12 litter)",
             scn_supersede_sweep_k12),
]
