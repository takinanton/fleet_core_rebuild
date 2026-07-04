#!/usr/bin/env python3
"""Decision-table parity harness for orphan_sweep.py (old venue file vs fleet_core canonical).

Run ON EACH BOT HOST inside its venv (stdlib-only, no network, no live exchange, no
writes outside tmp):

    python orphan_sweep_parity.py --old bot/orphan_sweep.py \
        --new fleet_core/orphan_sweep.py --venue {hl|pacifica|extended|nado}

Both files are loaded under synthetic module names with FAKE bot.config / bot.universe /
bot.journal / bot.exchange_nado pre-injected into sys.modules, configured with EXACTLY
the attributes that venue's real modules define (values = the venue's config.py defaults,
plus live-domain variants for env-driven knobs). A matrix of orphan-order scenarios is
fed to both modules; the harness asserts identical cancel/keep/heal decisions, identical
seen_state mutations, identical client call sequences, identical sweep-log records
(level+text) and identical module-logger (fence) log LEVELS.

Known waived divergence (documented in proofs/orphan_sweep_dossier.md): on HL the old
file's FX_EXCLUDE / UNIVERSE_SYMBOL_EXCLUDE / _is_fx fence arms are fail-OPEN
(`except Exception: pass`) while the canonical is fail-CLOSED (CRITICAL + fenced).
That branch is unreachable live (fences are literal frozensets of str in an
already-imported module), so the synthetic broken-fence scenario on HL is asserted to
show exactly old=fail-open/new=fail-closed and reported WAIVED, not FAIL.
"""

import argparse
import logging
import sys
import types

FIXED_NOW = 1_000_000.0


# --------------------------------------------------------------------------- fakes
class _TimeShim:
    def __init__(self, t=FIXED_NOW):
        self.t = t

    def time(self):
        return self.t


class LogRecorder:
    """Mimics a logging.Logger; records (level, rendered_text)."""

    def __init__(self):
        self.records = []

    def _rec(self, level, msg, *args):
        try:
            text = msg % args if args else msg
        except Exception:
            text = repr((msg, args))
        self.records.append((level, text))

    def debug(self, msg, *a, **k):
        self._rec("DEBUG", msg, *a)

    def info(self, msg, *a, **k):
        self._rec("INFO", msg, *a)

    def warning(self, msg, *a, **k):
        self._rec("WARNING", msg, *a)

    def error(self, msg, *a, **k):
        self._rec("ERROR", msg, *a)

    def critical(self, msg, *a, **k):
        self._rec("CRITICAL", msg, *a)

    def levels(self):
        return [lv for lv, _ in self.records]


class FakeClient:
    """Records every adapter call. NO network."""

    def __init__(self, triggers=None, positions=None, enum_exc=None,
                 pos_exc=None, cancel_exc=None, cancel_effective=True,
                 enum_exc_seq=None):
        self.triggers = list(triggers or [])
        self.positions = dict(positions or {})
        self.enum_exc = enum_exc
        self.enum_exc_seq = list(enum_exc_seq) if enum_exc_seq else None
        self.pos_exc = pos_exc
        self.cancel_exc = cancel_exc
        self.cancel_effective = cancel_effective
        self.calls = []

    def list_reduce_only_triggers(self):
        self.calls.append(("list_reduce_only_triggers",))
        if self.enum_exc_seq is not None:
            exc = self.enum_exc_seq.pop(0) if self.enum_exc_seq else None
            if exc is not None:
                raise exc
        elif self.enum_exc is not None:
            raise self.enum_exc
        return [dict(t) for t in self.triggers]

    def open_positions(self):
        self.calls.append(("open_positions",))
        if self.pos_exc is not None:
            raise self.pos_exc
        return dict(self.positions)

    def cancel_sl_order(self, coin, oid):
        self.calls.append(("cancel_sl_order", coin, oid))
        if self.cancel_exc is not None:
            raise self.cancel_exc
        if self.cancel_effective:
            self.triggers = [t for t in self.triggers if str(t.get("oid")) != str(oid)]
        return {"ok": True}


def _mk_module(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


def _nado_is_fx(sym):
    base = sym.replace("-PERP", "").replace("/", "").upper()
    _CCY = {"EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "SEK", "NOK",
            "DKK", "HKD", "SGD", "MXN", "ZAR", "TRY", "PLN", "HUF"}
    if len(base) == 6:
        left, right = base[:3], base[3:]
        if left in _CCY or right in _CCY:
            return True
    return False


class TriggerClientUnavailableFake(Exception):
    pass


# Per-venue fence-attribute presence + venue-default values (verified against each
# venue's bot/config.py + bot/universe.py + bot/journal.py, 2026-07-02):
#   fence symbol                       hl        pacifica   extended   nado
#   config.FX_EXCLUDE                  yes       yes        yes        yes
#   universe.UNIVERSE_SYMBOL_EXCLUDE   yes       NO         yes        yes
#   universe._is_fx                    NO        NO         NO         yes
#   config.FOREIGN_SKIP_PREFIXES       yes       yes        yes        NO
#   config.MANUAL_POSITION_PREFIXES    yes       NO         NO         NO
#   exchange_nado.TriggerClientUnavailable  NO   NO         NO         yes
#   journal coins_ever_traded/oids_ever_placed  yes  NO     NO         NO
HL_FX = frozenset({"EUR-USD", "GBP-USD", "JPY-USD", "CHF-USD", "EURUSD", "GBPUSD",
                   "USDJPY", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"})
PAC_FX = frozenset({"NEAR", "VIRTUAL", "BNB", "EUR-USD", "GBP-USD", "JPY-USD",
                    "CHF-USD", "EURUSD", "GBPUSD", "USDJPY",
                    "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"})
EXT_FX = frozenset({"BNB", "EUR-USD", "GBP-USD", "JPY-USD", "CHF-USD", "EURUSD",
                    "GBPUSD", "USDJPY", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"})
NADO_FX = frozenset({"EUR-PERP", "GBP-PERP", "JPY-PERP", "CHF-PERP", "EURUSD-PERP",
                     "GBPUSD-PERP", "USDJPY-PERP", "EURGBP-PERP", "EURJPY-PERP",
                     "AUDUSD-PERP", "NZDUSD-PERP", "USDCAD-PERP", "USDCHF-PERP"})

VENUES = {
    "hl": dict(fx=HL_FX, universe_exclude=frozenset(), has_universe_exclude=True,
               has_is_fx=False, foreign_skip=(), has_foreign_skip=True,
               manual_prefixes=("xyz_",), has_manual=True, has_exchange_nado=False,
               journal_history=True),
    "pacifica": dict(fx=PAC_FX, universe_exclude=None, has_universe_exclude=False,
                     has_is_fx=False, foreign_skip=(), has_foreign_skip=True,
                     manual_prefixes=None, has_manual=False, has_exchange_nado=False,
                     journal_history=False),
    "extended": dict(fx=EXT_FX, universe_exclude=frozenset(), has_universe_exclude=True,
                     has_is_fx=False, foreign_skip=(), has_foreign_skip=True,
                     manual_prefixes=None, has_manual=False, has_exchange_nado=False,
                     journal_history=False),
    "nado": dict(fx=NADO_FX, universe_exclude=frozenset({"BNB-PERP"}),
                 has_universe_exclude=True, has_is_fx=True, foreign_skip=None,
                 has_foreign_skip=False, manual_prefixes=None, has_manual=False,
                 has_exchange_nado=True, journal_history=False),
}


class FakeWorld:
    """Owns the injected fake bot.* modules; scenario code mutates attrs between runs."""

    def __init__(self, venue):
        self.venue = venue
        self.spec = VENUES[venue]
        self.bot = _mk_module("bot")
        self.bot.__path__ = []          # empty package: bot.<anything-not-faked> -> ImportError
        self.config = _mk_module("bot.config")
        self.universe = _mk_module("bot.universe")
        self.journal = _mk_module("bot.journal")
        self.exchange_nado = (_mk_module("bot.exchange_nado",
                                         TriggerClientUnavailable=TriggerClientUnavailableFake)
                              if self.spec["has_exchange_nado"] else None)
        self.reset()

    def reset(self):
        s = self.spec
        for a in ("FX_EXCLUDE", "FOREIGN_SKIP_PREFIXES", "MANUAL_POSITION_PREFIXES"):
            if hasattr(self.config, a):
                delattr(self.config, a)
        for a in ("UNIVERSE_SYMBOL_EXCLUDE", "_is_fx"):
            if hasattr(self.universe, a):
                delattr(self.universe, a)
        self.config.FX_EXCLUDE = s["fx"]
        if s["has_foreign_skip"]:
            self.config.FOREIGN_SKIP_PREFIXES = s["foreign_skip"]
        if s["has_manual"]:
            self.config.MANUAL_POSITION_PREFIXES = s["manual_prefixes"]
        if s["has_universe_exclude"]:
            self.universe.UNIVERSE_SYMBOL_EXCLUDE = s["universe_exclude"]
        if s["has_is_fx"]:
            self.universe._is_fx = _nado_is_fx
        # journal defaults: empty book
        self.set_journal(open_rows=[], ever=set(), placed=set())

    def set_journal(self, open_rows, ever=None, placed=None, open_exc=None,
                    ever_exc=None):
        def open_trades(*a, **k):
            if open_exc is not None:
                raise open_exc
            return [dict(coin=c) for c in open_rows]
        self.journal.open_trades = open_trades
        if self.spec["journal_history"]:
            def coins_ever_traded(*a, **k):
                if ever_exc is not None:
                    raise ever_exc
                return set(ever or set())
            def oids_ever_placed(*a, **k):
                return {str(x) for x in (placed or set())}
            self.journal.coins_ever_traded = coins_ever_traded
            self.journal.oids_ever_placed = oids_ever_placed
        else:
            for a in ("coins_ever_traded", "oids_ever_placed"):
                if hasattr(self.journal, a):
                    delattr(self.journal, a)

    def install(self):
        sys.modules["bot"] = self.bot
        sys.modules["bot.config"] = self.config
        sys.modules["bot.universe"] = self.universe
        sys.modules["bot.journal"] = self.journal
        sys.modules.pop("bot.exchange_nado", None)
        if self.exchange_nado is not None:
            sys.modules["bot.exchange_nado"] = self.exchange_nado


def load_module(path, synth_name, world):
    world.install()
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    mod = types.ModuleType(synth_name)
    mod.__file__ = path
    sys.modules[synth_name] = mod
    exec(compile(src, path, "exec"), mod.__dict__)
    # Freeze time + capture the module-level fence logger (name differs per variant).
    mod.__dict__["time"] = _TimeShim()
    mod_logger = LogRecorder()
    replaced = 0
    for cand in ("_log", "log", "_mlog"):
        obj = mod.__dict__.get(cand)
        if isinstance(obj, logging.Logger):
            mod.__dict__[cand] = mod_logger
            replaced += 1
    if replaced != 1:
        print(f"FATAL: expected exactly 1 module-level logger in {path}, found {replaced}")
        sys.exit(1)
    mod.__parity_fence_log__ = mod_logger
    return mod


# --------------------------------------------------------------------------- runner
def run_sweep(mod, client, open_positions, seen_state):
    log = LogRecorder()
    mod.__parity_fence_log__.records.clear()
    result = mod.sweep_orphan_triggers(client, open_positions, seen_state, log)
    return dict(
        result=result,
        calls=list(client.calls),
        seen=dict(seen_state),
        sweep_log=list(log.records),
        fence_levels=mod.__parity_fence_log__.levels(),
        streak=getattr(client, "_orphan_enum_fail_streak", None),
    )


def compare(name, old_snap, new_snap, failures, notes=""):
    if old_snap == new_snap:
        print(f"PASS {name}{(' — ' + notes) if notes else ''}")
        return True
    print(f"FAIL {name}")
    for k in old_snap:
        if old_snap[k] != new_snap[k]:
            print(f"  field {k}:\n    old={old_snap[k]}\n    new={new_snap[k]}")
    failures.append(name)
    return False


def trig(coin, oid, is_trigger=True, reduce_only=True):
    return {"coin": coin, "oid": oid, "is_trigger": is_trigger, "reduce_only": reduce_only}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--venue", required=True, choices=sorted(VENUES))
    args = ap.parse_args()

    venue = args.venue
    world = FakeWorld(venue)
    old = load_module(args.old, "parity_old_orphan_sweep", world)
    new = load_module(args.new, "parity_new_orphan_sweep", world)
    failures, waived = [], []

    SUSTAINED = {}  # helper: pre-seeded seen_state 300s ago (past 180s debounce)

    def seeded(coin, oid):
        return {(str(coin).upper(), str(oid)): FIXED_NOW - 300.0}

    def both(name, mk_client, open_pos=None, seen=None, setup=None, notes=""):
        """Run a scenario against old and new with independent state; compare."""
        snaps = []
        for mod in (old, new):
            world.reset()
            if setup:
                setup(world)
            client = mk_client()
            snaps.append(run_sweep(mod, client, dict(open_pos or {}),
                                   dict(seen or {})))
        return compare(name, snaps[0], snaps[1], failures, notes)

    C_ORPH = "DOGE"      # neutral coin, unfenced on every venue
    C_ORPH2 = "SOL"

    # ---- S1 own SL, DB-open row -> keep
    both("S01_own_sl_db_open_row",
         lambda: FakeClient(triggers=[trig(C_ORPH, "11")], positions={C_ORPH: {}}),
         setup=lambda w: w.set_journal(open_rows=[C_ORPH]))

    # ---- S2 orphan first sight -> defer INFO
    both("S02_orphan_first_sight_defer",
         lambda: FakeClient(triggers=[trig(C_ORPH, "12")]))

    # ---- S3 orphan sustained -> cancel + confirm
    both("S03_orphan_sustained_cancel",
         lambda: FakeClient(triggers=[trig(C_ORPH, "13")]),
         seen=seeded(C_ORPH, "13"))

    # ---- S4 cancel returns but trigger still resting -> ERROR, not cancelled
    both("S04_cancel_still_resting",
         lambda: FakeClient(triggers=[trig(C_ORPH, "14")], cancel_effective=False),
         seen=seeded(C_ORPH, "14"))

    # ---- S5 cancel_sl_order raises -> error + continue
    both("S05_cancel_raises",
         lambda: FakeClient(triggers=[trig(C_ORPH, "15")],
                            cancel_exc=RuntimeError("boom")),
         seen=seeded(C_ORPH, "15"))

    # ---- S6 FX_EXCLUDE fenced coin (sustained) -> keep forever
    fx_coin = {"hl": "EUR", "pacifica": "BNB", "extended": "BNB", "nado": "EUR-PERP"}[venue]
    both("S06_fenced_fx_exclude",
         lambda: FakeClient(triggers=[trig(fx_coin, "16")]),
         seen=seeded(fx_coin, "16"))

    # ---- S7 UNIVERSE_SYMBOL_EXCLUDE fenced (live nado BNB-PERP case; env-populated on hl/ext)
    if VENUES[venue]["has_universe_exclude"]:
        ue_coin = "BNB-PERP" if venue == "nado" else "PAXG"
        def _setup_ue(w, c=ue_coin):
            w.universe.UNIVERSE_SYMBOL_EXCLUDE = frozenset({c}) | VENUES[venue]["universe_exclude"]
        both("S07_fenced_universe_symbol_exclude",
             lambda: FakeClient(triggers=[trig(ue_coin, "17")]),
             seen=seeded(ue_coin, "17"), setup=_setup_ue)

    # ---- S8 _is_fx heuristic (nado only)
    if VENUES[venue]["has_is_fx"]:
        both("S08_fenced_is_fx_heuristic",
             lambda: FakeClient(triggers=[trig("EURSEK-PERP", "18")]),
             seen=seeded("EURSEK-PERP", "18"))

    # ---- S9 FOREIGN_SKIP_PREFIXES populated variant
    if VENUES[venue]["has_foreign_skip"]:
        def _setup_fs(w):
            w.config.FOREIGN_SKIP_PREFIXES = ("KAV",)
        both("S09_fenced_foreign_skip_prefix",
             lambda: FakeClient(triggers=[trig("KAVA", "19")]),
             seen=seeded("KAVA", "19"), setup=_setup_fs)

    # ---- S10 HL MANUAL_POSITION_PREFIXES per-oid discrimination
    if VENUES[venue]["has_manual"]:
        both("S10a_manual_prefix_bot_placed_oid_sweepable",
             lambda: FakeClient(triggers=[trig("xyz_GOLD", "777")]),
             seen=seeded("xyz_GOLD", "777"),
             setup=lambda w: w.set_journal(open_rows=[], ever={"xyz_GOLD"},
                                           placed={"777"}))
        both("S10b_manual_prefix_foreign_oid_fenced",
             lambda: FakeClient(triggers=[trig("xyz_GOLD", "888")]),
             seen=seeded("xyz_GOLD", "888"),
             setup=lambda w: w.set_journal(open_rows=[], ever={"xyz_GOLD"},
                                           placed={"777"}))
        both("S10c_manual_prefix_empty_tuple_inactive",
             lambda: FakeClient(triggers=[trig("xyz_GOLD", "999")]),
             seen=seeded("xyz_GOLD", "999"),
             setup=lambda w: (setattr(w.config, "MANUAL_POSITION_PREFIXES", ()),
                              w.set_journal(open_rows=[], ever=set(), placed=set())))

    # ---- S11 broken fence: FX_EXCLUDE = non-iterable
    def _setup_broken_fx(w):
        w.config.FX_EXCLUDE = 42
    if venue == "hl":
        # Documented waived divergence: old HL is fail-open here, canonical fail-closed.
        world.reset(); _setup_broken_fx(world)
        c1 = FakeClient(triggers=[trig(C_ORPH2, "21")])
        o = run_sweep(old, c1, {}, seeded(C_ORPH2, "21"))
        world.reset(); _setup_broken_fx(world)
        c2 = FakeClient(triggers=[trig(C_ORPH2, "21")])
        n = run_sweep(new, c2, {}, seeded(C_ORPH2, "21"))
        old_cancels = o["result"] == [(C_ORPH2, "21")]
        new_keeps = n["result"] == [] and "CRITICAL" in n["fence_levels"]
        if old_cancels and new_keeps:
            print("WAIVED S11_broken_fx_fence(hl) — old fail-OPEN cancels, new fail-CLOSED "
                  "keeps+CRITICAL (branch unreachable live: FX_EXCLUDE is a literal "
                  "frozenset[str] in already-imported bot.config; see dossier)")
            waived.append("S11_broken_fx_fence")
        else:
            print(f"FAIL S11_broken_fx_fence(hl) — unexpected shapes old={o['result']} "
                  f"new={n['result']} fence_levels new={n['fence_levels']}")
            failures.append("S11_broken_fx_fence")
    else:
        both("S11_broken_fx_fence_fail_closed",
             lambda: FakeClient(triggers=[trig(C_ORPH2, "21")]),
             seen=seeded(C_ORPH2, "21"), setup=_setup_broken_fx,
             notes="both fail-closed CRITICAL (texts differ by design; levels compared)")

    # ---- S12 enumeration failure (generic)
    both("S12_enum_generic_failure_skip",
         lambda: FakeClient(enum_exc=RuntimeError("api down")))
    if venue == "nado":
        # streak escalation WARNING->ERROR at 10 consecutive failures
        def run_streak(mod):
            world.reset()
            client = FakeClient(enum_exc=RuntimeError("api down"))
            logs = []
            for _ in range(10):
                snap = run_sweep(mod, client, {}, {})
                logs.append((snap["sweep_log"], snap["streak"]))
            return logs
        lo, ln = run_streak(old), run_streak(new)
        compare("S12b_enum_streak_escalation_10", {"seq": lo}, {"seq": ln}, failures)

    # ---- S13 nado structural TriggerClientUnavailable, 3 consecutive
    if VENUES[venue]["has_exchange_nado"]:
        def run_struct(mod):
            world.reset()
            client = FakeClient(enum_exc=TriggerClientUnavailableFake("trigger client None"))
            logs = []
            for _ in range(3):
                snap = run_sweep(mod, client, {}, {})
                logs.append((snap["sweep_log"], snap["streak"]))
            return logs
        compare("S13_structural_unavailable_escalation",
                {"seq": run_struct(old)}, {"seq": run_struct(new)}, failures)

    # ---- S14 empty triggers -> seen_state cleared
    both("S14_empty_triggers_clears_seen",
         lambda: FakeClient(triggers=[]),
         seen=seeded(C_ORPH, "99"))

    # ---- S15 indeterminate: pos empty but DB-open rows
    both("S15_indeterminate_pos_empty_db_open",
         lambda: FakeClient(triggers=[trig(C_ORPH, "31")], positions={}),
         setup=lambda w: w.set_journal(open_rows=["ETH"]))

    # ---- S16 open_positions raises
    both("S16_open_positions_raises_skip",
         lambda: FakeClient(triggers=[trig(C_ORPH, "32")],
                            pos_exc=RuntimeError("pos api down")))

    # ---- S17 journal read raises
    both("S17_journal_read_raises_skip",
         lambda: FakeClient(triggers=[trig(C_ORPH, "33")]),
         setup=lambda w: w.set_journal(open_rows=[], open_exc=RuntimeError("db locked")))
    if VENUES[venue]["journal_history"]:
        both("S17b_journal_history_raises_skip",
             lambda: FakeClient(triggers=[trig(C_ORPH, "34")]),
             setup=lambda w: w.set_journal(open_rows=[], ever_exc=RuntimeError("db locked")))

    # ---- S18 filter guards: non-reduce-only / non-trigger / None coin / None oid
    both("S18_filter_guards",
         lambda: FakeClient(triggers=[
             trig(C_ORPH, "41", reduce_only=False),
             trig(C_ORPH, "42", is_trigger=False),
             trig(None, "43"),
             trig(C_ORPH, None),
         ]))

    # ---- S19 live position under name variant -> keep
    both("S19_variant_position_match_keep",
         lambda: FakeClient(triggers=[trig("BNB", "51")],
                            positions={"BNB-PERP": {}}),
         seen=seeded("BNB", "51"))
    # (BNB is FX-fenced on pacifica/extended and UNIVERSE-fenced on nado — decision is
    #  keep on every venue either way; parity compares old-vs-new on the same venue.)

    # ---- S20 in-memory tracked -> keep; and prune of stale keys
    both("S20_inmemory_tracked_and_prune",
         lambda: FakeClient(triggers=[trig(C_ORPH, "61")]),
         open_pos={C_ORPH: object()},
         seen={("STALE", "1"): FIXED_NOW - 500.0})

    # ---- S21 standalone _fenced / _coin_present parity (main.py consumer callsites)
    world.reset()
    battery = ["BTC", "DOGE", "BNB", "BNB-PERP", "EUR", "EUR-PERP", "EURUSD-PERP",
               "xyz_GOLD", "KAVA", "PAXG", None, "eth-usd"]
    fen_old = [old._fenced(c) for c in battery]
    fen_new = [new._fenced(c) for c in battery]
    keys = {"BNB-PERP", "ETH"}
    cp_old = [old._coin_present(c, keys) for c in battery]
    cp_new = [new._coin_present(c, keys) for c in battery]
    compare("S21_standalone_fenced_coin_present",
            {"fenced": fen_old, "present": cp_old},
            {"fenced": fen_new, "present": cp_new}, failures)

    # ---- S22 DEBOUNCE constant + exports
    exp = {}
    for tag, mod in (("old", old), ("new", new)):
        exp[tag] = {"DEBOUNCE_SEC": mod.DEBOUNCE_SEC,
                    "has": sorted(n for n in ("_variants", "_coin_present", "_fenced",
                                              "sweep_orphan_triggers") if hasattr(mod, n))}
    compare("S22_exports_and_constants", exp["old"], exp["new"], failures)

    print("-" * 60)
    if failures:
        print(f"FAIL — {len(failures)} scenario(s) diverged: {failures}")
        sys.exit(1)
    print(f"PASS — all scenarios decision-identical on venue={venue}"
          + (f" (waived: {waived})" if waived else ""))
    sys.exit(0)


if __name__ == "__main__":
    main()
