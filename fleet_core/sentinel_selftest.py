#!/usr/bin/env python3
"""Offline self-test for fleet_core.sentinel — runs WITHOUT venue SDKs.

Fake bot.* modules are injected via sys.modules BEFORE importing the sentinel;
the FENCE logic under test is the REAL canonical fleet_core.orphan_sweep
(installed as bot.orphan_sweep, exactly like the P1 shim does on the hosts) fed
by fake bot.config / bot.universe / bot.journal. The exchange client, systemd
runner and clock are injected fakes.

Scenario matrix:
   1 healthy                       -> 0 violations
   2 naked-owned (alert)           -> I1a_naked CRITICAL, no order placed
   3 naked-owned (protect)         -> SL re-placed from DB sl_current, no cancel
   4 naked-fenced BNB (protect)    -> skipped entirely, nothing placed
   5 db-only row                   -> I2_phantom_db_row
   6 orphan trigger fenced/unfenced-> unfenced flagged I3, fenced skipped
   7 read-failure                  -> tick skipped (None), nothing asserted
   8 SL outside liq                -> I1a_sl_outside_liq
   9 duplicate-protect race        -> our just-placed duplicate cancelled, bot's kept
  10 dead-bot / hung-bot heartbeat -> I4 CRITICAL (and WARNING when flat)
  11 untracked position (has SL)   -> I1b_db_row_missing only
  12 short position on long-only   -> foreign by construction, skipped
  13 HL manual-prefix per-oid fence-> manual oid fenced, bot-placed oid flagged
  14 indeterminate empty-positions -> tick skipped (None)

Exit 0 on full pass, 1 otherwise. Run: /usr/bin/python3 fleet_core/sentinel_selftest.py
"""

import json
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

NOW = 1_800_000_000.0

# ---------------------------------------------------------------- fake bot.*
_bot = types.ModuleType("bot")
_bot.__path__ = []  # mark as package

_cfg = types.ModuleType("bot.config")
_cfg.FX_EXCLUDE = {"BNB", "VIRTUAL"}


class _FakeSettings:
    exchange = "pacifica"
    short_enabled_tfs = ()
    dry_run = False
    loop_interval_sec = 60


_cfg.settings = _FakeSettings()
_cfg.PROJECT_ROOT = Path(tempfile.mkdtemp(prefix="sentinel_selftest_"))

_journal = types.ModuleType("bot.journal")
_journal._rows = []
_journal.open_trades = lambda: list(_journal._rows)

_universe = types.ModuleType("bot.universe")
_universe.UNIVERSE_SYMBOL_EXCLUDE = set()
_universe._is_fx = lambda c: False

sys.modules["bot"] = _bot
sys.modules["bot.config"] = _cfg
sys.modules["bot.journal"] = _journal
sys.modules["bot.universe"] = _universe

# The REAL canonical fence logic, installed exactly like the P1 shim does.
import fleet_core.orphan_sweep as _osw  # noqa: E402

sys.modules["bot.orphan_sweep"] = _osw

from fleet_core.sentinel import Sentinel  # noqa: E402


# ---------------------------------------------------------------- fake client
class FakeClient:
    def __init__(self):
        self.positions = {}      # key -> pos entry dict
        self.triggers = []       # [{"coin","oid","px","reduce_only","is_trigger"}]
        self.sl_orders = {}      # coin -> [oid]
        self.placed = []         # trigger_sl calls
        self.cancelled = []      # cancel_sl_order calls
        self.marks = {}
        self.liq = {}
        self.fail_positions = False
        self.race_inject = None  # fn(coin) run right after our trigger_sl

    def open_positions(self):
        if self.fail_positions:
            raise RuntimeError("simulated venue outage")
        return dict(self.positions)

    def list_reduce_only_triggers(self):
        return [{"coin": t["coin"], "oid": t["oid"], "reduce_only": True,
                 "is_trigger": True} for t in self.triggers]

    def _signed_request(self, method, path, name, params):  # pacifica trigger meta
        return {"success": True, "data": [
            {"symbol": t["coin"], "order_id": t["oid"], "reduce_only": True,
             "order_type": "stop_loss_market", "stop_price": t.get("px")}
            for t in self.triggers]}

    def list_open_sl_orders(self, coin):
        return list(self.sl_orders.get(coin, []))

    def mark_price(self, coin):
        return self.marks.get(coin, 100.0)

    def position_liquidation(self, coin):
        liq = self.liq.get(coin)
        return {"liq_px": liq, "margin_mode": "cross", "leverage": 1} if liq else None

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        oid = f"sent-{len(self.placed) + 1}"
        self.placed.append({"coin": coin, "is_buy": is_buy, "sz": sz,
                            "px": trigger_px, "oid": oid})
        self.sl_orders.setdefault(coin, []).append(oid)
        if self.race_inject:
            self.race_inject(coin)
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"resting": {"oid": oid}}]}}}

    def cancel_sl_order(self, coin, oid):
        self.sl_orders.get(coin, []).remove(oid)
        self.cancelled.append((coin, oid))
        return {"status": "ok"}


def make_runner(state):
    def runner(cmd, capture_output=True, text=True, timeout=15):
        r = types.SimpleNamespace(stdout="", stderr="", returncode=0)
        if cmd[0] == "systemctl":
            r.stdout = state.get("unit_state", "active") + "\n"
        elif cmd[0] == "journalctl":
            r.stdout = json.dumps(
                {"__REALTIME_TIMESTAMP": str(int(state.get("hb_ts", NOW) * 1e6))}) + "\n"
        return r
    return runner


_BUILD_N = [0]


def build(mode="alert", unit_state="active", hb_ts=None):
    client = FakeClient()
    state = {"unit_state": unit_state, "hb_ts": hb_ts if hb_ts is not None else NOW - 5}
    _BUILD_N[0] += 1
    vpath = _cfg.PROJECT_ROOT / "data" / f"violations_{_BUILD_N[0]}.jsonl"
    s = Sentinel("pacifica", client, mode, violations_path=vpath,
                 runner=make_runner(state), now_fn=lambda: NOW,
                 heartbeat_max_s=900, liq_buf_pct=0.01, refire_ticks=10)
    return s, client, state, vpath


def pos(szi, entry="100", liq=None):
    d = {"szi": str(szi), "entryPx": entry}
    if liq is not None:
        d["liquidationPx"] = str(liq)
    return d


def row(coin, direction="long", sl=95.0, rid=1):
    return {"id": rid, "coin": coin, "direction": direction, "status": "open",
            "sl_current": sl, "sl_initial": sl, "size": 1.0}


FAILS = []


def check(name, cond, msg=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {msg}")
        FAILS.append(name)


def inv(violations, invariant):
    return [v for v in (violations or []) if v["invariant"] == invariant]


print("== 1 healthy ==")
s, c, st, vp = build()
c.positions["ETH"] = pos(1, liq="80")
c.triggers = [{"coin": "ETH", "oid": "t1", "px": 95.0}]
_journal._rows = [row("ETH")]
v = s.tick()
check("healthy_no_violations", v == [], f"got {v}")

print("== 2 naked-owned alert ==")
s, c, st, vp = build("alert")
c.positions["ETH"] = pos(1, liq="80")
_journal._rows = [row("ETH")]
v = s.tick()
check("naked_flagged", len(inv(v, "I1a_naked")) == 1, f"got {v}")
check("alert_mode_no_orders", c.placed == [], f"placed {c.placed}")
check("jsonl_written", vp.exists() and "I1a_naked" in vp.read_text())

print("== 3 naked-owned protect ==")
s, c, st, vp = build("protect")
c.positions["ETH"] = pos(1, liq="80")
_journal._rows = [row("ETH", sl=95.0)]
v = s.tick()
check("protect_placed_one", len(c.placed) == 1, f"placed {c.placed}")
check("protect_px_from_db", c.placed and c.placed[0]["px"] == 95.0 and
      c.placed[0]["coin"] == "ETH" and c.placed[0]["is_buy"] is False,
      f"placed {c.placed}")
check("protect_no_cancel", c.cancelled == [], f"cancelled {c.cancelled}")
# exchange now reflects our SL in the trigger list -> healthy, no re-place
c.triggers = [{"coin": "ETH", "oid": "sent-1", "px": 95.0}]
v2 = s.tick()
check("protect_idempotent", len(c.placed) == 1 and inv(v2, "I1a_naked") == [],
      f"placed {c.placed} v2={v2}")

print("== 4 naked-fenced BNB skip (protect) ==")
s, c, st, vp = build("protect")
c.positions["BNB"] = pos(-40, entry="600")   # user manual short, deliberately NO SL
_journal._rows = []
v = s.tick()
check("fenced_no_violations", v == [], f"got {v}")
check("fenced_nothing_placed", c.placed == [], f"placed {c.placed}")

print("== 5 db-only row ==")
s, c, st, vp = build()
c.positions["ETH"] = pos(1, liq="80")
c.triggers = [{"coin": "ETH", "oid": "t1", "px": 95.0}]
_journal._rows = [row("ETH"), row("COMP", rid=2)]
v = s.tick()
check("phantom_db_row", len(inv(v, "I2_phantom_db_row")) == 1 and
      inv(v, "I2_phantom_db_row")[0]["coin"] == "COMP", f"got {v}")

print("== 6 orphan trigger fenced/unfenced ==")
s, c, st, vp = build()
c.positions = {}
_journal._rows = []
c.triggers = [{"coin": "DOGE", "oid": "o1", "px": 0.1},   # unfenced orphan
              {"coin": "BNB", "oid": "o2", "px": 500.0}]  # fenced orphan (FX_EXCLUDE)
v = s.tick()
i3 = inv(v, "I3_orphan_trigger")
check("orphan_unfenced_flagged", len(i3) == 1 and i3[0]["coin"] == "DOGE", f"got {v}")
check("orphan_fenced_skipped", all(x["coin"] != "BNB" for x in i3), f"got {v}")
check("orphan_nothing_cancelled", c.cancelled == [], f"cancelled {c.cancelled}")

print("== 7 read-failure tick-skip ==")
s, c, st, vp = build()
c.fail_positions = True
_journal._rows = [row("ETH")]
v = s.tick()
check("read_failure_skips_tick", v is None, f"got {v}")
check("read_failure_no_jsonl", not vp.exists() or vp.read_text().strip() == "",
      f"file: {vp.read_text() if vp.exists() else ''}")

print("== 8 SL outside liq ==")
s, c, st, vp = build()
c.positions["ETH"] = pos(1, liq="90")
c.triggers = [{"coin": "ETH", "oid": "t1", "px": 89.0}]  # 89 <= 90*1.01
_journal._rows = [row("ETH", sl=89.0)]
v = s.tick()
check("sl_outside_liq", len(inv(v, "I1a_sl_outside_liq")) == 1, f"got {v}")
# short direction: liq above, SL must be BELOW liq*(1-buf)
s, c, st, vp = build()
_cfg.settings.short_enabled_tfs = ("4h",)
c.positions["SOL"] = pos(-2, entry="100", liq="110")
c.triggers = [{"coin": "SOL", "oid": "t2", "px": 111.0}]
_journal._rows = [row("SOL", direction="short", sl=111.0)]
v = s.tick()
_cfg.settings.short_enabled_tfs = ()
check("sl_outside_liq_short", len(inv(v, "I1a_sl_outside_liq")) == 1, f"got {v}")

print("== 9 duplicate-protect race ==")
s, c, st, vp = build("protect")
c.positions["ETH"] = pos(1, liq="80")
_journal._rows = [row("ETH", sl=95.0)]
c.race_inject = lambda coin: c.sl_orders[coin].append("bot-heal-oid")
v = s.tick()
check("race_cancelled_own_only", c.cancelled == [("ETH", "sent-1")],
      f"cancelled {c.cancelled}")
check("race_bot_sl_kept", "bot-heal-oid" in c.sl_orders.get("ETH", []),
      f"sl_orders {c.sl_orders}")

print("== 10 dead / hung bot ==")
s, c, st, vp = build(unit_state="inactive")
c.positions["ETH"] = pos(1, liq="80")
c.triggers = [{"coin": "ETH", "oid": "t1", "px": 95.0}]
_journal._rows = [row("ETH")]
v = s.tick()
check("dead_bot_critical", len(inv(v, "I4_bot_dead")) == 1, f"got {v}")
check("dead_bot_sev", "CRITICAL" in vp.read_text(), "severity not CRITICAL")
s, c, st, vp = build(unit_state="inactive")  # flat -> WARNING only
_journal._rows = []
v = s.tick()
check("dead_bot_flat_warning", len(inv(v, "I4_bot_dead")) == 1 and
      '"severity": "WARNING"' in vp.read_text(), f"got {v} / {vp.read_text()}")
s, c, st, vp = build(hb_ts=NOW - 2000)       # active but journal 2000s stale
c.positions["ETH"] = pos(1, liq="80")
c.triggers = [{"coin": "ETH", "oid": "t1", "px": 95.0}]
_journal._rows = [row("ETH")]
v = s.tick()
check("hung_bot_heartbeat", len(inv(v, "I4_bot_hung")) == 1, f"got {v}")

print("== 11 untracked position with SL ==")
s, c, st, vp = build()
c.positions["LINK"] = pos(3, entry="20", liq="15")
c.triggers = [{"coin": "LINK", "oid": "t9", "px": 18.0}]
_journal._rows = []
v = s.tick()
check("untracked_db_missing", len(inv(v, "I1b_db_row_missing")) == 1 and
      inv(v, "I1a_naked") == [], f"got {v}")

print("== 12 short on long-only bot -> foreign skip ==")
s, c, st, vp = build("protect")
c.positions["XRP"] = pos(-100, entry="2")   # short, not FX-fenced, no db row
_journal._rows = []
v = s.tick()
check("short_foreign_skipped", v == [] and c.placed == [], f"got {v} placed {c.placed}")

print("== 13 HL manual-prefix per-oid fence on orphan triggers ==")
_cfg.MANUAL_POSITION_PREFIXES = ("xyz_",)
_journal.coins_ever_traded = lambda: {"xyz_GOLD"}
_journal.oids_ever_placed = lambda: {"bot-oid-1"}
# (a) MANUAL oid (not in placed_oids) alone -> fenced, NO flag
s, c, st, vp = build()
c.positions = {}
_journal._rows = []
c.triggers = [{"coin": "xyz_GOLD", "oid": "manual-oid-7", "px": 3000.0}]
v = s.tick()
check("hl_manual_oid_fenced", inv(v, "I3_orphan_trigger") == [], f"got {v}")
# (b) BOT-placed oid (in placed_oids) alone -> NOT fenced, flagged
s, c, st, vp = build()
c.positions = {}
_journal._rows = []
c.triggers = [{"coin": "xyz_GOLD", "oid": "bot-oid-1", "px": 3100.0}]
v = s.tick()
check("hl_bot_oid_flagged", len(inv(v, "I3_orphan_trigger")) == 1, f"got {v}")
del _cfg.MANUAL_POSITION_PREFIXES
del _journal.coins_ever_traded
del _journal.oids_ever_placed

print("== 14 indeterminate: empty positions with DB rows ==")
s, c, st, vp = build()
c.positions = {}
_journal._rows = [row("ETH")]
v = s.tick()
check("indeterminate_skipped", v is None, f"got {v}")

print()
if FAILS:
    print(f"SELFTEST FAILED: {len(FAILS)} failing check(s): {FAILS}")
    sys.exit(1)
print("SELFTEST PASSED: all checks green")
sys.exit(0)
