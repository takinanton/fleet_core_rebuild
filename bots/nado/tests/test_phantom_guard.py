"""Generic phantom-guard verification — works across hl/nado/ext/paci trader.py variants.

Usage: python test_phantom_guard_generic.py /path/to/bot_repo
Verifies: (A) phantom auto-closes after K, (B) no SL placed while absent, (C) present-naked still
heals, (D) miss counter resets on a live sighting.
"""
import sys, types
REPO = sys.argv[1]
sys.path.insert(0, REPO)
import bot.trader as T


class _StopHere(Exception):
    pass


class FakeDF:
    empty = False


class Pos:
    def __init__(self, coin, size=100.0, sl_oid=111):
        self.coin = coin; self.tf = "4h"; self.size = size
        self.sl_current = 1.0; self.sl_initial = 0.9; self.entry_price = 1.1
        self.side = "long"; self.tp1_price = 1.5
        self.tp1_partial_done = False
        self.__dict__["_trade_id"] = 42
        self.__dict__["_sl_order_id"] = sl_oid
        self.__dict__["_orig_size"] = size


class Settings:
    dry_run = False; enable_trail_after_tp = False; max_run_r = 1000.0


class PMStop:
    def update_sl_on_new_bar(self, **k):
        raise _StopHere()


class FakeClient:
    def __init__(self, present):
        self._present = dict(present); self.invalidations = 0
    def open_positions(self, *a, **k):
        return dict(self._present)
    def invalidate_positions_cache(self):
        self.invalidations += 1
    def mark_price(self, coin, ttl=5.0):
        return 1.0
    def list_open_sl_orders(self, coin):
        return []
    def cancel_sl_order(self, coin, oid):
        return {}


placed, closes, orphan_cancels = [], [], []


def _setattr_if(name, val):
    if hasattr(T, name):
        setattr(T, name, val)


_setattr_if("_dry_block", lambda what: False)
_setattr_if("_sl_confirmed_live", lambda client, coin, oid: False)
_setattr_if("_confirm_sl_live_poll", lambda *a, **k: True)
_setattr_if("ensure_sl_inside_liq", lambda **k: (k.get("sl_px"), "ok"))
_setattr_if("_place_sl_with_retry", lambda **k: (placed.append(k.get("coin")) or 999))
_setattr_if("_record_close", lambda **k: closes.append(k))
_setattr_if("_cancel_orphan_triggers", lambda client, coin: (orphan_cancels.append(coin) or 0))
_setattr_if("_cancel_tp_limit", lambda client, pos: None)
_setattr_if("_cancel_tp_if_any", lambda client, pos: None)
_setattr_if("update_trade_sl_order", lambda *a, **k: None)
_setattr_if("_detect_partial_fill", lambda *a, **k: None)
_setattr_if("_handle_tp1_partial", lambda *a, **k: False)

S = Settings(); DFL = {"4h": FakeDF()}


def _call(client, pos):
    return T.manage_open_position(pos=pos, client=client, settings=S,
                                  position_manager=PMStop(), df_latest=DFL)


def reset():
    placed.clear(); closes.clear(); orphan_cancels.clear(); T._PHANTOM_MISS.clear()


fails = []
K = T.PHANTOM_MISS_CLOSE_K

# A + B: phantom absent → auto-close after K, NEVER heals.
reset()
client = FakeClient({}); pos = Pos("PHANTOMCOIN")
rets = [_call(client, pos) for _ in range(K)]
if rets[:K-1] != [None]*(K-1):
    fails.append(f"A: first {K-1} ticks should be None, got {rets[:K-1]}")
if rets[-1] != "phantom_no_exchange_position":
    fails.append(f"A: tick {K} should auto-close, got {rets[-1]!r}")
if placed:
    fails.append(f"B: heal placed SL on ABSENT position (churn!) placed={placed}")
if len(closes) != 1 or (closes and closes[0].get("exit_reason") != "phantom_no_exchange_position"):
    fails.append(f"A: expected 1 phantom close, got {closes}")
if orphan_cancels != ["PHANTOMCOIN"]:
    fails.append(f"A: orphan triggers not swept, got {orphan_cancels}")
if client.invalidations < 1:
    fails.append("A: final re-read did not invalidate cache")
print(f"[A/B] rets={rets} placed={placed} closes={len(closes)} orphan={orphan_cancels} inval={client.invalidations}")

# C: present + no SL → STILL heals.
reset()
client = FakeClient({"REALCOIN": {"szi": "100.0"}})
pos = Pos("REALCOIN", sl_oid=None)   # None so nado's `if sl_order_id is None` heal also fires
try:
    _call(client, pos)
    fails.append("C: expected heal+_StopHere, manage returned without reaching trail")
except Exception:
    pass
if placed != ["REALCOIN"]:
    fails.append(f"C: present naked NOT healed, placed={placed}")
if closes:
    fails.append(f"C: present position must NOT phantom-close, closes={closes}")
print(f"[C] placed={placed} closes={len(closes)}")

# D: miss counter resets on a live sighting.
reset()
empty = FakeClient({}); live = FakeClient({"FLAP": {"szi": "100.0"}})
pos = Pos("FLAP")
_call(empty, pos); _call(empty, pos)
miss_before = T._PHANTOM_MISS.get("FLAP")
try:
    _call(live, pos)
except Exception:
    pass
miss_after = T._PHANTOM_MISS.get("FLAP", 0)
_call(empty, pos)
miss_final = T._PHANTOM_MISS.get("FLAP")
if miss_before != 2:
    fails.append(f"D: expected miss=2 pre-sight, got {miss_before}")
if miss_after != 0:
    fails.append(f"D: sighting did not reset, got {miss_after}")
if miss_final != 1:
    fails.append(f"D: did not restart from 1, got {miss_final}")
if closes:
    fails.append(f"D: intermittent must NOT auto-close, closes={closes}")
print(f"[D] before={miss_before} after_sight={miss_after} final={miss_final}")

print("K =", K)
if fails:
    print("\nFAILED:"); [print("  -", f) for f in fails]; sys.exit(1)
print("\nALL PHANTOM-GUARD TESTS PASSED")
