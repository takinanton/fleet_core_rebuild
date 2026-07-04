"""test_resting_orders.py — dry logic test, zero live network calls.

Tests:
  (a) Resting order placed at forming trigger while price is below it.
  (b) Order re-placed when rolling-max rises (trigger moves up).
  (c) Exactly one order per (coin, tf, side) at all times.
  (d) On simulated cross (position appears), detect_resting_fills fires and
      the post-fill path receives the correct metadata.
  (e) Cancel on position open (account_coins guard).
  (f) [NEW] sz > 0 at placement — HL rejects sz=0.
  (g) [NEW] Fill → immediate SL path: fill detected, SL placed, Position created,
      opens_today incremented, resting key consumed.
  (h) [NEW] startup_orphan_sweep: cancels orphan entry triggers with no position.

All exchange calls are stubbed. No orders actually placed.
"""
from __future__ import annotations

import sys
import logging
import math
from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test_resting")

# ─── Stub Settings ─────────────────────────────────────────────────────────
@dataclass
class StubPerTFFilters:
    f1: float = 0.0
    f2: float = 0.0
    f3: float = 0.0
    donchian_k: int = 5  # small for test data

@dataclass
class StubPerTFShortFilters:
    f1: float = 0.0
    f2: float = 0.0
    f3: float = 0.0
    donchian_k: int = 5

@dataclass
class StubSettings:
    working_tfs: tuple = ("1h",)
    short_enabled_tfs: tuple = ()
    entry_limit_cap_pct: float = 0.0024
    require_ema50_up: bool = False
    require_ema50_down: bool = False
    raw_rr_target: float = 1.5
    min_sl_dist_pct: float = 0.002
    max_concurrent: int = 10
    tf_max_sl: dict = field(default_factory=lambda: {"1h": 0.10})
    risk_per_trade: float = 0.01
    leverage: int = 10
    liq_size_cap_pct: float = 0.05

    def get_tf_filters(self, tf):
        return StubPerTFFilters()

    def get_tf_short_filters(self, tf):
        return StubPerTFShortFilters()

    def short_enabled_for(self, tf):
        return tf in self.short_enabled_tfs


# ─── Stub AssetMeta ────────────────────────────────────────────────────────
@dataclass
class StubAssetMeta:
    sz_decimals: int = 4
    min_size: float = 0.001
    tick_size: float = 0.01


# ─── Stub Exchange Client ───────────────────────────────────────────────────
class StubExchangeClient:
    """Records calls instead of hitting any network."""
    def __init__(self):
        self._next_oid = 1000
        self.placed_orders: list = []
        self.cancelled_oids: list = []
        self._positions: dict = {}  # coin → {szi, entryPx}
        self._candles: dict = {}    # (coin, tf) → DataFrame
        # For startup_orphan_sweep test
        self._open_entry_triggers: list = []  # list of {coin, oid}
        self._equity: float = 50_000.0

    def set_candles(self, coin, tf, df):
        self._candles[(coin, tf)] = df

    def set_position(self, coin, szi, entry_px):
        self._positions[coin] = {"szi": szi, "entryPx": entry_px}

    def set_open_entry_triggers(self, triggers: list):
        """Stub for startup_orphan_sweep: [{coin, oid}, ...]"""
        self._open_entry_triggers = triggers

    def candles(self, coin, tf, limit=300):
        return self._candles.get((coin, tf))

    def round_price(self, coin, px):
        return round(px, 4)

    def round_size(self, coin, sz):
        return round(sz, 4)

    def open_positions(self):
        return dict(self._positions)

    def account_value(self):
        return self._equity

    def asset(self, coin):
        return StubAssetMeta()

    def resting_stop_limit(self, coin, is_buy, sz, trigger_px, limit_px):
        oid = self._next_oid
        self._next_oid += 1
        self.placed_orders.append({
            "coin": coin, "is_buy": is_buy, "sz": sz,
            "trigger_px": trigger_px, "limit_px": limit_px, "oid": oid,
        })
        log.info(
            "STUB place resting oid=%d %s buy=%s trigger=%.4f limit=%.4f sz=%.4f",
            oid, coin, is_buy, trigger_px, limit_px, sz,
        )
        return {"response": {"data": {"statuses": [{"resting": {"oid": oid}}]}}}

    def cancel_sl_order(self, coin, oid):
        self.cancelled_oids.append(oid)
        log.info("STUB cancel oid=%d %s", oid, coin)
        return {}

    def trigger_sl(self, coin, is_buy, sz, trigger_px):
        """Stub SL placement — returns ok response."""
        oid = self._next_oid
        self._next_oid += 1
        self.placed_orders.append({
            "coin": coin, "is_buy": is_buy, "sz": sz,
            "trigger_px": trigger_px, "oid": oid, "_is_sl": True,
        })
        log.info("STUB SL placed oid=%d %s", oid, coin)
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": oid}}]}}}

    def list_open_entry_trigger_orders(self):
        return list(self._open_entry_triggers)

    def invalidate_positions_cache(self):
        pass


# ─── Stub SnapshotHolder ──────────────────────────────────────────────────
class StubSnapshotHolder:
    def current(self):
        return None  # no liq cap in tests — sizing uses risk-only path


# ─── Build synthetic price series ─────────────────────────────────────────

def make_df(closes, highs=None, lows=None, opens=None):
    """Build a minimal OHLCV DataFrame for testing."""
    n = len(closes)
    closes = np.array(closes, dtype=float)
    highs = np.array(highs, dtype=float) if highs is not None else closes * 1.002
    lows = np.array(lows, dtype=float) if lows is not None else closes * 0.998
    opens = np.array(opens, dtype=float) if opens is not None else closes * 0.9995
    times = pd.date_range("2024-01-01", periods=n, freq="1h")
    df = pd.DataFrame({
        "time": times,
        "Open": opens,
        "High": highs,
        "Low": lows,
        "Close": closes,
        "Volume": np.ones(n) * 1000.0,
    })
    return df


# ─── Import the module under test ──────────────────────────────────────────

import os
import importlib.util
import types

DEV_ROOT = "/home/ubuntu/hl_bot_v2_dev"
sys.path.insert(0, DEV_ROOT)

# Monkey-patch TF_MS into bot.config so the import works without .env
config_mod = types.ModuleType("bot.config")
config_mod.TF_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

class _FakeSettings:
    pass
config_mod.Settings = _FakeSettings
sys.modules["bot.config"] = config_mod

# Provide strategy module
spec = importlib.util.spec_from_file_location(
    "bot.strategy_uk_v102",
    os.path.join(DEV_ROOT, "bot/strategy_uk_v102.py"),
)
strat_mod = importlib.util.module_from_spec(spec)
sys.modules["bot.strategy_uk_v102"] = strat_mod
spec.loader.exec_module(strat_mod)

# Provide risk module (needed by _compute_size)
spec_risk = importlib.util.spec_from_file_location(
    "bot.risk",
    os.path.join(DEV_ROOT, "bot/risk.py"),
)
risk_mod = importlib.util.module_from_spec(spec_risk)
sys.modules["bot.risk"] = risk_mod
spec_risk.loader.exec_module(risk_mod)

# Now import the module under test
spec2 = importlib.util.spec_from_file_location(
    "bot.resting_orders",
    os.path.join(DEV_ROOT, "bot/resting_orders.py"),
)
resting_mod = importlib.util.module_from_spec(spec2)
sys.modules["bot.resting_orders"] = resting_mod
spec2.loader.exec_module(resting_mod)

RestingOrderManager = resting_mod.RestingOrderManager

# ─── Asset stub ────────────────────────────────────────────────────────────
@dataclass
class StubAsset:
    symbol: str
    tier: int = 1
    vol_24h_usd: float = 1_000_000.0


# ─── Helpers ───────────────────────────────────────────────────────────────
FAILURES = 0

def check(cond, msg):
    global FAILURES
    if cond:
        log.info("PASS: %s", msg)
    else:
        log.error("FAIL: %s", msg)
        FAILURES += 1


def flat_pullback_df():
    """Flat plateau (high=100.5) then slight pullback. close < hh → resting placed."""
    flat_closes = [100.0] * 15 + [99.5, 99.4, 99.3, 99.2, 99.1]
    flat_highs  = [100.5] * 15 + [99.6, 99.5, 99.4, 99.3, 99.2]
    flat_lows   = [99.8] * 15  + [99.3, 99.2, 99.1, 99.0, 98.9]
    return strat_mod.compute_indicators(make_df(flat_closes, flat_highs, flat_lows))


# ─── Tests ─────────────────────────────────────────────────────────────────

def test_a_resting_placed_when_below_trigger():
    """(a) Resting order placed at forming trigger when price is below it."""
    log.info("=== TEST A: resting order placed ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    df = flat_pullback_df()
    client.set_candles("BTC", "1h", df)
    coins = [StubAsset("BTC")]

    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())

    check(len(client.placed_orders) == 1, "exactly one resting order placed")
    if client.placed_orders:
        o = client.placed_orders[0]
        check(o["coin"] == "BTC", "correct coin")
        check(o["is_buy"] == True, "is_buy=True for long")
        check(o["trigger_px"] > 99.9 and o["trigger_px"] < 100.6, f"trigger in band: {o['trigger_px']:.4f}")
        check(o["limit_px"] > o["trigger_px"], f"limit > trigger: {o['limit_px']:.4f}")
    check(("BTC", "1h", "long") in rom.active_keys(), "(BTC,1h,long) in active_keys")


def test_b_resting_repriced_on_trigger_rise():
    """(b) Resting order re-placed when rolling-max rises."""
    log.info("=== TEST B: re-price on trigger rise ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()

    flat_closes = [100.0] * 15 + [99.0] * 5
    flat_highs  = [100.5] * 15 + [99.2] * 5
    flat_lows   = [99.5] * 15  + [98.8] * 5
    df1 = strat_mod.compute_indicators(make_df(flat_closes, flat_highs, flat_lows))
    client.set_candles("ETH", "1h", df1)
    coins = [StubAsset("ETH")]
    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())

    check(len(client.placed_orders) == 1, "loop1: one resting order placed")
    first_oid = client.placed_orders[0]["oid"] if client.placed_orders else None
    first_trigger = client.placed_orders[0]["trigger_px"] if client.placed_orders else None

    flat_closes2 = [100.0] * 14 + [101.0] + [99.0] * 5
    flat_highs2  = [100.5] * 14 + [102.0] + [99.2] * 5
    flat_lows2   = [99.5] * 14  + [100.5] + [98.8] * 5
    df2 = strat_mod.compute_indicators(make_df(flat_closes2, flat_highs2, flat_lows2))
    client.set_candles("ETH", "1h", df2)
    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())

    new_orders = [o for o in client.placed_orders if o["oid"] != first_oid]
    check(len(new_orders) >= 1, "loop2: new resting order placed after re-price")
    check(first_oid in client.cancelled_oids, f"old oid {first_oid} cancelled on re-price")
    if new_orders:
        check(
            new_orders[-1]["trigger_px"] > first_trigger,
            f"new trigger {new_orders[-1]['trigger_px']:.4f} > old {first_trigger:.4f}",
        )
    check(len(rom.active_keys()) == 1, "still exactly one active key after re-price")


def test_c_exactly_one_order_per_key():
    """(c) Multiple refresh calls never produce two orders for same (coin,tf,side)."""
    log.info("=== TEST C: at most one order per key ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    df = flat_pullback_df()
    client.set_candles("SOL", "1h", df)
    coins = [StubAsset("SOL")]

    for _ in range(5):
        rom.refresh(coins, client, {}, set(), settings, n_open=0,
                    equity=client._equity, snapshot_holder=StubSnapshotHolder())

    check(len(rom.active_keys()) == 1, "5 loops → still 1 active key")
    check(len(client.placed_orders) == 1, "5 loops → only 1 place call")


def test_d_fill_detection_transitions_to_post_fill():
    """(d) After simulated fill, detect_resting_fills returns correct metadata."""
    log.info("=== TEST D: fill detection ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    df = flat_pullback_df()
    client.set_candles("AVAX", "1h", df)
    coins = [StubAsset("AVAX")]

    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(("AVAX", "1h", "long") in rom.active_keys(), "resting order active before fill")

    client.set_position("AVAX", szi=0.5, entry_px=100.65)
    account_positions = client.open_positions()
    open_positions_bot: dict = {}

    filled = rom.detect_resting_fills(account_positions, open_positions_bot)
    check(len(filled) == 1, "exactly one fill detected")
    if filled:
        key, entry_px, filled_sz = filled[0]
        check(key == ("AVAX", "1h", "long"), f"correct key: {key}")
        check(abs(entry_px - 100.65) < 0.01, f"entry_px from position: {entry_px}")
        check(abs(filled_sz - 0.5) < 0.001, f"filled_sz from position: {filled_sz}")

        ro = rom.consume_fill(key)
        check(ro is not None, "consume_fill returned RestingOrder")
        if ro:
            check(ro.sl_price > 0, f"sl_price populated: {ro.sl_price:.4f}")
            check(ro.tp1_price > 0, f"tp1_price populated: {ro.tp1_price:.4f}")

        check(("AVAX", "1h", "long") not in rom.active_keys(), "key consumed after fill")


def test_cancel_on_position_open():
    """(e) Cancel resting order when account_coins shows the coin."""
    log.info("=== TEST E: cancel on position open ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    df = flat_pullback_df()
    client.set_candles("LINK", "1h", df)
    coins = [StubAsset("LINK")]

    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(("LINK", "1h", "long") in rom.active_keys(), "resting order placed")
    oid = client.placed_orders[0]["oid"] if client.placed_orders else None

    rom.refresh(coins, client, {}, account_coins={"LINK"}, settings=settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(("LINK", "1h", "long") not in rom.active_keys(), "resting order cancelled")
    check(oid in client.cancelled_oids, f"oid {oid} explicitly cancelled")


def test_f_sz_nonzero_at_placement():
    """(f) FLAG 1: resting order sz > 0 at placement (HL rejects sz=0)."""
    log.info("=== TEST F: sz > 0 at placement ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    # equity=50000, risk_per_trade=0.01 → risk=$500, sl_dist~1.1 → size~454 units
    # but leverage cap: 50000*10/100 = 5000 units, so should be ~454
    df = flat_pullback_df()
    client.set_candles("BTC", "1h", df)
    coins = [StubAsset("BTC")]

    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())

    check(len(client.placed_orders) >= 1, "order was placed")
    for o in client.placed_orders:
        check(o["sz"] > 0, f"sz={o['sz']} > 0 for oid={o['oid']}")


def test_g_fill_immediate_sl_path():
    """(g) Simulate the main_loop fill→SL path inline (no main_loop needed):
    fill detected → consume_fill → trigger_sl called → Position constructed."""
    log.info("=== TEST G: fill → immediate SL path ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()
    df = flat_pullback_df()
    client.set_candles("BNB", "1h", df)
    coins = [StubAsset("BNB")]

    rom.refresh(coins, client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(len(rom.active_keys()) == 1, "resting order placed before fill")

    # Simulate fill: position appears on exchange
    client.set_position("BNB", szi=2.5, entry_px=100.51)
    _account_positions = client.open_positions()
    open_positions_bot: dict = {}

    resting_fills = rom.detect_resting_fills(_account_positions, open_positions_bot)
    check(len(resting_fills) == 1, "one fill detected")

    opens_today = 0
    for _rkey, _entry_px, _filled_sz in resting_fills:
        _coin, _tf, _side = _rkey
        _ro = rom.consume_fill(_rkey)
        check(_ro is not None, "consume_fill OK")
        check(_ro.size > 0, f"_ro.size={_ro.size:.4f} > 0")

        # Simulate _place_sl_with_retry: calls trigger_sl
        sl_resp = client.trigger_sl(coin=_coin, is_buy=False, sz=_filled_sz, trigger_px=_ro.sl_price)
        check(sl_resp.get("status") == "ok", "SL trigger_sl returned ok")

        # Construct Position (mirrors main_loop wiring)
        _pos = strat_mod.Position(
            coin=_coin, tf=_tf,
            entry_price=_entry_px,
            sl_initial=_ro.sl_price,
            sl_current=_ro.sl_price,
            tp1_price=_ro.tp1_price,
            size=_filled_sz,
            bar_entry_idx=0,
            side=_side,
        )
        open_positions_bot[_coin] = _pos
        opens_today += 1

    check(opens_today == 1, f"opens_today incremented: {opens_today}")
    check("BNB" in open_positions_bot, "Position added to open_positions")
    check(("BNB", "1h", "long") not in rom.active_keys(), "resting key consumed")

    # Verify SL order was placed (not a resting entry order)
    sl_orders = [o for o in client.placed_orders if o.get("_is_sl")]
    check(len(sl_orders) == 1, f"exactly 1 SL order placed (sl_orders={len(sl_orders)})")


def test_h_startup_orphan_sweep():
    """(h) FLAG 4: startup_orphan_sweep cancels orphan entry triggers with no position."""
    log.info("=== TEST H: startup orphan sweep ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()

    # 3 open entry triggers on exchange: oid=2001(BTC, no position), 2002(ETH, has position), 2003(SOL, no position)
    client.set_open_entry_triggers([
        {"coin": "BTC", "oid": 2001},
        {"coin": "ETH", "oid": 2002},
        {"coin": "SOL", "oid": 2003},
    ])
    # ETH has a live position
    client.set_position("ETH", szi=1.0, entry_px=3200.0)

    open_positions = {}  # bot's in-memory (empty at startup)
    rom.startup_orphan_sweep(client, open_positions)

    # BTC oid=2001: no position → should be cancelled
    check(2001 in client.cancelled_oids, "BTC orphan oid=2001 cancelled")
    # ETH oid=2002: has position → should be KEPT (not cancelled)
    check(2002 not in client.cancelled_oids, "ETH oid=2002 kept (position exists)")
    # SOL oid=2003: no position → should be cancelled
    check(2003 in client.cancelled_oids, "SOL orphan oid=2003 cancelled")


# ─── Run all tests ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_a_resting_placed_when_below_trigger()
    test_b_resting_repriced_on_trigger_rise()
    test_c_exactly_one_order_per_key()
    test_d_fill_detection_transitions_to_post_fill()
    test_cancel_on_position_open()
    test_f_sz_nonzero_at_placement()
    test_g_fill_immediate_sl_path()
    test_h_startup_orphan_sweep()

    print()
    if FAILURES == 0:
        print("ALL TESTS PASSED")
        sys.exit(0)
    else:
        print(f"{FAILURES} TEST(S) FAILED")
        sys.exit(1)
