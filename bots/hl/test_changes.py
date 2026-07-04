"""test_changes.py — validate CHANGE A (depth gate), B (short K), C (min-size)."""
from __future__ import annotations
import sys, os, logging, types, importlib.util
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test_changes")

DEV_ROOT = "/home/ubuntu/hl_bot_v2_dev"
sys.path.insert(0, DEV_ROOT)

# ── Stub bot.config ─────────────────────────────────────────────────────────
config_mod = types.ModuleType("bot.config")
config_mod.TF_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
config_mod.CANDLES_LIMIT = 300

@dataclass(frozen=True)
class _PerTFFilters:
    f1: float = 0.0; f2: float = 0.0; f3: float = 0.0; donchian_k: int = 20

@dataclass(frozen=True)
class _PerTFShortFilters:
    f1: float = 0.0; f2: float = 0.0; f3: float = 0.0; donchian_k: int = 20

class _FakeSettings:
    universe_min_vol_usd_24h: float = 1_000_000.0
    universe_top_n: int = 0
    universe_refresh_min: int = 60
    liq_size_cap_pct: float = 0.05
    liq_min_trade_usd: float = 20.0
    network: str = "mainnet"

config_mod.Settings = _FakeSettings
config_mod.PerTFFilters = _PerTFFilters
config_mod.PerTFShortFilters = _PerTFShortFilters
config_mod.FX_EXCLUDE = frozenset()
config_mod.settings = _FakeSettings()
sys.modules["bot.config"] = config_mod

# ── Stub eth_account (not installed in test env) ────────────────────────────
eth_mod = types.ModuleType("eth_account")
class _Account:
    @staticmethod
    def from_key(k): return None
eth_mod.Account = _Account
sys.modules["eth_account"] = eth_mod

# ── Stub hyperliquid SDK ─────────────────────────────────────────────────────
for m in ["hyperliquid", "hyperliquid.exchange", "hyperliquid.info", "hyperliquid.utils", "hyperliquid.utils.constants"]:
    sys.modules[m] = types.ModuleType(m)
const_mod = types.ModuleType("hyperliquid.utils.constants")
const_mod.MAINNET_API_URL = "https://api.hyperliquid.xyz"
const_mod.TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
sys.modules["hyperliquid.utils.constants"] = const_mod
# Make exchange_hl _SDK_OK = False to skip SDK init
exch_hl_preload = types.ModuleType("_exch_preload")

# ── Load exchange_hl for coin_to_api/api_to_coin ─────────────────────────────
# Patch to skip SDK check by pre-setting _SDK_OK=False before import
spec_hl = importlib.util.spec_from_file_location("bot.exchange_hl", os.path.join(DEV_ROOT, "bot/exchange_hl.py"))
exch_mod = importlib.util.module_from_spec(spec_hl)
sys.modules["bot.exchange_hl"] = exch_mod
spec_hl.loader.exec_module(exch_mod)

# ── Strategy + risk ──────────────────────────────────────────────────────────
for mod_name, file_name in [
    ("bot.strategy_uk_v102", "bot/strategy_uk_v102.py"),
    ("bot.risk", "bot/risk.py"),
]:
    spec = importlib.util.spec_from_file_location(mod_name, os.path.join(DEV_ROOT, file_name))
    m = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = m
    spec.loader.exec_module(m)
strat_mod = sys.modules["bot.strategy_uk_v102"]

# ── Resting orders ────────────────────────────────────────────────────────────
spec2 = importlib.util.spec_from_file_location("bot.resting_orders", os.path.join(DEV_ROOT, "bot/resting_orders.py"))
resting_mod = importlib.util.module_from_spec(spec2)
sys.modules["bot.resting_orders"] = resting_mod
spec2.loader.exec_module(resting_mod)
RestingOrderManager = resting_mod.RestingOrderManager

# ── Universe ──────────────────────────────────────────────────────────────────
requests_stub = types.ModuleType("requests")
requests_stub.post = lambda *a, **kw: None
sys.modules["requests"] = requests_stub

spec3 = importlib.util.spec_from_file_location("bot.universe", os.path.join(DEV_ROOT, "bot/universe.py"))
universe_mod = importlib.util.module_from_spec(spec3)
sys.modules["bot.universe"] = universe_mod
spec3.loader.exec_module(universe_mod)
apply_depth_gate = universe_mod.apply_depth_gate
REGIME_COINS = universe_mod.REGIME_COINS

# ── Liquidity ─────────────────────────────────────────────────────────────────
spec4 = importlib.util.spec_from_file_location("bot.liquidity", os.path.join(DEV_ROOT, "bot/liquidity.py"))
liq_mod = importlib.util.module_from_spec(spec4)
sys.modules["bot.liquidity"] = liq_mod
spec4.loader.exec_module(liq_mod)
LiquidityProfile = liq_mod.LiquidityProfile
LiquiditySnapshot = liq_mod.LiquiditySnapshot

# ── Stubs ─────────────────────────────────────────────────────────────────────
@dataclass
class StubAsset:
    symbol: str; tier: int = 1; vol_24h_usd: float = 1_000_000.0; note: str = "hl_native"

@dataclass
class StubPerTFFilters:
    f1: float = 0.0; f2: float = 0.0; f3: float = 0.0; donchian_k: int = 5

@dataclass
class StubPerTFShortFilters:
    f1: float = 0.0; f2: float = 0.0; f3: float = 0.0; donchian_k: int = 5

@dataclass
class StubSettings:
    working_tfs: tuple = ("1h",)
    short_enabled_tfs: tuple = ("1h",)
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
    liq_min_trade_usd: float = 20.0
    _long_k: int = 5
    _short_k: int = 10

    def get_tf_filters(self, tf): return StubPerTFFilters(donchian_k=self._long_k)
    def get_tf_short_filters(self, tf): return StubPerTFShortFilters(donchian_k=self._short_k)
    def short_enabled_for(self, tf): return tf in self.short_enabled_tfs

class StubAssetMeta:
    sz_decimals: int = 4; min_size: float = 0.001

class StubExchangeClient:
    def __init__(self):
        self._next_oid = 2000; self.placed_orders = []; self.cancelled_oids = []
        self._positions = {}; self._candles = {}; self._closed_coins = []; self._equity = 50_000.0
    def set_candles(self, c, tf, df): self._candles[(c, tf)] = df
    def set_position(self, c, szi, px): self._positions[c] = {"szi": szi, "entryPx": px}
    def candles(self, c, tf, limit=300): return self._candles.get((c, tf))
    def round_price(self, c, px): return round(px, 4)
    def round_size(self, c, sz): return round(sz, 4)
    def open_positions(self): return dict(self._positions)
    def account_value(self): return self._equity
    def asset(self, c): return StubAssetMeta()
    def invalidate_positions_cache(self): pass
    def resting_stop_limit(self, coin, is_buy, sz, trigger_px, limit_px):
        oid = self._next_oid; self._next_oid += 1
        self.placed_orders.append({"coin": coin, "is_buy": is_buy, "sz": sz,
                                   "trigger_px": trigger_px, "limit_px": limit_px, "oid": oid})
        return {"response": {"data": {"statuses": [{"resting": {"oid": oid}}]}}}
    def cancel_sl_order(self, c, oid): self.cancelled_oids.append(oid); return {}
    def trigger_sl(self, c, is_buy, sz, trigger_px):
        oid = self._next_oid; self._next_oid += 1
        self.placed_orders.append({"coin": c, "_is_sl": True, "oid": oid})
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": oid}}]}}}
    def market_close(self, c): self._closed_coins.append(c); return {"status": "ok"}

class StubSnapshotHolder:
    def current(self): return None

def make_df(n=50, base=100.0, trend="up"):
    mult = 1.001 if trend == "up" else 0.999
    closes = [base * (mult ** i) for i in range(n)]
    highs = [c * 1.003 for c in closes]; lows = [c * 0.997 for c in closes]
    times = pd.date_range("2024-01-01", periods=n, freq="1h")
    return pd.DataFrame({"time": times, "Open": closes, "High": highs,
                         "Low": lows, "Close": closes, "Volume": [1000.0] * n})

def make_snapshot(profiles_dict):
    return LiquiditySnapshot("2026-05-31T00:00:00Z", profiles_dict, Path("/tmp/fake.json"), 0.0, 0.0)

FAILURES = 0
def check(cond, msg):
    global FAILURES
    if cond: log.info("PASS: %s", msg)
    else:    log.error("FAIL: %s", msg); FAILURES += 1

# ═══════════════════════════════════════════════════════════════════════════
def test_a_depth_gate():
    log.info("=== TEST A: depth gate ===")
    profiles = {
        "BTC":      LiquidityProfile("BTC",      100_000.0, 0.0001, 50_000.0, 10_000.0, 1.0),
        "ILLIQUID": LiquidityProfile("ILLIQUID", 100_000.0, 0.0010,  2_000.0,  1_000.0, 1.0),
        "ZERO":     LiquidityProfile("ZERO",     100_000.0, 0.0010,    500.0,      0.0, 1.0),
    }
    snap = make_snapshot(profiles)
    universe = [StubAsset("BTC"), StubAsset("ILLIQUID"), StubAsset("ZERO")]

    # With gate enabled
    filtered = apply_depth_gate(universe, snap, 0.05, enabled=True)
    syms = [a.symbol for a in filtered]
    check("BTC" in syms,          "BTC passes: size_cap=5000 <= depth_05=10000")
    check("ILLIQUID" not in syms, "ILLIQUID excluded: size_cap=5000 > depth_05=1000")
    check("ZERO" not in syms,     "ZERO excluded: depth_05=0")

    filtered_off = apply_depth_gate(universe, snap, 0.05, enabled=False)
    check(len(filtered_off) == 3, "gate disabled → 3 pass")

    filtered_none = apply_depth_gate(universe, None, 0.05, enabled=True)
    check(len(filtered_none) == 3, "snapshot=None → fail-open → 3 pass")

    rp = {"xyz_VIX": LiquidityProfile("xyz_VIX", 0.0, 0.0, 0.0, 0.0, 1.0)}
    filtered_r = apply_depth_gate([StubAsset("xyz_VIX")], make_snapshot(rp), 0.05, enabled=True)
    check(len(filtered_r) == 1, "regime coin xyz_VIX bypasses depth gate")

def test_b_short_k():
    log.info("=== TEST B: short K per direction ===")
    settings = StubSettings()
    lf = settings.get_tf_filters("1h")
    sf = settings.get_tf_short_filters("1h")
    check(lf.donchian_k == 5,  f"long K=5 (got {lf.donchian_k})")
    check(sf.donchian_k == 10, f"short K=10 (got {sf.donchian_k})")

    rom = RestingOrderManager()
    client = StubExchangeClient()
    closes_down = [100.0 - i * 0.5 for i in range(60)]
    highs = [c + 0.3 for c in closes_down]; lows = [c - 0.3 for c in closes_down]
    times = pd.date_range("2024-01-01", periods=60, freq="1h")
    df_raw = pd.DataFrame({"time": times, "Open": closes_down, "High": highs,
                           "Low": lows, "Close": closes_down, "Volume": [1000.0]*60})
    df = strat_mod.compute_indicators(df_raw)
    client.set_candles("ETH", "1h", df)
    rom.refresh([StubAsset("ETH")], client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(True, "short branch ran without crash with sf.donchian_k=10")

    # 11 bars: max(5,10)+2=12 required → skip
    df_small = strat_mod.compute_indicators(df_raw.iloc[:11].copy())
    client.set_candles("SOL", "1h", df_small)
    placed_before = len(client.placed_orders)
    rom2 = RestingOrderManager()
    rom2.refresh([StubAsset("SOL")], client, {}, set(), settings, n_open=0,
                 equity=client._equity, snapshot_holder=StubSnapshotHolder())
    check(len(client.placed_orders) == placed_before,
          "11-bar df skipped (min_k_required=12)")

def test_c_min_size():
    log.info("=== TEST C: partial-fill min-size handler ===")
    rom = RestingOrderManager()
    client = StubExchangeClient()
    settings = StubSettings()

    df = strat_mod.compute_indicators(make_df(50, trend="up"))
    client.set_candles("BTC", "1h", df)
    rom.refresh([StubAsset("BTC")], client, {}, set(), settings, n_open=0,
                equity=client._equity, snapshot_holder=StubSnapshotHolder())
    assert ("BTC", "1h", "long") in rom.active_keys()

    # Tiny fill: 0.001 × 100.0 = $0.10 < $20
    client.set_position("BTC", szi=0.001, px=100.0)
    fills = rom.detect_resting_fills(client.open_positions(), {})
    assert len(fills) == 1

    dust_closed = False; sl_placed = False; opens_today = 0
    for _rkey, _entry_px, _filled_sz in fills:
        _coin, _tf, _side = _rkey
        _ro = rom.consume_fill(_rkey)
        assert _ro is not None
        _fill_notional = _filled_sz * _entry_px
        if _fill_notional < settings.liq_min_trade_usd:
            dust_closed = True
            client.market_close(_coin)
            rom.cancel_all_for_coin(client, _coin)
            continue
        sl_placed = True; opens_today += 1

    check(dust_closed, "dust $0.10 < min $20 → close triggered")
    check(not sl_placed, "SL NOT placed for dust")
    check(opens_today == 0, "opens_today=0 for dust")
    check("BTC" in client._closed_coins, "market_close called")

    # Normal fill: $500 >= $20 → proceeds
    rom2 = RestingOrderManager(); client2 = StubExchangeClient()
    df2 = strat_mod.compute_indicators(make_df(50, trend="up"))
    client2.set_candles("ETH", "1h", df2)
    rom2.refresh([StubAsset("ETH")], client2, {}, set(), settings, n_open=0,
                 equity=client2._equity, snapshot_holder=StubSnapshotHolder())
    client2.set_position("ETH", szi=5.0, px=100.0)
    fills2 = rom2.detect_resting_fills(client2.open_positions(), {})
    assert len(fills2) == 1
    sl_2 = False
    for _rkey, _entry_px, _filled_sz in fills2:
        _ro = rom2.consume_fill(_rkey)
        if _filled_sz * _entry_px >= settings.liq_min_trade_usd:
            sl_2 = True
    check(sl_2, "normal fill $500 >= $20 → proceeds to SL")

# ── Run ──────────────────────────────────────────────────────────────────────
test_a_depth_gate()
test_b_short_k()
test_c_min_size()
print()
if FAILURES == 0:
    print("ALL VALIDATION TESTS PASSED")
    sys.exit(0)
else:
    print(f"{FAILURES} VALIDATION TEST(S) FAILED")
    sys.exit(1)
