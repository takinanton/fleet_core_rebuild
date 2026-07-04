"""universe.py — Hyperliquid perp universe loader.

Sources:
  HL native perps: POST /info {"type":"metaAndAssetCtxs"} — returns [meta, assetCtxs].
    meta.universe[i].name = coin name (BTC, ETH, SOL, ...)
    assetCtxs[i].dayNtlVlm = 24h notional volume in USD
    Filter: dayNtlVlm >= UNIVERSE_MIN_VOL_USD_24H (default $500k)

  HIP-3 (xyz:*): POST /info {"type":"metaAndAssetCtxs","dex":"xyz"}
    Same structure. Coin names: xyz:GOLD, xyz:VIX, xyz:DXY, etc.
    Internal names: xyz_GOLD, xyz_VIX, xyz_DXY.
    Keep all listed xyz that have dayNtlVlm > 0.
    ALWAYS include xyz_VIX and xyz_DXY (regime indicators — skip from trading
    in scanner via SKIP_COINS env var, but include in universe for reference).

Tier classification (matches Extended/Pacifica tiers):
  TIER 1 = majors (BTC/ETH/SOL + top global perp vol)
  TIER 2 = mids (pass vol filter)
  xyz_* are always TIER 2

Universe refreshed every UNIVERSE_REFRESH_MIN min (default 60).
Capped to UNIVERSE_TOP_N if set > 0 (after sorting by tier + vol).

CLI: python -m bot.universe
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests

from bot.config import FX_EXCLUDE, settings
from bot.exchange_hl import api_to_coin, coin_to_api

log = logging.getLogger(__name__)

# XNN port 2026-06-10: crypto-only universe switches (previously hardcoded — no env knob).
#   UNIVERSE_HIP3_ENABLE=0  -> skip the xyz HIP-3 dex fetch AND the regime force-include
#                              (no xyz_* in universe at all). Default 1 = legacy behavior.
#   UNIVERSE_SYMBOL_EXCLUDE -> comma-separated internal symbols to drop (e.g. PAXG —
#                              native gold perp that passes the $1M vol filter).
UNIVERSE_HIP3_ENABLE = os.getenv("UNIVERSE_HIP3_ENABLE", "1").strip().lower() not in ("0", "false", "no", "off")
UNIVERSE_SYMBOL_EXCLUDE: frozenset = frozenset(
    s.strip() for s in os.getenv("UNIVERSE_SYMBOL_EXCLUDE", "").split(",") if s.strip()
)

# US29 trades ONLY HIP-3 xyz_* names (NOT native crypto perps). When set, the native
# HL perp fetch/loop is skipped entirely and ONLY xyz_* (+ regime coins) remain in the
# universe. Requires UNIVERSE_HIP3_ENABLE=1 to have anything to trade.
US29_ONLY_XYZ = os.getenv("US29_ONLY_XYZ", "0").strip().lower() in ("1", "true", "yes", "on")

_HL_INFO_URL = "https://api.hyperliquid.xyz/info"
_REQUEST_TIMEOUT_SEC = 15

# TIER 1 main-perp majors (source: original HL bot universe + HL market dominance)
_TIER1_MAIN: frozenset[str] = frozenset({
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "AVAX", "ADA", "LINK",
    "SUI", "PEPE", "WIF", "TRX", "HYPE", "ARB", "OP", "APT",
})

# Regime indicator coins — always included, NEVER traded.
# These are force-added to the universe so their candles are available for regime/context,
# but the scanner MUST skip them for ENTRY (note='regime_indicator(_forced)' is filtered in
# scanner.scan_all_coins — audit B3). xyz_SP500 is the US29 F4_vb regime proxy (SPY>SMA200
# index gate) — it drives the gate but is itself NEVER entered.
# Override via REGIME_COINS env (comma-separated) if a deploy needs a different set.
_REGIME_COINS_DEFAULT = "xyz_VIX,xyz_DXY,xyz_SP500"
REGIME_COINS: frozenset[str] = frozenset(
    s.strip() for s in os.getenv("REGIME_COINS", _REGIME_COINS_DEFAULT).split(",") if s.strip()
)


@dataclass
class AssetTier:
    symbol: str      # internal name (xyz_GOLD, BTC)
    tier: int        # 1 = major, 2 = mid
    vol_24h_usd: float
    note: str        # "hl_native" | "hl_hip3" | "regime_indicator"


class UniverseLoader:
    def __init__(self) -> None:
        self._cache: Optional[list[AssetTier]] = None
        self._cache_ts: float = 0.0

    def load(self, force_refresh: bool = False) -> list[AssetTier]:
        refresh_interval_sec = settings.universe_refresh_min * 60
        age = time.time() - self._cache_ts
        if not force_refresh and self._cache is not None and age < refresh_interval_sec:
            return self._cache
        result = _fetch_and_filter()
        self._cache = result
        self._cache_ts = time.time()
        return result


_loader = UniverseLoader()


def load_universe(force_refresh: bool = False) -> list[AssetTier]:
    """Public entry — returns filtered, tiered HL universe."""
    return _loader.load(force_refresh=force_refresh)


def _fetch_metaAndAssetCtxs(dex: Optional[str] = None) -> tuple[list, list]:
    """Fetch metaAndAssetCtxs for main dex or a HIP-3 dex.
    Returns (universe_list, asset_ctx_list).
    """
    payload: dict = {"type": "metaAndAssetCtxs"}
    if dex:
        payload["dex"] = dex

    resp = requests.post(_HL_INFO_URL, json=payload, timeout=_REQUEST_TIMEOUT_SEC)
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, list) or len(data) < 2:
        log.warning("metaAndAssetCtxs dex=%s returned unexpected shape", dex)
        return [], []

    meta = data[0]
    asset_ctxs = data[1]
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    if not isinstance(asset_ctxs, list):
        asset_ctxs = []
    return universe, asset_ctxs


def _fetch_and_filter() -> list[AssetTier]:
    """Fetch HL native perps + xyz HIP-3, filter by volume, tier-classify."""
    min_vol = settings.universe_min_vol_usd_24h
    result: list[AssetTier] = []
    skipped_vol = 0
    skipped_fx = 0

    # --- 1. Main HL perps ---
    # US29_ONLY_XYZ: skip native crypto entirely — US29 trades only xyz_* HIP-3 names.
    if US29_ONLY_XYZ:
        log.info("US29_ONLY_XYZ=1 — skipping native HL perps (xyz-only universe)")
        universe, asset_ctxs = [], []
    else:
        log.info("Fetching HL native perps from metaAndAssetCtxs")
        try:
            universe, asset_ctxs = _fetch_metaAndAssetCtxs(dex=None)
        except Exception as e:
            raise RuntimeError(
                f"HL metaAndAssetCtxs (main) failed — cannot load universe: {e}"
            ) from e

    for i, asset in enumerate(universe):
        api_name = asset.get("name", "")
        if not api_name:
            continue
        internal = api_to_coin(api_name)

        if internal in FX_EXCLUDE:
            skipped_fx += 1
            continue
        if internal in UNIVERSE_SYMBOL_EXCLUDE:
            log.debug("SKIP native %s: UNIVERSE_SYMBOL_EXCLUDE", internal)
            continue

        # 24h notional volume from assetCtxs[i].dayNtlVlm
        vol_24h = 0.0
        if i < len(asset_ctxs):
            try:
                vol_24h = float(asset_ctxs[i].get("dayNtlVlm", 0) or 0)
            except (TypeError, ValueError):
                vol_24h = 0.0

        if vol_24h < min_vol:
            skipped_vol += 1
            log.debug("SKIP native %s: dayNtlVlm=$%.0f < $%.0f", internal, vol_24h, min_vol)
            continue

        tier = 1 if internal in _TIER1_MAIN else 2
        result.append(AssetTier(symbol=internal, tier=tier,
                                vol_24h_usd=vol_24h, note="hl_native"))

    # --- 2. HIP-3 xyz perps ---
    if not UNIVERSE_HIP3_ENABLE:
        log.info("UNIVERSE_HIP3_ENABLE=0 — skipping HIP-3 xyz dex (crypto-only universe)")
        xyz_universe, xyz_ctxs = [], []
    else:
        log.info("Fetching HIP-3 xyz perps from metaAndAssetCtxs dex=xyz")
        try:
            xyz_universe, xyz_ctxs = _fetch_metaAndAssetCtxs(dex="xyz")
        except Exception as e:
            log.warning("HIP-3 xyz fetch failed (continuing with main only): %s", e)
            xyz_universe, xyz_ctxs = [], []

    # Track which xyz coins we've added (for regime coin forced-include below)
    xyz_added: set[str] = set()

    for i, asset in enumerate(xyz_universe):
        api_name = asset.get("name", "")
        if not api_name:
            continue
        internal = api_to_coin(api_name)  # "xyz:GOLD" → "xyz_GOLD"

        # FX exclude (HIP-3 forex): match internal name AND base symbol so
        # both "xyz_EUR" and "EUR" exclude "xyz:EUR" (2026-06-07).
        base = internal.split("_", 1)[1] if "_" in internal else internal
        if internal in FX_EXCLUDE or base in FX_EXCLUDE:
            skipped_fx += 1
            continue
        if internal in UNIVERSE_SYMBOL_EXCLUDE or base in UNIVERSE_SYMBOL_EXCLUDE:
            log.debug("SKIP xyz %s: UNIVERSE_SYMBOL_EXCLUDE", internal)
            continue

        vol_24h = 0.0
        if i < len(xyz_ctxs):
            try:
                vol_24h = float(xyz_ctxs[i].get("dayNtlVlm", 0) or 0)
            except (TypeError, ValueError):
                vol_24h = 0.0

        # Regime indicators: always include regardless of volume
        is_regime = internal in REGIME_COINS
        if not is_regime and vol_24h <= 0:
            log.debug("SKIP xyz %s: zero volume", internal)
            continue

        note = "regime_indicator" if is_regime else "hl_hip3"
        result.append(AssetTier(symbol=internal, tier=2,
                                vol_24h_usd=vol_24h, note=note))
        xyz_added.add(internal)

    # Force-include regime coins even if not in metaAndAssetCtxs (delisted detection).
    # XNN port 2026-06-10: skipped when HIP-3 disabled (crypto-only — no xyz_* at all).
    if UNIVERSE_HIP3_ENABLE:
        for rc in REGIME_COINS:
            if rc not in xyz_added:
                log.warning("Regime coin %s not in xyz metaAndAssetCtxs — adding with vol=0", rc)
                result.append(AssetTier(symbol=rc, tier=2, vol_24h_usd=0.0,
                                        note="regime_indicator_forced"))

    # Sort: TIER 1 first, then by vol desc
    result.sort(key=lambda a: (a.tier, -a.vol_24h_usd))

    top_n = settings.universe_top_n
    if top_n > 0:
        # Don't cap if would exclude regime coins — they're needed
        non_regime = [a for a in result if a.symbol not in REGIME_COINS]
        regime_assets = [a for a in result if a.symbol in REGIME_COINS]
        if len(non_regime) > top_n:
            log.info("Universe capped: %d → %d + %d regime (UNIVERSE_TOP_N=%d)",
                     len(non_regime), top_n, len(regime_assets), top_n)
            result = non_regime[:top_n] + regime_assets
        # Re-sort after cap
        result.sort(key=lambda a: (a.tier, -a.vol_24h_usd))

    n_native = sum(1 for a in result if a.note == "hl_native")
    n_hip3 = sum(1 for a in result if a.note in ("hl_hip3", "regime_indicator", "regime_indicator_forced"))
    log.info(
        "HL universe: %d total (native=%d hip3=%d | skipped: vol=%d fx=%d)",
        len(result), n_native, n_hip3, skipped_vol, skipped_fx,
    )

    if not result:
        raise RuntimeError(
            f"HL universe: 0 tradeable markets after filtering. "
            f"Check UNIVERSE_MIN_VOL_USD_24H={min_vol:.0f}"
        )
    return result



def apply_depth_gate(
    universe: list,
    snapshot,          # LiquiditySnapshot | None
    liq_size_cap_pct: float,
    enabled: bool = True,
    worst_order_notional_usd=None,
) -> list:
    """Filter universe to coins where the size-capped order fits within 0.5% depth.

    Gate condition: (liq_size_cap_pct * avg_1h_vol_usd) <= depth_at_half_pct_usd
                    AND depth_at_half_pct_usd > 0

    Coins failing the gate are excluded. Regime indicators (REGIME_COINS) bypass
    the depth gate unconditionally (they are never traded — just context).

    Args:
        universe:          list[AssetTier] from load_universe()
        snapshot:          LiquiditySnapshot (from SnapshotHolder.current()) or None
        liq_size_cap_pct:  settings.liq_size_cap_pct (e.g. 0.05)
        enabled:           DEPTH_GATE_ENABLE env flag (default True)

    Returns filtered list[AssetTier]. If snapshot is None or enabled=False, returns
    input unchanged (fail-open so bot doesn't go dark on missing snapshot).

    Source: validated gate spec — no invented $ floors.
    """
    if not enabled:
        return universe
    if snapshot is None:
        log.debug("apply_depth_gate: snapshot=None — gate bypassed (fail-open)")
        return universe

    # Pre-scan: do ANY universe coins have snapshot data? If none, the snapshot is
    # broken/irrelevant -> fail-open (do not go dark). If some do, missing coins =
    # failed liveness/liquidity collection = excluded under strict-liquidity.
    n_with_profile = sum(
        1 for a in universe
        if a.symbol not in REGIME_COINS and snapshot.get(a.symbol) is not None
    )
    fail_closed = n_with_profile > 0

    kept: list = []
    excluded: list = []
    for asset in universe:
        sym = asset.symbol
        # Regime indicators always pass (never traded, context-only)
        if sym in REGIME_COINS:
            kept.append(asset)
            continue
        profile = snapshot.get(sym)
        if profile is None:
            # Absent from snapshot = failed liveness/liquidity collection = not tradable
            # under strict-liquidity. Fail-CLOSED when snapshot has data; fail-open only
            # when snapshot is entirely irrelevant (n_with_profile==0) to avoid going dark.
            if fail_closed:
                excluded.append(sym)
            else:
                kept.append(asset)
            continue
        size_capped_notional = liq_size_cap_pct * profile.avg_1h_vol_usd
        # Test the REAL max order (risk-based, account-aware), capped by 5%xvol —
        # NOT the abstract 5%xvol cap (which over-rejects coins our small order fills).
        if worst_order_notional_usd is not None:
            test_notional = min(worst_order_notional_usd, size_capped_notional)
        else:
            test_notional = size_capped_notional
        depth = profile.depth_at_half_pct_usd
        if depth > 0 and test_notional <= depth:
            kept.append(asset)
        else:
            excluded.append(sym)
            log.debug(
                "depth_gate: EXCLUDE %s — order=%.0f (cap5pct=%.0f) depth_0.5pct=%.0f",
                sym, test_notional, size_capped_notional, depth,
            )
    if excluded and len(universe) and (len(excluded) / len(universe)) > 0.9:
        log.warning(
            "depth_gate ALARM: excluded %d/%d (>90%%) — snapshot likely lacks depth "
            "(check _source / depth_at_0.5pct_usd). Gate running on empty liquidity.",
            len(excluded), len(universe),
        )
    if excluded:
        log.info(
            "depth_gate: excluded %d/%d coins (size_cap > 0.5%% depth): %s",
            len(excluded), len(universe),
            ", ".join(excluded[:10]) + ("..." if len(excluded) > 10 else ""),
        )
    return kept


def print_universe() -> None:
    logging.basicConfig(level=logging.INFO)
    assets = load_universe(force_refresh=True)
    print(f"\n{'Symbol':<20} {'Tier':<6} {'24h Vol USD':>16}  Note")
    print("-" * 65)
    for a in assets:
        marker = " *REGIME*" if a.symbol in REGIME_COINS else ""
        print(f"{a.symbol:<20} {a.tier:<6} {a.vol_24h_usd:>16,.0f}  {a.note}{marker}")
    print(f"\nTotal: {len(assets)} symbols "
          f"(native={sum(1 for a in assets if a.note=='hl_native')} "
          f"hip3={sum(1 for a in assets if 'hip3' in a.note or 'regime' in a.note)})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HL universe loader")
    parser.add_argument("--print", action="store_true", help="Print universe and exit")
    args = parser.parse_args()
    print_universe()
