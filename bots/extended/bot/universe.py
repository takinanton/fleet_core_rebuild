"""universe.py — load Extended perp market list, filter inactive/FX/thin, tier-classify by 24h volume.

Extended API endpoint (source: user spec):
  GET https://api.starknet.extended.exchange/api/v1/info/markets
  Response JSON: list of market objects
    active: bool
    marketStats.dailyVolume: str (USD)
    name: str e.g. "BTC-USD"

Tier classification:
  TIER 1 = majors (BTC/ETH/SOL + top-5 by vol)
  TIER 2 = mids meeting UNIVERSE_MIN_VOL_USD_24H ($1M default)
  TIER 3 = thin/inactive — excluded entirely

Symbol handling:
  Extended names: "BTC-USD", "HYPE-USD", "SOL-USD"
  Bot-internal: stripped form "BTC", "HYPE", "SOL"
    (exchange_extended._market() accepts both bare + "-USD" suffix)

Universe is refreshed every UNIVERSE_REFRESH_MIN minutes (default 60).
UNIVERSE_TOP_N caps to top-N by 24h volume (default 30) after vol filter.

Fail mode: if API unreachable, raise loudly — do NOT silently pass with empty universe.
Source: feedback_no_invented_uk_parameters — no fake data sources.

CLI: python -m bot.universe
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests

from bot.config import FX_EXCLUDE, settings

log = logging.getLogger(__name__)

_MARKETS_URL = "https://api.starknet.extended.exchange/api/v1/info/markets"
_REQUEST_TIMEOUT_SEC = 15

# XNN patch 2026-06-11 (canon §0.7, adapted): Extended universe includes TradFi perps
# (category != Crypto: MSFT_24_5, metals, indices). XNN is certified on CRYPTO pools
# only -> crypto-only gate + explicit symbol exclude list.
import os as _os
UNIVERSE_CRYPTO_ONLY: bool = _os.getenv("UNIVERSE_CRYPTO_ONLY", "").strip().lower() in ("1", "true", "yes", "on")
UNIVERSE_SYMBOL_EXCLUDE: frozenset = frozenset(
    s.strip() for s in _os.getenv("UNIVERSE_SYMBOL_EXCLUDE", "").split(",") if s.strip()
)

# Known TIER 1 — majors (source: user spec + legacy exchange_extended.py slip calibration)
_TIER1_BARE: frozenset[str] = frozenset({
    "BTC", "ETH", "SOL", "XRP", "DOGE",
    "BNB", "AVAX", "ADA", "LINK", "UNI",
})


@dataclass
class AssetTier:
    symbol: str    # bare coin e.g. "BTC" (what ExtendedClient._market() accepts)
    tier: int      # 1 or 2
    vol_24h_usd: float
    note: str
    is_crypto: bool = True


class UniverseLoader:
    """Cached universe loader with periodic refresh."""

    def __init__(self) -> None:
        self._cache: Optional[list[AssetTier]] = None
        self._cache_ts: float = 0.0

    def load(self, force_refresh: bool = False) -> list[AssetTier]:
        """Return current universe, refreshing if cache is stale."""
        refresh_interval_sec = settings.universe_refresh_min * 60
        age = time.time() - self._cache_ts
        if not force_refresh and self._cache is not None and age < refresh_interval_sec:
            return self._cache

        result = _fetch_and_filter()
        self._cache = result
        self._cache_ts = time.time()
        return result


# Module-level singleton used by main.py
_loader = UniverseLoader()


def load_universe(force_refresh: bool = False) -> list[AssetTier]:
    """Public entry — returns filtered, tiered universe.

    Raises RuntimeError if API is unreachable (no silent fallback — bot must not trade
    with unknown universe).
    """
    return _loader.load(force_refresh=force_refresh)


def _fetch_and_filter() -> list[AssetTier]:
    """Fetch Extended markets API, filter, tier-classify, cap to TOP_N."""
    log.info("Fetching Extended universe from %s", _MARKETS_URL)
    try:
        resp = requests.get(_MARKETS_URL, timeout=_REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        markets_raw = resp.json()
    except Exception as e:
        # Fail loud — do NOT return empty list silently (source: project memory)
        raise RuntimeError(
            f"Extended universe fetch FAILED — cannot determine tradeable markets. "
            f"Bot should not trade with unknown universe. Error: {e}"
        ) from e

    if not isinstance(markets_raw, list):
        # API may nest under a key
        if isinstance(markets_raw, dict):
            markets_raw = (
                markets_raw.get("data")
                or markets_raw.get("markets")
                or markets_raw.get("result")
                or []
            )
    if not markets_raw:
        raise RuntimeError(
            f"Extended universe API returned empty list from {_MARKETS_URL} — "
            f"check endpoint or network"
        )

    result: list[AssetTier] = []
    skipped_inactive = 0
    skipped_vol = 0
    skipped_fx = 0

    for m in markets_raw:
        name = str(m.get("name") or m.get("market") or "")
        if not name:
            continue

        # Active filter
        active = m.get("active", True)
        if active is False or str(active).lower() == "false":
            skipped_inactive += 1
            continue

        # Strip "-USD" suffix for internal symbol
        if name.endswith("-USD"):
            bare = name[:-4]
        else:
            bare = name

        # Perp-only fleet: exclude SPOT markets (BTCSPOT/ETHSPOT/USDTSPOT carry
        # category=Crypto so they pass the crypto-only gate below). Class-safe:
        # any future SPOT-type listing is auto-excluded. fleet_perp_only memory.
        _mtype = str(m.get("marketType") or m.get("type") or "").strip().upper()
        if _mtype == "SPOT" or bare.endswith("SPOT"):
            skipped_fx += 1
            continue

        # FX exclusion (incl. FOREIGN_EXCLUDE_COINS merged in config.py — flip safety)
        if bare in FX_EXCLUDE or name in FX_EXCLUDE:
            skipped_fx += 1
            continue

        # XNN patch 2026-06-11: explicit symbol exclude (canon §0.7)
        if bare in UNIVERSE_SYMBOL_EXCLUDE or name in UNIVERSE_SYMBOL_EXCLUDE:
            skipped_fx += 1
            continue

        # Parse 24h volume
        stats = m.get("marketStats") or m.get("market_stats") or m.get("stats") or {}
        vol_str = (
            stats.get("dailyVolume")
            or stats.get("daily_volume")
            or stats.get("volume24h")
            or m.get("dailyVolume")
            or "0"
        )
        try:
            vol_24h = float(vol_str) if vol_str else 0.0
        except (ValueError, TypeError):
            vol_24h = 0.0

        # Volume floor
        if vol_24h < settings.universe_min_vol_usd_24h:
            skipped_vol += 1
            log.debug("UNIVERSE skip %s: vol_24h=$%.0f < $%.0f", bare, vol_24h, settings.universe_min_vol_usd_24h)
            continue

        tier = 1 if bare in _TIER1_BARE else 2
        category = str(m.get("category") or "")
        is_crypto = category.strip().lower() == "crypto"
        # XNN patch 2026-06-11: crypto-only universe (XNN certified on crypto pools;
        # blocks TradFi perps like MSFT_24_5 that pass the vol floor)
        if UNIVERSE_CRYPTO_ONLY and not is_crypto:
            skipped_fx += 1
            continue
        result.append(AssetTier(symbol=bare, tier=tier, vol_24h_usd=vol_24h, note=f"api_active", is_crypto=is_crypto))

    # Sort by tier then vol desc (majors first, then by size)
    result.sort(key=lambda a: (a.tier, -a.vol_24h_usd))

    # Cap to TOP_N if configured
    top_n = settings.universe_top_n
    if top_n > 0 and len(result) > top_n:
        log.info(
            "Universe capped: %d → %d (UNIVERSE_TOP_N=%d)",
            len(result), top_n, top_n,
        )
        result = result[:top_n]

    log.info(
        "Universe loaded: %d tradeable (TIER1=%d TIER2=%d | skipped: inactive=%d vol=%d fx=%d)",
        len(result),
        sum(1 for a in result if a.tier == 1),
        sum(1 for a in result if a.tier == 2),
        skipped_inactive, skipped_vol, skipped_fx,
    )

    if not result:
        raise RuntimeError(
            f"Extended universe: 0 tradeable markets after filtering. "
            f"Check UNIVERSE_MIN_VOL_USD_24H={settings.universe_min_vol_usd_24h:.0f} and API response."
        )

    return result


def print_universe() -> None:
    """CLI: print tier table."""
    logging.basicConfig(level=logging.INFO)
    assets = load_universe(force_refresh=True)
    print(f"\n{'Symbol':<16} {'Tier':<6} {'24h Vol USD':>14}  Note")
    print("-" * 55)
    for a in assets:
        print(f"{a.symbol:<16} {a.tier:<6} {a.vol_24h_usd:>14,.0f}  {a.note}")
    print(f"\nTotal: {len(assets)} symbols")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extended universe loader")
    parser.add_argument("--print", action="store_true", help="Print universe and exit")
    args = parser.parse_args()
    if args.print:
        print_universe()
    else:
        print_universe()
