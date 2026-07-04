"""universe.py — load Pacifica perp market list, filter inactive/FX/thin, tier-classify.

Pacifica info endpoint (source: legacy + user spec):
  GET https://api.pacifica.fi/api/v1/info
  Response: {"success": true, "data": [{symbol, max_leverage, tick_size, lot_size,
                                        min_order_size, ...}, ...]}

  GET https://api.pacifica.fi/api/v1/info/prices
  Response: {"success": true, "data": [{symbol, mark, volume_24h, funding, ...}, ...]}

Tier classification (parity with Extended universe):
  TIER 1 = majors (BTC/ETH/SOL + top global perp markets by 24h vol)
  TIER 2 = mids meeting UNIVERSE_MIN_VOL_USD_24H ($1M default)
  TIER 3 = thin — excluded entirely

Symbol handling:
  Pacifica symbols are bare ("BTC", "SOL", "HYPE") — no "-USD" suffix.

Universe refreshed every UNIVERSE_REFRESH_MIN min (default 60). Capped to
UNIVERSE_TOP_N (default 30) after vol filter.

Fail mode: API unreachable → raise (no silent fallback) — matches Extended.

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

log = logging.getLogger(__name__)

# XNN port 2026-06-11 (canon §0#7-lite; the HIP-3 part is not applicable on Pacifica):
# comma-separated symbols excluded from the universe regardless of volume.
_SYMBOL_EXCLUDE: frozenset[str] = frozenset(
    s.strip().upper() for s in os.getenv("UNIVERSE_SYMBOL_EXCLUDE", "").split(",") if s.strip()
)

_INFO_URL = "https://api.pacifica.fi/api/v1/info"
_PRICES_URL = "https://api.pacifica.fi/api/v1/info/prices"
_REQUEST_TIMEOUT_SEC = 15

# Pacifica TIER 1 majors. Pacifica perp universe ~ 30-50 markets, very
# similar to Hyperliquid roster. SOL kept as TIER 1 explicitly (native chain).
_TIER1_BARE: frozenset[str] = frozenset({
    "BTC", "ETH", "SOL", "XRP", "DOGE",
    "BNB", "AVAX", "ADA", "LINK", "HYPE",
    "SUI", "PEPE", "WIF", "TRX",
})


@dataclass
class AssetTier:
    symbol: str
    tier: int
    vol_24h_usd: float
    note: str


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
    """Public entry — returns filtered, tiered universe."""
    return _loader.load(force_refresh=force_refresh)


def _fetch_and_filter() -> list[AssetTier]:
    """Fetch Pacifica /info + /info/prices, merge by symbol, filter, tier-classify."""
    log.info("Fetching Pacifica universe from %s", _INFO_URL)

    # 1) Markets (perp instrument list)
    try:
        resp = requests.get(_INFO_URL, timeout=_REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        raise RuntimeError(
            f"Pacifica /info fetch FAILED — cannot determine tradeable markets. {e}"
        ) from e
    markets_raw = body.get("data") if isinstance(body, dict) else body
    if not markets_raw:
        raise RuntimeError(f"Pacifica /info returned empty list from {_INFO_URL}")

    # 2) Prices (24h volume per symbol)
    try:
        pr = requests.get(_PRICES_URL, timeout=_REQUEST_TIMEOUT_SEC)
        pr.raise_for_status()
        prices_body = pr.json()
        prices = prices_body.get("data") if isinstance(prices_body, dict) else prices_body
        prices = prices or []
    except Exception as e:
        log.warning("Pacifica /info/prices fetch failed — proceeding with 0 vols: %s", e)
        prices = []

    vol_by_sym: dict[str, float] = {}
    for p in prices:
        sym = p.get("symbol")
        if not sym:
            continue
        try:
            vol_by_sym[sym] = float(p.get("volume_24h", 0) or 0)
        except (TypeError, ValueError):
            vol_by_sym[sym] = 0.0

    result: list[AssetTier] = []
    skipped_inactive = 0
    skipped_vol = 0
    skipped_fx = 0

    for m in markets_raw:
        sym = str(m.get("symbol") or "")
        if not sym:
            continue

        # Active filter — Pacifica may use isActive / status fields
        active = m.get("is_active", m.get("isActive", m.get("status", "active")))
        if active is False or str(active).lower() in ("false", "inactive", "delisted"):
            skipped_inactive += 1
            continue

        # FX exclusion (defensive — Pacifica has no FX perps but kept for safety)
        if sym in FX_EXCLUDE:
            skipped_fx += 1
            continue

        # Explicit symbol exclusion (xnn port 2026-06-11, UNIVERSE_SYMBOL_EXCLUDE env)
        if sym.upper() in _SYMBOL_EXCLUDE:
            skipped_fx += 1
            log.info("UNIVERSE skip %s: UNIVERSE_SYMBOL_EXCLUDE", sym)
            continue

        vol_24h = vol_by_sym.get(sym, 0.0)
        if vol_24h < settings.universe_min_vol_usd_24h:
            skipped_vol += 1
            log.debug("UNIVERSE skip %s: vol_24h=$%.0f < $%.0f",
                      sym, vol_24h, settings.universe_min_vol_usd_24h)
            continue

        tier = 1 if sym in _TIER1_BARE else 2
        result.append(AssetTier(symbol=sym, tier=tier,
                                vol_24h_usd=vol_24h, note="api_active"))

    result.sort(key=lambda a: (a.tier, -a.vol_24h_usd))

    top_n = settings.universe_top_n
    if top_n > 0 and len(result) > top_n:
        log.info("Universe capped: %d → %d (UNIVERSE_TOP_N=%d)", len(result), top_n, top_n)
        result = result[:top_n]

    log.info(
        "Pacifica universe: %d tradeable (TIER1=%d TIER2=%d | skipped: inactive=%d vol=%d fx=%d)",
        len(result),
        sum(1 for a in result if a.tier == 1),
        sum(1 for a in result if a.tier == 2),
        skipped_inactive, skipped_vol, skipped_fx,
    )

    if not result:
        raise RuntimeError(
            f"Pacifica universe: 0 tradeable markets after filtering. "
            f"Check UNIVERSE_MIN_VOL_USD_24H={settings.universe_min_vol_usd_24h:.0f}"
        )
    # ── Durable TradFi-drift guard (2026-06-22) ──────────────────────────────
    # Pacifica has NO category field and keeps listing tokenized stock/commodity
    # perps; funding/leverage do NOT separate crypto from TradFi -> no programmatic
    # discriminator. Known TradFi live in UNIVERSE_SYMBOL_EXCLUDE. WARN (does NOT
    # block -- breadth preserved) on any accepted symbol outside the known-crypto
    # baseline: a NEW such symbol may be a fresh TradFi listing to add to the exclude
    # before it accrues enough bars to trade. crypto-only / fleet_perp_only mandate.
    _CRYPTO_BASELINE = frozenset({
        "2Z","AAVE","ADA","ARB","ASTER","AVAX","BCH","BP","BTC","CHIP","CRV","DOGE",
        "ENA","ETH","FARTCOIN","HYPE","ICP","JUP","LDO","LINK","LIT","LTC","MEGA",
        "MON","PAXG","PENGU","PIPPIN","PUMP","SOL","STRK","SUI","TAO","TRUMP","UNI",
        "WIF","WLD","WLFI","XMR","XPL","XRP","ZEC","ZK","ZRO","KBONK","KPEPE",
    })
    _unverified = sorted({a.symbol for a in result if a.symbol.upper() not in _CRYPTO_BASELINE})
    if _unverified:
        log.warning("UNVERIFIED universe symbols (not in crypto baseline) -- REVIEW for TradFi, "
                    "add to UNIVERSE_SYMBOL_EXCLUDE if non-crypto: %s", _unverified)
    # -------------------------------------------------------------------------
    return result


def print_universe() -> None:
    logging.basicConfig(level=logging.INFO)
    assets = load_universe(force_refresh=True)
    print(f"\n{'Symbol':<16} {'Tier':<6} {'24h Vol USD':>14}  Note")
    print("-" * 55)
    for a in assets:
        print(f"{a.symbol:<16} {a.tier:<6} {a.vol_24h_usd:>14,.0f}  {a.note}")
    print(f"\nTotal: {len(assets)} symbols")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pacifica universe loader")
    parser.add_argument("--print", action="store_true", help="Print universe and exit")
    args = parser.parse_args()
    print_universe()

# P1c fence-parity (2026-07-02): canonical orphan_sweep imports UNIVERSE_SYMBOL_EXCLUDE
# from bot.universe; this venue named it _SYMBOL_EXCLUDE, so the fence import raised
# ImportError and the exclude-fence was silently skipped in the orphan sweep. Public alias
# closes the gap — same object, universe filtering unchanged.
UNIVERSE_SYMBOL_EXCLUDE = _SYMBOL_EXCLUDE
