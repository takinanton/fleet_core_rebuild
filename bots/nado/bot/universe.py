"""universe.py — load Nado perp list, filter FX, apply 24h $-volume floor.

Refactor 2026-05-24 (post-deploy): hand-curated TIER lists removed; selection
is now data-driven via UNIVERSE_MIN_VOL_USD_24H floor (default $500K).

Selection pipeline:
  1. Symbol must contain "PERP" (Nado SDK product list).
  2. Symbol not in FX_EXCLUDE and not heuristic-FX (currency pair length 6).
  3. avg(1h $-volume over last 24 closed 1h candles) * 24 >= floor.

Mirrors Extended bot's data-driven selection style — no per-symbol manual
curation as new Nado perps list. Sizing discipline still comes from
settings.liq_size_cap_pct against the daily liquidity snapshot
(see bot/liquidity_snapshot.py).

CLI: python -m bot.universe --print
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import Optional

from bot.config import FX_EXCLUDE, settings

log = logging.getLogger(__name__)

# XNN port 2026-06-11 (foreign-safety, mirror of HL canon UNIVERSE_SYMBOL_EXCLUDE):
# comma-separated symbols to drop from the universe (exact match OR base name, e.g.
# "BTC" matches "BTC-PERP"). Use at LIVE flip to fence off coins where the old
# nado-bot-v2 / manual uk_signal_place positions live on the SAME subaccount —
# Nado merges same-product positions per subaccount, so an XNN entry on such a coin
# would silently average into the foreign position.
UNIVERSE_SYMBOL_EXCLUDE: frozenset[str] = frozenset(
    s.strip().upper() for s in os.getenv("UNIVERSE_SYMBOL_EXCLUDE", "").split(",") if s.strip()
)


def _is_excluded_symbol(sym: str) -> bool:
    if not UNIVERSE_SYMBOL_EXCLUDE:
        return False
    s = sym.upper()
    base = s.replace("-PERP", "")
    return s in UNIVERSE_SYMBOL_EXCLUDE or base in UNIVERSE_SYMBOL_EXCLUDE


@dataclass
class AssetTier:
    """Universe entry. `tier` retained at fixed value 2 for back-compat with
    callers (main.py log + journal) that still read `.tier`; selection no
    longer depends on it."""
    symbol: str
    tier: int      # always 2 post-refactor
    vol_24h_usd: float
    note: str


def _is_fx(sym: str) -> bool:
    """Heuristic FX detection for symbols not in explicit exclude set."""
    base = sym.replace("-PERP", "").replace("/", "").upper()
    _CCY = {
        "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "SEK", "NOK",
        "DKK", "HKD", "SGD", "MXN", "ZAR", "TRY", "PLN", "HUF",
    }
    if len(base) == 6:
        left, right = base[:3], base[3:]
        if left in _CCY or right in _CCY:
            return True
    return False


def _avg_1h_vol_usd(client, coin: str) -> Optional[float]:
    """Avg $-volume per 1h-bar over last 24 closed 1h candles.

    Mirror of bot/liquidity_snapshot.py:_avg_1h_vol_usd so universe-load and
    snapshot collection use the same metric.
    """
    try:
        df = client.candles(coin, "1h", limit=30)
    except Exception as e:
        log.warning("UNIVERSE vol fetch %s failed: %s", coin, e)
        return None
    if df is None or df.empty or len(df) < 2:
        return None
    closed = df.iloc[:-1] if len(df) >= 2 else df
    closed = closed.tail(24)
    if closed.empty:
        return None
    vol_usd = (closed["Volume"] * closed["Close"]).astype(float)
    vol_usd = vol_usd[vol_usd > 0]
    if vol_usd.empty:
        return None
    return float(vol_usd.mean())


def load_universe(client) -> list[AssetTier]:
    """Query live Nado markets, filter FX, apply $-volume floor.

    Volume floor = settings.universe_min_vol_usd_24h (default $500K). Each
    coin's avg 1h $-vol × 24 ≈ daily $-vol → compared to the floor.
    """
    try:
        symbols_raw = client._sdk.market.get_all_product_symbols()
        all_syms = sorted(s.symbol for s in symbols_raw if s.symbol and "PERP" in s.symbol.upper())
    except Exception as e:
        # Fail loud — bot must not trade with unknown universe.
        raise RuntimeError(
            f"Nado universe fetch FAILED — cannot determine tradeable markets: {e}"
        ) from e

    if not all_syms:
        raise RuntimeError(
            "Nado universe API returned empty list — check SDK or network"
        )

    floor_usd_24h = float(settings.universe_min_vol_usd_24h)
    floor_avg_1h = floor_usd_24h / 24.0
    result: list[AssetTier] = []
    excluded_fx = 0
    excluded_vol = 0
    excluded_no_vol = 0

    for sym in all_syms:
        # FX exclusion (user hard constraint)
        if sym in FX_EXCLUDE or _is_fx(sym):
            excluded_fx += 1
            log.debug("UNIVERSE EXCLUDE FX: %s", sym)
            continue

        # Explicit symbol exclude (XNN port 2026-06-11 — foreign-position fencing)
        if _is_excluded_symbol(sym):
            log.info("UNIVERSE EXCLUDE (UNIVERSE_SYMBOL_EXCLUDE): %s", sym)
            continue

        avg_1h = _avg_1h_vol_usd(client, sym)
        if avg_1h is None:
            excluded_no_vol += 1
            log.debug("UNIVERSE EXCLUDE %s: no 1h volume data", sym)
            continue
        daily_vol = avg_1h * 24.0
        if avg_1h < floor_avg_1h:
            excluded_vol += 1
            log.debug(
                "UNIVERSE EXCLUDE %s: daily_vol=$%.0f < floor=$%.0f",
                sym, daily_vol, floor_usd_24h,
            )
            continue

        result.append(AssetTier(symbol=sym, tier=2, vol_24h_usd=daily_vol, note="vol_floor_pass"))

    result.sort(key=lambda a: -a.vol_24h_usd)
    log.info(
        "Universe loaded: %d tradeable (vol>=$%.0f excluded: %d, no_vol: %d, FX excluded: %d)",
        len(result),
        floor_usd_24h,
        excluded_vol,
        excluded_no_vol,
        excluded_fx,
    )

    if not result:
        raise RuntimeError(
            f"Nado universe: 0 tradeable markets after filtering. "
            f"Check UNIVERSE_MIN_VOL_USD_24H=${floor_usd_24h:.0f}."
        )

    return result


def print_universe(client) -> None:
    """CLI: print universe table and FX sanity check."""
    tiers = load_universe(client)
    print(f"{'Symbol':<22} {'24h Vol USD':>14}  Note")
    print("-" * 55)
    for a in tiers:
        print(f"{a.symbol:<22} {a.vol_24h_usd:>14,.0f}  {a.note}")
    print(f"\nTotal: {len(tiers)} symbols")
    fx_leak = [a.symbol for a in tiers if _is_fx(a.symbol)]
    if fx_leak:
        print(f"\nWARNING: possible FX leak: {fx_leak}")
    else:
        print("FX check: clean")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true")
    args = parser.parse_args()
    if args.print:
        from bot.config import settings
        from bot.exchange_nado import NadoClient as NadoClient_
        logging.basicConfig(level=logging.INFO)
        client = NadoClient_(settings)
        print_universe(client)
