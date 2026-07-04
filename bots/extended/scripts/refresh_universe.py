"""refresh_universe.py — weekly cron: re-rank Nado perps by 1h volume → tier list.

Fetches live data from Nado, measures depth-walk slip for all symbols,
updates data/universe_tiers.json (read by universe.py on next restart).

Run: python scripts/refresh_universe.py
Cron (weekly Sunday 00:05 UTC):
  5 0 * * 0 /root/nado_bot_v2/venv/bin/python /root/nado_bot_v2/scripts/refresh_universe.py >> /root/nado_bot_v2/data/refresh_universe.log 2>&1
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "universe_tiers.json"
TEST_NOTIONAL = 5_000.0   # $5k test to measure depth-walk slip


def main() -> None:
    from bot.config import settings
    from bot.exchange_nado import NadoClient as NadoClient_
    from bot.config import FX_EXCLUDE

    log.info("refresh_universe starting")
    client = NadoClient_(settings)

    try:
        symbols_raw = client._sdk.market.get_all_product_symbols()
        all_syms = sorted(s.symbol for s in symbols_raw if s.symbol and "PERP" in s.symbol.upper())
    except Exception as e:
        log.error("Failed to load symbols: %s", e)
        sys.exit(1)

    results = {}
    for sym in all_syms:
        # Skip FX
        if sym in FX_EXCLUDE:
            continue

        # Measure 1h vol and depth-walk slip
        try:
            df = client.candles(sym, "1h", limit=5)
            if df is None or len(df) < 2:
                vol_1h = 0.0
            else:
                row = df.iloc[-2]
                vol_1h = float(row["Volume"]) * float(row["Close"])
        except Exception as e:
            log.warning("1h candles(%s): %s", sym, e)
            vol_1h = 0.0

        # Depth walk
        slip = None
        try:
            from bot.liquidity import _fetch_orderbook, _wavg_price
            book = _fetch_orderbook(client, sym, depth=20)
            if book is not None:
                bids, asks = book
                if asks and bids:
                    mid = (bids[0][0] + asks[0][0]) / 2.0
                    avg_px = _wavg_price(asks, TEST_NOTIONAL)
                    if avg_px is not None and mid > 0:
                        slip = (avg_px - mid) / mid
        except Exception as e:
            log.debug("depth walk(%s): %s", sym, e)

        tier = 3  # default: thin
        if slip is not None:
            if slip <= 0.0015:
                tier = 1
            elif slip <= 0.0030:
                tier = 2
        elif vol_1h >= 500_000:
            tier = 2   # high vol without book data → assume liquid
        elif vol_1h >= 50_000:
            tier = 2

        results[sym] = {
            "tier": tier,
            "vol_1h_usd": round(vol_1h, 2),
            "depth_walk_slip_pct": round(slip * 100, 4) if slip is not None else None,
        }
        time.sleep(0.1)  # rate limit

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(results, indent=2))
    t1 = sum(1 for v in results.values() if v["tier"] == 1)
    t2 = sum(1 for v in results.values() if v["tier"] == 2)
    t3 = sum(1 for v in results.values() if v["tier"] == 3)
    log.info(
        "refresh_universe done: %d symbols (T1=%d T2=%d T3-excl=%d) → %s",
        len(results), t1, t2, t3, OUTPUT_PATH,
    )


if __name__ == "__main__":
    main()
