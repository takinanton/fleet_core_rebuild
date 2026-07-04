"""liquidity_snapshot.py — daily liquidity screen for extended/pacifica bot.

Runs once per day (cron 00:05 UTC). For each coin in current universe:
  - avg_1h_vol_usd:  средний $-volume за час из последних 24h 1h-candles
  - spread_pct:      текущий top-of-book спред (1 snapshot — hint, не gate)
  - depth_top20_usd: суммарный $-notional первых 20 уровней (avg bid+ask)
  - pct_traded_60min: liveness gate (default 95% of last 60 1m bars must trade)

Output: data/liquidity_snapshot.json

Idempotent: skip if snapshot.json fresh (<23h) unless --force.

Exchange dispatch: settings.exchange → ExtendedClient or PacificaClient.
Both adapters MUST expose `.orderbook_snapshot(coin) → (bids, asks)`.

Usage:
  python -m bot.liquidity_snapshot           # write data/liquidity_snapshot.json
  python -m bot.liquidity_snapshot --force   # ignore 23h freshness check
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bot.config import settings
from bot.universe import load_universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("liquidity_snapshot")


def _build_client():
    """Exchange dispatch (mirrors bot/main.py)."""
    if settings.exchange == "pacifica":
        from bot.exchange_pacifica import PacificaClient
        return PacificaClient(settings)
    if settings.exchange == "extended":
        from bot.exchange_extended import ExtendedClient
        return ExtendedClient(settings)
    if settings.exchange == "hyperliquid":
        from bot.exchange_hl import HLClient
        return HLClient(settings)
    raise RuntimeError(f"Unknown exchange={settings.exchange!r}")


def _avg_1h_vol_usd(client, coin: str) -> Optional[float]:
    """Average $-volume за час за последние ~24 закрытых 1h-candles."""
    df = client.candles(coin, "1h", limit=30)
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


def _liveness_pct_60m(client, coin: str) -> Optional[float]:
    """Fraction of last 60 closed 1m bars with volume > 0."""
    try:
        df = client.candles(coin, "1m", limit=70)
    except Exception as e:
        log.warning("liveness 1m fetch %s failed: %s", coin, e)
        return None
    if df is None or df.empty or len(df) < 2:
        return None
    closed = df.iloc[:-1] if len(df) >= 2 else df
    closed = closed.tail(60)
    if closed.empty:
        return None
    vol = closed["Volume"].astype(float)
    bars_traded = int((vol > 0).sum())
    return float(bars_traded) / 60.0


def _orderbook_metrics(client, coin: str) -> tuple:
    """One snapshot of orderbook → (spread_pct, depth_top20_usd).

    Exchange-agnostic: relies on `client.orderbook_snapshot(coin) → (bids, asks)`
    where each is `[(price, qty), ...]` sorted best-first.

    depth_at_half_pct_usd: avg ask+bid notional within 0.5% of mid (see DEPTH_GATE).
    HIP-3: orderbook_snapshot uses coin_to_api (xyz_GOLD->xyz:GOLD, verified 2026-05-31).
    """
    try:
        bids, asks = client.orderbook_snapshot(coin)
    except AttributeError:
        log.warning("client %s missing orderbook_snapshot()", type(client).__name__)
        return None, None, None
    except Exception as e:
        log.warning("orderbook fetch %s failed: %s", coin, e)
        return None, None, None
    if not bids or not asks:
        return None, None, None

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None, None, None
    spread_pct = (best_ask - best_bid) / mid

    bid_depth = sum(px * sz for (px, sz) in bids[:20])
    ask_depth = sum(px * sz for (px, sz) in asks[:20])
    depth_top20_usd = (bid_depth + ask_depth) / 2.0

    # depth within 0.5% of mid (buys=ask side, sells=bid side; average)
    half_pct = 0.005
    ask_05 = sum(px * sz for (px, sz) in asks if px <= mid * (1.0 + half_pct))
    bid_05 = sum(px * sz for (px, sz) in bids if px >= mid * (1.0 - half_pct))
    depth_at_half_pct_usd = (ask_05 + bid_05) / 2.0

    return spread_pct, depth_top20_usd, depth_at_half_pct_usd


def collect_snapshot(client, universe) -> dict:
    coins_out: dict[str, dict] = {}
    ok = 0
    failed = 0
    excluded_liveness = 0
    liveness_floor = float(settings.liveness_min_pct_traded)

    for asset in universe:
        sym = asset.symbol
        try:
            vol = _avg_1h_vol_usd(client, sym)
            spread, depth, depth_05 = _orderbook_metrics(client, sym)
            pct_traded = _liveness_pct_60m(client, sym)
        except Exception as e:
            log.error("snapshot fail %s: %s", sym, e)
            failed += 1
            continue
        if vol is None or vol <= 0:
            log.warning("snapshot skip %s: no 1h volume", sym)
            failed += 1
            continue
        if pct_traded is None:
            log.warning("%s: liveness fetch failed — excluded", sym)
            excluded_liveness += 1
            continue
        if pct_traded < liveness_floor:
            log.info(
                "%s: thin — %.0f%% min traded (<%.0f%% req), excluded",
                sym, pct_traded * 100.0, liveness_floor * 100.0,
            )
            excluded_liveness += 1
            continue
        coins_out[sym] = {
            "avg_1h_vol_usd": round(vol, 2),
            "spread_pct": round(spread, 6) if spread is not None else 0.0,
            "depth_top20_usd": round(depth, 2) if depth is not None else 0.0,
            "depth_at_0.5pct_usd": round(depth_05, 2) if depth_05 is not None else 0.0,
            "pct_traded_60min": round(pct_traded, 4),
        }
        ok += 1

    log.info(
        "Snapshot: %d ok, %d failed, %d excluded by liveness (<%.0f%%)",
        ok, failed, excluded_liveness, liveness_floor * 100.0,
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "universe_size": len(universe),
        "coins": coins_out,
    }


last_fetch_one_reject_reason: Optional[str] = None


def fetch_one(client, coin: str):
    """Inline one-shot liquidity profile (called by trader when coin missing from snapshot)."""
    global last_fetch_one_reject_reason
    last_fetch_one_reject_reason = None
    from bot.liquidity import LiquidityProfile
    try:
        vol = _avg_1h_vol_usd(client, coin)
        spread, depth, depth_05 = _orderbook_metrics(client, coin)
        pct_traded = _liveness_pct_60m(client, coin)
    except Exception as e:
        log.warning("inline snapshot fetch %s failed: %s", coin, e)
        last_fetch_one_reject_reason = "liq_inline_fetch_error"
        return None
    if vol is None or vol <= 0:
        last_fetch_one_reject_reason = "liq_inline_no_volume"
        return None
    floor = float(settings.liveness_min_pct_traded)
    if pct_traded is None:
        log.info("%s: inline liveness fetch failed — rejecting", coin)
        last_fetch_one_reject_reason = "liq_inline_liveness_fail"
        return None
    if pct_traded < floor:
        log.info("%s: inline liveness %.0f%% < %.0f%%", coin, pct_traded * 100.0, floor * 100.0)
        last_fetch_one_reject_reason = "liq_inline_liveness_fail"
        return None
    return LiquidityProfile(
        coin=coin,
        avg_1h_vol_usd=float(vol),
        spread_pct=float(spread) if spread is not None else 0.0,
        depth_top20_usd=float(depth) if depth is not None else 0.0,
        depth_at_half_pct_usd=float(depth_05) if depth_05 is not None else 0.0,
        pct_traded_60min=float(pct_traded),
    )


def write_snapshot(out_path: Path, snapshot: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, sort_keys=True)
    tmp.replace(out_path)


def is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    try:
        age_sec = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return age_sec < (max_age_hours * 3600.0)


def main(force: bool = False) -> int:
    out_path = Path(settings.liq_snapshot_path)
    if not out_path.is_absolute():
        from bot.config import PROJECT_ROOT
        out_path = PROJECT_ROOT / out_path

    if not force and is_fresh(out_path, max_age_hours=23.0):
        log.info("Snapshot at %s is fresh (<23h), skipping. Use --force to override.", out_path)
        return 0

    log.info("Building %s client for snapshot…", settings.exchange)
    client = _build_client()
    universe = load_universe()
    log.info("Snapshotting %d coins", len(universe))

    snap = collect_snapshot(client, universe)
    write_snapshot(out_path, snap)
    log.info("Snapshot written to %s (%d coins)", out_path, len(snap["coins"]))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily liquidity snapshot")
    parser.add_argument("--force", action="store_true", help="Ignore freshness check")
    args = parser.parse_args()
    rc = main(force=args.force)
    import os as _os
    _os._exit(rc)
