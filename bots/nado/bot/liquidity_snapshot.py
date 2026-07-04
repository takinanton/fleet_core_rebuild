"""liquidity_snapshot.py — daily liquidity screen of universe.

Runs once per day (cron 00:05 UTC). For each coin in current universe:
  - avg_1h_vol_usd: средний $-volume за час из последних 24h 1h-candles
  - spread_pct: текущий top-of-book спред (1 snapshot — hint, не gate)
  - depth_top20_usd: суммарный $-notional первых 20 уровней (avg bid+ask)

Output: data/liquidity_snapshot.json

Idempotent: если snapshot.json уже есть и age < 23h — skip (no API calls).

Fail-safe: если конкретная монета падает на API — log error, continue;
весь script НЕ падает целиком.

Usage:
  python -m bot.liquidity_snapshot           # write data/liquidity_snapshot.json
  python -m bot.liquidity_snapshot --force   # ignore 23h freshness check

Source: refactor 2026-05-24 — replaces per-trade runtime liquidity gates.
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
from bot.exchange_nado import NadoClient as NadoClient_
from bot.universe import load_universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("liquidity_snapshot")


def _avg_1h_vol_usd(client, coin: str) -> Optional[float]:
    """Average $-volume за час, по последним 24 закрытым 1h-candles.

    Returns None если не получилось fetch'нуть candles или мало баров.
    """
    df = client.candles(coin, "1h", limit=30)
    if df is None or df.empty or len(df) < 2:
        return None
    # Last bar may be forming — drop, use up to 24 prior closed
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
    """Fraction of last 60 closed 1m bars that had any traded volume > 0.

    Used to filter dead/thin coins out of the snapshot — a coin trading in
    only e.g. 40% of the last hour's minutes is not safe to size against the
    avg-1h $-vol metric (the average lies). See user spec 2026-05-24.

    Returns None on fetch failure / insufficient bars — caller treats that as
    a fail and excludes the coin.
    """
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


def _orderbook_metrics(client, coin: str) -> tuple[Optional[float], Optional[float]]:
    """One snapshot of orderbook → (spread_pct, depth_top20_usd).

    depth_top20_usd = sum(price × size) для до 20 уровней с КАЖДОЙ стороны,
    усреднённое (bid+ask)/2. Это hint для visibility — не gate.

    Returns (None, None) если orderbook fetch failed.
    """
    X18 = 10**18
    try:
        pid = client._pid(coin)
        engine = client._sdk.context.engine_client
        book = engine.get_market_liquidity(product_id=pid, depth=20)
    except Exception as e:
        log.warning("orderbook fetch %s failed: %s", coin, e)
        return None, None

    def _parse_side(raw_side):
        result = []
        for lvl in (raw_side or []):
            try:
                px = int(lvl[0]) / X18
                sz = int(lvl[1]) / X18
                if px > 0 and sz > 0:
                    result.append((px, sz))
            except Exception:
                continue
        return result

    bids = sorted(_parse_side(getattr(book, "bids", [])), key=lambda x: -x[0])
    asks = sorted(_parse_side(getattr(book, "asks", [])), key=lambda x: x[0])
    if not bids or not asks:
        return None, None

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None, None
    spread_pct = (best_ask - best_bid) / mid

    bid_depth = sum(px * sz for (px, sz) in bids[:20])
    ask_depth = sum(px * sz for (px, sz) in asks[:20])
    depth_top20_usd = (bid_depth + ask_depth) / 2.0
    return spread_pct, depth_top20_usd


def collect_snapshot(client, universe) -> dict:
    """Walk universe and collect liquidity + liveness metrics per coin.

    Errors on one coin do not abort the run; that coin is skipped with a
    WARNING log line.

    Liveness gate: a coin is EXCLUDED from snapshot output entirely if it
    traded in < settings.liveness_min_pct_traded of the last 60 1m bars
    (default 95% = 57/60). Excluded coins log their pct for visibility.
    """
    coins_out: dict[str, dict] = {}
    ok = 0
    failed = 0
    excluded_liveness = 0
    liveness_floor = float(settings.liveness_min_pct_traded)

    for asset in universe:
        sym = asset.symbol
        try:
            vol = _avg_1h_vol_usd(client, sym)
            spread, depth = _orderbook_metrics(client, sym)
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
            log.warning(
                "%s: liveness 1m fetch failed — excluded from snapshot",
                sym,
            )
            excluded_liveness += 1
            continue
        if pct_traded < liveness_floor:
            log.info(
                "%s: thin/dead — only %.0f%% min traded (<%.0f%% req), excluded",
                sym, pct_traded * 100.0, liveness_floor * 100.0,
            )
            excluded_liveness += 1
            continue
        coins_out[sym] = {
            "avg_1h_vol_usd": round(vol, 2),
            "spread_pct": round(spread, 6) if spread is not None else 0.0,
            "depth_top20_usd": round(depth, 2) if depth is not None else 0.0,
            "pct_traded_60min": round(pct_traded, 4),
        }
        ok += 1

    log.info(
        "Snapshot collection: %d ok, %d failed/skipped, %d excluded by liveness (<%.0f%%)",
        ok, failed, excluded_liveness, liveness_floor * 100.0,
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "universe_size": len(universe),
        "coins": coins_out,
    }


# Per-call sidecar for fetch_one — tells trader the precise reject reason
# (liveness vs vol vs fetch_err) without changing the Optional[Profile] return type.
# Single-process bot, so module-level is fine. Always cleared at entry of fetch_one.
last_fetch_one_reject_reason: Optional[str] = None


def fetch_one(client, coin: str):
    """Inline one-shot liquidity profile for a single coin.

    Called by trader.py via SnapshotHolder.fetch_inline when a coin is missing
    from the daily snapshot (e.g. the 00:05 UTC cron's 1h-candles fetch returned
    empty for it that morning). Returns a LiquidityProfile or None on failure.

    On failure, sets module-level `last_fetch_one_reject_reason` to one of:
      - "liq_inline_fetch_error"     — exception during candle/orderbook fetch
      - "liq_inline_no_volume"       — 1h-vol returned None / 0
      - "liq_inline_liveness_fail"   — pct_traded_60min < liveness_min_pct_traded
                                       OR 1m fetch failed
    Trader can read this immediately after a None return to log a precise reject.

    Honors the same liveness gate as the daily snapshot — coins trading in
    less than settings.liveness_min_pct_traded of the last 60 1m bars are
    rejected.

    Imported lazily inside to avoid a circular import (liquidity.py is used by
    main.py before liquidity_snapshot is imported in some paths).
    """
    global last_fetch_one_reject_reason
    last_fetch_one_reject_reason = None
    from bot.liquidity import LiquidityProfile
    try:
        vol = _avg_1h_vol_usd(client, coin)
        spread, depth = _orderbook_metrics(client, coin)
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
        log.info(
            "%s: inline liveness %.0f%% < %.0f%% req — rejecting",
            coin, pct_traded * 100.0, floor * 100.0,
        )
        last_fetch_one_reject_reason = "liq_inline_liveness_fail"
        return None
    return LiquidityProfile(
        coin=coin,
        avg_1h_vol_usd=float(vol),
        spread_pct=float(spread) if spread is not None else 0.0,
        depth_top20_usd=float(depth) if depth is not None else 0.0,
        pct_traded_60min=float(pct_traded),
    )


def write_snapshot(out_path: Path, snapshot: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, sort_keys=True)
    tmp.replace(out_path)


def is_fresh(path: Path, max_age_hours: float) -> bool:
    """True if file exists and is younger than max_age_hours."""
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
        log.info(
            "Snapshot at %s is fresh (< 23h old), skipping. Use --force to override.",
            out_path,
        )
        return 0

    log.info("Building Nado client for snapshot…")
    client = NadoClient_(settings)
    universe = load_universe(client)
    log.info("Snapshotting %d coins", len(universe))

    snap = collect_snapshot(client, universe)
    write_snapshot(out_path, snap)
    log.info("Snapshot written to %s (%d coins)", out_path, len(snap["coins"]))

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily liquidity snapshot for nado_bot_v2")
    parser.add_argument("--force", action="store_true", help="Ignore freshness check, regenerate")
    args = parser.parse_args()
    rc = main(force=args.force)
    import os as _os
    _os._exit(rc)
