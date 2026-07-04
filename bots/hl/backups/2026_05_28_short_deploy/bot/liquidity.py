"""liquidity.py — snapshot-based liquidity profile loader.

Architecture refactor 2026-05-24: per-trade orderbook/spread/depth gates были
удалены целиком. Вместо них раз в сутки запускается `bot.liquidity_snapshot`
(см. systemd cron 00:05 UTC), который для всего universe собирает:
  - avg_1h_vol_usd  — средний $-volume за час (24h candles)
  - spread_pct      — top-of-book spread (snapshot hint)
  - depth_top20_usd — суммарный $-notional 20 уровней (avg bid+ask)
И сохраняет в data/liquidity_snapshot.json.

Bot загружает этот файл в память при старте и периодически перечитывает
(см. main.py SnapshotReloader). Trader использует snapshot для size cap
(LIQ_SIZE_CAP_PCT × avg_1h_vol_usd), а не runtime checks.

Per-trade liquidity gates (1h_vol floor, spread max, depth-walk slip) ушли
полностью (rip-and-rebuild — memory feedback_rip_and_rebuild). Если ликвы
мало — снижаем размер до того что вмещается, а не пропускаем сигнал.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiquidityProfile:
    """Per-coin liquidity profile from daily snapshot.

    `pct_traded_60min` = liveness metric (fraction of last 60 1m bars with
    traded volume > 0). Added 2026-05-24. Optional for back-compat with old
    snapshots that pre-date the liveness filter — defaults to 1.0 (assume
    live) so existing call-sites that don't read it keep working.
    """
    coin: str
    avg_1h_vol_usd: float
    spread_pct: float
    depth_top20_usd: float
    pct_traded_60min: float = 1.0


@dataclass(frozen=True)
class LiquiditySnapshot:
    """Full snapshot file contents — coin → profile, plus metadata."""
    generated_at_utc: str   # ISO timestamp from snapshot file
    coins: dict[str, LiquidityProfile]
    path: Path
    mtime_unix: float       # file mtime when loaded (for reload detection)
    age_seconds: float      # how old the file was when loaded

    def get(self, coin: str) -> Optional[LiquidityProfile]:
        return self.coins.get(coin)


def load_snapshot(path: Path) -> Optional[LiquiditySnapshot]:
    """Read snapshot JSON from disk and parse into LiquiditySnapshot.

    Returns None if file does not exist or parse fails. Caller decides
    whether to bootstrap (run snapshot inline) or proceed without.
    """
    if not path.exists():
        log.warning("Liquidity snapshot not found at %s", path)
        return None
    try:
        mtime = path.stat().st_mtime
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        log.error("Failed to read liquidity snapshot at %s: %s", path, e)
        return None

    coins_raw = raw.get("coins", {}) or {}
    profiles: dict[str, LiquidityProfile] = {}
    for sym, p in coins_raw.items():
        try:
            profiles[sym] = LiquidityProfile(
                coin=sym,
                avg_1h_vol_usd=float(p.get("avg_1h_vol_usd", 0.0) or 0.0),
                spread_pct=float(p.get("spread_pct", 0.0) or 0.0),
                depth_top20_usd=float(p.get("depth_top20_usd", 0.0) or 0.0),
                pct_traded_60min=float(p.get("pct_traded_60min", 1.0) or 1.0),
            )
        except (TypeError, ValueError) as e:
            log.warning("Snapshot parse skip %s: %s", sym, e)
            continue

    age_seconds = max(0.0, time.time() - mtime)
    snap = LiquiditySnapshot(
        generated_at_utc=str(raw.get("generated_at_utc", "")),
        coins=profiles,
        path=path,
        mtime_unix=mtime,
        age_seconds=age_seconds,
    )
    log.info(
        "Liquidity snapshot loaded: %d coins, generated_at=%s, age=%.1fh",
        len(profiles), snap.generated_at_utc, age_seconds / 3600.0,
    )
    return snap


class SnapshotHolder:
    """Thread-safe holder for current LiquiditySnapshot.

    Bot keeps one instance in main.py and updates it via reload thread that
    checks mtime every 10 minutes. Trader reads via `current()`.

    Also supports inline single-coin retry (`fetch_inline`) for the case where
    the daily snapshot dropped a coin (e.g. API returned 0 rows that morning) —
    instead of silent-rejecting all signals on it for 24h, trader can ask the
    holder to do a one-shot live fetch and patch the in-memory snapshot.
    """

    # Inline-fetch rate limit: don't retry same coin more than once per this window.
    _INLINE_RETRY_COOLDOWN_SEC: float = 600.0   # 10 min

    def __init__(self, initial: Optional[LiquiditySnapshot] = None) -> None:
        self._snap: Optional[LiquiditySnapshot] = initial
        self._lock = threading.Lock()
        self._inline_attempts: dict[str, float] = {}   # coin -> last attempt unix-ts

    def current(self) -> Optional[LiquiditySnapshot]:
        with self._lock:
            return self._snap

    def set(self, snap: Optional[LiquiditySnapshot]) -> None:
        with self._lock:
            self._snap = snap

    def maybe_reload(self, path: Path) -> bool:
        """Re-read snapshot from disk if mtime changed. Returns True if reloaded."""
        if not path.exists():
            return False
        try:
            current_mtime = path.stat().st_mtime
        except OSError:
            return False
        with self._lock:
            old_mtime = self._snap.mtime_unix if self._snap is not None else 0.0
            if current_mtime <= old_mtime:
                return False
        new_snap = load_snapshot(path)
        if new_snap is None:
            return False
        with self._lock:
            self._snap = new_snap
        log.info(
            "Snapshot reloaded from disk: %d coins, generated_at=%s",
            len(new_snap.coins), new_snap.generated_at_utc,
        )
        return True

    def fetch_inline(self, coin: str, fetch_callable) -> Optional[LiquidityProfile]:
        """Run a one-shot inline fetch for a coin missing from snapshot.

        `fetch_callable` is a zero-arg function returning either a
        LiquidityProfile (success) or None (failure). Caller injects it so
        this module stays exchange-agnostic.

        Behaviour:
          - returns immediately if a profile is already present (defensive)
          - rate-limited per-coin via _INLINE_RETRY_COOLDOWN_SEC (default 10 min)
            so a string of signals on the same dead coin doesn't hammer the API
          - on success, patches the LiquiditySnapshot dict in place under lock
        """
        now = time.time()
        with self._lock:
            snap = self._snap
            if snap is not None and coin in snap.coins:
                return snap.coins[coin]
            last_attempt = self._inline_attempts.get(coin, 0.0)
            if (now - last_attempt) < self._INLINE_RETRY_COOLDOWN_SEC:
                return None
            self._inline_attempts[coin] = now

        try:
            profile = fetch_callable()
        except Exception as e:
            log.warning("inline liquidity fetch failed for %s: %s", coin, e)
            return None
        if profile is None:
            log.info("inline liquidity fetch returned no data for %s", coin)
            return None

        with self._lock:
            if self._snap is not None:
                # mutate the inner dict — frozen dataclass holds a ref to the dict,
                # so this is safe and avoids cloning the whole snapshot per coin.
                self._snap.coins[coin] = profile
        log.info(
            "inline liquidity patched %s into snapshot: avg_1h_vol_usd=$%.0f",
            coin, profile.avg_1h_vol_usd,
        )
        return profile
