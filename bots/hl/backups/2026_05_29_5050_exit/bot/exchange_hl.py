"""exchange_hl.py — Hyperliquid SDK adapter mirroring ExtendedClient/PacificaClient interface.

SDK: hyperliquid-python-sdk (sync, Info + Exchange classes).
Mainnet: https://api.hyperliquid.xyz (constants.MAINNET_API_URL).
Auth: Agent wallet private key (NOT main wallet key) — see HL docs "Agents".

Key HL-specific behaviours (from old exchange.py bak + prod history):
  - Funding: hourly intervals (not 8h). Cap 4%/hr per HL docs.
  - Mark price = TWAP (median oracle EMA + bid/ask/last + CEX-weighted).
    Strategy uses close-based signals — safe. SL/TP trigger on mark price.
  - HIP-3 (xyz:*): separate dex=xyz info endpoint. Coin names use colon
    format on exchange API: "xyz:GOLD". Internal code uses underscore: "xyz_GOLD".
    Translator: internal→API = replace first "_" with ":" ONLY for xyz_ prefix.
  - price rounding: ≤5 sig figs AND ≤(6-szDecimals) decimal places.
  - min order size enforced per coin via szDecimals + lot_size from meta.
  - Trigger SL: order_type={"trigger": {"triggerPx": px, "isMarket": True, "tpsl": "sl"}}
  - All order ops: SYNC (no asyncio). Thread-safe via lock on caches.

Bugs fixed in old prod (from exchange.py.bak history):
  - 429 on SDK init → pre-flight probe + _sdk_call_with_429_retry
  - HIP-3 positions missing → explicit clearinghouseState dex=xyz call
  - all_mids() misses HIP-3 → separate allMids?dex=xyz call
  - cancel SL 429 silent fail → _retry_429 wrapper
  - frontier open_orders no trigger fields → frontendOpenOrders
  - xyz: prefix on exchange, xyz_ prefix internal → coin_to_api/api_to_coin
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests as _requests

from bot.config import Settings, TF_MS, CANDLES_LIMIT

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy SDK imports — fail-fast at __init__ if not installed
# ---------------------------------------------------------------------------
try:
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from hyperliquid.utils import constants as hl_constants
    from eth_account import Account
    _SDK_OK = True
    _SDK_ERR = ""
except ImportError as _e:
    _SDK_OK = False
    _SDK_ERR = str(_e)


# ---------------------------------------------------------------------------
# HIP-3 coin name translation
# Internal (DB, config, scanner): "xyz_GOLD", "xyz_BTC", "xyz_VIX"
# HL API (Info + Exchange calls):  "xyz:GOLD", "xyz:BTC",  "xyz:VIX"
# Main perps (BTC, ETH, SOL, ...): same on both sides, no translation.
# ---------------------------------------------------------------------------

_HIP3_DEXES = frozenset({"xyz"})  # whitelisted USDC-margin HIP-3 dexes


def coin_to_api(coin: str) -> str:
    """Convert internal coin name to HL API format.
    "xyz_GOLD" → "xyz:GOLD". Other coins unchanged.
    """
    for dex in _HIP3_DEXES:
        prefix = f"{dex}_"
        if coin.startswith(prefix):
            return f"{dex}:{coin[len(prefix):]}"
    return coin


def api_to_coin(api_name: str) -> str:
    """Convert HL API coin name to internal format.
    "xyz:GOLD" → "xyz_GOLD". Other coins unchanged.
    """
    for dex in _HIP3_DEXES:
        prefix = f"{dex}:"
        if api_name.startswith(prefix):
            return f"{dex}_{api_name[len(prefix):]}"
    return api_name


# ---------------------------------------------------------------------------
# Pre-flight + retry helpers (ported verbatim from old exchange.py)
# ---------------------------------------------------------------------------

def _wait_for_hl_api_stable(url: str, max_minutes: int = 30) -> None:
    """Wait until HL /info endpoints respond 200 in TWO consecutive probes ~5s apart.

    SDK does ~4 rapid /info calls at init without internal retry — any 429 raises.
    Pre-flight ensures rate-limit window is stable before SDK init.
    Added 2026-05-17 after 12h outage — bot self-heals on API throttle.
    """
    deadline = time.time() + max_minutes * 60
    consecutive_clean = 0
    sleep_dirty = float(os.getenv("HL_PREFLIGHT_DIRTY_SLEEP_SEC", "30"))
    while time.time() < deadline:
        all_ok = True
        for t in ["spotMeta", "perpDexs", "meta"]:
            body: dict = {"type": t}
            if t == "meta":
                body["dex"] = ""
            try:
                r = _requests.post(url + "/info", json=body, timeout=10)
                if r.status_code != 200:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break
        if all_ok:
            consecutive_clean += 1
            if consecutive_clean >= 2:
                log.info("HL API stable — proceeding with SDK init")
                return
            time.sleep(5)
        else:
            consecutive_clean = 0
            log.info("HL API not stable yet — sleeping %.0fs re-probing", sleep_dirty)
            time.sleep(sleep_dirty)
    raise RuntimeError(f"HL API not stable after {max_minutes} minutes")


def _sdk_call_with_429_retry(fn, *, what: str, max_attempts: int = 10, sleep_sec: float = 30.0):
    """Call fn() with retry on 429-like errors. Wraps SDK init calls.

    SDK Info()/Exchange() __init__ each make several /info HTTP calls with NO
    internal retry — a single 429 raises. This wrapper gives them a retry budget.
    """
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            is_429 = "429" in str(e) or "rate limit" in err_str
            if is_429 and attempt < max_attempts:
                log.warning("SDK init %s 429 (attempt %d/%d) — sleep %.0fs",
                            what, attempt, max_attempts, sleep_sec)
                time.sleep(sleep_sec)
                continue
            raise
    raise RuntimeError(f"SDK init {what} failed after {max_attempts}: {last_err}")


# ---------------------------------------------------------------------------
# AssetMeta — mirrors Extended/Pacifica adapters
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AssetMeta:
    name: str          # internal name (xyz_GOLD for HIP-3, BTC for main)
    sz_decimals: int   # quantity decimal places
    max_leverage: int
    min_size: float    # minimum order size in base units (from meta if available)


# ---------------------------------------------------------------------------
# HLClient — main adapter
# ---------------------------------------------------------------------------

class HLClient:
    """Hyperliquid adapter — mirrors ExtendedClient interface.

    All methods are SYNC (no asyncio). Thread-safe caches use threading.RLock.

    EXCHANGE field = "hyperliquid" — checked by main.py dispatch.
    """

    # HIP-3 dexes whitelisted for USDC margin (source: old exchange.py prod)
    HIP3_USDC_DEXES: list[str] = ["xyz"]

    def __init__(self, settings: Settings) -> None:
        if not _SDK_OK:
            raise RuntimeError(
                f"hyperliquid-python-sdk not installed: {_SDK_ERR}. "
                "pip install hyperliquid-python-sdk"
            )
        self.settings = settings

        url = (
            hl_constants.MAINNET_API_URL
            if settings.network == "mainnet"
            else hl_constants.TESTNET_API_URL
        )
        self.url = url

        # PRE-FLIGHT: ensure API is responsive before SDK init attempts their burst.
        _wait_for_hl_api_stable(url)

        # Fetch available HIP-3 dexes (xyz only USDC whitelist)
        hip3_names = self._fetch_perp_dex_names(url)
        # SDK requires "" (main dex) first in perp_dexs to include main perps.
        # Without "", BTC/ETH/SOL vanish from name_to_coin map.
        perp_dexs_list = [""] + hip3_names

        # SDK Info — WS disabled for sync bot (no background thread)
        self.info = _sdk_call_with_429_retry(
            lambda: Info(url, skip_ws=True, perp_dexs=perp_dexs_list),
            what="Info(perp_dexs)",
        )
        log.info("Info init: %d dexes (main + %d HIP-3 %s)",
                 len(perp_dexs_list), len(hip3_names), hip3_names)

        # Wallet from agent private key (NOT main wallet key)
        self.wallet = Account.from_key(settings.agent_private_key)

        # SDK Exchange — needs same perp_dexs for HIP-3 update_leverage/order to work.
        # Without perp_dexs: name_to_asset KeyError on xyz:GOLD → trade cancelled.
        # Bug fix documented in old exchange.py: "HIP-3 не торгует никогда" (2026-05-05).
        self.exchange = _sdk_call_with_429_retry(
            lambda: Exchange(
                self.wallet, url,
                account_address=settings.account_address,
                perp_dexs=perp_dexs_list,
            ),
            what="Exchange",
        )

        # Caches
        self._cache_lock = threading.RLock()
        self._meta_cache: dict[str, AssetMeta] | None = None
        self._candles_cache: dict[tuple[str, str], tuple[int, pd.DataFrame, float]] = {}
        # negative cache — coin/TF that failed last fetch, skip API for N seconds
        # (per user 2026-05-26: "разово получаешь и больше не спрашиваешь")
        self._candles_fail_cache: dict[tuple[str, str], float] = {}
        self._funding_cache: tuple[float, dict[str, float]] | None = None
        self._positions_cache: tuple[float, dict] | None = None
        self._mids_cache: tuple[float, dict] | None = None
        self._mids_cache_hip3: tuple[float, dict[str, dict]] | None = None
        self._av_cache: tuple[float, float] | None = None
        self._fills_cache: tuple[float, list] | None = None
        self._user_state_cache: tuple[float, dict] | None = None

        log.info("HLClient init: network=%s url=%s account=%s",
                 settings.network, url, settings.account_address[:10] + "...")

    @classmethod
    def _fetch_perp_dex_names(cls, url: str) -> list[str]:
        """Return HIP-3 dex names matching USDC-margin whitelist.
        Passed to Info(perp_dexs=) so name_to_coin knows HIP-3 markets.
        """
        try:
            r = _requests.post(url + "/info", json={"type": "perpDexs"}, timeout=10)
            data = r.json()
            if not isinstance(data, list):
                return []
            available = []
            for d in data:
                if not isinstance(d, dict):
                    continue
                name = d.get("name")
                if name and name in cls.HIP3_USDC_DEXES:
                    available.append(name)
            return available
        except Exception as e:
            log.warning("_fetch_perp_dex_names failed: %s", e)
            return []

    # -----------------------------------------------------------------------
    # Metadata
    # -----------------------------------------------------------------------

    def get_meta(self, force: bool = False) -> dict[str, AssetMeta]:
        """Return {internal_coin_name: AssetMeta} for main perps + HIP-3.

        Main perp: info.meta() (no dex param).
        HIP-3: info.post /info {"type":"meta","dex":"xyz"} — separate response.

        Bug fix (2026-05-05 old bot): without dex= on meta(), HIP-3 pairs
        get KeyError in asset() → trade cancelled silently.
        """
        with self._cache_lock:
            if self._meta_cache is not None and not force:
                return self._meta_cache

        out: dict[str, AssetMeta] = {}

        # Main universe
        try:
            raw = self.info.meta()
            for asset in raw.get("universe", []):
                api_name = asset["name"]
                internal = api_to_coin(api_name)
                # szDecimals: quantity decimal places (source: HL docs + old exchange.py)
                sz_dec = int(asset.get("szDecimals", 4))
                max_lev = int(asset.get("maxLeverage", 1))
                out[internal] = AssetMeta(
                    name=internal,
                    sz_decimals=sz_dec,
                    max_leverage=max_lev,
                    min_size=0.0,  # populated below if available
                )
        except Exception as e:
            log.warning("meta() main dex failed: %s", e)

        # HIP-3 dexes
        for dex in self.HIP3_USDC_DEXES:
            try:
                r = _requests.post(
                    self.url + "/info",
                    json={"type": "meta", "dex": dex},
                    timeout=10,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                for asset in data.get("universe", []):
                    api_name = asset["name"]
                    internal = api_to_coin(api_name)
                    sz_dec = int(asset.get("szDecimals", 4))
                    max_lev = int(asset.get("maxLeverage", 1))
                    out[internal] = AssetMeta(
                        name=internal,
                        sz_decimals=sz_dec,
                        max_leverage=max_lev,
                        min_size=0.0,
                    )
            except Exception as e:
                log.warning("meta(dex=%s) failed: %s", dex, e)

        with self._cache_lock:
            self._meta_cache = out
        log.info("get_meta: %d assets loaded (main + HIP-3 %s)", len(out), self.HIP3_USDC_DEXES)
        return out

    def asset(self, coin: str) -> AssetMeta:
        """Return AssetMeta for a coin (internal name)."""
        meta = self.get_meta()
        if coin not in meta:
            # Attempt refresh once
            meta = self.get_meta(force=True)
        return meta[coin]  # raises KeyError if truly unknown — desired loud failure

    # -----------------------------------------------------------------------
    # Price rounding (HL-specific rules)
    # -----------------------------------------------------------------------

    def round_price(self, coin: str, px: float) -> float:
        """Round price to HL requirements:
          - ≤ 5 significant figures
          - ≤ (6 - szDecimals) decimal places

        Source: old exchange.py round_price (2026-05-15 version).
        """
        if px <= 0:
            return px
        try:
            sz_dec = self.asset(coin).sz_decimals
        except (KeyError, Exception):
            sz_dec = 4

        max_dec = max(0, 6 - sz_dec)
        magnitude = math.floor(math.log10(abs(px)))
        sig_dec = max(0, 4 - magnitude)
        decimals = min(max_dec, sig_dec)
        rounded = round(px, decimals)
        fmt = f"{rounded:.{decimals}f}" if decimals > 0 else f"{int(round(rounded))}"
        return float(fmt)

    def round_size(self, coin: str, sz: float) -> float:
        """Round size to szDecimals precision (HL quantity step)."""
        try:
            sz_dec = self.asset(coin).sz_decimals
        except (KeyError, Exception):
            sz_dec = 4
        factor = 10 ** sz_dec
        return math.floor(sz * factor) / factor

    # -----------------------------------------------------------------------
    # Candles
    # -----------------------------------------------------------------------

    @staticmethod
    def _coin_cache_offset_ms(coin: str, interval: str, bar_ms: int) -> int:
        """Deterministic per-(coin, interval) jitter to stagger cache invalidation.

        Prevents thundering herd at top-of-bar when all coins invalidate
        simultaneously → 429 burst. Cap = min(30s, bar_ms//2).
        Source: old exchange.py _coin_cache_offset_ms (2026-05-08 version).
        """
        h = hashlib.md5(f"{coin}:{interval}".encode()).digest()
        raw = int.from_bytes(h[:4], "big")
        cap_ms = min(30_000, bar_ms // 2)
        return raw % max(1, cap_ms) if cap_ms > 0 else 0

    def candles(self, coin: str, interval: str, limit: int = CANDLES_LIMIT) -> pd.DataFrame:
        """Fetch OHLCV candles for coin/interval. Bar-aligned cache with jitter.

        HIP-3 coins: use API name (xyz:GOLD) for candles_snapshot call.
        REST-only (no WS push) — appropriate for long-TF bot (1h/2h/4h/8h/1d).

        Returns DataFrame with columns: time, Open, High, Low, Close, Volume.
        Last in-progress bar is dropped (only closed bars returned).
        """
        api_name = coin_to_api(coin)
        ms = TF_MS[interval]
        now_ms = int(time.time() * 1000)

        offset_ms = self._coin_cache_offset_ms(coin, interval, ms)
        effective_now = now_ms - offset_ms
        current_bar_start = (effective_now // ms) * ms

        cache_key = (coin, interval)
        with self._cache_lock:
            cached = self._candles_cache.get(cache_key)

        if cached is not None and cached[0] >= current_bar_start:
            return cached[1].copy()

        # Negative cache — skip API call if last fetch failed within FAIL_TTL
        fail_ttl = float(os.getenv("CANDLES_FAIL_TTL_SEC", "300"))
        with self._cache_lock:
            fail_ts = self._candles_fail_cache.get(cache_key)
        if fail_ts is not None and (time.time() - fail_ts) < fail_ttl:
            if cached is not None:
                return cached[1].copy()
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        # Min-TTL gate (30s) — avoids burst at bar boundary
        if cached is not None:
            min_ttl = float(os.getenv("CANDLES_MIN_TTL_SEC", "30"))
            now_sec = now_ms / 1000.0
            if min_ttl > 0 and (now_sec - cached[2]) < min_ttl:
                return cached[1].copy()

        end = now_ms
        start = end - ms * (limit + 1)

        # 300ms + jitter between cache-miss requests (rate-limit protection)
        time.sleep(0.5 + random.uniform(0, 0.3))

        raw = None
        for attempt in range(5):
            try:
                raw = self.info.candles_snapshot(api_name, interval, start, end)
                break
            except KeyError:
                log.warning("Coin %s not found on HL (%s) — skipping", coin, interval)
                return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 3:
                    backoff = 2 ** attempt * (1 + random.uniform(-0.25, 0.25))
                    log.warning("429 on candles %s %s — retry %.1fs (attempt %d/3)",
                                coin, interval, backoff, attempt + 1)
                    time.sleep(backoff)
                    continue
                log.warning("candles %s %s failed: %s — fail-cache 300s", coin, interval, e)
                with self._cache_lock:
                    self._candles_fail_cache[cache_key] = time.time()
                break

        if not raw:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        df_full = pd.DataFrame(raw)
        df_full["time"] = pd.to_datetime(df_full["t"], unit="ms", utc=True)
        df_full = df_full.rename(columns={
            "o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume",
        })
        for col in ("Open", "High", "Low", "Close", "Volume"):
            df_full[col] = df_full[col].astype(float)
        df_full = df_full[["time", "Open", "High", "Low", "Close", "Volume"]].reset_index(drop=True)

        # Drop last in-progress bar
        if len(df_full) > 1:
            df = df_full.iloc[:-1].reset_index(drop=True)
        else:
            df = df_full.reset_index(drop=True)

        last_bar_t_ms = current_bar_start
        if len(df_full) > 0:
            try:
                last_bar_t_ms = int(pd.Timestamp(df_full.iloc[-1]["time"]).value // 10**6)
            except Exception:
                pass

        with self._cache_lock:
            self._candles_cache[cache_key] = (last_bar_t_ms, df, time.time())
            # clear any prior failure mark on success
            self._candles_fail_cache.pop(cache_key, None)
        return df

    # -----------------------------------------------------------------------
    # Funding rate
    # -----------------------------------------------------------------------

    def funding_rate(self, coin: str) -> float:
        """Current hourly funding rate for coin. Cached 60s.

        HL funding is hourly (cap 4%/hr per HL docs). All rates from single
        meta_and_asset_ctxs() call — efficient.
        """
        return self._get_funding_map().get(coin_to_api(coin), 0.0)

    def _get_funding_map(self) -> dict[str, float]:
        now = time.time()
        with self._cache_lock:
            fc = self._funding_cache
        if fc is not None and (now - fc[0]) < 60.0:
            return fc[1]

        for attempt in range(4):
            try:
                ctxs = self.info.meta_and_asset_ctxs()
                if not isinstance(ctxs, list) or len(ctxs) < 2:
                    break
                universe = ctxs[0].get("universe", [])
                asset_ctxs = ctxs[1]
                fmap: dict[str, float] = {}
                for i, asset in enumerate(universe):
                    if i < len(asset_ctxs):
                        fmap[asset["name"]] = float(asset_ctxs[i].get("funding", 0.0))
                with self._cache_lock:
                    self._funding_cache = (now, fmap)
                return fmap
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 3:
                    time.sleep(2 ** attempt)
                    continue
                log.warning("funding map fetch failed: %s", e)
                break

        with self._cache_lock:
            fc = self._funding_cache
        return fc[1] if fc else {}

    # -----------------------------------------------------------------------
    # Account state
    # -----------------------------------------------------------------------

    def account_value(self) -> float:
        """Total equity via portfolio.day.accountValueHistory[-1].

        Single portfolio() call instead of 3 (spot + user_state + all_mids).
        Includes main perp + spot + HIP-3 acct in unified equity.
        Cache 5s. Bug fix 2026-05-15 old bot: 3-call sum missed HIP-3 acct.
        """
        now = time.time()
        with self._cache_lock:
            av = self._av_cache
        if av is not None and (now - av[0]) < 5.0:
            return av[1]

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                pf = self.info.portfolio(self.settings.account_address)
                day = None
                if isinstance(pf, list):
                    day = next((v for k, v in pf if k == "day"), None)
                elif isinstance(pf, dict):
                    day = pf.get("day")
                if not isinstance(day, dict):
                    raise RuntimeError(f"portfolio.day bad shape: {type(day)}")
                avh = day.get("accountValueHistory") or []
                if not avh:
                    raise RuntimeError("portfolio.day.accountValueHistory empty")
                val = float(avh[-1][1])
                with self._cache_lock:
                    self._av_cache = (now, val)
                log.debug("account_value: $%.2f", val)
                return val
            except Exception as e:
                last_exc = e
                msg = str(e)
                if "429" in msg and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                break
        raise last_exc or RuntimeError("account_value unreachable")

    def open_positions(self) -> dict[str, dict]:
        """Return {internal_coin: position_dict} for all open positions.

        Main perps: info.user_state(). HIP-3: clearinghouseState dex=xyz.
        Cache 20s. Per-dex retry budget (2026-05-15 fix): HIP-3 one-shot was
        returning empty on 429 → false close of live xyz:* positions.
        Fallback: on HIP-3 fetch failure use stale cached xyz:* entries.
        """
        now = time.time()
        with self._cache_lock:
            pc = self._positions_cache
        if pc is not None and (now - pc[0]) < 20.0:
            return pc[1]

        for attempt in range(3):
            try:
                state = self.info.user_state(self.settings.account_address)
                out: dict[str, dict] = {}
                for ap in state.get("assetPositions", []):
                    pos = ap.get("position", {})
                    api_name = pos.get("coin")
                    if api_name and float(pos.get("szi", 0)) != 0:
                        internal = api_to_coin(api_name)
                        out[internal] = pos

                # HIP-3 positions — separate clearinghouse per dex
                for dex in self.HIP3_USDC_DEXES:
                    dex_failed = False
                    for hip3_attempt in range(3):
                        try:
                            dex_state = self.info.post("/info", {
                                "type": "clearinghouseState",
                                "user": self.settings.account_address,
                                "dex": dex,
                            })
                            for ap in (dex_state or {}).get("assetPositions", []):
                                pos = ap.get("position", {})
                                api_name = pos.get("coin")
                                if api_name and float(pos.get("szi", 0)) != 0:
                                    internal = api_to_coin(api_name)
                                    out[internal] = pos
                            break
                        except Exception as e:
                            msg = str(e)
                            if "429" in msg and hip3_attempt < 2:
                                time.sleep(2 ** hip3_attempt)
                                continue
                            log.warning("open_positions HIP-3 dex=%s failed: %s", dex, e)
                            dex_failed = True
                            break
                    if dex_failed and pc is not None:
                        # Stale HIP-3 entries better than missing (prevents false close)
                        dex_prefix = f"{dex}_"
                        cached_dex = {k: v for k, v in pc[1].items()
                                      if k.startswith(dex_prefix) and k not in out}
                        if cached_dex:
                            log.warning(
                                "HIP-3 dex=%s fetch failed — using %d stale cache entries",
                                dex, len(cached_dex),
                            )
                            out.update(cached_dex)

                with self._cache_lock:
                    self._positions_cache = (now, out)
                return out
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                log.warning("open_positions main fetch failed: %s", e)
                break

        with self._cache_lock:
            pc = self._positions_cache
        return pc[1] if pc else {}

    def invalidate_positions_cache(self) -> None:
        """Call after open/close to force fresh fetch on next open_positions()."""
        with self._cache_lock:
            self._positions_cache = None

    def mark_price(self, coin: str, ttl: float = 5.0) -> float:
        """Current mark price from all_mids snapshot. Cached ttl seconds.

        HIP-3: separate allMids?dex=xyz call (main dex all_mids misses xyz:*).
        Bug fix 2026-05-06 old bot: HIP-3 returned 0 from main all_mids →
        SIGNAL FRESHNESS GATE silently skipped HIP-3 trades.
        """
        api_name = coin_to_api(coin)
        now = time.time()

        if api_name.startswith("xyz:"):
            dex = api_name.split(":", 1)[0]
            with self._cache_lock:
                hip3 = self._mids_cache_hip3
            if hip3 is not None:
                ts, dex_map = hip3
                if (now - ts) < ttl and dex in dex_map:
                    return float(dex_map[dex].get(api_name, 0.0))
            try:
                mids_dex = self.info.post("/info", {"type": "allMids", "dex": dex})
                if not isinstance(mids_dex, dict):
                    return 0.0
                with self._cache_lock:
                    prev_hip3 = self._mids_cache_hip3
                    cur_map = (prev_hip3[1].copy() if prev_hip3 else {})
                    cur_map[dex] = mids_dex
                    self._mids_cache_hip3 = (now, cur_map)
                return float(mids_dex.get(api_name, 0.0))
            except Exception as e:
                log.warning("mark_price(%s) HIP-3 failed: %s", coin, e)
                with self._cache_lock:
                    hip3 = self._mids_cache_hip3
                if hip3 and dex in hip3[1]:
                    return float(hip3[1][dex].get(api_name, 0.0))
                return 0.0

        # Main dex
        with self._cache_lock:
            mc = self._mids_cache
        if mc is not None and (now - mc[0]) < ttl:
            return float(mc[1].get(api_name, 0.0))
        try:
            mids = self.info.all_mids()
            with self._cache_lock:
                self._mids_cache = (now, mids)
            return float(mids.get(api_name, 0.0))
        except Exception as e:
            log.warning("mark_price(%s) main failed: %s", coin, e)
            with self._cache_lock:
                mc = self._mids_cache
            if mc:
                return float(mc[1].get(api_name, 0.0))
        return 0.0

    def user_fills(self, ttl_sec: float = 60.0) -> list[dict]:
        """Recent fills with TTL cache. Used for PnL/slippage tracking."""
        now = time.time()
        with self._cache_lock:
            fc = self._fills_cache
        if fc is not None and (now - fc[0]) < ttl_sec:
            return fc[1]
        for attempt in range(3):
            try:
                fills = self.info.user_fills(self.settings.account_address)
                if not isinstance(fills, list):
                    fills = []
                with self._cache_lock:
                    self._fills_cache = (now, fills)
                return fills
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                log.warning("user_fills failed: %s", e)
                break
        with self._cache_lock:
            fc = self._fills_cache
        return fc[1] if fc else []

    def compute_realized_pnl(self, coin: str, entry_price: float, exit_price: float,
                             size: float, is_long: bool) -> float:
        """Compute realized PnL from fills where available, else from prices."""
        fills = self.user_fills(ttl_sec=10.0)
        api_name = coin_to_api(coin)
        # Find matching close fill(s) — most recent
        relevant = [f for f in fills if f.get("coin") == api_name]
        if relevant:
            try:
                total_pnl = sum(float(f.get("closedPnl", 0) or 0) for f in relevant[-3:])
                if total_pnl != 0:
                    return total_pnl
            except Exception:
                pass
        # Fallback: price diff × size
        if is_long:
            return (exit_price - entry_price) * size
        return (entry_price - exit_price) * size

    def position_liquidation(self, coin: str) -> dict | None:
        """{liq_px, margin_mode, leverage} for open position. None if no position."""
        state = self._user_state_cached(ttl_sec=10.0)
        if state is None:
            return None
        api_name = coin_to_api(coin)
        for ap in state.get("assetPositions", []) or []:
            p = ap.get("position", {}) or {}
            if p.get("coin") != api_name:
                continue
            if abs(float(p.get("szi", 0) or 0)) < 1e-12:
                return None
            lev = p.get("leverage", {}) or {}
            liq_raw = p.get("liquidationPx")
            try:
                liq_px = float(liq_raw) if liq_raw is not None else None
            except (TypeError, ValueError):
                liq_px = None
            return {
                "liq_px": liq_px,
                "margin_mode": str(lev.get("type", "cross")).lower(),
                "leverage": int(lev.get("value", 1) or 1),
            }
        return None

    def _user_state_cached(self, ttl_sec: float = 10.0) -> dict | None:
        now = time.time()
        with self._cache_lock:
            usc = self._user_state_cache
        if usc is not None and (now - usc[0]) < ttl_sec:
            return usc[1]
        try:
            state = self.info.user_state(self.settings.account_address)
            with self._cache_lock:
                self._user_state_cache = (now, state)
            return state
        except Exception as e:
            with self._cache_lock:
                usc = self._user_state_cache
            if usc:
                log.warning("user_state failed (stale cache %.0fs): %s",
                            now - usc[0], e)
                return usc[1]
            log.warning("user_state failed (no cache): %s", e)
            return None

    # -----------------------------------------------------------------------
    # Slippage estimate
    # -----------------------------------------------------------------------

    # Flat 24 bps initial estimate (source: user spec + HL taker 4.5bp + slippage buffer)
    # Recalibrate after 30+ trades per project_exchange_slip_calibration_2026_05_20.
    _DEFAULT_SLIP_MAIN = 0.0024     # 24 bps — main perps
    _DEFAULT_SLIP_HIP3 = 0.0050     # 50 bps — HIP-3 thinner markets

    def slip_per_side(self, coin: str, notional_usd: float = 0) -> float:
        """Per-side fill slippage estimate as fraction of notional.
        Source: user spec 24 bps flat initial. HIP-3 50 bps (thinner markets).
        """
        del notional_usd  # not yet size-sensitive
        if coin.startswith("xyz_"):
            return self._DEFAULT_SLIP_HIP3
        return self._DEFAULT_SLIP_MAIN

    # -----------------------------------------------------------------------
    # Leverage
    # -----------------------------------------------------------------------

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> dict | None:
        """Set leverage for coin. Retry 4x on 429. Returns None on failure.

        Caller must abort order if None returned — HL may use wrong default lev.
        Source: old exchange.py update_leverage (2026-05-xx version).
        """
        api_name = coin_to_api(coin)
        for attempt in range(4):
            try:
                return self.exchange.update_leverage(leverage, api_name, is_cross)
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 3:
                    backoff = 2 ** attempt
                    log.warning("update_leverage(%s, %d) 429 — retry %ds", coin, leverage, backoff)
                    time.sleep(backoff)
                    continue
                log.warning("update_leverage(%s, %d) failed: %s", coin, leverage, e)
                return None
        return None

    # -----------------------------------------------------------------------
    # Order utilities
    # -----------------------------------------------------------------------

    def _retry_429(self, fn, op_name: str, attempts: int = 5):
        """Retry fn() on 429 with exponential backoff 1/2/4/8/16s.
        Non-429 errors re-raised immediately.
        Source: old exchange.py _retry_429.
        """
        last_err = None
        for attempt in range(attempts):
            try:
                return fn()
            except Exception as e:
                msg = str(e)
                last_err = e
                if "429" in msg and attempt < attempts - 1:
                    backoff = 2 ** attempt
                    log.warning("429 on %s — retry %ds (attempt %d/%d)",
                                op_name, backoff, attempt + 1, attempts)
                    time.sleep(backoff)
                    continue
                raise
        raise last_err

    def market_open(self, coin: str, is_buy: bool, sz: float) -> dict:
        """Market order to open a position.

        SDK market_open: slippage param sets max allowable fill slippage.
        Source: old exchange.py market_open + settings.slippage from .env.
        """
        api_name = coin_to_api(coin)
        sz_rounded = self.round_size(coin, sz)
        return self._retry_429(
            lambda: self.exchange.market_open(
                api_name, is_buy=is_buy, sz=sz_rounded,
                slippage=self.settings.slippage,
            ),
            f"market_open({coin})",
        )

    def market_close(self, coin: str) -> dict:
        """Market order to close entire position in coin."""
        api_name = coin_to_api(coin)
        return self._retry_429(
            lambda: self.exchange.market_close(api_name),
            f"market_close({coin})",
        )

    def trigger_sl(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Place stop-loss trigger (market on trigger).

        order_type trigger format (source: old exchange.py trigger_sl + HL docs):
          {"trigger": {"triggerPx": px, "isMarket": True, "tpsl": "sl"}}
        reduce_only=True: ensures order only reduces existing position.
        is_buy: True for short position SL (buy to close), False for long SL.
        """
        api_name = coin_to_api(coin)
        px = self.round_price(coin, trigger_px)
        sz_rounded = self.round_size(coin, sz)
        order_type = {"trigger": {"triggerPx": px, "isMarket": True, "tpsl": "sl"}}
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_rounded, limit_px=px,
                order_type=order_type, reduce_only=True,
            ),
            f"trigger_sl({coin})",
            attempts=3,
        )

    def trigger_tp(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Place take-profit trigger (limit on trigger).

        isMarket=False for TP (limit fill at trigger price).
        Source: old exchange.py trigger_tp.
        """
        api_name = coin_to_api(coin)
        px = self.round_price(coin, trigger_px)
        sz_rounded = self.round_size(coin, sz)
        order_type = {"trigger": {"triggerPx": px, "isMarket": False, "tpsl": "tp"}}
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_rounded, limit_px=px,
                order_type=order_type, reduce_only=True,
            ),
            f"trigger_tp({coin})",
            attempts=3,
        )

    def cancel_sl_order(self, coin: str, oid) -> dict:
        """Cancel a specific order by ID."""
        api_name = coin_to_api(coin)
        return self._retry_429(
            lambda: self.exchange.cancel(api_name, int(oid)),
            f"cancel_sl({coin})",
            attempts=3,
        )

    def list_open_sl_orders(self, coin: str) -> list[int]:
        """List all stop/trigger orders for coin (for orphan cleanup / SL trail).

        Uses frontendOpenOrders (includes trigger fields).
        Bug fix 2026-05-05 old bot: open_orders() lacks trigger fields.
        Bug fix 2026-05-06 old bot: frontendOpenOrders without dex= misses xyz:*.
        Both main dex and HIP-3 dexes queried.
        """
        api_name = coin_to_api(coin)
        addr = self.settings.account_address
        dexes = [""] + list(self.HIP3_USDC_DEXES)
        all_orders: list = []
        for dex in dexes:
            try:
                payload: dict = {"type": "frontendOpenOrders", "user": addr}
                if dex:
                    payload["dex"] = dex
                chunk = self.info.post("/info", payload)
                if chunk:
                    all_orders.extend(chunk)
            except Exception:
                continue

        if not all_orders:
            try:
                all_orders = self.info.open_orders(addr) or []
            except Exception:
                return []

        out: list[int] = []
        for o in all_orders:
            if o.get("coin") != api_name:
                continue
            t = str(o.get("orderType", "")).lower()
            if "stop" in t or o.get("isTrigger") or o.get("reduceOnly"):
                oid = o.get("oid")
                if oid:
                    try:
                        out.append(int(oid))
                    except (TypeError, ValueError):
                        pass
        return out

    def spot_usdc(self) -> float:
        """USDC spot balance (collateral pool check)."""
        try:
            spot = self.info.spot_user_state(self.settings.account_address)
            return sum(
                float(b.get("total", 0))
                for b in spot.get("balances", [])
                if b.get("coin") == "USDC"
            )
        except Exception as e:
            log.warning("spot_user_state failed: %s", e)
            return 0.0
