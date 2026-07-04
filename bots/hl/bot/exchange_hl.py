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


def _install_session_timeout(sdk_obj, what, connect=5.0, read=20.0):
    """ROOT-FIX 2026-07-02 (recurring hung-fetch stall class): the hyperliquid SDK's
    Info/Exchange hold a requests.Session whose .post()/.get() are called with NO timeout,
    so a stalled socket during the per-coin scan (candles/all_mids) hangs the main loop
    until the loop-watchdog force-restarts (Jun 25 burst: 7 restarts / 50 min). Inject a
    hard default (connect, read) timeout on EVERY request through this session so a dead
    socket fails fast (retryable) instead of stalling minutes. Env: HL_SDK_READ_TIMEOUT."""
    try:
        read_to = float(os.getenv("HL_SDK_READ_TIMEOUT", str(read)))
    except Exception:
        read_to = read
    sess = sdk_obj if isinstance(sdk_obj, _requests.Session) else getattr(sdk_obj, "session", None)
    if sess is None:
        log.warning("SDK %s has no .session — cannot install HTTP timeout guard", what)
        return
    if getattr(sess, "_hl_timeout_installed", False):
        return
    _orig_request = sess.request

    def _request(method, url, **kw):
        if kw.get("timeout") is None:
            kw["timeout"] = (connect, read_to)
        return _orig_request(method, url, **kw)

    sess.request = _request
    sess._hl_timeout_installed = True
    log.info("SDK %s: default HTTP timeout installed (connect=%.0fs read=%.0fs)",
             what, connect, read_to)


def _sweep_untimed_sessions():
    """CLASS-GUARD 2026-07-02: SDK composition hides embedded requests.Session
    objects (Exchange.__init__ builds its own Info with a separate session).
    Walk live objects and guard ANY session that slipped past the explicit
    installs above — and WARN, so a new embedded client gets noticed instead
    of silently running unbounded."""
    import gc
    missed = 0
    for _obj in gc.get_objects():
        try:
            if isinstance(_obj, _requests.Session) and not getattr(_obj, "_hl_timeout_installed", False):
                _install_session_timeout(_obj, "gc-sweep")
                missed += 1
        except Exception:
            continue
    if missed:
        log.warning("session-sweep: %d untimed requests.Session found+guarded — new embedded SDK client?", missed)
    else:
        log.info("session-sweep: all requests.Session timeout-guarded")


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
        _install_session_timeout(self.info, "Info")
        log.info("Info init: %d dexes (main + %d HIP-3 %s)",
                 len(perp_dexs_list), len(hip3_names), hip3_names)

        # Wallet from agent private key (NOT main wallet key)
        # COMBO-FIX (2026-06-19): mirror the cred-less-DRY intent of config.py
        # (DRY combo bot starts with a BLANK HYPERLIQUID_AGENT_PRIVATE_KEY by design,
        # creds are NOT required when DRY_RUN=1). Account.from_key("") raises
        # eth_account ValidationError ("private key must be exactly 32 bytes, got 0")
        # and crashes _build_client BEFORE the universe/scan/router ever run. When DRY
        # and the key is blank, synthesize an EPHEMERAL throwaway (non-funded, never
        # persisted) wallet so the SDK Exchange object can be built; every signed write
        # is already short-circuited by _dry_guard so this wallet NEVER signs a real
        # order. Going live (DRY_RUN=0) still requires a real key — config.py hard-
        # requires it and Account.from_key below raises on a blank live key.
        _agent_key = (settings.agent_private_key or "").strip()
        if getattr(settings, "dry_run", False) and not _agent_key:
            log.warning("[DRY] HYPERLIQUID_AGENT_PRIVATE_KEY blank — using EPHEMERAL "
                        "throwaway wallet (DRY: never signs; writes blocked by _dry_guard)")
            self.wallet = Account.create()
        else:
            self.wallet = Account.from_key(_agent_key)

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
        _install_session_timeout(self.exchange, "Exchange")
        # Exchange.__init__ embeds its OWN Info (sdk exchange.py: self.info =
        # Info(...)) with a SEPARATE requests.Session, hit in the order path
        # (_slippage_price -> all_mids). Cover it too, else a stalled socket
        # there still hangs the loop despite the two guards above.
        _exch_info = getattr(self.exchange, "info", None)
        if _exch_info is not None:
            _install_session_timeout(_exch_info, "Exchange-info")
        # CLASS-GUARD: catch any session a future/embedded SDK client creates
        _sweep_untimed_sessions()

        # --- Valantis Prime builder code (2026-06-15) ---------------------------
        # When trading on a Valantis Prime account via an API/agent wallet, Valantis
        # REQUIRES its builder code on every order (open + close + SL/TP); otherwise
        # "API traders pay builder fees themselves. Violation will result in account
        # shut down." We attach builder={"b": addr, "f": fee_tenths_bp} to all order
        # calls and approve the builder fee once at startup. Disabled (None) unless
        # HL_BUILDER_ADDRESS is set in .env, so this is a no-op for non-Prime accounts.
        self._builder = None
        _b_addr = os.getenv("HL_BUILDER_ADDRESS", "").strip().lower()
        if _b_addr:
            _b_fee = int(os.getenv("HL_BUILDER_FEE_TENTHS_BP", "0"))
            self._builder = {"b": _b_addr, "f": _b_fee}
            _b_max = os.getenv("HL_BUILDER_MAX_FEE_RATE", "0.001")  # decimal, e.g. 0.001 = 0.1%
            try:
                # Defense-in-depth DRY guard (audit LOW 2026-06-19): approve_builder_fee is a
                # SIGNED on-chain action. Under DRY_RUN=1 skip the live sign entirely; the
                # builder dict is still attached to (dry-blocked) orders so live parity holds.
                _b_dry = self._dry_guard("approve_builder_fee(%s)" % _b_addr)
                if _b_dry is not None:
                    _b_resp = _b_dry
                else:
                    _b_resp = _sdk_call_with_429_retry(
                        lambda: self.exchange.approve_builder_fee(_b_addr, _b_max),
                        what="approve_builder_fee",
                    )
                log.info("Valantis builder ENABLED b=%s f=%s(tenths-bp) maxRate=%s approve=%s",
                         _b_addr, _b_fee, _b_max, _b_resp)
            except Exception as _e:  # already-approved or transient — orders still carry builder
                log.warning("approve_builder_fee failed (may already be approved): %s", _e)
        else:
            log.info("Valantis builder DISABLED (HL_BUILDER_ADDRESS unset) — plain HL orders")

        # Caches
        self._cache_lock = threading.RLock()
        self._meta_cache: dict[str, AssetMeta] | None = None
        self._candles_cache: dict[tuple[str, str], tuple[int, pd.DataFrame, float]] = {}
        # negative cache — coin/TF that failed last fetch, skip API for N seconds
        # (per user 2026-05-26: "разово получаешь и больше не спрашиваешь")
        self._candles_fail_cache: dict[tuple[str, str], float] = {}
        # Cold-start throttle anchor: caches are empty right after (re)start →
        # 2 bot procs on 1 IP cold-fetch the full universe in lockstep → 429 burst.
        self._proc_start = time.time()
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

    def candles_in_fail_cache(self, coin: str, interval: str) -> bool:
        """True if candles(coin, interval) is currently negative-cached after a fetch
        failure (429-exhaustion / error within CANDLES_FAIL_TTL). Lets the scanner tell a
        429-driven EMPTY df (silent-drop risk → must WARN) apart from a genuinely
        short/young listing (handled by the PARITY-SKIP gate). (silent-429-drop fix 2026-06-20)
        """
        fail_ttl = float(os.getenv("CANDLES_FAIL_TTL_SEC", "300"))
        with self._cache_lock:
            ts = self._candles_fail_cache.get((coin, interval))
        return ts is not None and (time.time() - ts) < fail_ttl

    def candles(self, coin: str, interval: str, limit: int = CANDLES_LIMIT,
                prime: bool = False) -> pd.DataFrame:
        """Fetch OHLCV candles for coin/interval. Bar-aligned cache with jitter.

        HIP-3 coins: use API name (xyz:GOLD) for candles_snapshot call.
        REST-only (no WS push) — appropriate for long-TF bot (1h/2h/4h/8h/1d).

        Returns DataFrame with columns: time, Open, High, Low, Close, Volume.
        Last in-progress bar is dropped (only closed bars returned).

        prime=True (candidate-warmer 2026-06-29): skip the WS-ready-wait poll + the min-TTL
        gate and fetch the just-closed bar directly via REST (fresh at boundary+0s, REST has
        it immediately vs HL's sparse 9-54s WS push). Same bar + cache write as a normal call
        → parity-safe; lets the warmer pre-warm near-trigger coins ahead of the scan.
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

        # Min-TTL gate (30s) — avoids burst at bar boundary. prime bypasses it: the warmer
        # explicitly wants THIS bar's just-closed candle, not a <30s-old prior-bar cache.
        if cached is not None and not prime:
            min_ttl = float(os.getenv("CANDLES_MIN_TTL_SEC", "30"))
            now_sec = now_ms / 1000.0
            if min_ttl > 0 and (now_sec - cached[2]) < min_ttl:
                return cached[1].copy()

        # WS-PUSH fast path (HL_WS_CANDLE): serve closed bars from in-memory feed, 0 REST.
        # xyz/HIP-3 INCLUDED (HL WS serves HIP-3, verified 2026-06-27).
        _wsf = getattr(self, "_ws_feed", None)
        if _wsf is not None:
            _wdf = _wsf.get_df(api_name, interval, limit)
            # WS-READY WAIT (money-opt 2026-06-28): at a bar boundary the just-closed bar may
            # not be pushed to the store yet -> get_df None -> the REST fallback below storms
            # (~26s on double 4h+8h boundaries, throttle 1.5s x ~17 coins). Instead wait the
            # MINIMUM for WS to deliver the closed bar (poll the in-mem store), self-adjusting
            # (~1-2s typical, NOT a fixed settle), capped by HL_WS_READY_MAX_SEC. The first
            # coin's wait lets WS catch up for ~all coins (pushed together) -> the rest hit on
            # the first get_df. Gated: ONLY past cold-start warmup (else 260x poll slows boot)
            # AND only while the feed is LIVE (recent msg). Cold-start / WS-dead -> skip, REST
            # now (warmup stays fast; degraded-safe = no worse than today).
            if (_wdf is None or len(_wdf) == 0) and not prime \
                    and (time.time() - self._proc_start) >= float(os.getenv("HL_COLD_START_SEC", "120")):
                _lmt = getattr(_wsf, "_last_msg_ts", 0) or 0
                if _lmt and (time.time() - _lmt) < 5.0:  # feed live -> bar is coming, not dead
                    _ws_cap = float(os.getenv("HL_WS_READY_MAX_SEC", "3"))
                    _t0 = time.time()
                    while (time.time() - _t0) < _ws_cap:
                        time.sleep(0.25)
                        _wdf = _wsf.get_df(api_name, interval, limit)
                        if _wdf is not None and len(_wdf) > 0:
                            break
            if _wdf is not None and len(_wdf) > 0:
                with self._cache_lock:
                    self._candles_cache[cache_key] = (current_bar_start, _wdf, time.time())
                return _wdf.copy()

        end = now_ms
        start = end - ms * (limit + 1)

        # 300ms + jitter between cache-miss requests (rate-limit protection).
        # Cold-start ramp: throttle harder while caches are still cold (first
        # HL_COLD_START_SEC after proc start) to flatten the post-restart 429 burst.
        # HIP-3 (dex=xyz) candle endpoint is rate-limited far tighter than main
        # perps — with ~69 xyz coins it is the sole source of the 429 storm.
        # Throttle xyz fetches harder than crypto majors (2026-05-31). Tunable.
        _is_hip3 = api_name.startswith("xyz:")
        if (time.time() - self._proc_start) < float(os.getenv("HL_COLD_START_SEC", "120")):
            _cold = float(os.getenv("HL_HIP3_COLD_SLEEP_SEC", "3.0")) if _is_hip3 \
                else float(os.getenv("HL_COLD_START_SLEEP_SEC", "1.8"))
            time.sleep(_cold + random.uniform(0, 0.4))
        else:
            _warm = float(os.getenv("HL_HIP3_FETCH_SLEEP_SEC", "1.5")) if _is_hip3 \
                else float(os.getenv("HL_MAIN_FETCH_SLEEP_SEC", "0.5"))
            time.sleep(_warm + random.uniform(0, 0.3))

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

        # Drop the in-progress bar — CONDITIONALLY (review fix 2026-06-10, class:
        # mem:feedback_indexer_drop_last_assumes_forming). The old unconditional
        # iloc[:-1] assumed the API response always ends with the forming bar; when the
        # forming bar is not (yet) in the response, it cut the just-CLOSED bar -> the
        # strategy decided on bar N-1 and bar N's signal was lost forever (scan runs
        # once per bar). Drop the last row ONLY if its ts is inside the current forming
        # bar (computed from TRUE now, not the jittered effective_now).
        _true_forming_start = (now_ms // ms) * ms
        _last_ts_ms = None
        if len(df_full) > 0:
            try:
                _last_ts_ms = int(pd.Timestamp(df_full.iloc[-1]["time"]).value // 10**6)
            except Exception:
                _last_ts_ms = None
        if len(df_full) > 1 and (_last_ts_ms is None or _last_ts_ms >= _true_forming_start):
            df = df_full.iloc[:-1].reset_index(drop=True)
        else:
            df = df_full.reset_index(drop=True)

        # WS-seed: warm push-feed store from this REST df so get_df serves immediately.
        _wsf2 = getattr(self, "_ws_feed", None)
        if _wsf2 is not None:
            try:
                _wsf2.seed(api_name, interval, df_full)
            except Exception:
                pass
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

    def invalidate_candles_cache(self, coin: str, interval: str) -> None:
        """Drop the candles cache entry for (coin, interval) — forces a real refetch.

        Review fix 2026-06-10: the per-(coin,interval) cache-offset jitter (up to 30s)
        can mark a STALE df as fresh right after bar close (cached[0] >= offset-shifted
        current_bar_start) -> the scanner would decide on bar N-1. The scanner calls
        this when the fetched df's last closed bar is older than the bar that triggered
        the scan, then refetches past the cache (also clears the min-TTL gate, which
        keys off the cached entry).
        """
        with self._cache_lock:
            self._candles_cache.pop((coin, interval), None)

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
        """DEPLOYABLE trading equity = spot USDC total + perp accountValue (main + HIP-3 dexes).

        UNIFIED-ACCT FIX 2026-06-30: the prior body returned portfolio.day.accountValueHistory[-1]
        — a portfolio AGGREGATE that INCLUDES non-deployable staked/delegated HYPE (delegatorSummary,
        1-day unbond). That over-stated the risk-sizing base (~8-21% on this acct; up to ~7.6x on a
        near-empty one). On an HL UNIFIED account the spot USDC balance IS the perp cross-margin
        collateral; staked HYPE is NOT deployable margin. This now matches carry
        rpc_read._hl_unified_equity + ib_live/bot/exchange.py.

        account_value() here feeds ONLY sizing (trader compute_size, depth-gate worst-order, resting
        sizing) — there is NO catastrophe/baseline gate on it, so switching to the deployable basis
        is strictly safer (smaller, non-inflated risk base). Cache 5s, 429 retry.
        Memory: reference_hl_unified_account_per_dex_margin ·
        project_hl_valantis_ui_equity_is_spot_usdc_not_unified_2026_06_26.
        """
        now = time.time()
        with self._cache_lock:
            av = self._av_cache
        if av is not None and (now - av[0]) < 5.0:
            return av[1]

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                # spot USDC = deployable collateral pool (unified account)
                spot = self.info.spot_user_state(self.settings.account_address)
                spot_usdc = sum(
                    float(b.get("total", 0) or 0)
                    for b in spot.get("balances", [])
                    if b.get("coin") == "USDC"
                )
                # perp accountValue (margin + uPnL): main + each HIP-3 dex (cross-margin per dex)
                st = self.info.user_state(self.settings.account_address)
                perp_av = float((st.get("marginSummary") or {}).get("accountValue", 0) or 0)
                for dex in self.HIP3_USDC_DEXES:
                    csd = self._hip3_clearinghouse_state(dex)
                    if csd:
                        perp_av += float((csd.get("marginSummary") or {}).get("accountValue", 0) or 0)
                val = float(spot_usdc) + float(perp_av)
                with self._cache_lock:
                    self._av_cache = (now, val)
                log.debug("account_value (deployable): spot_usdc $%.2f + perp $%.2f = $%.2f",
                          spot_usdc, perp_av, val)
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

        RAISES on fetch failure after retries — main AND any HIP-3 dex
        (Phase-0 2026-07-02). The old silent fallbacks (stale cache / {} /
        a dict missing a failed dex's positions) let callers read "absent"
        where the truth was UNKNOWN → phantom-guard K=3 mass-close +
        SL-cancel class. Every caller wraps this in try/except and treats
        the tick as UNKNOWN (defer/skip), so raising is the safe direction.
        """
        now = time.time()
        with self._cache_lock:
            pc = self._positions_cache
        if pc is not None and (now - pc[0]) < 20.0:
            return pc[1]

        last_exc: Exception | None = None
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
                    dex_exc: Exception | None = None
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
                            dex_exc = e
                            break
                    if dex_exc is not None:
                        # Phase-0 (2026-07-02): a dex-scoped read failure must NOT
                        # yield a dict silently missing that dex's positions (the
                        # bot's xyz_* legs would read as CLOSED). Raise — presence
                        # is UNKNOWN for the whole snapshot. (A "429" message here
                        # re-enters the outer retry budget before propagating.)
                        log.error("open_positions HIP-3 dex=%s fetch failed: %s — "
                                  "raising (presence UNKNOWN)", dex, dex_exc)
                        raise RuntimeError(
                            f"open_positions: HIP-3 dex={dex} fetch failed: {dex_exc}"
                        ) from dex_exc

                with self._cache_lock:
                    self._positions_cache = (now, out)
                return out
            except Exception as e:
                last_exc = e
                msg = str(e)
                if "429" in msg and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                break

        # Phase-0 (2026-07-02): never degrade to stale-cache/{} on failure — that
        # converted UNKNOWN into "absent" (phantom-guard mass-close class). Callers
        # catch and treat the tick as UNKNOWN.
        log.error("open_positions fetch failed after retries: %s — raising "
                  "(callers treat tick as UNKNOWN)", last_exc)
        raise last_exc if last_exc is not None else RuntimeError(
            "open_positions: fetch failed (no exception captured)")

    def margin_used_usd(self) -> float:
        """REAL exchange initial-margin used ($), summed over ALL account positions.

        Reads the authoritative per-position `marginUsed` field (== UI "Margin"),
        which open_positions() already returns inside every position dict for BOTH
        the main perp (info.user_state) AND every HIP-3 dex (clearinghouseState).
        Summing per-position marginUsed is therefore dex-agnostic and unified —
        it naturally spans main + HIP-3 with no double-count and no per-dex
        accountValue caveat, and equals the UI Initial/Maint aggregate.

        Present for BOTH cross and isolated positions (live-verified: isolated
        LLY/JPY/DKNG all report marginUsed) → manual/foreign isolated positions
        are correctly counted. This is the primitive the MM-cap gate must use
        instead of the old notional/leverage model (which re-margined every
        existing position at the NEW coin's leverage — wrong, since each position
        carries its own leverage, cross AND isolated).

        Raises on a genuine fetch failure (propagated from open_positions()); the
        caller MUST fail-closed (do NOT open) rather than fall back to a model.
        Returns 0.0 only when there are genuinely no open positions.
        """
        positions = self.open_positions()
        total = 0.0
        for p in positions.values():
            try:
                total += float(p.get("marginUsed", 0) or 0)
            except (TypeError, ValueError):
                # A position present but with an unpar. marginUsed must NOT be
                # silently dropped to 0 — that would under-count and weaken the
                # gate. Surface it so the caller fails closed.
                raise RuntimeError(
                    f"margin_used_usd: unparseable marginUsed "
                    f"{p.get('marginUsed')!r} for {p.get('coin')!r}"
                )
        return total

    def invalidate_positions_cache(self) -> None:
        """Call after open/close to force fresh fetch on next open_positions()."""
        with self._cache_lock:
            self._positions_cache = None

    def invalidate_user_state(self) -> None:
        """Drop the cached user_state so the next position_liquidation()/_user_state_cached()
        re-fetches. MUST be called after add_isolated_margin() — otherwise the REMEDY-A
        re-read returns the SAME stale pre-top-up liqPx for up to 10s, the loop never sees
        its own deposit converge, and it tops up margin 3× then clamps anyway (over-funds).
        NOTE: invalidate_positions_cache() clears a DIFFERENT cache (_positions_cache).
        """
        with self._cache_lock:
            self._user_state_cache = None

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

    def verify_builder_on_recent_fill(self, coin: str, max_age_sec: float = 300.0,
                                      tol_frac: float = 0.5) -> tuple[bool, str]:
        """Builder-attribution monitor (panel must-fix 2026-06-21). On an ENTRY OK, confirm the
        most-recent fill for `coin` carried a builderFee at ≈ the configured rate. A SILENT drop
        (builderFee null / 0 / wrong rate) loses the Valantis airdrop attribution and risks
        "account shut down" — and a first-fill-only check would miss a later drift (an early
        BTC/SOL cohort once paid 0.0010% = 1/8 the configured 0.008% and nothing flagged it).

        Returns (ok, detail). ok=True (benign) when builder is disabled, no recent fill is found,
        or the fills read fails — we never false-alarm on a transient read. ok=False ONLY on a
        positively-observed missing/zero/off-rate builderFee. Caller logs LOUD on ok=False.
        Gated to ENTRY OK by the caller (one user_fills read per fill) to respect the 429 throttle.
        """
        if not getattr(self, "_builder", None):
            return True, "builder-disabled"
        exp_rate = float(self._builder.get("f", 0) or 0) * 1e-5  # f tenths-of-bp → fraction (8→0.00008)
        if exp_rate <= 0:
            return True, "builder-fee-zero-configured"
        api = coin_to_api(coin)
        try:
            fills = self.user_fills(ttl_sec=5.0)
        except Exception as e:
            return True, f"fills-read-failed:{e}"  # can't prove bad → don't false-alarm
        now_ms = int(time.time() * 1000)
        rel = [f for f in (fills or [])
               if f.get("coin") == api and (now_ms - int(f.get("time", 0) or 0)) < max_age_sec * 1000]
        if not rel:
            return True, "no-recent-fill"
        f = rel[-1]
        try:
            px = float(f.get("px", 0) or 0)
            sz = abs(float(f.get("sz", 0) or 0))
            bf_raw = f.get("builderFee", None)
            if bf_raw is None or bf_raw == "":
                return False, f"builderFee MISSING on {api} fill (px={px} sz={sz}) — airdrop UNATTRIBUTED"
            bf = float(bf_raw)
            notional = px * sz
            rate = (bf / notional) if notional > 0 else 0.0
            if rate <= 0:
                return False, f"builderFee=0 on {api} (rate 0) — airdrop UNATTRIBUTED"
            if abs(rate - exp_rate) / exp_rate > tol_frac:
                return False, (f"builderFee rate {rate*100:.4f}% != expected {exp_rate*100:.4f}% "
                               f"on {api} (off by >{tol_frac*100:.0f}%) — attribution DRIFTED")
            return True, f"builderFee OK {rate*100:.4f}% on {api}"
        except Exception as e:
            return True, f"parse-failed:{e}"  # don't false-alarm on a parse hiccup

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

    def _hip3_clearinghouse_state(self, dex: str) -> dict | None:
        """Per-dex clearinghouseState for a HIP-3 dex (NOT cached — callers that need
        liqPx after a margin top-up must read fresh). Retry 3x on 429. None on failure.

        HIP-3 (xyz:*) positions live on a SEPARATE clearinghouse from the main perp
        user_state; liquidationPx for an isolated xyz_* coin is ONLY visible here.
        Source: same dex=xyz post used by open_positions (exchange_hl.py:654-668).
        """
        for attempt in range(3):
            try:
                return self.info.post("/info", {
                    "type": "clearinghouseState",
                    "user": self.settings.account_address,
                    "dex": dex,
                })
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                log.warning("clearinghouseState dex=%s failed: %s", dex, e)
                return None
        return None

    def position_liquidation(self, coin: str, fresh: bool = False) -> dict | None:
        """{liq_px, margin_mode, leverage} for open position. None if no position.

        HIP-3 (xyz_*) coins live on a SEPARATE per-dex clearinghouse — the main
        user_state NEVER contains their assetPositions, so liqPx for a thin isolated
        HIP-3 market is read from clearinghouseState(dex=...) instead. Without this the
        ensure_sl_inside_liq guard silently no-ops on exactly the HIP-3-isolated case
        the rule targets (feedback_sl_must_be_inside_liquidation).

        `fresh=True` bypasses the 10s user_state cache (HIP-3 reads are always fresh) —
        REMEDY-A's re-read after add_isolated_margin MUST set this or it observes the
        same stale pre-top-up liqPx and never converges.
        """
        api_name = coin_to_api(coin)

        # --- HIP-3: read the per-dex clearinghouse (never in main user_state) ---
        for dex in self.HIP3_USDC_DEXES:
            if not api_name.startswith(f"{dex}:"):
                continue
            dex_state = self._hip3_clearinghouse_state(dex)
            if dex_state is None:
                return None
            for ap in dex_state.get("assetPositions", []) or []:
                p = ap.get("position", {}) or {}
                if p.get("coin") != api_name:
                    continue
                if abs(float(p.get("szi", 0) or 0)) < 1e-12:
                    return None
                return self._liq_from_position(p)
            return None

        # --- Main perp: cached user_state (fresh=True forces a re-fetch) ---
        state = self._user_state_cached(ttl_sec=(0.0 if fresh else 10.0))
        if state is None:
            return None
        for ap in state.get("assetPositions", []) or []:
            p = ap.get("position", {}) or {}
            if p.get("coin") != api_name:
                continue
            if abs(float(p.get("szi", 0) or 0)) < 1e-12:
                return None
            return self._liq_from_position(p)
        return None

    @staticmethod
    def _liq_from_position(p: dict) -> dict:
        """Extract {liq_px, margin_mode, leverage} from a single assetPositions entry."""
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
        # Defense-in-depth DRY guard (audit LOW 2026-06-19): updateLeverage is a SIGNED
        # exchange action. Return a benign success-shaped dict under DRY so the caller
        # proceeds (does NOT abort) without signing a real leverage change.
        _d = self._dry_guard("update_leverage(%s lev=%d cross=%s)" % (coin, leverage, is_cross))
        if _d is not None:
            return _d
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

    def add_isolated_margin(self, coin: str, usd: float) -> dict | None:
        """Add isolated margin (USD) to an ISOLATED position (REMEDY-A primitive).

        Pushes the per-position liquidation price further from the SL by topping up
        collateral. SDK action: updateIsolatedMargin (isBuy=True → add). Only valid on
        an ISOLATED position; a cross position has account-level (None) liqPx and
        nothing to clamp against — caller must guard on margin_mode first.
        Returns the SDK response dict, or None on failure. Retry 4x on 429.
        Source: hyperliquid SDK exchange.update_isolated_margin(amount, name).
        """
        if usd <= 0:
            return None
        # Defense-in-depth DRY guard (audit LOW 2026-06-19): updateIsolatedMargin MOVES
        # real USD collateral. Under DRY return a benign dict without touching funds.
        _d = self._dry_guard("add_isolated_margin(%s $%.2f)" % (coin, usd))
        if _d is not None:
            return _d
        api_name = coin_to_api(coin)
        for attempt in range(4):
            try:
                resp = self.exchange.update_isolated_margin(round(float(usd), 6), api_name)
                # liqPx just moved — drop the cached user_state so a follow-up
                # position_liquidation() (REMEDY-A re-read) observes the top-up.
                self.invalidate_user_state()
                return resp
            except Exception as e:
                msg = str(e)
                if "429" in msg and attempt < 3:
                    backoff = 2 ** attempt
                    log.warning("add_isolated_margin(%s, $%.2f) 429 — retry %ds",
                                coin, usd, backoff)
                    time.sleep(backoff)
                    continue
                log.warning("add_isolated_margin(%s, $%.2f) failed: %s", coin, usd, e)
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

    def _dry_guard(self, op: str) -> dict | None:
        """Base-level DRY short-circuit for ALL SIGNED write methods (defense-in-depth).

        Mirrors exchange_pacifica.py (`if self.dry_run and method=='POST'`): the HL
        adapter previously had ZERO dry awareness, so any caller that reached a write
        method bypassed DRY entirely. Returns a benign SDK-shaped success dict (empty
        `statuses`, so no oid/fill is ever parsed -> never a real order) when DRY_RUN=1,
        else None (caller proceeds to the live SDK). READ methods are unaffected.
        """
        if getattr(self.settings, "dry_run", False):
            log.info("[DRY] HL write blocked at adapter: %s", op)
            return {"status": "ok",
                    "response": {"type": "order",
                                 "data": {"statuses": [], "dry_run": True}}}
        return None

    def market_open(self, coin: str, is_buy: bool, sz: float) -> dict:
        """Market order to open a position.

        SDK market_open: slippage param sets max allowable fill slippage.
        Source: old exchange.py market_open + settings.slippage from .env.
        """
        _d = self._dry_guard(f"market_open({coin} is_buy={is_buy} sz={sz})")
        if _d is not None:
            return _d
        api_name = coin_to_api(coin)
        sz_rounded = self.round_size(coin, sz)
        return self._retry_429(
            lambda: self.exchange.market_open(
                api_name, is_buy=is_buy, sz=sz_rounded,
                slippage=self.settings.slippage,
                builder=self._builder,
            ),
            f"market_open({coin})",
        )

    def market_close(self, coin: str) -> dict:
        """Market order to close entire position in coin."""
        _d = self._dry_guard(f"market_close({coin})")
        if _d is not None:
            return _d
        api_name = coin_to_api(coin)
        return self._retry_429(
            lambda: self.exchange.market_close(api_name, builder=self._builder),
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
        _d = self._dry_guard(f"trigger_sl({coin} sz={sz} px={trigger_px})")
        if _d is not None:
            return _d
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_rounded, limit_px=px,
                order_type=order_type, reduce_only=True,
                builder=self._builder,
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
        _d = self._dry_guard(f"trigger_tp({coin} sz={sz} px={trigger_px})")
        if _d is not None:
            return _d
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_rounded, limit_px=px,
                order_type=order_type, reduce_only=True,
                builder=self._builder,
            ),
            f"trigger_tp({coin})",
            attempts=3,
        )

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float, limit_px: float) -> dict:
        """Resting MAKER reduce-only LIMIT — for the 50% partial take-profit @ fib.

        order_type {"limit": {"tif": "Alo"}} = Add-Liquidity-Only (post-only): rests as
        maker (cheaper than a market/trigger TP), and HL rejects it if it would cross —
        so it can never pay taker. reduce_only=True caps it at the live position size.
        is_buy: True to close a SHORT (buy), False to close a LONG (sell).
        Best-effort: a failure just means no partial TP — the SL still fully covers the
        position (reduce-only), so this can never create a naked position.
        """
        api_name = coin_to_api(coin)
        px = self.round_price(coin, limit_px)
        sz_rounded = self.round_size(coin, sz)
        order_type = {"limit": {"tif": "Alo"}}
        _d = self._dry_guard(f"limit_reduce_only({coin} sz={sz} px={limit_px})")
        if _d is not None:
            return _d
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_rounded, limit_px=px,
                order_type=order_type, reduce_only=True,
                builder=self._builder,
            ),
            f"limit_reduce_only({coin})",
            attempts=2,
        )


    def resting_stop_limit(
        self, coin: str, is_buy: bool, sz: float,
        trigger_px: float, limit_px: float,
    ) -> dict:
        """Place a RESTING (non-reduce-only) stop-limit to OPEN a position.

        Used by RestingOrderManager for pre-placed entry orders.
        When triggerPx is breached, HL sends a limit order at limit_px.
        If market gaps past limit_px the order does NOT fill (same cap semantics
        as the live market_open flow).

        order_type: trigger with isMarket=False (limit fill on trigger).
        tpsl="sl": stop-trigger so a BUY fires only when mark>=trigger (real upside breakout); "tp" was a BUG that fired instantly (mark<=trig already true) = market fill.
        reduce_only=False (default): opens a position.

        Source: Exchange.order SDK, isMarket=False pattern from trigger_tp,
        reduce_only=False = default (confirmed from SDK source inspect).
        sz=0 is not valid on HL; caller must pass actual size when using for
        full-pipeline. For pre-placed resting orders we pass the risk-sized
        quantity computed from account equity at placement time.

        Returns raw SDK response dict. Caller checks statuses[0]["resting"].oid.
        """
        api_name = coin_to_api(coin)
        trig = self.round_price(coin, trigger_px)
        lim = self.round_price(coin, limit_px)
        sz_r = self.round_size(coin, sz) if sz > 0 else sz
        order_type = {"trigger": {"triggerPx": trig, "isMarket": False, "tpsl": "sl"}}  # sl = stop-BUY fires at mark>=trigger (real breakout); tp fired instantly (mark<=trig) -> market fills
        _d = self._dry_guard(f"resting_stop_limit({coin} sz={sz} trig={trigger_px} lim={limit_px})")
        if _d is not None:
            return _d
        return self._retry_429(
            lambda: self.exchange.order(
                api_name, is_buy=is_buy, sz=sz_r, limit_px=lim,
                order_type=order_type, reduce_only=False,
                builder=self._builder,
            ),
            f"resting_stop_limit({coin})",
            attempts=3,
        )

    def cancel_sl_order(self, coin: str, oid) -> dict:
        """Cancel a specific order by ID."""
        # Defense-in-depth DRY guard (audit LOW 2026-06-19): cancel is a SIGNED write that
        # removes a LIVE resting order. Under DRY return a benign dict — never cancel real
        # orders (which under DRY shouldn't exist, but the guard makes that invariant true).
        _d = self._dry_guard("cancel_sl_order(%s oid=%s)" % (coin, oid))
        if _d is not None:
            return _d
        api_name = coin_to_api(coin)
        return self._retry_429(
            lambda: self.exchange.cancel(api_name, int(oid)),
            f"cancel_sl({coin})",
            attempts=3,
        )

    def list_reduce_only_triggers(self) -> list[dict]:
        """ALL resting reduce-only TRIGGER orders (SL stop + TP trigger) across the
        main perp dex AND every HIP-3 USDC dex, normalized for the orphan_sweep.

        Mirror of list_open_sl_orders' fail-loud contract: if ANY dex query fails we
        RAISE (can't prove the resting set — the sweep then skips the cycle rather
        than act on a partial view). reduce-only LIMIT take-profits (isTrigger=False)
        are intentionally NOT returned — they are not trigger orders. Cancel path is
        cancel_sl_order(coin, oid)."""
        addr = self.settings.account_address
        dexes = [""] + list(self.HIP3_USDC_DEXES)
        all_orders: list = []
        _failed: list = []
        for dex in dexes:
            payload: dict = {"type": "frontendOpenOrders", "user": addr}
            if dex:
                payload["dex"] = dex
            try:
                chunk = self._retry_429(
                    lambda p=payload: self.info.post("/info", p),
                    f"frontendOpenOrders(dex={dex or 'main'})",
                    attempts=3,
                )
                if chunk:
                    all_orders.extend(chunk)
            except Exception as e:
                _failed.append(f"{dex or 'main'}: {e}")
        if _failed:
            raise RuntimeError(
                f"list_reduce_only_triggers: {len(_failed)}/{len(dexes)} dex queries "
                f"failed — cannot enumerate resting triggers ({_failed[0]})"
            )
        out: list[dict] = []
        for o in all_orders:
            if not (o.get("isTrigger") and o.get("reduceOnly")):
                continue
            oid = o.get("oid")
            if oid is None:
                continue
            out.append({
                "coin": api_to_coin(o.get("coin")),
                "oid": oid,
                "reduce_only": True,
                "is_trigger": True,
            })
        return out

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
        # Review fix 2026-06-10: this method used to SWALLOW every exception (bare post,
        # except->continue; fallback fail -> return []). The trader contract
        # "list threw -> assume SL live" (trader.py:_sl_confirmed_live) was therefore
        # dead: a 429 storm read as "no SL" -> duplicate-SL churn every tick, and a
        # sustained storm escalated to emergency MARKET-CLOSE of HEALTHY protected
        # positions. Now: per-dex fetch retries 429s; if ANY dex query still fails we
        # RAISE (can't prove absence — the SL may live on the failed dex), so the
        # caller's fail-safe assume-live path actually engages.
        _failed: list[str] = []
        for dex in dexes:
            payload: dict = {"type": "frontendOpenOrders", "user": addr}
            if dex:
                payload["dex"] = dex
            try:
                chunk = self._retry_429(
                    lambda p=payload: self.info.post("/info", p),
                    f"frontendOpenOrders(dex={dex or 'main'})",
                    attempts=3,
                )
                if chunk:
                    all_orders.extend(chunk)
            except Exception as e:
                _failed.append(f"{dex or 'main'}: {e}")
        if _failed:
            raise RuntimeError(
                f"list_open_sl_orders({coin}): {len(_failed)}/{len(dexes)} dex queries "
                f"failed — cannot prove SL absence ({_failed[0]})"
            )
        # NOTE: the old `open_orders()` fallback was removed — it lacks trigger fields
        # (bug fix 2026-05-05) so its rows could never pass the stop filter below; it
        # only ever masked failures as a false-empty [].

        out: list[int] = []
        for o in all_orders:
            if o.get("coin") != api_name:
                continue
            # A PROTECTIVE STOP is a reduce-only trigger whose orderType is a Stop
            # ("Stop Market" / "Stop Limit"). It fires against the position to close it.
            # Bug fix 2026-06-07: the old condition ("stop" in t OR isTrigger OR reduceOnly)
            # also matched a reduce-only TP order — the 50% partial take-profit is a plain
            # reduce-only "Limit" (limit_reduce_only, tif=Alo, isTrigger=False, triggerPx=0,
            # orderType="Limit"). Counting that as an SL let a position holding ONLY a TP be
            # mis-seen as PROTECTED by the naked-guards -> it would never get a real SL
            # (effectively naked). Require reduce_only AND a genuine stop-trigger.
            # A reduce-only=False trigger is a resting ENTRY stop-limit, not a protective SL
            # (those are handled by list_open_entry_trigger_orders). A tpsl=tp trigger would
            # carry orderType "Take Profit ..." (no "stop") and is likewise excluded.
            # Verified live 2026-06-07 against xyz_GBP (SL oid 460805943435 kept, TP oid
            # 459495553698 dropped), xyz_JPY, xyz_BRENTOIL (both real SLs still counted).
            if not o.get("reduceOnly"):
                continue
            t = str(o.get("orderType", "")).lower()
            if not (o.get("isTrigger") and "stop" in t):
                continue
            oid = o.get("oid")
            if oid:
                try:
                    out.append(int(oid))
                except (TypeError, ValueError):
                    pass
        return out


    def list_open_entry_trigger_orders(self) -> list[dict]:
        """List all open non-reduce-only trigger orders on the account (resting entries).

        Used by orphan startup sweep: cancel any open resting-entry triggers
        that have no matching position.

        Returns list of {coin: str (internal name), oid: int} dicts.
        Note: list_open_sl_orders already handles reduce_only=True SL triggers.
        This method captures reduce_only=False entry-side triggers (resting stop-limits).
        """
        addr = self.settings.account_address
        dexes = [""] + list(self.HIP3_USDC_DEXES)
        all_orders: list = []
        # Review fix 2026-06-10: same contract as list_open_sl_orders — retry 429s
        # per dex, RAISE if any dex query still fails (a false-empty [] would make the
        # orphan sweep silently skip cleanup; callers wrap in try/except).
        _failed: list[str] = []
        for dex in dexes:
            payload: dict = {"type": "frontendOpenOrders", "user": addr}
            if dex:
                payload["dex"] = dex
            try:
                chunk = self._retry_429(
                    lambda p=payload: self.info.post("/info", p),
                    f"frontendOpenOrders(dex={dex or 'main'})",
                    attempts=3,
                )
                if chunk:
                    all_orders.extend(chunk)
            except Exception as e:
                _failed.append(f"{dex or 'main'}: {e}")
        if _failed:
            raise RuntimeError(
                f"list_open_entry_trigger_orders: {len(_failed)}/{len(dexes)} dex "
                f"queries failed ({_failed[0]})"
            )

        out: list[dict] = []
        for o in all_orders:
            # Entry trigger: has a trigger field AND is NOT reduce_only
            # (reduce_only=True → SL handled by list_open_sl_orders)
            is_trigger = o.get("isTrigger") or "stop" in str(o.get("orderType", "")).lower()
            is_reduce = o.get("reduceOnly", False)
            if is_trigger and not is_reduce:
                oid = o.get("oid")
                raw_coin = o.get("coin", "")
                # Normalise HIP-3 coin (xyz:COIN → xyz_COIN for internal name)
                internal_coin = raw_coin.replace(":", "_") if ":" in raw_coin else raw_coin
                if oid:
                    try:
                        out.append({"coin": internal_coin, "oid": int(oid)})
                    except (TypeError, ValueError):
                        pass
        return out


    def orderbook_snapshot(self, coin: str):
        """L2 book snapshot -> (bids, asks), each [(px, sz), ...] best-first.

        Used by liquidity_snapshot depth gate. HIP-3: coin_to_api (xyz_GOLD->xyz:GOLD).
        """
        api_name = coin_to_api(coin)
        def _do():
            return self.info.post("/info", {"type": "l2Book", "coin": api_name})
        raw = _sdk_call_with_429_retry(_do, what="l2Book(%s)" % coin, max_attempts=4, sleep_sec=10.0)
        if not isinstance(raw, dict):
            return [], []
        levels = raw.get("levels") or []
        if len(levels) < 2:
            return [], []
        def _parse(side):
            out = []
            for lvl in side:
                try:
                    out.append((float(lvl["px"]), float(lvl["sz"])))
                except (KeyError, TypeError, ValueError):
                    continue
            return out
        return _parse(levels[0]), _parse(levels[1])


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
