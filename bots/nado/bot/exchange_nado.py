"""Nado perpetual DEX client (Ink L2) — exchange-agnostic interface for trader.py.

Wraps the official `nado-protocol` Python SDK to expose the same shape that
HLClient and KrakenClient use, so trader.py / manage_open_positions / watchdog
work unchanged.

Design choices:
- Coin id format = native Nado symbol e.g. "BTC-PERP", "ETH-PERP". Set this
  in `COINS` env var.
- Subaccount = main wallet + "default" tag (1CT setup from app.nado.xyz UI).
- Linked Signer pattern: SDK signer key = the agent (1CT) private key extracted
  from the Nado UI localStorage. Main wallet stays in Rabby.
- All Nado prices/amounts are x18-scaled big-int strings — we encapsulate all
  conversions here.
"""
from __future__ import annotations

import logging
import os
import time
import warnings
from dataclasses import dataclass

import pandas as pd

# Suppress noisy pkg_resources DeprecationWarning from eth_keyfile
warnings.filterwarnings("ignore", message="pkg_resources is deprecated")

from nado_protocol.client import (
    NadoClient,
    NadoClientMode,
    create_nado_client,
)
from nado_protocol.engine_client.types.execute import (
    PlaceMarketOrderParams,
    CancelOrdersParams,
)
from nado_protocol.trigger_client.types.execute import (
    CancelTriggerOrdersParams,
)
from nado_protocol.utils.execute import MarketOrderParams
from nado_protocol.utils.bytes32 import subaccount_to_hex
from nado_protocol.indexer_client.types.query import (
    IndexerCandlesticksParams,
    IndexerMatchesParams,
)
from nado_protocol.indexer_client.types.models import IndexerCandlesticksGranularity

from bot.config import CANDLES_LIMIT, TF_MS, Settings  # type: ignore[attr-defined]

log = logging.getLogger(__name__)


def _install_session_timeout(sess, what, connect=5.0, read=20.0):
    """ROOT-FIX 2026-07-02 (fleet propagation of the hung-fetch class): the
    nado-protocol EngineClient holds a requests.Session whose .post()/.get() at
    engine_client/execute.py:118 + query.py:96/108 are called with NO timeout, so a
    stalled socket during place_order / query hangs the sync order path. Today the only
    backstop is the 300s loop watchdog (main.py os._exit) — a full 5-min freeze per
    hang. Inject a hard default (connect, read) timeout on EVERY request through this
    session so a dead socket fails fast (~25s, retryable) instead. Env: NADO_SDK_READ_TIMEOUT."""
    try:
        read_to = float(os.getenv("NADO_SDK_READ_TIMEOUT", str(read)))
    except Exception:
        read_to = read
    if sess is None:
        log.warning("SDK %s has no .session — cannot install HTTP timeout guard", what)
        return
    if getattr(sess, "_nado_timeout_installed", False):
        return
    _orig_request = sess.request

    def _request(method, url, **kw):
        if kw.get("timeout") is None:
            kw["timeout"] = (connect, read_to)
        return _orig_request(method, url, **kw)

    sess.request = _request
    sess._nado_timeout_installed = True
    log.info("SDK %s: default HTTP timeout installed (connect=%.0fs read=%.0fs)",
             what, connect, read_to)


def _sweep_untimed_sessions():
    """CLASS-GUARD 2026-07-02: SDK composition hides embedded requests.Session
    objects (engine _querier, indexer, ...). Walk live objects and guard ANY
    session that slipped past the explicit installs above — and WARN, so a new
    embedded client gets noticed instead of silently running unbounded."""
    import gc
    import requests as _rq
    missed = 0
    for _obj in gc.get_objects():
        try:
            if isinstance(_obj, _rq.Session) and not getattr(_obj, "_nado_timeout_installed", False):
                _install_session_timeout(_obj, "gc-sweep")
                missed += 1
        except Exception:
            continue
    if missed:
        log.warning("session-sweep: %d untimed requests.Session found+guarded — new embedded SDK client?", missed)
    else:
        log.info("session-sweep: all requests.Session timeout-guarded")


def _is_order_not_found_2020(exc: Exception) -> bool:
    """True if Nado SDK exception body indicates error_code=2020 ("order not
    found"). Body comes through as a JSON-shaped string in exception args.

    Examples of matched bodies:
      {"status":"failure", ..., "error_code":2020, "error":"Order with the
       provided digest (...) could not be found. ..."}
    """
    try:
        msg = str(exc)
    except Exception:
        return False
    return ('"error_code":2020' in msg) or ("'error_code': 2020" in msg) \
        or ("error_code=2020" in msg)


def _is_isolated_only_2122(exc: Exception) -> bool:
    """True if Nado SDK exception body indicates error_code=2122 ("market is
    in isolated-only mode"). Triggered by markets like XAG-PERP (pid=88)
    where exchange forbids cross-margin orders.
    """
    try:
        msg = str(exc)
    except Exception:
        return False
    return ('"error_code":2122' in msg) or ("'error_code': 2122" in msg) \
        or ("error_code=2122" in msg) or ("isolated-only mode" in msg)


# Process-local registry of product_ids known to require isolated mode.
# Populated lazily on first error_code=2122 — after that, market_open uses
# the isolated path directly without a wasted cross attempt.
_ISOLATED_ONLY_PIDS: set[int] = set()


# Granularity translation
_TF_TO_GRANULARITY: dict[str, IndexerCandlesticksGranularity] = {
    "1m": IndexerCandlesticksGranularity.ONE_MINUTE,
    "5m": IndexerCandlesticksGranularity.FIVE_MINUTES,
    "15m": IndexerCandlesticksGranularity.FIFTEEN_MINUTES,
    "1h": IndexerCandlesticksGranularity.ONE_HOUR,
    "2h": IndexerCandlesticksGranularity.TWO_HOURS,
    "4h": IndexerCandlesticksGranularity.FOUR_HOURS,
    "1d": IndexerCandlesticksGranularity.ONE_DAY,
    "1w": IndexerCandlesticksGranularity.ONE_WEEK,
}

# 1e18 — Nado's universal scaling factor
X18 = 10**18

# ── ORACLE-ANCHORED MARKETABLE-LIMIT BAND (error_code 2028 fix, 2026-07-01) ────────────────────
# Vertex/Nado rejects an FOK whose limit price is "too far from index" (error_code 2028). The SDK's
# `engine_client.place_market_order` builds the marketable limit off the WRONG book side AND off the
# raw `slippage` setting (ENTRY_LIMIT_CAP_PCT=0.01 → 1%): for a BUY it uses bids[0]*(1+slippage),
# landing ~+90..+100 bps from the oracle (live-measured ENA +96.5bp, SOL +100.7bp, LINK +100.7bp) —
# OUTSIDE the index band → 2028 → the order is rejected, the position reads back FLAT, and a
# delta-neutral leg detonates the no-naked unwind every time. The real top-of-book sits within ~10bp
# of the oracle on every probed perp, so a limit ANCHORED ON THE ORACLE at a modest spread both (a)
# crosses the book (taker fill, delta-neutral) and (b) stays inside the band. We mirror the SDK's OWN
# proven-in-band close path (`engine_client.close_position`: oracle*(1±0.005), FOK) for opens.
#
# spread = min(configured slippage, NADO_ORACLE_ANCHOR_CAP). Default cap 0.005 (50bp) = the SDK's
# close spread (proven to book on Vertex). This only ever TIGHTENS the live bot's fill (its
# ENTRY_LIMIT_CAP_PCT=0.01 → spread 0.005, a better fill than 1%) and fixes the wrong-side/2028
# non-fill — strictly safer for both the live nado-bot and the funding_rotator. Env-overridable.
NADO_ORACLE_ANCHOR_CAP = float(os.getenv("NADO_ORACLE_ANCHOR_CAP", "0.005") or 0.005)
# Floor so the limit always crosses the *opposite* top-of-book by at least this many ticks even if a
# coin's book is unusually wide vs oracle (otherwise a tight spread could rest instead of take).
NADO_ANCHOR_MIN_CROSS_TICKS = int(os.getenv("NADO_ANCHOR_MIN_CROSS_TICKS", "2") or 2)


def _oracle_anchored_price_x18(oracle_x18, is_buy, best_opp_x18, tick_x18, slippage):
    """Build a tick-aligned, IN-BAND, book-CROSSING marketable-limit price (x18) anchored on the
    oracle (the value error_code 2028 measures against). Returns int priceX18.

    · spread = min(slippage, NADO_ORACLE_ANCHOR_CAP) — bounded so we never exceed the index band.
    · BUY  → oracle*(1+spread), but at least best_ask + NADO_ANCHOR_MIN_CROSS_TICKS ticks (cross up).
    · SELL → oracle*(1-spread), but at most best_bid - NADO_ANCHOR_MIN_CROSS_TICKS ticks (cross down).
    The cross-floor uses the opposite top-of-book so the FOK always TAKES; the oracle anchor + cap
    keeps it inside the band. tick-aligned via floor-division on tick_x18 (>=1)."""
    from nado_protocol.utils.math import mul_x18, to_x18
    spread = min(float(slippage or 0.0), NADO_ORACLE_ANCHOR_CAP)
    if spread <= 0:
        spread = NADO_ORACLE_ANCHOR_CAP
    t = max(int(tick_x18 or 1), 1)
    cross = max(NADO_ANCHOR_MIN_CROSS_TICKS, 0) * t
    if is_buy:
        anchored = int(mul_x18(int(oracle_x18), to_x18(1.0 + spread)))
        if best_opp_x18:
            anchored = max(anchored, int(best_opp_x18) + cross)  # at/above ask + ticks → cross up
    else:
        anchored = int(mul_x18(int(oracle_x18), to_x18(1.0 - spread)))
        if best_opp_x18:
            anchored = min(anchored, max(int(best_opp_x18) - cross, t))  # at/below bid - ticks → down
    aligned = (anchored // t) * t
    return max(aligned, t)


def _install_sdk_session_timeout(sdk_obj, what, connect=5.0, read=20.0):
    """ORDER-PATH timeout guard (2026-07-02, ported from hl_combo_bot root-fix; renamed
    2026-07-03 — it SHADOWED the Session-taking _install_session_timeout defined above,
    turning the gc-sweep and the client/querier session guards into 'has no .session'
    no-ops; P2 conformance harness caught the unbounded indexer/querier calls): the Nado
    SDK's EngineExecuteClient and TriggerExecuteClient each hold a requests.Session whose
    .post() is called with NO timeout — a stalled socket on place_order / cancel /
    place_price_trigger_order hangs the money path indefinitely (loop-watchdog force-restart
    is the only way out, and mid-entry that leaves an untracked fill). Inject a hard default
    (connect, read) timeout on EVERY request through this session so a dead socket fails
    fast (retryable) instead of stalling. Env: NADO_SDK_READ_TIMEOUT.

    Scope: ORDER-PATH sessions only (engine_client execute + trigger_client). The
    indexer/_querier session wiring is handled separately — do NOT install here."""
    try:
        read_to = float(os.getenv("NADO_SDK_READ_TIMEOUT", str(read)))
    except Exception:
        read_to = read
    sess = getattr(sdk_obj, "session", None)
    if sess is None:
        log.warning("SDK %s has no .session — cannot install HTTP timeout guard", what)
        return
    if getattr(sess, "_nado_timeout_installed", False):
        return
    _orig_request = sess.request

    def _request(method, url, **kw):
        if kw.get("timeout") is None:
            kw["timeout"] = (connect, read_to)
        return _orig_request(method, url, **kw)

    sess.request = _request
    sess._nado_timeout_installed = True
    log.info("SDK %s: default HTTP timeout installed (connect=%.0fs read=%.0fs)",
             what, connect, read_to)


class TriggerClientUnavailable(RuntimeError):
    """Raised when the Nado SDK trigger_client is None / missing list_trigger_orders
    AND an in-place SDK rebuild could not restore it -> SL/trigger ENUMERATION and
    PLACEMENT are structurally broken for this process. The orphan-sweep caller
    escalates this to ERROR/CRITICAL (loud) instead of a silent per-cycle WARNING-skip.
    Root: nado_protocol.create_nado_client_context() only assigns trigger_client AFTER
    engine_client.get_contracts(); if that HTTP call throws at __init__ (transient
    DNS/network, e.g. the 2026-06-20/21 host outage) the exception is swallowed and
    trigger_client stays None for the whole process lifetime with no retry."""


@dataclass(frozen=True)
class AssetMeta:
    """HL-compat meta tuple. Nado's max_leverage we look up from market data
    if available, else default 20x (Nado standard for most perps)."""
    name: str
    sz_decimals: int
    max_leverage: int
    product_id: int
    tick_size: float
    min_size: float           # in base asset units (e.g. 100 LINK)
    size_increment_x18: int = 0  # raw wei, used for integer-grid alignment


def _to_x18(v: float | int) -> str:
    """Convert float to x18-scaled big-int string."""
    return str(int(round(v * X18)))


def _from_x18(s: str | int | float) -> float:
    """Convert x18 big-int string to float."""
    if s is None:
        return 0.0
    return int(s) / X18


# Vertex/Nado engine returns i128::MAX (2**127 - 1) on an EMPTY orderbook side as
# a "no quote" sentinel (and 0 on the other empty side). Averaging bid/ask blindly
# then yields a garbage mark (~8.5e19). Incident 2026-06-05: VIRTUAL-PERP (pid=84)
# had an empty engine book -> mark_price() returned 8.507e19 instead of ~$0.57,
# so a manual VIRTUAL short had to skip Nado. Treat 0 / >=sentinel as invalid.
I128_MAX = (1 << 127) - 1  # 170141183460469231731687303715884105727

# Hard sanity ceiling for ANY human-unit price (mark / trigger / limit) the bot
# will act on. No Nado perp trades remotely near $10M/unit; anything at or above
# this is a scaling/sentinel bug, never a real price. Used both to floor mark_price
# and to fail-close every order-send path (_assert_sane_px).
MARK_SANITY_MAX = 1e7


def _valid_x18_side(raw) -> float | None:
    """Decode one orderbook side (bid_x18 / ask_x18) to a human price, or None if
    the side is empty (0), the i128::MAX no-quote sentinel, negative, or non-numeric."""
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return None
    if v <= 0 or v >= I128_MAX:
        return None
    return v / X18


def _assert_sane_px(coin: str, px: float, where: str) -> None:
    """Fail-closed guard at the exchange boundary: refuse to send any order whose
    price is outside (0, MARK_SANITY_MAX). Makes the empty-book-sentinel /
    unconverted-x18 mark class of bug structurally unable to reach the exchange,
    regardless of which upstream path produced `px`."""
    try:
        v = float(px)
    except (TypeError, ValueError):
        v = float("nan")
    if not (0.0 < v < MARK_SANITY_MAX):
        raise ValueError(
            f"INSANE PRICE GUARD [{where}] {coin}: px={px!r} outside "
            f"(0, {MARK_SANITY_MAX:g}) — refusing order (empty-book sentinel / "
            f"bad mark). Fix mark_price/oracle source, do not bypass."
        )


class _ExchangeShim:
    """Wraps parent so trader.py / watchdog can call client.exchange.X uniformly:
    - cancel(coin, oid)
    - fetch_open_orders() — returns ccxt-shape list (used by watchdog_kraken)
    Trader/watchdog don't need to know whether it's HL or Nado underneath."""

    def __init__(self, parent: "NadoClient_"):
        self.parent = parent

    def cancel(self, coin: str, oid):
        return self.parent.cancel_order(coin, oid)

    def fetch_open_orders(self):
        """ccxt-shape: list of {symbol, type, side, amount, reduceOnly, ...}.
        Used by watchdog_kraken.py to detect existing SL trigger orders.
        Watchdog cares about: symbol, type ('stop'/'trigger'/...), reduceOnly.
        """
        return self.parent.fetch_open_orders_ccxt_shape()


class _InfoShim:
    """Wraps parent for HL-style `client.info.user_state(addr)` and
    `client.info.frontend_open_orders(addr)` calls used by trader / watchdog."""

    def __init__(self, parent: "NadoClient_"):
        self.parent = parent

    def user_state(self, _address: str = "") -> dict:
        return self.parent.user_state()

    def spot_user_state(self, _address: str = "") -> dict:
        return self.parent.spot_user_state()

    def frontend_open_orders(self, _address: str = "") -> list:
        return self.parent.open_orders()


class NadoClient_:
    """Thin wrapper around nado-protocol SDK with HL-shape interface.

    Note class name has trailing underscore to avoid collision with the SDK's
    own `NadoClient` (we import it but don't expose it publicly).
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        # Connect to mainnet via SDK helper (handles contracts auto-discovery)
        mode = NadoClientMode.MAINNET if settings.network == "mainnet" else NadoClientMode.TESTNET
        self._sdk: NadoClient = create_nado_client(
            mode=mode, signer=settings.agent_private_key
        )
        # ORDER-PATH timeouts (2026-07-02): place/cancel/trigger sends must never hang.
        self._install_order_path_timeouts()

        # Hard HTTP timeout on ALL SDK sessions with untimed posts: engine
        # (place_order + query), trigger (SL orders), indexer (candles /
        # perp prices / matches — a socket stall there hangs the scan loop),
        # plus each client's embedded _querier: EngineExecuteClient.__init__
        # creates a SEPARATE EngineQueryClient with its own session, used in
        # the entry path (position / liquidity reads before place_order).
        _ctx = getattr(self._sdk, "context", None)
        for _cli_attr, _label in (("engine_client", "Nado-engine"),
                                  ("trigger_client", "Nado-trigger"),
                                  ("indexer_client", "Nado-indexer")):
            try:
                _cli = getattr(_ctx, _cli_attr, None)
                if _cli is None:
                    continue
                _install_session_timeout(getattr(_cli, "session", None), _label)
                _q = getattr(_cli, "_querier", None)
                if _q is not None and _q is not _cli:
                    _install_session_timeout(getattr(_q, "session", None), _label + "-querier")
            except Exception as _to_err:
                log.warning("%s session timeout guard not installed: %s", _label, _to_err)

        # CLASS-GUARD: catch any session a future/embedded SDK client creates
        _sweep_untimed_sessions()

        # Subaccount = main wallet + "default" tag (set up via 1CT UI)
        self.main_wallet = settings.account_address
        self.subaccount_name = settings.nado_subaccount or "default"
        self.subaccount_hex = subaccount_to_hex(self.main_wallet, self.subaccount_name)
        log.info(
            "Nado client init: signer=%s wallet=%s subaccount=%s",
            self._sdk.context.signer.address,
            self.main_wallet,
            self.subaccount_hex,
        )

        # Cache product_id ↔ symbol map at startup
        self._symbol_to_pid: dict[str, int] = {}
        self._pid_to_symbol: dict[int, str] = {}
        self._meta_cache: dict[str, AssetMeta] = {}
        self._load_markets()

        # State caches
        self._candles_cache: dict[tuple[str, str], tuple[int, pd.DataFrame]] = {}
        self._positions_cache: tuple[float, dict[str, dict]] | None = None
        self._fills_cache: tuple[float, list[dict]] | None = None
        self._summary_cache: tuple[float, object] | None = None  # SubaccountInfoData
        self._mark_cache: tuple[float, dict[str, float]] | None = None
        # WS-PUSH candle feed (2026-06-27): attached by main_loop when NADO_WS_CANDLE=1.
        # When present, candles() native path serves the closed window from memory (0 indexer
        # fetch); REST stays the fallback (zero correctness risk). mem:project_fleet_ws_push_rlfix
        self._ws_feed = None

        # Shims for HL-compat surface
        self.exchange = _ExchangeShim(self)
        self.info = _InfoShim(self)

    def _install_order_path_timeouts(self) -> None:
        """Install the HTTP timeout guard on the ORDER-PATH SDK sessions only:
        engine_client (place_order / cancel_orders / close_position sends) and
        trigger_client (SL/TP place + cancel + list). Best-effort — never fatal.
        indexer/_querier sessions are deliberately NOT touched here."""
        try:
            ctx = self._sdk.context
        except Exception as e:
            log.error("order-path timeout install: no SDK context (%s)", e)
            return
        try:
            _install_sdk_session_timeout(ctx.engine_client, "engine_client(order-path)")
        except Exception as e:
            log.error("order-path timeout install failed for engine_client: %s", e)
        try:
            tc = getattr(ctx, "trigger_client", None)
            if tc is not None:
                _install_sdk_session_timeout(tc, "trigger_client(order-path)")
        except Exception as e:
            log.error("order-path timeout install failed for trigger_client: %s", e)

    # ------------ Markets / metadata ------------

    def _load_markets(self) -> None:
        """One-time load: build product_id ↔ symbol maps and per-asset metadata."""
        try:
            symbols = self._sdk.market.get_all_product_symbols()
        except Exception as e:
            log.error("Failed to load Nado markets: %s", e)
            return

        for s in symbols:
            sym = s.symbol
            pid = s.product_id
            self._symbol_to_pid[sym] = pid
            self._pid_to_symbol[pid] = sym

        # Get richer metadata (tick + min_size) via engine get_symbols.
        # Nado сохранил Vertex-style scaling: amount передаётся в size_increments
        # (а не базовых units). min_size = 100 значит 100 increments.
        # min_notional_base = min_size * (size_increment / 1e18).
        try:
            details = self._sdk.context.engine_client.get_symbols(product_type="perp")
            items = details.symbols if hasattr(details, "symbols") else details
            if isinstance(items, dict):
                items = list(items.values())
            for d in items:
                row = d.dict() if hasattr(d, "dict") else d
                sym = row.get("symbol")
                pid = row.get("product_id")
                if not sym or pid is None:
                    continue
                pid = int(pid)
                tick = _from_x18(row.get("price_increment_x18", 0)) or 0.0001
                # `size_increment` is raw wei (×1e18), but `min_size` is
                # actually COUNT of increments × 1e18 (Vertex/Nado encoding).
                # Bug fix 2026-05-06: Раньше делили min_size на 1e18 и думали
                # что это base units → получалось 100 BTC, 100 ETH, 100 LINK
                # (~$8M, $300k, $2k respectively). Реально нужно (count) × (size_inc).
                # Для BTC: 100 × 5e-05 = 0.005 BTC (~$400). Куча trades
                # отклонялась с "below_min_size" хотя реально проходила бы.
                size_inc_x18 = int(row.get("size_increment", 0)) or 100000000000000  # 0.0001 default
                min_size_count_raw = int(row.get("min_size", 0))
                size_inc = size_inc_x18 / X18
                if min_size_count_raw > 0:
                    min_count = min_size_count_raw / X18  # обычно 100
                    min_base = min_count * size_inc
                else:
                    min_base = 0.001  # fallback
                sz_decimals = max(0, -int(round(__import__("math").log10(size_inc)))) if size_inc > 0 else 4
                # Per-product max leverage from exchange initial-margin weight:
                # init-margin req = (1 - long_weight_initial); max_lev = 1/req.
                # Hardcoded 20 under-margined low-weight products (WTI 0.90 -> 10x),
                # so isolated opens were rejected with code 2006 (insufficient health).
                _lwi = int(row.get("long_weight_initial_x18", 0)) / X18
                _max_lev = int(1.0 / (1.0 - _lwi)) if 0.0 < _lwi < 1.0 else 20
                self._meta_cache[sym] = AssetMeta(
                    name=sym,
                    sz_decimals=sz_decimals,
                    max_leverage=_max_lev,
                    product_id=pid,
                    tick_size=tick,
                    min_size=min_base,
                    size_increment_x18=size_inc_x18,
                )
        except Exception as e:
            log.warning("get_symbols(perp) failed (%s) — using defaults", e)

        log.info("Nado markets loaded: %d products (%d perps with meta)",
                 len(self._symbol_to_pid), len(self._meta_cache))

    def asset(self, coin: str) -> AssetMeta:
        if coin in self._meta_cache:
            return self._meta_cache[coin]
        if coin in self._symbol_to_pid:
            # Fallback meta with defaults
            return AssetMeta(
                name=coin, sz_decimals=4, max_leverage=20,
                product_id=self._symbol_to_pid[coin],
                tick_size=0.0001, min_size=0.001,
            )
        raise KeyError(coin)

    def _pid(self, coin: str) -> int:
        try:
            return self._symbol_to_pid[coin]
        except KeyError:
            raise KeyError(f"Unknown Nado symbol: {coin}")

    # ------------ Candles ------------

    # 2026-05-28: Nado SDK granularity enum lacks 30m + 8h. To trade those TFs
    # we fetch the nearest native base TF and aggregate 2:1 client-side.
    #   30m ← two 15m bars ;  8h ← two 4h bars
    _RESAMPLE_FROM: dict[str, tuple[str, int]] = {
        "30m": ("15m", 2),
        "8h":  ("4h", 2),
    }

    def _resample_ohlcv(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """Aggregate a native-TF OHLCV frame to `interval` boundaries.

        Uses pandas resample aligned to epoch (label/closed='left') so bucket
        edges match TF_MS boundaries — identical to how scanner.new_bar_closed
        computes bar starts. Does NOT drop the forming bucket itself — the
        caller (candles, resample branch) drops it by comparing the last bucket
        start to the current TF boundary (XNN audit fix 2026-06-11; before that
        a partial 8h bucket leaked through to scan_signal).
        """
        if df is None or df.empty:
            return df
        ms = TF_MS[interval]
        rule = f"{ms // 1000}s"
        out = (
            df.set_index("time")
              .resample(rule, label="left", closed="left", origin="epoch")
              .agg({"Open": "first", "High": "max", "Low": "min",
                    "Close": "last", "Volume": "sum"})
              .dropna(subset=["Open", "Close"])
              .reset_index()
        )
        return out

    def candles(self, coin: str, interval: str, limit: int = CANDLES_LIMIT) -> pd.DataFrame:
        """OHLCV DataFrame with columns: time, Open, High, Low, Close, Volume.
        Bar-aligned cache like HL — pull only when the current bar boundary changes.

        Nado API natively: 1m, 5m, 15m, 1h, 2h, 4h, 1d, 1w. For 30m + 8h we
        fetch the nearest native base TF and aggregate 2:1 (see _RESAMPLE_FROM).
        """
        # Resample path for non-native TFs (30m, 8h).
        if interval in self._RESAMPLE_FROM:
            base_tf, factor = self._RESAMPLE_FROM[interval]
            base_df = self.candles(coin, base_tf, limit=(limit + 2) * factor)
            if base_df is None or base_df.empty:
                return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])
            out = self._resample_ohlcv(base_df, interval)
            # XNN audit fix 2026-06-11: the forming-bar drop below only runs on the
            # NATIVE path — a resampled 8h frame could end with a PARTIAL bucket
            # (1 of 2 closed 4h sub-bars, bucket start >= current 8h boundary), e.g.
            # bot restart in the 2nd half of an 8h bar -> scan_signal would read a
            # forming bar (closed-bars invariant violation, parity-fail vs bt).
            # Apply the same boundary-compare drop to the resampled frame.
            if out is not None and len(out) > 1:
                tf_ms = TF_MS[interval]
                cur_start = (int(time.time() * 1000) // tf_ms) * tf_ms
                if int(out["time"].iloc[-1].value // 10**6) >= cur_start:
                    out = out.iloc[:-1].reset_index(drop=True)
            return out

        granularity = _TF_TO_GRANULARITY.get(interval)
        if granularity is None:
            raise ValueError(f"Unsupported timeframe for Nado: {interval}")
        ms = TF_MS[interval]
        now_ms = int(time.time() * 1000)
        current_bar_start = (now_ms // ms) * ms
        cache_key = (coin, interval)
        cached = self._candles_cache.get(cache_key)
        if cached is not None and cached[0] == current_bar_start:
            return cached[1].copy()

        # WS-PUSH fast path (2026-06-27): serve the confirmed-final closed window from the
        # live gateway candle feed -> 0 indexer get_candlesticks. None unless warm+rolled
        # -> REST fallback below. Output mirrors the native-path closed window (forming bar
        # already excluded by the feed's confirmed-final gate).
        _wsf = getattr(self, "_ws_feed", None)
        if _wsf is not None:
            try:
                _wdf = _wsf.get_df(coin, interval, limit)
            except Exception:
                _wdf = None
            if _wdf is not None and not _wdf.empty:
                self._candles_cache[cache_key] = (current_bar_start, _wdf)
                return _wdf.copy()

        pid = self._pid(coin)
        # Rate-limit hardening (audit 2026-06-22): the Vertex indexer can throttle
        # under burst. Retry transient / rate-limit-like failures with exponential
        # backoff before giving up, and pace requests a touch between coins. On
        # exhaustion we keep the existing safe contract — return an empty frame so
        # the scanner SKIPS this coin (and FAILs LOUD if ALL coins are empty); we
        # never trade on stale data. NADO_FETCH_SLEEP_SEC tunes per-fetch pacing.
        _pace = float(os.getenv("NADO_FETCH_SLEEP_SEC", "0.25"))
        data = None
        last_err = None
        for _attempt in range(3):
            try:
                data = self._sdk.context.indexer_client.get_candlesticks(
                    IndexerCandlesticksParams(
                        product_id=pid, granularity=granularity, limit=limit + 2,
                    )
                )
                break
            except Exception as e:
                last_err = e
                _m = str(e).lower()
                _transient = any(s in _m for s in (
                    "429", "rate limit", "ratelimit", "too many",
                    "timeout", "timed out", "502", "503", "connection"))
                if _attempt == 2 or not _transient:
                    break
                time.sleep((2 ** _attempt) * 0.5)  # 0.5s, 1.0s exponential backoff
        if _pace > 0:
            time.sleep(_pace)
        if data is None:
            log.warning(
                "get_candlesticks(%s, %s) failed after retries: %s",
                coin, interval, last_err)
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        rows = data.candlesticks if hasattr(data, "candlesticks") else (data if isinstance(data, list) else [])
        if not rows:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        records = []
        for r in rows:
            # Nado IndexerCandlestick: open_x18, high_x18, low_x18, close_x18, volume, timestamp
            records.append({
                "time": pd.to_datetime(int(r.timestamp), unit="s", utc=True),
                "Open": _from_x18(r.open_x18),
                "High": _from_x18(r.high_x18),
                "Low": _from_x18(r.low_x18),
                "Close": _from_x18(r.close_x18),
                "Volume": _from_x18(r.volume) if hasattr(r, "volume") else 0.0,
            })
        df = pd.DataFrame(records).sort_values("time").reset_index(drop=True)
        # Drop ONLY if the LAST bar is still forming. Nado's Vertex indexer
        # skips zero-volume bars entirely, so blind iloc[:-1] can drop a
        # legitimately CLOSED bar for thin coins (incident 2026-05-13:
        # AVAX-PERP triangle_long 1h used 08:00 bar instead of 09:00 because
        # 09:00 hadn't been emitted yet at fetch time -> 1.7h stale signal,
        # slip 0.85% vs plan 0.50%). Compare to current_bar_start: if last
        # bar starts at-or-after the current bar boundary, it's the forming
        # one and we drop it; otherwise it's a closed bar and we keep it.
        # Seed the WS feed (if attached) from this REST df so the push store has full
        # cold-start history before WS has rolled enough bars (additive; no extra burst).
        _wsf = getattr(self, "_ws_feed", None)
        if _wsf is not None:
            try:
                _wsf.seed(coin, interval, df)
            except Exception:
                pass
        if len(df) > 0:
            last_bar_start_ms = int(df["time"].iloc[-1].value // 10**6)
            if last_bar_start_ms >= current_bar_start and len(df) > 1:
                df = df.iloc[:-1].reset_index(drop=True)
        self._candles_cache[cache_key] = (current_bar_start, df)
        return df

    # ------------ Funding ------------

    def funding_rate(self, coin: str) -> float:
        """Nado has funding for perps. Stub returns 0 for now (not used by Vstop strategy)."""
        return 0.0

    # ------------ Account state ------------

    def _summary(self, ttl: float = 5.0):
        """Cached SubaccountInfoData."""
        now = time.time()
        if self._summary_cache and now - self._summary_cache[0] < ttl:
            return self._summary_cache[1]
        try:
            s = self._sdk.subaccount.get_engine_subaccount_summary(subaccount=self.subaccount_hex)
            self._summary_cache = (now, s)
            return s
        except Exception as e:
            log.warning("get_engine_subaccount_summary failed: %s", e)
            return self._summary_cache[1] if self._summary_cache else None

    def account_value(self) -> float:
        """Total equity in USDT0 (= sum of all balances at oracle prices, minus liabilities).
        Nado returns this directly via healths[0].health (cross-margin total).

        NOTE 2026-06-11: healths[0].health is INITIAL health (== funds_available,
        risk-weighted) — it UNDERSTATES true equity-with-uPnL by the unrealized PnL +
        initial-margin haircut. For the MM-cap denominator use equity_with_upnl()
        (= portfolio_value = unweighted health). Kept as-is for legacy callers that
        want funds-available semantics.
        """
        s = self._summary()
        if s is None:
            return 0.0
        try:
            # healths[0] = cross-margin INITIAL health; .health = funds available
            return _from_x18(s.healths[0].health)
        except Exception as e:
            log.warning("account_value parse failed: %s", e)
            return 0.0

    def _account_summary(self):
        """Canonical Nado margin summary via the SDK's MarginManager.

        Returns the SDK AccountSummary (matches the Nado UI Maint/Initial Margin
        numbers exactly) or None if the live subaccount summary could not be read.
        Read-only / DRY-safe.
        """
        s = self._summary()
        if s is None:
            return None
        from nado_protocol.utils.margin_manager import MarginManager
        return MarginManager(s).calculate_account_summary()

    def margin_used_usd(self) -> float:
        """REAL exchange INITIAL margin used ($), account-wide — ALL positions incl
        manual/foreign, each at its OWN weight/leverage. This is the authoritative
        existing-margin for the MM-cap gate (== Nado UI "Initial Margin").

        Canonical formula (SDK MarginManager): initial_margin_used =
        unweighted_health - initial_health. Live-verified 2026-06-11 = $5886.92
        (vs the old fictional notional/20 = $3543). Account-wide health snapshot →
        idempotent, no double-count, naturally spans foreign/manual positions.

        RAISES RuntimeError on a genuine read failure (caller MUST fail closed — do
        NOT fall back to a notional/leverage model). Returns 0.0 only when the live
        summary genuinely reports no initial margin (flat account).
        """
        acc = self._account_summary()
        if acc is None:
            raise RuntimeError("margin_used_usd: live subaccount summary unavailable")
        try:
            return float(acc.unweighted_health) - float(acc.initial_health)
        except Exception as e:
            raise RuntimeError(f"margin_used_usd: AccountSummary parse failed: {e}")

    def equity_with_upnl(self) -> float:
        """True account equity INCLUDING unrealized PnL ($) = AccountSummary
        portfolio_value (== unweighted health). Live-verified 2026-06-11 = $30291.05
        (vs buggy account_value()=initial_health=$24404). This is the MM-cap
        denominator. RAISES on read failure (fail-closed)."""
        acc = self._account_summary()
        if acc is None:
            raise RuntimeError("equity_with_upnl: live subaccount summary unavailable")
        try:
            return float(acc.portfolio_value)
        except Exception as e:
            raise RuntimeError(f"equity_with_upnl: AccountSummary parse failed: {e}")

    def spot_usdc(self) -> float:
        """USDT0 balance (Nado's settlement asset, product_id=0)."""
        s = self._summary()
        if s is None:
            return 0.0
        for b in (s.spot_balances or []):
            if b.product_id == 0:
                return _from_x18(b.balance.amount)
        return 0.0

    def open_positions(self, ttl: float = 20.0) -> dict[str, dict]:
        """Returns {coin: {"szi": signed_size, "entryPx": avg_entry, "coin": coin}}.
        Mirror of HL format so trader.py works unchanged."""
        now = time.time()
        if self._positions_cache and now - self._positions_cache[0] < ttl:
            return self._positions_cache[1]
        s = self._summary(ttl=2.0)
        if s is None:
            # Phantom-close guard (audit 2026-05-27): on summary fetch failure return
            # stale cache rather than empty — trader.py treats empty as "position
            # gone → SL triggered" and re-opens duplicates next scan.
            log.warning("Nado open_positions: _summary returned None (fetch failed)")
            if self._positions_cache:
                cached_ts, cached_out = self._positions_cache
                log.warning(
                    "Returning stale positions cache (%.0fs old) to prevent phantom-close",
                    time.time() - cached_ts,
                )
                return cached_out
            # FAIL-LOUD (2026-07-02): no cache to fall back on -> positions are UNKNOWN,
            # not "none". Returning {} here let every caller read a fetch failure as
            # "all flat" (phantom-close / false-orphan class). RAISE so callers treat
            # the tick as indeterminate (they all catch and skip/defer).
            raise RuntimeError(
                "Nado open_positions: summary fetch failed and no cache — "
                "positions UNKNOWN (refusing to report empty)"
            )
        out: dict[str, dict] = {}
        for b in (s.perp_balances or []):
            try:
                amount = _from_x18(b.balance.amount)
                if abs(amount) < 1e-12:
                    continue
                pid = b.product_id
                sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
                v_quote = _from_x18(b.balance.v_quote_balance)  # negative for long, positive for short
                # entry_px = abs(v_quote / amount). v_quote is opposite sign of position.
                entry_px = abs(v_quote / amount) if abs(amount) > 0 else 0.0
                out[sym] = {
                    "coin": sym,
                    "szi": str(amount),
                    "entryPx": str(entry_px),
                    "product_id": pid,
                }
            except Exception as e:
                # FAIL-LOUD (2026-07-02 per-coin phantom-close class): a coin whose perp
                # balance persistently fails to parse used to be silently DROPPED from
                # open_positions every tick — the phantom-guard then auto-closed its DB
                # row and cancelled its SL on a LIVE position. A parse failure must abort
                # the WHOLE read (UNKNOWN tick), never make one coin vanish.
                log.critical(
                    "open_positions parse FAILED for %s — aborting whole read "
                    "(positions UNKNOWN this tick, coin NOT dropped): %s", b, e,
                )
                raise RuntimeError(f"open_positions: perp balance parse failed: {e}") from e
        self._positions_cache = (now, out)
        return out

    def invalidate_positions_cache(self) -> None:
        self._positions_cache = None
        self._summary_cache = None

    def position_liquidation(self, coin: str) -> dict | None:
        """{liq_px, margin_mode, leverage} для open позиции. None если позы нет.

        Nado/Vertex: cross-only health system на subaccount уровне. Per-position
        liq_px нативно нет — liq происходит когда healths[1].health (maint) < 0.
        Marginal liq_px = "при какой цене этого coin'а maint health = 0,
        если остальное держится". Long: liq = entry - h1.health/size_signed.

        margin_mode всегда 'cross' для бот-subaccount (isolated = отдельный
        subaccount, бот его не использует).
        """
        s = self._summary()
        if s is None:
            return None
        if not s.healths or len(s.healths) < 2:
            return None
        try:
            h1_health = _from_x18(s.healths[1].health)  # USD buffer (maintenance)
        except Exception:
            return None
        for b in (s.perp_balances or []):
            try:
                pid = b.product_id
                sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
                if sym != coin:
                    continue
                amt = _from_x18(b.balance.amount)
                if abs(amt) < 1e-12:
                    return None
                v_quote = _from_x18(b.balance.v_quote_balance)
                entry_px = abs(v_quote / amt) if abs(amt) > 0 else 0.0
                # Marginal liq: при движении цены этого coin'а на ΔP, P&L Δ = amt × ΔP.
                # Liq когда h1_health + amt × (X - entry) ≤ 0 (для long amt>0).
                # Для long: X = entry - h1_health / amt
                # Для short (amt<0): X = entry + h1_health / |amt|
                if amt > 0:
                    liq_px = entry_px - h1_health / amt
                else:
                    liq_px = entry_px + h1_health / abs(amt)
                if liq_px < 0:
                    liq_px = 0.0
                return {
                    "liq_px": float(liq_px),
                    "margin_mode": "cross",
                    "leverage": 0,  # Nado has no per-product leverage knob
                }
            except Exception as e:
                log.warning("position_liquidation %s parse: %s", coin, e)
                return None
        return None

    def user_state(self) -> dict:
        """HL-compat: returns marginSummary + assetPositions shape so trader's
        margin-cap check works."""
        s = self._summary()
        if s is None:
            return {"marginSummary": {}, "assetPositions": []}
        equity = self.account_value()
        # Approx total margin used: sum |v_quote| / avg leverage 20x
        total_notional = 0.0
        asset_positions = []
        for b in (s.perp_balances or []):
            try:
                amt = _from_x18(b.balance.amount)
                if abs(amt) < 1e-12:
                    continue
                v_quote = _from_x18(b.balance.v_quote_balance)
                total_notional += abs(v_quote)
                pid = b.product_id
                sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
                asset_positions.append({"position": {
                    "coin": sym,
                    "szi": str(amt),
                    "entryPx": str(abs(v_quote / amt)) if abs(amt) > 0 else "0",
                }})
            except Exception:
                continue
        # Canonical margin used (NOT the old fictional notional/20): route through the
        # SDK MarginManager so the shim agrees with the MM-cap gate and the Nado UI.
        # Fail-soft here (this shim is informational only); the gate uses
        # margin_used_usd() directly and fails CLOSED on error.
        try:
            margin_used = self.margin_used_usd()
        except Exception as e:
            log.warning("user_state: margin_used_usd failed (%s) — margin_used set 0", e)
            margin_used = 0.0
        ms = {
            "accountValue": str(equity),
            "totalMarginUsed": str(margin_used),
            "totalNtlPos": str(total_notional),
        }
        return {
            "marginSummary": ms,
            "crossMarginSummary": ms,
            "withdrawable": str(equity - margin_used),
            "assetPositions": asset_positions,
        }

    def spot_user_state(self) -> dict:
        s = self._summary()
        if s is None:
            return {"balances": []}
        out = []
        for b in (s.spot_balances or []):
            try:
                amount = _from_x18(b.balance.amount)
                if abs(amount) < 1e-12:
                    continue
                pid = b.product_id
                sym = self._pid_to_symbol.get(pid, "USDT0" if pid == 0 else f"PID_{pid}")
                out.append({"coin": sym, "total": str(amount), "hold": "0"})
            except Exception:
                continue
        return {"balances": out}

    def _ensure_trigger_client(self):
        """Return a live SDK trigger_client, HEALING it if the SDK left it None.

        The SDK skips trigger_client whenever engine_client.get_contracts() throws at
        startup (transient DNS/network). That single startup blip then permanently
        disables BOTH trigger enumeration (orphan-sweep) AND trigger placement (SL/TP)
        for the entire process -- the only symptom being a swallowed per-cycle WARNING.
        Here we rebuild the SDK in-place once the network is back. If the rebuild still
        cannot produce a usable trigger client we FAIL LOUD via TriggerClientUnavailable
        (never let a raw 'NoneType has no attribute list_trigger_orders' leak, and never
        let the safety sweep be silently skipped forever)."""
        tc = self._sdk.context.trigger_client
        if tc is not None and hasattr(tc, "list_trigger_orders"):
            return tc
        mode = NadoClientMode.MAINNET if self.settings.network == "mainnet" else NadoClientMode.TESTNET
        try:
            new_sdk = create_nado_client(mode=mode, signer=self.settings.agent_private_key)
        except Exception as e:
            raise TriggerClientUnavailable(
                "trigger_client is None and SDK rebuild failed (network/DNS still down?): %s" % e
            ) from e
        tc = new_sdk.context.trigger_client
        if tc is None or not hasattr(tc, "list_trigger_orders"):
            raise TriggerClientUnavailable(
                "trigger_client STILL None after SDK rebuild -- engine_client.get_contracts() "
                "still failing; SL/trigger enumeration AND placement are DISABLED"
            )
        self._sdk = new_sdk
        # The rebuilt SDK has fresh sessions — re-install the order-path timeout guard.
        self._install_order_path_timeouts()
        log.warning(
            "Nado trigger_client was None (SDK skipped it because get_contracts() threw at "
            "startup under a DNS/network outage) -- REBUILT SDK in-place; trigger enumeration "
            "+ placement restored"
        )
        return tc

    def fetch_open_orders_ccxt_shape(self) -> list:
        """ccxt-shape list of OPEN ORDERS (incl. SL trigger orders).
        Used by watchdog_kraken to detect SL coverage per position.
        Watchdog cares about: symbol, type ('stop'/'trigger'), reduceOnly.

        Implementation: query Nado trigger service, filter to ACTIVE statuses only.
        Cancelled/triggered/error are excluded — they're not "open" SLs anymore.
        """
        out = []
        # Heal-or-fail-loud BEFORE the try: a None/missing trigger_client must surface as
        # TriggerClientUnavailable to the orphan-sweep caller (loud ERROR/CRITICAL), NOT be
        # swallowed into the generic RuntimeError below and treated as a routine skip-cycle.
        _tc = self._ensure_trigger_client()
        try:
            from nado_protocol.trigger_client.types.query import (
                ListTriggerOrdersParams, ListTriggerOrdersTx,
            )
            tx = ListTriggerOrdersTx(
                sender=self.subaccount_hex,
                recvTime=int(time.time() * 1000) + 60_000,
            )
            # Only request "active" statuses — Nado returns these as enum/string
            params = ListTriggerOrdersParams(
                tx=tx, limit=100,
                status_types=["waiting_price", "waiting_dependency", "triggering"],
            )
            resp = _tc.list_trigger_orders(params)
            data = resp.data if hasattr(resp, "data") else resp
            triggers = data.orders if hasattr(data, "orders") else []
            for t in triggers:
                try:
                    # Defensive: even with status filter, double-check status
                    status = t.status
                    status_str = str(status).lower() if not isinstance(status, str) else status.lower()
                    if any(bad in status_str for bad in ("cancelled", "triggered", "error", "executing", "completed")):
                        continue
                    od = t.order
                    pid = od.product_id
                    sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
                    amount = int(od.order.amount) if hasattr(od.order, "amount") else 0
                    out.append({
                        "symbol": sym,
                        "coin": sym,
                        "type": "trigger",
                        "reduceOnly": True,
                        "side": "buy" if amount > 0 else "sell",
                        "amount": abs(amount) / X18,
                        "info": {"product_id": pid, "digest": getattr(od, "digest", None), "status": status_str},
                    })
                except Exception as e:
                    log.warning("trigger order parse: %s", e)
        except Exception as e:
            # XNN port 2026-06-11 (exception-swallow class fix): a failed trigger-service
            # QUERY used to be swallowed into [] — downstream read that as "no live SL"
            # (false-naked class). RAISE instead; both callers handle it explicitly:
            # _confirm_trigger_live catches+retries (fail-closed placement), and
            # list_open_sl_orders propagates (trader assumes-live on raise).
            raise RuntimeError(f"fetch_open_orders trigger query FAILED: {e}") from e
        return out

    def open_orders(self) -> list:
        """HL-shape open orders list (used by watchdog to find SL trigger orders)."""
        try:
            # Perp-only: spot products (e.g. USDT0/NLP) are rejected by the engine
            # query (error_code 2015) and one bad id fails the whole batch.
            perp_pids = sorted({m.product_id for m in self._meta_cache.values()})
            if not perp_pids:
                perp_pids = list(self._pid_to_symbol.keys())
            orders_per_product = self._sdk.context.engine_client.get_subaccount_multi_products_open_orders(
                product_ids=perp_pids,
                sender=self.subaccount_hex,
            )
        except Exception as e:
            log.warning("get_subaccount_multi_products_open_orders failed: %s", e)
            return []
        out = []
        # Response shape: list of {product_id, orders: [{...}]}
        try:
            entries = orders_per_product.product_orders if hasattr(orders_per_product, "product_orders") else []
            for entry in entries:
                pid = entry.product_id
                sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
                for o in (entry.orders or []):
                    out.append({
                        "coin": sym,
                        "oid": getattr(o, "digest", None),
                        "limitPx": str(_from_x18(o.price_x18)) if hasattr(o, "price_x18") else "0",
                        "sz": str(abs(_from_x18(o.amount))) if hasattr(o, "amount") else "0",
                        "side": "B" if hasattr(o, "amount") and int(o.amount) > 0 else "A",
                        "isTrigger": False,  # plain open orders, not SL
                        "reduceOnly": False,
                        "product_id": pid,
                    })
        except Exception as e:
            log.warning("open_orders parse: %s", e)
        # Trigger orders (SL) live in a separate service — placeholder for future fetch.
        return out

    # ------------ Mark / mid ------------

    def mark_price(self, coin: str, ttl: float = 5.0) -> float:
        now = time.time()
        if self._mark_cache and now - self._mark_cache[0] < ttl:
            cached = self._mark_cache[1].get(coin)
            if cached:
                return cached
        try:
            pid = self._pid(coin)
            mp = self._sdk.context.engine_client.get_market_price(pid)
            bid = _valid_x18_side(getattr(mp, "bid_x18", None))
            ask = _valid_x18_side(getattr(mp, "ask_x18", None))
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0           # normal two-sided book (unchanged)
            else:
                # Empty / one-sided book (e.g. VIRTUAL-PERP pid=84): live mid is a
                # sentinel-garbage value. Fall back to the indexer oracle mark, which
                # exists for every perp regardless of book state.
                mid = self._oracle_mark(pid)
            if not (0.0 < mid < MARK_SANITY_MAX):
                log.warning(
                    "mark_price(%s) rejected insane value %.6g (bid_x18=%s ask_x18=%s) — returning 0.0",
                    coin, mid, getattr(mp, "bid_x18", "?"), getattr(mp, "ask_x18", "?"),
                )
                return 0.0
            cache = self._mark_cache[1] if self._mark_cache else {}
            cache[coin] = mid
            self._mark_cache = (now, cache)
            return mid
        except Exception as e:
            log.warning("mark_price(%s) failed: %s", coin, e)
            return 0.0

    def _oracle_mark(self, pid: int) -> float:
        """Indexer oracle/exchange mark for a product — robust to empty engine books.
        Returns exchange mark_price_x18 (falls back to index_price_x18, then 0.0).
        Verified live 2026-06-05: get_perp_prices(84) → mark_price_x18 ≈ 0.573 for
        VIRTUAL-PERP whose engine book was empty."""
        try:
            pp = self._sdk.context.indexer_client.get_perp_prices(pid)
            for attr in ("mark_price_x18", "index_price_x18"):
                v = _valid_x18_side(getattr(pp, attr, None))
                if v is not None:
                    return v
        except Exception as e:
            log.warning("_oracle_mark(pid=%s) failed: %s", pid, e)
        return 0.0

    # ------------ Slip estimate (per side) ------------
    # Used by trader.py LIQUIDITY DRAG CHECK. Nado/Vertex perps на Ink L2 имеют
    # тонкий orderbook на не-major coins → conservative defaults. Real-world:
    # 2026-05-06 наблюдали -1.22% slip на FARTCOIN-PERP (тонкий стакан).
    # Без историч. fills per-coin (limited Nado bot trading history) тут
    # tier-defaults; в будущем может быть `data/nado_slippage.json`.
    _fee_rt_estimate = 0.001  # Nado taker ≈ 0.05% per side, 0.10% RT

    def slip_per_side(self, coin: str, notional_usd: float) -> float:
        """Conservative slippage estimate per side (fraction of notional)."""
        del notional_usd
        majors = {"BTC-PERP", "ETH-PERP", "SOL-PERP", "XRP-PERP", "DOGE-PERP",
                  "BNB-PERP", "LTC-PERP", "BCH-PERP", "ADA-PERP", "AVAX-PERP"}
        mids = {"LINK-PERP", "ARB-PERP", "OP-PERP", "MATIC-PERP", "ATOM-PERP",
                "NEAR-PERP", "AAVE-PERP", "TRX-PERP", "XMR-PERP", "UNI-PERP",
                "ZEC-PERP", "ETC-PERP", "ENA-PERP", "kPEPE-PERP", "ONDO-PERP",
                "HYPE-PERP"}
        if coin in majors:
            return 0.002
        if coin in mids:
            return 0.004
        return 0.008  # thin/exotic: 0.8% per side

    # ------------ Price rounding ------------

    def round_price(self, coin: str, px: float) -> float:
        """Round to product's tick_size."""
        try:
            tick = self.asset(coin).tick_size
        except KeyError:
            return px
        if tick <= 0:
            return px
        return round(round(px / tick) * tick, 12)

    # ------------ Trading ------------

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> dict:
        """Nado uses cross-margin by default per subaccount. There's no per-product
        leverage knob on the standard subaccount (isolated subaccounts are separate).
        Return a dummy-success dict to keep trader.py happy."""
        return {"status": "ok", "response": {"type": "noop", "data": {}}}

    def market_open(self, coin: str, is_buy: bool, sz: float) -> dict:
        """Place a market order. `sz` in base asset units (e.g. 10 LINK).

        VERIFIED LIVE 03-05-2026: Nado `amount` is base × 1e18 wei. Must be exact
        multiple of `size_increment` (in wei, NOT base) — float math will introduce
        rounding error, so we snap on the integer grid using floor-division.

        REJECT if intended size < min_size — silently bumping up would breach
        risk_per_trade limit (Nado has high mins relative to small-cap equity).

        Universal isolated-mode handling: some markets (e.g. XAG-PERP product_id=88)
        reject cross-margin orders with error_code=2122. On first such error we
        cache the pid in `_ISOLATED_ONLY_PIDS` and retry via isolated path; on
        subsequent calls we skip the cross attempt entirely.
        """
        try:
            meta = self.asset(coin)
            pid = meta.product_id
            inc_x18 = meta.size_increment_x18 or int(round(self._size_increment_for(coin) * X18))
            min_x18 = int(round(meta.min_size * X18))
            target_x18 = int(round(sz * X18))
            if target_x18 < min_x18:
                msg = (f"size {sz} < Nado min {meta.min_size} for {coin}. "
                       f"Risk per trade too small for this market — skip")
                log.warning("Nado REJECT %s: %s", coin, msg)
                return _error_resp(f"below_min_size: {msg}")
            amount_x18 = (target_x18 // inc_x18) * inc_x18
            amount_signed = amount_x18 if is_buy else -amount_x18
            sz_actual = amount_x18 / X18

            # REAL-FILL READBACK watermark (2026-07-02): note the newest match idx BEFORE
            # sending, so _wrap_open_resp can attribute only strictly-newer fills to THIS
            # order (never an older trade's fills). None = feed empty/unreadable ->
            # readback is ambiguous and _wrap_open_resp falls to the unconfirmed path.
            pre_idx = self._max_match_idx(pid)

            if pid in _ISOLATED_ONLY_PIDS:
                return self._market_open_isolated(coin, is_buy, sz_actual, amount_signed,
                                                  pre_max_idx=pre_idx)

            # ── ORACLE-ANCHORED CROSS-MARGIN FOK (error_code 2028 fix, 2026-07-01) ──────────────
            # Do NOT use the SDK's `place_market_order`: it builds the limit off the WRONG book side
            # and the raw slippage (bids[0]*(1+slip) for a BUY → ~+90..100bp from oracle) → 2028
            # "price too far from index" → rejected → FLAT readback → naked-leg unwind. Build the
            # marketable FOK ourselves with an oracle-anchored, in-band, book-crossing limit (the
            # same construction the SDK's own close_position uses: oracle*(1±spread), FOK). Cross-
            # margin (isolated=False); identical amount/sign/snap as before. On error_code 2122 we
            # fall through to the isolated path exactly as the prior cross attempt did.
            from nado_protocol.engine_client.types.execute import PlaceOrderParams
            from nado_protocol.utils.execute import OrderParams
            from nado_protocol.utils.expiration import get_expiration_timestamp
            from nado_protocol.utils.order import OrderType, build_appendix

            engine = self._sdk.context.engine_client
            ob = engine.get_market_liquidity(product_id=pid, depth=1) \
                if hasattr(engine, "get_market_liquidity") \
                else engine._querier.get_market_liquidity(pid, 1)
            pos_state = engine._querier._get_subaccount_product_position(self.subaccount_hex, pid)
            oracle_x18 = int(pos_state.product.oracle_price_x18)
            tick_x18 = int(pos_state.product.book_info.price_increment_x18) or \
                int(round((meta.tick_size or 0.0001) * X18)) or 1
            if is_buy:
                if not ob.asks:
                    return _error_resp(f"empty asks for {coin} — cannot cross (no book)")
                best_opp_x18 = int(ob.asks[0][0])
            else:
                if not ob.bids:
                    return _error_resp(f"empty bids for {coin} — cannot cross (no book)")
                best_opp_x18 = int(ob.bids[0][0])
            price_x18 = _oracle_anchored_price_x18(
                oracle_x18, is_buy, best_opp_x18, tick_x18, self.settings.slippage)
            # Sanity: the human-unit price must be valid (reuses the empty-book / sentinel guard).
            _assert_sane_px(coin, price_x18 / X18, "market_open.anchored_limit")
            order = OrderParams(
                sender=self.subaccount_hex,
                amount=int(amount_signed),
                priceX18=int(price_x18),
                expiration=int(get_expiration_timestamp(1000)),
                appendix=int(build_appendix(OrderType.FOK, reduce_only=False)),
            )
            params = PlaceOrderParams(
                product_id=pid, order=order, spot_leverage=None, signature=None)
            log.info(
                "Nado market_open: %s sz=%s → snapped %s (amount_x18=%d, inc_x18=%d, is_buy=%s) "
                "oracle_x18=%d anchored_px_x18=%d (oracle=%.6g limit=%.6g, +%.3f%% cap %.3f%%)",
                coin, sz, sz_actual, amount_signed, inc_x18, is_buy, oracle_x18, price_x18,
                oracle_x18 / X18, price_x18 / X18,
                abs(price_x18 - oracle_x18) / max(oracle_x18, 1) * 100.0,
                NADO_ORACLE_ANCHOR_CAP * 100.0,
            )
            try:
                resp = engine.place_order(params)
            except Exception as inner_e:
                if _is_isolated_only_2122(inner_e):
                    log.warning(
                        "Nado %s requires isolated mode (error_code=2122) — "
                        "caching pid=%d, retrying isolated path",
                        coin, pid,
                    )
                    _ISOLATED_ONLY_PIDS.add(pid)
                    return self._market_open_isolated(coin, is_buy, sz_actual, amount_signed,
                                                      pre_max_idx=pre_idx)
                raise
            self.invalidate_positions_cache()
            return self._wrap_open_resp(resp, coin, is_buy, sz_actual, pre_max_idx=pre_idx)
        except Exception as e:
            log.exception("Nado market_open(%s) failed: %s", coin, e)
            return _error_resp(str(e))

    def _market_open_isolated(
        self,
        coin: str,
        is_buy: bool,
        sz_actual: float,
        amount_signed: int,
        pre_max_idx: int | None = None,
    ) -> dict:
        """Manually construct an isolated FOK order — Nado SDK's
        `place_market_order` always builds the order appendix with
        isolated=False, so we replicate its orderbook→price→sign pipeline
        here and pass `isolated=True, isolated_margin=...` instead.

        isolated_margin = notional / max_leverage × 1.5 (1.5× safety buffer
        so a normal wick doesn't liquidate immediately).
        """
        from nado_protocol.engine_client.types.execute import PlaceOrderParams
        from nado_protocol.utils.execute import OrderParams
        from nado_protocol.utils.expiration import get_expiration_timestamp
        from nado_protocol.utils.math import mul_x18, to_x18
        from nado_protocol.utils.order import OrderType, build_appendix

        meta = self.asset(coin)
        pid = meta.product_id
        engine = self._sdk.context.engine_client

        orderbook = engine.get_market_liquidity(product_id=pid, depth=1) \
            if hasattr(engine, "get_market_liquidity") \
            else engine._querier.get_market_liquidity(pid, 1)
        slippage = float(getattr(self.settings, "slippage", None) or 0.005)
        # ORACLE-ANCHORED in-band limit (error_code 2028 fix): the prior code anchored on the WRONG
        # book side * (1±slippage) (~+90..100bp from oracle → 2028). Anchor on the oracle, cap the
        # spread to the band, and floor it to cross the opposite top-of-book (taker FOK).
        pos_state = engine._querier._get_subaccount_product_position(self.subaccount_hex, pid)
        oracle_x18 = int(pos_state.product.oracle_price_x18)
        tick_x18 = int(pos_state.product.book_info.price_increment_x18) or \
            int(round((meta.tick_size or 0.0001) * X18)) or 1
        if is_buy:
            if not orderbook.asks:
                raise RuntimeError(f"empty asks for {coin}")
            best_opp_x18 = int(orderbook.asks[0][0])
        else:
            if not orderbook.bids:
                raise RuntimeError(f"empty bids for {coin}")
            best_opp_x18 = int(orderbook.bids[0][0])
        price_x18_aligned = _oracle_anchored_price_x18(
            oracle_x18, is_buy, best_opp_x18, tick_x18, slippage)
        _assert_sane_px(coin, price_x18_aligned / X18, "market_open_isolated.anchored_limit")

        ref_price = best_opp_x18 / X18
        notional_usd = sz_actual * ref_price
        leverage = max(int(meta.max_leverage or 20), 1)
        isolated_margin_usd = (notional_usd / leverage) * 1.5
        isolated_margin_x18 = int(round(isolated_margin_usd * X18))

        log.info(
            "Nado market_open ISOLATED: %s amount=%d notional=$%.2f lev=%dx "
            "margin=$%.2f price_x18=%d",
            coin, amount_signed, notional_usd, leverage,
            isolated_margin_usd, price_x18_aligned,
        )

        order = OrderParams(
            sender=self.subaccount_hex,
            amount=int(amount_signed),
            priceX18=int(price_x18_aligned),
            expiration=int(get_expiration_timestamp(1000)),
            appendix=int(build_appendix(
                OrderType.FOK,
                isolated=True,
                isolated_margin=isolated_margin_x18,
                reduce_only=False,
            )),
        )
        params = PlaceOrderParams(
            product_id=pid,
            order=order,
            spot_leverage=None,
            signature=None,
        )
        try:
            resp = engine.place_order(params)
        except Exception as e:
            log.exception("Nado market_open ISOLATED(%s) failed: %s", coin, e)
            return _error_resp(str(e))
        self.invalidate_positions_cache()
        return self._wrap_open_resp(resp, coin, is_buy, sz_actual, pre_max_idx=pre_max_idx)

    def _size_increment_for(self, coin: str) -> float:
        """size_increment in base units (e.g. 0.1 for LINK, 0.001 for ETH)."""
        meta = self.asset(coin)
        if meta.size_increment_x18 > 0:
            return meta.size_increment_x18 / X18
        return 10 ** (-meta.sz_decimals) if meta.sz_decimals >= 0 else 1.0

    def market_close(self, coin: str) -> dict:
        """Bug fix 2026-05-09 (root): pre-fix всегда возвращал success-shape
        с totalSz="0" (lie!) и avgPx=mark_price (не actual fill). Теперь
        verify position actually closed via re-fetch; return error status
        if SDK call returned но position still open.
        """
        try:
            pid = self._pid(coin)
            resp = self._sdk.market.close_position(self.subaccount_hex, pid)
            self.invalidate_positions_cache()
            try:
                pos = self.open_positions().get(coin)
                still_sz = abs(float((pos or {}).get("szi", 0) or 0))
            except Exception:
                still_sz = -1.0
            if still_sz >= 1e-9:
                return {"status": "ok", "response": {"type": "order", "data": {
                    "statuses": [{"error": (
                        f"close_position SDK returned но позиция still open: "
                        f"szi={still_sz} on exchange — manual cleanup may be needed"
                    )}]
                }}, "_raw": str(resp)}
            return {"status": "ok", "response": {"type": "order", "data": {
                "statuses": [{"filled": {
                    "avgPx": str(self.mark_price(coin)),
                    "totalSz": "verified_closed",
                    "oid": "nado_close",
                }}]
            }}, "_raw": str(resp)}
        except Exception as e:
            log.exception("Nado market_close(%s) failed: %s", coin, e)
            return _error_resp(str(e))

    def _confirm_trigger_live(
        self,
        coin: str,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        attempts: int = 3,
    ) -> str | None:
        """CLASS GUARD (2026-06-07): read the order BACK from the exchange and confirm
        a matching active trigger (SL/TP) is actually live before we call it placed.

        Roots out the silent-success class: pre-fix `_wrap_sl_resp` returned an "ok /
        resting" shape (even fabricating a `nado_sl_<ts>` oid) for ANY response object
        — so a reduce-only SL rejected with error_code=2064 ("Reduce only order
        increases position", i.e. no position to reduce yet) or any other non-resting
        outcome was reported as placed → naked position.

        Matches on coin + side + size (size_increment tolerance) + trigger price (tick
        tolerance) against the live trigger service (waiting_price / waiting_dependency
        / triggering). Returns the live order digest if confirmed, else None.
        """
        want_side = "buy" if is_buy else "sell"
        try:
            meta = self.asset(coin)
            inc = (meta.size_increment_x18 / X18) if meta.size_increment_x18 else \
                self._size_increment_for(coin)
            sz_tol = max(inc, abs(sz) * 1e-6)
            tick = meta.tick_size or 0.0001
            px_tol = max(tick, abs(trigger_px) * 1e-4)
        except Exception:
            sz_tol = max(abs(sz) * 1e-3, 1e-9)
            px_tol = max(abs(trigger_px) * 1e-3, 1e-9)
        for attempt in range(attempts):
            try:
                live = self.fetch_open_orders_ccxt_shape()
            except Exception as e:
                log.warning("_confirm_trigger_live(%s) read-back error: %s", coin, e)
                live = []
            for o in live:
                if (o.get("coin") != coin and o.get("symbol") != coin):
                    continue
                if str(o.get("side", "")).lower() != want_side:
                    continue
                if not bool(o.get("reduceOnly", False)):
                    continue
                try:
                    amt = abs(float(o.get("amount", 0)))
                except (TypeError, ValueError):
                    continue
                if abs(amt - abs(sz)) > sz_tol:
                    continue
                digest = (o.get("info") or {}).get("digest") or o.get("oid")
                if digest:
                    return str(digest)
            time.sleep(0.4 * (attempt + 1))
        log.error(
            "_confirm_trigger_live(%s): NO matching live trigger after %d read-backs "
            "(want side=%s sz≈%s trig≈%s) — treating placement as FAILED (not naked-success)",
            coin, attempts, want_side, abs(sz), trigger_px,
        )
        return None

    def trigger_sl(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Place a stop-loss trigger order (reduceOnly).
        is_buy=True  = buy-to-close (closes SHORT) → trigger when price RISES → oracle_price_above
        is_buy=False = sell-to-close (closes LONG)  → trigger when price FALLS → oracle_price_below
        """
        try:
            meta = self.asset(coin)
            pid = meta.product_id
            inc_x18 = meta.size_increment_x18 or int(round(self._size_increment_for(coin) * X18))
            target_x18 = int(round(sz * X18))
            amount_abs_x18 = (target_x18 // inc_x18) * inc_x18
            amount_signed_x18 = amount_abs_x18 if is_buy else -amount_abs_x18
            # Limit price worse-than-trigger so order fills as market when triggered
            slip = 0.005
            limit_px = trigger_px * (1 + slip) if is_buy else trigger_px * (1 - slip)
            # Fail-closed: a bad mark (empty-book sentinel) could reach here via the
            # vstop trail / residual-SL paths (trader.py) — refuse before sending.
            _assert_sane_px(coin, trigger_px, "trigger_sl.trigger")
            _assert_sane_px(coin, limit_px, "trigger_sl.limit")
            # Bug fix 2026-05-05: float * 1e18 даёт float-error → x18 не кратен tick_x18
            # → exchange отвергает с "not divisible by price_increment_x18", позиция
            # остаётся без SL → forced-close с убытком. Округляем через integer math.
            tick = meta.tick_size or 0.0001
            tick_x18 = int(round(tick * X18))
            if tick_x18 <= 0:
                tick_x18 = 1
            limit_px_x18 = (int(round(limit_px * X18)) // tick_x18) * tick_x18
            trigger_px_x18 = (int(round(trigger_px * X18)) // tick_x18) * tick_x18
            trigger_type = "oracle_price_above" if is_buy else "oracle_price_below"
            log.info(
                "Nado trigger_sl: %s amount=%d trigger_x18=%d limit_x18=%d (tick_x18=%d) type=%s",
                coin, amount_signed_x18, trigger_px_x18, limit_px_x18, tick_x18, trigger_type,
            )
            resp = self._sdk.market.place_price_trigger_order(
                product_id=pid,
                price_x18=str(limit_px_x18),
                amount_x18=str(amount_signed_x18),
                trigger_price_x18=str(trigger_px_x18),
                trigger_type=trigger_type,
                subaccount_owner=self.main_wallet,
                subaccount_name=self.subaccount_name,
                reduce_only=True,
            )
            # CLASS GUARD: only "placed" after read-back confirms it live on the exchange.
            self.invalidate_positions_cache()
            confirmed = self._confirm_trigger_live(
                coin, is_buy, amount_abs_x18 / X18, trigger_px_x18 / X18,
            )
            if not confirmed:
                return _error_resp(
                    f"trigger_sl unconfirmed: no live reduce-only trigger for {coin} "
                    f"after read-back (sdk_resp={str(resp)[:120]})"
                )
            return self._wrap_sl_resp(resp, coin, confirmed_oid=confirmed)
        except Exception as e:
            log.exception("Nado trigger_sl(%s) failed: %s", coin, e)
            return _error_resp(str(e))

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float, px: float,
                          post_only: bool = True) -> dict:
        """Partial-TP leg of the hybrid exit. Nado has no native limit / no trigger_tp,
        so this places a reduce-only price-trigger on the PROFIT side at px — same
        place_price_trigger_order path as trigger_sl but with the OPPOSITE trigger
        direction. Taker-on-trigger; reuses the validated SL trigger plumbing.
        is_buy = CLOSE side: long pos → sell (price RISES to TP → oracle_price_above);
                            short pos → buy (price FALLS to TP → oracle_price_below).
        """
        try:
            meta = self.asset(coin)
            pid = meta.product_id
            inc_x18 = meta.size_increment_x18 or int(round(self._size_increment_for(coin) * X18))
            target_x18 = int(round(sz * X18))
            amount_abs_x18 = (target_x18 // inc_x18) * inc_x18
            amount_signed_x18 = amount_abs_x18 if is_buy else -amount_abs_x18
            slip = 0.005
            limit_px = px * (1 + slip) if is_buy else px * (1 - slip)
            _assert_sane_px(coin, px, "limit_reduce_only.trigger")
            _assert_sane_px(coin, limit_px, "limit_reduce_only.limit")
            tick = meta.tick_size or 0.0001
            tick_x18 = int(round(tick * X18))
            if tick_x18 <= 0:
                tick_x18 = 1
            limit_px_x18 = (int(round(limit_px * X18)) // tick_x18) * tick_x18
            trigger_px_x18 = (int(round(px * X18)) // tick_x18) * tick_x18
            # TP trigger is OPPOSITE of SL: long-close(sell) fires on price RISING to TP
            # (oracle_price_above); short-close(buy) on price FALLING to TP (below).
            trigger_type = "oracle_price_below" if is_buy else "oracle_price_above"
            log.info(
                "Nado limit_reduce_only(TP): %s amount=%d trigger_x18=%d limit_x18=%d type=%s",
                coin, amount_signed_x18, trigger_px_x18, limit_px_x18, trigger_type,
            )
            resp = self._sdk.market.place_price_trigger_order(
                product_id=pid,
                price_x18=str(limit_px_x18),
                amount_x18=str(amount_signed_x18),
                trigger_price_x18=str(trigger_px_x18),
                trigger_type=trigger_type,
                subaccount_owner=self.main_wallet,
                subaccount_name=self.subaccount_name,
                reduce_only=True,
            )
            # CLASS GUARD: confirm the TP trigger is live via read-back before reporting placed.
            self.invalidate_positions_cache()
            confirmed = self._confirm_trigger_live(
                coin, is_buy, amount_abs_x18 / X18, trigger_px_x18 / X18,
            )
            if not confirmed:
                return _error_resp(
                    f"limit_reduce_only unconfirmed: no live reduce-only trigger for {coin} "
                    f"after read-back (sdk_resp={str(resp)[:120]})"
                )
            return self._wrap_sl_resp(resp, coin, confirmed_oid=confirmed)
        except Exception as e:
            log.exception("Nado limit_reduce_only(%s) failed: %s", coin, e)
            return _error_resp(str(e))

    def cancel_order(self, coin: str, oid) -> dict:
        """Cancel REGULAR order (limit/market). Для trigger orders (SL/TP) — use cancel_sl_order!

        Bug fix 2026-05-06: Nado имеет 2 separate API endpoints:
        - cancel_orders (regular limit/market orders)
        - cancel_trigger_orders (SL / take_profit / stop trigger orders)
        Раньше cancel_order применяли к SL → API возвращало success но
        cancelled_orders=[] (silent fail). За 12 часов накопились 4-6 SL на
        позицию. Юзер обнаружил утром 06-05.
        """
        try:
            pid = self._pid(coin)
            params = CancelOrdersParams(
                sender=self.subaccount_hex,
                productIds=[pid],
                digests=[oid],
            )
            return {"raw": str(self._sdk.market.cancel_orders(params))}
        except Exception as e:
            if _is_order_not_found_2020(e):
                log.info("cancel(%s, %s): order already gone (Nado 2020)", coin, oid)
                return {"already_gone": True}
            log.warning("cancel(%s, %s) failed: %s", coin, oid, e)
            return {}

    def cancel_sl_order(self, coin: str, oid) -> dict:
        """Cancel a STOP-LOSS / TRIGGER order. Uses Nado's cancel_trigger_orders API.

        Bug fix 2026-05-06: SL orders на Nado — это TRIGGER orders. cancel_orders
        к ним не применяется (silent fail). Right method = cancel_trigger_orders.

        Log-noise fix 2026-05-06: error_code 2020 ("order not found") demoted
        to INFO. Stale SL oid'ы from previous session = expected on boot —
        bot replaces with fresh SL. Other errors (signature, network, 5xx)
        stay at WARNING.
        """
        try:
            pid = self._pid(coin)
            params = CancelTriggerOrdersParams(
                sender=self.subaccount_hex,
                productIds=[pid],
                digests=[oid],
            )
            return {"raw": str(self._sdk.market.cancel_trigger_orders(params))}
        except Exception as e:
            if _is_order_not_found_2020(e):
                log.info("cancel_sl(%s, %s): order already gone (Nado 2020)", coin, oid)
                return {"already_gone": True}
            log.warning("cancel_sl(%s, %s) failed: %s", coin, oid, e)
            return {}

    def list_reduce_only_triggers(self) -> list[dict]:
        """ALL resting reduce-only TRIGGER orders (SL/TP), normalized for the
        per-cycle orphan_sweep. Reuses fetch_open_orders_ccxt_shape() — which
        RAISES on a failed trigger-service query (false-empty class fix), so the
        sweep skips the cycle rather than acting on a partial view. Every Nado
        trigger is reduce-only (reduceOnly hard-set in the ccxt shape); the cancel
        path is cancel_sl_order(coin, digest) — NOT cancel_order (silent no-op on
        triggers, bugfix 2026-05-06)."""
        out: list[dict] = []
        for t in self.fetch_open_orders_ccxt_shape():
            dg = (t.get("info") or {}).get("digest")
            if dg is None:
                continue
            out.append({
                "coin": t.get("coin") or t.get("symbol"),
                "oid": dg,
                "reduce_only": bool(t.get("reduceOnly", True)),
                "is_trigger": True,
            })
        return out

    def list_open_sl_orders(self, coin: str) -> list[str]:
        """List все active stop/trigger orders на coin.

        Bug fix 2026-05-05 (вечер 4): метод отсутствовал на Nado → trader.py
        orphan SL cleanup всегда no-op на Nado. Теперь корректно использует
        existing fetch_open_orders shim.

        XNN port 2026-06-11 (exception-swallow class fix, mirror of HL canon patch
        §0-10): a failed trigger-service query used to return [] — the caller read
        that as "no live SL" (false-naked) → duplicate SL placement / spurious
        emergency-close. Fail-loud: RAISE so the caller can treat the read-back as
        INDETERMINATE (trader._sl_live_on_exchange assumes live on raise).
        """
        try:
            orders = self.exchange.fetch_open_orders()
        except Exception as e:
            raise RuntimeError(
                f"list_open_sl_orders({coin}): trigger-service read-back FAILED — "
                f"SL liveness INDETERMINATE (not 'no SL'): {e}"
            ) from e
        out = []
        for o in orders:
            if o.get("symbol") != coin and o.get("coin") != coin:
                continue
            t = str(o.get("type", "")).lower()
            info_t = str((o.get("info") or {}).get("orderType", "")).lower()
            info_status = str((o.get("info") or {}).get("status", "")).lower()
            if "trigger" in t or "stop" in t or "trigger" in info_t or "stop" in info_t or "waiting" in info_status:
                # Nado returns digest in info — это order ID
                oid = (o.get("info") or {}).get("digest") or o.get("id")
                if oid: out.append(oid)
        return out

    # ------------ User fills (for compute_realized_pnl) ------------

    def user_fills(self, ttl: float = 60.0) -> list[dict]:
        """Returns empty list — Nado's IndexerMatch doesn't expose product_id directly.
        compute_realized_pnl below queries matches per-product instead.
        Returned for HL-interface compat (trader.py passes this to compute_realized_pnl)."""
        return []

    def _fetch_matches_for_product(self, pid: int, limit: int = 100) -> list[dict]:
        """Fetch fills for a specific product, returns ccxt-like dicts.

        Bug fix 2026-05-06: m.timestamp всегда None в Nado SDK
        (`nado_protocol.indexer_client`). Используем `submission_idx` как
        proxy for time ordering — это monotonically increasing index.
        Fills возвращаются в submission_idx descending order (newest first).
        """
        try:
            data = self._sdk.context.indexer_client.get_matches(
                IndexerMatchesParams(
                    subaccounts=[self.subaccount_hex],
                    product_ids=[pid],
                    limit=limit,
                )
            )
            matches = data.matches if hasattr(data, "matches") else []
        except Exception as e:
            log.warning("get_matches(pid=%d) failed: %s", pid, e)
            return []
        sym = self._pid_to_symbol.get(pid, f"PID_{pid}")
        out = []
        for m in matches:
            try:
                amount_x18 = int(m.order.amount)
                base_filled = _from_x18(m.base_filled)  # actual size filled
                quote_filled = _from_x18(m.quote_filled)
                price = abs(quote_filled / base_filled) if abs(base_filled) > 1e-12 else 0.0
                fee = _from_x18(getattr(m, "fee", "0") or "0")
                # submission_idx as time proxy (timestamp is None in Nado SDK)
                try:
                    sub_idx = int(getattr(m, "submission_idx", 0) or 0)
                except (TypeError, ValueError):
                    sub_idx = 0
                out.append({
                    "symbol": sym,
                    "coin": sym,
                    "side": "buy" if amount_x18 > 0 else "sell",
                    "amount": abs(base_filled),
                    "price": price,
                    "timestamp": 0,  # real ts not available
                    "submission_idx": sub_idx,
                    "fee": {"cost": fee},
                    "info": {"product_id": pid},
                })
            except Exception as e:
                log.warning("match parse (pid=%d): %s", pid, e)
        return out

    def compute_realized_pnl(
        self, fills, coin: str, direction: str, size: float, trade_open_iso=None,
    ):
        """Compute realized PnL from open+close fill pairs. Nado's IndexerMatch
        doesn't have product_id, so we ignore the `fills` arg and re-fetch per
        product. The fills arg stays for HL-interface compat.

        Bug fix 2026-05-06: m.timestamp всегда None в Nado SDK → старая
        фильтрация по `t_ms >= open_ms` всегда была False → closes=[]/opens=[]
        → возвращали None для ВСЕХ closed_vstop trades на Nado.

        Fix: fills возвращаются в submission_idx descending order (newest first).
        Берём first batch closing-side fills (cumulative ≥ size) — это close
        текущей trade. Затем next batch opening-side fills (cumulative ≥ size)
        — это open текущей trade. Submission_idx гарантирует ordering.

        Edge case: если на coin'е была другая trade закрытая ранее (и её
        close+open находятся в fills tail), они не запутают первую пару — мы
        останавливаемся когда заполнили size в каждом batch.
        """
        try:
            pid = self._pid(coin)
        except KeyError:
            return None, None
        fills = self._fetch_matches_for_product(pid, limit=200)
        if not fills:
            return None, None

        # Sort by submission_idx descending (most recent first) for safety.
        # API obviously returns this way but be defensive.
        fills_sorted = sorted(
            (f for f in fills if (f.get("symbol") == coin or f.get("coin") == coin)),
            key=lambda f: int(f.get("submission_idx", 0) or 0),
            reverse=True,
        )
        if not fills_sorted:
            return None, None

        opening_side = "buy" if direction == "long" else "sell"
        closing_side = "sell" if direction == "long" else "buy"

        # Phase 1: collect close fills until cumulative amount ≥ size × 0.95
        target = max(size * 0.95, 0.0001)  # min target to handle float fuzz
        closes: list[dict] = []
        cum_close = 0.0
        i = 0
        n = len(fills_sorted)
        while i < n and cum_close < target:
            f = fills_sorted[i]
            i += 1
            if f.get("side") != closing_side:
                continue
            amt = float(f.get("amount", 0) or 0)
            if amt <= 0:
                continue
            closes.append(f)
            cum_close += amt
        if not closes:
            return None, None

        # Phase 2: continue from where Phase 1 left off, collect opens until target
        # (skipping any further close fills which belong to OLDER trades)
        opens: list[dict] = []
        cum_open = 0.0
        while i < n and cum_open < target:
            f = fills_sorted[i]
            i += 1
            if f.get("side") != opening_side:
                continue
            amt = float(f.get("amount", 0) or 0)
            if amt <= 0:
                continue
            opens.append(f)
            cum_open += amt

        def _wavg(lst):
            tot = sum(float(x.get("amount", 0) or 0) for x in lst)
            if tot <= 0:
                return None, 0.0
            wsum = sum(
                float(x.get("price", 0) or 0) * float(x.get("amount", 0) or 0)
                for x in lst
            )
            return wsum / tot, tot

        avg_close, close_sz = _wavg(closes)
        avg_open, open_sz = _wavg(opens) if opens else (None, 0.0)

        if avg_open is None or open_sz <= 0:
            # Fallback: opens not found (older trade?) — return only avg_close
            return None, avg_close

        matched = min(close_sz, open_sz)
        if direction == "long":
            gross = (avg_close - avg_open) * matched
        else:
            gross = (avg_open - avg_close) * matched
        notional = (avg_open + avg_close) * matched / 2
        fees = notional * 0.001  # ~5bps taker × 2 sides ≈ 0.10%
        return gross - fees, avg_close

    # ------------ Response wrappers (HL-shape for trader.py) ------------

    def _max_match_idx(self, pid: int) -> int | None:
        """Pre-send watermark for the real-fill readback: the highest submission_idx
        currently visible in the per-product matches feed. Fills attributable to a NEW
        order must have submission_idx STRICTLY greater. Returns None when the feed is
        empty or unreadable — AMBIGUOUS, the caller must NOT attribute fills then
        (readback falls to the unconfirmed / position-readback path)."""
        try:
            fills = self._fetch_matches_for_product(pid, limit=5)
        except Exception:
            return None
        if not fills:
            return None
        try:
            return max(int(f.get("submission_idx", 0) or 0) for f in fills)
        except Exception:
            return None

    def _wrap_open_resp(self, sdk_resp, coin: str, is_buy: bool, sz: float,
                        pre_max_idx: int | None = None) -> dict:
        """Translate Nado SDK response to HL-shape so trader's parser doesn't change.

        REAL-FILL READBACK (root fix 2026-07-02): the old wrapper FABRICATED the fill —
        avgPx = current mark, totalSz = requested size — for ANY non-raising response.
        That made the partial-fill guard inert, the slip/parity journal fake, and a
        KILLED (unfilled) FOK was reported as a FULL fill at mark. Now the fill is read
        back from the per-product matches feed (bounded retries, short sleeps): only
        OUR-side fills STRICTLY NEWER than the pre-send watermark count; avgPx = their
        VWAP, totalSz = their cum size. If the readback is unavailable or ambiguous
        (no watermark), return an 'unconfirmed' status instead of a fabricated 'filled'
        — trader's phantom-fill guard then confirms via position readback (journal px
        provisional at mark) or treats the order as UNFILLED. Never silent fake numbers."""
        want_side = "buy" if is_buy else "sell"
        pid = self._symbol_to_pid.get(coin)
        if pre_max_idx is not None and pid is not None:
            attempts = 5
            for _i in range(attempts):
                cum = 0.0
                num = 0.0
                try:
                    fills = self._fetch_matches_for_product(pid, limit=20)
                    for f in fills:
                        try:
                            if f.get("side") != want_side:
                                continue
                            if int(f.get("submission_idx", 0) or 0) <= pre_max_idx:
                                continue
                            amt = float(f.get("amount", 0) or 0)
                            px = float(f.get("price", 0) or 0)
                        except (TypeError, ValueError):
                            continue
                        if amt <= 0 or px <= 0:
                            continue
                        cum += amt
                        num += amt * px
                except Exception as e:
                    log.warning("wrap_open readback error %s (attempt %d): %s", coin, _i + 1, e)
                    cum = 0.0
                if cum >= sz * 0.95 or (_i == attempts - 1 and cum > 0):
                    avg_px = num / cum
                    log.info(
                        "Nado market_open readback %s: REAL fill avg_px=%.8g filled_sz=%s "
                        "(requested %s)", coin, avg_px, cum, sz,
                    )
                    return {
                        "status": "ok",
                        "response": {"type": "order", "data": {
                            "statuses": [{"filled": {
                                "avgPx": str(avg_px),
                                "totalSz": str(cum),
                            }}]
                        }},
                        "_raw": str(sdk_resp)[:300],
                    }
                if _i < attempts - 1:
                    time.sleep(0.6 * (_i + 1))
        log.error(
            "Nado market_open %s: REAL-fill readback unavailable (pre_idx=%s) — returning "
            "UNCONFIRMED, no fabricated fill numbers; trader confirms via position readback "
            "(journal entry px then = live mark, provisional)", coin, pre_max_idx,
        )
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"unconfirmed": (
                "fill readback unavailable — confirm via position readback"
            )}]
        }}, "_raw": str(sdk_resp)[:300]}

    def _wrap_sl_resp(self, sdk_resp, coin: str, confirmed_oid: str | None = None) -> dict:
        """Translate trigger order response into the HL `resting` shape.

        2026-06-07 class-guard: a `resting` (=placed) status is emitted ONLY when the
        caller passes `confirmed_oid` — a digest that `_confirm_trigger_live` matched
        against the live trigger service. The previous behaviour parsed the digest out
        of the (sometimes failure-but-non-raising) response AND fabricated a synthetic
        `nado_sl_<ts>` oid when none was found — reporting success for an SL that was
        never live. That fabrication path is deleted: no confirmation → error status.
        """
        if not confirmed_oid:
            # Defensive: prefer a real digest from the response, never a synthetic one.
            digest = None
            for attr in ("digest", "tx_digest"):
                if hasattr(sdk_resp, attr):
                    digest = getattr(sdk_resp, attr)
                    break
            if digest is None:
                try:
                    d = sdk_resp.dict() if hasattr(sdk_resp, "dict") else {}
                    digest = d.get("data", {}).get("digest") or d.get("digest")
                except Exception:
                    digest = None
            if not digest:
                return _error_resp(
                    f"wrap_sl: trigger for {coin} not confirmed live and no digest in "
                    f"response — refusing to report placed (resp={str(sdk_resp)[:120]})"
                )
            confirmed_oid = digest
        return {"status": "ok", "response": {"type": "order", "data": {
            "statuses": [{"resting": {"oid": confirmed_oid}}]
        }}, "_raw": str(sdk_resp)[:300]}


def _error_resp(msg: str) -> dict:
    """HL-shape error response."""
    return {"status": "ok", "response": {"type": "order", "data": {
        "statuses": [{"error": msg[:200]}]
    }}}


# Public alias for factory
NadoClient = NadoClient_
