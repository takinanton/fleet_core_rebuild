"""Extended Exchange (Starknet) adapter — mirror of HLClient interface.

DRAFT 2026-05-10 — pending testnet 4-step probe per
`feedback_verify_endpoint_returns_oid_not_just_signature`. НЕ использовать в prod
без verify isolated/cross, TPSL POSITION semantics, IOC partial-fill behaviour.

SDK: x10-python-trading-starknet v1.4.1+, async-only. Adapter использует
background event-loop pattern: один daemon-thread держит asyncio loop,
sync-стороне предоставляется .run(coro) bridge для совместимости с остальным
threading-based bot codebase.

Auth: Stark SNIP-12 для writes (через StarkPerpetualAccount.private_key),
X-Api-Key для read. ETH ключ не нужен в runtime — onboarding отдельным
скриптом, после которого .env содержит только api_key + stark keys + vault_id.
"""
from __future__ import annotations

import os
import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import pandas as pd

from bot.config import Settings, TF_MS

log = logging.getLogger(__name__)

# Lazy SDK imports — fail-fast при init если пакет не установлен
# Verified against x10-python-trading-starknet v1.4.0 (2026-05-11 на extended-bot)
try:
    from x10.config import MAINNET_CONFIG, TESTNET_CONFIG
    from x10.core.stark_account import StarkPerpetualAccount
    from x10.perpetual.trading_client import PerpetualTradingClient
    from x10.perpetual.simple_client.simple_trading_client import BlockingTradingClient
    from x10.perpetual.stream_client import PerpetualStreamClient
    from x10.perpetual.order_object import (
        create_order_object, OrderTpslTriggerParam,
    )
    from x10.models.order import (
        OrderSide, OrderType, OrderTpslType, OrderTriggerPriceType,
        OrderPriceType, TimeInForce,
    )
    from x10.errors import X10Error
    from x10.utils.http import RateLimitException, NotAuthorizedException
    from x10.utils.order import get_price_with_slippage
    _SDK_OK = True
except ImportError as _import_err:
    _SDK_OK = False
    _SDK_IMPORT_ERR = str(_import_err)
    log.error("x10-python-trading-starknet import failed: %s", _import_err)


# === SDK BUG FIX: TPSL signing for markets with min_order_size_change >= 1 ===
# x10.utils.order.calc_entire_position_size quantizes settlement_synthetic_amount
# via Decimal(10) ** -quantity_precision. quantity_precision = abs(log10(step)),
# which silently produces FRACTIONAL bucket for markets where step >= 1:
#   AZTEC step=100, qty_prec=2 → bucket=0.01 (WRONG; server rounds to step=100)
#   TRX/EDEN/XLM/GOAT/CC step=10, qty_prec=1 → bucket=0.1 (WRONG; server step=10)
# Result: client signs a settlement amount with extra decimals → server hashes a
# rounded amount → signatures diverge → "Invalid StarkEx signature" code 1101.
# Observed 2026-05-12 on extended-bot main+15m: ~30% SL placement failure rate,
# 2 naked positions (AZTEC #63, CC #114) when emergency close ALSO failed.
# Fix: monkey-patch __create_order_tpsl_trigger_model to use min_order_size_change.
if _SDK_OK:
    try:
        import x10.perpetual.order_object as _x10_oo
        from x10.perpetual.order_object_settlement import (
            create_order_settlement_data as _x10_settlement,
        )
        from x10.models.order import CreateOrderTpslTriggerModel as _CreateTpslModel
        from decimal import ROUND_FLOOR as _ROUND_FLOOR

        def _patched_tpsl_trigger_model(
            *, trigger_param, order_type, side, synthetic_amount,
            tp_sl_type, market, settlement_data_ctx,
        ):
            if tp_sl_type == OrderTpslType.ORDER:
                settlement_amt = synthetic_amount
            else:
                tc = market.trading_config
                raw = tc.max_position_value * Decimal(50) / trigger_param.price
                step = tc.min_order_size_change
                settlement_amt = raw.quantize(step, rounding=_ROUND_FLOOR)
            opp = side if order_type == OrderType.TPSL else (
                OrderSide.BUY if side == OrderSide.SELL else OrderSide.SELL
            )
            sd = _x10_settlement(
                side=opp,
                synthetic_amount=settlement_amt,
                price=trigger_param.price,
                ctx=settlement_data_ctx,
            )
            return _CreateTpslModel(
                trigger_price=trigger_param.trigger_price,
                trigger_price_type=trigger_param.trigger_price_type,
                price=trigger_param.price,
                price_type=trigger_param.price_type,
                settlement=sd.settlement,
                debugging_amounts=sd.debugging_amounts,
            )

        # Module-level __name is NOT name-mangled by Python (mangling only inside class).
        # Use setattr to be explicit and avoid any parser confusion.
        setattr(_x10_oo, "_order_object__create_order_tpsl_trigger_model", _patched_tpsl_trigger_model)
        setattr(_x10_oo, "__create_order_tpsl_trigger_model", _patched_tpsl_trigger_model)
        log.warning("x10 SDK monkey-patch: TPSL settlement_amount uses min_order_size_change (fixes step>=1 sig bug)")
    except Exception as _patch_err:
        log.error("Failed to monkey-patch x10 TPSL signing — naked-SL risk for AZTEC/TRX/EDEN/XLM/GOAT/CC: %s", _patch_err)


# Интервалы Extended → ISO 8601 duration
_INTERVAL_MAP = {
    "1m": "PT1M", "5m": "PT5M", "15m": "PT15M", "30m": "PT30M",
    "1h": "PT1H", "2h": "PT2H", "4h": "PT4H", "1d": "P1D",
}


@dataclass(frozen=True)
class AssetMeta:
    name: str
    sz_decimals: int
    max_leverage: int
    min_size: float = 0.0   # minimum order size in base asset units (from min_order_size)


class _AsyncBridge:
    """Daemon-thread asyncio loop для sync→async моста.

    SDK async-only; threading-based bot codebase зовёт adapter sync.
    Один loop переиспользует aiohttp session (без TCP/TLS handshake на каждый call).
    """

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self._thread.start()

    def run(self, coro, timeout: float = 30.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout=timeout)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)


class ExtendedClient:
    """Extended Exchange (Starknet) client — mirror HLClient interface."""

    def __init__(self, settings: Settings) -> None:
        if not _SDK_OK:
            raise RuntimeError(
                "x10-python-trading-starknet SDK not installed. "
                "pip install x10-python-trading-starknet"
            )
        self.settings = settings
        # DRY parity (defense-in-depth): mirror exchange_hl.HLClient / exchange_pacifica.
        # ALL signed write methods short-circuit via _dry_guard when DRY_RUN=1 so this
        # adapter can NEVER place a live Extended order under DRY (the HL/Pacifica
        # adapters already had this; Extended was the odd one out). Read ops unaffected.
        import os as _os_dry
        self.dry_run = bool(getattr(settings, "dry_run", False)) or bool(
            int(_os_dry.getenv("DRY_RUN", "0") or "0")
        )
        self._bridge = _AsyncBridge()

        cfg = MAINNET_CONFIG if settings.network == "mainnet" else TESTNET_CONFIG
        self._cfg = cfg

        self._stark_acc = StarkPerpetualAccount(
            vault=int(settings.extended_vault_id),
            api_key=settings.extended_api_key,
            public_key=settings.extended_stark_public,
            private_key=settings.extended_stark_private,
        )

        # PerpetualTradingClient как async context — мы держим его открытым через bg loop.
        # Создание клиента обёрнуто в init coroutine, выполняется через bridge.
        async def _init():
            rest = PerpetualTradingClient(cfg, self._stark_acc)
            # __aenter__ вручную чтобы не выходить из контекста
            await rest.__aenter__()
            return rest

        self._client: PerpetualTradingClient = self._bridge.run(_init())

        # Trading helper — даёт filled OpenOrderModel из create_and_place_order.
        # Lazy init: BlockingTradingClient.create() подписывается на WS account stream,
        # Extended rate-limits эти subscribes — крашлуп при ошибке адаптера = 429 storm.
        # Спинаем только на первом place_order/cancel/leverage call.
        self._trader: BlockingTradingClient | None = None
        self._trader_cfg = cfg  # сохраняем для lazy init

        # Кэши
        self._markets_cache: dict | None = None  # name → MarketModel
        self._candles_cache: dict[tuple[str, str], tuple[int, pd.DataFrame, float]] = {}
        self._mark_cache: dict[str, tuple[float, float]] = {}  # coin → (px, fetched_at)
        self._funding_cache: dict[str, tuple[float, float]] = {}
        self._taker_fees: dict[str, Decimal] = {}  # market_name → taker_fee_rate
        self._cache_lock = threading.RLock()
        self._positions_cache: tuple[dict, float] | None = None

        # Стартовая загрузка markets + fees (fees cached раз при init — меняются редко)
        self._load_markets()
        self._load_fees()

        log.info(
            "Extended client init: network=%s, vault=%s, %d markets loaded",
            settings.network, settings.extended_vault_id,
            len(self._markets_cache) if self._markets_cache else 0,
        )

    # ===== Internal helpers =====

    def _load_fees(self) -> None:
        async def _go():
            return await self._client.account.get_fees()
        try:
            r = self._bridge.run(_go(), timeout=15)
            fees = getattr(r, "data", None) or []
            self._taker_fees = {f.market: Decimal(str(f.taker_fee_rate)) for f in fees}
            log.info("Loaded %d taker_fee rates", len(self._taker_fees))
        except Exception as e:
            log.warning("get_fees failed: %s — будет default 0.00045", e)
            self._taker_fees = {}

    def _market_taker_fee(self, market_name: str) -> Decimal:
        return self._taker_fees.get(market_name, Decimal("0.00045"))

    def _load_markets(self) -> None:
        async def _go():
            return await self._client.markets_info.get_markets_dict()
        try:
            self._markets_cache = self._bridge.run(_go(), timeout=15)
        except Exception as e:
            log.warning("get_markets_dict failed: %s — будет lazy-load", e)
            self._markets_cache = {}

    def _market(self, coin: str):
        """Возвращает MarketModel; lazy refresh если miss."""
        if not self._markets_cache:
            self._load_markets()
        m = (self._markets_cache or {}).get(coin)
        if not m:
            # На Extended символы вида "BTC-USD"; coin-список в config может быть "BTC"
            m = (self._markets_cache or {}).get(f"{coin}-USD")
        if not m:
            raise KeyError(f"Extended market not found: {coin}")
        return m

    @staticmethod
    def _round_qty(market, sz: float) -> Decimal:
        """Quantize size к market's min_order_size_change, **enforcing divisibility**.

        Bug class: Decimal('100').quantize(...) только округляет до экспоненты step,
        не enforce divisibility by step. Для AZTEC/1000BONK step=100, GOAT step=10
        bot отправлял qty=12345 → Extended REJECT code 1121 "wrong size increment".
        Fix: qty = floor(sz / step) * step.
        """
        from decimal import ROUND_DOWN
        step = market.trading_config.min_order_size_change
        return (Decimal(str(sz)) / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step

    def _to_hl_response(self, sdk_model, kind: str = "filled") -> dict:
        """Wrap SDK response в HL-shape для совместимости с trader.py."""
        if kind == "filled":
            avg_px = getattr(sdk_model, "average_price", None) or getattr(sdk_model, "price", 0)
            sz = getattr(sdk_model, "filled_qty", None) or getattr(sdk_model, "qty", 0)
            oid = getattr(sdk_model, "id", "")
            status = {"filled": {"avgPx": str(avg_px), "totalSz": str(sz), "oid": oid}}
        else:
            oid = getattr(sdk_model, "id", "")
            status = {"resting": {"oid": oid}}
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": [status]}}}

    # ===== Account state =====

    def account_value(self) -> float:
        async def _go():
            r = await self._client.account.get_balance()
            return float(r.data.equity)
        try:
            return self._bridge.run(_go(), timeout=10)
        except Exception as e:
            log.warning("Extended account_value failed: %s", e)
            return 0.0

    def open_positions(self) -> dict:
        # 5s TTL cache как у KF
        with self._cache_lock:
            if self._positions_cache:
                pos, t = self._positions_cache
                if time.time() - t < 5.0:
                    return pos

        async def _go():
            r = await self._client.account.get_positions()
            return r.data

        try:
            sdk_positions = self._bridge.run(_go(), timeout=10)
        except Exception as e:
            log.warning("Extended open_positions failed: %s", e)
            return {}

        out = {}
        for p in sdk_positions:
            sz_signed = float(p.size) if str(p.side).upper() == "LONG" else -float(p.size)
            entry = {
                "szi": str(sz_signed),
                "entryPx": str(p.open_price),
                "leverage": {"value": int(p.leverage), "type": "cross"},  # cross-only по SDK 1.4.x
                "liquidationPx": str(p.liquidation_price) if p.liquidation_price else None,
                "marginUsed": str(p.value),
                "unrealizedPnl": str(p.unrealised_pnl),
            }
            # На Extended market names = "BTC-USD"; bot.trader ищет bare coin ("BTC").
            # Кладём под обоими ключами — full market_name + stripped coin — чтобы
            # bot.trader `if coin not in positions` не давал false-close.
            out[p.market] = entry
            if p.market.endswith("-USD"):
                bare = p.market.rsplit("-USD", 1)[0]
                out[bare] = entry  # alias on same dict
        with self._cache_lock:
            self._positions_cache = (out, time.time())
        return out

    def invalidate_positions_cache(self) -> None:
        with self._cache_lock:
            self._positions_cache = None

    def spot_usdc(self) -> float:
        async def _go():
            r = await self._client.account.get_spot_balances()
            for b in r.data:
                if str(getattr(b, "asset", "")).upper() == "USDC":
                    return float(getattr(b, "balance", 0))
            return 0.0
        try:
            return self._bridge.run(_go(), timeout=10)
        except Exception:
            return 0.0

    # ===== Market data =====

    @staticmethod
    def _cache_offset_ms(coin: str, interval: str, bar_ms: int) -> int:
        """Per-(coin,interval) deterministic offset — mirror KF/HL anti-thundering-herd."""
        import hashlib
        h = hashlib.md5(f"{coin}:{interval}".encode()).digest()
        raw = int.from_bytes(h[:4], "big")
        cap_ms = min(30_000, bar_ms // 2)
        return raw % max(1, cap_ms) if cap_ms > 0 else 0

    def candles(self, coin: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """OHLCV в pandas DataFrame с bar-aligned cache + 429 backoff (mirror KF)."""
        from bot.config import TF_MS
        bar_ms = TF_MS.get(interval)
        iso = _INTERVAL_MAP.get(interval)
        if not iso or not bar_ms:
            log.warning("Extended unsupported interval: %s", interval)
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        # Early reject: not in markets cache → skip API call entirely
        try:
            self._market(coin)
        except KeyError:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        now_ms = int(time.time() * 1000)
        offset_ms = self._cache_offset_ms(coin, interval, bar_ms)
        effective_now_ms = now_ms - offset_ms
        current_bar_start = (effective_now_ms // bar_ms) * bar_ms
        cache_key = (coin, interval)

        with self._cache_lock:
            cached = self._candles_cache.get(cache_key)
        if cached is not None and cached[0] >= current_bar_start:
            return cached[1].copy()
        # Min-TTL gate (even after boundary — avoid burst at top-of-hour)
        if cached is not None:
            min_ttl = float(os.getenv("CANDLES_MIN_TTL_SEC", "30"))
            if min_ttl > 0 and (now_ms / 1000.0 - cached[2]) < min_ttl:
                return cached[1].copy()

        df = self._fetch_candles_direct(coin, iso, limit)
        if df.empty:
            return df

        last_bar_t_ms = current_bar_start
        try:
            last_bar_t_ms = int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        except Exception:
            pass
        with self._cache_lock:
            self._candles_cache[cache_key] = (last_bar_t_ms, df, time.time())
        return df

    def _fetch_candles_direct(self, coin: str, iso_interval: str, limit: int) -> pd.DataFrame:
        """Direct fetch_candles_history с retry на 429 + exponential backoff."""
        market_name = self._market(coin).name
        async def _go():
            return await self._client.markets_info.get_candles_history(
                market_name=market_name, candle_type="trades",
                interval=iso_interval, limit=limit,
            )
        for attempt in range(3):
            try:
                sdk_candles = self._bridge.run(_go(), timeout=15)
                break
            except Exception as e:
                msg = str(e)
                if "429" in msg or "Too Many" in msg or "Rate limited" in msg:
                    if attempt < 2:
                        wait = 2 ** attempt  # 1, 2, 4
                        log.warning("candles(%s,%s) 429 — backoff %ss (attempt %d)", coin, iso_interval, wait, attempt + 1)
                        time.sleep(wait)
                        continue
                log.warning("candles(%s,%s) failed: %s", coin, iso_interval, msg[:200])
                return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])
        else:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])

        candle_list = getattr(sdk_candles, "data", None) or []
        if not candle_list:
            return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])
        rows = [{
            "t":      int(c.timestamp),
            "Open":   float(c.open), "High":   float(c.high),
            "Low":    float(c.low),  "Close":  float(c.close),
            "Volume": float(c.volume),
        } for c in candle_list]
        df = pd.DataFrame(rows).sort_values("t").reset_index(drop=True)
        df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        return df[["time", "Open", "High", "Low", "Close", "Volume"]]

    def funding_rate(self, coin: str) -> float:
        # 60s TTL
        with self._cache_lock:
            cached = self._funding_cache.get(coin)
            if cached and time.time() - cached[1] < 60:
                return cached[0]

        async def _go():
            r = await self._client.markets_info.get_market_statistics(
                market_name=self._market(coin).name
            )
            return float(r.data.funding_rate or 0.0)

        try:
            rate = self._bridge.run(_go(), timeout=10)
            with self._cache_lock:
                self._funding_cache[coin] = (rate, time.time())
            return rate
        except Exception as e:
            log.warning("funding_rate(%s) failed: %s", coin, e)
            return 0.0

    def asset(self, coin: str) -> AssetMeta:
        m = self._market(coin)
        tc = m.trading_config
        # min_size: use min_order_size if available, else derive from min_order_size_change
        # Extended SDK v1.4.x exposes both fields; min_order_size is the floor for any order.
        # Source: trader.py uses meta.min_size to reject undersized positions.
        min_size_raw = (
            getattr(tc, "min_order_size", None)
            or getattr(tc, "min_order_size_change", None)
            or Decimal("0")
        )
        try:
            min_size_float = float(min_size_raw)
        except Exception:
            min_size_float = 0.0
        return AssetMeta(
            name=m.name,
            sz_decimals=int(getattr(tc, "quantity_precision", 4)),
            max_leverage=int(getattr(tc, "max_leverage", 10)),
            min_size=min_size_float,
        )

    def mark_price(self, coin: str, ttl: float = 5.0) -> float:
        with self._cache_lock:
            cached = self._mark_cache.get(coin)
            if cached and time.time() - cached[1] < ttl:
                return cached[0]

        async def _go():
            r = await self._client.markets_info.get_market_statistics(
                market_name=self._market(coin).name
            )
            return float(r.data.mark_price)

        try:
            px = self._bridge.run(_go(), timeout=10)
            with self._cache_lock:
                self._mark_cache[coin] = (px, time.time())
            return px
        except Exception as e:
            log.warning("mark_price(%s) failed: %s", coin, e)
            return 0.0

    def slip_per_side(self, coin: str, notional_usd: float) -> float:
        # TODO: walk orderbook для accurate slippage; пока fallback на default 0.001 (10bps)
        # Implementation: c.info.get_orderbook_snapshot(market_name=coin).data.{bid,ask}
        # walk levels until cumulative qty*price >= notional_usd
        return 0.001

    def round_price(self, coin: str, px: float) -> float:
        """Legacy float-return API. Используется callers вне Stark signing path —
        для signing path вызывать _round_price_dec напрямую (no float roundtrip)."""
        m = self._market(coin)
        try:
            return float(m.trading_config.round_price(Decimal(str(px))))
        except Exception:
            return px

    @staticmethod
    def _round_price_dec(market, px, *, sl_side: "OrderSide | None" = None) -> Decimal:
        """Quantize price к market min_price_change, возвращает Decimal.

        Float roundtrip убивает precision (`Decimal(str(58.0891905))` → 7 decimals,
        но Extended требует exactly market.min_price_change exponent). Decimal-clean.
        Rounding direction: SL must remain "outside" entry — для LONG SL (SELL trigger) →
        ROUND_DOWN (SL чуть ниже = safer); для SHORT SL (BUY trigger) → ROUND_UP.
        """
        from decimal import ROUND_DOWN, ROUND_UP
        if not isinstance(px, Decimal):
            px = Decimal(str(px))
        rounding = ROUND_DOWN  # default safer for long SL
        if sl_side is not None:
            # If sl_side == BUY (closing short), we want price UP to stay above entry → ROUND_UP
            from x10.models.order import OrderSide as _OS
            if sl_side == _OS.BUY:
                rounding = ROUND_UP
        return px.quantize(market.trading_config.min_price_change, rounding=rounding)

    def _dry_guard(self, op: str):
        """Base-level DRY short-circuit for ALL SIGNED write methods (defense-in-depth).

        Mirrors exchange_hl.HLClient._dry_guard and exchange_pacifica (POST-level
        `if self.dry_run`): returns a benign HL-shaped success dict (empty `statuses`,
        so no oid/fill is ever parsed -> never a real order) when DRY_RUN=1, else None
        (caller proceeds to the live x10 SDK). READ methods are unaffected.
        """
        if self.dry_run:
            log.info("[DRY] Extended write blocked at adapter: %s", op)
            return {"status": "ok",
                    "response": {"type": "order",
                                 "data": {"statuses": [], "dry_run": True}}}
        return None

    # ===== Orders =====

    async def _ensure_trader(self):
        """Lazy spin-up of BlockingTradingClient (WS subscribe on first trade only)."""
        if self._trader is None:
            self._trader = await BlockingTradingClient.create(
                config=self._trader_cfg, account=self._stark_acc,
            )
        return self._trader

    def market_open(self, coin: str, is_buy: bool, sz: float) -> dict:
        """Market entry — IOC limit at aggressive price for fill guarantee."""
        _d = self._dry_guard(f"market_open({coin} is_buy={is_buy} sz={sz})")
        if _d is not None:
            return _d
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        # Aggressive price: mark + slippage protection (helper из SDK)
        mp = Decimal(str(m.market_stats.mark_price))
        slip = Decimal(str(self.settings.slippage or 0.01))
        agg_price = get_price_with_slippage(
            side=side, price=mp,
            min_price_change=m.trading_config.min_price_change,
            slippage=slip,
        )
        qty = self._round_qty(m, sz)
        taker_fee = self._market_taker_fee(m.name)
        log.info("market_open %s %s sz=%s→qty=%s mark=%s aggr=%s fee=%s",
                 coin, side.value, sz, qty, mp, agg_price, taker_fee)

        async def _go():
            trader = await self._ensure_trader()
            r = await trader.create_and_place_order(
                market_name=m.name,
                amount_of_synthetic=qty,
                price=agg_price,
                side=side,
                taker_fee=taker_fee,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.IOC,
            )
            return r  # OpenOrderModel с average_price, filled_qty, id, external_id

        # Snapshot pre-open positions (для recovery после timeout)
        pre_size = 0.0
        try:
            pre = (self.open_positions() or {}).get(coin) or (self.open_positions() or {}).get(m.name)
            if pre:
                pre_size = abs(float(pre.get("szi", 0)))
        except Exception:
            pass

        try:
            # 60s: первый trade'ов BlockingTradingClient.create() делает WS subscribe
            # (~15-30s); затем create_and_place_order ждёт fill confirmation через WS.
            # SDK внутри имеет свои timeouts (aiohttp 5s) — наш 60s upper bound для bridge.
            opened = self._bridge.run(_go(), timeout=60)
            return self._to_hl_response(opened, kind="filled")
        except Exception as e:
            err = str(e)[:200] or type(e).__name__
            # Lock-recovery retry (2026-05-15 audit): SDK x10 1.4.x periodically raises
            # asyncio "Lock is not acquired" — trader's internal state broken (likely
            # WS reconnect race). Re-creating BlockingTradingClient resolves. Audit
            # 2026-05-15 на Ext-15m нашёл 43 cancellations за 24h все от этой ошибки.
            if "Lock is not acquired" in err or "lock is not acquired" in err.lower():
                log.warning("market_open(%s) Lock-error — recreating trader for retry", coin)
                try:
                    # Force recreation; BlockingTradingClient.create rebuilds WS.
                    self._trader = None
                except Exception:
                    pass
                try:
                    opened = self._bridge.run(_go(), timeout=60)
                    log.info("market_open(%s) Lock-recovery retry SUCCEEDED", coin)
                    return self._to_hl_response(opened, kind="filled")
                except Exception as e2:
                    err = str(e2)[:200] or type(e2).__name__
                    log.warning("market_open(%s) Lock-recovery retry also failed: %s", coin, err)
            log.warning("market_open(%s) bridge err: %s — POST-TIMEOUT FALLBACK: querying positions",
                        coin, err)
            # Post-timeout recovery: order МОГ быть placed на бирже, polling positions
            # до 20s — Extended positions endpoint может отстаивать 5-10s после market order.
            # На DOT 2026-05-11 одна попытка через 2s упустила позицию которая появилась
            # на 5 минут позже (см. memory: post-timeout-but-placed bug).
            recovered = False
            for attempt in range(10):  # 10 × 2s = 20s
                try:
                    self.invalidate_positions_cache()
                    time.sleep(2)
                    positions = self.open_positions() or {}
                    post = positions.get(coin) or positions.get(m.name)
                    if post:
                        post_size = abs(float(post.get("szi", 0)))
                        delta = post_size - pre_size
                        # Bug 2026-05-14: 50% threshold dropped partial fills <50% (BABA 9.8%, DOT etc),
                        # bot marked cancelled → sl_audit cron later restored with WRONG DB.size.
                        # Lower to MIN_FILL_RATIO (default 0.05 = 5%); aligns with downstream
                        # trader.py partial-fill abort gate. Captures real partials as success.
                        min_fr = float(os.environ.get("MIN_FILL_RATIO", "0.05"))
                        if delta >= float(qty) * min_fr:
                            avg_px = post.get("entryPx", str(agg_price))
                            log.warning("market_open(%s) RECOVERED [attempt %d]: pre=%s post=%s delta=%s entry=%s",
                                        coin, attempt + 1, pre_size, post_size, delta, avg_px)
                            return {
                                "status": "ok",
                                "response": {"type": "order", "data": {
                                    "statuses": [{"filled": {
                                        "avgPx": str(avg_px),
                                        "totalSz": str(delta),
                                        "oid": str(post.get("oid", "recovered")),
                                    }}]
                                }},
                            }
                except Exception as rec_err:
                    log.warning("market_open(%s) recovery attempt %d err: %s", coin, attempt + 1, rec_err)
            log.error("market_open(%s) confirmed FAILED after 20s polling (no position appeared): %s — sl_audit cron defense will catch if order surfaces later", coin, err)
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": err}]
                }},
            }

    def market_close(self, coin: str) -> dict:
        _d = self._dry_guard(f"market_close({coin})")
        if _d is not None:
            return _d
        positions = self.open_positions()
        pos = positions.get(coin) or positions.get(self._market(coin).name)
        if not pos:
            log.warning("market_close: no position for %s", coin)
            return {}
        sz_signed = float(pos["szi"])
        if sz_signed == 0:
            return {}
        sz = abs(sz_signed)
        side = OrderSide.SELL if sz_signed > 0 else OrderSide.BUY
        m = self._market(coin)

        # Aggressive close price (mirror open path)
        mp = Decimal(str(m.market_stats.mark_price))
        slip = Decimal(str(self.settings.slippage or 0.01))
        agg_price = get_price_with_slippage(
            side=side, price=mp,
            min_price_change=m.trading_config.min_price_change,
            slippage=slip,
        )
        taker_fee = self._market_taker_fee(m.name)

        qty = self._round_qty(m, sz)

        async def _go():
            trader = await self._ensure_trader()
            return await trader.create_and_place_order(
                market_name=m.name,
                amount_of_synthetic=qty,
                price=agg_price,
                side=side,
                taker_fee=taker_fee,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.IOC,
                reduce_only=True,
            )

        try:
            closed = self._bridge.run(_go(), timeout=20)
            return self._to_hl_response(closed, kind="filled")
        except Exception as e:
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": str(e)[:200]}]
                }},
            }

    def trigger_sl(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Position-wide SL через TPSL POSITION (не synthetic_amount-based).

        is_buy = направление CLOSE side (т.е. для long-position SL это is_buy=False).
        Validator (order_object.py:190-201) требует amount_of_synthetic=0, price=0,
        reduce_only=True для tp_sl_type=POSITION.
        """
        _d = self._dry_guard(f"trigger_sl({coin} is_buy={is_buy} sz={sz} px={trigger_px})")
        if _d is not None:
            return _d
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        sl_px = self._round_price_dec(m, trigger_px, sl_side=side)
        log.info("trigger_sl %s %s trigger=%s→%s", coin, side.value, trigger_px, sl_px)

        # CRITICAL: Extended TPSL по умолчанию имеет 1h TTL — без явного expire_time
        # SL автоматически EXPIRED через час → naked position. См. AAVE 2026-05-11.
        from datetime import datetime, timedelta, timezone
        expire_at = datetime.now(timezone.utc) + timedelta(days=28)

        # CRITICAL: price_type=MARKET TPSL отвергаются Extended (NO_LIQUIDITY) при крупных
        # позициях на тонком orderbook — см. ZRO 2026-05-11 (status_reason=NO_LIQUIDITY).
        # Используем LIMIT с slippage buffer 1% — trigger fires → limit SELL/BUY rests в orderbook.
        # Filling guaranteed при large enough buffer, без market impact rejection.
        slip = Decimal(str(self.settings.slippage or 0.01))
        if side == OrderSide.SELL:  # closing LONG — limit price BELOW trigger for aggressive fill
            limit_price_raw = sl_px * (Decimal(1) - slip)
        else:  # closing SHORT — limit price ABOVE trigger
            limit_price_raw = sl_px * (Decimal(1) + slip)
        from decimal import ROUND_DOWN as _RDOWN, ROUND_UP as _RUP
        limit_price = limit_price_raw.quantize(
            m.trading_config.min_price_change,
            rounding=_RDOWN if side == OrderSide.SELL else _RUP,
        )

        async def _go():
            order = create_order_object(
                account=self._stark_acc, market=m,
                starknet_domain=self._cfg.signing.starknet_domain,
                order_type=OrderType.TPSL, time_in_force=TimeInForce.GTT,
                expire_time=expire_at,
                side=side,
                amount_of_synthetic=Decimal(0),
                price=Decimal(0),
                reduce_only=True,
                tp_sl_type=OrderTpslType.POSITION,
                stop_loss=OrderTpslTriggerParam(
                    trigger_price=sl_px,
                    trigger_price_type=OrderTriggerPriceType.MARK,
                    price=limit_price,
                    price_type=OrderPriceType.LIMIT,
                ),
            )
            return await self._client.orders.place_order(order)

        try:
            placed = self._bridge.run(_go(), timeout=15)
            return self._to_hl_response(placed.data, kind="resting")
        except Exception as e:
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": str(e)[:200]}]
                }},
            }

    def trigger_tp(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        _d = self._dry_guard(f"trigger_tp({coin} is_buy={is_buy} sz={sz} px={trigger_px})")
        if _d is not None:
            return _d
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        # TP: closing side (BUY closes short, SELL closes long) — для long TP — SELL → ROUND_DOWN; for short → ROUND_UP
        tp_px = self._round_price_dec(m, trigger_px, sl_side=side)

        # Same 28d expiry для TP (mirror SL)
        from datetime import datetime, timedelta, timezone
        expire_at = datetime.now(timezone.utc) + timedelta(days=28)

        async def _go():
            order = create_order_object(
                account=self._stark_acc, market=m,
                starknet_domain=self._cfg.signing.starknet_domain,
                order_type=OrderType.TPSL, time_in_force=TimeInForce.GTT,
                expire_time=expire_at,
                side=side,
                amount_of_synthetic=Decimal(0),
                price=Decimal(0),
                reduce_only=True,
                tp_sl_type=OrderTpslType.POSITION,
                take_profit=OrderTpslTriggerParam(
                    trigger_price=tp_px,
                    trigger_price_type=OrderTriggerPriceType.MARK,
                    price=tp_px,
                    price_type=OrderPriceType.MARKET,
                ),
            )
            return await self._client.orders.place_order(order)

        try:
            placed = self._bridge.run(_go(), timeout=15)
            return self._to_hl_response(placed.data, kind="resting")
        except Exception as e:
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": str(e)[:200]}]
                }},
            }

    def cancel_sl_order(self, coin: str, oid) -> dict:
        _d = self._dry_guard(f"cancel_sl_order({coin} oid={oid})")
        if _d is not None:
            return {"status": "ok", "dry_run": True}
        async def _go():
            return await self._client.orders.cancel_order(int(oid))
        try:
            self._bridge.run(_go(), timeout=10)
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)[:200]}

    def list_open_sl_orders(self, coin: str) -> list:
        m = self._market(coin)

        async def _go():
            r = await self._client.account.get_open_orders(
                market_names=[m.name], order_type=OrderType.TPSL,
            )
            return [str(o.id) for o in r.data]

        try:
            return self._bridge.run(_go(), timeout=10)
        except Exception as e:
            log.warning("list_open_sl_orders(%s) failed: %s", coin, e)
            return []

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> Optional[dict]:
        # SDK 1.4.x не поддерживает isolated/cross flag — assume cross-only (verify testnet!)
        _d = self._dry_guard(f"update_leverage({coin} lev={leverage} cross={is_cross})")
        if _d is not None:
            return {"status": "ok", "dry_run": True}
        if not is_cross:
            log.warning("Extended SDK 1.4.x does not expose isolated mode — leveraging cross")
        m = self._market(coin)

        async def _go():
            return await self._client.account.update_leverage(
                market_name=m.name, leverage=Decimal(int(leverage)),
            )

        try:
            self._bridge.run(_go(), timeout=10)
            return {"status": "ok"}
        except Exception as e:
            log.warning("update_leverage(%s, %dx) failed: %s", coin, leverage, e)
            return None

    def user_fills(self) -> list:
        async def _go():
            r = await self._client.account.get_trades()
            return r.data
        try:
            sdk_fills = self._bridge.run(_go(), timeout=15)
        except Exception as e:
            log.warning("user_fills failed: %s", e)
            return []
        # Bot стороной DB хранит bare coin ("XLM"), Extended market = "XLM-USD".
        # Strip suffix чтобы matching по coin работал (как в open_positions выше).
        out = []
        for f in sdk_fills:
            mkt = str(f.market)
            bare = mkt.rsplit("-USD", 1)[0] if mkt.endswith("-USD") else mkt
            out.append({
                "coin": bare,
                "market": mkt,
                "side": "B" if str(f.side).upper() == "BUY" else "A",
                "px": str(f.price),
                "sz": str(f.qty),
                "fee": str(f.fee),
                "time": int(f.created_time),  # AccountTradeModel.created_time (ms epoch)
                "oid": f.id,
                "trade_type": str(f.trade_type),
            })
        return out

    def compute_realized_pnl(
        self,
        fills: list,
        coin: str,
        direction: str,
        size: float,
        trade_open_iso: str | None = None,
    ):
        # Extended SDK не expose'ит realized PnL — собираем close-side fills
        # для этого trade и возвращаем weighted-avg exit_px. trader.py fallback
        # пересчитает gross PnL по DB entry × exit_px × size и применит ~0.10%
        # round-trip fee approx. Без этого метода default HL-style
        # _compute_realized_pnl ищет dir "Close Long"/"Close Short" + closedPnl —
        # ничего из этого в Extended fills нет, и pnl_dollars=None навсегда.
        if not fills:
            return None, None
        open_ms = 0
        if trade_open_iso:
            try:
                from datetime import datetime as _dt, timezone as _tz
                _t = _dt.fromisoformat(str(trade_open_iso).replace("Z", "+00:00"))
                if _t.tzinfo is None:
                    _t = _t.replace(tzinfo=_tz.utc)
                open_ms = int(_t.timestamp() * 1000) - 60_000
            except Exception:
                open_ms = 0
        expected_side = "A" if direction == "long" else "B"  # close opposite to open
        matching = []
        for f in fills:
            if f.get("coin") != coin:
                continue
            if f.get("side") != expected_side:
                continue
            try:
                t_ms = int(f.get("time", 0))
            except (TypeError, ValueError):
                t_ms = 0
            if open_ms and t_ms and t_ms < open_ms:
                continue
            matching.append(f)
        if not matching:
            return None, None
        total_sz = 0.0
        weighted = 0.0
        for f in matching:
            try:
                sz = float(f.get("sz", 0))
                px = float(f.get("px", 0))
            except (TypeError, ValueError):
                continue
            total_sz += sz
            weighted += px * sz
        avg_px = (weighted / total_sz) if total_sz > 0 else None
        return None, avg_px

    # ===== HL compat shims =====

    @property
    def info(self):
        """HL-compat: client.info.user_state() — заглушка."""
        return self

    def user_state(self, address: str = "") -> dict:
        """HL-compat: marginSummary shape."""
        async def _go():
            return (await self._client.account.get_balance()).data

        try:
            bal = self._bridge.run(_go(), timeout=10)
        except Exception:
            return {}
        ms = {
            "accountValue": str(bal.equity),
            "totalMarginUsed": str(bal.initial_margin),
            "totalNtlPos": str(bal.equity),  # TODO: точнее через positions sum
        }
        positions = self.open_positions()
        asset_positions = [{"position": p} for p in positions.values()]
        return {
            "marginSummary": ms,
            "crossMarginSummary": ms,
            "withdrawable": str(bal.available_for_withdrawal),
            "crossMaintenanceMarginUsed": "0",  # SDK не expose'ит maintenance отдельно
            "assetPositions": asset_positions,
        }


# ===== Pending TODOs (testnet probe) =====
# 1. Verify TPSL POSITION закрывает всю позицию post-trigger (synthetic_amount=0)
# 2. Verify cross-only assumption — попробовать update_leverage с isolated mode
# 3. Implement WS subscription для live position/order/fill updates (push в queue.Queue)
# 4. WS auto-reconnect: recursive pattern по примеру BlockingTradingClient.___order_stream
# 5. ApiRateLimitError backoff (нет Retry-After parsing в SDK)
# 6. IOC market partial-fill behaviour: probe oversized order, какой signal
# 7. Реализовать candles cache с bar-aligned offset (mirror exchange_kraken.py:165)
# 8. slip_per_side через get_orderbook_snapshot (сейчас fallback 0.001)
# 9. Add `extended_vault_id: int` в Settings dataclass + .env.extended.example
