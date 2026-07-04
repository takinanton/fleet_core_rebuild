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
        create_order_object, OrderTpslTriggerParam, OrderConditionalTriggerParam,
    )
    from x10.models.order import (
        OrderSide, OrderType, OrderTpslType, OrderTriggerPriceType,
        OrderPriceType, TimeInForce, OrderTriggerDirection,
    )
    from x10.errors import X10Error
    from x10.utils.http import RateLimitException, NotAuthorizedException
    from x10.utils.order import get_price_with_slippage
    _SDK_OK = True
except ImportError as _import_err:
    _SDK_OK = False
    _SDK_IMPORT_ERR = str(_import_err)
    log.error("x10-python-trading-starknet import failed: %s", _import_err)


class ExtOrderConfirmTimeout(Exception):
    """Order was sent (or send is still in-flight) but no WS fill-confirm arrived.

    Carries external_id so the sync adapter can REST-confirm the true order state
    instead of guessing from position polls alone (2026-07-01 burst: 6 coins ×50s
    serial WS-confirm timeouts, 0 fills booked while trader WS stream was dead).
    place_landed: True = REST place returned OK; None = place call still in-flight
    (may land late — retry only after REST lookup shows no order).
    """

    def __init__(self, external_id: str, place_landed: "bool | None"):
        super().__init__(
            f"no WS confirm for order external_id={external_id} (place_landed={place_landed})"
        )
        self.external_id = external_id
        self.place_landed = place_landed


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
    # ============================================================
    # PATCH 2 (2026-05-25, reworked 2026-07-02): WS confirm → fail-fast + REST-confirm
    # ============================================================
    # x10/perpetual/simple_client/simple_trading_client.py:243 hard-codes
    #   asyncio.wait_for(order_waiter.condition.wait(), 5)
    # — 5-second timeout on WebSocket order-event confirmation. Observed
    # 2026-05-25 on Extended-a XAU 1h: every hourly signal → ENTRY →
    # market_open(XAU) → TimeoutError in 5s → 0 trades opened in 24h (WS event
    # arrives but >5s after order placement on Tokyo→client RTT under load).
    # First fix raised the wait to 30s. 2026-07-01 16:00 showed the next failure
    # mode: the trader's WS account-stream dies silently in a long-lived process →
    # NO confirm ever arrives → every entry in a burst burned 30s WS + 20s position
    # poll serially (SOL/SUI/AERO/DOT/MON/APT ~50s each, 0 fills; +ADA/CRV/ONDO).
    # Rework: (1) hard timeout on the REST place call itself (hung-session class),
    # (2) short WS wait, (3) on miss raise ExtOrderConfirmTimeout carrying
    # external_id so market_open() REST-confirms the true order state, recreates
    # the trader (dead-WS heal, same as Lock-recovery) and retries once guarded.
    try:
        import asyncio as _asyncio_ws
        import time as _time_ws
        from x10.perpetual.simple_client.simple_trading_client import (
            BlockingTradingClient as _BTC, OrderWaiter as _OW,
            TimedOpenOrderModel as _TOM,
        )
        from x10.perpetual.order_object import create_order_object as _coo
        from x10.models.order import (
            NewOrderModel as _NOM, OrderSide as _OS, OrderType as _OT,
            TimeInForce as _TIF,
        )

        # Fail-fast knobs (env-tunable):
        # EXT_PLACE_TIMEOUT_S: signed REST place normally lands <1s from Tokyo;
        #   15s = generous ceiling before declaring the session hung (same hard-timeout
        #   class as the HL SDK hung-fetch rootfix 2026-07-02).
        # EXT_WS_CONFIRM_TIMEOUT_S: observed confirm lag tail on XAU/illiquid was
        #   >5s but <10s; anything slower is handled by the REST-confirm fallback,
        #   so 10s covers the real tail without burning burst time.
        _PLACE_TIMEOUT_S = float(os.environ.get("EXT_PLACE_TIMEOUT_S", "15"))
        _WS_CONFIRM_S = float(os.environ.get("EXT_WS_CONFIRM_TIMEOUT_S", "10"))

        async def _patched_create_and_place_order(
            self,
            market_name,
            amount_of_synthetic,
            price,
            side,
            taker_fee,
            post_only=False,
            previous_order_external_id=None,
            external_id=None,
            builder_fee=None,
            builder_id=None,
            time_in_force=_TIF.GTT,
            reduce_only=False,
            order_type=_OT.LIMIT,
        ):
            market = (await self.get_markets()).get(market_name)
            if not market:
                raise ValueError(f"Market '{market_name}' not found.")
            account = getattr(self, "_BlockingTradingClient__account")
            config = getattr(self, "_BlockingTradingClient__config")
            waiters = getattr(self, "_BlockingTradingClient__order_waiters")
            orders_module = getattr(self, "_BlockingTradingClient__orders_module")
            order = _coo(
                account=account,
                market=market,
                order_type=order_type,
                amount_of_synthetic=amount_of_synthetic,
                price=price,
                side=side,
                post_only=post_only,
                reduce_only=reduce_only,
                previous_order_external_id=previous_order_external_id,
                starknet_domain=config.signing.starknet_domain,
                order_external_id=external_id,
                builder_fee=builder_fee,
                builder_id=builder_id,
                time_in_force=time_in_force,
                taker_fee=taker_fee,
            )
            # order.id == external_id when one was passed (order_object.py:259 sets
            # id = order_external_id or stark hash) — and that is exactly the key the
            # WS handler (__handle_update) looks waiters up by, since the server echoes
            # external_id back on OpenOrderModel. Custom external_id is therefore safe.
            if order.id in waiters:
                raise ValueError(f"order with {order.id} hash already placed")
            waiters[order.id] = _OW(_asyncio_ws.Condition(), None, start_nanos=_time_ws.time_ns())
            order_waiter = waiters[order.id]
            placed_order_task = _asyncio_ws.create_task(orders_module.place_order(order))
            place_landed = None
            try:
                # 1) REST place with HARD timeout — SDK session can hang indefinitely.
                try:
                    await _asyncio_ws.wait_for(_asyncio_ws.shield(placed_order_task), _PLACE_TIMEOUT_S)
                    place_landed = True
                except (TimeoutError, _asyncio_ws.TimeoutError):
                    # Place still in-flight — may land late. Swallow its eventual
                    # result so asyncio doesn't warn "exception never retrieved".
                    placed_order_task.add_done_callback(
                        lambda t: t.exception() if not t.cancelled() else None
                    )
                # (a real API reject raises through here and reaches the caller as-is)
                # 2) WS fill-confirm wait, short — REST fallback covers the slow tail.
                async with order_waiter.condition:
                    if order_waiter.open_order is None:
                        try:
                            await _asyncio_ws.wait_for(order_waiter.condition.wait(), _WS_CONFIRM_S)
                        except (TimeoutError, _asyncio_ws.TimeoutError):
                            pass
                open_model = order_waiter.open_order
                if open_model is not None:
                    return open_model
                raise ExtOrderConfirmTimeout(order.id, place_landed)
            finally:
                # Always deregister — old code leaked the waiter on timeout (dict grew
                # forever; late WS events notified a Condition nobody would ever await).
                waiters.pop(order.id, None)

        _BTC.create_and_place_order = _patched_create_and_place_order
        log.warning(
            "x10 SDK monkey-patch: create_and_place_order fail-fast place=%ss ws-confirm=%ss + REST-confirm fallback",
            _PLACE_TIMEOUT_S, _WS_CONFIRM_S,
        )
    except Exception as _patch2_err:
        log.error("Failed to monkey-patch x10 create_and_place_order — XAU/slow-WS markets will keep failing: %s", _patch2_err)



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
        self._ws_feed = None  # WsCandleFeed attached by main_loop when EXTENDED_WS_CANDLE=1

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

    def margin_used_usd(self) -> float:
        """REAL exchange initial-margin used by ALL open positions (incl manual/foreign), in $.

        Account-level total margin used = get_balance().data.initial_margin (Decimal).
        This is the field Extended's UI shows as "Initial Margin"; it equals
        SUM(notional_i / leverage_i) where each position sits on its OWN leverage — so it
        is the authoritative existing_margin for the MM-cap gate. Do NOT reconstruct it
        from notional/leverage, and do NOT sum positions['marginUsed'] (that field is the
        position NOTIONAL, not margin — see open_positions() above, p.value is mark-valued).

        RAISES on read failure (does not return 0): the caller fails the MM-cap gate CLOSED
        when positions exist and margin cannot be read, rather than under-counting to $0.
        """
        async def _go():
            r = await self._client.account.get_balance()
            return float(r.data.initial_margin)
        return self._bridge.run(_go(), timeout=10)

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
            # Fail-direction law (phase0 2026-07-02): a failed read must NEVER surface as {}
            # — callers treat "empty" as "all positions gone" (phantom-guard mass-close /
            # false-flat class). Serve a RECENT cache on a transient blip (bias-to-present,
            # bounded to one loop interval so a dead API can't freeze a stale view forever);
            # otherwise RAISE so every caller handles UNKNOWN explicitly (trader.py
            # manage/entry/_ensure_flat and orphan_sweep already catch-and-defer).
            with self._cache_lock:
                cached = self._positions_cache
            if cached:
                pos, t = cached
                age = time.time() - t
                max_stale = float(getattr(self.settings, "loop_interval_sec", 60) or 60)
                if age <= max_stale:
                    log.warning(
                        "Extended open_positions failed (%s) — serving %.0fs-old cache "
                        "(bias-to-present, <= %.0fs)", e, age, max_stale,
                    )
                    return pos
            log.error("Extended open_positions failed with no usable cache — RAISING "
                      "(state UNKNOWN, never {}): %s", e)
            raise

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

    def invalidate_candles_cache(self, coin: str | None = None, interval: str | None = None) -> None:
        """XNN patch 2026-06-11 (canon §0.10): force the next candles() call past the
        bar-start cache — used by the scanner freshness-guard when the decision bar is
        stale. coin=None clears everything; interval=None clears all TFs of the coin
        (also flushes the 4h base of the 8h/1w resample path)."""
        with self._cache_lock:
            if coin is None:
                self._candles_cache.clear()
                return
            for k in [k for k in self._candles_cache
                      if k[0] == coin and (interval is None or k[1] == interval)]:
                self._candles_cache.pop(k, None)

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

    # 2026-05-28: Extended API lacks 8h + 1w natively (_INTERVAL_MAP has none).
    # Fetch nearest native base TF and aggregate client-side: 8h←4h (2:1),
    # 1w←1d (7:1). Memory: feedback_extended_no_8h_interval.
    _RESAMPLE_FROM: dict[str, tuple[str, int]] = {
        "8h": ("4h", 2),
        "1w": ("1d", 7),
    }

    def _resample_ohlcv(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """Aggregate native-TF OHLCV to `interval` boundaries (epoch-aligned,
        label/closed=left to match scanner bar-start computation)."""
        if df is None or df.empty:
            return df
        from bot.config import TF_MS
        ms = TF_MS[interval]
        rule = f"{ms // 1000}s"
        return (
            df.set_index("time")
              .resample(rule, label="left", closed="left", origin="epoch")
              .agg({"Open": "first", "High": "max", "Low": "min",
                    "Close": "last", "Volume": "sum"})
              .dropna(subset=["Open", "Close"])
              .reset_index()
        )

    def candles(self, coin: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """OHLCV в pandas DataFrame с bar-aligned cache + 429 backoff (mirror KF)."""
        from bot.config import TF_MS
        # Resample path for non-native TFs (8h, 1w).
        if interval in self._RESAMPLE_FROM:
            base_tf, factor = self._RESAMPLE_FROM[interval]
            base_df = self.candles(coin, base_tf, limit=(limit + 2) * factor)
            if base_df is None or base_df.empty:
                return pd.DataFrame(columns=["time", "Open", "High", "Low", "Close", "Volume"])
            return self._resample_ohlcv(base_df, interval)
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

        # WS-PUSH fast path (2026-06-27, port of Pacifica): serve the confirmed-final closed
        # window from the live candle feed -> 0 REST get_candles_history. None unless
        # warm+current -> REST fallback below (zero correctness risk). env-gated EXTENDED_WS_CANDLE.
        _wsf = getattr(self, "_ws_feed", None)
        if _wsf is not None:
            try:
                _wdf = _wsf.get_df(coin, interval, limit)
            except Exception:
                _wdf = None
            if _wdf is not None and not _wdf.empty:
                return _wdf

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

        # Warm the WS store from this REST df so cold-start reuses existing path (no burst).
        if _wsf is not None:
            try:
                _wsf.seed(coin, interval, df)
            except Exception:
                pass

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

    # ===== Orders =====

    async def _ensure_trader(self):
        """Lazy spin-up of BlockingTradingClient (WS subscribe on first trade only)."""
        if self._trader is None:
            self._trader = await BlockingTradingClient.create(
                config=self._trader_cfg, account=self._stark_acc,
            )
        return self._trader

    def _recreate_trader(self) -> None:
        """Drop a degraded BlockingTradingClient — dead WS account-stream and/or hung
        aiohttp session. Heal precedents: Lock-error (2026-05-15, 43 cancellations/24h)
        and WS-confirm burst (2026-07-01 16:00: 6 coins ×50s serial, 0 fills; process
        restart instantly fixed it → process-lifetime trader state is the root).
        Best-effort close of the old client, then lazy re-create on next _ensure_trader().
        """
        old, self._trader = self._trader, None
        if old is not None:
            try:
                self._bridge.run(old.close(), timeout=5)
            except Exception as close_err:
                log.warning("trader close during recreate failed (ignored): %s", close_err)

    def _lookup_order_fill(self, ext_id: str) -> tuple:
        """REST-confirm the true state of an order by external_id (WS confirm missed).

        Returns (verdict, response):
          ('filled', hl_response) — exchange shows filled_qty>0 → entry succeeded;
          ('no_fill', None)       — order reached the exchange and is TERMINAL with
                                    0 filled (IOC cancelled/rejected/expired) → a
                                    retry under a NEW external_id cannot double-fill;
          ('unknown', None)       — no order visible / lookup failed → the original
                                    place may still land late; retry ONLY under the
                                    SAME external_id so the exchange dedupes.
        """
        async def _q():
            r = await self._client.account.get_order_by_external_id(ext_id)
            return r.data or []
        try:
            orders = self._bridge.run(_q(), timeout=10) or []
        except Exception as q_err:
            log.warning("order-lookup(%s) failed: %s", ext_id, q_err)
            return "unknown", None
        filled = [o for o in orders if float(getattr(o, "filled_qty", 0) or 0) > 0]
        if filled:
            best = max(filled, key=lambda o: float(o.filled_qty))
            self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
            log.warning("order-lookup(%s): REST-confirmed FILL avg=%s qty=%s status=%s",
                        ext_id, best.average_price, best.filled_qty, best.status)
            return "filled", self._to_hl_response(best, kind="filled")
        if orders:
            statuses = {str(getattr(o, "status", "?")).rsplit(".", 1)[-1].upper() for o in orders}
            log.warning("order-lookup(%s): on exchange, 0 filled, statuses=%s", ext_id, statuses)
            # Only a fully terminal 0-fill set proves the external_id is consumed
            # and can never fill — anything else (NEW etc) stays 'unknown'.
            if statuses <= {"CANCELLED", "REJECTED", "EXPIRED"}:
                return "no_fill", None
        return "unknown", None

    def _poll_position_fill(self, coin: str, m, pre_size: float, qty, agg_price,
                            attempts: int = 10):
        """Poll positions (attempts × 2s) — recovery for a fill that never confirmed.

        Extended positions endpoint может отставать 5-10s после market order — на DOT
        2026-05-11 одна попытка через 2s упустила позицию, появившуюся 5 минут позже.
        Bug 2026-05-14: 50% threshold dropped partial fills <50% (BABA 9.8%, DOT etc),
        bot marked cancelled → sl_audit cron later restored with WRONG DB.size. Lower
        to MIN_FILL_RATIO (default 0.05); aligns with trader.py partial-fill abort gate.
        Returns HL-shape filled response, или None если позиция так и не появилась.
        """
        min_fr = float(os.environ.get("MIN_FILL_RATIO", "0.05"))
        for attempt in range(attempts):
            try:
                self.invalidate_positions_cache()
                time.sleep(2)
                positions = self.open_positions() or {}
                post = positions.get(coin) or positions.get(m.name)
                if post:
                    post_size = abs(float(post.get("szi", 0)))
                    delta = post_size - pre_size
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
        return None

    def market_open(self, coin: str, is_buy: bool, sz: float,
                    intended_px: float = 0.0, allow_marketable: bool = True) -> dict:
        """Market entry — IOC limit at aggressive price for fill guarantee.

        MONEY-SAFETY GUARD (2026-06-07): callers that mean to enter at a *level*
        (breakdown/breakout) should pass intended_px. If that level is on the wrong
        side of the book — i.e. it would fill IMMEDIATELY as a marketable order in the
        opposite direction of the intended trigger — this REJECTS unless the caller
        explicitly opts into a marketable fill (allow_marketable=True, e.g. a
        continuation entry whose price has already moved through the trigger). A
        breakdown SHORT (sell below) / breakout LONG (buy above) that has NOT yet moved
        must use trigger_entry() instead. Existing continuation callers pass no
        intended_px and keep the original (marketable) behaviour.
        """
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        # Guard: if an explicit entry level is given and we are NOT opting into a
        # marketable continuation fill, refuse an immediately-marketable entry.
        if intended_px and intended_px > 0 and not allow_marketable:
            self.assert_entry_marketability(coin, is_buy, float(intended_px))
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

        # Client-generated external_id — the queryable handle for REST-confirm and the
        # exchange-side dedupe key (duplicate external_id → reject, never a double fill).
        ext_id = f"mo-{m.name}-{time.time_ns()}"

        def _go(px, oid_ext):
            async def _inner():
                trader = await self._ensure_trader()
                r = await trader.create_and_place_order(
                    market_name=m.name,
                    amount_of_synthetic=qty,
                    price=px,
                    side=side,
                    taker_fee=taker_fee,
                    order_type=OrderType.MARKET,
                    time_in_force=TimeInForce.IOC,
                    external_id=oid_ext,
                )
                return r  # OpenOrderModel с average_price, filled_qty, id, external_id
            return _inner()

        # Snapshot pre-open positions (для recovery после timeout)
        pre_size = 0.0
        try:
            pre = (self.open_positions() or {}).get(coin) or (self.open_positions() or {}).get(m.name)
            if pre:
                pre_size = abs(float(pre.get("szi", 0)))
        except Exception:
            pass

        try:
            # 60s upper bound: первый trade BlockingTradingClient.create() делает WS
            # subscribe (~15-30s); place/confirm внутри теперь fail-fast (15s + 10s,
            # PATCH 2) — worst case ~55s < 60s bridge limit.
            opened = self._bridge.run(_go(agg_price, ext_id), timeout=60)
            self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
            return self._to_hl_response(opened, kind="filled")
        except Exception as e:
            err = str(e)[:200] or type(e).__name__
            # Lock-recovery retry (2026-05-15 audit): SDK x10 1.4.x periodically raises
            # asyncio "Lock is not acquired" — trader's internal state broken (likely
            # WS reconnect race). Re-creating BlockingTradingClient resolves. Audit
            # 2026-05-15 на Ext-15m нашёл 43 cancellations за 24h все от этой ошибки.
            # SAME ext_id on retry: if the first place actually landed, the exchange
            # rejects the duplicate instead of double-filling.
            if "lock is not acquired" in err.lower():
                log.warning("market_open(%s) Lock-error — recreating trader for retry", coin)
                self._recreate_trader()
                try:
                    opened = self._bridge.run(_go(agg_price, ext_id), timeout=60)
                    log.info("market_open(%s) Lock-recovery retry SUCCEEDED", coin)
                    self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
                    return self._to_hl_response(opened, kind="filled")
                except Exception as e2:
                    err = str(e2)[:200] or type(e2).__name__
                    log.warning("market_open(%s) Lock-recovery retry also failed: %s", coin, err)
            # WS-confirm-timeout class (root-caused 2026-07-02): the trader's WS
            # account-stream dies silently in a long-lived process → every entry in a
            # burst timed out serially (~50s each), fills LOST while the exchange was
            # healthy. Heal = recreate trader; truth = REST-confirm by external_id;
            # then ONE guarded retry on the fresh trader.
            if isinstance(e, ExtOrderConfirmTimeout) or "TimeoutError" in err:
                self._recreate_trader()
                verdict, resp = self._lookup_order_fill(ext_id)
                if verdict == "filled":
                    return resp
                if verdict == "unknown":
                    # Original place may still land late — position poll first, and
                    # retry ONLY under the SAME ext_id (exchange dedupes duplicates).
                    rec = self._poll_position_fill(coin, m, pre_size, qty, agg_price, attempts=10)
                    if rec:
                        return rec
                    retry_ext = ext_id
                else:  # 'no_fill': terminal 0-fill — ext_id consumed, new id is safe
                    retry_ext = ext_id + "-r2"
                try:
                    # Fresh mark for the retry — original agg_price is ~30-60s stale by
                    # now; a stale aggressive limit can sit on the wrong side of the book.
                    fresh_mark = self.mark_price(coin, ttl=0.0) or float(mp)
                    retry_agg = get_price_with_slippage(
                        side=side, price=Decimal(str(fresh_mark)),
                        min_price_change=m.trading_config.min_price_change,
                        slippage=slip,
                    )
                    log.warning("market_open(%s) RETRY on fresh trader: aggr=%s ext_id=%s",
                                coin, retry_agg, retry_ext)
                    opened = self._bridge.run(_go(retry_agg, retry_ext), timeout=60)
                    self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
                    return self._to_hl_response(opened, kind="filled")
                except Exception as e3:
                    err = str(e3)[:200] or type(e3).__name__
                    log.warning("market_open(%s) retry failed: %s — final REST-confirm", coin, err)
                    verdict3, resp3 = self._lookup_order_fill(retry_ext)
                    if verdict3 == "filled":
                        return resp3
                    rec = self._poll_position_fill(coin, m, pre_size, qty, agg_price, attempts=5)
                    if rec:
                        return rec
                log.error("market_open(%s) confirmed FAILED after REST-confirm + fresh-trader retry: %s — sl_audit cron defense will catch if order surfaces later", coin, err)
                return {
                    "status": "ok",
                    "response": {"type": "order", "data": {
                        "statuses": [{"error": err}]
                    }},
                }
            # Non-timeout errors: order МОГ быть placed — poll positions before failing.
            log.warning("market_open(%s) bridge err: %s — POST-TIMEOUT FALLBACK: querying positions",
                        coin, err)
            rec = self._poll_position_fill(coin, m, pre_size, qty, agg_price, attempts=10)
            if rec:
                return rec
            log.error("market_open(%s) confirmed FAILED after 20s polling (no position appeared): %s — sl_audit cron defense will catch if order surfaces later", coin, err)
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": err}]
                }},
            }

    def market_close(self, coin: str) -> dict:
        self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
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

        ext_id = f"mc-{m.name}-{time.time_ns()}"

        def _go(oid_ext):
            async def _inner():
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
                    external_id=oid_ext,
                )
            return _inner()

        try:
            # 40s: inner fail-fast place(15s)+ws(10s) may total 25s (PATCH 2) — the old
            # 20s bridge limit fired BEFORE the inner confirm path could finish.
            closed = self._bridge.run(_go(ext_id), timeout=40)
            return self._to_hl_response(closed, kind="filled")
        except Exception as e:
            err = str(e)[:200] or type(e).__name__
            # Same dead-trader class as market_open (WS-confirm timeout / Lock error):
            # heal + REST-confirm + one retry. reduce_only=True makes a duplicate close
            # harmless — it can never flip the position, only reduce toward flat.
            if (isinstance(e, ExtOrderConfirmTimeout) or "TimeoutError" in err
                    or "lock is not acquired" in err.lower()):
                log.warning("market_close(%s) %s — recreating trader + REST-confirm", coin, err)
                self._recreate_trader()
                verdict, resp = self._lookup_order_fill(ext_id)
                if verdict == "filled":
                    return resp
                retry_ext = ext_id if verdict == "unknown" else ext_id + "-r2"
                try:
                    closed = self._bridge.run(_go(retry_ext), timeout=40)
                    self.invalidate_positions_cache()  # cache_invalidate_fix_20260525
                    return self._to_hl_response(closed, kind="filled")
                except Exception as e2:
                    err = str(e2)[:200] or type(e2).__name__
                    verdict2, resp2 = self._lookup_order_fill(retry_ext)
                    if verdict2 == "filled":
                        return resp2
                    log.error("market_close(%s) retry failed: %s", coin, err)
            return {
                "status": "ok",
                "response": {"type": "order", "data": {
                    "statuses": [{"error": err}]
                }},
            }

    def trigger_sl(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
        """Position-wide SL через TPSL POSITION (не synthetic_amount-based).

        is_buy = направление CLOSE side (т.е. для long-position SL это is_buy=False).
        Validator (order_object.py:190-201) требует amount_of_synthetic=0, price=0,
        reduce_only=True для tp_sl_type=POSITION.
        """
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

    # ===== Entry marketability guard + stop-entry primitive =====
    # MONEY-SAFETY (2026-06-07): a breakdown/breakout ENTRY priced THROUGH the book
    # (SELL at/below best bid, BUY at/above best ask) is immediately marketable as a
    # plain LIMIT — it fills instantly at the current price instead of RESTING and
    # triggering on the move. Incident: manual BTC short 60704 sent while bid≈61526 →
    # filled at the top. A breakdown/breakout entry MUST be a trigger (stop) order, not
    # a marketable limit. These helpers make that the only correct path.

    def best_bid_ask(self, coin: str) -> tuple:
        """(best_bid, best_ask) floats from the live orderbook; (0.0, 0.0) on failure."""
        m = self._market(coin)

        async def _go():
            r = await self._client.markets_info.get_orderbook_snapshot(market_name=m.name)
            d = r.data
            bid = d.bid[0] if getattr(d, "bid", None) else None
            ask = d.ask[0] if getattr(d, "ask", None) else None
            b = float(getattr(bid, "price", 0) or 0) if bid is not None else 0.0
            a = float(getattr(ask, "price", 0) or 0) if ask is not None else 0.0
            return b, a

        try:
            return self._bridge.run(_go(), timeout=10)
        except Exception as e:
            log.warning("best_bid_ask(%s) failed: %s", coin, e)
            return 0.0, 0.0

    def assert_entry_marketability(self, coin: str, is_buy: bool, intended_px: float) -> None:
        """Raise if a LIMIT entry at intended_px would fill IMMEDIATELY (wrong side of book).

        SELL entry with intended_px <= best_bid  → instant fill  → must be a stop (trigger_entry).
        BUY  entry with intended_px >= best_ask  → instant fill  → must be a stop (trigger_entry).
        This is the class-guard for the manual-BTC-short incident (2026-06-07). Fail-closed:
        if the book is readable AND the order is marketable, REJECT loudly so the caller
        routes through trigger_entry(). If the book is unreadable we do not block (the
        downstream fill-readback assertion still catches a surprise fill).
        """
        if not intended_px or intended_px <= 0:
            return
        bid, ask = self.best_bid_ask(coin)
        if bid <= 0 and ask <= 0:
            log.warning("assert_entry_marketability(%s): orderbook unreadable — cannot verify", coin)
            return
        marketable = (is_buy and ask > 0 and intended_px >= ask) or \
                     ((not is_buy) and bid > 0 and intended_px <= bid)
        if marketable:
            raise ValueError(
                f"MARKETABILITY GUARD: {('BUY' if is_buy else 'SELL')} entry for {coin} at "
                f"{intended_px} is immediately marketable (bid={bid} ask={ask}); a plain LIMIT "
                f"would fill at the market instead of resting. Use trigger_entry() for a "
                f"breakdown/breakout stop entry."
            )

    def assert_resting(self, coin: str, expected_flat: bool = True) -> bool:
        """Read back AFTER placing an entry meant to REST: confirm no position appeared.

        Returns True if still flat (good — order is resting), False if a position
        materialised (a 'resting' entry filled immediately = the bug). Logs loudly on False.
        """
        try:
            self.invalidate_positions_cache()
            pos = (self.open_positions() or {})
            sz = 0.0
            for k in (coin, f"{coin}-USD"):
                if k in pos:
                    sz = abs(float(pos[k].get("szi", 0) or 0)); break
            if expected_flat and sz > 0:
                log.error(
                    "RESTING-ENTRY VIOLATION: %s shows szi=%.6f right after a 'resting' entry "
                    "placement — it FILLED instead of resting. Investigate immediately.", coin, sz,
                )
                return False
            return True
        except Exception as e:
            log.warning("assert_resting(%s) read-back failed: %s", coin, e)
            return True

    def trigger_entry(self, coin: str, is_buy: bool, sz: float, trigger_px: float,
                      execution: str = "limit", limit_cap_pct: float = None) -> dict:
        """RESTING stop-entry (CONDITIONAL) — opens a position only when price triggers.

        Breakout LONG  → is_buy=True,  direction=UP   (fires when price rises to trigger_px).
        Breakdown SHORT→ is_buy=False, direction=DOWN (fires when price falls to trigger_px).
        execution='limit' (capped) or 'market' on trigger. Mirrors trigger_sl but
        reduce_only=False and full size. Returns _to_hl_response(kind='resting').
        """
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        qty = self._round_qty(m, sz)
        trig_px = self._round_price_dec(m, trigger_px)
        direction = OrderTriggerDirection.UP if is_buy else OrderTriggerDirection.DOWN
        taker_fee = self._market_taker_fee(m.name)
        cap = Decimal(str(limit_cap_pct if limit_cap_pct is not None
                          else (self.settings.slippage or 0.01)))
        if execution == "market":
            exec_px_type = OrderPriceType.MARKET
            exec_price = trig_px
        else:
            exec_px_type = OrderPriceType.LIMIT
            # BUY fills up to trig*(1+cap); SELL down to trig*(1-cap) — bounded slippage.
            raw = trig_px * (Decimal(1) + cap) if is_buy else trig_px * (Decimal(1) - cap)
            from decimal import ROUND_UP as _RUP, ROUND_DOWN as _RDOWN
            exec_price = raw.quantize(m.trading_config.min_price_change,
                                      rounding=_RUP if is_buy else _RDOWN)
        from datetime import datetime, timedelta, timezone
        expire_at = datetime.now(timezone.utc) + timedelta(days=28)
        log.info("trigger_entry %s %s sz=%s trig=%s dir=%s exec=%s px=%s",
                 coin, side.value, qty, trig_px, direction, exec_px_type, exec_price)

        async def _go():
            order = create_order_object(
                account=self._stark_acc, market=m,
                starknet_domain=self._cfg.signing.starknet_domain,
                order_type=OrderType.CONDITIONAL, time_in_force=TimeInForce.GTT,
                expire_time=expire_at, side=side,
                amount_of_synthetic=qty, price=exec_price,
                reduce_only=False, taker_fee=taker_fee,
                trigger=OrderConditionalTriggerParam(
                    trigger_price=trig_px,
                    trigger_price_type=OrderTriggerPriceType.MARK,
                    direction=direction,
                    execution_price_type=exec_px_type,
                ),
            )
            return await self._client.orders.place_order(order)

        try:
            placed = self._bridge.run(_go(), timeout=15)
            self.invalidate_positions_cache()
            return self._to_hl_response(placed.data, kind="resting")
        except Exception as e:
            return {"status": "ok", "response": {"type": "order", "data": {
                "statuses": [{"error": str(e)[:200]}]}}}

    def limit_reduce_only(self, coin: str, is_buy: bool, sz: float, px: float,
                          post_only: bool = True) -> dict:
        """Resting reduce-only LIMIT — maker partial TP (cheaper than a market/trigger TP).

        is_buy = CLOSE side (long position → is_buy=False, sells at px ABOVE entry).
        post_only=True guarantees maker; on a 'would-be-marketable' post-only reject we
        retry as a plain GTT reduce-only limit. 28-day expiry avoids the 1h-TTL gotcha.

        BEST-EFFORT: the SL is already resting (placed first, with retry+emergency-close),
        so a failure here just means 'no partial booked' (full trail) — never a naked SL.
        Returns _to_hl_response(kind='resting') with oid, or {'statuses':[{'error':...}]}.
        """
        from datetime import datetime, timedelta, timezone
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        m = self._market(coin)
        qty = self._round_qty(m, sz)
        limit_px = self._round_price_dec(m, px, sl_side=side)
        expire_at = datetime.now(timezone.utc) + timedelta(days=28)

        def _build(po: bool):
            return create_order_object(
                account=self._stark_acc, market=m,
                starknet_domain=self._cfg.signing.starknet_domain,
                order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTT,
                expire_time=expire_at, side=side,
                amount_of_synthetic=qty, price=limit_px,
                post_only=po, reduce_only=True,
                taker_fee=self._market_taker_fee(m.name),
            )

        async def _go(po: bool):
            return await self._client.orders.place_order(_build(po))

        log.info("limit_reduce_only %s %s qty=%s px=%s post_only=%s",
                 coin, side.value, qty, limit_px, post_only)
        try:
            placed = self._bridge.run(_go(post_only), timeout=15)
            return self._to_hl_response(placed.data, kind="resting")
        except Exception as e:
            err = str(e)
            if post_only and any(k in err.lower() for k in ("post", "marketable", "would match")):
                log.warning("limit_reduce_only %s post-only rejected -> retry GTT: %s", coin, err[:120])
                try:
                    placed = self._bridge.run(_go(False), timeout=15)
                    return self._to_hl_response(placed.data, kind="resting")
                except Exception as e2:
                    err = str(e2)
            return {"status": "ok", "response": {"type": "order", "data": {
                "statuses": [{"error": err[:200]}]}}}

    def trigger_tp(self, coin: str, is_buy: bool, sz: float, trigger_px: float) -> dict:
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
        async def _go():
            return await self._client.orders.cancel_order(int(oid))
        try:
            self._bridge.run(_go(), timeout=10)
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)[:200]}

    def list_reduce_only_triggers(self) -> list[dict]:
        """ALL resting TP/SL trigger orders (OrderType.TPSL) across every market,
        normalized for the orphan_sweep. RAISES on persistent API failure so the
        sweep skips the cycle rather than act on a maybe-empty view. The market is
        read from `.market` (e.g. 'BNB-USD'); _variants() in orphan_sweep maps it to
        the bare coin. Cancel path: cancel_sl_order(coin, oid) -> orders.cancel_order."""
        async def _go():
            r = await self._client.account.get_open_orders(order_type=OrderType.TPSL)
            return r.data

        last_err = None
        for attempt in range(3):
            try:
                orders = self._bridge.run(_go(), timeout=10)
                break
            except Exception as e:
                last_err = e
                log.warning("list_reduce_only_triggers attempt %d/3 failed: %s", attempt + 1, e)
                time.sleep(0.5 * (attempt + 1))
        else:
            raise RuntimeError(
                f"list_reduce_only_triggers failed after 3 attempts: {last_err}"
            )
        out: list[dict] = []
        for o in orders:
            mkt = getattr(o, "market", None)
            oid = getattr(o, "id", None)
            if mkt is None or oid is None:
                continue
            out.append({
                "coin": mkt,
                "oid": oid,
                "reduce_only": bool(getattr(o, "reduce_only", True)),
                "is_trigger": True,
            })
        return out

    def list_open_sl_orders(self, coin: str) -> list:
        """XNN patch 2026-06-11 (canon §0.10): on persistent API failure RAISE, never
        return []. A silent [] here = false-naked: trader._sl_confirmed_live only treats
        a THROW as 'assume live' (trader.py) — an empty list reads as 'no SL on exchange'
        and triggers a duplicate-SL heal or even an emergency-close on a healthy position
        (anti '429→false-naked→дубль-SL/emergency-close' class)."""
        m = self._market(coin)

        async def _go():
            r = await self._client.account.get_open_orders(
                market_names=[m.name], order_type=OrderType.TPSL,
            )
            return [str(o.id) for o in r.data]

        last_err: Exception | None = None
        for attempt in range(3):
            try:
                return self._bridge.run(_go(), timeout=10)
            except Exception as e:
                last_err = e
                log.warning("list_open_sl_orders(%s) attempt %d/3 failed: %s", coin, attempt + 1, e)
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(
            f"list_open_sl_orders({coin}) failed after 3 attempts: {last_err}"
        )

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> Optional[dict]:
        # SDK 1.4.x не поддерживает isolated/cross flag — assume cross-only (verify testnet!)
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

    def position_liquidation(self, coin: str) -> Optional[dict]:
        """Live per-position liquidation reading for the SL-inside-liq guard (2026-06-11).

        Extended has NO standalone liquidation getter in the x10 SDK — liqPx is ONLY a
        field inside the PositionModel, already surfaced by open_positions() under the
        "liquidationPx" key (mapped at ~line 407, aliased under bare coin + "COIN-USD").
        We re-read it here and normalise to the adapter-neutral shape the trader's
        ensure_sl_inside_liq()/_sl_inside_liq_ok() expect:

            {"liq_px": float | None, "margin_mode": "cross"}

        margin_mode is ALWAYS "cross" — x10 SDK 1.4.x is cross-only by design (see
        update_leverage above: zero isolated/MarginMode support). liq_px may be None
        (SDK omits it on a cross account) → caller treats None as account-level-safe.
        Returns None only when there is no open position for `coin` (entry/fill race).
        """
        try:
            positions = self.open_positions()
        except Exception as e:
            log.warning("position_liquidation(%s): open_positions failed: %s", coin, e)
            return None
        entry = positions.get(coin) or positions.get(f"{coin}-USD")
        if entry is None:
            return None  # no open position → caller returns "no_position", SL unchanged
        liq_raw = entry.get("liquidationPx")
        liq_px = None
        if liq_raw is not None:
            try:
                liq_px = float(liq_raw)
                if liq_px <= 0:
                    liq_px = None
            except (TypeError, ValueError):
                liq_px = None
        return {"liq_px": liq_px, "margin_mode": "cross"}

    def add_isolated_margin(self, coin: str, usd: float) -> Optional[dict]:
        """REMEDY-A primitive — NOT IMPLEMENTABLE on Extended (honest no-op).

        x10 SDK 1.4.x exposes NO add/modify/isolated-margin endpoint (account methods are
        get/update_leverage, transfer, withdraw, asset_operations — none is a per-position
        isolated-margin top-up) AND the venue is cross-only, so there is no isolated margin
        to add. Returning None makes ensure_sl_inside_liq() fall straight through to
        REMEDY-B (clamp SL inside liqPx + LOUD warn), which is the only viable mechanism
        here. Kept as an explicit method so the call-closure matches the HL canon adapter.

        TODO(extended): if Extended ever ships an isolated mode + add-margin API, wire it
        here (SDK update_leverage-down is the only current lever to push liqPx away, but
        re-setting leverage mid-position has side effects and is NOT auto-applied).
        """
        log.warning(
            "add_isolated_margin(%s, $%.2f) — Extended is cross-only, NO add-margin API "
            "(REMEDY-A unavailable) → REMEDY-B clamp will handle it", coin, usd,
        )
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
                # SIG-PARITY/OID-FIRST 2026-07-02: f.id is the TRADE id; the parent order id is
                # what our DB stores as sl/tp order_id -- expose it for exit attribution.
                "order_id": getattr(f, "order_id", None),
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
