"""config.py — load .env + strategy constants for extended_bot_v2 + pacifica_bot_v2 + hl_bot_v2.

Exchange dispatch: settings.exchange in {"extended","pacifica","hyperliquid"} chooses adapter
(see bot/main.py + bot/liquidity_snapshot.py).

All numeric constants have explicit sources:
  - Extended auth fields: legacy exchange_extended.py __init__ (settings.extended_*)
  - Pacifica auth fields: legacy exchange_pacifica.py __init__ + user spec 2026-05-25
  - HL auth fields: HYPERLIQUID_ACCOUNT_ADDRESS + HYPERLIQUID_AGENT_PRIVATE_KEY
  - Strategy constants: uk_v102_ib_filtered.py + audit-corrected params (MEMORY)
  - Per-TF F1/F2/F3: .env TF_<TF>_F1/F2/F3 — sweep-determined per acct/TF
  - HL fees: maker 1.5bps taker 4.5bps (tier-0, source: HL docs 2026-05-26)
  - HL funding: hourly (source: HL docs + old exchange.py history)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(_ENV_PATH)


def _get(key: str, default: str = "", required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(f"Required env var not set: {key}")
    return val or ""


def _get_float(key: str, default: float) -> float:
    v = os.getenv(key)
    return float(v) if v else default


def _get_int(key: str, default: int) -> int:
    v = os.getenv(key)
    return int(v) if v else default


def _get_bool(key: str, default: bool) -> bool:
    v = os.getenv(key, "").strip().lower()
    if not v:
        return default
    return v in ("true", "1", "yes")


# Supported TFs and millisecond durations
TF_MS: dict[str, int] = {
    "1m":   60_000,
    "5m":   300_000,
    "15m":  900_000,
    "30m":  1_800_000,
    "1h":   3_600_000,
    "2h":   7_200_000,
    "4h":   14_400_000,
    "8h":   28_800_000,
    "1d":   86_400_000,
    "1w":   604_800_000,
}

CANDLES_LIMIT: int = 300


@dataclass(frozen=True)
class PerTFFilters:
    """Per-TF F1/F2/F3 filter overrides."""
    f1: float
    f2: float
    f3: float


def _load_per_tf_filters(tf: str, global_f1: float, global_f2: float, global_f3: float) -> PerTFFilters:
    tf_key = tf.upper()
    f1 = _get_float(f"TF_{tf_key}_F1", global_f1)
    f2 = _get_float(f"TF_{tf_key}_F2", global_f2)
    f3 = _get_float(f"TF_{tf_key}_F3", global_f3)
    return PerTFFilters(f1=f1, f2=f2, f3=f3)


@dataclass(frozen=True)
class PerTFShortFilters:
    """Per-TF SHORT F1/F2/F3 overrides (f2 = MAX RSI for short — oversold conf)."""
    f1: float
    f2: float
    f3: float


def _load_per_tf_short_filters(tf: str, gf1: float, gf2: float, gf3: float) -> PerTFShortFilters:
    tf_key = tf.upper()
    return PerTFShortFilters(
        f1=_get_float(f"TF_{tf_key}_SHORT_F1", gf1),
        f2=_get_float(f"TF_{tf_key}_SHORT_F2", gf2),
        f3=_get_float(f"TF_{tf_key}_SHORT_F3", gf3),
    )


@dataclass(frozen=True)
class Settings:
    # === Exchange dispatch ===
    exchange: str                      # "extended" | "pacifica" — chooses adapter class

    # === Exchange — Extended (Starknet) ===
    # Required only when exchange="extended".
    network: str                       # "mainnet" | "testnet"
    extended_api_key: str
    extended_stark_public: str
    extended_stark_private: str
    extended_vault_id: str
    extended_account_id: str
    extended_eth_address: str

    # === Exchange — Pacifica (Solana) ===
    # Required only when exchange="pacifica".
    pacifica_private_key: str          # base58 — main wallet (fallback to agent if no agent)
    pacifica_agent_private_key: str    # base58 — agent wallet (signs requests on behalf of main)
    pacifica_account_address: str      # base58 — main wallet pubkey

    # === Exchange — Hyperliquid (EVM) ===
    # Required only when exchange="hyperliquid".
    agent_private_key: str             # hex — Agent wallet private key (NOT main wallet key)
    account_address: str               # hex — main wallet address (MetaMask/0x...)

    # === Dry-run mode ===
    dry_run: bool                      # DRY_RUN=1 → log signals, no actual orders

    # === Risk ===
    risk_per_trade: float
    leverage: int
    mm_cap_pct: float
    max_concurrent: int
    max_opens_per_day: int             # NEW — per user spec for Pacifica deploy (2/day cap)

    # === Entry — STOP-LIMIT cap + TTL ===
    entry_limit_cap_pct: float
    entry_limit_ttl_sec: int
    slippage: float

    # === ZigZag / strategy ===
    zigzag_raw_length: int
    raw_rr_target: float
    require_ema50_up: bool
    require_ema50_down: bool            # symmetric trend filter for SHORT

    # === SL bounds per TF ===
    tf_max_sl: Dict[str, float]
    min_sl_dist_pct: float

    # === Trail-after-TP ===
    enable_trail_after_tp: bool
    trail_after_tp_buffer_pct: float
    trail_pivot_window: int
    max_run_r: float

    # === vstop ===
    vstop_buffer_pct: float
    vstop_pivot_window: int
    vstop_wick_check: bool

    # === Global F1/F2/F3 (LONG) ===
    f1_min_dist_ema20_atr: float
    f2_min_rsi14: float
    f3_max_dollar_vol_usd: float
    per_tf_filters: Dict[str, PerTFFilters]

    # === Global F1/F2/F3 (SHORT) ===
    short_f1_min_dist_ema20_atr: float
    short_f2_max_rsi14: float
    short_f3_max_dollar_vol_usd: float
    per_tf_short_filters: Dict[str, PerTFShortFilters]
    short_enabled_tfs: tuple           # subset of TFs where SHORT scanning is on

    # === Universe filter ===
    universe_min_vol_usd_24h: float
    universe_top_n: int
    universe_refresh_min: int

    # === Liquidity snapshot ===
    liq_size_cap_pct: float
    liq_min_trade_usd: float
    liq_snapshot_path: str
    liq_snapshot_max_age_hours: float
    liveness_min_pct_traded: float

    # === Fill quality ===
    min_fill_ratio: float

    # === Loop ===
    loop_interval_sec: int
    bar_age_max_sec: int
    per_tf_bar_age_sec: Dict[str, int]

    # === Notifier ===
    tg_bot_token: str
    tg_chat_id: str

    # === Working TFs for this instance ===
    working_tfs: tuple

    @classmethod
    def from_env(cls) -> "Settings":
        exchange = _get("EXCHANGE", "extended").lower()
        network = _get("NETWORK", "mainnet").lower()

        global_f1 = _get_float("F1_MIN_DIST_EMA20_ATR", 2.5)
        global_f2 = _get_float("F2_MIN_RSI14", 0.0)
        global_f3 = _get_float("F3_MAX_DOLLAR_VOL_USD", 0.0)

        # SHORT global defaults (bt-1 winning short configs: F1=3.0; F2/F3 per-TF override).
        short_global_f1 = _get_float("SHORT_F1_MIN_DIST_EMA20_ATR", 3.0)
        short_global_f2 = _get_float("SHORT_F2_MAX_RSI14", 0.0)
        short_global_f3 = _get_float("SHORT_F3_MAX_DOLLAR_VOL_USD", 0.0)

        working_tfs = tuple(
            t.strip() for t in _get("WORKING_TFS", "4h,1d").split(",") if t.strip()
        )
        short_enabled_tfs = tuple(
            t.strip() for t in _get("SHORT_TFS", "").split(",") if t.strip()
        )
        # Per-TF maps cover the UNION so SHORT can scan TFs where LONG is off (and vice versa).
        _all_cfg_tfs: list = []
        for _t in list(working_tfs) + list(short_enabled_tfs):
            if _t not in _all_cfg_tfs:
                _all_cfg_tfs.append(_t)

        # 2026-05-28: per-TF filters for the ACTUAL configured TFs (was hardcoded
        # → new TFs 15m/30m/1w silently used global F1 instead of TF_<X>_F*).
        per_tf_filters: Dict[str, PerTFFilters] = {}
        per_tf_short_filters: Dict[str, PerTFShortFilters] = {}
        for tf in _all_cfg_tfs:
            per_tf_filters[tf] = _load_per_tf_filters(tf, global_f1, global_f2, global_f3)
            per_tf_short_filters[tf] = _load_per_tf_short_filters(
                tf, short_global_f1, short_global_f2, short_global_f3
            )

        _max_sl_defaults = {
            "15m": 0.02, "30m": 0.025, "1h": 0.03, "2h": 0.04,
            "4h": 0.05, "8h": 0.07, "1d": 0.10, "1w": 0.15,
        }
        tf_max_sl = {
            tf: _get_float(f"MAX_SL_{tf.upper()}", _max_sl_defaults.get(tf, 0.05))
            for tf in _all_cfg_tfs
        }

        _bar_age_by_tf = {
            "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200,
            "4h": 14400, "8h": 28800, "1d": 43200, "1w": 604800,
        }
        per_tf_bar_age_sec = {
            tf: _get_int(f"BAR_AGE_{tf.upper()}_SEC", _bar_age_by_tf.get(tf, 14400))
            for tf in _all_cfg_tfs
        }
        default_bar_age = max(
            (_bar_age_by_tf.get(tf, 14400) for tf in _all_cfg_tfs),
            default=14400,
        )
        bar_age_max_sec = _get_int("BAR_AGE_MAX_SEC", default_bar_age)

        entry_limit_cap_pct = _get_float("ENTRY_LIMIT_CAP_PCT", 0.0025)

        # Required env vars depend on chosen exchange — only the active adapter's
        # creds are mandatory (so a Pacifica .env doesn't need to populate
        # EXTENDED_* placeholders and vice versa).
        if exchange == "extended":
            ext_required = True
            pac_required = False
            hl_required = False
        elif exchange == "pacifica":
            ext_required = False
            pac_required = True
            hl_required = False
        elif exchange == "hyperliquid":
            ext_required = False
            pac_required = False
            hl_required = True
        else:
            raise RuntimeError(f"Unknown EXCHANGE={exchange!r}; expected 'extended', 'pacifica', or 'hyperliquid'")

        return cls(
            exchange=exchange,
            network=network,

            extended_api_key=_get("EXTENDED_API_KEY", required=ext_required),
            extended_stark_public=_get("EXTENDED_STARK_PUBLIC", required=ext_required),
            extended_stark_private=_get("EXTENDED_STARK_PRIVATE", required=ext_required),
            extended_vault_id=_get("EXTENDED_VAULT_ID", required=ext_required),
            extended_account_id=_get("EXTENDED_ACCOUNT_ID", ""),
            extended_eth_address=_get("EXTENDED_ETH_ADDRESS", ""),

            pacifica_private_key=_get("PACIFICA_PRIVATE_KEY", required=pac_required),
            pacifica_agent_private_key=_get("PACIFICA_AGENT_PRIVATE_KEY", ""),
            pacifica_account_address=_get("PACIFICA_ACCOUNT_ADDRESS", required=pac_required),

            agent_private_key=_get("HYPERLIQUID_AGENT_PRIVATE_KEY", required=hl_required),
            account_address=_get("HYPERLIQUID_ACCOUNT_ADDRESS", required=hl_required),

            dry_run=_get_bool("DRY_RUN", False),

            risk_per_trade=_get_float("RISK_PER_TRADE", 0.005),
            leverage=_get_int("LEVERAGE", 5),
            mm_cap_pct=_get_float("MM_CAP_PCT", 0.50),
            max_concurrent=_get_int("MAX_CONCURRENT", 5),
            max_opens_per_day=_get_int("MAX_OPENS_PER_DAY", 0),  # 0 = unlimited

            entry_limit_cap_pct=entry_limit_cap_pct,
            entry_limit_ttl_sec=_get_int("ENTRY_LIMIT_TTL_SEC", 30),
            slippage=entry_limit_cap_pct,

            zigzag_raw_length=_get_int("ZIGZAG_RAW_LENGTH", 5),
            raw_rr_target=_get_float("RAW_RR_TARGET", 1.5),
            require_ema50_up=_get_bool("REQUIRE_EMA50_UP", True),
            require_ema50_down=_get_bool("REQUIRE_EMA50_DOWN", True),

            tf_max_sl=tf_max_sl,
            min_sl_dist_pct=_get_float("MIN_SL_DIST_PCT", 0.005),

            enable_trail_after_tp=_get_bool("ENABLE_TRAIL_AFTER_TP", True),
            trail_after_tp_buffer_pct=_get_float("TRAIL_AFTER_TP_BUFFER_PCT", 0.003),
            trail_pivot_window=_get_int("TRAIL_PIVOT_WINDOW", 5),
            max_run_r=_get_float("MAX_RUN_R", 5.0),

            vstop_buffer_pct=_get_float("VSTOP_BUFFER_PCT", 0.008),
            vstop_pivot_window=_get_int("VSTOP_PIVOT_WINDOW", 8),
            vstop_wick_check=_get_bool("VSTOP_WICK_CHECK", True),

            f1_min_dist_ema20_atr=global_f1,
            f2_min_rsi14=global_f2,
            f3_max_dollar_vol_usd=global_f3,
            per_tf_filters=per_tf_filters,

            short_f1_min_dist_ema20_atr=short_global_f1,
            short_f2_max_rsi14=short_global_f2,
            short_f3_max_dollar_vol_usd=short_global_f3,
            per_tf_short_filters=per_tf_short_filters,
            short_enabled_tfs=short_enabled_tfs,

            universe_min_vol_usd_24h=_get_float("UNIVERSE_MIN_VOL_USD_24H", 1_000_000.0),
            universe_top_n=_get_int("UNIVERSE_TOP_N", 30),
            universe_refresh_min=_get_int("UNIVERSE_REFRESH_MIN", 60),
            liq_size_cap_pct=_get_float("LIQ_SIZE_CAP_PCT", 0.05),
            liq_min_trade_usd=_get_float("LIQ_MIN_TRADE_USD", 20.0),
            liq_snapshot_path=_get("LIQ_SNAPSHOT_PATH", "data/liquidity_snapshot.json"),
            liq_snapshot_max_age_hours=_get_float("LIQ_SNAPSHOT_MAX_AGE_HOURS", 30.0),
            liveness_min_pct_traded=_get_float("LIVENESS_MIN_PCT_TRADED", 0.95),
            min_fill_ratio=_get_float("MIN_FILL_RATIO", 0.10),

            loop_interval_sec=_get_int("LOOP_INTERVAL_SEC", 60),
            bar_age_max_sec=bar_age_max_sec,
            per_tf_bar_age_sec=per_tf_bar_age_sec,

            tg_bot_token=_get("TG_BOT_TOKEN", ""),
            tg_chat_id=_get("TG_CHAT_ID", ""),

            working_tfs=working_tfs,
        )

    def bar_age_gate_for(self, tf: str) -> int:
        return self.per_tf_bar_age_sec.get(tf, self.bar_age_max_sec)

    def get_tf_filters(self, tf: str) -> PerTFFilters:
        return self.per_tf_filters.get(
            tf,
            PerTFFilters(
                f1=self.f1_min_dist_ema20_atr,
                f2=self.f2_min_rsi14,
                f3=self.f3_max_dollar_vol_usd,
            )
        )

    def get_tf_short_filters(self, tf: str) -> PerTFShortFilters:
        return self.per_tf_short_filters.get(
            tf,
            PerTFShortFilters(
                f1=self.short_f1_min_dist_ema20_atr,
                f2=self.short_f2_max_rsi14,
                f3=self.short_f3_max_dollar_vol_usd,
            )
        )

    def short_enabled_for(self, tf: str) -> bool:
        return tf in self.short_enabled_tfs


# Module-level singleton
settings: Settings = Settings.from_env()

DB_PATH: Path = PROJECT_ROOT / "data" / "trades.db"

FX_EXCLUDE: frozenset[str] = frozenset({
    "EUR-USD", "GBP-USD", "JPY-USD", "CHF-USD",
    "EURUSD", "GBPUSD", "USDJPY",
    "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD",
})
