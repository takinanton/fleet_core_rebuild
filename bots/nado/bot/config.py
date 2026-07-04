"""config.py — load .env + strategy constants for nado_bot_v2.
BIDIRECTIONAL (long + short) since 2026-05-27.

SHORT additions:
  - Per-TF SHORT filters: TF_<TF>_SHORT_F1, TF_<TF>_SHORT_F2, TF_<TF>_SHORT_F3
  - SHORT_TFS env var (subset of WORKING_TFS where short is enabled)
  - require_ema50_down: bool
  - Per-TF LONG filters added too (matches Paci/Ext interface so shared scanner works)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

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


@dataclass(frozen=True)
class PerTFFilters:
    f1: float
    f2: float
    f3: float


@dataclass(frozen=True)
class PerTFShortFilters:
    f1: float
    f2: float   # max RSI for short (oversold)
    f3: float


def _load_per_tf_filters(tf: str, gf1: float, gf2: float, gf3: float) -> PerTFFilters:
    tf_key = tf.upper()
    return PerTFFilters(
        f1=_get_float(f"TF_{tf_key}_F1", gf1),
        f2=_get_float(f"TF_{tf_key}_F2", gf2),
        f3=_get_float(f"TF_{tf_key}_F3", gf3),
    )


def _load_per_tf_short_filters(tf: str, gf1: float, gf2: float, gf3: float) -> PerTFShortFilters:
    tf_key = tf.upper()
    return PerTFShortFilters(
        f1=_get_float(f"TF_{tf_key}_SHORT_F1", gf1),
        f2=_get_float(f"TF_{tf_key}_SHORT_F2", gf2),
        f3=_get_float(f"TF_{tf_key}_SHORT_F3", gf3),
    )


@dataclass(frozen=True)
class Settings:
    # Nado / Vertex (Ink L2)
    network: str
    agent_private_key: str
    account_address: str
    nado_subaccount: str

    # Mode (XNN port 2026-06-11): DRY_RUN env — systemd ExecStart has no --dry-run flag,
    # so dry mode MUST be settable via EnvironmentFile. Read by main.py (entry gate +
    # adopt/reconcile skip) and trader.py (_dry_block CODE-level order gates).
    dry_run: bool

    # Risk
    risk_per_trade: float
    leverage: int
    mm_cap_pct: float
    max_concurrent: int                # SHARED long + short
    liq_sl_buffer_pct: float           # SL must sit INSIDE liq by this frac (2026-06-11)

    # Entry
    entry_limit_cap_pct: float
    entry_limit_ttl_sec: int

    # Strategy
    zigzag_raw_length: int
    raw_rr_target: float
    require_ema50_up: bool
    require_ema50_down: bool           # NEW — symmetric for short

    # SL bounds
    tf_max_sl: Dict[str, float]
    min_sl_dist_pct: float

    # Trail
    enable_trail_after_tp: bool
    trail_after_tp_buffer_pct: float
    trail_pivot_window: int
    max_run_r: float
    tp1_partial_frac: float

    # vstop
    vstop_buffer_pct: float
    vstop_pivot_window: int
    vstop_wick_check: bool

    # LONG F1/F2/F3
    f1_min_dist_ema20_atr: float
    f2_min_rsi14: float
    f3_max_dollar_vol_usd: float
    per_tf_filters: Dict[str, PerTFFilters]

    # SHORT F1/F2/F3
    short_f1_min_dist_ema20_atr: float
    short_f2_max_rsi14: float
    short_f3_max_dollar_vol_usd: float
    per_tf_short_filters: Dict[str, PerTFShortFilters]
    short_enabled_tfs: tuple

    # Liquidity
    liq_size_cap_pct: float
    liq_min_trade_usd: float
    liq_snapshot_path: str
    liq_snapshot_max_age_hours: float

    # Universe
    universe_min_vol_usd_24h: float
    liveness_min_pct_traded: float
    min_fill_ratio: float

    # Loop
    loop_interval_sec: int
    bar_age_max_sec: int
    per_tf_bar_age_sec: Dict[str, int]

    # TG
    tg_bot_token: str
    tg_chat_id: str

    # TFs
    working_tfs: tuple
    slippage: float

    @classmethod
    def from_env(cls) -> "Settings":
        gf1 = _get_float("F1_MIN_DIST_EMA20_ATR", 2.5)
        gf2 = _get_float("F2_MIN_RSI14", 0.0)
        gf3 = _get_float("F3_MAX_DOLLAR_VOL_USD", 0.0)
        # SHORT defaults from bt-1 winning configs (F1=3.0, F2/F3=0 disabled until per-TF override)
        gsf1 = _get_float("SHORT_F1_MIN_DIST_EMA20_ATR", 3.0)
        gsf2 = _get_float("SHORT_F2_MAX_RSI14", 0.0)
        gsf3 = _get_float("SHORT_F3_MAX_DOLLAR_VOL_USD", 0.0)

        # 2026-05-28: parse working/short TFs up-front so per-TF filters load
        # for the ACTUAL configured set (was hardcoded ("1h","2h","4h","1d") →
        # new TFs 30m/8h/1w silently used global F1/F2/F3 instead of TF_<X>_F*).
        _working_tfs = tuple(
            t.strip() for t in _get("WORKING_TFS", "4h,1d").split(",") if t.strip()
        )
        short_enabled_tfs = tuple(
            t.strip() for t in _get("SHORT_TFS", "").split(",") if t.strip()
        )
        _all_cfg_tfs = []
        for _t in list(_working_tfs) + list(short_enabled_tfs):
            if _t not in _all_cfg_tfs:
                _all_cfg_tfs.append(_t)

        per_tf_filters: Dict[str, PerTFFilters] = {}
        per_tf_short_filters: Dict[str, PerTFShortFilters] = {}
        for tf in _all_cfg_tfs:
            per_tf_filters[tf] = _load_per_tf_filters(tf, gf1, gf2, gf3)
            per_tf_short_filters[tf] = _load_per_tf_short_filters(tf, gsf1, gsf2, gsf3)

        return cls(
            network=_get("NETWORK", "mainnet").lower(),
            agent_private_key=_get("NADO_LINKED_SIGNER_PRIVATE_KEY", required=True),
            account_address=_get("NADO_ACCOUNT_ADDRESS", required=True),
            nado_subaccount=_get("NADO_SUBACCOUNT", "default"),

            dry_run=_get_bool("DRY_RUN", False),

            risk_per_trade=_get_float("RISK_PER_TRADE", 0.01),
            leverage=_get_int("LEVERAGE", 5),
            mm_cap_pct=_get_float("MM_CAP_PCT", 0.50),
            max_concurrent=_get_int("MAX_CONCURRENT", 5),
            liq_sl_buffer_pct=_get_float("LIQ_SL_BUFFER_PCT", 0.02),

            entry_limit_cap_pct=_get_float("ENTRY_LIMIT_CAP_PCT", 0.0025),
            entry_limit_ttl_sec=_get_int("ENTRY_LIMIT_TTL_SEC", 30),

            zigzag_raw_length=_get_int("ZIGZAG_RAW_LENGTH", 5),
            raw_rr_target=_get_float("RAW_RR_TARGET", 1.5),
            require_ema50_up=_get_bool("REQUIRE_EMA50_UP", True),
            require_ema50_down=_get_bool("REQUIRE_EMA50_DOWN", True),

            tf_max_sl={
                tf: _get_float(
                    f"MAX_SL_{tf.upper()}",
                    {"30m": 0.025, "1h": 0.03, "2h": 0.04, "4h": 0.05,
                     "8h": 0.07, "1d": 0.10, "1w": 0.15}.get(tf, 0.05),
                )
                for tf in _all_cfg_tfs
            },
            min_sl_dist_pct=_get_float("MIN_SL_DIST_PCT", 0.005),

            enable_trail_after_tp=_get_bool("ENABLE_TRAIL_AFTER_TP", True),
            trail_after_tp_buffer_pct=_get_float("TRAIL_AFTER_TP_BUFFER_PCT", 0.003),
            trail_pivot_window=_get_int("TRAIL_PIVOT_WINDOW", 5),
            max_run_r=_get_float("MAX_RUN_R", 5.0),
            tp1_partial_frac=_get_float("TP1_PARTIAL_FRAC", 0.5),

            vstop_buffer_pct=_get_float("VSTOP_BUFFER_PCT", 0.008),
            vstop_pivot_window=_get_int("VSTOP_PIVOT_WINDOW", 8),
            vstop_wick_check=_get_bool("VSTOP_WICK_CHECK", True),

            f1_min_dist_ema20_atr=gf1,
            f2_min_rsi14=gf2,
            f3_max_dollar_vol_usd=gf3,
            per_tf_filters=per_tf_filters,

            short_f1_min_dist_ema20_atr=gsf1,
            short_f2_max_rsi14=gsf2,
            short_f3_max_dollar_vol_usd=gsf3,
            per_tf_short_filters=per_tf_short_filters,
            short_enabled_tfs=short_enabled_tfs,

            liq_size_cap_pct=_get_float("LIQ_SIZE_CAP_PCT", 0.05),
            liq_min_trade_usd=_get_float("LIQ_MIN_TRADE_USD", 20.0),
            liq_snapshot_path=_get("LIQ_SNAPSHOT_PATH", "data/liquidity_snapshot.json"),
            liq_snapshot_max_age_hours=_get_float("LIQ_SNAPSHOT_MAX_AGE_HOURS", 30.0),

            universe_min_vol_usd_24h=_get_float("UNIVERSE_MIN_VOL_USD_24H", 500_000.0),
            liveness_min_pct_traded=_get_float("LIVENESS_MIN_PCT_TRADED", 0.95),
            min_fill_ratio=_get_float("MIN_FILL_RATIO", 0.10),

            loop_interval_sec=_get_int("LOOP_INTERVAL_SEC", 60),
            bar_age_max_sec=_get_int("BAR_AGE_MAX_SEC", 7200),
            per_tf_bar_age_sec={
                tf: _get_int(
                    f"BAR_AGE_{tf.upper()}_SEC",
                    {"30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
                     "8h": 28800, "1d": 43200, "1w": 604800}.get(tf, 14400),
                )
                for tf in _all_cfg_tfs
            },

            tg_bot_token=_get("TG_BOT_TOKEN", ""),
            tg_chat_id=_get("TG_CHAT_ID", ""),

            working_tfs=_working_tfs,
            slippage=_get_float("ENTRY_LIMIT_CAP_PCT", 0.0025),
        )

    def bar_age_gate_for(self, tf: str) -> int:
        return self.per_tf_bar_age_sec.get(tf, self.bar_age_max_sec)

    def get_tf_filters(self, tf: str) -> PerTFFilters:
        return self.per_tf_filters.get(
            tf,
            PerTFFilters(f1=self.f1_min_dist_ema20_atr, f2=self.f2_min_rsi14, f3=self.f3_max_dollar_vol_usd),
        )

    def get_tf_short_filters(self, tf: str) -> PerTFShortFilters:
        return self.per_tf_short_filters.get(
            tf,
            PerTFShortFilters(
                f1=self.short_f1_min_dist_ema20_atr,
                f2=self.short_f2_max_rsi14,
                f3=self.short_f3_max_dollar_vol_usd,
            ),
        )

    def short_enabled_for(self, tf: str) -> bool:
        return tf in self.short_enabled_tfs


settings: Settings = Settings.from_env()

DB_PATH: Path = PROJECT_ROOT / "data" / "trades.db"

TF_MS: dict[str, int] = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
    "1d": 86_400_000, "1w": 604_800_000,
}

CANDLES_LIMIT: int = 300

FX_EXCLUDE: frozenset[str] = frozenset({
    # XNN audit fix 2026-06-11: dropped UK-SIG-PAUSE leftovers (ONDO/NEAR/BNB) that
    # were copied from the live uk_v102 .env-era config — they silently narrowed the
    # XNN universe vs canon (these coins trade on the HL canon deploy). Coin-level
    # pauses belong in UNIVERSE_SYMBOL_EXCLUDE (env), not in the FX constant.
    "EUR-PERP", "GBP-PERP", "JPY-PERP", "CHF-PERP",
    "EURUSD-PERP", "GBPUSD-PERP", "USDJPY-PERP",
    "EURGBP-PERP", "EURJPY-PERP", "AUDUSD-PERP",
    "NZDUSD-PERP", "USDCAD-PERP", "USDCHF-PERP",
})
