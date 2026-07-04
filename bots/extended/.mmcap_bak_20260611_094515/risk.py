"""risk.py — position sizing, MM cap check, concurrent cap.
BIDIRECTIONAL (long + short) since 2026-05-27 — sl_dist uses abs() so caller passes
entry+sl regardless of side.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

from bot.config import Settings

log = logging.getLogger(__name__)


@dataclass
class SizeResult:
    size: float
    risk_dollars: float
    notional: float


def compute_size(
    entry_price: float,
    sl_price: float,
    account_value: float,
    settings: Settings,
    sz_decimals: int = 4,
    liquidity_cap_notional: Optional[float] = None,
    leverage_eff: Optional[int] = None,
) -> Optional[SizeResult]:
    """Compute position size based on fixed-fractional risk.

    SIDE-AGNOSTIC: uses abs(entry - sl), so long (sl<entry) AND short (sl>entry) both work.
    leverage_eff (XNN patch 2026-06-11, canon §0.8): per-asset effective leverage
    min(LEVERAGE, asset.max_leverage) actually set on the exchange — caps notional with
    the REAL leverage, not the .env ceiling. None = legacy settings.leverage.
    """
    sl_dist = abs(entry_price - sl_price)
    if sl_dist <= 0 or entry_price <= 0:
        log.warning("compute_size: invalid entry=%.6f sl=%.6f", entry_price, sl_price)
        return None

    risk_dollars = account_value * settings.risk_per_trade
    size_by_risk = risk_dollars / sl_dist

    lev = leverage_eff if (leverage_eff is not None and leverage_eff > 0) else settings.leverage
    max_notional = account_value * lev
    size_by_leverage = max_notional / entry_price

    size = min(size_by_risk, size_by_leverage)

    if liquidity_cap_notional is not None and liquidity_cap_notional > 0:
        size_by_liquidity = liquidity_cap_notional / entry_price
        size = min(size, size_by_liquidity)

    size = _floor_to_decimals(size, sz_decimals)
    if size <= 0:
        log.debug("compute_size: size rounds to 0 (risk=$%.2f sl_dist=%.6f)", risk_dollars, sl_dist)
        return None

    notional = size * entry_price
    actual_risk = size * sl_dist
    return SizeResult(size=size, risk_dollars=actual_risk, notional=notional)


def _floor_to_decimals(value: float, decimals: int) -> float:
    if value <= 0:
        return 0.0
    factor = 10 ** max(0, decimals)
    return math.floor(value * factor) / factor


def check_mm_cap(
    new_notional: float,
    current_positions_notional: float,
    account_value: float,
    leverage: int,
    mm_cap_pct: float,
) -> tuple[bool, str]:
    if account_value <= 0:
        return False, "account_value <= 0"
    current_margin = current_positions_notional / leverage
    new_margin = new_notional / leverage
    total_margin = current_margin + new_margin
    margin_pct = total_margin / account_value
    if margin_pct > mm_cap_pct:
        return False, (
            f"mm_cap_breach: current=${current_margin:.0f} + new=${new_margin:.0f} "
            f"= {margin_pct*100:.1f}% > {mm_cap_pct*100:.0f}%"
        )
    return True, f"mm_ok: {margin_pct*100:.1f}%"


def check_concurrent_cap(
    n_open_positions: int,
    max_concurrent: int,
) -> tuple[bool, str]:
    """SHARED long+short cap (Q1 user 2026-05-27: вместе под cap=5)."""
    if n_open_positions >= max_concurrent:
        return False, f"concurrent_cap: {n_open_positions}/{max_concurrent} open (long+short shared)"
    return True, f"concurrent_ok: {n_open_positions}/{max_concurrent}"
