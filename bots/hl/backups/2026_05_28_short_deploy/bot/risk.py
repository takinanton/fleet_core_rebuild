"""risk.py — position sizing, MM cap check, concurrent cap.

All constants from config.py which sources from .env or explicit defaults.
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
    size: float          # base asset units (e.g. 0.005 BTC)
    risk_dollars: float  # expected $ loss if SL hit
    notional: float      # size × entry_price


def compute_size(
    entry_price: float,
    sl_price: float,
    account_value: float,
    settings: Settings,
    sz_decimals: int = 4,
    liquidity_cap_notional: Optional[float] = None,
) -> Optional[SizeResult]:
    """Compute position size based on fixed-fractional risk.

    risk_dollars = account_value × risk_per_trade (1%)
    size = risk_dollars / (entry - sl)
    Capped by: leverage × equity, liquidity cap.

    Source: old bot risk.py compute_size, same formula.
    """
    sl_dist = entry_price - sl_price
    if sl_dist <= 0 or entry_price <= 0:
        log.warning("compute_size: invalid entry=%.6f sl=%.6f", entry_price, sl_price)
        return None

    risk_dollars = account_value * settings.risk_per_trade
    size_by_risk = risk_dollars / sl_dist

    # Max notional = equity × leverage
    max_notional = account_value * settings.leverage
    size_by_leverage = max_notional / entry_price

    size = min(size_by_risk, size_by_leverage)

    # Liquidity cap: user spec — max 1/20 of 1h volume
    if liquidity_cap_notional is not None and liquidity_cap_notional > 0:
        size_by_liquidity = liquidity_cap_notional / entry_price
        size = min(size, size_by_liquidity)

    # Floor to exchange increment
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
    """Check if opening a new position would breach mm_cap.

    mm_cap_pct = 0.50 → total margin used must not exceed 50% of equity.
    margin = notional / leverage (for fixed-leverage perps).

    Returns (allowed, reason).
    Source: user spec mm_cap=50% HARD (non-sweepable, memory feedback_mm_cap_50_only).
    """
    if account_value <= 0:
        return False, "account_value <= 0"

    # current margin used
    current_margin = current_positions_notional / leverage
    new_margin = new_notional / leverage
    total_margin = current_margin + new_margin
    margin_pct = total_margin / account_value

    if margin_pct > mm_cap_pct:
        return False, (
            f"mm_cap_breach: current_margin=${current_margin:.0f} + "
            f"new=${new_margin:.0f} = {margin_pct*100:.1f}% > {mm_cap_pct*100:.0f}%"
        )
    return True, f"mm_ok: {margin_pct*100:.1f}% used"


def check_concurrent_cap(
    n_open_positions: int,
    max_concurrent: int,
) -> tuple[bool, str]:
    """Check if max_concurrent positions would be exceeded.

    Source: user spec max_concurrent=5 combined across all TFs.
    """
    if n_open_positions >= max_concurrent:
        return False, f"concurrent_cap: {n_open_positions}/{max_concurrent} open"
    return True, f"concurrent_ok: {n_open_positions}/{max_concurrent}"
