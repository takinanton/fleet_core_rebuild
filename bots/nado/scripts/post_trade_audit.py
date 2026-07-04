"""post_trade_audit.py — daily slip audit: compare planned vs realized entry.

Reads trades.db, for each trade within last 24h computes:
  realized_slip = (actual_entry - trigger_price) / trigger_price
  vs planned cap = entry_limit_cap_pct (0.25%)

Output: table + warning if avg realized slip > cap.

Run: python scripts/post_trade_audit.py [--days N]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trades.db"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    since_iso = since.isoformat()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    trades = con.execute(
        "SELECT * FROM trades WHERE opened_at > ? ORDER BY opened_at",
        (since_iso,),
    ).fetchall()
    con.close()

    if not trades:
        print(f"No trades in last {args.days} day(s).")
        return

    print(f"\n{'#':<4} {'Coin':<15} {'TF':<5} {'Entry':<12} {'SL':<12} {'Exit':<12} "
          f"{'R':<7} {'Status':<10} {'Walk slip%':<12}")
    print("-" * 95)

    total_r = 0.0
    n_closed = 0

    for t in trades:
        r = t["realized_r"]
        r_str = f"{r:+.2f}R" if r is not None else "open"
        slip_str = f"{t['walk_slip_pct']*100:.3f}%" if t['walk_slip_pct'] is not None else "N/A"
        exit_str = f"{t['exit_price']:.4f}" if t['exit_price'] is not None else "open"
        print(
            f"{t['id']:<4} {t['coin']:<15} {t['tf']:<5} "
            f"{t['entry']:<12.6f} {t['sl_initial']:<12.6f} {exit_str:<12} "
            f"{r_str:<7} {t['status']:<10} {slip_str:<12}"
        )
        if r is not None:
            total_r += r
            n_closed += 1

    print("-" * 95)
    print(f"\nTrades: {len(trades)} ({n_closed} closed)")
    if n_closed > 0:
        print(f"Avg R (closed): {total_r/n_closed:+.2f}R  |  Total R: {total_r:+.2f}R")

    # Slip audit
    slips = [t['walk_slip_pct'] * 100 for t in trades if t['walk_slip_pct'] is not None]
    if slips:
        avg_slip = sum(slips) / len(slips)
        max_slip = max(slips)
        print(f"\nSlip audit: avg={avg_slip:.3f}% max={max_slip:.3f}% (cap=0.25%)")
        if avg_slip > 0.25:
            print("WARNING: avg realized slip exceeds 0.25% cap — review liquidity gates")
        if max_slip > 0.30:
            print(f"WARNING: max slip {max_slip:.3f}% > 0.30% threshold — check specific trade")


if __name__ == "__main__":
    main()
