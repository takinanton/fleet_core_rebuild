"""snapshot_spread_nado_helper.py — runs ON nado-bot, dumps L2 top-of-book.

Why a helper script: the Nado SDK requires a signer (private key) at client
init. Rather than ship the key to bt-1, we run this helper on nado-bot
(where the bot already lives) and have bt-1's snapshot_spread.py SSH out
to invoke it. Output is line-delimited TSV piped back over SSH.

Deploy:
  scp this file to nado-bot:/root/nado_bot_v2/scripts/

Invocation (from bt-1):
  ssh nado-bot /root/nado_bot_v2/venv/bin/python /root/nado_bot_v2/scripts/snapshot_spread_nado_helper.py

Output (stdout):
  ts_ms<TAB>coin<TAB>bid<TAB>ask
  1779886000000<TAB>BTC-PERP<TAB>75800.5<TAB>75801.0
  ...

Error lines go to stderr (skipped by parent).
"""

from __future__ import annotations

import sys
import time

X18 = 1e18

try:
    import os
    sys.path.insert(0, "/root/nado_bot_v2")
    from bot.exchange_nado import NadoClient_ as NadoClientWrapper  # uses Settings + private key
    from bot.config import settings
except Exception as e:
    print(f"# init error: {e}", file=sys.stderr)
    sys.exit(2)


def main() -> int:
    now_ms = int(time.time() * 1000)
    try:
        client = NadoClientWrapper(settings)
    except Exception as e:
        print(f"# client init failed: {e}", file=sys.stderr)
        return 3

    sdk = client._sdk
    # NadoClient_ already populates these via _load_markets() during __init__,
    # so reuse the same canonical symbol↔pid map the live bot uses (no risk of
    # divergence between snapshot and trading-time symbol resolution).
    pid_by_symbol = {
        sym: pid for sym, pid in client._symbol_to_pid.items()
        if sym.endswith("-PERP")
    }
    if not pid_by_symbol:
        # Fallback — call SDK directly if internal cache is empty
        try:
            symbols = sdk.market.get_all_product_symbols()
            for s in symbols:
                if s.symbol.endswith("-PERP"):
                    pid_by_symbol[s.symbol] = int(s.product_id)
        except Exception as e:
            print(f"# get_all_product_symbols failed: {e}", file=sys.stderr)
            return 4

    n_ok = n_err = 0
    for sym, pid in pid_by_symbol.items():
        try:
            liq = sdk.market.get_market_liquidity(pid, depth=5)
            bids = liq.bids or []
            asks = liq.asks or []
            if not bids or not asks:
                n_err += 1
                continue
            best_bid = max(float(b[0]) / X18 for b in bids)
            best_ask = min(float(a[0]) / X18 for a in asks)
            if best_ask <= best_bid or best_bid <= 0:
                n_err += 1
                continue
            print(f"{now_ms}\t{sym}\t{best_bid}\t{best_ask}")
            n_ok += 1
        except Exception as e:
            print(f"# {sym}: {e}", file=sys.stderr)
            n_err += 1

    print(f"# done: {n_ok} ok, {n_err} err", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
