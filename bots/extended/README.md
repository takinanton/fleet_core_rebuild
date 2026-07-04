# nado_bot_v2 — UK v102 ZigZag on Vertex/Nado (Ink L2)

## What this bot does

Trades UK v102 strategy (ZigZag pivot breakout + ATR-momentum gate F1=2.5) on Vertex perpetuals DEX (Ink L2, brand: Nado). Scans 4h and 1d timeframes simultaneously (Nado native TFs). Long-only. Liquidity-first entry methodology with STOP-LIMIT entries.

## Strategy

- Entry: ZigZag pivot high breakout (close > last pivot high → limit buy at trigger + tick)
- Filter: F1 = (close - ema20) / atr14 >= 2.5 (audit-corrected 2026-05-24)
- SL: last pivot low - tick (structure-based, not fixed %)
- TP1: entry + 1.5R
  - On 1d: trail SL after TP1 hit (matches uk_v84 TRAIL_TP_TFS)
  - On 4h: exit at TP1 fixed (4h trail HURT in bt-1, kept off)
- Exit: vstop trail via recent swing lows (both TFs)
- Expected backtest: Calmar 1092, MaxDD 5.9%, avgR +0.626, win 62.6%, ~77 trades/year

## Files

```
bot/config.py            — settings loader from .env
bot/exchange_nado.py     — Vertex SDK wrapper (reused from old bot verbatim)
bot/universe.py          — load Nado perps, filter FX, tier classify
bot/liquidity.py         — depth-walk slip check, spread gate, vol gate
bot/strategy_uk_v102.py  — ZigZag pivot detection, F1 filter, trail logic
bot/risk.py              — position sizing, MM cap (50% hard)
bot/scanner.py           — multi-TF scanner, cross-TF dedup
bot/trader.py            — stop-limit entry, fill polling, SL placement
bot/journal.py           — SQLite trades + rejected_signals log
bot/main.py              — main loop (60s), --dry-run, --once flags
scripts/refresh_universe.py   — weekly: re-rank symbols by live liquidity
scripts/post_trade_audit.py   — daily: slip audit vs plan
tests/test_strategy_parity.py — unit tests (no API calls needed)
DEPLOY_CHECKLIST.md      — step-by-step deploy + open assumptions
```

## Deploy

See DEPLOY_CHECKLIST.md for full procedure.

Quick version:
```bash
# On VPS (<HOST>)
scp -r nado_bot_v2/ root@<HOST>:/root/
ssh root@<HOST>
cd /root/nado_bot_v2
python3 -m venv venv && venv/bin/pip install -r requirements.txt
nano .env  # fill in NADO_LINKED_SIGNER_PRIVATE_KEY and NADO_ACCOUNT_ADDRESS
cp systemd/nado-bot-v2.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable nado-bot-v2 && systemctl start nado-bot-v2
journalctl -u nado-bot-v2 -f
```

## Daily ops

```bash
# Check status
journalctl -u nado-bot-v2 --since "24h ago" | tail -100

# Check trades
sqlite3 /root/nado_bot_v2/data/trades.db "SELECT coin, tf, entry, sl_initial, status, realized_r FROM trades ORDER BY opened_at DESC LIMIT 20"

# Slip audit
cd /root/nado_bot_v2 && venv/bin/python scripts/post_trade_audit.py

# Refresh universe (weekly)
venv/bin/python scripts/refresh_universe.py
```
