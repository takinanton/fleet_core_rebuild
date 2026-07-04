# nado_bot_v2 DEPLOY CHECKLIST

Strategy: UK v102 ZigZag breakout (source: bt-1 /root/hl-backtest/strategies/uk_v*)
Exchange: Vertex perpetuals DEX on Ink L2 (Nado)
TFs: 4h + 1d simultaneously (Nado native, audit-corrected from 8h+1d 2026-05-24)
F1=2.5, F2=0, F3=0 (audit-corrected for 4h+1d: F1=2.5 z=+3.35*** vs F1=2.0)
Target VPS: <HOST> root@ | /root/nado_bot_v2/

---

## PRE-DEPLOY (do in order)

[ ] 1. COPY .env.example → .env, fill in real values:
        - NADO_LINKED_SIGNER_PRIVATE_KEY (from app.nado.xyz UI LocalStorage)
        - NADO_ACCOUNT_ADDRESS (main wallet)
        - NADO_SUBACCOUNT=default
        - RISK_PER_TRADE=0.005 (HALF RISK for first 2 weeks, NOT 0.01)
        - NETWORK=mainnet
        - TG_BOT_TOKEN= (empty = muted per fleet rule)

[ ] 2. Verify universe filter dropped FX:
        python -m bot.universe --print
        Expected: EUR/GBP/JPY symbols NOT in output, "FX check: clean"

[ ] 3. Run unit tests (no API calls needed):
        python tests/test_strategy_parity.py --unit-only
        Expected: "All unit tests PASSED"

[ ] 4. DRY-RUN for 24h minimum (no orders placed):
        python -m bot.main --dry-run
        Watch logs for:
          - "Universe loaded: N symbols"
          - Signal logs every 4h/24h bar boundary
          - No error tracebacks

[ ] 5. Verify Nado testnet (if available) — if Vertex/Nado exposes testnet:
        NETWORK=testnet python -m bot.main --dry-run --once

[ ] 6. Verify TIER 1/2 classification matches expectation:
        TIER 1: BTC-PERP, ETH-PERP, SOL-PERP, XRP-PERP (majors)
        TIER 2: LINK-PERP, AAPL-PERP, XAU-PERP, etc.
        NOT in universe: any EURUSD/GBPUSD/JPY symbol

[ ] 7. Sanity-check sizing before first real trade:
        With equity $10,000, RISK_PER_TRADE=0.005:
          risk_$ = $50
          SL dist typical 5% → size = $50/0.05 = $1,000 notional
          leverage 5x → $200 margin
          MM cap: $200/$10,000 = 2% < 50% cap ✓

---

## DEPLOY

[ ] 8. SCP files to VPS:
        scp -r /Users/ak/Desktop/HL/nado_bot_v2/ root@<HOST>:/root/nado_bot_v2/

[ ] 9. On VPS: set up venv:
        cd /root/nado_bot_v2
        python3 -m venv venv
        venv/bin/pip install -r requirements.txt

[ ] 10. Copy .env (never scp .env — manually create on VPS):
         ssh root@<HOST> 'nano /root/nado_bot_v2/.env'

[ ] 11. Install and enable systemd service:
         cp /root/nado_bot_v2/systemd/nado-bot-v2.service /etc/systemd/system/
         systemctl daemon-reload
         systemctl enable nado-bot-v2
         systemctl start nado-bot-v2

[ ] 12. Verify startup:
         systemctl status nado-bot-v2
         journalctl -u nado-bot-v2 -f
         Expected log: "nado_bot_v2 starting" → "Universe loaded" → no errors

---

## POST-DEPLOY MONITORING (first 2 weeks)

[ ] 13. Monitor FIRST 10 live trades manually:
         - Confirm STOP-LIMIT fills are at or below entry_limit_cap (0.25%)
         - Confirm SL trigger placed immediately after fill
         - Confirm no positions left open without SL
         - Check scripts/post_trade_audit.py output daily

[ ] 14. After 30 trades: run slip recalibration:
         # On bt-1 (backtest compute host, per memory rule)
         # Compare walk_slip_pct (from trades.db) vs realized fill price
         python scripts/post_trade_audit.py --days 30

[ ] 15. After 2 weeks (30+ trades): if avg R > 0 and max slip < 0.25%:
         Edit .env: RISK_PER_TRADE=0.01 (full risk)
         systemctl restart nado-bot-v2

[ ] 16. Weekly: run universe refresh (sets data/universe_tiers.json):
         python scripts/refresh_universe.py

---

## EMERGENCY PROCEDURES

If position stuck open WITHOUT SL:
  1. ssh root@<HOST>
  2. journalctl -u nado-bot-v2 --since "1h ago"
  3. Check app.nado.xyz UI for position state
  4. If bot can't auto-close: manually close on Nado UI
  5. Kill bot: systemctl stop nado-bot-v2
  6. Investigate journal for root cause BEFORE restart

---

## OPEN ASSUMPTIONS (verify before live)

A. Nado SDK `market_open` with slippage parameter controls limit price.
   If SDK ignores limit and always executes at market → entry becomes market order.
   CHECK: test with $1 position on mainnet, verify fill price vs order price.

B. `get_market_liquidity` depth parameter returns correct ask-side levels.
   Old bot used this API but did not verify depth walking.
   CHECK: print first 5 ask levels for BTC-PERP on startup, confirm reasonable spread.

C. 4h granularity Nado-native (verified live 2026-05-24).
   client.candles("BTC-PERP", "4h", limit=10) returns data via FOUR_HOURS enum.
   1d native too. Old 8h plan dropped — Nado API doesn't support 8h.

D. Isolated-only market flag for XAG-PERP (error_code=2122).
   Old bot handled this; XAG-PERP may be isolated-only.
   Current bot uses cross margin → if 2122 on XAG-PERP, it will be rejected
   and logged. Old bot had isolated fallback — add if needed.

E. Bar timing: 4h bars at 00:00/04:00/08:00/12:00/16:00/20:00 UTC, 1d at 00:00 UTC.
   Scanner checks prev_bar_start vs last_seen_bar — verify alignment on first day.

---

## CONSTANTS SOURCES (audit trail)

| Parameter | Value | Source |
|---|---|---|
| zigzag_raw_length | 5 | uk_v75_zigzag_raw.py default_config |
| raw_rr_target | 1.5 | uk_v75_zigzag_raw.py default_config |
| max_sl_4h | 5% | uk_v75_zigzag_raw.py default_config (4h native) |
| max_sl_1d | 10% | uk_v84_optimal.py TF_MAX_SL["1d"] |
| min_sl_dist_pct | 0.5% | uk_v75_zigzag_raw.py default_config |
| vstop_buffer_pct | 0.8% | uk_v78_tf_adaptive.py TF_VSTOP (same for 4h/1d) |
| vstop_pivot_window | 8 | uk_v78_tf_adaptive.py TF_VSTOP (same for 4h/1d) |
| trail_buffer | 0.3% | uk_v84_optimal.py default_config (1d only — 4h trail OFF) |
| max_run_r | 5.0 | uk_v84_optimal.py default_config |
| f1_threshold | 2.5 | MEMORY project_nado_uk_v102_8h_2026_05_24 (4h+1d audit, z=+3.35*** vs F1=2.0) |
| mm_cap | 50% | MEMORY feedback_mm_cap_50_only (non-sweepable) |
| max_concurrent | 5 | user spec |
| entry_limit_cap | 0.25% | user spec |
| entry_limit_ttl | 30s | user spec |
| min_1h_vol | $50k | user spec |
| max_spread | 0.15% | user spec |
| max_depth_slip | 0.30% | user spec + MEMORY feedback_nado_liquidity_first |
| max_notional_pct_of_vol | 5% | user spec (1/20 of 1h vol) |
| min_fill_ratio | 10% | user spec |
| leverage | 5x | user spec (conservative, not max) |
| taker_fee | ~0.05%/side | nado-protocol SDK docs + old bot _fee_rt_estimate |
