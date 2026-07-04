#!/usr/bin/env python3
"""HL naked-SL sentinel (cron */2). Protective reduce-only SL on positions genuinely
lacking one. Guards against 429 false-empty from list_open_sl_orders:
 (1) mass-naked skip: if >25% (or >4) positions look naked, it is an API/429 issue, not real -> skip.
 (2) 2-consecutive-run confirm before acting.
Backstop for unjournaled resting-fill orphans.

COMBO-FIX (2026-06-19): this is a STALE, un-ported copy that hardcoded a FOREIGN bot's
code path (/home/ubuntu/hl_bot_v2_dev) and a FOREIGN HL account (.env.a) — NOT the combo
bot's 0x100B unified account — and had NO DRY_RUN gate (it would place REAL reduce-only
SL orders). No cron/systemd references this copy (the active */2 cron points at
hl_bot_v2_dev/scripts/...), so it is INERT. To prevent it ever firing on the wrong
account if someone wired it, it is now FAIL-CLOSED: it imports the COMBO bot's own
modules (sys.path = this dir's parent) + loads the combo bot's .env/.env.combo, and
HARD-EXITS unless COMBO_SENTINEL_ENABLE=1 AND DRY_RUN=0 are BOTH explicitly set. Until
then it does nothing. The combo bot's real naked-SL protection lives in
trader.attempt_entry / manage_open_position (atomic-SL-or-emergency-close) + the per-tick
untracked-protect sweep, all DRY-gated independently of this file."""
import os, sys, json

# Combo bot root = parent of this scripts/ dir (NOT the foreign hl_bot_v2_dev).
_COMBO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _COMBO_ROOT)
from dotenv import load_dotenv
# Prefer the combo bot's .env.combo (DRY_RUN=1, combo account) then .env fallback.
for _envf in (".env.combo", ".env"):
    _p = os.path.join(_COMBO_ROOT, _envf)
    if os.path.exists(_p):
        load_dotenv(_p, override=False)

# FAIL-CLOSED guard: refuse to place ANY order unless BOTH switches are explicitly set.
# This file is un-vetted for the combo bot (lacks the us29 xyz_ foreign-exempt) — do not
# let a stray cron/manual run touch the live unified account.
if os.getenv("COMBO_SENTINEL_ENABLE", "0").strip() != "1":
    print("skip: combo naked_sl_sentinel DISABLED (set COMBO_SENTINEL_ENABLE=1 to arm); "
          "combo bot uses in-loop atomic-SL protection instead")
    sys.exit(0)
if os.getenv("DRY_RUN", "1").strip() != "0":
    print("skip: DRY_RUN!=0 — combo naked_sl_sentinel will not place real orders")
    sys.exit(0)

from bot.exchange_hl import HLClient
from bot.config import Settings, FX_EXCLUDE
from bot.trader import _place_sl_with_retry

# COMBO-FIX (2026-06-19): STATE was hardcoded to the SHARED path
# /home/ubuntu/hl_naked_sentinel_state.json — collides with the foreign hl_bot_v2_dev
# sentinel and the us29/xnn services (cross-bot state clobber: one bot's "naked-now" set
# overwrites another's, breaking the 2-consecutive-run confirm + persisting wrong coins).
# Scope it under the combo bot's OWN dir so each bot owns its sentinel state.
STATE = os.path.join(_COMBO_ROOT, "data", "naked_sentinel_state.json")
os.makedirs(os.path.dirname(STATE), exist_ok=True)
c = HLClient(Settings.from_env())
try:
    pos = c.open_positions() or {}
except Exception as e:
    print("skip: open_positions failed:", e); sys.exit(0)
active = {k: v for k, v in pos.items() if abs(float(v.get("szi", v.get("size", 0)) or 0)) > 0}
if not active:
    json.dump([], open(STATE, "w")); sys.exit(0)
prev = set()
try: prev = set(json.load(open(STATE)))
except Exception: pass
naked_now = set()
for coin, v in active.items():
    # FX-exempt (2026-06-07): never auto-SL excluded/forex coins (manual/unmanaged).
    base = coin.split("_", 1)[1] if "_" in coin else coin
    if coin in FX_EXCLUDE or base in FX_EXCLUDE:
        continue
    try: sls = c.list_open_sl_orders(coin)
    except Exception: sls = ["?"]
    if not sls: naked_now.add(coin)
# (1) mass-naked => API/429 issue, never real: skip without acting or updating state
if len(naked_now) > max(4, 0.25 * len(active)):
    print("skip: %d/%d look naked = API/429 issue, not acting" % (len(naked_now), len(active)))
    sys.exit(0)
# (2) act only on coins naked in BOTH this run and the previous run
acted = []
for coin in (naked_now & prev):
    v = active.get(coin)
    if not v: continue
    szi = float(v.get("szi", v.get("size", 0)) or 0)
    is_long = szi > 0
    mark = c.mark_price(coin)
    if not mark or mark <= 0: continue
    sl = mark * 0.94 if is_long else mark * 1.06
    oid = _place_sl_with_retry(c, coin, abs(szi), sl, side=("long" if is_long else "short"))
    if oid: acted.append([coin, oid])
json.dump(list(naked_now), open(STATE, "w"))
if acted: print("SENTINEL PROTECTED:", acted)
