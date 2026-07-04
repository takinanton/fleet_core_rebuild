#!/usr/bin/env python3
"""Fire-once breakout watcher for a manual LONG.

Polls mark_price; when mark >= ENTRY, does a single market_open(buy) + native
trigger_sl, then exits. Idempotent: a /tmp flag and an exchange-position check
both guard against a double open if the process is restarted. Uses only the
bot client's already-tested primitives (mark_price / open_positions /
market_open / trigger_sl). Logs JSON lines to stdout (journald).

Run (from the bot repo dir, with its venv):
  python3 manual_gold_watcher.py --client-module bot.exchange_pacifica \
    --client-class PacificaClient --coin XAU --entry 4519.4 --sl 4423.6 \
    --notional 2000
Add --check for a dry probe (mark + planned size, no order).
"""
from __future__ import annotations
import sys, os, json, time, argparse, importlib

os.environ.setdefault("HL_WS_CANDLES", "false")

ap = argparse.ArgumentParser()
ap.add_argument("--client-module", required=True)
ap.add_argument("--client-class", required=True)
ap.add_argument("--coin", required=True)
ap.add_argument("--entry", type=float, required=True)
ap.add_argument("--sl", type=float, required=True)
ap.add_argument("--notional", type=float, required=True)
ap.add_argument("--poll", type=float, default=4.0)
ap.add_argument("--max-hours", type=float, default=336.0)  # 14 days
ap.add_argument("--check", action="store_true")
a = ap.parse_args()

sys.path.insert(0, ".")
from bot.config import Settings

m = importlib.import_module(a.client_module)
Client = getattr(m, a.client_class)
client = Client(Settings.from_env())

FLAG = f"/tmp/mgold_{a.coin.replace('/', '_').replace('-', '_')}.fired"


def log(msg, **kw):
    print(json.dumps({"wt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      "coin": a.coin, "msg": msg, **kw}, default=str), flush=True)


def get_pos():
    try:
        p = client.open_positions() or {}
    except Exception as e:
        log("pos_err", e=str(e)[:140]); return None
    for k in (a.coin, a.coin.replace("-PERP", ""), a.coin.split("-")[0]):
        if k in p:
            return p[k]
    return None


def pos_size(pos, fallback):
    if not isinstance(pos, dict):
        return fallback
    for key in ("szi", "size", "sz", "position", "net_size", "amount"):
        v = pos.get(key)
        if v not in (None, ""):
            try:
                return abs(float(v))
            except (TypeError, ValueError):
                pass
    return fallback


def round_sz(sz):
    try:
        return float(client.round_qty(a.coin, sz))
    except Exception:
        pass
    try:
        am = client.asset(a.coin)
        return round(sz, int(getattr(am, "sz_decimals", 3)))
    except Exception:
        return round(sz, 3)


def has_sl():
    try:
        return bool(client.list_open_sl_orders(a.coin))
    except Exception:
        return False  # unknown -> let caller place


def place_sl(sz):
    if has_sl():
        log("sl_already_present"); return
    try:
        r = client.trigger_sl(a.coin, is_buy=False, sz=sz, trigger_px=a.sl)
        log("sl_placed", size=sz, resp=str(r)[:220])
    except Exception as e:
        log("sl_err", e=str(e)[:220])


try:
    mark0 = client.mark_price(a.coin)
except Exception as e:
    mark0 = None
    log("mark0_err", e=str(e)[:140])

pos0 = get_pos()
log("start", entry=a.entry, sl=a.sl, notional=a.notional, mark=mark0,
    in_pos=bool(pos0), flag=os.path.exists(FLAG))

if a.check:
    sz = round_sz(a.notional / (mark0 or a.entry))
    log("check_only", planned_size=sz,
        would_fire=(mark0 is not None and mark0 >= a.entry))
    os._exit(0)

# Already fired or already holding -> only ensure SL, never re-open.
if os.path.exists(FLAG) or pos0:
    sz = pos_size(pos0, round_sz(a.notional / (mark0 or a.entry)))
    log("already_active_ensure_sl", size=sz)
    place_sl(round_sz(sz))
    os._exit(0)

deadline = time.time() + a.max_hours * 3600
while time.time() < deadline:
    try:
        mark = client.mark_price(a.coin)
        if mark and mark >= a.entry:
            if get_pos():
                log("pos_appeared_skip_open"); break
            sz = round_sz(a.notional / mark)
            log("breakout", mark=mark, size=sz)
            r = client.market_open(a.coin, is_buy=True, sz=sz)
            open(FLAG, "w").write(json.dumps({"size": sz, "ts": time.time()}))
            log("opened", resp=str(r)[:220])
            time.sleep(2.5)
            filled = pos_size(get_pos(), sz)
            place_sl(round_sz(filled))
            log("done", filled=filled)
            break
        time.sleep(a.poll)
    except Exception as e:
        log("loop_err", e=str(e)[:200]); time.sleep(5)
else:
    log("deadline_no_fill")

os._exit(0)
