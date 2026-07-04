#!/usr/bin/env python3
"""uk_signal_place.py — manual signal executor (entry near-market, capped by live
slip-band depth) + reduce-only TP + reduce-only SL. Modeled on the live
manual_sol_watcher_nado.py / manual_gold_watcher.py pattern, but parameterized
and with a --dry-run probe.

Venue-capability honesty (verified in clients 2026-06-07):
  * Nado / Pacifica: NO native maker limit. Entry = market_open (with the client's
    built-in slippage_percent guard). TP = limit_reduce_only(), which on these venues
    is a *reduce-only price-trigger / stop at TP* (taker-on-fire) — the same primitive
    the live SOL watcher uses as its TP. SL = trigger_sl().
  * Extended: limit_reduce_only() IS a resting reduce-only maker LIMIT (true limit TP).
    Entry = market_open (IOC-limit). SL = trigger_sl().

Slippage control = qty capped to live no-slip depth inside [mid*(1-SLIP_CAP), touch]
on the BID side for a SELL (short). The book is RE-FETCHED here at fire time; we do
NOT trust any stale number. leg_notional <= measured_depth is asserted.

Idempotency: /tmp/<flag> + exchange position check; never double-open.
Never writes trades.db. JSON lines to stdout.

Usage:
  ./venv/bin/python3 uk_signal_place.py --client-module bot.exchange_nado \
    --client-class NadoClient --coin JUP-PERP --side sell \
    --slip-cap 0.0015 --tp 0.1425 --sl 0.1609 --margin-cap 35000 [--dry-run]
"""
from __future__ import annotations
import sys, os, json, time, argparse, importlib

os.environ.setdefault("HL_WS_CANDLES", "false")

ap = argparse.ArgumentParser()
ap.add_argument("--client-module", required=True)
ap.add_argument("--client-class", required=True)
ap.add_argument("--coin", required=True)
ap.add_argument("--side", choices=["sell", "buy"], required=True)
ap.add_argument("--slip-cap", type=float, default=0.0015)   # 0.15%
ap.add_argument("--tp", type=float, required=True)
ap.add_argument("--sl", type=float, default=0.0)
ap.add_argument("--margin-cap", type=float, default=1e12)   # $ cap from margin
ap.add_argument("--flag", default="")
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()

sys.path.insert(0, ".")
from bot.config import settings as _settings  # noqa: E402

m = importlib.import_module(a.client_module)
Client = getattr(m, a.client_class)
client = Client(_settings)

IS_BUY = (a.side == "buy")            # entry side
CLOSE_IS_BUY = (not IS_BUY)           # reduce-only close side (short → buy to close)
FLAG = a.flag or f"/tmp/mjup_{a.coin.replace('/', '_').replace('-', '_')}.fired"


def log(msg, **kw):
    print(json.dumps({"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      "coin": a.coin, "msg": msg, **kw}, default=str), flush=True)


# ---------- book fetch (venue-specific) ----------
def fetch_bids_asks():
    """Return (bids, asks) as sorted lists of (px, sz). bids desc, asks asc."""
    mod = a.client_module
    if "nado" in mod:
        liq = client._sdk.context.engine_client.get_market_liquidity(
            product_id=client._pid(a.coin), depth=50)
        bids = [(int(p) / 1e18, int(s) / 1e18) for p, s in liq.bids]
        asks = [(int(p) / 1e18, int(s) / 1e18) for p, s in liq.asks]
    elif "pacifica" in mod:
        if hasattr(client, "orderbook_snapshot"):
            b, ak = client.orderbook_snapshot(a.coin)
            bids = [(float(x[0]), float(x[1])) for x in b]
            asks = [(float(x[0]), float(x[1])) for x in ak]
        else:
            import urllib.request
            d = json.load(urllib.request.urlopen(
                f"https://api.pacifica.fi/api/v1/book?symbol={a.coin}", timeout=15))["data"]
            bids = [(float(x["p"]), float(x["a"])) for x in d["l"][0]]
            asks = [(float(x["p"]), float(x["a"])) for x in d["l"][1]]
    elif "extended" in mod:
        import urllib.request
        d = json.load(urllib.request.urlopen(
            f"https://api.starknet.extended.exchange/api/v1/info/markets/{a.coin}/orderbook",
            timeout=15))["data"]
        bids = [(float(x["price"]), float(x["qty"])) for x in d["bid"]]
        asks = [(float(x["price"]), float(x["qty"])) for x in d["ask"]]
    else:
        raise RuntimeError(f"no book adapter for {mod}")
    bids.sort(key=lambda x: -x[0])
    asks.sort(key=lambda x: x[0])
    return bids, asks


def band_depth(bids, asks):
    """For a SELL (short) we sell into BIDS. cap_px = mid*(1-slip). depth = sum over
    bid levels with px >= cap_px. For a BUY we buy into ASKS, cap_px = mid*(1+slip)."""
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    if IS_BUY:
        cap_px = mid * (1 + a.slip_cap)
        levels = [(p, s) for p, s in asks if p <= cap_px]
        touch = best_ask
    else:
        cap_px = mid * (1 - a.slip_cap)
        levels = [(p, s) for p, s in bids if p >= cap_px]
        touch = best_bid
    depth_usd = sum(p * s for p, s in levels)
    qty = sum(s for p, s in levels)
    spread_pct = (best_ask - best_bid) / mid * 100.0
    return dict(best_bid=best_bid, best_ask=best_ask, mid=mid, cap_px=cap_px,
                touch=touch, depth_usd=depth_usd, depth_qty=qty,
                spread_pct=spread_pct, n_levels=len(levels))


def get_pos():
    try:
        p = client.open_positions() or {}
    except Exception as e:
        log("pos_err", e=str(e)[:160]); return None
    for k in (a.coin, a.coin.replace("-PERP", ""), a.coin.replace("-USD", ""),
              a.coin.split("-")[0]):
        if k in p:
            return p[k]
    return None


def pos_size(pos):
    if not isinstance(pos, dict):
        return 0.0
    for key in ("szi", "size", "sz", "position", "net_size", "amount"):
        v = pos.get(key)
        if v not in (None, ""):
            try:
                return abs(float(v))
            except (TypeError, ValueError):
                pass
    return 0.0


def liq_clamped_sl(filled_sz):
    """Ensure SL is inside liquidation. For short (CLOSE_IS_BUY) SL must be < liqPx."""
    sl = a.sl
    if not sl:
        return None
    try:
        if hasattr(client, "position_liquidation"):
            li = client.position_liquidation(a.coin) or {}
            liq = float(li.get("liquidationPx") or li.get("liq_px") or 0) or None
            if liq:
                if CLOSE_IS_BUY and sl >= liq:           # short
                    sl = liq * 0.99
                    log("sl_clamped", liq=liq, new_sl=sl)
                elif (not CLOSE_IS_BUY) and sl <= liq:   # long
                    sl = liq * 1.01
                    log("sl_clamped", liq=liq, new_sl=sl)
    except Exception as e:
        log("liq_check_skip", e=str(e)[:120])
    return sl


# ---------------- main ----------------
bids, asks = fetch_bids_asks()
band = band_depth(bids, asks)
try:
    mark = client.mark_price(a.coin, ttl=0)
except TypeError:
    mark = client.mark_price(a.coin)
except Exception as e:
    mark = None
    log("mark_err", e=str(e)[:120])

leg_notional = min(band["depth_usd"], a.margin_cap)
entry_ref = band["touch"]
qty = leg_notional / entry_ref if entry_ref else 0.0

pos0 = get_pos()
already = pos_size(pos0)

log("measure", mark=mark, **{k: round(v, 6) if isinstance(v, float) else v
                             for k, v in band.items()},
    margin_cap=a.margin_cap, leg_notional=round(leg_notional, 2),
    entry_ref=entry_ref, planned_qty=round(qty, 4),
    already_pos=already, flag_exists=os.path.exists(FLAG))

# Hard invariant: never size above measured in-band depth.
assert leg_notional <= band["depth_usd"] + 1e-6, \
    f"leg_notional {leg_notional} > measured_depth {band['depth_usd']}"

if band["spread_pct"] / 100.0 > a.slip_cap and band["n_levels"] == 0:
    log("spread_gt_cap_no_takeable", spread_pct=band["spread_pct"])

if a.dry_run:
    log("DRY_RUN", side=a.side, qty=round(qty, 4), entry_ref=entry_ref,
        floor=round(band["cap_px"], 6), tp=a.tp, sl=a.sl,
        tp_reduce_only=True, sl_reduce_only=True,
        note="entry=market_open(depth-capped qty); TP=limit_reduce_only; SL=trigger_sl")
    os._exit(0)

# Idempotency: never double-open.
if os.path.exists(FLAG) or already > 0:
    log("already_active_skip_open", already=already, flag=os.path.exists(FLAG))
    os._exit(0)

if qty <= 0:
    log("zero_qty_skip", reason="no in-band depth / spread>cap")
    os._exit(0)

# 1) ENTRY — market_open with depth-capped qty.
res = client.market_open(a.coin, is_buy=IS_BUY, sz=qty)
log("open_result", resp=str(res)[:400])

# Parse filled.
filled_sz = 0.0
avg_px = mark or entry_ref
try:
    statuses = res.get("response", {}).get("data", {}).get("statuses", [{}])
    f = statuses[0].get("filled") if statuses else None
    if f:
        filled_sz = float(f.get("totalSz") or f.get("filled") or 0)
        avg_px = float(f.get("avgPx") or avg_px)
except Exception as e:
    log("parse_fill_err", e=str(e)[:160])

if filled_sz <= 0:
    # re-read position as ground truth
    p = get_pos()
    filled_sz = pos_size(p)
    log("fill_from_pos", filled=filled_sz)

if filled_sz <= 0:
    log("OPEN_FAILED_NO_FILL", resp=str(res)[:300])
    os._exit(1)

open(FLAG, "w").write(str(time.time()))
log("FILLED", filled=filled_sz, avg_px=avg_px)

# 2) TP — reduce-only (resting LIMIT on Extended; reduce-only trigger on Nado/Pacifica).
tp_oid = None
try:
    tp = client.limit_reduce_only(a.coin, is_buy=CLOSE_IS_BUY, sz=filled_sz, px=a.tp)
    st = tp.get("response", {}).get("data", {}).get("statuses", [{}]) if isinstance(tp, dict) else []
    tp_oid = (st[0] if st else None)
    log("TP_placed", tp_px=a.tp, qty=filled_sz, resp=str(tp)[:300])
except Exception as e:
    log("TP_ERR", e=str(e)[:240])

# 3) SL — reduce-only stop, inside liquidation.
sl_oid = None
sl_use = liq_clamped_sl(filled_sz)
if sl_use:
    try:
        sl = client.trigger_sl(a.coin, is_buy=CLOSE_IS_BUY, sz=filled_sz, trigger_px=sl_use)
        st = sl.get("response", {}).get("data", {}).get("statuses", [{}]) if isinstance(sl, dict) else []
        sl_oid = (st[0] if st else None)
        log("SL_placed", sl_px=sl_use, qty=filled_sz, resp=str(sl)[:300])
    except Exception as e:
        log("SL_ERR", e=str(e)[:240])

log("DONE", filled=filled_sz, avg_px=avg_px, tp_oid=str(tp_oid)[:60],
    sl_oid=str(sl_oid)[:60])
print(json.dumps({"RESULT": {"filled": filled_sz, "avg_px": avg_px,
                             "tp_oid": str(tp_oid), "sl_oid": str(sl_oid)}}, default=str))
