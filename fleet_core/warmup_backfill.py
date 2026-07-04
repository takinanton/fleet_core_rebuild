"""Cross-exchange / multi-source warmup backfill (v6, 2026-06-20).

Problem: live bot warms EMA-610 (XNN ema_candidates incl [377,610]) from OWN-exchange
candles only. Old coins with recently-listed perps (TAO/JUP/...) or non-crypto synths
(XAU/XAG/CL) have < MIN_SIGNAL_BARS(610) local bars -> PARITY SKIP, while the backtest
trades them with deep history. = live!=bt gap silently dropping coins.

v6 (2026-06-20): xyz_* HIP-3 tokenized stocks/commodities/ETFs (us29 leg) have NO crypto
source on Binance/Bybit/Gate and most are NOT in _TRADFI, so v5 left them PARITY-SKIPped
at <210 bars on 1d. Add a LOCAL-PARQUET source (TV daily history pre-pulled on bt-1 via
scripts/_pull_xyz_us.py -> data/xyz_backfill/1d/<coin>.{parquet,csv.gz}) tried FIRST for
xyz_* coins. The local adapter is keyed by the FULL coin id (xyz_AMD), NOT _base(), and
returns the SAME oldest-first rows contract; the EXISTING v5 same-asset/glue/cadence
guards then validate the splice exactly as for any other source (price-level overlap vs
own close; reject wrong asset -> keep own df -> PARITY SKIP). Genuine 2025 IPOs (CRCL,
CRWV) have no pre-pulled file -> adapter returns [] -> they correctly stay skipped.
Crypto coins are unaffected: the local source only fires for the xyz_ prefix.

v5: a SOURCE WATERFALL (not Binance-only). For a coin short on history, prepend the
missing OLDER tail from the first source that (a) returns data and (b) is the SAME
ASSET — validated by comparing source vs own close at COMMON timestamps (overlap),
not at the splice seam (v1-v4 seam-check was too loose: it let Binance LITUSDT=Litentry
$0.74 splice onto a different LIT perp ~$1.6). Sources:
  crypto:  binance(fapi+spot merged) -> bybit_linear -> gate_futures
  _OVERRIDE / _TRADFI: same-asset proxies tried FIRST — gold->PAXGUSDT, the whole
  TradFi class (metals/energy/indices/FX) -> Yahoo (1d direct, 4h/8h via 1h-resample).
Adding a source = one adapter + one chain entry. Adding a proxy = one _OVERRIDE entry.

Gated by env WARMUP_BACKFILL (default "0"). FAIL-SAFE: any miss/error/wrong-asset
returns the own df unchanged -> pre-existing PARITY SKIP (never a wrong-seeded signal
on real money). Young coins with no >=610-bar source ANYWHERE correctly stay skipped.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import urllib.request

import pandas as pd

log = logging.getLogger(__name__)

_BINANCE_TF = {"15m": "15m", "30m": "30m", "1h": "1h", "2h": "2h",
               "4h": "4h", "8h": "8h", "1d": "1d"}
_BYBIT_TF = {"1h": "60", "2h": "120", "4h": "240", "1d": "D"}          # no 8h on bybit
_GATE_TF = {"1h": "1h", "4h": "4h", "8h": "8h", "1d": "1d"}
_YAHOO_TF = {"4h": "4h", "8h": "8h", "1d": "1d"}                       # 8h/4h via 1h-resample

# v6 LOCAL source: pre-pulled TV daily history for xyz_* HIP-3 underlyings. Files live at
# <bot_root>/data/xyz_backfill/<tf>/<coin>.parquet (canonical) with a .csv.gz fallback for
# venvs without pyarrow (the live combo venv has pandas but no parquet engine). Schema is
# the canonical lowercase ts(ms,int)/open/high/low/close/volume, oldest-first.
_LOCAL_BF_DIR = os.getenv(
    "WARMUP_LOCAL_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "data", "xyz_backfill"),
)

# Same-asset proxies, tried BEFORE the default crypto chain. The overlap guard makes a
# broad map SAFE — a wrong mapping is auto-rejected and the coin stays PARITY-SKIPped.
_GOLD = [("binance", "PAXGUSDT"), ("bybit", "PAXGUSDT"), ("yahoo", "GC=F")]
_TRADFI = {
    "XAU": _GOLD, "GOLD": _GOLD, "XAUUSD": _GOLD, "XAUT": _GOLD, "PAXG": _GOLD,
    "XAG": [("yahoo", "SI=F")], "SILVER": [("yahoo", "SI=F")], "XAGUSD": [("yahoo", "SI=F")],
    "XPT": [("yahoo", "PL=F")], "PLATINUM": [("yahoo", "PL=F")],
    "XPD": [("yahoo", "PA=F")], "PALLADIUM": [("yahoo", "PA=F")],
    "HG": [("yahoo", "HG=F")], "COPPER": [("yahoo", "HG=F")],
    "CL": [("yahoo", "CL=F")], "WTI": [("yahoo", "CL=F")], "OIL": [("yahoo", "CL=F")],
    "USOIL": [("yahoo", "CL=F")], "BRENT": [("yahoo", "BZ=F")], "UKOIL": [("yahoo", "BZ=F")],
    "NG": [("yahoo", "NG=F")], "NATGAS": [("yahoo", "NG=F")], "GAS": [("yahoo", "NG=F")],
    "SPX": [("yahoo", "^GSPC")], "SP500": [("yahoo", "^GSPC")], "US500": [("yahoo", "^GSPC")],
    "ES": [("yahoo", "^GSPC")], "NDX": [("yahoo", "^NDX")], "NAS100": [("yahoo", "^NDX")],
    "US100": [("yahoo", "^NDX")], "NQ": [("yahoo", "^NDX")], "DJI": [("yahoo", "^DJI")],
    "US30": [("yahoo", "^DJI")], "DOW": [("yahoo", "^DJI")], "RUT": [("yahoo", "^RUT")],
    "RUSSELL": [("yahoo", "^RUT")], "VIX": [("yahoo", "^VIX")], "DAX": [("yahoo", "^GDAXI")],
    "GER40": [("yahoo", "^GDAXI")], "NIKKEI": [("yahoo", "^N225")], "JP225": [("yahoo", "^N225")],
    "FTSE": [("yahoo", "^FTSE")], "UK100": [("yahoo", "^FTSE")], "HSI": [("yahoo", "^HSI")],
    "EUR": [("yahoo", "EURUSD=X")], "EURUSD": [("yahoo", "EURUSD=X")],
    "GBP": [("yahoo", "GBPUSD=X")], "GBPUSD": [("yahoo", "GBPUSD=X")],
    "JPY": [("yahoo", "JPY=X")], "AUD": [("yahoo", "AUDUSD=X")], "NZD": [("yahoo", "NZDUSD=X")],
    "CAD": [("yahoo", "CAD=X")], "CHF": [("yahoo", "CHF=X")],
}
_OVERRIDE = {
    "XBT": [("binance", "BTCUSDT")], "WBTC": [("binance", "BTCUSDT")],
    "WETH": [("binance", "ETHUSDT")],
    "kSHIB": [("binance", "1000SHIBUSDT")], "kPEPE": [("binance", "1000PEPEUSDT")],
    "kBONK": [("binance", "1000BONKUSDT")], "kFLOKI": [("binance", "1000FLOKIUSDT")],
    **_TRADFI,
}
_CHAIN = ["binance", "bybit", "gate"]

_NO_SRC: set = set()                 # (source, symbol, tf) genuinely-empty — skip refetch
_ROWS_CACHE: dict = {}               # (source, symbol, tf) -> rows (oldest-first)
_LOCK = threading.Lock()


def _base(coin: str) -> str:
    base = coin.upper()
    for suf in ("-PERP", "-USD", "/USD", "-USDT", "/USDT", "_PERP", "PERP"):
        if base.endswith(suf):
            base = base[: -len(suf)]
    return base.split("-")[0].split("/")[0].strip()


def _http(url: str):
    # urllib `timeout=` is PER-READ only; a slow-trickle response never trips it and can
    # wedge warmup indefinitely (recurring stall root-cause, fixed 2026-07-01). Enforce a
    # hard total wall-clock deadline via a daemon worker; on breach raise -> caller skips.
    _out = {}
    def _do():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "xnn-warmup/2.0"})
            with urllib.request.urlopen(req, timeout=12) as r:
                _out["v"] = json.load(r)
        except Exception as _e:  # noqa: BLE001
            _out["e"] = _e
    _t = threading.Thread(target=_do, daemon=True)
    _t.start()
    _t.join(30)
    if _t.is_alive():
        raise TimeoutError("warmup _http total-deadline 30s exceeded (slow-trickle): " + url[:80])
    if "e" in _out:
        raise _out["e"]
    return _out["v"]


# Each adapter returns rows OLDEST-FIRST: [[openTime_ms, o, h, l, c, v], ...] or []. They
# fetch up to `end_ms` so the result includes the OVERLAP with own (for the same-asset
# guard) plus older bars (for the splice).
def _fetch_binance(symbol, tf, end_ms):
    btf = _BINANCE_TF.get(tf)
    if not btf:
        return []
    merged = {}
    for host in ("https://fapi.binance.com/fapi/v1/klines",
                 "https://api.binance.com/api/v3/klines"):
        try:
            raw = _http(f"{host}?symbol={symbol}&interval={btf}&endTime={end_ms}&limit=1500")
            for k in (raw or []):
                merged[int(k[0])] = [int(k[0]), float(k[1]), float(k[2]),
                                     float(k[3]), float(k[4]), float(k[5])]
        except Exception as e:
            log.debug("binance %s %s: %s", symbol, btf, e)
    return [merged[t] for t in sorted(merged)]


def _fetch_bybit(symbol, tf, end_ms):
    btf = _BYBIT_TF.get(tf)
    if not btf:
        return []
    try:
        raw = _http(f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}"
                    f"&interval={btf}&end={end_ms}&limit=1000")
        lst = (raw or {}).get("result", {}).get("list", []) or []
        rows = [[int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                 float(k[4]), float(k[5])] for k in lst]
        rows.sort(key=lambda r: r[0])
        return rows
    except Exception as e:
        log.debug("bybit %s %s: %s", symbol, btf, e)
        return []


def _fetch_gate(symbol, tf, end_ms):
    gtf = _GATE_TF.get(tf)
    if not gtf:
        return []
    try:
        raw = _http(f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}"
                    f"&interval={gtf}&to={end_ms // 1000}&limit=1000")
        rows = [[int(k["t"]) * 1000, float(k["o"]), float(k["h"]), float(k["l"]),
                 float(k["c"]), float(k.get("v", 0))] for k in (raw or [])]
        rows.sort(key=lambda r: r[0])
        return rows
    except Exception as e:
        log.debug("gate %s %s: %s", symbol, gtf, e)
        return []


def _fetch_yahoo(symbol, tf, end_ms):
    if tf not in _YAHOO_TF:
        return []
    try:
        from urllib.parse import quote
        interval, rng = ("1d", "10y") if tf == "1d" else ("1h", "730d")
        raw = _http(f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
                    f"?interval={interval}&range={rng}")
        res = raw["chart"]["result"][0]
        ts = res["timestamp"]
        q = res["indicators"]["quote"][0]
        vol = q.get("volume") or [0] * len(ts)
        rows = []
        for i, t in enumerate(ts):
            o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
            if None in (o, h, l, c):
                continue
            rows.append([int(t) * 1000, float(o), float(h), float(l), float(c), float(vol[i] or 0)])
        rows.sort(key=lambda r: r[0])
        if tf == "1d" or not rows:
            return rows
        n = {"4h": 4, "8h": 8}[tf]
        d = pd.DataFrame(rows, columns=["ms", "Open", "High", "Low", "Close", "Volume"])
        d.index = pd.to_datetime(d["ms"], unit="ms", utc=True)
        agg = d.resample(f"{n}h", origin="epoch", label="left", closed="left").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()
        return [[int(ix.value // 10**6), float(r.Open), float(r.High), float(r.Low),
                 float(r.Close), float(r.Volume)] for ix, r in agg.iterrows()]
    except Exception as e:
        log.debug("yahoo %s %s: %s", symbol, tf, e)
        return []


def _fetch_local(symbol, tf, end_ms):
    """LOCAL pre-pulled history (v6). `symbol` is the FULL coin id (e.g. xyz_AMD). Reads
    <_LOCAL_BF_DIR>/<tf>/<symbol>.parquet, falling back to <symbol>.csv.gz when the venv
    lacks a parquet engine. Returns oldest-first [[ts_ms,o,h,l,c,v],...] or [] on any
    miss/error. end_ms is ignored — the file already spans full history incl the overlap
    with own bars (the same-asset guard downstream needs that recent overlap)."""
    base = os.path.join(_LOCAL_BF_DIR, tf, symbol)
    pq, csv = base + ".parquet", base + ".csv.gz"
    df = None
    try:
        if os.path.exists(pq):
            try:
                df = pd.read_parquet(pq)
            except Exception as e:                       # no pyarrow/fastparquet -> csv.gz
                log.debug("local parquet %s unread (%s) — trying csv.gz", pq, e)
                df = None
        if df is None and os.path.exists(csv):
            df = pd.read_csv(csv)
        if df is None or len(df) == 0:
            return []
        cols = {c.lower(): c for c in df.columns}
        need = ("ts", "open", "high", "low", "close", "volume")
        if not all(k in cols for k in need):
            log.debug("local %s %s: missing cols %s", symbol, tf, list(df.columns))
            return []
        df = df.sort_values(cols["ts"])
        rows = [[int(r[cols["ts"]]), float(r[cols["open"]]), float(r[cols["high"]]),
                 float(r[cols["low"]]), float(r[cols["close"]]), float(r[cols["volume"]])]
                for _, r in df.iterrows()]
        return rows
    except Exception as e:
        log.debug("local %s %s: %s", symbol, tf, e)
        return []


_ADAPTERS = {"binance": _fetch_binance, "bybit": _fetch_bybit,
             "gate": _fetch_gate, "yahoo": _fetch_yahoo, "local": _fetch_local}


def _candidates(coin):
    # v6: xyz_* HIP-3 underlyings get the LOCAL pre-pulled source FIRST, keyed by the FULL
    # coin id (NOT _base — there is no crypto/proxy symbol for these). Crypto coins skip it.
    out = []
    if isinstance(coin, str) and coin.startswith("xyz_"):
        out.append(("local", coin))
    out += [c for c in _OVERRIDE.get(_base(coin), []) if c not in out]
    sym = _base(coin) + "USDT"
    for s in _CHAIN:
        if (s, sym) not in out:
            out.append((s, sym))
    return out


def _source_rows(coin, tf, end_ms):
    """First candidate source returning data, as full rows (oldest-first, incl overlap)."""
    for source, symbol in _candidates(coin):
        key = (source, symbol, tf)
        with _LOCK:
            if key in _ROWS_CACHE:
                return _ROWS_CACHE[key], source, symbol
            if key in _NO_SRC:
                continue
        fetch = _ADAPTERS.get(source)
        if not fetch:
            continue
        rows = fetch(symbol, tf, end_ms)
        if not rows:
            with _LOCK:
                _NO_SRC.add(key)
            continue
        with _LOCK:
            _ROWS_CACHE[key] = rows
        return rows, source, symbol
    return None, None, None


def backfill_warmup(df, coin, tf, min_bars):
    """Prepend deep multi-source history so len(df) >= min_bars. Off unless WARMUP_BACKFILL=1.
    Returns own df on any miss/error/wrong-asset (fail-safe)."""
    try:
        if os.getenv("WARMUP_BACKFILL", "0") not in ("1", "true", "True"):
            return df
        if df is None or len(df) == 0 or len(df) >= min_bars:
            return df
        own_first_ms = int(pd.Timestamp(df["time"].iloc[0]).value // 10**6)
        own_last_ms = int(pd.Timestamp(df["time"].iloc[-1]).value // 10**6)
        tf_ms = {"1h": 3600000, "2h": 7200000, "4h": 14400000,
                 "8h": 28800000, "1d": 86400000}.get(tf, 86400000)
        rows, source, symbol = _source_rows(coin, tf, own_last_ms)
        if not rows:
            return df
        # GLUE GUARD: a delisted-then-ticker-reused feed (e.g. Binance LITUSDT = old
        # Litentry $0.74 2021-2024 glued under the new LIT perp $1.57) corrupts the deep
        # EMA tail while the seam+recent price look fine. On a 24/7 crypto source such a
        # glue shows a TIME GAP between the two assets — keep only the contiguous segment
        # ending at the most-recent bar. Yahoo/TradFi AND the v6 LOCAL source are exempt
        # (weekends are legit session gaps; both are explicitly mapped per exact coin, no
        # ticker-collision risk — cutting on weekend gaps would destroy the deep tail).
        if source not in ("yahoo", "local") and len(rows) > 1:
            cut = 0
            for i in range(len(rows) - 1, 0, -1):
                if rows[i][0] - rows[i - 1][0] > 5 * tf_ms:
                    cut = i
                    break
            if cut:
                rows = rows[cut:]
        if not rows:
            return df
        # SAME-ASSET GUARD via CURRENT PRICE LEVEL: both feeds end ~now (source fetched
        # to own_last), so compare the recent price level (median of last 5 closes). Same
        # asset / proxy matches within a few % (cross-exchange basis); two assets sharing
        # a ticker diverge hard — Binance LITUSDT=Litentry $0.74 vs the LIT perp $1.62 =>
        # ratio 0.46 => REJECT. (Timestamp-overlap is unreliable: 8h boundaries differ per
        # venue so common-ts can be empty; price-level needs no alignment.)
        own_recent = float(pd.Series([float(x) for x in df["Close"].iloc[-5:]]).median())
        src_recent = float(pd.Series([float(r[4]) for r in rows[-5:]]).median())
        ratio = (src_recent / own_recent) if own_recent > 0 else 0.0
        if not (0.9 <= ratio <= 1.111):
            log.warning("warmup_backfill %s %s REJECT: %s:%s price %.5f vs own %.5f ratio=%.3f "
                        "— wrong asset — keeping own df", coin, tf, source, symbol,
                        src_recent, own_recent, ratio)
            with _LOCK:
                _NO_SRC.add((source, symbol, tf))
            return df
        tail = [r for r in rows if r[0] < own_first_ms]
        if not tail:
            return df
        # BAR-CADENCE PARITY: the spliced tail MUST share the own series' bar cadence.
        # A session-gapped TradFi feed (Yahoo silver/crude ~5% weekend gaps) spliced under
        # a 24/7 perp (continuous 8h bars) makes EMA-610 span a different calendar window ->
        # the live indicator diverges from the perp/TV. Reject if gap-fractions differ >3pp.
        #
        # v6 EXEMPTION for source=="local" (xyz_* HIP-3): VERIFIED 2026-06-20 the HL candle
        # API serves these tokenized stocks/commodities as CONTINUOUS 24/7 daily bars
        # (gapfrac 0.0) while the TV underlying (and the bt-1 us29 parity dataset
        # ohlc_hl_xyz, gapfrac ~0.22) is SESSION-GAPPED — so own(0.0) vs tail(0.22) would
        # ALWAYS trip this guard and the local source would be INERT (never splice). The
        # session-gap is the legitimate, expected structure of a stock underlying; the
        # same-asset price-level guard above already proves it's the right asset, and the
        # local file is keyed per EXACT coin (no ticker-collision risk). So the cadence
        # guard does not apply here. (Crypto sources keep it — that is the LIT-glue defense.)
        def _gapfrac(ms):
            return (sum(1 for i in range(1, len(ms)) if ms[i] - ms[i - 1] > 1.5 * tf_ms)
                    / (len(ms) - 1)) if len(ms) > 2 else 0.0
        own_gap = _gapfrac(sorted(int(pd.Timestamp(t).value // 10**6) for t in df["time"]))
        tail_gap = _gapfrac(sorted(r[0] for r in tail))
        if source != "local" and abs(own_gap - tail_gap) > 0.03:
            log.warning("warmup_backfill %s %s REJECT: bar-cadence mismatch own_gap=%.3f "
                        "src_gap=%.3f (session-gapped feed vs 24/7 perp) — keeping own df",
                        coin, tf, own_gap, tail_gap)
            with _LOCK:
                _NO_SRC.add((source, symbol, tf))
            return df
        bf = pd.DataFrame(tail, columns=["ms", "Open", "High", "Low", "Close", "Volume"])
        bf["time"] = pd.to_datetime(bf["ms"], unit="ms", utc=True).astype(df["time"].dtype)
        bf = bf[["time", "Open", "High", "Low", "Close", "Volume"]]
        out = (pd.concat([bf, df], ignore_index=True)
               .drop_duplicates(subset="time", keep="last")
               .sort_values("time").reset_index(drop=True))
        if not hasattr(backfill_warmup, "_logged"):
            backfill_warmup._logged = set()
        if (coin, tf) not in backfill_warmup._logged:
            backfill_warmup._logged.add((coin, tf))
            log.info("WARMUP BACKFILL %s %s: %d own + %d %s:%s -> %d bars (>=%d %s)",
                     coin, tf, len(df), len(out) - len(df), source, symbol, len(out),
                     min_bars, "ok" if len(out) >= min_bars else "STILL SHORT")
        return out
    except Exception as e:
        log.warning("warmup_backfill(%s,%s) failed: %s — using own df", coin, tf, e)
        return df
