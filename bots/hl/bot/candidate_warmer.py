"""Candidate warm-keeper (2026-06-29).

Keep the just-closed candle PRIMED for the coins that could POTENTIALLY open a position on
this boundary, so the scan serves them INSTANTLY instead of waiting 9-54s for HL's sparse,
staggered WS push (measured: 1m close->arrival p50 9s / max 45s; single-4h scan walk 54s).

WHY this is safe to run on the live money bot:
  * ADDITIVE + PARITY-SAFE: it only calls client.candles(..., prime=True) — the SAME REST
    candle the scan would itself fetch (same bar, same cache write). It can ONLY make a
    signal appear sooner; it can NEVER change which signal fires or place an order.
  * RATE-SAFE: candidates are a small near-trigger subset, HARD-CAPPED per boundary
    (WARM_CAND_MAX), each prime goes through candles()'s existing per-fetch throttle. Worst
    case if the proximity logic is wrong = a few wasted (capped, throttled) REST reads —
    never a money action, never a 261-coin storm.
  * NON-BLOCKING: background daemon, errors swallowed, REST fallback in the scan intact.

Candidate = coin whose CURRENT mark is within WARM_CAND_PROXIMITY_PCT of its entry trigger,
computed cheaply from already-cached prior (final) bars + the bulk mark_price snapshot:
  * crypto Donchian leg (8h): mark within pct BELOW upper = max(High[-DONCHIAN_K:])
  * us29 xyz leg (4h/1d): fresh EMA20>50>100 stack reclaim estimated with mark as the
    forming close (per us29_core), OR mark within pct below EMA20 (about to reclaim).
Formulas mirror strategy_donchian.py / us29_core.py (no invented params).
"""
import os
import time
import threading

import numpy as np
import pandas as pd

_TF_MS = {"1m": 60_000, "4h": 14_400_000, "8h": 28_800_000, "1d": 86_400_000}


def _is_xyz(sym: str) -> bool:
    return sym.startswith("xyz")


def _ema(c: np.ndarray, p: int) -> np.ndarray:
    # VECTORIZED EMA == us29_core.ema bit-for-bit: pandas ewm(span=p, adjust=False) is the
    # SAME causal recursion (out[0]=c[0], alpha=2/(p+1)). Vectorized → runs in numpy/pandas
    # (releases the GIL) instead of a pure-Python hot loop — the warmer runs in a bg thread
    # DURING the latency-sensitive boundary scan, so a Python loop here would steal GIL/scan
    # time (GIL-fix 2026-06-29). Crypto leg note: live exchange_hl has NO binance_candles
    # (reverted to native HL), so crypto 8h IS fetched via candles() and IS in _candles_cache
    # → the Donchian branch below is live+useful (not dead).
    return pd.Series(c, dtype=float).ewm(span=p, adjust=False).mean().to_numpy()


def start_candidate_warmer(client, universe, settings, log) -> None:
    """Start the candidate warm-keeper daemon. No-op if disabled or no WS feed."""
    if os.getenv("WARM_CANDIDATES", "1") != "1":
        log.info("candidate-warmer: disabled (WARM_CANDIDATES!=1)")
        return
    if getattr(client, "_ws_feed", None) is None:
        log.info("candidate-warmer: no WS feed — skipped (REST scan already direct)")
        return

    THRESH = float(os.getenv("WARM_CAND_PROXIMITY_PCT", "0.02"))   # within 2% of trigger
    HARD_CAP = int(os.getenv("WARM_CAND_MAX", "25"))               # rate-safety hard cap
    POLL = float(os.getenv("WARM_POLL_SEC", "5"))                  # cheap age-check cadence
    PRIME_WINDOW = float(os.getenv("WARM_PRIME_WINDOW_SEC", "120"))  # prime for N s post-boundary
    LIMIT = int(os.getenv("SCAN_CANDLES_LIMIT", "300"))
    DONCH_N = int(os.getenv("DONCHIAN_K", "15"))
    donch_tfs = {t.strip() for t in os.getenv("DONCHIAN_TFS", "8h").split(",") if t.strip()}
    us29_tfs = {"4h", "1d"}
    watch_tfs = sorted(donch_tfs | us29_tfs, key=lambda t: _TF_MS.get(t, 0))

    syms = [s for s in (getattr(a, "symbol", None) for a in universe) if s]

    def _cached_df(sym, tf):
        # cache key mirrors candles(): (coin, interval) using the BOT symbol (not api name)
        try:
            with client._cache_lock:
                ent = client._candles_cache.get((sym, tf))
            return ent[1] if ent else None
        except Exception:
            return None

    def _proximity(sym, tf):
        """proximity in [0,THRESH] if near trigger (0 = qualifying now), else None.
        Cheap; worst-case-wrong = perf-only (additive prime), never money."""
        df = _cached_df(sym, tf)
        if df is None or len(df) < 25:
            return None
        try:
            mark = float(client.mark_price(sym))
        except Exception:
            return None
        if mark <= 0:
            return None
        try:
            H = df["High"].to_numpy(dtype=float)
            C = df["Close"].to_numpy(dtype=float)
        except Exception:
            return None

        if _is_xyz(sym) and tf in us29_tfs:
            if len(C) < 205:
                return None
            e20 = _ema(C, 20); e50 = _ema(C, 50); e100 = _ema(C, 100); e200 = _ema(C, 200)
            j = len(C) - 1
            aligned_jm1 = (C[j] > e20[j]) and (e20[j] > e50[j]) and (e50[j] > e100[j])
            e20c = mark * (2 / 21) + e20[j] * (1 - 2 / 21)
            e50c = mark * (2 / 51) + e50[j] * (1 - 2 / 51)
            e100c = mark * (2 / 101) + e100[j] * (1 - 2 / 101)
            e200c = mark * (2 / 201) + e200[j] * (1 - 2 / 201)
            aligned_c = (mark > e20c) and (e20c > e50c) and (e50c > e100c)
            regime_c = mark > e200c
            if aligned_c and (not aligned_jm1) and regime_c:
                return 0.0  # would fire on this close -> top priority
            if regime_c and e20c > 0:
                d = (e20c - mark) / e20c
                if 0.0 < d <= THRESH:   # mark just below fast EMA, about to reclaim
                    return d
            return None

        if (not _is_xyz(sym)) and tf in donch_tfs:
            if len(H) < DONCH_N:
                return None
            upper = float(np.max(H[-DONCH_N:]))
            d = (upper - mark) / mark
            if 0.0 <= d <= THRESH:
                return d
            return None
        return None

    def _loop():
        last_primed = {}   # (sym,tf) -> last_closed_bar_ts already primed
        log.info(
            "candidate-warmer: started (thresh=%.3f cap=%d poll=%.0fs window=%.0fs tfs=%s, "
            "%d symbols)", THRESH, HARD_CAP, POLL, PRIME_WINDOW, watch_tfs, len(syms),
        )
        while True:
            try:
                now_ms = int(time.time() * 1000)
                active = [tf for tf in watch_tfs
                          if (now_ms % _TF_MS[tf]) / 1000.0 <= PRIME_WINDOW]
                if active:
                    cands = []
                    for sym in syms:
                        for tf in active:
                            ms = _TF_MS[tf]
                            last_closed = (now_ms // ms) * ms - ms
                            if last_primed.get((sym, tf)) == last_closed:
                                continue   # already primed this bar
                            p = _proximity(sym, tf)
                            if p is not None:
                                cands.append((p, sym, tf, last_closed))
                    if cands:
                        cands.sort(key=lambda x: x[0])   # most imminent first
                        primed = []
                        for p, sym, tf, last_closed in cands[:HARD_CAP]:
                            try:
                                client.candles(sym, tf, limit=LIMIT, prime=True)
                                last_primed[(sym, tf)] = last_closed
                                primed.append("%s/%s" % (sym, tf))
                            except Exception as e:
                                log.debug("warm prime %s %s skip: %s", sym, tf, e)
                        if primed:
                            log.info(
                                "candidate-warmer: primed %d/%d near-trigger coins "
                                "(tfs=%s): %s", len(primed), len(cands), active,
                                ", ".join(primed[:12]) + (" …" if len(primed) > 12 else ""),
                            )
            except Exception as e:
                log.warning("candidate-warmer loop err (additive, ignored): %s", e)
            time.sleep(POLL)

    threading.Thread(target=_loop, name="candidate-warmer", daemon=True).start()
    log.info("candidate-warmer: thread launched (background, manage loop unblocked)")
