"""topk_pool.py — causal expanding-prior TOP-K score gate for US29.

The US29 backtest edge used a CROSS-UNIVERSE causal ranking: a candidate is accepted
only if its rank score is in the top (100-PCT)% of ALL prior candidate scores seen
across the whole universe. The per-coin scan_for_signal has no cross-coin pooled state,
so this gate lives here and is applied in main.py main_loop.

CAUSALITY: the threshold is computed from the pool of scores observed STRICTLY BEFORE
this tick's candidates are added. We:
  1. read the current persisted pool (all prior scores),
  2. compute thr = the value at the (100-PCT)th percentile of the PRIOR pool,
  3. accept a candidate iff score >= thr (if the pool is too small to form a stable
     threshold we accept — bootstrap, matching "no prior data => no cut"),
  4. THEN append this tick's candidate scores to the pool and persist.

DEVIATION FROM THE PURE BT EMITTER (documented per build_spec section E / open_risks #6):
The bt-1 deployable emitter (emit_us29_stocks.topk_threshold) keeps an UNBOUNDED pool
back to POOL_START=1990-01-01. A live bot starts with an EMPTY pool and grows it tick by
tick — it has NO historical pre-seed, so for the first N candidates the threshold is
weak/absent (bootstrap window) and the live cut diverges from the bt cut until the pool
fills. To bound disk/compute the pool is also capped at US29_TOPK_POOL_MAX (default
500000) most-recent scores (rolling tail). Both deviations are logged at startup. They
mean realized live selectivity ramps up over time rather than matching the bt's
full-history cut from bar 1. This is the build_spec-sanctioned "persist a rolling score
pool and document the deviation" fallback.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import List, Optional

import numpy as np

log = logging.getLogger(__name__)


class TopKPool:
    def __init__(self, path: str, pct: float = 70.0, min_pool: int = 30,
                 pool_max: int = 500_000):
        self.path = Path(path)
        self.pct = float(pct)               # accept top (100-pct)% => threshold at pct-percentile
        self.min_pool = int(min_pool)       # bootstrap: accept all until pool reaches this size
        self.pool_max = int(pool_max)
        self._scores: List[float] = []
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                with open(self.path) as f:
                    data = json.load(f)
                self._scores = [float(x) for x in data.get("scores", [])
                                if x is not None and np.isfinite(float(x))]
        except Exception as e:
            log.warning("TopKPool load failed (%s) — starting empty: %s", self.path, e)
            self._scores = []

    def _persist(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump({"scores": self._scores, "pct": self.pct}, f)
            os.replace(tmp, self.path)
        except Exception as e:
            log.warning("TopKPool persist failed (%s): %s", self.path, e)

    def threshold(self) -> Optional[float]:
        """70th-pct threshold of the PRIOR pool, or None if pool too small (bootstrap)."""
        n = len(self._scores)
        if n < self.min_pool:
            return None
        arr = np.asarray(self._scores, dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return None
        return float(np.percentile(arr, self.pct))

    def filter_and_update(self, signals: list) -> list:
        """Accept signals whose .f1_dist (rank score) >= prior-pool threshold (causal),
        then add THIS tick's scores to the pool and persist. Returns accepted signals,
        sorted by (-score, coin) so the bot fills the strongest first."""
        prior_thr = self.threshold()
        prior_n = len(self._scores)

        def _score(s) -> float:
            try:
                v = float(getattr(s, "f1_dist", float("nan")))
                return v if np.isfinite(v) else -1e9
            except Exception:
                return -1e9

        accepted = []
        new_scores = []
        for s in signals:
            sc = _score(s)
            new_scores.append(sc)
            if prior_thr is None or sc >= prior_thr:
                accepted.append(s)

        # grow pool with this tick's candidate scores (causal: added AFTER the cut)
        if new_scores:
            self._scores.extend(new_scores)
            if len(self._scores) > self.pool_max:
                self._scores = self._scores[-self.pool_max:]
            self._persist()

        accepted.sort(key=lambda s: (-_score(s), getattr(s, "coin", "")))
        if signals:
            log.info(
                "US29 TOPK gate: %d/%d accepted (pct=%.0f prior_pool=%d thr=%s)",
                len(accepted), len(signals), self.pct, prior_n,
                ("None(bootstrap)" if prior_thr is None else f"{prior_thr:.4f}"),
            )
        return accepted
