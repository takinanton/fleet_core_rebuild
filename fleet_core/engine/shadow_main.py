"""shadow_main — CLI launcher for the per-venue 48h shadow process (P3).

    python -m fleet_core.engine.shadow_main --venue <v> --bot-root <dir>

Design authority: proofs/p3_design_shadow_runner.md (round-3). This module is
the ASSEMBLY layer only — every decision comes from the pinned engine cores:

  * exit side  — fleet_core.engine.exit_engine.shadow_decide (the SAME pure
    tick() pipeline + r3 strategy-factory/quirks wiring live will run);
  * entry side — fleet_core.engine.entry_sm.shadow_decide (engine gate chain
    over the MIRRORED suppression state — F6) fed by SIGNALS FROM THE VENUE'S
    OWN LIVE SCANNER MODULES (bot.scanner / bot.strategy_xnn / bot.universe /
    bot.config imported from the bot root — byte-identical signal math).

RUNTIME CONTRACT (deploy/shadow_units/<venue>-shadow.service): the process
runs in the venue's LIVE bot venv with WorkingDirectory=<bot root> and the
live .env loaded (config parity by construction, design §1). The `bot`
package is therefore importable; fleet_core/ sits in the bot root.

HARD LAWS carried here:
  * ZERO venue writes — the venue client is wrapped in the shadow_runner
    ReadOnlyExchangeClient fence (startup write-fence selftest mandatory);
    scanner/universe code receives a whitelist proxy whose six P2 write
    methods raise ShadowWriteBlocked BEFORE any I/O.
  * ZERO writes to the LIVE trades.db — every live-DB read in this module
    uses sqlite URI mode=ro; the shadow's own state lives under
    <bot-root>/data/shadow/.
  * Read budget ≤50% of the venue budget (ReadBudget LAW): per-venue budgets
    below are DERIVED from the adapters' own documented pacing, never
    invented; a closed-bar scan burst is deferred (bar kept pending) when the
    bucket cannot cover universe+exit reads — logged as a coverage gap
    (`shadow_tick_skipped`-class record), never a crash and never venue 429s.
  * Graceful degrade — signal-production failure logs a coverage-gap record
    for that tick; the window never crashes on a scanner error (design §1).

Offline selftest (no venue SDKs, fake `bot` package injected via sys.modules):
    python3 -m fleet_core.engine.shadow_main --selftest
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from fleet_core.engine.shadow_runner import (
    ReadBudget,
    ReadOnlyExchangeClient,
    ShadowRunner,
    ShadowWriteBlocked,
    tf_to_ms,
)
from fleet_core.engine.suppression_mirror import JournalctlProvider, SuppressionMirror

log = logging.getLogger("fleet_core.engine.shadow_main")

__all__ = [
    "VENUE_LIVE_UNITS", "VENUE_READ_BUDGET_PER_MIN", "SCAN_BUDGET_RESERVE",
    "GlueReadOnlyClient", "SignalSource", "EntrySizer",
    "build_shadow", "main",
]

VENUES = ("hl", "pacifica", "extended", "nado")

# Live bot unit per venue — journalctl source for the SuppressionMirror
# (evidence: deploy/deploy_p1.sh venue map; the HL live unit is valantis-bot,
# see deploy/engine_units/fleet-hl-bot.service note).
VENUE_LIVE_UNITS = {
    "hl": "valantis-bot",
    "pacifica": "pacifica-bot",
    "extended": "extended-bot",
    "nado": "nado-bot",
}

# Venue read budgets (req/min) — DERIVED from each adapter's own documented
# pacing (never arbitrary). The shadow_runner ReadBudget then applies the
# ≤50% LAW on top of these:
#   pacifica : REST pacer min-interval 140 ms
#              (bots/pacifica/bot/exchange_pacifica.py:196
#               PACIFICA_REST_MIN_INTERVAL_MS=140) → 60/0.140 ≈ 428 req/min
#               (≈7.1 req/s — the documented "~7 req/s" pacer).
#   hl       : adapter self-pace "300ms + jitter between cache-miss requests
#              (rate-limit protection)" (bots/hl/bot/exchange_hl.py:602)
#              → 200 req/min.
#   extended : no numeric budget documented in the adapter — adopt the
#              fleet's STRICTEST documented self-pace (hl 300 ms → 200/min)
#              as the conservative stand-in (labeled, not invented).
#   nado     : same stand-in; the adapter documents Vertex indexer throttling
#              without a number (bots/nado/bot/exchange_nado.py:626).
VENUE_READ_BUDGET_PER_MIN = {
    "pacifica": 428.0,
    "hl": 200.0,
    "extended": 200.0,
    "nado": 200.0,
}

# Tokens reserved for the SAME tick's exit-pipeline reads when deciding
# whether a closed-bar scan burst fits the bucket. Derivation: the exit phase
# performs ≤4 budgeted reads per open row (mark, SL listing, candles, liq)
# + 1 shared positions read; 5 concurrent rows is the live MAX_CONCURRENT
# default (bots/*/bot/config.py MAX_CONCURRENT=5) → 5×4+1 ≈ 21.
SCAN_BUDGET_RESERVE = 21.0

# Per-candle-read wait ceiling while the scan self-paces against the bucket
# refill. Derivation: a FULL bucket refills in 60s by construction
# (ReadBudget.refill_per_sec = capacity/60), so any single token arrives
# within 60s/capacity; 90s = one full-bucket refill + 50% slack for exit-read
# contention. Bounded by the live bar-age gate (scans stay valid for minutes).
SCAN_TOKEN_WAIT_SEC = 90.0

# The six P2 write ops (shadow doc §2) — blocked on every glue surface.
_WRITE_OPS = ("market_open", "ensure_flat", "trigger_sl",
              "cancel_sl_order", "limit_reduce_only", "update_leverage")


# ============================================================================
# Glue read-only client (scanner/universe surface)
# ============================================================================

class GlueReadOnlyClient:
    """Read surface handed to the venue's OWN scanner/universe/sizing code.

    * P2 contract reads resolve on the BUDGETED fence client (ReadOnly-
      ExchangeClient) — candles/positions/… consume shadow budget tokens.
    * `candles` additionally SELF-PACES against the bucket refill (bounded
      wait, SCAN_TOKEN_WAIT_SEC): a closed-bar scan burst larger than the
      instantaneous bucket then completes at the ≤50%-budget rate instead of
      silently dropping tail coins (per-coin RateLimited would be swallowed
      by the scanner's defensive per-coin catch = shadow-blind coins). A
      VENUE-raised RateLimited re-raises immediately — the venue is never
      hammered.
    * Contract-exempt reads the live code uses (asset, round_price,
      invalidate_candles_cache, funding_rate, …) pass through to the P2
      binding (they are metadata/cache reads).
    * The six P2 write methods raise ShadowWriteBlocked BEFORE any I/O —
      structurally, on THIS object, regardless of what the inner objects
      would do (belt over the fence client's own belt)."""

    def __init__(self, fenced: ReadOnlyExchangeClient, p2: Any,
                 budget: Optional[ReadBudget] = None,
                 token_wait_sec: float = SCAN_TOKEN_WAIT_SEC,
                 sleep_fn: Callable[[float], None] = time.sleep) -> None:
        self._fenced = fenced
        self._p2 = p2
        self._budget = budget
        self._token_wait = float(token_wait_sec)
        self._sleep = sleep_fn

    def candles(self, coin: str, interval: str, limit: int = 200,
                max_stale_bars: float = 1.0) -> Any:
        budget = self._budget
        if budget is not None:
            deadline = time.time() + self._token_wait
            # wait for a LOCAL token (never a venue interaction); leave the
            # actual acquire to the fence client (single accounting point).
            while budget.available() < 1.0 and time.time() < deadline:
                self._sleep(0.5)
        return self._fenced.candles(coin, interval, limit, max_stale_bars)

    def __getattr__(self, name: str) -> Any:
        if name in _WRITE_OPS:
            def _blocked(*_a: Any, **_k: Any) -> Any:
                raise ShadowWriteBlocked(
                    "shadow glue: write %r blocked by construction" % name,
                    venue=getattr(self._fenced, "venue", ""), op=name)
            return _blocked
        fenced = object.__getattribute__(self, "_fenced")
        if hasattr(fenced, name):
            return getattr(fenced, name)
        p2 = object.__getattribute__(self, "_p2")
        if hasattr(p2, name):
            return getattr(p2, name)
        # F2 (shadow-wiring review 2026-07-03): nado's universe loader reads
        # client._sdk.market.get_all_product_symbols() (bots/nado/bot/
        # universe.py:106); the P2 binding exposes only .raw. Fall through to
        # the RAW adapter for this ONE read-only attribute — a general raw
        # fallthrough would expose raw write methods past the fence.
        if name == "_sdk":
            raw = getattr(p2, "raw", None)
            if raw is not None and hasattr(raw, "_sdk"):
                return raw._sdk
        return getattr(p2, name)  # raises the original AttributeError shape


class _DegradedSignalSource:
    """F3 (shadow-wiring review 2026-07-03): entry-side assembly failed at
    startup (bot-package import error / misdeploy). The shadow keeps its EXIT
    side alive and this stub returns a LOUD per-tick coverage gap for entries,
    retrying the real assembly every RETRY_EVERY collects — no systemd
    crash-loop, no silent-blind window. On a successful retry it builds the
    real SignalSource + EntrySizer in place (entry_decider picks up .sizer)."""

    RETRY_EVERY = 30

    def __init__(self, venue: str, glue_client: Any, budget: Any,
                 live_db_path: str, reason: str) -> None:
        self.venue = venue
        self._args = (venue, glue_client, budget, live_db_path)
        self.reason = reason
        self._real: Any = None
        self.sizer: Any = None
        self._calls = 0

    def collect(self, now: float) -> "Tuple[List[Any], Optional[str]]":
        if self._real is not None:
            return self._real.collect(now)
        self._calls += 1
        if self._calls % self.RETRY_EVERY == 1 and self._calls > 1:
            try:
                real = SignalSource(*self._args)
                self.sizer = EntrySizer(
                    self.venue, self._args[1], real.settings,
                    (lambda: real.snapshot_holder), real.universe_tiers,
                    bar_age_fn=getattr(real.scanner, "bar_age_sec", None))
                self._real = real
                log.critical("shadow entry-side assembly HEALED after %d "
                             "ticks — entry coverage resumes", self._calls)
                return real.collect(now)
            except Exception as e:  # noqa: BLE001
                self.reason = str(e)
        log.critical("shadow entry-side DEGRADED (assembly failed: %s) — "
                     "entry decisions are a coverage gap this tick", self.reason)
        return [], "entry_assembly_failed"

    @property
    def settings(self) -> Any:
        return self._real.settings if self._real is not None else None


# ============================================================================
# Live-DB read-only helpers (URI mode=ro everywhere — LAW)
# ============================================================================

def _ro_conn(path: str) -> sqlite3.Connection:
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    return con


class LiveRowReader:
    """Per-tick read-only snapshots of the live trades.db used to ENRICH the
    decision-core ctx (full row dicts, open/manual coin sets, registry oids).
    Cached per tick instant so the exit decider's per-row calls share one
    read."""

    def __init__(self, live_db_path: str) -> None:
        self.path = str(live_db_path)
        self._cache_key: Optional[float] = None
        self._cache: Dict[str, Any] = {}

    def snapshot(self, now: float) -> Dict[str, Any]:
        if self._cache_key == now and self._cache:
            return self._cache
        snap: Dict[str, Any] = {"open_coins": [], "manual_coins": set(),
                                "rows_full": {}, "registry_oids": frozenset(),
                                "pending_coins": []}
        try:
            con = _ro_conn(self.path)
        except sqlite3.Error as e:
            log.error("live-db snapshot open failed (%s) — ctx enrichment "
                      "degrades this tick", e)
            self._cache_key, self._cache = now, snap
            return snap
        try:
            cols = {r["name"] for r in con.execute("PRAGMA table_info(trades)")}
            for r in con.execute("SELECT * FROM trades WHERE status='open'"):
                d = dict(r)
                coin = str(d.get("coin") or "")
                snap["open_coins"].append(coin)
                snap["rows_full"][int(d["id"])] = d
                pattern = str(d.get("pattern") or "") if "pattern" in cols else ""
                mc = str(d.get("management_class") or "") \
                    if "management_class" in cols else ""
                if pattern.lower().startswith("manual") or \
                        mc in ("manual_claimed", "protect_only"):
                    snap["manual_coins"].add(coin)
            for r in con.execute("SELECT coin FROM trades WHERE status='pending'"):
                snap["pending_coins"].append(str(r["coin"] or ""))
            try:
                snap["registry_oids"] = frozenset(
                    str(r["oid"]) for r in
                    con.execute("SELECT oid FROM placed_trigger_oids"))
            except sqlite3.Error:
                pass  # pre-migration live DB — registry lives in the json file
        except sqlite3.Error as e:
            log.error("live-db snapshot read failed mid-way: %s", e)
        finally:
            con.close()
        self._cache_key, self._cache = now, snap
        return snap


# ============================================================================
# Signal source — the venue's OWN scanner in decision-only mode
# ============================================================================

class SignalSource:
    """Produces ctx['signals'] per tick from the venue's OWN bot.scanner over
    the read-only budgeted client (REST/candles path + the shadow's own
    SnapshotHolder — design choice: no second WS feed).

    Coverage semantics: scan happens only when the scanner sees a newly
    CLOSED bar (live cadence). If the read budget cannot cover the burst,
    the bar is left PENDING (scanner state untouched) and a
    'scan_deferred_budget' error is returned — the launcher logs the
    coverage gap and retries next tick within the live bar-age gate."""

    def __init__(self, venue: str, glue_client: Any, budget: ReadBudget,
                 live_db_path: str,
                 settings: Any = None, scanner: Any = None,
                 universe: Optional[Sequence[Any]] = None,
                 snapshot_holder: Any = None) -> None:
        self.venue = venue
        self.client = glue_client
        self.budget = budget
        self.live_db_path = live_db_path
        # venue bot modules — importable in the bot venv (WorkingDirectory=
        # bot root); selftests inject fakes via sys.modules['bot.*'].
        cfg_mod = importlib.import_module("bot.config")
        self.settings = settings if settings is not None \
            else cfg_mod.Settings.from_env()
        scan_mod = importlib.import_module("bot.scanner")
        self.scanner = scanner if scanner is not None \
            else scan_mod.Scanner(self.settings)
        self._universe = list(universe) if universe is not None else None
        self._universe_forced = universe is not None
        self._last_refresh = 0.0
        self.snapshot_holder = snapshot_holder
        if self.snapshot_holder is None:
            self.snapshot_holder = self._load_snapshot_holder()
        self._crypto_short_only = os.getenv("CRYPTO_SHORT_ONLY", "").strip() \
            .lower() in ("1", "true", "yes", "on")   # main.py:37 verbatim
        # HL us29 selection layer (main.py:1016-1035) — built lazily on first use
        self._hl_regime_gate = None
        self._hl_selector_ready = False

    # -- bootstrap pieces ----------------------------------------------------

    def _load_snapshot_holder(self) -> Any:
        """The shadow's OWN SnapshotHolder over the live bot's snapshot FILE,
        READ-ONLY: never runs the snapshot bootstrap (that writes the live
        file). Missing/stale snapshot ⇒ sizing degrades with a flag."""
        try:
            liq_mod = importlib.import_module("bot.liquidity")
            p = Path(getattr(self.settings, "liq_snapshot_path",
                             "data/liquidity_snapshot.json"))
            snap = liq_mod.load_snapshot(p)
            return liq_mod.SnapshotHolder(snap)
        except Exception as e:  # noqa: BLE001 — sizing degrades, flagged
            log.warning("liquidity snapshot unavailable (%s) — liq cap "
                        "inactive in shadow sizing (flagged)", e)
            return None

    def _load_universe(self, force: bool) -> List[Any]:
        uni_mod = importlib.import_module("bot.universe")
        if self.venue == "nado":
            # nado main.py:517 — load_universe(client)
            return list(uni_mod.load_universe(self.client))
        universe = list(uni_mod.load_universe(force_refresh=force))
        if self.venue == "hl":
            # hl main.py:830 — depth gate over the snapshot (verbatim args)
            try:
                depth_enable = os.getenv("DEPTH_GATE_ENABLE", "1").strip() \
                    .lower() not in ("0", "false", "no", "off")
                snap = self.snapshot_holder.current() \
                    if self.snapshot_holder is not None else None
                try:
                    eq = self.client.account_value()
                except Exception:  # noqa: BLE001 — live does the same
                    eq = 0.0
                worst = (self.settings.risk_per_trade * eq /
                         max(self.settings.min_sl_dist_pct, 1e-6)) if eq > 0 else None
                universe = list(uni_mod.apply_depth_gate(
                    universe, snap, self.settings.liq_size_cap_pct,
                    enabled=depth_enable, worst_order_notional_usd=worst))
            except Exception as e:  # noqa: BLE001 — degrade to ungated universe
                log.warning("hl depth gate unavailable (%s) — scanning "
                            "ungated universe (flagged in decisions)", e)
        return universe

    def universe(self, now: float) -> List[Any]:
        refresh_min = float(getattr(self.settings, "universe_refresh_min", 60))
        if self._universe is None:
            self._universe = self._load_universe(force=True)
            self._last_refresh = now
        elif not self._universe_forced and \
                now - self._last_refresh > refresh_min * 60.0:
            try:
                self._universe = self._load_universe(force=True)
                self._last_refresh = now
            except Exception as e:  # noqa: BLE001 — keep the stale universe
                log.error("universe refresh failed: %s (keeping previous)", e)
        return self._universe or []

    def universe_tiers(self, now: float) -> Dict[str, int]:
        return {a.symbol: getattr(a, "tier", 2) for a in self.universe(now)}

    # -- per-tick pieces -----------------------------------------------------

    def _mirror_positions(self) -> Dict[str, Any]:
        """Mirrored in-memory open_positions dict for the scanner's cross-side
        dedup (coin → object with .tf/.side attrs), from the LIVE db (ro)."""
        out: Dict[str, Any] = {}
        try:
            con = _ro_conn(self.live_db_path)
        except sqlite3.Error:
            return out
        try:
            for r in con.execute(
                    "SELECT coin, tf, direction FROM trades WHERE status='open'"):
                out[str(r["coin"])] = SimpleNamespace(
                    tf=str(r["tf"] or ""), side=str(r["direction"] or "long"))
        except sqlite3.Error:
            pass
        finally:
            con.close()
        return out

    def _pending_scan_burst(self, now: float) -> int:
        """Coins the scanner WOULD fetch this tick (a TF bar newly closed),
        WITHOUT consuming scanner state — replicates Scanner.new_bar_closed's
        check read-only for the budget preflight."""
        try:
            tfs = set(self.settings.working_tfs) | \
                set(getattr(self.settings, "short_enabled_tfs", []) or [])
            last = getattr(self.scanner, "_last_bar_ts", {})
            n_coins = len(self.universe(now))
            burst = 0
            for tf in tfs:
                step = tf_to_ms(tf)
                now_ms = int(now * 1000)
                prev_bar = (now_ms // step) * step - step
                if prev_bar > int(last.get(tf, 0) or 0):
                    burst = max(burst, n_coins)
            return burst
        except Exception:  # noqa: BLE001 — preflight is best-effort
            return 0

    def _hl_apply_selector(self, signals: List[Any]) -> List[Any]:
        """hl main.py:1447-1485 verbatim policy: xyz_ signals → US29 top-M +
        SPY/SMA200 regime gate; crypto signals pass through unfiltered."""
        try:
            sel = importlib.import_module("bot.selector_us29")
        except Exception:  # noqa: BLE001 — no selector module: pass-through
            return signals
        selector = os.getenv("US29_SELECTOR", "topm").strip().lower()
        if selector != "topm":
            return signals   # legacy pool30 is opt-in diagnostics; not mirrored
        top_m = int(os.getenv("US29_TOP_M", "3"))
        gate_on = os.getenv("US29_REGIME_GATE", "1").strip().lower() \
            not in ("0", "false", "no", "off")
        xyz = [s for s in signals if str(getattr(s, "coin", "")).startswith("xyz_")]
        crypto = [s for s in signals if not str(getattr(s, "coin", "")).startswith("xyz_")]
        if not xyz:
            return signals
        regime_on = True
        if gate_on:
            try:
                if self._hl_regime_gate is None:
                    self._hl_regime_gate = sel.RegimeGate(
                        self.client,
                        regime_coin=os.getenv("US29_REGIME_COIN", "xyz_SP500").strip(),
                        sma_n=int(os.getenv("US29_REGIME_SMA_N", "200")),
                        candles_limit=int(os.getenv("SCAN_CANDLES_LIMIT", "3000")),
                    )
                regime_on = bool(self._hl_regime_gate.state()[0])
            except Exception as e:  # noqa: BLE001 — gate unreadable: keep xyz,
                log.warning("hl regime gate unreadable (%s) — xyz signals kept "
                            "unfiltered (flagged)", e)
                return signals
        try:
            kept = list(sel.select_topm(xyz, top_m, regime_on))
        except Exception as e:  # noqa: BLE001
            log.warning("hl select_topm failed (%s) — xyz signals kept", e)
            return signals
        return kept + crypto

    def _maybe_reload_snapshot(self) -> None:
        """F4 (shadow-wiring review 2026-07-03): live extended reloads the
        snapshot holder every 600s over the cron-regenerated file
        (bots/extended/bot/main.py:362+); the shadow mirrored a ONE-TIME load,
        so over 48h it would size liq caps from a snapshot up to 48h staler
        than live's → strategy-delta stall. Mirror the reload read-only:
        mtime-gated maybe_reload every collect (cheap stat), and retry a
        failed startup load so a late-appearing file heals."""
        p = Path(getattr(self.settings, "liq_snapshot_path",
                         "data/liquidity_snapshot.json"))
        if self.snapshot_holder is None:
            self.snapshot_holder = self._load_snapshot_holder()
            return
        try:
            self.snapshot_holder.maybe_reload(p)
        except Exception as e:  # noqa: BLE001 — live degrades identically (main.py:370)
            log.warning("snapshot reload error: %s", e)

    def collect(self, now: float) -> Tuple[List[Any], Optional[str]]:
        """One scan pass at live cadence. Returns (signals, err); err is a
        coverage-gap label (never raises out)."""
        try:
            self._maybe_reload_snapshot()
            universe = self.universe(now)
            if not universe:
                return [], "universe_empty"
            burst = self._pending_scan_burst(now)
            if burst > 0:
                # Requirement capped at 90% of capacity: a burst larger than
                # the bucket is legal (candle reads self-pace at the refill
                # rate via GlueReadOnlyClient) — the preflight only ensures
                # the scan starts from a (nearly) full bucket so the same
                # tick's exit reads were already served and the defer state
                # is always clearable (a 100% requirement would race the
                # exit reads every tick and never observe a full bucket).
                need = min(burst + SCAN_BUDGET_RESERVE,
                           self.budget.capacity * 0.9)
                if self.budget.available(now) < need:
                    return [], "scan_deferred_budget"
            open_positions = self._mirror_positions()
            no_long = ({a.symbol for a in universe
                        if getattr(a, "is_crypto", True)}
                       if self._crypto_short_only else set())  # main.py:43-45
            kwargs: Dict[str, Any] = dict(
                coins=universe, client=self.client,
                open_positions=open_positions,
                no_long_symbols=no_long, on_crypto_signal=None)
            if self.venue == "hl":
                try:
                    kwargs["account_coins"] = set(self.client.open_positions().keys())
                except Exception:  # noqa: BLE001 — live falls back the same way
                    kwargs["account_coins"] = set(open_positions.keys())
            signals = list(self.scanner.scan_all_coins(**kwargs))
            if self.venue == "hl" and signals:
                signals = self._hl_apply_selector(signals)
            return signals, None
        except Exception as e:  # noqa: BLE001 — NEVER crash the window (design §1)
            log.error("signal source failed this tick: %s", e, exc_info=True)
            return [], "signal_source_error:%s" % type(e).__name__


# ============================================================================
# Entry sizer — the venue-native attempt_entry DECISION-ONLY prefix
# ============================================================================

class EntrySizer:
    """Replicates trader.attempt_entry's gate/sizing prefix decision-only,
    using the venue's OWN bot.risk math (compute_size / check_concurrent_cap /
    check_mm_cap) and live reject labels — through read-only surfaces.

    Deliberate deltas vs live (decision-only, each visible in flags/inputs):
      * update_leverage is NOT called (venue write) — eff_lev is still
        computed as min(LEVERAGE, asset.max_leverage); flag
        'leverage_not_set_decision_only'.
      * no order is sent; the post-send fill/cap policy is out of scope here
        (that is the SM's t_fill policy, exercised by replay).
    """

    def __init__(self, venue: str, glue_client: Any, settings: Any,
                 snapshot_holder: Any,
                 universe_tiers_fn: Callable[[float], Mapping[str, int]],
                 bar_age_fn: Optional[Callable[[str], float]] = None) -> None:
        self.venue = venue
        self.client = glue_client
        self.settings = settings
        self.snapshot_holder = snapshot_holder
        self.universe_tiers_fn = universe_tiers_fn
        self.bar_age_fn = bar_age_fn
        self._risk = importlib.import_module("bot.risk")

    # live label helper — rejected_signals rows exist for these (observable)
    @staticmethod
    def _obs(reason: str, **inputs: Any) -> Dict[str, Any]:
        return {"ok": False, "reason": reason, "observable": True,
                "inputs": inputs, "flags": []}

    @staticmethod
    def _defer(reason: str, **inputs: Any) -> Dict[str, Any]:
        return {"ok": False, "reason": None, "defer_reason": reason,
                "observable": False, "inputs": inputs, "flags": []}

    def __call__(self, sig: Any) -> Dict[str, Any]:
        s = self.settings
        coin = str(getattr(sig, "coin", None) or (sig.get("coin") if isinstance(sig, Mapping) else ""))
        tf = str(getattr(sig, "tf", None) or (sig.get("tf") if isinstance(sig, Mapping) else ""))
        entry = float(getattr(sig, "entry_price", 0.0) or 0.0)
        sl = float(getattr(sig, "sl_price", 0.0) or 0.0)
        side = str(getattr(sig, "side", "long") or "long")
        flags: List[str] = ["leverage_not_set_decision_only"]

        # bar age gate (trader.py:127-140; live label preserved)
        if self.bar_age_fn is not None:
            bar_age = float(self.bar_age_fn(tf))
            gate = float(s.bar_age_gate_for(tf))
            if bar_age > gate:
                return self._obs(
                    "stale_signal: bar_age=%.0fs > %ds (tf=%s)"
                    % (bar_age, int(gate), tf), bar_age=bar_age, gate=gate)

        # tier gate (trader.py:142-147)
        tier = int(self.universe_tiers_fn(time.time()).get(coin, 2))
        if tier not in (1, 2):
            return self._obs("tier_excluded: TIER %d" % tier, tier=tier)

        # account state (read failures = live 'skip', unobservable)
        try:
            equity = float(self.client.account_value())
        except Exception as e:  # noqa: BLE001
            return self._defer("account_read_failed", detail=str(e))
        if equity <= 0:
            return self._obs("equity_zero_or_negative", equity=equity)
        try:
            positions = self.client.open_positions()
            n_open = len({id(v) for v in positions.values()})
        except Exception as e:  # noqa: BLE001
            return self._defer("open_positions_read_failed", detail=str(e))

        # shared-account merge guard (trader.py:178-181)
        if coin in positions or ("%s-USD" % coin) in positions:
            return self._obs("account_position_exists")

        allowed, reason = self._risk.check_concurrent_cap(n_open, s.max_concurrent)
        if not allowed:
            return self._obs(reason, n_open=n_open)

        # asset meta + eff_lev (NO update_leverage — decision-only)
        try:
            meta = self.client.asset(coin)
        except KeyError:
            return self._obs("asset_not_found: %s" % coin)
        except Exception as e:  # noqa: BLE001
            return self._defer("asset_meta_read_failed", detail=str(e))
        eff_lev = max(1, min(int(s.leverage), int(meta.max_leverage)))

        size_result = self._risk.compute_size(
            entry_price=entry, sl_price=sl, account_value=equity,
            settings=s, sz_decimals=meta.sz_decimals, leverage_eff=eff_lev)
        if size_result is None:
            return self._obs("size_compute_failed: risk too small for min_size",
                             equity=equity)
        target_notional = float(size_result.notional)

        # liquidity cap (snapshot-based; READ-ONLY — no inline fetch/bootstrap
        # writes; missing profile degrades with the live generic label).
        # F4: holder may be a callable accessor (build_shadow passes the
        # SignalSource-bound ref so a reloaded/late-loaded holder propagates).
        _holder = self.snapshot_holder() if callable(self.snapshot_holder) \
            else self.snapshot_holder
        snap = _holder.current() if _holder is not None else None
        liq_profile = snap.get(coin) if snap is not None else None
        if snap is not None:
            # F4: stamp snapshot vintage so a cap-binding size divergence
            # triages mechanically against live's holder vintage.
            flags.append("snapshot_vintage:%s"
                         % getattr(snap, "generated_at_utc", "?"))
        final_notional = target_notional
        if liq_profile is not None:
            liq_cap = float(s.liq_size_cap_pct) * float(liq_profile.avg_1h_vol_usd)
            final_notional = min(target_notional, liq_cap)
            if final_notional < float(s.liq_min_trade_usd):
                return self._obs(
                    "liq_below_min_trade: cap=$%.2f < min=$%.2f (1h_vol=$%.0f)"
                    % (liq_cap, s.liq_min_trade_usd, liq_profile.avg_1h_vol_usd),
                    liq_cap=liq_cap)
            if final_notional < target_notional:
                size_result = self._risk.compute_size(
                    entry_price=entry, sl_price=sl, account_value=equity,
                    settings=s, sz_decimals=meta.sz_decimals,
                    liquidity_cap_notional=final_notional, leverage_eff=eff_lev)
                if size_result is None:
                    return self._obs("size_after_liq_cap_zero")
        else:
            flags.append("liq_snapshot_missing_no_inline_fetch")

        if self.venue == "pacifica":
            # F1 (shadow-wiring review): pacifica /info min_order_size is USD
            # NOTIONAL (UNIT FIX 2026-07-02, live pacifica trader.py:280-292) —
            # a unit-compare here re-creates the fixed bug and Rejects nearly
            # every high-priced coin the live bot enters.
            _notional_usd = float(size_result.size) * entry
            if _notional_usd < float(meta.min_size):
                return self._obs(
                    "below_min_notional: $%.2f (size=%s) < venue_min=$%s for %s"
                    % (_notional_usd, size_result.size, meta.min_size, coin))
        elif float(size_result.size) < float(meta.min_size):
            return self._obs(
                "below_min_size: size=%s < extended_min=%s for %s"
                % (size_result.size, meta.min_size, coin))

        # MM cap vs LIVE margin (trader.py:286-315; fail-closed with positions)
        try:
            existing_margin = float(self.client.margin_used_usd())
        except Exception as e:  # noqa: BLE001
            if positions:
                return self._obs("mm_cap_margin_read_failed: %s" % e)
            existing_margin = 0.0
        mm_ok, mm_reason = self._risk.check_mm_cap(
            new_notional=float(size_result.notional), eff_lev=eff_lev,
            existing_margin_usd=existing_margin, account_value=equity,
            mm_cap_pct=s.mm_cap_pct)
        if not mm_ok:
            return self._obs(mm_reason, existing_margin=existing_margin)

        # limit_px — the F3 frozen cap-breach reference (trader.py:325-330)
        cap = float(s.entry_limit_cap_pct)
        limit_px = entry * (1 + cap) if side == "long" else entry * (1 - cap)
        try:
            limit_px = float(self.client.round_price(coin, limit_px))
        except Exception:  # noqa: BLE001 — rounding hook optional
            pass

        return {"ok": True, "reason": None, "observable": True,
                "size": float(size_result.size),
                "risk_dollars": float(size_result.risk_dollars),
                "notional": float(size_result.notional),
                "eff_lev": eff_lev, "limit_px": limit_px,
                "tp1_frac": float(getattr(s, "tp1_partial_frac", 0.0) or 0.0),
                "flags": flags,
                "inputs": {"equity": equity, "n_open": n_open,
                           "existing_margin": existing_margin,
                           "tier": tier,
                           "liq_capped": final_notional < target_notional}}

    def breakout_validity(self, sig: Any) -> Dict[str, Any]:
        """Pre-send breakout-validity guard (trader.py:357-375) — mark must
        still sit inside the cap band around the signal close."""
        coin = str(getattr(sig, "coin", "") or "")
        entry = float(getattr(sig, "entry_price", 0.0) or 0.0)
        side = str(getattr(sig, "side", "long") or "long")
        cap = float(self.settings.entry_limit_cap_pct)
        try:
            mark = float(self.client.mark_price(coin) or 0.0)
        except Exception:  # noqa: BLE001 — live treats unreadable mark as pass
            return {"ok": True, "flags": ["validity_mark_unreadable"]}
        if mark <= 0:
            return {"ok": True, "flags": ["validity_mark_unreadable"]}
        lo, hi = entry * (1 - cap), entry * (1 + cap)
        bad = (side == "long" and mark < lo) or (side != "long" and mark > hi)
        if bad:
            return {"ok": False, "observable": True,
                    "reason": "breakout_invalidated: mark=%.6f outside cap band "
                              "[%.6f,%.6f] (entry=%.6f cap=%.4f)"
                              % (mark, lo, hi, entry, cap),
                    "inputs": {"mark": mark}}
        return {"ok": True, "inputs": {"mark": mark}}


# ============================================================================
# Assembly
# ============================================================================

def build_shadow(venue: str, bot_root: Path, *,
                 live_db: Optional[Path] = None,
                 data_dir: Optional[Path] = None,
                 unit: Optional[str] = None,
                 budget_per_min: Optional[float] = None,
                 client: Any = None,
                 journal_provider: Optional[Callable[[float], Sequence[Mapping[str, Any]]]] = None,
                 signal_source: Optional[SignalSource] = None,
                 sizer: Optional[EntrySizer] = None) -> ShadowRunner:
    """Assemble the per-venue ShadowRunner (design §1 topology). Every
    argument is injectable for the offline selftest; production uses the
    documented defaults."""
    if venue not in VENUES:
        raise SystemExit("unknown venue %r (expected %s)" % (venue, "|".join(VENUES)))
    bot_root = Path(bot_root)
    live_db = Path(live_db) if live_db else bot_root / "data" / "trades.db"
    data_dir = Path(data_dir) if data_dir else bot_root / "data" / "shadow"
    unit = unit or VENUE_LIVE_UNITS[venue]
    budget = float(budget_per_min if budget_per_min is not None
                   else VENUE_READ_BUDGET_PER_MIN[venue])

    os.environ["DRY_RUN"] = "1"          # belt-and-braces (design §2)
    if str(bot_root) not in sys.path:
        sys.path.insert(0, str(bot_root))

    if not live_db.exists():
        raise SystemExit("live trades.db not found at %s — the shadow mirrors "
                         "the LIVE bot's DB (read-only); check --bot-root" % live_db)

    # 1. venue client (P2 binding) + write fence + shared budget
    if client is None:
        from fleet_core import venues as venue_pkg
        client = venue_pkg.get_client(venue)
    read_budget = ReadBudget(budget)
    fenced = ReadOnlyExchangeClient(client, read_budget, venue=venue)
    glue_client = GlueReadOnlyClient(fenced, client, budget=read_budget)

    # 2. mirrored live suppression state (F6)
    provider = journal_provider or JournalctlProvider(unit)
    suppression = SuppressionMirror(venue, str(live_db), provider)

    # 3. signal source (venue's own scanner, decision-only) + sizer.
    # F3 (shadow-wiring review): bot-package import failure at assembly must
    # DEGRADE to an exit-only shadow with a loud per-tick coverage gap, not a
    # systemd 30s crash-loop (failure-mode-6 law). _DegradedSignalSource
    # retries the real assembly every 30 collects.
    if signal_source is not None:
        source = signal_source
    else:
        try:
            source = SignalSource(venue, glue_client, read_budget, str(live_db))
        except Exception as e:  # noqa: BLE001 — degrade loud, retry inside
            log.critical(
                "shadow entry-side assembly FAILED (%s) — starting EXIT-ONLY "
                "shadow; entry decisions are a coverage gap until assembly "
                "heals (retried every 30 ticks)", e)
            source = _DegradedSignalSource(
                venue, glue_client, read_budget, str(live_db), reason=str(e))
    if sizer is None and not isinstance(source, _DegradedSignalSource):
        sizer = EntrySizer(venue, glue_client, source.settings,
                           # F4: callable holder ref — reload/late-load propagates
                           (lambda: source.snapshot_holder), source.universe_tiers,
                           bar_age_fn=getattr(source.scanner, "bar_age_sec", None))

    # 4. decision cores — the REAL pinned pipelines, ctx-enriched
    from fleet_core.engine import entry_sm as esm_mod
    from fleet_core.engine import exit_engine as xng_mod
    reader = LiveRowReader(str(live_db))

    def exit_decider(ctx: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
        snap = reader.snapshot(float(ctx.get("now") or time.time()))
        ctx2 = dict(ctx)
        ctx2.setdefault("live_db_path", str(live_db))
        ctx2.setdefault("db_open_coins", snap["open_coins"])
        ctx2.setdefault("manual_coins", snap["manual_coins"])
        ctx2.setdefault("registry_oids", snap["registry_oids"])
        row = ctx.get("row")
        tid = int(getattr(row, "trade_id", 0) or 0)
        if tid in snap["rows_full"]:
            ctx2.setdefault("row_full", snap["rows_full"][tid])
        return xng_mod.shadow_decide(ctx2)

    def entry_decider(ctx: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
        now = float(ctx.get("now") or time.time())
        signals, err = source.collect(now)
        if err is not None:
            # coverage gap, never a crash (design §1/§4)
            return [{"phase": "entry_gate", "decision": "shadow_tick_skipped",
                     "params": {"cause": "signal_source", "detail": err},
                     "flags": ["coverage_gap"]}]
        if not signals:
            return []
        # F3: sizer may have been built late by a healed degraded source
        _sizer = sizer if sizer is not None else getattr(source, "sizer", None)
        if _sizer is None:
            return [{"phase": "entry_gate", "decision": "shadow_tick_skipped",
                     "params": {"cause": "sizer_unassembled",
                                "detail": "entry-side degraded"},
                     "flags": ["coverage_gap"]}]
        snap = reader.snapshot(now)
        s = source.settings
        ctx2 = dict(ctx)
        ctx2["signals"] = signals
        # live in-memory open set ≈ mirrored open rows + pending (pre-order rows)
        ctx2.setdefault("open_coins",
                        list(snap["open_coins"]) + list(snap["pending_coins"]))
        ctx2.setdefault("max_concurrent", int(getattr(s, "max_concurrent", 0) or 0))
        ctx2.setdefault("max_opens_per_day",
                        int(getattr(s, "max_opens_per_day", 0) or 0))
        ctx2.setdefault("sizing", _sizer)
        ctx2.setdefault("breakout_validity", _sizer.breakout_validity)
        return esm_mod.shadow_decide(ctx2)

    runner = ShadowRunner(
        venue=venue,
        client=fenced,
        live_db_path=str(live_db),
        suppression=suppression,
        decisions_path=str(data_dir / "shadow_decisions.jsonl"),
        state_path=str(data_dir / "shadow_state.db"),
        venue_budget_per_min=budget,
        exit_decider=exit_decider,
        entry_decider=entry_decider,
    )
    log.info("shadow assembled: venue=%s unit=%s live_db=%s budget=%.0f/min "
             "(shadow cap %.0f/min by the 50%% LAW) decisions=%s",
             venue, unit, live_db, budget, read_budget.capacity,
             data_dir / "shadow_decisions.jsonl")
    return runner


# ============================================================================
# CLI
# ============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="fleet_core.engine.shadow_main",
        description="P3 48h shadow launcher (DRY, read-only, one venue per process)")
    p.add_argument("--venue", choices=VENUES)
    p.add_argument("--bot-root", help="live bot root (WorkingDirectory of the unit)")
    p.add_argument("--live-db", default=None,
                   help="override live trades.db path (default <bot-root>/data/trades.db)")
    p.add_argument("--data-dir", default=None,
                   help="shadow output dir (default <bot-root>/data/shadow)")
    p.add_argument("--unit", default=None,
                   help="live bot systemd unit for journalctl mirroring "
                        "(default per-venue map)")
    p.add_argument("--budget-per-min", type=float, default=None,
                   help="override the venue read budget (req/min); the 50%% "
                        "shadow LAW still applies on top")
    p.add_argument("--once", action="store_true",
                   help="run ONE tick and exit (bring-up smoke)")
    p.add_argument("--rehearse", action="store_true",
                   help="run the §8 cutover-adoption rehearsal once and exit")
    p.add_argument("--selftest", action="store_true",
                   help="offline selftest with fakes (no venue SDKs, no live I/O)")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout)

    if args.selftest:
        return _selftest()

    if not args.venue or not args.bot_root:
        p.error("--venue and --bot-root are required to run the shadow")

    runner = build_shadow(
        args.venue, Path(args.bot_root),
        live_db=Path(args.live_db) if args.live_db else None,
        data_dir=Path(args.data_dir) if args.data_dir else None,
        unit=args.unit, budget_per_min=args.budget_per_min)

    runner.startup_selftest()            # write fence proven before tick 1 (§2)
    if args.rehearse:
        res = runner.adoption_rehearsal()
        print(json.dumps(res, indent=1, default=str))
        return 0 if res.get("passed") else 1
    if args.once:
        recs = runner.run_tick()
        print(json.dumps({"tick_records": len(recs)}, indent=1))
        return 0

    import signal as _signal

    def _stop(_s: int, _f: Any) -> None:
        runner.stop()

    _signal.signal(_signal.SIGTERM, _stop)
    _signal.signal(_signal.SIGINT, _stop)
    runner.run_forever()
    return 0


# ============================================================================
# Offline selftest — launcher assembly with fakes (design §2/§4 checks)
# ============================================================================

def _selftest() -> int:  # pragma: no cover — exercised via CLI
    import hashlib
    import tempfile
    import types

    failures: List[str] = []
    n = [0]

    def check(name: str, cond: bool, detail: str = "") -> None:
        n[0] += 1
        print("  [%s] %s %s" % ("ok" if cond else "FAIL", name, detail))
        if not cond:
            failures.append(name)

    try:
        import pandas as pd
    except ImportError:
        print("pandas required for the shadow selftest (decision dfs)")
        return 1

    from fleet_core.exchange_api import (ExchangeClient, OpenOrderInfo,
                                         PositionInfo, ReadUnknown)

    # ---- deterministic clock: noon UTC today (suppression-mirror lesson) ----
    now0 = float(int(time.time() // 86400) * 86400 + 43200)
    tf = "8h"
    step_ms = tf_to_ms(tf)
    last_closed_start_ms = (int(now0 * 1000) // step_ms) * step_ms - step_ms

    # ---- synthetic closed-bar df: flat 110s with ONE strict pivot low @106
    # near the end (TRAIL_PIVOT_WINDOW=2 in the pinned module) so the REAL
    # v10d pivot-low vstop can propose 106×(1−0.005)=105.47 — above the row's
    # sl_current=101 and > 5 bps from sl_placed_px ⇒ a genuine trail decision
    # through the pinned math. Highs stay < entry+max_run_r×R (125) so the
    # 5R take stays inert; lows stay > sl_current so check_sl_hit is quiet.
    def make_df(n_bars: int = 36, end_ms: int = last_closed_start_ms) -> "pd.DataFrame":
        first = end_ms - (n_bars - 1) * step_ms
        rows = []
        pivot_i = n_bars - 5           # k ≤ (i−1)−window ⇒ confirmable pivot
        for i in range(n_bars):
            lo = 106.0 if i == pivot_i else 109.0
            rows.append({"time": pd.Timestamp(first + i * step_ms, unit="ms", tz="UTC"),
                         "Open": 110.0, "High": 111.0, "Low": lo,
                         "Close": 110.0, "Volume": 1000.0})
        return pd.DataFrame(rows)

    df = make_df()
    entry_bar_ts = last_closed_start_ms - 6 * step_ms   # entered 6 bars ago

    # ---- fake venue client (P2 shapes; writes = AssertionError if reached) --
    class FakeClient(ExchangeClient):
        def __init__(self) -> None:
            self.df = df                     # mutable: selftest advances the bar
            # per-coin marks: held coin at last close; candidates AT their
            # signal close (breakout still valid — live guard must pass)
            self.marks = {"BTC": float(df["Close"].iloc[-1]),
                          "ETH": 200.0, "SOL": 50.0}

        def market_open(self, coin, is_buy, sz, intended_px=None, allow_marketable=True):
            raise AssertionError("inner write reached — fence broken")

        def ensure_flat(self, coin):
            raise AssertionError("inner write reached — fence broken")

        def trigger_sl(self, coin, is_buy, sz, trigger_px):
            raise AssertionError("inner write reached — fence broken")

        def cancel_sl_order(self, coin, oid):
            raise AssertionError("inner write reached — fence broken")

        def limit_reduce_only(self, coin, is_buy, sz, px):
            raise AssertionError("inner write reached — fence broken")

        def update_leverage(self, coin, leverage, is_cross=True):
            raise AssertionError("inner write reached — fence broken")

        def open_positions(self):
            return {"BTC": PositionInfo(coin="BTC", size_signed=0.5, entry_px=100.0)}

        def open_orders(self):
            return []

        def list_open_sl_orders(self, coin):
            return ["oid_sl_1"] if coin == "BTC" else []

        def list_reduce_only_triggers(self):
            return [OpenOrderInfo(coin="BTC", oid="oid_sl_1", side="sell", size=0.5,
                                  trigger_px=101.0, reduce_only=True, is_trigger=True)]

        def mark_price(self, coin, max_age_sec=5.0):
            return self.marks.get(coin, float(self.df["Close"].iloc[-1]))

        def candles(self, coin, interval, limit=200, max_stale_bars=1.0):
            if interval != tf:
                raise ReadUnknown("no such tf in fake")
            return self.df.copy()

        def equity_with_upnl(self):
            return 50_000.0

        def account_value(self):
            return 50_000.0

        def margin_used_usd(self):
            return 500.0

        def position_liquidation(self, coin):
            return 60.0

        def user_fills(self, max_age_sec=60.0):
            return []

        # contract-exempt reads the live scanner/sizer uses
        def asset(self, coin):
            return SimpleNamespace(max_leverage=20, sz_decimals=3, min_size=0.001,
                                   name=coin)

        def round_price(self, coin, px):
            return round(float(px), 6)

        def invalidate_candles_cache(self, coin=None):
            return None

    # ---- fake `bot` package injected via sys.modules ------------------------
    class FakeSettings:
        working_tfs = [tf]
        short_enabled_tfs: List[str] = []
        max_concurrent = 5
        max_opens_per_day = 3
        risk_per_trade = 0.005
        leverage = 10
        mm_cap_pct = 0.50
        entry_limit_cap_pct = 0.0025
        liq_size_cap_pct = 0.05
        liq_min_trade_usd = 20.0
        liq_snapshot_path = "data/liquidity_snapshot.json"
        universe_refresh_min = 60
        min_sl_dist_pct = 0.005
        tp1_partial_frac = 0.5

        @classmethod
        def from_env(cls):
            return cls()

        def bar_age_gate_for(self, _tf):
            return 10 ** 9   # never stale in the offline harness

    sig_eth = SimpleNamespace(coin="ETH", tf=tf, side="long",
                              trigger_price=200.0, entry_price=200.0,
                              sl_price=190.0, tp1_price=220.0,
                              sl_dist_pct=0.05, pivot_high=200.0, pivot_low=190.0,
                              bar_ts=last_closed_start_ms, atr14=2.0,
                              ema20=195.0, f1_dist=1.0)
    sig_sol = SimpleNamespace(coin="SOL", tf=tf, side="long",
                              trigger_price=50.0, entry_price=50.0,
                              sl_price=47.0, tp1_price=56.0,
                              sl_dist_pct=0.06, pivot_high=50.0, pivot_low=47.0,
                              bar_ts=last_closed_start_ms, atr14=0.9,
                              ema20=48.0, f1_dist=1.2)
    sig_btc = SimpleNamespace(coin="BTC", tf=tf, side="long",   # already open
                              trigger_price=110.0, entry_price=110.0,
                              sl_price=100.0, tp1_price=120.0,
                              sl_dist_pct=0.09, pivot_high=110.0, pivot_low=100.0,
                              bar_ts=last_closed_start_ms, atr14=1.5,
                              ema20=105.0, f1_dist=0.8)

    class FakeScanner:
        def __init__(self, cfg):
            self.cfg = cfg
            self._last_bar_ts: Dict[str, int] = {}

        def bar_age_sec(self, _tf):
            return 5.0

        def scan_all_coins(self, coins, client, open_positions,
                           no_long_symbols=None, on_crypto_signal=None):
            # consumes the bar exactly like the live scanner
            self._last_bar_ts[tf] = last_closed_start_ms
            # cross-side dedup: BTC held → live scanner would skip it; keep it
            # here so the ENTRY chain's already_open gate is exercised too.
            _ = client.candles("ETH", tf, limit=100)   # budgeted read path
            return [sig_btc, sig_eth, sig_sol]

    class _Tier:
        def __init__(self, symbol, tier=1, is_crypto=True):
            self.symbol, self.tier, self.is_crypto = symbol, tier, is_crypto

    def fake_load_universe(force_refresh=False, *a, **k):
        return [_Tier("BTC"), _Tier("ETH"), _Tier("SOL")]

    class _LiqProfile:
        avg_1h_vol_usd = 1_000_000.0

    class _Snap(dict):
        age_seconds = 0.0

    def fake_load_snapshot(_p):
        return _Snap(ETH=_LiqProfile(), SOL=_LiqProfile(), BTC=_LiqProfile())

    class FakeSnapHolder:
        def __init__(self, snap):
            self._snap = snap

        def current(self):
            return self._snap

    # bot.risk — the REAL shared math (fleet_core.risk is the canonical byte
    # source the venue shims re-export; importing it under the fake bot
    # package exercises the exact live formulas).
    bot_pkg = types.ModuleType("bot")
    bot_pkg.__path__ = []  # mark as package
    cfg_mod = types.ModuleType("bot.config")
    cfg_mod.Settings = FakeSettings
    cfg_mod.TF_MS = {tf: step_ms}
    cfg_mod.FX_EXCLUDE = set()
    scan_mod = types.ModuleType("bot.scanner")
    scan_mod.Scanner = FakeScanner
    uni_mod = types.ModuleType("bot.universe")
    uni_mod.load_universe = fake_load_universe
    uni_mod.AssetTier = _Tier
    uni_mod.UNIVERSE_SYMBOL_EXCLUDE = set()
    liq_mod = types.ModuleType("bot.liquidity")
    liq_mod.load_snapshot = fake_load_snapshot
    liq_mod.SnapshotHolder = FakeSnapHolder
    for name, mod in [("bot", bot_pkg), ("bot.config", cfg_mod),
                      ("bot.scanner", scan_mod), ("bot.universe", uni_mod),
                      ("bot.liquidity", liq_mod)]:
        sys.modules[name] = mod
    import fleet_core.risk as _real_risk
    sys.modules["bot.risk"] = _real_risk

    rc = 0
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "data").mkdir()
        live_db = root / "data" / "trades.db"
        con = sqlite3.connect(str(live_db))
        con.executescript(
            """
            CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, coin TEXT, tf TEXT, direction TEXT,
                entry REAL, size REAL, sl_initial REAL, sl_current REAL,
                sl_order_id TEXT, tp1 REAL, tp1_partial_done INTEGER DEFAULT 0,
                tp1_fill_price REAL, tp1_frac_at_entry REAL DEFAULT 0.5,
                risk_dollars REAL, atr14 REAL, entry_bar_ts INTEGER,
                pattern TEXT, opened_at TEXT, closed_at TEXT,
                exit_price REAL, exit_reason TEXT, realized_r REAL, status TEXT);
            CREATE TABLE rejected_signals (id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, coin TEXT, tf TEXT, direction TEXT,
                trigger_price REAL, entry_price REAL, sl_price REAL, reason TEXT);
            """)
        iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(t))
        con.execute(
            "INSERT INTO trades (created_at, coin, tf, direction, entry, size, "
            "sl_initial, sl_current, sl_order_id, tp1, entry_bar_ts, atr14, "
            "pattern, opened_at, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (iso(now0 - 6 * step_ms / 1000), "BTC", tf, "long", 100.0, 0.5,
             95.0, 101.0, "oid_sl_1", 120.0, entry_bar_ts, 1.5,
             "xnn_breakout", iso(now0 - 6 * step_ms / 1000 + 3), "open"))
        con.commit()
        con.close()
        live_md5_before = hashlib.md5(live_db.read_bytes()).hexdigest()

        # -- 1. assembly (fakes injected; REAL pinned pipelines resolved) ----
        os.environ.setdefault("TRAIL_AFTER_TP_BUFFER_PCT", "0.003")
        fake = FakeClient()
        runner = build_shadow(
            "extended", root, client=fake,
            journal_provider=lambda since: [],
            budget_per_min=1000.0)
        check("assembly: runner built with fenced client",
              isinstance(runner.client, ReadOnlyExchangeClient))

        # -- 2. write fence selftest green (design §2 hard invariant) --------
        try:
            runner.startup_selftest()
            check("write fence selftest green", True)
        except Exception as e:  # noqa: BLE001
            check("write fence selftest green", False, repr(e))

        # glue proxy: write blocked BEFORE I/O, exempt read passes through
        glue = GlueReadOnlyClient(runner.client, fake)
        try:
            glue.ensure_flat("BTC")
            check("glue proxy blocks writes", False, "returned")
        except ShadowWriteBlocked:
            check("glue proxy blocks writes", True)
        check("glue proxy passes exempt reads",
              glue.asset("ETH").max_leverage == 20)

        # -- 3. ONE full tick through the REAL pinned pipelines --------------
        recs = runner.run_tick(now=now0)
        by_phase: Dict[str, List[Mapping[str, Any]]] = {}
        for r in recs:
            by_phase.setdefault(str(r["phase"]), []).append(r)
        check("tick heartbeat emitted", "tick" in by_phase)
        exit_recs = [r for r in recs
                     if r["phase"] in ("pm", "trail", "heal", "sl_hit", "exit",
                                       "tp1_partial", "phantom")]
        check("exit decisions produced (real tick() pipeline)",
              len(exit_recs) >= 1,
              "phases=%s" % sorted(by_phase))
        pm_recs = [r for r in recs if r["phase"] in ("pm", "trail")]
        # v10d STAGES trail_{i-1} on bar i and promotes on bar i+1 (bt-parity);
        # on the entry tick the correct decision is therefore a labeled NoOp.
        check("exit side reached steps 9-14 (pinned PM ran; steady NoOp on bar i)",
              any(r["decision"] in ("NoOp", "Defer", "ReplaceSL") for r in pm_recs),
              pm_recs[0]["params"].get("reason", "") if pm_recs else "none")
        entry_recs = by_phase.get("entry_gate", [])
        check("entry decisions produced", len(entry_recs) >= 2,
              "n=%d" % len(entry_recs))
        by_coin = {r["coin"]: r for r in entry_recs}
        check("already-open coin deferred (unobservable skip → Defer)",
              by_coin.get("BTC", {}).get("decision") == "Defer"
              and by_coin.get("BTC", {}).get("params", {}).get("reason") == "already_open",
              str(by_coin.get("BTC", {}).get("params")))
        eth = by_coin.get("ETH", {})
        check("clean signal → EnterIntent with size/eff_lev/limit_px",
              eth.get("decision") == "EnterIntent"
              and eth.get("params", {}).get("size")
              and eth.get("params", {}).get("eff_lev") == 10
              and eth.get("params", {}).get("limit_px"),
              str(eth.get("params", {}).get("size")))
        if eth:
            # size = risk% × equity / dist through the REAL fleet_core.risk
            want = 50_000.0 * 0.005 / (200.0 - 190.0)
            got = float(eth["params"]["size"] or 0)
            check("size math = live compute_size (risk×eq/dist, rounded)",
                  abs(got - round(want, 3)) < 1e-9, "%s vs %s" % (got, want))
            check("suppression snapshot stamped on entry records (F6)",
                  "suppression" in eth.get("inputs", {}))
            check("entry verdict chain recorded (gates list)",
                  isinstance(eth.get("params", {}).get("gates"), list)
                  and any(g.get("gate") == "entry_cooldown"
                          for g in eth["params"]["gates"]))

        # -- 3b. BARE lazy-decider path: a ShadowRunner constructed WITHOUT
        #        injected deciders must resolve exit_engine.shadow_decide /
        #        entry_sm.shadow_decide via _lazy_decider and tick cleanly
        #        (this was the P3 unwired gap) ------------------------------
        from fleet_core.engine.suppression_mirror import SuppressionMirror as _SM
        bare = ShadowRunner(
            "extended", FakeClient(), str(live_db),
            _SM("extended", str(live_db), lambda since: []),
            decisions_path=str(root / "data" / "shadow" / "bare.jsonl"),
            state_path=str(root / "data" / "shadow" / "bare_state.db"),
            venue_budget_per_min=1000.0)
        check("lazy deciders resolved (exit_engine/entry_sm shadow_decide)",
              bare.exit_decider is not None and bare.entry_decider is not None)
        bare_recs = bare.run_tick(now=now0)
        check("bare runner tick: exit decisions flow, no decider_missing",
              any(r["phase"] in ("pm", "trail") for r in bare_recs)
              and not any("decider_missing" in (r.get("flags") or [])
                          for r in bare_recs),
              str([(r["phase"], r["decision"]) for r in bare_recs])[:160])

        # -- 4. mirrored suppression drives the cooldown gate (F6) -----------
        runner.suppression.journal_provider = lambda since: [
            {"kind": "abort", "ts": now0 + 30, "coin": "SOL", "raw": "abort"}]
        recs2 = runner.run_tick(now=now0 + 60)
        entry2 = [r for r in recs2 if r["phase"] == "entry_gate"]
        sol2 = [r for r in entry2 if r["coin"] == "SOL"]
        check("mirrored live abort → SOL rejected entry_cooldown_active (F6/KF11)",
              bool(sol2) and sol2[0]["decision"] == "Reject"
              and sol2[0]["params"].get("reason") == "entry_cooldown_active",
              str(sol2[0]["params"] if sol2 else "no SOL record"))

        # -- 5. graceful degrade: signal source failure → coverage gap -------
        # (the entry decider closure holds the SignalSource singleton; a
        # class-level monkeypatch reaches it for exactly one tick)
        orig_collect = SignalSource.collect

        def failing_collect(self, now):  # noqa: ANN001, ARG001
            return [], "signal_source_error:RuntimeError"

        SignalSource.collect = failing_collect  # type: ignore[method-assign]
        try:
            recs3 = runner.run_tick(now=now0 + 120)
        finally:
            SignalSource.collect = orig_collect  # type: ignore[method-assign]
        skipped = [r for r in recs3 if r["decision"] == "shadow_tick_skipped"
                   and r["phase"] == "entry_gate"]
        check("signal-source failure → shadow_tick_skipped coverage record, no crash",
              bool(skipped) and skipped[0]["params"].get("cause") == "signal_source")

        # -- 6. bar advance → the REAL pinned staged trail promotes ----------
        # (v10d: trail_{i-1} staged on bar i, promoted on the first tick of
        # bar i+1 — bt-parity; the promotion must surface as a ReplaceSL
        # trail decision through the engine churn gate)
        new_end = last_closed_start_ms + step_ms
        fake.df = make_df(37, end_ms=new_end)
        now4 = now0 + step_ms / 1000.0 + 5.0
        recs4 = runner.run_tick(now=now4)
        trail4 = [r for r in recs4 if r["phase"] == "trail"
                  and r["decision"] == "ReplaceSL"]
        check("staged trail promoted on the new bar (REAL pinned v10d path)",
              bool(trail4), str([(r["phase"], r["decision"]) for r in recs4
                                 if r["coin"] == "BTC"]))
        if trail4:
            want_sl = 106.0 * (1.0 - 0.005)   # pivot low × (1−TRAIL_VSTOP_BUFFER)
            got_sl = float(trail4[0]["params"].get("target_px") or 0.0)
            check("trail target = pinned pivot-low vstop math (106×0.995)",
                  abs(got_sl - want_sl) < 1e-9, "%s vs %s" % (got_sl, want_sl))
            check("trail decision carries placed px + cause (§4 params)",
                  trail4[0]["params"].get("cause") == "trail"
                  and trail4[0]["params"].get("placed_px") == 101.0)
            check("trail bar_ts = the NEW last closed bar (alignment key)",
                  trail4[0]["bar_ts"] == new_end,
                  "%s vs %s" % (trail4[0]["bar_ts"], new_end))

        # -- 7. zero live-db writes across ALL ticks (LAW: mirror is mode=ro)
        check("live trades.db byte-identical after 4 ticks (zero writes)",
              hashlib.md5(live_db.read_bytes()).hexdigest() == live_md5_before)
        check("shadow decisions land under data/shadow/",
              (root / "data" / "shadow" / "shadow_decisions.jsonl").exists())

        # -- 8. comparator over the produced decision log (schema contract) --
        from fleet_core.engine import comparator as cmp_mod
        sev = cmp_mod.shadow_events_from_log(runner.log.read_all())
        classes = {e.event_class for e in sev}
        check("comparator maps EnterIntent → entry_decision",
              "entry_decision" in classes, str(classes))
        check("comparator maps trail ReplaceSL → sl_replace",
              "sl_replace" in classes, str(classes))
        check("comparator maps cooldown Reject → reject",
              "reject" in classes, str(classes))
        extractor = cmp_mod.LiveExtractor("extended", str(live_db))
        lev = extractor.poll()
        pairs = cmp_mod.align(sev, lev)
        check("comparator aligns shadow vs live extraction without error",
              isinstance(pairs, list) and len(pairs) >= 1, "pairs=%d" % len(pairs))

        # -- 9. budget preflight defers the scan burst (bar kept pending) ----
        starved_budget = ReadBudget(2.0)   # capacity 1 token
        starved_budget.try_acquire(1.0)    # drain: bucket now ~empty
        src2 = SignalSource("extended",
                            GlueReadOnlyClient(
                                ReadOnlyExchangeClient(FakeClient(), starved_budget,
                                                       venue="extended"),
                                FakeClient()),
                            starved_budget, str(live_db))
        sigs, err = src2.collect(time.time())
        check("scan burst deferred on starved budget (coverage gap, bar pending)",
              err == "scan_deferred_budget" and sigs == [],
              str(err))

        # -- 9b. candle reads SELF-PACE on an emptied bucket (no tail-coin
        #        loss: the glue waits for the refill instead of raising) -----
        pace_budget = ReadBudget(1200.0)          # capacity 600, refill 10/s
        pace_fenced = ReadOnlyExchangeClient(FakeClient(), pace_budget,
                                             venue="extended")
        while pace_budget.try_acquire(50.0):      # drain to < 1 token
            pass
        while pace_budget.try_acquire(1.0):
            pass
        sleeps: List[float] = []

        def _rec_sleep(s: float) -> None:
            sleeps.append(s)
            time.sleep(min(s, 0.12))              # real refill, bounded test time
        pace_glue = GlueReadOnlyClient(pace_fenced, FakeClient(),
                                       budget=pace_budget, sleep_fn=_rec_sleep)
        try:
            got_df = pace_glue.candles("BTC", tf, limit=50)
            check("glue candles self-paced through an empty bucket",
                  got_df is not None and len(got_df) > 0 and len(sleeps) >= 1,
                  "waits=%d" % len(sleeps))
        except Exception as e:  # noqa: BLE001
            check("glue candles self-paced through an empty bucket", False, repr(e))

        # -- 10. venue budget map sanity (labeled derivations) ---------------
        check("pacifica budget = 428/min (140ms pacer derivation)",
              VENUE_READ_BUDGET_PER_MIN["pacifica"] == 428.0)
        check("all venues have a live-unit mapping",
              set(VENUE_LIVE_UNITS) == set(VENUES)
              and VENUE_LIVE_UNITS["hl"] == "valantis-bot")

    print()
    print("== shadow_main selftest: %d checks, %d failures ==" % (n[0], len(failures)))
    if failures:
        for f in failures:
            print("  FAILED: %s" % f)
        print("SHADOW_MAIN SELFTEST RED")
        rc = 1
    else:
        print("SHADOW_MAIN SELFTEST GREEN (offline, fakes only)")
    return rc


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
