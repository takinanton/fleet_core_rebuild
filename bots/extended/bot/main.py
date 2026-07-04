"""main.py — unified bot main loop for Extended + Pacifica perps.

Usage:
  python -m bot.main                # live trading
  python -m bot.main --dry-run      # signals logged, no orders placed
  python -m bot.main --once         # single loop iteration then exit
  python -m bot.universe            # print filtered universe and exit

Exchange dispatch: settings.exchange in {"extended","pacifica"} chooses client.
Strategy: UK v102 ZigZag breakout with per-TF F1/F2/F3 filters.
TFs: Acct A = 1h+2h  |  Acct B = 4h+1d  (set WORKING_TFS in .env)
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Dict

from bot.config import PROJECT_ROOT, Settings, settings, FX_EXCLUDE, FOREIGN_SKIP_PREFIXES
from bot.journal import (
    delete_pending, init_db, open_trades, pending_trades, promote_pending,
)
from bot.liquidity import SnapshotHolder, load_snapshot
from bot.scanner import Scanner, drop_forming_bars
from bot.strategy_xnn import Position, PositionManager
from bot.trader import (
    _place_sl_with_retry, attempt_entry, ensure_sl_inside_liq, manage_open_position,
)
from bot.universe import AssetTier, load_universe
from bot.orphan_sweep import sweep_orphan_triggers
import os

CRYPTO_SHORT_ONLY = os.getenv("CRYPTO_SHORT_ONLY", "").strip().lower() in ("1", "true", "yes", "on")

# XNN patch 2026-06-11 (canon §0.8): DRY_RUN env — systemd ExecStart has no --dry-run flag
# (systemd_units.txt:12,34), so env MUST be able to force dry. CLI flag OR env = dry.
DRY_RUN_ENV = os.getenv("DRY_RUN", "").strip().lower() in ("1", "true", "yes", "on")

def _no_long_symbols(universe) -> set:
    # crypto = Extended API category 'Crypto'; TradFi (metals/energy/idx/equities) stays bidirectional
    return {a.symbol for a in universe if getattr(a, "is_crypto", True)}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _build_client(cfg: Settings):
    """Dispatch exchange adapter based on settings.exchange."""
    if cfg.exchange == "pacifica":
        from bot.exchange_pacifica import PacificaClient
        return PacificaClient(cfg)
    if cfg.exchange == "extended":
        from bot.exchange_extended import ExtendedClient
        return ExtendedClient(cfg)
    raise RuntimeError(f"Unknown exchange={cfg.exchange!r}")


def _reconcile_pending(client, dry_run: bool = False) -> None:
    """write-db-row-PRE-order reconcile (phase0 2026-07-02, port of hl canon 2026-06-21).
    A status='pending' row is a pre-order trace written by attempt_entry BEFORE the market
    order is sent that never reached promote — i.e. a crash between order-send and
    journal-promote. At startup, for each pending row:
      * a LIVE exchange position exists on the coin AND no 'open' row already covers it
            -> PROMOTE to 'open' (best-effort intended entry/size) so the normal adopt path
               manages it + heals its SL (closes the crash-mid-entry naked window at SOURCE).
      * else (no live position, or already covered by an open row)
            -> DELETE the stale pending row.
    DRY: skipped (no live orders → no pending rows). Any read failure leaves pending rows
    intact (conservative — a later restart retries)."""
    if dry_run:
        return
    try:
        pend = pending_trades()
    except Exception as e:
        log.warning("reconcile_pending: pending_trades() failed: %s — skipping", e)
        return
    if not pend:
        return
    try:
        ex = client.open_positions() or {}
    except Exception as e:
        log.warning("reconcile_pending: open_positions() failed: %s — leaving %d pending "
                    "row(s) for the next restart", e, len(pend))
        return
    try:
        open_coins = {str(r["coin"]) for r in (open_trades() or [])}
    except Exception:
        open_coins = set()
    for r in pend:
        coin = str(r["coin"])
        live = (coin in ex) or (f"{coin}-USD" in ex)
        if live and coin not in open_coins:
            promote_pending(r["id"], entry=r["entry"], size=r["size"])
            open_coins.add(coin)
            log.critical(
                "RECONCILE-PENDING %s: crash-mid-entry pending row id=%s PROMOTED to open "
                "(live position found) — adopt will heal its SL. Verify entry/size vs exchange.",
                coin, r["id"],
            )
        else:
            delete_pending(r["id"])
            log.warning("reconcile_pending: deleted stale pending %s id=%s (live=%s already_open=%s)",
                        coin, r["id"], live, coin in open_coins)


def _adopt_db_open_positions(
    client,
    existing: Dict[str, Position] | None = None,
    startup: bool = False,
    dry_run: bool = False,
) -> Dict[str, Position]:
    """Adopt DB-open trades that are live on the exchange but not yet tracked in-memory.

    Single source for BOTH restart-restore (startup=True) and the per-tick reconciler
    (startup=False). The per-tick call is the class fix for naked manual/resting entries
    (2026-06-07; mem:feedback_order_placement_readback_and_invariants): a stop-entry that
    fills WHILE the bot runs but is not in the in-memory dict is otherwise never managed →
    its SL is never healed → NAKED until a post-fill restart. Re-running adopt every loop
    picks it up within ONE tick, after which the SL-liveness guard in manage_open_position
    places/confirms the SL (≤1 further tick). Only coins with a DB-open trade row are adopted
    (a manual position with no DB row — e.g. BNB — is deliberately never auto-managed here);
    in-memory positions are preserved as-is (never clobber trail state).
    """
    positions: Dict[str, Position] = dict(existing) if existing else {}
    # XNN patch 2026-06-11 (canon §0.9): in DRY the bot must never take over / heal /
    # emergency-close ANY position (old extended-bot a/b keep running on the SAME
    # account and manage their own positions until the flip). Adopt is OFF in DRY.
    if dry_run:
        if startup:
            log.info("DRY_RUN: position adopt/restore SKIPPED (foreign-safety)")
        return positions
    try:
        db_open = open_trades()
    except Exception as e:
        log.error("open_trades() DB query failed: %s", e)
        return positions

    try:
        exchange_pos = client.open_positions()
        # STARTUP BLIP GUARD: a transient-empty open_positions() at the restart instant would
        # drop every DB-open position from restore (orphan until next restart, loses in-memory
        # SL-trailing). If empty while DB has open trades, re-fetch before trusting it.
        if startup and db_open and not exchange_pos:
            for _i in range(3):
                time.sleep(3.0)
                try:
                    exchange_pos = client.open_positions()
                except Exception:
                    continue
                if exchange_pos:
                    log.info("restore: open_positions recovered on retry %d", _i + 1)
                    break
    except Exception as e:
        log.error("open_positions() in adopt failed: %s", e)
        return positions

    for row in db_open:
        coin = row["coin"]
        # XNN patch 2026-06-11 (canon §0.9): FOREIGN_SKIP_PREFIXES — never adopt a coin
        # that belongs to another bot by prefix (code-guard, not procedure).
        if FOREIGN_SKIP_PREFIXES and any(coin.startswith(p) for p in FOREIGN_SKIP_PREFIXES):
            log.error("adopt: %s matches FOREIGN_SKIP_PREFIXES — NEVER adopt foreign position", coin)
            continue
        # FX_EXCLUDE EXEMPT (2026-06-07): manual/excluded coins (e.g. BNB, BTC — UK-SIG
        # paused) are NEVER auto-adopted/healed/emergency-closed by the reconciler. A manual
        # no-SL signal must stay no-SL; re-adopting it here would let the SL-liveness guard in
        # manage_open_position re-add an SL (or emergency-close). Mirror universe.py matching
        # (bare coin OR "COIN-USD"). This is the class fix so the per-tick reconciler can never
        # touch a manually-managed position regardless of any stray DB-open row.
        _bare = coin[:-4] if coin.endswith("-USD") else coin
        if _bare in FX_EXCLUDE or coin in FX_EXCLUDE:
            if startup:
                log.info("adopt: %s is FX_EXCLUDE (manual-managed) — exempt from adopt/heal", coin)
            continue
        # Already tracked in-memory (live trail state) — never clobber.
        # Exception: UK-SIG pending-entry placeholder — clear flag when entry fills.
        if coin in positions:
            ph = positions[coin]
            if ph.__dict__.get("_pending_entry") and (
                coin in exchange_pos or f"{coin}-USD" in exchange_pos
            ):
                ph.__dict__.pop("_pending_entry", None)
                log.info("adopt: %s entry filled, clearing pending flag for heal", coin)
            continue
        # Accept both bare coin and "COIN-USD" suffix (Extended-style)
        if coin not in exchange_pos and f"{coin}-USD" not in exchange_pos:
            # UK-SIG pending-entry placeholder (2026-06-07): resting entry not yet
            # filled. Add placeholder Position to block scanner without SL-heal.
            # Cleared when entry fills (next _adopt_db_open_positions tick).
            notes_val = (row["notes"] if "notes" in row.keys() else "") or ""
            if "heal_after_fill" in notes_val and coin not in positions:
                direction = row["direction"] if "direction" in row.keys() else "long"
                ph = Position(
                    coin=coin, tf=row["tf"],
                    entry_price=float(row["entry"]),
                    sl_initial=float(row["sl_initial"]),
                    sl_current=float(row["sl_current"] or row["sl_initial"]),
                    tp1_price=float(row["tp1"] or 0),
                    size=float(row["size"]),
                    bar_entry_idx=0, side=direction,
                )
                ph.__dict__["_pending_entry"] = True
                ph.__dict__["_trade_id"] = row["id"]
                ph.__dict__["_sl_order_id"] = row["sl_order_id"]
                positions[coin] = ph
                log.info("adopt: placeholder for pending UK-SIG %s id=%d", coin, row["id"])
            elif startup:
                # DELIST-PHANTOM class fix 2026-07-02 (xyz_INTC on HL): a DB-open row absent from
                # the exchange at restore was skipped forever (never entered memory -> the manage
                # phantom-guard never saw it). Resolve HERE via REAL fills; keep the row (loud) only
                # when no close fill exists. Startup-only: at runtime the K=3 phantom-guard owns this.
                try:
                    from bot.trader import _lookup_real_close_px, _record_close, _cancel_orphan_triggers
                    _gr = row
                    _gd = _gr["direction"] if "direction" in _gr.keys() else "long"
                    _ghost = Position(
                        coin=coin, tf=_gr["tf"], entry_price=_gr["entry"],
                        sl_initial=_gr["sl_initial"],
                        sl_current=_gr["sl_current"] or _gr["sl_initial"],
                        tp1_price=_gr["tp1"] or _gr["entry"], size=_gr["size"],
                        bar_entry_idx=0, side=_gd,
                    )
                    _real = _lookup_real_close_px(
                        client, _ghost, sl_oid=_gr["sl_order_id"],
                        tp_oid=(_gr["tp1_order_id"] if "tp1_order_id" in _gr.keys() else None))
                    if _real is not None:
                        for _r2 in (row,):
                            _record_close(trade_id=_r2["id"], pos=_ghost,
                                          exit_price=_real[0], exit_reason=_real[1])
                        if not settings.dry_run:
                            try:
                                _cancel_orphan_triggers(client, coin)
                            except Exception:
                                pass
                        log.warning("restore-resolve %s: not on exchange — closed stale DB row(s) "
                                    "@%.6f via REAL fills (%s)", coin, _real[0], _real[1])
                    else:
                        log.critical("restore-resolve %s: not on exchange and NO close fill found — "
                                     "row(s) kept open, resolve manually", coin)
                except Exception as _rre:
                    log.critical("restore-resolve %s failed (%s) — row(s) kept open", coin, _rre)
            continue
        # SHORT support 2026-05-27 — restore side from DB direction column.
        direction = row["direction"] if "direction" in row.keys() else "long"
        entry = row["entry"]
        sl_initial = row["sl_initial"]
        rr = settings.raw_rr_target
        if row["tp1"]:
            tp1_recovered = row["tp1"]
        elif direction == "long":
            tp1_recovered = entry + rr * (entry - sl_initial)
        else:
            tp1_recovered = entry - rr * (sl_initial - entry)
        cur_size = row["size"]
        keys = row.keys()
        tp1_done = bool(row["tp1_partial_done"]) if "tp1_partial_done" in keys else False
        tp1_oid = row["tp1_order_id"] if "tp1_order_id" in keys else None
        frac = settings.tp1_partial_frac
        pos = Position(
            coin=coin,
            tf=row["tf"],
            entry_price=entry,
            sl_initial=sl_initial,
            sl_current=row["sl_current"] or sl_initial,
            tp1_price=tp1_recovered,
            size=cur_size,
            bar_entry_idx=0,
            side=direction,
        )
        pos.tp1_partial_done = tp1_done
        pos.tp1_hit = tp1_done
        pos.__dict__["_trade_id"] = row["id"]
        pos.__dict__["_sl_order_id"] = row["sl_order_id"]
        # _tp1_order_id: still-resting partial limit (None once consumed/marked done).
        pos.__dict__["_tp1_order_id"] = None if tp1_done else tp1_oid
        pos.__dict__["_tp1_frac"] = frac
        # _orig_size reconstructs pre-partial size so blended-R math holds after restart.
        pos.__dict__["_orig_size"] = (cur_size / (1.0 - frac)) if (tp1_done and frac < 1.0) else cur_size
        positions[coin] = pos
        log.info(
            "%s position %s %s: entry=%.6f sl=%.6f size=%s sl_oid=%s",
            "Restored" if startup else "ADOPTED (filled while running)",
            coin, row["tf"], row["entry"], pos.sl_current, row["size"], row["sl_order_id"],
        )

    if startup and positions:
        log.info("Restored %d open positions from DB", len(positions))
    return positions


def _resolve_snapshot_path() -> Path:
    p = Path(settings.liq_snapshot_path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _bootstrap_snapshot() -> SnapshotHolder:
    snap_path = _resolve_snapshot_path()
    snap = None
    try:
        snap = load_snapshot(snap_path)
        age_hours = (snap.age_seconds / 3600.0) if snap is not None else None
        needs_bootstrap = snap is None or (
            age_hours is not None and age_hours > settings.liq_snapshot_max_age_hours
        )
        if needs_bootstrap:
            log.warning(
                "Snapshot missing or stale (age=%s) — running bootstrap inline",
                f"{age_hours:.1f}h" if age_hours is not None else "n/a",
            )
            from bot.liquidity_snapshot import main as snapshot_main
            try:
                snapshot_main(force=True)
            except SystemExit:
                pass
            snap = load_snapshot(snap_path)
    except Exception as e:
        log.error("Snapshot bootstrap failed (continuing): %s", e, exc_info=True)
        snap = None

    if snap is None:
        log.error("FATAL: snapshot still missing after bootstrap — bot will reject signals")
    elif snap.age_seconds > settings.liq_snapshot_max_age_hours * 3600:
        log.warning(
            "Snapshot stale: age=%.1fh > %.1fh",
            snap.age_seconds / 3600.0, settings.liq_snapshot_max_age_hours,
        )
    return SnapshotHolder(snap)


_last_stale_warn_ts: float = 0.0


def _check_snapshot_age(holder: SnapshotHolder) -> None:
    global _last_stale_warn_ts
    snap = holder.current()
    if snap is None:
        return
    age_h = snap.age_seconds / 3600.0
    if age_h < 36.0:
        return
    now = time.time()
    if (now - _last_stale_warn_ts) < 3600.0:
        return
    _last_stale_warn_ts = now
    if age_h > 72.0:
        log.error("Snapshot is %.1fh old (>72h) — daily cron broken", age_h)
    else:
        log.warning("Snapshot is %.1fh old (>36h) — verify cron", age_h)


def _start_snapshot_reloader(holder: SnapshotHolder, interval_sec: int = 600) -> threading.Thread:
    snap_path = _resolve_snapshot_path()

    def _loop():
        while True:
            try:
                holder.maybe_reload(snap_path)
            except Exception as e:
                log.warning("snapshot reload error: %s", e)
            time.sleep(interval_sec)

    th = threading.Thread(target=_loop, name="snapshot-reloader", daemon=True)
    th.start()
    return th


# --- Loop-progress watchdog (ported from nado 2026-06-18 fix; kills the recurring
# silent-stall class -- an unbounded-blocking SDK call on the hot path freezes the
# single-thread loop while systemd still sees `active`. Daemon force-exits on
# no-progress so systemd (Restart=on-failure) self-heals. Heartbeat=time.monotonic()
# (clock-skew immune).
_loop_heartbeat = [time.monotonic()]


def _loop_progress_beat() -> None:
    _loop_heartbeat[0] = time.monotonic()


def _start_loop_watchdog(stall_limit_sec: float, check_interval_sec: float = 30.0) -> threading.Thread:
    def _watch() -> None:
        while True:
            time.sleep(check_interval_sec)
            stalled_for = time.monotonic() - _loop_heartbeat[0]
            if stalled_for > stall_limit_sec:
                log.critical(
                    "LOOP WATCHDOG: no main-loop progress for %.0fs (>%.0fs limit) "
                    "force-exiting so systemd restarts (recurring hung-fetch stall class).",
                    stalled_for, stall_limit_sec,
                )
                import os as _os
                _os._exit(1)
    th = threading.Thread(target=_watch, name="loop-watchdog", daemon=True)
    th.start()
    return th


def main_loop(dry_run: bool = False, run_once: bool = False) -> None:
    log.info(
        "bot starting: exchange=%s dry_run=%s once=%s",
        settings.exchange, dry_run, run_once,
    )
    log.info(
        "Config: tfs=%s lev=%dx mm_cap=%.0f%% max_conc=%d max_opens_day=%d "
        "risk=%.2f%% network=%s liq_size_cap=%.1f%%",
        settings.working_tfs, settings.leverage,
        settings.mm_cap_pct * 100, settings.max_concurrent,
        settings.max_opens_per_day, settings.risk_per_trade * 100,
        settings.network, settings.liq_size_cap_pct * 100,
    )
    for tf in settings.working_tfs:
        flt = settings.get_tf_filters(tf)
        log.info("  TF %s: f1=%.2f f2=%.1f f3=%.0f", tf, flt.f1, flt.f2, flt.f3)
    if dry_run:
        log.info("DRY-RUN: no orders will be placed")

    # P3 single-writer law (rollout §4, one of the TWO permitted old-bot
    # changes): exclusive flock on data/trades.db.lock — the SAME lock the
    # fleet-engine takes. A held lock means another writer owns trades.db
    # (engine mid-cutover): refuse to start LOUDLY, never dual-write.
    import fcntl as _fcntl
    _lockf = open("data/trades.db.lock", "a+")
    try:
        _fcntl.flock(_lockf.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        _lockf.seek(0); _lockf.truncate()
        _lockf.write("extended-bot pid=%d\n" % os.getpid()); _lockf.flush()
        globals()["_TRADES_DB_FLOCK"] = _lockf  # hold for process lifetime
    except OSError:
        log.critical("trades.db.lock HELD by another writer — refusing to "
                     "start (P3 single-writer law). Is fleet-engine running?")
        raise SystemExit(1)
    init_db()

    # XNN patch 2026-06-11 (canon §0.9): fresh-deploy guard — a brand-new xnn dir must
    # begin with an EMPTY trades.db (any open row = stale/foreign state → adopt class).
    if os.getenv("XNN_EXPECT_EMPTY_DB", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            _n_open = len(open_trades())
        except Exception as _e:
            log.critical("XNN_EXPECT_EMPTY_DB=1: cannot read trades.db (%s) — refusing to start", _e)
            sys.exit(1)
        if _n_open:
            log.critical(
                "XNN_EXPECT_EMPTY_DB=1: trades.db has %d open rows — refusing to start "
                "(fresh deploy must begin EMPTY; unset only after deliberate review)", _n_open)
            sys.exit(1)
        log.info("XNN_EXPECT_EMPTY_DB=1: trades.db has 0 open rows — OK")

    client = _build_client(settings)

    universe = load_universe(force_refresh=True)
    universe_tiers = {a.symbol: a.tier for a in universe}
    log.info("Universe: %d symbols (TIER1+2)", len(universe))

    # WS-PUSH candle feed (2026-06-27, port of Pacifica): subscribe once per (coin,interval),
    # scans served from memory -> 0 REST get_candles_history, 0 rate-limit, instant. REST stays
    # fallback (zero correctness risk). env-gated EXTENDED_WS_CANDLE. Non-native TFs (8h) are
    # auto-resampled by candles() from the native base (4h) which itself hits the WS fast path.
    if os.getenv("EXTENDED_WS_CANDLE", "0") == "1":
        try:
            from bot.ws_candle_feed import WsCandleFeed
            _ws_ivs: list = []
            for _k in ("WORKING_TFS", "SHORT_TFS", "DONCHIAN_TFS"):
                _ws_ivs += [t.strip() for t in os.getenv(_k, "").split(",") if t.strip()]
            _ws_ivs = list(dict.fromkeys(_ws_ivs)) or list(settings.working_tfs)
            # bare-coin -> Extended market name (e.g. "BTC" -> "BTC-USD"); skip unmapped
            _ws_map: dict = {}
            for _a in universe:
                try:
                    _ws_map[_a.symbol] = client._market(_a.symbol).name
                except Exception:
                    continue
            _stream_url = client._cfg.endpoints.stream_url
            _wsf = WsCandleFeed(_ws_map, _ws_ivs, _stream_url)
            client._ws_feed = _wsf
            _wsf.start()
            log.info("WS candle feed started: %d coins x %s (native subs only)",
                     len(_ws_map), _wsf._intervals)
        except Exception as _e:
            log.warning("WS candle feed start failed (REST fallback active): %s", _e)

    snapshot_holder = _bootstrap_snapshot()
    _start_snapshot_reloader(snapshot_holder, interval_sec=600)
    _start_loop_watchdog(max(300.0, 8.0 * settings.loop_interval_sec))
    snap = snapshot_holder.current()
    if snap is not None:
        log.info(
            "Liquidity snapshot active: %d coins, generated_at=%s, age=%.1fh",
            len(snap.coins), snap.generated_at_utc, snap.age_seconds / 3600.0,
        )

    # write-db-row-PRE-order reconcile (phase0 2026-07-02): promote/delete any 'pending'
    # pre-order rows left by a crash mid-entry BEFORE adopt, so a promoted one is picked
    # up + SL-healed by the startup restore below.
    _reconcile_pending(client, dry_run=dry_run)
    open_positions: Dict[str, Position] = _adopt_db_open_positions(
        client, startup=True, dry_run=dry_run)

    # SAFETY NET (phase0 2026-07-02, port of hl canon untracked-protect sweep): place a
    # protective reduce-only SL on any EXCHANGE position that is not tracked in-memory/DB
    # and has NO live SL — a naked orphan from a fill during a crash/restart window. The
    # account_position_exists guard in attempt_entry prevents re-entry; the position rides
    # the protective SL until manually adopted. Never touches FX_EXCLUDE (manual, e.g. BNB)
    # or FOREIGN_SKIP_PREFIXES coins; DRY skips (this sweep places REAL orders).
    if not dry_run:
        try:
            _ex = client.open_positions() or {}
            _seen_ids: set = set()
            for _coin, _v in _ex.items():
                # Alias-dedup: open_positions returns each position under TWO keys
                # ('BTC-USD' AND 'BTC' → SAME dict) — protect each position ONCE.
                if id(_v) in _seen_ids:
                    continue
                _seen_ids.add(id(_v))
                _bare = _coin[:-4] if _coin.endswith("-USD") else _coin
                if FOREIGN_SKIP_PREFIXES and any(_bare.startswith(p) for p in FOREIGN_SKIP_PREFIXES):
                    log.info("untracked-protect skip %s: foreign prefix (FOREIGN_SKIP_PREFIXES)", _coin)
                    continue
                if _bare in FX_EXCLUDE or _coin in FX_EXCLUDE:
                    log.info("untracked-protect skip %s: FX_EXCLUDE (manual-managed)", _coin)
                    continue
                if _bare in open_positions or _coin in open_positions:
                    continue  # tracked (restored/adopted) — manage loop heals its SL
                try:
                    _szi = float(_v.get("szi", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if abs(_szi) <= 0:
                    continue
                if _szi < 0 and not settings.short_enabled_tfs:
                    log.critical("untracked-protect skip %s: SHORT position but bot is long-only "
                                 "(short_enabled_tfs empty) — foreign/manual by construction, NOT touching", _coin)
                    continue
                try:
                    if client.list_open_sl_orders(_bare):
                        continue  # already protected by a live SL
                except Exception as _le:
                    # bias-to-protect (donor hl): can't read the SL list → place anyway;
                    # a duplicate reduce-only SL is harmless, a naked position is not.
                    log.warning("untracked-protect %s: SL list failed (%s) — placing "
                                "protective SL anyway (bias-to-protect)", _coin, _le)
                _long = _szi > 0
                _side = "long" if _long else "short"
                _mark = client.mark_price(_bare)
                if not _mark or _mark <= 0:
                    log.error("untracked-protect %s: no mark — STILL NAKED, manual", _coin)
                    continue
                _sl = _mark * 0.94 if _long else _mark * 1.06
                try:
                    _sl, _liq_act = ensure_sl_inside_liq(
                        client=client, coin=_bare, side=_side, sl_px=_sl, size=abs(_szi),
                    )
                    if _liq_act not in ("no_position", "cross_account_safe", "already_safe", "dry_skip"):
                        log.warning("untracked-protect liq-guard %s: action=%s sl→%.6f", _coin, _liq_act, _sl)
                except Exception as _le:
                    log.warning("untracked-protect %s: liq-guard threw (%s) — placing raw ±6%% SL", _coin, _le)
                _oid = _place_sl_with_retry(client=client, coin=_bare, size=abs(_szi),
                                            sl_price=_sl, side=_side)
                if _oid:
                    log.warning("PROTECTED untracked naked position %s sz=%.4f SL=%.6f oid=%s",
                                _coin, _szi, _sl, _oid)
                else:
                    log.error("untracked-protect %s: SL placement FAILED — STILL NAKED, manual", _coin)
        except Exception as _e:
            log.warning("untracked-position protect sweep failed: %s", _e)

    # Cross-cycle debounce state for the orphan reduce-only TRIGGER sweep.
    orphan_trigger_seen: dict = {}

    scanner = Scanner(settings)
    position_manager = PositionManager(
        be_buffer_pct=settings.trail_after_tp_buffer_pct,
        vstop_pivot_window=settings.vstop_pivot_window,
        max_run_r=settings.max_run_r,
        vstop_buffer_pct=settings.vstop_buffer_pct,
        tp1_partial_frac=settings.tp1_partial_frac,
    )

    # Daily opens counter (UTC day rollover)
    opens_today = 0
    current_day = time.gmtime().tm_yday
    iteration = 0
    last_universe_refresh = time.time()

    # ---- Entry-latency streaming (2026-07 fleet-propagation of hl_combo_bot fix) ----
    # Extended is ALL native crypto, pass-through, price-gate bypassed (market-at-close).
    # Enter each breakout the instant scanner detects it (via on_crypto_signal callback)
    # instead of waiting for the whole throttled universe pass to finish. attempt_entry's
    # live-mark re-check (trader.py bar_age gate + breakout-validity guard) is PRESERVED:
    # the streaming path calls the SAME attempt_entry, nothing bypassed.
    _stream_state = {"stopped": False, "streamed": 0}
    _STREAM_MAX = int(os.getenv("EXT_STREAM_MAX_PER_SCAN", "8"))

    def _try_enter_signal(signal, _via_stream=False):
        """Per-signal entry decision. Returns 'entered' | 'skip' | 'stop'.
        Shared by the batch loop and the on_crypto_signal stream callback."""
        nonlocal opens_today
        if len(open_positions) >= settings.max_concurrent:
            log.info("SKIP %s %s: max_concurrent=%d reached",
                     signal.coin, signal.tf, settings.max_concurrent)
            return "stop"
        if settings.max_opens_per_day > 0 and opens_today >= settings.max_opens_per_day:
            log.info("SKIP %s %s: max_opens_per_day=%d reached",
                     signal.coin, signal.tf, settings.max_opens_per_day)
            return "stop"
        if signal.coin in open_positions:
            return "skip"

        bar_age = scanner.bar_age_sec(signal.tf)
        if dry_run:
            log.info(
                "[DRY-RUN] Signal %s %s: trigger=%.6f sl=%.6f tp1=%.6f f1=%.2f bar_age=%.0fs",
                signal.coin, signal.tf,
                signal.trigger_price, signal.sl_price,
                signal.tp1_price, signal.f1_dist, bar_age,
            )
            return "skip"

        try:
            pos = attempt_entry(
                signal=signal, client=client, settings=settings,
                universe_tiers=universe_tiers, bar_age_sec=bar_age,
                snapshot_holder=snapshot_holder,
            )
        except Exception as e:
            log.error("attempt_entry(%s %s) error%s: %s",
                      signal.coin, signal.tf,
                      " [stream]" if _via_stream else "", e, exc_info=True)
            return "skip"

        if pos is not None:
            # MITIGATION: record tracked position as the VERY FIRST statement so a
            # crash between here and the batch loop cannot leave it double-entered.
            open_positions[signal.coin] = pos
            opens_today += 1
            log.info(
                "ENTRY OK%s %s %s: entry=%.6f sl=%.6f size=%s opens_today=%d",
                " [stream]" if _via_stream else "",
                pos.coin, pos.tf, pos.entry_price, pos.sl_current, pos.size,
                opens_today,
            )
            return "entered"
        return "skip"

    def _on_crypto_signal(sig):
        """Fired by the scanner the instant a crypto breakout is appended."""
        if _stream_state["stopped"]:
            return
        if _stream_state["streamed"] >= _STREAM_MAX:
            return
        res = _try_enter_signal(sig, _via_stream=True)
        if res == "stop":
            _stream_state["stopped"] = True
        elif res == "entered":
            _stream_state["streamed"] += 1

    while True:
        _loop_progress_beat()
        iteration += 1
        t_loop_start = time.time()

        # Daily counter reset
        day_now = time.gmtime().tm_yday
        if day_now != current_day:
            log.info("UTC day rollover %d→%d — resetting opens_today (was %d)",
                     current_day, day_now, opens_today)
            current_day = day_now
            opens_today = 0

        _check_snapshot_age(snapshot_holder)

        refresh_interval = settings.universe_refresh_min * 60
        if time.time() - last_universe_refresh > refresh_interval:
            try:
                universe = load_universe(force_refresh=True)
                universe_tiers = {a.symbol: a.tier for a in universe}
                log.info("Universe refreshed: %d symbols", len(universe))
                last_universe_refresh = time.time()
            except Exception as e:
                log.error("Universe refresh failed: %s", e)

        try:
            # PER-TICK RECONCILER (class fix 2026-06-07): adopt any DB-open position that is
            # live on the exchange but not tracked in-memory (e.g. a resting/manual stop-entry
            # that filled mid-run). Runs BEFORE the manage loop so the SL-liveness guard heals
            # it the same tick — bounding the naked window to ≤1 tick. Never clobbers a tracked
            # position. (mem:feedback_order_placement_readback_and_invariants)
            try:
                open_positions = _adopt_db_open_positions(client, open_positions, dry_run=dry_run)
            except Exception as _ae:
                log.warning("per-tick adopt failed: %s", _ae)
            closed_coins = set()
            for coin, pos in list(open_positions.items()):
                try:
                    df = client.candles(coin, pos.tf, limit=50)
                    # XNN patch 2026-06-11: manage decisions (trail / SL-hit wick check)
                    # on CLOSED bars only — bt-parity (ext candles include the forming bar).
                    df = drop_forming_bars(df, pos.tf)
                    if df is not None and not df.empty:
                        from bot.strategy_xnn import compute_indicators
                        df = compute_indicators(df)

                    exit_reason = manage_open_position(
                        pos=pos, client=client, settings=settings,
                        position_manager=position_manager,
                        df_latest={pos.tf: df},
                    )
                    if exit_reason is not None:
                        closed_coins.add(coin)
                        log.info("Position closed %s: %s", coin, exit_reason)
                except Exception as e:
                    log.error("manage_open_position(%s) error: %s", coin, e, exc_info=True)

            for coin in closed_coins:
                open_positions.pop(coin, None)

            # --- Sweep orphan reduce-only TRIGGER orders (class-fix 2026-06-15) ---
            # Cancel TP/SL triggers left resting after their position closed.
            # Exempts manual/foreign + any coin with a live position; fail-safe.
            if not dry_run:
                try:
                    sweep_orphan_triggers(client, open_positions, orphan_trigger_seen, log)
                except Exception as _se:
                    log.warning("orphan-sweep dispatch error: %s", _se)

            no_long = _no_long_symbols(universe) if CRYPTO_SHORT_ONLY else set()
            # Reset per-scan stream budget/stop before the pass.
            _stream_state["stopped"] = False
            _stream_state["streamed"] = 0
            signals = scanner.scan_all_coins(
                coins=universe, client=client, open_positions=open_positions,
                no_long_symbols=no_long,
                on_crypto_signal=(_on_crypto_signal
                                  if (not dry_run and os.getenv("CRYPTO_STREAM_ENTRY", "1") == "1")
                                  else None),
            )

            # Batch pass: coins already entered via the stream callback are in
            # open_positions -> _try_enter_signal returns 'skip' for them.
            for signal in signals:
                if _try_enter_signal(signal) == "stop":
                    break
        except Exception as e:
            log.error("Main loop error (iter %d): %s", iteration, e, exc_info=True)

        if run_once:
            log.info("--once flag: exiting after single iteration")
            break

        elapsed = time.time() - t_loop_start
        # BOUNDARY-ALIGNED WAKE (2026-06-23): keep ~loop_interval cadence for position
        # management, but shorten the sleep so the loop wakes right after a bar CLOSES
        # (00/08/16 UTC for 8h) -> entry-detect lag ~2s instead of up to loop_interval.
        # Floor 1s (never busy-loop); capped at loop_interval (never starve mgmt).
        try:
            from bot.config import TF_MS as _TFMS
            _atfs = set(settings.working_tfs) | set(getattr(settings, "short_enabled_tfs", []) or [])
            _nm = int(time.time() * 1000)
            _tnb = min((((_nm // _TFMS[_t]) + 1) * _TFMS[_t] - _nm) / 1000.0 for _t in _atfs if _t in _TFMS)
            _age = min((_nm - (_nm // _TFMS[_t]) * _TFMS[_t]) / 1000.0 for _t in _atfs if _t in _TFMS)
            _base = max(0.0, settings.loop_interval_sec - elapsed)
            _sleep = min(_base, 0.5) if _age < 15.0 else min(_base, _tnb + 0.1)
            sleep_sec = max(0.2, _sleep)
        except Exception:
            sleep_sec = max(0, settings.loop_interval_sec - elapsed)
        log.debug("Loop done in %.1fs, sleeping %.1fs (boundary-aligned)", elapsed, sleep_sec)
        time.sleep(sleep_sec)

    log.info("bot exiting cleanly")
    import os as _os
    _os._exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extended/pacifica bot — XNN per-TF")
    parser.add_argument("--dry-run", action="store_true", help="Scan but do not place orders")
    parser.add_argument("--once", action="store_true", help="Run one iteration then exit")
    args = parser.parse_args()
    # DRY = CLI flag OR env (systemd has no flag; trader.py enforces order-block in code too)
    main_loop(dry_run=(args.dry_run or DRY_RUN_ENV), run_once=args.once)
