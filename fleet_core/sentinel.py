"""fleet_core.sentinel — INDEPENDENT per-venue invariant sentinel (P4, audit item E).

A separate PROCESS (own systemd unit, same bot venv + .env, WorkingDirectory=<bot
root>) that survives bot hangs/crashes and re-verifies the money-safety invariants
every tick (default 60s, env SENTINEL_TICK_S):

  I1  every live exchange position that is NOT fenced/foreign has
      (a) a live reduce-only SL trigger resting on the exchange, with trigger px
          strictly INSIDE the liquidation px (SL-inside-liq law, ×(1+buf) buffer,
          env SENTINEL_LIQ_BUF_PCT default 0.01), and
      (b) a trades.db status='open' row;
  I2  every trades.db status='open' row has a live exchange position;
  I3  every orphan reduce-only trigger (trigger with no position) is either fenced
      or flagged (the bot's own orphan_sweep cancels after debounce — the sentinel
      only FLAGS, it never cancels anything except its own just-placed duplicate);
  I4  bot process liveness: systemd unit active + journal heartbeat age below
      threshold (env SENTINEL_HEARTBEAT_MAX_S, default 900) — a DEAD/hung bot with
      open positions is itself a CRITICAL.

FENCES (money-critical): manual/foreign positions are OUT OF SCOPE for protection
actions. Fence logic is NOT re-implemented — it is the EXACT canonical
bot.orphan_sweep._fenced / _coin_present / _variants (fleet_core canonical on all
four venues since P1):
  * positions   : _fenced(coin, db_open=<open-row coins>) — on HL the
                  MANUAL_POSITION_PREFIXES block then fences a prefix coin with no
                  open row (manual ЮК) while keeping the bot-owned us29 leg
                  (has an open row) in scope — the same test hl main.py's
                  untracked-protect uses.
  * triggers    : _fenced(coin, db_open=…, bot_owned=…, oid=…, placed_oids=…) —
                  the per-oid authoritative path, identical to the per-cycle
                  orphan sweep.
  * direction   : a SHORT position while the bot is long-only
                  (settings.short_enabled_tfs empty) is foreign by construction
                  (deployed Phase-0 direction-guard) — skipped like a fence.
Fenced coins are skipped ENTIRELY for I1 (one aggregated DEBUG line per tick max —
extended BNB deliberately has NO SL; the sentinel must never alert-spam it).

MODES (env SENTINEL_MODE):
  alert   (DEFAULT) log CRITICAL/WARNING + append <bot_root>/data/
          sentinel_violations.jsonl. NO exchange writes of any kind.
  protect additionally: for a BOT-OWNED position (DB open row present) whose SL is
          missing/dead, re-place a reduce-only trigger from the DB row's sl_current
          after re-verifying absence immediately before placing (race-safe vs the
          bot's own heal: place-then-recheck-and-cancel-own-duplicate). Protect
          NEVER closes positions, NEVER touches fenced coins, NEVER cancels
          anything except its own just-placed duplicate. Positions with NO DB row
          are alert-only even in protect mode (the bot's untracked-protect owns
          that placement; racing it from a second process is the greater risk).

FAIL DIRECTIONS:
  * any exchange/DB read failure = state UNKNOWN -> skip the tick with a WARNING;
    violations are NEVER asserted from a failed read (the R3 masking bug in
    reverse).
  * a broken fence check fences (inherited from _fenced — fail-closed).
  * trigger-price extraction is BEST-EFFORT per venue; when the live trigger px is
    unreadable the liq check falls back to the DB row's sl_current (labelled
    px_source=db) and never fabricates a violation from a missing number.
  * I4 with UNKNOWN exposure (positions read failed) still escalates a dead bot as
    CRITICAL — a dead bot with unknown exposure must not be quiet.

Runs inside the bot venv (venue SDKs available). All bot.* imports happen at CALL
time so the module itself imports clean anywhere and the offline selftest can
inject fake bot.* modules via sys.modules (see fleet_core/sentinel_selftest.py).
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

log = logging.getLogger("sentinel")

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

# Live bot systemd units per venue (source: deploy/deploy_p1.sh venue map — the
# units P1 actually restarted on the live hosts). Override: env SENTINEL_BOT_UNIT.
DEFAULT_BOT_UNITS = {
    "hl": "valantis-bot",
    "pacifica": "pacifica-bot",
    "extended": "extended-bot",
    "nado": "nado-bot",
}

# settings.exchange value -> sentinel venue name (nado's Settings has no .exchange
# field — detected by the presence of bot.exchange_nado instead).
_EXCHANGE_TO_VENUE = {"hyperliquid": "hl", "pacifica": "pacifica", "extended": "extended"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        return default


def detect_venue() -> str:
    """Venue autodetect: env SENTINEL_VENUE > bot.config settings.exchange >
    presence of bot.exchange_nado (nado has no settings.exchange field)."""
    v = (os.environ.get("SENTINEL_VENUE") or "").strip().lower()
    if v:
        if v not in DEFAULT_BOT_UNITS:
            raise RuntimeError(f"SENTINEL_VENUE={v!r} not in {sorted(DEFAULT_BOT_UNITS)}")
        return v
    cfg = importlib.import_module("bot.config")
    ex = getattr(getattr(cfg, "settings", None), "exchange", None)
    if ex:
        venue = _EXCHANGE_TO_VENUE.get(str(ex).lower())
        if venue is None:
            raise RuntimeError(f"unknown settings.exchange={ex!r}")
        return venue
    if importlib.util.find_spec("bot.exchange_nado") is not None:
        return "nado"
    raise RuntimeError("cannot autodetect venue: no settings.exchange, no bot.exchange_nado "
                       "— set SENTINEL_VENUE")


def build_client(venue: str):
    """Mirror of each main.py _build_client — read-mostly second client instance."""
    cfg = importlib.import_module("bot.config").settings
    if venue == "hl":
        from bot.exchange_hl import HLClient
        return HLClient(cfg)
    if venue == "pacifica":
        from bot.exchange_pacifica import PacificaClient
        return PacificaClient(cfg)
    if venue == "extended":
        from bot.exchange_extended import ExtendedClient
        return ExtendedClient(cfg)
    if venue == "nado":
        from bot.exchange_nado import NadoClient_
        return NadoClient_(cfg)
    raise RuntimeError(f"unknown venue {venue!r}")


# --------------------------------------------------------------------------
# Best-effort per-venue trigger metadata: {str(oid): {"px": float|None,
# "kind": "sl"|"tp"|None}}. Presence/authority stays with the canonical
# client.list_reduce_only_triggers(); this only ENRICHES with trigger px + side
# for the SL-inside-liq check. Any failure -> {} (caller logs WARNING and the px
# check falls back to the DB row's sl_current).
# --------------------------------------------------------------------------

def _trigger_meta_hl(client) -> dict:
    from bot.exchange_hl import api_to_coin  # noqa: F401 (coin not needed; oid-keyed)
    addr = client.settings.account_address
    dexes = [""] + list(getattr(client, "HIP3_USDC_DEXES", []) or [])
    out: dict = {}
    for dex in dexes:
        payload = {"type": "frontendOpenOrders", "user": addr}
        if dex:
            payload["dex"] = dex
        retry = getattr(client, "_retry_429", None)
        if callable(retry):
            chunk = retry(lambda p=payload: client.info.post("/info", p),
                          f"sentinel frontendOpenOrders(dex={dex or 'main'})", 3)
        else:
            chunk = client.info.post("/info", payload)
        for o in chunk or []:
            if not (o.get("isTrigger") and o.get("reduceOnly")):
                continue
            oid = o.get("oid")
            if oid is None:
                continue
            try:
                px = float(o.get("triggerPx"))
            except (TypeError, ValueError):
                px = None
            ot = str(o.get("orderType", "")).lower()
            kind = "sl" if "stop" in ot else ("tp" if ("take" in ot or "tp" in ot) else None)
            out[str(oid)] = {"px": px, "kind": kind}
    return out


def _trigger_meta_extended(client) -> dict:
    xm = importlib.import_module("bot.exchange_extended")

    async def _go():
        r = await client._client.account.get_open_orders(order_type=xm.OrderType.TPSL)
        return r.data

    orders = client._bridge.run(_go(), timeout=10)
    out: dict = {}
    for o in orders or []:
        oid = getattr(o, "id", None)
        if oid is None:
            continue
        px, kind = None, None
        sl = getattr(o, "stop_loss", None)
        tp = getattr(o, "take_profit", None)
        for cand, k in ((getattr(sl, "trigger_price", None), "sl"),
                        (getattr(o, "trigger_price", None), None),
                        (getattr(tp, "trigger_price", None), "tp")):
            if cand is not None:
                try:
                    px, kind = float(cand), k
                except (TypeError, ValueError):
                    continue
                break
        out[str(oid)] = {"px": px, "kind": kind}
    return out


def _trigger_meta_pacifica(client) -> dict:
    resp = client._signed_request("GET", "/orders", "get_orders", {})
    if not resp.get("success"):
        raise RuntimeError(f"sentinel: GET /orders failed: {str(resp.get('error', resp))[:200]}")
    out: dict = {}
    for o in resp.get("data") or []:
        if not o.get("reduce_only"):
            continue
        ot = (o.get("order_type") or "").lower()
        if "stop" not in ot:
            continue
        oid = o.get("order_id") or o.get("id")
        if oid is None:
            continue
        px = None
        for key in ("stop_price", "trigger_price", "price"):
            raw = o.get(key)
            if raw in (None, ""):
                continue
            try:
                px = float(raw)
                break
            except (TypeError, ValueError):
                continue
        kind = "sl" if "loss" in ot else ("tp" if "profit" in ot else None)
        if kind is None:
            kind = "sl" if "stop" in ot and "profit" not in ot else None
        out[str(oid)] = {"px": px, "kind": kind}
    return out


def _trigger_meta_nado(client) -> dict:  # noqa: ARG001
    # Nado's trigger service ccxt shape (fetch_open_orders_ccxt_shape) carries no
    # trigger price; px check falls back to the DB row's sl_current (px_source=db).
    return {}


TRIGGER_META_FNS = {
    "hl": _trigger_meta_hl,
    "extended": _trigger_meta_extended,
    "pacifica": _trigger_meta_pacifica,
    "nado": _trigger_meta_nado,
}


def _base(coin) -> str:
    return str(coin).upper().replace("-PERP", "").replace("-USD", "")


def _f(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


class ReadFailure(Exception):
    """A venue/DB read failed — state UNKNOWN, skip the tick."""


class Sentinel:
    def __init__(self, venue: str, client, mode: str = "alert", *,
                 bot_unit: str | None = None,
                 heartbeat_max_s: float | None = None,
                 liq_buf_pct: float | None = None,
                 refire_ticks: int | None = None,
                 violations_path: Path | None = None,
                 runner=subprocess.run,
                 now_fn=time.time):
        self.venue = venue
        self.client = client
        self.mode = mode if mode in ("alert", "protect") else "alert"
        self.bot_unit = bot_unit or os.environ.get("SENTINEL_BOT_UNIT") \
            or DEFAULT_BOT_UNITS[venue]
        self.heartbeat_max_s = heartbeat_max_s if heartbeat_max_s is not None \
            else _env_float("SENTINEL_HEARTBEAT_MAX_S", 900.0)
        self.liq_buf_pct = liq_buf_pct if liq_buf_pct is not None \
            else _env_float("SENTINEL_LIQ_BUF_PCT", 0.01)
        self.refire_ticks = refire_ticks if refire_ticks is not None \
            else _env_int("SENTINEL_REFIRE_TICKS", 10)
        self._liq_cache = {}    # (coin, szi, entryPx) -> (liq_px|None, read_ts) — 429-storm fix
        self._saw_429 = False   # set by any 429-tagged read failure within a tick
        self.runner = runner
        self.now_fn = now_fn
        if violations_path is None:
            try:
                root = importlib.import_module("bot.config").PROJECT_ROOT
            except Exception:
                root = Path.cwd()
            violations_path = Path(root) / "data" / "sentinel_violations.jsonl"
        self.violations_path = Path(violations_path)
        self.tick_n = 0
        # active violation state: key -> {"first_tick": n, "last_emit_tick": n}
        self._active: dict = {}
        self._hb_unknown_warned = False

    # ---------------- journal / fence access (lazy, sys.modules-patchable) ------

    @staticmethod
    def _journal():
        return importlib.import_module("bot.journal")

    @staticmethod
    def _fence_mod():
        return importlib.import_module("bot.orphan_sweep")

    @staticmethod
    def _settings():
        return importlib.import_module("bot.config").settings

    # ---------------- violation sink -------------------------------------------

    def _emit(self, invariant: str, severity: str, coin, detail: str, seen: set,
              extra: dict | None = None) -> dict:
        key = (invariant, _base(coin) if coin is not None else "-")
        seen.add(key)
        rec = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.now_fn())),
            "venue": self.venue,
            "mode": self.mode,
            "tick": self.tick_n,
            "invariant": invariant,
            "severity": severity,
            "coin": str(coin) if coin is not None else None,
            "detail": detail,
        }
        if extra:
            rec.update(extra)
        emit = log.critical if severity == "CRITICAL" else log.warning
        emit("SENTINEL %s %s coin=%s: %s", invariant, severity, coin, detail)
        st = self._active.get(key)
        if st is None:
            self._active[key] = {"first_tick": self.tick_n, "last_emit_tick": self.tick_n}
            self._write_jsonl(rec)
        elif self.tick_n - st["last_emit_tick"] >= self.refire_ticks:
            st["last_emit_tick"] = self.tick_n
            rec["refire"] = True
            self._write_jsonl(rec)
        return rec

    def _resolve_stale(self, seen: set) -> None:
        for key in [k for k in self._active if k not in seen]:
            st = self._active.pop(key)
            log.info("SENTINEL resolved %s coin=%s (active since tick %d)",
                     key[0], key[1], st["first_tick"])
            self._write_jsonl({
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.now_fn())),
                "venue": self.venue, "tick": self.tick_n,
                "invariant": key[0], "coin": key[1], "resolved": True,
            })

    def _write_jsonl(self, rec: dict) -> None:
        try:
            self.violations_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.violations_path, "a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as e:
            log.error("sentinel: cannot write %s: %s", self.violations_path, e)

    # ---------------- reads (UNKNOWN -> ReadFailure) ----------------------------

    def _read_positions(self) -> dict:
        """Canonical positions, keyed by base coin (Extended aliases deduped).
        {base: {"key": orig_key, "szi": float, "entry": raw_dict}}"""
        try:
            raw = self.client.open_positions()
        except Exception as e:
            raise ReadFailure(f"open_positions failed: {e}") from e
        out: dict = {}
        for k, v in (raw or {}).items():
            b = _base(k)
            if b in out:
                continue  # Extended aliases BTC / BTC-USD to the same entry
            szi = _f((v or {}).get("szi", (v or {}).get("size")), 0.0) or 0.0
            if abs(szi) <= 0:
                continue
            out[b] = {"key": k, "szi": szi, "entry": v or {}}
        return out

    def _read_db_rows(self) -> list:
        try:
            rows = self._journal().open_trades() or []
        except Exception as e:
            raise ReadFailure(f"open_trades failed: {e}") from e
        return [dict(r) for r in rows]

    def _read_triggers(self) -> list:
        try:
            return self.client.list_reduce_only_triggers() or []
        except Exception as e:
            raise ReadFailure(f"list_reduce_only_triggers failed: {e}") from e

    def _read_trigger_meta(self) -> dict:
        fn = TRIGGER_META_FNS.get(self.venue)
        if fn is None:
            return {}
        try:
            return fn(self.client) or {}
        except Exception as e:
            log.warning("sentinel: trigger-price enrichment failed (%s) — px checks "
                        "fall back to DB sl_current this tick", e)
            return {}

    def _read_journal_history(self):
        """HL shared-account seam: (bot_owned, placed_oids) when
        MANUAL_POSITION_PREFIXES is defined in bot.config; (None, None) elsewhere.
        Mirrors fleet_core.orphan_sweep step 3 exactly."""
        try:
            importlib.import_module("bot.config").MANUAL_POSITION_PREFIXES
        except (AttributeError, ImportError):
            return None, None
        try:
            j = self._journal()
            return j.coins_ever_traded(), j.oids_ever_placed()
        except Exception as e:
            raise ReadFailure(f"journal history read failed: {e}") from e

    def _liq_px(self, base: str, pos: dict):
        """Liquidation px for a position: prefer the raw pos dict's liquidationPx
        (HL/Extended/Pacifica), else client.position_liquidation (Nado marginal
        liq / HL isolated fresh read). None => cross/account-level = SAFE.

        429-storm fix (2026-07-03): the fallback is a SIGNED per-position read. On a
        venue whose positions payload lacks liquidationPx (Pacifica) this was N extra
        signed calls per tick and starved the venue rate budget shared with the live
        bot. The fallback result is now cached per (coin, side, size, entry) for
        SENTINEL_LIQ_TTL_S (default 600s): a changed position changes the key -> fresh
        read; an unchanged position's liq drifts only with funding, and the invariant
        check carries a liq buffer, so 10-min staleness is safe for defense-in-depth."""
        liq = _f(pos["entry"].get("liquidationPx"))
        if liq is not None and liq > 0:
            return liq
        fn = getattr(self.client, "position_liquidation", None)
        if not callable(fn):
            return None
        e = pos["entry"]
        cache_key = (pos["key"], _f(e.get("szi")), _f(e.get("entryPx")))
        ttl = _env_float("SENTINEL_LIQ_TTL_S", 600.0)
        hit = self._liq_cache.get(cache_key)
        if hit is not None and (time.time() - hit[1]) < ttl:
            return hit[0]
        try:
            info = fn(pos["key"])
        except Exception as ex:
            log.warning("sentinel: position_liquidation(%s) failed (%s) — liq UNKNOWN, "
                        "px check skipped", pos["key"], ex)
            if "429" in str(ex):
                self._saw_429 = True
            return None
        liq = _f((info or {}).get("liq_px"))
        liq = liq if liq and liq > 0 else None
        self._liq_cache[cache_key] = (liq, time.time())
        if len(self._liq_cache) > 512:  # prune stale keys (closed positions)
            cutoff = time.time() - ttl
            self._liq_cache = {k: v for k, v in self._liq_cache.items() if v[1] >= cutoff}
        return liq

    # ---------------- invariant checks ------------------------------------------

    @staticmethod
    def _is_sl_side(meta: dict | None, szi: float, entry_px) -> bool:
        """Classify a reduce-only trigger as SL-side. Conservative: when nothing is
        known, count it as an SL (mirror of the bot's list_open_sl_orders presence
        semantics — never false-CRITICAL from a missing label)."""
        if meta:
            if meta.get("kind") == "sl":
                return True
            if meta.get("kind") == "tp":
                return False
            px = meta.get("px")
            e = _f(entry_px)
            if px is not None and e:
                return px < e if szi > 0 else px > e
        return True

    def _check_positions(self, positions, db_rows, triggers, trig_meta, seen,
                         fenced_mod) -> None:
        db_open = {r["coin"] for r in db_rows}
        rows_by_base: dict = {}
        for r in db_rows:
            rows_by_base.setdefault(_base(r["coin"]), []).append(r)
        trig_by_base: dict = {}
        for t in triggers:
            if t.get("coin") is None or t.get("oid") is None:
                continue
            trig_by_base.setdefault(_base(t["coin"]), []).append(t)

        settings = self._settings()
        long_only = not getattr(settings, "short_enabled_tfs", ())
        fenced_skipped, foreign_skipped = [], []

        for b, pos in positions.items():
            if fenced_mod._fenced(pos["key"], db_open=db_open):
                fenced_skipped.append(pos["key"])
                continue
            if pos["szi"] < 0 and long_only:
                foreign_skipped.append(pos["key"])  # direction-guard: foreign by construction
                continue

            rows = rows_by_base.get(b, [])
            has_row = bool(rows)
            entry_px = pos["entry"].get("entryPx")

            coin_trigs = trig_by_base.get(b, [])
            sl_trigs = [t for t in coin_trigs
                        if self._is_sl_side(trig_meta.get(str(t["oid"])), pos["szi"], entry_px)]

            # I1b — DB open row
            if not has_row:
                self._emit("I1b_db_row_missing", "CRITICAL", pos["key"],
                           f"live position szi={pos['szi']} has NO trades.db open row "
                           "(untracked — bot cannot trail/heal it)", seen)

            # I1a — live SL trigger presence
            if not sl_trigs:
                detail = "NO live reduce-only SL trigger on exchange (NAKED)"
                if coin_trigs:
                    detail = ("no SL-side trigger (only TP-side triggers resting) — "
                              "effectively NAKED below entry")
                rec_extra = {}
                if self.mode == "protect" and has_row:
                    action = self._protect(pos, rows[0])
                    rec_extra = {"protect_action": action}
                self._emit("I1a_naked", "CRITICAL", pos["key"], detail, seen, rec_extra)
                continue

            # I1a — SL strictly inside liquidation
            liq = self._liq_px(b, pos)
            if liq is None:
                continue  # cross/account-level or unreadable => nothing to compare (safe/UNKNOWN)
            buf = self.liq_buf_pct
            for t in sl_trigs:
                meta = trig_meta.get(str(t["oid"])) or {}
                px, px_source = meta.get("px"), "live"
                if px is None:
                    row_sl = next((_f(r.get("sl_current")) or _f(r.get("sl_initial"))
                                   for r in rows), None) if rows else None
                    px, px_source = row_sl, "db"
                if px is None:
                    log.warning("sentinel: %s SL oid=%s trigger px unreadable and no DB "
                                "sl_current — liq check UNKNOWN (skipped)", pos["key"], t["oid"])
                    continue
                ok = px > liq * (1.0 + buf) if pos["szi"] > 0 else px < liq * (1.0 - buf)
                if not ok:
                    self._emit(
                        "I1a_sl_outside_liq", "CRITICAL", pos["key"],
                        f"SL trigger px={px} (source={px_source}) is NOT strictly inside "
                        f"liq px={liq} ×(1±{buf}) for {'long' if pos['szi'] > 0 else 'short'} "
                        "— stop is worthless, position liquidates first", seen,
                        {"oid": str(t["oid"]), "px": px, "liq_px": liq})

        if fenced_skipped:
            log.debug("sentinel: fenced coins skipped entirely this tick: %s", fenced_skipped)
        if foreign_skipped:
            log.debug("sentinel: short-position(s) on long-only bot skipped as foreign "
                      "(direction-guard): %s", foreign_skipped)

    def _check_db_rows(self, positions, db_rows, seen, fenced_mod) -> None:
        for r in db_rows:
            if fenced_mod._coin_present(r["coin"], positions.keys()):
                continue
            self._emit("I2_phantom_db_row", "CRITICAL", r["coin"],
                       f"trades.db open row id={r.get('id')} has NO live exchange position "
                       "(phantom row — exit unrecorded or foreign close)", seen)

    def _check_orphan_triggers(self, positions, db_rows, triggers, seen, fenced_mod,
                               bot_owned, placed_oids) -> None:
        db_open = {r["coin"] for r in db_rows}
        for t in triggers:
            coin, oid = t.get("coin"), t.get("oid")
            if coin is None or oid is None:
                continue
            if not (t.get("is_trigger") and t.get("reduce_only")):
                continue
            if fenced_mod._coin_present(coin, positions.keys()):
                continue  # has something to reduce
            if fenced_mod._coin_present(coin, db_open):
                continue  # fresh-open eventual-consistency race
            if fenced_mod._fenced(coin, db_open=db_open, bot_owned=bot_owned,
                                  oid=oid, placed_oids=placed_oids):
                log.debug("sentinel: orphan trigger %s oid=%s is FENCED (manual/foreign) "
                          "— left alone", coin, oid)
                continue
            self._emit("I3_orphan_trigger", "WARNING", coin,
                       f"reduce-only trigger oid={oid} has NO position and is not fenced "
                       "— bot's orphan_sweep should cancel it after debounce; flagging only",
                       seen, {"oid": str(oid)})

    def _check_bot_liveness(self, exposure: int | None, seen) -> None:
        """I4: systemd unit active + journal heartbeat age. exposure = live positions
        + DB open rows count, or None if UNKNOWN (reads failed)."""
        sev = "WARNING" if exposure == 0 else "CRITICAL"
        expo_txt = ("UNKNOWN" if exposure is None else str(exposure))
        try:
            r = self.runner(["systemctl", "is-active", self.bot_unit],
                            capture_output=True, text=True, timeout=15)
            state = (r.stdout or "").strip() or "unknown"
        except Exception as e:
            log.warning("sentinel: systemctl is-active %s failed (%s) — liveness UNKNOWN",
                        self.bot_unit, e)
            return
        if state != "active":
            self._emit("I4_bot_dead", sev, None,
                       f"bot unit {self.bot_unit} is {state!r} (exposure={expo_txt} "
                       "open positions+rows)", seen, {"unit": self.bot_unit, "state": state})
            return
        # heartbeat: age of the LAST journal line from the bot unit
        try:
            r = self.runner(["journalctl", "-u", self.bot_unit, "-n", "1", "-o", "json",
                             "--no-pager"], capture_output=True, text=True, timeout=15)
            line = (r.stdout or "").strip().splitlines()
            ts_us = int(json.loads(line[-1])["__REALTIME_TIMESTAMP"]) if line else None
        except Exception as e:
            if not self._hb_unknown_warned:
                self._hb_unknown_warned = True
                log.warning("sentinel: journalctl heartbeat read failed (%s) — heartbeat "
                            "UNKNOWN (unit is active; no violation asserted). Check that "
                            "the sentinel user can read the journal (adm group).", e)
            return
        if ts_us is None:
            return
        age = self.now_fn() - ts_us / 1e6
        if age > self.heartbeat_max_s:
            self._emit("I4_bot_hung", sev, None,
                       f"bot unit {self.bot_unit} is active but journal heartbeat is "
                       f"{age:.0f}s old (> {self.heartbeat_max_s:.0f}s) — hung process "
                       f"(exposure={expo_txt})", seen,
                       {"unit": self.bot_unit, "heartbeat_age_s": round(age, 1)})

    # ---------------- protect mode ----------------------------------------------

    def _protect(self, pos: dict, row: dict) -> str:
        """Re-place a reduce-only SL for a BOT-OWNED naked position from its DB row.
        Idempotent + race-safe vs the bot's own heal. Returns an action tag (logged
        + recorded in the violation)."""
        coin, szi = pos["key"], pos["szi"]
        settings = self._settings()
        if getattr(settings, "dry_run", False):
            return "skip_dry_run"
        direction = str(row.get("direction") or ("long" if szi > 0 else "short")).lower()
        if (direction == "long") != (szi > 0):
            log.critical("sentinel protect %s: DB row direction=%s contradicts live "
                         "szi=%s — NOT protecting (identity unclear)", coin, direction, szi)
            return "abort_direction_mismatch"
        sl_px = _f(row.get("sl_current")) or _f(row.get("sl_initial"))
        if not sl_px or sl_px <= 0:
            log.critical("sentinel protect %s: DB row has no usable sl_current/sl_initial "
                         "— cannot re-place, alert only", coin)
            return "abort_no_sl_in_row"

        # Re-verify absence IMMEDIATELY before placing (race vs bot heal). A raise
        # means UNKNOWN -> assume live -> do not place (canonical assume-live law).
        try:
            prior = {str(x) for x in (self.client.list_open_sl_orders(coin) or [])}
        except Exception as e:
            log.warning("sentinel protect %s: pre-place SL list failed (%s) — assume "
                        "live, NOT placing", coin, e)
            return "skip_assume_live"
        if prior:
            log.info("sentinel protect %s: SL appeared between detection and placement "
                     "(%d resting) — nothing to do", coin, len(prior))
            return "skip_already_covered"

        # Liq-law clamp (REMEDY-B only; the sentinel never adds margin).
        liq = self._liq_px(_base(coin), pos)
        buf = self.liq_buf_pct
        if liq is not None:
            if szi > 0 and sl_px <= liq * (1 + buf):
                sl_px = liq * (1 + buf) * 1.0001
                log.warning("sentinel protect %s: sl_current outside liq — clamped to %s",
                            coin, sl_px)
            elif szi < 0 and sl_px >= liq * (1 - buf):
                sl_px = liq * (1 - buf) * 0.9999
                log.warning("sentinel protect %s: sl_current outside liq — clamped to %s",
                            coin, sl_px)

        # Instant-fire guard: a stop on the wrong side of mark closes the position
        # NOW — the sentinel never closes positions -> abort loudly instead.
        try:
            mark = _f(self.client.mark_price(coin))
        except Exception as e:
            log.critical("sentinel protect %s: mark_price failed (%s) — cannot prove the "
                         "SL won't instant-fire, NOT placing", coin, e)
            return "abort_no_mark"
        if not mark or mark <= 0:
            log.critical("sentinel protect %s: no mark — NOT placing", coin)
            return "abort_no_mark"
        if (szi > 0 and sl_px >= mark) or (szi < 0 and sl_px <= mark):
            log.critical("sentinel protect %s: SL px=%s is beyond mark=%s — placing would "
                         "INSTANT-CLOSE the position; NOT placing (sentinel never closes)",
                         coin, sl_px, mark)
            return "abort_would_instant_fire"

        is_buy_to_close = szi < 0
        oid = None
        for attempt in range(3):
            try:
                resp = self.client.trigger_sl(coin=coin, is_buy=is_buy_to_close,
                                              sz=abs(szi), trigger_px=sl_px)
            except Exception as e:
                log.warning("sentinel protect %s: trigger_sl attempt %d/3 raised: %s",
                            coin, attempt + 1, e)
                time.sleep(0.5 * (attempt + 1))
                continue
            try:
                statuses = resp["response"]["data"]["statuses"]
                if statuses and "resting" in statuses[0]:
                    oid = statuses[0]["resting"].get("oid")
            except Exception:
                oid = None
            if oid is not None:
                break
            log.warning("sentinel protect %s: trigger_sl attempt %d/3 no resting oid: %s",
                        coin, attempt + 1, resp)
            time.sleep(0.5 * (attempt + 1))
        if oid is None:
            log.critical("sentinel protect %s: SL placement FAILED after retries — STILL "
                         "NAKED (will retry next tick)", coin)
            return "place_failed"

        log.critical("SENTINEL-PROTECT %s: re-placed reduce-only SL px=%s sz=%s oid=%s "
                     "(from DB row id=%s sl_current) — bot heal will adopt/rotate it",
                     coin, sl_px, abs(szi), oid, row.get("id"))

        # Place-then-recheck: if the bot's own heal placed concurrently, cancel OUR
        # just-placed duplicate (ONLY our oid — never anything else).
        try:
            after = {str(x) for x in (self.client.list_open_sl_orders(coin) or [])}
        except Exception as e:
            log.warning("sentinel protect %s: post-place re-list failed (%s) — cannot "
                        "check for duplicate; leaving our SL resting", coin, e)
            return f"placed_oid={oid}_dup_check_unknown"
        others = after - {str(oid)}
        if others:
            log.warning("sentinel protect %s: bot heal raced us (%d other SL(s) now "
                        "resting) — cancelling OUR duplicate oid=%s only", coin,
                        len(others), oid)
            try:
                self.client.cancel_sl_order(coin, oid)
                return "duplicate_cancelled_own"
            except Exception as e:
                log.error("sentinel protect %s: cancel of our duplicate oid=%s failed: %s "
                          "(harmless: reduce-only)", coin, oid, e)
                return "duplicate_cancel_failed"
        return f"placed_oid={oid}"

    # ---------------- tick -------------------------------------------------------

    def tick(self):
        """One sentinel pass. Returns the list of violation records asserted this
        tick, or None if the tick was skipped (reads UNKNOWN). I4 liveness runs even
        when exchange reads fail (it does not depend on them)."""
        self.tick_n += 1
        seen: set = set()
        exposure = None
        try:
            positions = self._read_positions()
            db_rows = self._read_db_rows()
            exposure = len(positions) + len(db_rows)
        except ReadFailure as e:
            log.warning("sentinel: tick %d SKIPPED — %s (state UNKNOWN, no violations "
                        "asserted)", self.tick_n, e)
            self._check_bot_liveness(exposure, seen)
            return None

        # Indeterminate-state guard (mirror of orphan_sweep step 4): an EMPTY
        # positions view while DB rows exist may be a transient false-empty.
        if not positions and db_rows:
            log.warning("sentinel: tick %d SKIPPED — open_positions empty but %d DB-open "
                        "row(s): indeterminate (fail-safe)", self.tick_n, len(db_rows))
            self._check_bot_liveness(exposure, seen)
            return None

        try:
            triggers = self._read_triggers()
        except ReadFailure as e:
            log.warning("sentinel: tick %d SKIPPED — %s (state UNKNOWN)", self.tick_n, e)
            self._check_bot_liveness(exposure, seen)
            return None

        try:
            bot_owned, placed_oids = self._read_journal_history()
        except ReadFailure as e:
            log.warning("sentinel: tick %d journal-history read failed (%s) — fences "
                        "degrade conservative (FENCED direction), I3 may under-flag",
                        self.tick_n, e)
            bot_owned, placed_oids = None, None

        trig_meta = self._read_trigger_meta()
        fenced_mod = self._fence_mod()

        out = []
        self._check_positions(positions, db_rows, triggers, trig_meta, seen, fenced_mod)
        self._check_db_rows(positions, db_rows, seen, fenced_mod)
        self._check_orphan_triggers(positions, db_rows, triggers, seen, fenced_mod,
                                    bot_owned, placed_oids)
        self._check_bot_liveness(exposure, seen)
        self._resolve_stale(seen)
        for key in seen:
            out.append({"invariant": key[0], "coin": key[1]})
        if not out:
            log.info("sentinel: tick %d OK — %d position(s), %d DB row(s), %d trigger(s), "
                     "0 violations", self.tick_n, len(positions), len(db_rows), len(triggers))
        return out

    def run_forever(self, tick_s: float | None = None):
        tick_s = tick_s if tick_s is not None else _env_float("SENTINEL_TICK_S", 60.0)
        log.info("sentinel starting: venue=%s mode=%s unit=%s tick=%.0fs hb_max=%.0fs "
                 "liq_buf=%.4f violations=%s", self.venue, self.mode, self.bot_unit,
                 tick_s, self.heartbeat_max_s, self.liq_buf_pct, self.violations_path)
        # 429-adaptive backoff (2026-07-03): the sentinel shares the venue rate budget
        # with the live bot from a SEPARATE process (no shared pacer). When a tick sees
        # any 429, stretch the next sleep (x2 per consecutive 429-tick, cap x5) so the
        # sentinel yields the budget to the bot instead of competing with it.
        backoff = 1
        while True:
            t0 = time.monotonic()
            self._saw_429 = False
            try:
                self.tick()
            except Exception as e:
                if "429" in str(e):
                    self._saw_429 = True
                log.error("sentinel: tick %d crashed (%s) — continuing", self.tick_n, e,
                          exc_info=True)
            if self._saw_429:
                backoff = min(backoff * 2, 5)
                log.warning("sentinel: 429 seen this tick — backing off to %.0fs "
                            "(x%d) to yield rate budget to the bot", tick_s * backoff, backoff)
            else:
                backoff = 1
            time.sleep(max(1.0, tick_s * backoff - (time.monotonic() - t0)))


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("SENTINEL_LOG_LEVEL", "INFO").upper(),
        format=LOG_FORMAT,
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    venue = detect_venue()
    mode = (os.environ.get("SENTINEL_MODE") or "alert").strip().lower()
    if mode not in ("alert", "protect"):
        log.warning("SENTINEL_MODE=%r unknown — defaulting to alert", mode)
        mode = "alert"
    client = build_client(venue)
    Sentinel(venue, client, mode).run_forever()


if __name__ == "__main__":
    main()
