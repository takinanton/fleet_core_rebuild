"""suppression_mirror — mirrored LIVE internal suppression state for the shadow runner.

P3 design authority: proofs/p3_design_shadow_runner.md §3 (input row "live internal
suppression state (F6)") + §6.4 KF11. Round-3 approved; do not re-design.

WHAT THIS MIRRORS AND WHY
The live bot's `_ENTRY_ABORT_COOLDOWN` (extended/pacifica/nado trader.py:73-76) and
`opens_today` counter are IN-MEMORY and fed by live-only events (post-fill aborts the
shadow never experiences). Without a mirror, "live rejects because of an abort-cooldown
the shadow never felt" manufactures an enter-vs-reject strategy-delta and stalls the
48h gate. The mirror reconstructs live's suppression state from LIVE-OBSERVABLE events:

  (a) `rejected_signals` rows in the live trades.db (opened read-only, sqlite URI
      mode=ro) — incl. reason `entry_cooldown_active` and cap-breach/partial abort
      labels;
  (b) journalctl abort / `_register_entry_abort` / restart lines (unit start markers)
      — delivered through an injected provider (offline-testable; the production
      provider shells journalctl with the mandatory ' UTC' --since suffix,
      feedback_journalctl_since_is_host_local_tz);
  (c) new pending/open rows in the live trades.db.

LAWS (verbatim from the design doc):
  * Suppression-mirror law (F6): the shadow's ENTRY pipeline consults the MIRRORED
    cooldown/opens_today state, never its own hypothetical history.
  * Every observed live abort arms a mirrored cooldown (coin, venue cooldown value —
    900s / 15 min, same value on all 4 venues).
  * Every observed live bot RESTART clears the mirrored cooldown map (live loses the
    in-memory map — W9/CW10) and re-arms `opens_today` from today's live-DB open rows.
  * Cap counters mirror LIVE actuals: `opens_today` = count of LIVE entries in the
    SAME UTC DAY (opens_today resets at UTC midnight — same-UTC-day horizon).
    Shadow hypothetical entries are logged elsewhere but NEVER increment the mirror.
  * The retained event log (aborts + restarts + rejects, with ts) is the KF11 matcher
    input (comparator §6.4): abort-cooldown sub-case horizon = cooldown_sec;
    opens_today sub-case horizon = since last live restart within the same UTC day.

This module is pure stdlib, imports no venue SDK, and performs no live I/O by itself
(the sqlite path and the journal provider are injected).
"""

from __future__ import annotations

import datetime as _dt
import logging
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger("fleet_core.engine.suppression_mirror")

__all__ = [
    "MirrorEvent",
    "SuppressionMirror",
    "JournalctlProvider",
    "utc_day",
]

# Venue entry-abort cooldown: ext/nado/hl 15 min == pac 900 s — SAME value fleet-wide
# (shadow doc §3; entry-SM: "900s value preserved"). Not arbitrary: live constant.
DEFAULT_COOLDOWN_SEC = 900.0

# rejected_signals.reason substrings that evidence a LIVE ENTRY ABORT (arm a mirrored
# cooldown). Sourced from the live abort label set (shadow doc §3: "cap-breach/partial
# abort labels"; exit-engine §6 abort strings). Substring match, case-insensitive.
ABORT_REASON_MARKERS: Tuple[str, ...] = (
    "cap_breach",
    "partial",
    "abort",
    "sl_placement_failed_3x_naked_position_closed",
    "sl_outside_liquidation_invariant_fail",
)

# reason that evidences live's cooldown GATE firing (a live reject we align against;
# it does NOT arm anything — the arming event is the abort itself).
COOLDOWN_REJECT_REASON = "entry_cooldown_active"


def utc_day(ts: float) -> str:
    """UTC calendar day 'YYYY-MM-DD' for an epoch ts."""
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%d")


def _parse_iso_ts(s: str) -> Optional[float]:
    """ISO text (journal created_at/opened_at) -> epoch seconds (UTC). None if unparseable."""
    if not s:
        return None
    txt = s.strip().replace("Z", "+00:00")
    try:
        d = _dt.datetime.fromisoformat(txt)
    except ValueError:
        try:  # bare 'YYYY-MM-DD HH:MM:SS'
            d = _dt.datetime.strptime(txt[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                d = _dt.datetime.strptime(txt[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_dt.timezone.utc)  # live journal writes UTC ISO
    return d.timestamp()


@dataclass(frozen=True)
class MirrorEvent:
    """One live-observable suppression event (retained — KF11 matcher corpus)."""
    kind: str            # 'abort' | 'restart' | 'cooldown_reject' | 'live_open'
    ts: float            # epoch seconds (event time as observed)
    coin: Optional[str] = None
    source: str = ""     # 'rejected_signals' | 'journal' | 'trades'
    detail: Mapping[str, Any] = field(default_factory=dict)


class JournalctlProvider:
    """Production journal-event provider: shells journalctl for the LIVE bot unit.

    Returns parsed event dicts: {'kind': 'abort'|'restart', 'ts': epoch, 'coin': str|None,
    'raw': line}. The --since argument ALWAYS carries the ' UTC' suffix
    (feedback_journalctl_since_is_host_local_tz — journalctl --since is host-local TZ).

    NOT exercised in offline selftests (no live I/O); injected fakes replace it.
    """

    # abort markers per shadow doc §3/§5: `_register_entry_abort` lines + abort labels.
    ABORT_LINE_MARKERS: Tuple[str, ...] = ("_register_entry_abort", "ENTRY ABORT", "entry_abort")
    # systemd unit start marker (restart evidence — W9/CW10 memory-loss event).
    RESTART_LINE_MARKERS: Tuple[str, ...] = ("Started ",)

    def __init__(self, unit: str, journalctl_bin: str = "journalctl") -> None:
        self.unit = unit
        self.journalctl_bin = journalctl_bin

    def __call__(self, since_epoch: float) -> List[Mapping[str, Any]]:
        since = _dt.datetime.fromtimestamp(since_epoch, _dt.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S")
        cmd = [
            self.journalctl_bin, "-u", self.unit,
            "--since", since + " UTC",          # ' UTC' suffix is MANDATORY
            "--no-pager", "-o", "short-unix",
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if out.returncode != 0:
            raise RuntimeError("journalctl failed rc=%s: %s" % (out.returncode, out.stderr[:200]))
        events: List[Mapping[str, Any]] = []
        for line in out.stdout.splitlines():
            parts = line.split(None, 1)
            if not parts:
                continue
            try:
                ts = float(parts[0])
            except ValueError:
                continue
            body = parts[1] if len(parts) > 1 else ""
            if any(m in body for m in self.RESTART_LINE_MARKERS):
                events.append({"kind": "restart", "ts": ts, "coin": None, "raw": body})
            elif any(m in body for m in self.ABORT_LINE_MARKERS):
                events.append({"kind": "abort", "ts": ts,
                               "coin": _extract_coin(body), "raw": body})
        return events


def _extract_coin(line: str) -> Optional[str]:
    """Best-effort coin extraction from a live abort log line (coin=XXX / [XXX])."""
    for token in line.replace(",", " ").split():
        if token.startswith("coin="):
            return token[5:].strip("'\"")
    return None


class SuppressionMirror:
    """Mirror of the live bot's in-memory suppression state (design §3, F6).

    Inputs are injected — no hidden I/O:
      live_db_path      : live trades.db (opened read-only per refresh, URI mode=ro)
      journal_provider  : Callable[[since_epoch], Sequence[event-dict]] — production =
                          JournalctlProvider(unit); tests = fakes.
      cooldown_sec      : venue cooldown value (default 900s — live constant).

    Public read surface (what the shadow ENTRY pipeline consults):
      cooldown_active(coin, now)  -> evidence dict | None
      opens_today(now)            -> int (LIVE actuals, same-UTC-day horizon)
      last_restart_same_utc_day(now) -> epoch | None (KF11 sub-case ii horizon input)
      snapshot(now)               -> stampable dict for the decision log
      events                      -> retained MirrorEvent list (KF11 matcher corpus)
    """

    def __init__(
        self,
        venue: str,
        live_db_path: str,
        journal_provider: Callable[[float], Sequence[Mapping[str, Any]]],
        cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
        abort_reason_markers: Sequence[str] = ABORT_REASON_MARKERS,
    ) -> None:
        self.venue = venue
        self.live_db_path = live_db_path
        self.journal_provider = journal_provider
        self.cooldown_sec = float(cooldown_sec)
        self.abort_reason_markers = tuple(m.lower() for m in abort_reason_markers)

        # Mirrored live state
        self._cooldowns: Dict[str, Dict[str, Any]] = {}   # coin -> {'until', 'armed_ts', 'source'}
        self._restarts: List[float] = []                  # observed live unit-start epochs
        self.events: List[MirrorEvent] = []               # retained corpus (append-only)

        # Poll cursors
        self._last_rejected_id = 0
        self._last_trade_id = 0
        self._last_journal_ts = 0.0
        self._primed = False

    # ------------------------------------------------------------------ refresh

    def _ro_conn(self) -> sqlite3.Connection:
        con = sqlite3.connect("file:%s?mode=ro" % self.live_db_path, uri=True, timeout=5.0)
        con.row_factory = sqlite3.Row
        return con

    def refresh(self, now: Optional[float] = None) -> None:
        """Refresh the mirror at tick start (mirroring rule, design §3).

        Order matters: journal events first (restarts clear the map), then DB rows
        (aborts observed via rejected_signals arm cooldowns; both sources may carry
        the same abort — arming twice with the same window is idempotent).
        """
        now = time.time() if now is None else now
        if not self._primed:
            # First refresh: look back one cooldown window so an abort just before
            # shadow start still arms (otherwise the first 15 min are mirror-blind
            # by construction — a KF11-documented gap, minimized here).
            self._last_journal_ts = now - self.cooldown_sec
            self._primed = True

        # (b) journal events
        try:
            jevents = list(self.journal_provider(self._last_journal_ts))
        except Exception as exc:  # provider failure = mirror-blind tick, logged loud
            log.error("suppression mirror journal provider failed: %s", exc)
            jevents = []
        for ev in sorted(jevents, key=lambda e: float(e.get("ts", 0.0))):
            ts = float(ev.get("ts", 0.0))
            if ts <= self._last_journal_ts:
                continue
            kind = ev.get("kind")
            if kind == "restart":
                self._observe_restart(ts)
            elif kind == "abort":
                self._observe_abort(str(ev.get("coin") or ""), ts, "journal",
                                    {"raw": ev.get("raw", "")})
            self._last_journal_ts = max(self._last_journal_ts, ts)

        # (a) + (c) live DB rows
        try:
            con = self._ro_conn()
        except sqlite3.Error as exc:
            log.error("suppression mirror: live DB open failed: %s", exc)
            return
        try:
            for row in con.execute(
                "SELECT id, created_at, coin, reason FROM rejected_signals WHERE id > ? "
                "ORDER BY id", (self._last_rejected_id,)
            ):
                self._last_rejected_id = max(self._last_rejected_id, int(row["id"]))
                ts = _parse_iso_ts(row["created_at"]) or now
                reason = (row["reason"] or "").lower()
                if COOLDOWN_REJECT_REASON in reason:
                    self.events.append(MirrorEvent(
                        "cooldown_reject", ts, row["coin"], "rejected_signals",
                        {"reason": row["reason"], "row_id": int(row["id"])}))
                elif any(m in reason for m in self.abort_reason_markers):
                    self._observe_abort(row["coin"], ts, "rejected_signals",
                                        {"reason": row["reason"], "row_id": int(row["id"])})
            for row in con.execute(
                "SELECT id, coin, opened_at, status FROM trades WHERE id > ? ORDER BY id",
                (self._last_trade_id,)
            ):
                self._last_trade_id = max(self._last_trade_id, int(row["id"]))
                if row["status"] in ("open", "closed") and row["opened_at"]:
                    ts = _parse_iso_ts(row["opened_at"]) or now
                    self.events.append(MirrorEvent(
                        "live_open", ts, row["coin"], "trades",
                        {"trade_id": int(row["id"])}))
        finally:
            con.close()

        # expire mirrored cooldowns
        for coin in [c for c, cd in self._cooldowns.items() if cd["until"] <= now]:
            del self._cooldowns[coin]

    def _observe_abort(self, coin: str, ts: float, source: str,
                       detail: Mapping[str, Any]) -> None:
        coin = coin or ""
        self.events.append(MirrorEvent("abort", ts, coin or None, source, dict(detail)))
        if coin:
            cur = self._cooldowns.get(coin)
            until = ts + self.cooldown_sec
            if cur is None or until > cur["until"]:
                self._cooldowns[coin] = {"until": until, "armed_ts": ts, "source": source}
                log.info("mirror: cooldown armed coin=%s until=%.0f (live abort @%.0f via %s)",
                         coin, until, ts, source)

    def _observe_restart(self, ts: float) -> None:
        """Live unit restart: live LOSES its in-memory cooldown map (W9/CW10) — the
        mirror clears too. opens_today re-arms from live-DB rows (which is how
        opens_today() always computes — live actuals, so no extra state needed;
        the restart ts itself is the KF11 sub-case (ii) horizon anchor)."""
        self.events.append(MirrorEvent("restart", ts, None, "journal", {}))
        self._restarts.append(ts)
        if self._cooldowns:
            log.info("mirror: live restart @%.0f — clearing %d mirrored cooldowns "
                     "(live lost its in-memory map)", ts, len(self._cooldowns))
        self._cooldowns.clear()

    # ------------------------------------------------------------- read surface

    def cooldown_active(self, coin: str, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Mirrored live cooldown for `coin` — the shadow entry pipeline MUST gate on
        this, never on shadow-hypothetical history (F6 law)."""
        now = time.time() if now is None else now
        cd = self._cooldowns.get(coin)
        if cd and cd["until"] > now:
            return {"coin": coin, "until": cd["until"], "armed_ts": cd["armed_ts"],
                    "source": cd["source"], "cooldown_sec": self.cooldown_sec}
        return None

    def opens_today(self, now: Optional[float] = None) -> int:
        """LIVE entries opened in the decision's UTC day (live-DB actuals; shadow
        hypothetical entries NEVER counted — design §3 cap-counter law)."""
        now = time.time() if now is None else now
        day = utc_day(now)
        try:
            con = self._ro_conn()
        except sqlite3.Error as exc:
            log.error("suppression mirror: opens_today DB read failed: %s", exc)
            raise
        try:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM trades WHERE opened_at IS NOT NULL "
                "AND substr(replace(opened_at,'T',' '),1,10)=?", (day,)).fetchone()
            return int(row["n"])
        finally:
            con.close()

    def last_restart_same_utc_day(self, now: Optional[float] = None) -> Optional[float]:
        """Most recent observed live restart within the SAME UTC day as `now` —
        the KF11 opens_today sub-case horizon anchor (R3 / R2-F2)."""
        now = time.time() if now is None else now
        day = utc_day(now)
        same_day = [t for t in self._restarts if t <= now and utc_day(t) == day]
        return max(same_day) if same_day else None

    def restarts(self) -> Sequence[float]:
        return tuple(self._restarts)

    def snapshot(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Stampable mirror state for the decision log (audit / KF11 evidence)."""
        now = time.time() if now is None else now
        try:
            opens = self.opens_today(now)
        except Exception:
            opens = None  # logged loud in opens_today; snapshot records the blindness
        return {
            "cooldowns": {c: dict(cd) for c, cd in self._cooldowns.items()},
            "opens_today": opens,
            "last_restart_same_utc_day": self.last_restart_same_utc_day(now),
            "events_n": len(self.events),
        }


# ============================================================================ selftest

def _selftest() -> None:  # pragma: no cover — exercised by module __main__
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "trades.db")
        con = sqlite3.connect(db)
        con.executescript(
            """
            CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT,
                coin TEXT, tf TEXT, status TEXT, opened_at TEXT);
            CREATE TABLE rejected_signals (id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, coin TEXT, tf TEXT, direction TEXT,
                trigger_price REAL, entry_price REAL, sl_price REAL,
                reason TEXT, walk_slip_pct REAL, vol_1h_usd REAL);
            """
        )
        # Deterministic anchor: 12:00 UTC of the CURRENT day. `time.time()`
        # flaked for the first hour after UTC midnight (the −3600 open row
        # landed on YESTERDAY's UTC day → opens_today counted 0; caught
        # 2026-07-03 00:43 UTC). Noon keeps every offset used below
        # (−3600 … +1400) inside one UTC day, always.
        now = float(int(time.time() // 86400) * 86400 + 43200)
        iso = lambda t: _dt.datetime.fromtimestamp(t, _dt.timezone.utc).isoformat()
        con.execute("INSERT INTO rejected_signals (created_at, coin, tf, reason) "
                    "VALUES (?,?,?,?)", (iso(now - 100), "XRP", "8h",
                                         "entry_price_cap_breach_partial_abort"))
        con.execute("INSERT INTO trades (created_at, coin, tf, status, opened_at) "
                    "VALUES (?,?,?,?,?)", (iso(now - 3600), "BTC", "8h", "open", iso(now - 3600)))
        con.commit(); con.close()

        jevents: List[Mapping[str, Any]] = []
        mirror = SuppressionMirror("extended", db, lambda since: jevents)
        mirror.refresh(now)
        # abort from rejected_signals armed a 900s cooldown
        cd = mirror.cooldown_active("XRP", now)
        assert cd and abs(cd["until"] - (now - 100 + 900.0)) < 1e-6, cd
        assert mirror.cooldown_active("BTC", now) is None
        # opens_today = live actuals (1 open row today)
        assert mirror.opens_today(now) == 1
        # after cooldown horizon it expires
        mirror.refresh(now + 1000)
        assert mirror.cooldown_active("XRP", now + 1000) is None

        # journal abort arms; restart clears (live memory-loss mirrored)
        jevents = [{"kind": "abort", "ts": now + 1100, "coin": "SOL", "raw": "x"}]
        mirror.journal_provider = lambda since: jevents
        mirror.refresh(now + 1200)
        assert mirror.cooldown_active("SOL", now + 1200)
        jevents = [{"kind": "restart", "ts": now + 1300, "coin": None, "raw": "Started"}]
        mirror.refresh(now + 1400)
        assert mirror.cooldown_active("SOL", now + 1400) is None
        assert mirror.last_restart_same_utc_day(now + 1400) is not None or \
            utc_day(now + 1300) != utc_day(now + 1400)
        # event corpus retained for KF11
        kinds = [e.kind for e in mirror.events]
        assert "abort" in kinds and "restart" in kinds and "live_open" in kinds, kinds
        snap = mirror.snapshot(now + 1400)
        assert snap["opens_today"] == 1
    print("suppression_mirror selftest OK")


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    _selftest()
