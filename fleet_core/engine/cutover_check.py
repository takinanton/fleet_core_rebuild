"""fleet_core.engine.cutover_check — flat-or-protected assert (P3 rollout §1).

Spec: p3_rollout.md §1 (F1-F7) + §4 GATE (--pre-rollback) + §1-F7 / exit-engine
§8.1 (--verify-strategy-pins). READ-ONLY: the trades.db is opened with sqlite
URI mode=ro; the only venue calls are P2 reads. Exit codes per §1: 0 GREEN /
1 RED / 2 UNKNOWN-reads (F6: ReadUnknown on ANY probe = the check FAILS —
unknown != ok, bias-to-protect; never proceed on UNKNOWN).

Conditions (rollout §1):
  F1  no non-terminal rows: legacy status='pending' OR (post-migration)
      sm_state in {INTENT, PENDING, FILLED, ABORTING}.
  F2  every venue position (live open_positions(), cache-invalidated) is
      FENCED (manual/foreign/FX_EXCLUDE/direction-guard) or matches EXACTLY
      one DB open row. Fence source = orphan_sweep._fenced with the PINNED
      position-context kwargs (entry-SM §4.2 R2-F1d):
        _fenced(coin, db_open=<open-state row coins>, bot_owned=None,
                oid=None, placed_oids=None)
      Direction-guard stacks on top (short on a long-only bot = foreign,
      07-02 rule; env ALLOW_SHORTS=1 disables). HL manual ЮК positions are
      excluded from F2-match but included in fence verification (§5-HL) —
      naturally: fenced positions need no row match.
      management_class='manual_claimed' rows fence their position (full
      hands-off, entry-SM §2.1) — verdict FENCED_CLAIMED, F3 skipped.
  F3  every matched open row's SL CONFIRMED LIVE via list_open_sl_orders(coin)
      readback (not just non-NULL in DB). protect_only rows (entry-SM
      §4.2.3-3a/3b) are protected iff ANY reduce-only trigger covers the coin
      (their sl_* columns are NULL by design).
  F4  no unmatched resting BOT-OWN reduce-only triggers (orphans): bot-own =
      oid in placed_trigger_oids registry (DB table ∪ trades oid columns ∪
      legacy placed_trigger_oids.json next to the DB — entry-SM §2.2).
      Non-registry triggers are NEVER flagged (manual bracket awaiting fill).
  F5  entry path quiesced: env ENTRY_FREEZE=1 OR >=120s past every venue-TF
      bar boundary (N=120s DERIVED = 2 x 60s tick period, rollout F11b).
  F6  ReadUnknown on any probe => exit 2.
  F7  --verify-strategy-pins: md5 of the venue's LIVE deployed strategy files
      == exit-engine §8.1 pins; HL adds the NEGATIVE pin (no import resolves
      to bot/strategy_xnn.py) and the effective-N==15 env assert
      (hl/bot/main.py:660 block re-run against the live .env: TF_8H_K /
      DONCHIAN_K seam; DONCHIAN_TFS must be {8h}).

PHANTOM ROWS (open-state row, position ABSENT) are not part of the §1 F-set
(money is flat => not unprotected) and therefore do NOT flip the verdict:
  * management_class in {protect_only, manual_claimed}: WARN with the
    explicit remedy "DB-only row-resolve — drain.py --resolve-phantom"
    (review nit 2: manual_claimed gets the SAME phantom-K3 DB-only
    row-resolve as protect_only; the row-close touches the DB, not the venue).
  * strategy rows: WARN "reconciler resolves at startup (entry-SM §4.1
    OPEN-absent: fills-attributed t_close or keep-OPEN + CRITICAL)".

--pre-rollback (rollout §4 GATE): same check, gating on F1+F2+F3 (the three
conditions the GATE row lists); F4 reported informationally; F5/F7 skipped
(the engine may be dead — that is exactly when rollback is contemplated).
--engine-view (step 6): identical checks + admin-socket status line; requires
the post-migration sm_state column; output table format identical so step-3 /
step-6 outputs can be diffed.

Offline selftest: ``python3 cutover_check.py --selftest`` (fake client + temp
DB + temp bot-root; no venue SDKs).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

__all__ = ["run_check", "verify_strategy_pins", "STRATEGY_PINS"]

# ---------------------------------------------------------------------------
# Labeled constants
# ---------------------------------------------------------------------------

NON_TERMINAL_SM = ("INTENT", "PENDING", "FILLED", "ABORTING")  # rollout §1 F1
QUIESCE_SEC = 120.0   # rollout §1 F5: N=120s DERIVED = 2 x 60s tick period (F11b)
DEFAULT_TFS = "8h"    # all venues run DONCHIAN_TFS=8h (rollout §2)
VALIDATED_DONCHIAN_N = 15  # exit-engine §8.1 / hl main.py:660: effective N must == 15

# exit-engine §8.1 md5 pins (re-measured on the live host by this tool;
# a mismatch = RED — pins are evidence-refreshed, never trusted from doc text)
CANONICAL_XNN_MD5 = "4a96b9793923b7ba0cf70440a25b40e4"   # fleet_core/strategy_xnn.py
SHIM_XNN_MD5 = "12705f970a5697f2324d670be3e771ab"        # bot/strategy_xnn.py 11-line shim
HL_DONCHIAN_MD5 = "e903953f4a45c77340d6d49240a70fe2"     # hl bot/strategy_donchian.py
HL_US29_MD5 = "f04e747eeed6163718c1ab336523460d"         # hl bot/strategy_us29.py
HL_NEGATIVE_XNN_MD5 = "72b2f1870c64310b4b0e34f3d21c053b"  # hl bot/strategy_xnn.py — NEVER on import path

STRATEGY_PINS: Dict[str, List[Tuple[str, str, str]]] = {
    # venue -> [(relpath under bot_root, expected md5, label)]
    "extended": [("fleet_core/strategy_xnn.py", CANONICAL_XNN_MD5, "canonical"),
                 ("bot/strategy_xnn.py", SHIM_XNN_MD5, "loader shim")],
    "pacifica": [("fleet_core/strategy_xnn.py", CANONICAL_XNN_MD5, "canonical"),
                 ("bot/strategy_xnn.py", SHIM_XNN_MD5, "loader shim")],
    "nado":     [("fleet_core/strategy_xnn.py", CANONICAL_XNN_MD5, "canonical"),
                 ("bot/strategy_xnn.py", SHIM_XNN_MD5, "loader shim")],
    "hl":       [("bot/strategy_donchian.py", HL_DONCHIAN_MD5, "crypto leg"),
                 ("bot/strategy_us29.py", HL_US29_MD5, "us29 leg")],
}


class ProbeUnknown(RuntimeError):
    """F6: a probe could not produce verified data — whole check = UNKNOWN."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_env_file(path: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _variants(coin: str) -> Set[str]:
    c = str(coin).upper()
    base = c.replace("-PERP", "").replace("-USD", "")
    return {c, base, base + "-USD", base + "-PERP"}


def _tf_seconds(tf: str) -> int:
    tf = tf.strip().lower()
    mult = {"m": 60, "h": 3600, "d": 86400}[tf[-1]]
    return int(tf[:-1]) * mult


def _ro_conn(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    return con


def _has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    return col in {r["name"] for r in con.execute("PRAGMA table_info(%s)" % table)}


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _default_fence(coin: str, db_open: Sequence[str]) -> bool:
    """orphan_sweep._fenced with the PINNED position-context kwargs
    (entry-SM §4.2 R2-F1d). Lazy import; any machinery failure raises
    ProbeUnknown (F6 — cannot verify the fence => UNKNOWN, not exposed)."""
    try:
        from fleet_core.orphan_sweep import _fenced  # lazy; stdlib-only module
    except Exception as e:  # noqa: BLE001
        raise ProbeUnknown("fence machinery unavailable: %s" % e)
    return bool(_fenced(coin, db_open=list(db_open), bot_owned=None,
                        oid=None, placed_oids=None))


def _default_client_factory(venue: str):
    from fleet_core.venues import get_client  # lazy — pulls SDKs only on live host
    return get_client(venue)


# ---------------------------------------------------------------------------
# Core check
# ---------------------------------------------------------------------------

def run_check(venue: str,
              db_path: str,
              env_path: Optional[str] = None,
              pre_rollback: bool = False,
              engine_view: bool = False,
              verify_pins: bool = False,
              bot_root: Optional[str] = None,
              sock_path: Optional[str] = None,
              client: Any = None,
              fence_fn: Optional[Callable[[str, Sequence[str]], bool]] = None,
              now_utc: Optional[float] = None,
              out: Callable[[str], None] = print) -> int:
    """Run the flat-or-protected check. Returns 0 GREEN / 1 RED / 2 UNKNOWN."""
    reds: List[str] = []
    warns: List[str] = []
    unknowns: List[str] = []
    rows_out: List[List[str]] = []
    env = _parse_env_file(env_path)
    fence = fence_fn or _default_fence

    mode = "pre-rollback GATE (rollout §4)" if pre_rollback else \
        ("engine-view (rollout §3 step 6)" if engine_view else "cutover (rollout §3 step 3)")
    out("# cutover_check venue=%s mode=%s db=%s" % (venue, mode, db_path))

    if client is None:
        try:
            client = _default_client_factory(venue)
        except Exception as e:  # noqa: BLE001
            unknowns.append("venue client construction failed: %s" % e)
            client = None

    # ---- engine-view: admin socket status (informational + sm_state required)
    if engine_view:
        try:
            from fleet_core.engine.write_canary import EngineAdminClient, resolve_sock_path
        except Exception:  # direct-run fallback (no package __init__ needed)
            import importlib.util
            _p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "write_canary.py")
            _s = importlib.util.spec_from_file_location("_wc_cc", _p)
            _m = importlib.util.module_from_spec(_s)
            _s.loader.exec_module(_m)  # type: ignore[union-attr]
            EngineAdminClient, resolve_sock_path = _m.EngineAdminClient, _m.resolve_sock_path
        sp = resolve_sock_path(sock_path, db_path)
        try:
            st = EngineAdminClient(sp).request("state")
            out("engine status: %s" % json.dumps(st, sort_keys=True))
        except Exception as e:  # noqa: BLE001
            unknowns.append("engine admin socket unreachable (%s): %s" % (sp, e))

    # ---- DB reads (read-only) ----
    try:
        con = _ro_conn(db_path)
    except sqlite3.Error as e:
        out("RED: cannot open db read-only: %s" % e)
        return 1
    try:
        has_sm = _has_column(con, "trades", "sm_state")
        has_mc = _has_column(con, "trades", "management_class")
        if engine_view and not has_sm:
            reds.append("--engine-view requires the post-migration sm_state column")

        # F1 — non-terminal rows
        if has_sm:
            q = ("SELECT id, coin, tf, COALESCE(sm_state,'') AS st FROM trades "
                 "WHERE sm_state IN (%s)" % ",".join("?" * len(NON_TERMINAL_SM)))
            nonterm = con.execute(q, NON_TERMINAL_SM).fetchall()
        else:
            nonterm = con.execute(
                "SELECT id, coin, tf, status AS st FROM trades WHERE status='pending'"
            ).fetchall()
        for r in nonterm:
            reds.append("F1 non-terminal row id=%s coin=%s state=%s%s"
                        % (r["id"], r["coin"], r["st"],
                           " — DRAIN required before rollback (rollout §4)" if pre_rollback else ""))

        # open rows
        open_rows = con.execute(
            "SELECT * FROM trades WHERE status='open' ORDER BY id").fetchall()
        # db_open coins for the pinned fence kwargs: open-state rows
        if has_sm:
            db_open = [r["coin"] for r in con.execute(
                "SELECT coin FROM trades WHERE sm_state IN "
                "('FILLED','PROTECTED','OPEN','ABORTING')")]
        else:
            db_open = [r["coin"] for r in open_rows]

        # bot-own oid registry (entry-SM §2.2 union)
        registry: Set[str] = set()
        if _has_table(con, "placed_trigger_oids"):
            registry |= {str(r["oid"]) for r in con.execute(
                "SELECT oid FROM placed_trigger_oids")}
        for r in con.execute("SELECT sl_order_id, tp1_order_id FROM trades"):
            for v in (r["sl_order_id"], r["tp1_order_id"]):
                if v:
                    registry.add(str(v))
        legacy_json = os.path.join(os.path.dirname(os.path.abspath(db_path)),
                                   "placed_trigger_oids.json")
        if os.path.exists(legacy_json):
            try:
                with open(legacy_json) as f:
                    data = json.load(f)
                oids = data.get("oids", data) if isinstance(data, dict) else data
                registry |= {str(x) for x in oids}
            except Exception as e:  # noqa: BLE001
                unknowns.append("legacy placed_trigger_oids.json unreadable: %s" % e)
    finally:
        con.close()

    # ---- venue probes ----
    positions: Dict[str, Any] = {}
    triggers: List[Any] = []
    if client is not None:
        try:
            client.invalidate_positions_cache()   # F2: live probe, not snapshot
            positions = dict(client.open_positions())
        except Exception as e:  # noqa: BLE001
            unknowns.append("open_positions ReadUnknown: %s" % e)
        try:
            triggers = list(client.list_reduce_only_triggers())
        except Exception as e:  # noqa: BLE001
            unknowns.append("list_reduce_only_triggers ReadUnknown: %s" % e)

    def _sl_oids(coin: str) -> Optional[List[str]]:
        if client is None:
            return None
        try:
            return [str(o) for o in client.list_open_sl_orders(coin)]
        except Exception as e:  # noqa: BLE001
            unknowns.append("list_open_sl_orders(%s) ReadUnknown: %s" % (coin, e))
            return None

    matched_row_ids: Set[int] = set()

    # F2 + F3 per position
    for coin, pos in sorted(positions.items()):
        size = getattr(pos, "size_signed", None)
        fenced_label = ""
        # direction-guard stacks on top (07-02 rule; long-only fleet)
        allow_shorts = env.get("ALLOW_SHORTS", "0").strip().lower() in ("1", "true", "yes")
        try:
            if fence(coin, db_open):
                fenced_label = "fenced"
        except ProbeUnknown as e:
            unknowns.append(str(e))
            rows_out.append([coin, "-", str(size), "-", "-", "UNKNOWN", "UNKNOWN-fence"])
            continue
        if not fenced_label and size is not None and size < 0 and not allow_shorts:
            fenced_label = "fenced(direction-guard)"

        matches = [r for r in open_rows if _variants(r["coin"]) & _variants(coin)]
        # manual_claimed rows fence their position (entry-SM §2.1 full hands-off)
        claimed = [r for r in matches
                   if has_mc and (r["management_class"] or "") == "manual_claimed"]
        if claimed and not fenced_label:
            fenced_label = "fenced(manual_claimed)"
            matched_row_ids.update(r["id"] for r in claimed)

        if fenced_label:
            rows_out.append([coin, matches and "open" or "-", "%s" % size, "-", "-",
                             fenced_label, "FENCED"])
            continue

        if len(matches) != 1:
            reds.append("F2 position %s matches %d open rows (need exactly 1)"
                        % (coin, len(matches)))
            rows_out.append([coin, "%d rows" % len(matches), "%s" % size, "-", "-",
                             "-", "RED F2"])
            continue
        row = matches[0]
        matched_row_ids.add(row["id"])
        mclass = (row["management_class"] or "strategy") if has_mc else "strategy"
        live = _sl_oids(coin)
        if live is None:
            rows_out.append([coin, "open#%d" % row["id"], "%s" % size,
                             str(row["sl_order_id"] or ""), "UNKNOWN", mclass, "UNKNOWN F3"])
            continue
        if mclass == "protect_only":
            # 3a/3c: protected iff ANY reduce-only trigger covers the coin
            ok = bool(live)
            if not ok:
                reds.append("F3 protect_only %s: no covering trigger resting" % coin)
            rows_out.append([coin, "open#%d" % row["id"], "%s" % size,
                             str(row["sl_order_id"] or "NULL(3a)"),
                             "yes" if ok else "NO", mclass,
                             "OK" if ok else "RED F3"])
            continue
        sl_oid = row["sl_order_id"]
        ok = bool(sl_oid) and str(sl_oid) in live
        if not ok:
            reds.append("F3 %s row#%d sl_order_id=%r NOT confirmed live "
                        "(list_open_sl_orders=%s)" % (coin, row["id"], sl_oid, live))
        rows_out.append([coin, "open#%d" % row["id"], "%s" % size,
                         str(sl_oid or "NULL"), "yes" if ok else "NO", mclass,
                         "OK" if ok else "RED F3 NAKED"])

    # phantom rows (row open, position absent) — WARN, not part of the §1 F-set
    for row in open_rows:
        if row["id"] in matched_row_ids:
            continue
        if any(_variants(row["coin"]) & _variants(c) for c in positions):
            continue
        mclass = (row["management_class"] or "strategy") if has_mc else "strategy"
        if mclass in ("protect_only", "manual_claimed"):
            remedy = ("DB-only row-resolve — drain.py --resolve-phantom "
                      "(phantom-K3, entry-SM §4.2.3 authority (ii); manual_claimed "
                      "gets the SAME DB-only resolve — review nit 2)")
        else:
            remedy = ("reconciler resolves at startup (entry-SM §4.1 OPEN-absent: "
                      "fills-attributed t_close or keep-OPEN + CRITICAL)")
        warns.append("PHANTOM_ROW open#%d %s (%s): %s"
                     % (row["id"], row["coin"], mclass, remedy))
        rows_out.append([row["coin"], "open#%d" % row["id"], "ABSENT",
                         str(row["sl_order_id"] or ""), "-", mclass, "WARN phantom"])

    # F4 — orphan bot-own triggers
    open_coins_v: Set[str] = set()
    for r in open_rows:
        open_coins_v |= _variants(r["coin"])
    pos_coins_v: Set[str] = set()
    for c in positions:
        pos_coins_v |= _variants(c)
    for trg in triggers:
        t_oid = str(getattr(trg, "oid", ""))
        t_coin = str(getattr(trg, "coin", ""))
        own = t_oid in registry
        orphan = own and not (_variants(t_coin) & (open_coins_v | pos_coins_v))
        if orphan:
            msg = "F4 orphan bot-own trigger oid=%s coin=%s (no row, no position)" % (t_oid, t_coin)
            if pre_rollback:
                warns.append(msg + " [informational in pre-rollback mode]")
            else:
                reds.append(msg)
            rows_out.append([t_coin, "-", "-", t_oid, "yes",
                             "bot-own", "WARN F4" if pre_rollback else "RED F4 orphan"])
        elif not own:
            rows_out.append([t_coin, "-", "-", t_oid, "yes",
                             "non-registry", "leave (manual bracket)"])

    # F5 — entry path quiesced (skipped in pre-rollback: engine may be dead)
    if not pre_rollback:
        frozen = env.get("ENTRY_FREEZE", "0").strip() == "1"
        if frozen:
            out("F5: ENTRY_FREEZE=1 — entries frozen, quiesce satisfied")
        else:
            t = now_utc if now_utc is not None else time.time()
            tfs = [x.strip() for x in env.get("DONCHIAN_TFS", DEFAULT_TFS).split(",") if x.strip()]
            for tf in tfs:
                try:
                    sec = _tf_seconds(tf)
                except (KeyError, ValueError):
                    unknowns.append("F5: unparseable TF %r in DONCHIAN_TFS" % tf)
                    continue
                since = t - (int(t) // sec) * sec
                if since < QUIESCE_SEC:
                    reds.append("F5 not quiesced: only %.0fs past %s bar boundary "
                                "(<%.0fs = 2 x 60s tick, F11b) and ENTRY_FREEZE!=1"
                                % (since, tf, QUIESCE_SEC))

    # F7 — strategy pins
    if verify_pins:
        pr, pu = verify_strategy_pins(venue, bot_root, env, out=out)
        reds.extend(pr)
        unknowns.extend(pu)

    # ---- output ----
    out("")
    out("| coin | DB state | exchange size | SL oid | SL live? | fence | verdict |")
    out("|---|---|---|---|---|---|---|")
    for r in rows_out:
        out("| " + " | ".join(r) + " |")
    out("")
    for w in warns:
        out("WARN: %s" % w)
    for r in reds:
        out("RED: %s" % r)
    for u in unknowns:
        out("UNKNOWN: %s" % u)
    if reds:
        out("VERDICT: RED (%d)" % len(reds))
        return 1
    if unknowns:
        out("VERDICT: UNKNOWN (%d) — F6: unknown != ok; never proceed" % len(unknowns))
        return 2
    out("VERDICT: GREEN")
    return 0


# ---------------------------------------------------------------------------
# F7 — strategy pins (exit-engine §8.1)
# ---------------------------------------------------------------------------

def verify_strategy_pins(venue: str, bot_root: Optional[str],
                         env: Dict[str, str],
                         pins: Optional[Dict[str, List[Tuple[str, str, str]]]] = None,
                         out: Callable[[str], None] = print) -> Tuple[List[str], List[str]]:
    """Returns (reds, unknowns). Re-hashes the LIVE deployed files vs the
    §8.1 pins; HL adds the negative pin + the effective-N==15 env assert."""
    reds: List[str] = []
    unknowns: List[str] = []
    pins = pins or STRATEGY_PINS
    if not bot_root:
        unknowns.append("F7: --verify-strategy-pins requires --bot-root")
        return reds, unknowns
    for rel, want, label in pins.get(venue, []):
        p = os.path.join(bot_root, rel)
        if not os.path.exists(p):
            reds.append("F7 pin file missing: %s (%s)" % (p, label))
            continue
        got = _md5_file(p)
        if got != want:
            reds.append("F7 pin MISMATCH %s (%s): md5 %s != pinned %s"
                        % (rel, label, got, want))
        else:
            out("F7 pin OK: %s == %s (%s)" % (rel, want, label))
    if venue == "hl":
        # NEGATIVE pin: no HL import resolves to bot/strategy_xnn.py (§8.1)
        for mod in ("main.py", "scanner.py", "trader.py"):
            p = os.path.join(bot_root, "bot", mod)
            if not os.path.exists(p):
                continue
            try:
                with open(p) as f:
                    src = f.read()
            except OSError as e:
                unknowns.append("F7 negative pin: cannot read %s: %s" % (p, e))
                continue
            for ln in src.splitlines():
                s = ln.split("#", 1)[0]
                if ("import strategy_xnn" in s) or ("from strategy_xnn" in s) or \
                        ("from bot.strategy_xnn" in s) or ("from .strategy_xnn" in s) or \
                        ("import bot.strategy_xnn" in s):
                    reds.append("F7 NEGATIVE pin violated: %s imports strategy_xnn "
                                "(stale XNN adapter, VSTOP 0.15 — never port): %r"
                                % (mod, ln.strip()))
        # effective-N assert (hl/bot/main.py:660 block, re-run vs live .env):
        # per-TF K = env TF_8H_K else global env DONCHIAN_K (config default 20);
        # eff_n = K if K>0 else DONCHIAN_N(15); must == 15. TFS must be {8h}.
        try:
            k = int(float(env.get("TF_8H_K", env.get("DONCHIAN_K", "20"))))
        except ValueError:
            k = 20
        eff_n = k if k > 0 else VALIDATED_DONCHIAN_N
        if eff_n != VALIDATED_DONCHIAN_N:
            reds.append("F7 CONFIG-ASSERT: HL donchian effective N=%d != validated %d "
                        "(env DONCHIAN_K/TF_8H_K seam; config default is 20) — set "
                        "DONCHIAN_K=%d" % (eff_n, VALIDATED_DONCHIAN_N, VALIDATED_DONCHIAN_N))
        else:
            out("F7 CONFIG-ASSERT: HL donchian effective N=%d == validated — OK" % eff_n)
        tfs = {x.strip().lower() for x in env.get("DONCHIAN_TFS", DEFAULT_TFS).split(",") if x.strip()}
        if tfs != {"8h"}:
            reds.append("F7 CONFIG-ASSERT: DONCHIAN_TFS=%s != validated {8h} "
                        "(main.py:660 block)" % sorted(tfs))
    return reds, unknowns


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="P3 flat-or-protected check (p3_rollout.md §1). Read-only. "
                    "Exit 0 GREEN / 1 RED / 2 UNKNOWN-reads.")
    p.add_argument("--venue", choices=["extended", "pacifica", "nado", "hl"])
    p.add_argument("--db", help="live trades.db path (opened read-only)")
    p.add_argument("--env", help="live .env path (ENTRY_FREEZE / DONCHIAN_* seams)")
    p.add_argument("--pre-rollback", action="store_true",
                   help="rollout §4 GATE variant (F1+F2+F3 gate; F5/F7 skipped)")
    p.add_argument("--engine-view", action="store_true",
                   help="rollout §3 step 6: run against the new engine's view")
    p.add_argument("--verify-strategy-pins", action="store_true",
                   help="rollout §1 F7: re-hash live strategy files vs §8.1 pins")
    p.add_argument("--bot-root", help="live bot deploy root (for --verify-strategy-pins)")
    p.add_argument("--sock", help="engine admin socket (for --engine-view status)")
    p.add_argument("--selftest", action="store_true")
    a = p.parse_args(argv)
    if a.selftest:
        return _selftest()
    if not a.venue or not a.db:
        p.error("--venue and --db are required (unless --selftest)")
    return run_check(a.venue, a.db, env_path=a.env, pre_rollback=a.pre_rollback,
                     engine_view=a.engine_view, verify_pins=a.verify_strategy_pins,
                     bot_root=a.bot_root, sock_path=a.sock)


# ---------------------------------------------------------------------------
# Offline selftest — fakes only
# ---------------------------------------------------------------------------

class _FakePos:
    def __init__(self, coin: str, size_signed: float):
        self.coin, self.size_signed = coin, size_signed


class _FakeTrig:
    def __init__(self, coin: str, oid: str):
        self.coin, self.oid = coin, oid


class _FakeClient:
    def __init__(self, positions=None, sl=None, triggers=None,
                 pos_unknown=False):
        self._pos = positions or {}
        self._sl = sl or {}
        self._trg = triggers or []
        self._pos_unknown = pos_unknown

    def invalidate_positions_cache(self):
        return None

    def open_positions(self):
        if self._pos_unknown:
            raise RuntimeError("ReadUnknown: venue down")
        return dict(self._pos)

    def list_open_sl_orders(self, coin):
        return list(self._sl.get(coin, []))

    def list_reduce_only_triggers(self):
        return list(self._trg)


def _mkdb(path: str, rows: List[Dict[str, Any]], migrated: bool = True,
          registry: Optional[List[Tuple[str, str]]] = None) -> None:
    con = sqlite3.connect(path)
    cols = ("id INTEGER PRIMARY KEY, coin TEXT, tf TEXT, status TEXT, "
            "sl_order_id TEXT, tp1_order_id TEXT")
    if migrated:
        cols += ", sm_state TEXT, management_class TEXT"
    con.execute("CREATE TABLE trades (%s)" % cols)
    for r in rows:
        keys = sorted(r)
        con.execute("INSERT INTO trades (%s) VALUES (%s)"
                    % (",".join(keys), ",".join("?" * len(keys))),
                    [r[k] for k in keys])
    if registry is not None:
        con.execute("CREATE TABLE placed_trigger_oids (oid TEXT PRIMARY KEY, "
                    "coin TEXT, trade_id INTEGER, placed_at TEXT, kind TEXT)")
        for oid, coin in registry:
            con.execute("INSERT INTO placed_trigger_oids VALUES (?,?,NULL,'t','sl')",
                        (oid, coin))
    con.commit()
    con.close()


def _selftest() -> int:
    import tempfile
    fails: List[str] = []
    no_fence = lambda coin, db_open: False  # noqa: E731
    sink = lambda s: None  # noqa: E731
    frozen_env_dir = tempfile.mkdtemp(prefix="cc_env_")
    envp = os.path.join(frozen_env_dir, ".env")
    with open(envp, "w") as f:
        f.write("ENTRY_FREEZE=1\nDONCHIAN_TFS=8h\n")

    def case(name, expect, **kw):
        d = tempfile.mkdtemp(prefix="cc_st_")
        db = os.path.join(d, "trades.db")
        _mkdb(db, kw.pop("rows"), migrated=kw.pop("migrated", True),
              registry=kw.pop("registry", None))
        rc = run_check("extended", db, env_path=envp, fence_fn=kw.pop("fence", no_fence),
                       out=sink, **kw)
        ok = rc == expect
        print("[%s] %s (rc=%d want %d)" % ("PASS" if ok else "FAIL", name, rc, expect))
        if not ok:
            fails.append(name)

    strat_open = {"id": 1, "coin": "BTC", "tf": "8h", "status": "open",
                  "sl_order_id": "sl1", "tp1_order_id": None,
                  "sm_state": "OPEN", "management_class": "strategy"}
    # 1. GREEN: protected matched position
    case("green_protected", 0, rows=[strat_open],
         client=_FakeClient({"BTC": _FakePos("BTC", 0.1)}, {"BTC": ["sl1"]}))
    # 2. RED F3: SL not live
    case("red_naked_sl", 1, rows=[strat_open],
         client=_FakeClient({"BTC": _FakePos("BTC", 0.1)}, {"BTC": []}))
    # 3. RED F1: FILLED row (post-migration)
    case("red_nonterminal_filled", 1,
         rows=[dict(strat_open, id=2, sm_state="FILLED", status="pending")],
         client=_FakeClient())
    # 4. RED F1 legacy: status=pending, no sm_state column
    case("red_legacy_pending", 1, migrated=False,
         rows=[{"id": 3, "coin": "ETH", "tf": "8h", "status": "pending",
                "sl_order_id": None, "tp1_order_id": None}],
         client=_FakeClient())
    # 5. RED F2: unfenced unmatched position
    case("red_unmatched_position", 1, rows=[],
         client=_FakeClient({"SOL": _FakePos("SOL", 5.0)}, {}))
    # 6. GREEN: fenced position needs no row (manual/foreign)
    case("green_fenced_position", 0, rows=[],
         client=_FakeClient({"BNB": _FakePos("BNB", 1.0)}, {}),
         fence=lambda c, dbo: c == "BNB")
    # 7. GREEN: direction-guard fences a short on the long-only fleet
    case("green_direction_guard", 0, rows=[],
         client=_FakeClient({"DOGE": _FakePos("DOGE", -100.0)}, {}))
    # 8. UNKNOWN: open_positions ReadUnknown -> exit 2 (F6)
    case("unknown_positions", 2, rows=[], client=_FakeClient(pos_unknown=True))
    # 9. RED F4: orphan bot-own trigger (registry member, no row/position)
    case("red_orphan_trigger", 1, rows=[], registry=[("t9", "XRP")],
         client=_FakeClient({}, {}, [_FakeTrig("XRP", "t9")]))
    # 10. GREEN: non-registry trigger left alone (manual bracket)
    case("green_manual_trigger_left", 0, rows=[],
         client=_FakeClient({}, {}, [_FakeTrig("XRP", "user1")]))
    # 11. GREEN + WARN: manual_claimed phantom row = DB-only resolve (nit 2)
    case("green_phantom_manual_claimed", 0,
         rows=[dict(strat_open, id=4, coin="xyz_GOLD", sl_order_id=None,
                    management_class="manual_claimed")],
         client=_FakeClient({}, {}))
    # 12. GREEN: manual_claimed row fences its live position (hands-off)
    case("green_manual_claimed_position", 0,
         rows=[dict(strat_open, id=5, coin="xyz_GOLD", sl_order_id=None,
                    management_class="manual_claimed")],
         client=_FakeClient({"xyz_GOLD": _FakePos("xyz_GOLD", 2.0)}, {}))
    # 13. protect_only covered by user trigger (sl_* NULL by design, 3a)
    case("green_protect_only_covered", 0,
         rows=[dict(strat_open, id=6, coin="OP", sl_order_id=None,
                    management_class="protect_only")],
         client=_FakeClient({"OP": _FakePos("OP", 10.0)}, {"OP": ["user_sl"]}))
    # 14. RED: protect_only cover vanished
    case("red_protect_only_uncovered", 1,
         rows=[dict(strat_open, id=7, coin="OP", sl_order_id=None,
                    management_class="protect_only")],
         client=_FakeClient({"OP": _FakePos("OP", 10.0)}, {"OP": []}))
    # 15. pre-rollback GATE: ABORTING row blocks
    case("red_prerollback_aborting", 1, pre_rollback=True,
         rows=[dict(strat_open, id=8, sm_state="ABORTING", status="pending")],
         client=_FakeClient())
    # 16. pre-rollback: orphan trigger is informational only
    case("green_prerollback_orphan_warn", 0, pre_rollback=True,
         rows=[], registry=[("t16", "XRP")],
         client=_FakeClient({}, {}, [_FakeTrig("XRP", "t16")]))

    # 17/18. pins: mechanism check via custom pins on temp files
    d = tempfile.mkdtemp(prefix="cc_pin_")
    os.makedirs(os.path.join(d, "bot"))
    fp = os.path.join(d, "bot", "s.py")
    with open(fp, "w") as f:
        f.write("N = 15\n")
    good = _md5_file(fp)
    env_ok = {"DONCHIAN_K": "15", "TF_8H_K": "15", "DONCHIAN_TFS": "8h"}
    r, u = verify_strategy_pins("hl", d, env_ok,
                                pins={"hl": [("bot/s.py", good, "test")]}, out=sink)
    ok = not r and not u
    print("[%s] pins_match (reds=%s unknowns=%s)" % ("PASS" if ok else "FAIL", r, u))
    if not ok:
        fails.append("pins_match")
    r, u = verify_strategy_pins("hl", d, {"DONCHIAN_K": "20"},
                                pins={"hl": [("bot/s.py", "deadbeef", "test")]}, out=sink)
    ok = len(r) >= 2  # md5 mismatch + effective-N=20 assert RED
    print("[%s] pins_mismatch_and_effN (reds=%d)" % ("PASS" if ok else "FAIL", len(r)))
    if not ok:
        fails.append("pins_mismatch_and_effN")
    # 19. real repo pins: canonical strategy_xnn.py hash equals the §8.1 pin
    canon = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                         "strategy_xnn.py")
    if os.path.exists(canon):
        ok = _md5_file(canon) == CANONICAL_XNN_MD5
        print("[%s] repo_canonical_pin" % ("PASS" if ok else "FAIL"))
        if not ok:
            fails.append("repo_canonical_pin")

    print("\nselftest: %d failures" % len(fails))
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
