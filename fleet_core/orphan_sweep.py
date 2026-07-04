"""Per-cycle orphan reduce-only TRIGGER sweep (class-fix 2026-06-15).

Incident (mem:project_nado_ondo_orphan_trigger_2026_06_15): a reduce-only TRIGGER
order (SL/TP) can be left resting on the exchange after its position closes. The
reconciler only cancels protective triggers while it is ACTIVELY MANAGING a
tracked position; when the position disappears — bot close, an exchange-side SL
firing, or a foreign/manual position the bot was briefly tracking — the leftover
/ sibling trigger is orphaned and persists. It is inert (reduce-only with nothing
to reduce) but never auto-cancelled. Nado example: ONDO-PERP sell-trigger
0xb82ca928… resting with NO ONDO position (ONDO closed 06-11).

This sweep runs once per main loop and cancels a reduce-only trigger ONLY when its
coin has NO live position AT ALL, behind several fail-safe guards so it can never
cancel a live position's protective stop or a deliberately-placed manual order:

  * is_trigger AND reduce_only        -> never touches resting ENTRY stop-orders
                                         nor reduce-only LIMIT take-profits (those
                                         are how the manual ЮК workflow places TPs)
  * coin NOT explicitly fenced        -> FX_EXCLUDE / UNIVERSE_SYMBOL_EXCLUDE /
                                         FOREIGN_EXCLUDE_COINS / FOREIGN_SKIP_PREFIXES
                                         / _is_fx are user/foreign-managed and are
                                         left completely alone (pre-placed-bracket
                                         safety on a deliberately fenced coin)
  * coin has NO live exchange position-> open_positions(), variant-normalized across
                                         bare / -USD / -PERP. A live position (bot OR
                                         manual) has something to reduce -> EXEMPT.
  * coin NOT in the bot in-memory dict -> the bot is not tracking/expecting it
  * coin has NO status='open' row in trades.db
                                       -> the bot's own bookkeeping agrees the coin
                                          is flat. Blip-immune protection for bot
                                          positions and for the fresh-open
                                          eventual-consistency race (trigger placed,
                                          position not yet visible in open_positions).
  * indeterminate-state guard          -> if open_positions() is empty WHILE the bot
                                          still has DB-open rows, the read is treated
                                          as a possible fetch failure and the whole
                                          sweep is skipped (never act on a maybe-empty
                                          positions view). mem:ib_instant_max_hold —
                                          money-path fallback must fail-SAFE.
  * sustained >= DEBOUNCE_SEC, >=2 obs -> mirror of the manage_open_position
                                          phantom-close 90s guard (here 180s, since
                                          cancelling an inert order is non-urgent);
                                          lets the bot's own close path cancel its
                                          sibling trigger first and absorbs blips.

On cancel: cancels via the venue trigger path (cancel_sl_order — NOT cancel_order,
which silently no-ops on Nado triggers), then RE-LISTS and asserts the oid is gone,
emitting a greppable ORPHAN-TRIGGER-SWEEP journal line. See
mem:feedback_naked_heal_must_exempt_fx_exclude_manual.
"""

import logging
import time

# Venue seam (Nado): structural trigger-client-unavailable class. Only Nado ships
# bot/exchange_nado.py; on HL / Pacifica / Extended this import fails -> None -> the
# plain step-1 enumeration handler below (identical to those venues' original path).
# On Nado the exchange module is already a hard dependency of bot.main, so a broken
# exchange_nado kills the process at startup either way.
try:
    from bot.exchange_nado import TriggerClientUnavailable
except ImportError:
    TriggerClientUnavailable = None

_log = logging.getLogger(__name__)

DEBOUNCE_SEC = 180.0  # sustained-orphan window before cancelling (non-urgent cleanup)


def _variants(coin) -> set:
    """Name variants to match a coin across open_positions / trades.db / fences.
    Venues use bare 'BNB', 'BNB-USD' or 'BNB-PERP' interchangeably."""
    if coin is None:
        return set()
    c = str(coin).upper()
    base = c.replace("-PERP", "").replace("-USD", "")
    return {c, base, f"{base}-USD", f"{base}-PERP"}


def _coin_present(coin, keys) -> bool:
    v = _variants(coin)
    for k in keys:
        if _variants(k) & v:
            return True
    return False


def _fenced(coin, db_open=None, bot_owned=None, oid=None, placed_oids=None) -> bool:
    """True if `coin` is explicitly fenced as manual/foreign (user-managed). Such a
    coin's triggers are NEVER touched, even with no position (a deliberately
    pre-placed manual bracket awaiting fill must survive).

    FAIL DIRECTION (audit 2026-07-02): a BROKEN fence check must fence, not expose.
    The old 'except Exception: pass' meant any fence that started throwing silently
    downgraded to NOT-FENCED — the sweep would then cancel manual/foreign SL triggers.
    Now: ImportError = fence not defined on this venue (expected, skip that fence);
    ANY other exception = fence broken -> CRITICAL naming it + treat coin as FENCED.

    db_open / bot_owned / oid / placed_oids are the HL shared-unified-account per-oid
    discriminators consumed ONLY by the MANUAL_POSITION_PREFIXES block below; venues
    whose bot.config does not define MANUAL_POSITION_PREFIXES never read them (the
    standalone 1-arg `_fenced(coin)` callsites in main.py keep working unchanged)."""
    v = _variants(coin)
    up = {str(x).upper() for x in v}
    # FX_EXCLUDE (all venues; on Extended this already merges FOREIGN_EXCLUDE_COINS)
    try:
        from bot.config import FX_EXCLUDE
        if up & {str(x).upper() for x in FX_EXCLUDE}:
            return True
    except ImportError:
        pass  # fence not defined on this venue
    except Exception as e:
        _log.critical("orphan-sweep: FX_EXCLUDE fence check BROKEN for %s (%s) — "
                      "fail-closed, treating as FENCED", coin, e)
        return True
    # UNIVERSE_SYMBOL_EXCLUDE (HL / Extended / Nado; Pacifica names it _SYMBOL_EXCLUDE
    # so the import legitimately fails there)
    try:
        from bot.universe import UNIVERSE_SYMBOL_EXCLUDE
        if up & {str(x).upper() for x in UNIVERSE_SYMBOL_EXCLUDE}:
            return True
    except ImportError:
        pass  # fence not defined on this venue
    except Exception as e:
        _log.critical("orphan-sweep: UNIVERSE_SYMBOL_EXCLUDE fence check BROKEN for %s (%s) — "
                      "fail-closed, treating as FENCED", coin, e)
        return True
    # Heuristic FX (Nado)
    try:
        from bot.universe import _is_fx
        if _is_fx(coin):
            return True
    except ImportError:
        pass  # fence not defined on this venue
    except Exception as e:
        _log.critical("orphan-sweep: _is_fx fence check BROKEN for %s (%s) — "
                      "fail-closed, treating as FENCED", coin, e)
        return True
    # Prefix-based MANUAL position skip (shared HL account; e.g. manual ЮК xyz_ trades).
    # HL-only: MANUAL_POSITION_PREFIXES exists only in HL's bot.config -> ImportError on
    # the other venues skips this block entirely (fence not defined = venue seam).
    # The fence was first added because MANUAL_POSITION_PREFIXES lived only in main.py, so a
    # pre-placed manual reduce-only TRIGGER SL with no live position yet would be cancelled by
    # this sweep after the 180s debounce = manual SL silently removed before fill. Same
    # manual-exemption class as the untracked-protect sweep (main.py:600). Mirrored into
    # bot.config so the sweep can read it; failure is LOUD + fail-safe FENCED.
    #
    # Audit MED (2026-06-20) — the disambiguation MUST be PER-OID, not coin-level. The combo bot
    # OWNS the entire xyz_ HIP-3 us29 leg AND the manual ЮК workflow places deliberate reduce-only
    # SL TRIGGERS on the SAME xyz_ instruments on the SAME shared unified account (live config:
    # MANUAL_POSITION_PREFIXES=xyz_, FOREIGN_SKIP_PREFIXES empty). A coin-level marker is only
    # unambiguous when the bot-traded and manual symbol sets are DISJOINT — they are NOT here:
    #   * The prior Audit-HIGH used a coin any-row test (coins_ever_traded). But once xyz_GOLD has
    #     been bot-traded even once it is PERMANENTLY in bot_owned, so a later PRE-PLACED manual SL
    #     trigger awaiting fill on xyz_GOLD (no live position, no open row) -> _has_any_row=True ->
    #     NOT fenced -> swept after the 180s debounce -> the manual SL is cancelled = manual
    #     position goes NAKED. That is the INVERSE failure of the HIGH it fixed.
    #   * A coin no-row test (the pre-HIGH behaviour) is the other horn: it neuters the sweep for
    #     the whole bot-owned leg once a bot-own row flips to 'closed' (the Nado-ONDO incident).
    #
    # The unambiguous, time-invariant marker is the TRIGGER's OWN oid: the bot registers every
    # SL/TP oid it places (journal.sl_order_id/tp1_order_id, the same value the HL API returns in
    # frontendOpenOrders -> list_reduce_only_triggers). A trigger whose oid IS in placed_oids
    # (= journal.oids_ever_placed()) is provably BOT-placed -> sweepable (NOT fenced here). A
    # prefix-matching trigger whose oid is NOT bot-placed is MANUAL/foreign -> FENCED. This is
    # exactly resting_orders.startup_orphan_sweep's _placed_oids approach, applied per-cycle.
    #
    # Fallbacks (degrade conservative = FENCED, the HIGH-risk-safe direction):
    #   * oid known + placed_oids known  -> authoritative per-oid test (the live path).
    #   * oid/placed_oids unknown (standalone callsites, or a registry-read failure upstream that
    #     passed None) -> fall back to the coin-level any-row marker (bot_owned/db_open); if THOSE
    #     are also None -> pure-prefix fence (original conservative behaviour).
    try:
        from bot.config import MANUAL_POSITION_PREFIXES
        if MANUAL_POSITION_PREFIXES:
            base = str(coin).upper().replace("-PERP", "").replace("-USD", "")
            _prefix_match = any(p and base.startswith(str(p).upper())
                                for p in MANUAL_POSITION_PREFIXES)
            if _prefix_match:
                if oid is not None and placed_oids is not None:
                    # PER-OID authoritative (live path): bot-placed oid -> sweepable; else
                    # manual -> FENCED. NO coin-level OR-in.
                    #
                    # Audit MED (2026-06-20) HOLE FIX — the prior revision OR-in'd the coin-level
                    # bot-own marker (db_open/bot_owned): a prefix-matching trigger whose oid was
                    # NOT in placed_oids was un-fenced (treated sweepable) whenever the coin had
                    # ANY trades.db row. But the us29 leg trades the ENTIRE xyz_ HIP-3 universe, so
                    # every xyz_ coin permanently has a row (coins_ever_traded), and the manual ЮК
                    # workflow places deliberate reduce-only SL TRIGGERS on the SAME xyz_ symbols on
                    # the SAME shared unified account. Net: a PRE-PLACED manual SL awaiting fill (no
                    # live position, no open row) on a bot-traded xyz_ coin -> has-any-row=True ->
                    # NOT fenced -> swept after the 180s debounce -> manual position goes NAKED.
                    # That is the exact INVERSE failure the per-OID fix was introduced to kill.
                    #
                    # The OR-in only existed to compensate for an INCOMPLETE placed_oids set: the
                    # sl_order_id/tp1_order_id COLUMNS hold only the LATEST oid (update_trade_*_order
                    # OVERWRITES), so a rotated chandelier-trail oid that stayed resting (cancel-old
                    # blip) had left the column set and looked manual. That gap is now closed at the
                    # SOURCE: journal.register_placed_trigger_oid appends EVERY placed oid to an
                    # append-only registry (placed_trigger_oids.json, mirroring resting_orders'
                    # _placed_oids) and oids_ever_placed() unions it -> a bot-own rotated oid is
                    # ALWAYS in placed_oids -> sweepable WITHOUT the coin OR-in. So the per-OID test
                    # alone is authoritative: oid NOT in placed_oids => MANUAL => FENCED.
                    if str(oid) not in {str(x) for x in placed_oids}:
                        return True
                else:
                    # Degraded (standalone callsite or upstream registry-read failure -> oid/
                    # placed_oids None): no per-oid info. Fall back to the conservative coin-level
                    # any-row marker; if the coin has no bot row at all -> MANUAL -> FENCED. Errs
                    # toward FENCED (never cancels a manual trigger when the discriminator is blind).
                    _has_open_row = db_open is not None and _coin_present(coin, db_open)
                    _has_any_row = bot_owned is not None and _coin_present(coin, bot_owned)
                    if not (_has_open_row or _has_any_row):
                        return True
    except ImportError:
        pass  # fence not defined on this venue (HL-only shared-account manual fence)
    except Exception as e:
        _log.error(
            "orphan-sweep MANUAL_POSITION_PREFIXES fence FAILED for %s (%s) — treating as "
            "FENCED (fail-safe); dead-fence guard, investigate import", coin, e,
        )
        return True
    # Prefix-based foreign skip (Pacifica / Extended / HL native-crypto on a shared account).
    # FIX-5 (2026-06-19): this fence was SILENTLY DEAD — `from bot.config import
    # FOREIGN_SKIP_PREFIXES` raised ImportError (the symbol lived only in main.py) and the
    # old bare `except Exception: pass` swallowed it, so a fence that LOOKS active never ran.
    # FOREIGN_SKIP_PREFIXES now lives in bot.config on HL/Pacifica/Extended (mirrors main.py);
    # Nado genuinely does not define it -> ImportError -> skip. A non-import failure must be
    # LOUD (a dead money-path fence can never be silent again), but must not crash the sweep.
    try:
        from bot.config import FOREIGN_SKIP_PREFIXES
        if FOREIGN_SKIP_PREFIXES:
            base = str(coin).upper().replace("-PERP", "").replace("-USD", "")
            for p in FOREIGN_SKIP_PREFIXES:
                if p and base.startswith(str(p).upper()):
                    return True
    except ImportError:
        pass  # fence not defined on this venue
    except Exception as e:
        _log.critical("orphan-sweep: FOREIGN_SKIP_PREFIXES fence check BROKEN for %s (%s) — "
                      "fail-closed, treating as FENCED", coin, e)
        return True
    return False


def sweep_orphan_triggers(client, open_positions, seen_state, log):
    """Cancel orphan reduce-only triggers (coins with no live position anywhere).

    client       : exchange adapter exposing list_reduce_only_triggers(),
                   open_positions(), cancel_sl_order(coin, oid).
    open_positions: the bot's in-memory {coin: Position} dict.
    seen_state   : caller-owned dict[(coin_upper, oid_str)] -> first_seen_ts for the
                   cross-cycle debounce. Pruned in place.
    Returns the list of cancelled (coin, oid).
    """
    # 1) Enumerate resting reduce-only triggers. The adapter RAISES on a failed
    #    venue query (false-empty class fix) -> we skip this cycle rather than act
    #    on a partial/empty view.
    #    Venue seam (Nado): where the venue defines TriggerClientUnavailable, a
    #    STRUCTURALLY dead trigger client escalates loudly and transient failures
    #    escalate WARNING->ERROR on a consecutive-failure streak (Nado variant kept
    #    verbatim). Other venues keep the plain skip-cycle warning verbatim.
    if TriggerClientUnavailable is not None:
        try:
            triggers = client.list_reduce_only_triggers()
            client._orphan_enum_fail_streak = 0
        except TriggerClientUnavailable as e:
            # Structural: the venue trigger client is None/unusable (heal also failed).
            # SL/orphan safety sweep is DISABLED and trigger PLACEMENT is likely broken
            # too. Fail LOUD (ERROR, ->CRITICAL after 3 cycles) -- never a silent skip.
            n = getattr(client, "_orphan_enum_fail_streak", 0) + 1
            client._orphan_enum_fail_streak = n
            emit = log.critical if n >= 3 else log.error
            emit("orphan-sweep: trigger client STRUCTURALLY UNAVAILABLE (%d consec cycle(s)) — "
                 "SL/orphan safety sweep DISABLED and SL placement likely broken too: %s", n, e)
            return []
        except Exception as e:
            # Transient venue query error: skip this cycle (false-empty fail-safe), but if it
            # persists, escalate WARNING->ERROR so a stuck sweep can't hide as a silent per-cycle skip.
            n = getattr(client, "_orphan_enum_fail_streak", 0) + 1
            client._orphan_enum_fail_streak = n
            if n >= 10:
                log.error("orphan-sweep: trigger enumeration FAILED %d consecutive cycles — safety "
                          "sweep effectively disabled (escalated from WARNING): %s", n, e)
            else:
                log.warning("orphan-sweep: trigger enumeration failed — skip cycle (%d): %s", n, e)
            return []
    else:
        try:
            triggers = client.list_reduce_only_triggers()
        except Exception as e:
            log.warning("orphan-sweep: trigger enumeration failed — skip cycle: %s", e)
            return []
    if not triggers:
        seen_state.clear()
        return []

    # 2) Live exchange positions — fail-SAFE on error.
    try:
        live_pos = client.open_positions()
    except Exception as e:
        log.warning("orphan-sweep: open_positions failed — skip cycle (fail-safe): %s", e)
        return []
    pos_keys = set(live_pos.keys())

    # 3) trades.db coins the bot still considers OPEN — blip-immune guard.
    #    Venue seam (HL): on the shared unified account, ALSO load the set of coins the
    #    bot has EVER traded (any status) and every SL/TP oid it EVER placed — the
    #    positive markers _fenced's MANUAL_POSITION_PREFIXES block uses to tell a
    #    bot-OWNED xyz_ trigger from a MANUAL ЮК one (Audit HIGH 2026-06-19 / MED
    #    2026-06-20). The seam is the presence of MANUAL_POSITION_PREFIXES in
    #    bot.config (HL-only): the journal history feeds ONLY that fence, so venues
    #    without the fence never read it (their journal.py may not export the helpers,
    #    and importing them there would fail-closed the whole sweep for nothing).
    try:
        from bot.config import MANUAL_POSITION_PREFIXES as _manual_fence_defined  # noqa: F401
        _journal_history = True
    except ImportError:
        _journal_history = False
    if _journal_history:
        try:
            from bot.journal import coins_ever_traded, open_trades, oids_ever_placed
            db_open = {r["coin"] for r in open_trades()}
            bot_owned = coins_ever_traded()
            placed_oids = oids_ever_placed()   # PER-OID bot-own marker (Audit MED 2026-06-20)
        except Exception as e:
            log.warning("orphan-sweep: open_trades read failed — skip cycle (fail-safe): %s", e)
            return []
    else:
        bot_owned = None
        placed_oids = None
        try:
            from bot.journal import open_trades
            db_open = {r["coin"] for r in open_trades()}
        except Exception as e:
            log.warning("orphan-sweep: open_trades read failed — skip cycle (fail-safe): %s", e)
            return []

    # 4) Indeterminate-state guard: an EMPTY positions read while the bot still has
    #    DB-open rows may be a transient fetch failure (venues return {} on error
    #    with no cache). Never treat that as "everything is flat" — skip the cycle.
    if not pos_keys and db_open:
        log.warning(
            "orphan-sweep: open_positions empty but %d DB-open row(s) — indeterminate, "
            "skip cycle (fail-safe)", len(db_open),
        )
        return []

    now = time.time()
    cancelled = []
    live_candidate_keys = set()
    for t in triggers:
        coin = t.get("coin")
        oid = t.get("oid")
        if coin is None or oid is None:
            continue
        if not (t.get("is_trigger") and t.get("reduce_only")):
            continue
        if _fenced(coin, db_open=db_open, bot_owned=bot_owned, oid=oid, placed_oids=placed_oids):
            continue                                   # manual/foreign — never touch
        if _coin_present(coin, pos_keys):
            continue                                   # live position -> has something to reduce
        if _coin_present(coin, open_positions.keys()):
            continue                                   # bot is tracking it
        if _coin_present(coin, db_open):
            continue                                   # bot bookkeeping still open (fresh-open race)

        key = (str(coin).upper(), str(oid))
        live_candidate_keys.add(key)
        first = seen_state.get(key)
        if first is None:
            seen_state[key] = now
            log.info(
                "orphan-sweep: %s reduce-only trigger %s has NO position — defer %ds to confirm",
                coin, oid, int(DEBOUNCE_SEC),
            )
            continue
        if now - first < DEBOUNCE_SEC:
            continue

        # Confirmed orphan. Cancel via the TRIGGER path (cancel_sl_order), never
        # cancel_order (silent no-op on Nado triggers — bugfix 2026-05-06).
        log.warning(
            "ORPHAN-TRIGGER-SWEEP: cancelling reduce-only trigger coin=%s oid=%s "
            "(no position, sustained %ds)", coin, oid, int(now - first),
        )
        try:
            res = client.cancel_sl_order(coin, oid)
        except Exception as e:
            log.error("orphan-sweep: cancel_sl_order(%s,%s) raised: %s", coin, oid, e)
            continue

        # ASSERT/GUARD: re-list triggers and confirm the oid is gone.
        gone = None
        try:
            after = client.list_reduce_only_triggers()
            gone = not any(str(x.get("oid")) == str(oid) for x in after)
        except Exception as e:
            log.warning("orphan-sweep: post-cancel verify failed for oid=%s: %s", oid, e)
        if gone is False:
            log.error(
                "ORPHAN-TRIGGER-SWEEP: cancel_sl_order(%s,%s) returned but trigger STILL "
                "resting — investigate (res=%s)", coin, oid, res,
            )
        else:
            log.warning("ORPHAN-TRIGGER-SWEEP: confirmed cancelled coin=%s oid=%s", coin, oid)
            cancelled.append((coin, oid))
            seen_state.pop(key, None)

    # Prune debounce state for candidates no longer present (cleared/closed).
    for k in list(seen_state.keys()):
        if k not in live_candidate_keys:
            seen_state.pop(k, None)
    return cancelled
