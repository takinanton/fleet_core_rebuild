"""fleet_core.conformance.suite — pytest suite over the contract checks.

Run (local, reference fake binding — must be green before any venue work):
    /usr/bin/python3 -m pytest fleet_core/conformance -q

Run (on a bot host, from the bot root, venue binding):
    venv/bin/python -m pytest fleet_core/conformance -q --venue=hl
(pytest missing in the bot venv? use the no-dependency runner:
    venv/bin/python fleet_core/conformance/runner.py --venue=hl)

The actual assertions live in checks.py (pytest-free — runner.py drives the
same list). This module only parameterizes them and adds the P3 crash-at-
transition placeholders.
"""

from __future__ import annotations

import pytest

from fleet_core.conformance.checks import (
    CONFORMANCE_CHECKS,
    CONSTRUCTION_CHECKS,
    DRY_WRITE_CHECKS,
    P3_TRANSITIONS,
    SkipCheck,
)


# ---------------------------------------------------------------------------
# Construction-invariant tests — pure, venue-independent, always run
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name,fn", CONSTRUCTION_CHECKS, ids=[n for n, _ in CONSTRUCTION_CHECKS])
def test_construction(name, fn):
    fn()


# ---------------------------------------------------------------------------
# Contract conformance × fault matrix — parameterized by --venue binding
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name,fn", CONFORMANCE_CHECKS, ids=[n for n, _ in CONFORMANCE_CHECKS])
def test_conformance(fresh_ctx, name, fn):
    try:
        fn(fresh_ctx)
    except SkipCheck as e:
        pytest.skip(str(e))


# ---------------------------------------------------------------------------
# Offline-construction smoke — proves the venue adapter builds under the
# intercept-all guard with fabricated creds (raw adapter only; does not
# require the venue's contract implementation to have landed)
# ---------------------------------------------------------------------------

def test_raw_adapter_constructs_offline(venue_name):
    from fleet_core.conformance.bindings import (
        BindingUnavailable,
        get_binding,
    )
    binding = get_binding(venue_name)
    smoke = getattr(binding, "smoke_construct", None)
    if smoke is None:
        pytest.skip("binding %s has no smoke_construct" % venue_name)
    try:
        proof = smoke()
    except BindingUnavailable as e:
        pytest.skip(str(e))
    assert proof


# ---------------------------------------------------------------------------
# P3 scaffolding — crash-at-each-transition (entry state machine).
# SKIPPED placeholders by design: they land with the P3 pending-journal-row /
# startup-reconcile / untracked-protect state machine. The transition matrix
# is frozen here so P3 implements against a stable contract.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("transition", P3_TRANSITIONS)
def test_crash_at_transition_recovers(transition):
    pytest.skip(
        "P3 placeholder: kill -9 at %r must leave no naked position and no "
        "phantom/missing trades.db row after restart reconcile — lands with "
        "the fleet_core entry state machine (P3)" % transition)


# ---------------------------------------------------------------------------
# DRY-mode write-isolation lane (F2 regression guard) — defined LAST so the
# DRY context (own transport gate) is only instantiated after every main-
# context test ran (pytest executes in definition order; the two patched
# gates must never interleave).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name,fn", DRY_WRITE_CHECKS, ids=[n for n, _ in DRY_WRITE_CHECKS])
def test_dry_write_isolation(fresh_dry_ctx, name, fn):
    try:
        fn(fresh_dry_ctx)
    except SkipCheck as e:
        pytest.skip(str(e))


@pytest.mark.parametrize("transition", P3_TRANSITIONS)
def test_crash_at_transition_journal_consistent(transition):
    pytest.skip(
        "P3 placeholder: journal/exchange consistency after crash at %r "
        "(pending-row present or absent exactly per transition) — lands "
        "with the fleet_core entry state machine (P3)" % transition)
