"""pytest wiring for the conformance suite: --venue option + binding fixtures.

sys.path note: run as `python -m pytest fleet_core/conformance` from either
the rebuild root (local) or a bot root (host) — `python -m` puts the cwd on
sys.path, which is what makes `fleet_core.*` (and on hosts `bot.*`)
importable as namespace packages.
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--venue", action="store", default="fake",
        help="binding to run the conformance suite against: "
             "fake (default, pure-python reference) | hl | pacifica | "
             "extended | nado")


@pytest.fixture(scope="session")
def venue_name(request):
    return request.config.getoption("--venue")


@pytest.fixture(scope="session")
def bound_ctx(venue_name):
    from fleet_core.conformance.bindings import (
        BindingUnavailable,
        get_binding,
    )
    try:
        binding = get_binding(venue_name)
        ctx = binding.build_context()
    except BindingUnavailable as e:
        pytest.skip("venue binding %r unavailable here: %s" % (venue_name, e))
        return  # pragma: no cover
    yield ctx
    ctx.close()


@pytest.fixture
def fresh_ctx(bound_ctx):
    """Per-test isolation: faults cleared, venue truth reset, client rebuilt."""
    bound_ctx.reset()
    return bound_ctx


@pytest.fixture(scope="session")
def dry_bound_ctx(venue_name):
    """DRY_RUN=1 context for the write-isolation lane (F2 regression guard).

    Session-scoped and instantiated by the LAST tests in suite.py (pytest
    runs in definition order), so the DRY gate's transport patchers never
    interleave with the main context's tests; torn down before bound_ctx
    (reverse creation order)."""
    from fleet_core.conformance.bindings import (
        BindingUnavailable,
        get_binding,
    )
    try:
        binding = get_binding(venue_name)
        ctx = binding.build_context(dry_run=True)
    except BindingUnavailable as e:
        pytest.skip("venue binding %r unavailable here: %s" % (venue_name, e))
        return  # pragma: no cover
    except TypeError as e:
        pytest.skip("binding %r lacks a dry_run mode: %s" % (venue_name, e))
        return  # pragma: no cover
    yield ctx
    ctx.close()


@pytest.fixture
def fresh_dry_ctx(dry_bound_ctx):
    dry_bound_ctx.reset()
    return dry_bound_ctx
