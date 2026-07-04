"""Regression gate for the empty-book-sentinel mark bug (incident 2026-06-05).

VIRTUAL-PERP (pid=84) had an empty engine book -> get_market_price returned
bid_x18='0', ask_x18=i128::MAX (2**127-1, the Vertex no-quote sentinel) ->
mark_price() naively averaged them to ~8.5e19 instead of ~$0.57. That bad mark
could reach trigger_sl / limit_reduce_only via the vstop trail / residual-SL
paths and send a garbage SL/TP order to the exchange.

Two defenses are locked in here (pure-unit, no network/SDK):
  1. _valid_x18_side  — rejects 0 / i128::MAX sentinel / negative / non-numeric.
  2. _assert_sane_px  — fail-closes any order price outside (0, MARK_SANITY_MAX).
mark_price()'s indexer-oracle fallback is covered by the live verify script.
"""
from bot.exchange_nado import (
    _valid_x18_side, _assert_sane_px, I128_MAX, MARK_SANITY_MAX,
)

try:                                    # pytest optional — repo runs tests standalone
    import pytest
    raises = pytest.raises
except ModuleNotFoundError:
    import contextlib

    @contextlib.contextmanager
    def raises(exc):
        try:
            yield
        except exc:
            return
        raise AssertionError(f"DID NOT RAISE {exc.__name__}")


def test_sentinel_and_empty_sides_are_invalid():
    assert _valid_x18_side(str(I128_MAX)) is None       # i128::MAX no-quote sentinel
    assert _valid_x18_side(str(I128_MAX + 5)) is None    # above sentinel
    assert _valid_x18_side("0") is None                  # empty side
    assert _valid_x18_side("-5") is None                 # negative
    assert _valid_x18_side(None) is None
    assert _valid_x18_side("abc") is None


def test_real_x18_side_decodes():
    assert abs(_valid_x18_side("572771779999999919") - 0.5727717) < 1e-4   # VIRTUAL
    assert abs(_valid_x18_side("4350400000000000000000") - 4350.4) < 1e-6  # XAUT


def test_guard_rejects_insane_prices():
    for bad in (8.5e19, I128_MAX / 1e18, MARK_SANITY_MAX, 1e7, 0.0, -1.0, float("nan")):
        with raises(ValueError):
            _assert_sane_px("X-PERP", bad, "test")


def test_guard_allows_real_prices():
    for ok in (0.5695, 4350.4, 583.0, 60427.5, 9_999_999.0):
        _assert_sane_px("X-PERP", ok, "test")  # must not raise


if __name__ == "__main__":
    test_sentinel_and_empty_sides_are_invalid()
    test_real_x18_side_decodes()
    test_guard_rejects_insane_prices()
    test_guard_allows_real_prices()
    print("ALL PASS")
