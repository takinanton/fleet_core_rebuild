"""fleet_core.venues — per-venue ExchangeClient implementations (P2).

One module per venue (fleet_core/venues/<venue>.py), each exposing:

    build_client(raw) -> ExchangeClient   # wrap an already-built raw adapter
                                          # (conformance-harness hook)
    get_client()      -> ExchangeClient   # build the raw adapter from the
                                          # bot env, then wrap (production)

This package is a trivial lazy dispatcher ONLY — importing it pulls in no
venue SDK and no bot code (venue modules import bot.exchange_* lazily).
Keep it minimal so parallel per-venue work never conflicts here.
"""

from __future__ import annotations

from importlib import import_module

__all__ = ["get_client"]


def get_client(venue: str, *args, **kwargs):
    """Lazy factory: fleet_core.venues.<venue>.get_client(*args, **kwargs)."""
    name = (venue or "").strip().lower()
    mod = import_module("fleet_core.venues.%s" % name)
    return mod.get_client(*args, **kwargs)
