"""fleet_core.engine — P3 execution engine package.

Modules (P3 round-3 approved designs, proofs/p3_design_*.md):

    entry_sm    — persisted entry state machine + bidirectional reconciler
                  (p3_design_entry_sm.md; owned by the entry_sm builder)
    registry    — durable placed_trigger_oids bot-own trigger registry (F5,
                  entry-SM §2.2; fleet-wide "bot-own = oid ∈ registry" law)
    migrate_v3  — additive, idempotent trades.db migration to the SM schema
                  (entry-SM §2; rollback semantics documented in-module)
    exit_engine — shared exit/SL engine (p3_design_exit_engine.md; owned by
                  the exit_engine builder — may not exist yet in this tree)
    shadow      — shadow runner (p3_design_shadow_runner.md; separate owner)

IMPORT DISCIPLINE: this package must import WITHOUT venue SDKs and WITHOUT
the per-venue `bot` package. Submodules are therefore NOT imported eagerly —
use `from fleet_core.engine import entry_sm` (PEP 562 lazy loading below) or
import the submodule directly. Anything venue-flavored is a lazy in-function
import inside the submodules themselves.
"""
from __future__ import annotations

import importlib

__all__ = ["entry_sm", "registry", "migrate_v3", "exit_engine", "shadow"]


def __getattr__(name):  # PEP 562 — lazy submodule access, no eager SDK pulls
    if name in __all__:
        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
