"""pytest collection shim.

pytest only collects `test_*.py`; the suite itself lives in suite.py (named
per the P2 spec). Importing its test functions here puts them in a collected
module's namespace — parametrize marks and conftest fixtures apply unchanged.
"""

from fleet_core.conformance.suite import (  # noqa: F401
    test_conformance,
    test_construction,
    test_crash_at_transition_journal_consistent,
    test_crash_at_transition_recovers,
    test_dry_write_isolation,
    test_raw_adapter_constructs_offline,
)
