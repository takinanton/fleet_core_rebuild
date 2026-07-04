"""fleet_core.conformance.runner — pytest-free fallback runner.

Bot-host venvs may not ship pytest; this drives the exact same check list
(checks.py) with plain python and mirrors the suite's semantics:

    cd <bot root> && venv/bin/python fleet_core/conformance/runner.py --venue=hl
    (local reference run:  /usr/bin/python3 fleet_core/conformance/runner.py)

Exit code: 0 = all executed checks passed (skips allowed), 1 = any failure,
2 = venue binding unavailable (reason printed).
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback


def _ensure_root_on_path() -> None:
    """Make `fleet_core.*` importable when invoked by file path: walk up from
    this file to the directory CONTAINING fleet_core/ and prepend it."""
    here = os.path.abspath(os.path.dirname(__file__))       # .../fleet_core/conformance
    root = os.path.dirname(os.path.dirname(here))            # dir containing fleet_core/
    if root not in sys.path:
        sys.path.insert(0, root)


def main(argv=None) -> int:
    _ensure_root_on_path()
    from fleet_core.conformance.checks import (
        CONFORMANCE_CHECKS,
        CONSTRUCTION_CHECKS,
        DRY_WRITE_CHECKS,
        P3_TRANSITIONS,
        SkipCheck,
    )
    from fleet_core.conformance.bindings import (
        BindingUnavailable,
        get_binding,
    )

    ap = argparse.ArgumentParser(description="conformance harness (no pytest)")
    ap.add_argument("--venue", default="fake",
                    help="fake|hl|pacifica|extended|nado (default: fake)")
    ap.add_argument("-k", default="", help="substring filter on check names")
    ap.add_argument("-v", action="store_true", help="print every check name")
    args = ap.parse_args(argv)

    passed = failed = skipped = 0
    failures = []

    def report(kind: str, name: str, msg: str = "") -> None:
        nonlocal passed, failed, skipped
        if kind == "PASS":
            passed += 1
            if args.v:
                print("PASS  %s" % name)
        elif kind == "SKIP":
            skipped += 1
            print("SKIP  %s%s" % (name, (" — " + msg) if msg else ""))
        else:
            failed += 1
            failures.append((name, msg))
            print("FAIL  %s\n      %s" % (name, msg))

    # 1. construction invariants (always)
    for name, fn in CONSTRUCTION_CHECKS:
        if args.k and args.k not in name:
            continue
        try:
            fn()
            report("PASS", "construction::" + name)
        except Exception as e:
            report("FAIL", "construction::" + name,
                   "%s: %s" % (type(e).__name__, e))

    # 2. binding
    ctx = None
    try:
        binding = get_binding(args.venue)
        smoke = getattr(binding, "smoke_construct", None)
        if smoke is not None:
            try:
                print("SMOKE %s" % smoke())
            except BindingUnavailable as e:
                report("SKIP", "smoke::raw_adapter_constructs_offline", str(e))
        ctx = binding.build_context()
    except BindingUnavailable as e:
        print("\nvenue binding %r UNAVAILABLE here: %s" % (args.venue, e))
        print("construction checks: %d passed, %d failed" % (passed, failed))
        return 2 if failed == 0 else 1

    # 3. conformance checks
    try:
        for name, fn in CONFORMANCE_CHECKS:
            if args.k and args.k not in name:
                continue
            try:
                ctx.reset()
                fn(ctx)
                report("PASS", name)
            except SkipCheck as e:
                report("SKIP", name, str(e))
            except AssertionError as e:
                report("FAIL", name, str(e) or traceback.format_exc(limit=3))
            except Exception as e:
                report("FAIL", name, "%s: %s" % (type(e).__name__, e))
    finally:
        ctx.close()

    # 3b. DRY-mode write-isolation lane (F2 regression guard): a FRESH
    # context with DRY_RUN=1 — no write op may ever reach the transport.
    # Built AFTER the main context closed so the two gates never coexist.
    dry_ctx = None
    try:
        dry_ctx = binding.build_context(dry_run=True)
    except BindingUnavailable as e:
        for name, _ in DRY_WRITE_CHECKS:
            if args.k and args.k not in name:
                continue
            report("SKIP", name, str(e))
    except TypeError as e:
        for name, _ in DRY_WRITE_CHECKS:
            if args.k and args.k not in name:
                continue
            report("SKIP", name, "binding lacks a dry_run mode: %s" % e)
    if dry_ctx is not None:
        try:
            for name, fn in DRY_WRITE_CHECKS:
                if args.k and args.k not in name:
                    continue
                try:
                    dry_ctx.reset()
                    fn(dry_ctx)
                    report("PASS", name)
                except SkipCheck as e:
                    report("SKIP", name, str(e))
                except AssertionError as e:
                    report("FAIL", name,
                           str(e) or traceback.format_exc(limit=3))
                except Exception as e:
                    report("FAIL", name, "%s: %s" % (type(e).__name__, e))
        finally:
            dry_ctx.close()

    # 4. P3 placeholders — mirrored as skips
    for t in P3_TRANSITIONS:
        report("SKIP", "p3::crash_at_%s" % t,
               "P3 entry state machine lands later")

    print("\n%s: %d passed, %d failed, %d skipped (venue=%s)"
          % ("GREEN" if failed == 0 else "RED", passed, failed, skipped,
             args.venue))
    if failures:
        print("failed checks: %s" % ", ".join(n for n, _ in failures))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
