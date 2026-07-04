#!/usr/bin/env python3
"""P2 harness-caught LIVE bug fix (2026-07-03): bot/exchange_nado.py defines
_install_session_timeout TWICE — line ~54 takes a requests.Session directly (used by the
gc-sweep at ~:95 and the client/querier session sweep at ~:385/388); line ~203 (parallel
session's order-path port 07-02) takes an SDK OBJECT and getattr's .session off it, and
SHADOWS the first module-wide. Every Session-taking call therefore getattr'd .session off
a Session -> None -> 'SDK <x> has no .session' WARNING and NO timeout installed: the
indexer/querier/engine/trigger *session-level* guards and the whole gc-sweep were no-ops
(audit defect #0 class half-alive). The order-path sites (:438/:444) pass SDK objects and
worked.

Fix: rename the object-taking variant to _install_sdk_session_timeout + repoint its two
call sites. The Session-taking def is then unshadowed for :95/:385/:388.
Run ON nado-bot host: python3 p2_nado_session_sweep_fix.py /root/nado_xnn_bot/bot/exchange_nado.py
Idempotent; timestamped .bak.
"""
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/root/nado_xnn_bot/bot/exchange_nado.py"
src = open(path, encoding="utf-8").read()

if "_install_sdk_session_timeout" in src:
    print("ALREADY_APPLIED")
    sys.exit(0)

# 1. rename the second (object-taking) definition — anchor on its unique docstring head
a1 = '''def _install_session_timeout(sdk_obj, what, connect=5.0, read=20.0):
    """ORDER-PATH timeout guard (2026-07-02, ported from hl_combo_bot root-fix):'''
assert src.count(a1) == 1, f"order-path def anchor != 1 ({src.count(a1)})"
src = src.replace(a1, '''def _install_sdk_session_timeout(sdk_obj, what, connect=5.0, read=20.0):
    """ORDER-PATH timeout guard (2026-07-02, ported from hl_combo_bot root-fix; renamed
    2026-07-03 — it SHADOWED the Session-taking _install_session_timeout defined above,
    turning the gc-sweep and the client/querier session guards into 'has no .session'
    no-ops; P2 conformance harness caught the unbounded indexer/querier calls):''', 1)

# 2. repoint the two SDK-object call sites
a2 = '_install_session_timeout(ctx.engine_client, "engine_client(order-path)")'
assert src.count(a2) == 1, f"engine_client site != 1 ({src.count(a2)})"
src = src.replace(a2, '_install_sdk_session_timeout(ctx.engine_client, "engine_client(order-path)")', 1)

a3 = '_install_session_timeout(tc, "trigger_client(order-path)")'
assert src.count(a3) == 1, f"trigger_client site != 1 ({src.count(a3)})"
src = src.replace(a3, '_install_sdk_session_timeout(tc, "trigger_client(order-path)")', 1)

bak = f"{path}.bak_p2sess_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
