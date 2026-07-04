#!/usr/bin/env python3
"""P1b: port per-asset effective-leverage cap (fix 2026-06-11, canon pacifica/bot/trader.py:196-217)
to nado/bot/trader.py. DELIBERATE behavior fix (restores fleet knob-parity; audit divergence #1).

Scope: sizing + MM-cap parity ONLY — does NOT add pacifica's update_leverage-at-entry call
(nado is cross-margin per-subaccount default; sizing cap is the audited defect).
Run ON nado-bot host: python3 p1b_nado_leverage_eff.py /root/nado_xnn_bot/bot/trader.py
Idempotent: refuses to run twice (marker check). Creates .bak_p1b_<ts>.
"""
import re
import shutil
import sys
import time

path = sys.argv[1] if len(sys.argv) > 1 else "/root/nado_xnn_bot/bot/trader.py"
src = open(path, encoding="utf-8").read()

if "eff_lev = min(settings.leverage" in src:
    print("ALREADY_APPLIED — marker found, nothing to do")
    sys.exit(0)

EFF_BLOCK = '''
    # --- EFFECTIVE leverage (P1b knob-parity port 2026-07-02, canon pacifica §0#8 fix 2026-06-11) ---
    # nado sized off raw .env LEVERAGE even when the asset's max leverage is lower
    # (audit 2026-07-02 divergence #1). eff_lev = min(settings.leverage, asset.max_leverage)
    # is used in the sizing cap + MM-cap margin math. NOTE: no update_leverage-at-entry here —
    # nado is cross-margin per-subaccount default; this is the sizing/MM parity only.
    _meta_max_lev = int(getattr(meta, "max_leverage", 0) or 0)
    eff_lev = min(settings.leverage, _meta_max_lev) if _meta_max_lev > 0 else settings.leverage
    if eff_lev < settings.leverage:
        log.info("%s: asset max_leverage=%dx < LEVERAGE=%dx — using %dx",
                 signal.coin, _meta_max_lev, settings.leverage, eff_lev)
'''

# 1. insert eff_lev block after the asset-meta gate (before the first compute_size)
anchor = """        _reject_and_log(signal, f"asset_not_found: {signal.coin}")
        return None
"""
assert src.count(anchor) == 1, f"asset_not_found anchor count != 1 ({src.count(anchor)})"
src = src.replace(anchor, anchor + EFF_BLOCK, 1)

# 2. both compute_size calls gain leverage_eff=eff_lev (append after sz_decimals arg,
#    preserving each call site's indentation)
pat = re.compile(r"^(\s*)sz_decimals=meta\.sz_decimals,$", re.M)
sites = pat.findall(src)
assert len(sites) == 2, f"compute_size sz_decimals sites != 2 ({len(sites)})"
src = pat.sub(lambda m: f"{m.group(0)}\n{m.group(1)}leverage_eff=eff_lev,", src)

# 3. MM-cap uses eff_lev
mm_old = "eff_lev=settings.leverage,"
assert src.count(mm_old) == 1, f"check_mm_cap eff_lev site != 1 ({src.count(mm_old)})"
src = src.replace(mm_old, "eff_lev=eff_lev,", 1)

bak = f"{path}.bak_p1b_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
shutil.copy2(path, bak)
open(path, "w", encoding="utf-8").write(src)
print(f"APPLIED — backup {bak}")
print("Next: venv/bin/python -m py_compile", path, "&& systemctl restart nado-bot && journalctl verify")
