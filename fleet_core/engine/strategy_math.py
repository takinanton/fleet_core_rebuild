"""strategy_math — byte-copied, hash-pinned strategy modules (P3 exit-engine design §8).

LAW (p3_design_exit_engine.md §8, F1 root fix): STRATEGY MATH IS BYTE-PRESERVED.
The engine never re-implements signal/trail/exit math — it carries the EXACT live
modules, md5-pinned per venue (§8.1), and refuses to run on any pin mismatch.

Per-venue module table (design §8.1, pins measured on the fresh 2026-07-02
snapshot `fleet_core_rebuild/bots/`; `cutover_check --verify-strategy-pins`
re-measures them on the LIVE host at cutover — pins are evidence-refreshed,
never trusted from this file alone):

| venue           | module                                   | md5 pin                          |
|-----------------|------------------------------------------|----------------------------------|
| extended        | fleet_core/strategy_xnn.py (canonical;   | 4a96b9793923b7ba0cf70440a25b40e4 |
| pacifica        |   live bots run an 11-line loader shim,  | (shim: 12705f970a5697f2324d670be3e771ab) |
| nado            |   pin taken on the CANONICAL, not shim)  |                                  |
| hl crypto leg   | bot/strategy_donchian.py (env-seam N)    | e903953f4a45c77340d6d49240a70fe2 |
| hl us29 leg     | bot/strategy_us29.py                     | f04e747eeed6163718c1ab336523460d  |
| hl NEVER PORT   | bot/strategy_xnn.py (stale XNN adapter,  | 72b2f1870c64310b4b0e34f3d21c053b |
|                 |   VSTOP 0.15) — NEGATIVE pin             |   (negative)                     |

DEPENDENCY PIN (not in design §8.1 — interface note): bot/strategy_us29.py does
`from bot import us29_core` (the actual chandelier/signal math lives there,
strategy_us29 is the adapter). Byte-copying the pinned adapter therefore requires
byte-copying its math module too; us29_core.py is embedded and pinned at
7b3af50157d43d711eb3db821d506284 (md5 measured on the SAME 2026-07-02 snapshot). The
engine wires `bot.us29_core` in sys.modules to THIS pinned copy before exec'ing
strategy_us29, so the adapter's import resolves to the engine's byte-copy even on
a host where the legacy `bot` package is importable (byte-copy law: the engine
never runs foreign math). cutover_check should extend --verify-strategy-pins to
re-hash us29_core.py on the live host as well.

MECHANISM. ext/pac/nado math = the P1 canonical `fleet_core/strategy_xnn.py`,
already shipped inside this package — get_module() imports it directly after
hashing the package file against the pin (drift in the canonical fails loud).
The HL modules cannot live as package files here (file-ownership: this builder
creates exactly exit_engine.py + strategy_math.py), so their EXACT bytes are
embedded below (zlib+base64) and md5-asserted against the pins at import time
(decode+hash only — no exec, no pandas needed to import this module). Module
exec is LAZY: get_module() compiles the pinned bytes on first use (numpy/pandas
required then, matching the source modules' own imports).

STALE-DOCSTRING footnote (design §8.2 ¹): the copied sources carry stale
docstring numbers (N=20, pivot 3/0.003). Byte-copy preserves them verbatim —
the REAL constants are code: DONCHIAN_N=15, TRAIL_PIVOT_WINDOW=2,
TRAIL_VSTOP_BUFFER=0.005. Never cite the docstrings.

HL DONCHIAN-K ENV SEAM (design §8.1 HL row — the engine MUST port BOTH the seam
AND the startup assert): HL's live scan_for_signal takes
`n = int(donchian_k) if donchian_k>0 else DONCHIAN_N` (strategy_donchian.py:287),
fed per-TF by the scanner from env `DONCHIAN_K` (config.py:316, DEFAULT 20 ≠
validated 15) / `TF_<TF>_K` (config.py:164). Guarded live by the startup
CONFIG-ASSERT (hl/bot/main.py:660–686): effective N must equal DONCHIAN_N(15)
else refuse-to-start. Ported here as read_hl_effective_donchian_k() +
assert_hl_effective_config(). `TF_<TF>_SHORT_K` is INERT (R3/R2-F4c: the
short-side kwarg is IGNORED at strategy_donchian.py:359) — seeded for env parity
only, exposed by read_hl_short_donchian_k() but NOT asserted (not part of the
effective seam).

Importable on any machine: stdlib only at import time. py_compile clean on 3.9+.
"""
from __future__ import annotations

import base64
import hashlib
import sys
import types
import zlib
from typing import Any, Callable, Mapping, Optional

__all__ = [
    "PINS",
    "NEGATIVE_PINS",
    "DEPENDENCY_PINS",
    "SHIM_PIN",
    "EXPORTED_CONTRACT",
    "StrategyPinMismatch",
    "ConfigAssertError",
    "get_module",
    "verify_pins",
    "_verify_pins",
    "read_hl_effective_donchian_k",
    "read_hl_short_donchian_k",
    "hl_effective_n",
    "assert_hl_effective_config",
]


class StrategyPinMismatch(RuntimeError):
    """A strategy-module hash does not match its design §8.1 pin — the build/run
    MUST fail (conformance-harness law; strategy math is byte-preserved or absent)."""


class ConfigAssertError(RuntimeError):
    """HL effective-N / DONCHIAN_TFS env seam diverges from the validated config —
    refuse-to-start (port of hl/bot/main.py:660–686 CONFIG-ASSERT, sys.exit(1) live)."""


# ── Pins (design p3_design_exit_engine.md §8.1; md5 of SOURCE files, 2026-07-02 snapshot) ──
PINS = {
    "canonical_strategy_xnn": "4a96b9793923b7ba0cf70440a25b40e4",   # ext/pac/nado math (behind 11-line shim)
    "hl_strategy_donchian":   "e903953f4a45c77340d6d49240a70fe2",   # HL crypto leg (env-seam N)
    "hl_strategy_us29":       "f04e747eeed6163718c1ab336523460d",   # HL us29 leg (adapter)
}
# Live loader shim on ext/pac/nado (`bot/strategy_xnn.py`, 11 lines exec-ing the canonical).
# The pin MUST be taken on the canonical, not the shim (§8.1); shim pin kept for
# cutover_check --verify-strategy-pins live-host verification only.
SHIM_PIN = "12705f970a5697f2324d670be3e771ab"
# NEGATIVE pin (§8.1 last row): hl/bot/strategy_xnn.py is a STALE XNN adapter
# (math delegated to xnn_core.py, VSTOP_BUFFER_PCT=0.15 — NOT the live trail math).
# The harness fails if any engine import resolves to this module for HL.
NEGATIVE_PINS = {
    "hl_strategy_xnn_stale": "72b2f1870c64310b4b0e34f3d21c053b",
}
# Dependency pin (interface note above — NOT a design §8.1 row): the math module
# strategy_us29 imports. Measured on the same snapshot; drift fails the build too.
DEPENDENCY_PINS = {
    "hl_us29_core": "7b3af50157d43d711eb3db821d506284",
}

# The 7 exported names every strategy module must provide (framework contract,
# identical across strategy_xnn / strategy_donchian / strategy_us29).
EXPORTED_CONTRACT = (
    "Signal", "Position", "compute_indicators", "scan_for_signal",
    "scan_for_short_signal", "PositionManager", "_estimate_tick",
)

# ── Embedded byte-copies (zlib+base64 of the EXACT pinned source bytes) ──────────────
_BLOBS = {
    "hl_strategy_donchian": (
        "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"
        "3foaXv45a6CKrz+whkZW+v8tlxND"
    ),
    "hl_strategy_us29": (
        "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"
        "+3cIr4Ocyntg/hvrxOwMK7sA6Iw878LuFi3+8x8I+jm5h3LarkLZ7Hyx3QmbxQVNvfydgda3i2TjRfmSnKR2rOfuZdcGrAEe"
        "t0l3bsI6D4QMy68Dc66xosGM9npuc3cfrhbwlqgATQkPIFyF4Cw34lggmKKLXG7iC4JVjETFKn12HuUu1ttexnIzpgDb9fM0"
        "p8E7+81Bd9XHXsfKmdwmZuO2GI5u1BpTWi+AzaRZ7x1gO5NqCxWJereSCdiN5GVk4rvVVqZarki3ueZHOPSYxz5vlZp3kzCr"
        "DD3hOTsjyk73rvcVp/ym5dvfCBqzsRirT/xkiYn5N3ln0++PT9+eE6RIhoYfV5z6pFK+5ZkeqHnT+SNmnP5wUgFDqse9mJ4d"
        "zKezn7gQXiPD21UUayW/wpMb9UKLexlb5PH+Nt3GSBQwJrslCB3r4EYMmvFmZZLwwQEjaK6aZHqI/Srn2X5eYl1qkuWffQbW"
        "Ak9We6NQUigJNKYSw1uhNnLeSgXea8Betplzv/Nzwemcb6SJRb4bpNfA3EGg9DqEWArQSulCOZ475Bk8L/jMUi1L/aXb4f6F"
        "aMXbkDvky/QluMHXlsxyW/whcHFaEF+Wv2g4Bx2K5KTaVWnDbIfoDuGsaOHQ691TdiBfYufMlaW1r9vFzg0ar6oSwWYT31XF"
        "46t7SqDNG00pGDQKBC91wKnF7q922UEyOL/S9LOCKpHyQ2Sxi6IuXNOvpteBdGmQnFtpx66PaHqCK90B1R8pZ+TtU0Nj88rA"
        "bQNmzP6wo+l0Nle6c4zlbjpGGH1Yb6g29/Gb+fxjNvPNv20zjYvo0szs/PiTFb3Kix1QZvqmCaEudFUWKxUv0xWwW96qf7RG"
        "9Q66RHvzeowseDMsPbwUOwL6GbBGCkRr0jck9/+35LhnN0XKD2c4GRyOapCUVmkLxlz0iYO+lSuumq8Ftdev4+Zr7ti0k1xj"
        "Z7Ul9P6VQ4chKWwtj8f3WYaTIsOCKH0jShhBf8dg25qUeEKF6A9RJm/WN1UdiyafJv5NLDdUONuE07OdBL/i//kfw/+K8++P"
        "57/zBP0vf2cJOg=="
    ),
    "hl_us29_core": (
        "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"
    ),
}


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _blob_bytes(key: str) -> bytes:
    return zlib.decompress(base64.b64decode("".join(_BLOBS[key])))


def _all_pins() -> Mapping[str, str]:
    out = dict(PINS)
    out.update(DEPENDENCY_PINS)
    return out


def _verify_embedded() -> None:
    """Import-time gate: every embedded blob must hash to its pin. Decode+hash only
    (no exec) so `import strategy_math` works with stdlib alone."""
    pins = _all_pins()
    for key in _BLOBS:
        want = pins[key]
        got = _md5_hex(_blob_bytes(key))
        if got != want:
            raise StrategyPinMismatch(
                "embedded byte-copy %r md5 %s != design pin %s — "
                "strategy math is NOT byte-preserved; refusing" % (key, got, want))
        if got in NEGATIVE_PINS.values():
            raise StrategyPinMismatch(
                "embedded byte-copy %r hashes to a NEGATIVE pin (stale HL XNN "
                "adapter, VSTOP 0.15) — never port" % key)


_verify_embedded()


# ── Canonical (ext/pac/nado) module: the P1 package file, pin-checked then imported ──

def _canonical_source_path() -> str:
    import importlib.util
    spec = importlib.util.find_spec("fleet_core.strategy_xnn")
    if spec is None or not spec.origin:
        raise StrategyPinMismatch(
            "fleet_core.strategy_xnn not found in the package — canonical "
            "strategy module missing")
    return spec.origin


def _verify_canonical_file() -> str:
    path = _canonical_source_path()
    with open(path, "rb") as fh:
        got = _md5_hex(fh.read())
    want = PINS["canonical_strategy_xnn"]
    if got != want:
        raise StrategyPinMismatch(
            "fleet_core/strategy_xnn.py md5 %s != design pin %s (path %s) — "
            "the P1 canonical drifted; refusing to serve ext/pac/nado math"
            % (got, want, path))
    return path


# ── Lazy exec of embedded pinned modules ─────────────────────────────────────────────

_MODULE_CACHE: dict = {}
_VIRTUAL_PKG = "fleet_core.engine._pinned"


def _exec_pinned(key: str, virtual_name: str) -> types.ModuleType:
    """Exec the pinned bytes into a fresh module object. The SOURCE BYTES are the
    pinned bytes verbatim (byte-copy law); only the module/file names are virtual."""
    if key in _MODULE_CACHE:
        return _MODULE_CACHE[key]
    src = _blob_bytes(key)  # md5 already asserted at import
    mod = types.ModuleType(virtual_name)
    mod.__file__ = "<pinned:%s md5=%s>" % (key, _md5_hex(src))
    mod.__package__ = _VIRTUAL_PKG
    sys.modules[virtual_name] = mod
    code = compile(src, mod.__file__, "exec")
    exec(code, mod.__dict__)
    _MODULE_CACHE[key] = mod
    return mod


def _wire_bot_us29_core() -> types.ModuleType:
    """Make `from bot import us29_core` (inside pinned strategy_us29.py) resolve to
    the ENGINE'S pinned byte-copy, never a host-local legacy module.

    Mechanism: exec the pinned us29_core, then register it as sys.modules
    ['bot.us29_core'] and as the `us29_core` attribute of whatever `bot` module
    exists (a synthetic empty package is created when none is importable).
    Process-local; documented interface note in the module docstring."""
    core = _exec_pinned("hl_us29_core", _VIRTUAL_PKG + ".us29_core")
    bot_mod = sys.modules.get("bot")
    if bot_mod is None:
        bot_mod = types.ModuleType("bot")
        bot_mod.__path__ = []  # mark as package so `from bot import x` works
        bot_mod.__dict__["__engine_synthetic__"] = True
        sys.modules["bot"] = bot_mod
    sys.modules["bot.us29_core"] = core
    setattr(bot_mod, "us29_core", core)
    return core


def _check_contract(mod: types.ModuleType, key: str) -> types.ModuleType:
    missing = [n for n in EXPORTED_CONTRACT if not hasattr(mod, n)]
    if missing:
        raise StrategyPinMismatch(
            "pinned module %r lacks contract exports %s — wrong module"
            % (key, missing))
    return mod


def get_module(venue: str, leg: str = "crypto") -> types.ModuleType:
    """Return the byte-copied strategy module for (venue, leg), pin-verified.

    extended / pacifica / nado -> P1 canonical fleet_core.strategy_xnn
        (N hard-forced to DONCHIAN_N=15 at :287 — the kwarg is IGNORED by design).
    hl, leg='crypto'           -> pinned bot/strategy_donchian.py byte-copy
        (LIVE env seam: n = donchian_k if >0 else DONCHIAN_N — caller feeds the
        per-TF k via read_hl_effective_donchian_k; assert_hl_effective_config()
        MUST have passed at startup).
    hl, leg='us29'             -> pinned bot/strategy_us29.py byte-copy
        (bot.us29_core wired to the pinned dependency copy first).

    Raises StrategyPinMismatch on any hash/contract failure; ValueError on an
    unknown venue/leg (caller bug, not venue flakiness)."""
    v = venue.strip().lower()
    if v in ("extended", "pacifica", "nado"):
        cache_key = "canonical_strategy_xnn"
        if cache_key not in _MODULE_CACHE:
            _verify_canonical_file()
            import fleet_core.strategy_xnn as _canon
            _MODULE_CACHE[cache_key] = _check_contract(_canon, cache_key)
        return _MODULE_CACHE[cache_key]
    if v in ("hl", "hyperliquid"):
        lg = (leg or "crypto").strip().lower()
        if lg == "crypto":
            mod = _exec_pinned("hl_strategy_donchian",
                               _VIRTUAL_PKG + ".hl_strategy_donchian")
            return _check_contract(mod, "hl_strategy_donchian")
        if lg == "us29":
            _wire_bot_us29_core()
            mod = _exec_pinned("hl_strategy_us29",
                               _VIRTUAL_PKG + ".hl_strategy_us29")
            return _check_contract(mod, "hl_strategy_us29")
        raise ValueError("unknown HL leg %r (crypto|us29)" % leg)
    raise ValueError("unknown venue %r" % venue)


# ── Pin verification entrypoint (shadow runner + cutover_check call this) ────────────

def verify_pins(live_paths: Optional[Mapping[str, str]] = None) -> Mapping[str, Any]:
    """Re-verify EVERY pin; raise StrategyPinMismatch on the first failure.

    Always checks: embedded blobs (incl. dependency pin) + the in-package canonical
    fleet_core/strategy_xnn.py file.

    live_paths (optional; shadow/cutover tooling): mapping of pin key -> filesystem
    path of the venue's LIVE deployed file, re-hashed against the same pins —
    p3_rollout.md §1-F7 `cutover_check --verify-strategy-pins`. Recognised keys:
    the PINS/DEPENDENCY_PINS keys, plus 'shim' (checked vs SHIM_PIN) and
    'hl_strategy_xnn_stale' (checked vs the NEGATIVE pin — here a MATCH is the
    expected state of the live file; what must never happen is get_module()
    serving those bytes, which the positive pins make impossible).

    Returns a report dict {key: {'md5':…, 'pin':…, 'ok': True}} for the gate log."""
    report: dict = {}
    pins = _all_pins()
    for key in _BLOBS:
        got = _md5_hex(_blob_bytes(key))
        report[key] = {"md5": got, "pin": pins[key], "ok": got == pins[key],
                       "source": "embedded"}
        if got != pins[key]:
            raise StrategyPinMismatch("pin FAIL %s: %s != %s" % (key, got, pins[key]))
    path = _verify_canonical_file()
    report["canonical_strategy_xnn"] = {
        "md5": PINS["canonical_strategy_xnn"], "pin": PINS["canonical_strategy_xnn"],
        "ok": True, "source": path}
    if live_paths:
        for key, p in live_paths.items():
            if key == "shim":
                want = SHIM_PIN
            elif key in NEGATIVE_PINS:
                want = NEGATIVE_PINS[key]
            elif key in pins:
                want = pins[key]
            else:
                raise ValueError("verify_pins: unknown live pin key %r" % key)
            with open(p, "rb") as fh:
                got = _md5_hex(fh.read())
            ok = got == want
            report["live:" + key] = {"md5": got, "pin": want, "ok": ok, "source": p}
            if not ok:
                raise StrategyPinMismatch(
                    "LIVE pin FAIL %s (%s): %s != %s" % (key, p, got, want))
    return report


# Alias per the build spec wording ("a _verify_pins() the shadow/cutover tools call").
_verify_pins = verify_pins


# ── HL DONCHIAN-K env seam + startup CONFIG-ASSERT (port of main.py:660–686) ─────────

# config.py:316 default — 20, deliberately ≠ validated 15: THIS mismatch is why the
# startup assert exists (ENV-FOOTGUN class, project_combo 2026-06-20).
DONCHIAN_K_CONFIG_DEFAULT = 20


def _get_int_env(getenv: Callable[[str], Optional[str]], key: str,
                 default: int) -> int:
    raw = getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def read_hl_effective_donchian_k(tf: str,
                                 getenv: Callable[[str], Optional[str]]) -> int:
    """Per-TF LONG Donchian window from env — faithful port of hl config.py:
    global_k = int(env DONCHIAN_K, default 20); k = int(env TF_<TF>_K, default
    global_k), tf_key = tf.upper() (config.py:316 + :161–166)."""
    global_k = _get_int_env(getenv, "DONCHIAN_K", DONCHIAN_K_CONFIG_DEFAULT)
    return _get_int_env(getenv, "TF_%s_K" % tf.upper(), global_k)


def read_hl_short_donchian_k(tf: str,
                             getenv: Callable[[str], Optional[str]]) -> int:
    """INERT seam (R3 / R2-F4c): TF_<TF>_SHORT_K / DONCHIAN_K for the SHORT side.
    The short-side kwarg is IGNORED at strategy_donchian.py:359 (`donchian_k: int,
    # IGNORED` in scan_for_short_signal — long-only module), so this env var has NO
    effect on live signal math. Exposed for .env seeding parity only; the startup
    assert deliberately does NOT cover it (round-2 overstated the seam's scope)."""
    global_k = _get_int_env(getenv, "DONCHIAN_K", DONCHIAN_K_CONFIG_DEFAULT)
    return _get_int_env(getenv, "TF_%s_SHORT_K" % tf.upper(), global_k)


def hl_effective_n(tf: str = "8h",
                   getenv: Optional[Callable[[str], Optional[str]]] = None,
                   validated_n: Optional[int] = None) -> int:
    """Effective Donchian N the HL crypto leg would trade on `tf`:
    int(k) if k>0 else DONCHIAN_N — mirrors strategy_donchian.py:287 exactly.
    validated_n lets callers avoid the module exec (pandas) when they already
    know DONCHIAN_N; default resolves it from the pinned module."""
    if getenv is None:
        import os
        getenv = os.environ.get
    k = read_hl_effective_donchian_k(tf, getenv)
    if k and int(k) > 0:
        return int(k)
    if validated_n is not None:
        return int(validated_n)
    return int(get_module("hl", "crypto").DONCHIAN_N)


def assert_hl_effective_config(getenv: Optional[Callable[[str], Optional[str]]] = None,
                               tf: str = "8h") -> None:
    """Port of the HL startup CONFIG-ASSERT (main.py:660–686) into the engine's
    config layer — MUST be called at engine startup on HL (and by cutover_check's
    effective-N re-run against the live .env, rollout §1-F7). Raises
    ConfigAssertError (caller refuses to start — live equivalent: sys.exit(1)) when:

      * effective N (env DONCHIAN_K / TF_<TF>_K seam; config default 20!) !=
        the module's validated DONCHIAN_N (15), or
      * DONCHIAN_TFS != frozenset({'8h'}) (env DONCHIAN_TFS override; 4h bt = DD74%).

    Code-pinned knobs (TP_R_MULTIPLE=999, TRAIL_PIVOT_WINDOW=2, SL_BUFFER_PCT)
    are not env-overridable -> not asserted (same scoping as live).
    NOTE: DONCHIAN_TFS is read from env at module exec — call this AFTER the
    process env is final, same as the live startup ordering."""
    if getenv is None:
        import os
        getenv = os.environ.get
    mod = get_module("hl", "crypto")
    vd_n = int(mod.DONCHIAN_N)
    eff_n = hl_effective_n(tf, getenv, validated_n=vd_n)
    if eff_n != vd_n:
        raise ConfigAssertError(
            "CONFIG-ASSERT: donchian leg N=%d != validated %d (env DONCHIAN_K "
            "override; config default is %d). Set .env.combo DONCHIAN_K=%d. "
            "REFUSING to trade unvalidated config."
            % (eff_n, vd_n, DONCHIAN_K_CONFIG_DEFAULT, vd_n))
    if mod.DONCHIAN_TFS != frozenset({"8h"}):
        raise ConfigAssertError(
            "CONFIG-ASSERT: donchian leg TFS=%s != validated {'8h'} (env "
            "DONCHIAN_TFS override; 4h bt = DD74%%). Set .env.combo "
            "DONCHIAN_TFS=8h. REFUSING to trade unvalidated config."
            % sorted(mod.DONCHIAN_TFS))


if __name__ == "__main__":  # pragma: no cover — manual pin check
    import json
    print(json.dumps({k: v for k, v in verify_pins().items()}, indent=2,
                     default=str))
    print("strategy_math: all pins OK")
