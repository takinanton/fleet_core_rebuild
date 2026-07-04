"""Cross-venue guard: every main.py call into bot.trader must BIND to the trader.py def.

Class guard for the 2026-07-02 restore-resolve incident: main.py (all venues) called
_lookup_real_close_px(client, _ghost, sl_oid=...) while pacifica/extended/nado trader.py
still had the 2-arg def. The TypeError was swallowed by the surrounding blanket except,
so a DB-open row absent from the exchange at startup was kept open FOREVER (never closed,
never adopted, invisible to the runtime K=3 phantom-guard). Interface drift between the
per-venue trader.py ports is silent by construction — this gate makes it loud.

Pure-AST (no bot imports, no venue SDKs needed) so it runs anywhere, incl. pre-deploy:
    python tests/test_trader_call_signature_parity.py   # exit 1 on violation
    pytest tests/test_trader_call_signature_parity.py
"""
import ast
import sys
from pathlib import Path

BOTS_ROOT = Path(__file__).resolve().parent.parent / "bots"


def _def_signatures(trader_src: str):
    """name -> dict describing the def's parameters (top-level defs only)."""
    sigs = {}
    for node in ast.parse(trader_src).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            pos = [p.arg for p in a.posonlyargs + a.args]
            sigs[node.name] = {
                "pos": pos,
                "required": len(pos) - len(a.defaults),
                "vararg": a.vararg is not None,
                "kwonly": {p.arg for p in a.kwonlyargs},
                "kwonly_required": {
                    p.arg for p, d in zip(a.kwonlyargs, a.kw_defaults) if d is None
                },
                "kwarg": a.kwarg is not None,
            }
    return sigs


def _trader_imports(main_tree: ast.AST):
    """Names bound in main.py via `from bot.trader import ...` (incl. nested imports)."""
    names = {}
    for node in ast.walk(main_tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.endswith("trader"):
            for alias in node.names:
                names[alias.asname or alias.name] = alias.name
    return names


def _bind_errors(call: ast.Call, sig: dict, fname: str):
    """Simulate Python argument binding; return list of violation strings."""
    errs = []
    starred = any(isinstance(a, ast.Starred) for a in call.args)
    n_pos = sum(1 for a in call.args if not isinstance(a, ast.Starred))
    kw_names = [k.arg for k in call.keywords if k.arg is not None]
    dstar = any(k.arg is None for k in call.keywords)

    if n_pos > len(sig["pos"]) and not sig["vararg"]:
        errs.append(f"{fname}: {n_pos} positional args, def takes {len(sig['pos'])}")
    filled_pos = set(sig["pos"][:n_pos])
    for kw in kw_names:
        if kw in filled_pos:
            errs.append(f"{fname}: keyword '{kw}' already bound positionally")
        elif kw not in sig["pos"] and kw not in sig["kwonly"] and not sig["kwarg"]:
            errs.append(f"{fname}: unexpected keyword '{kw}' (def has no such param, no **kwargs)")
    if not starred and not dstar:
        supplied = n_pos + sum(1 for kw in kw_names if kw in sig["pos"])
        if supplied < sig["required"]:
            errs.append(f"{fname}: only {supplied} of {sig['required']} required args supplied")
        missing_kw = sig["kwonly_required"] - set(kw_names)
        if missing_kw:
            errs.append(f"{fname}: missing required kw-only {sorted(missing_kw)}")
    return errs


def check_bot(bot_dir: Path):
    """Return violations for one bots/<venue>/bot dir."""
    main_py, trader_py = bot_dir / "main.py", bot_dir / "trader.py"
    if not (main_py.exists() and trader_py.exists()):
        return []
    sigs = _def_signatures(trader_py.read_text())
    tree = ast.parse(main_py.read_text())
    imported = _trader_imports(tree)
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            local = node.func.id
            if local in imported and imported[local] in sigs:
                for e in _bind_errors(node, sigs[imported[local]], imported[local]):
                    out.append(f"{bot_dir.parent.name}/bot/main.py:{node.lineno}: {e}")
    return out


def iter_bot_dirs():
    return sorted(p / "bot" for p in BOTS_ROOT.iterdir() if (p / "bot" / "main.py").exists())


def test_all_venues_bind():
    violations = [v for d in iter_bot_dirs() for v in check_bot(d)]
    assert not violations, "trader-call signature drift:\n" + "\n".join(violations)


def test_guard_catches_restore_resolve_class(tmp_path=None):
    """Negative self-test: the exact 2026-07-02 bug shape MUST be flagged."""
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        bot = Path(td) / "fake_venue" / "bot"
        bot.mkdir(parents=True)
        (bot / "trader.py").write_text("def _lookup_real_close_px(client, pos):\n    return None\n")
        (bot / "main.py").write_text(
            "from bot.trader import _lookup_real_close_px\n"
            "_real = _lookup_real_close_px(client, ghost, sl_oid='x')\n"
        )
        v = check_bot(bot)
        assert v and "sl_oid" in v[0], f"guard failed to catch the known-bug shape: {v}"


if __name__ == "__main__":
    test_guard_catches_restore_resolve_class()
    bad = [v for d in iter_bot_dirs() for v in check_bot(d)]
    if bad:
        print("SIGNATURE-PARITY VIOLATIONS:")
        print("\n".join(bad))
        sys.exit(1)
    print(f"OK: {len(iter_bot_dirs())} venues, all main.py->trader.py calls bind (self-test passed)")
