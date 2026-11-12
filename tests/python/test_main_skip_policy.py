#!/usr/bin/env python3
"""The skip policy of ``_main.py``: it skips (exit 77) ONLY when the pyo3 shim (``_sqlite_engine``) is not built.

``_main.py`` is copied next to a fake ``_sqlite_engine.py`` in a temporary directory whose import does what the case names,
and imported in a child process. "Not built" must exit 77; every other failure (a load error, a missing module inside the
extension, an unexpected exception) must fail with a traceback, never skip. Needs neither a build nor any Python dependency.
"""

import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = 60


def run_case(body):
    """(exit status, output) of ``import _main`` where the fake extension runs ``body``; ``None`` leaves it absent."""
    with tempfile.TemporaryDirectory(prefix="skip-policy.", dir=os.path.expanduser("~")) as tmp:
        shutil.copy(os.path.join(HERE, "_main.py"), tmp)
        if body is not None:
            with open(os.path.join(tmp, "_sqlite_engine.py"), "w") as f:
                f.write(body + "\n")
        code = f"import sys; sys.path.insert(0, {tmp!r}); import _main"
        r = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT)
        return r.returncode, r.stdout + r.stderr


def expect(label, got, want_skip):
    status, out = got
    if want_skip:
        assert status == 77 and "SKIP:" in out, f"{label}: expected SKIP/77, got {status}: {out[-400:]}"
    else:
        assert status not in (0, 77), f"{label}: a real failure must not pass or skip, got {status}: {out[-400:]}"
        assert "Traceback" in out, f"{label}: the failure must carry a traceback: {out[-400:]}"
    print(f"ok {label} -> {status}")


def main():
    expect("extension not built", run_case(None), True)
    expect("extension fails to load", run_case("raise ImportError('undefined symbol: _foo')"), False)
    expect("extension needs a missing module", run_case("import module_that_does_not_exist"), False)
    expect("unexpected error in the extension", run_case("raise RuntimeError('boom')"), False)
    status, out = run_case("pass")
    assert status == 0, f"a good load must import cleanly, got {status}: {out[-400:]}"
    print("ok good load -> 0")
    print("all passed")


if __name__ == "__main__":
    main()
