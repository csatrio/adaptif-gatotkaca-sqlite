"""Helper for the Python tests: loads the dev-only pyo3 shim (``tests/pyshim``) that exposes this crate's engine.

Importing this module puts ``tests/python`` on ``sys.path`` (so ``import sqlite`` finds the ported Python layer in
``tests/python/sqlite``) and imports the built extension ``_sqlite_engine``. Build it with ``scripts/build-pyshim.sh``.
When the extension file is absent (and only then) it prints ``SKIP: <reason>`` and exits 77, the skip status of
``run_all.py``. Any other failure (a load error, an undefined symbol, a bug) propagates with its traceback.
Exports ``skip(reason)`` and ``require_module(name)`` (skip when a Python dependency such as tortoise is missing).
"""

import os
import sys

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
SKIP_EXIT = 77
EXTENSION = "_sqlite_engine"


def skip(reason):
    print(f"SKIP: {reason}")
    sys.exit(SKIP_EXIT)


def require_module(name):
    import importlib.util

    if importlib.util.find_spec(name) is None:
        skip(f"the Python module '{name}' is not installed")


if HERE not in sys.path:
    sys.path.insert(0, HERE)
try:
    __import__(EXTENSION)
except ModuleNotFoundError as exc:
    # BRANCH: skip only when the extension itself is not built; a missing module inside it is a real failure
    if exc.name != EXTENSION:
        raise
    skip(f"the pyo3 shim is not built (run scripts/build-pyshim.sh): {exc}")
