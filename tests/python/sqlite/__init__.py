"""Test port of the Gatotkaca SQLite Python layer over the pyo3 shim (``tests/pyshim``).

``sqlite`` (sqlite3-style), ``aio_sqlite`` (aiosqlite-style) and ``tortoise_sqlite`` (Tortoise ORM, needs the ``tortoise``
package). ``from sqlite import connect`` is the synchronous API; the asyncio one is ``from sqlite.aio_sqlite import connect``.
Importing this package loads neither Tortoise nor the engine until a submodule needs it.
"""

from __future__ import annotations

from typing import Any

_SQLITE_NAMES = (
    "Connection", "Cursor", "DataError", "DatabaseError", "Error", "IntegrityError", "InterfaceError", "InternalError",
    "NotSupportedError", "OperationalError", "ProgrammingError", "Row", "Warning", "apilevel", "connect", "paramstyle",
    "threadsafety",
)
_SUBMODULES = ("aio_sqlite", "engine", "sqlite", "tortoise_sqlite")

__all__ = [*_SQLITE_NAMES, "create_tortoise_config", *_SUBMODULES]


def __getattr__(name: str) -> Any:
    import importlib

    # BRANCH: DB-API names come from sqlite.sqlite, the Tortoise helper from sqlite.tortoise_sqlite, submodules import themselves
    if name in _SQLITE_NAMES:
        value = getattr(importlib.import_module("sqlite.sqlite"), name)
    elif name == "create_tortoise_config":
        value = getattr(importlib.import_module("sqlite.tortoise_sqlite"), name)
    elif name in _SUBMODULES:
        value = importlib.import_module(f"{__name__}.{name}")
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
