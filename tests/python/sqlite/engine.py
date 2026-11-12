"""Python face of the pyo3 test shim (``tests/pyshim``, module ``_sqlite_engine``).

The Rust objects only have the synchronous calls (``*_sync``, GIL released while they wait). ``Engine`` and
``Transaction`` wrap them and add the awaitable twins (``execute``, ``query``, ``begin_transaction``, ...) by running the
sync call on the event loop's default executor, so no async runtime lives in the engine. Named parameters (a dict with
``:name`` / ``@name`` / ``$name``) are rewritten to positional ``?`` here, because the engine binds positionally.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Callable, Optional, Tuple

import _sqlite_engine as _raw

__all__ = ["Engine", "Transaction", "PySqliteDb", "PySqliteTransaction", "create_pool", "sqlite_version"]

sqlite_version = _raw.sqlite_version

# string literals and quoted identifiers are skipped; a named parameter is :name, @name or $name
_NAMED = re.compile(r"""('(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\])|[:@$]([A-Za-z_][A-Za-z0-9_]*)""")


def bind(sql: str, parameters: Any) -> Tuple[str, Any]:
    """Return ``(sql, positional parameters)``; a mapping is turned into ``?`` placeholders in order of appearance."""
    # BRANCH: positional or no parameters pass through
    if parameters is None or not hasattr(parameters, "keys"):
        return sql, parameters
    ordered: list = []

    def repl(match: "re.Match[str]") -> str:
        if match.group(1) is not None:
            return match.group(1)
        ordered.append(parameters[match.group(2)])
        return "?"

    return _NAMED.sub(repl, sql), ordered


async def _offload(fn: Callable[..., Any], *args: Any) -> Any:
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)


class _Runner:
    """Statement calls shared by the engine and a leased transaction session."""

    __slots__ = ("_raw",)

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    # --- sync -----------------------------------------------------------------------------
    def query_tuples_sync(self, sql: str, parameters: Any = None) -> list:
        sql, parameters = bind(sql, parameters)
        return self._raw.query_tuples_sync(sql, parameters)

    def query_sync(self, sql: str, parameters: Any = None) -> list:
        sql, parameters = bind(sql, parameters)
        return self._raw.query_sync(sql, parameters)

    def execute_sync(self, sql: str, parameters: Any = None) -> int:
        sql, parameters = bind(sql, parameters)
        return self._raw.execute_sync(sql, parameters)

    def execute_insert_sync(self, sql: str, parameters: Any = None) -> int:
        sql, parameters = bind(sql, parameters)
        return self._raw.execute_insert_sync(sql, parameters)

    def execute_batch_sync(self, sql: str) -> None:
        self._raw.execute_batch_sync(sql)

    # --- awaitable twins ------------------------------------------------------------------
    async def query_tuples(self, sql: str, parameters: Any = None) -> list:
        return await _offload(self.query_tuples_sync, sql, parameters)

    async def query(self, sql: str, parameters: Any = None) -> list:
        return await _offload(self.query_sync, sql, parameters)

    async def execute(self, sql: str, parameters: Any = None) -> int:
        return await _offload(self.execute_sync, sql, parameters)

    async def execute_insert(self, sql: str, parameters: Any = None) -> int:
        return await _offload(self.execute_insert_sync, sql, parameters)

    async def execute_batch(self, sql: str) -> None:
        await _offload(self.execute_batch_sync, sql)


class Transaction(_Runner):
    """A leased writer session (``begin_transaction``)."""

    __slots__ = ()

    def savepoint_sync(self, name: str) -> None:
        self._raw.savepoint_sync(name)

    def rollback_to_sync(self, name: str) -> None:
        self._raw.rollback_to_sync(name)

    def release_savepoint_sync(self, name: str) -> None:
        self._raw.release_savepoint_sync(name)

    def commit_sync(self) -> None:
        self._raw.commit_sync()

    def rollback_sync(self) -> None:
        self._raw.rollback_sync()

    def close_sync(self) -> None:
        self._raw.close_sync()

    async def savepoint(self, name: str) -> None:
        await _offload(self._raw.savepoint_sync, name)

    async def rollback_to(self, name: str) -> None:
        await _offload(self._raw.rollback_to_sync, name)

    async def release_savepoint(self, name: str) -> None:
        await _offload(self._raw.release_savepoint_sync, name)

    async def commit(self) -> None:
        await _offload(self._raw.commit_sync)

    async def rollback(self) -> None:
        await _offload(self._raw.rollback_sync)

    async def close(self) -> None:
        await _offload(self._raw.close_sync)


class Engine(_Runner):
    """One database: thread-local readers plus one writer thread (``adaptif-gatotkaca-sqlite``)."""

    __slots__ = ()

    def execute_many_sync(self, sql: str, batch: Any) -> int:
        return self._raw.execute_many_sync(*self._bind_many(sql, list(batch)))

    @staticmethod
    def _bind_many(sql: str, rows: list) -> Tuple[str, list]:
        # BRANCH: mappings share one rewritten statement (same names, same order)
        if rows and hasattr(rows[0], "keys"):
            bound_sql, _ = bind(sql, rows[0])
            return bound_sql, [bind(sql, r)[1] for r in rows]
        return sql, rows

    def begin_transaction_sync(self) -> Transaction:
        return Transaction(self._raw.begin_transaction_sync())

    def last_committed(self) -> int:
        return self._raw.last_committed()

    def close_sync(self) -> None:
        self._raw.close_sync()

    async def execute_many(self, sql: str, batch: Any) -> int:
        return await _offload(self.execute_many_sync, sql, list(batch))

    async def begin_transaction(self) -> Transaction:
        return await _offload(self.begin_transaction_sync)

    def close(self) -> None:
        """Release the engine (stops the writer thread once no transaction still holds it). Synchronous on purpose."""
        self._raw.close_sync()


PySqliteDb = Engine
PySqliteTransaction = Transaction


def create_pool(path: str, max_size: Optional[int] = None, wal: bool = True, busy_timeout_ms: int = 5000) -> Engine:
    return Engine(_raw.create_pool(path, max_size=max_size, wal=wal, busy_timeout_ms=busy_timeout_ms))
