"""
aiosqlite-style asyncio API over the Gatotkaca SQLite engine (``sqlite.aio_sqlite``).

Same surface as ``aiosqlite``: ``async with connect(path) as db``, ``await db.execute(...)`` /
``async with db.execute(...) as cursor``, ``await cursor.fetchall()``, ``async for row in cursor``,
``await db.commit()``. Each call runs the engine's synchronous call on the event loop's default executor
(``sqlite.engine``); no async runtime is involved.

See ``sqlite.sqlite`` for the semantics shared with the synchronous API (autocommit with explicit
``BEGIN``, ``Row``, ``sqlite3`` exception classes). ``transaction()`` is the async-with form of a
transaction block.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Iterable, List, Optional

from sqlite.sqlite import (
    INSERT,
    QUERY,
    WRITE,
    Error,
    IntegrityError,
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Row,
    acquire_engine,
    classify,
    make_rows,
    release_engine,
    translate,
)

if TYPE_CHECKING:
    from sqlite.engine import PySqliteDb, PySqliteTransaction

__all__ = [
    "Connection",
    "Cursor",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Row",
    "connect",
]


class _Result:
    """What ``execute()`` returns: awaitable (-> cursor) and async context manager (yields it)."""

    __slots__ = ("_cursor", "_method", "_args")

    def __init__(self, cursor: "Cursor", method: str, args: tuple) -> None:
        self._cursor = cursor
        self._method = method
        self._args = args

    async def _run(self) -> "Cursor":
        await getattr(self._cursor, f"_{self._method}")(*self._args)
        return self._cursor

    def __await__(self):
        return self._run().__await__()

    async def __aenter__(self) -> "Cursor":
        return await self._run()

    async def __aexit__(self, *exc: Any) -> None:
        await self._cursor.close()


class Cursor:
    __slots__ = ("connection", "arraysize", "description", "rowcount", "lastrowid", "row_factory", "_rows", "_pos")

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.arraysize = 1
        self.description: Optional[tuple] = None
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self.row_factory = connection.row_factory
        self._rows: List[Any] = []
        self._pos = 0

    # --- execution -------------------------------------------------------------------------

    async def _execute(self, sql: str, parameters: Any = None) -> None:
        conn = self.connection
        conn._check()
        kind, command = classify(sql)
        target = conn._tx or conn._db
        self.description = None
        self._rows = []
        self._pos = 0
        try:
            if kind == QUERY:
                factory = self.row_factory
                if factory is None:
                    self._rows = await target.query_tuples(sql, parameters)
                else:
                    self._rows, self.description = make_rows(await target.query(sql, parameters), factory, self)
                self.rowcount = -1
            elif kind == INSERT:
                self.lastrowid = await target.execute_insert(sql, parameters)
                self.rowcount = -1
            elif kind == WRITE:
                self.rowcount = await target.execute(sql, parameters)
            else:
                assert command is not None
                await conn._transaction_command(*command)
        except RuntimeError as exc:
            raise translate(exc) from None

    async def _executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> None:
        conn = self.connection
        conn._check()
        batch = seq_of_parameters if isinstance(seq_of_parameters, (list, tuple)) else list(seq_of_parameters)
        try:
            if conn._tx is None:
                self.rowcount = await conn._db.execute_many(sql, batch)  # one atomic writer batch
            else:
                total = 0
                for params in batch:
                    total += await conn._tx.execute(sql, params)
                self.rowcount = total
        except RuntimeError as exc:
            raise translate(exc) from None
        self.description = None
        self._rows = []

    async def _executescript(self, sql_script: str) -> None:
        conn = self.connection
        conn._check()
        try:
            await (conn._tx or conn._db).execute_batch(sql_script)
        except RuntimeError as exc:
            raise translate(exc) from None

    def execute(self, sql: str, parameters: Any = None) -> _Result:
        return _Result(self, "execute", (sql, parameters))

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> _Result:
        return _Result(self, "executemany", (sql, seq_of_parameters))

    def executescript(self, sql_script: str) -> _Result:
        return _Result(self, "executescript", (sql_script,))

    # --- fetching (rows are already in memory; these are coroutines for aiosqlite parity) ---

    async def fetchone(self) -> Any:
        pos = self._pos
        if pos >= len(self._rows):
            return None
        self._pos = pos + 1
        return self._rows[pos]

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        size = self.arraysize if size is None else size
        rows = self._rows[self._pos : self._pos + size]
        self._pos += len(rows)
        return rows

    async def fetchall(self) -> List[Any]:
        rows = self._rows[self._pos :] if self._pos else self._rows
        self._pos = len(self._rows)
        return rows

    def __aiter__(self) -> "Cursor":
        return self

    async def __anext__(self) -> Any:
        pos = self._pos
        if pos >= len(self._rows):
            raise StopAsyncIteration
        self._pos = pos + 1
        return self._rows[pos]

    async def close(self) -> None:
        self._rows = []

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


class Connection:
    """Async connection. Connections to the same file share one engine."""

    def __init__(
        self,
        database: Any,
        *,
        max_size: Optional[int] = None,
        wal: bool = True,
        busy_timeout_ms: int = 5000,
        row_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._key, self._db = acquire_engine(database, max_size, wal, busy_timeout_ms)
        self._tx: Optional["PySqliteTransaction"] = None
        self._closed = False
        self.row_factory = row_factory

    @property
    def engine(self) -> "PySqliteDb":
        """The underlying Rust engine."""
        return self._db

    @property
    def in_transaction(self) -> bool:
        return self._tx is not None

    def _check(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed database.")

    async def _transaction_command(self, verb: str, name: str) -> None:
        tx = self._tx
        if verb == "begin":
            if tx is not None:
                raise OperationalError("cannot start a transaction within a transaction")
            self._tx = await self._db.begin_transaction()
            return
        if tx is None:
            raise OperationalError(f"cannot {verb.replace('_', ' ')} - no transaction is active")
        if verb == "commit":
            self._tx = None
            await tx.commit()
        elif verb == "rollback":
            self._tx = None
            await tx.rollback()
        elif verb == "savepoint":
            await tx.savepoint(name)
        elif verb == "rollback_to":
            await tx.rollback_to(name)
        else:
            await tx.release_savepoint(name)

    # --- aiosqlite surface -----------------------------------------------------------------

    async def cursor(self) -> Cursor:
        self._check()
        return Cursor(self)

    def execute(self, sql: str, parameters: Any = None) -> _Result:
        return Cursor(self).execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> _Result:
        return Cursor(self).executemany(sql, seq_of_parameters)

    def executescript(self, sql_script: str) -> _Result:
        return Cursor(self).executescript(sql_script)

    async def execute_fetchall(self, sql: str, parameters: Any = None) -> List[Any]:
        cursor = Cursor(self)
        await cursor._execute(sql, parameters)
        return cursor._rows

    async def execute_insert(self, sql: str, parameters: Any = None) -> Optional[int]:
        """Run an INSERT and return its new rowid."""
        cursor = Cursor(self)
        await cursor._execute(sql, parameters)
        return cursor.lastrowid

    async def commit(self) -> None:
        """Commit the open transaction (a no-op in autocommit mode)."""
        self._check()
        if self._tx is not None:
            await self._transaction_command("commit", "")

    async def rollback(self) -> None:
        """Roll back the open transaction (a no-op in autocommit mode)."""
        self._check()
        if self._tx is not None:
            await self._transaction_command("rollback", "")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["Connection"]:
        """``BEGIN`` ... ``COMMIT`` (or ``ROLLBACK`` when the block raises)."""
        await self._transaction_command("begin", "")
        try:
            yield self
        except BaseException:
            if self._tx is not None:
                await self._transaction_command("rollback", "")
            raise
        else:
            if self._tx is not None:
                await self._transaction_command("commit", "")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tx, self._tx = self._tx, None
        if tx is not None:
            await tx.close()  # rolls back
        release_engine(self._key)

    def __await__(self):
        async def _opened() -> "Connection":
            return self

        return _opened().__await__()

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._tx is not None:
                await (self.commit() if exc_type is None else self.rollback())
        finally:
            await self.close()


def connect(
    database: Any,
    *,
    max_size: Optional[int] = None,
    wal: bool = True,
    busy_timeout_ms: int = 5000,
    row_factory: Optional[Callable[..., Any]] = None,
) -> Connection:
    """Open ``database``; use ``async with connect(...) as db`` or ``db = await connect(...)``."""
    return Connection(database, max_size=max_size, wal=wal, busy_timeout_ms=busy_timeout_ms, row_factory=row_factory)
