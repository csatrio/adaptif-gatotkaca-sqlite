"""
sqlite3-style synchronous API over the Gatotkaca SQLite engine (``sqlite.sqlite``), test port over the pyo3 shim.

``connect()`` returns a ``Connection`` with the familiar ``cursor()`` / ``execute()`` /
``executemany()`` / ``executescript()`` / ``fetch*()`` surface, backed by the Rust engine: reads run
on thread-local read-only connections, writes on one dedicated writer thread. Nothing here needs the
async runtime; every call releases the GIL while it waits.

Semantics (the engine's, equivalent to ``sqlite3`` with ``isolation_level=None``):

- Statements autocommit. ``BEGIN`` leases the writer thread for a real transaction until
  ``COMMIT`` / ``ROLLBACK`` (``SAVEPOINT`` / ``ROLLBACK TO`` / ``RELEASE`` work inside it);
  ``with conn:`` and ``conn.transaction()`` wrap that. ``executescript`` cannot contain ``BEGIN`` /
  ``COMMIT`` (use the statement form or ``transaction()``).
- Rows are tuples. Set ``row_factory = Row`` (or a ``factory(cursor, row)``) to fetch with column
  names; ``cursor.description`` is filled in that mode only (tuple fetches skip it for speed).
- ``cursor.lastrowid`` is set by INSERT; ``cursor.rowcount`` is exact for UPDATE/DELETE/DDL and
  ``-1`` for INSERT (use ``lastrowid``, or ``executemany`` for the total).
- Errors are the stdlib ``sqlite3`` exception classes, so ``except sqlite3.IntegrityError`` works.
"""

from __future__ import annotations

import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from functools import lru_cache
from sqlite3 import (  # noqa: F401  (re-exported DB-API exceptions)
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from sqlite import engine

if TYPE_CHECKING:
    from sqlite.engine import PySqliteDb, PySqliteTransaction

apilevel = "2.0"
threadsafety = 1  # the engine is thread-safe; a Connection (and its transaction) is not
paramstyle = "qmark"  # also accepts named (:name, @name, $name) with a dict

__all__ = [
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Row",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

# ============================================================================
# Statement classification (cached per SQL string)
# ============================================================================

QUERY, INSERT, WRITE, TXN = range(4)

_LEADING = re.compile(r"(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.S)
_WORD = re.compile(r"[A-Za-z]+")
_RETURNING = re.compile(r"\bRETURNING\b", re.I)
_QUERY_WORDS = frozenset({"SELECT", "WITH", "VALUES", "PRAGMA", "EXPLAIN"})
_TXN_WORDS = frozenset({"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"})
_SAVEPOINT_NAME = re.compile(r"(?:TRANSACTION\s+)?(?:TO\s+)?(?:SAVEPOINT\s+)?([A-Za-z_][\w$]*)\s*;?\s*$", re.I)


@lru_cache(maxsize=4096)
def classify(sql: str) -> Tuple[int, Optional[Tuple[str, str]]]:
    """Return ``(kind, command)``: how the engine must run ``sql``. ``command`` is only set for
    transaction control: ``(verb, savepoint_name)`` with verb in begin/commit/rollback/rollback_to/
    savepoint/release."""
    leading = _LEADING.match(sql)  # always matches (may be empty)
    start = leading.end() if leading else 0
    match = _WORD.match(sql, start)
    word = match.group().upper() if match else ""
    # BRANCH: row-producing statements
    if word in _QUERY_WORDS:
        return QUERY, None
    if word in ("INSERT", "REPLACE"):
        return (QUERY if _RETURNING.search(sql) else INSERT), None
    if word in ("UPDATE", "DELETE"):
        return (QUERY if _RETURNING.search(sql) else WRITE), None
    # BRANCH: transaction control is routed to the engine's session API
    if word in _TXN_WORDS:
        rest = sql[match.end():] if match else ""
        if word == "BEGIN":
            return TXN, ("begin", "")
        if word in ("COMMIT", "END"):
            return TXN, ("commit", "")
        if word == "ROLLBACK":
            named = _SAVEPOINT_NAME.match(rest.strip()) if re.search(r"\bTO\b", rest, re.I) else None
            return TXN, (("rollback_to", named.group(1)) if named else ("rollback", ""))
        named = _SAVEPOINT_NAME.match(rest.strip())
        if named is None:
            raise ProgrammingError(f"{word} needs a savepoint name")
        return TXN, (("savepoint" if word == "SAVEPOINT" else "release"), named.group(1))
    return WRITE, None


def translate(exc: BaseException) -> BaseException:
    """Map the engine's ``RuntimeError("SQLite error: ...")`` to the matching ``sqlite3`` class."""
    if not isinstance(exc, RuntimeError) or isinstance(exc, sqlite3.Error):
        return exc
    message = str(exc)
    low = message.lower()
    if low.startswith("sqlite error: "):
        message = message[14:]
        low = low[14:]
    if "constraint" in low:
        return IntegrityError(message)
    if "already finalized" in low or "transaction api" in low:
        return ProgrammingError(message)
    if any(s in low for s in ("no such", "syntax error", "locked", "busy", "readonly", "unable to open", "runtime is not started", "disconnected")):
        return OperationalError(message)
    return DatabaseError(message)


# ============================================================================
# Rows
# ============================================================================


class Row:
    """Result row addressable by index or column name (like ``sqlite3.Row``)."""

    __slots__ = ("_keys", "_values")

    def __init__(self, keys: Tuple[str, ...], values: Tuple[Any, ...]) -> None:
        self._keys = keys
        self._values = values

    def keys(self) -> List[str]:
        return list(self._keys)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            try:
                return self._values[self._keys.index(key)]
            except ValueError:
                folded = key.casefold()
                for index, name in enumerate(self._keys):
                    if name.casefold() == folded:
                        return self._values[index]
                raise IndexError("No item with that key") from None
        return self._values[key]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Row) and self._keys == other._keys and self._values == other._values

    def __hash__(self) -> int:
        return hash((self._keys, self._values))

    def __repr__(self) -> str:
        return f"<Row {dict(zip(self._keys, self._values))!r}>"


def make_rows(dicts: List[Dict[str, Any]], factory: Callable[..., Any], cursor: Any) -> Tuple[List[Any], Tuple[Any, ...]]:
    """Convert the engine's dict rows with ``factory``; returns ``(rows, description)``."""
    if not dicts:
        return [], ()
    keys = tuple(dicts[0])
    description = tuple((key, None, None, None, None, None, None) for key in keys)
    if factory is Row:
        return [Row(keys, tuple(d.values())) for d in dicts], description
    return [factory(cursor, tuple(d.values())) for d in dicts], description


# ============================================================================
# Engines shared between connections to the same database
# ============================================================================

# (path, wal, busy_timeout_ms) identifies an engine
EngineKey = Tuple[str, bool, int]

_engines: Dict[EngineKey, List[Any]] = {}
_engines_lock = threading.Lock()


def acquire_engine(database: Any, max_size: Optional[int], wal: bool, busy_timeout_ms: int) -> Tuple[EngineKey, "PySqliteDb"]:
    """Open (or reuse) the engine for ``database``; pair with ``release_engine``."""
    path = os.fspath(database)
    # BRANCH: plain file paths are normalised so "a.db" and "./a.db" share one engine
    if not path.startswith("file:") and path != ":memory:":
        path = os.path.abspath(path)
    key = (path, wal, busy_timeout_ms)
    with _engines_lock:
        entry = _engines.get(key)
        if entry is None:
            entry = _engines[key] = [engine.create_pool(path, max_size=max_size, wal=wal, busy_timeout_ms=busy_timeout_ms), 0]
        entry[1] += 1
        return key, entry[0]


def release_engine(key: EngineKey) -> None:
    with _engines_lock:
        entry = _engines.get(key)
        if entry is None:
            return
        entry[1] -= 1
        if entry[1] <= 0:
            del _engines[key]
            entry[0].close()


# ============================================================================
# Cursor and Connection
# ============================================================================


class Cursor:
    __slots__ = ("connection", "arraysize", "description", "rowcount", "lastrowid", "row_factory", "_rows", "_pos")

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.arraysize = 1
        self.description: Optional[Tuple[Any, ...]] = None
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self.row_factory = connection.row_factory
        self._rows: List[Any] = []
        self._pos = 0

    def execute(self, sql: str, parameters: Any = None) -> "Cursor":
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
                    self._rows = target.query_tuples_sync(sql, parameters)
                else:
                    self._rows, self.description = make_rows(target.query_sync(sql, parameters), factory, self)
                self.rowcount = -1
            elif kind == INSERT:
                self.lastrowid = target.execute_insert_sync(sql, parameters)
                self.rowcount = -1
            elif kind == WRITE:
                self.rowcount = target.execute_sync(sql, parameters)
            else:
                assert command is not None
                conn._transaction_command(*command)
        except RuntimeError as exc:
            raise translate(exc) from None
        return self

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> "Cursor":
        conn = self.connection
        conn._check()
        batch = seq_of_parameters if isinstance(seq_of_parameters, (list, tuple)) else list(seq_of_parameters)
        try:
            if conn._tx is None:
                self.rowcount = conn._db.execute_many_sync(sql, batch)  # one atomic writer batch
            else:
                self.rowcount = sum(conn._tx.execute_sync(sql, params) for params in batch)
        except RuntimeError as exc:
            raise translate(exc) from None
        self.description = None
        self._rows = []
        return self

    def executescript(self, sql_script: str) -> "Cursor":
        conn = self.connection
        conn._check()
        try:
            (conn._tx or conn._db).execute_batch_sync(sql_script)
        except RuntimeError as exc:
            raise translate(exc) from None
        return self

    def fetchone(self) -> Any:
        pos = self._pos
        if pos >= len(self._rows):
            return None
        self._pos = pos + 1
        return self._rows[pos]

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        size = self.arraysize if size is None else size
        rows = self._rows[self._pos : self._pos + size]
        self._pos += len(rows)
        return rows

    def fetchall(self) -> List[Any]:
        rows = self._rows[self._pos :] if self._pos else self._rows
        self._pos = len(self._rows)
        return rows

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Any:
        pos = self._pos
        if pos >= len(self._rows):
            raise StopIteration
        self._pos = pos + 1
        return self._rows[pos]

    def close(self) -> None:
        self._rows = []

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class Connection:
    """A connection to a database; connections to the same file share one engine."""

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
        """The underlying Rust engine (async API, pool status, ...)."""
        return self._db

    @property
    def in_transaction(self) -> bool:
        return self._tx is not None

    def _check(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed database.")

    def _transaction_command(self, verb: str, name: str) -> None:
        tx = self._tx
        if verb == "begin":
            if tx is not None:
                raise OperationalError("cannot start a transaction within a transaction")
            self._tx = self._db.begin_transaction_sync()
            return
        if tx is None:
            raise OperationalError(f"cannot {verb.replace('_', ' ')} - no transaction is active")
        if verb == "commit":
            self._tx = None
            tx.commit_sync()
        elif verb == "rollback":
            self._tx = None
            tx.rollback_sync()
        elif verb == "savepoint":
            tx.savepoint_sync(name)
        elif verb == "rollback_to":
            tx.rollback_to_sync(name)
        else:
            tx.release_savepoint_sync(name)

    def cursor(self) -> Cursor:
        self._check()
        return Cursor(self)

    def execute(self, sql: str, parameters: Any = None) -> Cursor:
        return Cursor(self).execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> Cursor:
        return Cursor(self).executemany(sql, seq_of_parameters)

    def executescript(self, sql_script: str) -> Cursor:
        return Cursor(self).executescript(sql_script)

    def commit(self) -> None:
        """Commit the open transaction (a no-op in autocommit mode)."""
        self._check()
        if self._tx is not None:
            self._transaction_command("commit", "")

    def rollback(self) -> None:
        """Roll back the open transaction (a no-op in autocommit mode)."""
        self._check()
        if self._tx is not None:
            self._transaction_command("rollback", "")

    @contextmanager
    def transaction(self) -> Iterator["Connection"]:
        """``BEGIN`` ... ``COMMIT`` (or ``ROLLBACK`` when the block raises)."""
        self._transaction_command("begin", "")
        try:
            yield self
        except BaseException:
            if self._tx is not None:
                self._transaction_command("rollback", "")
            raise
        else:
            if self._tx is not None:
                self._transaction_command("commit", "")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tx, self._tx = self._tx, None
        if tx is not None:
            tx.close_sync()  # rolls back
        release_engine(self._key)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._tx is not None:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()


def connect(
    database: Any,
    *,
    max_size: Optional[int] = None,
    wal: bool = True,
    busy_timeout_ms: int = 5000,
    row_factory: Optional[Callable[..., Any]] = None,
) -> Connection:
    """Open ``database`` (a path or a ``file:`` URI). Connections to the same file share one engine."""
    return Connection(database, max_size=max_size, wal=wal, busy_timeout_ms=busy_timeout_ms, row_factory=row_factory)
