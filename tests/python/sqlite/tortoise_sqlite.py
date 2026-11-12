"""
Tortoise ORM backend for the Gatotkaca SQLite engine (``sqlite.tortoise_sqlite``).

``Tortoise.init(config=create_tortoise_config("app.db", ["models"]))``. The client talks to the Rust
engine (``PySqliteDb``) directly: no connection pool objects, no locks and no per-call wrappers,
because the engine already serialises writes on its writer thread and runs reads in parallel on
reader threads. ``in_transaction()`` leases the writer for a session (``PySqliteTransaction``) and
nested blocks become savepoints. The engine is shared with ``sqlite.sqlite`` / ``sqlite.aio_sqlite``
connections to the same file.

Engine errors are classified by ``sqlite.sqlite.translate`` (one mapping for all three APIs) and
re-raised as Tortoise's exception types.
"""

from __future__ import annotations

import os
from itertools import count
from typing import Any, Dict, List, Optional, Sequence, Union

from pypika_tortoise import SQLLiteQuery
from tortoise.backends.base.client import (
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    NestedTransactionContext,
    TransactionalDBClient,
    TransactionContext,
)
from tortoise.backends.sqlite.executor import SqliteExecutor
from tortoise.backends.sqlite.schema_generator import SqliteSchemaGenerator
from tortoise.connection import get_connections
from tortoise.exceptions import IntegrityError, OperationalError, TransactionManagementError

from sqlite.engine import PySqliteDb, PySqliteTransaction
from sqlite.sqlite import QUERY, EngineKey, acquire_engine, classify, release_engine, translate
from sqlite.sqlite import IntegrityError as EngineIntegrityError

__all__ = ["create_tortoise_config"]

# What the statement methods run on: the engine, or a leased transaction session. Both have the
# same execute / execute_insert / query / execute_batch coroutines.
Session = Union[PySqliteDb, PySqliteTransaction]


def _convert(exc: RuntimeError) -> Exception:
    """Engine error -> Tortoise exception (constraint failures are integrity errors)."""
    return IntegrityError(exc) if isinstance(translate(exc), EngineIntegrityError) else OperationalError(exc)


def _savepoint_name(_c=count()) -> str:
    return f"adaptif_savepoint_{next(_c)}"


class DirectConnection(ConnectionWrapper[Session]):
    """``acquire_connection()`` result: hands out the session without locking (the engine is
    thread-safe), connecting on first use."""

    __slots__ = ("owner",)

    def __init__(self, owner: "AdaptifSqliteClient") -> None:
        self.owner = owner

    async def __aenter__(self) -> Session:
        return self.owner._conn or await self.owner._connect()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class AdaptifSqliteClient(BaseDBAsyncClient):
    """Tortoise client for the Rust ``adaptif-gatotkaca-sqlite`` dual-routing engine."""

    executor_class = SqliteExecutor
    query_class = SQLLiteQuery
    schema_generator = SqliteSchemaGenerator
    capabilities = Capabilities(
        "sqlite",
        daemon=False,
        requires_limit=True,
        inline_comment=True,
        support_for_update=False,
        support_update_limit_order_by=False,
        can_rollback_ddl=True,
        support_returning=True,
    )

    def __init__(
        self,
        file_path: str = ":memory:",
        max_size: int = 16,
        wal: bool = True,
        busy_timeout_ms: int = 5000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.filename = file_path
        self.max_size = int(max_size) if max_size else 16
        self.wal = bool(wal)
        self.busy_timeout_ms = int(busy_timeout_ms) if busy_timeout_ms else 5000
        self._db: Optional[PySqliteDb] = None
        self._conn: Optional[Session] = None
        self._engine_key: Optional[EngineKey] = None

    async def _connect(self) -> Session:
        await self.create_connection(with_db=True)
        assert self._conn is not None
        return self._conn

    async def create_connection(self, with_db: bool) -> None:
        if self._db is not None:
            return
        self._engine_key, self._db = acquire_engine(self.filename, self.max_size, self.wal, self.busy_timeout_ms)
        self._conn = self._db
        await self._post_connect()
        self.log.debug(
            "Opened Gatotkaca SQLite engine for '%s' (max_size=%d, wal=%s)", self.filename, self.max_size, self.wal
        )

    async def close(self) -> None:
        if self._db is not None and self._engine_key is not None:
            release_engine(self._engine_key)
            self.log.debug("Closed Gatotkaca SQLite engine for '%s'", self.filename)
            self._db = self._conn = self._engine_key = None

    async def db_create(self) -> None:
        pass  # SQLite creates the file on first use

    async def db_delete(self) -> None:
        await self.close()
        if self.filename == ":memory:":
            return
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(self.filename + suffix)
            except OSError:
                pass

    def acquire_connection(self) -> DirectConnection:
        return DirectConnection(self)

    def _in_transaction(self) -> TransactionContext:
        return AdaptifTransactionContext(self)

    async def execute_insert(self, query: str, values: list) -> int:
        if "\x00" in query:
            query = query.replace("\x00", "'||CHAR(0)||'")
        conn = self._conn or await self._connect()
        self.log.debug("%s: %s", query, values)
        try:
            return await conn.execute_insert(query, values)
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def execute_many(self, query: str, values: list[list]) -> None:
        if "\x00" in query:
            query = query.replace("\x00", "'||CHAR(0)||'")
        db = self._db or (await self._connect() and self._db)
        assert db is not None
        self.log.debug("%s: %s", query, values)
        try:
            await db.execute_many(query, values)  # one atomic writer batch, single hop
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def execute_query(self, query: str, values: list | None = None) -> tuple[int, Sequence[dict]]:
        if "\x00" in query:
            query = query.replace("\x00", "'||CHAR(0)||'")
        conn = self._conn or await self._connect()
        self.log.debug("%s: %s", query, values)
        try:
            # BRANCH: row-producing statements (SELECT, PRAGMA, ..., RETURNING) vs writes
            if classify(query)[0] == QUERY:
                rows = await conn.query(query, values)
                return len(rows), rows
            return await conn.execute(query, values), []
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def execute_query_dict(self, query: str, values: list | None = None) -> list[dict]:
        if "\x00" in query:
            query = query.replace("\x00", "'||CHAR(0)||'")
        conn = self._conn or await self._connect()
        self.log.debug("%s: %s", query, values)
        try:
            return await conn.query(query, values)
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def execute_script(self, query: str) -> None:
        conn = self._conn or await self._connect()
        self.log.debug(query)
        try:
            await conn.execute_batch(query)
        except RuntimeError as exc:
            raise _convert(exc) from None


class AdaptifTransactionWrapper(AdaptifSqliteClient, TransactionalDBClient):
    """The client bound to one leased writer session; every statement runs inside it."""

    def __init__(self, parent: AdaptifSqliteClient, txn: PySqliteTransaction) -> None:
        self.capabilities = parent.capabilities
        self.connection_name = parent.connection_name
        self.filename = parent.filename
        self.max_size = parent.max_size
        self.wal = parent.wal
        self.busy_timeout_ms = parent.busy_timeout_ms
        self.log = parent.log
        self.fetch_inserted = parent.fetch_inserted
        self._parent = parent
        self._db = parent._db
        self._conn = txn
        self._txn = txn
        self._finalized = False
        self._savepoint_stack: list[str] = []

    def _in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(self)

    async def execute_many(self, query: str, values: list[list]) -> None:
        if "\x00" in query:
            query = query.replace("\x00", "'||CHAR(0)||'")
        self.log.debug("%s: %s", query, values)
        try:
            for params in values:  # the session has no batch call; the writer is already leased
                await self._txn.execute(query, params)
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def begin(self) -> None:
        pass  # the session was begun when the lease was taken

    async def _finish(self, commit: bool) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        self._finalized = True
        try:
            await (self._txn.commit() if commit else self._txn.rollback())
        except RuntimeError as exc:
            raise _convert(exc) from None

    async def rollback(self) -> None:
        await self._finish(False)

    async def commit(self) -> None:
        await self._finish(True)

    async def savepoint(self) -> None:
        name = _savepoint_name()
        self._savepoint_stack.append(name)
        await self._txn.savepoint(name)

    async def savepoint_rollback(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        if not self._savepoint_stack:
            raise TransactionManagementError("No savepoint to rollback to")
        await self._txn.rollback_to(self._savepoint_stack.pop())

    async def release_savepoint(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        if not self._savepoint_stack:
            raise TransactionManagementError("No savepoint to release")
        await self._txn.release_savepoint(self._savepoint_stack.pop())


class AdaptifTransactionContext(TransactionContext):
    """``in_transaction()``: lease the writer, route the connection name to the session."""

    __slots__ = ("parent", "client", "connection_name", "wrapper", "token")

    def __init__(self, parent: AdaptifSqliteClient) -> None:
        self.parent = parent
        self.connection_name = parent.connection_name
        self.wrapper: Optional[AdaptifTransactionWrapper] = None
        self.token: Any = None

    async def __aenter__(self) -> TransactionalDBClient:
        client = self.parent
        db = client._db
        if db is None:
            await client._connect()
            db = client._db
        assert db is not None
        try:
            txn = await db.begin_transaction()
        except RuntimeError as exc:
            raise _convert(exc) from None
        self.wrapper = self.client = AdaptifTransactionWrapper(client, txn)
        self.token = get_connections().set(self.connection_name, self.wrapper)
        return self.wrapper

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        wrapper = self.wrapper
        assert wrapper is not None
        try:
            if not wrapper._finalized:
                # a failed transaction cannot be rolled back again after TransactionManagementError
                if exc_type is None:
                    await wrapper.commit()
                elif exc_type is not TransactionManagementError:
                    await wrapper.rollback()
        finally:
            get_connections().reset(self.token)
            if not wrapper._finalized:
                await wrapper._txn.close()  # releases the lease (rolls back)


# Module export for Tortoise engine discovery
client_class = AdaptifSqliteClient


def create_tortoise_config(
    db_path: "str | os.PathLike[str]" = "app.db",
    model_modules: Optional[List[str]] = None,
    max_size: int = 16,
    wal: bool = True,
    connection_name: str = "default",
    app_name: str = "models",
    busy_timeout_ms: int = 5000,
    **credentials: Any,
) -> Dict[str, Any]:
    """
    Build a Tortoise ``config`` dict backed by the Gatotkaca SQLite engine.

    ``Tortoise.init(config=create_tortoise_config(...))`` and ``TORTOISE_ORM`` for aerich both work.
    Extra keyword arguments are passed to the client as connection credentials.
    """
    return {
        "connections": {
            connection_name: {
                "engine": "sqlite.tortoise_sqlite",
                "credentials": {
                    "file_path": os.fspath(db_path),
                    "max_size": max_size,
                    "wal": wal,
                    "busy_timeout_ms": busy_timeout_ms,
                    **credentials,
                },
            }
        },
        "apps": {
            app_name: {
                "models": list(model_modules) if model_modules else ["__main__"],
                "default_connection": connection_name,
            }
        },
    }
