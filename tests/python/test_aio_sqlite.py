"""aiosqlite-style API (sqlite.aio_sqlite). Runs on the event loop's executor; no server or runtime needed."""

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import asyncio
import os
import sys
import tempfile

import sqlite
from sqlite import aio_sqlite


async def test_aio_sqlite():
    path = os.path.join(tempfile.mkdtemp(), "aio.db")
    async with aio_sqlite.connect(path) as db:
        await db.executescript("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        assert (await db.execute("INSERT INTO t(v) VALUES (?)", ("a",))).lastrowid == 1
        await db.executemany("INSERT INTO t(v) VALUES (?)", [("b",), ("c",)])
        async with db.execute("SELECT v FROM t ORDER BY id") as cursor:
            assert [row async for row in cursor] == [("a",), ("b",), ("c",)]
        assert await db.execute_fetchall("SELECT count(*) FROM t") == [(3,)]
        try:
            await db.execute("INSERT INTO t(v) VALUES ('a')")
            raise AssertionError("duplicate must fail")
        except sqlite.IntegrityError:
            pass

        async with db.transaction():
            await db.execute("INSERT INTO t(v) VALUES ('tx')")
            await db.execute("SAVEPOINT s")
            await db.execute("INSERT INTO t(v) VALUES ('no')")
            await db.execute("ROLLBACK TO s")
        assert (await db.execute_fetchall("SELECT count(*) FROM t"))[0] == (4,)
        try:
            async with db.transaction():
                await db.execute("INSERT INTO t(v) VALUES ('boom')")
                raise ValueError
        except ValueError:
            pass
        assert (await db.execute_fetchall("SELECT count(*) FROM t"))[0] == (4,)

        db.row_factory = aio_sqlite.Row
        cursor = await db.execute("SELECT id, v FROM t")
        row = await cursor.fetchone()
        assert row["v"] == "a" and cursor.description[1][0] == "v"
        assert await db.execute_insert("INSERT INTO t(v) VALUES ('z')") == 5

        # the sync API on the same file shares the engine
        shared = sqlite.connect(path)
        assert shared.engine is db.engine and shared.execute("SELECT count(*) FROM t").fetchone() == (5,)
        shared.close()


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(test_aio_sqlite(), 60))
    print("aio sqlite OK")
