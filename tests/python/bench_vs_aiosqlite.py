#!/usr/bin/env python3
"""Benchmark: this crate's aio layer (tests/python/sqlite/aio_sqlite.py over the pyo3 shim) against ``aiosqlite``.

Not a test (no ``test_`` prefix, run_all.py does not run it). Build the shim with ``scripts/build-pyshim.sh --release`` first and run
inside a virtualenv that has ``aiosqlite``:  ``python3 tests/python/bench_vs_aiosqlite.py [--rows N] [--tasks N] [--rounds N]``.
Every scenario is bounded by ``--timeout`` seconds. Both sides use WAL, autocommit statements and a database file under $HOME.
Scenarios (median of the rounds, higher ops/s is better):
  insert_seq     sequential single-row INSERTs, one await each
  insert_batch   one executemany of --rows rows
  read_seq       sequential point SELECTs by primary key
  read_conc      --tasks concurrent tasks, each doing point SELECTs (asyncio.gather)
  mixed_conc     --tasks concurrent tasks, 1 write : 4 reads
"""

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import argparse
import asyncio
import os
import shutil
import statistics
import tempfile
import time

try:
    import aiosqlite
except ImportError:
    _main.skip("the Python module 'aiosqlite' is not installed")

from sqlite import aio_sqlite

SCHEMA = "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT NOT NULL, n INTEGER NOT NULL)"


class Gatotkaca:
    name = "gatotkaca"

    async def open(self, path):
        self.db = await aio_sqlite.connect(path)
        await self.db.executescript(SCHEMA)  # the engine sets WAL + synchronous=NORMAL itself

    async def write(self, sql, params=()):
        await self.db.execute(sql, params)

    async def write_many(self, sql, rows):
        await self.db.executemany(sql, rows)

    async def read(self, sql, params=()):
        return await self.db.execute_fetchall(sql, params)

    async def close(self):
        await self.db.close()


class Aiosqlite:
    name = "aiosqlite"

    async def open(self, path):
        self.db = await aiosqlite.connect(path, isolation_level=None)  # autocommit, like the other side
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.execute(SCHEMA)

    async def write(self, sql, params=()):
        await self.db.execute(sql, params)

    async def write_many(self, sql, rows):
        await self.db.execute("BEGIN")
        await self.db.executemany(sql, rows)
        await self.db.execute("COMMIT")

    async def read(self, sql, params=()):
        async with self.db.execute(sql, params) as cur:
            return await cur.fetchall()

    async def close(self):
        await self.db.close()


INS = "INSERT INTO t(v, n) VALUES (?, ?)"
SEL = "SELECT v, n FROM t WHERE id = ?"


async def scenario(name, impl, path, args):
    n, tasks = args.rows, args.tasks
    if name == "insert_seq":
        count = max(n // 5, 200)
        t0 = time.perf_counter()
        for i in range(count):
            await impl.write(INS, (f"v{i}", i))
        return count, time.perf_counter() - t0
    if name == "insert_batch":
        rows = [(f"v{i}", i) for i in range(n)]
        t0 = time.perf_counter()
        await impl.write_many(INS, rows)
        return n, time.perf_counter() - t0
    # read scenarios need data: seeded outside the timer
    await impl.write_many(INS, [(f"v{i}", i) for i in range(n)])
    if name == "read_seq":
        count = max(n // 2, 500)
        t0 = time.perf_counter()
        for i in range(count):
            await impl.read(SEL, (1 + i % n,))
        return count, time.perf_counter() - t0
    per_task = max(n // tasks, 20)
    if name == "read_conc":
        async def worker(k):
            for i in range(per_task):
                await impl.read(SEL, (1 + (k * 7919 + i) % n,))
        t0 = time.perf_counter()
        await asyncio.gather(*(worker(k) for k in range(tasks)))
        return per_task * tasks, time.perf_counter() - t0
    if name == "mixed_conc":
        async def worker(k):
            for i in range(per_task):
                if i % 5 == 0:
                    await impl.write(INS, (f"m{k}-{i}", i))
                else:
                    await impl.read(SEL, (1 + (k * 7919 + i) % n,))
        t0 = time.perf_counter()
        await asyncio.gather(*(worker(k) for k in range(tasks)))
        return per_task * tasks, time.perf_counter() - t0
    raise ValueError(name)


SCENARIOS = ["insert_seq", "insert_batch", "read_seq", "read_conc", "mixed_conc"]


async def run_once(impl_cls, name, args, workdir):
    path = os.path.join(workdir, f"{impl_cls.name}-{name}-{time.perf_counter_ns()}.db")
    impl = impl_cls()
    try:
        await impl.open(path)
        ops, secs = await asyncio.wait_for(scenario(name, impl, path, args), args.timeout)
    finally:
        await impl.close()
    return ops / secs


async def main(args):
    workdir = tempfile.mkdtemp(prefix="bench_aio_", dir=os.path.expanduser("~"))
    results = {}
    try:
        for name in SCENARIOS:
            for impl_cls in (Gatotkaca, Aiosqlite):
                runs = []
                for _ in range(args.rounds):
                    runs.append(await run_once(impl_cls, name, args, workdir))
                    print(f"  {name:<13}{impl_cls.name:<10}{runs[-1]:>12,.0f} ops/s", file=_sys.stderr, flush=True)
                results[(name, impl_cls.name)] = statistics.median(runs)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    print(f"rows={args.rows} tasks={args.tasks} rounds={args.rounds} python={_sys.version.split()[0]} aiosqlite={aiosqlite.__version__}")
    print(f"{'scenario':<14}{'gatotkaca ops/s':>18}{'aiosqlite ops/s':>18}{'speedup':>10}")
    for name in SCENARIOS:
        g, a = results[(name, "gatotkaca")], results[(name, "aiosqlite")]
        print(f"{name:<14}{g:>18,.0f}{a:>18,.0f}{g / a:>9.2f}x")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--rows", type=int, default=20000)
    p.add_argument("--tasks", type=int, default=64)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--timeout", type=float, default=120.0, help="seconds per scenario run")
    asyncio.run(main(p.parse_args()))
