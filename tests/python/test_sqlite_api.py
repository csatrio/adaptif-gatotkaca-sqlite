"""sqlite3-style API (sqlite.sqlite): semantics, transactions, errors. No server needed."""

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import os
import sqlite3
import sys
import tempfile

from sqlite import IntegrityError, OperationalError, ProgrammingError, Row, connect
from sqlite.sqlite import classify


def test_classify():
    assert classify("  -- c\n select 1")[0] == 0
    assert classify("/*x*/ insert or replace into t")[0] == 1
    assert classify("delete from t returning id")[0] == 0
    assert classify("create table z(a)")[0] == 2
    assert classify("rollback to savepoint a1")[1] == ("rollback_to", "a1")
    assert classify("end transaction")[1] == ("commit", "")


def test_connection():
    path = os.path.join(tempfile.mkdtemp(), "api.db")
    conn = connect(path)
    conn.executescript("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE, n REAL)")

    assert conn.execute("INSERT INTO t(v, n) VALUES (?, ?)", ("a", 1.5)).lastrowid == 1
    assert conn.executemany("INSERT INTO t(v, n) VALUES (?, ?)", [("b", 2), ("c", 3)]).rowcount == 2
    assert conn.execute("SELECT v, n FROM t ORDER BY id").fetchall() == [("a", 1.5), ("b", 2.0), ("c", 3.0)]
    assert conn.execute("UPDATE t SET n = n + 1 WHERE v IN (:a, :b)", {"a": "a", "b": "b"}).rowcount == 2

    # errors are the stdlib sqlite3 classes
    for sql, exc in (("INSERT INTO t(v) VALUES ('a')", IntegrityError), ("SELECT * FROM nope", OperationalError)):
        try:
            conn.execute(sql)
            raise AssertionError(f"{sql} should have failed")
        except exc as e:
            assert isinstance(e, sqlite3.Error)

    # transactions: BEGIN leases the writer; rollback / savepoints / with-block / transaction()
    count = lambda: conn.execute("SELECT count(*) FROM t").fetchone()[0]
    conn.execute("BEGIN")
    conn.execute("INSERT INTO t(v) VALUES ('tx1')")
    conn.execute("SAVEPOINT s")
    conn.execute("INSERT INTO t(v) VALUES ('tx2')")
    conn.execute("ROLLBACK TO s")
    conn.execute("RELEASE s")
    assert conn.in_transaction and count() == 4
    conn.rollback()
    assert not conn.in_transaction and count() == 3
    with conn:
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t(v) VALUES ('w')")
    assert count() == 4
    try:
        with conn.transaction():
            conn.execute("INSERT INTO t(v) VALUES ('boom')")
            raise ValueError
    except ValueError:
        pass
    assert count() == 4

    # Row factory
    conn.row_factory = Row
    cur = conn.execute("SELECT id, v FROM t ORDER BY id")
    first = cur.fetchone()
    assert first["v"] == "a" and first[0] == 1 and first.keys() == ["id", "v"]
    assert cur.description[1][0] == "v" and len(cur.fetchmany(2)) == 2
    assert conn.execute("INSERT INTO t(v) VALUES ('r') RETURNING id").fetchall()[0]["id"] == 5

    # connections to one file share an engine; closed connections refuse work
    other = connect(path)
    assert other.engine is conn.engine
    other.close()
    conn.close()
    try:
        conn.execute("SELECT 1")
        raise AssertionError("closed connection must raise")
    except ProgrammingError:
        pass


if __name__ == "__main__":
    test_classify()
    test_connection()
    print("sqlite API OK")
