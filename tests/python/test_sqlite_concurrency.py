#!/usr/bin/env python3
"""Concurrency test for the SQLite engine: hammers a plain-HTTP stdlib server (``http_server.py``) with ``wrk``.

The server runs the ported Python layer over the pyo3 shim. The Lua script alternates POST /sqlite/write and
GET /sqlite/read; the test fails on any non-2xx/3xx response, on a dead server, or when the written rows are not a
contiguous run of ids afterwards. Plain HTTP only; no threads are pinned to cores.
"""

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
WRK = shutil.which("wrk")
if WRK is None:
    _main.skip("the 'wrk' load generator is not installed")

LUA = """local counter = 0

request = function()
    counter = counter + 1
    if counter % 2 == 0 then
        local body = '{"payload":"wrk-worker-' .. counter .. '"}'
        return wrk.format("POST", "/sqlite/write", {["Content-Type"] = "application/json"}, body)
    else
        return wrk.format("GET", "/sqlite/read?limit=5")
    end
end
"""


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method: str, url: str, body: dict = None, timeout: float = 5.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def test_sqlite_concurrency(wrk_threads: int = 4, wrk_connections: int = 100, duration_seconds: int = 5) -> None:
    port = find_free_port()
    workdir = tempfile.mkdtemp(prefix="test_sqlite_conc_", dir=os.path.expanduser("~"))
    db_path = os.path.join(workdir, "conc.db")
    lua_path = os.path.join(workdir, "load.lua")
    with open(lua_path, "w") as f:
        f.write(LUA)
    log_path = os.path.join(workdir, "server.log")
    base_url = f"http://127.0.0.1:{port}"
    env = dict(os.environ, HTTP_PORT=str(port), SQLITE_DB_PATH=db_path)

    print(f"[1/4] Starting the plain-HTTP server on {base_url} (db {db_path})")
    with open(log_path, "w+") as log:
        server = subprocess.Popen([sys.executable, os.path.join(HERE, "http_server.py")], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.time() + 10.0
            while True:
                assert server.poll() is None, f"server exited early:\n{open(log_path).read()}"
                try:
                    status, body = http("GET", f"{base_url}/health", timeout=1.0)
                    if status == 200 and "healthy" in body:
                        break
                except OSError:
                    pass
                assert time.time() < deadline, f"server not healthy within 10s:\n{open(log_path).read()}"
                time.sleep(0.1)

            print("[2/4] Smoke test")
            status, body = http("POST", f"{base_url}/sqlite/write", {"payload": "initial_seed"})
            assert status == 200, (status, body)
            status, body = http("GET", f"{base_url}/sqlite/read?limit=5")
            assert status == 200 and json.loads(body)["count"] == 1, (status, body)

            print(f"[3/4] wrk: {wrk_threads} threads, {wrk_connections} connections, {duration_seconds}s")
            cmd = [WRK, f"-t{wrk_threads}", f"-c{wrk_connections}", f"-d{duration_seconds}s", "-s", lua_path, "--latency", f"{base_url}/"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration_seconds + 60)
            print(result.stdout.strip())
            assert result.returncode == 0, f"wrk failed: {result.stderr}"
            assert "Non-2xx or 3xx" not in result.stdout, "the server answered with error statuses under load"
            match = re.search(r"(\d+) requests in", result.stdout)
            assert match and int(match.group(1)) > 0, "wrk completed no requests"
            assert server.poll() is None, f"server died under load:\n{open(log_path).read()}"

            print("[4/4] Verifying the database")
            status, body = http("GET", f"{base_url}/health")
            assert status == 200
            db = sqlite3.connect(db_path)
            count, max_id = db.execute("SELECT count(*), max(id) FROM test_records").fetchone()
            db.close()
            assert count >= 1 and count == max_id, f"rows are not a contiguous run of ids: count={count} max_id={max_id}"
            print(f"✓ {count} rows written, ids contiguous")
        finally:
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=2.0)
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SQLite concurrency hammer test with 'wrk' against a plain-HTTP stdlib server")
    parser.add_argument("--threads", type=int, default=4, help="wrk threads (default: 4)")
    parser.add_argument("--connections", type=int, default=100, help="wrk connections (default: 100)")
    parser.add_argument("--duration", type=int, default=5, help="wrk duration in seconds (default: 5)")
    args = parser.parse_args()
    test_sqlite_concurrency(args.threads, args.connections, args.duration)
    print("sqlite concurrency OK")
