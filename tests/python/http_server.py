#!/usr/bin/env python3
"""Plain-HTTP stdlib server over the ported SQLite layer, the target of ``test_sqlite_concurrency.py``.

Routes: GET /, GET /health, POST /sqlite/write (JSON ``{"payload": ...}``), GET /sqlite/read?limit=N.
Environment: ``HTTP_PORT`` (required), ``SQLITE_DB_PATH`` (required). No TLS, no metrics endpoint, no CPU pinning.
"""

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import sqlite

CONN = sqlite.connect(os.environ["SQLITE_DB_PATH"])
CONN.executescript(
    "CREATE TABLE IF NOT EXISTS test_records ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created_at REAL NOT NULL)"
)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # silence per-request logging
        pass

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        # BRANCH: one arm per route
        if url.path == "/":
            self.reply(200, {"message": "Hello from the sqlite test server"})
        elif url.path == "/health":
            self.reply(200, {"status": "healthy"})
        elif url.path == "/sqlite/read":
            try:
                limit = int(parse_qs(url.query).get("limit", ["10"])[0])
            except ValueError:
                return self.reply(400, {"error": "limit must be an integer"})
            rows = CONN.execute(
                "SELECT id, payload, created_at FROM test_records ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            self.reply(200, {"count": len(rows), "records": rows})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if urlparse(self.path).path != "/sqlite/write":
            return self.reply(404, {"error": "not found"})
        payload = "sample_payload"
        try:
            body = json.loads(raw or b"{}")
            if isinstance(body, dict) and "payload" in body:
                payload = str(body["payload"])
        except ValueError:
            pass  # BRANCH: a malformed body keeps the default payload
        affected = CONN.execute(
            "INSERT INTO test_records (payload, created_at) VALUES (?, ?)", (payload, time.time())
        ).lastrowid
        self.reply(200, {"status": "ok", "id": affected})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024


if __name__ == "__main__":
    server = Server(("127.0.0.1", int(os.environ["HTTP_PORT"])), Handler)
    print("listening", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)
