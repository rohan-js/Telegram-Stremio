#!/usr/bin/env python3
"""
Host-only WARP control bridge.

Run this on the VPS host, bound to 127.0.0.1. The app container should call it
through Docker's host-gateway address with WARP_CONTROL_URL and
WARP_CONTROL_SECRET. This avoids mounting the Docker socket inside the app.
"""

from __future__ import annotations

import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


HOST = os.environ.get("WARP_CONTROL_BIND", "127.0.0.1")
PORT = int(os.environ.get("WARP_CONTROL_PORT", "8765"))
SECRET = os.environ.get("WARP_CONTROL_SECRET", "")
SCRIPT = os.environ.get("WARP_TOGGLE_SCRIPT", "/home/ubuntu/telegram-stremio/deploy/warp_toggle.sh")
CONFIG_FILE = os.environ.get("CONFIG_FILE", "/home/ubuntu/telegram-stremio/config.env")


def _json(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_proxy_enabled() -> bool:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            for line in file:
                if line.startswith("TELEGRAM_PROXY_ENABLED="):
                    value = line.split("=", 1)[1].strip().strip('"').strip("'").lower()
                    return value == "true"
    except FileNotFoundError:
        return False
    return False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        return

    def _authorized(self) -> bool:
        if not SECRET:
            return False
        return self.headers.get("x-warp-control-secret") == SECRET

    def do_GET(self) -> None:
        if self.path not in {"/health", "/status"}:
            _json(self, 404, {"ok": False, "message": "not found"})
            return
        if self.path == "/status" and not self._authorized():
            _json(self, 401, {"ok": False, "message": "unauthorized"})
            return
        _json(
            self,
            200,
            {
                "ok": True,
                "telegram_proxy_enabled": _read_proxy_enabled(),
                "script": SCRIPT,
            },
        )

    def do_POST(self) -> None:
        if self.path != "/apply":
            _json(self, 404, {"ok": False, "message": "not found"})
            return
        if not self._authorized():
            _json(self, 401, {"ok": False, "message": "unauthorized"})
            return

        length = int(self.headers.get("content-length") or 0)
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            _json(self, 400, {"ok": False, "message": "invalid json"})
            return

        mode = str(payload.get("mode") or "").lower()
        if mode not in {"enable", "disable"}:
            _json(self, 400, {"ok": False, "message": "mode must be enable or disable"})
            return

        try:
            result = subprocess.run(
                [SCRIPT, mode],
                text=True,
                capture_output=True,
                timeout=190,
                check=False,
            )
        except Exception as exc:
            _json(self, 500, {"ok": False, "message": str(exc), "error": str(exc)})
            return

        ok = result.returncode == 0
        _json(
            self,
            200 if ok else 500,
            {
                "ok": ok,
                "message": "WARP switch completed." if ok else "WARP switch failed.",
                "returncode": result.returncode,
                "stdout": (result.stdout or "")[-4000:],
                "stderr": (result.stderr or "")[-4000:],
                "telegram_proxy_enabled": _read_proxy_enabled(),
            },
        )


def main() -> int:
    if not SECRET:
        raise SystemExit("WARP_CONTROL_SECRET is required")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"warp-control listening on {HOST}:{PORT}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
