from __future__ import annotations

import asyncio
import shlex
import subprocess
from datetime import datetime
from typing import Dict

import httpx

from Backend.config import Telegram
from Backend.helper.custom_dl import ACTIVE_STREAMS

_LAST_RESULT: dict = {}


def _active_stream_count() -> int:
    return sum(1 for item in ACTIVE_STREAMS.values() if item.get("status", "active") == "active")


def _mode() -> str:
    return "WARP" if Telegram.TELEGRAM_PROXY_ENABLED else "Direct"


def get_warp_status() -> Dict:
    command = getattr(Telegram, "WARP_CONTROL_COMMAND", "") or ""
    url = getattr(Telegram, "WARP_CONTROL_URL", "") or ""
    return {
        "mode": _mode(),
        "telegram_proxy_enabled": bool(Telegram.TELEGRAM_PROXY_ENABLED),
        "proxy_scheme": Telegram.TELEGRAM_PROXY_SCHEME,
        "proxy_host": Telegram.TELEGRAM_PROXY_HOST,
        "proxy_port": Telegram.TELEGRAM_PROXY_PORT,
        "control_available": bool(command or url),
        "control_command_configured": bool(command),
        "control_url_configured": bool(url),
        "active_streams": _active_stream_count(),
        "last_result": _LAST_RESULT,
    }


async def apply_warp_mode(enable: bool, force: bool = False) -> Dict:
    active = _active_stream_count()
    if active and not force:
        return {
            "ok": False,
            "message": "Active streams are running. Use force only if interruption is acceptable.",
            "status": get_warp_status(),
        }

    command = getattr(Telegram, "WARP_CONTROL_COMMAND", "") or ""
    url = getattr(Telegram, "WARP_CONTROL_URL", "") or ""
    if not command and not url:
        return {
            "ok": False,
            "message": "WARP control helper is not configured on this deployment.",
            "status": get_warp_status(),
        }

    if url:
        return await _apply_via_control_url(url, enable)

    args = shlex.split(command) + (["enable"] if enable else ["disable"])

    def _run():
        return subprocess.run(args, text=True, capture_output=True, timeout=180)

    try:
        result = await asyncio.to_thread(_run)
        ok = result.returncode == 0
        _LAST_RESULT.clear()
        _LAST_RESULT.update(
            {
                "ok": ok,
                "mode_requested": "WARP" if enable else "Direct",
                "returncode": result.returncode,
                "stdout": (result.stdout or "")[-1200:],
                "stderr": (result.stderr or "")[-1200:],
                "at": datetime.utcnow().isoformat(),
            }
        )
        return {
            "ok": ok,
            "message": "WARP switch command completed." if ok else "WARP switch command failed.",
            "status": get_warp_status(),
            "result": dict(_LAST_RESULT),
        }
    except Exception as exc:
        _LAST_RESULT.clear()
        _LAST_RESULT.update(
            {
                "ok": False,
                "mode_requested": "WARP" if enable else "Direct",
                "error": str(exc),
                "at": datetime.utcnow().isoformat(),
            }
        )
        return {"ok": False, "message": str(exc), "status": get_warp_status(), "result": dict(_LAST_RESULT)}


async def _apply_via_control_url(url: str, enable: bool) -> Dict:
    headers = {}
    secret = getattr(Telegram, "WARP_CONTROL_SECRET", "") or ""
    if secret:
        headers["x-warp-control-secret"] = secret
    mode = "enable" if enable else "disable"
    try:
        async with httpx.AsyncClient(timeout=190.0) as client:
            response = await client.post(f"{url.rstrip('/')}/apply", json={"mode": mode}, headers=headers)
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        ok = response.status_code < 400 and bool(payload.get("ok", True))
        _LAST_RESULT.clear()
        _LAST_RESULT.update(
            {
                "ok": ok,
                "mode_requested": "WARP" if enable else "Direct",
                "status_code": response.status_code,
                "helper": "url",
                "stdout": str(payload.get("stdout") or "")[-1200:],
                "stderr": str(payload.get("stderr") or "")[-1200:],
                "error": payload.get("error"),
                "at": datetime.utcnow().isoformat(),
            }
        )
        return {
            "ok": ok,
            "message": payload.get("message") or ("WARP switch completed." if ok else "WARP switch failed."),
            "status": get_warp_status(),
            "result": dict(_LAST_RESULT),
        }
    except Exception as exc:
        _LAST_RESULT.clear()
        _LAST_RESULT.update(
            {
                "ok": False,
                "mode_requested": "WARP" if enable else "Direct",
                "helper": "url",
                "error": str(exc),
                "at": datetime.utcnow().isoformat(),
            }
        )
        return {"ok": False, "message": str(exc), "status": get_warp_status(), "result": dict(_LAST_RESULT)}
