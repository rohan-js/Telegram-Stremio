import asyncio
import time
from datetime import datetime

import httpx

from Backend import db
from Backend.config import Telegram
from Backend.helper.production_ops import get_launch_readiness
from Backend.helper.settings_manager import SettingsManager
from Backend.pyrofork.bot import StreamBot, client_dc_map, client_failures, multi_clients, work_loads

_CACHE = {}
_TTL = {"db": 30, "base": 60}


async def _cached(key: str, producer, force: bool = False):
    now = time.monotonic()
    cached = _CACHE.get(key)
    if not force and cached and now - cached[0] < _TTL[key]:
        return cached[1]
    value = await producer()
    _CACHE[key] = (now, value)
    return value


async def _db_health() -> dict:
    items = []
    for name, database in db.dbs.items():
        entry = {"name": name, "status": "down", "message": ""}
        try:
            await asyncio.wait_for(database.command("ping"), timeout=5)
            entry["status"] = "ok"
        except Exception as exc:
            entry["message"] = str(exc)[:160]
        items.append(entry)
    up = sum(1 for item in items if item["status"] == "ok")
    return {"key": "databases", "status": "ok" if up == len(items) else "degraded", "up": up, "total": len(items), "items": items}


def _bot_health() -> dict:
    clients = []
    for index in sorted(multi_clients.keys()):
        client = multi_clients[index]
        clients.append(
            {
                "name": "Userbot" if index < 0 else f"Bot {index + 1}",
                "connected": bool(getattr(client, "is_connected", False)),
                "dc": client_dc_map.get(index),
                "workload": work_loads.get(index, 0),
                "failures": client_failures.get(index, 0),
            }
        )
    up = sum(1 for item in clients if item["connected"])
    return {
        "key": "telegram_clients",
        "status": "ok" if clients and up == len(clients) else "degraded",
        "up": up,
        "total": len(clients),
        "primary_connected": bool(getattr(StreamBot, "is_connected", False)),
        "items": clients,
    }


async def _base_url_health() -> dict:
    base = getattr(SettingsManager.current(), "base_url", "") or Telegram.BASE_URL
    if not base:
        return {"key": "base_url", "status": "not_configured", "message": "BASE_URL is empty."}
    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            response = await client.get(base)
        return {"key": "base_url", "status": "ok", "message": f"HTTP {response.status_code}", "url": base}
    except Exception as exc:
        return {"key": "base_url", "status": "error", "message": str(exc)[:160], "url": base}


async def run_health_checks(force: bool = False) -> dict:
    db_section = await _cached("db", _db_health, force)
    base_section = await _cached("base", _base_url_health, force)
    bots = _bot_health()
    readiness = await get_launch_readiness(db)
    sections = [db_section, bots, base_section]
    statuses = [section.get("status") for section in sections]
    if any(status in ("down", "error") for status in statuses):
        overall = "critical"
    elif any(status in ("degraded", "warning", "not_configured") for status in statuses) or readiness.get("warnings"):
        overall = "warning"
    else:
        overall = "ok"
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "overall": overall,
        "sections": sections,
        "launch_readiness": readiness,
    }
