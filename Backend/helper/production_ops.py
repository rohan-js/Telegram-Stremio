from __future__ import annotations

import asyncio
import gzip
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from Backend.config import Telegram
from Backend.helper.beta_access import beta_enabled, invited_user_ids
from Backend.helper.host_outbound import get_vps_outbound_summary
from Backend.helper.nginx_egress import get_nginx_egress_summary
from Backend.helper.warp_control import get_warp_status
from Backend.logger import LOGGER


def _json_default(value: Any):
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _meminfo() -> dict:
    result = {"available_mb": None, "total_mb": None, "swap_free_mb": None, "swap_total_mb": None}
    try:
        values = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                key, raw = line.split(":", 1)
                values[key] = int(raw.strip().split()[0]) // 1024
        result.update(
            {
                "available_mb": values.get("MemAvailable"),
                "total_mb": values.get("MemTotal"),
                "swap_free_mb": values.get("SwapFree"),
                "swap_total_mb": values.get("SwapTotal"),
            }
        )
    except Exception:
        pass
    return result


def _diskinfo(path: str = ".") -> dict:
    try:
        usage = shutil.disk_usage(path)
        return {
            "total_gb": round(usage.total / 1024 ** 3, 2),
            "free_gb": round(usage.free / 1024 ** 3, 2),
            "used_percent": round((usage.used / usage.total) * 100, 2) if usage.total else 0,
        }
    except Exception:
        return {"total_gb": None, "free_gb": None, "used_percent": None}


async def cleanup_old_diagnostics(db) -> dict:
    now = datetime.utcnow()
    stream_days = max(1, int(getattr(Telegram, "STREAM_LOG_RETENTION_DAYS", 30) or 30))
    billing_days = max(1, int(getattr(Telegram, "BILLING_LOG_RETENTION_DAYS", 180) or 180))
    deleted = {}
    try:
        result = await db.dbs["tracking"]["stream_analytics"].delete_many(
            {"logged_at": {"$lt": now - timedelta(days=stream_days)}}
        )
        deleted["stream_analytics"] = result.deleted_count
    except Exception as exc:
        LOGGER.debug("stream_analytics cleanup skipped: %s", exc)
    try:
        result = await db.dbs["tracking"]["watch_link_requests"].delete_many(
            {"clicked_at": {"$lt": now - timedelta(days=stream_days)}}
        )
        deleted["watch_link_requests"] = result.deleted_count
    except Exception as exc:
        LOGGER.debug("watch request cleanup skipped: %s", exc)
    try:
        result = await db.dbs["tracking"]["payment_audit"].delete_many(
            {"created_at": {"$lt": now - timedelta(days=billing_days)}}
        )
        deleted["payment_audit"] = result.deleted_count
    except Exception:
        pass
    return deleted


async def create_tracking_backup(db, *, reason: str = "scheduled") -> dict:
    if not getattr(Telegram, "BACKUP_ENABLED", True):
        return {"ok": False, "message": "Backups disabled"}
    backup_dir = Path(getattr(Telegram, "BACKUP_DIR", "backups/production") or "backups/production")
    backup_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.utcnow()
    stamp = started_at.strftime("%Y%m%d_%H%M%S")
    archive_path = backup_dir / f"tracking_{stamp}.jsonl.gz"
    collection_counts = {}
    try:
        names = await db.dbs["tracking"].list_collection_names()
        with gzip.open(archive_path, "wt", encoding="utf-8") as fh:
            for name in sorted(names):
                count = 0
                async for doc in db.dbs["tracking"][name].find({}):
                    payload = {"collection": name, "document": doc}
                    fh.write(json.dumps(payload, default=_json_default, ensure_ascii=False) + "\n")
                    count += 1
                collection_counts[name] = count
        finished_at = datetime.utcnow()
        status = {
            "ok": True,
            "reason": reason,
            "path": str(archive_path),
            "collections": collection_counts,
            "started_at": started_at,
            "finished_at": finished_at,
            "size_bytes": archive_path.stat().st_size,
        }
        await db.dbs["tracking"]["state"].update_one(
            {"_id": "production_backup_status"},
            {"$set": status},
            upsert=True,
        )
        return status
    except Exception as exc:
        status = {
            "ok": False,
            "reason": reason,
            "path": str(archive_path),
            "started_at": started_at,
            "finished_at": datetime.utcnow(),
            "error": str(exc),
        }
        await db.dbs["tracking"]["state"].update_one(
            {"_id": "production_backup_status"},
            {"$set": status},
            upsert=True,
        )
        LOGGER.exception("Tracking backup failed")
        return status


async def get_backup_status(db) -> dict:
    try:
        doc = await db.dbs["tracking"]["state"].find_one({"_id": "production_backup_status"}) or {}
        doc.pop("_id", None)
        return doc
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def start_backup_loop(db) -> None:
    if not getattr(Telegram, "BACKUP_ENABLED", True):
        LOGGER.info("Production backup loop disabled.")
        return
    interval = max(1, int(getattr(Telegram, "BACKUP_INTERVAL_HOURS", 24) or 24)) * 3600
    await asyncio.sleep(60)
    while True:
        try:
            await cleanup_old_diagnostics(db)
            await create_tracking_backup(db, reason="scheduled")
        except asyncio.CancelledError:
            break
        except Exception:
            LOGGER.exception("Production backup loop failed")
        await asyncio.sleep(interval)


async def get_launch_readiness(db) -> dict:
    from Backend.helper.custom_dl import ACTIVE_STREAMS

    tokens = await db.get_all_api_tokens()
    active_streams = [
        info for info in ACTIVE_STREAMS.values()
        if info.get("status", "active") == "active"
    ]
    backup = await get_backup_status(db)
    disk = _diskinfo(".")
    mem = _meminfo()
    checks = {
        "subscription_enabled": bool(Telegram.SUBSCRIPTION),
        "shared_default_token_configured": bool(Telegram.DEFAULT_ADDON_TOKEN),
        "public_beta_enabled": beta_enabled(),
        "invite_only": bool(getattr(Telegram, "BETA_INVITE_ONLY", True)),
        "invited_users": len(invited_user_ids()),
        "api_tokens": len(tokens),
        "active_streams": len(active_streams),
        "global_stream_limit": int(getattr(Telegram, "MAX_ACTIVE_STREAMS_GLOBAL", 4) or 4),
        "backup_ok": bool(backup.get("ok")),
        "memory_available_mb": mem.get("available_mb"),
        "swap_free_mb": mem.get("swap_free_mb"),
        "disk_free_gb": disk.get("free_gb"),
        "warp": get_warp_status(),
        "egress": get_nginx_egress_summary(),
        "vps_outbound": await get_vps_outbound_summary(db),
    }
    warnings = []
    if not checks["subscription_enabled"]:
        warnings.append("Subscription mode is disabled.")
    if checks["shared_default_token_configured"]:
        warnings.append("DEFAULT_ADDON_TOKEN is configured; avoid sharing it for paid beta.")
    if not checks["backup_ok"]:
        warnings.append("No successful production backup recorded.")
    if mem.get("available_mb") is not None and mem["available_mb"] < 150:
        warnings.append("Available memory is low.")
    if disk.get("free_gb") is not None and disk["free_gb"] < 5:
        warnings.append("Disk free space is low.")
    return {
        "status": "ready" if not warnings else "needs_attention",
        "checks": checks,
        "warnings": warnings,
        "backup": backup,
        "generated_at": datetime.utcnow(),
    }
