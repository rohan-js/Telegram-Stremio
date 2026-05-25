import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from Backend.config import Telegram
from Backend.logger import LOGGER


_SAMPLE_LOCK = asyncio.Lock()


def parse_proc_net_dev(text: str) -> dict[str, dict[str, int]]:
    counters: dict[str, dict[str, int]] = {}
    for line in str(text or "").splitlines():
        if ":" not in line:
            continue
        iface, values = line.split(":", 1)
        parts = values.split()
        if len(parts) < 16:
            continue
        try:
            counters[iface.strip()] = {
                "rx_bytes": int(parts[0]),
                "tx_bytes": int(parts[8]),
            }
        except ValueError:
            continue
    return counters


def read_interface_tx_bytes(interface: str, net_dev_path: str | Path) -> Optional[int]:
    path = Path(net_dev_path)
    if not path.is_file():
        return None
    counters = parse_proc_net_dev(path.read_text(encoding="utf-8", errors="replace"))
    item = counters.get(str(interface or "").strip())
    if not item:
        return None
    return int(item["tx_bytes"])


def read_tx_bytes_counter(tx_bytes_path: str | Path) -> Optional[int]:
    path = Path(tx_bytes_path)
    if not path.is_file():
        return None
    return int(path.read_text(encoding="utf-8", errors="replace").strip())


def empty_vps_outbound_summary(status: str = "unavailable", error: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    month_limit = int(getattr(Telegram, "VPS_OUTBOUND_MONTHLY_LIMIT_BYTES", 10 * 1024 ** 4) or 10 * 1024 ** 4)
    summary = {
        "enabled": bool(getattr(Telegram, "VPS_OUTBOUND_ENABLED", True)),
        "status": status,
        "source": "host interface tx",
        "interface": getattr(Telegram, "VPS_OUTBOUND_INTERFACE", "ens3"),
        "today": {"date": now.strftime("%Y-%m-%d"), "bytes": 0},
        "month": {"month": now.strftime("%Y-%m"), "bytes": 0, "limit_bytes": month_limit, "percent": 0.0},
        "total": {"bytes": 0},
        "current_tx_bytes": None,
        "tracking_started_at": None,
        "last_sample_at": None,
        "reset_count": 0,
    }
    if error:
        summary["error"] = error
    return summary


def build_vps_outbound_sample(
    existing: Optional[dict],
    *,
    interface: str,
    current_tx_bytes: int,
    monthly_limit_bytes: int,
    now: Optional[datetime] = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    month = now.strftime("%Y-%m")
    current_tx_bytes = max(0, int(current_tx_bytes or 0))
    monthly_limit_bytes = max(1, int(monthly_limit_bytes or 1))

    if not existing:
        return {
            "_id": "vps_outbound_tx",
            "interface": interface,
            "last_tx_bytes": current_tx_bytes,
            "today": {"date": today, "bytes": 0},
            "month": {"month": month, "bytes": 0, "limit_bytes": monthly_limit_bytes, "percent": 0.0},
            "total": {"bytes": 0},
            "current_tx_bytes": current_tx_bytes,
            "tracking_started_at": now,
            "last_sample_at": now,
            "reset_count": 0,
        }

    last_tx = int(existing.get("last_tx_bytes") or 0)
    reset_count = int(existing.get("reset_count") or 0)
    if current_tx_bytes < last_tx:
        delta = 0
        reset_count += 1
    else:
        delta = current_tx_bytes - last_tx

    current_daily = existing.get("today") or {}
    current_monthly = existing.get("month") or {}
    daily_bytes = 0 if current_daily.get("date") != today else int(current_daily.get("bytes") or 0)
    monthly_bytes = 0 if current_monthly.get("month") != month else int(current_monthly.get("bytes") or 0)
    total_bytes = int((existing.get("total") or {}).get("bytes") or 0)

    daily_bytes += delta
    monthly_bytes += delta
    total_bytes += delta
    percent = min(999.0, (monthly_bytes / monthly_limit_bytes) * 100)

    return {
        "_id": "vps_outbound_tx",
        "interface": interface,
        "last_tx_bytes": current_tx_bytes,
        "today": {"date": today, "bytes": daily_bytes},
        "month": {
            "month": month,
            "bytes": monthly_bytes,
            "limit_bytes": monthly_limit_bytes,
            "percent": round(percent, 3),
        },
        "total": {"bytes": total_bytes},
        "current_tx_bytes": current_tx_bytes,
        "tracking_started_at": existing.get("tracking_started_at") or now,
        "last_sample_at": now,
        "reset_count": reset_count,
    }


async def get_vps_outbound_summary(db, force: bool = False) -> dict:
    if not getattr(Telegram, "VPS_OUTBOUND_ENABLED", True):
        return empty_vps_outbound_summary(status="disabled")

    interface = getattr(Telegram, "VPS_OUTBOUND_INTERFACE", "ens3")
    tx_bytes_path = getattr(Telegram, "VPS_OUTBOUND_TX_BYTES_PATH", "")
    net_dev_path = getattr(Telegram, "VPS_OUTBOUND_NET_DEV_PATH", "/host/proc/net/dev")
    month_limit = int(getattr(Telegram, "VPS_OUTBOUND_MONTHLY_LIMIT_BYTES", 10 * 1024 ** 4) or 10 * 1024 ** 4)

    try:
        tx_bytes = read_tx_bytes_counter(tx_bytes_path) if tx_bytes_path else None
        if tx_bytes is None:
            tx_bytes = read_interface_tx_bytes(interface, net_dev_path)
    except OSError as exc:
        LOGGER.warning("Unable to read VPS outbound interface counter: %s", exc)
        return empty_vps_outbound_summary(error=str(exc))
    except ValueError as exc:
        LOGGER.warning("Invalid VPS outbound interface counter: %s", exc)
        return empty_vps_outbound_summary(error=str(exc))

    if tx_bytes is None:
        return empty_vps_outbound_summary(error=f"Interface {interface} counter was not found.")

    async with _SAMPLE_LOCK:
        return await db.record_vps_outbound_sample(
            interface=interface,
            current_tx_bytes=tx_bytes,
            monthly_limit_bytes=month_limit,
            force=force,
        )
