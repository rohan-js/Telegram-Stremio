import asyncio
import time
from typing import Optional

from Backend.config import Telegram
from Backend.logger import LOGGER

_LAST_ALERT_AT: dict[str, float] = {}


async def send_owner_alert(message: str, *, key: Optional[str] = None, cooldown_sec: int = 300) -> bool:
    if not getattr(Telegram, "OWNER_ALERTS_ENABLED", True):
        return False
    owner_id = int(getattr(Telegram, "OWNER_ID", 0) or 0)
    if not owner_id:
        return False
    alert_key = key or message[:80]
    now = time.time()
    if now - _LAST_ALERT_AT.get(alert_key, 0) < cooldown_sec:
        return False
    _LAST_ALERT_AT[alert_key] = now
    try:
        from Backend.pyrofork.bot import Helper, StreamBot

        client = Helper if getattr(Helper, "is_connected", False) else StreamBot
        if not getattr(client, "is_connected", False):
            return False
        await client.send_message(owner_id, message)
        return True
    except Exception as exc:
        LOGGER.debug("Owner alert failed: %s", exc)
        return False


def schedule_owner_alert(message: str, *, key: Optional[str] = None, cooldown_sec: int = 300) -> None:
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_owner_alert(message, key=key, cooldown_sec=cooldown_sec))
    except RuntimeError:
        pass
