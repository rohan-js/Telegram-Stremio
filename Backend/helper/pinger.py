import asyncio
import traceback
import aiohttp
from Backend.config import Telegram  
from Backend.logger import LOGGER

async def ping():

    sleep_time = 1200
    # Internal health ping should not depend on external DNS.
    manifest_url = f"http://127.0.0.1:{Telegram.PORT}/api/system/stats"

    while True:
        await asyncio.sleep(sleep_time)
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.get(manifest_url) as resp:
                    LOGGER.info(f"Pinged manifest URL — Status: {resp.status}")
        except asyncio.TimeoutError:
            LOGGER.warning("Timeout: Could not connect to manifest URL.")
        except Exception:
            LOGGER.error("Ping failed:\n" + traceback.format_exc())
