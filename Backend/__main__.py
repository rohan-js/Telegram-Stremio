from asyncio import get_event_loop, sleep as asleep
import asyncio
import logging
from traceback import format_exc
from datetime import datetime, timezone
from pyrogram import idle
from pyrogram.errors import FloodWait
from Backend import __version__, db
from Backend.helper.pinger import ping
from Backend.logger import LOGGER
from Backend.fastapi import server
from Backend.helper.pyro import restart_notification, setup_bot_commands, notify_admin
from Backend.pyrofork.bot import Helper, StreamBot
from Backend.pyrofork.clients import initialize_clients

loop = get_event_loop()


async def _start_client_with_retry(client, label: str):
    while True:
        try:
            await client.start()
            return
        except FloodWait as e:
            wait_for = int(getattr(e, "value", 60)) + 5
            LOGGER.warning(f"{label} hit FloodWait. Retrying in {wait_for}s")
            await asleep(wait_for)

async def start_services():
    try:
        LOGGER.info(f"Initializing Telegram-Stremio v-{__version__}")

        await db.connect()

        LOGGER.info('Initializing Telegram-Stremio Web Server...')
        loop.create_task(server.serve())
        loop.create_task(ping())

        await _start_client_with_retry(StreamBot, "Bot Client")
        StreamBot.username = StreamBot.me.username
        LOGGER.info(f"Bot Client : [@{StreamBot.username}]")

        await _start_client_with_retry(Helper, "Helper Bot Client")
        Helper.username = Helper.me.username
        LOGGER.info(f"Helper Bot Client : [@{Helper.username}]")

        LOGGER.info("Initializing Multi Clients...")
        await initialize_clients()

        await asyncio.gather(
            setup_bot_commands(StreamBot),
            restart_notification(),
        )
        
        LOGGER.info("Telegram-Stremio Started Successfully!")
        await notify_admin(
            f"<b>Telegram-Stremio is online.</b>\n\nVersion: {__version__}\nTime: {datetime.now(timezone.utc).isoformat()}"
        )
        await idle()
    except Exception:
        error_text = format_exc()
        LOGGER.error("Error during startup:\n" + error_text)
        try:
            await notify_admin(f"<b>Telegram-Stremio startup error</b>\n<pre>{error_text[-3500:]}</pre>")
        except Exception:
            pass

async def stop_services():
    try:
        LOGGER.info("Stopping services...")

        pending_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in pending_tasks:
            task.cancel()
        
        await asyncio.gather(*pending_tasks, return_exceptions=True)

        try:
            await StreamBot.stop()
        except Exception:
            pass

        try:
            await Helper.stop()
        except Exception:
            pass

        await db.disconnect()
        
        LOGGER.info("Services stopped successfully.")
    except Exception:
        LOGGER.error("Error during shutdown:\n" + format_exc())

if __name__ == '__main__':
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        LOGGER.info('Service Stopping...')
    except Exception:
        LOGGER.error(format_exc())
    finally:
        loop.run_until_complete(stop_services())
        loop.stop()
        logging.shutdown()  
