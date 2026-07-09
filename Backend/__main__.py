from asyncio import get_event_loop, sleep as asleep
import asyncio
import logging
from traceback import format_exc
from pyrogram import idle
from Backend import __version__, db
from Backend.helper.pinger import ping
from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.fastapi import server
from Backend.helper.pyro import restart_notification, setup_bot_commands
from Backend.helper.settings_manager import SettingsManager
from Backend.pyrofork.bot import Helper, StreamBot, USERBOT_CLIENT_INDEX, Userbot, client_dc_map, client_failures, client_avg_mbps, work_loads
from Backend.pyrofork.clients import initialize_clients
from Backend.pyrofork.plugins.channels import _load_channels_from_db
from Backend.helper.subscription_checker import subscription_checker_loop
from Backend.helper.link_checker import DeadLinkChecker
from Backend.helper.torrent_downloads import TORRENT_DOWNLOAD_MANAGER
from Backend.helper.auto_catalog import (
    AUTO_CATALOG_FULL_REBUILD_ON_STARTUP,
    AUTO_CATALOG_ON_STARTUP,
    AUTO_SYNC_DELAY_SECONDS,
    start_auto_catalog_interval_loop,
    start_auto_catalog_sync_background,
)
from Backend.helper.iptv import (
    start_iptv_interval_loop,
    start_iptv_sync_background,
)
from Backend.helper.scan_manager import scan_manager
from Backend.fastapi.main import app
from Backend.helper.owner_alerts import schedule_owner_alert
from Backend.helper.production_ops import start_backup_loop


loop = get_event_loop()

async def start_telegram_services():
    while True:
        try:
            await asyncio.wait_for(StreamBot.start(), timeout=Telegram.TELEGRAM_CLIENT_START_TIMEOUT_SEC)
            StreamBot.username = StreamBot.me.username
            LOGGER.info(f"Bot Client : [@{StreamBot.username}]")
            await asleep(1.2)

            await asyncio.wait_for(Helper.start(), timeout=Telegram.TELEGRAM_CLIENT_START_TIMEOUT_SEC)
            Helper.username = Helper.me.username
            LOGGER.info(f"Helper Bot Client : [@{Helper.username}]")
            await asleep(1.2)

            if Userbot is not None:
                await asyncio.wait_for(Userbot.start(), timeout=Telegram.TELEGRAM_CLIENT_START_TIMEOUT_SEC)
                Userbot.username = Userbot.me.username
                try:
                    client_dc_map[USERBOT_CLIENT_INDEX] = await Userbot.storage.dc_id()
                except Exception:
                    client_dc_map[USERBOT_CLIENT_INDEX] = None
                work_loads[USERBOT_CLIENT_INDEX] = 0
                client_failures[USERBOT_CLIENT_INDEX] = 0
                client_avg_mbps[USERBOT_CLIENT_INDEX] = 0.0
                LOGGER.info(f"Userbot Client : [@{Userbot.username}]")
            else:
                LOGGER.info("Userbot not configured (USER_SESSION_STRING empty); Global Search disabled.")
            await asleep(1.2)

            LOGGER.info("Initializing Multi Clients...")
            await initialize_clients()
            await asleep(2)

            await _load_channels_from_db()
            await asleep(2)

            await setup_bot_commands(StreamBot)
            await asleep(2)

            if Telegram.TORRENT_DOWNLOADS_ENABLED:
                await TORRENT_DOWNLOAD_MANAGER.start()

            await restart_notification()

            if Telegram.SUBSCRIPTION:
                loop.create_task(subscription_checker_loop(StreamBot))
                LOGGER.info("Subscription Checker Task Started.")

            LOGGER.info("Telegram clients started successfully.")
            schedule_owner_alert(
                f"Telegram-Stremio v{__version__} started successfully.",
                key="app-started",
                cooldown_sec=300,
            )
            return
        except Exception:
            LOGGER.error("Telegram client startup failed; retrying in 60 seconds:\n" + format_exc())
            for client in (StreamBot, Helper, Userbot):
                try:
                    if client is not None and getattr(client, "is_connected", False):
                        await client.stop()
                except Exception:
                    LOGGER.warning("Ignoring Telegram client cleanup error during retry.", exc_info=True)
            await asleep(60)


async def start_services():
    try:
        LOGGER.info(f"Initializing Telegram-Stremio v-{__version__}")
        await asleep(1.2)
        
        await db.connect()
        await asleep(1.2)

        await SettingsManager.initialize(db)
        await asleep(0.3)

        await scan_manager.load(db)
        await asleep(0.2)

        LOGGER.info("Initializing Telegram-Stremio Web Server...")
        loop.create_task(server.serve())
        loop.create_task(ping())
        
        link_checker_task = DeadLinkChecker(db, app, check_interval_hours=24)
        loop.create_task(link_checker_task.start())

        if Telegram.BACKUP_ENABLED:
            loop.create_task(start_backup_loop(db))
            LOGGER.info("Production backup loop started.")

        if AUTO_CATALOG_ON_STARTUP:
            loop.create_task(start_auto_catalog_sync_background(
                db,
                delay_seconds=AUTO_SYNC_DELAY_SECONDS,
                full_rebuild=AUTO_CATALOG_FULL_REBUILD_ON_STARTUP,
            ))

        loop.create_task(start_auto_catalog_interval_loop(db))

        if Telegram.IPTV_ENABLED and Telegram.IPTV_AUTO_SYNC:
            loop.create_task(start_iptv_sync_background(
                db,
                force=False,
                delay_seconds=Telegram.IPTV_SYNC_START_DELAY_SECONDS,
            ))
            loop.create_task(start_iptv_interval_loop(db))

        loop.create_task(start_telegram_services())
        
        LOGGER.info("Telegram-Stremio Started Successfully!")
        await idle()
    except Exception:
        LOGGER.error("Error during startup:\n" + format_exc())

async def stop_services():
    try:
        LOGGER.info("Stopping services...")

        pending_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in pending_tasks:
            task.cancel()
        
        await asyncio.gather(*pending_tasks, return_exceptions=True)

        await StreamBot.stop()
        await Helper.stop()
        if Userbot is not None:
            await Userbot.stop()

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
