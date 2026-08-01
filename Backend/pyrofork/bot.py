from pyrogram import Client
from Backend.config import Telegram

StreamBot = Client(
    name='bot',
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN,
    plugins={"root": "Backend/pyrofork/plugins"},
    proxy=Telegram.telegram_proxy(),
    sleep_threshold=20,
    workers=6,
    max_concurrent_transmissions=10
)

Helper = Client(
    "helper",
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.HELPER_BOT_TOKEN,
    proxy=Telegram.telegram_proxy(),
    sleep_threshold=20,
    workers=6,
    max_concurrent_transmissions=10
)

USERBOT_CLIENT_INDEX = -1


#----- Build (and register) the Userbot client from a session string at runtime
def build_userbot(session_string: str):
    global Userbot
    Userbot = Client(
        name="userbot",
        api_id=Telegram.API_ID,
        api_hash=Telegram.API_HASH,
        session_string=session_string,
        proxy=Telegram.telegram_proxy(),
        sleep_threshold=20,
        workers=6,
        max_concurrent_transmissions=10,
        no_updates=True,
        in_memory=True,
    )
    work_loads[USERBOT_CLIENT_INDEX] = 0
    client_failures[USERBOT_CLIENT_INDEX] = 0
    client_avg_mbps[USERBOT_CLIENT_INDEX] = 0.0
    return Userbot


Userbot = None
if Telegram.USER_SESSION_STRING:
    Userbot = Client(
        name="userbot",
        api_id=Telegram.API_ID,
        api_hash=Telegram.API_HASH,
        session_string=Telegram.USER_SESSION_STRING,
        proxy=Telegram.telegram_proxy(),
        sleep_threshold=20,
        workers=6,
        max_concurrent_transmissions=10,
        no_updates=True,
    )

multi_clients = {}
work_loads = {}
client_dc_map = {}
client_failures = {}  
client_avg_mbps = {}
client_cooldowns = {}
client_dc_cooldowns = {}
client_last_errors = {}
