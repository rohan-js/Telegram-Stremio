from asyncio import gather, create_task
from urllib.parse import urlparse, unquote
import re
from pyrogram import Client
from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.pyrofork.bot import multi_clients, work_loads, StreamBot, client_dc_map
from os import environ

class TokenParser:
    @staticmethod
    def parse_from_env():
        """client_id -> {"key": env name, "token": bot token}.

        Only MULTI_TOKEN<n> keys are tokens — the per-client overrides
        (MULTI_TOKEN<n>_PROXY) must not be mistaken for credentials.
        The env key is carried through so each client can carry its own
        proxy (egress diversity: some clients route via the Filelink relay
        while the rest keep the WARP netns path)."""
        entries = sorted(
            (key, value) for key, value in environ.items()
            if re.fullmatch(r"MULTI_TOKEN\d+", key)
        )
        return {
            index + 1: {"key": key, "token": value}
            for index, (key, value) in enumerate(entries)
        }


def parse_proxy_url(raw: str):
    """Parse socks5://user:pass@host:port into a pyrogram proxy dict."""
    raw = (raw or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    try:
        port = parsed.port
    except ValueError:
        LOGGER.warning(f"Ignoring invalid client proxy URL: {raw[:80]}")
        return None
    if parsed.scheme not in ("socks5", "socks4", "http") or not parsed.hostname or not port:
        LOGGER.warning(f"Ignoring invalid client proxy URL: {raw[:80]}")
        return None
    proxy = {"scheme": parsed.scheme, "hostname": parsed.hostname, "port": port}
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def client_proxy_for(env_key: str):
    """Per-client proxy override from MULTI_TOKEN<n>_PROXY (None = default)."""
    return parse_proxy_url(environ.get(f"{env_key}_PROXY", ""))

async def start_client(client_id, token_info):
    try:
        if isinstance(token_info, dict):
            token = token_info["token"]
            proxy = client_proxy_for(token_info.get("key", "")) or Telegram.telegram_proxy()
        else:
            token = token_info
            proxy = Telegram.telegram_proxy()
        LOGGER.info(f"Starting - Bot Client {client_id}")
        client = await Client(
            name=str(client_id),
            api_id=Telegram.API_ID,
            api_hash=Telegram.API_HASH,
            bot_token=token,
            proxy=proxy,
            sleep_threshold=100,
            no_updates=True,
            in_memory=True
        ).start()
        
        try:
            client_dc = await client.storage.dc_id()
            client_dc_map[client_id] = client_dc
            LOGGER.info(f"Client {client_id} connected to DC {client_dc}")
        except Exception as e:
            LOGGER.warning(f"Could not get DC for Client {client_id}: {e}")
            client_dc_map[client_id] = None
        
        work_loads[client_id] = 0
        return client_id, client
    except Exception as e:
        LOGGER.error(f"Failed to start Client - {client_id} Error: {e}", exc_info=True)
        return None

async def initialize_clients():
    multi_clients[0], work_loads[0] = StreamBot, 0
    
    try:
        main_dc = await StreamBot.storage.dc_id()
        client_dc_map[0] = main_dc
        LOGGER.info(f"Main StreamBot connected to DC {main_dc}")
    except Exception as e:
        LOGGER.warning(f"Could not get DC for StreamBot: {e}")
        client_dc_map[0] = None
    
    all_tokens = TokenParser.parse_from_env()
    if not all_tokens:
        LOGGER.info("No additional Bot Clients found, Using default client")
        return

    tasks = [create_task(start_client(i, info)) for i, info in all_tokens.items()]
    results = await gather(*tasks)
    clients = {
        client_id: client
        for result in results
        if result
        for client_id, client in [result]
        if client
    }
    multi_clients.update(clients)

    if len(multi_clients) != 1:
        LOGGER.info(f"Multi-Client Mode Enabled with {len(multi_clients)} clients")
        LOGGER.info(f"DC Distribution: {client_dc_map}")
    else:
        LOGGER.info("No additional clients were initialized, using default client")
