import asyncio
import re
import time
from typing import Dict, List, Optional

import PTN
from pyrogram import enums
from pyrogram.errors import (
    AuthKeyUnregistered,
    ChannelPrivate,
    ChatAdminRequired,
    FloodWait,
    PeerIdInvalid,
    RPCError,
    SessionRevoked,
    UserNotParticipant,
)

from Backend.helper.encrypt import encode_string
from Backend.helper.pyro import get_readable_file_size
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER
from Backend.pyrofork.bot import Userbot

MAX_RESULTS = 50
MAX_RESULTS_PER_CHAT = 50
SEARCH_COOLDOWN_SECONDS = 5
MAX_CONCURRENT_SEARCHES = 3
MIN_TITLE_SCORE = 0.6

_last_search_ts: Dict[str, float] = {}
_inflight_queries: set = set()
_search_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEARCHES)
_userbot_session_dead = False
_chat_title_cache: Dict[int, str] = {}

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MULTIPART_RE = re.compile(r"(?:part|cd|disc|disk)[s._-]*\d+(?=\.\w+$)", re.IGNORECASE)
_VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".ts", ".m4v", ".mov", ".wmv", ".webm", ".flv")


def is_userbot_available() -> bool:
    return Userbot is not None and not _userbot_session_dead


def is_global_search_enabled() -> bool:
    if not is_userbot_available():
        return False
    settings = SettingsManager.current()
    return bool(settings.global_search and settings.global_search_channels)


def _tokens(value: str) -> set:
    return set(_TOKEN_RE.findall((value or "").lower()))


def _title_score(result_title: str, expected_title: str) -> float:
    expected = _tokens(expected_title)
    return len(expected & _tokens(result_title)) / len(expected) if expected else 0.0


def _matches_episode(parsed: dict, season: Optional[int], episode: Optional[int]) -> bool:
    for expected, key in ((season, "season"), (episode, "episode")):
        if expected is None:
            continue
        actual = parsed.get(key)
        if actual is None:
            continue
        if isinstance(actual, list):
            if int(expected) not in [int(x) for x in actual]:
                return False
        elif int(actual) != int(expected):
            return False
    return True


def _parse_and_validate(filename: str, expected_title: str, season: Optional[int], episode: Optional[int]) -> Optional[dict]:
    if _MULTIPART_RE.search(filename or ""):
        return None
    try:
        parsed = PTN.parse(filename)
    except Exception:
        return None
    if "excess" in parsed and any("combined" in str(item).lower() for item in parsed["excess"]):
        return None
    if not _matches_episode(parsed, season, episode):
        return None
    if _title_score(parsed.get("title", ""), expected_title) < MIN_TITLE_SCORE:
        return None
    return parsed


def _video_filename(message) -> Optional[str]:
    if getattr(message, "video", None):
        return (message.caption or "").strip() or getattr(message.video, "file_name", None) or "video.mkv"
    if getattr(message, "document", None):
        mime = message.document.mime_type or ""
        name = message.document.file_name
        if mime.startswith("video/") or (name and name.lower().endswith(_VIDEO_EXTS)):
            return (message.caption or "").strip() or name or "video.mkv"
    return None


def _resolve_channel_ids(channel_ids: List[str]) -> List[int]:
    resolved: List[int] = []
    seen: set = set()
    for channel in channel_ids:
        try:
            raw = int(str(channel).strip())
        except ValueError:
            continue
        canonical = raw if raw < 0 else int(f"-100{raw}")
        if canonical not in seen:
            seen.add(canonical)
            resolved.append(canonical)
    return resolved


async def _get_chat_title(client, chat_id: int) -> str:
    if chat_id in _chat_title_cache:
        return _chat_title_cache[chat_id]
    try:
        chat = await client.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)
    _chat_title_cache[chat_id] = title
    return title


async def _search_channel(
    client,
    chat_id: int,
    chat_title: str,
    search_query: str,
    expected_title: str,
    season: Optional[int],
    episode: Optional[int],
) -> List[Dict]:
    global _userbot_session_dead
    results: List[Dict] = []
    seen_msg_ids: set = set()

    for msg_filter in (enums.MessagesFilter.VIDEO, enums.MessagesFilter.DOCUMENT):
        if len(results) >= MAX_RESULTS_PER_CHAT:
            break
        try:
            async for message in client.search_messages(
                chat_id=chat_id,
                query=search_query,
                filter=msg_filter,
                limit=MAX_RESULTS_PER_CHAT,
            ):
                if message.id in seen_msg_ids:
                    continue
                seen_msg_ids.add(message.id)

                filename = _video_filename(message)
                if not filename:
                    continue
                parsed = _parse_and_validate(filename, expected_title, season, episode)
                if parsed is None:
                    continue

                media = message.video or message.document
                size = get_readable_file_size(getattr(media, "file_size", 0) or 0)
                quality = parsed.get("resolution") or "HD"
                token = await encode_string(
                    {
                        "global": True,
                        "chat_id": chat_id,
                        "msg_id": message.id,
                        "title": filename,
                        "size": size,
                        "quality": quality,
                        "source": chat_title,
                    }
                )
                results.append(
                    {
                        "token": token,
                        "title": filename,
                        "size": size,
                        "source_chat": chat_title,
                        "quality": quality,
                    }
                )
                if len(results) >= MAX_RESULTS_PER_CHAT:
                    break
        except FloodWait as e:
            LOGGER.warning("[USERBOT] FloodWait for %s: sleeping %ss", chat_title, e.value)
            await asyncio.sleep(e.value)
        except (ChatAdminRequired, ChannelPrivate, PeerIdInvalid, UserNotParticipant) as e:
            LOGGER.warning("[USERBOT] Cannot access channel %s: %s", chat_title, type(e).__name__)
            break
        except (AuthKeyUnregistered, SessionRevoked) as e:
            LOGGER.error("[USERBOT] Session invalid (%s): %s", type(e).__name__, e)
            _userbot_session_dead = True
            break
        except RPCError as e:
            LOGGER.warning("[USERBOT] RPC error in %s (%s): %s", chat_title, msg_filter, e)

    return results


async def global_search(
    expected_title: str,
    auth_channels: List[str],
    *,
    year: Optional[int] = None,
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> List[Dict]:
    expected_title = (expected_title or "").strip()
    if not expected_title or not is_global_search_enabled():
        return []

    settings = SettingsManager.current()
    target_ids = _resolve_channel_ids(settings.global_search_channels)
    if not target_ids:
        return []

    if season is not None and episode is not None:
        search_query = f"{expected_title} S{int(season):02d}E{int(episode):02d}"
    elif year is not None:
        search_query = f"{expected_title} {year}"
    else:
        search_query = expected_title

    key = search_query.lower()
    now = time.time()
    if now - _last_search_ts.get(key, 0) < SEARCH_COOLDOWN_SECONDS:
        return []
    if key in _inflight_queries:
        return []

    _inflight_queries.add(key)
    _last_search_ts[key] = now
    try:
        async with _search_semaphore:
            LOGGER.info("[USERBOT] Search started: %r across %s channel(s)", search_query, len(target_ids))
            chat_titles = await asyncio.gather(
                *(_get_chat_title(Userbot, cid) for cid in target_ids),
                return_exceptions=True,
            )
            tasks = []
            for cid, title in zip(target_ids, chat_titles):
                if _userbot_session_dead:
                    break
                tasks.append(_search_channel(Userbot, cid, str(title if not isinstance(title, Exception) else cid), search_query, expected_title, season, episode))
            per_channel_results = await asyncio.gather(*tasks, return_exceptions=True)
            results: List[Dict] = []
            for item in per_channel_results:
                if isinstance(item, list):
                    results.extend(item)
            return results[:MAX_RESULTS]
    finally:
        _inflight_queries.discard(key)
