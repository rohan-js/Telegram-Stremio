from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from pyrogram.errors import ChatAdminRequired, ChannelPrivate, FloodWait

from Backend.config import Telegram
from Backend.helper.encrypt import decode_string, encode_string
from Backend.helper.metadata import extract_default_id, metadata
from Backend.helper.pyro import clean_filename, finalize_media_name, get_readable_file_size
from Backend.helper.split_files import parse_split_info
from Backend.helper.announcer import announce_new_media
from Backend.helper.requests_manager import mark_uploaded_for_media
from Backend.helper.subtitles import ingest_subtitle, is_subtitle_file
from Backend.logger import LOGGER

SCAN_BATCH_SIZE = 200
SCAN_MAX_EMPTY_BATCHES = 10
SCAN_MAX_ID_CAP = 1_000_000
SCAN_BATCH_DELAY = 0.4

_STATE_COLLECTION = "scan_state"
_SCAN_DOC_ID = "scan"


def _now() -> float:
    return time.time()


def _elapsed(seconds: float) -> str:
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


class ScanManager:
    def __init__(self) -> None:
        self._db = None
        self._task: Optional[asyncio.Task] = None
        self._cancel = False
        self._lock = asyncio.Lock()
        self.state = self._blank_state()

    @staticmethod
    def _blank_state() -> Dict[str, Any]:
        return {
            "status": "idle",
            "mode": "scan",
            "selected_channels": [],
            "pending": [],
            "current_channel": None,
            "current_channel_name": "",
            "current_id": 0,
            "current_target_id": 0,
            "cursors": {},
            "counters": {
                "total_found": 0,
                "processed": 0,
                "indexed": 0,
                "skipped_dup": 0,
                "skipped_meta": 0,
                "skipped_nonvid": 0,
                "subtitles_added": 0,
                "subtitles_skipped": 0,
                "errors": 0,
            },
            "started_at": 0.0,
            "updated_at": 0.0,
            "finished_at": 0.0,
            "error": None,
        }

    async def load(self, db) -> None:
        self._db = db
        doc = await db.dbs["tracking"][_STATE_COLLECTION].find_one({"_id": _SCAN_DOC_ID})
        if doc:
            doc.pop("_id", None)
            state = self._blank_state()
            state.update(doc)
            if state.get("status") == "running":
                state["status"] = "paused"
            self.state = state
            await self._persist()

    async def _persist(self) -> None:
        if self._db is None:
            return
        self.state["updated_at"] = _now()
        doc = dict(self.state)
        doc["_id"] = _SCAN_DOC_ID
        await self._db.dbs["tracking"][_STATE_COLLECTION].update_one(
            {"_id": _SCAN_DOC_ID},
            {"$set": doc},
            upsert=True,
        )

    def get_status(self) -> Dict[str, Any]:
        started = float(self.state.get("started_at") or 0)
        end = float(self.state.get("finished_at") or _now())
        elapsed = max(0.0, end - started) if started else 0.0
        current = int(self.state.get("current_id") or 0)
        target = int(self.state.get("current_target_id") or 0)
        progress = max(0, min(100, round(current / target * 100))) if target else 0
        return {
            **self.state,
            "is_running": self.state.get("status") == "running",
            "resumable": self.state.get("status") in {"paused", "cancelled"} and bool(self.state.get("pending")),
            "elapsed": _elapsed(elapsed),
            "progress": progress,
            "has_progress": bool(target),
        }

    async def start(self, client, channels: List[str], mode: str = "scan") -> Dict[str, Any]:
        async with self._lock:
            if self.state.get("status") == "running":
                return {"ok": False, "message": "A scan is already running.", "status": self.get_status()}
            channels = [str(c).strip() for c in (channels or []) if str(c).strip()]
            if not channels:
                channels = list(Telegram.AUTH_CHANNEL)
            if not channels:
                return {"ok": False, "message": "No channels selected.", "status": self.get_status()}

            if mode == "rescan":
                for channel in channels:
                    try:
                        await self._purge_channel_entries(int(str(channel).replace("-100", "")))
                    except Exception as exc:
                        LOGGER.warning("[ScanManager] purge failed for %s: %s", channel, exc)
                self.state = self._blank_state()
            elif self.state.get("status") not in {"paused", "cancelled"}:
                self.state = self._blank_state()

            self.state["mode"] = mode
            self.state["status"] = "running"
            self.state["selected_channels"] = channels
            self.state["pending"] = channels
            self.state["started_at"] = _now()
            self.state["finished_at"] = 0.0
            self.state["error"] = None
            self._cancel = False
            await self._persist()
            self._task = asyncio.create_task(self._run(client))
            return {"ok": True, "message": f"{mode.title()} started.", "status": self.get_status()}

    async def cancel(self) -> Dict[str, Any]:
        if self.state.get("status") != "running":
            return {"ok": False, "message": "No scan is running.", "status": self.get_status()}
        self._cancel = True
        return {"ok": True, "message": "Cancel requested.", "status": self.get_status()}

    async def _run(self, client) -> None:
        try:
            while self.state.get("pending") and not self._cancel:
                channel = self.state["pending"][0]
                completed = await self._scan_channel(client, int(channel))
                if completed and self.state.get("pending") and self.state["pending"][0] == channel:
                    self.state["pending"].pop(0)
                await self._persist()
            self.state["status"] = "cancelled" if self._cancel else "completed"
            self.state["finished_at"] = _now()
            await self._persist()
        except (ChannelPrivate, ChatAdminRequired) as exc:
            self.state["status"] = "error"
            self.state["error"] = f"Access denied: {exc}"
            self.state["finished_at"] = _now()
            await self._persist()
        except Exception as exc:
            LOGGER.exception("[ScanManager] unexpected error")
            self.state["status"] = "error"
            self.state["error"] = str(exc)
            self.state["finished_at"] = _now()
            await self._persist()

    async def _scan_channel(self, client, chat_id: int) -> bool:
        try:
            chat = await client.get_chat(chat_id)
            self.state["current_channel_name"] = getattr(chat, "title", str(chat_id))
        except Exception:
            self.state["current_channel_name"] = str(chat_id)
        self.state["current_channel"] = str(chat_id)

        current = int((self.state.get("cursors") or {}).get(str(chat_id), 1) or 1)
        empty_streak = 0
        while not self._cancel and current < SCAN_MAX_ID_CAP and empty_streak < SCAN_MAX_EMPTY_BATCHES:
            upper = min(current + SCAN_BATCH_SIZE, SCAN_MAX_ID_CAP)
            try:
                messages = await client.get_messages(chat_id, list(range(current, upper)))
            except FloodWait as exc:
                await asyncio.sleep(exc.value)
                messages = await client.get_messages(chat_id, list(range(current, upper)))
            except Exception as exc:
                LOGGER.warning("[ScanManager] batch fetch failed at %s: %s", current, exc)
                self.state["counters"]["errors"] += 1
                empty_streak += 1
                current = upper
                continue

            if not isinstance(messages, list):
                messages = [messages]
            had_content = False
            for message in messages:
                if self._cancel:
                    break
                if message is None or getattr(message, "empty", False):
                    continue
                had_content = True
                self.state["counters"]["total_found"] += 1
                await self._process_message(message, chat_id)
                self.state["counters"]["processed"] += 1

            empty_streak = 0 if had_content else empty_streak + 1
            current = upper
            self.state.setdefault("cursors", {})[str(chat_id)] = current
            self.state["current_id"] = current
            await self._persist()
            await asyncio.sleep(SCAN_BATCH_DELAY)
        return True

    async def _stream_id_exists(self, channel: int, msg_id: int) -> bool:
        stream_hash = await encode_string({"chat_id": channel, "msg_id": msg_id})
        part_match = {"chat_id": channel, "msg_id": msg_id}
        for i in range(1, self._db.current_db_index + 1):
            storage = self._db.dbs.get(f"storage_{i}")
            if not storage:
                continue
            if await storage["movie"].find_one({"telegram.id": stream_hash}):
                return True
            if await storage["tv"].find_one({"seasons.episodes.telegram.id": stream_hash}):
                return True
            if await storage["movie"].find_one({"telegram.parts": {"$elemMatch": part_match}}):
                return True
            if await storage["tv"].find_one({"seasons.episodes.telegram.parts": {"$elemMatch": part_match}}):
                return True
        return False

    async def _process_message(self, message, chat_id: int) -> None:
        file = message.video or message.document
        if not file:
            self.state["counters"]["skipped_nonvid"] += 1
            return

        mime = getattr(file, "mime_type", "") or ""
        name = message.caption or getattr(file, "file_name", "") or ""
        file_name = getattr(file, "file_name", "") or ""
        if message.document and is_subtitle_file(file_name):
            channel_int = int(str(chat_id).replace("-100", ""))
            ok = await ingest_subtitle(file_name, channel_int, int(message.id), caption=message.caption)
            self.state["counters"]["subtitles_added" if ok else "subtitles_skipped"] += 1
            return

        if not (message.video or mime.startswith("video/") or parse_split_info(name)):
            self.state["counters"]["skipped_nonvid"] += 1
            return

        channel_int = int(str(chat_id).replace("-100", ""))
        msg_id = int(message.id)
        if await self._stream_id_exists(channel_int, msg_id):
            self.state["counters"]["skipped_dup"] += 1
            return

        title = finalize_media_name(name)

        metadata_info = await metadata(
            clean_filename(title),
            channel_int,
            msg_id,
            override_id=extract_default_id(message.caption) if message.caption else None,
        )
        if not metadata_info:
            self.state["counters"]["skipped_meta"] += 1
            return

        title = finalize_media_name(title, is_split=bool(metadata_info.get("group_key")))

        updated = await self._db.insert_media(
            metadata_info,
            channel=channel_int,
            msg_id=msg_id,
            size=get_readable_file_size(int(getattr(file, "file_size", 0) or 0)),
            name=title,
            raw_size=int(getattr(file, "file_size", 0) or 0),
        )
        if updated:
            self.state["counters"]["indexed"] += 1
            announce_new_media(metadata_info)
            try:
                await mark_uploaded_for_media(metadata_info)
            except Exception:
                pass
        else:
            self.state["counters"]["skipped_meta"] += 1

    async def _purge_channel_entries(self, channel_int: int) -> int:
        purged = 0
        for i in range(1, self._db.current_db_index + 1):
            storage = self._db.dbs.get(f"storage_{i}")
            if not storage:
                continue

            async for movie in storage["movie"].find({}):
                remaining = []
                changed = False
                for quality in movie.get("telegram", []):
                    if await self._quality_matches_channel(quality, channel_int):
                        purged += 1
                        changed = True
                        continue
                    remaining.append(quality)
                if changed:
                    if remaining:
                        movie["telegram"] = remaining
                        await storage["movie"].replace_one({"_id": movie["_id"]}, movie)
                    else:
                        await storage["movie"].delete_one({"_id": movie["_id"]})

            async for tv in storage["tv"].find({}):
                tv_changed = False
                for season in tv.get("seasons", []):
                    for episode in season.get("episodes", []):
                        remaining = []
                        for quality in episode.get("telegram", []):
                            if await self._quality_matches_channel(quality, channel_int):
                                purged += 1
                                tv_changed = True
                                continue
                            remaining.append(quality)
                        episode["telegram"] = remaining
                    season["episodes"] = [ep for ep in season.get("episodes", []) if ep.get("telegram")]
                tv["seasons"] = [season for season in tv.get("seasons", []) if season.get("episodes")]
                if tv_changed:
                    if tv["seasons"]:
                        await storage["tv"].replace_one({"_id": tv["_id"]}, tv)
                    else:
                        await storage["tv"].delete_one({"_id": tv["_id"]})
        return purged

    async def _quality_matches_channel(self, quality: dict, channel_int: int) -> bool:
        def same_channel(value: Any) -> bool:
            try:
                return int(str(value).replace("-100", "")) == channel_int
            except Exception:
                return False

        if same_channel(quality.get("origin_chat_id")):
            return True

        for part in quality.get("parts") or []:
            if same_channel(part.get("chat_id")):
                return True

        stream_id = quality.get("id")
        if not stream_id:
            return False

        try:
            decoded = await decode_string(stream_id)
        except Exception:
            return False

        if isinstance(decoded, dict):
            for part in decoded.get("parts") or []:
                if same_channel(part.get("chat_id")):
                    return True
            if same_channel(decoded.get("chat_id")):
                return True
        return False


class DbCheckManager:
    def __init__(self) -> None:
        self.state = {"status": "idle", "checked": 0, "alive": 0, "dead": 0, "errors": 0, "dead_entries": []}
        self._cancel = False

    def get_status(self) -> Dict[str, Any]:
        return dict(self.state)

    async def start(self, client, db) -> Dict[str, Any]:
        if self.state.get("status") == "running":
            return {"ok": False, "message": "DB check is already running.", "status": self.get_status()}
        self.state = {"status": "running", "checked": 0, "alive": 0, "dead": 0, "errors": 0, "dead_entries": []}
        self._cancel = False
        asyncio.create_task(self._run(client, db))
        return {"ok": True, "message": "DB check started.", "status": self.get_status()}

    async def cancel(self) -> Dict[str, Any]:
        self._cancel = True
        return {"ok": True, "message": "Cancel requested.", "status": self.get_status()}

    async def _run(self, client, db) -> None:
        try:
            for i in range(1, db.current_db_index + 1):
                storage = db.dbs.get(f"storage_{i}")
                if not storage:
                    continue
                for collection in ("movie", "tv"):
                    cursor = storage[collection].find({})
                    async for _doc in cursor:
                        if self._cancel:
                            self.state["status"] = "cancelled"
                            return
                        self.state["checked"] += 1
            self.state["status"] = "completed"
        except Exception as exc:
            self.state["status"] = "error"
            self.state["errors"] += 1
            self.state["error"] = str(exc)

    async def purge(self, db, stream_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        purged = 0
        for stream_id in stream_ids or []:
            if await db.delete_media_by_stream_id(stream_id):
                purged += 1
        return {"ok": True, "message": f"Purged {purged} dead link(s).", "purged": purged}


scan_manager = ScanManager()
dbcheck_manager = DbCheckManager()
