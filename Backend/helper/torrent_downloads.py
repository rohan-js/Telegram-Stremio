import asyncio
import html
import mimetypes
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

import httpx
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend.config import Telegram
from Backend.helper.torrent_source import VIDEO_EXTENSIONS, get_readable_file_size
from Backend.logger import LOGGER


DOWNLOADING_STATES = {"queued", "downloading"}
DONE_STATES = {
    "uploading",
    "stalledUP",
    "queuedUP",
    "checkingUP",
    "forcedUP",
    "stoppedUP",
    "pausedUP",
}
ERROR_STATES = {"error", "missingFiles", "unknown"}


class QBitTorrentError(RuntimeError):
    pass


def torrent_downloads_enabled() -> bool:
    return bool(getattr(Telegram, "TORRENT_DOWNLOADS_ENABLED", True))


def download_root_dir() -> Path:
    return Path(getattr(Telegram, "TORRENT_DOWNLOAD_ROOT", "/downloads/completed") or "/downloads/completed")


def _first_existing_path(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def download_disk_free_gb(
    root: Optional[Path] = None,
    usage_func: Callable[[Path], Any] = shutil.disk_usage,
) -> float:
    root = root or download_root_dir()
    usage = usage_func(_first_existing_path(Path(root)))
    return float(usage.free) / (1024 ** 3)


def has_enough_download_space(
    min_free_gb: Optional[float] = None,
    root: Optional[Path] = None,
    usage_func: Callable[[Path], Any] = shutil.disk_usage,
) -> tuple[bool, float, float]:
    if min_free_gb is None:
        min_free_gb = float(getattr(Telegram, "TORRENT_DOWNLOAD_MIN_FREE_GB", 10) or 10)
    free_gb = download_disk_free_gb(root=root, usage_func=usage_func)
    return free_gb >= float(min_free_gb), free_gb, float(min_free_gb)


def safe_download_file_path(root: Path, rel_path: str) -> Path:
    clean_rel = str(rel_path or "").replace("\\", "/").lstrip("/")
    if not clean_rel or "\x00" in clean_rel:
        raise ValueError("Invalid download path")

    base = Path(root).resolve()
    target = (base / clean_rel).resolve()
    if not target.is_relative_to(base):
        raise ValueError("Download path escapes root")
    return target


def nginx_download_redirect_uri(rel_path: str) -> str:
    prefix = (getattr(Telegram, "NGINX_DOWNLOAD_ACCEL_REDIRECT_LOCATION", "") or "/_downloads/").strip()
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    if not prefix.endswith("/"):
        prefix += "/"
    return prefix + quote(str(rel_path).replace("\\", "/").lstrip("/"), safe="/")


def normalize_qbit_file(raw: dict, fallback_index: int = 0) -> dict:
    name = str(raw.get("name") or raw.get("path") or "")
    rel_path = name.replace("\\", "/").lstrip("/")
    try:
        size = int(raw.get("size") or 0)
    except Exception:
        size = 0
    try:
        progress = float(raw.get("progress") if raw.get("progress") is not None else 0.0)
    except Exception:
        progress = 0.0
    index = raw.get("index")
    try:
        index = int(index)
    except Exception:
        index = int(fallback_index)

    return {
        "index": index,
        "name": name,
        "rel_path": rel_path,
        "size": size,
        "size_text": get_readable_file_size(size),
        "progress": progress,
        "priority": raw.get("priority"),
        "is_video": rel_path.lower().endswith(VIDEO_EXTENSIONS),
    }


def normalize_qbit_files(files: list[dict]) -> list[dict]:
    return [normalize_qbit_file(item, idx) for idx, item in enumerate(files or [])]


def _episode_pattern(season_number: int, episode_number: int) -> re.Pattern:
    season = int(season_number)
    episode = int(episode_number)
    return re.compile(
        rf"(?i)(?:s0*{season}[\s._-]*e0*{episode}|(?:^|[^\d])0*{season}x0*{episode}(?:[^\d]|$))"
    )


def select_completed_torrent_file(
    files: list[dict],
    quality: dict,
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> Optional[dict]:
    normalized = normalize_qbit_files(files)
    videos = [item for item in normalized if item.get("is_video")]
    if not videos:
        return None

    file_idx = quality.get("file_idx")
    if file_idx is not None:
        try:
            file_idx = int(file_idx)
            for item in videos:
                if int(item.get("index", -1)) == file_idx:
                    return item
        except Exception:
            pass

    target_name = (quality.get("filename") or quality.get("name") or "").strip().lower()
    if target_name:
        target_base = Path(target_name.replace("\\", "/")).name
        exact = [
            item for item in videos
            if Path(str(item.get("rel_path") or item.get("name") or "")).name.lower() == target_base
        ]
        if len(exact) == 1:
            return exact[0]

    if season_number is not None and episode_number is not None:
        pattern = _episode_pattern(int(season_number), int(episode_number))
        matches = [
            item for item in videos
            if pattern.search(str(item.get("rel_path") or item.get("name") or ""))
        ]
        if len(matches) == 1:
            return matches[0]

    if len(videos) == 1:
        return videos[0]

    return None


def torrent_download_callback_data(info_hash: str) -> str:
    return f"tdl_{str(info_hash).lower()}"


def torrent_download_keyboard(info_hash: str, stremio_link: Optional[str] = None, completed: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if stremio_link:
        rows.append([InlineKeyboardButton("▶️ Watch in Stremio", url=stremio_link)])
    if not completed:
        rows.append([InlineKeyboardButton("⬇️ Download to VPS", callback_data=torrent_download_callback_data(info_hash))])
    return InlineKeyboardMarkup(rows) if rows else None


def format_torrent_download_message(job: dict) -> str:
    status = str(job.get("status") or "queued")
    title = html.escape(str(job.get("name") or job.get("title") or job.get("info_hash") or "Torrent"))
    progress = float(job.get("progress") or 0.0)
    downloaded = int(job.get("downloaded") or 0)
    size = int(job.get("size") or 0)
    dlspeed = int(job.get("dlspeed") or 0)
    eta = int(job.get("eta") or 0)

    if status == "completed":
        status_line = "✅ <b>Downloaded to VPS</b>"
    elif status == "failed":
        reason = html.escape(str(job.get("failed_reason") or "Download failed"))
        status_line = f"❌ <b>Download failed:</b> {reason}"
    elif status == "downloading":
        status_line = "⬇️ <b>Downloading to VPS</b>"
    else:
        status_line = "⏳ <b>Queued for VPS download</b>"

    lines = [
        f"🎬 <b>{title}</b>",
        status_line,
    ]

    if status in {"queued", "downloading"}:
        lines.extend(
            [
                f"Progress: <b>{progress * 100:.1f}%</b>",
                f"Downloaded: <b>{get_readable_file_size(downloaded)}</b>{f' / {get_readable_file_size(size)}' if size else ''}",
                f"Speed: <b>{get_readable_file_size(dlspeed)}/s</b>",
            ]
        )
        if eta and eta > 0 and eta < 8640000:
            lines.append(f"ETA: <b>{eta // 60} min</b>")
    elif status == "completed":
        lines.append(f"Size: <b>{get_readable_file_size(size)}</b>" if size else "Ready to stream from VPS.")

    return "\n".join(lines)


class QBitTorrentClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 20.0,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = (base_url or getattr(Telegram, "QBITTORRENT_BASE_URL", "http://qbittorrent:8080")).rstrip("/")
        self.username = username if username is not None else getattr(Telegram, "QBITTORRENT_USERNAME", "")
        self.password = password if password is not None else getattr(Telegram, "QBITTORRENT_PASSWORD", "")
        self._own_client = client is None
        self.client = client or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self._logged_in = False

    async def close(self) -> None:
        if self._own_client:
            await self.client.aclose()

    async def login(self) -> None:
        if not self.username or not self.password:
            raise QBitTorrentError("qBittorrent authentication required but no credentials are configured")
        response = await self.client.post(
            "/api/v2/auth/login",
            data={"username": self.username, "password": self.password},
            headers={"Referer": self.base_url + "/"},
        )
        if response.status_code != 200 or response.text.strip() != "Ok.":
            raise QBitTorrentError(f"qBittorrent login failed: HTTP {response.status_code}")
        self._logged_in = True

    async def _request(self, method: str, path: str, retry_login: bool = True, **kwargs) -> httpx.Response:
        headers = kwargs.pop("headers", {}) or {}
        headers.setdefault("Referer", self.base_url + "/")
        last_error = None
        response = None
        for attempt in range(3):
            try:
                response = await self.client.request(method, path, headers=headers, **kwargs)
                break
            except httpx.RequestError as e:
                last_error = e
                if attempt < 2:
                    await asyncio.sleep(2)
        if response is None:
            raise QBitTorrentError(f"{method} {path} failed: {last_error}")
        if response.status_code == 403 and retry_login:
            await self.login()
            response = await self.client.request(method, path, headers=headers, **kwargs)
        if response.status_code >= 400:
            raise QBitTorrentError(f"{method} {path} failed: HTTP {response.status_code} {response.text[:160]}")
        return response

    async def add_torrent(
        self,
        *,
        magnet_uri: Optional[str] = None,
        torrent_bytes: Optional[bytes] = None,
        save_path: Optional[str] = None,
        temp_path: Optional[str] = None,
    ) -> None:
        data = {
            "savepath": save_path or getattr(Telegram, "QBITTORRENT_SAVE_PATH", "/downloads/completed"),
            "category": "stremio",
            "paused": "false",
            "root_folder": "true",
        }
        temp_path = temp_path or getattr(Telegram, "QBITTORRENT_TEMP_PATH", "")
        if temp_path:
            data["download_path"] = temp_path

        if magnet_uri:
            data["urls"] = magnet_uri
            await self._request("POST", "/api/v2/torrents/add", data=data)
            return

        if torrent_bytes:
            files = {"torrents": ("source.torrent", torrent_bytes, "application/x-bittorrent")}
            await self._request("POST", "/api/v2/torrents/add", data=data, files=files)
            return

        raise QBitTorrentError("No magnet URI or torrent bytes supplied")

    async def torrent_info(self, info_hash: str) -> Optional[dict]:
        response = await self._request(
            "GET",
            "/api/v2/torrents/info",
            params={"hashes": str(info_hash).lower()},
        )
        data = response.json()
        if not data:
            return None
        return data[0]

    async def torrent_files(self, info_hash: str) -> list[dict]:
        response = await self._request(
            "GET",
            "/api/v2/torrents/files",
            params={"hash": str(info_hash).lower()},
        )
        return response.json() or []

    async def stop_torrent(self, info_hash: str) -> None:
        try:
            await self._request("POST", "/api/v2/torrents/stop", data={"hashes": str(info_hash).lower()})
        except QBitTorrentError:
            await self._request("POST", "/api/v2/torrents/pause", data={"hashes": str(info_hash).lower()})

    async def delete_torrent(self, info_hash: str, delete_files: bool = False) -> None:
        await self._request(
            "POST",
            "/api/v2/torrents/delete",
            data={"hashes": str(info_hash).lower(), "deleteFiles": "true" if delete_files else "false"},
        )


class TorrentDownloadManager:
    def __init__(self) -> None:
        self._started = False
        self._start_lock: Optional[asyncio.Lock] = None
        self._wake_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        if self._started or not torrent_downloads_enabled():
            return
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            if self._started:
                return
            self._wake_event = asyncio.Event()
            concurrency = max(1, min(int(getattr(Telegram, "TORRENT_DOWNLOAD_CONCURRENCY", 1) or 1), 1))
            for worker_id in range(concurrency):
                asyncio.create_task(self._worker_loop(worker_id))
            self._started = True
            LOGGER.info("Torrent download manager started (concurrency=%s)", concurrency)

    def wake(self) -> None:
        if self._wake_event:
            self._wake_event.set()

    async def queue_from_info_hash(
        self,
        client,
        info_hash: str,
        requester_user_id: Optional[int],
        status_message_chat_id: int,
        status_message_id: int,
    ) -> tuple[bool, str, Optional[dict]]:
        from Backend import db

        if not torrent_downloads_enabled():
            return False, "Torrent downloads are disabled.", None

        existing = await db.get_torrent_download(info_hash)
        if existing and existing.get("status") in {"queued", "downloading", "completed"}:
            source = await db.find_torrent_download_source(info_hash) or {}
            await db.update_torrent_download_job(
                info_hash,
                {
                    "requester_user_id": requester_user_id,
                    "status_message_chat_id": status_message_chat_id,
                    "status_message_id": status_message_id,
                    "stremio_link": source.get("stremio_link") or existing.get("stremio_link"),
                    "updated_at": datetime.utcnow(),
                },
            )
            job = await db.get_torrent_download(info_hash) or existing
            await self.start()
            if job.get("status") != "completed":
                self.wake()
            await self.edit_status_message(client, job, force=True)
            if job.get("status") == "completed":
                return True, "Already downloaded to VPS.", job
            if job.get("status") == "downloading":
                return True, "Torrent download is already running.", job
            return True, "Torrent download queued.", job

        ok, free_gb, min_free_gb = has_enough_download_space()
        if not ok:
            return False, f"Not enough download disk space ({free_gb:.1f} GB free, need {min_free_gb:.1f} GB).", None

        source = await db.find_torrent_download_source(info_hash)
        if not source:
            return False, "Torrent source metadata was not found.", None

        if not source.get("torrent_source_uri") and not source.get("torrent_file_msg_id"):
            return False, "This torrent source cannot be re-added for downloading.", None

        source.update(
            {
                "requester_user_id": requester_user_id,
                "status_message_chat_id": status_message_chat_id,
                "status_message_id": status_message_id,
            }
        )
        job = await db.upsert_torrent_download_job(source)
        await self.start()
        self.wake()
        await self.edit_status_message(client, job, force=True)

        status = job.get("status")
        if status == "completed":
            return True, "Already downloaded to VPS.", job
        if status == "downloading":
            return True, "Torrent download is already running.", job
        return True, "Torrent download queued.", job

    async def _worker_loop(self, worker_id: int) -> None:
        from Backend import db

        while True:
            try:
                job = await db.get_next_torrent_download_job()
                if not job:
                    if self._wake_event:
                        try:
                            await asyncio.wait_for(self._wake_event.wait(), timeout=60)
                            self._wake_event.clear()
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await asyncio.sleep(60)
                    continue
                await self._process_job(job)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                LOGGER.warning("Torrent download worker %s error: %s", worker_id, e)
                await asyncio.sleep(10)

    async def _process_job(self, job: dict) -> None:
        from Backend import db
        from Backend.pyrofork.bot import StreamBot

        info_hash = str(job.get("info_hash") or job.get("_id") or "").lower()
        if not info_hash:
            return

        qbit = QBitTorrentClient()
        try:
            now = datetime.utcnow()
            await db.update_torrent_download_job(
                info_hash,
                {
                    "status": "downloading",
                    "started_at": job.get("started_at") or now,
                    "updated_at": now,
                    "failed_reason": None,
                },
            )
            job = await db.get_torrent_download(info_hash) or job
            await self.edit_status_message(StreamBot, job, force=True)

            await self._ensure_torrent_added(StreamBot, qbit, job)
            await self._poll_until_done(StreamBot, qbit, info_hash)
        except Exception as e:
            LOGGER.warning("Torrent download job %s failed: %s", info_hash, e)
            await self._fail_job(StreamBot, qbit, info_hash, str(e)[:200])
        finally:
            await qbit.close()

    async def _ensure_torrent_added(self, client, qbit: QBitTorrentClient, job: dict) -> None:
        info_hash = str(job.get("info_hash") or "").lower()
        existing = await qbit.torrent_info(info_hash)
        if existing:
            return

        magnet_uri = job.get("torrent_source_uri")
        torrent_bytes = None
        if not magnet_uri:
            torrent_bytes = await self._download_torrent_file_bytes(client, job)

        await qbit.add_torrent(
            magnet_uri=magnet_uri,
            torrent_bytes=torrent_bytes,
            save_path=getattr(Telegram, "QBITTORRENT_SAVE_PATH", "/downloads/completed"),
            temp_path=getattr(Telegram, "QBITTORRENT_TEMP_PATH", "/downloads/incomplete"),
        )

    async def _download_torrent_file_bytes(self, client, job: dict) -> bytes:
        chat_id = int(job.get("torrent_file_chat_id") or job.get("origin_chat_id") or 0)
        msg_id = int(job.get("torrent_file_msg_id") or job.get("origin_msg_id") or 0)
        if not chat_id or not msg_id:
            raise QBitTorrentError("Torrent file Telegram message is missing")
        message = await client.get_messages(chat_id, msg_id)
        torrent_file = await client.download_media(message, in_memory=True)
        torrent_file.seek(0)
        data = torrent_file.read()
        torrent_file.close()
        if not data:
            raise QBitTorrentError("Torrent file download returned no bytes")
        return data

    async def _poll_until_done(self, client, qbit: QBitTorrentClient, info_hash: str) -> None:
        from Backend import db

        poll_sec = max(5, int(getattr(Telegram, "TORRENT_DOWNLOAD_POLL_SEC", 15) or 15))
        stall_timeout = max(60, int(getattr(Telegram, "TORRENT_DOWNLOAD_STALL_TIMEOUT_SEC", 3600) or 3600))
        max_runtime = max(300, int(getattr(Telegram, "TORRENT_DOWNLOAD_MAX_RUNTIME_SEC", 172800) or 172800))
        started_mono = time.monotonic()
        last_progress_mono = time.monotonic()
        last_progress = 0.0

        while True:
            info = await qbit.torrent_info(info_hash)
            if not info:
                if time.monotonic() - last_progress_mono >= stall_timeout:
                    await self._fail_job(client, qbit, info_hash, "No torrent metadata or progress for 1 hour")
                    return
                if time.monotonic() - started_mono >= max_runtime:
                    await self._fail_job(client, qbit, info_hash, "Download exceeded 48 hour limit")
                    return
                await asyncio.sleep(poll_sec)
                continue

            progress = float(info.get("progress") or 0.0)
            last_progress_at = None
            if progress > last_progress + 0.0001:
                last_progress = progress
                last_progress_mono = time.monotonic()
                last_progress_at = datetime.utcnow()

            state = str(info.get("state") or "")
            update = {
                "status": "downloading",
                "qbit_hash": str(info.get("hash") or info_hash).lower(),
                "name": info.get("name") or info.get("save_path") or info_hash,
                "size": int(info.get("size") or 0),
                "progress": progress,
                "downloaded": int(info.get("downloaded") or 0),
                "dlspeed": int(info.get("dlspeed") or 0),
                "eta": int(info.get("eta") or 0),
                "save_path": info.get("save_path"),
                "content_path": info.get("content_path"),
                "qbit_state": state,
                "updated_at": datetime.utcnow(),
            }
            if last_progress_at:
                update["last_progress_at"] = last_progress_at
            await db.update_torrent_download_job(info_hash, update)
            job = await db.get_torrent_download(info_hash)
            if job:
                await self.edit_status_message(client, job, force=False)

            if progress >= 0.999 or state in DONE_STATES:
                try:
                    files = normalize_qbit_files(await qbit.torrent_files(info_hash))
                except Exception as e:
                    LOGGER.warning("Could not fetch completed torrent files for %s: %s", info_hash, e)
                    files = []
                try:
                    await qbit.stop_torrent(info_hash)
                except Exception as e:
                    LOGGER.warning("Could not stop completed torrent %s: %s", info_hash, e)
                await db.update_torrent_download_job(
                    info_hash,
                    {
                        **update,
                        "status": "completed",
                        "progress": 1.0,
                        "files": files,
                        "completed_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                    },
                )
                job = await db.get_torrent_download(info_hash)
                if job:
                    await self.edit_status_message(client, job, force=True)
                return

            if state in ERROR_STATES:
                await self._fail_job(client, qbit, info_hash, f"qBittorrent state: {state}")
                return

            if time.monotonic() - last_progress_mono >= stall_timeout:
                await self._fail_job(client, qbit, info_hash, "No progress for 1 hour")
                return

            if time.monotonic() - started_mono >= max_runtime:
                await self._fail_job(client, qbit, info_hash, "Download exceeded 48 hour limit")
                return

            await asyncio.sleep(poll_sec)

    async def _fail_job(self, client, qbit: QBitTorrentClient, info_hash: str, reason: str) -> None:
        from Backend import db

        try:
            await qbit.delete_torrent(info_hash, delete_files=True)
        except Exception as e:
            LOGGER.debug("Failed to delete incomplete torrent %s: %s", info_hash, e)

        await db.update_torrent_download_job(
            info_hash,
            {
                "status": "failed",
                "failed_reason": reason,
                "failed_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
        )
        job = await db.get_torrent_download(info_hash)
        if job:
            await self.edit_status_message(client, job, force=True)

    async def edit_status_message(self, client, job: dict, force: bool = False) -> None:
        from Backend import db

        chat_id = job.get("status_message_chat_id")
        msg_id = job.get("status_message_id")
        if not chat_id or not msg_id:
            return

        if not force:
            last_edit = job.get("last_status_edit_at")
            if last_edit:
                if last_edit.tzinfo is not None:
                    last_edit = last_edit.replace(tzinfo=None)
                interval = max(10, int(getattr(Telegram, "TORRENT_DOWNLOAD_PROGRESS_EDIT_SEC", 60) or 60))
                if (datetime.utcnow() - last_edit).total_seconds() < interval:
                    return

        info_hash = str(job.get("info_hash") or job.get("_id") or "").lower()
        keyboard = torrent_download_keyboard(
            info_hash,
            stremio_link=job.get("stremio_link"),
            completed=job.get("status") == "completed",
        )
        try:
            await client.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(msg_id),
                text=format_torrent_download_message(job),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
            await db.update_torrent_download_job(info_hash, {"last_status_edit_at": datetime.utcnow()})
        except Exception as e:
            LOGGER.debug("Torrent download status edit failed for %s: %s", info_hash, e)


def guess_mime_type(path: Path) -> str:
    return mimetypes.guess_type(str(path))[0] or "application/octet-stream"


TORRENT_DOWNLOAD_MANAGER = TorrentDownloadManager()
