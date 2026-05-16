import asyncio
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyrogram import Client

from Backend.config import Telegram
from Backend.logger import LOGGER


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _as_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _cache_max_bytes() -> int:
    # Prefer bytes if provided; else GB.
    max_bytes = getattr(Telegram, "DISK_CACHE_MAX_BYTES", 0) or 0
    try:
        max_bytes = int(max_bytes)
    except Exception:
        max_bytes = 0

    if max_bytes > 0:
        return max_bytes

    max_gb = getattr(Telegram, "DISK_CACHE_MAX_GB", 0) or 0
    try:
        max_gb = float(max_gb)
    except Exception:
        max_gb = 0

    if max_gb <= 0:
        return 0

    return int(max_gb * 1024 * 1024 * 1024)


def disk_cache_enabled() -> bool:
    if not _as_bool(getattr(Telegram, "DISK_CACHE_ENABLED", False)):
        return False
    # Require a bounded cache so LRU eviction is meaningful/safe.
    return _cache_max_bytes() > 0


def cache_root_dir() -> Path:
    raw = (getattr(Telegram, "DISK_CACHE_DIR", "") or "cache").strip()
    p = Path(raw)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    return p


def _hash_key(chat_id: int, msg_id: int, unique_id: str) -> str:
    raw = f"{int(chat_id)}:{int(msg_id)}:{unique_id or ''}"
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def cache_relpath(chat_id: int, msg_id: int, unique_id: str) -> str:
    h = _hash_key(chat_id, msg_id, unique_id)
    return f"{h[:2]}/{h}.bin"


def cache_abspath(chat_id: int, msg_id: int, unique_id: str) -> Path:
    return cache_root_dir() / cache_relpath(chat_id, msg_id, unique_id)


def nginx_accel_enabled() -> bool:
    return _as_bool(getattr(Telegram, "NGINX_ACCEL_REDIRECT_ENABLED", False))


def nginx_accel_location_prefix() -> str:
    loc = (getattr(Telegram, "NGINX_ACCEL_REDIRECT_LOCATION", "") or "/_cache/").strip()
    if not loc.startswith("/"):
        loc = "/" + loc
    if not loc.endswith("/"):
        loc += "/"
    return loc


def nginx_accel_redirect_uri(chat_id: int, msg_id: int, unique_id: str) -> str:
    rel = cache_relpath(chat_id, msg_id, unique_id).replace("\\", "/")
    return nginx_accel_location_prefix() + rel


def is_complete_cache_file(path: Path, expected_size: Optional[int] = None) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        if expected_size is not None and expected_size > 0:
            return path.stat().st_size == int(expected_size)
        return True
    except Exception:
        return False


def touch_cache_file(path: Path) -> None:
    try:
        os.utime(path, None)
    except Exception:
        pass


def _evict_lru_sync(root: Path, max_bytes: int) -> None:
    try:
        if max_bytes <= 0:
            return
        if not root.exists():
            return

        files = []
        total = 0
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.endswith(".part"):
                    continue
                full = Path(dirpath) / fn
                try:
                    st = full.stat()
                except Exception:
                    continue
                if not full.is_file():
                    continue
                size = int(st.st_size)
                total += size
                files.append((float(st.st_mtime), size, full))

        if total <= max_bytes:
            return

        files.sort(key=lambda t: t[0])  # oldest first
        removed = 0
        while total > max_bytes and files:
            _, size, fp = files.pop(0)
            try:
                fp.unlink(missing_ok=True)
                total -= size
                removed += 1
            except Exception:
                # If we can't delete one file, skip it.
                continue

        if removed:
            LOGGER.info("Disk cache eviction removed %s files; now %s bytes", removed, total)

    except Exception as e:
        LOGGER.warning("Disk cache eviction failed: %s", e)


async def evict_lru(root: Optional[Path] = None, max_bytes: Optional[int] = None) -> None:
    if root is None:
        root = cache_root_dir()
    if max_bytes is None:
        max_bytes = _cache_max_bytes()

    await asyncio.to_thread(_evict_lru_sync, Path(root), int(max_bytes))


@dataclass(frozen=True)
class PrecacheJob:
    chat_id: int
    msg_id: int
    unique_id: str
    expected_size: int


class DiskPrecacheManager:
    def __init__(self) -> None:
        self._started = False
        self._start_lock: Optional[asyncio.Lock] = None
        self._queue: Optional[asyncio.Queue] = None
        self._inflight: Optional[set[str]] = None

    async def _ensure_started(self) -> None:
        if self._started:
            return
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            if self._started:
                return
            self._queue = asyncio.Queue()
            self._inflight = set()

            concurrency = int(getattr(Telegram, "DISK_CACHE_CONCURRENCY", 1) or 1)
            concurrency = max(1, min(concurrency, 4))

            for i in range(concurrency):
                asyncio.create_task(self._worker(i))

            self._started = True
            LOGGER.info("Disk precache workers started (concurrency=%s)", concurrency)

    async def enqueue(self, client: Client, job: PrecacheJob) -> None:
        if not disk_cache_enabled() or not _as_bool(getattr(Telegram, "DISK_CACHE_PRECACHE_ON_INGEST", False)):
            return

        await self._ensure_started()
        assert self._queue is not None
        assert self._inflight is not None

        key = _hash_key(job.chat_id, job.msg_id, job.unique_id)
        dest = cache_abspath(job.chat_id, job.msg_id, job.unique_id)

        if key in self._inflight:
            return
        if is_complete_cache_file(dest, expected_size=job.expected_size):
            touch_cache_file(dest)
            return

        self._inflight.add(key)
        await self._queue.put((client, key, job))

    async def _worker(self, worker_id: int) -> None:
        assert self._queue is not None
        assert self._inflight is not None

        while True:
            client, key, job = await self._queue.get()
            try:
                await self._run_job(client, job)
            except Exception as e:
                LOGGER.warning("Disk precache worker %s failed job %s: %s", worker_id, key, e)
            finally:
                try:
                    self._inflight.discard(key)
                except Exception:
                    pass
                try:
                    self._queue.task_done()
                except Exception:
                    pass

    async def _run_job(self, client: Client, job: PrecacheJob) -> None:
        if not disk_cache_enabled():
            return

        dest = cache_abspath(job.chat_id, job.msg_id, job.unique_id)
        dest.parent.mkdir(parents=True, exist_ok=True)

        if is_complete_cache_file(dest, expected_size=job.expected_size):
            touch_cache_file(dest)
            return

        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
        except Exception:
            pass

        msg = await client.get_messages(job.chat_id, job.msg_id)
        if getattr(msg, "empty", False):
            return

        LOGGER.info(
            "Pre-caching Telegram media chat_id=%s msg_id=%s → %s",
            job.chat_id, job.msg_id, str(dest),
        )

        # Download to temp file then atomically rename.
        await client.download_media(msg, file_name=str(tmp))

        if not is_complete_cache_file(tmp, expected_size=job.expected_size):
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            LOGGER.warning(
                "Pre-cache size mismatch chat_id=%s msg_id=%s expected=%s got=%s",
                job.chat_id, job.msg_id, job.expected_size,
                (tmp.stat().st_size if tmp.exists() else None),
            )
            return

        tmp.replace(dest)
        touch_cache_file(dest)

        await evict_lru(cache_root_dir(), _cache_max_bytes())


PRECACHE_MANAGER = DiskPrecacheManager()
