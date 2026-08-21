"""Streamed-chunk spill cache — persist delivered chunks to a sparse disk file.

Every chunk the live streamer delivers to a viewer is ALSO written (best
effort, never blocking) into a sparse per-file disk buffer. Backward seeks,
pause/resume replays, and a second viewer of the same file are then served
from NVMe with zero MTProto calls.

Design constraints (E2.1.Micro: 1 OCPU, bounded disk):
  - Writes go through a single background writer task with a bounded queue;
    when the queue is full chunks are DROPPED (streaming always wins).
  - Extents (which byte ranges are on disk) are in-RAM truth; the disk file is
    just byte storage. On process start all ``*.ranges.bin`` files are swept
    (they are stale by definition once the extent map is gone).
  - A dedicated ``SPILL_CACHE_MAX_GB`` budget (inside the shared disk cache
    root); eviction removes whole range files LRU. The existing first-MB /
    full-file LRU evictor skips ``.ranges.bin`` files, and this module never
    touches the other cache files.
"""

import asyncio
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from Backend.config import Telegram
from Backend.logger import LOGGER
from Backend.helper.disk_cache import _hash_key, cache_root_dir

_RANGE_SUFFIX = ".ranges.bin"
_WRITE_QUEUE_MAX = 64

# key: (chat_id, msg_id, unique_id) -> entry dict:
#   {"path": Path, "ranges": [(start, end_exclusive), ...] merged,
#    "bytes": int (sum of merged lengths), "last_touch": float}
_extents: Dict[Tuple[int, int, str], dict] = {}
_extents_lock = asyncio.Lock()

_write_queue: Optional[asyncio.Queue] = None
_writer_task: Optional[asyncio.Task] = None
_writer_lock = asyncio.Lock()

_dropped_chunks = 0
_swept_at_startup = False
_stats_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------
def spill_enabled() -> bool:
    try:
        return bool(getattr(Telegram, "SPILL_CACHE_ENABLED", True)) and _budget_bytes() > 0
    except Exception:
        return False


def _budget_bytes() -> int:
    try:
        gb = float(getattr(Telegram, "SPILL_CACHE_MAX_GB", 2.0) or 2.0)
    except Exception:
        gb = 2.0
    return int(gb * 1024 * 1024 * 1024)


def _range_relpath(chat_id: int, msg_id: int, unique_id: str) -> str:
    h = _hash_key(chat_id, msg_id, unique_id)
    return f"{h[:2]}/{h}{_RANGE_SUFFIX}"


def _range_abspath(chat_id: int, msg_id: int, unique_id: str) -> Path:
    return cache_root_dir() / _range_relpath(chat_id, msg_id, unique_id)


async def get_spill_stats() -> dict:
    async with _extents_lock:
        files = len(_extents)
        total_bytes = sum(int(e.get("bytes", 0)) for e in _extents.values())
    async with _stats_lock:
        dropped = _dropped_chunks
    return {
        "enabled": spill_enabled(),
        "files": files,
        "bytes": total_bytes,
        "bytes_mb": round(total_bytes / (1024 * 1024), 2),
        "budget_gb": round(_budget_bytes() / (1024 * 1024 * 1024), 2),
        "dropped_chunks": dropped,
    }


# ---------------------------------------------------------------------------
# Write path (called from the live streaming consumer — must never block)
# ---------------------------------------------------------------------------
def enqueue_spill(chat_id: int, msg_id: int, unique_id: str, offset: int, chunk: bytes) -> None:
    """Queue one delivered chunk for background disk write. Sync + non-blocking."""
    global _dropped_chunks, _swept_at_startup
    if not spill_enabled() or not chunk or not unique_id:
        return
    try:
        if not _swept_at_startup:
            # Stale range files from a previous process are unusable (extent
            # map is gone) — sweep once on first use instead of at import so
            # tests that never enable the spill don't pay for it.
            _swept_at_startup = True
            _purge_all_range_files()
        q = _ensure_write_queue()
        _ensure_writer_task()
        q.put_nowait((int(chat_id), int(msg_id), unique_id, int(offset), chunk))
    except asyncio.QueueFull:
        _dropped_chunks += 1
    except Exception:
        pass


def _ensure_write_queue() -> asyncio.Queue:
    global _write_queue
    if _write_queue is None:
        _write_queue = asyncio.Queue(maxsize=_WRITE_QUEUE_MAX)
    return _write_queue


async def _spill_writer_loop() -> None:
    global _dropped_chunks
    q = _ensure_write_queue()
    while True:
        item = await q.get()
        try:
            await _handle_write(item)
        except Exception as exc:
            LOGGER.debug("spill write error: %s", exc)
        finally:
            q.task_done()


def _ensure_writer_task() -> None:
    global _writer_task
    if _writer_task is None or _writer_task.done():
        _writer_task = asyncio.create_task(_spill_writer_loop())


async def _handle_write(item) -> None:
    chat_id, msg_id, unique_id, offset, chunk = item
    key = (chat_id, msg_id, unique_id)
    async with _extents_lock:
        entry = _extents.get(key)
    if entry is None:
        entry = {
            "path": _range_abspath(chat_id, msg_id, unique_id),
            "ranges": [],
            "bytes": 0,
            "last_touch": time.time(),
        }

    wrote = await asyncio.to_thread(_pwrite_sync, entry["path"], offset, chunk)
    if wrote <= 0:
        return

    grew = _insert_merged_range(entry["ranges"], offset, offset + len(chunk))
    entry["bytes"] = sum(e - s for s, e in entry["ranges"])
    entry["last_touch"] = time.time()
    async with _extents_lock:
        _extents[key] = entry
    if grew:
        await _maybe_evict()


def _insert_merged_range(ranges: List[Tuple[int, int]], start: int, end: int) -> bool:
    """Insert [start, end) into a sorted merged-interval list. Returns True if total grew."""
    grew = end - start
    new: List[Tuple[int, int]] = []
    placed = False
    for s, e in ranges:
        if e < start:
            new.append((s, e))
        elif end < s:
            if not placed:
                new.append((start, end))
                placed = True
            new.append((s, e))
        else:
            # overlapping/adjacent — merge into the running interval
            if not placed:
                new.append((min(s, start), max(e, end)))
                placed = True
            else:
                ls, le = new[-1]
                new[-1] = (ls, max(le, e))
    if not placed:
        new.append((start, end))
    total_before = sum(e - s for s, e in ranges)
    total_after = sum(e - s for s, e in new)
    ranges[:] = new
    return total_after > total_before


def _pwrite_sync(path: Path, offset: int, chunk: bytes) -> int:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(path), "r+b") as f:
            f.seek(offset)
            f.write(chunk)
            return len(chunk)
    except FileNotFoundError:
        try:
            with open(str(path), "wb") as f:
                f.seek(offset)
                f.write(chunk)
                return len(chunk)
        except Exception:
            return 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Read path (checked in media_streamer before MTProto)
# ---------------------------------------------------------------------------
async def read_spilled(chat_id: int, msg_id: int, unique_id: str, start: int, length: int) -> Optional[bytes]:
    """Return ``length`` bytes at ``start`` if the range is fully on disk."""
    if not spill_enabled() or length <= 0:
        return None
    key = (int(chat_id), int(msg_id), unique_id)
    async with _extents_lock:
        entry = _extents.get(key)
        if entry is None:
            return None
        covered = _range_covered(entry["ranges"], start, start + length)
        if not covered:
            return None
        entry["last_touch"] = time.time()
        path = entry["path"]
    data = await asyncio.to_thread(_pread_sync, path, start, length)
    if data is None or len(data) != length:
        return None
    return data


def _range_covered(ranges: List[Tuple[int, int]], start: int, end: int) -> bool:
    """True if [start, end) is fully inside the merged intervals."""
    cur = start
    for s, e in ranges:
        if s <= cur < e:
            cur = e
            if cur >= end:
                return True
        elif s > cur:
            return False
    return cur >= end


def _pread_sync(path: Path, start: int, length: int) -> Optional[bytes]:
    try:
        with open(str(path), "rb") as f:
            f.seek(start)
            return f.read(length)
    except Exception:
        return None


async def has_spilled_range(chat_id: int, msg_id: int, unique_id: str, start: int, length: int) -> bool:
    """Cheap coverage check without reading (used by skip pre-warm dedup)."""
    key = (int(chat_id), int(msg_id), unique_id)
    async with _extents_lock:
        entry = _extents.get(key)
        if entry is None:
            return False
        return _range_covered(entry["ranges"], start, start + length)


# ---------------------------------------------------------------------------
# Budget / eviction
# ---------------------------------------------------------------------------
async def _maybe_evict() -> None:
    budget = _budget_bytes()
    async with _extents_lock:
        total = sum(int(e.get("bytes", 0)) for e in _extents.values())
        if total <= budget:
            return
        victims = sorted(_extents.items(), key=lambda kv: kv[1].get("last_touch", 0))
        removed_paths = []
        for key, entry in victims:
            if total <= budget:
                break
            total -= int(entry.get("bytes", 0))
            removed_paths.append((key, entry["path"]))
            _extents.pop(key, None)
    for _key, path in removed_paths:
        await asyncio.to_thread(_unlink_safe, path)


def _unlink_safe(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _purge_all_range_files() -> None:
    """Delete every ``*.ranges.bin`` under the cache root (stale after restart)."""
    try:
        root = cache_root_dir()
        if not root.exists():
            return
        count = 0
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                if fn.endswith(_RANGE_SUFFIX):
                    try:
                        os.unlink(os.path.join(dirpath, fn))
                        count += 1
                    except Exception:
                        pass
        if count:
            LOGGER.info("Spill cache: swept %d stale range files at startup", count)
    except Exception as exc:
        LOGGER.debug("Spill cache startup sweep failed: %s", exc)
