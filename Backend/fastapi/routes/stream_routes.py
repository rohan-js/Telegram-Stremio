import secrets
import mimetypes
import time
from typing import Dict, Optional
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse

from collections import deque

from Backend import db
from Backend.helper.encrypt import decode_string
from Backend.helper.analytics import client_ip_from, record_stream_start
from Backend.helper.exceptions import InvalidHash
from Backend.helper.custom_dl import (
    ByteStreamer,
    ACTIVE_STREAMS,
    RECENT_STREAMS,
    TAIL_CACHE,
    prefetch_file_tail,
    HEAD_CACHE,
    prefetch_stream_head,
    SEEK_CACHE,
    prefetch_seek_window,
    client_dc_avg_mbps,
    client_dc_ttfb_sec,
    client_dc_last_seen,
    smart_client_score,
    get_client_cooldown_state,
    is_client_cooled_down,
    record_route_failure,
)
from Backend.helper.virtual_dl import resolve_virtual_parts, virtual_stream_generator
from Backend.helper.zip_stream import resolve_zip_entry
from Backend.helper.disk_cache import (
    disk_cache_enabled,
    cache_abspath,
    cache_root_dir,
    is_complete_cache_file,
    touch_cache_file,
    nginx_accel_enabled,
    nginx_accel_redirect_uri,
    first_cache_bytes,
    first_cache_enabled,
    first_cache_relpath,
    first_cache_abspath,
    is_complete_first_cache,
    get_first_cache_available_bytes,
    evict_lru,
)
from Backend.helper import media_index, spill_cache
from Backend.helper.torrent_downloads import (
    download_root_dir,
    guess_mime_type,
    nginx_download_redirect_uri,
    safe_download_file_path,
)
from Backend.pyrofork.bot import (
    StreamBot,
    USERBOT_CLIENT_INDEX,
    Userbot,
    work_loads,
    multi_clients,
    client_dc_map,
    client_failures,
    client_avg_mbps,
    client_cooldowns,
    client_dc_cooldowns,
)
from Backend.config import Telegram
from Backend.logger import LOGGER
from Backend.fastapi.security.tokens import enforce_playback_token, verify_token
import asyncio
from pyrogram.file_id import FileId

router = APIRouter(tags=["Streaming"])

_streamer_by_client: Dict = {}
_failure_decay_started: bool = False

_title_cache: Dict[str, tuple] = {}
_TITLE_CACHE_TTL = 300


async def _lookup_title(stream_id_hash: str, decoded_name: str) -> str:
    """Resolve a stream title from the TTL cache, DB, or the decoded URL name."""
    if not stream_id_hash:
        return decoded_name
    now = time.time()
    cached = _title_cache.get(stream_id_hash)
    if cached and now < cached[1]:
        return cached[0] or decoded_name
    db_title = await db.get_title_by_stream_id(stream_id_hash)
    _title_cache[stream_id_hash] = (db_title, now + _TITLE_CACHE_TTL)
    return db_title or decoded_name


def _content_disposition(file_name, disposition="inline"):
    ascii_fallback = str(file_name).encode("ascii", "ignore").decode("ascii").replace('"', "").strip() or "file"
    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(str(file_name), safe='')}"

VIDEO_MIME_BY_EXTENSION = {
    ".mkv": "video/x-matroska",
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".webm": "video/webm",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
    ".flv": "video/x-flv",
    ".wmv": "video/x-ms-wmv",
    ".ts": "video/mp2t",
}


def resolve_video_mime_type(file_name: str | None, telegram_mime: str | None = None) -> str:
    ext = Path(file_name or "").suffix.lower()
    if ext in VIDEO_MIME_BY_EXTENSION:
        return VIDEO_MIME_BY_EXTENSION[ext]

    telegram_mime = (telegram_mime or "").split(";", 1)[0].strip().lower()
    if telegram_mime.startswith("video/"):
        return telegram_mime

    guessed_mime = mimetypes.guess_type(file_name or "")[0]
    return guessed_mime or "application/octet-stream"


def make_json_safe(obj):
    if isinstance(obj, deque):
        return list(obj)
    if isinstance(obj, (set, tuple)):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    return obj


SUBTITLE_MIME_BY_EXTENSION = {
    ".srt": "application/x-subrip; charset=utf-8",
    ".vtt": "text/vtt; charset=utf-8",
    ".ass": "text/x-ssa; charset=utf-8",
    ".ssa": "text/x-ssa; charset=utf-8",
    ".sub": "text/plain; charset=utf-8",
}


def subtitle_mime_type(name: str | None) -> str:
    ext = Path(name or "").suffix.lower()
    return SUBTITLE_MIME_BY_EXTENSION.get(ext, "text/plain; charset=utf-8")


def parse_range_header(range_header: str, file_size: int):
    """
    Parse HTTP Range header.

    Supports:
    bytes=1000-2000
    bytes=1000-
    bytes=-2000
    """
    if not range_header:
        return 0, file_size - 1

    try:
        value = range_header.replace("bytes=", "").strip()
        start_str, end_str = value.split("-")

        if start_str == "":
            length = int(end_str)
            start = file_size - length
            end = file_size - 1
        elif end_str == "":
            start = int(start_str)
            end = file_size - 1
        else:
            start = int(start_str)
            end = int(end_str)

    except Exception:
        raise HTTPException(
            status_code=416,
            detail="Invalid Range header",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    if start < 0:
        start = 0

    if end >= file_size:
        end = file_size - 1

    if end < start:
        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    return start, end


def should_probe_request(range_header: str, start: int) -> bool:
    """
    Decide whether a stream request should run the live smart-routing probe.

    Session-open and suffixed-range requests still probe (the fastest route
    matters for first-frame). Mid-file seeks (start > 0) and HEAD requests
    skip the probe: the historical best-known client is used directly and the
    per-chunk fallback machinery still rescues on failure.
    """
    if bool(range_header) and start > 0:
        return False
    return True


def _client_route_trusted(client_index: int, target_dc: int) -> bool:
    """True when (client, DC) recently produced a successful fetch — skip the live probe.

    The probe costs up to SMART_ROUTING_PROBE_TIMEOUT_SEC on a cold route; for
    repeat opens (same helper already streaming this DC) the historical route is
    trusted instead so the first frame arrives after a single round-trip.
    """
    try:
        trust_sec = float(getattr(Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0))
    except Exception:
        trust_sec = 60.0
    if trust_sec <= 0:
        return False
    now = time.time()
    last_seen = client_dc_last_seen.get((int(client_index), int(target_dc or 0)))
    if last_seen is not None and (now - float(last_seen)) <= trust_sec:
        return True
    # DC-level fallback: if ANY helper recently proved this file's DC is
    # reachable, skip re-probing. The base client itself may simply carry a
    # colder session — one session handshake is cheaper than 3 fresh probes,
    # and this closes the fresh-boot gap where the first open's background
    # probe hasn't finished stamping the base client yet.
    dc = int(target_dc or 0)
    if dc > 0:
        for (_, seen_dc), seen in client_dc_last_seen.items():
            if int(seen_dc or 0) == dc and (now - float(seen)) <= trust_sec:
                return True
    return False


async def stream_file_range_with_usage(
    path: Path | str,
    start_pos: int,
    end_pos: int,
    token: str | None = None,
    read_size: int = 1024 * 1024,
):
    import aiofiles

    remaining = (end_pos - start_pos) + 1
    pending_usage = 0
    async with aiofiles.open(path, "rb") as f:
        await f.seek(start_pos)
        try:
            while remaining > 0:
                chunk = await f.read(min(read_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                pending_usage += len(chunk)
                if token and pending_usage >= 8 * 1024 * 1024:
                    await db.update_token_usage(token, pending_usage)
                    pending_usage = 0
                yield chunk
        finally:
            if token and pending_usage > 0:
                await db.update_token_usage(token, pending_usage)


# In-flight first-N-MiB head-cache fills (dedup by cache relpath).
_first_fill_inflight: set = set()


async def _fill_first_cache_head(
    chat_id: int,
    msg_id: int,
    unique_id: str,
    expected_bytes: int,
    client_index: int,
    target_bytes: Optional[int] = None,
) -> None:
    """Download the file's head (first expected_bytes) from Telegram to disk.

    Runs fully in the background — a failure never affects the live stream.
    The prefix file lives under the same LRU-bounded cache root, so the shared
    DISK_CACHE_MAX_BYTES budget and eviction apply. ``target_bytes`` (runway
    head-boost) may raise the fill size above the default first-MB window.
    """
    rel = first_cache_relpath(chat_id, msg_id, unique_id)
    if rel in _first_fill_inflight:
        return
    _first_fill_inflight.add(rel)
    try:
        streamer = get_streamer(client_index)
        fid = await streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)
        size = int(getattr(fid, "file_size", 0) or 0)
        if size <= 0:
            return
        want = min(int(target_bytes or expected_bytes), size)
        if want <= 0:
            return
        dest = first_cache_abspath(chat_id, msg_id, unique_id)
        if get_first_cache_available_bytes(dest) >= want:
            return
        location = await streamer._get_location(fid)
        media_session = await streamer._get_media_session(fid)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
        except Exception:
            pass

        written = 0
        chunk_size = 512 * 1024
        try:
            with open(str(tmp), "wb") as f:
                while written < want:
                    limit = min(chunk_size, want - written)
                    data = await streamer._fetch_file_bytes(
                        media_session,
                        location,
                        offset=written,
                        limit=limit,
                    )
                    if not data:
                        break
                    f.write(data)
                    written += len(data)
            if written == want:
                tmp.replace(dest)
                touch_cache_file(dest)
                LOGGER.info(
                    "First-cache filled chat_id=%s msg_id=%s (%s bytes)",
                    chat_id, msg_id, written,
                )
                await evict_lru()
            else:
                LOGGER.debug("First-cache fill short chat_id=%s msg_id=%s (%s/%s)", chat_id, msg_id, written, want)
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception as exc:
            LOGGER.warning("First-cache fill failed chat_id=%s msg_id=%s: %s", chat_id, msg_id, exc)
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as exc:
        LOGGER.debug("First-cache fill skipped chat_id=%s msg_id=%s: %s", chat_id, msg_id, exc)
    finally:
        _first_fill_inflight.discard(rel)


@router.get("/sub/{token}/{id}/{name}")
async def subtitle_handler(token: str, id: str, name: str, token_data: dict = Depends(verify_token)):
    enforce_playback_token(token_data)
    try:
        decoded = await decode_string(id)
    except InvalidHash:
        raise HTTPException(status_code=403, detail="Invalid subtitle token")

    if not isinstance(decoded, dict) or decoded.get("source_type") != "subtitle":
        raise HTTPException(status_code=400, detail="Invalid subtitle source")

    chat_id = int(f"-100{str(decoded['chat_id']).replace('-100', '')}")
    msg_id = int(decoded["msg_id"])
    try:
        message = await StreamBot.get_messages(chat_id, msg_id)
        if not message or not (message.document or message.video):
            raise HTTPException(status_code=404, detail="Subtitle message not found")
        file_obj = message.document or message.video
        file_name = getattr(file_obj, "file_name", None) or name or "subtitle.srt"
        downloaded = await StreamBot.download_media(message, in_memory=True)
        downloaded.seek(0)
        data = downloaded.read()
        try:
            downloaded.close()
        except Exception:
            pass
        return StreamingResponse(
            iter([data]),
            media_type=subtitle_mime_type(file_name),
            headers={
                "Content-Disposition": _content_disposition(Path(file_name).name),
                "Cache-Control": "private, max-age=3600",
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.error("Subtitle delivery failed chat=%s msg=%s: %s", chat_id, msg_id, exc)
        raise HTTPException(status_code=502, detail="Subtitle fetch failed")


def select_best_client(target_dc: int) -> int:
    """Pick the best available helper using DC-aware live performance."""
    if multi_clients:
        available = [idx for idx in multi_clients.keys() if not is_client_cooled_down(idx, target_dc)]
        pool = available or list(multi_clients.keys())
        selected = min(pool, key=lambda idx: smart_client_score(idx, target_dc))
        LOGGER.debug(
            "Selected client %s (DC %s) score=%s",
            selected, client_dc_map.get(selected, "?"), smart_client_score(selected, target_dc),
        )
        return selected

    return 0


def get_streamer(index: int) -> ByteStreamer:
    if index == USERBOT_CLIENT_INDEX:
        if Userbot is None:
            raise HTTPException(status_code=503, detail="Global Search userbot is not configured")
        work_loads.setdefault(index, 0)
        client_failures.setdefault(index, 0)
        client_avg_mbps.setdefault(index, 0.0)
        if Userbot not in _streamer_by_client:
            _streamer_by_client[Userbot] = ByteStreamer(Userbot, index)
        return _streamer_by_client[Userbot]
    tg_client = multi_clients[index]
    if tg_client not in _streamer_by_client:
        _streamer_by_client[tg_client] = ByteStreamer._instances.get(index) or ByteStreamer(tg_client, index)
    return _streamer_by_client[tg_client]


def select_probe_candidates(target_dc: int, base_index: int) -> list[int]:
    limit = max(1, min(int(getattr(Telegram, "SMART_ROUTING_PROBE_CLIENTS", 3) or 3), len(multi_clients)))
    ranked = sorted(
        [idx for idx in multi_clients.keys() if not is_client_cooled_down(idx, target_dc)],
        key=lambda idx: smart_client_score(idx, target_dc),
    )
    if not ranked:
        ranked = sorted(multi_clients.keys(), key=lambda idx: smart_client_score(idx, target_dc))
    candidates = []
    if base_index in multi_clients and not is_client_cooled_down(base_index, target_dc):
        candidates.append(base_index)
    for idx in ranked:
        if idx not in candidates:
            candidates.append(idx)
        if len(candidates) >= limit:
            break
    return candidates


async def choose_smart_client(
    request: Request,
    chat_id: int,
    msg_id: int,
    target_dc: int,
    base_index: int,
    probe_offset: int,
) -> tuple[int, ByteStreamer, FileId, list[dict]]:
    streamer = get_streamer(base_index)
    file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)

    if (
        request.method == "HEAD"
        or not getattr(Telegram, "SMART_ROUTING_ENABLED", True)
        or not getattr(Telegram, "SMART_ROUTING_PROBE_ENABLED", True)
        or len(multi_clients) <= 1
    ):
        await streamer._get_media_session(file_id)
        return base_index, streamer, file_id, []

    probe_size = int(getattr(Telegram, "SMART_ROUTING_PROBE_BYTES", 32768) or 32768)
    probe_timeout = float(getattr(Telegram, "SMART_ROUTING_PROBE_TIMEOUT_SEC", 4.0) or 4.0)
    candidates = select_probe_candidates(target_dc or getattr(file_id, "dc_id", 0), base_index)

    async def _probe(idx: int) -> dict:
        candidate_streamer = get_streamer(idx)
        result = await candidate_streamer.probe_file(
            chat_id=chat_id,
            message_id=msg_id,
            offset=probe_offset,
            limit=probe_size,
            timeout=probe_timeout,
        )
        if not result.get("ok"):
            record_route_failure(
                idx,
                target_dc,
                result.get("error") or "probe_failed",
                stream_id=None,
                offset=probe_offset,
                attempt=1,
            )
        return result

    probe_results = await asyncio.gather(*[_probe(idx) for idx in candidates], return_exceptions=True)
    clean_results = []
    for idx, result in zip(candidates, probe_results):
        if isinstance(result, Exception):
            record_route_failure(
                idx,
                target_dc,
                f"probe_exception:{type(result).__name__}",
                stream_id=None,
                offset=probe_offset,
                attempt=1,
            )
            clean_results.append({"client_index": idx, "ok": False, "error": str(result)})
        else:
            clean_results.append(result)

    ok_results = [r for r in clean_results if r.get("ok") and r.get("file_id") is not None]
    if not ok_results:
        LOGGER.warning("Smart routing probe found no usable helper for msg=%s dc=%s", msg_id, target_dc)
        await streamer._get_media_session(file_id)
        return base_index, streamer, file_id, clean_results

    # A successful probe is itself proof the (client, DC) route works, so stamp
    # every probed helper as recently-seen. Without this the trust window only
    # knows about the client that actually streamed chunks, which is rarely the
    # base client picked on the next open — so repeat opens kept re-probing.
    now = time.time()
    for r in ok_results:
        try:
            client_dc_last_seen[(int(r["client_index"]), int(target_dc or 0))] = now
        except Exception:
            pass

    best = min(
        ok_results,
        key=lambda r: (
            float(r.get("ttfb_sec") or 999.0),
            -float(r.get("mbps") or 0.0),
            work_loads.get(int(r.get("client_index")), 0),
        ),
    )
    best_index = int(best["client_index"])
    best_streamer = get_streamer(best_index)
    LOGGER.info(
        "Smart routing selected client=%s target_dc=%s ttfb=%.3fs probe_mibps=%.3f candidates=%s",
        best_index,
        best.get("target_dc") or target_dc,
        float(best.get("ttfb_sec") or 0.0),
        float(best.get("mbps") or 0.0),
        [
            {
                "client": r.get("client_index"),
                "ok": r.get("ok"),
                "ttfb": round(float(r.get("ttfb_sec") or 0.0), 3) if r.get("ttfb_sec") else None,
                "mibps": round(float(r.get("mbps") or 0.0), 3) if r.get("mbps") else None,
            }
            for r in clean_results
        ],
    )
    return best_index, best_streamer, best["file_id"], clean_results


async def decay_client_failures() -> None:
    """Every 5 minutes reduce each client's failure count by 1 (floor 0).

    This lets bots self-recover after a temporary DC issue without manual
    intervention.  The coroutine is started once as a background task on
    first import.
    """
    while True:
        await asyncio.sleep(300)  # 5 minutes
        for k in list(client_failures):
            if client_failures.get(k, 0) > 0:
                client_failures[k] = max(0, client_failures[k] - 1)
                LOGGER.debug("Failure decay: client %s failures → %s", k, client_failures[k])
        now = time.time()
        for k in list(client_cooldowns):
            if float(client_cooldowns.get(k) or 0.0) <= now:
                client_cooldowns.pop(k, None)
        for k in list(client_dc_cooldowns):
            if float(client_dc_cooldowns.get(k) or 0.0) <= now:
                client_dc_cooldowns.pop(k, None)


def get_mem_available_mb() -> int | None:
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(int(line.split()[1]) / 1024)
    except Exception:
        return None
    return None


def count_active_telegram_streams() -> int:
    return sum(1 for info in ACTIVE_STREAMS.values() if info.get("status", "active") == "active")


def get_configured_stream_concurrency() -> tuple[int, int]:
    """Return (prefetch, parallelism) from env-backed Telegram config."""
    prefetch = max(1, int(getattr(Telegram, "PRE_FETCH", 1) or 1))
    parallelism = max(1, int(getattr(Telegram, "PARALLEL", 1) or 1))
    return prefetch, parallelism


def select_telegram_chunk_size(range_header: str | None, start: int = 0, req_length: int | None = None) -> int:
    """Use smaller chunks for probe/seek requests and 1 MB for full streams."""
    if range_header:
        ramp_up_kb = max(64, int(getattr(Telegram, "STREAM_RAMP_UP_CHUNK_KB", 256) or 256))
        if start == 0 and req_length is not None and req_length <= ramp_up_kb * 1024:
            return ramp_up_kb * 1024
        return 512 * 1024
    return ByteStreamer.CHUNK_SIZE


def choose_effective_prefetch(
    configured_prefetch: int,
    configured_parallelism: int,
    *,
    file_size: int,
    request_length: int,
    active_streams: int,
    mem_available_mb: int | None,
) -> tuple[int, int, str]:
    prefetch = max(1, int(configured_prefetch or 1))
    parallelism = max(1, int(configured_parallelism or 1))

    if not getattr(Telegram, "ADAPTIVE_PREFETCH_ENABLED", True):
        return prefetch, parallelism, "disabled"

    low_mem_limit = int(getattr(Telegram, "ADAPTIVE_PREFETCH_LOW_MEM_MB", 150) or 150)
    multi_limit = int(getattr(Telegram, "ADAPTIVE_PREFETCH_MULTI_STREAM_THRESHOLD", 2) or 2)
    small_req = int(getattr(Telegram, "ADAPTIVE_PREFETCH_SMALL_REQUEST_BYTES", 16 * 1024 * 1024) or 16 * 1024 * 1024)
    small_file = int(getattr(Telegram, "ADAPTIVE_PREFETCH_SMALL_FILE_BYTES", 64 * 1024 * 1024) or 64 * 1024 * 1024)

    if mem_available_mb is not None and mem_available_mb < low_mem_limit:
        return min(prefetch, 1), min(parallelism, 1), f"low_mem:{mem_available_mb}mb"

    if request_length <= small_req or file_size <= small_file:
        return min(prefetch, 1), min(parallelism, 1), "small_request"

    if active_streams >= multi_limit:
        return min(prefetch, 2), min(parallelism, 2), f"multi_stream:{active_streams}"

    return prefetch, parallelism, "healthy"



async def track_usage_from_stats(stream_id: str, token: str, token_data: dict):
    await asyncio.sleep(2)
    
    limits = token_data.get("limits", {}) if token_data else {}
    usage = token_data.get("usage", {}) if token_data else {}
    
    daily_limit_gb = limits.get("daily_limit_gb")
    monthly_limit_gb = limits.get("monthly_limit_gb")
    
    initial_daily_bytes = usage.get("daily", {}).get("bytes", 0)
    initial_monthly_bytes = usage.get("monthly", {}).get("bytes", 0)
    
    last_tracked_bytes = 0
    update_interval = 10
    
    try:
        while True:
            await asyncio.sleep(update_interval)
            stream_info = ACTIVE_STREAMS.get(stream_id)
            if not stream_info:
                for rec in RECENT_STREAMS:
                    if rec.get("stream_id") == stream_id:
                        final_bytes = rec.get("total_bytes", 0)
                        delta = final_bytes - last_tracked_bytes
                        if delta > 0:
                            try:
                                await db.update_token_usage(token, delta)
                                LOGGER.debug(f"Final usage update for {stream_id}: {delta} bytes")
                            except Exception as e:
                                LOGGER.error(f"Final usage update failed: {e}")
                        break
                return
            
            current_bytes = stream_info.get("total_bytes", 0)
            delta = current_bytes - last_tracked_bytes
            
            if delta > 0:
                try:
                    await db.update_token_usage(token, delta)
                    last_tracked_bytes = current_bytes
                    LOGGER.debug(f"Updated usage for {stream_id}: +{delta} bytes (total: {current_bytes})")
                except Exception as e:
                    LOGGER.error(f"Periodic usage update failed: {e}")
            
            # Check limits (don't stop stream, just log - client manages connection)
            if daily_limit_gb and daily_limit_gb > 0:
                current_daily_gb = (initial_daily_bytes + current_bytes) / (1024 ** 3)
                if current_daily_gb >= daily_limit_gb:
                    LOGGER.debug(f"Daily limit reached for token, stream {stream_id} may be blocked by verify_token")
            
            if monthly_limit_gb and monthly_limit_gb > 0:
                current_monthly_gb = (initial_monthly_bytes + current_bytes) / (1024 ** 3)
                if current_monthly_gb >= monthly_limit_gb:
                    LOGGER.debug(f"Monthly limit reached for token, stream {stream_id} may be blocked by verify_token")
                    
    except asyncio.CancelledError:
        stream_info = ACTIVE_STREAMS.get(stream_id)
        if stream_info:
            current_bytes = stream_info.get("total_bytes", 0)
            delta = current_bytes - last_tracked_bytes
            if delta > 0:
                try:
                    await db.update_token_usage(token, delta)
                    LOGGER.info(f"Cancelled - final update for {stream_id}: {delta} bytes")
                except Exception as e:
                    LOGGER.error(f"Cancelled usage update failed: {e}")


@router.get("/downloaded/{token}/{id}/{name}")
@router.head("/downloaded/{token}/{id}/{name}")
async def downloaded_torrent_stream_handler(
    request: Request,
    token: str,
    id: str,
    name: str,
    token_data: dict = Depends(verify_token),
):
    enforce_playback_token(token_data)
    decoded = await decode_string(id)
    if decoded.get("source_type") not in {"downloaded_torrent", "local_vps"}:
        raise HTTPException(status_code=400, detail="Invalid downloaded stream id")

    rel_path = decoded.get("rel_path")
    if not rel_path:
        raise HTTPException(status_code=400, detail="Missing downloaded file path")

    try:
        file_path = safe_download_file_path(download_root_dir(), rel_path)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid downloaded file path")

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Downloaded file not found")

    file_size = file_path.stat().st_size
    range_header = request.headers.get("Range", "")
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1

    file_name = Path(str(decoded.get("name") or file_path.name)).name or file_path.name
    mime_type = guess_mime_type(file_path)
    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": _content_disposition(file_name),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }

    from fastapi.responses import Response as PlainResponse

    if getattr(Telegram, "NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED", True):
        # Nginx sends the body after this internal redirect. Counting the full
        # requested range here overcounts badly when players request "bytes=N-"
        # and disconnect before reading the rest of a large file.
        # Do not set Content-Length/Content-Range here: this response has no
        # app body, and h11 will reject a non-zero declared length before nginx
        # can serve the redirected file.
        headers["X-Accel-Redirect"] = nginx_download_redirect_uri(rel_path)
        return PlainResponse(status_code=200, headers=headers)

    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    if range_header or request.method == "HEAD":
        headers["Content-Length"] = str(req_length)

    if request.method == "HEAD":
        return PlainResponse(status_code=206 if range_header else 200, headers=headers)

    return StreamingResponse(
        stream_file_range_with_usage(file_path, start, end, token),
        headers=headers,
        status_code=206 if range_header else 200,
        media_type=mime_type,
    )


@router.get("/dl/{token}/{id}/{name}")
@router.head("/dl/{token}/{id}/{name}")
async def stream_handler(
    request: Request,
    token: str,
    id: str,
    name: str,
    token_data: dict = Depends(verify_token),
):
    enforce_playback_token(token_data)
    asyncio.create_task(record_stream_start(
        token,
        token_data.get("name") or "Unknown",
        client_ip_from(request),
        request.headers.get("user-agent"),
    ))
    decoded = await decode_string(id)
    if decoded.get("zip"):
        if decoded.get("global"):
            return await global_zip_media_streamer(
                request=request,
                parts_payload=decoded["parts"],
                token=token,
                token_data=token_data,
                stream_id_hash=id,
            )
        return await db_zip_media_streamer(
            request=request,
            parts_payload=decoded["parts"],
            token=token,
            token_data=token_data,
            stream_id_hash=id,
        )
    if decoded.get("parts"):
        return await virtual_media_streamer(
            request=request,
            parts_payload=decoded["parts"],
            token=token,
            token_data=token_data,
            stream_id_hash=id,
        )

    msg_id = decoded.get("msg_id")
    if not msg_id:
        raise HTTPException(status_code=400, detail="Missing id")

    if decoded.get("global"):
        if Userbot is None:
            raise HTTPException(status_code=503, detail="Global Search userbot is not configured")
        chat_id = int(decoded["chat_id"])
        message = await Userbot.get_messages(chat_id, int(msg_id))
        source_type = "global_search"
        forced_client_index = USERBOT_CLIENT_INDEX
        file = message.video or message.document
        if not file:
            raise HTTPException(status_code=404, detail="No media found")
        secure_hash = file.file_unique_id[:6]
        try:
            target_dc = FileId.decode(file.file_id).dc_id
        except Exception:
            target_dc = None
    else:
        chat_id = int(f"-100{decoded['chat_id']}")
        source_type = "telegram"
        forced_client_index = None
        base_streamer = get_streamer(0)
        file_id = await base_streamer.get_file_properties(chat_id=chat_id, message_id=int(msg_id))
        secure_hash = getattr(file_id, "unique_id", "")[:6]
        target_dc = getattr(file_id, "dc_id", None)

    return await media_streamer(
        request=request,
        chat_id=chat_id,
        msg_id=int(msg_id),
        secure_hash=secure_hash,
        token=token,
        token_data=token_data,
        stream_id_hash=id,
        target_dc=target_dc,
        forced_client_index=forced_client_index,
        source_type=source_type,
    )

async def virtual_media_streamer(
    request: Request,
    parts_payload: list,
    token: str,
    token_data: dict = None,
    stream_id_hash: str = None,
):
    base_index = select_best_client(0)
    streamer = get_streamer(base_index)
    parts, file_size = await resolve_virtual_parts(parts_payload, streamer)
    if not parts or file_size <= 0:
        raise HTTPException(status_code=404, detail="Split media parts not found")

    range_header = request.headers.get("Range", "")
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1
    chunk_size = select_telegram_chunk_size(range_header)

    first_file_id = parts[0]["file_id"]
    target_dc = int(getattr(first_file_id, "dc_id", 0) or 0)
    index = select_best_client(target_dc)
    streamer = get_streamer(index)

    file_name = first_file_id.file_name or f"{secrets.token_hex(4)}.bin"
    mime_type = resolve_video_mime_type(file_name, first_file_id.mime_type)
    if "." not in file_name and "/" in mime_type:
        file_name = f"{file_name}.{mime_type.split('/')[1]}"

    from urllib.parse import unquote

    stream_id = secrets.token_hex(8)
    decoded_name = unquote(request.path_params.get("name", ""))
    final_title = await _lookup_title(stream_id_hash, decoded_name)

    configured_prefetch, configured_parallelism = get_configured_stream_concurrency()
    active_streams = count_active_telegram_streams()
    mem_available_mb = get_mem_available_mb()
    prefetch_count, parallelism, prefetch_reason = choose_effective_prefetch(
        configured_prefetch,
        configured_parallelism,
        file_size=file_size,
        request_length=req_length,
        active_streams=active_streams,
        mem_available_mb=mem_available_mb,
    )

    meta = {
        "request_path": str(request.url.path),
        "request_range": range_header or None,
        "request_start": start,
        "request_end": end,
        "request_length": req_length,
        "client_host": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
        "title": final_title,
        "filename": file_name,
        "source_type": "telegram_split",
        "token": token,
        "token_user_id": token_data.get("user_id") if token_data else None,
        "split_parts": len(parts),
        "user_name": token_data.get("name", "Unknown") if token_data else "Unknown",
        "adaptive_prefetch": {
            "configured_prefetch": configured_prefetch,
            "configured_parallelism": configured_parallelism,
            "effective_prefetch": prefetch_count,
            "effective_parallelism": parallelism,
            "reason": prefetch_reason,
            "active_streams": active_streams,
            "mem_available_mb": mem_available_mb,
        },
        "smart_routing": {
            "target_dc": target_dc,
            "selected_client": index,
            "probe_results": [],
        },
    }

    from fastapi.responses import Response as PlainResponse

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": _content_disposition(file_name),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status = 206
    else:
        status = 200

    if request.method == "HEAD":
        headers["Content-Length"] = str(req_length)
        return PlainResponse(status_code=status, headers=headers)

    body_gen = virtual_stream_generator(
        parts=parts,
        start=start,
        end=end,
        chunk_size=chunk_size,
        streamer=streamer,
        client_index=index,
        request=request,
        meta=meta,
        stream_id=stream_id,
        parallelism=parallelism,
        prefetch_count=prefetch_count,
    )

    asyncio.create_task(track_usage_from_stats(stream_id, token, token_data))
    return StreamingResponse(
        body_gen,
        headers=headers,
        status_code=status,
        media_type=mime_type,
    )

#----- Read a byte range from the concatenated virtual parts into memory
async def _read_virtual_range(parts, start, length, streamer, request, client_index=USERBOT_CLIENT_INDEX, parallelism=1, prefetch_count=1):
    buf = bytearray()
    gen = virtual_stream_generator(
        parts=parts, start=start, end=start + length - 1, chunk_size=1024 * 1024,
        streamer=streamer, client_index=client_index, request=request,
        meta={"title": "zip-index", "user_name": "system", "token": ""},
        stream_id=secrets.token_hex(6), parallelism=parallelism, prefetch_count=prefetch_count,
    )
    try:
        async for chunk in gen:
            buf.extend(chunk)
            if len(buf) >= length:
                break
    finally:
        await gen.aclose()
    return bytes(buf[:length])


#----- Stream a split ZIP archive (.zip.001/.002 ...) as its inner video, with seeking.
#----- Only STORED (uncompressed) archives are seekable; the inner file bytes are served
#----- directly at their offset inside the concatenated zip (no stream-unzip needed).
async def _zip_media_streamer(request, parts_payload, token, token_data, stream_id_hash, streamer, client_index, parallelism=None, prefetch_count=None):
    if streamer is None:
        raise HTTPException(status_code=503, detail="ZIP streaming is unavailable (no client/session)")

    parts, zip_size = await resolve_virtual_parts(parts_payload, streamer)
    if not parts or zip_size <= 0:
        raise HTTPException(status_code=404, detail="Split archive parts not accessible")

    async def _read(off, length):
        return await _read_virtual_range(parts, off, length, streamer, request, client_index, parallelism or 1, prefetch_count or 1)

    entry = await resolve_zip_entry(_read, zip_size)
    if not entry:
        raise HTTPException(status_code=415, detail="Unreadable or incomplete split archive")
    if entry["method"] != 0:
        raise HTTPException(
            status_code=415,
            detail="This archive is compressed; only stored (uncompressed) ZIP archives can be seek-streamed.",
        )

    inner_size = entry["size"]
    data_offset = entry["data_offset"]
    if inner_size <= 0 or data_offset + inner_size > zip_size:
        raise HTTPException(status_code=415, detail="Split archive has an unexpected layout")

    range_header = request.headers.get("Range", "")
    start, end = parse_range_header(range_header, inner_size)
    req_length = end - start + 1
    stream_id = secrets.token_hex(8)

    if parallelism is None or prefetch_count is None:
        configured_prefetch, configured_parallelism = get_configured_stream_concurrency()
        active_streams = count_active_telegram_streams()
        mem_available_mb = get_mem_available_mb()
        prefetch_count, parallelism, _prefetch_reason = choose_effective_prefetch(
            configured_prefetch,
            configured_parallelism,
            file_size=inner_size,
            request_length=req_length,
            active_streams=active_streams,
            mem_available_mb=mem_available_mb,
        )

    from urllib.parse import unquote

    inner_name = (entry.get("name") or "").split("/")[-1] or unquote(request.path_params.get("name", "")) or "video.mkv"
    mime_type = resolve_video_mime_type(inner_name) or "video/x-matroska"

    db_title = await db.get_title_by_stream_id(stream_id_hash) if stream_id_hash else None
    final_title = db_title if db_title else inner_name

    meta = {
        "request_path": str(request.url.path),
        "request_range": range_header or None,
        "request_start": start,
        "request_end": end,
        "request_length": req_length,
        "client_host": request.client.host if request.client else None,
        "title": final_title,
        "filename": inner_name,
        "source_type": "zip_parts",
        "token": token,
        "token_user_id": token_data.get("user_id") if token_data else None,
        "zip_parts": len(parts),
        "user_name": token_data.get("name", "Unknown") if token_data else "Unknown",
    }

    from fastapi.responses import Response as PlainResponse

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": _content_disposition(inner_name),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{inner_size}"
        status = 206
    else:
        status = 200

    if request.method == "HEAD":
        headers["Content-Length"] = str(req_length)
        return PlainResponse(status_code=status, headers=headers)

    body_gen = virtual_stream_generator(
        parts=parts, start=data_offset + start, end=data_offset + end, chunk_size=select_telegram_chunk_size(range_header),
        streamer=streamer, client_index=client_index, request=request, meta=meta,
        stream_id=stream_id, parallelism=parallelism, prefetch_count=prefetch_count,
    )
    asyncio.create_task(track_usage_from_stats(stream_id, token, token_data))
    return StreamingResponse(body_gen, headers=headers, status_code=status, media_type=mime_type)


#----- ZIP split from Global Search (streamed via the Userbot session)
async def global_zip_media_streamer(request: Request, parts_payload: list, token: str, token_data: dict = None, stream_id_hash: str = None):
    if Userbot is None:
        raise HTTPException(status_code=503, detail="Global Search streaming is unavailable (no Userbot connected)")
    return await _zip_media_streamer(
        request, parts_payload, token, token_data, stream_id_hash,
        get_streamer(USERBOT_CLIENT_INDEX), USERBOT_CLIENT_INDEX, 1, 1,
    )


#----- ZIP split from the indexed library (streamed via the multi-bot pool)
async def db_zip_media_streamer(request: Request, parts_payload: list, token: str, token_data: dict = None, stream_id_hash: str = None):
    index = select_best_client(0)
    streamer = get_streamer(index)
    return await _zip_media_streamer(
        request, parts_payload, token, token_data, stream_id_hash,
        streamer, index, None, None,
    )

def _dc_feed_capacity_bps(target_dc: int) -> Optional[float]:
    """Best measured sustained feed speed (bytes/sec) for a DC, else global best."""
    try:
        best: Optional[float] = None
        for (_idx, dc), mbps in client_dc_avg_mbps.items():
            if int(dc) == int(target_dc) and mbps and mbps > 0:
                best = max(best or 0.0, float(mbps))
        if best is None:
            vals = [float(v) for v in client_avg_mbps.values() if v and v > 0]
            best = max(vals) if vals else None
        if best is None or best <= 0:
            return None
        return best * 1024 * 1024
    except Exception:
        return None


async def media_streamer(
    request: Request,
    chat_id: int,
    msg_id: int,
    secure_hash: str,
    token: str,
    token_data: dict = None,
    stream_id_hash: str = None,
    target_dc: int | None = None,
    forced_client_index: int | None = None,
    source_type: str = "telegram",
):
    global _failure_decay_started
    if not _failure_decay_started:
        try:
            asyncio.create_task(decay_client_failures())
            _failure_decay_started = True
        except Exception:
            pass

    base_index = forced_client_index if forced_client_index is not None else select_best_client(target_dc or 0)
    base_streamer = get_streamer(base_index)

    # Fetch one FileId first so we can parse Range and validate the media.
    file_id = await base_streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)

    if secure_hash != "SKIP_HASH_CHECK":  # Don't check this it is for my Webdav
        if file_id.unique_id[:6] != secure_hash:
            raise InvalidHash

    real_dc = file_id.dc_id
    if target_dc is None:
        target_dc = real_dc
    LOGGER.debug(f"File msg_id={msg_id} is in DC {real_dc}")

    file_size = file_id.file_size
    range_header = request.headers.get("Range", "")
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1

    file_name = file_id.file_name or f"{secrets.token_hex(4)}.bin"
    mime_type = resolve_video_mime_type(file_name, file_id.mime_type)

    if "." not in file_name and "/" in mime_type:
        file_name = f"{file_name}.{mime_type.split('/')[1]}"

    # ------------------------------------------------------------------
    # Runway bitrate guard — if the file's bitrate outruns the best known
    # feed speed for its DC, boost the first-MB head fill (30s runway) so
    # playback gets a buffer cushion instead of a minute-3 stall. Needs a
    # parsed container index (built at picker time / first open).
    # ------------------------------------------------------------------
    starved_bitrate = False
    head_fill_bytes: Optional[int] = None
    bitrate_bps: Optional[float] = None
    try:
        if bool(getattr(Telegram, "RUNWAY_PREFETCH_ENABLED", True)) and chat_id and msg_id:
            _idx = await media_index.get_media_index(chat_id, msg_id)
            if _idx is not None and _idx.duration_sec:
                bitrate_bps = float(file_size) / float(_idx.duration_sec)
                _feed_cap = _dc_feed_capacity_bps(int(target_dc or real_dc or 0))
                if _feed_cap and bitrate_bps * 1.3 > _feed_cap:
                    starved_bitrate = True
                    _boost_mb = min(
                        float(getattr(Telegram, "RUNWAY_HEAD_BOOST_MAX_MB", 60) or 60),
                        max(first_cache_bytes() / 1048576.0, (bitrate_bps * 30.0) / 1048576.0),
                    )
                    head_fill_bytes = int(_boost_mb * 1024 * 1024)
                    LOGGER.info(
                        "Runway guard: starved bitrate %.2f MB/s vs feed cap %.2f MB/s — head boost to %.0f MB",
                        bitrate_bps / 1048576.0, _feed_cap / 1048576.0, _boost_mb,
                    )
    except Exception as exc:
        LOGGER.debug("Runway guard skipped: %s", exc)

    # ------------------------------------------------------------------
    # Tail cache hit path (instant MKV Cues / MP4 Moov for ExoPlayer)
    # ------------------------------------------------------------------
    if bool(getattr(Telegram, "TELEGRAM_TAIL_CACHE_ENABLED", True)) and file_size > 524288 and chat_id and msg_id:
        tail_cache_kb = max(64, int(getattr(Telegram, "TELEGRAM_TAIL_CACHE_SIZE_KB", 256) or 256))
        tail_threshold = max(0, file_size - tail_cache_kb * 1024)
        if start >= tail_threshold:
            cached_tail = await TAIL_CACHE.get_tail(chat_id, msg_id, start, req_length)
            if cached_tail is not None and len(cached_tail) == req_length:
                LOGGER.debug("TailCache HIT (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, len(cached_tail))
                from fastapi.responses import Response as PlainResponse
                return PlainResponse(
                    content=cached_tail,
                    status_code=206,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{file_size}",
                        "Content-Length": str(len(cached_tail)),
                        "Content-Type": mime_type,
                        "Accept-Ranges": "bytes",
                        "Content-Disposition": _content_disposition(file_name),
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
                    },
                )

    # ------------------------------------------------------------------
    # Head cache hit path (instant Chunk 0 playback from Stream Picker pre-buffering)
    # ------------------------------------------------------------------
    if bool(getattr(Telegram, "STREAM_PICKER_PREBUFFER_ENABLED", True)) and chat_id and msg_id:
        head_cache_kb = max(64, int(getattr(Telegram, "STREAM_PICKER_PREBUFFER_SIZE_KB", 256) or 256))
        head_ceiling = head_cache_kb * 1024
        if start < head_ceiling and (req_length <= head_ceiling or end < head_ceiling):
            cached_head = await HEAD_CACHE.get_head(chat_id, msg_id, start, req_length)
            if cached_head is not None and len(cached_head) == req_length:
                LOGGER.info("HeadCache HIT (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, len(cached_head))
                from fastapi.responses import Response as PlainResponse
                return PlainResponse(
                    content=cached_head,
                    status_code=206,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{file_size}",
                        "Content-Length": str(len(cached_head)),
                        "Content-Type": mime_type,
                        "Accept-Ranges": "bytes",
                        "Content-Disposition": _content_disposition(file_name),
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
                    },
                )

    # ------------------------------------------------------------------
    # First-MB Disk Cache hit path (Tier 2 fast-start from NVMe disk)
    # ------------------------------------------------------------------
    unique_id = getattr(file_id, "unique_id", None) or ""
    if first_cache_enabled() and start < first_cache_bytes() and unique_id:
        fc_bytes = min(first_cache_bytes(), file_size)
        fc_path = first_cache_abspath(chat_id, msg_id, unique_id)
        if end < fc_bytes and get_first_cache_available_bytes(fc_path) >= fc_bytes:
            touch_cache_file(fc_path)
            LOGGER.info("FirstDiskCache HIT (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, req_length)
            from fastapi.responses import Response as PlainResponse
            headers = {
                "Content-Type": mime_type,
                "Content-Disposition": _content_disposition(file_name),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
            }
            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            headers["Content-Length"] = str(req_length)

            if request.method == "HEAD":
                return PlainResponse(status_code=206 if range_header else 200, headers=headers)

            return StreamingResponse(
                stream_file_range_with_usage(str(fc_path), start, end, token),
                headers=headers,
                status_code=206 if range_header else 200,
                media_type=mime_type,
            )
        elif start == 0:
            # Populate Tier 2 NVMe disk in background while Tier 1 RAM serves Chunk 0.
            # A runway head-boost raises the fill target for starved-bitrate files.
            asyncio.create_task(
                _fill_first_cache_head(
                    chat_id, msg_id, unique_id, int(fc_bytes), base_index,
                    target_bytes=head_fill_bytes,
                )
            )

    # ------------------------------------------------------------------
    # Spill cache hit path (backward seeks / replays / 2nd viewer from NVMe
    # disk, zero MTProto calls). Chunks delivered by any earlier stream of
    # this file were persisted to a sparse range file.
    # ------------------------------------------------------------------
    if spill_cache.spill_enabled() and unique_id and chat_id and msg_id and req_length <= 8 * 1024 * 1024:
        try:
            spilled = await spill_cache.read_spilled(chat_id, msg_id, unique_id, start, req_length)
            if spilled is not None and len(spilled) == req_length:
                LOGGER.info("SpillCache HIT (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, req_length)
                from fastapi.responses import Response as PlainResponse
                return PlainResponse(
                    content=spilled,
                    status_code=206,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{file_size}",
                        "Content-Length": str(len(spilled)),
                        "Content-Type": mime_type,
                        "Accept-Ranges": "bytes",
                        "Content-Disposition": _content_disposition(file_name),
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
                    },
                )
        except Exception as exc:
            LOGGER.debug("SpillCache read skipped: %s", exc)

    # ------------------------------------------------------------------
    # Seek Cache hit path (ExoPlayer / VLC micro-range seek probes)
    # ------------------------------------------------------------------
    if bool(getattr(Telegram, "SEEK_COALESCING_ENABLED", True)) and req_length <= 131072 and start > 0 and chat_id and msg_id:
        cached_seek = await SEEK_CACHE.get_seek_range(chat_id, msg_id, start, req_length)
        if cached_seek is not None and len(cached_seek) == req_length:
            LOGGER.debug("SeekCache HIT (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, len(cached_seek))
            from fastapi.responses import Response as PlainResponse
            return PlainResponse(
                content=cached_seek,
                status_code=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(len(cached_seek)),
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Disposition": _content_disposition(file_name),
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
                },
            )
        else:
            # Fetch aligned 512 KB seek window and satisfy micro-probe immediately
            window_data = await prefetch_seek_window(file_id, base_streamer, None, chat_id=chat_id, message_id=msg_id, start_offset=start)
            if window_data:
                cached_seek = await SEEK_CACHE.get_seek_range(chat_id, msg_id, start, req_length)
                if cached_seek is not None and len(cached_seek) == req_length:
                    LOGGER.debug("SeekCache FETCHED (%s, %s) range=%s-%s len=%d", chat_id, msg_id, start, end, len(cached_seek))
                    from fastapi.responses import Response as PlainResponse
                    return PlainResponse(
                        content=cached_seek,
                        status_code=206,
                        headers={
                            "Content-Range": f"bytes {start}-{end}/{file_size}",
                            "Content-Length": str(len(cached_seek)),
                            "Content-Type": mime_type,
                            "Accept-Ranges": "bytes",
                            "Content-Disposition": _content_disposition(file_name),
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
                        },
                    )

    probe_granularity = max(4096, min(int(getattr(Telegram, "SMART_ROUTING_PROBE_BYTES", 32768) or 32768), 1024 * 1024))
    probe_offset = start - (start % probe_granularity)
    # -- Instant-start client selection ---------------------------------------
    # The live probe can cost up to SMART_ROUTING_PROBE_TIMEOUT_SEC on a cold
    # route. Three ways to avoid holding the first frame hostage:
    #   1) forced/seek/HEAD requests never probe;
    #   2) trust window: if this (client, DC) route succeeded within the last
    #      SMART_ROUTING_PROBE_TRUST_SEC a repeat open skips the probe entirely
    #      and the first byte arrives after one Telegram round-trip;
    #   3) overlap: a real probe starts the stream on the best-known base client
    #      immediately; if the probe finishes within SMART_ROUTING_PROBE_OVERLAP_SEC
    #      its pick is used instead, otherwise it continues in the background and
    #      only sharpens future route choices.
    probe_decision = "probe"
    if forced_client_index is not None:
        probe_decision = "forced"
    elif request.method == "HEAD":
        probe_decision = "head"
    elif not should_probe_request(range_header, start):
        probe_decision = "seek"
    elif _client_route_trusted(base_index, target_dc or real_dc):
        probe_decision = "trust_window"

    if probe_decision != "probe":
        index, streamer, probe_results = base_index, base_streamer, []
    else:
        overlap_sec = max(0.0, float(getattr(Telegram, "SMART_ROUTING_PROBE_OVERLAP_SEC", 0.4) or 0.4))

        async def _choose_with_overlap() -> tuple:
            return await choose_smart_client(
                request=request,
                chat_id=chat_id,
                msg_id=msg_id,
                target_dc=target_dc or real_dc,
                base_index=base_index,
                probe_offset=probe_offset,
            )

        probe_task = asyncio.create_task(_choose_with_overlap())

        def _probe_done(t: asyncio.Task) -> None:
            # Consume late probe results so a background failure isn't reported
            # as an unretrieved task exception; metrics were already updated.
            try:
                t.exception()
            except Exception:
                pass

        probe_task.add_done_callback(_probe_done)
        try:
            index, streamer, file_id, probe_results = await asyncio.wait_for(
                asyncio.shield(probe_task),
                timeout=max(overlap_sec, 0.05),
            )
        except asyncio.TimeoutError:
            index, streamer, probe_results = base_index, base_streamer, []
            probe_decision = "overlap_base"
            LOGGER.debug(
                "Smart-routing probe still running after %.2fs — streaming starts on base client %s (DC %s)",
                overlap_sec, base_index, target_dc or real_dc,
            )

    if secure_hash != "SKIP_HASH_CHECK":
        if file_id.unique_id[:6] != secure_hash:
            raise InvalidHash

    real_dc = file_id.dc_id
    file_size = file_id.file_size
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1

    chunk_size = select_telegram_chunk_size(range_header, start=start, req_length=req_length)
    offset = start - (start % chunk_size)
    first_part_cut = start - offset
    last_part_cut = (end % chunk_size) + 1
    part_count = (end // chunk_size) - (offset // chunk_size) + 1

    from urllib.parse import unquote

    stream_id = secrets.token_hex(8)

    # Extract original title from the URL path name, fallback to raw name
    decoded_name = unquote(request.path_params.get("name", ""))

    # Look up the real title from the database using the Stremio stream_id_hash
    final_title = await _lookup_title(stream_id_hash, decoded_name)

    meta = {
        "request_path": str(request.url.path),
        "request_range": range_header or None,
        "request_start": start,
        "request_end": end,
        "request_length": req_length,
        "client_host": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
        "title": final_title,
        "filename": file_name,
        "source_type": source_type,
        "token": token,
        "token_user_id": token_data.get("user_id") if token_data else None,
        "user_name": token_data.get("name", "Unknown") if token_data else "Unknown",
        "bitrate_bps": round(bitrate_bps, 1) if bitrate_bps else None,
        "starved_bitrate": starved_bitrate,
        "smart_routing": {
            "target_dc": real_dc,
            "selected_client": index,
            "decision": probe_decision,
            "probe_results": [
                {
                    "client": r.get("client_index"),
                    "ok": r.get("ok"),
                    "ttfb_sec": round(float(r.get("ttfb_sec") or 0.0), 3) if r.get("ttfb_sec") else None,
                    "mibps": round(float(r.get("mbps") or 0.0), 3) if r.get("mbps") else None,
                    "error": r.get("error"),
                }
                for r in (probe_results or [])
            ],
        },
    }

    # ------------------------------------------------------------------
    # Disk cache hit path (optional)
    # ------------------------------------------------------------------
    if disk_cache_enabled():
        try:
            unique_id = getattr(file_id, "unique_id", None) or ""
            cache_path = cache_abspath(chat_id, msg_id, unique_id)
            if unique_id and is_complete_cache_file(cache_path, expected_size=file_size):
                touch_cache_file(cache_path)

                asyncio.create_task(
                    db.log_stream_stats(
                        {
                            "stream_id": stream_id,
                            "msg_id": msg_id,
                            "chat_id": chat_id,
                            "dc_id": file_id.dc_id,
                            "client_index": index,
                            "total_bytes": 0 if nginx_accel_enabled() else req_length,
                            "duration": 0.0,
                            "avg_mbps": 0.0,
                            "peak_mbps": 0.0,
                            "status": "finished",
                            "parallelism": 0,
                            "chunk_size": 0,
                            "ttfb_sec": 0.0,
                            "chunk_timeouts": 0,
                            "chunk_errors": 0,
                            "fallback_chunks": 0,
                            "zero_pad_chunks": 0,
                            "cached": True,
                            "served_via": "nginx" if nginx_accel_enabled() else "disk",
                            "usage_accounted": not nginx_accel_enabled(),
                            "meta": meta,
                        }
                    )
                )

                from fastapi.responses import Response as PlainResponse

                headers = {
                    "Content-Type": mime_type,
                    "Content-Disposition": _content_disposition(file_name),
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=3600",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
                }
                if range_header:
                    headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

                if range_header or request.method == "HEAD":
                    headers["Content-Length"] = str(req_length)

                if nginx_accel_enabled():
                    headers["X-Accel-Redirect"] = nginx_accel_redirect_uri(chat_id, msg_id, unique_id)
                    return PlainResponse(status_code=206 if range_header else 200, headers=headers)

                if request.method == "HEAD":
                    return PlainResponse(status_code=206 if range_header else 200, headers=headers)

                # Fallback: stream from disk directly and account only bytes
                # actually yielded by the app process.
                return StreamingResponse(
                    stream_file_range_with_usage(str(cache_path), start, end, token),
                    headers=headers,
                    status_code=206 if range_header else 200,
                    media_type=mime_type,
                )
        except Exception as e:
            LOGGER.debug("Disk cache lookup failed: %s", e)

    # ------------------------------------------------------------------
    # Fast-start head cache (first N MiB on disk, LRU-bounded) — optional
    # ------------------------------------------------------------------
    # Serves only requests fully inside the cached head; anything beyond falls
    # through to Telegram. A completed head is byte-identical (exact-size match)
    # and starts the opening of repeat streams from local disk.
    first_cache_hit = False
    first_cache_fill = False
    try:
        if first_cache_enabled() and start < first_cache_bytes():
            unique_id = getattr(file_id, "unique_id", None) or ""
            if unique_id:
                fc_bytes = min(first_cache_bytes(), file_size)
                fc_path = first_cache_abspath(chat_id, msg_id, unique_id)
                if end < fc_bytes and get_first_cache_available_bytes(fc_path) >= fc_bytes:
                    first_cache_hit = True
                    touch_cache_file(fc_path)

                    asyncio.create_task(
                        db.log_stream_stats(
                            {
                                "stream_id": stream_id,
                                "msg_id": msg_id,
                                "chat_id": chat_id,
                                "dc_id": file_id.dc_id,
                                "client_index": index,
                                "total_bytes": req_length,
                                "duration": 0.0,
                                "avg_mbps": 0.0,
                                "peak_mbps": 0.0,
                                "status": "finished",
                                "parallelism": 0,
                                "chunk_size": 0,
                                "ttfb_sec": 0.0,
                                "chunk_timeouts": 0,
                                "chunk_errors": 0,
                                "fallback_chunks": 0,
                                "zero_pad_chunks": 0,
                                "cached": True,
                                "served_via": "head_cache",
                                "usage_accounted": True,
                                "meta": meta,
                            }
                        )
                    )

                    from fastapi.responses import Response as PlainResponse

                    headers = {
                        "Content-Type": mime_type,
                        "Content-Disposition": _content_disposition(file_name),
                        "Accept-Ranges": "bytes",
                        "Cache-Control": "public, max-age=3600",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
                    }
                    if range_header:
                        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
                    if range_header or request.method == "HEAD":
                        headers["Content-Length"] = str(req_length)

                    if request.method == "HEAD":
                        return PlainResponse(status_code=206 if range_header else 200, headers=headers)

                    return StreamingResponse(
                        stream_file_range_with_usage(str(fc_path), start, end, token),
                        headers=headers,
                        status_code=206 if range_header else 200,
                        media_type=mime_type,
                    )
                elif start == 0:
                    # Front-load the head in the background while the stream
                    # starts from Telegram — nothing waits on the cache.
                    first_cache_fill = True
                    asyncio.create_task(
                        _fill_first_cache_head(
                            chat_id, msg_id, unique_id, int(fc_bytes), base_index,
                            target_bytes=head_fill_bytes,
                        )
                    )
    except Exception as e:
        LOGGER.debug("First-cache lookup failed: %s", e)

    meta["first_cache"] = {
        "enabled": first_cache_enabled(),
        "cached_bytes": min(first_cache_bytes(), file_size) if first_cache_enabled() else 0,
        "hit": first_cache_hit,
        "fill_started": first_cache_fill,
    }

    configured_prefetch, configured_parallelism = get_configured_stream_concurrency()
    active_streams = count_active_telegram_streams()
    mem_available_mb = get_mem_available_mb()
    prefetch_count, parallelism, prefetch_reason = choose_effective_prefetch(
        configured_prefetch,
        configured_parallelism,
        file_size=file_size,
        request_length=req_length,
        active_streams=active_streams,
        mem_available_mb=mem_available_mb,
    )
    meta["adaptive_prefetch"] = {
        "configured_prefetch": configured_prefetch,
        "configured_parallelism": configured_parallelism,
        "effective_prefetch": prefetch_count,
        "effective_parallelism": parallelism,
        "reason": prefetch_reason,
        "active_streams": active_streams,
        "mem_available_mb": mem_available_mb,
    }

    # HEAD: return headers only, include Content-Length so the client knows
    # the file/range size without opening a stream.
    # GET 200: keep full-file Telegram streams chunked. If Telegram ends early,
    # h11 would enforce Content-Length strictly and log protocol errors.
    # GET 206: keep Telegram-backed live streams chunked. Telegram may fail a
    # later chunk after headers are sent; strict Content-Length would then cause
    # h11 LocalProtocolError ("Too little data for declared Content-Length").
    # Disk-cache/downloaded paths can still use Content-Length because their
    # byte source is local and deterministic.

    # HEAD request support
    from fastapi.responses import Response as PlainResponse

    if request.method == "HEAD":
        headers = {
            "Content-Type": mime_type,
            "Content-Length": str(req_length),
            "Content-Disposition": _content_disposition(file_name),
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        }

        if range_header:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

        return PlainResponse(
            status_code=206 if range_header else 200,
            headers=headers,
        )

    extra_clients_for_stream = []
    if parallelism > 1 and len(multi_clients) > 1:
        # Build a multi-client chunk pool: other healthy helpers feed the same
        # file in parallel (round-robin by chunk), multiplying per-stream
        # throughput. Cooldowns are respected; failures are skipped.
        other_indices = sorted(
            (i for i in multi_clients if i != index and not is_client_cooled_down(i, target_dc or real_dc)),
            key=lambda i: work_loads.get(i, 0),
        )
        want = parallelism - 1

        async def _get_extra_file_id(ec_idx: int):
            ec_streamer = get_streamer(ec_idx)
            try:
                ec_fid = await ec_streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)
                return (ec_idx, ec_streamer, ec_fid)
            except Exception as e:
                LOGGER.debug("Extra client %s file_id fetch failed: %s", ec_idx, e)
                return None

        results = await asyncio.gather(*[_get_extra_file_id(i) for i in other_indices[:want]], return_exceptions=True)
        extra_clients_for_stream = [r for r in results if r is not None]
        LOGGER.info(
            "Stream %s pool: primary=%s extras=%s effective_parallelism=%s",
            stream_id, index, [ec[0] for ec in extra_clients_for_stream], parallelism,
        )

    meta["stream_pool"] = {
        "primary": index,
        "extras": [ec[0] for ec in extra_clients_for_stream],
        "pool_size": 1 + len(extra_clients_for_stream),
        "effective_parallelism": parallelism,
    }

    if start == 0 and file_size > 524288 and chat_id and msg_id:
        asyncio.create_task(prefetch_file_tail(file_id, streamer, extra_clients_for_stream, chat_id=chat_id, message_id=msg_id))
        if bool(getattr(Telegram, "STREAM_INDEX_ENABLED", True)):
            # Parse MKV Cues / MP4 moov once in the background — powers exact
            # skip pre-warm and runway bitrate math for this file. Reuse the
            # already-warm DC session when available (no throwaway Client).
            try:
                _idx_sess = await streamer._get_media_session(file_id)
                _idx_loc = await streamer._get_location(file_id)
            except Exception:
                _idx_sess = None
                _idx_loc = None
            asyncio.create_task(
                media_index.build_media_index(
                    file_id,
                    streamer,
                    chat_id=chat_id,
                    message_id=msg_id,
                    media_session=_idx_sess,
                    location=_idx_loc,
                )
            )

    body_gen = await streamer.prefetch_stream(
        file_id=file_id,
        client_index=index,
        offset=offset,
        first_part_cut=first_part_cut,
        last_part_cut=last_part_cut,
        part_count=part_count,
        chunk_size=chunk_size,
        prefetch=prefetch_count,
        stream_id=stream_id,
        meta=meta,
        parallelism=parallelism,
        request=request,
        chat_id=chat_id,
        message_id=msg_id,
        extra_clients=extra_clients_for_stream,
    )

    asyncio.create_task(track_usage_from_stats(stream_id, token, token_data))

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": _content_disposition(file_name),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }

    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status = 206
    else:
        status = 200

    return StreamingResponse(
        body_gen,
        headers=headers,
        status_code=status,
        media_type=mime_type,
    )


@router.get("/stream/stats")
async def get_stream_stats():
    now = time.time()

    PRUNE_SECONDS = 3
    INACTIVE_TIMEOUT = 15  # 15 sec no data = inactive

    for sid, info in list(ACTIVE_STREAMS.items()):
        status = info.get("status", "active")

        current_bytes = info.get("total_bytes", 0)

        if "last_bytes" not in info:
            info["last_bytes"] = current_bytes
            info["last_activity_ts"] = now

        
        if current_bytes > info["last_bytes"]:
            # Data is flowing → update activity timestamp
            info["last_bytes"] = current_bytes
            info["last_activity_ts"] = now
            info["status"] = "active"  # ensure it stays active if resumed
        else:
            # No data flow → check inactivity timeout
            if now - info["last_activity_ts"] > INACTIVE_TIMEOUT:
                if status == "active":
                    info["status"] = "cancelled"
                    info["end_ts"] = now
                    
        if info.get("status") in ("cancelled", "error", "finished", "inactive"):
            last_ts = info.get("end_ts", info.get("last_activity_ts", now))
            if now - last_ts > PRUNE_SECONDS:
                try:
                    RECENT_STREAMS.appendleft(ACTIVE_STREAMS.pop(sid))
                except KeyError:
                    pass

    active = []
    for sid, info in ACTIVE_STREAMS.items():
        meta = info.get("meta", {}) or {}
        active.append(
            {
                "stream_id": sid,
                "msg_id": info.get("msg_id"),
                "chat_id": info.get("chat_id"),
                "title": meta.get("title"),
                "filename": meta.get("filename"),
                "source_type": meta.get("source_type", "telegram"),
                "user_name": meta.get("user_name"),
                "client_host": meta.get("client_host"),
                "user_agent": meta.get("user_agent"),
                "request_range": meta.get("request_range"),
                "request_start": meta.get("request_start"),
                "request_end": meta.get("request_end"),
                "request_length": meta.get("request_length"),
                "adaptive_prefetch": meta.get("adaptive_prefetch"),
                "smart_routing": meta.get("smart_routing"),
                "client_index": info.get("client_index"),
                "dc_id": info.get("dc_id"),
                "status": info.get("status"),
                "total_bytes": info.get("total_bytes"),
                "ttfb_sec": info.get("ttfb_sec"),
                "instant_mbps": round(info.get("instant_mbps", 0.0), 3),
                "avg_mbps": round(info.get("avg_mbps", 0.0), 3),
                "peak_mbps": round(info.get("peak_mbps", 0.0), 3),
                "chunk_timeouts": info.get("chunk_timeouts", 0),
                "chunk_errors": info.get("chunk_errors", 0),
                "fallback_chunks": info.get("fallback_chunks", 0),
                "hedge_rescues": info.get("hedge_rescues", 0),
                "zero_pad_chunks": info.get("zero_pad_chunks", 0),
                "cdn_redirects": info.get("cdn_redirects", 0),
                "cdn_chunks": info.get("cdn_chunks", 0),
                "cdn_bytes": info.get("cdn_bytes", 0),
                "cdn_errors": info.get("cdn_errors", 0),
                "cdn_fallbacks": info.get("cdn_fallbacks", 0),
                "cdn_dc": info.get("cdn_dc"),
                "error_reason": info.get("error_reason"),
                "route_attempts": info.get("route_attempts", []),
                "start_ts": info.get("start_ts"),
            }
        )

    recent = []
    for info in RECENT_STREAMS:
        meta = info.get("meta", {}) or {}
        recent.append(
            {
                "stream_id": info.get("stream_id"),
                "msg_id": info.get("msg_id"),
                "chat_id": info.get("chat_id"),
                "title": meta.get("title"),
                "filename": meta.get("filename"),
                "source_type": meta.get("source_type", "telegram"),
                "user_name": meta.get("user_name"),
                "client_host": meta.get("client_host"),
                "user_agent": meta.get("user_agent"),
                "request_range": meta.get("request_range"),
                "adaptive_prefetch": meta.get("adaptive_prefetch"),
                "smart_routing": meta.get("smart_routing"),
                "client_index": info.get("client_index"),
                "dc_id": info.get("dc_id"),
                "status": info.get("status"),
                "total_bytes": info.get("total_bytes"),
                "ttfb_sec": info.get("ttfb_sec"),
                "duration": info.get("duration"),
                "avg_mbps": round(info.get("avg_mbps", 0.0), 3),
                "peak_mbps": round(info.get("peak_mbps", 0.0), 3),
                "chunk_timeouts": info.get("chunk_timeouts", 0),
                "chunk_errors": info.get("chunk_errors", 0),
                "fallback_chunks": info.get("fallback_chunks", 0),
                "hedge_rescues": info.get("hedge_rescues", 0),
                "zero_pad_chunks": info.get("zero_pad_chunks", 0),
                "cdn_redirects": info.get("cdn_redirects", 0),
                "cdn_chunks": info.get("cdn_chunks", 0),
                "cdn_bytes": info.get("cdn_bytes", 0),
                "cdn_errors": info.get("cdn_errors", 0),
                "cdn_fallbacks": info.get("cdn_fallbacks", 0),
                "cdn_dc": info.get("cdn_dc"),
                "error_reason": info.get("error_reason"),
                "route_attempts": info.get("route_attempts", []),
                "start_ts": info.get("start_ts"),
                "end_ts": info.get("end_ts"),
            }
        )
    recent_failed = [
        stream for stream in recent
        if stream.get("status") == "error" or stream.get("error_reason")
    ]

    try:
        recent_watch_requests = await db.get_recent_watch_link_requests(20)
    except Exception as e:
        LOGGER.warning(f"Could not load recent watch link requests: {e}")
        recent_watch_requests = []

    return JSONResponse(
        {
            "active_streams": active,
            "recent_streams": recent,
            "recent_failed_streams": recent_failed,
            "recent_watch_requests": recent_watch_requests,
            "client_dc_map": client_dc_map,
            "work_loads": work_loads,
            "client_cooldowns": get_client_cooldown_state(),
            "client_avg_mibps": {str(k): round(float(v or 0.0), 3) for k, v in client_avg_mbps.items()},
            "client_dc_avg_mibps": {
                f"{idx}->dc{dc}": round(float(v or 0.0), 3)
                for (idx, dc), v in client_dc_avg_mbps.items()
            },
            "client_dc_ttfb_sec": {
                f"{idx}->dc{dc}": round(float(v or 0.0), 3)
                for (idx, dc), v in client_dc_ttfb_sec.items()
            },
            "head_cache": {
                "size": len(HEAD_CACHE._cache),
                "max_entries": HEAD_CACHE.max_entries,
                "entries": [f"({c},{m})" for (c, m) in HEAD_CACHE._cache.keys()],
            },
            "seek_cache": {
                "size": len(SEEK_CACHE._cache),
                "max_entries": SEEK_CACHE.max_entries,
                "entries": [f"({c},{m})" for (c, m) in SEEK_CACHE._cache.keys()],
            },
            "first_cache": {
                "enabled": first_cache_enabled(),
                "head_mb": first_cache_bytes() / (1024 * 1024),
            },
            "index_cache": media_index.get_index_cache_stats(),
            "spill_cache": await spill_cache.get_spill_stats(),
        }
    )

@router.get("/stream/stats/{stream_id}")
async def get_stream_detail(stream_id: str):
    info = ACTIVE_STREAMS.get(stream_id)
    if info:
        return JSONResponse(make_json_safe(info))

    for rec in RECENT_STREAMS:
        if rec.get("stream_id") == stream_id:
            return JSONResponse(make_json_safe(rec))

    raise HTTPException(status_code=404, detail="Stream not found")
