import secrets
import mimetypes
import time
from typing import Dict

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse

from collections import deque

from Backend import db
from Backend.helper.encrypt import decode_string
from Backend.helper.exceptions import InvalidHash
from Backend.helper.custom_dl import (
    ByteStreamer,
    ACTIVE_STREAMS,
    RECENT_STREAMS,
    get_adaptive_chunk_size,
    client_dc_avg_mbps,
    client_dc_ttfb_sec,
    smart_client_score,
)
from Backend.helper.disk_cache import (
    disk_cache_enabled,
    cache_abspath,
    is_complete_cache_file,
    touch_cache_file,
    nginx_accel_enabled,
    nginx_accel_redirect_uri,
)
from Backend.pyrofork.bot import StreamBot, work_loads, multi_clients, client_dc_map, client_failures, client_avg_mbps
from Backend.config import Telegram
from Backend.logger import LOGGER
from Backend.fastapi.security.tokens import verify_token
import asyncio
from pyrogram.file_id import FileId

router = APIRouter(tags=["Streaming"])

_streamer_by_client: Dict = {}
_failure_decay_started: bool = False


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


def select_best_client(target_dc: int) -> int:
    """Pick the best available helper using DC-aware live performance."""
    if multi_clients:
        selected = min(multi_clients.keys(), key=lambda idx: smart_client_score(idx, target_dc))
        LOGGER.debug(
            "Selected client %s (DC %s) score=%s",
            selected, client_dc_map.get(selected, "?"), smart_client_score(selected, target_dc),
        )
        return selected

    return 0


def get_streamer(index: int) -> ByteStreamer:
    tg_client = multi_clients[index]
    if tg_client not in _streamer_by_client:
        _streamer_by_client[tg_client] = ByteStreamer(tg_client, index)
    return _streamer_by_client[tg_client]


def select_probe_candidates(target_dc: int, base_index: int) -> list[int]:
    limit = max(1, min(int(getattr(Telegram, "SMART_ROUTING_PROBE_CLIENTS", 3) or 3), len(multi_clients)))
    ranked = sorted(multi_clients.keys(), key=lambda idx: smart_client_score(idx, target_dc))
    candidates = []
    if base_index in multi_clients:
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

    probe_size = int(getattr(Telegram, "SMART_ROUTING_PROBE_BYTES", 262144) or 262144)
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
            client_failures[idx] = client_failures.get(idx, 0) + 1
        return result

    probe_results = await asyncio.gather(*[_probe(idx) for idx in candidates], return_exceptions=True)
    clean_results = []
    for idx, result in zip(candidates, probe_results):
        if isinstance(result, Exception):
            client_failures[idx] = client_failures.get(idx, 0) + 1
            clean_results.append({"client_index": idx, "ok": False, "error": str(result)})
        else:
            clean_results.append(result)

    ok_results = [r for r in clean_results if r.get("ok") and r.get("file_id") is not None]
    if not ok_results:
        LOGGER.warning("Smart routing probe found no usable helper for msg=%s dc=%s", msg_id, target_dc)
        await streamer._get_media_session(file_id)
        return base_index, streamer, file_id, clean_results

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


@router.get("/dl/{token}/{id}/{name}")
@router.head("/dl/{token}/{id}/{name}")
async def stream_handler(
    request: Request,
    token: str,
    id: str,
    name: str,
    token_data: dict = Depends(verify_token),
):
    decoded = await decode_string(id)
    msg_id = decoded.get("msg_id")
    if not msg_id:
        raise HTTPException(status_code=400, detail="Missing id")

    chat_id = int(f"-100{decoded['chat_id']}")
    message = await StreamBot.get_messages(chat_id, int(msg_id))
    file = message.video or message.document
    if not file:
        raise HTTPException(status_code=404, detail="No media found")
    secure_hash = file.file_unique_id[:6]

    try:
        target_dc = FileId.decode(file.file_id).dc_id
    except Exception:
        target_dc = None

    return await media_streamer(
        request=request,
        chat_id=chat_id,
        msg_id=int(msg_id),
        secure_hash=secure_hash,
        token=token,
        token_data=token_data,
        stream_id_hash=id,
        target_dc=target_dc,
    )

async def media_streamer(
    request: Request,
    chat_id: int,
    msg_id: int,
    secure_hash: str,
    token: str,
    token_data: dict = None,
    stream_id_hash: str = None,
    target_dc: int | None = None,
):
    global _failure_decay_started
    if not _failure_decay_started:
        try:
            asyncio.create_task(decay_client_failures())
            _failure_decay_started = True
        except Exception:
            pass

    base_index = select_best_client(target_dc or 0)
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

    probe_granularity = max(4096, min(int(getattr(Telegram, "SMART_ROUTING_PROBE_BYTES", 262144) or 262144), 1024 * 1024))
    probe_offset = start - (start % probe_granularity)
    index, streamer, file_id, probe_results = await choose_smart_client(
        request=request,
        chat_id=chat_id,
        msg_id=msg_id,
        target_dc=target_dc or real_dc,
        base_index=base_index,
        probe_offset=probe_offset,
    )

    if secure_hash != "SKIP_HASH_CHECK":
        if file_id.unique_id[:6] != secure_hash:
            raise InvalidHash

    real_dc = file_id.dc_id
    file_size = file_id.file_size
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1

    # Adaptive chunk size based on this client's recent measured throughput
    chunk_size = get_adaptive_chunk_size(index)
    offset = start - (start % chunk_size)
    first_part_cut = start - offset
    last_part_cut = (end % chunk_size) + 1
    part_count = (end // chunk_size) - (offset // chunk_size) + 1

    file_name = file_id.file_name or f"{secrets.token_hex(4)}.bin"
    mime_type = file_id.mime_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    if "." not in file_name and "/" in mime_type:
        file_name = f"{file_name}.{mime_type.split('/')[1]}"

    from urllib.parse import unquote

    stream_id = secrets.token_hex(8)

    # Extract original title from the URL path name, fallback to raw name
    decoded_name = unquote(request.path_params.get("name", ""))

    # Look up the real title from the database using the Stremio stream_id_hash
    db_title = None
    if stream_id_hash:
        db_title = await db.get_title_by_stream_id(stream_id_hash)
        LOGGER.info(f"Stream lookup for hash '{stream_id_hash}' returned title: {db_title}")

    final_title = db_title if db_title else decoded_name

    meta = {
        "request_path": str(request.url.path),
        "client_host": request.client.host if request.client else None,
        "title": final_title,
        "user_name": token_data.get("name", "Unknown") if token_data else "Unknown",
        "smart_routing": {
            "target_dc": real_dc,
            "selected_client": index,
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

                # Best-effort usage accounting: when offloading to nginx (or
                # streaming from disk), we may not observe actual bytes sent.
                # Count requested bytes to avoid undercounting.
                if request.method != "HEAD":
                    try:
                        asyncio.create_task(db.update_token_usage(token, int(req_length)))
                    except Exception:
                        pass

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
                            "served_via": "nginx" if nginx_accel_enabled() else "disk",
                            "meta": meta,
                        }
                    )
                )

                from fastapi.responses import Response as PlainResponse

                headers = {
                    "Content-Type": mime_type,
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=3600",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
                }
                if range_header:
                    headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

                if request.method == "HEAD":
                    headers["Content-Length"] = str(req_length)

                if nginx_accel_enabled():
                    headers["X-Accel-Redirect"] = nginx_accel_redirect_uri(chat_id, msg_id, unique_id)
                    return PlainResponse(status_code=206 if range_header else 200, headers=headers)

                # Fallback: stream from disk directly (still supports Range)
                import aiofiles

                async def _iter_file_range(path, start_pos: int, end_pos: int, read_size: int = 1024 * 1024):
                    remaining = (end_pos - start_pos) + 1
                    async with aiofiles.open(path, "rb") as f:
                        await f.seek(start_pos)
                        while remaining > 0:
                            chunk = await f.read(min(read_size, remaining))
                            if not chunk:
                                break
                            remaining -= len(chunk)
                            yield chunk

                if request.method == "HEAD":
                    return PlainResponse(status_code=206 if range_header else 200, headers=headers)

                return StreamingResponse(
                    _iter_file_range(str(cache_path), start, end),
                    headers=headers,
                    status_code=206 if range_header else 200,
                    media_type=mime_type,
                )
        except Exception as e:
            LOGGER.debug("Disk cache lookup failed: %s", e)

    prefetch_count = Telegram.PARALLEL
    parallelism = Telegram.PRE_FETCH

    # HEAD: return headers only (no body), include Content-Length so the
    # client knows the file size without opening a stream.
    # GET: do NOT set Content-Length on the StreamingResponse.
    # If a Telegram chunk fetch times out mid-stream the generator exits early,
    # delivering fewer bytes than the declared length.  h11 enforces
    # Content-Length strictly and raises LocalProtocolError in that case.
    # Without Content-Length, uvicorn uses chunked transfer encoding which
    # handles early termination gracefully.  Stremio / media players
    # are fine with chunked 206 responses.

    # HEAD request support
    from fastapi.responses import Response as PlainResponse

    if request.method == "HEAD":
        headers = {
            "Content-Type": mime_type,
            "Content-Length": str(req_length),
            "Content-Disposition": f'inline; filename="{file_name}"',
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
    )

    asyncio.create_task(track_usage_from_stats(stream_id, token, token_data))

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f'inline; filename="{file_name}"',
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
        active.append(
            {
                "stream_id": sid,
                "msg_id": info.get("msg_id"),
                "chat_id": info.get("chat_id"),
                "title": info.get("meta", {}).get("title"),
                "client_index": info.get("client_index"),
                "dc_id": info.get("dc_id"),
                "status": info.get("status"),
                "total_bytes": info.get("total_bytes"),
                "instant_mbps": round(info.get("instant_mbps", 0.0), 3),
                "avg_mbps": round(info.get("avg_mbps", 0.0), 3),
                "peak_mbps": round(info.get("peak_mbps", 0.0), 3),
                "start_ts": info.get("start_ts"),
            }
        )

    recent = []
    for info in RECENT_STREAMS:
        recent.append(
            {
                "stream_id": info.get("stream_id"),
                "msg_id": info.get("msg_id"),
                "chat_id": info.get("chat_id"),
                "title": info.get("meta", {}).get("title"),
                "client_index": info.get("client_index"),
                "dc_id": info.get("dc_id"),
                "status": info.get("status"),
                "total_bytes": info.get("total_bytes"),
                "duration": info.get("duration"),
                "avg_mbps": round(info.get("avg_mbps", 0.0), 3),
                "start_ts": info.get("start_ts"),
                "end_ts": info.get("end_ts"),
            }
        )

    return JSONResponse(
        {
            "active_streams": active,
            "recent_streams": recent,
            "client_dc_map": client_dc_map,
            "work_loads": work_loads,
            "client_avg_mibps": {str(k): round(float(v or 0.0), 3) for k, v in client_avg_mbps.items()},
            "client_dc_avg_mibps": {
                f"{idx}->dc{dc}": round(float(v or 0.0), 3)
                for (idx, dc), v in client_dc_avg_mbps.items()
            },
            "client_dc_ttfb_sec": {
                f"{idx}->dc{dc}": round(float(v or 0.0), 3)
                for (idx, dc), v in client_dc_ttfb_sec.items()
            },
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
