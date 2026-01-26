import math
import secrets
import mimetypes
import time
import asyncio
from typing import Dict

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from collections import deque

from Backend import db
from Backend.helper.encrypt import decode_string
from Backend.helper.exceptions import InvalidHash
from Backend.helper.custom_dl import ByteStreamer, ACTIVE_STREAMS, RECENT_STREAMS
from Backend.pyrofork.bot import StreamBot, work_loads, multi_clients, client_dc_map
from Backend.config import Telegram
from Backend.logger import LOGGER
from Backend.fastapi.security.tokens import verify_token

router = APIRouter(tags=["Streaming"])

_streamer_by_client: Dict = {}


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
    if not range_header:
        return 0, file_size - 1

    try:
        value = range_header.replace("bytes=", "")
        start_str, end_str = value.split("-")
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
    except Exception:
        raise HTTPException(
            status_code=416,
            detail="Invalid Range header",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    if start < 0 or end >= file_size or end < start:
        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    return start, end


def select_best_client(target_dc: int) -> int:
    """Select the best client based on workload and DC matching"""
    if multi_clients:
        selected = min(work_loads, key=work_loads.get)
        selected_dc = client_dc_map.get(selected, "unknown")
        LOGGER.debug(
            f"Using client {selected} (DC {selected_dc}) with workload {work_loads[selected]}"
        )
        return selected
    return 0


async def track_usage(stream_id: str, token: str):
    """Track streaming usage in background"""
    await asyncio.sleep(2)
    last_tracked = 0
    
    try:
        while True:
            await asyncio.sleep(10)
            stream_info = ACTIVE_STREAMS.get(stream_id)
            
            if not stream_info:
                # Stream ended, check recent streams for final bytes
                for rec in RECENT_STREAMS:
                    if rec.get("stream_id") == stream_id:
                        final_bytes = rec.get("total_bytes", 0)
                        delta = final_bytes - last_tracked
                        if delta > 0:
                            await db.update_token_usage(token, delta)
                        break
                return
            
            current_bytes = stream_info.get("total_bytes", 0)
            delta = current_bytes - last_tracked
            
            if delta > 0:
                await db.update_token_usage(token, delta)
                last_tracked = current_bytes
                
    except asyncio.CancelledError:
        pass


# Token-protected streaming endpoint
@router.get("/dl/{token}/{id}/{name}")
@router.head("/dl/{token}/{id}/{name}")
async def stream_handler(request: Request, token: str, id: str, name: str):
    # Verify token
    token_data = await verify_token(token)
    
    # Check if daily limit exceeded
    if token_data.get("limit_exceeded"):
        raise HTTPException(
            status_code=429,
            detail="Daily streaming limit exceeded. Try again tomorrow!"
        )
    
    decoded = await decode_string(id)
    msg_id = decoded.get("msg_id")
    if not msg_id:
        raise HTTPException(status_code=400, detail="Missing id")

    chat_id = int(f"-100{decoded['chat_id']}")
    message = await StreamBot.get_messages(chat_id, int(msg_id))
    file = message.video or message.document
    secure_hash = file.file_unique_id[:6]

    return await media_streamer(
        request=request,
        chat_id=chat_id,
        msg_id=int(msg_id),
        secure_hash=secure_hash,
        token=token,
    )


async def media_streamer(
    request: Request,
    chat_id: int,
    msg_id: int,
    secure_hash: str,
    token: str,
):
    temp_client = multi_clients[min(work_loads, key=work_loads.get)]
    if temp_client not in _streamer_by_client:
        _streamer_by_client[temp_client] = ByteStreamer(temp_client)
    temp_streamer = _streamer_by_client[temp_client]

    file_id = await temp_streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)

    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash

    target_dc = file_id.dc_id
    LOGGER.debug(f"File msg_id={msg_id} is in DC {target_dc}")

    index = select_best_client(target_dc)
    tg_client = multi_clients[index]

    if tg_client not in _streamer_by_client:
        _streamer_by_client[tg_client] = ByteStreamer(tg_client)
    streamer: ByteStreamer = _streamer_by_client[tg_client]

    file_size = file_id.file_size
    range_header = request.headers.get("Range", "")
    start, end = parse_range_header(range_header, file_size)
    req_length = end - start + 1

    chunk_size = streamer.CHUNK_SIZE
    offset = start - (start % chunk_size)
    first_part_cut = start - offset
    last_part_cut = (end % chunk_size) + 1
    part_count = math.ceil(end / chunk_size) - math.floor(offset / chunk_size)

    stream_id = secrets.token_hex(8)
    meta = {
        "request_path": str(request.url.path),
        "client_host": request.client.host if request.client else None,
        "token": token[:8] + "...",  # Partial token for logging
    }

    prefetch_count = Telegram.PARALLEL
    parallelism = Telegram.PRE_FETCH

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
    )

    # Track usage in background
    asyncio.create_task(track_usage(stream_id, token))

    file_name = file_id.file_name or f"{secrets.token_hex(4)}.bin"
    mime_type = file_id.mime_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    if "." not in file_name and "/" in mime_type:
        file_name = f"{file_name}.{mime_type.split('/')[1]}"

    headers = {
        "Content-Type": mime_type,
        "Content-Length": str(req_length),
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        "X-Stream-Id": stream_id,
    }

    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status = 206
    else:
        status = 200

    return StreamingResponse(
        content=body_gen,
        headers=headers,
        status_code=status,
        media_type=mime_type,
    )


@router.get("/stream/stats")
async def get_stream_stats():
    """Get streaming statistics for monitoring"""
    now = time.time()
    PRUNE_SECONDS = 3

    for sid, info in list(ACTIVE_STREAMS.items()):
        status = info.get("status")
        last_ts = info.get("last_ts", info.get("start_ts", now))
        if status in ("cancelled", "error", "finished"):
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
        }
    )


@router.get("/stream/stats/{stream_id}")
async def get_stream_detail(stream_id: str):
    """Get detailed info for a specific stream"""
    info = ACTIVE_STREAMS.get(stream_id)
    if info:
        return JSONResponse(make_json_safe(info))

    for rec in RECENT_STREAMS:
        if rec.get("stream_id") == stream_id:
            return JSONResponse(make_json_safe(rec))

    raise HTTPException(status_code=404, detail="Stream not found")