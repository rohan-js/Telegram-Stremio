import math
import secrets
import mimetypes
import time
import asyncio
from typing import Dict

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, Response
from collections import deque

from Backend.helper.encrypt import decode_string
from Backend.helper.exceptions import InvalidHash
from Backend.helper.custom_dl import ByteStreamer, ACTIVE_STREAMS, RECENT_STREAMS
from Backend.helper.audio_tracks import (
    probe_audio_tracks_from_stream,
    get_cached_audio_tracks,
    cache_audio_tracks,
)
from Backend.helper.subtitle_tracks import (
    probe_subtitle_tracks,
    extract_subtitle_to_vtt,
    get_cached_subtitle_tracks,
    cache_subtitle_tracks,
    format_subtitle_track_label,
)
from Backend.helper.hls_transcoder import (
    generate_master_playlist,
    generate_variant_playlist,
    transcode_segment,
    get_cached_segment,
    cache_segment,
    get_cache_key,
    estimate_qualities_from_resolution,
    QUALITY_PRESETS,
    SEGMENT_DURATION,
)
from Backend.pyrofork.bot import StreamBot, work_loads, multi_clients, client_dc_map
from Backend.config import Telegram
from Backend.logger import LOGGER

router = APIRouter(tags=["Streaming"])

_streamer_by_client: Dict = {}

# Store video metadata for HLS
_video_metadata: Dict[str, dict] = {}


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


@router.get("/dl/{id}/{name}")
@router.head("/dl/{id}/{name}")
async def stream_handler(request: Request, id: str, name: str):
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
    )


@router.get("/stream/{id}")
async def transcoded_stream(request: Request, id: str, audio: int = 0):
    """
    Stream video with transcoded audio for browser compatibility.
    FFmpeg copies video (no re-encode), transcodes audio to AAC,
    outputs fragmented MP4 that browsers can play.
    
    Args:
        id: File ID
        audio: Audio track index (0-based, default 0)
    """
    import asyncio as aio
    
    decoded = await decode_string(id)
    msg_id = decoded.get("msg_id")
    if not msg_id:
        raise HTTPException(status_code=400, detail="Missing id")

    chat_id = int(f"-100{decoded['chat_id']}")
    
    # Get file properties
    index = min(work_loads, key=work_loads.get)
    tg_client = multi_clients[index]
    if tg_client not in _streamer_by_client:
        _streamer_by_client[tg_client] = ByteStreamer(tg_client)
    streamer: ByteStreamer = _streamer_by_client[tg_client]
    
    file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=int(msg_id))
    file_size = file_id.file_size
    chunk_size = streamer.CHUNK_SIZE
    
    # FFmpeg command: pipe input, copy video, transcode audio to AAC, fragmented MP4 output
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", "pipe:0",           # Read from stdin
        "-map", "0:v:0",          # First video stream
        "-map", f"0:a:{audio}",   # Selected audio track
        "-c:v", "copy",           # Copy video (no re-encode)
        "-c:a", "aac",            # Transcode audio to AAC
        "-b:a", "192k",           # Audio bitrate
        "-ac", "2",               # Stereo output
        "-f", "mp4",              # MP4 container
        "-movflags", "frag_keyframe+empty_moov+faststart",  # Fragmented for streaming
        "pipe:1"                  # Output to stdout
    ]
    
    async def generate():
        process = None
        try:
            # Start FFmpeg process
            process = await aio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdin=aio.subprocess.PIPE,
                stdout=aio.subprocess.PIPE,
                stderr=aio.subprocess.PIPE,
            )
            
            # Feed data to FFmpeg in background
            async def feed_input():
                try:
                    part_count = max(1, file_size // chunk_size)
                    gen = await streamer.prefetch_stream(
                        file_id=file_id,
                        client_index=index,
                        offset=0,
                        first_part_cut=0,
                        last_part_cut=chunk_size,
                        part_count=part_count,
                        chunk_size=chunk_size,
                        prefetch=5,
                        parallelism=3,
                    )
                    async for item in gen:
                        if isinstance(item, tuple) and len(item) >= 2:
                            data = item[1]
                        else:
                            data = item
                        if data and process.stdin:
                            process.stdin.write(data)
                            await process.stdin.drain()
                except Exception as e:
                    LOGGER.warning(f"Feed input error: {e}")
                finally:
                    if process.stdin:
                        process.stdin.close()
            
            # Start feeding input
            feed_task = aio.create_task(feed_input())
            
            # Read FFmpeg output and yield
            while True:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
            
            await feed_task
            
            # Check for errors
            stderr_data = await process.stderr.read()
            if process.returncode and process.returncode != 0:
                LOGGER.warning(f"FFmpeg transcode stderr: {stderr_data.decode()[:500]}")
            
        except Exception as e:
            LOGGER.exception(f"Transcode stream error: {e}")
        finally:
            if process and process.returncode is None:
                try:
                    process.kill()
                except Exception:
                    pass
    
    return StreamingResponse(
        generate(),
        media_type="video/mp4",
        headers={
            "Content-Type": "video/mp4",
            "Accept-Ranges": "none",
            "Cache-Control": "no-cache",
        }
    )


async def media_streamer(
    request: Request,
    chat_id: int,
    msg_id: int,
    secure_hash: str,
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


# =============================================================================
# HLS STREAMING ENDPOINTS
# =============================================================================

@router.get("/hls/{id}/master.m3u8")
async def hls_master_playlist(request: Request, id: str):
    """
    Return HLS master playlist with available quality variants.
    This is the entry point for HLS streaming.
    """
    try:
        decoded = await decode_string(id)
        msg_id = decoded.get("msg_id")
        chat_id = decoded.get("chat_id")
        
        if not msg_id:
            raise HTTPException(status_code=400, detail="Missing id")
        
        full_chat_id = int(f"-100{chat_id}")
        message = await StreamBot.get_messages(full_chat_id, int(msg_id))
        file = message.video or message.document
        
        if not file:
            raise HTTPException(status_code=404, detail="No video found")
        
        # Get video dimensions if available
        width = getattr(file, "width", 1920) or 1920
        height = getattr(file, "height", 1080) or 1080
        duration = getattr(file, "duration", 0) or 0
        
        # If duration not in metadata, estimate from file size (rough: 1MB = 8 seconds for 1Mbps)
        if not duration and file.file_size:
            duration = file.file_size / (1024 * 1024) * 8  # Rough estimate
        
        # Store metadata for later use
        _video_metadata[id] = {
            "msg_id": msg_id,
            "chat_id": chat_id,
            "width": width,
            "height": height,
            "duration": duration,
            "file_size": file.file_size,
            "file_unique_id": file.file_unique_id,
        }
        
        # Determine available qualities based on source resolution
        available_qualities = estimate_qualities_from_resolution(width, height)
        
        # Generate master playlist
        base_url = str(request.base_url).rstrip("/")
        playlist = generate_master_playlist(id, base_url, available_qualities)
        
        return Response(
            content=playlist,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"HLS master playlist error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hls/{id}/{quality}/playlist.m3u8")
async def hls_variant_playlist(request: Request, id: str, quality: str):
    """
    Return HLS variant playlist for a specific quality level.
    Lists all segments for this quality.
    """
    try:
        if quality not in QUALITY_PRESETS:
            raise HTTPException(status_code=400, detail=f"Invalid quality: {quality}")
        
        # Get stored metadata or fetch it
        meta = _video_metadata.get(id)
        if not meta:
            # Trigger master playlist to populate metadata
            decoded = await decode_string(id)
            msg_id = decoded.get("msg_id")
            chat_id = decoded.get("chat_id")
            
            full_chat_id = int(f"-100{chat_id}")
            message = await StreamBot.get_messages(full_chat_id, int(msg_id))
            file = message.video or message.document
            
            if not file:
                raise HTTPException(status_code=404, detail="No video found")
            
            duration = getattr(file, "duration", 0) or 0
            if not duration and file.file_size:
                duration = file.file_size / (1024 * 1024) * 8
            
            meta = {"duration": duration}
            _video_metadata[id] = meta
        
        duration = meta.get("duration", 0)
        if duration <= 0:
            duration = 3600  # Default to 1 hour if unknown
        
        base_url = str(request.base_url).rstrip("/")
        playlist = generate_variant_playlist(id, quality, duration, base_url)
        
        return Response(
            content=playlist,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"HLS variant playlist error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hls/{id}/{quality}/segment_{segment_num}.ts")
async def hls_segment(request: Request, id: str, quality: str, segment_num: int):
    """
    Return a transcoded HLS segment.
    Each segment is ~6 seconds of video transcoded to the requested quality.
    """
    try:
        if quality not in QUALITY_PRESETS:
            raise HTTPException(status_code=400, detail=f"Invalid quality: {quality}")
        
        # Check cache first
        cache_key = get_cache_key(id, segment_num)
        cached = get_cached_segment(cache_key)
        if cached:
            LOGGER.debug(f"HLS cache hit: {cache_key}")
            return Response(
                content=cached,
                media_type="video/mp2t",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "public, max-age=3600",
                }
            )
        
        # Decode video ID and get file
        decoded = await decode_string(id)
        msg_id = decoded.get("msg_id")
        chat_id = decoded.get("chat_id")
        
        if not msg_id:
            raise HTTPException(status_code=400, detail="Missing id")
        
        full_chat_id = int(f"-100{chat_id}")
        
        # Get streaming client
        index = min(work_loads, key=work_loads.get)
        tg_client = multi_clients[index]
        
        if tg_client not in _streamer_by_client:
            _streamer_by_client[tg_client] = ByteStreamer(tg_client)
        streamer: ByteStreamer = _streamer_by_client[tg_client]
        
        file_id = await streamer.get_file_properties(full_chat_id, int(msg_id))
        file_size = file_id.file_size
        
        # Calculate byte range for this segment
        # Approximate: segment_num * duration * bitrate
        # For simplicity, we'll stream from start and let FFmpeg seek
        
        # Create async generator for the video data
        async def video_data_generator():
            """Stream video data from Telegram"""
            chunk_size = 1024 * 1024  # 1MB chunks
            offset = 0
            
            # We need enough data for FFmpeg to process
            # Stream first 50MB or file size, whichever is smaller
            max_bytes = min(file_size, 50 * 1024 * 1024)
            
            media_session = await streamer._get_media_session(file_id)
            location = await streamer._get_location(file_id)
            
            while offset < max_bytes:
                try:
                    from pyrogram import raw
                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size
                        )
                    )
                    chunk = getattr(r, "bytes", b"")
                    if not chunk:
                        break
                    yield chunk
                    offset += len(chunk)
                except Exception as e:
                    LOGGER.error(f"Error fetching chunk at offset {offset}: {e}")
                    break
        
        # Transcode the segment
        segment_data = await transcode_segment(
            input_generator=video_data_generator(),
            quality=quality,
            segment_num=segment_num,
            segment_duration=SEGMENT_DURATION,
            file_size=file_size,
        )
        
        if not segment_data:
            raise HTTPException(status_code=500, detail="Remux failed")
        
        # Cache the segment
        cache_segment(cache_key, segment_data)
        
        return Response(
            content=segment_data,
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=3600",
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"HLS segment error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# AUDIO TRACK PROBE ENDPOINT
# =============================================================================

@router.get("/probe/audio/{id}")
async def probe_audio_tracks(request: Request, id: str):
    """
    Probe a video file and return its audio tracks.
    This helps identify multi-audio files for the Stremio UI.
    """
    try:
        # Check cache first
        cached = get_cached_audio_tracks(id)
        if cached is not None:
            return JSONResponse({
                "file_id": id,
                "audio_tracks": cached,
                "cached": True
            })
        
        # Decode ID and get file info
        decoded = await decode_string(id)
        msg_id = decoded.get("msg_id")
        if not msg_id:
            raise HTTPException(status_code=400, detail="Missing id")

        chat_id = int(f"-100{decoded['chat_id']}")
        
        # Get file info
        index = min(work_loads, key=work_loads.get)
        tg_client = multi_clients[index]
        
        if tg_client not in _streamer_by_client:
            _streamer_by_client[tg_client] = ByteStreamer(tg_client)
        streamer: ByteStreamer = _streamer_by_client[tg_client]
        
        file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=int(msg_id))
        
        # Create generator to stream file data for probing
        async def video_data_generator():
            chunk_size = streamer.CHUNK_SIZE
            offset = 0
            part_count = max(10, file_id.file_size // chunk_size)  # Get first 10MB for probing
            
            gen = await streamer.prefetch_stream(
                file_id=file_id,
                client_index=index,
                offset=offset,
                first_part_cut=0,
                last_part_cut=chunk_size,
                part_count=min(part_count, 10),  # Limit to first 10 chunks
                chunk_size=chunk_size,
                prefetch=3,
                parallelism=2,
            )
            async for item in gen:
                # Handle both tuple (offset, chunk) and other formats
                if isinstance(item, tuple) and len(item) >= 2:
                    _, chunk_data = item[0], item[1]
                    if chunk_data:
                        yield chunk_data
                elif item:
                    yield item
        
        # Probe audio tracks
        audio_tracks = await probe_audio_tracks_from_stream(
            input_generator=video_data_generator(),
            file_size=file_id.file_size,
        )
        
        # Cache the result
        cache_audio_tracks(id, audio_tracks)
        
        return JSONResponse({
            "file_id": id,
            "audio_tracks": audio_tracks,
            "cached": False
        })
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"Audio probe error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/probe/subtitles/{id}")
async def probe_subtitles(id: str):
    """
    Probe subtitle tracks for a video file.
    Returns list of available subtitle tracks with language and format info.
    """
    # Check cache first
    cached = get_cached_subtitle_tracks(id)
    if cached is not None:
        return JSONResponse({
            "file_id": id,
            "subtitle_tracks": cached,
            "cached": True
        })
    
    try:
        import tempfile
        import os
        
        decoded = await decode_string(id)
        msg_id = decoded.get("msg_id")
        if not msg_id:
            raise HTTPException(status_code=400, detail="Missing id")

        chat_id = int(f"-100{decoded['chat_id']}")
        
        # Use same pattern as audio probe
        index = min(work_loads, key=work_loads.get)
        tg_client = multi_clients[index]
        
        if tg_client not in _streamer_by_client:
            _streamer_by_client[tg_client] = ByteStreamer(tg_client)
        streamer: ByteStreamer = _streamer_by_client[tg_client]
        
        file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=int(msg_id))
        
        temp_dir = tempfile.mkdtemp(prefix="sub_probe_")
        input_path = os.path.join(temp_dir, "input.mkv")
        
        try:
            # Download first ~10MB for probing (subtitle metadata is in file headers)
            chunk_size = streamer.CHUNK_SIZE
            max_probe_chunks = 10
            downloaded = 0
            
            with open(input_path, "wb") as f:
                gen = await streamer.prefetch_stream(
                    file_id=file_id,
                    client_index=index,
                    offset=0,
                    first_part_cut=0,
                    last_part_cut=chunk_size,
                    part_count=max_probe_chunks,
                    chunk_size=chunk_size,
                    prefetch=3,
                    parallelism=2,
                )
                async for item in gen:
                    if isinstance(item, tuple) and len(item) >= 2:
                        chunk = item[1]
                    else:
                        chunk = item
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            LOGGER.info(f"Downloaded {downloaded} bytes for subtitle probing")
            
            # Probe subtitle tracks
            subtitle_tracks = await probe_subtitle_tracks(input_path)
            
            # Cache the result
            cache_subtitle_tracks(id, subtitle_tracks)
            
            return JSONResponse({
                "file_id": id,
                "subtitle_tracks": subtitle_tracks,
                "cached": False
            })
            
        finally:
            try:
                if os.path.exists(input_path):
                    os.remove(input_path)
                os.rmdir(temp_dir)
            except Exception:
                pass
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"Subtitle probe error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subtitles/{id}/{track}.vtt")
async def get_subtitle(id: str, track: int):
    """
    Extract and return a subtitle track as WebVTT.
    
    Args:
        id: File ID
        track: Subtitle track index (0-based)
    """
    try:
        import tempfile
        import os
        import asyncio as aio
        
        decoded = await decode_string(id)
        msg_id = decoded.get("msg_id")
        if not msg_id:
            raise HTTPException(status_code=400, detail="Missing id")

        chat_id = int(f"-100{decoded['chat_id']}")
        
        # Use same pattern as audio probe
        index = min(work_loads, key=work_loads.get)
        tg_client = multi_clients[index]
        
        if tg_client not in _streamer_by_client:
            _streamer_by_client[tg_client] = ByteStreamer(tg_client)
        streamer: ByteStreamer = _streamer_by_client[tg_client]
        
        file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=int(msg_id))
        
        temp_dir = tempfile.mkdtemp(prefix="sub_extract_")
        input_path = os.path.join(temp_dir, "input.mkv")
        output_path = os.path.join(temp_dir, "output.vtt")
        
        try:
            # Download the file for extraction
            chunk_size = streamer.CHUNK_SIZE
            part_count = max(1, file_id.file_size // chunk_size)
            
            with open(input_path, "wb") as f:
                gen = await streamer.prefetch_stream(
                    file_id=file_id,
                    client_index=index,
                    offset=0,
                    first_part_cut=0,
                    last_part_cut=chunk_size,
                    part_count=part_count,
                    chunk_size=chunk_size,
                    prefetch=5,
                    parallelism=3,
                )
                async for item in gen:
                    if isinstance(item, tuple) and len(item) >= 2:
                        chunk = item[1]
                    else:
                        chunk = item
                    if chunk:
                        f.write(chunk)
            
            # Extract subtitle using FFmpeg
            cmd = [
                "ffmpeg",
                "-y",
                "-i", input_path,
                "-map", f"0:s:{track}",
                "-c:s", "webvtt",
                output_path
            ]
            
            process = await aio.create_subprocess_exec(
                *cmd,
                stdout=aio.subprocess.PIPE,
                stderr=aio.subprocess.PIPE
            )
            
            _, stderr = await aio.wait_for(
                process.communicate(),
                timeout=120
            )
            
            if process.returncode != 0:
                LOGGER.warning(f"FFmpeg subtitle error: {stderr.decode()[:500]}")
                raise HTTPException(status_code=500, detail="Failed to extract subtitle")
            
            if os.path.exists(output_path):
                with open(output_path, "r", encoding="utf-8") as f:
                    vtt_content = f.read()
                
                return Response(
                    content=vtt_content,
                    media_type="text/vtt",
                    headers={
                        "Content-Disposition": f"inline; filename=subtitle_{track}.vtt",
                        "Access-Control-Allow-Origin": "*"
                    }
                )
            else:
                raise HTTPException(status_code=404, detail="Subtitle track not found")
            
        finally:
            try:
                if os.path.exists(input_path):
                    os.remove(input_path)
                if os.path.exists(output_path):
                    os.remove(output_path)
                os.rmdir(temp_dir)
            except Exception:
                pass
        
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.exception(f"Subtitle extraction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/{id}")
async def get_all_tracks(id: str):
    """
    Get both audio and subtitle tracks for a video file.
    Convenience endpoint for the web player.
    """
    audio_tracks = get_cached_audio_tracks(id) or []
    subtitle_tracks = get_cached_subtitle_tracks(id) or []
    
    return JSONResponse({
        "file_id": id,
        "audio_tracks": audio_tracks,
        "subtitle_tracks": subtitle_tracks
    })
