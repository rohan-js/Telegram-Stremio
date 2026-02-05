"""
HLS Transcoder Module
Provides on-the-fly HLS transcoding using FFmpeg for YouTube-like adaptive streaming.
"""

import asyncio
import os
import time
import hashlib
import tempfile
import shutil
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
from Backend.logger import LOGGER

# Segment cache with TTL
SEGMENT_CACHE: Dict[str, Tuple[bytes, float]] = OrderedDict()
CACHE_MAX_SIZE = 100  # Max segments in cache
CACHE_TTL = 300  # 5 minutes

# Active transcoding jobs
ACTIVE_JOBS: Dict[str, asyncio.Task] = {}


@dataclass
class QualityPreset:
    """Video quality preset for transcoding"""
    name: str
    resolution: str  # e.g., "1280x720"
    video_bitrate: str  # e.g., "3000k"
    audio_bitrate: str  # e.g., "128k"
    bandwidth: int  # For m3u8 BANDWIDTH tag


# Quality presets - YouTube-like
QUALITY_PRESETS = {
    "360p": QualityPreset("360p", "640x360", "800k", "96k", 900000),
    "480p": QualityPreset("480p", "854x480", "1500k", "128k", 1700000),
    "720p": QualityPreset("720p", "1280x720", "3000k", "128k", 3500000),
    "1080p": QualityPreset("1080p", "1920x1080", "6000k", "192k", 6500000),
}

# Default qualities to offer (adjust based on source resolution)
DEFAULT_QUALITIES = ["480p", "720p", "1080p"]

# Segment duration in seconds
SEGMENT_DURATION = 6


def get_cache_key(file_id: str, quality: str, segment_num: int) -> str:
    """Generate unique cache key for a segment"""
    return f"{file_id}:{quality}:{segment_num}"


def clean_cache():
    """Remove expired segments from cache"""
    now = time.time()
    expired = [k for k, (_, ts) in SEGMENT_CACHE.items() if now - ts > CACHE_TTL]
    for k in expired:
        SEGMENT_CACHE.pop(k, None)
    
    # If still too large, remove oldest
    while len(SEGMENT_CACHE) > CACHE_MAX_SIZE:
        SEGMENT_CACHE.popitem(last=False)


def get_cached_segment(cache_key: str) -> Optional[bytes]:
    """Get segment from cache if exists and not expired"""
    if cache_key in SEGMENT_CACHE:
        data, ts = SEGMENT_CACHE[cache_key]
        if time.time() - ts < CACHE_TTL:
            return data
        else:
            SEGMENT_CACHE.pop(cache_key, None)
    return None


def cache_segment(cache_key: str, data: bytes):
    """Store segment in cache"""
    clean_cache()
    SEGMENT_CACHE[cache_key] = (data, time.time())


def generate_master_playlist(file_id: str, base_url: str, available_qualities: list) -> str:
    """
    Generate HLS master playlist with multiple quality variants.
    
    Example output:
    #EXTM3U
    #EXT-X-VERSION:3
    #EXT-X-STREAM-INF:BANDWIDTH=3500000,RESOLUTION=1280x720
    720p/playlist.m3u8
    """
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
    ]
    
    for quality in available_qualities:
        preset = QUALITY_PRESETS.get(quality)
        if preset:
            lines.append(
                f"#EXT-X-STREAM-INF:BANDWIDTH={preset.bandwidth},"
                f"RESOLUTION={preset.resolution},NAME=\"{preset.name}\""
            )
            lines.append(f"{quality}/playlist.m3u8")
    
    return "\n".join(lines)


def generate_variant_playlist(
    file_id: str, 
    quality: str, 
    total_duration: float,
    base_url: str
) -> str:
    """
    Generate HLS variant playlist for a specific quality.
    
    Example output:
    #EXTM3U
    #EXT-X-VERSION:3
    #EXT-X-TARGETDURATION:6
    #EXT-X-MEDIA-SEQUENCE:0
    #EXTINF:6.0,
    segment_0.ts
    #EXTINF:6.0,
    segment_1.ts
    ...
    #EXT-X-ENDLIST
    """
    segment_count = int(total_duration / SEGMENT_DURATION) + 1
    
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{SEGMENT_DURATION}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    
    for i in range(segment_count):
        # Last segment might be shorter
        if i == segment_count - 1:
            remaining = total_duration - (i * SEGMENT_DURATION)
            duration = max(0.5, remaining)
        else:
            duration = SEGMENT_DURATION
        
        lines.append(f"#EXTINF:{duration:.3f},")
        lines.append(f"segment_{i}.ts")
    
    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines)


async def get_video_duration(input_source) -> float:
    """
    Get video duration using ffprobe.
    input_source can be a file path or bytes.
    """
    try:
        if isinstance(input_source, bytes):
            # Write to temp file for ffprobe
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
                f.write(input_source[:1024*1024*10])  # First 10MB should have duration info
                temp_path = f.name
            input_path = temp_path
        else:
            input_path = input_source
            temp_path = None
        
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        
        if temp_path:
            os.unlink(temp_path)
        
        duration = float(stdout.decode().strip())
        return duration
    except Exception as e:
        LOGGER.error(f"Failed to get video duration: {e}")
        return 0.0


async def transcode_segment(
    input_generator,
    quality: str,
    segment_num: int,
    segment_duration: float = SEGMENT_DURATION,
    file_size: int = 0,
) -> Optional[bytes]:
    """
    Transcode a specific segment of the video using FFmpeg.
    
    Uses ffmpeg to:
    1. Seek to the segment start position
    2. Transcode to specified quality
    3. Output as MPEG-TS segment
    
    Args:
        input_generator: Async generator yielding video bytes
        quality: Quality preset name (e.g., "720p")
        segment_num: Segment number (0-indexed)
        segment_duration: Duration of each segment
        file_size: Total file size for seeking
    
    Returns:
        Transcoded segment bytes or None on failure
    """
    preset = QUALITY_PRESETS.get(quality)
    if not preset:
        LOGGER.error(f"Unknown quality preset: {quality}")
        return None
    
    start_time = segment_num * segment_duration
    
    # Create temp directory for this transcode job
    temp_dir = tempfile.mkdtemp(prefix="hls_")
    output_path = os.path.join(temp_dir, "segment.ts")
    
    try:
        # FFmpeg command for segment transcoding
        # Using pipe input for streaming from Telegram
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-ss", str(start_time),  # Seek to segment start
            "-i", "pipe:0",  # Read from stdin
            "-t", str(segment_duration),  # Segment duration
            "-c:v", "libx264",
            "-preset", "ultrafast",  # Fast encoding for real-time
            "-tune", "zerolatency",
            "-profile:v", "main",
            "-level", "4.0",
            "-b:v", preset.video_bitrate,
            "-maxrate", preset.video_bitrate,
            "-bufsize", f"{int(preset.video_bitrate[:-1]) * 2}k",
            "-vf", f"scale={preset.resolution}:force_original_aspect_ratio=decrease,pad={preset.resolution}:(ow-iw)/2:(oh-ih)/2",
            "-c:a", "aac",
            "-b:a", preset.audio_bitrate,
            "-ac", "2",
            "-ar", "44100",
            "-f", "mpegts",
            "-muxdelay", "0",
            "-muxpreload", "0",
            output_path
        ]
        
        LOGGER.debug(f"Transcoding segment {segment_num} at {quality}: {' '.join(cmd)}")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Stream input data to ffmpeg
        async def feed_input():
            try:
                async for chunk in input_generator:
                    if proc.stdin:
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
                if proc.stdin:
                    proc.stdin.close()
                    await proc.stdin.wait_closed()
            except Exception as e:
                LOGGER.debug(f"Feed input ended (expected): {e}")
        
        # Run feeding and wait for process
        feed_task = asyncio.create_task(feed_input())
        
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            LOGGER.error(f"Transcode timeout for segment {segment_num}")
            return None
        finally:
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass
        
        if proc.returncode != 0:
            LOGGER.error(f"FFmpeg failed: {stderr.decode()}")
            return None
        
        # Read output segment
        if os.path.exists(output_path):
            with open(output_path, "rb") as f:
                segment_data = f.read()
            LOGGER.debug(f"Segment {segment_num} transcoded: {len(segment_data)} bytes")
            return segment_data
        else:
            LOGGER.error(f"Output segment not found: {output_path}")
            return None
            
    except Exception as e:
        LOGGER.exception(f"Transcode error: {e}")
        return None
    finally:
        # Cleanup temp directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


async def transcode_segment_from_file(
    input_path: str,
    quality: str,
    segment_num: int,
    segment_duration: float = SEGMENT_DURATION,
) -> Optional[bytes]:
    """
    Transcode a segment from a local file (simpler path for cached files).
    """
    preset = QUALITY_PRESETS.get(quality)
    if not preset:
        return None
    
    start_time = segment_num * segment_duration
    temp_dir = tempfile.mkdtemp(prefix="hls_")
    output_path = os.path.join(temp_dir, "segment.ts")
    
    try:
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-ss", str(start_time),
            "-i", input_path,
            "-t", str(segment_duration),
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-b:v", preset.video_bitrate,
            "-vf", f"scale={preset.resolution}:force_original_aspect_ratio=decrease",
            "-c:a", "aac",
            "-b:a", preset.audio_bitrate,
            "-f", "mpegts",
            output_path
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        
        if proc.returncode == 0 and os.path.exists(output_path):
            with open(output_path, "rb") as f:
                return f.read()
        
        LOGGER.error(f"FFmpeg error: {stderr.decode()}")
        return None
        
    except Exception as e:
        LOGGER.exception(f"Transcode error: {e}")
        return None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def estimate_qualities_from_resolution(width: int, height: int) -> list:
    """
    Estimate available qualities based on source resolution.
    Don't offer qualities higher than source.
    """
    available = []
    
    if height >= 360:
        available.append("360p")
    if height >= 480:
        available.append("480p")
    if height >= 720:
        available.append("720p")
    if height >= 1080:
        available.append("1080p")
    
    # If we couldn't determine, offer all
    if not available:
        available = ["480p", "720p"]
    
    return available
