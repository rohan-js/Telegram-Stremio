"""
HLS Remux Module
Provides on-the-fly HLS segmentation using FFmpeg REMUX (copy mode).
NO transcoding - just repackages video into HLS format for smooth streaming.
"""

import asyncio
import os
import time
import tempfile
import shutil
from typing import Dict, Optional, Tuple
from collections import OrderedDict
from Backend.logger import LOGGER

# Segment cache with TTL
SEGMENT_CACHE: Dict[str, Tuple[bytes, float]] = OrderedDict()
CACHE_MAX_SIZE = 100  # Max segments in cache
CACHE_TTL = 600  # 10 minutes

# Segment duration in seconds
SEGMENT_DURATION = 10


def get_cache_key(file_id: str, segment_num: int) -> str:
    """Generate unique cache key for a segment"""
    return f"{file_id}:{segment_num}"


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


def generate_master_playlist(file_id: str, base_url: str, available_qualities: list = None) -> str:
    """
    Generate simple HLS master playlist (single quality - original).
    """
    return """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=5000000,NAME="Original"
original/playlist.m3u8"""


def generate_variant_playlist(
    file_id: str, 
    quality: str,
    total_duration: float,
    base_url: str
) -> str:
    """
    Generate HLS playlist for remuxed segments.
    """
    segment_count = int(total_duration / SEGMENT_DURATION) + 1
    
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{SEGMENT_DURATION + 1}",
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


async def remux_segment(
    input_generator,
    segment_num: int,
    segment_duration: float = SEGMENT_DURATION,
    file_size: int = 0,
) -> Optional[bytes]:
    """
    Remux a specific segment of the video using FFmpeg COPY mode.
    
    NO TRANSCODING - just copies streams into MPEG-TS container.
    This is extremely fast and uses minimal CPU.
    """
    start_time = segment_num * segment_duration
    
    # Create temp files for input and output
    temp_dir = tempfile.mkdtemp(prefix="hls_remux_")
    input_path = os.path.join(temp_dir, "input.mkv")
    output_path = os.path.join(temp_dir, "segment.ts")
    
    try:
        LOGGER.debug(f"Remuxing segment {segment_num}, start_time={start_time}")
        
        # Collect video data and write to temp file
        with open(input_path, "wb") as f:
            async for chunk in input_generator:
                f.write(chunk)
        
        input_size = os.path.getsize(input_path)
        LOGGER.debug(f"Input file size: {input_size} bytes")
        
        if input_size < 1024:
            LOGGER.error("Input file too small")
            return None
        
        # FFmpeg REMUX command - NO ENCODING (copy mode)
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-ss", str(start_time),
            "-i", input_path,
            "-t", str(segment_duration),
            "-c", "copy",  # COPY MODE - no encoding!
            "-f", "mpegts",
            "-copyts",
            "-avoid_negative_ts", "make_zero",
            "-y",
            output_path
        ]
        
        LOGGER.debug(f"FFmpeg remux command: {' '.join(cmd)}")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            LOGGER.error(f"Remux timeout for segment {segment_num}")
            return None
        
        if proc.returncode != 0:
            LOGGER.error(f"FFmpeg remux failed: {stderr.decode()}")
            return None
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            with open(output_path, "rb") as f:
                segment_data = f.read()
            LOGGER.debug(f"Segment {segment_num} remuxed: {len(segment_data)} bytes")
            return segment_data
        else:
            LOGGER.error(f"Output segment empty or not found")
            return None
            
    except Exception as e:
        LOGGER.exception(f"Remux error: {e}")
        return None
    finally:
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


# Compatibility aliases
def estimate_qualities_from_resolution(width: int, height: int) -> list:
    """Return single 'original' quality since we're remuxing"""
    return ["original"]


QUALITY_PRESETS = {
    "original": type('Preset', (), {'name': 'Original', 'bandwidth': 5000000})()
}


async def transcode_segment(input_generator, quality: str = None, segment_num: int = 0, 
                           segment_duration: float = SEGMENT_DURATION, file_size: int = 0):
    """Alias for remux_segment for backwards compatibility"""
    return await remux_segment(input_generator, segment_num, segment_duration, file_size)
