"""
Subtitle Track Detection and Extraction Module
Uses FFprobe to detect and FFmpeg to extract subtitles from video files.
"""

import asyncio
import json
import tempfile
import os
from typing import Dict, List, Optional
from Backend.logger import LOGGER

# Cache for subtitle track info (file_id -> tracks)
SUBTITLE_TRACK_CACHE: Dict[str, List[dict]] = {}


async def probe_subtitle_tracks(file_path: str) -> List[dict]:
    """
    Probe subtitle tracks from a local video file using FFprobe.
    
    Args:
        file_path: Path to the video file
    
    Returns:
        List of subtitle track dicts with index, language, codec, title
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "s",  # Only subtitle streams
            file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=30
        )
        
        if process.returncode != 0:
            LOGGER.warning(f"FFprobe subtitle error: {stderr.decode()}")
            return []
        
        data = json.loads(stdout.decode())
        streams = data.get("streams", [])
        
        tracks = []
        for i, stream in enumerate(streams):
            tags = stream.get("tags", {})
            
            # Get language
            lang = tags.get("language", "und")
            lang_name = get_language_name(lang)
            
            # Get codec
            codec = stream.get("codec_name", "unknown")
            
            # Get title if available
            title = tags.get("title", "")
            
            tracks.append({
                "index": stream.get("index", i),
                "stream_index": i,
                "language": lang,
                "language_name": lang_name,
                "codec": codec,
                "title": title,
                "forced": tags.get("forced", "0") == "1",
                "default": stream.get("disposition", {}).get("default", 0) == 1
            })
        
        return tracks
        
    except asyncio.TimeoutError:
        LOGGER.error("FFprobe subtitle timeout")
        return []
    except Exception as e:
        LOGGER.exception(f"Subtitle probe error: {e}")
        return []


async def extract_subtitle_to_vtt(
    input_generator,
    subtitle_index: int,
    file_size: int = 0,
    max_download_size: int = 0
) -> Optional[str]:
    """
    Extract a subtitle track from streaming video and convert to VTT.
    
    Args:
        input_generator: Async generator yielding video bytes
        subtitle_index: Index of the subtitle track to extract
        file_size: Total file size
        max_download_size: Maximum bytes to download (0 = entire file)
    
    Returns:
        VTT subtitle content as string, or None on failure
    """
    temp_dir = tempfile.mkdtemp(prefix="sub_extract_")
    input_path = os.path.join(temp_dir, "input.mkv")
    output_path = os.path.join(temp_dir, "output.vtt")
    
    try:
        # Download file (or portion for subtitle extraction)
        # For text subtitles, we often need the full file
        downloaded = 0
        max_size = max_download_size if max_download_size > 0 else float('inf')
        
        with open(input_path, "wb") as f:
            async for item in input_generator:
                if isinstance(item, tuple):
                    _, chunk = item
                else:
                    chunk = item
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded >= max_size:
                    break
        
        if downloaded < 1024:
            LOGGER.warning("Not enough data for subtitle extraction")
            return None
        
        # Extract subtitle using FFmpeg
        cmd = [
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-map", f"0:s:{subtitle_index}",
            "-c:s", "webvtt",
            output_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        _, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=120
        )
        
        if process.returncode != 0:
            LOGGER.warning(f"FFmpeg subtitle extraction error: {stderr.decode()[:500]}")
            return None
        
        # Read the VTT content
        if os.path.exists(output_path):
            with open(output_path, "r", encoding="utf-8") as f:
                return f.read()
        
        return None
        
    except asyncio.TimeoutError:
        LOGGER.error("FFmpeg subtitle extraction timeout")
        return None
    except Exception as e:
        LOGGER.exception(f"Subtitle extraction error: {e}")
        return None
    finally:
        # Cleanup temp files
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
            if os.path.exists(output_path):
                os.remove(output_path)
            os.rmdir(temp_dir)
        except Exception:
            pass


def get_language_name(lang_code: str) -> str:
    """Convert ISO 639 language code to human readable name."""
    LANGUAGE_MAP = {
        "eng": "English",
        "hin": "Hindi",
        "tam": "Tamil",
        "tel": "Telugu",
        "mal": "Malayalam",
        "kan": "Kannada",
        "ben": "Bengali",
        "mar": "Marathi",
        "guj": "Gujarati",
        "pan": "Punjabi",
        "urd": "Urdu",
        "spa": "Spanish",
        "fra": "French",
        "deu": "German",
        "ita": "Italian",
        "por": "Portuguese",
        "rus": "Russian",
        "jpn": "Japanese",
        "kor": "Korean",
        "chi": "Chinese",
        "zho": "Chinese",
        "ara": "Arabic",
        "tha": "Thai",
        "vie": "Vietnamese",
        "ind": "Indonesian",
        "may": "Malay",
        "und": "Unknown",
    }
    return LANGUAGE_MAP.get(lang_code, lang_code.upper())


def cache_subtitle_tracks(file_id: str, tracks: List[dict]) -> None:
    """Cache subtitle track info for a file."""
    SUBTITLE_TRACK_CACHE[file_id] = tracks


def get_cached_subtitle_tracks(file_id: str) -> Optional[List[dict]]:
    """Get cached subtitle track info for a file."""
    return SUBTITLE_TRACK_CACHE.get(file_id)


def format_subtitle_track_label(track: dict) -> str:
    """Format a subtitle track for display."""
    label = track.get("language_name", "Unknown")
    
    if track.get("title"):
        label = f"{label} - {track['title']}"
    
    if track.get("forced"):
        label = f"{label} [Forced]"
    
    if track.get("default"):
        label = f"{label} [Default]"
    
    return label
