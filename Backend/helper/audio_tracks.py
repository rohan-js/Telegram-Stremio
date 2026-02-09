"""
Audio Track Detection Module
Uses FFprobe to detect audio tracks in video files from Telegram.
"""

import asyncio
import json
from typing import Dict, List, Optional
from Backend.logger import LOGGER

# Cache for audio track info (file_id -> tracks)
AUDIO_TRACK_CACHE: Dict[str, List[dict]] = {}


async def probe_audio_tracks_from_stream(
    input_generator,
    file_size: int = 0,
) -> List[dict]:
    """
    Probe audio tracks from a streaming source using FFprobe.
    
    Args:
        input_generator: Async generator yielding video bytes
        file_size: Total file size (for estimation)
    
    Returns:
        List of audio track dicts with language, codec, channels
    """
    import tempfile
    import os
    
    temp_dir = tempfile.mkdtemp(prefix="audio_probe_")
    input_path = os.path.join(temp_dir, "input.mkv")
    
    try:
        # Download first 10MB for probing (enough to read headers)
        max_probe_size = 10 * 1024 * 1024
        downloaded = 0
        
        with open(input_path, "wb") as f:
            async for chunk in input_generator:
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded >= max_probe_size:
                    break
        
        if downloaded < 1024:
            LOGGER.warning("Not enough data for audio probe")
            return []
        
        # Run FFprobe
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "a",  # Audio streams only
            input_path
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            LOGGER.error("FFprobe timeout")
            return []
        
        if proc.returncode != 0:
            LOGGER.error(f"FFprobe failed: {stderr.decode()}")
            return []
        
        # Parse output
        data = json.loads(stdout.decode())
        streams = data.get("streams", [])
        
        tracks = []
        for idx, stream in enumerate(streams):
            # Get language from tags
            tags = stream.get("tags", {})
            language = tags.get("language", "und")
            title = tags.get("title", "")
            
            # Get codec and channels
            codec = stream.get("codec_name", "unknown")
            channels = stream.get("channels", 2)
            channel_layout = stream.get("channel_layout", "")
            
            # Format channel info
            if channels == 6 or "5.1" in channel_layout:
                channel_str = "5.1"
            elif channels == 8 or "7.1" in channel_layout:
                channel_str = "7.1"
            elif channels == 2:
                channel_str = "Stereo"
            elif channels == 1:
                channel_str = "Mono"
            else:
                channel_str = f"{channels}ch"
            
            # Language code to name mapping
            lang_names = {
                "eng": "English", "en": "English",
                "hin": "Hindi", "hi": "Hindi",
                "tam": "Tamil", "ta": "Tamil",
                "tel": "Telugu", "te": "Telugu",
                "kan": "Kannada", "kn": "Kannada",
                "mal": "Malayalam", "ml": "Malayalam",
                "mar": "Marathi", "mr": "Marathi",
                "ben": "Bengali", "bn": "Bengali",
                "guj": "Gujarati", "gu": "Gujarati",
                "pan": "Punjabi", "pa": "Punjabi",
                "spa": "Spanish", "es": "Spanish",
                "fra": "French", "fr": "French",
                "deu": "German", "de": "German",
                "ita": "Italian", "it": "Italian",
                "por": "Portuguese", "pt": "Portuguese",
                "jpn": "Japanese", "ja": "Japanese",
                "kor": "Korean", "ko": "Korean",
                "zho": "Chinese", "zh": "Chinese",
                "rus": "Russian", "ru": "Russian",
                "ara": "Arabic", "ar": "Arabic",
                "und": "Unknown",
            }
            
            lang_name = lang_names.get(language.lower(), language.upper())
            
            # Use title if available and descriptive
            if title and title.lower() not in ["audio", "stereo", "5.1", "default"]:
                display_name = title
            else:
                display_name = lang_name
            
            tracks.append({
                "index": stream.get("index", idx),
                "stream_index": idx,  # For -map selection
                "language": language,
                "language_name": lang_name,
                "display_name": display_name,
                "codec": codec.upper(),
                "channels": channels,
                "channel_str": channel_str,
                "title": title,
                "is_default": stream.get("disposition", {}).get("default", 0) == 1,
            })
        
        LOGGER.debug(f"Found {len(tracks)} audio tracks")
        return tracks
        
    except Exception as e:
        LOGGER.exception(f"Audio probe error: {e}")
        return []
    finally:
        # Cleanup
        import shutil
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


async def probe_audio_from_telegram(encoded_string: str) -> List[dict]:
    """
    Probe audio tracks from a Telegram file using the API endpoint.
    This is simpler than streaming - just makes HTTP request.
    
    Args:
        encoded_string: The encoded file ID string
        
    Returns:
        List of audio track dicts
    """
    import httpx
    from Backend.config import Telegram
    
    try:
        base_url = Telegram.BASE_URL.rstrip('/')
        url = f"{base_url}/probe/audio/{encoded_string}"
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            
            if response.status_code != 200:
                LOGGER.warning(f"Audio probe API returned {response.status_code}")
                return []
            
            data = response.json()
            tracks = data.get("audio_tracks", [])
            
            LOGGER.debug(f"API returned {len(tracks)} audio tracks")
            return tracks
            
    except Exception as e:
        LOGGER.warning(f"Audio probe API error: {e}")
        return []


def get_cached_audio_tracks(file_id: str) -> Optional[List[dict]]:
    """Get cached audio track info"""
    return AUDIO_TRACK_CACHE.get(file_id)


def cache_audio_tracks(file_id: str, tracks: List[dict]):
    """Cache audio track info"""
    AUDIO_TRACK_CACHE[file_id] = tracks


def format_audio_track_label(track: dict) -> str:
    """Format audio track for display in stream name"""
    display = track.get("display_name", "Audio")
    channel = track.get("channel_str", "")
    codec = track.get("codec", "")
    
    parts = [display]
    if channel:
        parts.append(channel)
    
    return " ".join(parts)
