"""YouTube link ingestion helpers (videos + Shorts).

Extracts video IDs from channel message text, validates them, and fetches
display metadata through YouTube's keyless oEmbed endpoint. No API key, no
new dependencies (stdlib urllib only). Playlists / channels / live URLs are
deliberately out of scope for v1.
"""

import json
import re
import urllib.parse
import urllib.request
from typing import Optional

YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

# Matches the v1-supported watch-style URL forms. The video ID is captured
# from either the `v` query parameter or the first path segment.
YOUTUBE_URL_RE = re.compile(
    r"https?://(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com)"
    r"(?P<path>/[^\s<>\")']*)",
    re.IGNORECASE,
)

_VALID_PATH_PREFIXES = ("/watch", "/shorts/", "/embed/", "/live/", "/v/")


def _id_from_url(url: str) -> Optional[str]:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    if "youtu.be" in host:
        candidate = parsed.path.strip("/").split("/")[0] if parsed.path.strip("/") else ""
        return candidate or None
    if "youtube" not in host:
        return None
    path = parsed.path or ""
    if path == "/watch":
        params = urllib.parse.parse_qs(parsed.query or "")
        for value in params.get("v", []):
            if value:
                return value
        return None
    for prefix in _VALID_PATH_PREFIXES:
        if path.startswith(prefix):
            rest = path[len(prefix):].strip("/")
            candidate = rest.split("/")[0] if rest else ""
            return candidate or None
    return None


def is_valid_youtube_id(video_id: str) -> bool:
    return bool(video_id) and bool(YOUTUBE_ID_RE.match(video_id))


def extract_youtube_ids(text: str) -> list:
    """Return validated, de-duplicated YouTube video IDs in order of appearance."""
    found: list = []
    seen = set()
    for match in YOUTUBE_URL_RE.finditer(text or ""):
        video_id = _id_from_url(match.group(0))
        if video_id and is_valid_youtube_id(video_id) and video_id not in seen:
            seen.add(video_id)
            found.append(video_id)
    return found


def strip_youtube_urls(text: str) -> str:
    """Remove YouTube URLs from caption text (matcher title intent)."""
    return YOUTUBE_URL_RE.sub(" ", text or "").strip()


def youtube_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def fetch_youtube_oembed(video_id: str, timeout: float = 4.0) -> Optional[dict]:
    """Keyless oEmbed lookup → {"title", "author"} or None on any failure."""
    if not is_valid_youtube_id(video_id):
        return None
    endpoint = "https://www.youtube.com/oembed?url=" + urllib.parse.quote(
        youtube_watch_url(video_id), safe=""
    ) + "&format=json"
    try:
        request = urllib.request.Request(endpoint, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request, timeout=max(1.0, float(timeout))) as response:
            payload = json.loads(response.read().decode("utf-8", "replace"))
        title = str(payload.get("title") or "").strip()
        if not title:
            return None
        return {"title": title, "author": str(payload.get("author_name") or "").strip()}
    except Exception:
        return None
