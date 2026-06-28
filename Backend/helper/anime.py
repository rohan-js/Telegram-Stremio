import asyncio
import re
from difflib import SequenceMatcher
from typing import Optional

import httpx
try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover - used only in lightweight local environments
    fuzz = None

from Backend.logger import LOGGER

ANILIST_URL = "https://graphql.anilist.co"
ANIZIP_URL = "https://api.ani.zip/mappings"
_ANIME_TITLE_THRESHOLD = 0.55
_HTML_RE = re.compile(r"<[^>]+>")

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()
_SEARCH_CACHE: dict = {}
_MAP_CACHE: dict = {}

_FIELDS = """
    id
    title { romaji english }
    synonyms
    seasonYear
    startDate { year }
    description(asHtml: false)
    genres
    averageScore
    duration
    coverImage { extraLarge large }
    bannerImage
"""

_QUERY = "query ($search: String) { Media(search: $search, type: ANIME) {" + _FIELDS + "} }"
_MOVIE_QUERY = "query ($search: String) { Media(search: $search, type: ANIME, format: MOVIE) {" + _FIELDS + "} }"


async def _get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.AsyncClient(timeout=12.0, follow_redirects=True)
        return _client


def _strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", _HTML_RE.sub(" ", text or "")).strip()


def _normalize_title(title: str) -> str:
    value = (title or "").lower().strip()
    value = re.sub(r"^\b(the|a|an)\b\s+", "", value)
    value = re.sub(r"[^\w\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _fuzzy_ratio(left: str, right: str) -> float:
    left_norm = _normalize_title(left)
    right_norm = _normalize_title(right)
    if not left_norm or not right_norm:
        return 0.0
    if fuzz is not None:
        set_ratio = fuzz.token_set_ratio(left_norm, right_norm) / 100.0
        sort_ratio = fuzz.token_sort_ratio(left_norm, right_norm) / 100.0
    else:
        set_ratio = SequenceMatcher(None, left_norm, right_norm).ratio()
        sort_ratio = SequenceMatcher(
            None,
            " ".join(sorted(left_norm.split())),
            " ".join(sorted(right_norm.split())),
        ).ratio()
    left_tokens = left_norm.split()
    right_tokens = right_norm.split()
    coverage = min(len(left_tokens), len(right_tokens)) / max(len(left_tokens), len(right_tokens))
    return max(sort_ratio, set_ratio * coverage)


def _title_match_score(query: str, media: dict) -> float:
    titles = media.get("title") or {}
    candidates = [titles.get("english"), titles.get("romaji"), *(media.get("synonyms") or [])]
    return max((_fuzzy_ratio(query, candidate) for candidate in candidates if candidate), default=0.0)


async def _anilist_request(search: str, query: str) -> Optional[dict]:
    try:
        client = await _get_client()
        response = await client.post(ANILIST_URL, json={"query": query, "variables": {"search": search}})
        if response.status_code != 200:
            return None
        return ((response.json() or {}).get("data") or {}).get("Media")
    except Exception as exc:
        LOGGER.warning("[ANIME] AniList request failed for %s: %s", search, exc)
        return None


async def _search(title: str, season: Optional[int], movie: bool = False) -> Optional[dict]:
    key = f"{'movie' if movie else 'tv'}:{title}:{season or ''}"
    if key in _SEARCH_CACHE:
        return _SEARCH_CACHE[key]
    queries = [title]
    if season and int(season) > 1:
        queries = [f"{title} Season {season}", f"{title} {season}", title]
    result = None
    for query in queries:
        result = await _anilist_request(query, _MOVIE_QUERY if movie else _QUERY)
        if result:
            break
    if result and _title_match_score(title, result) < _ANIME_TITLE_THRESHOLD:
        result = None
    _SEARCH_CACHE[key] = result
    return result


async def _mappings(anilist_id: int) -> Optional[dict]:
    if anilist_id in _MAP_CACHE:
        return _MAP_CACHE[anilist_id]
    try:
        client = await _get_client()
        response = await client.get(ANIZIP_URL, params={"anilist_id": anilist_id})
        data = response.json() if response.status_code == 200 else None
    except Exception as exc:
        LOGGER.warning("[ANIME] ani.zip failed for %s: %s", anilist_id, exc)
        data = None
    _MAP_CACHE[anilist_id] = data
    return data


def _image(images, cover_type: str) -> str:
    for image in images or []:
        if str(image.get("coverType", "")).lower() == cover_type.lower() and image.get("url"):
            return image["url"]
    return ""


def _common_payload(media: dict, doc: dict, fallback_title: str) -> dict:
    mappings = doc.get("mappings") or {}
    tmdb_id = mappings.get("themoviedb_id")
    try:
        tmdb_id = int(tmdb_id) if tmdb_id else None
    except Exception:
        tmdb_id = None
    titles = media.get("title") or {}
    images = doc.get("images") or []
    cover = media.get("coverImage") or {}
    score = media.get("averageScore")
    duration = media.get("duration")
    return {
        "tmdb_id": tmdb_id,
        "imdb_id": mappings.get("imdb_id"),
        "title": titles.get("english") or titles.get("romaji") or fallback_title,
        "year": media.get("seasonYear") or (media.get("startDate") or {}).get("year") or 0,
        "rate": round(score / 10, 1) if score else 0,
        "description": _strip_html(media.get("description") or ""),
        "poster": cover.get("extraLarge") or cover.get("large") or _image(images, "Poster"),
        "backdrop": media.get("bannerImage") or _image(images, "Fanart") or _image(images, "Banner"),
        "logo": _image(images, "Clearlogo"),
        "genres": media.get("genres") or [],
        "cast": [],
        "runtime": f"{duration} min" if duration else "",
    }


async def fetch_anime_metadata(title, season, episode, encoded_string, year=None, quality=None) -> Optional[dict]:
    media = await _search(title, season, movie=False)
    if not media:
        return None
    doc = await _mappings(media["id"]) or {}
    payload = _common_payload(media, doc, title)
    if not payload.get("imdb_id") and not payload.get("tmdb_id"):
        return None
    ep = (doc.get("episodes") or {}).get(str(episode)) or {}
    ep_title = (ep.get("title") or {}).get("en") if isinstance(ep.get("title"), dict) else None
    payload.update({
        "media_type": "tv",
        "season_number": season,
        "episode_number": episode,
        "episode_title": ep_title or f"S{season:02d}E{episode:02d}",
        "episode_backdrop": ep.get("image", "") or "",
        "episode_overview": ep.get("overview") or ep.get("summary") or "",
        "episode_released": ep.get("airDate") or ep.get("airdate") or "",
        "quality": quality,
        "encoded_string": encoded_string,
        "is_anime": True,
    })
    return payload


async def fetch_anime_movie_metadata(title, encoded_string, year=None, quality=None) -> Optional[dict]:
    media = await _search(title, None, movie=True)
    if not media:
        return None
    doc = await _mappings(media["id"]) or {}
    payload = _common_payload(media, doc, title)
    if not payload.get("imdb_id") and not payload.get("tmdb_id"):
        return None
    payload.update({
        "media_type": "movie",
        "quality": quality,
        "encoded_string": encoded_string,
        "is_anime": True,
    })
    return payload
