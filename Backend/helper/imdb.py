import httpx
import re
import asyncio
from typing import Optional, Dict, Any

BASE_URL = "https://v3-cinemeta.strem.io"

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()

async def _get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=True
            )
        return _client

def extract_first_year(year_string) -> int:
    if not year_string:
        return 0
    year_str = str(year_string)
    year_match = re.search(r'(\d{4})', year_str)
    if year_match:
        return int(year_match.group(1))
    return 0

async def search_title(query: str, type: str) -> Optional[Dict[str, Any]]:
    client = await _get_client()
    cinemeta_type = "series" if type == "tvSeries" else type
    url = f"{BASE_URL}/catalog/{cinemeta_type}/imdb/search={query}.json"
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data or 'metas' not in data or not data['metas']:
            return None
        
        # Extract the title and year from the query
        # Query may be "Title 2025" or just "Title"
        query_year = None
        query_title = query.strip()
        year_match = re.search(r'\b((?:19|20)\d{2})\s*$', query_title)
        if year_match:
            query_year = int(year_match.group(1))
            query_title = query_title[:year_match.start()].strip()
        
        query_lower = query_title.lower()
        
        # Score each result by title similarity + year match
        best_meta = None
        best_score = -1
        
        for meta in data['metas']:
            name = (meta.get('name') or '').lower()
            score = 0
            
            # Title matching (most important)
            if name == query_lower:
                score += 100  # Exact match
            elif name.startswith(query_lower) or query_lower.startswith(name):
                score += 60   # Prefix match
            elif query_lower in name or name in query_lower:
                score += 30   # Substring match
            else:
                score += 0    # No match
            
            # Year matching
            if query_year:
                meta_year = extract_first_year(meta.get('releaseInfo', ''))
                if meta_year == query_year:
                    score += 50   # Exact year match
                elif meta_year and abs(meta_year - query_year) <= 1:
                    score += 20   # Off by 1 year (release date variations)
            
            if score > best_score:
                best_score = score
                best_meta = meta
        
        if best_meta:
            return {
                'id': best_meta.get('imdb_id', best_meta.get('id', '')),
                'type': type,
                'title': best_meta.get('name', ''),
                'year': best_meta.get('releaseInfo', ''),
                'poster': best_meta.get('poster', '')
            }
        return None
    except Exception:
        return None

async def get_detail(imdb_id: str, media_type: str) -> Optional[Dict[str, Any]]:
    client = await _get_client()
    cinemeta_type = "series" if media_type in ["tvSeries", "tv"] else "movie"

    try:
        url = f"{BASE_URL}/meta/{cinemeta_type}/{imdb_id}.json"
        resp = await client.get(url)

        if resp.status_code != 200:
            return None

        data = resp.json()
        meta = data.get("meta")
        if not meta:
            return None

        year_value = 0
        for field in ["year", "releaseInfo", "released"]:
            if meta.get(field):
                year_value = extract_first_year(meta[field])
                if year_value:
                    break

        return {
            "id": meta.get("imdb_id") or meta.get("id"),
            "moviedb_id": meta.get("moviedb_id"),
            "type": meta.get("type", media_type),
            "title": meta.get("name", ""),
            "plot": meta.get("description", ""),
            "genre": meta.get("genres") or meta.get("genre", []),
            "releaseDetailed": {"year": year_value},
            "rating": {"star": float(meta.get("imdbRating", 0) or 0)},
            "poster": meta.get("poster", ""),
            "background": meta.get("background", ""),
            "logo": meta.get("logo", ""),
            "runtime": meta.get("runtime") or 0,
            "director": meta.get("director", []),
            "cast": meta.get("cast", []),
            "videos": meta.get("videos", [])
        }

    except Exception:
        return None

async def get_season(imdb_id: str, season_id: int, episode_id: int) -> Optional[Dict[str, Any]]:
    client = await _get_client()
    try:
        url = f"{BASE_URL}/meta/series/{imdb_id}.json"
        resp = await client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if 'meta' in data and 'videos' in data['meta']:
            for video in data['meta']['videos']:
                if (str(video.get('season', '')) == str(season_id) and
                        str(video.get('episode', '')) == str(episode_id)):
                    return {
                        'title': video.get('title', f'Episode {episode_id}'),
                        'no': str(episode_id),
                        'season': str(season_id),
                        'image': video.get('thumbnail', ''),
                        'plot': video.get('overview', ''),
                        'released': video.get('released', '')
                    }
        return None
    except Exception:
        return None