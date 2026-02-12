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

def _score_result(name: str, result_year: int, query_title: str, query_year: int = None) -> int:
    """Score a search result by title similarity + year match."""
    name_lower = name.lower().strip()
    query_lower = query_title.lower().strip()
    score = 0
    
    # Title matching
    if name_lower == query_lower:
        score += 100
    elif name_lower.startswith(query_lower) or query_lower.startswith(name_lower):
        score += 60
    elif query_lower in name_lower or name_lower in query_lower:
        score += 30
    
    # Year matching
    if query_year and result_year:
        if result_year == query_year:
            score += 50
        elif abs(result_year - query_year) <= 1:
            score += 20
    
    return score

# Map Indian language names to ISO 639-1 codes for TMDb API
LANG_TO_ISO = {
    "Hindi": "hi", "Malayalam": "ml", "Tamil": "ta", "Telugu": "te",
    "Kannada": "kn", "Bengali": "bn", "Marathi": "mr",
    "Gujarati": "gu", "Punjabi": "pa",
}


async def search_title(query: str, type: str, languages: list = None) -> Optional[Dict[str, Any]]:
    """Search for a title using multiple sources, prioritizing language-specific TMDb for Indian movies."""
    # Extract title and year from query
    query_year = None
    query_title = query.strip()
    year_match = re.search(r'\b((?:19|20)\d{2})\s*$', query_title)
    if year_match:
        query_year = int(year_match.group(1))
        query_title = query_title[:year_match.start()].strip()
    
    # Step 1: If Indian languages detected, try TMDb with language filter FIRST
    # This is the most reliable way to find Indian regional movies
    if languages:
        for lang in languages:
            iso_code = LANG_TO_ISO.get(lang)
            if iso_code:
                tmdb_result = await _tmdb_direct_search(
                    query_title, type, query_year, iso_code
                )
                if tmdb_result:
                    return tmdb_result
    
    # Step 2: Try IMDb suggestion API
    result = await _imdb_suggestion_search(query_title, type, query_year)
    if result:
        return result
    
    # Step 3: Fallback to Cinemeta
    return await _cinemeta_search(query, type, query_title, query_year)


async def _tmdb_direct_search(title: str, type: str, year: int = None, lang_code: str = None) -> Optional[Dict[str, Any]]:
    """Search TMDb API directly with language filter for regional movies."""
    from Backend.config import Telegram
    
    api_key = Telegram.TMDB_API
    if not api_key:
        return None
    
    client = await _get_client()
    media_type = "tv" if type in ("tvSeries", "series") else "movie"
    
    params = {
        "api_key": api_key,
        "query": title,
    }
    if year:
        params["year" if media_type == "movie" else "first_air_date_year"] = year
    
    url = f"https://api.themoviedb.org/3/search/{media_type}"
    
    try:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None
        
        # Filter by original_language if specified, then score
        best = None
        best_score = -1
        
        for item in results:
            # Skip results that don't match the target language
            if lang_code and item.get("original_language") != lang_code:
                continue
            
            item_title = item.get("title") or item.get("name") or ""
            release = item.get("release_date") or item.get("first_air_date") or ""
            item_year = int(release[:4]) if release and len(release) >= 4 else 0
            
            score = _score_result(item_title, item_year, title, year)
            if score > best_score:
                best_score = score
                best = item
        
        if best and best_score >= 50:
            # Get IMDb ID from TMDb external IDs
            tmdb_id = best.get("id")
            imdb_id = await _get_imdb_from_tmdb(tmdb_id, media_type, api_key)
            
            best_title = best.get("title") or best.get("name") or ""
            release = best.get("release_date") or best.get("first_air_date") or ""
            best_year = release[:4] if release and len(release) >= 4 else ""
            
            return {
                'id': imdb_id or f"tmdb:{tmdb_id}",
                'type': type,
                'title': best_title,
                'year': best_year,
                'poster': f"https://image.tmdb.org/t/p/w500{best.get('poster_path')}" if best.get('poster_path') else ''
            }
        return None
    except Exception:
        return None


async def _get_imdb_from_tmdb(tmdb_id: int, media_type: str, api_key: str) -> Optional[str]:
    """Get IMDb ID from TMDb external IDs."""
    client = await _get_client()
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids?api_key={api_key}"
        resp = await client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            imdb_id = data.get("imdb_id")
            if imdb_id and imdb_id.startswith("tt"):
                return imdb_id
    except Exception:
        pass
    return None


async def _imdb_suggestion_search(title: str, type: str, year: int = None) -> Optional[Dict[str, Any]]:
    """Search using IMDb's own suggestion API - most accurate."""
    client = await _get_client()
    
    # IMDb suggestion API: first char of query in URL path
    search_query = f"{title} {year}" if year else title
    safe_query = re.sub(r'[^\w\s]', '', search_query).strip().replace(' ', '%20')
    first_char = safe_query[0].lower() if safe_query else 'a'
    url = f"https://v2.sg.media-imdb.com/suggestion/{first_char}/{safe_query}.json"
    
    # Map our types to IMDb qid values
    type_map = {"movie": "movie", "tvSeries": "tvSeries", "series": "tvSeries"}
    target_qid = type_map.get(type, "movie")
    
    try:
        resp = await client.get(url, headers={"Accept": "application/json"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("d", [])
        if not results:
            return None
        
        best = None
        best_score = -1
        
        for item in results:
            # Filter by type (qid: "movie", "tvSeries", "tvMiniSeries", etc.)
            item_qid = item.get("qid", "")
            if target_qid == "movie" and item_qid not in ("movie", "short", ""):
                continue
            if target_qid == "tvSeries" and item_qid not in ("tvSeries", "tvMiniSeries", ""):
                continue
            
            item_id = item.get("id", "")
            if not item_id.startswith("tt"):
                continue
            
            item_title = item.get("l", "")
            item_year = item.get("y", 0)
            
            score = _score_result(item_title, item_year, title, year)
            if score > best_score:
                best_score = score
                best = item
        
        # Require minimum score of 50 (at least decent match)
        if best and best_score >= 50:
            return {
                'id': best.get('id', ''),
                'type': type,
                'title': best.get('l', ''),
                'year': str(best.get('y', '')),
                'poster': (best.get('i', {}) or {}).get('imageUrl', '')
            }
        return None
    except Exception:
        return None


async def _cinemeta_search(query: str, type: str, query_title: str, query_year: int = None) -> Optional[Dict[str, Any]]:
    """Fallback search using Cinemeta/Stremio API."""
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
        
        best_meta = None
        best_score = -1
        
        for meta in data['metas']:
            name = meta.get('name') or ''
            meta_year = extract_first_year(meta.get('releaseInfo', ''))
            score = _score_result(name, meta_year, query_title, query_year)
            
            if score > best_score:
                best_score = score
                best_meta = meta
        
        if best_meta and best_score >= 60:
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