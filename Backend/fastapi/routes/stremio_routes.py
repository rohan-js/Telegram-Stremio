from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from typing import Optional
from urllib.parse import unquote
import re
from Backend.config import Telegram
from Backend import db, __version__
import PTN
from datetime import datetime, timezone, timedelta
from Backend.fastapi.security.tokens import verify_token


# --- Configuration ---
BASE_URL = Telegram.BASE_URL
ADDON_NAME = "Telegram"
ADDON_VERSION = __version__
PAGE_SIZE = 15

router = APIRouter(prefix="/stremio", tags=["Stremio Addon"])

# Define available genres
GENRES = [
    "Action", "Adventure", "Animation", "Biography", "Comedy",
    "Crime", "Documentary", "Drama", "Family", "Fantasy",
    "History", "Horror", "Music", "Mystery", "Romance",
    "Sci-Fi", "Sport", "Thriller", "War", "Western"
]


def format_released_date(media):
    year = media.get("release_year")
    if year:
        try:
            return datetime(int(year), 1, 1).isoformat() + "Z"
        except:
            return None

    return None

# --- Helper Functions ---
def convert_to_stremio_meta(item: dict) -> dict:
    media_type = "series" if item.get("media_type") == "tv" else "movie"
    
    meta = {
        "id": item.get('imdb_id'),
        "type": media_type,
        "name": item.get("title"),
        "poster": item.get("poster") or "",
        "logo": item.get("logo") or "",
        "year": item.get("release_year"),
        "releaseInfo": str(item.get("release_year", "")),
        "imdb_id": item.get("imdb_id", ""),
        "moviedb_id": item.get("tmdb_id", ""),
        "background": item.get("backdrop") or "",
        "genres": item.get("genres") or [],
        "imdbRating": str(item.get("rating") or ""),
        "description": item.get("description") or "",
        "cast": item.get("cast") or [],
        "runtime": item.get("runtime") or "",
    }

    return meta


def format_stream_details(filename: str, quality: str, size: str) -> tuple[str, str]:
    try:
        parsed = PTN.parse(filename)
    except Exception:
        return (f"Telegram {quality}", f"📁 {filename}\n💾 {size}")

    codec_parts = []
    if parsed.get("codec"):
        codec_parts.append(f"🎥 {parsed.get('codec')}")
    if parsed.get("bitDepth"):
        codec_parts.append(f"🌈 {parsed.get('bitDepth')}bit")
    if parsed.get("audio"):
        codec_parts.append(f"🔊 {parsed.get('audio')}")
    if parsed.get("encoder"):
        codec_parts.append(f"👤 {parsed.get('encoder')}")

    codec_info = " ".join(codec_parts) if codec_parts else ""

    resolution = parsed.get("resolution", quality)
    quality_type = parsed.get("quality", "")
    stream_name = f"Telegram {resolution} {quality_type}".strip()

    stream_title_parts = [
        f"📁 {filename}",
        f"💾 {size}",
    ]
    if codec_info:
        stream_title_parts.append(codec_info)

    stream_title = "\n".join(stream_title_parts)
    return (stream_name, stream_title)


def get_resolution_priority(stream_name: str) -> int:
    resolution_map = {
        "2160p": 2160, "4k": 2160, "uhd": 2160,
        "1080p": 1080, "fhd": 1080,
        "720p": 720, "hd": 720,
        "480p": 480, "sd": 480,
        "360p": 360,
    }
    for res_key, res_value in resolution_map.items():
        if res_key in stream_name.lower():
            return res_value
    return 1


def normalize_imdb_id(raw_id: str) -> str:
    if not raw_id:
        return ""

    value = raw_id.strip().lstrip("#")
    match = re.search(r"tt\d+", value)
    return match.group(0) if match else value


async def _get_or_create_default_token() -> str:
    tokens = await db.get_all_api_tokens()
    if tokens:
        token = tokens[0].get("token")
        if token:
            return token

    created = await db.add_api_token("default-addon", None, None)
    token = created.get("token")
    if not token:
        raise HTTPException(status_code=500, detail="Could not create default API token")
    return token

# --- Routes ---
@router.get("/manifest.json")
async def get_manifest_compat():
    """Compatibility route for old install URLs without token segment."""
    token = await _get_or_create_default_token()
    return RedirectResponse(url=f"/stremio/{token}/manifest.json", status_code=307)


@router.get("/{token}/manifest.json")
async def get_manifest(token: str, token_data: dict = Depends(verify_token)):
    if Telegram.HIDE_CATALOG:
        resources = ["stream"]
        catalogs = []
    else:
        resources = ["catalog", "meta", "stream"]
        catalogs = [
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Latest",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "movie",
                "id": "top_movies",
                "name": "Popular",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            },
            {
                "type": "series",
                "id": "latest_series",
                "name": "Latest",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "series",
                "id": "top_series",
                "name": "Popular",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            }
        ]

    return {
        "id": "telegram.media",
        "version": ADDON_VERSION,
        "name": ADDON_NAME,
        "logo": "https://i.postimg.cc/XqWnmDXr/Picsart-25-10-09-08-09-45-867.png",
        "description": "Streams movies and series from your Telegram.",
        "types": ["movie", "series"],
        "resources": resources,
        "catalogs": catalogs,
        "idPrefixes": ["tt"],
        "behaviorHints": {
            "configurable": False,
            "configurationRequired": False
        }
    }


@router.get("/open/{media_type}/{imdb_id}")
async def stremio_open(media_type: str, imdb_id: str, season: int = None, episode: int = None):
    imdb = normalize_imdb_id(imdb_id)
    if not imdb:
        raise HTTPException(status_code=400, detail="Invalid media id")

    if media_type in ("series", "tv"):
        if season and episode:
            detail_path = f"detail/series/{imdb}/{imdb}:{season}:{episode}"
        else:
            detail_path = f"detail/series/{imdb}"
    else:
        detail_path = f"detail/movie/{imdb}/{imdb}"

    stremio_url = f"stremio:///{detail_path}"
    web_url = f"https://web.stremio.com/#/{detail_path}"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset=\"UTF-8\">
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
        <title>Opening Stremio...</title>
        <style>
            body {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                color: white;
            }}
            .container {{
                text-align: center;
                padding: 40px;
                background: rgba(255,255,255,0.1);
                border-radius: 20px;
                backdrop-filter: blur(10px);
                max-width: 420px;
            }}
            h1 {{ margin-bottom: 20px; }}
            .spinner {{
                width: 50px;
                height: 50px;
                border: 4px solid rgba(255,255,255,0.3);
                border-top-color: white;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                margin: 20px auto;
            }}
            @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
            .btn {{
                margin-top: 12px;
                padding: 14px 28px;
                background: white;
                color: #667eea;
                border-radius: 10px;
                text-decoration: none;
                font-weight: bold;
                display: inline-block;
            }}
            .btn:hover {{ opacity: 0.9; }}
            .btn-secondary {{
                background: rgba(255,255,255,0.2);
                color: white;
                font-size: 0.9em;
            }}
            .links {{ margin-top: 25px; }}
            #status {{ font-size: 0.85em; opacity: 0.8; margin-top: 15px; }}
        </style>
    </head>
    <body>
        <div class=\"container\">
            <h1>🎬 Opening Stremio...</h1>
            <div class=\"spinner\"></div>
            <p id=\"status\">Launching Stremio app...</p>
            <div class=\"links\" style=\"display:none\" id=\"fallback\">
                <p>If Stremio didn't open automatically:</p>
                <a href=\"{stremio_url}\" class=\"btn\">Open in Stremio</a><br>
                <a href=\"{web_url}\" class=\"btn btn-secondary\">Open in Web Player</a>
            </div>
        </div>
        <script>
            var isAndroid = /android/i.test(navigator.userAgent);

            if (isAndroid) {{
                var intentUrl = \"intent://{detail_path}#Intent;scheme=stremio;package=com.stremio.one;end\";
                window.location.href = intentUrl;
            }} else {{
                window.location.href = \"{stremio_url}\";
            }}

            setTimeout(function() {{
                document.getElementById('fallback').style.display = 'block';
                document.getElementById('status').textContent = 'Taking too long?';
            }}, 3000);
        </script>
    </body>
    </html>
    """

    return HTMLResponse(content=html_content)


@router.get("/open/movie/{imdb_id}")
async def stremio_open_movie(imdb_id: str):
    return await stremio_open(media_type="movie", imdb_id=imdb_id)


@router.get("/open/series/{imdb_id}")
async def stremio_open_series(imdb_id: str, season: int = 1, episode: int = 1):
    return await stremio_open(media_type="series", imdb_id=imdb_id, season=season, episode=episode)


@router.get("/install")
async def stremio_install_helper(request: Request):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.hostname))
    manifest_url = f"{scheme}://{host}/stremio/manifest.json"
    stremio_url = f"stremio://{host}/stremio/manifest.json"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset=\"UTF-8\">
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
        <title>Installing Stremio Addon...</title>
        <style>
            body {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                color: white;
            }}
            .container {{
                text-align: center;
                padding: 40px;
                background: rgba(255,255,255,0.1);
                border-radius: 20px;
                backdrop-filter: blur(10px);
                max-width: 420px;
            }}
            h1 {{ margin-bottom: 20px; }}
            .spinner {{
                width: 50px;
                height: 50px;
                border: 4px solid rgba(255,255,255,0.3);
                border-top-color: white;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                margin: 20px auto;
            }}
            @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
            .btn {{
                margin-top: 12px;
                padding: 14px 28px;
                background: white;
                color: #667eea;
                border-radius: 10px;
                text-decoration: none;
                font-weight: bold;
                display: inline-block;
            }}
            .btn:hover {{ opacity: 0.9; }}
            .btn-secondary {{
                background: rgba(255,255,255,0.2);
                color: white;
                font-size: 0.9em;
            }}
            .links {{ margin-top: 25px; }}
            #status {{ font-size: 0.85em; opacity: 0.8; margin-top: 15px; }}
        </style>
    </head>
    <body>
        <div class=\"container\">
            <h1>🎬 Installing Addon...</h1>
            <div class=\"spinner\"></div>
            <p id=\"status\">Opening Stremio to install addon...</p>
            <div class=\"links\" style=\"display:none\" id=\"fallback\">
                <p>If Stremio didn't open automatically:</p>
                <a href=\"{stremio_url}\" class=\"btn\">Install in Stremio</a><br>
                <a href=\"https://web.stremio.com/#/addons\" class=\"btn btn-secondary\">Open Stremio Web</a>
            </div>
        </div>
        <script>
            var isAndroid = /android/i.test(navigator.userAgent);
            var manifestUrl = \"{manifest_url}\";

            if (isAndroid) {{
                var intentUrl = \"intent://\" + manifestUrl.replace(/^https?:\\/\\//, '') + \"#Intent;scheme=stremio;package=com.stremio.one;end\";
                window.location.href = intentUrl;
            }} else {{
                window.location.href = \"{stremio_url}\";
            }}

            setTimeout(function() {{
                document.getElementById('fallback').style.display = 'block';
                document.getElementById('status').textContent = 'Taking too long?';
            }}, 3000);
        </script>
    </body>
    </html>
    """

    return HTMLResponse(content=html_content)




@router.get("/{token}/catalog/{media_type}/{id}/{extra:path}.json")
@router.get("/{token}/catalog/{media_type}/{id}.json")
async def get_catalog(token: str, media_type: str, id: str, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    if Telegram.HIDE_CATALOG:
        raise HTTPException(status_code=404, detail="Catalog disabled")

    if media_type not in ["movie", "series"]:
        raise HTTPException(status_code=404, detail="Invalid catalog type")

    genre_filter = None
    search_query = None
    stremio_skip = 0

    if extra:
        params = extra.replace("&", "/").split("/")
        for param in params:
            if param.startswith("genre="):
                genre_filter = unquote(param.removeprefix("genre="))
            elif param.startswith("search="):
                search_query = unquote(param.removeprefix("search="))
            elif param.startswith("skip="):
                try:
                    stremio_skip = int(param.removeprefix("skip="))
                except ValueError:
                    stremio_skip = 0

    page = (stremio_skip // PAGE_SIZE) + 1

    try:
        if search_query:
            search_results = await db.search_documents(query=search_query, page=page, page_size=PAGE_SIZE)
            all_items = search_results.get("results", [])
            db_media_type = "tv" if media_type == "series" else "movie"
            items = [item for item in all_items if item.get("media_type") == db_media_type]
        else:
            if "latest" in id:
                sort_params = [("updated_on", "desc")]
            elif "top" in id:
                sort_params = [("rating", "desc")]
            else:
                sort_params = [("updated_on", "desc")]

            if media_type == "movie":
                data = await db.sort_movies(sort_params, page, PAGE_SIZE, genre_filter=genre_filter)
                items = data.get("movies", [])
            else:
                data = await db.sort_tv_shows(sort_params, page, PAGE_SIZE, genre_filter=genre_filter)
                items = data.get("tv_shows", [])
    except Exception as e:
        return {"metas": []}

    metas = [convert_to_stremio_meta(item) for item in items]
    return {"metas": metas}


@router.get("/{token}/meta/{media_type}/{id}.json")
async def get_meta(token: str, media_type: str, id: str, token_data: dict = Depends(verify_token)):
    if Telegram.HIDE_CATALOG:
        raise HTTPException(status_code=404, detail="Catalog disabled")
    try:
        imdb_id = id
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid Stremio ID format")

    media = await db.get_media_details(imdb_id=imdb_id)
    if not media:
        return {"meta": {}}

    meta_obj = {
        "id": id,
        "type": "series" if media.get("media_type") == "tv" else "movie",
        "name": media.get("title", ""),
        "description": media.get("description", ""),
        "year": str(media.get("release_year", "")),
        "imdbRating": str(media.get("rating", "")),
        "genres": media.get("genres", []),
        "poster": media.get("poster", ""),
        "logo": media.get("logo", ""),
        "background": media.get("backdrop", ""),
        "imdb_id": media.get("imdb_id", ""),
        "releaseInfo": str(media.get("release_year", "")),
        "moviedb_id": media.get("tmdb_id", ""),
        "cast": media.get("cast") or [],
        "runtime": media.get("runtime") or "",
    }

    if media.get("media_type") == "movie":
        released_date = format_released_date(media)
        if released_date:
            meta_obj["released"] = released_date

    # --- Add Episodes ---
    if media_type == "series" and "seasons" in media:

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        videos = []

        for season in sorted(media.get("seasons", []), key=lambda s: s.get("season_number")):
            for episode in sorted(season.get("episodes", []), key=lambda e: e.get("episode_number")):

                episode_id = f"{id}:{season['season_number']}:{episode['episode_number']}"

                videos.append({
                    "id": episode_id,
                    "title": episode.get("title", f"Episode {episode['episode_number']}"),
                    "season": season.get("season_number"),
                    "episode": episode.get("episode_number"),
                    "overview": episode.get("overview") or "No description available for this episode yet.",
                    "released": episode.get("released") or yesterday,
                    "thumbnail": episode.get("episode_backdrop") or "https://raw.githubusercontent.com/weebzone/Colab-Tools/refs/heads/main/no_episode_backdrop.png",
                    "imdb_id": episode.get("imdb_id") or media.get("imdb_id"),
                })

        meta_obj["videos"] = videos
    return {"meta": meta_obj}

@router.get("/{token}/stream/{media_type}/{id}.json")
async def get_streams(
    token: str,
    media_type: str,
    id: str,
    token_data: dict = Depends(verify_token)
):

    if token_data.get("limit_exceeded"):
        limit_type = token_data["limit_exceeded"]

        title = (
            "🚫 Daily Limit Reached – Upgrade Required"
            if limit_type == "daily"
            else "🚫 Monthly Limit Reached – Upgrade Required"
        )

        return {
            "streams": [
                {
                    "name": "Limit Reached",
                    "title": title,
                    "url": token_data["limit_video"]
                }
            ]
        }


    try:
        parts = id.split(":")
        imdb_id = parts[0]
        season_num = int(parts[1]) if len(parts) > 1 else None
        episode_num = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid Stremio ID format")

    media_details = await db.get_media_details(
        imdb_id=imdb_id,
        season_number=season_num,
        episode_number=episode_num
    )

    if not media_details or "telegram" not in media_details:
        return {"streams": []}

    streams = []
    for quality in media_details.get("telegram", []):
        if quality.get("id"):
            filename = quality.get("name", "")
            quality_str = quality.get("quality", "HD")
            size = quality.get("size", "")

            stream_name, stream_title = format_stream_details(
                filename, quality_str, size
            )

            streams.append({
                "name": stream_name,
                "title": stream_title,
                "url": f"{BASE_URL}/dl/{token}/{quality.get('id')}/video.mkv"
            })

            if Telegram.LEGACY_FALLBACK_STREAM:
                streams.append({
                    "name": f"{stream_name} (Compat)",
                    "title": f"{stream_title}\n🛟 Legacy fallback URL",
                    "url": f"{BASE_URL}/dl/{quality.get('id')}/video.mkv"
                })

    streams.sort(
        key=lambda s: get_resolution_priority(s.get("name", "")),
        reverse=True
    )
    return {"streams": streams}