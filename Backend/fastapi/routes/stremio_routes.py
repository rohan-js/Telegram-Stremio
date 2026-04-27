from fastapi import APIRouter, HTTPException, Depends, Request, Response
from typing import Optional
from urllib.parse import unquote, quote
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


LANG_CODE_MAP = {
    "malayalam": "mal",
    "hindi": "hin",
    "tamil": "tam",
    "telugu": "tel",
    "kannada": "kan",
    "english": "eng",
    "multi": "multi",
    "dual": "dual",
}


def _extract_language_codes(value) -> str:
    if not value:
        return ""

    parts = value if isinstance(value, list) else [value]
    found = []

    for part in parts:
        token_blob = str(part).lower()
        for sep in ["/", "&", "+", "|", "-", "_", "(", ")", "[", "]", ".", ","]:
            token_blob = token_blob.replace(sep, " ")
        for token in token_blob.split():
            cleaned = token.strip(".# ")
            code = LANG_CODE_MAP.get(cleaned)
            if code and code not in found:
                found.append(code)

    return "/".join(found[:3])


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

    resolution = parsed.get("resolution") or quality
    quality_type = parsed.get("quality", "")

    stream_name_parts = ["Telegram", str(resolution)]
    if quality_type:
        stream_name_parts.append(str(quality_type))

    language_codes = _extract_language_codes(parsed.get("language"))
    if not language_codes:
        language_codes = _extract_language_codes(filename)
    if language_codes:
        stream_name_parts.append(f"[{language_codes}]")

    stream_name = " ".join(part for part in stream_name_parts if part).strip()

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


def apply_stremio_no_cache(response: Response) -> None:
    # Encourage clients/proxies to fetch fresh addon data after new uploads.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, s-maxage=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"

# --- Routes ---
@router.get("/{token}/manifest.json")
async def get_manifest(token: str, response: Response, token_data: dict = Depends(verify_token)):
    apply_stremio_no_cache(response)
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

    # Build dynamic name/description/version with subscription info
    addon_name = ADDON_NAME
    addon_desc = "Streams movies and series from your Telegram."
    addon_version = ADDON_VERSION
    expiry_obj = None

    if Telegram.SUBSCRIPTION:
        user_id = token_data.get("user_id")
        if user_id:
            from Backend import db as _db
            try:
                user = await _db.get_user(int(user_id))
                if user and user.get("subscription_status") == "active":
                    expiry_obj = user.get("subscription_expiry")
                    if expiry_obj:
                        expiry_str = expiry_obj.strftime("%d %b %Y").lstrip("0")
                        addon_name = f"{ADDON_NAME} — Expires {expiry_str}"
                        addon_desc = (
                            f"📅 Subscription active until {expiry_str}.\n"
                            f"Streams movies and series from your Telegram."
                        )
                        # Encode expiry epoch (low 16 bits, hex) into version so
                        # Stremio detects a change when subscription is updated.
                        epoch_tag = format(int(expiry_obj.timestamp()) & 0xFFFF, "x")
                        addon_version = f"{ADDON_VERSION}-{epoch_tag}"
                    else:
                        addon_name = f"{ADDON_NAME} — Active"
                        addon_desc = "✅ Subscription active.\nStreams movies and series from your Telegram."
            except Exception:
                pass  # Fallback to defaults on error

    # Configure URL — opening this reinstalls the addon with latest manifest
    configure_url = f"{Telegram.BASE_URL}/stremio/{token}/configure"

    return {
        "id": f"telegram.media.{token[:8]}",   # per-user ID so each token is independent
        "version": addon_version,
        "name": addon_name,
        "logo": "https://i.postimg.cc/XqWnmDXr/Picsart-25-10-09-08-09-45-867.png",
        "description": addon_desc,
        "types": ["movie", "series"],
        "resources": resources,
        "catalogs": catalogs,
        "idPrefixes": ["tt"],
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": False
        },
        "config": [
            {
                "key": "manifest_url",
                "title": "Your Addon URL (copy to reinstall)",
                "type": "text",
                "default": f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
            }
        ]
    }


@router.head("/{token}/manifest.json")
async def get_manifest_head(token: str, token_data: dict = Depends(verify_token)):
    response = Response(status_code=200)
    apply_stremio_no_cache(response)
    return response


@router.get("/{token}/configure")
async def configure_addon(token: str):
    """
    Configure/update page for the Stremio addon.
    Uses the correct stremio://addon_install?manifest= deep-link so Stremio
    actually shows the Install/Update dialog when the button is clicked.
    """
    from urllib.parse import quote
    from fastapi.responses import HTMLResponse
    from Backend import db as _db

    manifest_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
    # Universal Stremio web install — works on desktop and mobile
    install_page_url = f"{Telegram.BASE_URL}/stremio/{token}/install"

    # Fetch user info for display
    token_doc = await _db.get_api_token(token)
    user_name = "Unknown"
    expiry_str = "N/A"
    status_color = "#ef4444"
    status_text = "Unknown"

    if token_doc:
        uid = token_doc.get("user_id")
        if uid:
            try:
                user = await _db.get_user(int(uid))
                if user:
                    user_name = user.get("first_name") or user.get("username") or f"User {uid}"
                    sub_status = user.get("subscription_status", "")
                    expiry = user.get("subscription_expiry")
                    if expiry:
                        expiry_str = expiry.strftime("%d %b %Y").lstrip("0")
                    if sub_status == "active":
                        status_color = "#22c55e"
                        status_text = "✅ Active"
                    else:
                        status_color = "#ef4444"
                        status_text = "🔴 Expired"
            except Exception:
                pass

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Update Telegram Stremio Addon</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f0f1a; color: #e2e8f0;
      min-height: 100vh; display: flex; align-items: center; justify-content: center;
      padding: 24px;
    }}
    .card {{
      background: #1e1e2e; border: 1px solid #2d2d44; border-radius: 16px;
      padding: 40px 32px; max-width: 480px; width: 100%; text-align: center;
    }}
    .logo {{ font-size: 48px; margin-bottom: 12px; }}
    h1 {{ font-size: 1.5rem; font-weight: 700; color: #f8fafc; margin-bottom: 6px; }}
    .sub-title {{ color: #94a3b8; font-size: 0.9rem; margin-bottom: 28px; }}
    .info-row {{
      display: flex; justify-content: space-between; align-items: center;
      background: #2a2a3e; border-radius: 10px; padding: 12px 16px;
      margin-bottom: 12px; font-size: 0.9rem;
    }}
    .info-label {{ color: #94a3b8; }}
    .info-val {{ font-weight: 600; color: #f1f5f9; }}
    .status-badge {{
      display: inline-block; padding: 2px 10px; border-radius: 999px;
      font-size: 0.8rem; font-weight: 700;
      background: {status_color}22; color: {status_color};
    }}
    .btn-update {{
      display: block; width: 100%;
      background: linear-gradient(135deg, #7c3aed, #4f46e5);
      color: white; font-weight: 700; font-size: 1rem;
      padding: 14px 24px; border-radius: 12px; border: none;
      cursor: pointer; text-decoration: none; margin: 28px 0 12px;
      transition: opacity 0.2s;
    }}
    .btn-update:hover {{ opacity: 0.85; }}
    .btn-web {{
      display: block; color: #6366f1; font-size: 0.85rem;
      text-decoration: underline; margin-bottom: 20px;
    }}
    .steps {{
      background: #2a2a3e; border-radius: 10px; padding: 14px 18px;
      margin: 16px 0; text-align: left; font-size: 0.85rem; color: #cbd5e1;
    }}
    .steps b {{ color: #f1f5f9; }}
    .steps ol {{ margin-top: 8px; margin-left: 18px; line-height: 1.8; }}
    .url-box {{
      background: #111827; border: 1px solid #374151; border-radius: 8px;
      padding: 10px 14px; font-family: monospace; font-size: 0.75rem;
      color: #94a3b8; word-break: break-all; text-align: left; margin-top: 16px;
    }}
    .btn-copy {{
      margin-top: 10px; width: 100%; padding: 10px;
      background: #1e293b; border: 1px solid #374151; color: #94a3b8;
      border-radius: 8px; cursor: pointer; font-size: 0.85rem; transition: all 0.2s;
    }}
    .btn-copy:hover {{ background: #334155; color: #f1f5f9; }}
    .hint {{ color: #64748b; font-size: 0.78rem; margin-top: 6px; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">🎬</div>
    <h1>Telegram Stremio Addon</h1>
    <p class="sub-title">Click the button below to install or update your addon in Stremio.</p>

    <div class="info-row">
      <span class="info-label">User</span>
      <span class="info-val">{user_name}</span>
    </div>
    <div class="info-row">
      <span class="info-label">Status</span>
      <span class="status-badge">{status_text}</span>
    </div>
    <div class="info-row">
      <span class="info-label">Expires</span>
      <span class="info-val">{expiry_str}</span>
    </div>

    <a href="{install_page_url}" class="btn-update" target="_blank">
      ⚡ Install / Update in Stremio
    </a>

    <div class="steps">
      <b>Or install manually:</b>
      <ol>
        <li>Open Stremio → <b>Add-ons</b> tab</li>
        <li>Click the <b>🔍 Search / URL</b> icon</li>
        <li>Paste the URL below and press Enter</li>
      </ol>
    </div>

    <div class="url-box" id="murl">{manifest_url}</div>
    <button onclick="copyUrl()" class="btn-copy">📋 Copy URL</button>
    <script>
      function copyUrl() {{
        navigator.clipboard.writeText('{manifest_url}').then(() => {{
          const b = document.querySelector('.btn-copy');
          b.textContent = '✅ Copied!';
          setTimeout(() => b.textContent = '📋 Copy URL', 2000);
        }});
      }}
    </script>
  </div>
</body>
</html>"""
    return HTMLResponse(html)




@router.api_route("/{token}/catalog/{media_type}/{id}/{extra:path}.json", methods=["GET", "HEAD"])
@router.api_route("/{token}/catalog/{media_type}/{id}.json", methods=["GET", "HEAD"])
async def get_catalog(token: str, media_type: str, id: str, response: Response, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    apply_stremio_no_cache(response)
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
        return {
            "metas": [],
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
        }

    metas = [convert_to_stremio_meta(item) for item in items]
    return {
        "metas": metas,
        "cacheMaxAge": 0,
        "staleRevalidate": 0,
        "staleError": 0,
    }


@router.api_route("/{token}/meta/{media_type}/{id}.json", methods=["GET", "HEAD"])
async def get_meta(token: str, media_type: str, id: str, response: Response, token_data: dict = Depends(verify_token)):
    apply_stremio_no_cache(response)
    if Telegram.HIDE_CATALOG:
        raise HTTPException(status_code=404, detail="Catalog disabled")
    try:
        imdb_id = id
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid Stremio ID format")

    media = await db.get_media_details(imdb_id=imdb_id)
    if not media:
        return {
            "meta": {},
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
        }

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
    return {
        "meta": meta_obj,
        "cacheMaxAge": 0,
        "staleRevalidate": 0,
        "staleError": 0,
    }

@router.api_route("/{token}/stream/{media_type}/{id}.json", methods=["GET", "HEAD"])
async def get_streams(
    token: str,
    media_type: str,
    id: str,
    response: Response,
    token_data: dict = Depends(verify_token)
):
    apply_stremio_no_cache(response)

    if token_data.get("subscription_expired"):
        from Backend.config import Telegram as _TG
        return {
            "streams": [
                {
                    "name": "🚫 Subscription Expired",
                    "title": "Your subscription has expired.\nRenew via the bot to continue watching.",
                    "url": _TG.SUBSCRIPTION_URL
                }
            ],
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
        }

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
            ],
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
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
        return {
            "streams": [],
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
        }

    streams = []
    for quality in media_details.get("telegram", []):
        if quality.get("id"):
            filename = quality.get("name", "")
            quality_str = quality.get("quality", "HD")
            size = quality.get("size", "")

            stream_name, stream_title = format_stream_details(
                filename, quality_str, size
            )

            original_url = f"{BASE_URL}/dl/{token}/{quality.get('id')}/video.mkv"
            proxy_url = f"{Telegram.HTTP_PROXY_URL}{original_url}" if Telegram.PROXY and Telegram.HTTP_PROXY_URL else None

            if Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH and proxy_url:
                streams.append({
                    "name": f"{stream_name} (Proxy)",
                    "title": stream_title,
                    "url": proxy_url
                })
                streams.append({
                    "name": f"{stream_name} (Direct)",
                    "title": stream_title,
                    "url": original_url
                })
            elif proxy_url:
                streams.append({
                    "name": stream_name,
                    "title": stream_title,
                    "url": proxy_url
                })
            else:
                streams.append({
                    "name": stream_name,
                    "title": stream_title,
                    "url": original_url
                })

    streams.sort(
        key=lambda s: get_resolution_priority(s.get("name", "")),
        reverse=True
    )

    # Deduplicate stream names — Stremio collapses streams with identical names,
    # so when two files share the same caption we append (1), (2) ... to each duplicate.
    name_count: dict = {}
    for s in streams:
        name_count[s["name"]] = name_count.get(s["name"], 0) + 1

    seen: dict = {}
    for s in streams:
        if name_count[s["name"]] > 1:
            seen[s["name"]] = seen.get(s["name"], 0) + 1
            s["name"] = f"{s['name']} ({seen[s['name']]})"

    return {
        "streams": streams,
        "cacheMaxAge": 0,
        "staleRevalidate": 0,
        "staleError": 0,
    }

@router.head("/{token}/install")
async def stremio_install_head(token: str, token_data: dict = Depends(verify_token)):
    from fastapi.responses import Response
    return Response(status_code=200)


@router.get("/{token}/install")
async def stremio_install(request: Request, token: str, token_data: dict = Depends(verify_token)):
    from fastapi.responses import HTMLResponse

    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc))
    manifest_url = f"{scheme}://{host}/stremio/{token}/manifest.json"
    stremio_url = f"stremio://{host}/stremio/{token}/manifest.json"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
        <div class="container">
            <h1>🎬 Installing Addon...</h1>
            <div class="spinner"></div>
            <p id="status">Opening Stremio to install addon...</p>
            <div class="links" style="display:none" id="fallback">
                <p>If Stremio didn't open automatically:</p>
                <a href="{stremio_url}" class="btn">Open in Stremio App</a><br>
                <a href="{manifest_url}" class="btn btn-secondary">Open Manifest URL</a>
            </div>
        </div>
        <script>
            var isAndroid = /android/i.test(navigator.userAgent);
            var manifestUrl = {manifest_url!r};
            var switchedApp = false;

            document.addEventListener('visibilitychange', function() {{
                if (document.hidden) switchedApp = true;
            }});

            function showManualInstallHint() {{
                if (!switchedApp) {{
                    document.getElementById('status').textContent = 'If app did not open, use the button below.';
                }}
            }}

            if (isAndroid) {{
                var intentUrl = "intent://" + manifestUrl.replace(/^https?:\/\//, '') + "#Intent;scheme=stremio;package=com.stremio.one;end";
                window.location.href = intentUrl;
                setTimeout(showManualInstallHint, 1600);
            }} else {{
                window.location.href = {stremio_url!r};
                setTimeout(showManualInstallHint, 1800);
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


@router.get("/open/{media_type}/{id}")
async def stremio_open(request: Request, media_type: str, id: str, season: int = None, episode: int = None):
    from fastapi.responses import HTMLResponse

    if media_type in ("series", "tv"):
        if season and episode:
            detail_path = f"detail/series/{id}/{id}:{season}:{episode}"
        else:
            detail_path = f"detail/series/{id}"
    else:
        detail_path = f"detail/movie/{id}/{id}"

    stremio_url = f"stremio:///{detail_path}"
    web_url = f"https://web.stremio.com/#/{detail_path}"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
        <div class="container">
            <h1>🎬 Opening Stremio...</h1>
            <div class="spinner"></div>
            <p id="status">Launching Stremio app...</p>
            <div class="links" style="display:none" id="fallback">
                <p>If Stremio didn't open automatically:</p>
                <a href="{stremio_url}" class="btn">Open in Stremio</a><br>
                <a href="{web_url}" class="btn btn-secondary">Open in Web Player</a>
            </div>
        </div>
        <script>
            var isAndroid = /android/i.test(navigator.userAgent);
            if (isAndroid) {{
                window.location.href = "intent://{detail_path}#Intent;scheme=stremio;package=com.stremio.one;end";
            }} else {{
                window.location.href = {stremio_url!r};
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
