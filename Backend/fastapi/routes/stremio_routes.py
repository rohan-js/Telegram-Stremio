import asyncio
import re
from fastapi import APIRouter, HTTPException, Depends, Request, Response
from fastapi.templating import Jinja2Templates
from typing import Optional
from urllib.parse import unquote, quote
from Backend.config import Telegram
from Backend import db, __version__
from Backend.fastapi.themes import get_theme, get_all_themes, DEFAULT_THEME
import PTN
from datetime import datetime, timezone, timedelta
from Backend.fastapi.security.tokens import verify_token
from Backend.helper.encrypt import encode_string
from Backend.helper.analytics import client_ip_from, record_client
from Backend.helper.global_search import global_search, is_global_search_enabled
from Backend.logger import LOGGER
from Backend.helper.torrent_downloads import (
    download_root_dir,
    safe_download_file_path,
    select_completed_torrent_file,
)
from Backend.helper.subtitles import get_subtitles_for, stremio_subtitle_entries
from Backend.helper.fanart import fanart_artwork
from Backend.helper.imdb import get_detail, get_season
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.iptv import (
    IPTV_ALL_CATALOG_ID,
    IPTV_CATALOG_ID,
    IPTV_CATEGORY_CATALOG_PREFIX,
    IPTV_ID_PREFIX,
    build_iptv_streams,
    get_iptv_channel,
    get_iptv_settings,
    iptv_catalog_id_for_category,
    iptv_catalog_name,
    iptv_category_from_catalog_id,
    iptv_meta,
    list_iptv_channels,
)


# --- Configuration ---
BASE_URL = Telegram.BASE_URL
ADDON_NAME = "Telegram"
ADDON_VERSION = __version__
PAGE_SIZE = 15

router = APIRouter(prefix="/stremio", tags=["Stremio Addon"])
templates = Jinja2Templates(directory="Backend/fastapi/templates")

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
def _abs_media_url(value) -> str:
    if not value:
        return ""
    value = str(value)
    if value.startswith(("http://", "https://")):
        return value
    base = str(SettingsManager.current().base_url or "").rstrip("/")
    return f"{base}/{value.lstrip('/')}" if base else value


BETTERPOSTER_DEFAULT = "https://btttr.cc/poster/imdb/poster-default/{imdb_id}.jpg"
RPDB_FREE = "https://api.ratingposterdb.com/t0-free-rpdb/imdb/poster-default/{imdb_id}.jpg"


def _poster_url(imdb_id: str, fallback: str) -> str:
    settings = SettingsManager.current()
    if imdb_id:
        if settings.better_poster_enabled:
            template = settings.better_poster or BETTERPOSTER_DEFAULT
            return template.replace("{imdb_id}", str(imdb_id))
        if settings.rpdb_enabled:
            key = settings.rpdb_api_key
            template = (
                f"https://api.ratingposterdb.com/{key}/imdb/poster-default/{{imdb_id}}.jpg"
                if key else RPDB_FREE
            )
            return template.replace("{imdb_id}", str(imdb_id))
    return _abs_media_url(fallback)


async def _apply_fanart(meta: dict, item: dict) -> None:
    if not SettingsManager.current().fanart_enabled:
        return
    try:
        art = await fanart_artwork(item.get("imdb_id"), item.get("tmdb_id"), item.get("media_type"))
    except Exception as e:
        LOGGER.warning(f"[FANART] artwork lookup failed for {item.get('imdb_id')}: {e}")
        return
    if art.get("poster"):
        meta["poster"] = art["poster"]
    if art.get("logo"):
        meta["logo"] = art["logo"]
    if art.get("background"):
        meta["background"] = art["background"]


def convert_to_stremio_meta(item: dict) -> dict:
    media_type = "series" if item.get("media_type") == "tv" else "movie"
    
    meta = {
        "id": item.get('imdb_id'),
        "type": media_type,
        "name": item.get("title"),
        "poster": _poster_url(item.get("imdb_id"), item.get("poster")),
        "logo": item.get("logo") or "",
        "year": item.get("release_year"),
        "releaseInfo": str(item.get("release_year", "")),
        "imdb_id": item.get("imdb_id", ""),
        "moviedb_id": item.get("tmdb_id", ""),
        "background": _abs_media_url(item.get("backdrop")),
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


def _format_torrent_stats_line(torrent_stats: Optional[dict]) -> str:
    if not torrent_stats or torrent_stats.get("status") != "ok":
        return ""
    seeders = torrent_stats.get("seeders")
    peers = torrent_stats.get("peers")
    if seeders is None and peers is None:
        return ""
    try:
        seeders_text = str(max(0, int(seeders or 0)))
    except (TypeError, ValueError):
        seeders_text = "0"
    try:
        peers_text = str(max(0, int(peers or 0)))
    except (TypeError, ValueError):
        peers_text = "0"
    return f"Seeds: {seeders_text} | Peers: {peers_text}"


def build_torrent_stream(
    quality: dict,
    stream_name: str,
    stream_title: str,
    torrent_stats: Optional[dict] = None,
) -> Optional[dict]:
    info_hash = quality.get("info_hash")
    if not info_hash:
        return None

    torrent_name = stream_name.replace("Telegram", "Torrent", 1)
    title_parts = [stream_title]
    stats_line = _format_torrent_stats_line(torrent_stats)
    if stats_line:
        title_parts.append(stats_line)
    title_parts.extend(["🧲 Torrent stream", "Speed depends on seeders/peers."])
    stream = {
        "name": torrent_name,
        "title": "\n".join(title_parts),
        "infoHash": str(info_hash).lower(),
    }

    file_idx = quality.get("file_idx")
    if file_idx is not None:
        try:
            stream["fileIdx"] = int(file_idx)
        except (TypeError, ValueError):
            pass

    sources = quality.get("sources") or []
    if sources:
        stream["sources"] = [str(source) for source in sources if source]

    behavior_hints = {}
    filename = quality.get("filename") or quality.get("name")
    if filename:
        behavior_hints["filename"] = filename
    video_size = quality.get("video_size")
    if video_size:
        try:
            behavior_hints["videoSize"] = int(video_size)
        except (TypeError, ValueError):
            pass
    if behavior_hints:
        stream["behaviorHints"] = behavior_hints

    return stream


async def build_downloaded_torrent_stream(
    token: str,
    quality: dict,
    stream_name: str,
    download_job: Optional[dict],
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> Optional[dict]:
    if not download_job or download_job.get("status") != "completed":
        return None

    selected = select_completed_torrent_file(
        download_job.get("files") or [],
        quality,
        season_number=season_number,
        episode_number=episode_number,
    )
    if not selected:
        return None

    rel_path = selected.get("rel_path")
    if not rel_path:
        return None

    payload = {
        "source_type": "downloaded_torrent",
        "info_hash": str(quality.get("info_hash") or "").lower(),
        "rel_path": rel_path,
        "name": selected.get("name") or selected.get("rel_path") or quality.get("filename") or "video.mkv",
        "size": int(selected.get("size") or 0),
    }
    encoded = await encode_string(payload)
    filename = selected.get("name") or selected.get("rel_path") or quality.get("filename") or "video.mkv"
    size_text = selected.get("size_text") or quality.get("size") or ""

    downloaded_name = stream_name.replace("Telegram", "Downloaded", 1)
    if downloaded_name == stream_name:
        downloaded_name = f"Downloaded {stream_name}".strip()

    return {
        "name": downloaded_name,
        "title": "\n".join(
            part for part in [
                f"📁 {filename}",
                f"💾 {size_text}" if size_text else "",
                "✅ Downloaded to VPS",
            ]
            if part
        ),
        "url": f"{BASE_URL}/downloaded/{token}/{encoded}/video.mkv",
    }


async def build_local_vps_stream(token: str, quality: dict, stream_name: str) -> Optional[dict]:
    rel_path = str(quality.get("local_rel_path") or "").replace("\\", "/").lstrip("/")
    if not rel_path:
        return None
    try:
        file_path = safe_download_file_path(download_root_dir(), rel_path)
    except ValueError:
        LOGGER.warning("Ignoring local VPS quality with invalid path")
        return None
    if not file_path.is_file():
        LOGGER.warning(f"Ignoring missing local VPS file: {rel_path}")
        return None

    file_size = file_path.stat().st_size
    filename = str(quality.get("filename") or file_path.name)
    payload = {
        "source_type": "local_vps",
        "rel_path": rel_path,
        "name": filename,
        "size": file_size,
    }
    encoded = await encode_string(payload)
    local_name = stream_name.replace("Telegram", "VPS Local", 1)
    if local_name == stream_name:
        local_name = f"VPS Local {stream_name}".strip()
    size_text = quality.get("size") or f"{file_size / (1024 ** 3):.2f} GB"
    return {
        "name": local_name,
        "title": f"📁 {filename}\n💾 {size_text}\n✅ Stored on VPS",
        "url": f"{BASE_URL}/downloaded/{token}/{encoded}/video.mkv",
        "behaviorHints": {
            "filename": filename,
            "videoSize": file_size,
        },
    }


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


def parse_size_to_bytes(size_str: str) -> int:
    if not size_str:
        return 0
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", size_str.strip())
    if not match:
        return 0
    value, unit = float(match.group(1)), match.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(value * multipliers.get(unit, 1))


#----- Canonical quality label used by per-token quality filtering
def stream_res_label(stream_name: str) -> str:
    return {2160: "4K", 1080: "1080p", 720: "720p", 480: "480p", 360: "360p"}.get(
        get_resolution_priority(stream_name), "other"
    )


def apply_stremio_no_cache(response: Response) -> None:
    # Encourage clients/proxies to fetch fresh addon data after new uploads.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, s-maxage=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"


def _token_can_view(mode: str | None, allowed_tokens: list | None, token_data: dict) -> bool:
    mode = mode or "public"
    if mode == "public":
        return True
    if mode == "tokens":
        return (token_data or {}).get("token") in (allowed_tokens or [])
    return False


def _media_visible_to_token(
    media: dict | None,
    token_data: dict,
    *,
    allow_searchable_exclusive: bool = False,
    catalog_id: str | None = None,
) -> bool:
    if not media:
        return False
    exclusive_catalog_id = str(media.get("exclusive_catalog_id") or "")
    inside_exclusive_catalog = bool(catalog_id and exclusive_catalog_id == str(catalog_id))
    if exclusive_catalog_id and not inside_exclusive_catalog and not (
        allow_searchable_exclusive and media.get("exclusive_searchable")
    ):
        return False
    return _token_can_view(media.get("visibility") or "public", media.get("allowed_tokens") or [], token_data)


def _filter_visible_media(items: list[dict], token_data: dict, *, allow_searchable_exclusive: bool = False) -> list[dict]:
    return [item for item in items if _media_visible_to_token(item, token_data, allow_searchable_exclusive=allow_searchable_exclusive)]

# --- Routes ---
@router.get("/{token}/manifest.json")
async def get_manifest(request: Request, token: str, response: Response, token_data: dict = Depends(verify_token)):
    apply_stremio_no_cache(response)
    asyncio.create_task(record_client(
        token,
        token_data.get("name") or "Unknown",
        client_ip_from(request),
        request.headers.get("user-agent"),
    ))
    if Telegram.HIDE_CATALOG:
        resources = ["stream", "subtitles"]
        catalogs = []
    else:
        resources = ["catalog", "meta", "stream", "subtitles"]
        catalogs = [
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Latest Movies",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "series",
                "id": "latest_series",
                "name": "Latest Series",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "movie",
                "id": "top_movies",
                "name": "Popular Movies",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            },
            {
                "type": "series",
                "id": "top_series",
                "name": "Popular Series",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            }
        ]

        try:
            custom_catalogs = await db.get_custom_catalogs(visible_only=True)
            for catalog in custom_catalogs:
                if not _token_can_view(catalog.get("visibility") or "public", catalog.get("allowed_tokens") or [], token_data):
                    continue
                catalog_id = str(catalog.get("_id"))
                catalog_name = catalog.get("name") or "Custom Catalog"
                catalogs.append({
                    "type": "movie",
                    "id": f"custom_{catalog_id}",
                    "name": catalog_name,
                    "extra": [{"name": "skip"}],
                    "extraSupported": ["skip"],
                })
                catalogs.append({
                    "type": "series",
                    "id": f"custom_{catalog_id}",
                    "name": catalog_name,
                    "extra": [{"name": "skip"}],
                    "extraSupported": ["skip"],
                })
        except Exception:
            pass

        try:
            iptv_settings = await get_iptv_settings(db)
            if iptv_settings.get("enabled"):
                iptv_category_ids = await db.dbs["tracking"]["iptv_channels"].distinct(
                    "category_ids",
                    {"hidden": {"$ne": True}},
                )
                catalogs.append({
                    "type": "tv",
                    "id": IPTV_ALL_CATALOG_ID,
                    "name": "Live TV - All",
                    "extra": [
                        {"name": "skip"},
                        {"name": "search", "isRequired": False},
                    ],
                    "extraSupported": ["skip", "search"],
                })
                for category_id in sorted(str(item).lower() for item in iptv_category_ids if item and str(item).lower() != "xxx"):
                    catalogs.append({
                        "type": "tv",
                        "id": iptv_catalog_id_for_category(category_id),
                        "name": iptv_catalog_name(category_id),
                        "extra": [
                            {"name": "skip"},
                            {"name": "search", "isRequired": False},
                        ],
                        "extraSupported": ["skip", "search"],
                    })
        except Exception:
            pass

    #----- Per-token addon config: hide catalogs and reorder the manifest list
    _token_config = token_data.get("config") or {}
    _hidden_catalogs = set(_token_config.get("hidden_catalogs") or [])
    _catalog_order = _token_config.get("catalog_order") or []
    if _hidden_catalogs:
        catalogs = [c for c in catalogs if c["id"] not in _hidden_catalogs]
    if _catalog_order:
        rank = {cid: i for i, cid in enumerate(_catalog_order)}
        catalogs.sort(key=lambda c: rank.get(c["id"], len(_catalog_order) + 1))

    # Build dynamic name/description/version with subscription info
    addon_name = ADDON_NAME
    addon_desc = "Streams movies, series, torrents, and live TV."
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
                            f"Streams movies, series, torrents, and live TV."
                        )
                        # Encode expiry epoch (low 16 bits, hex) into version so
                        # Stremio detects a change when subscription is updated.
                        epoch_tag = format(int(expiry_obj.timestamp()) & 0xFFFF, "x")
                        addon_version = f"{ADDON_VERSION}-{epoch_tag}"
                    else:
                        addon_name = f"{ADDON_NAME} — Active"
                        addon_desc = "✅ Subscription active.\nStreams movies, series, torrents, and live TV."
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
        "types": ["movie", "series", "tv"],
        "resources": resources,
        "catalogs": catalogs,
        "idPrefixes": ["tt", IPTV_ID_PREFIX],
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
async def configure_addon(request: Request, token: str):
    """
    Configure/update page for the Stremio addon.
    Uses the correct stremio://addon_install?manifest= deep-link so Stremio
    actually shows the Install/Update dialog when the button is clicked.
    """
    from Backend import db as _db

    manifest_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
    # Universal Stremio web install — works on desktop and mobile
    install_page_url = f"{Telegram.BASE_URL}/stremio/{token}/install"

    # Fetch user info for display
    token_doc = await _db.get_api_token(token)
    user_name = "Unknown"
    expiry_str = "N/A"
    status_kind = "danger"
    status_text = "Unknown"

    if token_doc:
        status_text = "Active"
        status_kind = "success"
        if token_doc.get("revoked"):
            status_text = "Revoked"
            status_kind = "danger"
        elif token_doc.get("subscription_exempt") or token_doc.get("is_admin"):
            status_text = "Lifetime"
            status_kind = "success"
            expiry_str = "Lifetime"
        elif token_doc.get("expires_at"):
            token_expiry = token_doc.get("expires_at")
            if isinstance(token_expiry, str):
                try:
                    token_expiry = datetime.fromisoformat(token_expiry.replace("Z", "+00:00"))
                except ValueError:
                    token_expiry = None
            if token_expiry:
                expiry_str = token_expiry.strftime("%d %b %Y").lstrip("0")
                compare_now = datetime.now(timezone.utc) if token_expiry.tzinfo else datetime.utcnow()
                if token_expiry <= compare_now:
                    status_text = "Expired"
                    status_kind = "danger"
        uid = token_doc.get("user_id")
        if uid:
            try:
                user = await _db.get_user(int(uid))
                if user:
                    user_name = user.get("first_name") or user.get("username") or f"User {uid}"
                    sub_status = user.get("subscription_status", "")
                    expiry = user.get("subscription_expiry")
                    if expiry:
                        if expiry_str == "N/A":
                            expiry_str = expiry.strftime("%d %b %Y").lstrip("0")
                    if not token_doc.get("subscription_exempt") and not token_doc.get("is_admin") and not token_doc.get("expires_at"):
                        if sub_status == "active":
                            status_text = "Active"
                            status_kind = "success"
                        elif Telegram.SUBSCRIPTION:
                            status_text = "Expired"
                            status_kind = "danger"
            except Exception:
                pass
    return templates.TemplateResponse(request=request, name="stremio_configure.html", context={
        "manifest_url": manifest_url,
        "install_page_url": install_page_url,
        "user_name": user_name,
        "expiry_str": expiry_str,
        "status_kind": status_kind,
        "status_text": status_text,
        "token": token,
        "theme": get_theme(request.session.get("theme", DEFAULT_THEME)),
        "themes": get_all_themes(),
        "current_theme": request.session.get("theme", DEFAULT_THEME),
    })


#----- Per-token addon configuration (quality filter/sort, hidden catalogs, catalog order)
@router.get("/{token}/addon-config")
async def get_addon_config(token: str, token_data: dict = Depends(verify_token)):
    config = await db.get_token_config(token)
    catalogs = [
        {"id": "latest_movies", "name": "Latest Movies", "type": "movie"},
        {"id": "latest_series", "name": "Latest Series", "type": "series"},
        {"id": "top_movies", "name": "Popular Movies", "type": "movie"},
        {"id": "top_series", "name": "Popular Series", "type": "series"},
    ]
    try:
        for catalog in await db.get_custom_catalogs(visible_only=True):
            if not _token_can_view(catalog.get("visibility") or "public", catalog.get("allowed_tokens") or [], token_data):
                continue
            cid = str(catalog.get("_id"))
            cname = catalog.get("name") or "Custom Catalog"
            catalogs.append({"id": f"custom_{cid}", "name": f"{cname} (Movies)", "type": "movie"})
            catalogs.append({"id": f"custom_{cid}", "name": f"{cname} (Series)", "type": "series"})
    except Exception:
        pass
    try:
        iptv_settings = await get_iptv_settings(db)
        if iptv_settings.get("enabled"):
            catalogs.append({"id": IPTV_ALL_CATALOG_ID, "name": "Live TV - All", "type": "tv"})
    except Exception:
        pass
    return {
        "config": config,
        "catalogs": catalogs,
        "quality_options": ["4K", "1080p", "720p", "480p", "360p", "other"],
    }


@router.post("/{token}/addon-config")
async def save_addon_config(token: str, payload: dict, token_data: dict = Depends(verify_token)):
    allowed_keys = {"quality_filter", "quality_sort", "hidden_catalogs", "catalog_order"}
    config = {k: payload.get(k) for k in allowed_keys if k in payload}
    if "quality_filter" in config:
        qf = config["quality_filter"]
        config["quality_filter"] = [str(x) for x in (qf if isinstance(qf, list) else [qf])]
    if "quality_sort" in config and config["quality_sort"] not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="quality_sort must be 'asc' or 'desc'")
    for key in ("hidden_catalogs", "catalog_order"):
        if key in config:
            val = config[key]
            config[key] = [str(x) for x in (val if isinstance(val, list) else [val])]
    await db.set_token_config(token, config)
    return {"ok": True, "config": config}




@router.api_route("/{token}/catalog/{media_type}/{id}/{extra:path}.json", methods=["GET", "HEAD"])
@router.api_route("/{token}/catalog/{media_type}/{id}.json", methods=["GET", "HEAD"])
async def get_catalog(token: str, media_type: str, id: str, response: Response, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    apply_stremio_no_cache(response)
    if Telegram.HIDE_CATALOG:
        raise HTTPException(status_code=404, detail="Catalog disabled")

    if media_type not in ["movie", "series", "tv"]:
        raise HTTPException(status_code=404, detail="Invalid catalog type")

    #----- Per-token config: hidden catalogs are served as empty
    _token_config = token_data.get("config") or {}
    if id in set(_token_config.get("hidden_catalogs") or []):
        return {
            "metas": [],
            "cacheMaxAge": 0,
            "staleRevalidate": 0,
            "staleError": 0,
        }

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

    if media_type == "tv":
        if id in {IPTV_CATALOG_ID, IPTV_ALL_CATALOG_ID}:
            iptv_category = ""
        elif id.startswith(IPTV_CATEGORY_CATALOG_PREFIX):
            iptv_category = iptv_category_from_catalog_id(id)
        else:
            raise HTTPException(status_code=404, detail="Unknown live TV catalog")
        settings = await get_iptv_settings(db)
        if not settings.get("enabled"):
            return {
                "metas": [],
                "cacheMaxAge": 300,
                "staleRevalidate": 300,
                "staleError": 3600,
            }
        data = await list_iptv_channels(
            db,
            search=search_query or "",
            category=iptv_category or genre_filter or "",
            hidden=False,
            page=(stremio_skip // int(Telegram.IPTV_PAGE_SIZE)) + 1,
            page_size=int(Telegram.IPTV_PAGE_SIZE),
        )
        return {
            "metas": [iptv_meta(item) for item in data.get("channels", [])],
            "cacheMaxAge": 300,
            "staleRevalidate": 300,
            "staleError": 3600,
        }

    try:
        if id.startswith("custom_"):
            catalog_id = id.removeprefix("custom_")
            catalog = await db.get_custom_catalog(catalog_id)
            if (
                not catalog
                or not catalog.get("visible", True)
                or not _token_can_view(catalog.get("visibility") or "public", catalog.get("allowed_tokens") or [], token_data)
            ):
                return {
                    "metas": [],
                    "cacheMaxAge": 0,
                    "staleRevalidate": 0,
                    "staleError": 0,
                }

            db_media_type = "tv" if media_type == "series" else "movie"
            data = await db.get_custom_catalog_items(
                catalog_id=catalog_id,
                media_type=db_media_type,
                page=page,
                page_size=PAGE_SIZE,
            )
            items = [
                media_doc
                for media_doc in data.get("items", [])
                if _media_visible_to_token(media_doc, token_data, catalog_id=catalog_id)
            ]
        elif search_query:
            search_results = await db.search_documents(query=search_query, page=page, page_size=PAGE_SIZE)
            all_items = search_results.get("results", [])
            db_media_type = "tv" if media_type == "series" else "movie"
            items = _filter_visible_media(
                [item for item in all_items if item.get("media_type") == db_media_type],
                token_data,
                allow_searchable_exclusive=True,
            )
        else:
            if "latest" in id:
                sort_params = [("updated_on", "desc")]
            elif "top" in id:
                sort_params = [("rating", "desc")]
            else:
                sort_params = [("updated_on", "desc")]

            if media_type == "movie":
                data = await db.sort_movies(sort_params, page, PAGE_SIZE, genre_filter=genre_filter)
                items = _filter_visible_media(data.get("movies", []), token_data)
            else:
                data = await db.sort_tv_shows(sort_params, page, PAGE_SIZE, genre_filter=genre_filter)
                items = _filter_visible_media(data.get("tv_shows", []), token_data)
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

    if media_type == "tv" and id.startswith(IPTV_ID_PREFIX):
        channel_id = id.removeprefix(IPTV_ID_PREFIX)
        channel = await get_iptv_channel(db, channel_id)
        return {
            "meta": iptv_meta(channel) if channel else {},
            "cacheMaxAge": 300,
            "staleRevalidate": 300,
            "staleError": 3600,
        }

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
    if not _media_visible_to_token(media, token_data, allow_searchable_exclusive=True):
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
        "poster": _poster_url(media.get("imdb_id") or id, media.get("poster")),
        "logo": media.get("logo", ""),
        "background": _abs_media_url(media.get("backdrop")),
        "imdb_id": media.get("imdb_id", ""),
        "releaseInfo": str(media.get("release_year", "")),
        "moviedb_id": media.get("tmdb_id", ""),
        "cast": media.get("cast") or [],
        "runtime": media.get("runtime") or "",
    }
    await _apply_fanart(meta_obj, media)

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


@router.get("/{token}/subtitles/{media_type}/{id}/{extra:path}.json")
@router.get("/{token}/subtitles/{media_type}/{id}.json")
async def get_subtitles(
    token: str,
    media_type: str,
    id: str,
    extra: Optional[str] = None,
    token_data: dict = Depends(verify_token),
):
    try:
        parts = id.split(":")
        imdb_id = parts[0]
        season_num = int(parts[1]) if len(parts) > 1 else None
        episode_num = int(parts[2]) if len(parts) > 2 else None
    except Exception:
        return {"subtitles": []}

    db_media_type = "tv" if media_type in ("series", "tv") else "movie"
    media = await db.get_media_details(
        imdb_id=imdb_id,
        season_number=season_num,
        episode_number=episode_num,
    )
    if not _media_visible_to_token(media, token_data, allow_searchable_exclusive=True):
        return {"subtitles": []}
    subtitles = await get_subtitles_for(imdb_id, db_media_type, season_num, episode_num)
    return {"subtitles": stremio_subtitle_entries(subtitles, token, BASE_URL)}

#----- Collect Global Search streams for a title/episode via IMDb lookup
async def _global_streams_for(token: str, imdb_id: str, media_type: str, season_num: Optional[int], episode_num: Optional[int]) -> list:
    imdb_media_type = "tvSeries" if media_type == "series" else "movie"

    detail = await get_detail(imdb_id=imdb_id, media_type=imdb_media_type)
    if not detail or not detail.get("title"):
        return []

    expected_title = detail["title"]
    year = (detail.get("releaseDetailed") or {}).get("year") or None

    if season_num is not None and episode_num is not None:
        try:
            await get_season(imdb_id=imdb_id, season_id=season_num, episode_id=episode_num)
        except Exception:
            pass

    try:
        global_results = await global_search(
            expected_title,
            Telegram.AUTH_CHANNEL,
            year=year,
            season=season_num,
            episode=episode_num,
        )
    except Exception as e:
        LOGGER.error(f"[GLOBAL SEARCH] search failed for '{expected_title}': {e}")
        return []

    streams = []
    for r in global_results:
        is_split = bool(r.get("is_split"))
        quality = r.get("quality") or "HD"
        filename = r.get("title") or "video.mkv"
        title_parts = [
            f"📁 {filename}",
            f"💾 {r.get('size')}" if r.get("size") else "",
            f"📡 {r.get('source_chat')}" if r.get("source_chat") else "📡 Global Search",
        ]
        if is_split:
            kind = "zip parts" if r.get("is_zip") else "parts"
            title_parts.append(f"📦 {r.get('part_count', 0)} {kind}")
        streams.append({
            "name": f"🌐 GLOBAL {quality}",
            "title": "\n".join(part for part in title_parts if part),
            "url": f"{BASE_URL}/dl/{token}/{r.get('token')}/video.mkv",
            "size_bytes": parse_size_to_bytes(r.get("size", "")),
            "_recommended": False,
        })
    return streams


@router.api_route("/{token}/stream/{media_type}/{id}.json", methods=["GET", "HEAD"])
async def get_streams(
    request: Request,
    token: str,
    media_type: str,
    id: str,
    response: Response,
    token_data: dict = Depends(verify_token)
):
    apply_stremio_no_cache(response)
    asyncio.create_task(record_client(
        token,
        token_data.get("name") or "Unknown",
        client_ip_from(request),
        request.headers.get("user-agent"),
    ))

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

        if limit_type == "daily":
            title = "🚫 Daily Limit Reached - Upgrade Required"
        elif limit_type == "monthly":
            title = "🚫 Monthly Limit Reached - Upgrade Required"
        elif limit_type == "active_streams":
            title = "🚫 Active Stream Limit Reached\nClose another stream and try again."
        else:
            title = "🚫 Server Stream Limit Reached\nPlease try again shortly."

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

    if media_type == "tv" and id.startswith(IPTV_ID_PREFIX):
        settings = await get_iptv_settings(db)
        if not settings.get("enabled"):
            return {
                "streams": [],
                "cacheMaxAge": 300,
                "staleRevalidate": 300,
                "staleError": 3600,
            }
        channel_id = id.removeprefix(IPTV_ID_PREFIX)
        channel = await get_iptv_channel(db, channel_id)
        return {
            "streams": build_iptv_streams(channel, token) if channel else [],
            "cacheMaxAge": 120,
            "staleRevalidate": 120,
            "staleError": 600,
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

    streams = []
    if media_details:
        if not _media_visible_to_token(media_details, token_data, allow_searchable_exclusive=True):
            return {
                "streams": [],
                "cacheMaxAge": 0,
                "staleRevalidate": 0,
                "staleError": 0,
            }
        for quality in media_details.get("telegram", []):
            if quality.get("hidden_from_stremio"):
                continue
            filename = quality.get("name", "")
            quality_str = quality.get("quality", "HD")
            size = quality.get("size", "")
            size_bytes = parse_size_to_bytes(size)

            stream_name, stream_title = format_stream_details(
                filename, quality_str, size
            )
            badges = []
            if quality.get("recommended"):
                badges.append("Recommended")
            if quality.get("parts"):
                badges.append("Split")
            if quality.get("flagged_duplicate"):
                badges.append("Duplicate")
            if quality.get("quality_note"):
                badges.append(str(quality.get("quality_note"))[:40])
            if badges:
                stream_name = f"{' | '.join(badges)} | {stream_name}"

            source_type = quality.get("source_type") or "telegram"
            if source_type == "local_vps":
                local_stream = await build_local_vps_stream(token, quality, stream_name)
                if local_stream:
                    local_stream["_recommended"] = bool(quality.get("recommended"))
                    streams.append(local_stream)
                continue

            if source_type == "torrent":
                info_hash = quality.get("info_hash")
                torrent_stats = await db.get_torrent_stats(info_hash) if info_hash else None
                download_job = await db.get_torrent_download(info_hash) if info_hash else None
                db.queue_torrent_stats_refresh(
                    info_hash,
                    quality.get("sources") or [],
                    torrent_private=bool(quality.get("torrent_private", False)),
                )
                torrent_stream = build_torrent_stream(quality, stream_name, stream_title, torrent_stats)
                if torrent_stream:
                    torrent_stream["_recommended"] = bool(quality.get("recommended"))
                    streams.append(torrent_stream)
                downloaded_stream = await build_downloaded_torrent_stream(
                    token,
                    quality,
                    stream_name,
                    download_job,
                    season_number=season_num,
                    episode_number=episode_num,
                )
                if downloaded_stream:
                    downloaded_stream["_recommended"] = bool(quality.get("recommended"))
                    streams.append(downloaded_stream)
                continue

            if not quality.get("id"):
                continue

            original_url = f"{BASE_URL}/dl/{token}/{quality.get('id')}/video.mkv"
            proxy_url = f"{Telegram.HTTP_PROXY_URL}{original_url}" if Telegram.PROXY and Telegram.HTTP_PROXY_URL else None

            if Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH and proxy_url:
                streams.append({
                    "name": f"{stream_name} (Proxy)",
                    "title": stream_title,
                    "url": proxy_url,
                    "size_bytes": size_bytes,
                    "_recommended": bool(quality.get("recommended")),
                })
                streams.append({
                    "name": f"{stream_name} (Direct)",
                    "title": stream_title,
                    "url": original_url,
                    "size_bytes": size_bytes,
                    "_recommended": bool(quality.get("recommended")),
                })
            elif proxy_url:
                streams.append({
                    "name": stream_name,
                    "title": stream_title,
                    "url": proxy_url,
                    "size_bytes": size_bytes,
                    "_recommended": bool(quality.get("recommended")),
                })
            else:
                streams.append({
                    "name": stream_name,
                    "title": stream_title,
                    "url": original_url,
                    "size_bytes": size_bytes,
                    "_recommended": bool(quality.get("recommended")),
                })

    #----- Global Search fallback when the library has no streams for this title/episode
    if not streams and is_global_search_enabled():
        try:
            streams.extend(
                await _global_streams_for(token, imdb_id, media_type, season_num, episode_num)
            )
        except Exception as e:
            LOGGER.error(f"[GLOBAL SEARCH] stream search failed for {imdb_id}: {e}")

    #----- Per-token quality filter (fall back to all if it would hide everything)
    config = token_data.get("config") or {}
    quality_filter = set(config.get("quality_filter") or [])
    if quality_filter and streams:
        filtered = [s for s in streams if stream_res_label(s.get("name", "")) in quality_filter]
        if filtered:
            streams = filtered

    #----- Recommended first, then quality sort (per-token asc/desc, size as tie-breaker)
    streams.sort(key=lambda s: 0 if s.get("_recommended") else 1)
    streams.sort(
        key=lambda s: (get_resolution_priority(s.get("name", "")), s.get("size_bytes") or 0),
        reverse=config.get("quality_sort") != "asc",
    )
    for s in streams:
        s.pop("_recommended", None)

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
                var intentUrl = "intent://" + manifestUrl.replace('https://', '').replace('http://', '') + "#Intent;scheme=stremio;package=com.stremio.one;end";
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
