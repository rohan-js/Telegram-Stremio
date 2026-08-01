import asyncio
import json
import random
import secrets
from datetime import datetime
from fastapi import Request, Query, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
import Backend.pyrofork.bot as botmod
from pyrogram.errors import FloodWait
from pyrogram.enums.chat_member_status import ChatMemberStatus
from pyrogram.enums.chat_members_filter import ChatMembersFilter
from pyrogram.types import ChatPrivileges
from Backend import db, StartTime, __version__
from Backend.logger import LOGGER
from Backend.helper.pyro import get_readable_time
from Backend.pyrofork.bot import multi_clients, StreamBot
from Backend.helper.custom_dl import run_speed_test, _speed_test_single_client
from Backend.helper.nginx_egress import get_nginx_egress_summary
from Backend.helper.host_outbound import get_vps_outbound_summary
from Backend.helper.auto_catalog import (
    get_auto_catalog_settings,
    get_auto_catalog_sync_status,
    start_auto_catalog_sync_background,
    update_auto_catalog_settings,
)
from Backend.helper.encrypt import encode_string
from Backend.helper.manual_add import resolve_telegram_message
from Backend.helper.manual_session import is_personal_media, manual_session_manager
from Backend.helper.iptv import (
    get_iptv_settings,
    get_iptv_sync_status,
    list_iptv_channels,
    set_iptv_channel_hidden,
    start_iptv_sync_background,
    update_iptv_settings,
)
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.scan_manager import dbcheck_manager, duplicate_manager, scan_manager
from Backend.helper.analytics import get_activity_overview
from Backend.helper.subtitles import (
    list_languages,
    list_title_subtitles,
    manual_ingest_subtitle,
    remove_subtitle,
    resolve_subtitle_message,
)
from Backend.config import Telegram
from Backend.helper.warp_control import apply_warp_mode, get_warp_status
from Backend.helper.production_ops import create_tracking_backup, get_launch_readiness
from Backend.helper.beta_access import is_exempt_token
from Backend.helper.owner_alerts import schedule_owner_alert
from Backend.helper.backup import export_config, import_config
from Backend.helper.health import run_health_checks
from Backend.helper.log_tools import read_recent_logs
from Backend.helper.requests_manager import (
    delete_request,
    list_requests,
    popular_requests,
    search_titles as search_requested_titles,
    set_status as set_request_status,
    submit_request,
)
from Backend.helper.metadata import (
    extract_default_id,
    fetch_selected_movie_metadata,
    fetch_selected_tv_metadata,
    search_any_candidates,
    search_movie_candidates,
    search_tv_candidates,
)
from Backend.helper.split_files import strip_part_suffix
from time import time


def _public_settings(data: dict) -> dict:
    public = dict(data or {})
    public["admin_password_set"] = bool(public.get("admin_password"))
    public["admin_password"] = ""
    public["session_secret_set"] = bool(public.get("session_secret"))
    public["session_secret"] = ""
    public["secrets_env_only"] = [
        "BOT_TOKEN",
        "HELPER_BOT_TOKEN",
        "API_HASH",
        "DATABASE",
        "TMDB_API",
        "GEMINI_API_KEY",
        "GROQ_API_KEY",
        "USER_SESSION_STRING",
        "SESSION_SECRET",
        "WARP_PRIVATE_KEYS",
    ]
    public["secret_statuses"] = SettingsManager.secret_statuses()
    return public


# --- API Routes for System Stats ---

async def get_system_stats_api():
    try:
        db_stats = await db.get_database_stats()
        total_movies = sum(stat.get("movie_count", 0) for stat in db_stats)
        total_tv_shows = sum(stat.get("tv_count", 0) for stat in db_stats)
        api_tokens = await db.get_all_api_tokens()
        
        return {
            "server_status": "running",
            "uptime": get_readable_time(time() - StartTime),
            "telegram_bot": f"@{StreamBot.username}" if StreamBot and StreamBot.username else "@StreamBot",
            "connected_bots": len(multi_clients),
            "version": __version__,
            "movies": total_movies,
            "tv_shows": total_tv_shows,
            "databases": db_stats,
            "total_databases": len(db_stats),
            "current_db_index": db.current_db_index,
            "egress": get_nginx_egress_summary(),
            "vps_outbound": await get_vps_outbound_summary(db),
            "api_tokens": api_tokens
        }
    except Exception as e:
        print(f"System Stats API Error: {e}")
        return {
            "server_status": "error", 
            "error": str(e)
        }


# --- IPTV Live TV Administration ---

async def get_iptv_status_api():
    try:
        return await get_iptv_sync_status(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def sync_iptv_api(force: bool = True):
    try:
        result = await start_iptv_sync_background(db, force=force)
        if not result.get("started") and result.get("reason") == "sync_already_running":
            raise HTTPException(status_code=409, detail="IPTV sync is already running.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def list_iptv_channels_api(
    search: str = "",
    category: str = "",
    country: str = "",
    hidden: bool | None = None,
    page: int = 1,
    page_size: int = 50,
):
    try:
        return await list_iptv_channels(
            db,
            search=search,
            category=category,
            country=country,
            hidden=hidden,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def update_iptv_channel_api(channel_id: str, payload: dict):
    if "hidden" not in payload:
        raise HTTPException(status_code=400, detail="hidden is required.")
    updated = await set_iptv_channel_hidden(db, channel_id, bool(payload.get("hidden")))
    if not updated:
        raise HTTPException(status_code=404, detail="IPTV channel not found.")
    return {"success": True, "channel_id": channel_id, "hidden": bool(payload.get("hidden"))}


async def get_iptv_settings_api():
    try:
        return await get_iptv_settings(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def update_iptv_settings_api(payload: dict):
    try:
        return await update_iptv_settings(db, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_settings_api():
    try:
        return {"settings": _public_settings(SettingsManager.current().to_dict())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def update_settings_api(payload: dict):
    try:
        results = await SettingsManager.update(db, payload or {})
        return {
            "message": "Settings saved.",
            "settings": _public_settings(SettingsManager.current().to_dict()),
            "results": results,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        LOGGER.error(f"update_settings_api failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Public content requests ---

async def request_search_api(q: str) -> dict:
    if not SettingsManager.current().content_requests_enabled:
        return {"status": "disabled", "results": []}
    return {"status": "success", "results": await search_requested_titles(q)}


async def request_popular_api() -> dict:
    if not SettingsManager.current().content_requests_enabled:
        return {"status": "disabled", "requests": []}
    return {"status": "success", "requests": await popular_requests()}


async def request_submit_api(payload: dict, client_ip: str) -> dict:
    settings = SettingsManager.current()
    if not settings.content_requests_enabled:
        return {"ok": False, "reason": "disabled"}
    if settings.content_requests_beta_only and Telegram.SUBSCRIPTION:
        user_id = str((payload or {}).get("user_id") or "").strip()
        allowed = {str(x) for x in getattr(Telegram, "BETA_ALLOWED_USER_IDS", [])}
        exempt = {str(x) for x in getattr(Telegram, "BETA_EXEMPT_USER_IDS", [])}
        if not user_id or (user_id not in allowed and user_id not in exempt):
            return {"ok": False, "reason": "beta_only"}
    return await submit_request(
        media_type=(payload or {}).get("media_type"),
        tmdb_id=(payload or {}).get("tmdb_id"),
        imdb_id=(payload or {}).get("imdb_id"),
        title=(payload or {}).get("title"),
        year=(payload or {}).get("year"),
        poster=(payload or {}).get("poster"),
        client_ip=client_ip,
    )


async def get_requests_api() -> dict:
    return {"status": "success", "data": await list_requests()}


async def update_request_api(request_id: str, payload: dict) -> dict:
    doc = await set_request_status(request_id, (payload or {}).get("status"))
    if not doc:
        raise HTTPException(status_code=404, detail="Request not found or invalid status.")
    return {"status": "success", "request": doc}


async def delete_request_api(request_id: str) -> dict:
    if not await delete_request(request_id):
        raise HTTPException(status_code=404, detail="Request not found.")
    return {"status": "success", "message": "Request deleted."}


async def get_health_api(force: bool = False) -> dict:
    return await run_health_checks(force=force)


async def get_admin_logs_api(max_bytes: int = 200_000) -> dict:
    return read_recent_logs(max_bytes=max_bytes)


async def download_admin_logs_api(max_bytes: int = 500_000):
    data = read_recent_logs(max_bytes=max_bytes)
    return PlainTextResponse(
        data.get("text") or "",
        headers={"Content-Disposition": "attachment; filename=telegram-stremio-redacted.log"},
    )


async def export_config_api():
    return JSONResponse(
        await export_config(),
        headers={"Content-Disposition": "attachment; filename=telegram-stremio-config-backup.json"},
    )


async def import_config_api(payload: dict) -> dict:
    try:
        return {"status": "success", "result": await import_config(payload or {})}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


async def get_tools_channels_api():
    return {
        "channels": [{"id": channel, "label": str(channel)} for channel in Telegram.AUTH_CHANNEL],
    }


def _manual_session_result(document: dict) -> dict:
    media_type = "tv" if str(document.get("media_type") or document.get("type")).lower() in {"tv", "series"} else "movie"
    return {
        "tmdb_id": document.get("tmdb_id"),
        "db_index": document.get("db_index"),
        "media_type": media_type,
        "title": document.get("title") or "",
        "year": document.get("release_year") or "",
        "poster": document.get("poster") or "",
        "imdb_id": document.get("imdb_id") or "",
        "is_personal": is_personal_media(document.get("tmdb_id")),
    }


async def search_manual_session_api(query: str) -> dict:
    query = (query or "").strip()
    if not query:
        return {"results": []}

    results, seen = [], set()

    def add(document: dict, *, db_index: int | None = None, media_type: str | None = None) -> None:
        item = dict(document)
        if db_index is not None:
            item["db_index"] = db_index
        if media_type is not None:
            item["media_type"] = media_type
        entry = _manual_session_result(item)
        key = (entry["tmdb_id"], entry["db_index"], entry["media_type"])
        if entry["tmdb_id"] is None or key in seen:
            return
        seen.add(key)
        results.append(entry)

    selected_id = extract_default_id(query)
    if not selected_id and (query.startswith("tt") and query[2:].isdigit() or query.lstrip("-").isdigit()):
        selected_id = query
    if selected_id:
        try:
            if str(selected_id).startswith("tt"):
                document = await db.get_media_details(str(selected_id))
                if document:
                    add(document)
            else:
                tmdb_id = int(selected_id)
                storage_keys = sorted((key for key in db.dbs if key.startswith("storage_")), reverse=True)
                for db_key in storage_keys:
                    db_index = int(db_key.split("_", 1)[1])
                    for media_type in ("movie", "tv"):
                        document = await db.dbs[db_key][media_type].find_one({"tmdb_id": tmdb_id})
                        if document:
                            add(document, db_index=db_index, media_type=media_type)
        except Exception as e:
            LOGGER.warning(f"Manual session ID lookup failed for {query}: {e}")

    if not results:
        data = await db.search_documents(query, 1, 20)
        for document in data.get("results", []):
            add(document)

    library_ids = {(entry["imdb_id"] or "", str(entry["tmdb_id"] or "")) for entry in results}
    try:
        online = await search_any_candidates(query)
    except Exception as e:
        LOGGER.warning(f"Manual session online search failed for '{query}': {e}")
        online = []

    for cand in online:
        if not cand.get("title"):
            continue
        imdb_id = cand.get("imdb_id") or ""
        tmdb_id = cand.get("tmdb_id")
        if (imdb_id, str(tmdb_id or "")) in library_ids:
            continue
        selected_id = cand.get("selected_id") or (imdb_id if imdb_id.startswith("tt") else str(tmdb_id or ""))
        results.append({
            "tmdb_id": tmdb_id,
            "db_index": None,
            "media_type": "tv" if cand.get("media_type") == "tv" else "movie",
            "title": cand.get("title") or "",
            "year": cand.get("year") or "",
            "poster": cand.get("poster") or "",
            "imdb_id": imdb_id,
            "selected_id": selected_id,
            "is_personal": False,
        })
    return {"results": results[:30]}


async def get_manual_session_api() -> dict:
    return {
        "session": manual_session_manager.current(),
        "manual_channels": SettingsManager.current().manual_channels,
    }


async def set_manual_session_api(payload: dict) -> dict:
    media_type = "tv" if payload.get("media_type") in {"tv", "series"} else "movie"
    selected_id = str(payload.get("selected_id") or "").strip()

    try:
        tmdb_id = int(payload.get("tmdb_id"))
        db_index = int(payload.get("db_index"))
    except (TypeError, ValueError):
        tmdb_id = None
        db_index = None

    in_library = payload.get("in_library", True) and tmdb_id is not None and db_index is not None
    if not in_library:
        return await _set_online_manual_session(payload, media_type, selected_id)

    document = await db.get_document(media_type, tmdb_id, db_index)
    if not document:
        raise HTTPException(status_code=404, detail="That title was not found in the library.")
    if not SettingsManager.current().manual_channels:
        raise HTTPException(status_code=400, detail="Configure at least one Manual Channel in Settings first.")

    personal = is_personal_media(tmdb_id)
    session = {
        "tmdb_id": tmdb_id,
        "db_index": db_index,
        "media_type": media_type,
        "title": document.get("title") or "",
        "year": document.get("release_year") or "",
        "is_personal": personal,
        "kind": "personal" if personal else "real",
    }
    season = payload.get("season")
    if season not in (None, ""):
        try:
            season = int(season)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Season must be a number.")
    else:
        season = None

    if personal:
        if media_type == "tv" and season is None:
            raise HTTPException(status_code=400, detail="A season is required for personal TV titles.")
        episode = payload.get("episode")
        if episode not in (None, ""):
            try:
                episode = int(episode)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Episode must be a number.")
        else:
            episode = None
        session.update({
            "season": season,
            "episode": episode,
            "quality": str(payload.get("quality") or "").strip() or None,
            "default_id": None,
        })
    else:
        imdb_id = str(document.get("imdb_id") or "")
        session.update({
            "season": season if media_type == "tv" else None,
            "episode": None,
            "quality": None,
            "default_id": imdb_id if imdb_id.startswith("tt") else str(tmdb_id),
        })

    return {"status": "success", "session": await manual_session_manager.activate(session)}


async def _set_online_manual_session(payload: dict, media_type: str, selected_id: str) -> dict:
    if not selected_id:
        raise HTTPException(status_code=400, detail="A library title or a selected id is required.")
    if not SettingsManager.current().manual_channels:
        raise HTTPException(status_code=400, detail="Configure at least one Manual Channel in Settings first.")

    meta = await (
        fetch_selected_movie_metadata(selected_id) if media_type == "movie"
        else fetch_selected_tv_metadata(selected_id)
    )
    if not meta:
        raise HTTPException(status_code=404, detail="Could not fetch metadata for the selected title.")

    imdb_id = meta.get("imdb_id") or ""
    default_id = imdb_id if str(imdb_id).startswith("tt") else selected_id

    season = payload.get("season")
    if media_type == "tv" and season not in (None, ""):
        try:
            season = int(season)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Season must be a number.")
    else:
        season = None

    try:
        display_tmdb = int(meta.get("tmdb_id")) if meta.get("tmdb_id") is not None else 0
    except (TypeError, ValueError):
        display_tmdb = 0

    session = {
        "tmdb_id": display_tmdb,
        "db_index": None,
        "media_type": media_type,
        "title": meta.get("title") or "",
        "year": str(meta.get("release_year") or meta.get("year") or ""),
        "is_personal": False,
        "kind": "real",
        "default_id": default_id,
        "season": season,
        "episode": None,
        "quality": None,
    }
    return {"status": "success", "session": await manual_session_manager.activate(session)}


async def clear_manual_session_api() -> dict:
    await manual_session_manager.clear()
    return {"status": "success"}


async def start_tools_scan_api(payload: dict):
    channels = payload.get("channels") or list(Telegram.AUTH_CHANNEL)
    mode = payload.get("mode") or "scan"
    if mode not in {"scan", "rescan"}:
        raise HTTPException(status_code=400, detail="mode must be scan or rescan")
    result = await scan_manager.start(StreamBot, channels, mode=mode)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message") or "Scan could not start")
    return result


async def cancel_tools_scan_api():
    return await scan_manager.cancel()


async def get_tools_scan_status_api():
    return {"status": scan_manager.get_status()}


async def start_tools_dbcheck_api():
    result = await dbcheck_manager.start(StreamBot, db)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message") or "DB check could not start")
    return result


async def cancel_tools_dbcheck_api():
    return await dbcheck_manager.cancel()


async def get_tools_dbcheck_status_api():
    return {"status": dbcheck_manager.get_status()}


async def purge_tools_dead_links_api(payload: dict):
    return await dbcheck_manager.purge(db, payload.get("stream_ids") or [])


#----- Duplicate check & cleanup
async def start_duplicate_check_api() -> dict:
    result = await duplicate_manager.start()
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message", "Could not start duplicate scan."))
    return {"status": "success", **result}


async def cancel_duplicate_check_api() -> dict:
    result = await duplicate_manager.cancel()
    return {"status": "success" if result.get("ok") else "error", **result}


async def duplicate_check_status_api() -> dict:
    return {"status": "success", "data": duplicate_manager.get_status()}


#----- Remove selected duplicate streams, or (delete_all) keep the newest per group
async def purge_duplicates_api(payload: dict | None = None) -> dict:
    payload = payload or {}
    delete_all = bool(payload.get("delete_all"))
    stream_ids = payload.get("stream_ids")
    if not delete_all and (not isinstance(stream_ids, list) or not stream_ids):
        raise HTTPException(status_code=400, detail="Provide 'stream_ids' or set 'delete_all'.")
    result = await duplicate_manager.purge(stream_ids, delete_all=delete_all)
    return {"status": "success" if result.get("ok") else "error", **result}


# ---------------------------------------------------------------------------
# Bot Admin tool: promote managed bots to admins in served channels
# ---------------------------------------------------------------------------

_bot_admin_apply_state: dict = {
    "running": False,
    "status": "idle",
    "total": 0,
    "done": 0,
    "results": [],
    "error": "",
    "task": None,
}


def _norm_chat_id(ch):
    s = str(ch).strip()
    if not s:
        return None
    return int(s) if s.lstrip("-").isdigit() else s


async def _managed_bots() -> list[dict]:
    bots: list[dict] = []
    for cid in sorted(multi_clients.keys()):
        client = multi_clients.get(cid)
        if client is None:
            continue
        me = getattr(client, "me", None)
        if me is None:
            try:
                me = await client.get_me()
            except Exception as e:
                LOGGER.warning(f"[BotAdmin] Could not resolve bot client {cid}: {e}")
                me = None
        if not me:
            continue
        bots.append({
            "client_id": cid,
            "user_id": me.id,
            "username": me.username,
            "name": me.first_name or me.username or f"Bot {cid + 1}",
            "is_main": cid == 0,
        })
    return bots


def _bot_served_channels() -> list[dict]:
    s = SettingsManager.current()
    order: list[str] = []
    mapping: dict[str, dict] = {}

    def add(ch, role):
        nid = _norm_chat_id(ch)
        if nid is None:
            return
        key = str(nid)
        if key not in mapping:
            mapping[key] = {"id": nid, "roles": []}
            order.append(key)
        if role not in mapping[key]["roles"]:
            mapping[key]["roles"].append(role)

    for ch in s.auth_channels:
        add(ch, "auth")
    for ch in s.manual_channels:
        add(ch, "manual")
    for ch in s.anime_channels:
        add(ch, "anime")
    if s.announcement_channel:
        add(s.announcement_channel, "announce")
    if s.skip_channel:
        add(s.skip_channel, "skip")
    return [mapping[k] for k in order]


def _bot_admin_privileges() -> ChatPrivileges:
    return ChatPrivileges(
        can_manage_chat=True,
        can_post_messages=True,
        can_edit_messages=True,
        can_delete_messages=True,
        can_invite_users=True,
        can_pin_messages=False,
        can_promote_members=False,
        can_change_info=False,
        can_restrict_members=False,
        can_manage_video_chats=False,
        is_anonymous=False,
    )


def _no_privileges() -> ChatPrivileges:
    return ChatPrivileges(
        can_manage_chat=False,
        can_post_messages=False,
        can_edit_messages=False,
        can_delete_messages=False,
        can_invite_users=False,
        can_pin_messages=False,
        can_promote_members=False,
        can_change_info=False,
        can_restrict_members=False,
        can_manage_video_chats=False,
        is_anonymous=False,
    )


async def _bot_member_status(chat_id, bot_user_id) -> str:
    try:
        m = await botmod.Userbot.get_chat_member(chat_id, bot_user_id)
        st = m.status
        if st in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return "admin"
        if st == ChatMemberStatus.BANNED:
            return "banned"
        if st == ChatMemberStatus.RESTRICTED:
            return "restricted"
        if st == ChatMemberStatus.MEMBER:
            return "member"
        return "missing"
    except Exception:
        return "missing"


def _friendly_promote_error(exc) -> str:
    msg = str(exc)
    up = msg.upper()
    if "CHAT_ADMIN_REQUIRED" in up:
        return "Your session account isn't an admin with rights to do this here."
    if "USER_CREATOR" in up or "ADMIN_RANK" in up:
        return "Can't modify the channel creator."
    if "ADD_ADMINS" in up or ("PROMOTE" in up and "RIGHT" in up):
        return "Your session account can't grant these rights (it doesn't hold them itself)."
    if "PARTICIPANT" in up or "USER_NOT_MUTUAL_CONTACT" in up:
        return "The bot isn't in the channel and couldn't be added automatically."
    if "BOTS_TOO_MUCH" in up:
        return "This channel already has the maximum number of bots."
    return msg


async def _session_rights(chat_id) -> dict:
    try:
        me = await botmod.Userbot.get_chat_member(chat_id, "me")
    except Exception as e:
        return {"manageable": False, "status": "unknown", "reason": f"Couldn't check your rights: {e}"}
    st = me.status
    if st == ChatMemberStatus.OWNER:
        return {"manageable": True, "status": "owner", "reason": ""}
    if st == ChatMemberStatus.ADMINISTRATOR:
        can_promote = bool(getattr(me, "privileges", None) and me.privileges.can_promote_members)
        return {
            "manageable": can_promote,
            "status": "admin_can_promote" if can_promote else "admin_no_promote",
            "reason": "" if can_promote else "You're an admin here but without the 'Add New Admins' permission.",
        }
    return {"manageable": False, "status": "not_admin", "reason": "Your session account is not an admin here."}


async def bot_admin_scan_api() -> dict:
    if botmod.Userbot is None:
        return {"status": "error", "reason": "no_session",
                "message": "Connect your Telegram session from the Settings page to manage channel admins."}

    bots = await _managed_bots()
    if len(bots) <= 1:
        return {"status": "error", "reason": "single_token", "bots": bots,
                "message": "Add at least one extra bot token (multi-token) to use this tool."}

    channels = _bot_served_channels()
    managed_ids = {b["user_id"] for b in bots}
    out: list[dict] = []

    for ch in channels:
        cid = ch["id"]
        entry = {
            "id": str(cid), "roles": ch["roles"], "name": str(cid),
            "accessible": False, "manageable": False, "session_status": "",
            "reason": "", "bots": {}, "orphans": [],
        }

        try:
            chat = await botmod.Userbot.get_chat(cid)
            entry["name"] = getattr(chat, "title", None) or getattr(chat, "first_name", None) or str(cid)
            entry["accessible"] = True
        except Exception as e:
            entry["reason"] = f"Session account can't access this channel: {e}"
            out.append(entry)
            continue

        rights = await _session_rights(cid)
        entry["manageable"] = rights["manageable"]
        entry["session_status"] = rights["status"]
        entry["reason"] = rights["reason"]

        for b in bots:
            entry["bots"][str(b["user_id"])] = await _bot_member_status(cid, b["user_id"])

        try:
            async for m in botmod.Userbot.get_chat_members(cid, filter=ChatMembersFilter.ADMINISTRATORS):
                u = getattr(m, "user", None)
                if u and getattr(u, "is_bot", False) and u.id not in managed_ids:
                    entry["orphans"].append({
                        "user_id": u.id, "username": u.username,
                        "name": u.first_name or u.username or str(u.id),
                    })
        except Exception as e:
            LOGGER.warning(f"[BotAdmin] Could not list admins for {cid}: {e}")

        out.append(entry)

    return {"status": "success", "data": {"bots": bots, "channels": out}}


async def _promote_one(chat_id, bot: dict, privileges: ChatPrivileges, _retry: bool = True) -> dict:
    label = bot.get("name") or (f"@{bot['username']}" if bot.get("username") else str(bot["user_id"]))
    bid = bot["user_id"]

    if await _bot_member_status(chat_id, bid) == "admin":
        return {"bot": label, "user_id": bid, "status": "already", "message": "Already an admin."}

    try:
        await botmod.Userbot.promote_chat_member(chat_id, bid, privileges=privileges)
        return {"bot": label, "user_id": bid, "status": "added", "message": "Promoted to admin."}
    except FloodWait as fw:
        wait = int(getattr(fw, "value", getattr(fw, "x", 5)) or 5)
        if _retry:
            await asyncio.sleep(wait + 1)
            return await _promote_one(chat_id, bot, privileges, _retry=False)
        return {"bot": label, "user_id": bid, "status": "error",
                "message": f"Rate-limited by Telegram (wait {wait}s) — try again."}
    except Exception as e:
        up = str(e).upper()
        if _retry and ("PARTICIPANT" in up or "USER_NOT_MUTUAL_CONTACT" in up):
            try:
                await botmod.Userbot.add_chat_members(chat_id, bid)
                await asyncio.sleep(0.5)
                await botmod.Userbot.promote_chat_member(chat_id, bid, privileges=privileges)
                return {"bot": label, "user_id": bid, "status": "added", "message": "Added and promoted to admin."}
            except Exception as e2:
                return {"bot": label, "user_id": bid, "status": "error", "message": _friendly_promote_error(e2)}
        return {"bot": label, "user_id": bid, "status": "error", "message": _friendly_promote_error(e)}


async def _demote_one(chat_id, user) -> dict:
    label = getattr(user, "first_name", None) or (f"@{user.username}" if getattr(user, "username", None) else str(user.id))
    try:
        await botmod.Userbot.promote_chat_member(chat_id, user.id, privileges=_no_privileges())
        return {"bot": label, "user_id": user.id, "status": "demoted", "message": "Admin rights removed (orphan)."}
    except Exception as e:
        return {"bot": label, "user_id": user.id, "status": "error", "message": _friendly_promote_error(e)}


async def _run_bot_admin_apply(channel_ids, selected, demote_orphans, managed_ids) -> None:
    state = _bot_admin_apply_state
    privileges = _bot_admin_privileges()
    try:
        for raw in channel_ids:
            cid = _norm_chat_id(raw)
            ch_result = {"id": str(cid), "name": str(cid), "items": []}

            try:
                chat = await botmod.Userbot.get_chat(cid)
                ch_result["name"] = getattr(chat, "title", None) or getattr(chat, "first_name", None) or str(cid)
            except Exception as e:
                ch_result["items"].append({"bot": "—", "status": "error", "message": f"Channel not accessible: {e}"})
                state["results"].append(ch_result)
                state["done"] += 1
                continue

            rights = await _session_rights(cid)
            if not rights["manageable"]:
                ch_result["items"].append({
                    "bot": "—", "status": "skipped",
                    "message": rights["reason"] or "Your session account can't add admins here.",
                })
                state["results"].append(ch_result)
                state["done"] += 1
                continue

            for b in selected:
                ch_result["items"].append(await _promote_one(cid, b, privileges))
                await asyncio.sleep(0.3)

            if demote_orphans:
                try:
                    async for m in botmod.Userbot.get_chat_members(cid, filter=ChatMembersFilter.ADMINISTRATORS):
                        u = getattr(m, "user", None)
                        if u and getattr(u, "is_bot", False) and u.id not in managed_ids:
                            ch_result["items"].append(await _demote_one(cid, u))
                            await asyncio.sleep(0.3)
                except Exception as e:
                    ch_result["items"].append({"bot": "orphans", "status": "error", "message": f"Couldn't scan orphans: {e}"})

            state["results"].append(ch_result)
            state["done"] += 1

        state["status"] = "completed"
    except Exception as e:
        LOGGER.error(f"[BotAdmin] Apply run failed: {e}")
        state["status"] = "error"
        state["error"] = str(e)
    finally:
        state["running"] = False


async def bot_admin_apply_api(payload: dict | None = None) -> dict:
    if botmod.Userbot is None:
        raise HTTPException(status_code=503, detail="No Telegram session connected. Connect one from Settings.")

    if _bot_admin_apply_state["running"]:
        raise HTTPException(status_code=409, detail="An apply run is already in progress.")

    payload = payload or {}
    channel_ids = payload.get("channel_ids") or []
    if not isinstance(channel_ids, list) or not channel_ids:
        raise HTTPException(status_code=400, detail="Select at least one channel.")

    bots = await _managed_bots()
    if len(bots) <= 1:
        raise HTTPException(status_code=400, detail="Need a session string and more than one bot token.")

    bot_by_id = {str(b["user_id"]): b for b in bots}
    sel_ids = payload.get("bot_ids")
    if isinstance(sel_ids, list) and sel_ids:
        selected = [bot_by_id[str(x)] for x in sel_ids if str(x) in bot_by_id]
    else:
        selected = bots
    if not selected:
        raise HTTPException(status_code=400, detail="No matching bots selected.")

    demote_orphans = bool(payload.get("demote_orphans"))
    managed_ids = {b["user_id"] for b in bots}
    state = _bot_admin_apply_state
    state.update({
        "running": True,
        "status": "running",
        "total": len(channel_ids),
        "done": 0,
        "results": [],
        "error": "",
    })
    state["task"] = asyncio.create_task(
        _run_bot_admin_apply(channel_ids, selected, demote_orphans, managed_ids)
    )
    return {"status": "success", "message": f"Applying to {len(channel_ids)} channel(s)…", "state": {
        "running": True, "status": "running", "total": state["total"], "done": 0, "results": [], "error": "",
    }}


async def bot_admin_apply_status_api() -> dict:
    st = _bot_admin_apply_state
    return {
        "status": "success",
        "data": {
            "running": st["running"],
            "status": st["status"],
            "total": st["total"],
            "done": st["done"],
            "results": st["results"],
            "error": st["error"],
        },
    }


async def get_warp_status_api():
    return get_warp_status()


async def apply_warp_api(payload: dict):
    enable = bool(payload.get("enable"))
    force = bool(payload.get("force", False))
    result = await apply_warp_mode(enable, force=force)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message") or "WARP switch failed")
    return result


async def get_launch_readiness_api():
    try:
        return await get_launch_readiness(db)
    except Exception as e:
        LOGGER.error(f"get_launch_readiness_api failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def run_backup_api(payload: dict | None = None):
    try:
        reason = (payload or {}).get("reason") or "manual"
        result = await create_tracking_backup(db, reason=reason)
        if not result.get("ok"):
            raise HTTPException(status_code=500, detail=result.get("error") or result.get("message") or "Backup failed")
        return {"status": "success", "backup": result}
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.error(f"run_backup_api failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def admin_takedown_api(payload: dict):
    media_type = payload.get("media_type")
    if media_type not in {"movie", "tv"}:
        raise HTTPException(status_code=400, detail="media_type must be movie or tv.")
    try:
        tmdb_id = int(payload.get("tmdb_id"))
        db_index = int(payload.get("db_index"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="tmdb_id and db_index are required.")

    quality_id = payload.get("id") or payload.get("quality_id")
    if not quality_id:
        raise HTTPException(status_code=400, detail="quality_id is required.")

    action = (payload.get("action") or "hide").strip().lower()
    reason = (payload.get("reason") or "Takedown/admin review").strip()
    season = payload.get("season")
    episode = payload.get("episode")

    if action not in {"hide", "delete"}:
        raise HTTPException(status_code=400, detail="action must be hide or delete.")

    if action == "hide":
        ok = await db.update_quality_flags(
            media_type=media_type,
            tmdb_id=tmdb_id,
            db_index=db_index,
            quality_id=quality_id,
            flags={
                "hidden_from_stremio": True,
                "flagged_duplicate": False,
                "quality_note": reason,
            },
            season=season,
            episode=episode,
            clear=False,
        )
    elif media_type == "movie":
        ok = await db.delete_movie_quality(tmdb_id, db_index, quality_id)
    else:
        if season is None or episode is None:
            raise HTTPException(status_code=400, detail="season and episode are required for TV takedown delete.")
        ok = await db.delete_tv_quality(tmdb_id, db_index, int(season), int(episode), quality_id)

    if not ok:
        raise HTTPException(status_code=404, detail="Quality not found.")

    try:
        await db.dbs["tracking"]["takedown_log"].insert_one(
            {
                "media_type": media_type,
                "tmdb_id": tmdb_id,
                "db_index": db_index,
                "quality_id": quality_id,
                "season": season,
                "episode": episode,
                "action": action,
                "reason": reason,
                "created_at": datetime.utcnow(),
            }
        )
    except Exception as exc:
        LOGGER.debug("takedown_log insert skipped: %s", exc)

    schedule_owner_alert(
        f"Admin takedown applied: {action} {media_type} tmdb={tmdb_id} quality={str(quality_id)[:12]} reason={reason}",
        key=f"takedown:{media_type}:{tmdb_id}:{quality_id}:{action}",
        cooldown_sec=60,
    )
    return {"status": "success", "message": f"Takedown {action} applied."}
    
# --- API Routes for Media Management ---

async def list_media_api(
    media_type: str = Query("movie", regex="^(movie|tv)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=100),
    search: str = Query("", max_length=100)
):
    try:
        if search:
            result = await db.search_documents(search, page, page_size)
            filtered_results = [item for item in result['results'] if item.get('media_type') == media_type]
            total_filtered = len(filtered_results)
            start_index = (page - 1) * page_size
            end_index = start_index + page_size
            paged_results = filtered_results[start_index:end_index]
            
            return {
                "total_count": total_filtered,
                "current_page": page,
                "total_pages": (total_filtered + page_size - 1) // page_size,
                "movies" if media_type == "movie" else "tv_shows": paged_results
            }
        else:
            if media_type == "movie":
                return await db.sort_movies([], page, page_size)
            else:
                return await db.sort_tv_shows([], page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_media_api(
    tmdb_id: int,
    db_index: int,
    media_type: str = Query(regex="^(movie|tv)$")
):
    try:
        media_type_formatted = "Movie" if media_type == "movie" else "Series"
        result = await db.delete_document(media_type_formatted, tmdb_id, db_index)
        if result:
            return {"message": "Media deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Media not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def update_media_api(
    request: Request,
    tmdb_id: int,
    db_index: int,
    media_type: str = Query(regex="^(movie|tv)$")
):
    try:
        update_data = await request.json()
        if 'rating' in update_data and update_data['rating']:
            try:
                update_data['rating'] = float(update_data['rating'])
            except (ValueError, TypeError):
                update_data['rating'] = 0.0
        
        if 'release_year' in update_data and update_data['release_year']:
            try:
                update_data['release_year'] = int(update_data['release_year'])
            except (ValueError, TypeError):
                pass
        if 'genres' in update_data:
            if isinstance(update_data['genres'], str):
                update_data['genres'] = [g.strip() for g in update_data['genres'].split(',') if g.strip()]
            elif not isinstance(update_data['genres'], list):
                update_data['genres'] = []
        
        if 'languages' in update_data:
            if isinstance(update_data['languages'], str):
                update_data['languages'] = [l.strip() for l in update_data['languages'].split(',') if l.strip()]
            elif not isinstance(update_data['languages'], list):
                update_data['languages'] = []
        if media_type == "movie":
            if 'runtime' in update_data and update_data['runtime']:
                try:
                    update_data['runtime'] = int(update_data['runtime'])
                except (ValueError, TypeError):
                    pass
        elif media_type == "tv":
            if 'total_seasons' in update_data and update_data['total_seasons']:
                try:
                    update_data['total_seasons'] = int(update_data['total_seasons'])
                except (ValueError, TypeError):
                    pass
            
            if 'total_episodes' in update_data and update_data['total_episodes']:
                try:
                    update_data['total_episodes'] = int(update_data['total_episodes'])
                except (ValueError, TypeError):
                    pass
        update_data = {k: v for k, v in update_data.items() if v != ""}
        result = await db.update_document(media_type, tmdb_id, db_index, update_data)
        if result:
            return {"message": "Media updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Media not found or no changes made")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_media_details_api(
    tmdb_id: int,
    db_index: int,
    media_type: str = Query(regex="^(movie|tv)$")
):
    try:
        result = await db.get_document(media_type, tmdb_id, db_index)
        if result:
            return result
        else:
            raise HTTPException(status_code=404, detail="Media not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_movie_quality_api(tmdb_id: int, db_index: int, id: str):
    try:
        result = await db.delete_movie_quality(tmdb_id, db_index, id)
        if result:
            return {"message": "Quality deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Quality not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_tv_quality_api(
    tmdb_id: int, db_index: int, season: int, episode: int, id: str
):
    try:
        result = await db.delete_tv_quality(tmdb_id, db_index, season, episode, id)
        if result:
            return {"message": "deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Quality not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_tv_episode_api(
    tmdb_id: int, db_index: int, season: int, episode: int
):
    try:
        result = await db.delete_tv_episode(tmdb_id, db_index, season, episode)
        if result:
            return {"message": "Episode deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Episode not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_tv_season_api(tmdb_id: int, db_index: int, season: int):
    try:
        result = await db.delete_tv_season(tmdb_id, db_index, season)
        if result:
            return {"message": "Season deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Season not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- API Routes for Token Management ---

async def create_token_api(payload: dict):
    try:
        token_name = payload.get("name")
        daily_limit = payload.get("daily_limit_gb")
        monthly_limit = payload.get("monthly_limit_gb")
        
        if not token_name:
             raise HTTPException(status_code=400, detail="Token name is required")
        def parse_limit(val):
            try:
                v = float(val)
                return v if v > 0 else None
            except (ValueError, TypeError):
                return None

        user_id = payload.get("user_id")
        if user_id not in (None, "", 0, "0"):
            try:
                user_id = int(user_id)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Invalid Telegram user id.")
            existing = await db.get_api_token_by_user(user_id)
            if existing:
                raise HTTPException(status_code=409, detail="That Telegram user already has a token.")
        else:
            user_id = None

        new_token = await db.add_api_token(
            token_name,
            parse_limit(daily_limit),
            parse_limit(monthly_limit),
            user_id=user_id,
            subscription_exempt=bool(payload.get("subscription_exempt", False)),
        )
        return new_token
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def set_token_lifetime_api(token: str, payload: dict) -> dict:
    lifetime = bool(payload.get("subscription_exempt"))
    if not await db.set_token_lifetime(token, lifetime):
        raise HTTPException(status_code=404, detail="Token not found.")
    return {"status": "success", "subscription_exempt": lifetime}


async def set_token_expiry_api(token: str, payload: dict) -> dict:
    action = str(payload.get("action") or "set").lower()
    if action not in {"set", "extend", "reduce"}:
        raise HTTPException(status_code=400, detail="action must be set, extend, or reduce.")
    try:
        days = int(payload.get("days") or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="days must be a number.")
    if days < 0 or (action in {"extend", "reduce"} and days < 1):
        raise HTTPException(status_code=400, detail="A positive day count is required.")

    user_id = payload.get("user_id")
    if user_id not in (None, "", 0, "0"):
        await link_token_user_api(token, int(user_id))
    result = await db.update_token_expiry(token, action, days)
    if not result:
        raise HTTPException(status_code=404, detail="Token not found or expiry update is invalid.")
    expiry = result.get("expires_at")
    return {
        "status": "success",
        "expires_at": expiry.isoformat() if hasattr(expiry, "isoformat") else expiry,
        "subscription_exempt": bool(result.get("subscription_exempt")),
    }


async def subscription_preflight_api() -> dict:
    result = await db.count_uncovered_tokens()
    return {"status": "success", **result}


async def grant_lifetime_api() -> dict:
    count = await db.grant_lifetime_to_unlinked()
    return {
        "status": "success",
        "updated": count,
        "message": f"{count} unlinked token(s) marked as lifetime.",
    }


async def _telegram_display_name(user_id: int) -> str | None:
    try:
        user = await StreamBot.get_users(int(user_id))
        name = " ".join(part for part in (user.first_name, user.last_name) if part).strip()
        return name or (f"@{user.username}" if user.username else None)
    except Exception as e:
        LOGGER.debug(f"Could not backfill Telegram name for {user_id}: {e}")
        return None


async def backfill_token_names_api() -> dict:
    updated = 0
    for token_doc in await db.get_all_api_tokens():
        user_id = token_doc.get("user_id")
        current_name = str(token_doc.get("name") or "")
        if not user_id or (current_name and current_name != f"User {user_id}" and not current_name.startswith("Token ")):
            continue
        name = await _telegram_display_name(int(user_id))
        if name and await db.update_token_name(str(token_doc.get("token")), name):
            updated += 1
    return {"status": "success", "updated": updated, "message": f"{updated} token name(s) updated."}

async def update_token_limits_api(token: str, payload: dict):
    try:
        daily_limit = payload.get("daily_limit_gb")
        monthly_limit = payload.get("monthly_limit_gb")
        max_active_streams = payload.get("max_active_streams")
        
        def parse_limit(val):
            try:
                v = float(val)
                return v if v > 0 else None
            except (ValueError, TypeError, AttributeError):
                return None

        result = await db.update_api_token_limits(
            token,
            parse_limit(daily_limit),
            parse_limit(monthly_limit),
            max_active_streams=max_active_streams,
        )
        
        if result:
            return {"message": "Limits updated successfully"}
        else:
            return {"message": "Limits updated successfully"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def revoke_token_api(token: str):
    try:
        result = await db.revoke_api_token(token)
        if result:
            return {"message": "Token revoked successfully"}
        else:
            raise HTTPException(status_code=404, detail="Token not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Speed Test API ---

async def speed_test_api(
    quality_id: str = Query(..., description="Encoded quality ID from DB"),
    tmdb_id: int = Query(...),
    db_index: int = Query(...),
    media_type: str = Query(..., regex="^(movie|tv)$"),
):
    """
    Decode quality_id using the same decode_string logic as the stream handler,
    then run a parallel download speed test across all connected bot clients.
    """
    from Backend.helper.encrypt import decode_string

    try:
        decoded = await decode_string(quality_id)
        msg_id  = decoded.get("msg_id")
        raw_cid = decoded.get("chat_id")

        if not msg_id or not raw_cid:
            raise HTTPException(
                status_code=422,
                detail=f"Decoded quality data is missing msg_id or chat_id. Decoded: {decoded}"
            )

        # Stream handler adds -100 prefix for channel IDs
        chat_id = int(f"-100{raw_cid}")

        results = await run_speed_test(int(chat_id), int(msg_id))
        return {"results": results, "total_clients_tested": len(results)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Speed Test SSE Streaming API ---

async def speed_test_stream_api(
    quality_id: str,
    tmdb_id: int,
    db_index: int,
    media_type: str,
):
    """
    SSE version of the speed test. Streams each per-client result as a
    'data:' event the moment that client finishes, so the UI can update live.
    """
    from Backend.helper.encrypt import decode_string

    async def event_generator():
        # Decode quality_id → chat_id + message_id
        try:
            decoded = await decode_string(quality_id)
            msg_id  = decoded.get("msg_id")
            raw_cid = decoded.get("chat_id")
            if not msg_id or not raw_cid:
                payload = json.dumps({"type": "error", "message": f"Cannot decode quality_id. Got: {decoded}"})
                yield f"data: {payload}\n\n"
                return
            chat_id = int(f"-100{raw_cid}")
        except Exception as exc:
            payload = json.dumps({"type": "error", "message": str(exc)})
            yield f"data: {payload}\n\n"
            return

        total = len(multi_clients)
        if total == 0:
            payload = json.dumps({"type": "error", "message": "No bot clients connected"})
            yield f"data: {payload}\n\n"
            return
            
        # Try to resolve the FileId to get the target DC
        target_dc = "?"
        try:
            from Backend.helper.custom_dl import ByteStreamer
            primary_client = multi_clients.get(0) or next(iter(multi_clients.values()))
            streamer = ByteStreamer(primary_client)
            file_id = await streamer.get_file_properties(chat_id, int(msg_id))
            target_dc = file_id.dc_id
        except Exception:
            pass

        # Send initial "start" event so the frontend can set up the table
        yield f"data: {json.dumps({'type': 'start', 'total': total, 'target_dc': target_dc})}\n\n"

        # Run all clients in parallel; feed results into a queue as they finish
        queue: asyncio.Queue = asyncio.Queue()

        async def run_one(client, idx):
            async def on_progress(prog_data):
                await queue.put({"type": "progress", "data": prog_data})
                
            result = await _speed_test_single_client(
                client, idx, chat_id, int(msg_id), progress_callback=on_progress
            )
            await queue.put({"type": "result", "data": result})

        tasks = [
            asyncio.create_task(run_one(client, idx))
            for idx, client in multi_clients.items()
        ]

        completed = 0
        while completed < total:
            msg = await queue.get()
            
            if msg["type"] == "progress":
                payload = json.dumps(msg)
                yield f"data: {payload}\n\n"
            
            elif msg["type"] == "result":
                completed += 1
                payload = json.dumps({
                    "type": "result",
                    "data": msg["data"],
                    "completed": completed,
                    "total": total,
                })
                yield f"data: {payload}\n\n"

        # Wait for any remaining tasks (should already be done)
        await asyncio.gather(*tasks, return_exceptions=True)

        # Final done event
        yield f"data: {json.dumps({'type': 'done', 'total': total})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # prevent nginx from buffering SSE
        },
    )

# ---------------------------------------------------------------------------
# Admin API Routes
# ---------------------------------------------------------------------------

async def get_admin_stats_api() -> dict:
    from Backend.pyrofork.bot import work_loads, multi_clients, client_failures, client_avg_mbps
    from Backend.helper.custom_dl import get_client_cooldown_state
    from Backend.fastapi.routes.stream_routes import _streamer_by_client
    
    # Sum cache entries across all active ByteStreamer instances
    cache_size = sum(len(s._file_id_cache) for s in _streamer_by_client.values())
    
    # Calculate bot workloads and health
    bot_stats = []
    cooldowns = get_client_cooldown_state()
    for client_index in multi_clients:
        load = work_loads.get(client_index, 0)
        failures = client_failures.get(client_index, 0)
        mbps = client_avg_mbps.get(client_index, 0.0)
        cooldown = cooldowns.get(str(client_index), {})
        
        status = "healthy"
        if cooldown.get("global_sec", 0) or cooldown.get("dc"):
            status = "cooldown"
        elif failures > 5:
            status = "degraded"
        if failures > 15:
            status = "failing"
            
        bot_stats.append({
            "client_index": client_index,
            "display_name": f"Bot {client_index + 1}",
            "current_load": load,
            "failures": failures,
            "avg_mbps": round(mbps, 2),
            "cooldown": cooldown,
            "status": status
        })
        
    return {
        "cache_size": cache_size,
        "total_bots": len(multi_clients),
        "bot_workloads": bot_stats
    }

async def clear_cache_api() -> dict:
    from Backend.fastapi.routes.stream_routes import _streamer_by_client
    from Backend.logger import LOGGER
    
    # Clear cache across all active ByteStreamer instances
    total_cleared = sum(len(s._file_id_cache) for s in _streamer_by_client.values())
    for streamer in _streamer_by_client.values():
        streamer._file_id_cache.clear()
    LOGGER.info(f"Admin cleared the FileId cache ({total_cleared} items purged across {len(_streamer_by_client)} clients).")
    
    return {"status": "success", "message": f"{total_cleared} cached items cleared."}

async def get_dead_links_api() -> dict:
    from Backend import db
    try:
        dead_links = await db.get_all_dead_links()
        return {"status": "success", "data": dead_links}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def get_stream_analytics_api() -> dict:
    from Backend import db
    try:
        data = await db.get_stream_analytics(limit=200)
        return {"status": "success", "data": data}
    except Exception as e:
        from Backend.logger import LOGGER
        LOGGER.error(f"Stream analytics API error: {e}")
        return {"status": "error", "message": str(e)}

async def clear_stream_analytics_api() -> dict:
    try:
        result = await db.dbs["tracking"]["stream_analytics"].delete_many({})
        LOGGER.info(f"Admin cleared stream analytics ({result.deleted_count} records deleted).")

        return {
            "status": "success",
            "message": f"{result.deleted_count} analytics records cleared."
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


async def get_user_activity_api(page: int = 1, per_page: int = 12):
    try:
        return await get_activity_overview(page, per_page)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Duplicate / Quality Flag API
# ---------------------------------------------------------------------------

async def get_duplicate_media_api() -> dict:
    groups = await db.get_duplicate_quality_groups()
    return {"status": "success", "data": groups}


async def update_quality_flags_api(payload: dict) -> dict:
    flags = payload.get("flags") or {
        key: payload[key]
        for key in ("hidden_from_stremio", "recommended", "quality_note", "flagged_duplicate")
        if key in payload
    }
    ok = await db.update_quality_flags(
        media_type=payload.get("media_type"),
        tmdb_id=int(payload.get("tmdb_id")),
        db_index=int(payload.get("db_index")),
        quality_id=payload.get("id") or payload.get("quality_id"),
        flags=flags,
        season=payload.get("season"),
        episode=payload.get("episode"),
        clear=False,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Quality not found")
    return {"status": "success", "message": "Quality flags updated"}


async def clear_quality_flags_api(payload: dict) -> dict:
    ok = await db.update_quality_flags(
        media_type=payload.get("media_type"),
        tmdb_id=int(payload.get("tmdb_id")),
        db_index=int(payload.get("db_index")),
        quality_id=payload.get("id") or payload.get("quality_id"),
        flags={},
        season=payload.get("season"),
        episode=payload.get("episode"),
        clear=True,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Quality not found")
    return {"status": "success", "message": "Quality flags cleared"}

# ---------------------------------------------------------------------------
# Admin Subscription Management API Routes
# ---------------------------------------------------------------------------

async def get_subscription_plans_api() -> dict:
    from Backend import db
    try:
        plans = await db.get_subscription_plans()
        return {"status": "success", "data": plans}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def add_subscription_plan_api(payload: dict) -> dict:
    from Backend import db
    try:
        days = int(payload.get("days", 0))
        price = float(payload.get("price", 0.0))
        if days <= 0 or price < 0:
            raise HTTPException(status_code=400, detail="Invalid plan parameters")
            
        plan_id = await db.add_subscription_plan(days, price)
        if plan_id:
            return {"status": "success", "message": "Plan added successfully", "plan_id": plan_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to add plan")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def update_subscription_plan_api(plan_id: str, payload: dict) -> dict:
    from Backend import db
    try:
        days = int(payload.get("days", 0))
        price = float(payload.get("price", 0.0))
        if days <= 0 or price < 0:
             raise HTTPException(status_code=400, detail="Invalid plan parameters")
             
        success = await db.update_subscription_plan(plan_id, days, price)
        if success:
             return {"status": "success", "message": "Plan updated successfully"}
        else:
             raise HTTPException(status_code=404, detail="Plan not found or update failed")
    except HTTPException:
         raise
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

async def delete_subscription_plan_api(plan_id: str) -> dict:
    from Backend import db
    try:
        success = await db.delete_subscription_plan(plan_id)
        if success:
            return {"status": "success", "message": "Plan deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Plan not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_all_subscribers_api() -> dict:
    from Backend import db
    try:
        users = await db.get_all_subscribers()
        return {"status": "success", "data": users}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def manage_subscriber_api(user_id: int, payload: dict) -> dict:
    from Backend import db
    try:
        action = payload.get("action")
        days = int(payload.get("days", 0))
        
        if action not in ["extend", "reduce", "delete"]:
            raise HTTPException(status_code=400, detail="Invalid action")
            
        success = await db.manage_subscriber(user_id, action, days)
        if success:
            return {"status": "success", "message": "User subscription updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found or update failed")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Access Management API ---

async def get_all_tokens_api() -> dict:
    try:
        tokens = await db.get_all_api_tokens()
        now = datetime.utcnow()
        result, subscriber_map = [], {}
        if Telegram.SUBSCRIPTION:
            try:
                subscriber_map = {
                    str(user.get("_id")): user
                    for user in await db.get_all_subscribers()
                }
            except Exception:
                pass

        def display_name(user, user_id, token_name=None) -> str:
            if user:
                name = user.get("first_name") or user.get("username")
                if name:
                    return name
            if token_name:
                return token_name
            return f"User {user_id}" if user_id else "Telegram User"

        def build_entry(user_id, user, token_doc):
            token_str = token_doc.get("token") if token_doc else None
            created = token_doc.get("created_at") if token_doc else (user.get("created_at") if user else None)
            token_expiry = token_doc.get("expires_at") if token_doc else None
            subscription_expiry = user.get("subscription_expiry") if user else None
            sub_status = user.get("subscription_status") if user else None
            lifetime = bool(token_doc and token_doc.get("subscription_exempt"))
            admin = bool(token_doc and token_doc.get("is_admin"))
            beta_exempt = bool(token_doc and is_exempt_token(token_doc))

            if beta_exempt:
                is_expired, access_source = False, "internal_exemption"
            elif admin:
                is_expired, access_source = False, "admin"
            elif lifetime:
                is_expired, access_source = False, "lifetime"
            elif token_expiry is not None:
                is_expired, access_source = token_expiry <= now, "token_expiry"
            elif Telegram.SUBSCRIPTION:
                is_expired = not (
                    user
                    and sub_status == "active"
                    and subscription_expiry
                    and subscription_expiry > now
                )
                access_source = "subscription"
            else:
                is_expired, access_source = False, "open_mode"

            if not token_str:
                is_expired, access_source = True, "no_token"
            expiry = token_expiry if token_expiry is not None else subscription_expiry

            return {
                "token": token_str,
                "user_id": user_id,
                "user_name": display_name(user, user_id, token_doc.get("name") if token_doc else None),
                "user_found": bool(user),
                "has_token": bool(token_str),
                "created_at": created.isoformat() if hasattr(created, "isoformat") else created,
                "expires_at": expiry.isoformat() if hasattr(expiry, "isoformat") else expiry,
                "is_expired": is_expired,
                "sub_status": sub_status,
                "lifetime": lifetime,
                "subscription_exempt": lifetime,
                "is_admin": admin,
                "is_beta_exempt": beta_exempt,
                "access_source": access_source,
                "limits": (token_doc or {}).get("limits") or {},
                "addon_url": (
                    f"{Telegram.BASE_URL}/stremio/{token_str}/manifest.json"
                    if token_str else (f"{Telegram.BASE_URL}/stremio/{Telegram.DEFAULT_ADDON_TOKEN}/manifest.json" if Telegram.DEFAULT_ADDON_TOKEN else None)
                ),
            }

        seen_user_ids = set()
        for t in tokens:
            token_user_id = t.get("user_id")
            user = None
            if token_user_id:
                uid_str = str(token_user_id)
                user = subscriber_map.get(uid_str)
                if not user:
                    try:
                        user = await db.get_user(int(token_user_id))
                    except Exception:
                        pass
                seen_user_ids.add(uid_str)

            result.append(build_entry(token_user_id, user, t))

        for uid_str, u in subscriber_map.items():
            if uid_str in seen_user_ids:
                continue
            result.append(build_entry(u.get("_id"), u, None))

        result.sort(key=lambda x: (x["is_expired"], not x["has_token"]))
        return {"tokens": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def revoke_token_api(token: str) -> dict:
    from Backend import db
    try:
        success = await db.revoke_api_token(token)
        if success:
            return {"status": "success", "message": "Token revoked."}
        raise HTTPException(status_code=404, detail="Token not found.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def assign_plan_api(user_id: int, days: int) -> dict:
    """Assign (or extend) a subscription for any user by user_id, even if not in DB."""
    from Backend import db
    try:
        if days < 1:
            raise HTTPException(status_code=400, detail="Days must be at least 1.")
        result = await db.assign_subscription(user_id, days)
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def link_token_user_api(token: str, user_id: int) -> dict:
    """Link an orphan token (no user_id) to a Telegram user_id."""
    from Backend import db
    try:
        success = await db.link_token_user(token, user_id)
        if success:
            return {"status": "success", "message": f"Token linked to user {user_id}."}
        raise HTTPException(status_code=409, detail="Token was not found, or that user already has another token.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Metadata Rescan API
# ---------------------------------------------------------------------------

async def search_media_rescan_api(media_type: str, query: str, year: int | None = None):
    query = (query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required.")

    if media_type == "movie":
        results = await search_movie_candidates(query=query, year=year)
    elif media_type == "tv":
        results = await search_tv_candidates(query=query)
    else:
        raise HTTPException(status_code=400, detail="Invalid media_type.")

    return {"results": results}


async def apply_media_rescan_api(request: Request, tmdb_id: int, db_index: int, media_type: str):
    body = await request.json()
    selected_id = str(body.get("selected_id") or "").strip()

    if not selected_id:
        raise HTTPException(status_code=400, detail="selected_id is required.")

    current_doc = await db.get_document(media_type, tmdb_id, db_index)
    if not current_doc:
        raise HTTPException(status_code=404, detail="Media not found.")

    if media_type == "movie":
        metadata = await fetch_selected_movie_metadata(selected_id)
    elif media_type == "tv":
        metadata = await fetch_selected_tv_metadata(selected_id)
    else:
        raise HTTPException(status_code=400, detail="Invalid media_type.")

    if not metadata:
        raise HTTPException(status_code=404, detail="Unable to fetch metadata for selected item.")

    updated_doc = await db.replace_media_metadata(
        media_type=media_type,
        tmdb_id=tmdb_id,
        db_index=db_index,
        metadata=metadata,
    )

    if not updated_doc:
        raise HTTPException(status_code=500, detail="Failed to replace media metadata.")

    return {
        "success": True,
        "message": "Metadata rescanned successfully.",
        "redirect_tmdb_id": updated_doc.get("tmdb_id"),
        "db_index": updated_doc.get("db_index", db_index),
        "media_type": media_type,
        "data": updated_doc,
    }


# ---------------------------------------------------------------------------
# Manual Add API
# ---------------------------------------------------------------------------

def _scan_client():
    if StreamBot is not None:
        return StreamBot
    if multi_clients:
        return multi_clients.get(0) or next(iter(multi_clients.values()))
    return None


async def resolve_telegram_api(payload: dict) -> dict:
    client = _scan_client()
    if client is None:
        raise HTTPException(status_code=503, detail="No Telegram client is connected yet.")
    try:
        data = await resolve_telegram_message(
            client,
            url=payload.get("url"),
            chat_id=payload.get("chat_id"),
            msg_id=payload.get("msg_id"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read that message: {exc}")
    return {"status": "success", "data": data}


async def search_manual_add_metadata_api(media_type: str, query: str, year: int | None = None) -> dict:
    query = (query or "").strip()
    if not query:
        return {"results": []}
    if media_type == "movie":
        return {"results": await search_movie_candidates(query=query, year=year)}
    if media_type == "tv":
        return {"results": await search_tv_candidates(query=query)}
    raise HTTPException(status_code=400, detail="media_type must be movie or tv.")


async def resolve_manual_add_metadata_api(media_type: str, selected_id: str) -> dict:
    selected_id = str(selected_id or "").strip()
    if not selected_id:
        raise HTTPException(status_code=400, detail="selected_id is required.")
    if media_type == "movie":
        result = await fetch_selected_movie_metadata(selected_id)
    elif media_type == "tv":
        result = await fetch_selected_tv_metadata(selected_id)
    else:
        raise HTTPException(status_code=400, detail="media_type must be movie or tv.")
    if not result:
        raise HTTPException(status_code=404, detail="Metadata could not be loaded for that result.")
    return {"metadata": result}


async def get_manual_add_catalogs_api() -> dict:
    catalogs = await db.get_custom_catalogs()
    return {
        "catalogs": [
            {
                "id": catalog.get("_id"),
                "name": catalog.get("name") or "Untitled",
                "visibility": catalog.get("visibility") or "public",
                "exclusive": bool(catalog.get("exclusive")),
                "searchable": bool(catalog.get("searchable")),
            }
            for catalog in catalogs
            if not catalog.get("auto")
        ]
    }


def _metadata_base(source: dict, from_doc: bool = False) -> dict:
    genres = source.get("genres")
    if isinstance(genres, str):
        genres = [g.strip() for g in genres.split(",") if g.strip()]
    year = source.get("release_year") if from_doc else source.get("year")
    rate = source.get("rating") if from_doc else source.get("rate")
    return {
        "tmdb_id": source.get("tmdb_id"),
        "imdb_id": source.get("imdb_id") or None,
        "title": (source.get("title") or "").strip(),
        "year": int(year) if str(year or "").strip().lstrip("-").isdigit() else 0,
        "rate": float(rate) if str(rate or "").replace(".", "", 1).isdigit() else 0,
        "description": source.get("description") or "",
        "poster": source.get("poster") or "",
        "backdrop": source.get("backdrop") or "",
        "logo": source.get("logo") or "",
        "genres": genres or [],
        "cast": source.get("cast") or [],
        "runtime": str(source.get("runtime") or ""),
    }


_PLACEHOLDER_GENRES = ["Action", "Adventure", "Comedy", "Drama", "Thriller", "Mystery"]
_PLACEHOLDER_DESCRIPTIONS = [
    "A manually added title from a Telegram source.",
    "A Telegram-hosted stream added from the admin panel.",
    "A manually indexed media entry.",
]


def _fill_placeholder_metadata(meta: dict) -> None:
    if not meta.get("genres"):
        meta["genres"] = random.sample(_PLACEHOLDER_GENRES, 2)
    if not meta.get("rate"):
        meta["rate"] = round(random.uniform(6.0, 8.5), 1)
    if not meta.get("description"):
        meta["description"] = random.choice(_PLACEHOLDER_DESCRIPTIONS)


async def _find_media_db_index(media_type: str, tmdb_id: int) -> int:
    collection_name = "movie" if media_type == "movie" else "tv"
    try:
        tmdb_id = int(tmdb_id)
    except Exception:
        return db.current_db_index
    for db_key in sorted(key for key in db.dbs if key.startswith("storage_")):
        doc = await db.dbs[db_key][collection_name].find_one({"tmdb_id": tmdb_id}, {"_id": 1})
        if doc:
            try:
                return int(db_key.split("_", 1)[1])
            except Exception:
                return db.current_db_index
    return db.current_db_index


async def manual_add_media_api(payload: dict) -> dict:
    media_type = payload.get("media_type")
    if media_type not in ("movie", "tv"):
        raise HTTPException(status_code=400, detail="media_type must be 'movie' or 'tv'.")

    raw_catalog_ids = payload.get("catalog_ids") or []
    if isinstance(raw_catalog_ids, str):
        raw_catalog_ids = [item.strip() for item in raw_catalog_ids.split(",")]
    catalog_ids = list(dict.fromkeys(str(item).strip() for item in raw_catalog_ids if str(item).strip()))
    selected_catalogs = []
    for catalog_id in catalog_ids:
        catalog = await db.get_custom_catalog(catalog_id)
        if not catalog:
            raise HTTPException(status_code=400, detail=f"Catalog {catalog_id} was not found.")
        selected_catalogs.append(catalog)
    exclusive_catalogs = [catalog for catalog in selected_catalogs if catalog.get("exclusive")]
    if exclusive_catalogs and len(selected_catalogs) > 1:
        raise HTTPException(status_code=400, detail="An exclusive catalog must be selected by itself.")

    stream = payload.get("stream") or {}
    quality = str(stream.get("quality") or "").strip()
    if not quality:
        raise HTTPException(status_code=400, detail="A quality label like 1080p is required.")

    part_sources = stream.get("parts")
    if not isinstance(part_sources, list) or not part_sources:
        part_sources = [{"url": stream.get("url"), "chat_id": stream.get("chat_id"), "msg_id": stream.get("msg_id")}]
    part_sources = [p for p in part_sources if p and (p.get("url") or (p.get("chat_id") and p.get("msg_id")))]
    if not part_sources:
        raise HTTPException(status_code=400, detail="Provide at least one Telegram message link or chat/message id.")

    client = _scan_client()
    if client is None:
        raise HTTPException(status_code=503, detail="No Telegram client is connected yet.")

    resolved_parts = []
    for src in part_sources:
        try:
            resolved_parts.append(
                await resolve_telegram_message(
                    client,
                    url=src.get("url"),
                    chat_id=src.get("chat_id"),
                    msg_id=src.get("msg_id"),
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Could not read that message: {exc}")

    primary = resolved_parts[0]
    is_split = len(resolved_parts) > 1
    raw_name = (stream.get("name") or primary["name"]).strip()
    name = strip_part_suffix(raw_name) if is_split else raw_name

    tmdb_id = payload.get("tmdb_id")
    db_index = payload.get("db_index")
    selected_id = str(payload.get("selected_id") or "").strip()

    base = None
    if tmdb_id and db_index:
        doc = await db.get_document(media_type, int(tmdb_id), int(db_index))
        if doc:
            base = _metadata_base(doc, from_doc=True)
    if base is None and selected_id:
        selected = await (
            fetch_selected_movie_metadata(selected_id)
            if media_type == "movie"
            else fetch_selected_tv_metadata(selected_id)
        )
        if not selected:
            raise HTTPException(status_code=404, detail="Could not fetch metadata for the selected title.")
        base = _metadata_base(selected, from_doc=True)
    if base is None:
        base = _metadata_base(payload.get("manual_metadata") or {})
        if not base["title"]:
            raise HTTPException(status_code=400, detail="A title is required for manual entry.")
        if not base["year"]:
            base["year"] = int(primary.get("upload_year") or 0)

    if not base.get("tmdb_id"):
        base["tmdb_id"] = -(secrets.randbelow(2_000_000_000) + 1)
    if not base.get("imdb_id"):
        base["imdb_id"] = f"tg{abs(int(base['tmdb_id']))}"
    _fill_placeholder_metadata(base)

    group_key = f"manual:{primary['chat_id']}:{quality}:{secrets.token_hex(6)}" if is_split else None

    tv_extra = {}
    if media_type == "tv":
        try:
            season_number = int(payload.get("season_number"))
            episode_number = int(payload.get("episode_number"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Season and episode numbers are required for TV.")
        tv_extra = {
            "season_number": season_number,
            "episode_number": episode_number,
            "episode_title": (payload.get("episode_title") or "").strip() or f"S{season_number:02d}E{episode_number:02d}",
            "episode_backdrop": payload.get("episode_backdrop") or base.get("backdrop") or "",
            "episode_overview": payload.get("episode_overview") or "",
            "episode_released": payload.get("episode_released") or "",
        }

    for index, part in enumerate(resolved_parts, start=1):
        channel = int(part["chat_id"])
        msg_id = int(part["msg_id"])
        encoded = await encode_string({"chat_id": channel, "msg_id": msg_id})
        metadata_info = dict(base)
        metadata_info.update(
            {
                "media_type": media_type,
                "quality": quality,
                "encoded_string": encoded,
                "group_key": group_key,
                "part_number": index if is_split else None,
                "is_anime": False,
            }
        )
        metadata_info.update(tv_extra)
        updated_id = await db.insert_media(
            metadata_info,
            channel=channel,
            msg_id=msg_id,
            size=part["size"],
            name=name,
            raw_size=int(part.get("raw_size") or 0),
        )
        if not updated_id:
            raise HTTPException(status_code=500, detail="Failed to add media.")

    result_tmdb_id = int(base["tmdb_id"])
    result_db_index = await _find_media_db_index(media_type, result_tmdb_id)
    added_catalogs = []
    for catalog in selected_catalogs:
        catalog_id = str(catalog.get("_id"))
        if await db.add_item_to_custom_catalog(catalog_id, result_tmdb_id, result_db_index, media_type):
            added_catalogs.append(catalog_id)
    return {
        "status": "success",
        "message": f"Split stream added ({len(resolved_parts)} parts)." if is_split else "Stream added successfully.",
        "tmdb_id": result_tmdb_id,
        "db_index": result_db_index,
        "media_type": media_type,
        "catalog_ids": added_catalogs,
    }


# ---------------------------------------------------------------------------
# Custom Catalog API
# ---------------------------------------------------------------------------

def _normalize_media_type(media_type: str) -> str:
    return "tv" if media_type in ["tv", "series"] else "movie"


_VISIBILITY_MODES = {"public", "tokens", "owner"}


def _clean_visibility(payload: dict) -> tuple[str | None, list[str]]:
    visibility = (payload or {}).get("visibility")
    if visibility not in _VISIBILITY_MODES:
        visibility = None
    raw_tokens = (payload or {}).get("allowed_tokens") or []
    if isinstance(raw_tokens, str):
        raw_tokens = [item.strip() for item in raw_tokens.replace("\n", ",").split(",")]
    tokens = [str(item).strip() for item in raw_tokens if str(item).strip()]
    return visibility, tokens


async def list_custom_catalogs_api(
    tmdb_id: int | None = None,
    db_index: int | None = None,
    media_type: str | None = None,
):
    try:
        catalogs = await db.get_custom_catalogs()
        if tmdb_id is not None and db_index is not None and media_type:
            normalized_type = _normalize_media_type(media_type)
            for catalog in catalogs:
                catalog["contains_current"] = any(
                    int(item.get("tmdb_id", -1)) == int(tmdb_id)
                    and int(item.get("db_index", -1)) == int(db_index)
                    and item.get("media_type") == normalized_type
                    for item in catalog.get("items", []) or []
                )
        return {"catalogs": catalogs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_media_visibility_api(tmdb_id: int, db_index: int, media_type: str):
    visibility = await db.get_media_visibility(
        int(tmdb_id), int(db_index), _normalize_media_type(media_type)
    )
    if not visibility:
        raise HTTPException(status_code=404, detail="Media not found.")
    tokens = await db.get_all_api_tokens()
    visibility["available_tokens"] = [
        {
            "token": item.get("token"),
            "name": item.get("name") or item.get("telegram_first_name") or item.get("telegram_username") or "Unnamed token",
            "revoked": bool(item.get("revoked", False)),
        }
        for item in tokens
        if item.get("token") and not item.get("revoked", False)
    ]
    return visibility


async def set_media_visibility_api(payload: dict):
    tmdb_id = payload.get("tmdb_id")
    db_index = payload.get("db_index")
    media_type = _normalize_media_type(payload.get("media_type", "movie"))
    visibility, allowed_tokens = _clean_visibility(payload)
    if not tmdb_id or not db_index:
        raise HTTPException(status_code=400, detail="tmdb_id and db_index are required.")
    if visibility not in _VISIBILITY_MODES:
        raise HTTPException(status_code=400, detail="visibility must be public, tokens, or owner.")
    if visibility == "tokens" and not allowed_tokens:
        raise HTTPException(status_code=400, detail="Select at least one access token.")

    known_tokens = {
        str(item.get("token"))
        for item in await db.get_all_api_tokens()
        if item.get("token") and not item.get("revoked", False)
    }
    unknown_tokens = [token for token in allowed_tokens if token not in known_tokens]
    if unknown_tokens:
        raise HTTPException(status_code=400, detail="One or more selected access tokens are invalid or revoked.")

    result = await db.set_media_visibility(
        int(tmdb_id), int(db_index), media_type, visibility, allowed_tokens
    )
    if not result:
        raise HTTPException(status_code=404, detail="Media not found.")
    return {"message": "Title visibility updated.", "visibility": result}


async def create_custom_catalog_api(payload: dict):
    name = (payload.get("name") or "").strip()
    visible = bool(payload.get("visible", True))
    visibility, allowed_tokens = _clean_visibility(payload)
    if not name:
        raise HTTPException(status_code=400, detail="Catalog name is required.")

    catalog_id = await db.create_custom_catalog(
        name=name,
        visible=visible,
        visibility=visibility,
        allowed_tokens=allowed_tokens,
        exclusive=bool(payload.get("exclusive", False)),
        searchable=bool(payload.get("searchable", False)),
    )
    if not catalog_id:
        raise HTTPException(status_code=500, detail="Failed to create catalog.")

    catalog = await db.get_custom_catalog(catalog_id)
    return {"message": "Catalog created successfully.", "catalog": catalog}


async def update_custom_catalog_api(catalog_id: str, payload: dict):
    name = payload.get("name")
    visible = payload.get("visible") if "visible" in payload else None
    visibility, allowed_tokens = _clean_visibility(payload)
    result = await db.update_custom_catalog(
        catalog_id,
        name=name,
        visible=visible,
        visibility=visibility,
        allowed_tokens=allowed_tokens if "allowed_tokens" in payload else None,
        exclusive=payload.get("exclusive") if "exclusive" in payload else None,
        searchable=payload.get("searchable") if "searchable" in payload else None,
    )
    if not result:
        catalog = await db.get_custom_catalog(catalog_id)
        if not catalog:
            raise HTTPException(status_code=404, detail="Catalog not found.")
    return {"message": "Catalog updated successfully.", "catalog": await db.get_custom_catalog(catalog_id)}


async def delete_custom_catalog_api(catalog_id: str):
    result = await db.delete_custom_catalog(catalog_id)
    if not result:
        raise HTTPException(status_code=404, detail="Catalog not found.")
    return {"message": "Catalog deleted successfully."}


async def get_custom_catalog_items_api(
    catalog_id: str,
    media_type: str | None = None,
    page: int = 1,
    page_size: int = 24,
):
    try:
        data = await db.get_custom_catalog_items(catalog_id, media_type, page, page_size)
        if not data.get("catalog"):
            raise HTTPException(status_code=404, detail="Catalog not found.")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def search_catalog_media_api(
    query: str,
    media_type: str = "movie",
    page: int = 1,
    page_size: int = 12,
):
    query = (query or "").strip()
    if not query:
        return {"results": [], "total_count": 0}

    try:
        result = await db.search_documents(query, page, page_size)
        normalized_type = _normalize_media_type(media_type)
        filtered = [item for item in result.get("results", []) if item.get("media_type") == normalized_type]
        return {"results": filtered, "total_count": len(filtered)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def add_custom_catalog_item_api(catalog_id: str, payload: dict):
    tmdb_id = payload.get("tmdb_id")
    db_index = payload.get("db_index")
    media_type = _normalize_media_type(payload.get("media_type", "movie"))

    if not tmdb_id or not db_index:
        raise HTTPException(status_code=400, detail="tmdb_id and db_index are required.")

    media = await db.get_document(media_type, int(tmdb_id), int(db_index))
    if not media:
        raise HTTPException(status_code=404, detail="Media not found.")

    catalog = await db.get_custom_catalog(catalog_id)
    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found.")

    added = await db.add_item_to_custom_catalog(catalog_id, int(tmdb_id), int(db_index), media_type)
    message = "Added to catalog." if added else "Already exists in this catalog."
    return {"message": message, "added": added}


async def remove_custom_catalog_item_api(
    catalog_id: str,
    tmdb_id: int,
    db_index: int,
    media_type: str,
):
    catalog = await db.get_custom_catalog(catalog_id)
    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found.")

    removed = await db.remove_item_from_custom_catalog(
        catalog_id, int(tmdb_id), int(db_index), _normalize_media_type(media_type)
    )
    if not removed:
        return {"message": "Item was not in this catalog.", "removed": False}
    return {"message": "Removed from catalog.", "removed": True}


async def auto_sync_custom_catalogs_api(full_rebuild: bool = False):
    try:
        result = await start_auto_catalog_sync_background(db, force=True, full_rebuild=full_rebuild)
        return {"message": result.get("message", "Auto sync started."), "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def auto_catalog_sync_status_api():
    try:
        return {"status": await get_auto_catalog_sync_status(db)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_auto_catalog_settings_api():
    try:
        return {"settings": await get_auto_catalog_settings(db)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def update_auto_catalog_settings_api(payload: dict):
    try:
        enabled_keys = payload.get("enabled_keys", [])
        if not isinstance(enabled_keys, list):
            raise HTTPException(status_code=400, detail="enabled_keys must be a list.")
        settings = await update_auto_catalog_settings(db, enabled_keys)
        return {"message": "Auto catalog settings saved.", "settings": settings}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Manual Subtitle API
# ---------------------------------------------------------------------------

async def resolve_subtitle_api(payload: dict) -> dict:
    client = _scan_client()
    if client is None:
        raise HTTPException(status_code=503, detail="No Telegram client is connected yet.")
    try:
        data = await resolve_subtitle_message(
            client, url=payload.get("url"),
            chat_id=payload.get("chat_id"), msg_id=payload.get("msg_id"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read that message: {exc}")
    return {"status": "success", "data": data}


async def _resolve_imdb_id(media_type: str, tmdb_id, db_index) -> str:
    if not (tmdb_id and db_index):
        raise HTTPException(status_code=400, detail="tmdb_id and db_index are required.")
    doc = await db.get_document(media_type, int(tmdb_id), int(db_index))
    if not doc or not doc.get("imdb_id"):
        raise HTTPException(status_code=404, detail="Title not found.")
    return doc["imdb_id"]


def list_subtitle_languages_api() -> dict:
    return {"status": "success", "languages": list_languages()}


async def list_subtitles_api(media_type: str, tmdb_id, db_index) -> dict:
    mt = "tv" if media_type in ("tv", "series") else "movie"
    imdb_id = await _resolve_imdb_id(mt, tmdb_id, db_index)
    return {"status": "success", "subtitles": await list_title_subtitles(imdb_id)}


async def add_subtitles_api(payload: dict) -> dict:
    media_type = "tv" if payload.get("media_type") in ("tv", "series") else "movie"
    imdb_id = await _resolve_imdb_id(media_type, payload.get("tmdb_id"), payload.get("db_index"))
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Provide at least one subtitle to add.")

    client = _scan_client()
    if client is None:
        raise HTTPException(status_code=503, detail="No Telegram client is connected yet.")

    added, errors = [], []
    for item in items:
        try:
            season = item.get("season") if media_type == "tv" else None
            episode = item.get("episode") if media_type == "tv" else None
            if media_type == "tv" and (not season or not episode):
                raise ValueError("Season and episode are required for series subtitles.")
            resolved = await resolve_subtitle_message(
                client, url=item.get("url"),
                chat_id=item.get("chat_id"), msg_id=item.get("msg_id"),
            )
            doc = await manual_ingest_subtitle(
                imdb_id, media_type, season, episode,
                item.get("lang_code") or resolved["lang_code"],
                resolved["chat_id"], resolved["msg_id"], resolved["name"],
            )
            added.append({
                "name": doc["name"], "lang_label": doc["lang_label"],
                "season": doc["season"], "episode": doc["episode"],
            })
        except ValueError as exc:
            errors.append(str(exc))
        except Exception as exc:
            errors.append(f"Could not add subtitle: {exc}")

    if not added and errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))
    message = f"Added {len(added)} subtitle(s)."
    if errors:
        message += f" {len(errors)} failed: {' '.join(errors)}"
    return {"status": "success", "message": message, "added": added, "errors": errors}


async def remove_subtitle_api(payload: dict) -> dict:
    chat_id = payload.get("chat_id")
    msg_id = payload.get("msg_id")
    if chat_id in (None, "") or msg_id in (None, ""):
        raise HTTPException(status_code=400, detail="chat_id and msg_id are required.")
    if not await remove_subtitle(chat_id, msg_id):
        raise HTTPException(status_code=404, detail="Subtitle not found.")
    return {"status": "success", "message": "Subtitle removed."}
