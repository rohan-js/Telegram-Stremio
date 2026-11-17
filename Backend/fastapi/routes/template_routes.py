from fastapi import Request, Form, HTTPException, Depends
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from Backend.fastapi.security.credentials import verify_credentials, require_auth, is_authenticated, get_current_user, revoke_session
from Backend.fastapi.themes import get_theme, get_all_themes, theme_key
import secrets
from Backend.helper.analytics import client_ip_from
from Backend.helper.bounded import BoundedDict
from Backend import db
from Backend.pyrofork.bot import work_loads, multi_clients, StreamBot
from Backend.helper.pyro import get_readable_time
from Backend import StartTime, __version__
import time
from Backend.helper.custom_dl import ACTIVE_STREAMS, RECENT_STREAMS
from Backend.helper.nginx_egress import get_nginx_egress_summary
from Backend.helper.host_outbound import get_vps_outbound_summary, empty_vps_outbound_summary
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.analytics import get_activity_overview
from Backend.config import Telegram
from datetime import datetime, timezone

templates = Jinja2Templates(directory="Backend/fastapi/templates")


#----- Shared template context (request, theme metadata) for every page
def _base_context(request):
    return {
        "request": request,
        "theme": get_theme(theme_key(request)),
        "themes": get_all_themes(),
        "current_theme": theme_key(request),
        "current_user": get_current_user(request),
    }

templates.env.globals["cover_url"] = lambda value: str(value or "")


async def admin_dashboard_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("admin_dashboard.html", _base_context(request))


async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", _base_context(request))


#----- Per-IP failed-login limiter (upstream 9ea62e5, owner-tuned 10/15)
_FAILED_LOGINS = BoundedDict(1000)
_LOGIN_MAX_FAILS = 10
_LOGIN_WINDOW = 15 * 60


def _login_error_context(request: Request, error: str):
    ctx = _base_context(request)
    ctx["current_user"] = None
    ctx["error"] = error
    return ctx


async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    ip = client_ip_from(request)
    now = time.time()
    fails = [t for t in _FAILED_LOGINS.get(ip, []) if now - t < _LOGIN_WINDOW]
    if len(fails) >= _LOGIN_MAX_FAILS:
        return templates.TemplateResponse(
            "login.html", _login_error_context(request, "Too many failed attempts. Try again in 15 minutes."),
            status_code=429,
        )
    if verify_credentials(username, password):
        _FAILED_LOGINS.pop(ip, None)
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["sid"] = secrets.token_urlsafe(16)
        return RedirectResponse(url="/", status_code=302)
    _FAILED_LOGINS[ip] = fails + [now]
    return templates.TemplateResponse(
        "login.html", _login_error_context(request, "Invalid credentials")
    )


async def logout(request: Request):
    revoke_session(request)
    return RedirectResponse(url="/login", status_code=302)


async def set_theme(request: Request, theme: str = Form(None)):
    # Upstream v5.3.3: single flat theme, persisted to both the session and a
    # 1-year cookie (applyTheme on the client also writes the cookie directly).
    if theme and theme in get_all_themes():
        request.session["theme"] = theme
        response = RedirectResponse(url=request.headers.get("referer", "/"), status_code=302)
        response.set_cookie("theme", theme, max_age=365 * 24 * 3600, samesite="lax")
        return response
    return RedirectResponse(url=request.headers.get("referer", "/"), status_code=302)


async def dashboard_page(request: Request, _: bool = Depends(require_auth)):
    try:
        db_stats = await db.get_database_stats()
        total_movies = sum(stat.get("movie_count", 0) for stat in db_stats)
        total_tv_shows = sum(stat.get("tv_count", 0) for stat in db_stats)

        now = time.time()
        PRUNE_SECONDS = 3
        for sid, info in list(ACTIVE_STREAMS.items()):
            status = info.get("status")
            # Check end_ts first to see when it organically finished
            last_ts = info.get("end_ts") or info.get("last_ts") or info.get("start_ts", now)

            if status in ("cancelled", "error", "finished") and (now - last_ts > PRUNE_SECONDS):

                info["duration"] = round(now - info.get("start_ts", now), 1)
                info["stream_id"] = sid
                try:
                    RECENT_STREAMS.appendleft(info)
                    ACTIVE_STREAMS.pop(sid)
                except KeyError:
                    pass

        active_streams_data = []
        for stream_id, info in ACTIVE_STREAMS.items():
            active_streams_data.append({
                "stream_id": stream_id,
                "msg_id": info.get("msg_id"),
                "chat_id": info.get("chat_id"),
                "status": info.get("status", "active"),
                "total_bytes": info.get("total_bytes", 0),
                "avg_mbps": round(info.get("avg_mbps", 0.0), 2),
                "instant_mbps": round(info.get("instant_mbps", 0.0), 2),
                "peak_mbps": round(info.get("peak_mbps", 0.0), 2),
                "client_index": info.get("client_index", 0),
                "dc_id": info.get("dc_id", 0),
                "duration": round(now - info.get("start_ts", now), 1),
                "meta": info.get("meta", {})
            })

        system_stats = {
            "server_status": "running",
            "uptime": get_readable_time(now - StartTime),
            "telegram_bot": f"@{StreamBot.username}" if StreamBot and StreamBot.username else "@StreamBot",
            "connected_bots": len(multi_clients),
            "loads": {
                f"bot{c+1}": l
                for c, (_, l) in enumerate(sorted(work_loads.items(), key=lambda x: x[1], reverse=True))
            } if work_loads else {},
            "version": __version__,
            "movies": total_movies,
            "tv_shows": total_tv_shows,
            "databases": db_stats,
            "total_databases": len(db_stats),
            "current_db_index": db.current_db_index,
            "egress": get_nginx_egress_summary(),
            "vps_outbound": await get_vps_outbound_summary(db),
            "active_streams": active_streams_data,
            "total_active_streams": len(active_streams_data)
        }

    except Exception as e:
        print(f"Dashboard error: {e}")
        system_stats = {
            "server_status": "error",
            "error": str(e),
            "uptime": "N/A",
            "telegram_bot": "@StreamBot",
            "connected_bots": 0,
            "loads": {},
            "version": __version__,
            "movies": 0,
            "tv_shows": 0,
            "databases": [],
            "total_databases": 0,
            "current_db_index": 1,
            "egress": get_nginx_egress_summary(),
            "vps_outbound": empty_vps_outbound_summary(status="error"),
            "active_streams": [],
            "total_active_streams": 0
        }

    api_tokens = await db.get_all_api_tokens()
    try:
        user_activity_initial = await get_activity_overview(1, 12)
    except Exception:
        user_activity_initial = {"users": [], "online_count": 0, "total": 0, "page": 1, "per_page": 12, "total_pages": 1}
    ctx = _base_context(request)
    ctx.update({
        "system_stats": system_stats,
        "api_tokens": api_tokens,
        "user_activity_initial": user_activity_initial,
    })
    return templates.TemplateResponse("dashboard.html", ctx)


async def media_management_page(request: Request, media_type: str = "movie", custom: str | None = None, _: bool = Depends(require_auth)):
    ctx = _base_context(request)
    ctx.update({"media_type": media_type, "custom": custom})
    return templates.TemplateResponse("media_management.html", ctx)


async def custom_catalogs_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("custom_catalogs.html", _base_context(request))


async def watch_requests_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("watch_requests.html", _base_context(request))


async def live_tv_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("live_tv.html", _base_context(request))


async def settings_page(request: Request, _: bool = Depends(require_auth)):
    settings = SettingsManager.current().to_dict()
    settings["admin_password_set"] = bool(settings.get("admin_password"))
    settings["admin_password"] = ""
    settings["session_secret_set"] = bool(settings.get("session_secret"))
    settings["session_secret"] = ""
    settings["multi_token_count"] = len([k for k in __import__("os").environ if k.startswith("MULTI_TOKEN")])
    settings["secret_statuses"] = SettingsManager.secret_statuses()

    from Backend.helper.stream_formatter import PRESETS
    ctx = _base_context(request)
    ctx.update({"settings": settings, "formatter_presets": PRESETS})
    return templates.TemplateResponse("settings.html", ctx)


async def tools_page(request: Request, _: bool = Depends(require_auth)):
    from Backend.config import Telegram as _TG
    ctx = _base_context(request)
    ctx.update({
        # Upstream's tools page gates its Bot Admin card on these; the fork
        # has no userbot streaming, so userbot_configured follows the session.
        "userbot_configured": bool(getattr(_TG, "USER_SESSION_STRING", "") or ""),
        "multi_token_available": settings_page_multi_token_available(),
    })
    return templates.TemplateResponse("tools.html", ctx)


def settings_page_multi_token_available() -> bool:
    return bool([k for k in __import__("os").environ if k.startswith("MULTI_TOKEN")])


async def insights_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("insights.html", _base_context(request))


async def launch_readiness_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("launch_readiness.html", _base_context(request))


async def admin_requests_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("requests_manage.html", _base_context(request))


async def public_request_page(request: Request):
    ctx = _base_context(request)
    ctx.update({
        "is_authenticated": is_authenticated(request),
        "requests_enabled": SettingsManager.current().content_requests_enabled,
    })
    return templates.TemplateResponse("request_public.html", ctx)


async def wrapped_page(request: Request, token: str):
    ctx = _base_context(request)
    ctx.update({
        "is_authenticated": is_authenticated(request),
        "token": token,
        "year": datetime.now(timezone.utc).year,
    })
    return templates.TemplateResponse("wrapped.html", ctx)


async def policy_page(request: Request, policy: str):
    titles = {
        "terms": "Terms of Service",
        "privacy": "Privacy Policy",
        "acceptable-use": "Acceptable Use",
        "takedown": "Takedown Policy",
    }
    if policy not in titles:
        raise HTTPException(status_code=404, detail="Policy not found")
    ctx = _base_context(request)
    ctx.update({
        "policy": policy,
        "title": titles[policy],
        "terms_version": Telegram.TERMS_VERSION,
    })
    return templates.TemplateResponse("policy.html", ctx)


async def edit_media_page(request: Request, tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    try:
        media_details = await db.get_document(media_type, tmdb_id, db_index)
        if not media_details:
            raise HTTPException(status_code=404, detail="Media not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    api_token = None
    try:
        tokens = await db.get_all_api_tokens()
        if tokens:
            api_token = tokens[0].get("token")
    except Exception:
        api_token = None

    ctx = _base_context(request)
    ctx.update({
        "tmdb_id": tmdb_id,
        "db_index": db_index,
        "media_type": media_type,
        "media_details": media_details,
        "api_token": api_token,
    })
    return templates.TemplateResponse("media_edit.html", ctx)


async def admin_subscriptions_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("subscriptions_manage.html", _base_context(request))


async def admin_access_page(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("access_manage.html", _base_context(request))


async def vlc_redirect(request: Request, token: str, id: str):
    from fastapi.responses import Response, HTMLResponse
    from Backend.helper.encrypt import decode_string

    base_url = Telegram.BASE_URL.rstrip('/')
    stream_url = f"{base_url}/dl/{token}/{id}/video.mkv"
    user_agent = (request.headers.get("user-agent") or "").lower()
    is_android = "android" in user_agent

    if is_android:
        intent_url = (
            f"intent://{base_url.replace('https://', '').replace('http://', '')}"
            f"/dl/{token}/{id}/video.mkv"
            f"#Intent;scheme=https;package=org.videolan.vlc;type=video/*;end"
        )
        html = (
            '<!DOCTYPE html><html><head><meta charset="UTF-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
            '<title>Opening VLC...</title>'
            '<style>body{background:#0a0a1a;color:#fff;font-family:sans-serif;'
            'display:flex;align-items:center;justify-content:center;height:100vh;'
            'text-align:center}a{color:#a855f7;text-decoration:underline}</style>'
            '</head><body><div>'
            '<h2>🎬 Opening in VLC...</h2>'
            '<p style="margin-top:15px;opacity:0.6;font-size:0.85rem">'
            'Make sure VLC is installed.</p>'
            f'<p style="margin-top:10px"><a href="{intent_url}">Tap here if VLC didn\'t open</a></p>'
            '</div>'
            f'<script>window.location.href="{intent_url}";</script>'
            '</body></html>'
        )
        return HTMLResponse(content=html)

    try:
        decoded = await decode_string(id)
        title = decoded.get("title", "stream")
        quality = decoded.get("quality", "")
        raw_name = f"{title}_{quality}" if quality else title
        filename = "".join(c for c in raw_name if c.isascii() and (c.isalnum() or c in "._- ")).strip() or "stream"
        filename += ".m3u"
    except Exception:
        filename = "play.m3u"

    content = f"#EXTM3U\n#EXTINF:-1,Telegram Stream\n{stream_url}\n"
    return Response(
        content=content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
