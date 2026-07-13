from fastapi import FastAPI, Request, Form, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from Backend import __version__
from Backend.config import Telegram
from Backend.fastapi.security.credentials import require_auth
from Backend.fastapi.routes.stream_routes import router as stream_router, decay_client_failures
from Backend.fastapi.routes.stremio_routes import router as stremio_router
from Backend.fastapi.routes.iptv_routes import router as iptv_router
from Backend.fastapi.routes.template_routes import (
    login_page, login_post, logout, set_theme, dashboard_page,
    media_management_page, edit_media_page, public_status_page, stremio_guide_page,
    admin_dashboard_page, admin_subscriptions_page, admin_access_page, vlc_redirect,
    custom_catalogs_page, watch_requests_page, live_tv_page, settings_page, tools_page,
    launch_readiness_page, policy_page, admin_requests_page, public_request_page
)
from Backend.fastapi.routes.api_routes import (
    list_media_api, delete_media_api, update_media_api,
    delete_movie_quality_api, delete_tv_quality_api,
    delete_tv_episode_api, delete_tv_season_api,
    create_token_api, revoke_token_api, update_token_limits_api,
    speed_test_api, speed_test_stream_api,
    get_admin_stats_api, clear_cache_api, get_dead_links_api,
    get_stream_analytics_api, clear_stream_analytics_api,
    get_subscription_plans_api, add_subscription_plan_api,
    update_subscription_plan_api, delete_subscription_plan_api,
    get_all_subscribers_api, manage_subscriber_api,
    get_all_tokens_api, assign_plan_api, link_token_user_api,
    search_media_rescan_api, apply_media_rescan_api,
    resolve_telegram_api, manual_add_media_api,
    search_manual_add_metadata_api, resolve_manual_add_metadata_api, get_manual_add_catalogs_api,
    list_custom_catalogs_api, create_custom_catalog_api, update_custom_catalog_api,
    delete_custom_catalog_api, get_custom_catalog_items_api, search_catalog_media_api,
    add_custom_catalog_item_api, remove_custom_catalog_item_api,
    auto_sync_custom_catalogs_api, auto_catalog_sync_status_api,
    get_auto_catalog_settings_api, update_auto_catalog_settings_api,
    get_duplicate_media_api,
    update_quality_flags_api, clear_quality_flags_api,
    get_iptv_status_api, sync_iptv_api, list_iptv_channels_api,
    update_iptv_channel_api, get_iptv_settings_api, update_iptv_settings_api,
    get_settings_api, update_settings_api,
    get_tools_channels_api, start_tools_scan_api, cancel_tools_scan_api,
    get_tools_scan_status_api, start_tools_dbcheck_api, cancel_tools_dbcheck_api,
    get_tools_dbcheck_status_api, purge_tools_dead_links_api,
    get_manual_session_api, search_manual_session_api,
    set_manual_session_api, clear_manual_session_api,
    get_warp_status_api, apply_warp_api,
    get_launch_readiness_api, run_backup_api, admin_takedown_api,
    request_search_api, request_popular_api, request_submit_api,
    get_requests_api, update_request_api, delete_request_api,
    get_health_api, get_admin_logs_api, download_admin_logs_api,
    export_config_api, import_config_api
)

app = FastAPI(
    title="Telegram Stremio Media Server",
    description="A powerful, self-hosted Telegram Stremio Media Server built with FastAPI, MongoDB, and PyroFork seamlessly integrated with Stremio for automated media streaming and discovery.",
    version=__version__
)

# --- Middleware Setup ---
app.add_middleware(SessionMiddleware, secret_key=Telegram.SESSION_SECRET)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://web.stremio.com",
        "https://stremio.com",
        "https://app.strem.io",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    app.mount("/static", StaticFiles(directory="Backend/fastapi/static"), name="static")
except Exception:
    pass

@app.on_event("startup")
async def _startup():
    import asyncio
    asyncio.create_task(decay_client_failures())

# --- Include existing API routers ---
app.include_router(stream_router)
app.include_router(stremio_router)
app.include_router(iptv_router)

# --- Public Routes (No Authentication Required) ---
@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return await login_page(request)

@app.post("/login", response_class=HTMLResponse)
async def login_post_route(request: Request, username: str = Form(...), password: str = Form(...)):
    return await login_post(request, username, password)

@app.get("/logout")
async def logout_route(request: Request):
    return await logout(request)

@app.post("/set-theme")
async def set_theme_route(request: Request, theme: str = Form(...)):
    return await set_theme(request, theme)

@app.get("/status", response_class=HTMLResponse)
async def public_status(request: Request):
    return await public_status_page(request)

@app.get("/stremio", response_class=HTMLResponse)
async def stremio_guide(request: Request):
    return await stremio_guide_page(request)

@app.get("/vlc/{token}/{id}", response_class=HTMLResponse)
async def vlc(request: Request, token: str, id: str):
    return await vlc_redirect(request, token, id)

@app.get("/terms", response_class=HTMLResponse)
async def terms(request: Request):
    return await policy_page(request, "terms")

@app.get("/privacy", response_class=HTMLResponse)
async def privacy(request: Request):
    return await policy_page(request, "privacy")

@app.get("/acceptable-use", response_class=HTMLResponse)
async def acceptable_use(request: Request):
    return await policy_page(request, "acceptable-use")

@app.get("/takedown", response_class=HTMLResponse)
async def takedown(request: Request):
    return await policy_page(request, "takedown")

@app.get("/request", response_class=HTMLResponse)
async def public_request(request: Request):
    return await public_request_page(request)

@app.get("/api/request/search")
async def request_search(q: str = Query("", max_length=120)):
    return await request_search_api(q)

@app.get("/api/request/popular")
async def request_popular():
    return await request_popular_api()

@app.post("/api/request/submit")
async def request_submit(request: Request):
    payload = await request.json()
    client_ip = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    client_ip = client_ip or (request.client.host if request.client else "unknown")
    return await request_submit_api(payload, client_ip)

# --- Protected Routes (Authentication Required) ---
@app.get("/", response_class=HTMLResponse)
async def root(request: Request, _: bool = Depends(require_auth)):
    return await dashboard_page(request, _)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, _: bool = Depends(require_auth)):
    return await dashboard_page(request, _)

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request, _: bool = Depends(require_auth)):
    return await admin_dashboard_page(request, _)

@app.get("/media/manage", response_class=HTMLResponse)
async def media_management(request: Request, media_type: str = "movie", _: bool = Depends(require_auth)):
    return await media_management_page(request, media_type, _)

@app.get("/catalogs", response_class=HTMLResponse)
async def custom_catalogs(request: Request, _: bool = Depends(require_auth)):
    return await custom_catalogs_page(request, _)

@app.get("/watch-requests", response_class=HTMLResponse)
async def watch_requests(request: Request, _: bool = Depends(require_auth)):
    return await watch_requests_page(request, _)

@app.get("/live-tv", response_class=HTMLResponse)
async def live_tv(request: Request, _: bool = Depends(require_auth)):
    return await live_tv_page(request, _)

@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request, _: bool = Depends(require_auth)):
    return await settings_page(request, _)

@app.get("/admin/tools", response_class=HTMLResponse)
async def admin_tools(request: Request, _: bool = Depends(require_auth)):
    return await tools_page(request, _)

@app.get("/api/admin/tools/manual-session")
async def get_manual_session(_: bool = Depends(require_auth)):
    return await get_manual_session_api()

@app.get("/api/admin/tools/manual-session/search")
async def search_manual_session(query: str = Query("", max_length=160), _: bool = Depends(require_auth)):
    return await search_manual_session_api(query)

@app.post("/api/admin/tools/manual-session")
async def set_manual_session(payload: dict, _: bool = Depends(require_auth)):
    return await set_manual_session_api(payload)

@app.delete("/api/admin/tools/manual-session")
async def clear_manual_session(_: bool = Depends(require_auth)):
    return await clear_manual_session_api()

@app.get("/admin/launch-readiness", response_class=HTMLResponse)
async def admin_launch_readiness(request: Request, _: bool = Depends(require_auth)):
    return await launch_readiness_page(request, _)

@app.get("/admin/requests", response_class=HTMLResponse)
async def admin_requests(request: Request, _: bool = Depends(require_auth)):
    return await admin_requests_page(request, _)

@app.get("/media/edit", response_class=HTMLResponse)
async def edit_media(request: Request, tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await edit_media_page(request, tmdb_id, db_index, media_type, _)

@app.get("/api/media/list")
async def list_media(
    media_type: str = Query("movie", regex="^(movie|tv)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=100),
    search: str = Query("", max_length=100),
    _: bool = Depends(require_auth)
):
    return await list_media_api(media_type, page, page_size, search)

@app.delete("/api/media/delete")
async def delete_media(tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await delete_media_api(tmdb_id, db_index, media_type)

@app.put("/api/media/update")
async def update_media(request: Request, tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await update_media_api(request, tmdb_id, db_index, media_type)

@app.get("/api/media/rescan/search")
async def search_media_rescan(
    media_type: str = Query(..., regex="^(movie|tv)$"),
    query: str = Query(..., min_length=1, max_length=120),
    year: int | None = Query(None),
    _: bool = Depends(require_auth),
):
    return await search_media_rescan_api(media_type, query, year)

@app.post("/api/media/rescan/apply")
async def apply_media_rescan(
    request: Request,
    tmdb_id: int,
    db_index: int,
    media_type: str = Query(..., regex="^(movie|tv)$"),
    _: bool = Depends(require_auth),
):
    return await apply_media_rescan_api(request, tmdb_id, db_index, media_type)

@app.post("/api/media/resolve-telegram")
async def resolve_telegram_message_route(payload: dict, _: bool = Depends(require_auth)):
    return await resolve_telegram_api(payload)

@app.post("/api/media/manual-add")
async def manual_add_media_route(payload: dict, _: bool = Depends(require_auth)):
    return await manual_add_media_api(payload)

@app.get("/api/media/manual-add/search")
async def search_manual_add_metadata(
    media_type: str = Query(..., regex="^(movie|tv)$"),
    query: str = Query(..., min_length=1, max_length=160),
    year: int | None = Query(None),
    _: bool = Depends(require_auth),
):
    return await search_manual_add_metadata_api(media_type, query, year)

@app.get("/api/media/manual-add/metadata")
async def resolve_manual_add_metadata(
    media_type: str = Query(..., regex="^(movie|tv)$"),
    selected_id: str = Query(..., min_length=1, max_length=80),
    _: bool = Depends(require_auth),
):
    return await resolve_manual_add_metadata_api(media_type, selected_id)

@app.get("/api/media/manual-add/catalogs")
async def get_manual_add_catalogs(_: bool = Depends(require_auth)):
    return await get_manual_add_catalogs_api()

@app.delete("/api/media/delete-quality")
async def delete_movie_quality(tmdb_id: int, db_index: int, id: str, _: bool = Depends(require_auth)):
    return await delete_movie_quality_api(tmdb_id, db_index, id)

@app.delete("/api/media/delete-tv-quality")
async def delete_tv_quality(tmdb_id: int, db_index: int, season: int, episode: int, id: str, _: bool = Depends(require_auth)):
    return await delete_tv_quality_api(tmdb_id, db_index, season, episode, id)

@app.delete("/api/media/delete-tv-episode")
async def delete_tv_episode(tmdb_id: int, db_index: int, season: int, episode: int, _: bool = Depends(require_auth)):
    return await delete_tv_episode_api(tmdb_id, db_index, season, episode)

@app.delete("/api/media/delete-tv-season")
async def delete_tv_season(tmdb_id: int, db_index: int, season: int, _: bool = Depends(require_auth)):
    return await delete_tv_season_api(tmdb_id, db_index, season)

@app.get("/api/media/duplicates")
async def get_media_duplicates(_: bool = Depends(require_auth)):
    return await get_duplicate_media_api()

@app.post("/api/media/quality-flags")
async def update_quality_flags(payload: dict, _: bool = Depends(require_auth)):
    return await update_quality_flags_api(payload)

@app.post("/api/media/quality-flags/clear")
async def clear_quality_flags(payload: dict, _: bool = Depends(require_auth)):
    return await clear_quality_flags_api(payload)

@app.get("/api/iptv/status")
async def get_iptv_status(_: bool = Depends(require_auth)):
    return await get_iptv_status_api()

@app.post("/api/iptv/sync")
async def sync_iptv(force: bool = True, _: bool = Depends(require_auth)):
    return await sync_iptv_api(force)

@app.get("/api/iptv/channels")
async def get_iptv_channels(
    search: str = "",
    category: str = "",
    country: str = "",
    hidden: bool | None = None,
    page: int = 1,
    page_size: int = 50,
    _: bool = Depends(require_auth),
):
    return await list_iptv_channels_api(search, category, country, hidden, page, page_size)

@app.patch("/api/iptv/channels/{channel_id}")
async def update_iptv_channel(
    channel_id: str,
    payload: dict,
    _: bool = Depends(require_auth),
):
    return await update_iptv_channel_api(channel_id, payload)

@app.get("/api/iptv/settings")
async def get_iptv_settings_route(_: bool = Depends(require_auth)):
    return await get_iptv_settings_api()

@app.put("/api/iptv/settings")
async def update_iptv_settings_route(payload: dict, _: bool = Depends(require_auth)):
    return await update_iptv_settings_api(payload)

@app.get("/api/admin/settings")
async def get_settings(_: bool = Depends(require_auth)):
    return await get_settings_api()

@app.put("/api/admin/settings")
async def update_settings(payload: dict, _: bool = Depends(require_auth)):
    return await update_settings_api(payload)

@app.get("/api/admin/tools/channels")
async def admin_tools_channels(_: bool = Depends(require_auth)):
    return await get_tools_channels_api()

@app.post("/api/admin/tools/scan/start")
async def admin_tools_scan_start(payload: dict, _: bool = Depends(require_auth)):
    return await start_tools_scan_api(payload)

@app.post("/api/admin/tools/scan/cancel")
async def admin_tools_scan_cancel(_: bool = Depends(require_auth)):
    return await cancel_tools_scan_api()

@app.get("/api/admin/tools/scan/status")
async def admin_tools_scan_status(_: bool = Depends(require_auth)):
    return await get_tools_scan_status_api()

@app.post("/api/admin/tools/dbcheck/start")
async def admin_tools_dbcheck_start(_: bool = Depends(require_auth)):
    return await start_tools_dbcheck_api()

@app.post("/api/admin/tools/dbcheck/cancel")
async def admin_tools_dbcheck_cancel(_: bool = Depends(require_auth)):
    return await cancel_tools_dbcheck_api()

@app.get("/api/admin/tools/dbcheck/status")
async def admin_tools_dbcheck_status(_: bool = Depends(require_auth)):
    return await get_tools_dbcheck_status_api()

@app.post("/api/admin/tools/dead-links/purge")
async def admin_tools_dead_links_purge(payload: dict, _: bool = Depends(require_auth)):
    return await purge_tools_dead_links_api(payload)

@app.get("/api/admin/warp/status")
async def admin_warp_status(_: bool = Depends(require_auth)):
    return await get_warp_status_api()

@app.post("/api/admin/warp/apply")
async def admin_warp_apply(payload: dict, _: bool = Depends(require_auth)):
    return await apply_warp_api(payload)

@app.get("/api/admin/launch-readiness")
async def admin_launch_readiness_api(_: bool = Depends(require_auth)):
    return await get_launch_readiness_api()

@app.post("/api/admin/backup/run")
async def admin_backup_run(payload: dict | None = None, _: bool = Depends(require_auth)):
    return await run_backup_api(payload)

@app.get("/api/admin/backup/export")
async def admin_backup_export(_: bool = Depends(require_auth)):
    return await export_config_api()

@app.post("/api/admin/backup/import")
async def admin_backup_import(payload: dict, _: bool = Depends(require_auth)):
    return await import_config_api(payload)

@app.get("/api/admin/health")
async def admin_health(force: bool = False, _: bool = Depends(require_auth)):
    return await get_health_api(force)

@app.get("/api/admin/logs")
async def admin_logs(max_bytes: int = 200000, _: bool = Depends(require_auth)):
    return await get_admin_logs_api(max_bytes)

@app.get("/api/admin/logs/download")
async def admin_logs_download(max_bytes: int = 500000, _: bool = Depends(require_auth)):
    return await download_admin_logs_api(max_bytes)

@app.get("/api/admin/requests")
async def admin_requests_list(_: bool = Depends(require_auth)):
    return await get_requests_api()

@app.patch("/api/admin/requests/{request_id}")
async def admin_request_update(request_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_request_api(request_id, payload)

@app.delete("/api/admin/requests/{request_id}")
async def admin_request_delete(request_id: str, _: bool = Depends(require_auth)):
    return await delete_request_api(request_id)

@app.post("/api/admin/takedown")
async def admin_takedown(payload: dict, _: bool = Depends(require_auth)):
    return await admin_takedown_api(payload)

@app.get("/api/system/workloads")
async def get_workloads(_: bool = Depends(require_auth)):
    try:
        from Backend.pyrofork.bot import work_loads
        return {
            "loads": {
                f"bot{c + 1}": l
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            } if work_loads else {}
        }
    except Exception as e:
        return {"loads": {}}

@app.post("/api/tokens")
async def create_token(payload: dict, _: bool = Depends(require_auth)):
    return await create_token_api(payload)

@app.put("/api/tokens/{token}")
async def update_token(token: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_token_limits_api(token, payload)

@app.delete("/api/tokens/{token}")
async def revoke_token(token: str, _: bool = Depends(require_auth)):
    return await revoke_token_api(token)

@app.get("/api/system/stats")
async def get_system_stats(_: bool = Depends(require_auth)):
    from Backend.fastapi.routes.api_routes import get_system_stats_api
    return await get_system_stats_api()

@app.get("/api/admin/system-stats")
async def admin_system_stats(_: bool = Depends(require_auth)):
    return await get_admin_stats_api()

@app.post("/api/admin/clear-cache")
async def clear_cache(_: bool = Depends(require_auth)):
    return await clear_cache_api()

@app.get("/api/admin/dead-links")
async def get_dead_links(_: bool = Depends(require_auth)):
    return await get_dead_links_api()

@app.get("/api/admin/stream-analytics")
async def get_stream_analytics(_: bool = Depends(require_auth)):
    return await get_stream_analytics_api()

@app.post("/api/admin/clear-analytics")
async def clear_analytics(_: bool = Depends(require_auth)):
    return await clear_stream_analytics_api()

@app.get("/admin/subscriptions", response_class=HTMLResponse)
async def admin_subscriptions(request: Request, _: bool = Depends(require_auth)):
    return await admin_subscriptions_page(request, _)

@app.get("/api/admin/subscriptions/plans")
async def get_subscription_plans(_: bool = Depends(require_auth)):
    return await get_subscription_plans_api()

@app.post("/api/admin/subscriptions/plans")
async def add_subscription_plan(payload: dict, _: bool = Depends(require_auth)):
    return await add_subscription_plan_api(payload)

@app.put("/api/admin/subscriptions/plans/{plan_id}")
async def update_subscription_plan(plan_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_subscription_plan_api(plan_id, payload)

@app.delete("/api/admin/subscriptions/plans/{plan_id}")
async def delete_subscription_plan(plan_id: str, _: bool = Depends(require_auth)):
    return await delete_subscription_plan_api(plan_id)

@app.get("/api/admin/subscriptions/users")
async def get_subscribers(_: bool = Depends(require_auth)):
    return await get_all_subscribers_api()

@app.post("/api/admin/subscriptions/users/{user_id}/manage")
async def manage_subscriber(user_id: int, payload: dict, _: bool = Depends(require_auth)):
    return await manage_subscriber_api(user_id, payload)

# --- Access Management ---
@app.get("/admin/access", response_class=HTMLResponse)
async def admin_access(request: Request, _: bool = Depends(require_auth)):
    return await admin_access_page(request, _)

@app.get("/api/admin/access/tokens")
async def get_access_tokens(_: bool = Depends(require_auth)):
    return await get_all_tokens_api()

@app.delete("/api/admin/access/tokens/{token}")
async def delete_access_token(token: str, _: bool = Depends(require_auth)):
    from Backend.fastapi.routes.api_routes import revoke_token_api as _revoke_token_api
    return await _revoke_token_api(token)

@app.post("/api/admin/access/users/{user_id}/assign-plan")
async def assign_access_plan(user_id: int, payload: dict, _: bool = Depends(require_auth)):
    days = int(payload.get("days", 0))
    return await assign_plan_api(user_id, days)

@app.patch("/api/admin/access/tokens/{token}/link-user")
async def link_token_to_user(token: str, payload: dict, _: bool = Depends(require_auth)):
    user_id = int(payload.get("user_id", 0))
    if not user_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="user_id is required.")
    return await link_token_user_api(token, user_id)

@app.get("/api/custom-catalogs")
async def list_custom_catalogs(
    tmdb_id: int | None = None,
    db_index: int | None = None,
    media_type: str | None = None,
    _: bool = Depends(require_auth),
):
    return await list_custom_catalogs_api(tmdb_id, db_index, media_type)

@app.post("/api/custom-catalogs")
async def create_custom_catalog(payload: dict, _: bool = Depends(require_auth)):
    return await create_custom_catalog_api(payload)

@app.put("/api/custom-catalogs/{catalog_id}")
async def update_custom_catalog(catalog_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_custom_catalog_api(catalog_id, payload)

@app.delete("/api/custom-catalogs/{catalog_id}")
async def delete_custom_catalog(catalog_id: str, _: bool = Depends(require_auth)):
    return await delete_custom_catalog_api(catalog_id)

@app.get("/api/custom-catalogs/search-media")
async def search_catalog_media(
    query: str,
    media_type: str = "movie",
    page: int = 1,
    page_size: int = 12,
    _: bool = Depends(require_auth),
):
    return await search_catalog_media_api(query, media_type, page, page_size)

@app.post("/api/custom-catalogs/auto-sync")
async def auto_sync_custom_catalogs(
    full_rebuild: bool = False,
    _: bool = Depends(require_auth),
):
    return await auto_sync_custom_catalogs_api(full_rebuild)

@app.get("/api/custom-catalogs/auto-sync/status")
async def auto_catalog_sync_status(_: bool = Depends(require_auth)):
    return await auto_catalog_sync_status_api()

@app.get("/api/custom-catalogs/auto-sync/settings")
async def get_auto_catalog_settings_route(_: bool = Depends(require_auth)):
    return await get_auto_catalog_settings_api()

@app.put("/api/custom-catalogs/auto-sync/settings")
async def update_auto_catalog_settings_route(payload: dict, _: bool = Depends(require_auth)):
    return await update_auto_catalog_settings_api(payload)

@app.get("/api/custom-catalogs/{catalog_id}/items")
async def get_custom_catalog_items(
    catalog_id: str,
    media_type: str | None = None,
    page: int = 1,
    page_size: int = 24,
    _: bool = Depends(require_auth),
):
    return await get_custom_catalog_items_api(catalog_id, media_type, page, page_size)

@app.post("/api/custom-catalogs/{catalog_id}/items")
async def add_custom_catalog_item(catalog_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await add_custom_catalog_item_api(catalog_id, payload)

@app.delete("/api/custom-catalogs/{catalog_id}/items")
async def remove_custom_catalog_item(
    catalog_id: str,
    tmdb_id: int,
    db_index: int,
    media_type: str,
    _: bool = Depends(require_auth),
):
    return await remove_custom_catalog_item_api(catalog_id, tmdb_id, db_index, media_type)

@app.get("/api/system/speedtest")
async def speed_test(
    quality_id: str = Query(...),
    tmdb_id: int = Query(...),
    db_index: int = Query(...),
    media_type: str = Query(...),
    _: bool = Depends(require_auth)
):
    return await speed_test_api(quality_id, tmdb_id, db_index, media_type)

@app.get("/api/system/speedtest/stream")
async def speed_test_stream(
    quality_id: str = Query(...),
    tmdb_id: int = Query(...),
    db_index: int = Query(...),
    media_type: str = Query(...),
    _: bool = Depends(require_auth)
):
    return await speed_test_stream_api(quality_id, tmdb_id, db_index, media_type)

@app.exception_handler(401)
async def auth_exception_handler(request: Request, exc):
    return RedirectResponse(url="/login", status_code=302)
