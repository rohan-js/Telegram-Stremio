from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from Backend.config import Telegram
from Backend.helper.nuvio import (
    build_nuvio_android_intent,
    build_nuvio_deep_link,
    build_nuvio_install_link,
    normalize_nuvio_media_id,
    normalize_nuvio_media_type,
)


router = APIRouter(prefix="/nuvio", tags=["Nuvio"])
templates = Jinja2Templates(directory="Backend/fastapi/templates")
NUVIO_DESKTOP_RELEASES_URL = "https://github.com/NuvioMedia/NuvioDesktop/releases/latest"


@router.get("/open/{media_type}/{media_id}", response_class=HTMLResponse)
async def nuvio_open(
    request: Request,
    media_type: str,
    media_id: str,
    season: int | None = None,
    episode: int | None = None,
):
    try:
        normalized_type = normalize_nuvio_media_type(media_type)
        normalized_id = normalize_nuvio_media_id(media_id)
        deep_link = build_nuvio_deep_link(normalized_type, normalized_id)
        android_intent = build_nuvio_android_intent(deep_link)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    base_url = Telegram.BASE_URL.rstrip("/") or str(request.base_url).rstrip("/")
    token = Telegram.DEFAULT_ADDON_TOKEN
    manifest_url = f"{base_url}/stremio/{token}/manifest.json" if token else None
    install_link = build_nuvio_install_link(manifest_url) if manifest_url else None
    episode_label = None
    if normalized_type == "series" and season and episode:
        episode_label = f"S{int(season):02d}E{int(episode):02d}"

    return templates.TemplateResponse(
        request=request,
        name="nuvio_open.html",
        context={
            "deep_link": deep_link,
            "android_intent": android_intent,
            "manifest_url": manifest_url,
            "install_link": install_link,
            "episode_label": episode_label,
            "desktop_releases_url": NUVIO_DESKTOP_RELEASES_URL,
        },
        headers={
            "Cache-Control": "no-store",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )
