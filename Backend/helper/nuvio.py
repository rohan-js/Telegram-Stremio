import re
from urllib.parse import quote, urlencode


_SAFE_MEDIA_ID = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9:._-]{0,127}$")
_TMDB_ID = re.compile(r"^[0-9]{1,12}$")


def normalize_nuvio_media_type(media_type: str) -> str:
    normalized = str(media_type or "").strip().lower()
    if normalized == "movie":
        return "movie"
    if normalized in {"tv", "series"}:
        return "series"
    raise ValueError("Nuvio media type must be movie or series")


def normalize_nuvio_media_id(media_id: str) -> str:
    normalized = str(media_id or "").strip()
    if not normalized or not _SAFE_MEDIA_ID.fullmatch(normalized):
        raise ValueError("Invalid Nuvio media ID")
    if normalized.lower().startswith("tmdb:"):
        tmdb_id = normalized.split(":", 1)[1]
        if not _TMDB_ID.fullmatch(tmdb_id):
            raise ValueError("Invalid TMDb ID")
        return f"tmdb:{tmdb_id}"
    return normalized


def select_nuvio_media_id(imdb_id=None, tmdb_id=None) -> str | None:
    if imdb_id:
        try:
            return normalize_nuvio_media_id(str(imdb_id))
        except ValueError:
            pass
    if tmdb_id is not None and str(tmdb_id).strip():
        candidate = str(tmdb_id).strip().removeprefix("tmdb:")
        if _TMDB_ID.fullmatch(candidate):
            return f"tmdb:{candidate}"
    return None


def build_nuvio_deep_link(media_type: str, media_id: str) -> str:
    normalized_type = normalize_nuvio_media_type(media_type)
    normalized_id = normalize_nuvio_media_id(media_id)
    # Nuvio itself uses the meta query form for notifications. It is the
    # canonical format and works across more client versions than shorthand
    # links such as nuvio://movie/<id>.
    return f"nuvio://meta?{urlencode({'type': normalized_type, 'id': normalized_id})}"


def build_nuvio_android_intent(deep_link: str) -> str:
    if not str(deep_link or "").startswith("nuvio://"):
        raise ValueError("Invalid Nuvio deep link")
    return f"intent://{deep_link[len('nuvio://'):]}#Intent;scheme=nuvio;end"


def build_nuvio_bridge_url(
    base_url: str,
    media_type: str,
    imdb_id=None,
    tmdb_id=None,
    season=None,
    episode=None,
) -> str | None:
    media_id = select_nuvio_media_id(imdb_id=imdb_id, tmdb_id=tmdb_id)
    if not media_id:
        return None
    normalized_type = normalize_nuvio_media_type(media_type)
    query = {}
    if season is not None:
        try:
            query["season"] = int(season)
        except (TypeError, ValueError):
            pass
    if episode is not None:
        try:
            query["episode"] = int(episode)
        except (TypeError, ValueError):
            pass
    suffix = f"?{urlencode(query)}" if query else ""
    return (
        f"{str(base_url or '').rstrip('/')}/nuvio/open/{normalized_type}/"
        f"{quote(media_id, safe='')}{suffix}"
    )


def build_nuvio_install_link(manifest_url: str) -> str | None:
    manifest_url = str(manifest_url or "").strip()
    for prefix in ("https://", "http://"):
        if manifest_url.lower().startswith(prefix):
            remainder = manifest_url[len(prefix):]
            return f"nuvio://{remainder}" if remainder else None
    return None
