import hashlib
import re
import time
from datetime import datetime

from bson import ObjectId

from Backend import db
from Backend.config import Telegram
from Backend.helper.imdb import extract_first_year, get_detail as cinemeta_detail, search_titles as cinemeta_search_titles
from Backend.helper.metadata import (
    extract_default_id,
    fetch_selected_movie_metadata,
    fetch_selected_tv_metadata,
    search_movie_candidates,
    search_tv_candidates,
)
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

STATUSES = ("pending", "uploaded", "denied", "banned")
_IMDB_RE = re.compile(r"(tt\d{7,10})")
_RATE_BUCKET: dict[str, list[float]] = {}


def requests_enabled() -> bool:
    return bool(getattr(SettingsManager.current(), "content_requests_enabled", False))


def _coll():
    return db.dbs["tracking"]["requests"]


def _norm_type(media_type: str) -> str:
    return "tv" if media_type in ("tv", "series") else "movie"


def _hash_ip(ip: str) -> str:
    return hashlib.sha256((ip or "unknown").encode()).hexdigest()[:16]


def _rate_ok(ip_hash: str, limit: int = 8, window: int = 3600) -> bool:
    now = time.time()
    items = [ts for ts in _RATE_BUCKET.get(ip_hash, []) if now - ts < window]
    if len(items) >= limit:
        _RATE_BUCKET[ip_hash] = items
        return False
    items.append(now)
    _RATE_BUCKET[ip_hash] = items
    return True


async def _cinemeta_name_search(query: str) -> list[dict]:
    out = []
    for media_type, cm_type in (("movie", "movie"), ("tv", "series")):
        try:
            hits = await cinemeta_search_titles(query, cm_type, limit=8)
        except Exception:
            hits = []
        for hit in hits:
            if hit.get("id"):
                out.append(
                    {
                        "media_type": media_type,
                        "tmdb_id": None,
                        "imdb_id": hit.get("id"),
                        "title": hit.get("title") or "Untitled",
                        "year": extract_first_year(hit.get("year")) or None,
                        "poster": hit.get("poster") or "",
                        "overview": "",
                    }
                )
    return out


def _candidate_entry(item: dict, media_type: str) -> dict:
    return {
        "media_type": media_type,
        "tmdb_id": item.get("tmdb_id"),
        "imdb_id": item.get("imdb_id"),
        "title": item.get("title") or "Untitled",
        "year": extract_first_year(item.get("year")) or item.get("year") or None,
        "poster": item.get("poster") or "",
        "overview": "",
    }


async def _cinemeta_id_search(imdb_id: str) -> list[dict]:
    out = []
    for media_type, cm_type in (("movie", "movie"), ("tv", "series")):
        try:
            detail = await cinemeta_detail(imdb_id, cm_type)
        except Exception:
            detail = None
        if detail and detail.get("title"):
            tmdb_id = detail.get("moviedb_id")
            out.append(
                {
                    "media_type": media_type,
                    "tmdb_id": int(tmdb_id) if str(tmdb_id or "").isdigit() else None,
                    "imdb_id": detail.get("id") or imdb_id,
                    "title": detail.get("title"),
                    "year": (detail.get("releaseDetailed") or {}).get("year") or None,
                    "poster": detail.get("poster") or "",
                    "overview": (detail.get("plot") or "")[:220],
                }
            )
    return out


async def search_titles(query: str) -> list[dict]:
    query = (query or "").strip()
    if len(query) < 2:
        return []
    results = []
    imdb_id = None
    tmdb_id = None
    match = _IMDB_RE.search(query)
    if match:
        imdb_id = match.group(1)
    elif query.isdigit():
        tmdb_id = int(query)
    else:
        found_id = extract_default_id(query)
        if found_id and str(found_id).startswith("tt"):
            imdb_id = str(found_id)
        elif found_id and str(found_id).isdigit():
            tmdb_id = int(found_id)
    try:
        if imdb_id:
            results = await _cinemeta_id_search(imdb_id)
        elif tmdb_id:
            results = []
            for media_type, fetcher in (("movie", fetch_selected_movie_metadata), ("tv", fetch_selected_tv_metadata)):
                try:
                    meta = await fetcher(str(tmdb_id))
                    if meta:
                        results.append(
                            {
                                "media_type": media_type,
                                "tmdb_id": meta.get("tmdb_id"),
                                "imdb_id": meta.get("imdb_id"),
                                "title": meta.get("title") or "Untitled",
                                "year": meta.get("year"),
                                "poster": meta.get("poster") or "",
                                "overview": (meta.get("description") or "")[:220],
                            }
                        )
                except Exception:
                    pass
        else:
            movie_results = await search_movie_candidates(query, limit=8)
            tv_results = await search_tv_candidates(query, limit=8)
            results = [_candidate_entry(item, "movie") for item in movie_results]
            results += [_candidate_entry(item, "tv") for item in tv_results]
    except Exception as exc:
        LOGGER.warning("[REQUEST] search failed for %s: %s", query, exc)
        return []

    seen = set()
    clean = []
    for item in results:
        if not item.get("tmdb_id") and not item.get("imdb_id"):
            continue
        key = (item.get("media_type"), item.get("imdb_id") or f"tmdb:{item.get('tmdb_id')}")
        if key in seen:
            continue
        seen.add(key)
        clean.append(item)
    return clean[:15]


async def media_exists(media_type: str, tmdb_id=None, imdb_id=None, title: str = "", year=None) -> bool:
    media_type = _norm_type(media_type)
    try:
        if imdb_id and await db.get_media_details(imdb_id=imdb_id):
            return True
        if tmdb_id:
            for db_index in range(1, db.current_db_index + 1):
                if await db.get_document(media_type, int(tmdb_id), db_index):
                    return True
        if title:
            found = await db.search_documents(query=title, page=1, page_size=8)
            target = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
            want_year = int(year) if str(year or "").isdigit() else 0
            for item in found.get("results") or []:
                if item.get("media_type") != media_type:
                    continue
                if re.sub(r"[^a-z0-9]+", " ", str(item.get("title") or "").lower()).strip() != target:
                    continue
                if want_year and int(item.get("release_year") or 0) != want_year:
                    continue
                return True
    except Exception as exc:
        LOGGER.warning("[REQUEST] library existence check failed: %s", exc)
    return False


async def submit_request(*, media_type, tmdb_id, imdb_id, title, year, poster, client_ip) -> dict:
    if not requests_enabled():
        return {"ok": False, "reason": "disabled"}
    ip_hash = _hash_ip(client_ip)
    if not _rate_ok(ip_hash):
        return {"ok": False, "reason": "rate_limited"}
    media_type = _norm_type(media_type)
    try:
        tmdb_id = int(tmdb_id) if tmdb_id else None
    except (TypeError, ValueError):
        tmdb_id = None
    imdb_id = imdb_id or None
    if not tmdb_id and not imdb_id:
        return {"ok": False, "reason": "invalid"}

    if await media_exists(media_type, tmdb_id, imdb_id, title, year):
        return {"ok": True, "reason": "already_available"}

    now = datetime.utcnow()
    ors = []
    if imdb_id:
        ors.append({"imdb_id": imdb_id})
    if tmdb_id:
        ors.append({"tmdb_id": tmdb_id})
    existing = await _coll().find_one({"media_type": media_type, "$or": ors})
    if existing:
        if existing.get("status") == "banned":
            return {"ok": False, "reason": "banned"}
        update = {
            "$addToSet": {"requesters": ip_hash},
            "$set": {"last_requested_at": now, "updated_at": now},
        }
        if existing.get("status") in ("denied", "uploaded"):
            update["$set"]["status"] = "pending"
        await _coll().update_one({"_id": existing["_id"]}, update)
        return {"ok": True, "reason": "added"}

    doc = {
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title": title or "Untitled",
        "year": int(year) if str(year or "").isdigit() else None,
        "poster": poster or "",
        "status": "pending",
        "requesters": [ip_hash],
        "created_at": now,
        "updated_at": now,
        "last_requested_at": now,
    }
    await _coll().insert_one(doc)
    return {"ok": True, "reason": "created"}


def _public_doc(doc: dict) -> dict:
    out = dict(doc or {})
    if "_id" in out:
        out["_id"] = str(out["_id"])
    out["request_count"] = len(out.get("requesters") or [])
    out.pop("requesters", None)
    return out


async def popular_requests(limit: int = 12) -> list[dict]:
    docs = await _coll().find({"status": "pending"}).sort("last_requested_at", -1).limit(100).to_list(None)
    docs = [_public_doc(doc) for doc in docs]
    docs.sort(key=lambda item: item.get("request_count", 0), reverse=True)
    return docs[:limit]


async def list_requests() -> list[dict]:
    docs = await _coll().find({}).sort("last_requested_at", -1).to_list(None)
    return [_public_doc(doc) for doc in docs]


async def set_status(request_id: str, status: str) -> dict | None:
    if status not in STATUSES:
        return None
    result = await _coll().find_one_and_update(
        {"_id": ObjectId(request_id)},
        {"$set": {"status": status, "updated_at": datetime.utcnow()}},
        return_document=True,
    )
    return _public_doc(result) if result else None


async def delete_request(request_id: str) -> bool:
    result = await _coll().delete_one({"_id": ObjectId(request_id)})
    return result.deleted_count > 0


async def mark_uploaded_for_media(info: dict) -> None:
    if not info:
        return
    media_type = _norm_type(info.get("media_type"))
    ors = []
    if info.get("imdb_id"):
        ors.append({"imdb_id": info.get("imdb_id")})
    if info.get("tmdb_id"):
        ors.append({"tmdb_id": int(info.get("tmdb_id"))})
    if not ors:
        return
    await _coll().update_many(
        {"media_type": media_type, "$or": ors, "status": {"$ne": "banned"}},
        {"$set": {"status": "uploaded", "uploaded_at": datetime.utcnow(), "updated_at": datetime.utcnow()}},
    )
