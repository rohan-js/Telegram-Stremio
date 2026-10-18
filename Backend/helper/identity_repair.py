"""Self-healing identity reconciliation for storage documents.

A doc accepted on a TMDb-only candidate has no ``imdb_id``, which makes it
invisible to Stremio lookups (``get_media_details`` resolves strictly by
``imdb_id``) and renders catalog items with a synthetic id (2026-10-17
Machante incident).  This job heals those docs without any external
provider call:

1. Re-run :func:`enrich_candidate_identity` over the ``match_candidates``
   already persisted on each quality (the same fuzzy merge used at ingest,
   including the one-letter-variant gate).
2. If a unique imdb id is found, store it.
3. Otherwise fall back to the manual-add synthetic convention
   ``tg<abs(tmdb_id)>`` so catalog / meta / stream lookups resolve
   consistently.

Runs once shortly after startup and then daily; a healed-count summary is
DM'd to the owner.  ``/api/admin/identity-audit`` lists whatever remains.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Iterable, Optional

from Backend import db
from Backend.logger import LOGGER

IDENTITY_REPAIR_INTERVAL_SEC = 24 * 3600
_MISSING_ID_QUERY = {
    "$or": [
        {"imdb_id": None},
        {"imdb_id": ""},
        {"imdb_id": {"$exists": False}},
    ]
}
_DOC_PROJECTION = {
    "title": 1,
    "imdb_id": 1,
    "tmdb_id": 1,
    "release_year": 1,
    "year": 1,
    "media_type": 1,
    "telegram": 1,
    "seasons": 1,
}


def _iter_quality_lists(doc: dict) -> Iterable[list]:
    """Yield every telegram quality list on a movie doc or on tv episodes."""
    tg = doc.get("telegram")
    if isinstance(tg, list):
        yield tg
    for season in doc.get("seasons") or []:
        if not isinstance(season, dict):
            continue
        for episode in season.get("episodes") or []:
            if not isinstance(episode, dict):
                continue
            ep_tg = episode.get("telegram")
            if isinstance(ep_tg, list):
                yield ep_tg


def _iter_match_candidate_lists(doc: dict) -> Iterable[list]:
    for quality_list in _iter_quality_lists(doc):
        for quality in quality_list:
            if isinstance(quality, dict) and isinstance(quality.get("match_candidates"), list):
                yield quality["match_candidates"]


def _best_missing_id(doc: dict) -> Optional[str]:
    """Unique imdb id derivable from the doc's persisted match candidates."""
    if doc.get("imdb_id"):
        return None
    from Backend.helper.metadata_matcher import MatchCandidate, enrich_candidate_identity

    media_type = "tv" if (doc.get("media_type") == "tv" or doc.get("seasons")) else "movie"
    selected = MatchCandidate(
        source="database",
        title=str(doc.get("title") or ""),
        year=doc.get("release_year") or doc.get("year"),
        media_type=media_type,
        imdb_id=None,
        tmdb_id=doc.get("tmdb_id"),
    )
    for raw_list in _iter_match_candidate_lists(doc):
        candidates = []
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            candidates.append(
                MatchCandidate(
                    source=str(entry.get("source") or ""),
                    title=str(entry.get("title") or ""),
                    year=entry.get("year"),
                    media_type=str(entry.get("media_type") or media_type),
                    imdb_id=entry.get("imdb_id"),
                    tmdb_id=entry.get("tmdb_id"),
                )
            )
        merged = enrich_candidate_identity(selected, candidates)
        if merged is not None and merged.imdb_id:
            return str(merged.imdb_id)
    return None


def _synthetic_id(doc: dict) -> Optional[str]:
    tmdb = doc.get("tmdb_id")
    try:
        if tmdb is None or tmdb == "":
            return None
        return f"tg{abs(int(tmdb))}"
    except (TypeError, ValueError):
        return None


def _storage_keys() -> list:
    return sorted(k for k in getattr(db, "dbs", {}) if str(k).startswith("storage_"))


async def repair_missing_identities(*, notify: bool = True) -> dict:
    """Heal every storage doc with a missing imdb_id. Returns counters."""
    stats = {"scanned": 0, "healed_real": 0, "healed_synthetic": 0, "unrecoverable": 0, "errors": 0}
    storage_keys = _storage_keys()
    if not storage_keys:
        return stats
    for key in storage_keys:
        storage = db.dbs[key]
        for coll_name in ("movie", "tv"):
            try:
                collection = storage[coll_name]
            except Exception:
                continue
            try:
                cursor = collection.find(_MISSING_ID_QUERY, _DOC_PROJECTION)
                async for doc in cursor:
                    stats["scanned"] += 1
                    real_id = _best_missing_id(doc)
                    new_id = real_id or _synthetic_id(doc)
                    if not new_id:
                        stats["unrecoverable"] += 1
                        continue
                    if real_id:
                        stats["healed_real"] += 1
                    else:
                        stats["healed_synthetic"] += 1
                    LOGGER.info(
                        "Identity repair: %s/%s %r → %s",
                        key, coll_name, doc.get("title"), new_id,
                    )
                    await collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"imdb_id": new_id, "identity_repaired_at": time.time()}},
                    )
            except Exception as exc:
                stats["errors"] += 1
                LOGGER.warning("Identity repair failed for %s/%s: %s", key, coll_name, exc)

    if notify and (stats["healed_real"] or stats["healed_synthetic"]):
        try:
            from Backend.helper.owner_alerts import schedule_owner_alert

            schedule_owner_alert(
                "🧬 Identity repair: healed "
                f"{stats['healed_real']} real + {stats['healed_synthetic']} synthetic id(s) "
                f"across {stats['scanned']} imdb-less doc(s).",
                key="identity-repair",
                cooldown_sec=6 * 3600,
            )
        except Exception:
            pass
    return stats


async def list_imdb_less_docs(limit: int = 200) -> dict:
    """Admin audit view: docs that still have no usable identity."""
    items = []
    for key in _storage_keys():
        storage = db.dbs[key]
        for coll_name in ("movie", "tv"):
            try:
                collection = storage[coll_name]
                cursor = collection.find(_MISSING_ID_QUERY, _DOC_PROJECTION).limit(limit)
                async for doc in cursor:
                    items.append(
                        {
                            "db": key,
                            "collection": coll_name,
                            "id": str(doc.get("_id")),
                            "title": doc.get("title"),
                            "tmdb_id": doc.get("tmdb_id"),
                            "release_year": doc.get("release_year") or doc.get("year"),
                        }
                    )
                    if len(items) >= limit:
                        return {"count": len(items), "truncated": True, "items": items}
            except Exception:
                continue
    return {"count": len(items), "truncated": False, "items": items}


async def identity_repair_loop() -> None:
    while True:
        try:
            await asyncio.sleep(IDENTITY_REPAIR_INTERVAL_SEC)
        except asyncio.CancelledError:
            break
        try:
            stats = await repair_missing_identities()
            if stats["scanned"]:
                LOGGER.info("Identity repair sweep: %s", stats)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Identity repair loop error: %s", exc)


def start_identity_repair_service() -> None:
    """Kick the boot sweep (after startup settles) + the daily loop."""

    async def _boot() -> None:
        await asyncio.sleep(30)
        try:
            stats = await repair_missing_identities()
            LOGGER.info("Identity repair boot sweep: %s", stats)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Identity repair boot sweep failed: %s", exc)
        await identity_repair_loop()

    asyncio.create_task(_boot())
