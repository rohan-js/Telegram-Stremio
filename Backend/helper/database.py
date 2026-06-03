import secrets
import string
import math
from asyncio import create_task
from bson import ObjectId
import motor.motor_asyncio
from datetime import datetime, timezone, timedelta
from pydantic import ValidationError
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from typing import Dict, List, Optional, Tuple, Any

from Backend.logger import LOGGER
from Backend.config import Telegram
import re
from Backend.helper.encrypt import decode_string
from Backend.helper.modal import Episode, MovieSchema, QualityDetail, Season, TVShowSchema
from Backend.helper.task_manager import delete_message
from Backend.helper.torrent_stats import scrape_torrent_trackers
from Backend.helper.host_outbound import build_vps_outbound_sample


def convert_objectid_to_str(document: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in document.items():
        if isinstance(value, ObjectId):
            document[key] = str(value)
        elif isinstance(value, list):
            document[key] = [convert_objectid_to_str(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            document[key] = convert_objectid_to_str(value)
    return document


class Database:
    def __init__(self, db_name: str = "dbFyvio"):
        self.db_uris = Telegram.DATABASE
        self.db_name = db_name

        if len(self.db_uris) < 2:
            raise ValueError("At least 2 database URIs are required (1 for tracking + 1 for storage).")

        self.clients: Dict[str, motor.motor_asyncio.AsyncIOMotorClient] = {}
        self.dbs: Dict[str, motor.motor_asyncio.AsyncIOMotorDatabase] = {}

        self.current_db_index = 1
        self._torrent_stats_refreshing = set()

    async def connect(self):
        try:
            for index, uri in enumerate(self.db_uris):
                client = motor.motor_asyncio.AsyncIOMotorClient(uri)
                db_key = "tracking" if index == 0 else f"storage_{index}"
                self.clients[db_key] = client
                self.dbs[db_key] = client[self.db_name]
                db_type = "Tracking" if index == 0 else f"Storage {index}"

                masked_uri = re.sub(r"://(.*?):.*?@", r"://\1:*****@", uri)
                masked_uri = masked_uri.split('?')[0]
                
                LOGGER.info(f"{db_type} Database connected successfully: {masked_uri}")

            state = await self.dbs["tracking"]["state"].find_one({"_id": "db_index"})
            if not state:
                await self.dbs["tracking"]["state"].insert_one({"_id": "db_index", "current_index": 1})
                self.current_db_index = 1
            else:
                self.current_db_index = state["current_index"]

            LOGGER.info(f"Active storage DB: storage_{self.current_db_index}")

        except Exception as e:
            LOGGER.error(f"Database connection error: {e}")

    async def disconnect(self):
        for client in self.clients.values():
            client.close()
        LOGGER.info("All database connections closed.")

    async def update_current_db_index(self):
        await self.dbs["tracking"]["state"].update_one(
            {"_id": "db_index"},
            {"$set": {"current_index": self.current_db_index}},
            upsert=True
        )

    async def record_vps_outbound_sample(
        self,
        interface: str,
        current_tx_bytes: int,
        monthly_limit_bytes: int,
        force: bool = False,
    ) -> dict:
        now = datetime.now(timezone.utc)
        current_tx_bytes = max(0, int(current_tx_bytes or 0))
        monthly_limit_bytes = max(1, int(monthly_limit_bytes or 1))

        collection = self.dbs["tracking"]["state"]
        existing = await collection.find_one({"_id": "vps_outbound_tx"})
        update = build_vps_outbound_sample(
            existing,
            interface=interface,
            current_tx_bytes=current_tx_bytes,
            monthly_limit_bytes=monthly_limit_bytes,
            now=now,
        )
        await collection.update_one({"_id": "vps_outbound_tx"}, {"$set": update}, upsert=True)
        return {
            "enabled": True,
            "status": "ok",
            "source": "host interface tx",
            **update,
        }

    # -------------------------------
    # Torrent tracker stats cache
    # -------------------------------
    async def get_torrent_stats(self, info_hash: str) -> Optional[dict]:
        if not info_hash:
            return None
        try:
            return await self.dbs["tracking"]["torrent_stats"].find_one({"_id": str(info_hash).lower()})
        except Exception as e:
            LOGGER.debug(f"Torrent stats lookup failed for {info_hash}: {e}")
            return None

    def _torrent_stats_is_fresh(self, stats: Optional[dict]) -> bool:
        if not stats:
            return False
        expires_at = stats.get("expires_at")
        if not expires_at:
            return False
        if expires_at.tzinfo is not None:
            expires_at = expires_at.replace(tzinfo=None)
        return expires_at > datetime.utcnow()

    def queue_torrent_stats_refresh(
        self,
        info_hash: str,
        sources: list,
        torrent_private: bool = False,
        force: bool = False,
    ) -> None:
        if not getattr(Telegram, "TORRENT_STATS_ENABLED", True):
            return
        if torrent_private:
            return
        if not info_hash or not sources:
            return

        info_hash = str(info_hash).lower()
        if info_hash in self._torrent_stats_refreshing:
            return

        async def _refresh_if_needed():
            if not force:
                cached = await self.get_torrent_stats(info_hash)
                if self._torrent_stats_is_fresh(cached):
                    return
            await self.refresh_torrent_stats(info_hash, sources)

        create_task(_refresh_if_needed())

    async def refresh_torrent_stats(self, info_hash: str, sources: list) -> Optional[dict]:
        if not getattr(Telegram, "TORRENT_STATS_ENABLED", True):
            return None
        if not info_hash or not sources:
            return None

        info_hash = str(info_hash).lower()
        if info_hash in self._torrent_stats_refreshing:
            return await self.get_torrent_stats(info_hash)

        self._torrent_stats_refreshing.add(info_hash)
        now = datetime.utcnow()
        try:
            stats = await scrape_torrent_trackers(
                info_hash=info_hash,
                sources=sources,
                max_trackers=max(0, int(getattr(Telegram, "TORRENT_STATS_MAX_TRACKERS", 5) or 5)),
                timeout=max(0.5, float(getattr(Telegram, "TORRENT_STATS_TIMEOUT_SEC", 2.5) or 2.5)),
                concurrency=max(1, int(getattr(Telegram, "TORRENT_STATS_CONCURRENCY", 3) or 3)),
            )
            ok = stats.get("status") == "ok"
            ttl = (
                int(getattr(Telegram, "TORRENT_STATS_TTL_SEC", 21600) or 21600)
                if ok else
                int(getattr(Telegram, "TORRENT_STATS_FAILURE_TTL_SEC", 3600) or 3600)
            )
            doc = {
                "_id": info_hash,
                "info_hash": info_hash,
                "seeders": stats.get("seeders"),
                "peers": stats.get("peers"),
                "completed": stats.get("completed"),
                "checked_at": now,
                "expires_at": now + timedelta(seconds=max(60, ttl)),
                "trackers_checked": int(stats.get("trackers_checked") or 0),
                "trackers_responded": int(stats.get("trackers_responded") or 0),
                "status": stats.get("status") or "unavailable",
                "errors": stats.get("errors") or [],
            }
            await self.dbs["tracking"]["torrent_stats"].update_one(
                {"_id": info_hash},
                {"$set": doc},
                upsert=True,
            )
            return doc
        except Exception as e:
            LOGGER.warning(f"Torrent stats refresh failed for {info_hash}: {e}")
            doc = {
                "_id": info_hash,
                "info_hash": info_hash,
                "seeders": None,
                "peers": None,
                "completed": None,
                "checked_at": now,
                "expires_at": now + timedelta(seconds=max(60, int(getattr(Telegram, "TORRENT_STATS_FAILURE_TTL_SEC", 3600) or 3600))),
                "trackers_checked": 0,
                "trackers_responded": 0,
                "status": "error",
                "errors": [str(e)[:160]],
            }
            try:
                await self.dbs["tracking"]["torrent_stats"].update_one(
                    {"_id": info_hash},
                    {"$set": doc},
                    upsert=True,
                )
            except Exception:
                pass
            return doc
        finally:
            self._torrent_stats_refreshing.discard(info_hash)

    # -------------------------------
    # Torrent download-to-VPS tracking
    # -------------------------------
    async def get_torrent_download(self, info_hash: str) -> Optional[dict]:
        if not info_hash:
            return None
        try:
            return await self.dbs["tracking"]["torrent_downloads"].find_one({"_id": str(info_hash).lower()})
        except Exception as e:
            LOGGER.debug(f"Torrent download lookup failed for {info_hash}: {e}")
            return None

    async def update_torrent_download_job(self, info_hash: str, update_data: dict) -> None:
        if not info_hash:
            return
        update_data = dict(update_data or {})
        update_data.pop("_id", None)
        await self.dbs["tracking"]["torrent_downloads"].update_one(
            {"_id": str(info_hash).lower()},
            {"$set": update_data},
            upsert=True,
        )

    async def get_next_torrent_download_job(self) -> Optional[dict]:
        try:
            now = datetime.utcnow()
            return await self.dbs["tracking"]["torrent_downloads"].find_one_and_update(
                {"status": {"$in": ["queued", "downloading"]}},
                {"$set": {"status": "downloading", "started_at": now, "updated_at": now}},
                sort=[("created_at", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )
        except Exception as e:
            LOGGER.warning(f"Could not claim torrent download job: {e}")
            return None

    async def upsert_torrent_download_job(self, source: dict) -> dict:
        info_hash = str(source.get("info_hash") or "").lower()
        if not info_hash:
            raise ValueError("Missing torrent info_hash")

        now = datetime.utcnow()
        existing = await self.get_torrent_download(info_hash)

        status_message_fields = {
            "requester_user_id": source.get("requester_user_id"),
            "status_message_chat_id": source.get("status_message_chat_id"),
            "status_message_id": source.get("status_message_id"),
            "stremio_link": source.get("stremio_link"),
        }

        if existing and existing.get("status") in {"queued", "downloading"}:
            await self.dbs["tracking"]["torrent_downloads"].update_one(
                {"_id": info_hash},
                {"$set": {**status_message_fields, "updated_at": now}},
            )
            existing.update(status_message_fields)
            existing["updated_at"] = now
            return existing

        if existing and existing.get("status") == "completed":
            await self.dbs["tracking"]["torrent_downloads"].update_one(
                {"_id": info_hash},
                {"$set": {**status_message_fields, "updated_at": now}},
            )
            existing.update(status_message_fields)
            existing["updated_at"] = now
            return existing

        doc = {
            "_id": info_hash,
            "info_hash": info_hash,
            "status": "queued",
            "qbit_hash": None,
            "name": source.get("name") or source.get("filename") or info_hash,
            "title": source.get("title"),
            "size": int(source.get("video_size") or 0),
            "progress": 0.0,
            "downloaded": 0,
            "dlspeed": 0,
            "eta": 0,
            "save_path": None,
            "content_path": None,
            "files": [],
            "sources": source.get("sources") or [],
            "file_idx": source.get("file_idx"),
            "filename": source.get("filename"),
            "torrent_private": bool(source.get("torrent_private", False)),
            "torrent_source_uri": source.get("torrent_source_uri"),
            "torrent_file_chat_id": source.get("torrent_file_chat_id"),
            "torrent_file_msg_id": source.get("torrent_file_msg_id"),
            "origin_chat_id": source.get("origin_chat_id"),
            "origin_msg_id": source.get("origin_msg_id"),
            "media_type": source.get("media_type"),
            "imdb_id": source.get("imdb_id"),
            "season_number": source.get("season_number"),
            "episode_number": source.get("episode_number"),
            "created_at": existing.get("created_at") if existing else now,
            "updated_at": now,
            "started_at": None,
            "last_progress_at": None,
            "last_status_edit_at": None,
            "completed_at": None,
            "failed_at": None,
            "failed_reason": None,
            **status_message_fields,
        }
        await self.dbs["tracking"]["torrent_downloads"].replace_one({"_id": info_hash}, doc, upsert=True)
        return doc

    def _stremio_open_link(self, media_type: str, imdb_id: str, season_number=None, episode_number=None) -> str:
        base_url = Telegram.BASE_URL.rstrip("/")
        token = Telegram.DEFAULT_ADDON_TOKEN
        if not imdb_id:
            return f"{base_url}/stremio/{token}/configure" if token else f"{base_url}/stremio"
        if media_type == "tv":
            return f"{base_url}/stremio/open/series/{imdb_id}?season={int(season_number or 1)}&episode={int(episode_number or 1)}"
        return f"{base_url}/stremio/open/movie/{imdb_id}"

    async def find_torrent_download_source(self, info_hash: str) -> Optional[dict]:
        info_hash = str(info_hash or "").lower()
        if not info_hash:
            return None

        for i in range(1, self.current_db_index + 1):
            db = self.dbs[f"storage_{i}"]

            movie = await db["movie"].find_one({"telegram.info_hash": info_hash})
            if movie:
                for quality in movie.get("telegram", []):
                    if str(quality.get("info_hash") or "").lower() == info_hash:
                        source = dict(quality)
                        source.update(
                            {
                                "media_type": "movie",
                                "imdb_id": movie.get("imdb_id"),
                                "title": movie.get("title"),
                                "stremio_link": self._stremio_open_link("movie", movie.get("imdb_id")),
                            }
                        )
                        return source

            tv = await db["tv"].find_one({"seasons.episodes.telegram.info_hash": info_hash})
            if tv:
                for season in tv.get("seasons", []):
                    for episode in season.get("episodes", []):
                        for quality in episode.get("telegram", []):
                            if str(quality.get("info_hash") or "").lower() == info_hash:
                                source = dict(quality)
                                season_number = season.get("season_number")
                                episode_number = episode.get("episode_number")
                                source.update(
                                    {
                                        "media_type": "tv",
                                        "imdb_id": tv.get("imdb_id"),
                                        "title": tv.get("title"),
                                        "season_number": season_number,
                                        "episode_number": episode_number,
                                        "stremio_link": self._stremio_open_link(
                                            "tv",
                                            tv.get("imdb_id"),
                                            season_number=season_number,
                                            episode_number=episode_number,
                                        ),
                                    }
                                )
                                return source

        return None

    # -------------------------------
    # Custom Catalog Management
    # -------------------------------
    async def create_custom_catalog(self, name: str, visible: bool = True) -> Optional[str]:
        name = (name or "").strip()
        if not name:
            return None

        now = datetime.utcnow()
        result = await self.dbs["tracking"]["custom_catalogs"].insert_one({
            "name": name,
            "visible": bool(visible),
            "auto": False,
            "auto_key": None,
            "items": [],
            "item_count": 0,
            "created_at": now,
            "updated_at": now,
        })
        return str(result.inserted_id)

    async def get_custom_catalogs(self, visible_only: bool = False) -> List[dict]:
        query = {"visible": True} if visible_only else {}
        cursor = self.dbs["tracking"]["custom_catalogs"].find(query).sort("updated_at", DESCENDING)
        catalogs = await cursor.to_list(None)
        return [convert_objectid_to_str(catalog) for catalog in catalogs]

    async def get_custom_catalog(self, catalog_id: str) -> Optional[dict]:
        try:
            catalog = await self.dbs["tracking"]["custom_catalogs"].find_one({"_id": ObjectId(catalog_id)})
            return convert_objectid_to_str(catalog) if catalog else None
        except Exception:
            return None

    async def update_custom_catalog(self, catalog_id: str, name: Optional[str] = None, visible: Optional[bool] = None) -> bool:
        update_data = {"updated_at": datetime.utcnow()}
        if name is not None:
            clean_name = name.strip()
            if clean_name:
                update_data["name"] = clean_name
        if visible is not None:
            update_data["visible"] = bool(visible)

        try:
            result = await self.dbs["tracking"]["custom_catalogs"].update_one(
                {"_id": ObjectId(catalog_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception:
            return False

    async def delete_custom_catalog(self, catalog_id: str) -> bool:
        try:
            result = await self.dbs["tracking"]["custom_catalogs"].delete_one({"_id": ObjectId(catalog_id)})
            return result.deleted_count > 0
        except Exception:
            return False

    async def add_item_to_custom_catalog(
        self, catalog_id: str, tmdb_id: int, db_index: int, media_type: str
    ) -> bool:
        media_type = "tv" if media_type in ["tv", "series"] else "movie"
        item = {
            "tmdb_id": int(tmdb_id),
            "db_index": int(db_index),
            "media_type": media_type,
            "added_at": datetime.utcnow(),
        }
        try:
            result = await self.dbs["tracking"]["custom_catalogs"].update_one(
                {
                    "_id": ObjectId(catalog_id),
                    "items": {
                        "$not": {
                            "$elemMatch": {
                                "tmdb_id": int(tmdb_id),
                                "db_index": int(db_index),
                                "media_type": media_type,
                            }
                        }
                    },
                },
                {
                    "$push": {"items": {"$each": [item], "$position": 0}},
                    "$set": {"updated_at": datetime.utcnow()},
                    "$inc": {"item_count": 1},
                }
            )
            return result.modified_count > 0
        except Exception:
            return False

    async def remove_item_from_custom_catalog(
        self, catalog_id: str, tmdb_id: int, db_index: int, media_type: str
    ) -> bool:
        media_type = "tv" if media_type in ["tv", "series"] else "movie"
        try:
            result = await self.dbs["tracking"]["custom_catalogs"].update_one(
                {
                    "_id": ObjectId(catalog_id),
                    "items": {
                        "$elemMatch": {
                            "tmdb_id": int(tmdb_id),
                            "db_index": int(db_index),
                            "media_type": media_type,
                        }
                    },
                },
                {
                    "$pull": {
                        "items": {
                            "tmdb_id": int(tmdb_id),
                            "db_index": int(db_index),
                            "media_type": media_type,
                        }
                    },
                    "$set": {"updated_at": datetime.utcnow()},
                    "$inc": {"item_count": -1},
                }
            )
            if result.modified_count > 0:
                await self._repair_custom_catalog_item_count(catalog_id)
            return result.modified_count > 0
        except Exception:
            return False

    async def _repair_custom_catalog_item_count(self, catalog_id: str) -> None:
        try:
            catalog = await self.dbs["tracking"]["custom_catalogs"].find_one({"_id": ObjectId(catalog_id)})
            if catalog:
                count = len(catalog.get("items", []) or [])
                await self.dbs["tracking"]["custom_catalogs"].update_one(
                    {"_id": ObjectId(catalog_id)},
                    {"$set": {"item_count": count}},
                )
        except Exception:
            pass

    async def custom_catalog_contains_item(
        self, catalog_id: str, tmdb_id: int, db_index: int, media_type: str
    ) -> bool:
        media_type = "tv" if media_type in ["tv", "series"] else "movie"
        try:
            catalog = await self.dbs["tracking"]["custom_catalogs"].find_one({
                "_id": ObjectId(catalog_id),
                "items": {
                    "$elemMatch": {
                        "tmdb_id": int(tmdb_id),
                        "db_index": int(db_index),
                        "media_type": media_type,
                    }
                }
            })
            return bool(catalog)
        except Exception:
            return False

    async def get_custom_catalog_items(
        self, catalog_id: str, media_type: Optional[str] = None, page: int = 1, page_size: int = 24
    ) -> dict:
        catalog = await self.get_custom_catalog(catalog_id)
        if not catalog:
            return {"catalog": None, "items": [], "total_count": 0, "current_page": page, "total_pages": 0}

        db_media_type = None
        if media_type:
            db_media_type = "tv" if media_type in ["tv", "series"] else "movie"

        raw_items = catalog.get("items", []) or []
        if db_media_type:
            raw_items = [item for item in raw_items if item.get("media_type") == db_media_type]

        total_count = len(raw_items)
        skip = (page - 1) * page_size
        selected_items = raw_items[skip:skip + page_size]

        hydrated_items = []
        for item in selected_items:
            doc = await self.get_document(
                item.get("media_type", "movie"),
                int(item.get("tmdb_id")),
                int(item.get("db_index", 1))
            )
            if doc:
                hydrated_items.append(doc)

        total_pages = (total_count + page_size - 1) // page_size if total_count else 0
        return {
            "catalog": catalog,
            "items": hydrated_items,
            "total_count": total_count,
            "current_page": page,
            "total_pages": total_pages,
        }

    async def update_custom_catalog_item_reference(
        self,
        media_type: str,
        old_tmdb_id: int,
        new_tmdb_id: int,
        db_index: int,
    ) -> None:
        media_type = "tv" if media_type in ["tv", "series"] else "movie"
        if int(old_tmdb_id) == int(new_tmdb_id):
            return
        now = datetime.utcnow()
        await self.dbs["tracking"]["custom_catalogs"].update_many(
            {
                "items": {
                    "$elemMatch": {
                        "tmdb_id": int(old_tmdb_id),
                        "db_index": int(db_index),
                        "media_type": media_type,
                    }
                }
            },
            {
                "$set": {
                    "items.$[item].tmdb_id": int(new_tmdb_id),
                    "items.$[item].added_at": now,
                    "updated_at": now,
                }
            },
            array_filters=[
                {
                    "item.tmdb_id": int(old_tmdb_id),
                    "item.db_index": int(db_index),
                    "item.media_type": media_type,
                }
            ],
        )

    # -------------------------------
    # User Subscription Management
    # -------------------------------
    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self.dbs["tracking"]["users"].find_one({"_id": user_id})

    async def update_user_interaction(self, user_id: int, first_name: str, username: str):
        await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {"$set": {"first_name": first_name, "username": username, "last_interaction": datetime.utcnow()}},
            upsert=True
        )

    async def set_pending_payment(self, user_id: int, plan_duration: int, msg_id: int, price=0, admin_messages: list = None):
        update_data = {
            "pending_payment": {
                "duration": plan_duration,
                "price": price,
                "msg_id": msg_id,
                "date": datetime.utcnow(),
            }
        }
        if admin_messages is not None:
            update_data["pending_payment"]["admin_messages"] = admin_messages
        await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {"$set": update_data},
            upsert=True
        )

    async def approve_payment(self, user_id: int) -> Optional[dict]:
        user = await self.get_user(user_id)
        if not user or "pending_payment" not in user:
            return None

        duration = user["pending_payment"]["duration"]
        
        # Calculate new expiry
        current_expiry = user.get("subscription_expiry")
        now = datetime.utcnow()
        if current_expiry and current_expiry > now:
            from datetime import timedelta
            new_expiry = current_expiry + timedelta(days=duration)
        else:
            from datetime import timedelta
            new_expiry = now + timedelta(days=duration)

        await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {
                "$set": {"subscription_expiry": new_expiry, "subscription_status": "active"},
                "$unset": {"pending_payment": ""}
            }
        )
        return await self.get_user(user_id)

    async def reject_payment(self, user_id: int) -> bool:
        result = await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {"$unset": {"pending_payment": ""}}
        )
        return result.modified_count > 0

    async def get_expired_users(self) -> List[dict]:
        cursor = self.dbs["tracking"]["users"].find({
            "subscription_expiry": {"$lt": datetime.utcnow()},
            "subscription_status": "active"
        })
        return await cursor.to_list(None)

    async def mark_user_expired(self, user_id: int):
        await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {"$set": {"subscription_status": "expired"}}
        )

    async def get_expiring_users(self, hours: int = 24) -> List[dict]:
        from datetime import timedelta
        now = datetime.utcnow()
        target_time = now + timedelta(hours=hours)
        cursor = self.dbs["tracking"]["users"].find({
            "subscription_expiry": {"$gt": now, "$lte": target_time},
            "reminder_sent": {"$ne": True},
            "subscription_status": "active"
        })
        return await cursor.to_list(None)
        
    async def mark_reminder_sent(self, user_id: int):
         await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {"$set": {"reminder_sent": True}}
        )

    # -------------------------------
    # Admin Subscription Management
    # -------------------------------
    async def get_subscription_plans(self) -> List[dict]:
        cursor = self.dbs["tracking"]["sub_plans"].find().sort("days", ASCENDING)
        plans = await cursor.to_list(None)
        return [convert_objectid_to_str(plan) for plan in plans]

    async def add_subscription_plan(self, days: int, price: float) -> Optional[str]:
        result = await self.dbs["tracking"]["sub_plans"].insert_one({
            "days": days,
            "price": price,
            "created_at": datetime.utcnow()
        })
        return str(result.inserted_id)

    async def update_subscription_plan(self, plan_id: str, days: int, price: float) -> bool:
        try:
            result = await self.dbs["tracking"]["sub_plans"].update_one(
                {"_id": ObjectId(plan_id)},
                {"$set": {"days": days, "price": price, "updated_at": datetime.utcnow()}}
            )
            return result.modified_count > 0
        except Exception:
            return False

    async def delete_subscription_plan(self, plan_id: str) -> bool:
        try:
            result = await self.dbs["tracking"]["sub_plans"].delete_one({"_id": ObjectId(plan_id)})
            return result.deleted_count > 0
        except Exception:
            return False

    async def get_all_subscribers(self) -> List[dict]:
        cursor = self.dbs["tracking"]["users"].find({
            "subscription_status": {"$in": ["active", "expired"]}
        }).sort("subscription_expiry", DESCENDING)
        users = await cursor.to_list(None)
        return [convert_objectid_to_str(u) for u in users]

    async def manage_subscriber(self, user_id: int, action: str, days: int = 0) -> bool:
        user = await self.get_user(user_id)
        if not user:
            return False
            
        now = datetime.utcnow()
        if action == "extend" or action == "reduce":
            from datetime import timedelta
            current_expiry = user.get("subscription_expiry")
            
            if action == "extend":
                if current_expiry and current_expiry > now:
                    new_expiry = current_expiry + timedelta(days=days)
                else:
                    new_expiry = now + timedelta(days=days)
            else: # reduce
                if current_expiry:
                    new_expiry = current_expiry - timedelta(days=days)
                    if new_expiry < now:
                        new_expiry = now # Just expire them
                else:
                    new_expiry = now # Already expired or none
            
            status = "active" if new_expiry > now else "expired"
            
            result = await self.dbs["tracking"]["users"].update_one(
                {"_id": user_id},
                {"$set": {"subscription_expiry": new_expiry, "subscription_status": status}}
            )
            return result.modified_count > 0
            
        elif action == "delete":
            result = await self.dbs["tracking"]["users"].update_one(
                {"_id": user_id},
                {"$unset": {"subscription_expiry": "", "subscription_status": ""}}
            )
            return result.modified_count > 0
            
        return False

    async def assign_subscription(self, user_id: int, days: int) -> dict:
        """Upsert a subscription for any user_id, creating a record if it doesn't exist."""
        from datetime import timedelta
        now = datetime.utcnow()

        user = await self.get_user(user_id)
        if user:
            current_expiry = user.get("subscription_expiry")
            if current_expiry and current_expiry > now:
                new_expiry = current_expiry + timedelta(days=days)
            else:
                new_expiry = now + timedelta(days=days)
        else:
            new_expiry = now + timedelta(days=days)

        await self.dbs["tracking"]["users"].update_one(
            {"_id": user_id},
            {
                "$set": {
                    "subscription_expiry": new_expiry,
                    "subscription_status": "active",
                },
                "$setOnInsert": {
                    "_id": user_id,
                    "first_name": f"User {user_id}",
                    "username": None,
                    "created_at": now,
                }
            },
            upsert=True
        )
        return {
            "user_id": user_id,
            "subscription_expiry": new_expiry.isoformat(),
            "subscription_status": "active",
            "days_assigned": days,
        }


    # -------------------------------
    # Helper Methods for Repeated Logic
    # -------------------------------
    def _get_sort_dict(self, sort_params: List[Tuple[str, str]]) -> Dict[str, int]:
        if sort_params:
            sort_field, sort_direction = sort_params[0]
            return {sort_field: DESCENDING if sort_direction.lower() == "desc" else ASCENDING}
        return {"updated_on": DESCENDING}

    async def _paginate_collection(
        self,
        collection_name: str,
        sort_dict: Dict[str, int],
        page: int,
        page_size: int,
        filter_dict: Optional[dict] = None
    ):
        filter_dict = filter_dict or {}
        skip = (page - 1) * page_size
        results = []
        dbs_checked = []
        total_count = 0

        db_counts = []
        for i in range(1, self.current_db_index + 1):
            db_key = f"storage_{i}"
            db = self.dbs[db_key]
            count = await db[collection_name].count_documents(filter_dict)
            db_counts.append((i, count))
            total_count += count

        start_db_index = None
        for db_index, count in reversed(db_counts):
            if skip < count:
                start_db_index = db_index
                break
            skip -= count

        if not start_db_index:
            return [], [], total_count

        for db_index, count in reversed(db_counts):
            if db_index < start_db_index:
                continue

            db_key = f"storage_{db_index}"
            db = self.dbs[db_key]
            dbs_checked.append(db_index)

            cursor = (
                db[collection_name]
                .find(filter_dict)
                .sort(sort_dict)
                .skip(skip if db_index == start_db_index else 0)
                .limit(page_size - len(results))
            )

            docs = await cursor.to_list(None)
            results.extend(docs)

            if len(results) >= page_size:
                break

        return results, dbs_checked, total_count

    async def _move_document(
        self, collection_name: str, document: dict, old_db_index: int
    ) -> bool:
        current_db_key = f"storage_{self.current_db_index}"
        old_db_key = f"storage_{old_db_index}"
        document["db_index"] = self.current_db_index
        try:
            await self.dbs[current_db_key][collection_name].insert_one(document)
            await self.dbs[old_db_key][collection_name].delete_one({"_id": document["_id"]})
            LOGGER.info(f"✅ Moved document {document.get('tmdb_id')} from {old_db_key} to {current_db_key}")
            return True
        except Exception as e:
            LOGGER.error(f"Error moving document to {current_db_key}: {e}")
            return False

    async def _handle_storage_error(self, func, *args, total_storage_dbs: int) -> Optional[Any]:
        next_db_index = (self.current_db_index % total_storage_dbs) + 1
        if next_db_index == 1:
            LOGGER.warning("⚠️ All storage databases are full! Add more.")
            return None
        self.current_db_index = next_db_index
        await self.update_current_db_index()
        LOGGER.info(f"Switched to storage_{self.current_db_index}")
        return await func(*args)

    # -------------------------------
    # Multi Database Method for insert/update/delete/list
    # -------------------------------

    async def insert_media(
        self, metadata_info: dict,
        channel: int, msg_id: int, size: str, name: str
    ) -> Optional[ObjectId]:
        quality_detail = self._quality_from_metadata(metadata_info, size, name)
        
        if metadata_info['media_type'] == "movie":
            media = MovieSchema(
                tmdb_id=metadata_info['tmdb_id'],
                imdb_id=metadata_info['imdb_id'],
                db_index=self.current_db_index,
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                logo=metadata_info['logo'],
                cast=metadata_info['cast'],
                runtime=metadata_info['runtime'],
                media_type=metadata_info['media_type'],
                telegram=[quality_detail]
            )
            return await self.update_movie(media)
        else:
            if metadata_info.get("season_pack") and metadata_info.get("season_pack_episodes"):
                episodes = [
                    Episode(
                        episode_number=int(ep.get("episode_number")),
                        title=ep.get("episode_title") or f"Episode {ep.get('episode_number')}",
                        episode_backdrop=ep.get("episode_backdrop"),
                        overview=ep.get("episode_overview"),
                        released=ep.get("episode_released"),
                        telegram=[quality_detail]
                    )
                    for ep in metadata_info["season_pack_episodes"]
                    if ep.get("episode_number") is not None
                ]
            else:
                episodes = [Episode(
                    episode_number=metadata_info['episode_number'],
                    title=metadata_info['episode_title'],
                    episode_backdrop=metadata_info['episode_backdrop'],
                    overview=metadata_info['episode_overview'],
                    released=metadata_info['episode_released'],
                    telegram=[quality_detail]
                )]

            tv_show = TVShowSchema(
                tmdb_id=metadata_info['tmdb_id'],
                imdb_id=metadata_info['imdb_id'],
                db_index=self.current_db_index,
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                logo=metadata_info['logo'],
                cast=metadata_info['cast'],
                runtime=metadata_info['runtime'],
                media_type=metadata_info['media_type'],
                seasons=[Season(
                    season_number=metadata_info['season_number'],
                    episodes=episodes
                )]
            )
            return await self.update_tv_show(tv_show)

    def _quality_from_metadata(self, metadata_info: dict, size: str, name: str) -> QualityDetail:
        return QualityDetail(
            quality=metadata_info["quality"],
            id=metadata_info["encoded_string"],
            name=name,
            size=size,
            source_type=metadata_info.get("source_type", "telegram"),
            info_hash=metadata_info.get("info_hash"),
            file_idx=metadata_info.get("file_idx"),
            sources=metadata_info.get("sources"),
            filename=metadata_info.get("filename") or name,
            video_size=metadata_info.get("video_size"),
            origin_chat_id=metadata_info.get("origin_chat_id"),
            origin_msg_id=metadata_info.get("origin_msg_id"),
            torrent_private=bool(metadata_info.get("torrent_private", False)),
            torrent_source_uri=metadata_info.get("torrent_source_uri"),
            torrent_file_chat_id=metadata_info.get("torrent_file_chat_id"),
            torrent_file_msg_id=metadata_info.get("torrent_file_msg_id"),
            hidden_from_stremio=bool(metadata_info.get("hidden_from_stremio", False)),
            recommended=bool(metadata_info.get("recommended", False)),
            quality_note=metadata_info.get("quality_note"),
            flagged_duplicate=bool(metadata_info.get("flagged_duplicate", False)),
        )

    def _source_type(self, quality: dict) -> str:
        return quality.get("source_type") or "telegram"

    def _same_replace_group(self, existing_quality: dict, new_quality: dict) -> bool:
        return (
            existing_quality.get("quality") == new_quality.get("quality")
            and self._source_type(existing_quality) == self._source_type(new_quality)
        )

    def _should_delete_telegram_source(self, quality: dict) -> bool:
        return self._source_type(quality) == "telegram" and bool(quality.get("id"))

    def _queue_delete_telegram_source(self, quality: dict) -> None:
        if not self._should_delete_telegram_source(quality):
            return
        create_task(self._delete_encoded_quality_safely(quality.get("id")))

    async def _delete_encoded_quality_safely(self, encoded_id: str) -> None:
        try:
            decoded_data = await decode_string(encoded_id)
            chat_id = int(f"-100{decoded_data['chat_id']}")
            msg_id = int(decoded_data["msg_id"])
            await delete_message(chat_id, msg_id)
        except Exception as e:
            LOGGER.error(f"Failed to queue file for deletion: {e}")

    async def update_movie(self, movie_data: MovieSchema) -> Optional[ObjectId]:
        try:
            movie_dict = movie_data.dict()
        except ValidationError as e:
            LOGGER.error(f"Validation error: {e}")
            return None

        imdb_id = movie_dict["imdb_id"]
        tmdb_id = movie_dict["tmdb_id"]
        title = movie_dict["title"]
        release_year = movie_dict["release_year"]

        quality_to_update = movie_dict["telegram"][0]
        target_quality = quality_to_update["quality"]

        current_db_key = f"storage_{self.current_db_index}"
        total_storage_dbs = len(self.dbs) - 1

        existing_movie = None
        existing_db_key = None
        existing_db_index = None

        for db_index in range(1, total_storage_dbs + 1):
            db_key = f"storage_{db_index}"
            movie = None

            if imdb_id:
                movie = await self.dbs[db_key]["movie"].find_one({"imdb_id": imdb_id})
            if not movie and tmdb_id:
                movie = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
            if not movie and title and release_year:
                movie = await self.dbs[db_key]["movie"].find_one({
                    "title": title,
                    "release_year": release_year
                })

            if movie:
                existing_movie = movie
                existing_db_key = db_key
                existing_db_index = db_index
                break

        # ---------------- INSERT NEW MOVIE ----------------
        if not existing_movie:
            try:
                movie_dict["db_index"] = self.current_db_index
                result = await self.dbs[current_db_key]["movie"].insert_one(movie_dict)
                return result.inserted_id
            except Exception as e:
                LOGGER.error(f"Insertion failed in {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)
                return None

        # ---------------- UPDATE MOVIE ----------------
        movie_id = existing_movie["_id"]
        existing_qualities = existing_movie.get("telegram", [])

        if Telegram.REPLACE_MODE:
            to_delete = [q for q in existing_qualities if self._same_replace_group(q, quality_to_update)]

            for q in to_delete:
                self._queue_delete_telegram_source(q)

            existing_qualities = [
                q for q in existing_qualities if not self._same_replace_group(q, quality_to_update)
            ]
            existing_qualities.append(quality_to_update)

        else:
            # allow duplicate qualities
            existing_qualities.append(quality_to_update)

        existing_movie["telegram"] = existing_qualities
        existing_movie["updated_on"] = datetime.utcnow()

        if existing_db_index != self.current_db_index:
            try:
                if await self._move_document("movie", existing_movie, existing_db_index):
                    return movie_id
            except Exception as e:
                LOGGER.error(f"Error moving movie to {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)

        try:
            await self.dbs[existing_db_key]["movie"].replace_one({"_id": movie_id}, existing_movie)
            return movie_id
        except Exception as e:
            LOGGER.error(f"Failed to update movie {tmdb_id} in {existing_db_key}: {e}")
            if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)

    async def update_tv_show(self, tv_show_data: TVShowSchema) -> Optional[ObjectId]:
        try:
            tv_show_dict = tv_show_data.dict()
        except ValidationError as e:
            LOGGER.error(f"Validation error: {e}")
            return None

        imdb_id = tv_show_dict.get("imdb_id")
        tmdb_id = tv_show_dict.get("tmdb_id")
        title = tv_show_dict["title"]
        release_year = tv_show_dict["release_year"]

        current_db_key = f"storage_{self.current_db_index}"
        total_storage_dbs = len(self.dbs) - 1

        existing_tv = None
        existing_db_key = None
        existing_db_index = None

        for db_index in range(1, total_storage_dbs + 1):
            db_key = f"storage_{db_index}"
            tv = None

            if imdb_id:
                tv = await self.dbs[db_key]["tv"].find_one({"imdb_id": imdb_id})
            if not tv and tmdb_id:
                tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if not tv and title and release_year:
                tv = await self.dbs[db_key]["tv"].find_one({
                    "title": title,
                    "release_year": release_year
                })

            if tv:
                existing_tv = tv
                existing_db_key = db_key
                existing_db_index = db_index
                break

        # ---------------- INSERT NEW TV ----------------
        if not existing_tv:
            try:
                tv_show_dict["db_index"] = self.current_db_index
                result = await self.dbs[current_db_key]["tv"].insert_one(tv_show_dict)
                return result.inserted_id
            except Exception as e:
                LOGGER.error(f"Insertion failed in {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
                return None

        # ---------------- UPDATE TV ----------------
        tv_id = existing_tv["_id"]

        for season in tv_show_dict["seasons"]:
            existing_season = next(
                (s for s in existing_tv["seasons"]
                if s["season_number"] == season["season_number"]),
                None
            )

            if not existing_season:
                existing_tv["seasons"].append(season)
                continue

            for episode in season["episodes"]:
                existing_episode = next(
                    (e for e in existing_season["episodes"]
                    if e["episode_number"] == episode["episode_number"]),
                    None
                )

                if not existing_episode:
                    existing_season["episodes"].append(episode)
                    continue

                existing_episode.setdefault("telegram", [])

                for quality in episode["telegram"]:
                    target_quality = quality.get("quality")

                    if Telegram.REPLACE_MODE:
                        to_delete = [
                            q for q in existing_episode["telegram"]
                            if self._same_replace_group(q, quality)
                        ]

                        for q in to_delete:
                            self._queue_delete_telegram_source(q)

                        existing_episode["telegram"] = [
                            q for q in existing_episode["telegram"]
                            if not self._same_replace_group(q, quality)
                        ]
                        existing_episode["telegram"].append(quality)

                    else:
                        existing_episode["telegram"].append(quality)

        existing_tv["updated_on"] = datetime.utcnow()

        # ---------------- MOVE DB IF NEEDED ----------------
        if existing_db_index != self.current_db_index:
            try:
                if await self._move_document("tv", existing_tv, existing_db_index):
                    return tv_id
            except Exception as e:
                LOGGER.error(f"Error moving TV show to {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
            return tv_id

        try:
            await self.dbs[existing_db_key]["tv"].replace_one({"_id": tv_id}, existing_tv)
            return tv_id
        except Exception as e:
            LOGGER.error(f"Failed to update TV show {tmdb_id} in {existing_db_key}: {e}")
            if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
    
    async def sort_movies(self, sort_params, page, page_size, genre_filter=None):
        sort_dict = self._get_sort_dict(sort_params)
        filter_dict = {"genres": {"$in": [genre_filter]}} if genre_filter else {}
        results, dbs_checked, total_count = await self._paginate_collection(
            "movie", sort_dict, page, page_size, filter_dict=filter_dict
        )
        total_pages = (total_count + page_size - 1) // page_size
        return {
            "total_count": total_count,
            "total_pages": total_pages,
            "databases_checked": dbs_checked,
            "current_page": page,
            "movies": [convert_objectid_to_str(result) for result in results],
        }

    async def sort_tv_shows(self, sort_params, page, page_size, genre_filter=None):
        sort_dict = self._get_sort_dict(sort_params)
        filter_dict = {"genres": {"$in": [genre_filter]}} if genre_filter else {}
        results, dbs_checked, total_count = await self._paginate_collection(
            "tv", sort_dict, page, page_size, filter_dict=filter_dict
        )
        total_pages = (total_count + page_size - 1) // page_size
        return {
            "total_count": total_count,
            "total_pages": total_pages,
            "databases_checked": dbs_checked,
            "current_page": page,
            "tv_shows": [convert_objectid_to_str(result) for result in results],
        }

    async def search_documents(
            self, 
            query: str, 
            page: int, 
            page_size: int
        ) -> dict:

            skip = (page - 1) * page_size
            
            words = query.split()
            regex_query = {
                '$regex': '.*' + '.*'.join(words) + '.*', 
                '$options': 'i'
            }
            
            tv_pipeline = [
                {"$match": {"$or": [
                    {"title": regex_query},
                    {"seasons.episodes.telegram.name": regex_query}
                ]}},
                {"$project": {
                    "_id": 1, "tmdb_id": 1, "title": 1, "genres": 1, "rating": 1, "imdb_id": 1,
                    "release_year": 1, "poster": 1, "backdrop": 1, "description": 1, "logo": 1,
                    "media_type": 1, "db_index": 1
                }}
            ]
            
            movie_pipeline = [
                {"$match": {"$or": [
                    {"title": regex_query},
                    {"telegram.name": regex_query}
                ]}},
                {"$project": {
                    "_id": 1, "tmdb_id": 1, "title": 1, "genres": 1, "rating": 1,
                    "release_year": 1, "poster": 1, "backdrop": 1, "description": 1,
                    "media_type": 1, "db_index": 1, "imdb_id": 1, "logo": 1
                }}
            ]
            
            results = []
            dbs_checked = []
            
            active_db_key = f"storage_{self.current_db_index}"
            active_db = self.dbs[active_db_key]
            dbs_checked.append(self.current_db_index)
            
            tv_results = await active_db["tv"].aggregate(tv_pipeline).to_list(None)
            movie_results = await active_db["movie"].aggregate(movie_pipeline).to_list(None)
            combined = tv_results + movie_results
            results.extend(combined)
            
            if len(results) < page_size:
                previous_db_index = self.current_db_index - 1
                while previous_db_index > 0 and len(results) < page_size:
                    prev_db_key = f"storage_{previous_db_index}"
                    prev_db = self.dbs[prev_db_key]
                    tv_results_prev = await prev_db["tv"].aggregate(tv_pipeline).to_list(None)
                    movie_results_prev = await prev_db["movie"].aggregate(movie_pipeline).to_list(None)
                    combined_prev = tv_results_prev + movie_results_prev
                    results.extend(combined_prev)
                    dbs_checked.append(previous_db_index)
                    previous_db_index -= 1

            total_count = 0
            for db_index in dbs_checked:
                key = f"storage_{db_index}"
                db = self.dbs[key]
                tv_count = await db["tv"].count_documents({
                    "$or": [
                        {"title": regex_query},
                        {"seasons.episodes.telegram.name": regex_query}
                    ]
                })
                movie_count = await db["movie"].count_documents({
                    "$or": [
                        {"title": regex_query},
                        {"telegram.name": regex_query}
                    ]
                })
                total_count += (tv_count + movie_count)
            
            paged_results = results[skip:skip + page_size]

            return {
                "total_count": total_count,
                "results": [convert_objectid_to_str(doc) for doc in paged_results]
            }


    async def get_media_details(
        self, 
        imdb_id: str,
        season_number: Optional[int] = None, 
        episode_number: Optional[int] = None
    ) -> Optional[dict]:

        for db_idx in range(self.current_db_index, 0, -1):
            db_key = f"storage_{db_idx}"
            
            if episode_number is not None and season_number is not None:
                tv_show = await self.dbs[db_key]["tv"].find_one({"imdb_id": imdb_id})
                if tv_show:
                    for season in tv_show.get("seasons", []):
                        if season.get("season_number") == season_number:
                            for episode in season.get("episodes", []):
                                if episode.get("episode_number") == episode_number:
                                    details = convert_objectid_to_str(episode)
                                    details.update({
                                        "imdb_id": imdb_id,
                                        "type": "tv",
                                        "season_number": season_number,
                                        "episode_number": episode_number,
                                        "backdrop": episode.get("episode_backdrop"),
                                        "db_index": db_idx
                                    })
                                    return details
            
            elif season_number is not None:
                tv_show = await self.dbs[db_key]["tv"].find_one({"imdb_id": imdb_id})
                if tv_show:
                    for season in tv_show.get("seasons", []):
                        if season.get("season_number") == season_number:
                            details = convert_objectid_to_str(season)
                            details.update({
                                "imdb_id": imdb_id,
                                "type": "tv",
                                "season_number": season_number,
                                "db_index": db_idx
                            })
                            return details
            
            else:
                tv_doc = await self.dbs[db_key]["tv"].find_one({"imdb_id": imdb_id})
                if tv_doc:
                    tv_doc = convert_objectid_to_str(tv_doc)
                    tv_doc["type"] = "tv"
                    tv_doc["db_index"] = db_idx
                    return tv_doc
                
                movie_doc = await self.dbs[db_key]["movie"].find_one({"imdb_id": imdb_id})
                if movie_doc:
                    movie_doc = convert_objectid_to_str(movie_doc)
                    movie_doc["type"] = "movie"
                    movie_doc["db_index"] = db_idx
                    return movie_doc
        
        return None

    # -------------------------------
    # DB Method for Edit Post
    # -------------------------------

    async def get_document(self, media_type: str, tmdb_id: int, db_index: int) -> Optional[Dict[str, Any]]:
        db_key = f"storage_{db_index}"
        if media_type.lower() in ["tv", "series"]:
            collection_name = "tv"
        else:
            collection_name = "movie"
        document = await self.dbs[db_key][collection_name].find_one({"tmdb_id": int(tmdb_id)})
        return convert_objectid_to_str(document) if document else None

    async def update_document(
        self, media_type: str, tmdb_id: int, db_index: int, update_data: Dict[str, Any]
    ):
        update_data.pop('_id', None)
        db_key = f"storage_{db_index}"
        if media_type.lower() in ["tv", "series"]:
            collection_name = "tv"
        else:
            collection_name = "movie"
        collection = self.dbs[db_key][collection_name]

        try:
            result = await collection.update_one({"tmdb_id": int(tmdb_id)}, {"$set": update_data})

            return result.modified_count > 0

        except Exception as e:
            err_str = str(e).lower()
            LOGGER.error(f"Error updating document in {db_key}: {e}")
            if "storage" in err_str or "quota" in err_str:
                total_storage_dbs = len(self.dbs) - 1
                db_index_int = int(db_index)
                next_db_index = (db_index_int % total_storage_dbs) + 1
                if next_db_index == 1:
                    LOGGER.warning("⚠️ All storage databases are full! Add more.")
                    return False

                new_db_key = f"storage_{next_db_index}"
                LOGGER.info(f"Switching from {db_key} to {new_db_key} due to storage error.")

                try:
                    old_doc = await self.dbs[db_key][collection_name].find_one({"tmdb_id": int(tmdb_id)})
                    if not old_doc:
                        LOGGER.error(f"Document with tmdb_id {tmdb_id} not found in {db_key} during migration.")
                        return False

                    old_doc.update(update_data)
                    old_doc["db_index"] = next_db_index
                    old_doc.pop("_id", None)
                    insert_result = await self.dbs[new_db_key][collection_name].insert_one(old_doc)
                    LOGGER.info(f"Inserted document {insert_result.inserted_id} into {new_db_key}")
                    await self.dbs[db_key][collection_name].delete_one({"tmdb_id": int(tmdb_id)})
                    LOGGER.info(f"Deleted document tmdb_id {tmdb_id} from {db_key}")
                    self.current_db_index = next_db_index
                    await self.update_current_db_index()
                    LOGGER.info(f"Switched to {new_db_key} and document migrated successfully.")
                    return True

                except Exception as migrate_error:
                    LOGGER.error(f"Error migrating document tmdb_id {tmdb_id} to {new_db_key}: {migrate_error}")
                    return False
            raise

    async def replace_media_metadata(
        self,
        media_type: str,
        tmdb_id: int,
        db_index: int,
        metadata: Dict[str, Any]
    ) -> Optional[dict]:
        db_key = f"storage_{db_index}"
        collection_name = "tv" if media_type.lower() in ["tv", "series"] else "movie"
        collection = self.dbs[db_key][collection_name]

        current_doc = await collection.find_one({"tmdb_id": int(tmdb_id)})
        if not current_doc:
            return None

        current_doc.pop("_id", None)
        old_tmdb_id = int(tmdb_id)
        new_tmdb_id = int(metadata.get("tmdb_id") or old_tmdb_id)
        now = datetime.utcnow()

        common_update = {
            "tmdb_id": new_tmdb_id,
            "imdb_id": metadata.get("imdb_id") or current_doc.get("imdb_id"),
            "title": metadata.get("title") or current_doc.get("title"),
            "release_year": metadata.get("release_year", current_doc.get("release_year")),
            "rating": metadata.get("rating", current_doc.get("rating")),
            "description": metadata.get("description", current_doc.get("description")),
            "poster": metadata.get("poster", current_doc.get("poster")),
            "backdrop": metadata.get("backdrop", current_doc.get("backdrop")),
            "logo": metadata.get("logo", current_doc.get("logo")),
            "genres": metadata.get("genres", current_doc.get("genres", [])),
            "cast": metadata.get("cast", current_doc.get("cast", [])),
            "runtime": metadata.get("runtime", current_doc.get("runtime")),
            "updated_on": now,
        }

        if collection_name == "movie":
            preserved_telegram = current_doc.get("telegram", [])
            current_doc.update({
                **common_update,
                "media_type": "movie",
                "telegram": preserved_telegram,
            })
        else:
            preserved_seasons = current_doc.get("seasons", [])
            current_doc.update({
                **common_update,
                "media_type": "tv",
                "seasons": preserved_seasons,
            })

        current_doc.pop("auto_catalog", None)
        current_doc.pop("auto_tags_updated_at", None)

        if new_tmdb_id != old_tmdb_id:
            await collection.delete_one({"tmdb_id": old_tmdb_id})
            await collection.replace_one({"tmdb_id": new_tmdb_id}, current_doc, upsert=True)
            await self.update_custom_catalog_item_reference(
                collection_name,
                old_tmdb_id,
                new_tmdb_id,
                db_index,
            )
        else:
            await collection.replace_one({"tmdb_id": old_tmdb_id}, current_doc, upsert=True)

        updated_doc = await collection.find_one({"tmdb_id": new_tmdb_id})
        return convert_objectid_to_str(updated_doc) if updated_doc else None

    async def delete_document(self, media_type: str, tmdb_id: int, db_index: int) -> bool:
        db_key = f"storage_{db_index}"

        if media_type == "Movie":
            doc = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
            if doc and "telegram" in doc:
                for quality in doc["telegram"]:
                    self._queue_delete_telegram_source(quality)
            
            result = await self.dbs[db_key]["movie"].delete_one({"tmdb_id": tmdb_id})
        else:
            doc = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if doc and "seasons" in doc:
                for season in doc["seasons"]:
                    for episode in season.get("episodes", []):
                        for quality in episode.get("telegram", []):
                            self._queue_delete_telegram_source(quality)
            
            result = await self.dbs[db_key]["tv"].delete_one({"tmdb_id": tmdb_id})
        
        if result.deleted_count > 0:
            LOGGER.info(f"{media_type} with tmdb_id {tmdb_id} deleted successfully.")
            return True
        LOGGER.info(f"No document found with tmdb_id {tmdb_id}.")
        return False

    async def get_title_by_stream_id(self, stream_id_hash: str) -> Optional[str]:
        """Look up the original media title across all storage DBs using the telegram file ID hash.
        For TV shows, it includes the Season and Episode number in the title."""
        for i in range(1, self.current_db_index + 1):
            db = self.dbs[f"storage_{i}"]
            
            # Check Movies
            movie = await db["movie"].find_one({"telegram.id": stream_id_hash})
            if movie and "telegram" in movie:
                for t in movie["telegram"]:
                    if t.get("id") == stream_id_hash:
                        return movie.get("title")

            # Check TV Shows
            tv = await db["tv"].find_one({"seasons.episodes.telegram.id": stream_id_hash})
            if tv and "seasons" in tv:
                title = tv.get("title", "Unknown Series")
                for season in tv.get("seasons", []):
                    for episode in season.get("episodes", []):
                        for t in episode.get("telegram", []):
                            if t.get("id") == stream_id_hash:
                                s_num = season.get("season_number", 0)
                                e_num = episode.get("episode_number", 0)
                                return f"{title} S{s_num:02d}E{e_num:02d}"

        return None

    async def delete_media_by_stream_id(self, stream_id_hash: str) -> bool:
        """Finds and removes a specific stream quality by its hash across all DBs. 
        If it's the last quality, it cleans up the movie or episode/season/show."""
        for i in range(1, self.current_db_index + 1):
            db = self.dbs[f"storage_{i}"]
            
            # Check Movies
            movie = await db["movie"].find_one({"telegram.id": stream_id_hash})
            if movie:
                movie["telegram"] = [q for q in movie.get("telegram", []) if q.get("id") != stream_id_hash]
                if len(movie["telegram"]) == 0:
                    await db["movie"].delete_one({"_id": movie["_id"]})
                else:
                    movie['updated_on'] = datetime.utcnow()
                    await db["movie"].replace_one({"_id": movie["_id"]}, movie)
                return True

            # Check TV Shows
            tv = await db["tv"].find_one({"seasons.episodes.telegram.id": stream_id_hash})
            if tv:
                for season in tv.get("seasons", []):
                    for episode in season.get("episodes", []):
                        for q in episode.get("telegram", []):
                            if q.get("id") == stream_id_hash:
                                episode["telegram"] = [t for t in episode.get("telegram", []) if t.get("id") != stream_id_hash]
                                if len(episode["telegram"]) == 0:
                                    season["episodes"] = [e for e in season.get("episodes", []) if e.get("episode_number") != episode.get("episode_number")]
                                    if len(season["episodes"]) == 0:
                                        tv["seasons"] = [s for s in tv.get("seasons", []) if s.get("season_number") != season.get("season_number")]
                                        if len(tv["seasons"]) == 0:
                                            await db["tv"].delete_one({"_id": tv["_id"]})
                                            return True
                                tv['updated_on'] = datetime.utcnow()
                                await db["tv"].replace_one({"_id": tv["_id"]}, tv)
                                return True
        return False

    async def delete_media_by_origin(self, origin_chat_id: int, origin_msg_id: int) -> bool:
        """Remove every quality entry created from a source Telegram post."""
        changed = False
        for i in range(1, self.current_db_index + 1):
            db = self.dbs[f"storage_{i}"]

            movie_cursor = db["movie"].find({
                "telegram.origin_chat_id": int(origin_chat_id),
                "telegram.origin_msg_id": int(origin_msg_id),
            })
            for movie in await movie_cursor.to_list(None):
                before = len(movie.get("telegram", []))
                movie["telegram"] = [
                    q for q in movie.get("telegram", [])
                    if not (
                        q.get("origin_chat_id") == int(origin_chat_id)
                        and q.get("origin_msg_id") == int(origin_msg_id)
                    )
                ]
                if len(movie["telegram"]) == before:
                    continue
                if movie["telegram"]:
                    movie["updated_on"] = datetime.utcnow()
                    await db["movie"].replace_one({"_id": movie["_id"]}, movie)
                else:
                    await db["movie"].delete_one({"_id": movie["_id"]})
                changed = True

            tv_cursor = db["tv"].find({
                "seasons.episodes.telegram.origin_chat_id": int(origin_chat_id),
                "seasons.episodes.telegram.origin_msg_id": int(origin_msg_id),
            })
            for tv in await tv_cursor.to_list(None):
                doc_changed = False
                new_seasons = []
                for season in tv.get("seasons", []):
                    new_episodes = []
                    for episode in season.get("episodes", []):
                        before = len(episode.get("telegram", []))
                        episode["telegram"] = [
                            q for q in episode.get("telegram", [])
                            if not (
                                q.get("origin_chat_id") == int(origin_chat_id)
                                and q.get("origin_msg_id") == int(origin_msg_id)
                            )
                        ]
                        if len(episode["telegram"]) != before:
                            doc_changed = True
                        if episode.get("telegram"):
                            new_episodes.append(episode)
                    season["episodes"] = new_episodes
                    if season["episodes"]:
                        new_seasons.append(season)

                if not doc_changed:
                    continue
                tv["seasons"] = new_seasons
                if tv["seasons"]:
                    tv["updated_on"] = datetime.utcnow()
                    await db["tv"].replace_one({"_id": tv["_id"]}, tv)
                else:
                    await db["tv"].delete_one({"_id": tv["_id"]})
                changed = True

        return changed

    async def delete_movie_quality(self, tmdb_id: int, db_index: int, id: str) -> bool:
        db_key = f"storage_{db_index}"
        movie = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
        
        if not movie or "telegram" not in movie:
            return False

        for q in movie["telegram"]:
            if q.get("id") == id:
                self._queue_delete_telegram_source(q)
                break
        
        original_len = len(movie["telegram"])
        movie["telegram"] = [q for q in movie["telegram"] if q.get("id") != id]
        
        if len(movie["telegram"]) == original_len:
            return False
        
        movie['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["movie"].replace_one({"tmdb_id": tmdb_id}, movie)
        return result.modified_count > 0

    async def delete_tv_episode(self, tmdb_id: int, db_index: int, season_number: int, episode_number: int) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        found = False
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for ep in season["episodes"]:
                    if ep.get("episode_number") == episode_number:
                        for quality in ep.get("telegram", []):
                            self._queue_delete_telegram_source(quality)
                        break
                
                original_len = len(season["episodes"])
                season["episodes"] = [ep for ep in season["episodes"] if ep.get("episode_number") != episode_number]
                found = original_len > len(season["episodes"])
                break
        
        if not found:
            return False
        
        tv['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
        return result.modified_count > 0

    async def delete_tv_season(self, tmdb_id: int, db_index: int, season_number: int) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for episode in season.get("episodes", []):
                    for quality in episode.get("telegram", []):
                        self._queue_delete_telegram_source(quality)
                break
        
        original_len = len(tv["seasons"])
        tv["seasons"] = [s for s in tv["seasons"] if s.get("season_number") != season_number]
        
        if len(tv["seasons"]) == original_len:
            return False
        
        tv['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
        return result.modified_count > 0

    async def delete_tv_quality(self, tmdb_id: int, db_index: int, season_number: int, episode_number: int, id: str) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        found = False
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for episode in season["episodes"]:
                    if episode.get("episode_number") == episode_number and "telegram" in episode:
                        for q in episode["telegram"]:
                            if q.get("id") == id:
                                self._queue_delete_telegram_source(q)
                                break
                        
                        original_len = len(episode["telegram"])
                        episode["telegram"] = [q for q in episode["telegram"] if q.get("id") != id]
                        found = original_len > len(episode["telegram"])
                        break
        
        if not found:
            return False
        tv['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
        return result.modified_count > 0


    # Get per-DB statistics (movies, tv shows, used size, etc.)
    async def get_database_stats(self):
        stats = []
        for key in self.dbs.keys():
            if key.startswith("storage_"):
                db = self.dbs[key]
                movie_count = await db["movie"].count_documents({})
                tv_count = await db["tv"].count_documents({})
                db_stats = await db.command("dbstats")
                stats.append({
                    "db_name": key,
                    "movie_count": movie_count,
                    "tv_count": tv_count,
                    "storageSize": db_stats.get("storageSize", 0),
                    "dataSize": db_stats.get("dataSize", 0)
                })
        return stats



    # -------------------------------
    # API Token Methods
    # -------------------------------

    async def add_api_token(self, name: str, daily_limit_gb: float = None, monthly_limit_gb: float = None, user_id: int = None) -> dict:
        # If a user_id is provided, return existing token if already created
        if user_id:
            existing = await self.dbs["tracking"]["api_tokens"].find_one({"user_id": user_id})
            if existing:
                return convert_objectid_to_str(existing)

        alphabet = string.ascii_letters + string.digits
        token = ''.join(secrets.choice(alphabet) for _ in range(32))
        
        token_doc = {
            "name": name,
            "token": token,
            "user_id": user_id,
            "created_at": datetime.utcnow(),
            "limits": {
                "daily_limit_gb": daily_limit_gb if daily_limit_gb else 0,
                "monthly_limit_gb": monthly_limit_gb if monthly_limit_gb else 0
            },
            "usage": {
                "total_bytes": 0,
                "daily": {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "bytes": 0},
                "monthly": {"month": datetime.now(timezone.utc).strftime("%Y-%m"), "bytes": 0}
            }
        }
        
        await self.dbs["tracking"]["api_tokens"].insert_one(token_doc)
        return convert_objectid_to_str(token_doc)

    async def get_api_token(self, token: str) -> Optional[dict]:
        doc = await self.dbs["tracking"]["api_tokens"].find_one({"token": token})
        return convert_objectid_to_str(doc) if doc else None

    async def get_all_api_tokens(self) -> List[dict]:
        cursor = self.dbs["tracking"]["api_tokens"].find().sort("created_at", DESCENDING)
        tokens = await cursor.to_list(None)
        return [convert_objectid_to_str(token) for token in tokens]

    async def revoke_api_token(self, token: str) -> bool:
        result = await self.dbs["tracking"]["api_tokens"].delete_one({"token": token})
        return result.deleted_count > 0

    async def link_token_user(self, token: str, user_id: int) -> bool:
        """Link an existing token to a Telegram user_id."""
        result = await self.dbs["tracking"]["api_tokens"].update_one(
            {"token": token},
            {"$set": {"user_id": user_id}}
        )
        return result.modified_count > 0

    async def update_token_usage(self, token: str, bytes_delta: int):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        month_str = datetime.now(timezone.utc).strftime("%Y-%m")
        
        token_doc = await self.dbs["tracking"]["api_tokens"].find_one({"token": token})
        if not token_doc:
             return

        current_daily = token_doc.get("usage", {}).get("daily", {})
        if current_daily.get("date") != today_str:
            await self.dbs["tracking"]["api_tokens"].update_one(
                {"token": token},
                {"$set": {"usage.daily": {"date": today_str, "bytes": 0}}}
            )

        current_monthly = token_doc.get("usage", {}).get("monthly", {})
        if current_monthly.get("month") != month_str:
            await self.dbs["tracking"]["api_tokens"].update_one(
                {"token": token},
                {"$set": {"usage.monthly": {"month": month_str, "bytes": 0}}}
            )

        await self.dbs["tracking"]["api_tokens"].update_one(
            {"token": token},
            {
                "$inc": {
                    "usage.total_bytes": bytes_delta,
                    "usage.daily.bytes": bytes_delta,
                    "usage.monthly.bytes": bytes_delta
                }
            }
        )

    async def update_api_token_limits(self, token: str, daily_limit_gb: float, monthly_limit_gb: float) -> bool:
        result = await self.dbs["tracking"]["api_tokens"].update_one(
            {"token": token},
            {"$set": {
                "limits": {
                    "daily_limit_gb": daily_limit_gb if daily_limit_gb else 0,
                    "monthly_limit_gb": monthly_limit_gb if monthly_limit_gb else 0
                }
            }}
        )
        return result.modified_count > 0

    # -------------------------------
    # Admin / Link Checker Methods
    # -------------------------------
    async def flag_dead_link(self, media_type: str, tmdb_id: int, db_index: int, quality_id: str) -> bool:
        """
        Flags a specific telegram quality entry as 'is_dead: True'.
        """
        db_key = f"storage_{db_index}"
        
        if media_type == "movie":
            # Direct update in the telegram array for movies
            result = await self.dbs[db_key]["movie"].update_one(
                {"tmdb_id": tmdb_id, "telegram.id": quality_id},
                {"$set": {"telegram.$.is_dead": True, "updated_on": datetime.utcnow()}}
            )
            return result.modified_count > 0
            
        elif media_type == "tv":
            # Nested update for TV (arrayFilters needed since we don't know the exact indices)
            # Find the TV show docs
            tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if not tv or "seasons" not in tv:
                return False
                
            found = False
            for s_idx, season in enumerate(tv["seasons"]):
                for e_idx, episode in enumerate(season.get("episodes", [])):
                    for q_idx, quality in enumerate(episode.get("telegram", [])):
                        if quality.get("id") == quality_id:
                            tv["seasons"][s_idx]["episodes"][e_idx]["telegram"][q_idx]["is_dead"] = True
                            found = True
                            break
                    if found: break
                if found: break
                
            if found:
                tv["updated_on"] = datetime.utcnow()
                result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
                return result.modified_count > 0
                
        return False

    async def get_all_dead_links(self) -> List[dict]:
        """
        Scans all active storage databases for both movies and TV shows, returning a
        flattened list of dead links with their metadata for the Admin UI.
        """
        dead_links = []
        
        for i in range(1, self.current_db_index + 1):
            db_key = f"storage_{i}"
            db = self.dbs[db_key]
            
            # --- Scan Movies ---
            # Match any movie where at least one telegram entry has is_dead=True
            movie_cursor = db["movie"].find({"telegram.is_dead": True})
            async for movie in movie_cursor:
                for quality in movie.get("telegram", []):
                    if quality.get("is_dead"):
                        dead_links.append({
                            "type": "movie",
                            "tmdb_id": movie.get("tmdb_id"),
                            "db_index": movie.get("db_index", i),
                            "title": movie.get("title"),
                            "year": movie.get("year"),
                            "poster": movie.get("poster"),
                            "quality_id": quality.get("id"),
                            "quality": quality.get("quality"),
                            "size": quality.get("size"),
                            "date_added": quality.get("date_added")
                        })
                        
            # --- Scan TV Shows ---
            # Match any TV where seasons.episodes.telegram.is_dead=True
            tv_cursor = db["tv"].find({"seasons.episodes.telegram.is_dead": True})
            async for tv in tv_cursor:
                title = tv.get("title")
                year = tv.get("year")
                poster = tv.get("poster")
                for season in tv.get("seasons", []):
                    s_num = season.get("season_number")
                    for ep in season.get("episodes", []):
                        e_num = ep.get("episode_number")
                        for quality in ep.get("telegram", []):
                            if quality.get("is_dead"):
                                dead_links.append({
                                    "type": "tv",
                                    "tmdb_id": tv.get("tmdb_id"),
                                    "db_index": tv.get("db_index", i),
                                    "title": f"{title} (S{s_num:02d}E{e_num:02d})",
                                    "year": year,
                                    "poster": poster,
                                    "season": s_num,
                                    "episode": e_num,
                                    "quality_id": quality.get("id"),
                                    "quality": quality.get("quality"),
                                    "size": quality.get("size"),
                                    "date_added": quality.get("date_added")
                                })
                                
        return dead_links

    # -------------------------------
    # Stream Analytics
    # -------------------------------

    async def log_stream_stats(self, stats: dict) -> None:
        """Persist a finished-stream record to the tracking DB for analytics."""
        try:
            meta = stats.get("meta", {}) or {}
            record = {
                "stream_id":   stats.get("stream_id"),
                "msg_id":      stats.get("msg_id"),
                "chat_id":     stats.get("chat_id"),
                "dc_id":       stats.get("dc_id"),
                "title":       meta.get("title"),
                "filename":    meta.get("filename"),
                "source_type": meta.get("source_type", "telegram"),
                "user_name":   meta.get("user_name"),
                "client_host": meta.get("client_host"),
                "user_agent":  meta.get("user_agent"),
                "request_path": meta.get("request_path"),
                "request_range": meta.get("request_range"),
                "client_index": stats.get("client_index"),
                "total_bytes": stats.get("total_bytes", 0),
                "ttfb_sec":    stats.get("ttfb_sec"),
                "duration_sec": round(stats.get("duration", 0.0), 2),
                "avg_mbps":    round(stats.get("avg_mbps", 0.0), 3),
                "peak_mbps":   round(stats.get("peak_mbps", 0.0), 3),
                "status":      stats.get("status", "finished"),
                "parallelism": stats.get("parallelism"),
                "chunk_size":  stats.get("chunk_size"),
                "cached":      stats.get("cached", False),
                "served_via":  stats.get("served_via", "telegram"),
                "chunk_timeouts": stats.get("chunk_timeouts", 0),
                "chunk_errors":   stats.get("chunk_errors", 0),
                "fallback_chunks": stats.get("fallback_chunks", 0),
                "zero_pad_chunks": stats.get("zero_pad_chunks", 0),
                "buffering_events": stats.get("buffering_events", 0),
                "buffering_rate": stats.get("buffering_rate", 0.0),
                "error_reason": stats.get("error_reason"),
                "adaptive_prefetch": meta.get("adaptive_prefetch"),
                "smart_routing": meta.get("smart_routing"),
                "route_attempts": stats.get("route_attempts", [])[-10:],
                "logged_at":   datetime.utcnow(),
            }
            await self.dbs["tracking"]["stream_analytics"].insert_one(record)
        except Exception as e:
            LOGGER.warning(f"Stream analytics log failed: {e}")

    async def get_stream_analytics(self, limit: int = 200) -> dict:
        """Return summary stats + recent stream records from the tracking DB."""
        try:
            col = self.dbs["tracking"]["stream_analytics"]

            # Aggregate totals
            pipeline = [
                {"$group": {
                    "_id": None,
                    "total_streams":     {"$sum": 1},
                    "cached_streams":    {"$sum": {"$cond": [{"$eq": ["$cached", True]}, 1, 0]}},
                    "error_streams":     {"$sum": {"$cond": [{"$eq": ["$status", "error"]}, 1, 0]}},
                    "cancelled_streams": {"$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}},
                    "non_finished_streams": {"$sum": {"$cond": [{"$ne": ["$status", "finished"]}, 1, 0]}},
                    "total_bytes":       {"$sum": "$total_bytes"},
                    "avg_speed":         {"$avg": "$avg_mbps"},
                    "peak_speed":        {"$max": "$peak_mbps"},
                    "avg_duration":      {"$avg": "$duration_sec"},
                    "avg_ttfb_sec":      {"$avg": "$ttfb_sec"},
                    "avg_buffering_rate": {"$avg": "$buffering_rate"},
                    "buffering_streams": {"$sum": {"$cond": [{"$gt": ["$buffering_events", 0]}, 1, 0]}},
                }},
            ]
            agg = await col.aggregate(pipeline).to_list(1)
            summary = agg[0] if agg else {}
            summary.pop("_id", None)

            # Per-client breakdown
            per_client_pipeline = [
                {"$group": {
                    "_id":          "$client_index",
                    "streams":      {"$sum": 1},
                    "cached_streams": {"$sum": {"$cond": [{"$eq": ["$cached", True]}, 1, 0]}},
                    "error_streams": {"$sum": {"$cond": [{"$eq": ["$status", "error"]}, 1, 0]}},
                    "cancelled_streams": {"$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}},
                    "non_finished_streams": {"$sum": {"$cond": [{"$ne": ["$status", "finished"]}, 1, 0]}},
                    "avg_mbps":     {"$avg": "$avg_mbps"},
                    "peak_mbps":    {"$max": "$peak_mbps"},
                    "total_bytes":  {"$sum": "$total_bytes"},
                    "avg_ttfb_sec": {"$avg": "$ttfb_sec"},
                    "avg_buffering_rate": {"$avg": "$buffering_rate"},
                    "buffering_streams": {"$sum": {"$cond": [{"$gt": ["$buffering_events", 0]}, 1, 0]}},
                }},
                {"$sort": {"_id": 1}},
            ]
            per_client = await col.aggregate(per_client_pipeline).to_list(None)
            for row in per_client:
                row["client_index"] = row.pop("_id")
                row["avg_mbps"]     = round(row.get("avg_mbps", 0), 3)
                row["peak_mbps"]    = round(row.get("peak_mbps", 0), 3)
                row["avg_ttfb_sec"]  = round(row.get("avg_ttfb_sec", 0) or 0, 3)
                row["avg_buffering_rate"] = round(row.get("avg_buffering_rate", 0) or 0, 4)

            # Recent records (newest first)
            recent_cursor = col.find(
                {},
                {"_id": 0, "stream_id": 1, "client_index": 1, "dc_id": 1,
                 "total_bytes": 1, "duration_sec": 1, "avg_mbps": 1,
                 "peak_mbps": 1, "status": 1, "logged_at": 1, "title": 1,
                  "filename": 1, "source_type": 1, "user_name": 1, "client_host": 1,
                  "user_agent": 1, "request_range": 1, "error_reason": 1,
                  "adaptive_prefetch": 1, "smart_routing": 1, "route_attempts": 1,
                  "cached": 1, "served_via": 1,
                 "ttfb_sec": 1, "chunk_timeouts": 1, "chunk_errors": 1,
                  "fallback_chunks": 1, "zero_pad_chunks": 1,
                  "buffering_events": 1, "buffering_rate": 1}
            ).sort("logged_at", DESCENDING).limit(limit)
            recent = await recent_cursor.to_list(None)
            for r in recent:
                if "logged_at" in r:
                    r["logged_at"] = r["logged_at"].isoformat()

            # Best-effort percentiles computed over the returned window.
            ttfb_vals = sorted([v for v in (r.get("ttfb_sec") for r in recent) if isinstance(v, (int, float))])
            if ttfb_vals:
                def pct(p: float) -> float:
                    if len(ttfb_vals) == 1:
                        return float(ttfb_vals[0])
                    k = (len(ttfb_vals) - 1) * p
                    f = int(math.floor(k))
                    c = int(math.ceil(k))
                    if f == c:
                        return float(ttfb_vals[int(k)])
                    return float(ttfb_vals[f] * (c - k) + ttfb_vals[c] * (k - f))

                summary["ttfb_p50_sec"] = round(pct(0.50), 3)
                summary["ttfb_p95_sec"] = round(pct(0.95), 3)
            else:
                summary["ttfb_p50_sec"] = None
                summary["ttfb_p95_sec"] = None

            if summary.get("total_streams"):
                summary["cache_hit_rate"] = round((summary.get("cached_streams", 0) / summary["total_streams"]) * 100, 2)
                summary["error_rate"] = round((summary.get("error_streams", 0) / summary["total_streams"]) * 100, 2)
                summary["cancel_rate"] = round((summary.get("cancelled_streams", 0) / summary["total_streams"]) * 100, 2)
                summary["non_finished_rate"] = round((summary.get("non_finished_streams", 0) / summary["total_streams"]) * 100, 2)
                summary["buffering_stream_rate"] = round((summary.get("buffering_streams", 0) / summary["total_streams"]) * 100, 2)
                summary["avg_buffering_rate"] = round(summary.get("avg_buffering_rate", 0) or 0, 4)
            else:
                summary["cache_hit_rate"] = 0
                summary["error_rate"] = 0
                summary["cancel_rate"] = 0
                summary["non_finished_rate"] = 0
                summary["buffering_stream_rate"] = 0
                summary["avg_buffering_rate"] = 0

            return {
                "summary":    summary,
                "per_client": per_client,
                "recent":     recent,
            }
        except Exception as e:
            LOGGER.error(f"get_stream_analytics error: {e}")
            return {"summary": {}, "per_client": [], "recent": []}

    # -------------------------------
    # Unmatched Media Repair Queue
    # -------------------------------

    async def upsert_unmatched_media(self, job: dict, reason: str, suggestions: Optional[List[dict]] = None) -> str:
        """Store a metadata-failed Telegram/torrent item for manual repair."""
        now = datetime.utcnow()
        source_key = job.get("source_key") or f"{job.get('source_type', 'telegram')}:{job.get('chat_id')}:{job.get('msg_id')}:{job.get('item_key', '')}"
        doc = {
            "source_key": source_key,
            "source_type": job.get("source_type", "telegram"),
            "origin_chat_id": job.get("chat_id"),
            "origin_msg_id": job.get("msg_id"),
            "channel": job.get("channel"),
            "title": job.get("title") or job.get("file_name") or "",
            "file_name": job.get("file_name") or job.get("title") or "",
            "size": job.get("size"),
            "file_size": job.get("file_size"),
            "override_id": job.get("override_id"),
            "torrent": job.get("torrent"),
            "failure_reason": str(reason or "metadata_failed")[:500],
            "suggestions": suggestions or [],
            "status": "open",
            "updated_at": now,
        }
        result = await self.dbs["tracking"]["unmatched_media"].find_one_and_update(
            {"source_key": source_key},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return str(result["_id"])

    async def get_unmatched_media(self, unmatched_id: str) -> Optional[dict]:
        try:
            doc = await self.dbs["tracking"]["unmatched_media"].find_one({"_id": ObjectId(unmatched_id)})
            return convert_objectid_to_str(doc) if doc else None
        except Exception:
            return None

    async def list_unmatched_media(self, status: str = "open", limit: int = 100) -> List[dict]:
        query = {} if status == "all" else {"status": status}
        docs = await self.dbs["tracking"]["unmatched_media"].find(query).sort("updated_at", DESCENDING).limit(limit).to_list(None)
        return [convert_objectid_to_str(doc) for doc in docs]

    async def update_unmatched_suggestions(self, unmatched_id: str, suggestions: List[dict]) -> bool:
        result = await self.dbs["tracking"]["unmatched_media"].update_one(
            {"_id": ObjectId(unmatched_id)},
            {"$set": {"suggestions": suggestions, "updated_at": datetime.utcnow()}},
        )
        return result.modified_count > 0

    async def mark_unmatched_status(self, unmatched_id: str, status: str, extra: Optional[dict] = None) -> bool:
        update = {"status": status, "updated_at": datetime.utcnow()}
        if extra:
            update.update(extra)
        result = await self.dbs["tracking"]["unmatched_media"].update_one(
            {"_id": ObjectId(unmatched_id)},
            {"$set": update},
        )
        return result.modified_count > 0

    # -------------------------------
    # Manual Quality Flags / Duplicates
    # -------------------------------

    def _apply_quality_flags(self, quality: dict, flags: dict, clear: bool = False) -> dict:
        allowed = {"hidden_from_stremio", "recommended", "quality_note", "flagged_duplicate"}
        for key in allowed:
            if clear:
                if key == "quality_note":
                    quality[key] = None
                else:
                    quality[key] = False
            elif key in flags:
                if key == "quality_note":
                    quality[key] = (flags.get(key) or None)
                else:
                    quality[key] = bool(flags.get(key))
        return quality

    async def update_quality_flags(
        self,
        media_type: str,
        tmdb_id: int,
        db_index: int,
        quality_id: str,
        flags: dict,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        clear: bool = False,
    ) -> bool:
        db_key = f"storage_{int(db_index)}"
        collection = "movie" if media_type == "movie" else "tv"
        doc = await self.dbs[db_key][collection].find_one({"tmdb_id": int(tmdb_id)})
        if not doc:
            return False

        updated = False
        if media_type == "movie":
            for quality in doc.get("telegram", []):
                if quality.get("id") == quality_id:
                    self._apply_quality_flags(quality, flags, clear=clear)
                    updated = True
                    break
        else:
            for season_doc in doc.get("seasons", []):
                if season is not None and int(season_doc.get("season_number", 0) or 0) != int(season):
                    continue
                for ep_doc in season_doc.get("episodes", []):
                    if episode is not None and int(ep_doc.get("episode_number", 0) or 0) != int(episode):
                        continue
                    for quality in ep_doc.get("telegram", []):
                        if quality.get("id") == quality_id:
                            self._apply_quality_flags(quality, flags, clear=clear)
                            updated = True
                            break
                    if updated:
                        break
                if updated:
                    break

        if not updated:
            return False
        doc["updated_on"] = datetime.utcnow()
        result = await self.dbs[db_key][collection].replace_one({"_id": doc["_id"]}, doc)
        return result.modified_count > 0

    def _duplicate_key_for_quality(self, quality: dict) -> str:
        source_type = quality.get("source_type") or "telegram"
        if source_type == "torrent" and quality.get("info_hash"):
            return f"torrent:{str(quality.get('info_hash')).lower()}:{quality.get('file_idx')}"
        if quality.get("id"):
            return f"id:{quality.get('id')}"
        filename = (quality.get("filename") or quality.get("name") or "").strip().lower()
        size = quality.get("video_size") or quality.get("size") or ""
        return f"file:{filename}:{size}"

    def _duplicate_group(self, doc: dict, media_type: str, qualities: list, season: int | None = None, episode: int | None = None) -> Optional[dict]:
        if len(qualities or []) <= 1:
            return None
        buckets = {}
        for quality in qualities:
            buckets.setdefault(self._duplicate_key_for_quality(quality), []).append(quality)
        exact_duplicates = [items for items in buckets.values() if len(items) > 1]
        return {
            "media_type": media_type,
            "tmdb_id": doc.get("tmdb_id"),
            "db_index": doc.get("db_index"),
            "title": doc.get("title"),
            "season": season,
            "episode": episode,
            "quality_count": len(qualities),
            "exact_duplicate_count": sum(len(items) for items in exact_duplicates),
            "qualities": [
                {
                    "id": q.get("id"),
                    "quality": q.get("quality"),
                    "name": q.get("name"),
                    "size": q.get("size"),
                    "source_type": q.get("source_type", "telegram"),
                    "hidden_from_stremio": bool(q.get("hidden_from_stremio", False)),
                    "recommended": bool(q.get("recommended", False)),
                    "quality_note": q.get("quality_note"),
                    "flagged_duplicate": bool(q.get("flagged_duplicate", False)),
                }
                for q in qualities
            ],
        }

    async def get_duplicate_quality_groups(self, limit: int = 200) -> List[dict]:
        groups: List[dict] = []
        for db_index in range(1, len(self.dbs)):
            db_key = f"storage_{db_index}"
            for doc in await self.dbs[db_key]["movie"].find({"telegram.1": {"$exists": True}}).limit(limit).to_list(None):
                group = self._duplicate_group(doc, "movie", doc.get("telegram", []))
                if group:
                    groups.append(group)
            cursor = self.dbs[db_key]["tv"].find({})
            for doc in await cursor.limit(limit).to_list(None):
                for season_doc in doc.get("seasons", []):
                    for ep_doc in season_doc.get("episodes", []):
                        qualities = ep_doc.get("telegram", [])
                        group = self._duplicate_group(
                            doc,
                            "tv",
                            qualities,
                            season=int(season_doc.get("season_number", 0) or 0),
                            episode=int(ep_doc.get("episode_number", 0) or 0),
                        )
                        if group:
                            groups.append(group)
        return groups[:limit]
