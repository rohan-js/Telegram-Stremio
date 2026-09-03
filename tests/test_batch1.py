"""Batch-1 regression tests: dead-link filter, maintenance mode, stable
Latest ordering, videoSize behaviorHint, analytics TTL index."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import Response

from Backend.config import Telegram
from Backend.fastapi.routes import stremio_routes
from Backend.helper.database import Database
from Backend.helper.settings_manager import DEFAULTS


def fake_request():
    return SimpleNamespace(headers={}, client=SimpleNamespace(host="1.2.3.4"))


def _fake_settings(maintenance_mode=False):
    return SimpleNamespace(
        maintenance_mode=maintenance_mode,
        fanart_enabled=False,
        better_poster_enabled=False,
        better_poster="",
        rpdb_enabled=False,
        rpdb_api_key="",
        base_url="",
    )


class FakeStreamsDB:
    async def get_media_details(self, imdb_id, season_number=None, episode_number=None):
        return {
            "title": "Rocky III",
            "title_english": "Rocky III",
            "telegram": [
                {"quality": "1080p", "id": "good", "name": "Rocky.III.1080p.mkv", "size": "2 GB"},
                {"quality": "720p", "id": "deadone", "name": "Rocky.III.720p.mkv", "size": "1 GB", "is_dead": True},
                {"quality": "480p", "id": "hiddenone", "name": "Rocky.III.480p.mkv", "size": "700 MB", "hidden_from_stremio": True},
            ],
        }


class FakeLatestDB:
    def __init__(self):
        self.captured_sort = None

    async def sort_movies(self, sort_params, page, page_size, genre_filter=None):
        self.captured_sort = sort_params
        return {"movies": [], "total_count": 0}

    async def sort_tv_shows(self, sort_params, page, page_size, genre_filter=None):
        self.captured_sort = sort_params
        return {"tv_shows": [], "total_count": 0}


class Batch1StreamsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    async def tearDown(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    async def _fetch(self, token_data=None, settings=None):
        with (
            patch.object(stremio_routes, "db", FakeStreamsDB()),
            patch.object(stremio_routes, "BASE_URL", "https://example.test"),
            patch.object(stremio_routes.SettingsManager, "current", lambda: settings or _fake_settings()),
        ):
            return await stremio_routes.get_streams(
                fake_request(), "tokB1", "movie", "tt0084602", Response(),
                token_data=token_data or {},
            )

    async def test_dead_and_hidden_qualities_filtered(self):
        result = await self._fetch()
        ids = [s["url"].rsplit("/", 2)[-2] for s in result["streams"]]
        self.assertIn("good", ids)
        self.assertNotIn("deadone", ids)
        self.assertNotIn("hiddenone", ids)

    async def test_videosize_behavior_hint_standard_field(self):
        result = await self._fetch()
        self.assertTrue(result["streams"])
        for stream in result["streams"]:
            hints = stream.get("behaviorHints") or {}
            self.assertEqual(hints.get("videoSize"), 2 * 1024 ** 3)

    async def test_maintenance_blocks_non_admin_with_placeholder(self):
        result = await self._fetch(settings=_fake_settings(maintenance_mode=True))
        self.assertEqual(len(result["streams"]), 1)
        self.assertIn("Maintenance", result["streams"][0]["name"])

    async def test_maintenance_admin_token_passes_through(self):
        result = await self._fetch(
            token_data={"is_admin": True},
            settings=_fake_settings(maintenance_mode=True),
        )
        ids = [s["url"].rsplit("/", 2)[-2] for s in result["streams"]]
        self.assertIn("good", ids)

    async def test_maintenance_off_normal_streams(self):
        result = await self._fetch(settings=_fake_settings(maintenance_mode=False))
        self.assertTrue(any("Maintenance" not in s["name"] for s in result["streams"]))


class Batch1CatalogTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_movies_sort_by_origin_msg_id(self):
        fake_db = FakeLatestDB()
        with (
            patch.object(stremio_routes, "db", fake_db),
            patch.object(stremio_routes.SettingsManager, "current", lambda: _fake_settings()),
        ):
            await stremio_routes.get_catalog(
                "tokB2", "movie", "latest_movies", Response(), token_data={},
            )
        self.assertEqual(fake_db.captured_sort, [("telegram.origin_msg_id", "desc")])

    async def test_latest_series_sort_by_episode_origin_msg_id(self):
        fake_db = FakeLatestDB()
        with (
            patch.object(stremio_routes, "db", fake_db),
            patch.object(stremio_routes.SettingsManager, "current", lambda: _fake_settings()),
        ):
            await stremio_routes.get_catalog(
                "tokB3", "series", "latest_series", Response(), token_data={},
            )
        self.assertEqual(
            fake_db.captured_sort,
            [("seasons.episodes.telegram.origin_msg_id", "desc")],
        )

    async def test_maintenance_catalog_empty_for_non_admin(self):
        with (
            patch.object(stremio_routes, "db", FakeLatestDB()),
            patch.object(stremio_routes.SettingsManager, "current", lambda: _fake_settings(maintenance_mode=True)),
        ):
            result = await stremio_routes.get_catalog(
                "tokB4", "movie", "latest_movies", Response(), token_data={},
            )
        self.assertEqual(result["metas"], [])


class MaintenanceMetaTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_meta_empty_for_non_admin(self):
        class FakeMetaDB:
            async def get_media_details(self, imdb_id=None, **kwargs):
                return {"media_type": "movie", "title": "X", "imdb_id": "tt1"}

        with (
            patch.object(stremio_routes, "db", FakeMetaDB()),
            patch.object(stremio_routes.SettingsManager, "current", lambda: _fake_settings(maintenance_mode=True)),
        ):
            result = await stremio_routes.get_meta(
                "tokB5", "movie", "tt1", Response(), token_data={},
            )
        self.assertEqual(result["meta"], {})


class MaintenanceSettingsTests(unittest.TestCase):
    def test_maintenance_mode_in_defaults_whitelist(self):
        self.assertIn("maintenance_mode", DEFAULTS)
        self.assertFalse(DEFAULTS["maintenance_mode"])


class AnalyticsTtlIndexTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_indexes_creates_retention_ttl(self):
        created = []

        class FakeCollection:
            async def create_index(self, keys, **kwargs):
                created.append((keys, kwargs))

        class FakeTracking:
            def __getitem__(self, name):
                return FakeCollection()

        db = Database.__new__(Database)  # skip URI parsing entirely
        db.dbs = {"tracking": FakeTracking()}
        with patch.object(Telegram, "STREAM_LOG_RETENTION_DAYS", 30):
            await db.ensure_indexes()

        ttl_calls = [c for c in created if any(k == "logged_at_retention_ttl" for k in c[1].values())]
        self.assertEqual(len(ttl_calls), 1)
        keys, kwargs = ttl_calls[0]
        self.assertEqual(kwargs.get("expireAfterSeconds"), 30 * 86400)


if __name__ == "__main__":
    unittest.main()
