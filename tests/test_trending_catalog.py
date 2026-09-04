"""Tests: Trending Now auto-catalog (TMDb weekly trending ∩ library)."""

import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from Backend.config import Telegram
from Backend.helper import auto_catalog as ac


def _fake_db(storage_docs=None, catalog_doc=None, enabled=True):
    """Fake Database: one storage shard + tracking custom_catalogs/state."""
    storage_docs = storage_docs or {}

    class FakeCursor:
        def __init__(self, docs):
            self._docs = docs

        def __call__(self, *args, **kwargs):
            return self

        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            for d in self._docs:
                yield d

    class FakeCollection:
        def __init__(self, docs):
            self._docs = docs
            self.upserts = []

        def find(self, query, projection=None):
            tmdb_ids = (query.get("tmdb_id") or {}).get("$in") or []
            matched = [d for d in self._docs if d.get("tmdb_id") in tmdb_ids]
            return FakeCursor(matched)

        async def find_one(self, query):
            return catalog_doc

        async def update_one(self, query, update, upsert=False):
            self.upserts.append((query, update, upsert))
            return MagicMock()

    storage = {"movie": FakeCollection(storage_docs.get("movie", [])),
               "tv": FakeCollection(storage_docs.get("tv", []))}
    tracking = {
        "custom_catalogs": FakeCollection([]),
        "state": FakeCollection([]),
    }

    db = MagicMock()
    db.current_db_index = 1
    db.dbs = {"tracking": tracking, "storage_1": storage}
    db._custom_catalogs = tracking["custom_catalogs"]
    return db


class TrendingTests(unittest.IsolatedAsyncioTestCase):
    async def test_matches_library_and_replaces_items(self):
        db = _fake_db(storage_docs={
            "movie": [{"tmdb_id": 111, "media_type": "movie", "db_index": 1}],
            "tv": [{"tmdb_id": 222, "media_type": "tv", "db_index": 1}],
        })
        with (
            patch.object(ac, "_fetch_trending_tmdb_ids", AsyncMock(return_value=[999, 222, 111])),
            patch.object(ac, "_enabled_catalog_names", AsyncMock(return_value={"Trending Now"})),
        ):
            result = await ac.sync_trending_catalog(db)

        self.assertEqual(result["matched"], 2)  # 999 not in library
        coll = db._custom_catalogs
        self.assertEqual(len(coll.upserts), 1)
        _, update, upsert = coll.upserts[0]
        items = update["$set"]["items"]
        # TMDb trending order preserved (222 before 111), movie/tv tagged
        self.assertEqual(
            [(i["tmdb_id"], i["media_type"]) for i in items],
            [(222, "tv"), (111, "movie")],
        )
        self.assertTrue(update["$set"]["visible"])
        self.assertTrue(upsert)

    async def test_skips_when_synced_recently(self):
        catalog_doc = {"auto_key": ac.TRENDING_AUTO_KEY,
                       "last_auto_sync": datetime.utcnow() - timedelta(hours=1)}
        db = _fake_db(catalog_doc=catalog_doc)
        with patch.object(ac, "_fetch_trending_tmdb_ids", AsyncMock()) as fetch:
            result = await ac.sync_trending_catalog(db)
        fetch.assert_not_awaited()
        self.assertEqual(result.get("skipped"), "recently synced")

    async def test_noop_when_trending_not_enabled(self):
        db = _fake_db()
        with (
            patch.object(ac, "_enabled_catalog_names", AsyncMock(return_value={"Bollywood"})),
            patch.object(ac, "_fetch_trending_tmdb_ids", AsyncMock()) as fetch,
        ):
            result = await ac.sync_trending_catalog(db, force=True)
        fetch.assert_not_awaited()
        self.assertEqual(result.get("skipped"), "trending not enabled")

    async def test_no_catalog_when_no_library_matches(self):
        db = _fake_db()  # empty library
        with (
            patch.object(ac, "_fetch_trending_tmdb_ids", AsyncMock(return_value=[1, 2, 3])),
            patch.object(ac, "_enabled_catalog_names", AsyncMock(return_value={"Trending Now"})),
        ):
            result = await ac.sync_trending_catalog(db, force=True)
        self.assertEqual(result["matched"], 0)
        self.assertEqual(len(db._custom_catalogs.upserts), 0)

    async def test_trending_in_definitions(self):
        self.assertIn("trending", ac.CATALOG_BY_KEY)
        self.assertEqual(ac.CATALOG_BY_KEY["trending"]["name"], "Trending Now")


if __name__ == "__main__":
    unittest.main()
