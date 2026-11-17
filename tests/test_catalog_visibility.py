"""Tests: admin catalog visibility (global hide) + ordering (upstream v5.3.4).

Covers the DB state round-trip, the admin order API (entries include every
custom/auto catalog expanded per media type, hidden flags), the addon-config
entry filtering (_addon_catalogs_for_token), and the fork improvement that
Nuvio collections respect the hide per media type.
"""

import unittest
from unittest.mock import patch

from Backend.fastapi.routes import stremio_routes, nuvio_collections
from Backend.fastapi.routes import api_routes
from Backend.helper.database import Database


class _FakeStateCollection:
    def __init__(self):
        self.docs = {}

    async def find_one(self, query):
        return self.docs.get(query.get("_id"))

    async def update_one(self, query, update, upsert=False):
        doc_id = query.get("_id")
        doc = self.docs.setdefault(doc_id, {"_id": doc_id})
        doc.update(update.get("$set", {}))


class _FakeVisibilityDB:
    def __init__(self, catalogs=None):
        self.state = {"tracking": {"state": _FakeStateCollection()}}
        self.catalogs = catalogs or []

    def __getitem__(self, name):
        return self.state[name]

    async def get_custom_catalogs(self, visible_only=False, slim=False, item_fields=None):
        return [dict(c) for c in self.catalogs]

    async def get_catalog_order(self):
        doc = self.state["tracking"]["state"].docs.get("catalog_order") or {}
        return list(doc.get("order", []))

    async def save_catalog_order(self, order):
        self.saved_order = order
        return True

    async def get_hidden_catalogs(self):
        return list(getattr(self, "hidden", []))

    async def save_hidden_catalogs(self, keys):
        self.saved_hidden = keys


class _FakeCatalog:
    def __init__(self, cid, name, items, auto=False):
        self.doc = {
            "_id": cid, "name": name, "visible": True, "visibility": "public",
            "allowed_tokens": [], "auto": auto,
            "auto_key": f"auto_{name.lower().replace(' ', '_')}" if auto else None,
            "items": items,
        }


_MOVIE = {"media_type": "movie", "tmdb_id": 1, "db_index": 1}
_SERIES = {"media_type": "tv", "tmdb_id": 2, "db_index": 1}


class DatabaseStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_and_hidden_round_trip(self):
        database = object.__new__(Database)
        database.dbs = {"tracking": {"state": _FakeStateCollection()}}
        self.assertEqual(await database.get_catalog_order(), [])
        self.assertEqual(await database.get_hidden_catalogs(), [])
        self.assertTrue(await database.save_catalog_order(["a::movie", "b::series"]))
        await database.save_hidden_catalogs(["top_movies::movie"])
        self.assertEqual(await database.get_catalog_order(), ["a::movie", "b::series"])
        self.assertEqual(await database.get_hidden_catalogs(), ["top_movies::movie"])


class CatalogOrderApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_entries_include_custom_per_type_and_flags(self):
        fake_db = _FakeVisibilityDB([
            _FakeCatalog("c1", "Netflix", [_MOVIE, _SERIES], auto=True).doc,
            _FakeCatalog("c2", "Movies only", [_MOVIE]).doc,
        ])
        with patch.object(api_routes, "db", fake_db):
            result = await api_routes.get_catalog_order_api()
        ids = {(e["id"], e["type"]) for e in result["entries"]}
        self.assertIn(("custom_c1", "movie"), ids)
        self.assertIn(("custom_c1", "series"), ids)
        self.assertIn(("custom_c2", "movie"), ids)
        self.assertNotIn(("custom_c2", "series"), ids)
        self.assertTrue(all("hidden" in e and "key" in e for e in result["entries"]))

    async def test_update_saves_order_and_hidden(self):
        fake_db = _FakeVisibilityDB()
        with patch.object(api_routes, "db", fake_db):
            result = await api_routes.update_catalog_order_api({
                "order": ["top_movies::movie"],
                "hidden": ["latest_series::series"],
            })
        self.assertTrue(result["ok"])
        self.assertEqual(fake_db.saved_order, ["top_movies::movie"])
        self.assertEqual(fake_db.saved_hidden, ["latest_series::series"])

    async def test_update_rejects_non_list_order(self):
        with patch.object(api_routes, "db", _FakeVisibilityDB()):
            with self.assertRaises(Exception):
                await api_routes.update_catalog_order_api({"order": "nope"})


class AddonCatalogsTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_hidden_filtered_per_type(self):
        fake_db = _FakeVisibilityDB([
            _FakeCatalog("c1", "Netflix", [_MOVIE, _SERIES]).doc,
        ])
        fake_db.hidden = ["custom_c1::movie"]
        with patch.object(stremio_routes, "db", fake_db):
            entries = await stremio_routes._addon_catalogs_for_token({})
        keys = [(e["id"], e["type"]) for e in entries]
        self.assertIn(("custom_c1", "series"), keys)
        self.assertNotIn(("custom_c1", "movie"), keys)
        self.assertIn(("latest_movies", "movie"), keys)

    async def test_token_order_overrides_global(self):
        fake_db = _FakeVisibilityDB([])
        fake_db.state["tracking"]["state"].docs["catalog_order"] = {
            "_id": "catalog_order", "order": ["top_movies::movie", "latest_movies::movie"],
        }
        with patch.object(stremio_routes, "db", fake_db):
            entries = await stremio_routes._addon_catalogs_for_token({
                "config": {"catalog_order": ["latest_movies::movie", "top_movies::movie"]},
            })
        movie_ids = [e["id"] for e in entries if e["type"] == "movie"]
        self.assertEqual(movie_ids[:2], ["latest_movies", "top_movies"])

    async def test_global_order_applies_when_token_has_none(self):
        fake_db = _FakeVisibilityDB([])
        fake_db.state["tracking"]["state"].docs["catalog_order"] = {
            "_id": "catalog_order", "order": ["top_movies::movie", "latest_movies::movie"],
        }
        with patch.object(stremio_routes, "db", fake_db):
            entries = await stremio_routes._addon_catalogs_for_token({})
        movie_ids = [e["id"] for e in entries if e["type"] == "movie"]
        self.assertEqual(movie_ids[:2], ["top_movies", "latest_movies"])


class NuvioHiddenTests(unittest.IsolatedAsyncioTestCase):
    async def _build(self, catalogs, hidden):
        fake_db = _FakeVisibilityDB(catalogs)
        fake_db.hidden = hidden
        with patch.object(nuvio_collections, "db", fake_db):
            return await nuvio_collections.build_nuvio_collection_data("tokT", "full")

    def _addon_catalog_ids(self, result):
        out = set()
        for coll in result:
            for folder in coll.get("folders", []):
                for source in folder.get("sources", []):
                    if source.get("provider") == "addon":
                        out.add((source.get("catalogId"), source.get("type")))
        return out

    async def test_hidden_catalog_dropped_from_nuvio(self):
        catalogs = [_FakeCatalog("c9", "Netflix", [_MOVIE, _SERIES], auto=True).doc]
        result = await self._build(catalogs, ["custom_c9"])
        self.assertNotIn(("custom_c9", "movie"), self._addon_catalog_ids(result))
        self.assertNotIn(("custom_c9", "series"), self._addon_catalog_ids(result))

    async def test_type_scoped_hidden_keeps_other_type(self):
        catalogs = [_FakeCatalog("c9", "Netflix", [_MOVIE, _SERIES], auto=True).doc]
        result = await self._build(catalogs, ["custom_c9::movie"])
        ids = self._addon_catalog_ids(result)
        self.assertNotIn(("custom_c9", "movie"), ids)
        self.assertIn(("custom_c9", "series"), ids)


if __name__ == "__main__":
    unittest.main()
