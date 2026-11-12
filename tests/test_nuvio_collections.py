"""Tests: Nuvio collection endpoints (upstream fb33de8..9f0d68a port).

Pins the fork contract: folders whose catalogs don't exist in this library
are dropped (strict resolution), sanitized output never carries a null
catalogId, per-item visibility is honored, and tmdb/trakt remote folders
survive regardless of library content.
"""

import unittest
from unittest.mock import patch

from Backend.fastapi.routes import nuvio_collections as nc


def _catalog(cid, name, *, auto_key=None, items=None, visibility="public"):
    doc = {
        "_id": cid,
        "name": name,
        "visibility": visibility,
        "allowed_tokens": [],
        "visible": True,
        "auto": bool(auto_key),
        "auto_key": auto_key,
        "items": items or [],
    }
    return doc


def _item(media_type="movie", visibility=None):
    item = {"media_type": media_type}
    if visibility:
        item["visibility"] = visibility
        item["allowed_tokens"] = ["tokX"]
    return item


class FakeNuvioDB:
    def __init__(self, catalogs):
        self.catalogs = catalogs
        self.token_requests = []

    async def get_custom_catalogs(self, visible_only=False, slim=False, item_fields=None):
        self.slim_requested = slim
        return [dict(c) for c in self.catalogs]

    async def get_api_token(self, token):
        self.token_requests.append(token)
        return {"token": token}


def _catalog_ids(collection):
    out = set()
    for folder in collection.get("folders", []):
        for source in folder.get("sources", []):
            if source.get("provider") == "addon":
                out.add(source.get("catalogId"))
    return out


class BuilderTests(unittest.IsolatedAsyncioTestCase):
    async def _build(self, catalogs, **kw):
        fake_db = FakeNuvioDB(catalogs)
        with patch.object(nc, "db", fake_db):
            return await nc.build_nuvio_collection_data("tokT", **kw)

    async def test_full_collections_shape(self):
        # With an empty library: OTT has no addon sources → drops; the
        # discover (top/latest builtins), franchises (tmdb/trakt) and
        # spotlight (Seasonal tmdb folder) collections still render.
        result = await self._build([], collection_type="full")
        ids = {c["id"] for c in result}
        self.assertIn("collection-discover-main", ids)
        self.assertIn("collection-franchises-main", ids)
        self.assertNotIn("collection-ott-main", ids)
        for coll in result:
            self.assertTrue(coll["folders"])
        # All four render once an OTT catalog exists.
        result = await self._build(
            [_catalog("c1", "Netflix", auto_key="auto_netflix", items=[_item("movie"), _item("tv")])],
            collection_type="full",
        )
        self.assertEqual(len(result), 4)

    async def test_type_filtering(self):
        discover = await self._build([], collection_type="discover")
        self.assertEqual(len(discover), 1)
        self.assertEqual(discover[0]["id"], "collection-discover-main")
        franchises = await self._build([], collection_type="franchises")
        self.assertEqual(len(franchises), 1)

    async def test_missing_catalogs_drop_folders_not_crash(self):
        # No auto catalogs at all: OTT folders vanish, tmdb-only franchise
        # folders still render, addon sources carry no null ids.
        result = await self._build([], collection_type="full")
        for coll in result:
            for folder in coll["folders"]:
                for source in folder["sources"]:
                    if source.get("provider") == "addon":
                        self.assertTrue(source.get("catalogId"))

    async def test_netflix_auto_key_resolves(self):
        catalogs = [
            _catalog("c1", "Netflix", auto_key="auto_netflix", items=[_item("movie"), _item("tv")]),
        ]
        result = await self._build(catalogs, collection_type="ott")
        addon_ids = _catalog_ids(result[0]) if result else set()
        self.assertIn("custom_c1", addon_ids)

    async def test_trending_auto_key_maps_discover(self):
        catalogs = [
            _catalog("c2", "Trending Now", auto_key="auto_trending", items=[_item("movie"), _item("tv")]),
        ]
        result = await self._build(catalogs, collection_type="discover")
        addon_ids = _catalog_ids(result[0])
        self.assertIn("custom_c2", addon_ids)

    async def test_owner_only_catalog_invisible_to_public_token(self):
        catalogs = [
            _catalog("c3", "Netflix", auto_key="auto_netflix", items=[_item("movie")], visibility="owner"),
        ]
        result = await self._build(catalogs, collection_type="ott")
        addon_ids = _catalog_ids(result[0]) if result else set()
        self.assertNotIn("custom_c3", addon_ids)

    async def test_item_visibility_respected(self):
        catalogs = [
            # catalog is public but its only movie item is token-locked
            _catalog("c4", "Netflix", auto_key="auto_netflix", items=[_item("movie", visibility="tokens")]),
        ]
        result = await self._build(catalogs, collection_type="ott")
        # no visible movie items → has_movie False → no movie mapping
        addon_sources = [
            s for s in (result[0]["folders"][0]["sources"] if result else [])
            if s.get("provider") == "addon"
        ]
        for s in addon_sources:
            self.assertNotEqual(s.get("type"), "movie")

    async def test_null_token_data_with_empty_token_skips_lookup(self):
        fake_db = FakeNuvioDB([])
        with patch.object(nc, "db", fake_db):
            result = await nc.build_nuvio_collection_data("", "full", None)
        self.assertEqual(fake_db.token_requests, [])
        self.assertTrue(result)


class VisibilityHelperTests(unittest.TestCase):
    def test_catalog_level_deny(self):
        catalog = {"_id": "c9", "visibility": "owner", "allowed_tokens": []}
        self.assertFalse(nc._nuvio_item_visible(catalog, {"media_type": "movie"}, {}))

    def test_public_catalog_public_item(self):
        catalog = {"_id": "c9", "visibility": "public", "allowed_tokens": []}
        self.assertTrue(nc._nuvio_item_visible(catalog, {"media_type": "movie"}, {}))


if __name__ == "__main__":
    unittest.main()
