"""Tests: custom-catalog search bar (upstream 51d0775) + TeleStremio library
export (upstream 3326dec)."""

import gzip
import json
import unittest
import unittest.mock
from unittest.mock import patch

from Backend.helper.database import Database
from Backend.helper import backup as backup_mod


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if str(doc.get("_id")) == str(query.get("_id")) or doc.get("name") == query.get("name"):
                return dict(doc)
        return None

    def find(self, query=None):
        return self

    def sort(self, *_a, **_k):
        return self

    async def to_list(self, _n):
        return [dict(d) for d in self.docs]

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for doc in self.docs:
            yield dict(doc)


class _FakeStorage:
    def __init__(self, **collections):
        self.collections = collections

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCollection()
        return self.collections[name]


class _FakeDBWrapper:
    def __init__(self, dbs):
        self.dbs = dbs


def _database_with(items, catalog=None):
    database = object.__new__(Database)
    catalog_doc = catalog or {
        "_id": "aaaaaaaaaaaaaaaaaaaa0001", "name": "My Catalog", "visible": True, "visibility": "public",
        "allowed_tokens": [], "items": items,
    }
    tracking = _FakeStorage(custom_catalogs=_FakeCollection([catalog_doc]))
    database.dbs = {"tracking": tracking}
    return database


async def _fake_get_documents(self, items):
    by_key = {
        (i.get("media_type"), i.get("tmdb_id")): {
            "title": i.get("title"), "title_english": i.get("title_english"),
            "media_type": i.get("media_type"), "tmdb_id": i.get("tmdb_id"),
            "db_index": i.get("db_index", 1),
        }
        for i in items
    }
    return [dict(v) for v in by_key.values() if v.get("tmdb_id") is not None]


class CatalogSearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_matches_across_all_pages(self):
        items = [
            {"media_type": "movie", "tmdb_id": i + 1, "db_index": 1, "title": f"Title {i + 1}",
             "title_english": f"Title {i + 1}"}
            for i in range(30)
        ]
        items[25]["title_english"] = "Interstellar"
        database = _database_with(items)

        with patch.object(Database, "get_documents", _fake_get_documents):
            result = await database.get_custom_catalog_items(
                "aaaaaaaaaaaaaaaaaaaa0001", media_type="movie", page=1, page_size=24, search="interstellar",
            )
        self.assertEqual(result["total_count"], 1)
        self.assertEqual(result["items"][0]["title_english"], "Interstellar")

    async def test_search_empty_needle_behaves_like_browse(self):
        items = [
            {"media_type": "movie", "tmdb_id": 1, "db_index": 1, "title": "Rocky", "title_english": "Rocky"},
        ]
        database = _database_with(items)
        with patch.object(Database, "get_documents", _fake_get_documents):
            result = await database.get_custom_catalog_items(
                "aaaaaaaaaaaaaaaaaaaa0001", media_type="movie", page=1, page_size=24, search="  ",
            )
        self.assertEqual(result["total_count"], 1)

    async def test_no_search_paginates_slice(self):
        items = [
            {"media_type": "movie", "tmdb_id": i + 1, "db_index": 1, "title": f"T{i}", "title_english": f"T{i}"}
            for i in range(5)
        ]
        database = _database_with(items)
        with patch.object(Database, "get_documents", _fake_get_documents):
            page2 = await database.get_custom_catalog_items(
                "aaaaaaaaaaaaaaaaaaaa0001", media_type="movie", page=2, page_size=2,
            )
        self.assertEqual([d["tmdb_id"] for d in page2["items"]], [3, 4])


class LibraryExportTests(unittest.IsolatedAsyncioTestCase):
    async def test_export_stream_decodes_to_catalog_json(self):
        movie = {
            "tmdb_id": 42, "imdb_id": "tt42", "title": "Exported", "media_type": "movie",
            "db_index": 1, "updated_on": None,
            "telegram": [{"quality": "1080p", "id": "ENCODED", "name": "f.mkv", "size": "1 GB"}],
        }
        storage = _FakeStorage(movie=_FakeCollection([movie]))
        database = _FakeDBWrapper({"storage_1": storage, "tracking": _FakeStorage()})

        async def fake_decode(encoded):
            assert encoded == "ENCODED"
            return {"chat_id": -100123, "msg_id": 55}

        settings = unittest.mock.Mock()
        settings.to_dict.return_value = {"anime_channels": [], "auth_channels": ["-100123"]}

        with patch.object(backup_mod, "db", database), \
                patch.object(backup_mod, "decode_string", fake_decode), \
                patch.object(backup_mod.SettingsManager, "current", lambda: settings):
            chunks = [chunk async for chunk in backup_mod.export_library()]

        payload = json.loads(gzip.decompress(b"".join(chunks)).decode())
        self.assertEqual(payload["format"], "telestremio-catalog")
        self.assertEqual(payload["movie"][0]["title"], "Exported")
        part = payload["movie"][0]["telegram"][0]["parts"][0]
        self.assertEqual(part["chat_id"], -100123)
        self.assertEqual(part["msg_id"], 55)
        self.assertEqual(payload["channels"][0]["chat_id"], -100123)

    async def test_unreadable_stream_id_is_dropped(self):
        movie = {
            "tmdb_id": 7, "title": "Broken", "media_type": "movie", "db_index": 1,
            "telegram": [{"quality": "720p", "id": "BAD", "name": "x.mkv", "size": "700 MB"}],
        }
        storage = _FakeStorage(movie=_FakeCollection([movie]))
        database = _FakeDBWrapper({"storage_1": storage, "tracking": _FakeStorage()})

        async def fake_decode(encoded):
            raise ValueError("bad id")

        settings = unittest.mock.Mock()
        settings.to_dict.return_value = {"anime_channels": [], "auth_channels": []}

        with patch.object(backup_mod, "db", database), \
                patch.object(backup_mod, "decode_string", fake_decode), \
                patch.object(backup_mod.SettingsManager, "current", lambda: settings):
            chunks = [chunk async for chunk in backup_mod.export_library()]

        payload = json.loads(gzip.decompress(b"".join(chunks)).decode())
        self.assertEqual(payload["movie"][0]["telegram"], [])


if __name__ == "__main__":
    unittest.main()
