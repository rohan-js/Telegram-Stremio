"""Tests for MediaIndex Mongo persistence (serialize / restore round-trip)."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from Backend.helper.media_index import (
    MediaIndex,
    _load_persisted_index,
    _persist_index,
    build_media_index,
    _INDEX_CACHE,
)


def _sample_index():
    return MediaIndex("mkv", 600.0, [(float(i * 10), i * 1048576) for i in range(61)])


class _FakeCollection:
    def __init__(self):
        self.docs = {}

    async def update_one(self, filt, update, upsert=False):
        self.docs[filt["_id"]] = update["$set"]

    async def find_one(self, filt):
        return self.docs.get(filt.get("_id"))


def _fake_db():
    db = MagicMock()
    db.dbs = {"tracking": {"media_indexes": _FakeCollection()}}
    return db


class IndexPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _INDEX_CACHE.clear()

    async def test_roundtrip_equality(self):
        import Backend

        db = _fake_db()
        idx = _sample_index()
        key = (-100, 558)
        with patch.object(Backend, "db", db, create=True):
            await _persist_index(key, idx)
            restored = await _load_persisted_index(key)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.container, "mkv")
        self.assertAlmostEqual(restored.duration_sec, 600.0, places=3)
        self.assertEqual(restored.keyframes, idx.keyframes)
        # behavioral equivalence: a +10s skip resolves to the same byte
        mid = idx.keyframes[10]
        self.assertEqual(
            restored.seek_delta_byte(mid[1], 10.0, 10**9),
            idx.seek_delta_byte(mid[1], 10.0, 10**9),
        )

    async def test_load_missing_returns_none(self):
        import Backend

        with patch.object(Backend, "db", _fake_db(), create=True):
            self.assertIsNone(await _load_persisted_index((-100, 404)))

    async def test_build_media_index_restores_from_mongo_without_mtproto(self):
        import Backend
        import Backend.helper.media_index as mi

        idx = _sample_index()
        db = _fake_db()

        async def fake_persist(key, i):
            await db.dbs["tracking"]["media_indexes"].update_one(
                {"_id": f"{key[0]}:{key[1]}"},
                {"$set": {
                    "container": i.container,
                    "duration_sec": i.duration_sec,
                    "keyframes": [[round(t, 3), int(b)] for t, b in i.keyframes],
                }},
                upsert=True,
            )

        with patch.object(Backend, "db", db, create=True):
            await fake_persist((-100, 777), idx)

        _INDEX_CACHE.clear()
        streamer = MagicMock()
        streamer._fetch_file_bytes = AsyncMock(return_value=b"")
        streamer._get_media_session = AsyncMock(return_value=object())
        streamer._get_location = AsyncMock(return_value=object())
        fid = MagicMock()
        fid.file_size = 100 * 1024 * 1024

        with patch.object(Backend, "db", db, create=True), patch.object(
            mi, "_persist_index", fake_persist
        ):
            got = await build_media_index(fid, streamer, chat_id=-100, message_id=777)

        self.assertIsNotNone(got)
        self.assertEqual(got.keyframes, idx.keyframes)
        # no MTProto fetch happened — restored purely from Mongo
        streamer._fetch_file_bytes.assert_not_called()

    async def test_degenerate_doc_returns_none(self):
        import Backend

        db = _fake_db()
        db.dbs["tracking"]["media_indexes"].docs["-100:888"] = {
            "container": "mkv",
            "duration_sec": 10,
            "keyframes": [[0.0, 1]],  # < 2 keyframes — unusable
        }
        with patch.object(Backend, "db", db, create=True):
            self.assertIsNone(await _load_persisted_index((-100, 888)))


if __name__ == "__main__":
    unittest.main()
