"""Tests for the stream-list micro-cache (_stream_list_cache_* in stremio_routes)."""

import time
import unittest
from unittest.mock import patch

from Backend.config import Telegram
from Backend.fastapi.routes import stremio_routes
from Backend.fastapi.routes.stremio_routes import _stream_list_cache_get, _stream_list_cache_put


class StreamListCacheTests(unittest.TestCase):
    def setUp(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    def tearDown(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    def test_put_get_roundtrip(self):
        payload = {"streams": [{"name": "1080p"}], "cacheMaxAge": 0}
        _stream_list_cache_put(("t", "movie", "tt1"), payload)
        got = _stream_list_cache_get(("t", "movie", "tt1"))
        self.assertIs(got, payload)

    def test_miss_returns_none(self):
        self.assertIsNone(_stream_list_cache_get(("t", "movie", "tt404")))

    def test_ttl_expiry(self):
        payload = {"streams": []}
        key = ("t", "movie", "tt2")
        _stream_list_cache_put(key, payload)
        # Not expired at TTL-5s
        with patch.object(Telegram, "STREAM_LIST_CACHE_TTL_SEC", 100):
            self.assertIsNotNone(_stream_list_cache_get(key))
        # Expired past TTL
        stremio_routes._STREAM_LIST_CACHE[key] = (payload, time.time() - 999)
        self.assertIsNone(_stream_list_cache_get(key))
        self.assertNotIn(key, stremio_routes._STREAM_LIST_CACHE)

    def test_lru_eviction(self):
        with patch.object(Telegram, "STREAM_LIST_CACHE_MAX_ENTRIES", 4):
            for i in range(6):
                _stream_list_cache_put((f"t{i}", "movie", f"tt{i}"), {"streams": []})
            self.assertLessEqual(len(stremio_routes._STREAM_LIST_CACHE), 4)
            self.assertIsNone(_stream_list_cache_get(("t0", "movie", "tt0")))
            self.assertIsNone(_stream_list_cache_get(("t1", "movie", "tt1")))
            self.assertIsNotNone(_stream_list_cache_get(("t5", "movie", "tt5")))

    def test_keys_are_token_scoped(self):
        a = {"streams": [{"name": "A"}]}
        _stream_list_cache_put(("tokenA", "movie", "tt1"), a)
        self.assertIsNone(_stream_list_cache_get(("tokenB", "movie", "tt1")))


if __name__ == "__main__":
    unittest.main()
