"""Tests for the memory-bound hardening: spill write-queue byte cap and the
TTL/LRU sweeps on long-lived caches (title, thumb, owner-alert cooldowns)."""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from Backend.config import Telegram
from Backend.fastapi.routes import stream_routes
from Backend.helper import owner_alerts, spill_cache


class SpillQueueByteCapTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._backups = {
            "queue": spill_cache._write_queue,
            "writer": spill_cache._writer_task,
            "queued": spill_cache._queued_bytes,
            "dropped": spill_cache._dropped_chunks,
            "swept": spill_cache._swept_at_startup,
        }
        spill_cache._write_queue = None
        spill_cache._writer_task = None
        spill_cache._queued_bytes = 0
        spill_cache._dropped_chunks = 0
        spill_cache._swept_at_startup = True

    def tearDown(self):
        spill_cache._write_queue = self._backups["queue"]
        spill_cache._writer_task = self._backups["writer"]
        spill_cache._queued_bytes = self._backups["queued"]
        spill_cache._dropped_chunks = self._backups["dropped"]
        spill_cache._swept_at_startup = self._backups["swept"]

    async def test_chunk_dropped_when_byte_cap_exceeded(self):
        with (
            patch.object(Telegram, "SPILL_CACHE_MAX_GB", 2.0),
            patch.object(spill_cache, "_queue_byte_cap", return_value=1024),
            patch.object(spill_cache, "_handle_write", new=AsyncMock()),
        ):
            spill_cache.enqueue_spill(1, 2, "uid", 0, b"x" * 2048)
            self.assertEqual(spill_cache._dropped_chunks, 1)
            self.assertEqual(spill_cache._queued_bytes, 0)

            spill_cache.enqueue_spill(1, 2, "uid", 0, b"x" * 512)
            self.assertEqual(spill_cache._queued_bytes, 512)
            # Second 512-byte chunk fits exactly; third must drop.
            spill_cache.enqueue_spill(1, 2, "uid", 512, b"y" * 512)
            self.assertEqual(spill_cache._queued_bytes, 1024)
            spill_cache.enqueue_spill(1, 2, "uid", 1024, b"z" * 512)
            self.assertEqual(spill_cache._dropped_chunks, 2)
            self.assertEqual(spill_cache._queued_bytes, 1024)

    async def test_cap_shrinks_under_memory_pressure(self):
        with (
            patch.object(Telegram, "SPILL_CACHE_QUEUE_MAX_MB", 16),
            patch.object(spill_cache, "_mem_available_bytes", return_value=200 * 1024 * 1024),
        ):
            self.assertEqual(spill_cache._queue_byte_cap(), 16 * 1024 * 1024)
        with (
            patch.object(Telegram, "SPILL_CACHE_QUEUE_MAX_MB", 16),
            patch.object(spill_cache, "_mem_available_bytes", return_value=120 * 1024 * 1024),
        ):
            self.assertEqual(spill_cache._queue_byte_cap(), 4 * 1024 * 1024)
        with (
            patch.object(Telegram, "SPILL_CACHE_QUEUE_MAX_MB", 16),
            patch.object(spill_cache, "_mem_available_bytes", return_value=50 * 1024 * 1024),
        ):
            self.assertEqual(spill_cache._queue_byte_cap(), 0)


class CachePruneTests(unittest.TestCase):
    def setUp(self):
        self._title_backup = dict(stream_routes._title_cache)
        self._thumb_backup = dict(stream_routes._thumb_cache)
        stream_routes._title_cache.clear()
        stream_routes._thumb_cache.clear()

    def tearDown(self):
        stream_routes._title_cache.clear()
        stream_routes._title_cache.update(self._title_backup)
        stream_routes._thumb_cache.clear()
        stream_routes._thumb_cache.update(self._thumb_backup)

    def test_title_cache_prunes_expired_and_overflow(self):
        now = time.time()
        for i in range(600):
            stream_routes._title_cache[f"hash{i}"] = (f"title{i}", now + 300 + i)
        stream_routes._title_cache["expired"] = ("old", now - 1)

        stream_routes._prune_title_cache()

        self.assertLessEqual(len(stream_routes._title_cache), stream_routes._TITLE_CACHE_MAX)
        self.assertNotIn("expired", stream_routes._title_cache)
        # Lowest expiry wins pruning → the first 88 keys are gone.
        self.assertNotIn("hash0", stream_routes._title_cache)
        self.assertIn("hash599", stream_routes._title_cache)

    def test_title_cache_noop_below_limit(self):
        stream_routes._title_cache["a"] = ("t", time.time() + 300)
        stream_routes._prune_title_cache()
        self.assertIn("a", stream_routes._title_cache)

    def test_thumb_cache_prunes_to_bound(self):
        now = time.time()
        for i in range(200):
            stream_routes._thumb_cache[f"id{i}"] = (b"\xff\xd8jpeg", now + 3600 + i)

        stream_routes._prune_thumb_cache()

        self.assertLessEqual(len(stream_routes._thumb_cache), stream_routes._THUMB_CACHE_MAX)
        self.assertNotIn("id0", stream_routes._thumb_cache)
        self.assertIn("id199", stream_routes._thumb_cache)


class OwnerAlertPruneTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(owner_alerts._LAST_ALERT_AT)
        owner_alerts._LAST_ALERT_AT.clear()

    def tearDown(self):
        owner_alerts._LAST_ALERT_AT.clear()
        owner_alerts._LAST_ALERT_AT.update(self._backup)

    def test_stale_keys_pruned_when_dict_large(self):
        now = time.time()
        for i in range(600):
            owner_alerts._LAST_ALERT_AT[f"chunk-fail:stream{i}"] = now - 2 * 24 * 3600
        owner_alerts._LAST_ALERT_AT["recent"] = now

        owner_alerts._prune_last_alerts(now)

        self.assertEqual(list(owner_alerts._LAST_ALERT_AT), ["recent"])

    def test_noop_below_limit(self):
        now = time.time()
        owner_alerts._LAST_ALERT_AT["old-but-small-dict"] = now - 10 * 24 * 3600
        owner_alerts._prune_last_alerts(now)
        self.assertIn("old-but-small-dict", owner_alerts._LAST_ALERT_AT)


if __name__ == "__main__":
    unittest.main()
