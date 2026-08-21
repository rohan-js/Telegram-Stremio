"""Tests for Backend.helper.spill_cache — streamed-chunk range disk cache."""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper import disk_cache, spill_cache


class SpillCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._patches = [
            patch.object(Telegram, "SPILL_CACHE_ENABLED", True),
            patch.object(Telegram, "SPILL_CACHE_MAX_GB", 1.0),
            patch.object(Telegram, "DISK_CACHE_ENABLED", True),
            patch.object(disk_cache.Telegram, "DISK_CACHE_DIR", self._tmp.name),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)
        spill_cache._extents.clear()
        spill_cache._write_queue = None
        spill_cache._writer_task = None
        spill_cache._swept_at_startup = False
        spill_cache._dropped_chunks = 0

    async def test_write_and_read_back(self):
        data = b"A" * 1024
        await spill_cache._handle_write((-100, 5, "uid", 0, data))
        got = await spill_cache.read_spilled(-100, 5, "uid", 0, 1024)
        self.assertEqual(got, data)

    async def test_contiguous_chunks_merge(self):
        await spill_cache._handle_write((-100, 6, "uid", 0, b"B" * 1024))
        await spill_cache._handle_write((-100, 6, "uid", 1024, b"C" * 1024))
        # Read spanning the chunk boundary
        got = await spill_cache.read_spilled(-100, 6, "uid", 512, 1024)
        self.assertEqual(got, b"B" * 512 + b"C" * 512)

    async def test_partial_coverage_returns_none(self):
        await spill_cache._handle_write((-100, 7, "uid", 0, b"D" * 1024))
        self.assertIsNone(await spill_cache.read_spilled(-100, 7, "uid", 0, 2048))
        self.assertIsNone(await spill_cache.read_spilled(-100, 7, "uid", 2048, 100))
        self.assertIsNone(await spill_cache.read_spilled(-100, 8, "uid", 0, 10))

    async def test_duplicate_write_does_not_double_count(self):
        entry_key = (-100, 9, "uid")
        await spill_cache._handle_write((entry_key[0], entry_key[1], entry_key[2], 0, b"E" * 1000))
        before = spill_cache._extents[entry_key]["bytes"]
        await spill_cache._handle_write((entry_key[0], entry_key[1], entry_key[2], 0, b"E" * 1000))
        self.assertEqual(spill_cache._extents[entry_key]["bytes"], before)

    async def test_enqueue_never_blocks_on_full_queue(self):
        # Freeze the writer so the queue fills up
        with patch.object(spill_cache, "_ensure_writer_task", lambda: None):
            q = spill_cache._ensure_write_queue()
            for i in range(spill_cache._WRITE_QUEUE_MAX):
                q.put_nowait((1, 1, "uid", i * 16, b"x" * 16))
            spill_cache.enqueue_spill(1, 1, "uid", 9999, b"y" * 16)  # must not raise
            self.assertEqual(spill_cache._dropped_chunks, 1)

    async def test_budget_evicts_oldest_file(self):
        with patch.object(Telegram, "SPILL_CACHE_MAX_GB", 0.000004):  # ~4290 bytes
            await spill_cache._handle_write((-100, 10, "uidA", 0, b"F" * 2048))
            await asyncio.sleep(0.01)
            await spill_cache._handle_write((-100, 11, "uidB", 0, b"G" * 4096))
            await asyncio.sleep(0.05)
            # First (older) file evicted; second survives
            self.assertIsNone(await spill_cache.read_spilled(-100, 10, "uidA", 0, 16))
            self.assertIsNotNone(await spill_cache.read_spilled(-100, 11, "uidB", 0, 16))
            stats = await spill_cache.get_spill_stats()
            self.assertLessEqual(stats["bytes"], 4290 + 4096)

    async def test_disabled_returns_none(self):
        with patch.object(Telegram, "SPILL_CACHE_ENABLED", False):
            self.assertFalse(spill_cache.spill_enabled())
            await spill_cache._handle_write((-100, 12, "uid", 0, b"H" * 64))
            self.assertIsNone(await spill_cache.read_spilled(-100, 12, "uid", 0, 64))

    def test_startup_sweep_removes_stale_range_files(self):
        root = Path(self._tmp.name)
        stale = root / "aa" / "deadbeef.ranges.bin"
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_bytes(b"stale")
        keep = root / "bb" / "cafe.someother.bin"
        keep.parent.mkdir(parents=True, exist_ok=True)
        keep.write_bytes(b"keep")
        spill_cache._purge_all_range_files()
        self.assertFalse(stale.exists())
        self.assertTrue(keep.exists())


if __name__ == "__main__":
    unittest.main()
