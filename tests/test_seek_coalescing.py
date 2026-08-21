import unittest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
from Backend.helper.custom_dl import (
    SeekCache,
    SEEK_CACHE,
    prefetch_seek_window,
)


class TestSeekCoalescing(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        SEEK_CACHE.clear()

    async def test_seek_cache_put_and_get_exact(self):
        cache = SeekCache(max_entries=10, ttl_sec=5.0)
        block = b"S" * (512 * 1024)
        offset = 52428800  # 50 MB
        await cache.put_seek_block(chat_id=123, message_id=456, block_start=offset, block_bytes=block)

        # Exact probe fetch (16 KB)
        res = await cache.get_seek_range(chat_id=123, message_id=456, start_offset=offset, length=16384)
        self.assertIsNotNone(res)
        self.assertEqual(len(res), 16384)
        self.assertEqual(res, block[:16384])

    async def test_seek_cache_multiple_micro_ranges_within_window(self):
        cache = SeekCache(max_entries=10, ttl_sec=5.0)
        block = b"0123456789ABCDEF" * (32 * 1024)  # 512 KB
        offset = 10000000
        await cache.put_seek_block(chat_id=100, message_id=200, block_start=offset, block_bytes=block)

        # Micro-range 1 (16 KB)
        r1 = await cache.get_seek_range(100, 200, offset, 16384)
        self.assertIsNotNone(r1)
        self.assertEqual(r1, block[:16384])

        # Micro-range 2 (32 KB at offset + 16KB)
        r2 = await cache.get_seek_range(100, 200, offset + 16384, 32768)
        self.assertIsNotNone(r2)
        self.assertEqual(r2, block[16384:16384 + 32768])

        # Micro-range 3 (64 KB at offset + 48KB)
        r3 = await cache.get_seek_range(100, 200, offset + 49152, 65536)
        self.assertIsNotNone(r3)
        self.assertEqual(r3, block[49152:49152 + 65536])

    async def test_seek_cache_miss_outside_window(self):
        cache = SeekCache(max_entries=10, ttl_sec=5.0)
        block = b"DATA" * 1000
        await cache.put_seek_block(chat_id=1, message_id=1, block_start=1000, block_bytes=block)

        # Range before block
        self.assertIsNone(await cache.get_seek_range(1, 1, 500, 200))
        # Range after block
        self.assertIsNone(await cache.get_seek_range(1, 1, 6000, 200))

    async def test_seek_cache_ttl_expiration(self):
        cache = SeekCache(max_entries=10, ttl_sec=0.05)  # 50ms TTL
        await cache.put_seek_block(1, 1, 1000, b"EXPIRE_TEST")
        # Immediate read -> hit
        self.assertIsNotNone(await cache.get_seek_range(1, 1, 1000, 11))

        # Wait past TTL
        await asyncio.sleep(0.08)
        self.assertIsNone(await cache.get_seek_range(1, 1, 1000, 11))

    async def test_seek_cache_lru_eviction(self):
        cache = SeekCache(max_entries=2, ttl_sec=10.0)
        await cache.put_seek_block(1, 1, 0, b"BLOCK_1")
        await cache.put_seek_block(2, 2, 0, b"BLOCK_2")
        # Put 3rd -> 1st evicted
        await cache.put_seek_block(3, 3, 0, b"BLOCK_3")

        self.assertIsNone(await cache.get_seek_range(1, 1, 0, 7))
        self.assertIsNotNone(await cache.get_seek_range(2, 2, 0, 7))
        self.assertIsNotNone(await cache.get_seek_range(3, 3, 0, 7))

    async def test_prefetch_seek_window_deduplication(self):
        mock_file_id = MagicMock()
        mock_file_id.chat_id = -100999
        mock_file_id.message_id = 777
        mock_file_id.file_size = 500000000

        mock_streamer = MagicMock()
        fetch_mock = AsyncMock(return_value=b"SEEK_WINDOW_DATA_" * 1000)
        mock_streamer._fetch_file_bytes = fetch_mock
        mock_streamer._get_media_session = AsyncMock(return_value="session")
        mock_streamer._get_location = AsyncMock(return_value="loc")

        # Concurrent prefetch calls for the same seek region
        t1 = asyncio.create_task(
            prefetch_seek_window(mock_file_id, mock_streamer, chat_id=-100999, message_id=777, start_offset=1048576)
        )
        t2 = asyncio.create_task(
            prefetch_seek_window(mock_file_id, mock_streamer, chat_id=-100999, message_id=777, start_offset=1048576)
        )
        r1, r2 = await asyncio.gather(t1, t2)

        self.assertEqual(r1, r2)
        # Should only fetch from MTProto once due to single-flight deduplication
        self.assertEqual(fetch_mock.call_count, 1)


class TestSeekCacheMultiWindow(unittest.IsolatedAsyncioTestCase):
    """SEEK_CACHE_WINDOWS_PER_FILE: several 512KB windows coexist per file."""

    async def asyncSetUp(self):
        SEEK_CACHE.clear()

    async def test_multiple_windows_coexist(self):
        cache = SeekCache(max_entries=10, ttl_sec=10.0, windows_per_file=3)
        await cache.put_seek_block(5, 5, 0, b"A" * 1000)
        await cache.put_seek_block(5, 5, 524288, b"B" * 1000)
        r1 = await cache.get_seek_range(5, 5, 0, 10)
        r2 = await cache.get_seek_range(5, 5, 524288, 10)
        self.assertEqual(r1, b"A" * 10)
        self.assertEqual(r2, b"B" * 10)

    async def test_per_file_window_cap_evicts_oldest(self):
        cache = SeekCache(max_entries=10, ttl_sec=10.0, windows_per_file=2)
        await cache.put_seek_block(6, 6, 0, b"W1")
        await cache.put_seek_block(6, 6, 524288, b"W2")
        await cache.put_seek_block(6, 6, 1048576, b"W3")  # evicts W1
        self.assertIsNone(await cache.get_seek_range(6, 6, 0, 2))
        self.assertIsNotNone(await cache.get_seek_range(6, 6, 524288, 2))
        self.assertIsNotNone(await cache.get_seek_range(6, 6, 1048576, 2))

    async def test_window_access_refreshes_recency(self):
        cache = SeekCache(max_entries=10, ttl_sec=10.0, windows_per_file=2)
        await cache.put_seek_block(7, 7, 0, b"W1")
        await cache.put_seek_block(7, 7, 524288, b"W2")
        # Touch W1 so W2 becomes the LRU window
        self.assertIsNotNone(await cache.get_seek_range(7, 7, 0, 1))
        await cache.put_seek_block(7, 7, 1048576, b"W3")
        self.assertIsNotNone(await cache.get_seek_range(7, 7, 0, 2))
        self.assertIsNone(await cache.get_seek_range(7, 7, 524288, 2))
        self.assertIsNotNone(await cache.get_seek_range(7, 7, 1048576, 2))

    async def test_ttl_expires_single_window_only(self):
        cache = SeekCache(max_entries=10, ttl_sec=0.05, windows_per_file=3)
        await cache.put_seek_block(8, 8, 0, b"W1")
        await asyncio.sleep(0.08)
        await cache.put_seek_block(8, 8, 524288, b"W2")
        self.assertIsNone(await cache.get_seek_range(8, 8, 0, 2))
        self.assertIsNotNone(await cache.get_seek_range(8, 8, 524288, 2))


if __name__ == "__main__":
    unittest.main()
