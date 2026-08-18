import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from Backend.helper.custom_dl import (
    HeadCache,
    HEAD_CACHE,
    prefetch_stream_head,
)
from Backend.config import Telegram


class TestStreamPickerPreBuffer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        HEAD_CACHE.clear()

    async def test_head_cache_put_and_get_exact(self):
        cache = HeadCache(max_entries=10)
        data = b"0" * (256 * 1024)
        await cache.put_head(chat_id=123, message_id=456, head_bytes=data)

        # Full head fetch
        res = await cache.get_head(chat_id=123, message_id=456, start_offset=0, length=len(data))
        self.assertIsNotNone(res)
        self.assertEqual(res, data)

        # Small header probe (32 KB)
        res_probe = await cache.get_head(chat_id=123, message_id=456, start_offset=0, length=32768)
        self.assertIsNotNone(res_probe)
        self.assertEqual(len(res_probe), 32768)
        self.assertEqual(res_probe, data[:32768])

    async def test_head_cache_sub_slice(self):
        cache = HeadCache(max_entries=10)
        data = b"ABCDEFGHIJ" * 1000
        await cache.put_head(chat_id=123, message_id=456, head_bytes=data)

        res = await cache.get_head(chat_id=123, message_id=456, start_offset=10, length=20)
        self.assertIsNotNone(res)
        self.assertEqual(res, data[10:30])

    async def test_head_cache_miss_beyond_head(self):
        cache = HeadCache(max_entries=10)
        data = b"HEAD_DATA" * 100
        await cache.put_head(chat_id=123, message_id=456, head_bytes=data)

        # Start beyond head
        res = await cache.get_head(chat_id=123, message_id=456, start_offset=2000, length=500)
        self.assertIsNone(res)

    async def test_head_cache_lru_eviction(self):
        cache = HeadCache(max_entries=3)
        await cache.put_head(1, 1, b"HEAD_1")
        await cache.put_head(2, 2, b"HEAD_2")
        await cache.put_head(3, 3, b"HEAD_3")
        # Put 4th entry -> 1st should be evicted
        await cache.put_head(4, 4, b"HEAD_4")

        self.assertIsNone(await cache.get_head(1, 1, 0, 6))
        self.assertIsNotNone(await cache.get_head(2, 2, 0, 6))
        self.assertIsNotNone(await cache.get_head(3, 3, 0, 6))
        self.assertIsNotNone(await cache.get_head(4, 4, 0, 6))

    async def test_single_flight_head_deduplication(self):
        mock_file_id = MagicMock()
        mock_file_id.chat_id = -100888
        mock_file_id.message_id = 111
        mock_file_id.file_size = 50 * 1024 * 1024

        mock_streamer = MagicMock()
        mock_streamer._get_media_session = AsyncMock(return_value="mock_session")
        mock_streamer._get_location = AsyncMock(return_value="mock_location")

        call_count = 0

        async def slow_head_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return b"HEAD_CHUNK_0" * 1000

        mock_streamer._fetch_file_bytes = slow_head_fetch

        # 4 concurrent prefetch tasks for the same stream head
        await asyncio.gather(
            prefetch_stream_head(mock_file_id, mock_streamer),
            prefetch_stream_head(mock_file_id, mock_streamer),
            prefetch_stream_head(mock_file_id, mock_streamer),
            prefetch_stream_head(mock_file_id, mock_streamer),
        )

        # Only 1 network fetch executed
        self.assertEqual(call_count, 1)

        # Stored in HEAD_CACHE
        cached = await HEAD_CACHE.get_head(-100888, 111, 0, 100)
        self.assertIsNotNone(cached)


if __name__ == "__main__":
    unittest.main()
