import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from Backend.helper.custom_dl import TailCache, prefetch_file_tail
from Backend.config import Telegram


class TestTailCache(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cache = TailCache(max_entries=3)

    async def test_put_and_get_exact_tail(self):
        tail_data = b"X" * 1024
        await self.cache.put_tail(chat_id=-100123, message_id=456, tail_offset=9000, tail_bytes=tail_data)
        
        # Exact fetch
        result = await self.cache.get_tail(-100123, 456, start_offset=9000, length=1024)
        self.assertEqual(result, tail_data)

    async def test_get_sub_slice_of_tail(self):
        tail_data = b"0123456789" * 100  # 1000 bytes
        await self.cache.put_tail(chat_id=-100123, message_id=456, tail_offset=5000, tail_bytes=tail_data)
        
        # Sub-slice inside the tail (e.g. 5200 to 5300)
        result = await self.cache.get_tail(-100123, 456, start_offset=5200, length=100)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 100)
        self.assertEqual(result, tail_data[200:300])

    async def test_cache_miss_outside_tail(self):
        tail_data = b"Y" * 500
        await self.cache.put_tail(chat_id=-100123, message_id=456, tail_offset=9500, tail_bytes=tail_data)
        
        # Offset 0 is outside tail
        result = await self.cache.get_tail(-100123, 456, start_offset=0, length=500)
        self.assertIsNone(result)
        
        # Overlapping boundary but starts before tail_offset
        result2 = await self.cache.get_tail(-100123, 456, start_offset=9400, length=300)
        self.assertIsNone(result2)

    async def test_lru_eviction(self):
        await self.cache.put_tail(-1001, 1, 1000, b"one")
        await self.cache.put_tail(-1001, 2, 1000, b"two")
        await self.cache.put_tail(-1001, 3, 1000, b"three")
        
        # Cache max_entries=3; all 3 should be present
        self.assertEqual(await self.cache.get_tail(-1001, 1, 1000, 3), b"one")
        
        # Adding 4th should evict the least recently used entry (which is key 2 since key 1 was touched)
        await self.cache.put_tail(-1001, 4, 1000, b"four")
        
        self.assertIsNone(await self.cache.get_tail(-1001, 2, 1000, 3))
        self.assertEqual(await self.cache.get_tail(-1001, 1, 1000, 3), b"one")
        self.assertEqual(await self.cache.get_tail(-1001, 4, 1000, 4), b"four")

    async def test_prefetch_file_tail_fetches_and_caches(self):
        mock_file_id = MagicMock()
        mock_file_id.chat_id = -100999
        mock_file_id.message_id = 888
        mock_file_id.file_size = 10 * 1024 * 1024  # 10 MB

        mock_streamer = MagicMock()
        mock_streamer._get_media_session = AsyncMock(return_value="mock_session")
        mock_streamer._get_location = AsyncMock(return_value="mock_location")
        mock_streamer._fetch_file_bytes = AsyncMock(return_value=b"END_OF_FILE_INDEX_DATA")

        await prefetch_file_tail(mock_file_id, mock_streamer)

        from Backend.helper.custom_dl import TAIL_CACHE
        tail_offset = 10 * 1024 * 1024 - (256 * 1024)
        cached = await TAIL_CACHE.get_tail(-100999, 888, tail_offset, len(b"END_OF_FILE_INDEX_DATA"))
        self.assertEqual(cached, b"END_OF_FILE_INDEX_DATA")


if __name__ == "__main__":
    unittest.main()
