import unittest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
from Backend.helper.custom_dl import (
    get_client_semaphore,
    get_global_chunk_semaphore,
    record_client_floodwait,
    prefetch_file_tail,
    is_client_cooled_down,
    TAIL_CACHE,
)
from Backend.pyrofork.bot import client_cooldowns, client_dc_cooldowns
from Backend.config import Telegram


class TestRateLimitShield(unittest.IsolatedAsyncioTestCase):
    def test_client_semaphore_bounds(self):
        sem = get_client_semaphore(2)
        self.assertIsInstance(sem, asyncio.Semaphore)
        # Should match config default
        self.assertEqual(sem._value, getattr(Telegram, "TELEGRAM_MAX_CONCURRENT_PER_CLIENT", 6))

    def test_global_semaphore_bounds(self):
        sem = get_global_chunk_semaphore()
        self.assertIsInstance(sem, asyncio.Semaphore)
        self.assertEqual(sem._value, getattr(Telegram, "TELEGRAM_MAX_GLOBAL_CONCURRENT_CHUNKS", 24))

    def test_record_client_floodwait(self):
        client_cooldowns.pop(3, None)
        client_dc_cooldowns.pop((3, 5), None)

        now = time.time()
        record_client_floodwait(client_index=3, target_dc=5, wait_sec=15)

        self.assertTrue(is_client_cooled_down(3, target_dc=5))
        self.assertGreaterEqual(client_cooldowns.get(3, 0), now + 15)
        self.assertGreaterEqual(client_dc_cooldowns.get((3, 5), 0), now + 15)

        # Cleanup
        client_cooldowns.pop(3, None)
        client_dc_cooldowns.pop((3, 5), None)

    async def test_single_flight_prefetch_deduplication(self):
        mock_file_id = MagicMock()
        mock_file_id.chat_id = -100777
        mock_file_id.message_id = 999
        mock_file_id.file_size = 20 * 1024 * 1024

        mock_streamer = MagicMock()
        mock_streamer._get_media_session = AsyncMock(return_value="mock_session")
        mock_streamer._get_location = AsyncMock(return_value="mock_location")

        call_count = 0

        async def slow_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return b"TAIL_DATA_DEDUP"

        mock_streamer._fetch_file_bytes = slow_fetch

        # Trigger 4 concurrent pre-fetches for the same file
        await asyncio.gather(
            prefetch_file_tail(mock_file_id, mock_streamer),
            prefetch_file_tail(mock_file_id, mock_streamer),
            prefetch_file_tail(mock_file_id, mock_streamer),
            prefetch_file_tail(mock_file_id, mock_streamer),
        )

        # Should execute only 1 fetch call thanks to single-flight deduplication
        self.assertEqual(call_count, 1)


if __name__ == "__main__":
    unittest.main()
