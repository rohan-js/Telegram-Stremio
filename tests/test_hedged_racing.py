import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyrogram.file_id import FileId

from Backend.config import Telegram
from Backend.helper import custom_dl
from Backend.helper.custom_dl import ByteStreamer


class HedgedChunkRacingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        custom_dl.ACTIVE_STREAMS.clear()
        custom_dl.RECENT_STREAMS.clear()
        custom_dl.work_loads[0] = 0
        custom_dl.work_loads[1] = 0
        self.mock_client0 = MagicMock()
        self.mock_client0.media_sessions = {}
        self.mock_client1 = MagicMock()
        self.mock_client1.media_sessions = {}

        self.streamer0 = ByteStreamer(self.mock_client0, 0)
        self.streamer1 = ByteStreamer(self.mock_client1, 1)

        self.dummy_file_id = MagicMock(spec=FileId)
        self.dummy_file_id.dc_id = 5
        self.dummy_file_id.file_type = 1
        self.dummy_file_id.chat_id = -100123
        self.dummy_file_id.local_id = 456

    async def test_fast_chunk_does_not_trigger_hedge(self):
        """When primary worker responds within hedge delay, no hedge is triggered."""
        call_counts = {"primary": 0, "hedge": 0}

        async def mock_fetch_primary(*args, **kwargs):
            call_counts["primary"] += 1
            await asyncio.sleep(0.05)  # 50ms < 200ms hedge delay
            return b"primary_fast_bytes"

        async def mock_fetch_hedge(*args, **kwargs):
            call_counts["hedge"] += 1
            return b"hedge_bytes"

        with (
            patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", True),
            patch.object(Telegram, "SMART_ROUTING_HEDGE_DELAY_SEC", 0.2),
            patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch_primary),
            patch.object(self.streamer1, "_fetch_file_bytes", side_effect=mock_fetch_hedge),
            patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer0, "_get_location", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_location", return_value=MagicMock()),
            patch("Backend.db.log_stream_stats", new=AsyncMock()),
        ):
            extra = [(1, self.streamer1, self.dummy_file_id)]
            stream_gen = await self.streamer0.prefetch_stream(
                file_id=self.dummy_file_id,
                client_index=0,
                offset=0,
                first_part_cut=0,
                last_part_cut=1024,
                part_count=1,
                chunk_size=1024,
                prefetch=1,
                stream_id="test-fast-chunk",
                extra_clients=extra,
            )

            chunks = []
            async for chunk in stream_gen:
                chunks.append(chunk)

            self.assertEqual(chunks, [b"primary_fast_bytes"])
            self.assertEqual(call_counts["primary"], 1)
            self.assertEqual(call_counts["hedge"], 0)
            entry = custom_dl.ACTIVE_STREAMS.get("test-fast-chunk") or {}
            self.assertEqual(entry.get("hedge_rescues", 0), 0)

    async def test_stalled_primary_triggers_hedge_and_hedge_wins(self):
        """When primary worker stalls (>hedge delay), hedge task is launched and delivers chunk."""
        call_counts = {"primary": 0, "hedge": 0}

        async def mock_fetch_primary(*args, **kwargs):
            call_counts["primary"] += 1
            await asyncio.sleep(2.0)  # stalls for 2 seconds
            return b"primary_stalled_bytes"

        async def mock_fetch_hedge(*args, **kwargs):
            call_counts["hedge"] += 1
            await asyncio.sleep(0.05)  # fast 50ms response
            return b"hedge_rescued_bytes"

        with (
            patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", True),
            patch.object(Telegram, "SMART_ROUTING_HEDGE_DELAY_SEC", 0.1),
            patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch_primary),
            patch.object(self.streamer1, "_fetch_file_bytes", side_effect=mock_fetch_hedge),
            patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer0, "_get_location", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_location", return_value=MagicMock()),
            patch("Backend.db.log_stream_stats", new=AsyncMock()),
        ):
            extra = [(1, self.streamer1, self.dummy_file_id)]
            stream_gen = await self.streamer0.prefetch_stream(
                file_id=self.dummy_file_id,
                client_index=0,
                offset=0,
                first_part_cut=0,
                last_part_cut=1024,
                part_count=1,
                chunk_size=1024,
                prefetch=1,
                stream_id="test-hedge-rescue",
                extra_clients=extra,
            )

            chunks = []
            async for chunk in stream_gen:
                chunks.append(chunk)

            self.assertEqual(chunks, [b"hedge_rescued_bytes"])
            self.assertEqual(call_counts["primary"], 1)
            self.assertEqual(call_counts["hedge"], 1)
            entry = custom_dl.ACTIVE_STREAMS.get("test-hedge-rescue") or {}
            self.assertEqual(entry.get("hedge_rescues", 0), 1)

            # Confirm hedge event logged in route_attempts
            events = [e.get("event") for e in entry.get("route_attempts", [])]
            self.assertIn("hedge_race_started", events)
            self.assertIn("hedge_race_won", events)

    async def test_primary_finishes_before_hedge_wins(self):
        """When primary stalls slightly past delay but finishes before hedge, primary result is accepted."""
        async def mock_fetch_primary(*args, **kwargs):
            await asyncio.sleep(0.12)  # finishes at 120ms
            return b"primary_bytes"

        async def mock_fetch_hedge(*args, **kwargs):
            await asyncio.sleep(0.40)  # hedge takes 400ms
            return b"hedge_bytes"

        with (
            patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", True),
            patch.object(Telegram, "SMART_ROUTING_HEDGE_DELAY_SEC", 0.05),
            patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch_primary),
            patch.object(self.streamer1, "_fetch_file_bytes", side_effect=mock_fetch_hedge),
            patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer0, "_get_location", return_value=MagicMock()),
            patch.object(self.streamer1, "_get_location", return_value=MagicMock()),
            patch("Backend.db.log_stream_stats", new=AsyncMock()),
        ):
            extra = [(1, self.streamer1, self.dummy_file_id)]
            stream_gen = await self.streamer0.prefetch_stream(
                file_id=self.dummy_file_id,
                client_index=0,
                offset=0,
                first_part_cut=0,
                last_part_cut=1024,
                part_count=1,
                chunk_size=1024,
                prefetch=1,
                stream_id="test-primary-wins",
                extra_clients=extra,
            )

            chunks = []
            async for chunk in stream_gen:
                chunks.append(chunk)

            self.assertEqual(chunks, [b"primary_bytes"])
            entry = custom_dl.ACTIVE_STREAMS.get("test-primary-wins") or {}
            self.assertEqual(entry.get("hedge_rescues", 0), 0)

    async def test_single_client_bypasses_hedge(self):
        """Single-client pools don't attempt hedging."""
        async def mock_fetch_primary(*args, **kwargs):
            await asyncio.sleep(0.05)
            return b"single_client_bytes"

        with (
            patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", True),
            patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch_primary),
            patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer0, "_get_location", return_value=MagicMock()),
            patch("Backend.db.log_stream_stats", new=AsyncMock()),
        ):
            stream_gen = await self.streamer0.prefetch_stream(
                file_id=self.dummy_file_id,
                client_index=0,
                offset=0,
                first_part_cut=0,
                last_part_cut=1024,
                part_count=1,
                chunk_size=1024,
                prefetch=1,
                stream_id="test-single-client",
                extra_clients=None,
            )

            chunks = []
            async for chunk in stream_gen:
                chunks.append(chunk)

            self.assertEqual(chunks, [b"single_client_bytes"])

    async def test_timeout_then_success_recovers_chunk(self):
        """Regression: a chunk attempt failing with asyncio.TimeoutError must be
        retried, not crash fetch_chunk_with_retries with UnboundLocalError
        (`is_flood` was only assigned in the generic Exception branch —
        prod incident 2026-08-23 02:40 IST, log "Error processing completed
        fetch task")."""
        calls = {"n": 0}

        async def mock_fetch_flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise asyncio.TimeoutError("simulated chunk timeout")
            return b"recovered_after_timeout"

        with (
            patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", False),
            patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch_flaky),
            patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()),
            patch.object(self.streamer0, "_get_location", return_value=MagicMock()),
            patch("Backend.db.log_stream_stats", new=AsyncMock()),
        ):
            stream_gen = await self.streamer0.prefetch_stream(
                file_id=self.dummy_file_id,
                client_index=0,
                offset=0,
                first_part_cut=0,
                last_part_cut=1024,
                part_count=1,
                chunk_size=1024,
                prefetch=1,
                stream_id="test-timeout-retry",
                extra_clients=None,
            )

            chunks = []
            async for chunk in stream_gen:
                chunks.append(chunk)

            self.assertEqual(chunks, [b"recovered_after_timeout"])
            self.assertEqual(calls["n"], 2)  # first attempt timed out, retry succeeded
            entry = custom_dl.ACTIVE_STREAMS.get("test-timeout-retry") or {}
            self.assertEqual(entry.get("chunk_timeouts"), 1)
            self.assertNotEqual(entry.get("status"), "error")


if __name__ == "__main__":
    unittest.main()
