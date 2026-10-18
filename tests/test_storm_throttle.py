"""Tests for the 2026-10-18 retry-storm hardening in custom_dl:

1. Round-robin chunks served by non-primary pool slots must NOT count as the
   stream primary "failing" (the attribution bug that inflated failure
   counters on every chunk and drove the cooldown storm).
2. A hedge win records the lagging slot client, not the stream primary.
3. Hedge races are capped by SMART_ROUTING_HEDGE_MAX_CONCURRENT.
4. A chunk whose pool slot points at a cooled client is substituted with the
   best non-cooled pool member instead of burning a timeout on it.
5. record_route_failure logs/alerts once per incident, not once per failure.
"""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyrogram.file_id import FileId

from Backend.config import Telegram
from Backend.helper import custom_dl
from Backend.helper.custom_dl import ByteStreamer


class _StormTestBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        custom_dl.ACTIVE_STREAMS.clear()
        custom_dl.RECENT_STREAMS.clear()
        self._failures_backup = dict(custom_dl.client_failures)
        self._cooldowns_backup = dict(custom_dl.client_cooldowns)
        self._dc_cooldowns_backup = dict(custom_dl.client_dc_cooldowns)
        custom_dl.client_failures.clear()
        custom_dl.client_cooldowns.clear()
        custom_dl.client_dc_cooldowns.clear()
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

    async def asyncTearDown(self):
        custom_dl.client_failures.clear()
        custom_dl.client_failures.update(self._failures_backup)
        custom_dl.client_cooldowns.clear()
        custom_dl.client_cooldowns.update(self._cooldowns_backup)
        custom_dl.client_dc_cooldowns.clear()
        custom_dl.client_dc_cooldowns.update(self._dc_cooldowns_backup)

    def _enter_common(self, stack, delay=0.2):
        stack.enter_context(patch.object(Telegram, "SMART_ROUTING_HEDGE_ENABLED", True))
        stack.enter_context(patch.object(Telegram, "SMART_ROUTING_HEDGE_DELAY_SEC", delay))
        stack.enter_context(patch.object(self.streamer0, "_get_media_session", return_value=MagicMock()))
        stack.enter_context(patch.object(self.streamer1, "_get_media_session", return_value=MagicMock()))
        stack.enter_context(patch.object(self.streamer0, "_get_location", return_value=MagicMock()))
        stack.enter_context(patch.object(self.streamer1, "_get_location", return_value=MagicMock()))
        stack.enter_context(patch("Backend.db.log_stream_stats", new=AsyncMock()))

    async def _run_stream(self, stream_id, part_count=1, last_part_cut=1024):
        extra = [(1, self.streamer1, self.dummy_file_id)]
        stream_gen = await self.streamer0.prefetch_stream(
            file_id=self.dummy_file_id,
            client_index=0,
            offset=0,
            first_part_cut=0,
            last_part_cut=last_part_cut,
            part_count=part_count,
            chunk_size=1024,
            prefetch=1,
            stream_id=stream_id,
            extra_clients=extra,
        )
        chunks = []
        async for chunk in stream_gen:
            chunks.append(chunk)
        return chunks


class RoundRobinAttributionTests(_StormTestBase):
    async def test_non_primary_slot_success_is_not_a_failure(self):
        """seq=1 is served by pool slot 1 (client 1) through the normal
        round-robin; the old `actual_client_idx != client_index` check
        recorded a failure on the stream primary for every such chunk."""

        async def mock_fetch(*args, **kwargs):
            await asyncio.sleep(0.02)  # fast: no hedge launches
            return b"chunk_bytes"

        from contextlib import ExitStack

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.streamer0, "_fetch_file_bytes", side_effect=mock_fetch))
            stack.enter_context(patch.object(self.streamer1, "_fetch_file_bytes", side_effect=mock_fetch))
            self._enter_common(stack, delay=0.2)
            chunks = await self._run_stream("test-robin-attribution", part_count=2, last_part_cut=2048)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks, [b"chunk_bytes", b"chunk_bytes"])
        self.assertEqual(custom_dl.client_failures.get(0, 0), 0)
        self.assertEqual(custom_dl.client_failures.get(1, 0), 0)


class HedgeAttributionTests(_StormTestBase):
    async def test_hedge_win_records_lagging_slot_client(self):
        async def slow_primary(*args, **kwargs):
            await asyncio.sleep(2.0)
            return b"primary_late_bytes"

        async def fast_hedge(*args, **kwargs):
            await asyncio.sleep(0.02)
            return b"hedge_bytes"

        from contextlib import ExitStack

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.streamer0, "_fetch_file_bytes", side_effect=slow_primary))
            stack.enter_context(patch.object(self.streamer1, "_fetch_file_bytes", side_effect=fast_hedge))
            self._enter_common(stack, delay=0.05)
            chunks = await self._run_stream("test-hedge-attribution")

        self.assertEqual(chunks, [b"hedge_bytes"])
        self.assertGreaterEqual(custom_dl.client_failures.get(0, 0), 1)
        self.assertEqual(custom_dl.client_failures.get(1, 0), 0)


class HedgeConcurrencyCapTests(_StormTestBase):
    async def test_hedge_skipped_when_cap_reached(self):
        call_counts = {"primary": 0, "hedge": 0}

        async def primary(*args, **kwargs):
            call_counts["primary"] += 1
            await asyncio.sleep(0.2)
            return b"primary_bytes"

        async def hedge(*args, **kwargs):
            call_counts["hedge"] += 1
            return b"hedge_bytes"

        from contextlib import ExitStack

        custom_dl._hedge_inflight = 1  # simulate another stream already hedging
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(Telegram, "SMART_ROUTING_HEDGE_MAX_CONCURRENT", 1))
                stack.enter_context(patch.object(Telegram, "SMART_ROUTING_CHUNK_TIMEOUT_SEC", 2.0))
                stack.enter_context(patch.object(self.streamer0, "_fetch_file_bytes", side_effect=primary))
                stack.enter_context(patch.object(self.streamer1, "_fetch_file_bytes", side_effect=hedge))
                self._enter_common(stack, delay=0.05)
                chunks = await self._run_stream("test-hedge-cap")
        finally:
            custom_dl._hedge_inflight = 0

        self.assertEqual(chunks, [b"primary_bytes"])
        self.assertEqual(call_counts["primary"], 1)
        self.assertEqual(call_counts["hedge"], 0)


class CooledSlotSubstitutionTests(_StormTestBase):
    async def test_cooled_slot_client_is_substituted(self):
        call_counts = {"client0": 0, "client1": 0}

        async def fetch0(*args, **kwargs):
            call_counts["client0"] += 1
            await asyncio.sleep(0.02)
            return b"client0_bytes"

        async def fetch1(*args, **kwargs):
            call_counts["client1"] += 1
            await asyncio.sleep(0.02)
            return b"client1_bytes"

        custom_dl.client_dc_cooldowns[(0, 5)] = time.time() + 60

        from contextlib import ExitStack

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.streamer0, "_fetch_file_bytes", side_effect=fetch0))
            stack.enter_context(patch.object(self.streamer1, "_fetch_file_bytes", side_effect=fetch1))
            self._enter_common(stack, delay=0.2)
            chunks = await self._run_stream("test-cooled-substitution")

        self.assertEqual(chunks, [b"client1_bytes"])
        self.assertEqual(call_counts["client0"], 0, "cooled slot client must not be dispatched")
        self.assertEqual(call_counts["client1"], 1)

        entry = custom_dl.ACTIVE_STREAMS.get("test-cooled-substitution") or {}
        events = [e.get("event") for e in entry.get("route_attempts", [])]
        self.assertIn("cooled_slot_substituted", events)
        self.assertEqual(custom_dl.client_failures.get(0, 0), 0)


class CooldownLogGatingTests(unittest.TestCase):
    def setUp(self):
        self._failures_backup = dict(custom_dl.client_failures)
        self._errors_backup = {k: dict(v) for k, v in custom_dl.client_last_errors.items()}
        self._cooldowns_backup = dict(custom_dl.client_cooldowns)
        self._dc_cooldowns_backup = dict(custom_dl.client_dc_cooldowns)
        custom_dl.client_failures.clear()
        custom_dl.client_last_errors.clear()
        custom_dl.client_cooldowns.clear()
        custom_dl.client_dc_cooldowns.clear()

    def tearDown(self):
        custom_dl.client_failures.clear()
        custom_dl.client_failures.update(self._failures_backup)
        custom_dl.client_last_errors.clear()
        custom_dl.client_last_errors.update(self._errors_backup)
        custom_dl.client_cooldowns.clear()
        custom_dl.client_cooldowns.update(self._cooldowns_backup)
        custom_dl.client_dc_cooldowns.clear()
        custom_dl.client_dc_cooldowns.update(self._dc_cooldowns_backup)

    def test_warning_logged_once_per_incident_not_per_failure(self):
        with (
            patch.object(Telegram, "SMART_ROUTING_COOLDOWN_FAILURES", 2),
            patch.object(Telegram, "SMART_ROUTING_COOLDOWN_SEC", 60),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", return_value=False),
            patch.object(custom_dl.LOGGER, "warning") as warn,
        ):
            for i in range(6):
                custom_dl.record_route_failure(1, 4, "fallback_needed", stream_id="t", offset=i, attempt=1)

            self.assertEqual(warn.call_count, 1, "one incident → one cooldown warning")
            self.assertTrue(custom_dl.is_client_cooled_down(1, 4))
            # Counter is clamped while the same incident keeps extending the
            # cooldown (it reached 1533 during the 2026-10-18 storm).
            self.assertLessEqual(custom_dl.client_failures.get(1), 8)

            # Cooldown expiry → next failure is a NEW incident → logs again.
            past = time.time() - 1
            custom_dl.client_cooldowns[1] = past
            custom_dl.client_dc_cooldowns[(1, 4)] = past
            custom_dl.record_route_failure(1, 4, "fallback_needed", stream_id="t", offset=99, attempt=1)
            self.assertEqual(warn.call_count, 2, "re-entry after expiry → new cooldown warning")


if __name__ == "__main__":
    unittest.main()
