import unittest
from unittest.mock import patch

from Backend.fastapi.routes import stream_routes
from Backend.fastapi.routes.stream_routes import (
    _client_route_trusted,
    choose_effective_prefetch,
    choose_smart_client,
    get_configured_stream_concurrency,
    resolve_video_mime_type,
    select_telegram_chunk_size,
    should_probe_request,
)
from Backend.helper import custom_dl


class StreamMimeTypeTests(unittest.TestCase):
    def test_known_video_extension_overrides_generic_telegram_mime(self):
        self.assertEqual(
            resolve_video_mime_type("movie.mkv", "application/octet-stream"),
            "video/x-matroska",
        )

    def test_known_video_extension_works_without_telegram_mime(self):
        self.assertEqual(resolve_video_mime_type("movie.mp4", None), "video/mp4")

    def test_unknown_extension_preserves_valid_telegram_video_mime(self):
        self.assertEqual(
            resolve_video_mime_type("movie.custom", "video/x-custom"),
            "video/x-custom",
        )

    def test_unknown_extension_with_generic_mime_falls_back_safely(self):
        self.assertEqual(
            resolve_video_mime_type("movie.unknownext", "application/octet-stream"),
            "application/octet-stream",
        )


class AdaptivePrefetchTests(unittest.TestCase):
    def test_parallel_controls_parallelism_and_pre_fetch_controls_prefetch(self):
        with (
            patch.object(stream_routes.Telegram, "PARALLEL", 5),
            patch.object(stream_routes.Telegram, "PRE_FETCH", 2),
            patch.object(stream_routes.Telegram, "ADAPTIVE_PREFETCH_ENABLED", True),
        ):
            configured_prefetch, configured_parallelism = get_configured_stream_concurrency()
            result = choose_effective_prefetch(
                configured_prefetch,
                configured_parallelism,
                file_size=2 * 1024 ** 3,
                request_length=256 * 1024 ** 2,
                active_streams=1,
                mem_available_mb=512,
            )

        self.assertEqual((configured_prefetch, configured_parallelism), (2, 5))
        self.assertEqual(result, (2, 5, "healthy"))

    def test_healthy_single_stream_keeps_configured_values(self):
        with patch.object(stream_routes.Telegram, "ADAPTIVE_PREFETCH_ENABLED", True):
            result = choose_effective_prefetch(
                3,
                3,
                file_size=2 * 1024 ** 3,
                request_length=256 * 1024 ** 2,
                active_streams=1,
                mem_available_mb=512,
            )
        self.assertEqual(result, (3, 3, "healthy"))

    def test_low_memory_reduces_to_one_one(self):
        with patch.object(stream_routes.Telegram, "ADAPTIVE_PREFETCH_ENABLED", True):
            result = choose_effective_prefetch(
                3,
                3,
                file_size=2 * 1024 ** 3,
                request_length=256 * 1024 ** 2,
                active_streams=1,
                mem_available_mb=80,
            )
        self.assertEqual(result[0:2], (1, 1))
        self.assertTrue(result[2].startswith("low_mem"))

    def test_multi_stream_reduces_to_two_two(self):
        with patch.object(stream_routes.Telegram, "ADAPTIVE_PREFETCH_ENABLED", True):
            result = choose_effective_prefetch(
                3,
                3,
                file_size=2 * 1024 ** 3,
                request_length=256 * 1024 ** 2,
                active_streams=2,
                mem_available_mb=512,
            )
        self.assertEqual(result[0:2], (2, 2))


class ClientCooldownTests(unittest.TestCase):
    def tearDown(self):
        custom_dl.client_cooldowns.clear()
        custom_dl.client_dc_cooldowns.clear()
        custom_dl.client_failures.clear()
        custom_dl.client_last_errors.clear()

    def test_record_route_failure_sets_cooldown_after_threshold(self):
        with patch.object(custom_dl.Telegram, "SMART_ROUTING_COOLDOWN_FAILURES", 2), patch.object(custom_dl.Telegram, "SMART_ROUTING_COOLDOWN_SEC", 60):
            custom_dl.record_route_failure(1, 5, "timeout", stream_id="abc", offset=0, attempt=1)
            self.assertFalse(custom_dl.is_client_cooled_down(1, 5))
            custom_dl.record_route_failure(1, 5, "timeout", stream_id="abc", offset=1024, attempt=2)
            self.assertTrue(custom_dl.is_client_cooled_down(1, 5))

        state = custom_dl.get_client_cooldown_state()
        self.assertIn("1", state)
        self.assertEqual(state["1"]["last_error"]["reason"], "timeout")


class TelegramChunkSizeTests(unittest.TestCase):
    def test_range_request_uses_seek_friendly_chunk_size(self):
        self.assertEqual(select_telegram_chunk_size("bytes=1000-"), 512 * 1024)

    def test_full_stream_uses_throughput_chunk_size(self):
        self.assertEqual(select_telegram_chunk_size(""), 1024 * 1024)
        self.assertEqual(select_telegram_chunk_size(None), 1024 * 1024)


class ShouldProbeRequestTests(unittest.TestCase):
    def test_open_session_request_probes(self):
        self.assertTrue(should_probe_request("", 0))
        self.assertTrue(should_probe_request(None, 0))

    def test_suffix_range_probes(self):
        self.assertTrue(should_probe_request("bytes=-2048", 0))

    def test_full_from_start_range_probes(self):
        self.assertTrue(should_probe_request("bytes=0-", 0))

    def test_mid_file_seek_skips_probe(self):
        self.assertFalse(should_probe_request("bytes=1048576-", 1048576))
        self.assertFalse(should_probe_request("bytes=1024-2048", 1024))


class ClientRouteTrustTests(unittest.TestCase):
    """Instant-start trust window: a fresh (client, DC) route skips the probe."""

    def setUp(self):
        custom_dl.client_dc_last_seen.clear()

    def tearDown(self):
        custom_dl.client_dc_last_seen.clear()

    def test_fresh_route_is_trusted(self):
        custom_dl.client_dc_last_seen[(0, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_010.0
                self.assertTrue(_client_route_trusted(0, 2))

    def test_stale_route_is_not_trusted(self):
        custom_dl.client_dc_last_seen[(0, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_061.0
                self.assertFalse(_client_route_trusted(0, 2))

    def test_unknown_route_is_not_trusted(self):
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_000.0
                self.assertFalse(_client_route_trusted(3, 2))

    def test_zero_trust_window_disables_skip(self):
        custom_dl.client_dc_last_seen[(0, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 0.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_001.0
                self.assertFalse(_client_route_trusted(0, 2))

    def test_wrong_dc_is_never_trusted(self):
        custom_dl.client_dc_last_seen[(0, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_001.0
                self.assertFalse(_client_route_trusted(0, 3))

    def test_any_fresh_route_for_dc_skips_probe(self):
        # A different client proved DC 2 reachable; the base client itself has
        # no stamp yet (fresh-boot case) but re-probing is still unnecessary.
        custom_dl.client_dc_last_seen[(5, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_010.0
                self.assertTrue(_client_route_trusted(0, 2))

    def test_stale_any_route_for_dc_still_probes(self):
        custom_dl.client_dc_last_seen[(5, 2)] = 1_000_000.0
        with patch.object(stream_routes.Telegram, "SMART_ROUTING_PROBE_TRUST_SEC", 60.0):
            with patch.object(stream_routes, "time") as fake_time:
                fake_time.time.return_value = 1_000_061.0
                self.assertFalse(_client_route_trusted(0, 2))


class ProbeStampsTrustWindowTests(unittest.TestCase):
    """A successful probe must stamp every candidate route as recently-seen,
    otherwise repeat opens keep re-probing because the base client itself never
    streams chunks and therefore never gets a client_dc_last_seen entry."""

    def setUp(self):
        custom_dl.client_dc_last_seen.clear()

    def tearDown(self):
        custom_dl.client_dc_last_seen.clear()

    def test_successful_probe_stamps_every_candidate_route(self):
        import asyncio
        from types import SimpleNamespace

        class FakeFileId:
            dc_id = 2
            file_size = 1000
            unique_id = "abc123"

        def make_streamer(idx):
            streamer = SimpleNamespace(client_index=idx)

            async def get_file_properties(chat_id=None, message_id=None):
                return FakeFileId()

            async def probe_file(chat_id=None, message_id=None, offset=0, limit=0, timeout=0):
                return {
                    "ok": True,
                    "file_id": FakeFileId(),
                    "client_index": idx,
                    "ttfb_sec": 0.3,
                    "mbps": 10.0,
                }

            async def _get_media_session(file_id=None):
                return None

            streamer.get_file_properties = get_file_properties
            streamer.probe_file = probe_file
            streamer._get_media_session = _get_media_session
            return streamer

        import time as real_time

        with (
            patch.object(stream_routes, "select_probe_candidates", return_value=[2, 0, 5]),
            patch.object(stream_routes, "get_streamer", side_effect=make_streamer),
            patch.dict(stream_routes.multi_clients, {0: object(), 2: object(), 5: object()}),
        ):
            asyncio.run(
                stream_routes.choose_smart_client(
                    request=SimpleNamespace(method="GET"),
                    chat_id=1,
                    msg_id=2,
                    target_dc=2,
                    base_index=2,
                    probe_offset=0,
                )
            )

        for idx in (2, 0, 5):
            stamp = custom_dl.client_dc_last_seen.get((idx, 2))
            self.assertIsNotNone(stamp, f"no trust stamp for probed client {idx}")
            self.assertLessEqual(real_time.time() - float(stamp), 5)

    def test_failed_probe_leaves_no_stamp(self):
        import asyncio
        from types import SimpleNamespace

        class FakeFileId:
            dc_id = 2
            file_size = 1000
            unique_id = "abc123"

        def make_streamer(idx):
            streamer = SimpleNamespace(client_index=idx)

            async def get_file_properties(chat_id=None, message_id=None):
                return FakeFileId()

            async def probe_file(chat_id=None, message_id=None, offset=0, limit=0, timeout=0):
                if idx == 2:
                    return {"ok": False, "client_index": idx, "error": "boom"}
                return {
                    "ok": True,
                    "file_id": FakeFileId(),
                    "client_index": idx,
                    "ttfb_sec": 0.3,
                    "mbps": 10.0,
                }

            async def _get_media_session(file_id=None):
                return None

            streamer.get_file_properties = get_file_properties
            streamer.probe_file = probe_file
            streamer._get_media_session = _get_media_session
            return streamer

        with (
            patch.object(stream_routes, "select_probe_candidates", return_value=[2, 0, 5]),
            patch.object(stream_routes, "get_streamer", side_effect=make_streamer),
            patch.dict(stream_routes.multi_clients, {0: object(), 2: object(), 5: object()}),
        ):
            asyncio.run(
                stream_routes.choose_smart_client(
                    request=SimpleNamespace(method="GET"),
                    chat_id=1,
                    msg_id=2,
                    target_dc=2,
                    base_index=2,
                    probe_offset=0,
                )
            )

        self.assertNotIn((2, 2), custom_dl.client_dc_last_seen)
        self.assertIsNotNone(custom_dl.client_dc_last_seen.get((0, 2)))
        self.assertIsNotNone(custom_dl.client_dc_last_seen.get((5, 2)))


class LookupTitleCacheTests(unittest.TestCase):
    def tearDown(self):
        stream_routes._title_cache.clear()

    def test_first_lookup_is_miss_second_is_hit(self):
        import asyncio

        with patch.object(stream_routes.db, "get_title_by_stream_id", side_effect=["Alpha"]) as mocked:
            first = asyncio.run(stream_routes._lookup_title("hash1", "fallback"))
            second = asyncio.run(stream_routes._lookup_title("hash1", "fallback"))

        self.assertEqual(first, "Alpha")
        self.assertEqual(second, "Alpha")
        self.assertEqual(mocked.call_count, 1)

    def test_cache_expires_after_ttl(self):
        import asyncio

        with patch.object(stream_routes.db, "get_title_by_stream_id", side_effect=["One", "Two"]) as mocked:
            first = asyncio.run(stream_routes._lookup_title("hash2", "fallback"))
            cached_title, cached_expiry = stream_routes._title_cache["hash2"]
            stream_routes._title_cache["hash2"] = (cached_title, cached_expiry - stream_routes._TITLE_CACHE_TTL - 1)
            second = asyncio.run(stream_routes._lookup_title("hash2", "fallback"))

        self.assertEqual(first, "One")
        self.assertEqual(second, "Two")
        self.assertEqual(mocked.call_count, 2)

    def test_fallback_name_used_when_db_has_no_title(self):
        import asyncio

        with patch.object(stream_routes.db, "get_title_by_stream_id", return_value=None):
            self.assertEqual(
                asyncio.run(stream_routes._lookup_title("hash3", "Fallback Name")),
                "Fallback Name",
            )


if __name__ == "__main__":
    unittest.main()
