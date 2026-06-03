import unittest
from unittest.mock import patch

from Backend.fastapi.routes import stream_routes
from Backend.fastapi.routes.stream_routes import choose_effective_prefetch, resolve_video_mime_type
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


if __name__ == "__main__":
    unittest.main()
