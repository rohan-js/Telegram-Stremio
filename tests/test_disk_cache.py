import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from Backend.helper import disk_cache
from Backend.fastapi.routes import stream_routes


class FirstCacheConfigTests(unittest.TestCase):
    """DISK_CACHE_FIRST_MB head-cache sizing and gating."""

    def tearDown(self):
        stream_routes._first_fill_inflight.clear()

    def test_zero_first_mb_means_disabled(self):
        with patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 0.0):
            self.assertEqual(disk_cache.first_cache_bytes(), 0)
            self.assertFalse(disk_cache.first_cache_enabled())

    def test_first_mb_converts_to_bytes(self):
        with patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0):
            self.assertEqual(disk_cache.first_cache_bytes(), 10 * 1024 * 1024)

    def test_first_mb_capped_by_budget(self):
        with (
            patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0),
            patch.object(disk_cache.Telegram, "DISK_CACHE_MAX_BYTES", 5 * 1024 * 1024),
        ):
            self.assertEqual(disk_cache.first_cache_bytes(), 5 * 1024 * 1024)

    def test_enabled_requires_disk_cache_on(self):
        with (
            patch.object(disk_cache.Telegram, "DISK_CACHE_ENABLED", False),
            patch.object(disk_cache.Telegram, "DISK_CACHE_MAX_BYTES", 1024 * 1024 * 1024),
            patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0),
        ):
            self.assertFalse(disk_cache.first_cache_enabled())

    def test_enabled_requires_budget(self):
        with (
            patch.object(disk_cache.Telegram, "DISK_CACHE_ENABLED", True),
            patch.object(disk_cache.Telegram, "DISK_CACHE_MAX_BYTES", 0),
            patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0),
        ):
            self.assertFalse(disk_cache.first_cache_enabled())

    def test_enabled_holds_when_configured(self):
        with (
            patch.object(disk_cache.Telegram, "DISK_CACHE_ENABLED", True),
            patch.object(disk_cache.Telegram, "DISK_CACHE_MAX_BYTES", 1024 * 1024 * 1024),
            patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0),
        ):
            self.assertTrue(disk_cache.first_cache_enabled())

    def test_head_paths_are_stable_and_namespaced(self):
        with patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 10.0):
            rel = disk_cache.first_cache_relpath(-1001234567890, 42, "unique-1")
            self.assertTrue(rel.endswith(".first.bin"))
            self.assertEqual(rel, disk_cache.first_cache_relpath(-1001234567890, 42, "unique-1"))
            self.assertNotEqual(rel, disk_cache.first_cache_relpath(-1001234567890, 43, "unique-1"))
            abspath = disk_cache.first_cache_abspath(-1001234567890, 42, "unique-1")
            self.assertTrue(str(abspath).replace("\\", "/").endswith(rel))


class FakeHeadStreamer:
    def __init__(self, data: bytes, short: bool = False):
        self.data = data
        self.short = short
        self.fetch_calls = 0

    async def get_file_properties(self, chat_id, message_id):
        return SimpleNamespace(file_size=len(self.data))

    async def _get_location(self, fid):
        return SimpleNamespace(ok=True)

    async def _get_media_session(self, fid):
        return SimpleNamespace(ok=True)

    async def _fetch_file_bytes(self, media_session, location, offset, limit):
        self.fetch_calls += 1
        if self.short:
            return self.data[:1000]
        end = min(offset + limit, len(self.data))
        if offset >= len(self.data):
            return b""
        return self.data[offset:end]


class FirstCacheFillTests(unittest.IsolatedAsyncioTestCase):
    """_fill_first_cache_head writes the head, dedups, and never raises."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        stream_routes._first_fill_inflight.clear()

    def _patch_config(self):
        return (
            patch.object(disk_cache.Telegram, "DISK_CACHE_ENABLED", True),
            patch.object(disk_cache.Telegram, "DISK_CACHE_MAX_BYTES", 1024 * 1024 * 1024),
            patch.object(disk_cache.Telegram, "DISK_CACHE_FIRST_MB", 2.0),
            patch.object(disk_cache.Telegram, "DISK_CACHE_DIR", self._tmp.name),
        )

    def _patches(self):
        return list(self._patch_config())

    async def test_fill_writes_exact_head_then_clears_inflight(self):
        data = b"x" * (3 * 1024 * 1024)
        fake = FakeHeadStreamer(data)
        with ExitStack() as stack:
            for patcher in self._patches() + [patch.object(stream_routes, "get_streamer", return_value=fake)]:
                stack.enter_context(patcher)
            await stream_routes._fill_first_cache_head(-1001, 7, "uid", 2 * 1024 * 1024, 0)
            dest = disk_cache.first_cache_abspath(-1001, 7, "uid")
            self.assertTrue(dest.exists())
            self.assertEqual(dest.stat().st_size, 2 * 1024 * 1024)
            self.assertFalse(dest.with_suffix(dest.suffix + ".part").exists())
        self.assertFalse(stream_routes._first_fill_inflight)

    async def test_inflight_dedup_skips_second_fill(self):
        data = b"y" * (3 * 1024 * 1024)
        fake = FakeHeadStreamer(data)
        with ExitStack() as stack:
            for patcher in self._patches() + [patch.object(stream_routes, "get_streamer", return_value=fake)]:
                stack.enter_context(patcher)
            await stream_routes._fill_first_cache_head(-1001, 8, "uid-2", 2 * 1024 * 1024, 0)
            await stream_routes._fill_first_cache_head(-1001, 8, "uid-2", 2 * 1024 * 1024, 0)
            dest = disk_cache.first_cache_abspath(-1001, 8, "uid-2")
            self.assertTrue(dest.exists())
            # Second call hit the in-flight/complete guard: no extra fetch round.
            self.assertEqual(fake.fetch_calls, 4)  # 2MiB / ~512KiB chunks

    async def test_short_download_removes_partial_file(self):
        fake = FakeHeadStreamer(b"z" * (3 * 1024 * 1024), short=True)
        with ExitStack() as stack:
            for patcher in self._patches() + [patch.object(stream_routes, "get_streamer", return_value=fake)]:
                stack.enter_context(patcher)
            await stream_routes._fill_first_cache_head(-1001, 9, "uid-3", 2 * 1024 * 1024, 0)
            dest = disk_cache.first_cache_abspath(-1001, 9, "uid-3")
            self.assertFalse(dest.exists())
            self.assertFalse(dest.with_suffix(dest.suffix + ".part").exists())
        self.assertFalse(stream_routes._first_fill_inflight)

    async def test_streamer_error_is_swallowed(self):
        bad = AsyncMock()
        bad.get_file_properties.side_effect = RuntimeError("boom")
        with ExitStack() as stack:
            for patcher in self._patches() + [patch.object(stream_routes, "get_streamer", return_value=bad)]:
                stack.enter_context(patcher)
            # Must not raise; the live stream is unaffected.
            await stream_routes._fill_first_cache_head(-1001, 10, "uid-4", 1024 * 1024, 0)
        self.assertFalse(stream_routes._first_fill_inflight)


if __name__ == "__main__":
    unittest.main()