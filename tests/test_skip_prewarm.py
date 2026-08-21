"""Tests for skip-target speculative pre-warm (_prewarm_skip_targets)."""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from Backend.config import Telegram
from Backend.helper import custom_dl, media_index
from Backend.helper.custom_dl import SEEK_CACHE
from Backend.helper.media_index import MediaIndex


def _fake_index():
    # 61 keyframes: every 10s at 1 MiB apart (already 512K-aligned)
    kfs = [(float(i * 10), i * 1024 * 1024) for i in range(61)]
    return MediaIndex("mkv", 600.0, kfs)


def _fake_file_id(size=200 * 1024 * 1024):
    fid = MagicMock()
    fid.unique_id = "uid-test"
    fid.file_size = size
    return fid


class SkipPrewarmTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        SEEK_CACHE.clear()

    async def asyncTearDown(self):
        SEEK_CACHE.clear()

    async def test_prewarms_forward_and_backward_targets(self):
        idx = _fake_index()
        fetch_mock = AsyncMock(return_value=b"Z" * 1024)
        with (
            patch.object(Telegram, "SKIP_PREWARM_ENABLED", True),
            patch.object(Telegram, "SKIP_PREWARM_MAX_INFLIGHT", 3),
            patch.object(Telegram, "SKIP_PREWARM_TARGETS_SEC", [10.0, 30.0, -10.0]),
            patch.object(media_index, "get_media_index", AsyncMock(return_value=idx)),
            patch.object(custom_dl, "prefetch_seek_window", fetch_mock),
        ):
            current = 7 * 1024 * 1024  # kf at 7 MiB → t = 70s
            await custom_dl._prewarm_skip_targets(
                _fake_file_id(),
                MagicMock(),
                chat_id=-100,
                message_id=42,
                file_offset=current,
                file_size=200 * 1024 * 1024,
                inflight_holder={},
                session_pool=None,
            )
            await asyncio.sleep(0.1)

            offsets = sorted(c.kwargs.get("start_offset") for c in fetch_mock.call_args_list)
            # current t=70s: +10s → 8 MiB, +30s → 10 MiB, -10s → 6 MiB
            self.assertEqual(offsets, [6 * 1024 * 1024, 8 * 1024 * 1024, 10 * 1024 * 1024])

    async def test_inflight_cap_limits_fetches(self):
        idx = _fake_index()
        fetch_mock = AsyncMock(return_value=b"Z" * 1024)
        with (
            patch.object(Telegram, "SKIP_PREWARM_ENABLED", True),
            patch.object(Telegram, "SKIP_PREWARM_MAX_INFLIGHT", 1),
            patch.object(Telegram, "SKIP_PREWARM_TARGETS_SEC", [10.0, 30.0, -10.0]),
            patch.object(media_index, "get_media_index", AsyncMock(return_value=idx)),
            patch.object(custom_dl, "prefetch_seek_window", fetch_mock),
        ):
            await custom_dl._prewarm_skip_targets(
                _fake_file_id(),
                MagicMock(),
                chat_id=-100,
                message_id=43,
                file_offset=7 * 1024 * 1024,
                file_size=200 * 1024 * 1024,
                inflight_holder={},
                session_pool=None,
            )
            await asyncio.sleep(0.1)
            self.assertEqual(fetch_mock.call_count, 1)

    async def test_no_index_no_fetches(self):
        fetch_mock = AsyncMock()
        with (
            patch.object(Telegram, "SKIP_PREWARM_ENABLED", True),
            patch.object(media_index, "get_media_index", AsyncMock(return_value=None)),
            patch.object(custom_dl, "prefetch_seek_window", fetch_mock),
        ):
            await custom_dl._prewarm_skip_targets(
                _fake_file_id(),
                MagicMock(),
                chat_id=-100,
                message_id=44,
                file_offset=7 * 1024 * 1024,
                file_size=200 * 1024 * 1024,
                inflight_holder={},
                session_pool=None,
            )
            await asyncio.sleep(0.05)
            fetch_mock.assert_not_called()

    async def test_already_cached_window_skipped(self):
        idx = _fake_index()
        fetch_mock = AsyncMock(return_value=b"Z" * 1024)
        # Pre-cache the +10s window (8 MiB; current t=70s)
        await SEEK_CACHE.put_seek_block(-100, 45, 8 * 1024 * 1024, b"W" * 1024)
        with (
            patch.object(Telegram, "SKIP_PREWARM_ENABLED", True),
            patch.object(Telegram, "SKIP_PREWARM_MAX_INFLIGHT", 3),
            patch.object(Telegram, "SKIP_PREWARM_TARGETS_SEC", [10.0]),
            patch.object(media_index, "get_media_index", AsyncMock(return_value=idx)),
            patch.object(custom_dl, "prefetch_seek_window", fetch_mock),
        ):
            await custom_dl._prewarm_skip_targets(
                _fake_file_id(),
                MagicMock(),
                chat_id=-100,
                message_id=45,
                file_offset=7 * 1024 * 1024,
                file_size=200 * 1024 * 1024,
                inflight_holder={},
                session_pool=None,
            )
            await asyncio.sleep(0.05)
            fetch_mock.assert_not_called()

    async def test_disabled_flag_no_fetches(self):
        fetch_mock = AsyncMock()
        with (
            patch.object(Telegram, "SKIP_PREWARM_ENABLED", False),
            patch.object(custom_dl, "prefetch_seek_window", fetch_mock),
        ):
            await custom_dl._prewarm_skip_targets(
                _fake_file_id(),
                MagicMock(),
                chat_id=-100,
                message_id=46,
                file_offset=7 * 1024 * 1024,
                file_size=200 * 1024 * 1024,
                inflight_holder={},
                session_pool=None,
            )
            fetch_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
