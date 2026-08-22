"""Tests for ingest-time prewarm (_prepare_new_media in reciever.py)."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from Backend.config import Telegram


def _fake_job(**overrides):
    job = {
        "source_type": "telegram",
        "chat_id": -1003625383282,
        "msg_id": 558,
        "channel": 3625383282,
    }
    job.update(overrides)
    return job


def _mock_streamer():
    streamer = MagicMock()
    fid = MagicMock()
    fid.file_size = 50 * 1024 * 1024
    streamer.get_file_properties = AsyncMock(return_value=fid)
    streamer._get_media_session = AsyncMock(return_value=object())
    streamer._get_location = AsyncMock(return_value=object())
    return streamer


class IngestPrewarmTests(unittest.IsolatedAsyncioTestCase):
    async def test_prewarms_head_tail_and_index(self):
        from Backend.pyrofork.plugins.reciever import _prepare_new_media

        streamer = _mock_streamer()
        with (
            patch("Backend.fastapi.routes.stream_routes.get_streamer", return_value=streamer) as gs,
            patch("Backend.helper.custom_dl.prefetch_stream_head", new_callable=AsyncMock) as head,
            patch("Backend.helper.custom_dl.prefetch_file_tail", new_callable=AsyncMock) as tail,
            patch("Backend.helper.media_index.build_media_index", new_callable=AsyncMock) as build,
            patch.object(Telegram, "INGEST_PREWARM_ENABLED", True),
        ):
            await _prepare_new_media(_fake_job())

        gs.assert_called_once_with(0)
        head.assert_awaited_once()
        self.assertEqual(head.call_args.kwargs.get("chat_id"), -1003625383282)
        self.assertEqual(head.call_args.kwargs.get("message_id"), 558)
        tail.assert_awaited_once()
        build.assert_awaited_once()
        self.assertIsNotNone(build.call_args.kwargs.get("media_session"))

    async def test_non_telegram_source_skipped(self):
        from Backend.pyrofork.plugins.reciever import _prepare_new_media

        with (
            patch("Backend.fastapi.routes.stream_routes.get_streamer", new_callable=AsyncMock) as gs,
            patch.object(Telegram, "INGEST_PREWARM_ENABLED", True),
        ):
            await _prepare_new_media(_fake_job(source_type="torrent"))
        gs.assert_not_called()

    async def test_flag_off_skipped(self):
        from Backend.pyrofork.plugins.reciever import _prepare_new_media

        with (
            patch("Backend.fastapi.routes.stream_routes.get_streamer", new_callable=AsyncMock) as gs,
            patch.object(Telegram, "INGEST_PREWARM_ENABLED", False),
        ):
            await _prepare_new_media(_fake_job())
        gs.assert_not_called()

    async def test_missing_ids_skipped(self):
        from Backend.pyrofork.plugins.reciever import _prepare_new_media

        with (
            patch("Backend.fastapi.routes.stream_routes.get_streamer", new_callable=AsyncMock) as gs,
            patch.object(Telegram, "INGEST_PREWARM_ENABLED", True),
        ):
            await _prepare_new_media(_fake_job(chat_id=None))
            await _prepare_new_media(_fake_job(msg_id=None))
        gs.assert_not_called()

    async def test_exception_swallowed(self):
        from Backend.pyrofork.plugins.reciever import _prepare_new_media

        with (
            patch(
                "Backend.fastapi.routes.stream_routes.get_streamer",
                side_effect=RuntimeError("boom"),
            ),
            patch.object(Telegram, "INGEST_PREWARM_ENABLED", True),
        ):
            # must not raise
            await _prepare_new_media(_fake_job())


if __name__ == "__main__":
    unittest.main()
