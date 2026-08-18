import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyrogram import raw

from Backend.config import Telegram
from Backend.helper import custom_dl
from Backend.helper.custom_dl import (
    ByteStreamer,
    _dc_keepalive_loop,
    initialize_all_streamers,
    start_dc_keepalive_service,
)


class PrewarmKeepaliveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        ByteStreamer._instances.clear()
        self.mock_client = MagicMock()
        self.mock_client.media_sessions = {}
        self.mock_client.is_connected = True
        self.mock_client.storage.test_mode = AsyncMock(return_value=False)
        self.mock_client.storage.dc_id = AsyncMock(return_value=2)
        self.mock_client.storage.auth_key = AsyncMock(return_value=b"test_key_dc2")
        self.mock_client.invoke = AsyncMock(
            return_value=MagicMock(id=12345, bytes=b"export_bytes")
        )

    async def test_get_or_create_media_session_same_dc(self):
        """When target DC matches client main DC, uses storage auth key directly."""
        streamer = ByteStreamer(self.mock_client, 0)
        with patch("Backend.helper.custom_dl.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.start = AsyncMock()
            mock_session.send = AsyncMock()
            mock_session_cls.return_value = mock_session

            session = await streamer.get_or_create_media_session(2)

            self.assertEqual(session, mock_session)
            self.assertEqual(self.mock_client.media_sessions.get(2), mock_session)
            mock_session.start.assert_awaited_once()
            # Since DC is same as main DC (2), no ExportAuthorization needed
            self.mock_client.invoke.assert_not_awaited()

    async def test_get_or_create_media_session_different_dc(self):
        """When target DC differs from main DC, performs Export/ImportAuthorization."""
        streamer = ByteStreamer(self.mock_client, 0)
        with (
            patch("Backend.helper.custom_dl.Session") as mock_session_cls,
            patch("Backend.helper.custom_dl.Auth") as mock_auth_cls,
        ):
            mock_auth = MagicMock()
            mock_auth.create = AsyncMock(return_value=b"auth_key_dc5")
            mock_auth_cls.return_value = mock_auth

            mock_session = MagicMock()
            mock_session.start = AsyncMock()
            mock_session.send = AsyncMock()
            mock_session_cls.return_value = mock_session

            session = await streamer.get_or_create_media_session(5)

            self.assertEqual(session, mock_session)
            self.assertEqual(self.mock_client.media_sessions.get(5), mock_session)
            mock_session.start.assert_awaited_once()
            self.mock_client.invoke.assert_awaited_once()
            mock_session.send.assert_awaited_once()

    async def test_get_or_create_media_session_returns_cached(self):
        """Subsequent calls return cached session without creating a new one."""
        streamer = ByteStreamer(self.mock_client, 0)
        cached_session = MagicMock()
        self.mock_client.media_sessions[4] = cached_session

        session = await streamer.get_or_create_media_session(4)
        self.assertEqual(session, cached_session)

    async def test_prewarm_sessions_calls_target_dcs(self):
        """_prewarm_sessions attempts to create sessions for all configured DCs."""
        streamer = ByteStreamer(self.mock_client, 0)
        prewarmed_dcs = []

        async def mock_get_or_create(dc):
            prewarmed_dcs.append(dc)
            return MagicMock()

        with (
            patch.object(Telegram, "TELEGRAM_PREWARM_ENABLED", True),
            patch.object(Telegram, "TELEGRAM_PREWARM_DCS", [1, 2, 4, 5]),
            patch.object(streamer, "get_or_create_media_session", side_effect=mock_get_or_create),
        ):
            await streamer._prewarm_sessions()
            self.assertEqual(sorted(prewarmed_dcs), [1, 2, 4, 5])

    async def test_prewarm_disabled_skips(self):
        """When prewarm is disabled, _prewarm_sessions does nothing."""
        streamer = ByteStreamer(self.mock_client, 0)
        prewarmed_dcs = []

        async def mock_get_or_create(dc):
            prewarmed_dcs.append(dc)
            return MagicMock()

        with (
            patch.object(Telegram, "TELEGRAM_PREWARM_ENABLED", False),
            patch.object(streamer, "get_or_create_media_session", side_effect=mock_get_or_create),
        ):
            await streamer._prewarm_sessions()
            self.assertEqual(prewarmed_dcs, [])

    async def test_dc_keepalive_sends_ping(self):
        """Keepalive loop iterates media sessions and sends MTProto Ping."""
        streamer = ByteStreamer(self.mock_client, 0)
        session_dc5 = MagicMock()
        session_dc5.send = AsyncMock()
        self.mock_client.media_sessions[5] = session_dc5

        mock_sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with (
            patch.object(Telegram, "TELEGRAM_PREWARM_ENABLED", True),
            patch.object(Telegram, "TELEGRAM_KEEPALIVE_INTERVAL_SEC", 1),
            patch("Backend.helper.custom_dl.asyncio.sleep", mock_sleep),
        ):
            try:
                await _dc_keepalive_loop()
            except asyncio.CancelledError:
                pass

        session_dc5.send.assert_awaited_once()
        call_args = session_dc5.send.call_args[0][0]
        self.assertIsInstance(call_args, raw.functions.Ping)

    async def test_initialize_all_streamers_creates_instances(self):
        """initialize_all_streamers creates ByteStreamer for all connected clients."""
        client0 = MagicMock()
        client0.media_sessions = {}
        client1 = MagicMock()
        client1.media_sessions = {}

        with (
            patch.dict(custom_dl.multi_clients, {0: client0, 1: client1}, clear=True),
            patch("Backend.helper.custom_dl.start_dc_keepalive_service") as mock_start_service,
        ):
            initialize_all_streamers()
            self.assertIn(0, ByteStreamer._instances)
            self.assertIn(1, ByteStreamer._instances)
            mock_start_service.assert_called_once()


if __name__ == "__main__":
    unittest.main()
