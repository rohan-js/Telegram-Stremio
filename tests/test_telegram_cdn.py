import unittest
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pyrogram import raw

from Backend.helper import custom_dl


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def send(self, request):
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"Unexpected request: {type(request).__name__}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def make_streamer():
    streamer = object.__new__(custom_dl.ByteStreamer)
    streamer.client = SimpleNamespace()
    streamer.client_index = 0
    streamer._file_id_cache = {}
    streamer._cdn_sessions = {}
    streamer._cdn_getfile_supported = True
    return streamer


def encrypt_cdn_payload(payload: bytes, key: bytes, iv: bytes, offset: int = 0) -> bytes:
    ctr_iv = bytearray(bytes(iv)[:-4] + (offset // 16).to_bytes(4, "big"))
    return custom_dl.aes.ctr256_encrypt(payload, key, ctr_iv)


class TelegramCdnFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_upload_file_returns_bytes_with_cdn_supported(self):
        streamer = make_streamer()
        session = FakeSession([SimpleNamespace(bytes=b"normal-bytes")])

        with patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True):
            result = await streamer._fetch_file_bytes(
                media_session=session,
                location=SimpleNamespace(),
                offset=0,
                limit=1024,
            )

        self.assertEqual(result, b"normal-bytes")
        self.assertEqual(len(session.requests), 1)
        self.assertTrue(getattr(session.requests[0], "cdn_supported", False))

    async def test_cdn_disabled_does_not_request_cdn_support(self):
        streamer = make_streamer()
        session = FakeSession([SimpleNamespace(bytes=b"plain")])

        with patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", False):
            result = await streamer._fetch_file_bytes(
                media_session=session,
                location=SimpleNamespace(),
                offset=0,
                limit=1024,
            )

        self.assertEqual(result, b"plain")
        self.assertFalse(getattr(session.requests[0], "cdn_supported", False))

    async def test_cdn_redirect_decrypts_verifies_and_records_stats(self):
        streamer = make_streamer()
        key = b"1" * 32
        iv = b"2" * 16
        payload = b"cdn-data" * 64
        encrypted = encrypt_cdn_payload(payload, key, iv)
        file_hash = raw.types.FileHash(offset=0, limit=len(payload), hash=sha256(payload).digest())
        redirect = raw.types.upload.FileCdnRedirect(
            dc_id=5,
            file_token=b"token",
            encryption_key=key,
            encryption_iv=iv,
            file_hashes=[file_hash],
        )
        origin_session = FakeSession([redirect])
        cdn_session = FakeSession([raw.types.upload.CdnFile(bytes=encrypted)])
        streamer._get_cdn_session = AsyncMock(return_value=cdn_session)
        stats = {}
        events = []

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True),
        ):
            result = await streamer._fetch_file_bytes(
                media_session=origin_session,
                location=SimpleNamespace(),
                offset=0,
                limit=len(payload),
                route_event=events.append,
                stream_stats=stats,
            )

        self.assertEqual(result, payload)
        self.assertEqual(stats["cdn_redirects"], 1)
        self.assertEqual(stats["cdn_chunks"], 1)
        self.assertEqual(stats["cdn_bytes"], len(payload))
        self.assertEqual(stats["cdn_dc"], 5)
        self.assertEqual([event["event"] for event in events], ["cdn_redirect", "cdn_fetch"])

    async def test_cdn_reupload_needed_is_retried(self):
        streamer = make_streamer()
        key = b"1" * 32
        iv = b"2" * 16
        payload = b"after-reupload"
        encrypted = encrypt_cdn_payload(payload, key, iv)
        file_hash = raw.types.FileHash(offset=0, limit=len(payload), hash=sha256(payload).digest())
        redirect = raw.types.upload.FileCdnRedirect(
            dc_id=5,
            file_token=b"token",
            encryption_key=key,
            encryption_iv=iv,
            file_hashes=[file_hash],
        )
        origin_session = FakeSession([redirect, []])
        cdn_session = FakeSession([
            raw.types.upload.CdnFileReuploadNeeded(request_token=b"request"),
            raw.types.upload.CdnFile(bytes=encrypted),
        ])
        streamer._get_cdn_session = AsyncMock(return_value=cdn_session)

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS", 2),
        ):
            result = await streamer._fetch_file_bytes(
                media_session=origin_session,
                location=SimpleNamespace(),
                offset=0,
                limit=len(payload),
            )

        self.assertEqual(result, payload)
        self.assertTrue(any(isinstance(req, raw.functions.upload.ReuploadCdnFile) for req in origin_session.requests))

    async def test_cdn_hash_mismatch_raises_controlled_error(self):
        streamer = make_streamer()
        key = b"1" * 32
        iv = b"2" * 16
        payload = b"bad-hash"
        encrypted = encrypt_cdn_payload(payload, key, iv)
        bad_hash = raw.types.FileHash(offset=0, limit=len(payload), hash=b"0" * 32)
        redirect = raw.types.upload.FileCdnRedirect(
            dc_id=5,
            file_token=b"token",
            encryption_key=key,
            encryption_iv=iv,
            file_hashes=[bad_hash],
        )
        origin_session = FakeSession([redirect])
        cdn_session = FakeSession([raw.types.upload.CdnFile(bytes=encrypted)])
        streamer._get_cdn_session = AsyncMock(return_value=cdn_session)
        stats = {}

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True),
        ):
            with self.assertRaises(custom_dl.TelegramCdnFetchError):
                await streamer._fetch_file_bytes(
                    media_session=origin_session,
                    location=SimpleNamespace(),
                    offset=0,
                    limit=len(payload),
                    stream_stats=stats,
                )

        self.assertEqual(stats["cdn_errors"], 1)


if __name__ == "__main__":
    unittest.main()
