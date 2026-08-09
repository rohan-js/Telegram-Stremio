import unittest
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pyrogram import raw
from pyrogram.errors import RPCError

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


class FakeRPCError(RPCError):
    CODE = -404
    ID = "AUTH_KEY_DELETED"

    def __init__(self):
        pass


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


def make_redirect(payload: bytes, key: bytes, iv: bytes, dc_id: int = 5) -> raw.types.upload.FileCdnRedirect:
    file_hash = raw.types.FileHash(offset=0, limit=len(payload), hash=sha256(payload).digest())
    return raw.types.upload.FileCdnRedirect(
        dc_id=dc_id,
        file_token=b"token",
        encryption_key=key,
        encryption_iv=iv,
        file_hashes=[file_hash],
    )


class TelegramCdnFetchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        custom_dl._cdn_file_failures.clear()
        custom_dl._cdn_file_disabled_until.clear()

    def tearDown(self):
        custom_dl._cdn_file_failures.clear()
        custom_dl._cdn_file_disabled_until.clear()
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

    async def test_cdn_hash_mismatch_falls_back_to_master_dc(self):
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
        origin_session = FakeSession([redirect, SimpleNamespace(bytes=b"master-bytes")])
        cdn_session = FakeSession([raw.types.upload.CdnFile(bytes=encrypted)])
        streamer._get_cdn_session = AsyncMock(return_value=cdn_session)
        stats = {}

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True),
        ):
            result = await streamer._fetch_file_bytes(
                media_session=origin_session,
                location=SimpleNamespace(id=111, access_hash=222),
                offset=0,
                limit=len(payload),
                stream_stats=stats,
            )

        self.assertEqual(result, b"master-bytes")
        self.assertEqual(stats["cdn_errors"], 1)
        self.assertEqual(stats["cdn_fallbacks"], 1)
        # The fallback re-fetches from the master DC without CDN support.
        self.assertFalse(getattr(origin_session.requests[1], "cdn_supported", True))

    async def test_cdn_empty_response_falls_back_to_master_dc(self):
        streamer = make_streamer()
        payload = b"real-data"
        redirect = raw.types.upload.FileCdnRedirect(
            dc_id=5,
            file_token=b"token",
            encryption_key=b"1" * 32,
            encryption_iv=b"2" * 16,
            file_hashes=[],
        )
        origin_session = FakeSession([redirect, SimpleNamespace(bytes=payload)])
        # CDN returns a response without bytes -> empty_cdn_response error.
        streamer._get_cdn_session = AsyncMock(return_value=FakeSession([SimpleNamespace(bytes=None)]))
        stats = {}

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", False),
        ):
            result = await streamer._fetch_file_bytes(
                media_session=origin_session,
                location=SimpleNamespace(id=333, access_hash=444),
                offset=0,
                limit=1024,
                stream_stats=stats,
            )

        self.assertEqual(result, payload)
        self.assertEqual(stats["cdn_errors"], 1)
        self.assertEqual(stats["cdn_fallbacks"], 1)

    async def test_cdn_auth_key_deleted_recreates_session_and_succeeds(self):
        streamer = make_streamer()
        key = b"1" * 32
        iv = b"2" * 16
        payload = b"fresh-session-data"
        encrypted = encrypt_cdn_payload(payload, key, iv)
        redirect = make_redirect(payload, key, iv)
        origin_session = FakeSession([redirect])
        broken_session = FakeSession([FakeRPCError()])
        fresh_session = FakeSession([raw.types.upload.CdnFile(bytes=encrypted)])
        streamer._get_cdn_session = AsyncMock(side_effect=[broken_session, fresh_session])
        streamer._cdn_sessions[5] = broken_session

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True),
        ):
            result = await streamer._fetch_file_bytes(
                media_session=origin_session,
                location=SimpleNamespace(id=555, access_hash=666),
                offset=0,
                limit=len(payload),
            )

        self.assertEqual(result, payload)
        # The dead session was dropped and a fresh CDN session created.
        self.assertNotIn(5, streamer._cdn_sessions)
        self.assertEqual(streamer._get_cdn_session.await_count, 2)
        self.assertTrue(any(isinstance(req, raw.functions.upload.GetCdnFile) for req in fresh_session.requests))

    async def test_per_file_cdn_blacklist_disables_cdn_after_max_failures(self):
        streamer = make_streamer()
        location = SimpleNamespace(id=777, access_hash=888)
        key = b"1" * 32
        iv = b"2" * 16
        redirect = raw.types.upload.FileCdnRedirect(
            dc_id=5,
            file_token=b"token",
            encryption_key=key,
            encryption_iv=iv,
            file_hashes=[],
        )
        # Call 1: redirect + CDN empty failure + master-DC fallback.
        # Call 2: redirect (still allowed, 1 failure < 2) + failure + fallback -> blacklisted.
        # Call 3: blacklisted -> plain GetFile, straight from master DC.
        origin_session = FakeSession([
            redirect, SimpleNamespace(bytes=b"m1"),
            redirect, SimpleNamespace(bytes=b"m2"),
            SimpleNamespace(bytes=b"m3"),
        ])
        streamer._get_cdn_session = AsyncMock(return_value=FakeSession([
            SimpleNamespace(bytes=None),  # call 1 CDN failure
            SimpleNamespace(bytes=None),  # call 2 CDN failure (then blacklisted)
        ]))

        with (
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_ENABLED", True),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_VERIFY_HASHES", False),
            patch.object(custom_dl.Telegram, "TELEGRAM_CDN_MAX_FILE_FAILURES", 2),
        ):
            r1 = await streamer._fetch_file_bytes(origin_session, location, 0, 1024)
            r2 = await streamer._fetch_file_bytes(origin_session, location, 0, 1024)
            r3 = await streamer._fetch_file_bytes(origin_session, location, 0, 1024)

        self.assertEqual((r1, r2, r3), (b"m1", b"m2", b"m3"))
        self.assertTrue(streamer._cdn_file_disabled(location))
        self.assertEqual(len(origin_session.requests), 5)
        # The final (3rd) fetch no longer asks for CDN support.
        self.assertFalse(getattr(origin_session.requests[4], "cdn_supported", True))

    async def test_cdn_file_disable_ttl_reenables_file(self):
        streamer = make_streamer()
        location = SimpleNamespace(id=901, access_hash=902)
        streamer._cdn_file_disabled(location)
        _cdn_location_key = custom_dl._cdn_location_key(location)
        custom_dl._cdn_file_failures[_cdn_location_key] = 2
        custom_dl._cdn_file_disabled_until[_cdn_location_key] = 0.0  # already expired
        self.assertFalse(streamer._cdn_file_disabled(location))
        self.assertNotIn(_cdn_location_key, custom_dl._cdn_file_disabled_until)


if __name__ == "__main__":
    unittest.main()
