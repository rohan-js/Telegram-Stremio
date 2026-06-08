import unittest
from unittest.mock import AsyncMock, patch

from fastapi import Response

from Backend.fastapi.routes import iptv_routes, stremio_routes
from Backend.helper.iptv import (
    IPTV_CATALOG_ID,
    build_iptv_streams,
    channel_is_eligible,
    iptv_meta,
    sign_proxy_target,
    verify_proxy_target,
)


class IptvHelperTests(unittest.TestCase):
    def test_channel_filter_rejects_wrong_country_blocked_and_unsafe_channels(self):
        base = {
            "id": "Test.in",
            "country": "IN",
            "is_nsfw": False,
            "closed": None,
            "replaced_by": None,
        }
        self.assertTrue(channel_is_eligible(base, {"IN"}, set()))
        self.assertFalse(channel_is_eligible({**base, "country": "US"}, {"IN"}, set()))
        self.assertFalse(channel_is_eligible({**base, "is_nsfw": True}, {"IN"}, set()))
        self.assertFalse(channel_is_eligible({**base, "closed": "2025-01-01"}, {"IN"}, set()))
        self.assertFalse(channel_is_eligible({**base, "replaced_by": "Other.in"}, {"IN"}, set()))
        self.assertFalse(channel_is_eligible(base, {"IN"}, {"Test.in"}))

    def test_direct_and_header_streams_are_returned_in_direct_first_order(self):
        channel = {
            "_id": "News.in",
            "name": "News",
            "streams": [
                {
                    "id": "direct",
                    "url": "https://example.test/live.m3u8",
                    "quality": "1080p",
                    "request_headers": {},
                },
                {
                    "id": "headers",
                    "url": "https://example.test/secure.m3u8",
                    "quality": "720p",
                    "request_headers": {
                        "Referer": "https://example.test/",
                        "User-Agent": "Test Agent",
                    },
                },
            ],
        }
        with (
            patch.object(stremio_routes.Telegram, "BASE_URL", "https://addon.test"),
            patch.object(stremio_routes.Telegram, "IPTV_PROXY_FALLBACK_ENABLED", True),
            patch("Backend.helper.iptv.Telegram.BASE_URL", "https://addon.test"),
            patch("Backend.helper.iptv.Telegram.IPTV_PROXY_FALLBACK_ENABLED", True),
        ):
            streams = build_iptv_streams(channel, "token123")

        self.assertEqual(len(streams), 3)
        self.assertEqual(streams[0]["url"], "https://example.test/live.m3u8")
        self.assertEqual(streams[1]["url"], "https://example.test/secure.m3u8")
        self.assertEqual(
            streams[1]["behaviorHints"]["proxyHeaders"]["request"]["Referer"],
            "https://example.test/",
        )
        self.assertEqual(
            streams[2]["url"],
            "https://addon.test/iptv/token123/stream/headers",
        )
        self.assertIn("Uses VPS bandwidth", streams[2]["title"])

    def test_signed_proxy_target_round_trip_and_tamper_rejection(self):
        with patch("Backend.helper.iptv.Telegram.IPTV_PROXY_SECRET", "test-secret"):
            signed = sign_proxy_target("stream1", "https://example.test/segment.ts")
            payload = verify_proxy_target(signed)
            self.assertEqual(payload["s"], "stream1")
            self.assertEqual(payload["u"], "https://example.test/segment.ts")
            with self.assertRaises(ValueError):
                verify_proxy_target(signed[:-1] + ("0" if signed[-1] != "0" else "1"))

    def test_hls_rewriter_proxies_relative_segments_and_key_urls(self):
        manifest = (
            "#EXTM3U\n"
            '#EXT-X-KEY:METHOD=AES-128,URI="key.bin"\n'
            "#EXTINF:5,\n"
            "segments/one.ts\n"
        )
        with (
            patch.object(iptv_routes.Telegram, "BASE_URL", "https://addon.test"),
            patch("Backend.helper.iptv.Telegram.IPTV_PROXY_SECRET", "test-secret"),
        ):
            result = iptv_routes._rewrite_hls_manifest(
                manifest,
                "https://origin.test/live/master.m3u8",
                "token123",
                "stream1",
            )

        self.assertIn("https://addon.test/iptv/token123/fetch/", result)
        self.assertNotIn('URI="key.bin"', result)
        self.assertNotIn("\nsegments/one.ts\n", result)

    def test_meta_uses_tv_type_and_stable_iptv_id(self):
        meta = iptv_meta(
            {
                "_id": "News.in",
                "name": "News",
                "logo": "https://example.test/logo.png",
                "categories": ["News"],
                "languages": ["Hindi"],
                "country_name": "India",
            }
        )
        self.assertEqual(meta["id"], "iptv:News.in")
        self.assertEqual(meta["type"], "tv")
        self.assertEqual(meta["genres"], ["News"])


class _DistinctCollection:
    async def distinct(self, field, query):
        return ["News", "Entertainment"]


class _TrackingDB:
    def __getitem__(self, name):
        return _DistinctCollection()


class _ManifestDB:
    dbs = {"tracking": _TrackingDB()}

    async def get_custom_catalogs(self, visible_only=False):
        return []


class IptvStremioRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_manifest_advertises_tv_catalog_and_prefix(self):
        with (
            patch.object(stremio_routes, "db", _ManifestDB()),
            patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False),
            patch.object(
                stremio_routes,
                "get_iptv_settings",
                AsyncMock(return_value={"enabled": True}),
            ),
        ):
            manifest = await stremio_routes.get_manifest(
                "token123",
                Response(),
                token_data={},
            )

        self.assertIn("tv", manifest["types"])
        self.assertIn("iptv:", manifest["idPrefixes"])
        live_catalog = [
            item
            for item in manifest["catalogs"]
            if item["id"] == IPTV_CATALOG_ID
        ]
        self.assertEqual(len(live_catalog), 1)
        self.assertEqual(live_catalog[0]["type"], "tv")

    async def test_tv_catalog_returns_iptv_metas(self):
        channel = {
            "_id": "News.in",
            "stremio_id": "iptv:News.in",
            "name": "News",
            "categories": ["News"],
            "languages": ["Hindi"],
        }
        with (
            patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False),
            patch.object(
                stremio_routes,
                "get_iptv_settings",
                AsyncMock(return_value={"enabled": True}),
            ),
            patch.object(
                stremio_routes,
                "list_iptv_channels",
                AsyncMock(return_value={"channels": [channel]}),
            ),
        ):
            result = await stremio_routes.get_catalog(
                "token123",
                "tv",
                IPTV_CATALOG_ID,
                Response(),
                token_data={},
            )

        self.assertEqual(result["metas"][0]["id"], "iptv:News.in")
        self.assertEqual(result["metas"][0]["type"], "tv")

    async def test_tv_stream_route_returns_direct_url(self):
        channel = {
            "_id": "News.in",
            "name": "News",
            "streams": [
                {
                    "id": "direct",
                    "url": "https://example.test/live.m3u8",
                    "quality": "720p",
                    "request_headers": {},
                }
            ],
        }
        with (
            patch.object(
                stremio_routes,
                "get_iptv_settings",
                AsyncMock(return_value={"enabled": True}),
            ),
            patch.object(
                stremio_routes,
                "get_iptv_channel",
                AsyncMock(return_value=channel),
            ),
        ):
            result = await stremio_routes.get_streams(
                "token123",
                "tv",
                "iptv:News.in",
                Response(),
                token_data={},
            )

        self.assertEqual(result["streams"][0]["url"], "https://example.test/live.m3u8")
        self.assertTrue(result["streams"][0]["behaviorHints"]["notWebReady"])


if __name__ == "__main__":
    unittest.main()
