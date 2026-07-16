import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

from Backend.fastapi.routes.nuvio_routes import nuvio_open
from Backend.helper.database import Database
from Backend.helper.nuvio import (
    build_nuvio_android_intent,
    build_nuvio_bridge_url,
    build_nuvio_deep_link,
    build_nuvio_install_link,
    normalize_nuvio_media_id,
    select_nuvio_media_id,
)


class NuvioLinkTests(unittest.TestCase):
    def test_builds_imdb_movie_and_series_links(self):
        self.assertEqual(
            build_nuvio_deep_link("movie", "tt1234567"),
            "nuvio://meta?type=movie&id=tt1234567",
        )
        self.assertEqual(
            build_nuvio_deep_link("tv", "tt7654321"),
            "nuvio://meta?type=series&id=tt7654321",
        )

    def test_builds_tmdb_fallback_links(self):
        self.assertEqual(select_nuvio_media_id(None, 1399), "tmdb:1399")
        self.assertEqual(
            build_nuvio_deep_link("series", "tmdb:1399"),
            "nuvio://meta?type=series&id=tmdb%3A1399",
        )
        self.assertEqual(
            build_nuvio_deep_link("movie", "tmdb:550"),
            "nuvio://meta?type=movie&id=tmdb%3A550",
        )

    def test_bridge_url_prefers_imdb_and_keeps_episode_context(self):
        url = build_nuvio_bridge_url(
            "https://addon.test/",
            "tv",
            imdb_id="tt0944947",
            tmdb_id=1399,
            season=1,
            episode=8,
        )
        self.assertEqual(
            url,
            "https://addon.test/nuvio/open/series/tt0944947?season=1&episode=8",
        )

    def test_invalid_or_unsafe_ids_are_rejected(self):
        for value in ("", "../secret", "tt123<script>", "tmdb:not-a-number"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    normalize_nuvio_media_id(value)

    def test_android_and_install_links_do_not_lock_to_package(self):
        self.assertEqual(
            build_nuvio_android_intent("nuvio://meta?type=movie&id=tt1234567"),
            "intent://meta?type=movie&id=tt1234567#Intent;scheme=nuvio;end",
        )
        self.assertEqual(
            build_nuvio_install_link("https://addon.test/stremio/token/manifest.json"),
            "nuvio://addon.test/stremio/token/manifest.json",
        )


class NuvioRouteTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(path="/nuvio/open/movie/tt1234567"):
        return Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "https",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 50000),
                "server": ("addon.test", 443),
            }
        )

    async def test_bridge_renders_deep_link_and_manifest_fallback(self):
        with (
            patch("Backend.fastapi.routes.nuvio_routes.Telegram.BASE_URL", "https://addon.test"),
            patch("Backend.fastapi.routes.nuvio_routes.Telegram.DEFAULT_ADDON_TOKEN", "token123"),
        ):
            response = await nuvio_open(
                self.request(),
                "series",
                "tt0944947",
                season=1,
                episode=8,
            )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn(r"nuvio://meta?type=series\u0026id=tt0944947", body)
        self.assertIn("S01E08", body)
        self.assertIn("https://addon.test/stremio/token123/manifest.json", body)
        self.assertIn("https://github.com/NuvioMedia/NuvioDesktop/releases/latest", body)
        self.assertEqual(response.headers.get("cache-control"), "no-store")

    async def test_bridge_rejects_invalid_media_id(self):
        with self.assertRaises(HTTPException) as error:
            await nuvio_open(self.request(), "movie", "../secret")
        self.assertEqual(error.exception.status_code, 400)


class NuvioCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_nuvio_callback_posts_to_channel_and_records_platform(self):
        from Backend.pyrofork.plugins import reciever

        fake_db = SimpleNamespace(
            mark_watch_link_requested=AsyncMock(
                return_value={
                    "nuvio_link": "https://addon.test/nuvio/open/movie/tt1234567",
                    "media_title": "Example Movie",
                    "media_type": "movie",
                }
            ),
            update_user_interaction=AsyncMock(),
            mark_watch_link_delivery=AsyncMock(),
        )
        client = SimpleNamespace(send_message=AsyncMock())
        callback = SimpleNamespace(
            from_user=SimpleNamespace(
                id=123,
                first_name="Rohan",
                last_name=None,
                username="rohan_js",
            ),
            message=SimpleNamespace(chat=SimpleNamespace(id=-100987), id=77),
            answer=AsyncMock(),
        )

        with patch.object(reciever, "db", fake_db):
            await reciever._deliver_watch_link_callback(client, callback, "AbCdEf12", "nuvio")

        fake_db.mark_watch_link_requested.assert_awaited_once()
        self.assertEqual(fake_db.mark_watch_link_requested.await_args.kwargs["platform"], "nuvio")
        send_kwargs = client.send_message.await_args.kwargs
        self.assertEqual(send_kwargs["chat_id"], -100987)
        self.assertEqual(send_kwargs["reply_to_message_id"], 77)
        self.assertIn("Watch in Nuvio", send_kwargs["text"])
        self.assertEqual(
            send_kwargs["reply_markup"].inline_keyboard[0][0].url,
            "https://addon.test/nuvio/open/movie/tt1234567",
        )
        fake_db.mark_watch_link_delivery.assert_awaited_with("AbCdEf12", "sent", platform="nuvio")
        callback.answer.assert_awaited_with("Nuvio link posted in the channel.", show_alert=False)


class _WatchRequestCollection:
    def __init__(self):
        self.request_update = None
        self.delivery_update = None

    async def find_one_and_update(self, query, update, return_document=None):
        self.request_update = update
        return {"_id": "AbCdEf12", "expires_at": query["expires_at"]["$gt"]}

    async def update_one(self, query, update):
        self.delivery_update = update


class NuvioWatchDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.collection = _WatchRequestCollection()
        self.database = Database.__new__(Database)
        self.database.dbs = {"tracking": {"watch_link_requests": self.collection}}

    async def test_request_tracking_increments_the_selected_platform(self):
        await self.database.mark_watch_link_requested(
            "AbCdEf12",
            {"user_id": 123, "name": "Rohan"},
            platform="nuvio",
        )

        update = self.collection.request_update
        self.assertEqual(update["$set"]["last_platform"], "nuvio")
        self.assertEqual(update["$set"]["platform_delivery.nuvio.status"], "pending")
        self.assertEqual(update["$inc"]["platform_click_counts.nuvio"], 1)

    async def test_delivery_tracking_is_kept_per_platform(self):
        await self.database.mark_watch_link_delivery("AbCdEf12", "sent", platform="nuvio")

        update = self.collection.delivery_update
        self.assertEqual(update["$set"]["last_platform"], "nuvio")
        self.assertEqual(update["$set"]["platform_delivery.nuvio.status"], "sent")
        self.assertIn("platform_delivery.nuvio.error", update["$unset"])


if __name__ == "__main__":
    unittest.main()
