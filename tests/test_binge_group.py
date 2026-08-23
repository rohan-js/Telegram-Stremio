"""Tests: TV episode streams carry behaviorHints.bingeGroup (Stremio
auto-advance pairs with the 90% binge prewarm); movies carry none."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import Response

from Backend.fastapi.routes import stremio_routes


def fake_request():
    return SimpleNamespace(headers={}, client=SimpleNamespace(host="1.2.3.4"))


class FakeEpisodeDB:
    async def get_media_details(self, imdb_id, season_number=None, episode_number=None):
        return {
            "title": "Breakout",
            "telegram": [
                {"quality": "1080p", "id": "ep-a", "name": "Breakout.S01E02.mkv", "size": "2 GB"},
                {"quality": "720p", "id": "ep-b", "name": "Breakout.S01E02.720p.mkv", "size": "1 GB"},
            ],
        }


class BingeGroupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    async def tearDown(self):
        stremio_routes._STREAM_LIST_CACHE.clear()

    async def test_episode_streams_have_binge_group(self):
        with (
            patch.object(stremio_routes, "db", FakeEpisodeDB()),
            patch.object(stremio_routes, "BASE_URL", "https://example.test"),
        ):
            result = await stremio_routes.get_streams(
                fake_request(),
                "tokenBinge1",
                "series",
                "tt1234567:1:2",
                Response(),
                token_data={},
            )

        self.assertTrue(result["streams"])
        for stream in result["streams"]:
            hints = stream.get("behaviorHints") or {}
            self.assertEqual(
                hints.get("bingeGroup"),
                "telegram-stremio-tt1234567",
                f"missing bingeGroup on stream {stream.get('name')!r}",
            )

    async def test_movie_streams_have_no_binge_group(self):
        with (
            patch.object(stremio_routes, "db", FakeEpisodeDB()),
            patch.object(stremio_routes, "BASE_URL", "https://example.test"),
        ):
            result = await stremio_routes.get_streams(
                fake_request(),
                "tokenBinge2",
                "movie",
                "tt1234567",
                Response(),
                token_data={},
            )

        self.assertTrue(result["streams"])
        for stream in result["streams"]:
            self.assertNotIn("bingeGroup", stream.get("behaviorHints") or {})

    async def test_series_without_episode_id_no_group(self):
        # series id without :season:episode -> full-series context, not an episode
        with (
            patch.object(stremio_routes, "db", FakeEpisodeDB()),
            patch.object(stremio_routes, "BASE_URL", "https://example.test"),
        ):
            result = await stremio_routes.get_streams(
                fake_request(),
                "tokenBinge3",
                "series",
                "tt1234567",
                Response(),
                token_data={},
            )
        for stream in result.get("streams", []):
            self.assertNotIn("bingeGroup", stream.get("behaviorHints") or {})


if __name__ == "__main__":
    unittest.main()
