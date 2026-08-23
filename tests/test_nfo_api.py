"""Tests for NFO admin API handlers (direct handler calls, patched db)."""

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from Backend.config import Telegram
from Backend.fastapi.routes import api_routes


def _movie_doc():
    return {
        "media_type": "movie",
        "title": "Rocky III",
        "release_year": 1982,
        "description": "d",
        "rating": 6.8,
        "imdb_id": "tt0084602",
        "genres": ["Drama"],
    }


def _tv_doc():
    return {
        "media_type": "tv",
        "title": "Breakout",
        "release_year": 2024,
        "imdb_id": "tt123",
        "seasons": [
            {
                "season_number": 1,
                "episodes": [
                    {"episode_number": 2, "title": "Ep 2", "overview": "o", "released": "2024-03-05"},
                ],
            }
        ],
    }


class FakeDB:
    def __init__(self, doc):
        self._doc = doc

    async def get_media_details(self, imdb_id=None, **kwargs):
        if imdb_id == (self._doc or {}).get("imdb_id"):
            return self._doc
        return None


class MovieNfoApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path_xml(self):
        with patch.object(api_routes, "db", FakeDB(_movie_doc())):
            resp = await api_routes.get_movie_nfo_api("tt0084602")
        body = resp.body.decode()
        self.assertIn("<movie>", body)
        self.assertIn("<title>Rocky III</title>", body)
        self.assertEqual(resp.media_type, "application/xml")
        self.assertIn("attachment", resp.headers["content-disposition"])
        self.assertIn("Rocky III (1982).nfo", resp.headers["content-disposition"])

    async def test_unknown_imdb_404(self):
        with patch.object(api_routes, "db", FakeDB(_movie_doc())):
            with self.assertRaises(HTTPException) as ctx:
                await api_routes.get_movie_nfo_api("tt9999999")
            self.assertEqual(ctx.exception.status_code, 404)

    async def test_flag_off_404(self):
        with (
            patch.object(api_routes, "db", FakeDB(_movie_doc())),
            patch.object(Telegram, "NFO_DOWNLOAD_ENABLED", False),
        ):
            with self.assertRaises(HTTPException) as ctx:
                await api_routes.get_movie_nfo_api("tt0084602")
            self.assertEqual(ctx.exception.status_code, 404)

    async def test_tv_doc_on_movie_endpoint_404(self):
        with patch.object(api_routes, "db", FakeDB(_tv_doc())):
            with self.assertRaises(HTTPException):
                await api_routes.get_movie_nfo_api("tt123")


class TvNfoApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_show_level_nfo(self):
        with patch.object(api_routes, "db", FakeDB(_tv_doc())):
            resp = await api_routes.get_tv_nfo_api("tt123")
        self.assertIn(b"<tvshow>", resp.body)

    async def test_episode_variant(self):
        with patch.object(api_routes, "db", FakeDB(_tv_doc())):
            resp = await api_routes.get_tv_nfo_api("tt123", season=1, episode=2)
        body = resp.body.decode()
        self.assertIn("<episodedetails>", body)
        self.assertIn("<episode>2</episode>", body)
        self.assertIn("S01E02.nfo", resp.headers["content-disposition"])

    async def test_missing_episode_404(self):
        with patch.object(api_routes, "db", FakeDB(_tv_doc())):
            with self.assertRaises(HTTPException) as ctx:
                await api_routes.get_tv_nfo_api("tt123", season=1, episode=9)
            self.assertEqual(ctx.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
