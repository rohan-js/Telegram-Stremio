"""Tests: trailer capture (_pick_trailer), meta trailers field, lazy backfill."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import Response

from Backend.config import Telegram
from Backend.fastapi.routes import stremio_routes
from Backend.helper.metadata import _pick_trailer


def _v(key, site="YouTube", vtype="Trailer", official=False):
    return SimpleNamespace(key=key, site=site, type=vtype, official=official)


class PickTrailerTests(unittest.TestCase):
    def test_prefers_official_trailer(self):
        videos = SimpleNamespace(results=[
            _v("abc1", vtype="Teaser"),
            _v("abc2", vtype="Trailer", official=False),
            _v("abc3", vtype="Trailer", official=True),
        ])
        self.assertEqual(_pick_trailer(videos), "abc3")

    def test_trailer_beats_teaser_without_official(self):
        videos = SimpleNamespace(results=[
            _v("t1", vtype="Teaser"),
            _v("t2", vtype="Trailer"),
        ])
        self.assertEqual(_pick_trailer(videos), "t2")

    def test_falls_back_to_any_youtube_video(self):
        videos = SimpleNamespace(results=[_v("clip1", vtype="Clip")])
        self.assertEqual(_pick_trailer(videos), "clip1")

    def test_ignores_non_youtube(self):
        videos = SimpleNamespace(results=[_v("vimeo1", site="Vimeo")])
        self.assertEqual(_pick_trailer(videos), "")

    def test_none_and_empty(self):
        self.assertEqual(_pick_trailer(None), "")
        self.assertEqual(_pick_trailer(SimpleNamespace(results=[])), "")

    def test_none_key_ignored(self):
        videos = SimpleNamespace(results=[_v(None), _v("real")])
        self.assertEqual(_pick_trailer(videos), "real")


def fake_request():
    return SimpleNamespace(headers={}, client=SimpleNamespace(host="1.2.3.4"))


class MetaTrailerTests(unittest.IsolatedAsyncioTestCase):
    def _fake_db(self, trailer="ytKEY123"):
        class FakeDB:
            async def get_media_details(self, imdb_id=None, **kwargs):
                return {
                    "media_type": "movie",
                    "title": "Rocky III",
                    "imdb_id": "tt0084602",
                    "tmdb_id": 10614,
                    "db_index": 1,
                    "trailer_youtube_id": trailer,
                    "poster": "",
                    "backdrop": "",
                }
        return FakeDB()

    def _settings(self):
        return SimpleNamespace(
            maintenance_mode=False, fanart_enabled=False,
            better_poster_enabled=False, better_poster="",
            rpdb_enabled=False, rpdb_api_key="", base_url="",
        )

    async def test_meta_includes_trailers_shape(self):
        with (
            patch.object(stremio_routes, "db", self._fake_db()),
            patch.object(stremio_routes.SettingsManager, "current", lambda: self._settings()),
        ):
            result = await stremio_routes.get_meta(
                "tok", "movie", "tt0084602", Response(), token_data={},
            )
        trailers = result["meta"].get("trailers")
        self.assertIn({"source": "ytKEY123", "type": "Trailer"}, trailers)
        self.assertIn({"title": "Official Trailer", "ytId": "ytKEY123"}, trailers)

    async def test_meta_omits_trailers_when_missing(self):
        with (
            patch.object(stremio_routes, "db", self._fake_db(trailer="")),
            patch.object(stremio_routes.SettingsManager, "current", lambda: self._settings()),
            patch.object(stremio_routes, "_backfill_trailer", new=AsyncMock()),
        ):
            result = await stremio_routes.get_meta(
                "tok", "movie", "tt0084602", Response(), token_data={},
            )
        self.assertNotIn("trailers", result["meta"])


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _fake_tmdb(results):
        videos_obj = SimpleNamespace(results=results)
        return SimpleNamespace(
            movie=MagicMock(return_value=SimpleNamespace(videos=AsyncMock(return_value=videos_obj))),
            tv=MagicMock(return_value=SimpleNamespace(videos=AsyncMock(return_value=videos_obj))),
        )

    async def test_backfill_stores_key_when_found(self):
        media = {"media_type": "movie", "imdb_id": "tt1", "tmdb_id": 42, "db_index": 1}
        with (
            patch.object(Telegram, "TMDB_API", "k"),
            patch("Backend.helper.metadata.tmdb", self._fake_tmdb([_v("trKEY")])),
            patch.object(stremio_routes.db, "update_document", new=AsyncMock()) as upd,
        ):
            await stremio_routes._backfill_trailer(media)
        upd.assert_awaited_once_with("movie", 42, 1, {"trailer_youtube_id": "trKEY"})

    async def test_backfill_tv_uses_tv_endpoint(self):
        media = {"media_type": "tv", "imdb_id": "tt2", "tmdb_id": 7, "db_index": 1}
        with (
            patch.object(Telegram, "TMDB_API", "k"),
            patch("Backend.helper.metadata.tmdb", self._fake_tmdb([_v("tvKEY")])),
            patch.object(stremio_routes.db, "update_document", new=AsyncMock()) as upd,
        ):
            await stremio_routes._backfill_trailer(media)
        upd.assert_awaited_once_with("tv", 7, 1, {"trailer_youtube_id": "tvKEY"})

    async def test_backfill_noop_without_key(self):
        media = {"media_type": "movie", "imdb_id": "tt1", "tmdb_id": 42, "db_index": 1}
        with (
            patch.object(Telegram, "TMDB_API", "k"),
            patch("Backend.helper.metadata.tmdb", self._fake_tmdb([])),
            patch.object(stremio_routes.db, "update_document", new=AsyncMock()) as upd,
        ):
            await stremio_routes._backfill_trailer(media)
        upd.assert_not_awaited()

    async def test_backfill_skips_when_no_tmdb_api(self):
        with patch.object(Telegram, "TMDB_API", ""):
            await stremio_routes._backfill_trailer(
                {"media_type": "movie", "tmdb_id": 42, "db_index": 1}
            )  # must not raise


if __name__ == "__main__":
    unittest.main()
