"""Tests: catalog/meta display names prefer the stored English title."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import Response

from Backend.fastapi.routes import stremio_routes


class DisplayTitleUnitTests(unittest.TestCase):
    def test_prefers_english(self):
        self.assertEqual(
            stremio_routes.display_title({"title": "ऑपरेशन सागर", "title_english": "Operation Sagar"}),
            "Operation Sagar",
        )

    def test_falls_back_to_original(self):
        self.assertEqual(stremio_routes.display_title({"title": "기생충"}), "기생충")
        self.assertEqual(
            stremio_routes.display_title({"title": "Parasite", "title_english": ""}),
            "Parasite",
        )

    def test_english_same_as_title(self):
        self.assertEqual(
            stremio_routes.display_title({"title": "Rocky III", "title_english": "Rocky III"}),
            "Rocky III",
        )

    def test_missing_both(self):
        self.assertEqual(stremio_routes.display_title({}), "Unknown")


def fake_request():
    return SimpleNamespace(headers={}, client=SimpleNamespace(host="1.2.3.4"))


class FakeCatalogDB:
    async def get_custom_catalogs(self, visible_only=False):
        return []

    async def get_catalog_items_sorted(self, *args, **kwargs):
        return {"items": []}

    async def sort_movies(self, sort_params, page, page_size, genre_filter=None):
        return {"movies": [_MOVIE]}

    async def sort_tv_shows(self, sort_params, page, page_size, genre_filter=None):
        return {"tv_shows": []}


_MOVIE = {
    "media_type": "movie",
    "imdb_id": "tt999",
    "tmdb_id": 42,
    "title": "ऑपरेशन सागर",
    "title_english": "Operation Sagar",
    "release_year": 2026,
    "poster": "",
    "backdrop": "",
    "genres": ["Action"],
    "rating": 7.5,
    "description": "d",
    "cast": [],
    "runtime": "120 min",
}


class CatalogDisplayTitleTests(unittest.IsolatedAsyncioTestCase):
    async def test_catalog_row_uses_english_name(self):
        fake_settings = SimpleNamespace(
            maintenance_mode=False,
            fanart_enabled=False,
            better_poster_enabled=False,
            better_poster="",
            rpdb_enabled=False,
            rpdb_api_key="",
            base_url="",
        )
        with (
            patch.object(stremio_routes, "db", FakeCatalogDB()),
            patch.object(stremio_routes.SettingsManager, "current", lambda: fake_settings),
        ):
            result = await stremio_routes.get_catalog(
                "tokT", "movie", "latest_movies", Response(), token_data={},
            )
        self.assertEqual(result["metas"][0]["name"], "Operation Sagar")


if __name__ == "__main__":
    unittest.main()
