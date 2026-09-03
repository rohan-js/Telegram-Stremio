"""Tests: fanart applied to catalog rows (ported upstream gather pattern)."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import Response

from Backend.fastapi.routes import stremio_routes


def _fake_settings(fanart_enabled: bool) -> SimpleNamespace:
    return SimpleNamespace(
        maintenance_mode=False,
        fanart_enabled=fanart_enabled,
        better_poster_enabled=False,
        better_poster="",
        rpdb_enabled=False,
        rpdb_api_key="",
        base_url="https://example.test",
    )


class FakeCatalogDB:
    async def get_custom_catalogs(self, visible_only=False):
        return [{"_id": "abc123", "name": "Picks", "visible": True}]

    async def get_custom_catalog(self, catalog_id):
        if catalog_id == "abc123":
            return {"_id": "abc123", "name": "Picks", "visible": True}
        return None

    async def get_custom_catalog_items(self, catalog_id, media_type=None, page=1, page_size=15):
        return {
            "catalog": {"_id": catalog_id},
            "items": [
                {
                    "media_type": media_type,
                    "imdb_id": "tt111",
                    "tmdb_id": 1,
                    "title": "Movie A",
                    "release_year": 2020,
                    "poster": "",
                    "backdrop": "",
                    "logo": "",
                    "genres": [],
                    "rating": 7.0,
                    "description": "d",
                    "cast": [],
                    "runtime": "90 min",
                }
            ],
        }


class CatalogFanartTests(unittest.IsolatedAsyncioTestCase):
    async def _fetch(self, fanart_enabled: bool):
        art = AsyncMock(return_value={
            "poster": "https://fan/poster.jpg",
            "logo": "https://fan/logo.png",
            "background": "https://fan/bg.jpg",
        })
        with (
            patch.object(stremio_routes, "db", FakeCatalogDB()),
            patch.object(stremio_routes, "fanart_artwork", art),
            patch.object(
                stremio_routes.SettingsManager, "current",
                lambda: _fake_settings(fanart_enabled),
            ),
        ):
            result = await stremio_routes.get_catalog(
                "token123", "movie", "custom_abc123", Response(), token_data={},
            )
        return result, art

    async def test_fanart_overrides_catalog_meta(self):
        result, art = await self._fetch(fanart_enabled=True)
        meta = result["metas"][0]
        self.assertEqual(meta["poster"], "https://fan/poster.jpg")
        self.assertEqual(meta["logo"], "https://fan/logo.png")
        self.assertEqual(meta["background"], "https://fan/bg.jpg")
        art.assert_awaited_once()

    async def test_disabled_fanart_not_called(self):
        result, art = await self._fetch(fanart_enabled=False)
        meta = result["metas"][0]
        self.assertNotEqual(meta["poster"], "https://fan/poster.jpg")
        art.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
