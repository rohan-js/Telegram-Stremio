import unittest
from fastapi import Response
from unittest.mock import patch

from Backend.fastapi.routes import stremio_routes
from Backend.helper.auto_catalog import classify_media_from_tmdb


class FakeCatalogDB:
    async def get_custom_catalogs(self, visible_only=False):
        self.visible_only = visible_only
        return [
            {"_id": "abc123", "name": "Tamil Picks", "visible": True},
        ]

    async def get_custom_catalog(self, catalog_id):
        if catalog_id == "abc123":
            return {"_id": "abc123", "name": "Tamil Picks", "visible": True}
        return None

    async def get_custom_catalog_items(self, catalog_id, media_type=None, page=1, page_size=15):
        return {
            "catalog": {"_id": catalog_id},
            "items": [
                {
                    "media_type": media_type,
                    "imdb_id": "tt1234567",
                    "tmdb_id": 100,
                    "title": "Catalog Movie",
                    "release_year": 2026,
                    "poster": "",
                    "backdrop": "",
                    "genres": ["Drama"],
                    "rating": 8.1,
                    "description": "A catalog item",
                    "cast": [],
                    "runtime": "120 min",
                }
            ],
        }


class CustomCatalogStremioTests(unittest.IsolatedAsyncioTestCase):
    async def test_manifest_includes_visible_custom_catalogs(self):
        fake_db = FakeCatalogDB()
        with patch.object(stremio_routes, "db", fake_db), patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False):
            manifest = await stremio_routes.get_manifest(
                "token123",
                Response(),
                token_data={},
            )

        custom_catalogs = [catalog for catalog in manifest["catalogs"] if catalog["id"] == "custom_abc123"]
        self.assertEqual(len(custom_catalogs), 2)
        self.assertEqual({catalog["type"] for catalog in custom_catalogs}, {"movie", "series"})

    async def test_custom_catalog_route_returns_hydrated_metas(self):
        fake_db = FakeCatalogDB()
        with patch.object(stremio_routes, "db", fake_db), patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False):
            result = await stremio_routes.get_catalog(
                "token123",
                "movie",
                "custom_abc123",
                Response(),
                token_data={},
            )

        self.assertEqual(result["metas"][0]["id"], "tt1234567")
        self.assertEqual(result["metas"][0]["name"], "Catalog Movie")

    async def test_streams_hide_flagged_quality_and_prioritize_recommended(self):
        class FakeStreamDB:
            async def get_media_details(self, imdb_id, season_number=None, episode_number=None):
                return {
                    "telegram": [
                        {
                            "quality": "720p",
                            "id": "hidden",
                            "name": "Hidden.mkv",
                            "size": "1 GB",
                            "hidden_from_stremio": True,
                        },
                        {
                            "quality": "720p",
                            "id": "normal",
                            "name": "Normal.mkv",
                            "size": "1 GB",
                        },
                        {
                            "quality": "1080p",
                            "id": "recommended",
                            "name": "Recommended.mkv",
                            "size": "2 GB",
                            "recommended": True,
                        },
                    ]
                }

        with patch.object(stremio_routes, "db", FakeStreamDB()), patch.object(stremio_routes, "BASE_URL", "https://example.test"):
            result = await stremio_routes.get_streams(
                "token123",
                "movie",
                "tt1234567",
                Response(),
                token_data={},
            )

        names = [stream["name"] for stream in result["streams"]]
        urls = [stream["url"] for stream in result["streams"]]
        self.assertEqual(len(result["streams"]), 2)
        self.assertIn("Recommended", names[0])
        self.assertTrue(any("recommended" in url for url in urls))
        self.assertFalse(any("hidden" in url for url in urls))


class AutoCatalogClassificationTests(unittest.TestCase):
    def test_classification_uses_enabled_language_and_provider_buckets(self):
        doc = {
            "media_type": "movie",
            "rating": 8.2,
            "release_year": 2026,
        }
        details = {
            "original_language": "ta",
            "origin_country": ["IN"],
            "production_countries": [{"iso_3166_1": "IN"}],
            "genres": [{"name": "Drama"}],
            "keywords": {"keywords": []},
        }
        watch_data = {
            "results": {
                "IN": {
                    "flatrate": [{"provider_name": "Netflix"}],
                }
            }
        }

        result = classify_media_from_tmdb(
            doc,
            details,
            watch_data,
            {"Tamil", "South Indian", "Netflix", "Top Rated", "Recently Added"},
        )

        self.assertEqual(
            set(result["auto_tags"]),
            {"Tamil", "South Indian", "Netflix", "Top Rated", "Recently Added"},
        )


if __name__ == "__main__":
    unittest.main()
