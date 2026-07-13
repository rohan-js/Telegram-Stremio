import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import Response

from Backend.fastapi.routes import stremio_routes
from Backend.helper.database import Database


class TitleVisibilityDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_visibility_update_syncs_media_and_catalog_references(self):
        media_collection = {
            "movie": SimpleNamespace(
                update_one=AsyncMock(return_value=SimpleNamespace(matched_count=1)),
                find_one=AsyncMock(return_value={
                    "tmdb_id": 100,
                    "visibility": "tokens",
                    "allowed_tokens": ["token-a"],
                }),
            )
        }
        catalogs = SimpleNamespace(update_many=AsyncMock())
        database = Database.__new__(Database)
        database.dbs = {
            "storage_1": media_collection,
            "tracking": {"custom_catalogs": catalogs},
        }

        result = await database.set_media_visibility(100, 1, "movie", "tokens", ["token-a", "token-a"])

        self.assertEqual(result["visibility"], "tokens")
        self.assertEqual(result["allowed_tokens"], ["token-a"])
        media_collection["movie"].update_one.assert_awaited_once_with(
            {"tmdb_id": 100},
            {"$set": {"visibility": "tokens", "allowed_tokens": ["token-a"]}},
        )
        update = catalogs.update_many.await_args.args[1]["$set"]
        self.assertEqual(update["items.$[item].visibility"], "tokens")
        self.assertEqual(update["items.$[item].allowed_tokens"], ["token-a"])


class TitleVisibilityRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_catalog_requires_catalog_and_title_access(self):
        class FakeDB:
            async def get_custom_catalog(self, catalog_id):
                return {
                    "_id": catalog_id,
                    "visible": True,
                    "visibility": "public",
                    "allowed_tokens": [],
                }

            async def get_custom_catalog_items(self, **kwargs):
                return {
                    "items": [{
                        "media_type": "movie",
                        "imdb_id": "tt1234567",
                        "tmdb_id": 100,
                        "db_index": 1,
                        "title": "Private Movie",
                        "visibility": "tokens",
                        "allowed_tokens": ["allowed"],
                    }]
                }

        with patch.object(stremio_routes, "db", FakeDB()), patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False):
            denied = await stremio_routes.get_catalog(
                "denied", "movie", "custom_abc", Response(), token_data={"token": "denied"}
            )
            allowed = await stremio_routes.get_catalog(
                "allowed", "movie", "custom_abc", Response(), token_data={"token": "allowed"}
            )

        self.assertEqual(denied["metas"], [])
        self.assertEqual(allowed["metas"][0]["id"], "tt1234567")

    def test_exclusive_title_is_visible_inside_its_catalog_only(self):
        media = {
            "visibility": "public",
            "exclusive_catalog_id": "exclusive-1",
            "exclusive_searchable": False,
        }
        self.assertTrue(
            stremio_routes._media_visible_to_token(media, {"token": "any"}, catalog_id="exclusive-1")
        )
        self.assertFalse(stremio_routes._media_visible_to_token(media, {"token": "any"}))


if __name__ == "__main__":
    unittest.main()
