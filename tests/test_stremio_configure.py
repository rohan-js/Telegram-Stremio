import unittest
from unittest.mock import AsyncMock, patch

from starlette.requests import Request

from Backend.fastapi.routes import stremio_routes


class StremioConfigureTemplateTests(unittest.IsolatedAsyncioTestCase):
    async def test_configure_uses_template_and_lifetime_access_status(self):
        scope = {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "path": "/stremio/token/configure",
            "raw_path": b"/stremio/token/configure",
            "query_string": b"",
            "headers": [],
            "server": ("example.test", 443),
            "client": ("127.0.0.1", 1234),
        }
        request = Request(scope)
        with (
            patch.object(stremio_routes.db, "get_api_token", AsyncMock(return_value={
                "token": "token",
                "user_id": 42,
                "subscription_exempt": True,
            })),
            patch.object(stremio_routes.db, "get_user", AsyncMock(return_value={
                "first_name": "<Admin>",
                "subscription_status": "active",
            })),
            patch.object(stremio_routes.Telegram, "BASE_URL", "https://example.test"),
        ):
            response = await stremio_routes.configure_addon(request, "token")

        body = response.body.decode("utf-8")
        self.assertIn("stremio/token/manifest.json", body)
        self.assertIn("Lifetime", body)
        self.assertIn("&lt;Admin&gt;", body)
        self.assertNotIn("<Admin>", body)


if __name__ == "__main__":
    unittest.main()
