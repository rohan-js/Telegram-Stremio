"""Tests: flat theme system (upstream v5.3.3 adoption).

The 13-theme × 3-style registry was retired on purpose — the fork now ships
upstream's 3 flat themes (Ink/Paper/Night) with Ink as the fork default, a
cookie→session theme key, and a single-field /set-theme.
"""

import asyncio
import unittest
from types import SimpleNamespace

from starlette.requests import Request

from Backend.fastapi.themes import (
    DEFAULT_THEME,
    THEMES,
    get_all_themes,
    get_theme,
    theme_key,
)
from Backend.fastapi.routes import template_routes


def _request(theme_cookie=None, session=None):
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"cookie", f"theme={theme_cookie}".encode())] if theme_cookie else [],
        "session": session or {},
    }
    return Request(scope)


class ThemeRegistryTests(unittest.TestCase):
    def test_flat_theme_set(self):
        self.assertEqual(set(THEMES.keys()), {"ink", "paper", "night"})

    def test_fork_default_is_ink(self):
        self.assertEqual(DEFAULT_THEME, "ink")
        self.assertEqual(get_theme(), THEMES["ink"])
        self.assertTrue(THEMES["ink"]["is_dark"])
        self.assertFalse(THEMES["paper"]["is_dark"])

    def test_theme_dict_shape(self):
        for key, theme in THEMES.items():
            self.assertIn("is_dark", theme)
            for color in (
                "primary", "on_primary", "background", "card", "card_high",
                "border", "text", "text_secondary", "danger", "ok",
            ):
                self.assertIn(color, theme["colors"], f"{key} missing {color}")

    def test_unknown_theme_falls_back(self):
        self.assertEqual(get_theme("does-not-exist"), THEMES[DEFAULT_THEME])

    def test_get_all_themes_is_registry(self):
        self.assertIs(get_all_themes(), THEMES)


class ThemeKeyTests(unittest.TestCase):
    def test_cookie_wins(self):
        self.assertEqual(theme_key(_request(theme_cookie="night")), "night")

    def test_session_fallback(self):
        self.assertEqual(theme_key(_request(session={"theme": "paper"})), "paper")

    def test_default_when_neither(self):
        self.assertEqual(theme_key(_request()), DEFAULT_THEME)

    def test_invalid_value_falls_back(self):
        self.assertEqual(theme_key(_request(theme_cookie="graphite_amber")), DEFAULT_THEME)


class SetThemeTests(unittest.IsolatedAsyncioTestCase):
    async def test_set_theme_writes_session_and_cookie(self):
        session = {}
        request = SimpleNamespace(session=session, headers={"referer": "/admin/settings"})
        response = await template_routes.set_theme(request, "paper")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(session.get("theme"), "paper")
        cookie_headers = [v for k, v in response.raw_headers if k == b"set-cookie"]
        self.assertTrue(any(b"theme=paper" in c for c in cookie_headers))

    async def test_set_theme_ignores_unknown(self):
        session = {}
        request = SimpleNamespace(session=session, headers={"referer": "/"})
        response = await template_routes.set_theme(request, "nope")
        self.assertEqual(response.status_code, 302)
        self.assertNotIn("theme", session)


class BaseContextTests(unittest.TestCase):
    def test_base_context_shape(self):
        ctx = template_routes._base_context(_request())
        self.assertIn("theme", ctx)
        self.assertIn("themes", ctx)
        self.assertIn("current_theme", ctx)
        self.assertIn("current_user", ctx)
        self.assertNotIn("styles", ctx)
        self.assertNotIn("current_style", ctx)


if __name__ == "__main__":
    unittest.main()
