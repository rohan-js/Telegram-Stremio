"""Tests: UI style axis (default / glassy / neo_brutal) ported from upstream v5.0.2."""

import unittest
from types import SimpleNamespace

from Backend.fastapi.themes import (
    THEMES,
    DEFAULT_STYLE,
    DEFAULT_THEME,
    STYLES,
    get_all_styles,
    get_theme,
)
from Backend.fastapi.routes import template_routes


class StyleRegistryTests(unittest.TestCase):
    def test_three_styles_registered(self):
        self.assertEqual(
            set(STYLES.keys()), {"default", "glassy", "neo_brutal"}
        )

    def test_get_theme_injects_style(self):
        theme = get_theme(DEFAULT_THEME, "glassy")
        self.assertEqual(theme["style"], "glassy")
        self.assertEqual(theme["name"], THEMES[DEFAULT_THEME]["name"])

    def test_bad_style_falls_back_to_default(self):
        self.assertEqual(get_theme(DEFAULT_THEME, "bogus")["style"], "default")
        self.assertEqual(get_theme(DEFAULT_THEME)["style"], "default")

    def test_registry_not_mutated(self):
        get_theme(DEFAULT_THEME, "neo_brutal")
        self.assertNotIn("style", THEMES[DEFAULT_THEME])
        # colors dict still shared reference — but theme dict itself is a copy
        theme = get_theme(DEFAULT_THEME, "glassy")
        theme["colors"] = None
        self.assertIsNotNone(THEMES[DEFAULT_THEME]["colors"])

    def test_theme_keys_unchanged(self):
        # bridal_blush stays (fork-specific); all themes keep 8 color keys
        self.assertIn("bridal_blush", THEMES)
        for name, data in THEMES.items():
            self.assertEqual(
                set(data["colors"].keys()),
                {"primary", "secondary", "accent", "background", "card",
                 "border", "text", "text_secondary"},
                f"theme {name} color keys drifted",
            )


class SetThemeTests(unittest.IsolatedAsyncioTestCase):
    def _request(self):
        class FakeSession(dict):
            pass
        req = SimpleNamespace(session=FakeSession(), headers={"referer": "/"})
        return req

    async def test_stores_style_and_theme(self):
        req = self._request()
        await template_routes.set_theme(req, "graphite_amber", "glassy")
        self.assertEqual(req.session["theme"], "graphite_amber")
        self.assertEqual(req.session["style"], "glassy")

    async def test_style_only_change_keeps_theme(self):
        req = self._request()
        req.session["theme"] = "amoled_midnight"
        await template_routes.set_theme(req, "", "neo_brutal")
        self.assertEqual(req.session["theme"], "amoled_midnight")
        self.assertEqual(req.session["style"], "neo_brutal")

    async def test_theme_only_change_keeps_style(self):
        req = self._request()
        req.session["style"] = "glassy"
        await template_routes.set_theme(req, "royal_violet", "")
        self.assertEqual(req.session["theme"], "royal_violet")
        self.assertEqual(req.session["style"], "glassy")

    async def test_invalid_style_ignored(self):
        req = self._request()
        await template_routes.set_theme(req, None, "bogus_style")
        self.assertNotIn("style", req.session)

    async def test_base_context_exposes_styles(self):
        req = SimpleNamespace(
            session={"theme": "graphite_amber", "style": "glassy"},
            headers={"referer": "/"},
        )
        ctx = template_routes._base_context(req)
        self.assertEqual(ctx["current_style"], "glassy")
        self.assertEqual(ctx["theme"]["style"], "glassy")
        self.assertIn("styles", ctx)
        self.assertEqual(set(ctx["styles"].keys()), {"default", "glassy", "neo_brutal"})


class BaseTemplateStyleClassTests(unittest.TestCase):
    def test_html_tag_emits_style_class(self):
        from pathlib import Path

        src = (Path("Backend/fastapi/templates/base.html")).read_text(encoding="utf-8")
        self.assertIn("style-{{ theme.style|default('default') }}", src.splitlines()[1])
        self.assertIn("html.style-glassy .glass-panel", src)
        self.assertIn("html.style-neo_brutal .glass-panel", src)
        self.assertIn("function selectStyle(", src)


if __name__ == "__main__":
    unittest.main()
