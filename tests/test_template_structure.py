"""Tests: template markup must be structurally balanced.

The upstream-UI adoption merge (ddaa171) left orphan ``</div>`` closers in
settings.html; the browser auto-closed ``#settings-root``/``main``/``shell``
early and 10 settings cards rendered as direct children of ``<body>`` — sliding
under the fixed left rail. A net-count scan misses swapped/stray closers, so
this test stack-parses every template and fails on the first mismatched close.
"""

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "Backend" / "fastapi" / "templates"

VOID_ELEMENTS = {
    "area", "base", "br", "col", "embed", "hr", "img", "input",
    "link", "meta", "param", "source", "track", "wbr",
}


class _BalanceParser(HTMLParser):
    """Stack parser that flags close tags which don't match the open element."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[tuple[str, int]] = []
        self.errors: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag not in VOID_ELEMENTS:
            self.stack.append((tag, self.getpos()[0]))

    def handle_startendtag(self, tag, attrs):
        pass  # self-closing (<input ... />) never pushes

    def handle_endtag(self, tag):
        if tag in VOID_ELEMENTS:
            return
        if not self.stack:
            self.errors.append(f"line {self.getpos()[0]}: stray </{tag}> with empty stack")
            return
        # pop intermediates only when the matched open is in the stack (HTML5-style);
        # a mismatched close (e.g. </div> closing a <label>) is the bug class we guard
        open_tags = [t for t, _ in self.stack]
        if tag not in open_tags:
            top_tag, top_line = self.stack[-1]
            self.errors.append(
                f"line {self.getpos()[0]}: </{tag}> does not match open <{top_tag}> (line {top_line})"
            )
            return
        while self.stack:
            open_tag, open_line = self.stack.pop()
            if open_tag == tag:
                break


def _strip_non_structure(src: str) -> str:
    """Drop Jinja syntax, comments and script/style innards before parsing."""
    src = re.sub(r"\{%.*?%\}", "", src, flags=re.S)
    src = re.sub(r"\{\{.*?\}\}", "", src, flags=re.S)
    src = re.sub(r"\{#.*?#\}", "", src, flags=re.S)
    src = re.sub(r"<!--.*?-->", "", src, flags=re.S)
    src = re.sub(r"<script\b[^>]*>.*?</script>", "<script></script>", src, flags=re.S | re.I)
    src = re.sub(r"<style\b[^>]*>.*?</style>", "<style></style>", src, flags=re.S | re.I)
    return src


class TemplateBalanceTests(unittest.TestCase):
    def test_every_template_is_tag_balanced(self):
        templates = sorted(TEMPLATES_DIR.glob("*.html"))
        self.assertGreater(len(templates), 10, "template dir not found")
        failures = []
        for path in templates:
            src = _strip_non_structure(path.read_text(encoding="utf-8"))
            parser = _BalanceParser()
            parser.feed(src)
            parser.close()
            for err in parser.errors[:3]:
                failures.append(f"{path.name}: {err}")
            if parser.stack:
                top_tag, top_line = parser.stack[-1]
                failures.append(f"{path.name}: unclosed <{top_tag}> from line {top_line}")
        self.assertEqual(failures, [], "unbalanced template markup:\n" + "\n".join(failures))

    def test_settings_page_cards_keep_formerly_broken_wraps(self):
        """Regression guard: the RPDB/Fanart wraps must exist inside their card."""
        src = (TEMPLATES_DIR / "settings.html").read_text(encoding="utf-8")
        for anchor in ('<div id="rpdb-wrap"', '<div id="fanart-wrap"',
                       '<div id="better-poster-wrap"'):
            self.assertIn(anchor, src)
        # the stray close that popped the containers is gone
        self.assertNotIn("</label>\n        </div>\n\n        <label", src)

    def test_content_templates_extend_base(self):
        """Every template with a content block must extend base.html.

        A repair script once dropped ``{% extends %}`` from admin_dashboard.html;
        the page then rendered standalone (no shell, no <title>) without any
        render error.
        """
        for path in sorted(TEMPLATES_DIR.glob("*.html")):
            if path.name in ("base.html", "nuvio_open.html", "stremio_configure.html"):
                continue  # base defines the blocks; the two are standalone pages
            src = path.read_text(encoding="utf-8")
            if "{% block content %}" in src:
                self.assertIn("{% extends", src, f"{path.name} must extend base.html")
                self.assertTrue(
                    src.lstrip().startswith('{% extends "base.html" %}'),
                    f"{path.name} must be the child template (extends first)",
                )


if __name__ == "__main__":
    unittest.main()
