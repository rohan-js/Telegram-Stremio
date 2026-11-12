"""Tests: AIOStreams-compatible stream formatter (upstream v5.2.0 port).

The engine is upstream's stream_formatter.py verbatim; these tests pin the
fork-facing contract: preset rendering, default-mode passthrough (routes keep
the built-in renderer), custom templates, and no-exception degradation.
"""

import unittest

from Backend.helper.stream_formatter import (
    MAX_LEN,
    PRESETS,
    build_parse_value,
    format_stream,
    render,
)

SAMPLE = "Dune.Part.Two.2024.2160p.UHD.BluRay.REMUX.DV.HDR10.HEVC.TrueHD.Atmos.7.1.ITA.ENG-GROUP.mkv"


class FakeSettings:
    stream_formatter = "default"
    formatter_name = ""
    formatter_description = ""


def make_settings(preset="custom", name="", description=""):
    settings = FakeSettings()
    settings.stream_formatter = preset
    settings.formatter_name = name
    settings.formatter_description = description
    return settings


class PresetTests(unittest.TestCase):
    def test_every_preset_renders_a_name(self):
        pv = build_parse_value(SAMPLE, 2_500_000_000, addon_name="Telegram", meta={"queryType": "movie", "type": "movie"})
        for key, tpl in PRESETS.items():
            name = render(tpl["name"], pv)
            self.assertTrue(name.strip(), f"preset {key} rendered an empty name")
            self.assertLessEqual(len(name), MAX_LEN)

    def test_torrentio_style_name_carries_resolution_and_addon(self):
        pv = build_parse_value(SAMPLE, 2_500_000_000, addon_name="Telegram", meta={"queryType": "movie", "type": "movie"})
        name = render(PRESETS["torrentio"]["name"], pv)
        self.assertIn("Telegram", name)
        self.assertIn("2160p", name)

    def test_parse_value_fields(self):
        pv = build_parse_value(SAMPLE, 65_000_000_000)
        self.assertEqual(pv["stream"]["resolution"], "2160p")
        self.assertEqual(render("{stream.size::sbytes}", pv), "65 GB")
        self.assertEqual(render("{stream.languages::join(' / ')::upper}", pv), "ENGLISH / ITALIAN")

    def test_line_collapsing_rule(self):
        pv = build_parse_value("file.mkv", 100)
        self.assertEqual(render("a\n\n{tools.removeLine}b\nc", pv), "a\nc")


class FormatStreamTests(unittest.TestCase):
    def test_default_returns_none(self):
        self.assertIsNone(format_stream(FakeSettings(), "file.mkv", 100))

    def test_custom_template_renders(self):
        settings = make_settings(
            name="{stream.resolution} {addon.name}",
            # Optional blocks with an unresolved field drop their whole line
            # (upstream: {?🌐 {stream.subtitles}?} → empty).
            description="{?🌐 {stream.subtitles::join(', ')}?}\n{stream.size::bytes}",
        )
        name, description = format_stream(settings, "Movie.2024.1080p.WEB.mkv", 1_500_000_000, addon_name="Telegram")
        self.assertIn("1080p", name)
        self.assertIn("Telegram", name)
        self.assertEqual(description, "1.5 GB")

    def test_custom_empty_templates_return_none(self):
        self.assertIsNone(format_stream(make_settings(name="", description=""), "file.mkv", 100))

    def test_unknown_field_does_not_raise(self):
        # Upstream degradation: unknown expressions render as visible
        # placeholders — a broken template must never 500 a stream request.
        settings = make_settings(name="{stream.not_a_field}", description="")
        name, _ = format_stream(settings, "file.mkv", 100)
        self.assertIn("invalid_expression", name)


if __name__ == "__main__":
    unittest.main()
