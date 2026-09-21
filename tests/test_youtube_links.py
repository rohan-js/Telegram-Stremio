"""Tests: YouTube link ingestion (videos + Shorts attached to matched titles).

Covers URL extraction/validation, oEmbed lookup, ytId stream building,
source-identity dedup, and the dead-link checker guard.
"""

import io
import unittest
from unittest.mock import patch

from Backend.fastapi.routes.stremio_routes import build_youtube_stream
from Backend.helper import youtube_links
from Backend.helper.youtube_links import (
    extract_youtube_ids,
    fetch_youtube_oembed,
    is_valid_youtube_id,
    strip_youtube_urls,
    youtube_watch_url,
)
from Backend.helper.database import Database
from Backend.helper.link_checker import is_liveness_checkable


class YoutubeIdValidationTests(unittest.TestCase):
    def test_accepts_standard_11_char_ids(self):
        self.assertTrue(is_valid_youtube_id("dQw4w9WgXcQ"))
        self.assertTrue(is_valid_youtube_id("o7vbDPUMWDc"))
        self.assertTrue(is_valid_youtube_id("aB1-2_cD3eF"))

    def test_rejects_wrong_lengths_and_chars(self):
        self.assertFalse(is_valid_youtube_id(""))
        self.assertFalse(is_valid_youtube_id("short"))
        self.assertFalse(is_valid_youtube_id("toolongvideoid123"))
        self.assertFalse(is_valid_youtube_id("invalid id!"))
        self.assertFalse(is_valid_youtube_id(None))


class YoutubeExtractTests(unittest.TestCase):
    def test_watch_url(self):
        self.assertEqual(
            extract_youtube_ids("Interstellar https://www.youtube.com/watch?v=zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )

    def test_watch_url_v_param_not_first(self):
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/watch?si=abc123&v=zSWdZVtXT7E&t=42s"),
            ["zSWdZVtXT7E"],
        )

    def test_short_link(self):
        self.assertEqual(extract_youtube_ids("https://youtu.be/zSWdZVtXT7E"), ["zSWdZVtXT7E"])

    def test_shorts_link(self):
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/shorts/zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )

    def test_embed_and_live_links(self):
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/embed/zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/live/zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )

    def test_music_and_mobile_hosts(self):
        self.assertEqual(
            extract_youtube_ids("https://music.youtube.com/watch?v=zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )
        self.assertEqual(
            extract_youtube_ids("https://m.youtube.com/watch?v=zSWdZVtXT7E"),
            ["zSWdZVtXT7E"],
        )

    def test_rejects_playlists_channels_handles(self):
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/playlist?list=PLabc123"),
            [],
        )
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/@SomeChannel"),
            [],
        )
        self.assertEqual(
            extract_youtube_ids("https://www.youtube.com/channel/UCabc123"),
            [],
        )
        self.assertEqual(extract_youtube_ids("https://vimeo.com/123456"), [])

    def test_rejects_invalid_ids(self):
        self.assertEqual(extract_youtube_ids("https://youtu.be/tooshort"), [])
        self.assertEqual(extract_youtube_ids("no links here"), [])
        self.assertEqual(extract_youtube_ids(""), [])

    def test_dedupes_preserving_order(self):
        self.assertEqual(
            extract_youtube_ids(
                "https://youtu.be/AAAAAAAAAAA and https://youtu.be/BBBBBBBBBBB and https://youtu.be/AAAAAAAAAAA"
            ),
            ["AAAAAAAAAAA", "BBBBBBBBBBB"],
        )

    def test_strip_leaves_caption_text(self):
        stripped = strip_youtube_urls("Ponman 2025 https://youtu.be/AAAAAAAAAAA 1080p")
        self.assertNotIn("youtu.be", stripped)
        self.assertIn("Ponman", stripped)

    def test_watch_url_builder(self):
        self.assertEqual(
            youtube_watch_url("zSWdZVtXT7E"),
            "https://www.youtube.com/watch?v=zSWdZVtXT7E",
        )


class YoutubeOembedTests(unittest.TestCase):
    def _fake_response(self, payload: bytes):
        response = io.BytesIO(payload)
        response.__enter__ = lambda self: self
        response.__exit__ = lambda self, *a: False
        return response

    def test_parses_title_and_author(self):
        body = b'{"title": "Some Video", "author_name": "Some Channel"}'
        with patch("urllib.request.urlopen", return_value=self._fake_response(body)):
            result = fetch_youtube_oembed("AAAAAAAAAAA")
        self.assertEqual(result, {"title": "Some Video", "author": "Some Channel"})

    def test_network_failure_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=Exception("blocked")):
            self.assertIsNone(fetch_youtube_oembed("AAAAAAAAAAA"))

    def test_invalid_id_skips_network(self):
        with patch("urllib.request.urlopen", side_effect=AssertionError("must not call")):
            self.assertIsNone(fetch_youtube_oembed("bad"))


class YoutubeStreamBuilderTests(unittest.TestCase):
    def test_builds_ytid_stream(self):
        stream = build_youtube_stream(
            {"youtube_id": "zSWdZVtXT7E", "recommended": True},
            "Telegram 1080p",
            "Title detail",
            binge_group="telegram-stremio-tt0133093",
        )
        self.assertEqual(stream["ytId"], "zSWdZVtXT7E")
        self.assertIn("YouTube", stream["name"])
        self.assertNotIn("url", stream)
        self.assertNotIn("infoHash", stream)
        self.assertEqual(stream["behaviorHints"]["bingeGroup"], "telegram-stremio-tt0133093")
        self.assertNotIn("_recommended", stream)

    def test_missing_id_returns_none(self):
        self.assertIsNone(build_youtube_stream({}, "n", "t"))
        self.assertIsNone(build_youtube_stream({"youtube_id": "  "}, "n", "t"))


class YoutubeIdentityTests(unittest.TestCase):
    def setUp(self):
        self.database = object.__new__(Database)

    def test_same_video_id_is_same_identity(self):
        self.assertTrue(
            self.database._same_source_identity(
                {"source_type": "youtube", "youtube_id": "AAAAAAAAAAA"},
                {"source_type": "youtube", "youtube_id": "AAAAAAAAAAA"},
            )
        )

    def test_different_video_ids_differ(self):
        self.assertFalse(
            self.database._same_source_identity(
                {"source_type": "youtube", "youtube_id": "AAAAAAAAAAA"},
                {"source_type": "youtube", "youtube_id": "BBBBBBBBBBB"},
            )
        )

    def test_missing_id_has_no_identity(self):
        self.assertIsNone(self.database._source_identity_key({"source_type": "youtube"}))


class LivenessGuardTests(unittest.TestCase):
    def test_telegram_checkable(self):
        self.assertTrue(is_liveness_checkable({}))
        self.assertTrue(is_liveness_checkable({"source_type": "telegram"}))

    def test_other_sources_skipped(self):
        self.assertFalse(is_liveness_checkable({"source_type": "youtube", "youtube_id": "x"}))
        self.assertFalse(is_liveness_checkable({"source_type": "torrent", "info_hash": "x"}))
        self.assertFalse(
            is_liveness_checkable({"source_type": "local_vps", "local_rel_path": "x"})
        )


if __name__ == "__main__":
    unittest.main()
