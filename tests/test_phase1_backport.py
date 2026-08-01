import unittest
from types import SimpleNamespace

from Backend.fastapi.routes.api_routes import _norm_chat_id
from Backend.fastapi.routes.stream_routes import _content_disposition
from Backend.helper import metadata
from Backend.helper.analytics import client_ip_from, parse_app
from Backend.helper.database import Database
from Backend.helper.skip_channel import is_skip_channel
from Backend.helper.settings_manager import Settings, SettingsManager
from Backend.helper.subtitles import (
    _label_for,
    _strip_language_and_ext,
    detect_language,
    list_languages,
    stremio_subtitle_entries,
)


class FakeChat:
    def __init__(self, chat_id, username=None):
        self.id = chat_id
        self.username = username


class FakeMessage:
    def __init__(self, chat_id, username=None):
        self.chat = FakeChat(chat_id, username)


class FakeRequest:
    def __init__(self, headers, client_host="9.9.9.9"):
        self.headers = headers
        self.client = SimpleNamespace(host=client_host)


class PhaseOneBackportTests(unittest.TestCase):
    def setUp(self):
        self._settings = SettingsManager._current

    def tearDown(self):
        SettingsManager._current = self._settings

    def test_parse_media_name_extracts_title_year_quality(self):
        parsed = metadata.parse_media_name("The Matrix 1999 1080p.mkv")
        self.assertEqual(parsed["title"], "The Matrix")
        self.assertEqual(parsed["year"], 1999)
        self.assertEqual(parsed["quality"], "1080p")

    def test_parse_media_name_extracts_season_episode(self):
        parsed = metadata.parse_media_name("Show.Name.S01E02.720p.WEB-DL.mkv")
        self.assertEqual(parsed["title"], "Show Name")
        self.assertEqual(parsed["season"], 1)
        self.assertEqual(parsed["episode"], 2)
        self.assertEqual(parsed["quality"], "720p")

    def test_parse_media_name_tolerates_garbage(self):
        parsed = metadata.parse_media_name("")
        self.assertIn("title", parsed)
        self.assertIsNone(parsed.get("year"))

    def test_analyze_metadata_failure_multipart(self):
        msg = metadata.analyze_metadata_failure("Movie.2021.1080p.part1.mkv")
        self.assertIn("multi-part", msg)

    def test_analyze_metadata_failure_missing_quality(self):
        msg = metadata.analyze_metadata_failure("Some Random Movie.mkv")
        self.assertIn("No video quality", msg)

    def test_analyze_metadata_failure_unmatchable(self):
        msg = metadata.analyze_metadata_failure("Inception.2010.1080p.BluRay.x264.mkv")
        self.assertIn("Could not match this title", msg)

    def test_build_id_link_imdb_and_tmdb(self):
        self.assertEqual(
            metadata.build_id_link("tt0111161"),
            "https://www.imdb.com/title/tt0111161/",
        )
        self.assertEqual(
            metadata.build_id_link("12345", "tv"),
            "https://www.themoviedb.org/tv/12345",
        )
        self.assertEqual(metadata.build_id_link(""), "")

    def test_caption_with_id_appends_and_deduplicates(self):
        info = {"imdb_id": "tt0111161", "media_type": "movie"}
        stamped = metadata.caption_with_id("My caption", info)
        self.assertIn("https://www.imdb.com/title/tt0111161/", stamped)
        self.assertEqual(metadata.caption_with_id(stamped, info), stamped)
        self.assertEqual(metadata.caption_with_id("", {}), "")

    def test_database_dup_key_normalizes_name_and_size(self):
        key = Database._dup_key(
            {"quality": "1080p", "name": "  A.B. 2024.mkv  ", "size": " 1.5 GB "}
        )
        self.assertEqual(key, ("1080p", "a.b. 2024.mkv", "1.5 gb"))

    def test_skip_channel_matches_numeric_and_username(self):
        SettingsManager._current = Settings({"skip_channel": "-100123456789"})
        self.assertTrue(is_skip_channel(FakeMessage(-100123456789)))
        self.assertFalse(is_skip_channel(FakeMessage(-100999999999)))

        SettingsManager._current = Settings({"skip_channel": "@skipchan"})
        self.assertTrue(is_skip_channel(FakeMessage(-100123456789, "skipchan")))
        self.assertFalse(is_skip_channel(FakeMessage(-100123456789, "other")))
        self.assertFalse(is_skip_channel(FakeMessage(-100123456789, None)))

        SettingsManager._current = Settings({"skip_channel": ""})
        self.assertFalse(is_skip_channel(FakeMessage(-100123456789)))

    def test_norm_chat_id_handles_numeric_and_username(self):
        self.assertEqual(_norm_chat_id("-100123"), -100123)
        self.assertEqual(_norm_chat_id("-100abc"), "-100abc")
        self.assertEqual(_norm_chat_id(""), None)

    def test_parse_app_maps_user_agents(self):
        self.assertEqual(parse_app("Stremio/4.4.168 (Android)"), "Stremio")
        self.assertEqual(parse_app("Nuvio/1.2.3"), "Nuvio")
        self.assertEqual(parse_app(""), "Unknown")
        self.assertEqual(parse_app("Totally Unknown Thing"), "Unknown")

    def test_client_ip_from_proxy_headers(self):
        req = FakeRequest(
            {"cf-connecting-ip": "1.2.3.4", "x-forwarded-for": "5.6.7.8, 9.9.9.9"}
        )
        self.assertEqual(client_ip_from(req), "1.2.3.4")

        req = FakeRequest({"x-forwarded-for": "5.6.7.8, 9.9.9.9"})
        self.assertEqual(client_ip_from(req), "5.6.7.8")

        req = FakeRequest({})
        self.assertEqual(client_ip_from(req), "9.9.9.9")

    def test_subtitle_language_detection(self):
        self.assertEqual(detect_language("Movie.2024.Hindi.srt"), ("hin", "Hindi"))
        self.assertEqual(detect_language("episode.eng.srt"), ("eng", "English"))
        self.assertEqual(detect_language("Movie.2024.srt"), ("und", "Unknown"))

    def test_strip_language_and_ext(self):
        self.assertEqual(_strip_language_and_ext("Movie.2024.English.srt"), "Movie.2024")
        self.assertEqual(_strip_language_and_ext("Movie.2024.srt"), "Movie.2024")

    def test_list_languages_includes_common(self):
        languages = list_languages()
        self.assertGreaterEqual(len(languages), 20)
        self.assertIn({"code": "eng", "label": "English"}, languages)

    def test_label_for_known_and_unknown(self):
        self.assertEqual(_label_for("hin"), "Hindi")
        self.assertEqual(_label_for("xx"), "Unknown")
        self.assertEqual(_label_for(""), "Unknown")

    def test_stremio_subtitle_entries_disambiguate_duplicates(self):
        subtitles = [
            {"lang_label": "English", "name": "a.srt", "encoded": "e1", "msg_id": 1},
            {"lang_label": "English", "name": "b.srt", "encoded": "e2", "msg_id": 2},
            {"lang_label": "Hindi", "name": "c.srt", "msg_id": 3},
        ]
        entries = stremio_subtitle_entries(subtitles, "tok", "https://example.com/")
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["lang"], "English (1)")
        self.assertEqual(entries[1]["lang"], "English (2)")
        self.assertTrue(entries[0]["url"].startswith("https://example.com/sub/tok/e1/subtitle.srt"))

    def test_content_disposition_rfc5987_and_ascii_fallback(self):
        header = _content_disposition("Movie 2024.mkv")
        self.assertTrue(header.startswith('inline; filename="Movie 2024.mkv"'))
        self.assertIn("filename*=UTF-8''Movie%202024.mkv", header)

        unicode_header = _content_disposition("Мой файл.mkv")
        self.assertIn("UTF-8''", unicode_header)
        self.assertNotIn("Мой", unicode_header.split("filename*=")[0])


if __name__ == "__main__":
    unittest.main()
