"""Tests for Backend.helper.nfo_generator — Kodi-style XML builders."""

import unittest

from Backend.helper.nfo_generator import (
    movie_nfo,
    tvshow_nfo,
    episode_nfo,
    season_nfo,
    _runtime_minutes,
)


def _movie_doc(**overrides):
    doc = {
        "title": "Rocky III",
        "title_english": "Rocky III",
        "original_title": "Rocky III",
        "release_year": 1982,
        "description": "Boxer fights & wins <big>",
        "rating": 6.8,
        "imdb_id": "tt0084602",
        "tmdb_id": 10614,
        "genres": ["Drama", "drama", "Sport "],
        "cast": ["Sylvester Stallone", "Talia Shire"],
        "runtime": "1h 39m",
        "poster": "https://x/p.jpg",
        "backdrop": "https://x/b.jpg",
        "production_countries": ["US"],
        "original_language": "en",
    }
    doc.update(overrides)
    return doc


class MovieNfoTests(unittest.TestCase):
    def test_structure_and_fields(self):
        xml = movie_nfo(_movie_doc())
        self.assertIn("<movie>", xml)
        self.assertIn("<title>Rocky III</title>", xml)
        self.assertIn("<year>1982</year>", xml)
        self.assertIn("<premiered>1982-01-01</premiered>", xml)
        self.assertIn("<runtime>99</runtime>", xml)
        self.assertIn('<uniqueid type="imdb" default="true">tt0084602</uniqueid>', xml)
        self.assertIn("<tmdbid>10614</tmdbid>", xml)
        self.assertIn('<thumb aspect="poster">https://x/p.jpg</thumb>', xml)
        self.assertIn("<fanart><thumb>https://x/b.jpg</thumb></fanart>", xml)
        self.assertIn("<source>Telegram-Stremio</source>", xml)

    def test_escaping(self):
        xml = movie_nfo(_movie_doc(description="a & b <c> \"q\""))
        self.assertIn("a &amp; b &lt;c&gt;", xml)
        self.assertNotIn("a & b", xml)

    def test_genre_dedupe_case_insensitive(self):
        xml = movie_nfo(_movie_doc())
        self.assertEqual(xml.count("<genre>Drama</genre>"), 1)
        self.assertEqual(xml.count("<genre>Sport</genre>"), 1)

    def test_missing_fields_resilient(self):
        xml = movie_nfo({"title": "Bare"})
        self.assertIn("<movie>", xml)
        self.assertNotIn("<year>", xml)
        self.assertNotIn("<rating>", xml)
        self.assertNotIn("<runtime>", xml)
        self.assertNotIn("<genre>", xml)

    def test_bad_rating_ignored(self):
        xml = movie_nfo(_movie_doc(rating="N/A"))
        self.assertNotIn("<rating>", xml)

    def test_cast_cap_40(self):
        xml = movie_nfo(_movie_doc(cast=[f"Actor {i}" for i in range(55)]))
        self.assertEqual(xml.count("<actor>"), 40)

    def test_runtime_minutes(self):
        self.assertEqual(_runtime_minutes("1h 57m"), 117)
        self.assertEqual(_runtime_minutes("99 min"), 99)
        self.assertEqual(_runtime_minutes("88"), 88)
        self.assertIsNone(_runtime_minutes(""))
        self.assertIsNone(_runtime_minutes("soon"))


class TvNfoTests(unittest.TestCase):
    def test_tvshow_structure(self):
        doc = {
            "title": "Breakout",
            "release_year": 2024,
            "release_year_end": 2026,
            "description": "d",
            "rating": 7.2,
            "imdb_id": "tt123",
            "genres": ["Thriller"],
        }
        xml = tvshow_nfo(doc)
        self.assertIn("<tvshow>", xml)
        self.assertIn("<showtitle>Breakout</showtitle>", xml)
        self.assertIn("<ended>2026</ended>", xml)

    def test_episode_nfo(self):
        show = {"title": "Breakout", "imdb_id": "tt123", "tmdb_id": 9}
        ep = {
            "title": "Pilot & Part 2",
            "episode_number": 2,
            "overview": "o",
            "released": "2024-03-05T00:00:00",
            "episode_backdrop": "https://x/e.jpg",
        }
        xml = episode_nfo(show, 1, ep)
        self.assertIn("<episodedetails>", xml)
        self.assertIn("<season>1</season>", xml)
        self.assertIn("<episode>2</episode>", xml)
        self.assertIn("<title>Pilot &amp; Part 2</title>", xml)
        self.assertIn("<aired>2024-03-05</aired>", xml)
        self.assertIn('<uniqueid type="imdb">tt123</uniqueid>', xml)

    def test_season_nfo(self):
        xml = season_nfo({"title": "Breakout"}, 2)
        self.assertIn("<seasonnumber>2</seasonnumber>", xml)
        self.assertIn("<showtitle>Breakout</showtitle>", xml)


if __name__ == "__main__":
    unittest.main()
