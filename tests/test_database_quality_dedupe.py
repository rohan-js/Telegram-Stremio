import unittest
from types import SimpleNamespace
from unittest.mock import patch

from Backend.helper.database import Database


class DatabaseQualityDedupeTests(unittest.TestCase):
    def setUp(self):
        self.database = object.__new__(Database)

    def test_exact_torrent_source_replaces_existing_entry(self):
        existing = [
            {
                "source_type": "torrent",
                "info_hash": "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                "file_idx": 0,
                "quality": "1080p",
                "name": "Old.mkv",
                "recommended": True,
                "quality_note": "good seeders",
            },
            {
                "source_type": "torrent",
                "info_hash": "1111111111111111111111111111111111111111",
                "file_idx": 0,
                "quality": "1080p",
                "name": "Other.mkv",
            },
        ]
        incoming = {
            "source_type": "torrent",
            "info_hash": "abcdef0123456789abcdef0123456789abcdef01",
            "file_idx": 0,
            "quality": "1080p",
            "name": "Fresh.mkv",
        }

        updated, replaced = self.database._replace_exact_source_quality(existing, incoming)

        self.assertTrue(replaced)
        self.assertEqual(len(updated), 2)
        self.assertEqual(updated[0]["name"], "Fresh.mkv")
        self.assertTrue(updated[0]["recommended"])
        self.assertEqual(updated[0]["quality_note"], "good seeders")

    def test_same_torrent_hash_with_different_file_index_is_not_duplicate(self):
        existing = [
            {
                "source_type": "torrent",
                "info_hash": "abcdef0123456789abcdef0123456789abcdef01",
                "file_idx": 0,
                "quality": "1080p",
            },
        ]
        incoming = {
            "source_type": "torrent",
            "info_hash": "abcdef0123456789abcdef0123456789abcdef01",
            "file_idx": 1,
            "quality": "1080p",
        }

        updated, replaced = self.database._replace_exact_source_quality(existing, incoming)

        self.assertFalse(replaced)
        self.assertEqual(updated, existing)

    def test_exact_telegram_source_replaces_existing_entry(self):
        existing = [
            {
                "source_type": "telegram",
                "id": "encoded-source-id",
                "quality": "720p",
                "name": "Old.mkv",
                "hidden_from_stremio": True,
            }
        ]
        incoming = {
            "source_type": "telegram",
            "id": "encoded-source-id",
            "quality": "1080p",
            "name": "New.mkv",
        }

        updated, replaced = self.database._replace_exact_source_quality(existing, incoming)

        self.assertTrue(replaced)
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["quality"], "1080p")
        self.assertEqual(updated[0]["name"], "New.mkv")
        self.assertTrue(updated[0]["hidden_from_stremio"])

    def test_exact_local_vps_path_replaces_existing_entry(self):
        existing = [
            {
                "source_type": "local_vps",
                "local_rel_path": "manual/Pizza/movie.mkv",
                "quality": "1080p",
                "name": "Old.mkv",
                "recommended": True,
            }
        ]
        incoming = {
            "source_type": "local_vps",
            "local_rel_path": "manual/Pizza/movie.mkv",
            "quality": "1080p",
            "name": "Pizza.mkv",
        }

        updated, replaced = self.database._replace_exact_source_quality(existing, incoming)

        self.assertTrue(replaced)
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["name"], "Pizza.mkv")
        self.assertTrue(updated[0]["recommended"])

    def test_partial_media_record_backfills_missing_identity_and_metadata(self):
        existing = {
            "title": "Farzi",
            "imdb_id": None,
            "tmdb_id": 132117,
            "release_year": 0,
            "description": "",
            "poster": "",
        }
        incoming = {
            "title": "Farzi",
            "imdb_id": "tt15477488",
            "tmdb_id": 132117,
            "release_year": 2023,
            "description": "An artist is pulled into counterfeiting.",
            "poster": "https://example.invalid/farzi.jpg",
        }

        self.database._backfill_missing_media_metadata(existing, incoming)

        self.assertEqual(existing["imdb_id"], "tt15477488")
        self.assertEqual(existing["release_year"], 2023)
        self.assertEqual(existing["description"], incoming["description"])
        self.assertEqual(existing["poster"], incoming["poster"])

    def test_backfill_does_not_overwrite_existing_manual_metadata(self):
        existing = {
            "imdb_id": "tt15477488",
            "tmdb_id": 132117,
            "release_year": 2023,
            "description": "Manual description",
        }
        incoming = {
            "imdb_id": "tt99999999",
            "tmdb_id": 999,
            "release_year": 2024,
            "description": "Provider description",
        }

        self.database._backfill_missing_media_metadata(existing, incoming)

        self.assertEqual(existing["imdb_id"], "tt15477488")
        self.assertEqual(existing["tmdb_id"], 132117)
        self.assertEqual(existing["release_year"], 2023)
        self.assertEqual(existing["description"], "Manual description")


class DuplicateResolutionTests(unittest.TestCase):
    """REPLACE_MODE off: identical (quality, name, size) qualities are skipped
    when duplicate_protection is on, but still auto-FLAGGED (badge only, never
    hidden) when it's off — the 2026-10-17 double-upload case."""

    def setUp(self):
        self.database = object.__new__(Database)
        self.existing = [
            {
                "source_type": "telegram",
                "id": "enc-1",
                "quality": "1080p",
                "name": "Machante.Maalakha.2025.1080p.mkv",
                "size": "3.03GB",
            }
        ]
        self.incoming = {
            "source_type": "telegram",
            "id": "enc-2",
            "quality": "1080p",
            "name": "Machante.Maalakha.2025.1080p.mkv",
            "size": "3.03GB",
        }

    def test_flags_when_protection_off(self):
        from Backend.helper.settings_manager import SettingsManager

        with patch.object(SettingsManager, "current", return_value=SimpleNamespace(duplicate_protection=False)):
            resolution = self.database._duplicate_resolution(self.existing, self.incoming, tmdb_id=1503565)
        self.assertEqual(resolution, "flag")

    def test_skips_when_protection_on(self):
        from Backend.helper.settings_manager import SettingsManager

        with patch.object(SettingsManager, "current", return_value=SimpleNamespace(duplicate_protection=True)):
            resolution = self.database._duplicate_resolution(self.existing, self.incoming, tmdb_id=1503565)
        self.assertEqual(resolution, "skip")

    def test_different_size_is_not_a_duplicate(self):
        from Backend.helper.settings_manager import SettingsManager

        other = dict(self.incoming, size="4.10GB")
        with patch.object(SettingsManager, "current", return_value=SimpleNamespace(duplicate_protection=False)):
            resolution = self.database._duplicate_resolution(self.existing, other, tmdb_id=1503565)
        self.assertIsNone(resolution)

    def test_personal_media_never_flagged(self):
        from Backend.helper.settings_manager import SettingsManager

        with patch.object(SettingsManager, "current", return_value=SimpleNamespace(duplicate_protection=False)):
            resolution = self.database._duplicate_resolution(self.existing, self.incoming, tmdb_id=-1234)
        self.assertIsNone(resolution)


if __name__ == "__main__":
    unittest.main()
