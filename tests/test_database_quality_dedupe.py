import unittest

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


if __name__ == "__main__":
    unittest.main()
