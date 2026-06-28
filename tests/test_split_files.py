import unittest

from Backend.helper.split_files import (
    parse_combined_episodes,
    parse_split_info,
    strip_part_suffix,
)


class SplitFileParsingTests(unittest.TestCase):
    def test_video_extension_numeric_parts_group_together(self):
        first = parse_split_info("Movie.Name.2024.1080p.mkv.001")
        second = parse_split_info("Movie_Name_2024_1080p.mkv.002")

        self.assertEqual(first, ("movie.name.2024.1080p.mkv", 1))
        self.assertEqual(second, ("movie.name.2024.1080p.mkv", 2))

    def test_strip_part_suffix_restores_media_filename(self):
        self.assertEqual(
            strip_part_suffix("Movie.Name.2024.1080p.mkv.001"),
            "Movie.Name.2024.1080p.mkv",
        )

    def test_non_split_quality_tokens_are_ignored(self):
        self.assertIsNone(parse_split_info("Movie.Name.2024.1080p.x265.mkv"))

    def test_combined_episode_range(self):
        parsed = parse_combined_episodes("Show.Name.S01.E01-E08.720p.mkv")
        self.assertEqual(parsed, {"season": 1, "start": 1, "end": 8})

    def test_combined_season_pack_keyword(self):
        parsed = parse_combined_episodes("Show.Name.S02.COMBINED.720p.mkv")
        self.assertEqual(parsed, {"season": 2, "start": None, "end": None})


if __name__ == "__main__":
    unittest.main()
