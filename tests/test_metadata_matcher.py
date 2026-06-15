import unittest

from Backend.helper.metadata_matcher import (
    MatchCandidate,
    MatchIntent,
    build_title_variants,
    choose_best_candidate,
    normalize_title,
)


class MetadataMatcherTests(unittest.TestCase):
    def test_patriot_2026_rejects_the_patriot_2000(self):
        intent = MatchIntent(
            raw_title="Patriot",
            clean_title="patriot",
            year=2026,
            media_type="movie",
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("imdb", "The Patriot", 2000, "movie", imdb_id="tt0187393"),
            MatchCandidate("tmdb", "Patriot", 2026, "movie", imdb_id="tt33412884", tmdb_id=123),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.imdb_id, "tt33412884")

    def test_wrong_year_candidate_is_rejected(self):
        intent = MatchIntent(
            raw_title="Patriot",
            clean_title="patriot",
            year=2026,
            media_type="movie",
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("imdb", "The Patriot", 2000, "movie", imdb_id="tt0187393"),
        ])
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "metadata_year_mismatch")

    def test_duplicate_imdb_and_tmdb_same_title_is_not_ambiguous(self):
        intent = MatchIntent(
            raw_title="F9 The Fast Saga",
            clean_title="f9 the fast saga",
            year=2021,
            media_type="movie",
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("imdb", "F9: The Fast Saga", 2021, "movie", imdb_id="tt5433138", tmdb_id=385128),
            MatchCandidate("tmdb", "F9", 2021, "movie", tmdb_id=385128),
        ])
        self.assertTrue(decision.accepted)

    def test_generic_title_without_year_goes_unmatched(self):
        intent = MatchIntent(
            raw_title="War",
            clean_title="war",
            year=None,
            media_type="movie",
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("imdb", "War", 2019, "movie", imdb_id="tt7430722"),
        ])
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "metadata_generic_title_needs_year")

    def test_normalize_title_removes_release_noise(self):
        self.assertEqual(
            normalize_title("Patriot.2026.Malayalam.1080p.ZEE5.WEB-DL.DD+5.1.H.264-JeRi.mkv"),
            "patriot jeri",
        )

    def test_release_site_prefix_builds_clean_title_variant(self):
        variants = build_title_variants(
            raw_title="www_1TamilMV_cards_Patriot_2026_TRUE_WEB_DL_1080p_AVC.mkv",
            parsed_title="www 1TamilMV cards Patriot",
            year=2026,
        )
        self.assertEqual(variants[0], "patriot")
        self.assertIn("patriot", variants)

    def test_clean_variant_accepts_correct_generic_title(self):
        intent = MatchIntent(
            raw_title="www 1TamilMV cards Patriot",
            clean_title="www 1tamilmv cards patriot",
            year=2026,
            media_type="movie",
            title_variants=["patriot", "www 1tamilmv cards patriot"],
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("imdb", "The Patriot", 2000, "movie", imdb_id="tt0187393"),
            MatchCandidate("database", "Patriot", 2026, "movie", imdb_id="tt33412884", tmdb_id=1300501),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.imdb_id, "tt33412884")

    def test_generic_exact_title_beats_partial_title_with_same_year(self):
        intent = MatchIntent(
            raw_title="Patriot",
            clean_title="patriot",
            year=2026,
            media_type="movie",
            title_variants=["patriot"],
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Patriot", 2026, "movie", tmdb_id=1300501),
            MatchCandidate("tmdb", "Antoni Patek, Patriot and Watchmaker", 2025, "movie", tmdb_id=1600775),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.tmdb_id, 1300501)


    def test_arabic_sequel_number_matches_roman_title(self):
        variants = build_title_variants("Rocky.3.1982.1080p.mkv", "Rocky 3", 1982)
        intent = MatchIntent(
            raw_title="Rocky 3",
            clean_title=variants[0],
            year=1982,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Rocky III", 1982, "movie", tmdb_id=1371),
            MatchCandidate("tmdb", "Rocky", 1976, "movie", tmdb_id=1366),
            MatchCandidate("tmdb", "Rocky IV", 1985, "movie", tmdb_id=1374),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.tmdb_id, 1371)

    def test_roman_sequel_number_matches_roman_title(self):
        variants = build_title_variants("Rocky.III.1982.1080p.mkv", "Rocky III", 1982)
        intent = MatchIntent(
            raw_title="Rocky III",
            clean_title=variants[0],
            year=1982,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Rocky III", 1982, "movie", tmdb_id=1371),
            MatchCandidate("tmdb", "Rocky IV", 1985, "movie", tmdb_id=1374),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.tmdb_id, 1371)

    def test_rocky_four_matches_roman_four(self):
        variants = build_title_variants("Rocky.4.1985.1080p.mkv", "Rocky 4", 1985)
        intent = MatchIntent(
            raw_title="Rocky 4",
            clean_title=variants[0],
            year=1985,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Rocky IV", 1985, "movie", tmdb_id=1374),
            MatchCandidate("tmdb", "Rocky III", 1982, "movie", tmdb_id=1371),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.tmdb_id, 1374)

    def test_part_number_matches_roman_part_title(self):
        variants = build_title_variants("Movie.Part.2.2024.1080p.mkv", "Movie Part 2", 2024)
        self.assertIn("movie part ii", variants)
        self.assertIn("movie ii", variants)
        intent = MatchIntent(
            raw_title="Movie Part 2",
            clean_title=variants[0],
            year=2024,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Movie Part II", 2024, "movie", tmdb_id=2002),
            MatchCandidate("tmdb", "Movie Part III", 2025, "movie", tmdb_id=2003),
        ])
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.candidate.tmdb_id, 2002)

    def test_chapter_and_volume_build_equivalent_variants(self):
        chapter_variants = build_title_variants("Saga.Chapter.3.2024.mkv", "Saga Chapter 3", 2024)
        volume_variants = build_title_variants("Saga.Vol.2.2024.mkv", "Saga Vol 2", 2024)
        season_variants = build_title_variants("Saga.Season.2.2024.mkv", "Saga Season 2", 2024)
        self.assertIn("saga chapter iii", chapter_variants)
        self.assertIn("saga iii", chapter_variants)
        self.assertIn("saga vol ii", volume_variants)
        self.assertIn("saga ii", volume_variants)
        self.assertIn("saga season ii", season_variants)
        self.assertIn("saga ii", season_variants)

    def test_wrong_sequel_number_is_rejected(self):
        variants = build_title_variants("Rocky.3.1982.1080p.mkv", "Rocky 3", 1982)
        intent = MatchIntent(
            raw_title="Rocky 3",
            clean_title=variants[0],
            year=1982,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Rocky IV", 1982, "movie", tmdb_id=1374),
        ])
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "metadata_sequel_mismatch")

    def test_no_year_ambiguous_franchise_goes_unmatched(self):
        variants = build_title_variants("Rocky.3.1080p.mkv", "Rocky 3", None)
        intent = MatchIntent(
            raw_title="Rocky 3",
            clean_title=variants[0],
            year=None,
            media_type="movie",
            title_variants=variants,
        )
        decision = choose_best_candidate(intent, [
            MatchCandidate("tmdb", "Rocky III", 1982, "movie", tmdb_id=1371),
            MatchCandidate("tmdb", "Rocky 3", 2024, "movie", tmdb_id=9999),
        ])
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "metadata_ambiguous_match")


if __name__ == "__main__":
    unittest.main()
