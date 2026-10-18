"""Tests for the fuzzy identity merge (2026-10-17 Machante incident):
near-duplicate provider spellings of the same title must share external ids,
so an accepted TMDb-only candidate still inherits the IMDb id — otherwise the
stored doc has no imdb_id and /meta + /stream lookups by the IMDb id return
nothing (title invisible in Stremio)."""

import unittest

from Backend.helper.metadata_matcher import MatchCandidate, enrich_candidate_identity


def _cand(source, title, year, imdb_id=None, tmdb_id=None, media_type="movie"):
    return MatchCandidate(
        source=source, title=title, year=year, media_type=media_type,
        imdb_id=imdb_id, tmdb_id=tmdb_id,
    )


class FuzzyIdentityMergeTests(unittest.TestCase):
    def test_one_letter_variant_merges_imdb_id(self):
        selected = _cand("tmdb", "Machante Maalakha", 2025, tmdb_id=1503565)
        imdb_candidate = _cand("imdb", "Machante Malakha", 2025, imdb_id="tt28364526")

        merged = enrich_candidate_identity(selected, [selected, imdb_candidate])

        self.assertIsNotNone(merged)
        self.assertEqual(merged.imdb_id, "tt28364526")
        self.assertEqual(merged.tmdb_id, 1503565)

    def test_fuzzy_merge_requires_both_years(self):
        selected = _cand("tmdb", "Machante Maalakha", 2025, tmdb_id=1503565)
        imdb_candidate = _cand("imdb", "Machante Malakha", None, imdb_id="tt28364526")

        merged = enrich_candidate_identity(selected, [selected, imdb_candidate])

        self.assertIsNone(merged.imdb_id, "missing-year near-duplicate must not donate ids")

    def test_generic_titles_do_not_fuzzy_merge(self):
        selected = _cand("tmdb", "Rocky", 2025, tmdb_id=111)
        imdb_candidate = _cand("imdb", "Rocks", 2025, imdb_id="tt999")

        merged = enrich_candidate_identity(selected, [selected, imdb_candidate])

        self.assertIsNone(merged.imdb_id)

    def test_year_conflict_blocks_fuzzy_merge(self):
        selected = _cand("tmdb", "Machante Maalakha", 2025, tmdb_id=1503565)
        imdb_candidate = _cand("imdb", "Machante Malakha", 1999, imdb_id="tt28364526")

        merged = enrich_candidate_identity(selected, [selected, imdb_candidate])

        self.assertIsNone(merged.imdb_id)

    def test_multiple_distinct_imdb_candidates_stay_unmerged(self):
        """Two different IMDb ids among near-duplicates → ambiguous, no merge."""
        selected = _cand("tmdb", "Machante Maalakha", 2025, tmdb_id=1503565)
        c1 = _cand("imdb", "Machante Malakha", 2025, imdb_id="tt1111111")
        c2 = _cand("imdb", "Machante Maalakha", 2025, imdb_id="tt2222222")

        merged = enrich_candidate_identity(selected, [selected, c1, c2])

        self.assertIsNone(merged.imdb_id)

    def test_exact_match_with_missing_year_still_merges(self):
        """Existing behavior preserved: exact title + missing candidate year
        (non-generic title) still donates the id."""
        selected = _cand("tmdb", "Kumbalangi Nights", 2019, tmdb_id=222)
        imdb_candidate = _cand("imdb", "Kumbalangi Nights", None, imdb_id="tt8108198")

        merged = enrich_candidate_identity(selected, [selected, imdb_candidate])

        self.assertEqual(merged.imdb_id, "tt8108198")

    def test_no_merge_when_selected_already_complete(self):
        selected = _cand("tmdb", "Machante Maalakha", 2025, imdb_id="tt1", tmdb_id=1503565)
        other = _cand("imdb", "Machante Malakha", 2025, imdb_id="tt2")

        merged = enrich_candidate_identity(selected, [selected, other])

        self.assertEqual(merged.imdb_id, "tt1")
        self.assertIs(merged, selected)


if __name__ == "__main__":
    unittest.main()
