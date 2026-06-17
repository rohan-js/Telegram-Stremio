import asyncio
import time
import unittest
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper import gemini_matcher
from Backend.helper.gemini_matcher import maybe_rerank_with_gemini
from Backend.helper.metadata_matcher import MatchCandidate, MatchDecision, MatchIntent


class GeminiMatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        gemini_matcher._CACHE.clear()
        self.old_values = {
            "METADATA_RERANKER_ENABLED": Telegram.METADATA_RERANKER_ENABLED,
            "GEMINI_MATCHER_ENABLED": Telegram.GEMINI_MATCHER_ENABLED,
            "METADATA_RERANKER_PROVIDER": Telegram.METADATA_RERANKER_PROVIDER,
            "GEMINI_API_KEY": Telegram.GEMINI_API_KEY,
            "GEMINI_MATCHER_MODEL": Telegram.GEMINI_MATCHER_MODEL,
            "GEMINI_MATCHER_FALLBACK_MODEL": Telegram.GEMINI_MATCHER_FALLBACK_MODEL,
            "GROQ_API_KEY": Telegram.GROQ_API_KEY,
            "GROQ_MATCHER_MODEL": Telegram.GROQ_MATCHER_MODEL,
            "GROQ_MATCHER_FALLBACK_MODEL": Telegram.GROQ_MATCHER_FALLBACK_MODEL,
            "GEMINI_MATCHER_TIMEOUT_SECONDS": Telegram.GEMINI_MATCHER_TIMEOUT_SECONDS,
            "GEMINI_MATCHER_MAX_CANDIDATES": Telegram.GEMINI_MATCHER_MAX_CANDIDATES,
            "GEMINI_MATCHER_MIN_TOP_MARGIN": Telegram.GEMINI_MATCHER_MIN_TOP_MARGIN,
            "GEMINI_MATCHER_CACHE_TTL_SECONDS": Telegram.GEMINI_MATCHER_CACHE_TTL_SECONDS,
            "GEMINI_MATCHER_CACHE_MAX": Telegram.GEMINI_MATCHER_CACHE_MAX,
        }
        Telegram.METADATA_RERANKER_ENABLED = True
        Telegram.GEMINI_MATCHER_ENABLED = True
        Telegram.METADATA_RERANKER_PROVIDER = "gemini"
        Telegram.GEMINI_API_KEY = "test-key"
        Telegram.GEMINI_MATCHER_MODEL = "gemini-test"
        Telegram.GEMINI_MATCHER_FALLBACK_MODEL = ""
        Telegram.GROQ_API_KEY = ""
        Telegram.GROQ_MATCHER_MODEL = "groq-test"
        Telegram.GROQ_MATCHER_FALLBACK_MODEL = ""
        Telegram.GEMINI_MATCHER_TIMEOUT_SECONDS = 0.05
        Telegram.GEMINI_MATCHER_MAX_CANDIDATES = 4
        Telegram.GEMINI_MATCHER_MIN_TOP_MARGIN = 8.0
        Telegram.GEMINI_MATCHER_CACHE_TTL_SECONDS = 86400
        Telegram.GEMINI_MATCHER_CACHE_MAX = 2000

    def tearDown(self):
        for key, value in self.old_values.items():
            setattr(Telegram, key, value)
        gemini_matcher._CACHE.clear()

    def _intent(self):
        return MatchIntent(
            raw_title="War.1080p.mkv",
            clean_title="war",
            year=None,
            media_type="movie",
            title_variants=["war"],
        )

    def _candidates(self):
        return [
            MatchCandidate("tmdb", "War", 2019, "movie", tmdb_id=1),
            MatchCandidate("tmdb", "War", 2025, "movie", tmdb_id=2),
        ]

    def _decision(self, reason="accepted_low_confidence:metadata_ambiguous_match"):
        return MatchDecision(
            True,
            self._candidates()[0],
            100.0,
            reason,
            [
                {"source": "tmdb", "title": "War", "year": 2019, "media_type": "movie", "tmdb_id": 1, "score": 100.0},
                {"source": "tmdb", "title": "War", "year": 2025, "media_type": "movie", "tmdb_id": 2, "score": 96.0},
            ],
        )

    async def test_high_confidence_match_skips_gemini(self):
        decision = self._decision(reason="accepted")
        with patch.object(gemini_matcher, "_request_with_optional_fallback") as request:
            result = await maybe_rerank_with_gemini(self._intent(), decision, self._candidates())
        self.assertIs(result, decision)
        request.assert_not_called()

    async def test_low_confidence_close_candidates_call_gemini(self):
        async def fake_request(prompt, timeout):
            return {"selected_candidate_index": 1, "confidence": 88, "reason": "better release year"}, "gemini", "gemini-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            result = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertEqual(result.candidate.tmdb_id, 2)
        self.assertEqual(result.reason, "accepted_llm_rerank")
        self.assertTrue(result.extra["rerank_used"])
        self.assertEqual(result.extra["rerank_provider"], "gemini")
        self.assertEqual(result.extra["rerank_confidence"], 88)

    async def test_timeout_falls_back_quickly(self):
        async def slow_request(prompt, timeout):
            await asyncio.sleep(0.5)
            return {"selected_candidate_index": 1, "confidence": 88, "reason": "late"}, "gemini", "gemini-test"

        start = time.monotonic()
        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=slow_request):
            result = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertLess(time.monotonic() - start, 0.3)
        self.assertEqual(result.candidate.tmdb_id, 1)
        self.assertTrue(result.extra["rerank_timeout"])

    async def test_invalid_json_shape_falls_back(self):
        async def fake_request(prompt, timeout):
            return {"selected_candidate_index": 99, "confidence": 50, "reason": "bad"}, "gemini", "gemini-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            result = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertEqual(result.candidate.tmdb_id, 1)
        self.assertTrue(result.extra["rerank_invalid"])

    async def test_cache_prevents_duplicate_gemini_calls(self):
        calls = 0

        async def fake_request(prompt, timeout):
            nonlocal calls
            calls += 1
            return {"selected_candidate_index": 1, "confidence": 90, "reason": "cached"}, "gemini", "gemini-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            first = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())
            second = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertEqual(calls, 1)
        self.assertEqual(first.candidate.tmdb_id, 2)
        self.assertEqual(second.candidate.tmdb_id, 2)
        self.assertTrue(second.extra["rerank_cached"])

    async def test_wrong_media_type_selection_is_ignored(self):
        decision = self._decision()
        decision.candidates[1]["media_type"] = "tv"

        async def fake_request(prompt, timeout):
            return {"selected_candidate_index": 1, "confidence": 80, "reason": "wrong type"}, "gemini", "gemini-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            result = await maybe_rerank_with_gemini(self._intent(), decision, self._candidates())

        self.assertEqual(result.candidate.tmdb_id, 1)
        self.assertTrue(result.extra["rerank_invalid"])

    async def test_auto_provider_prefers_groq_when_configured(self):
        Telegram.METADATA_RERANKER_PROVIDER = "auto"
        Telegram.GROQ_API_KEY = "groq-key"
        Telegram.GROQ_MATCHER_MODEL = "groq-test"

        async def fake_request(prompt, timeout):
            return {"selected_candidate_index": 1, "confidence": 91, "reason": "groq chose newer"}, "groq", "groq-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            result = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertEqual(result.candidate.tmdb_id, 2)
        self.assertEqual(result.extra["rerank_provider"], "groq")
        self.assertEqual(result.extra["rerank_model"], "groq-test")

    async def test_groq_without_gemini_key_still_enabled(self):
        Telegram.METADATA_RERANKER_PROVIDER = "groq"
        Telegram.GEMINI_API_KEY = ""
        Telegram.GROQ_API_KEY = "groq-key"
        Telegram.GROQ_MATCHER_MODEL = "groq-test"

        async def fake_request(prompt, timeout):
            return {"selected_candidate_index": 1, "confidence": 87, "reason": "groq only"}, "groq", "groq-test"

        with patch.object(gemini_matcher, "_request_with_optional_fallback", side_effect=fake_request):
            result = await maybe_rerank_with_gemini(self._intent(), self._decision(), self._candidates())

        self.assertTrue(result.extra["rerank_used"])
        self.assertEqual(result.extra["rerank_provider"], "groq")


if __name__ == "__main__":
    unittest.main()
