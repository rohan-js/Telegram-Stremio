"""Tests for the identity reconciliation job (Machante-class healing):
imdb-less docs must be healed from persisted match_candidates — or fall back
to the synthetic tg<tmdb> id — so catalog/meta/stream lookups resolve."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from Backend.helper import identity_repair


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)
        self._it = None

    def limit(self, n):
        self._docs = self._docs[: int(n)]
        return self

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class _FakeCollection:
    def __init__(self, docs):
        self.docs = docs
        self.updates = []

    def find(self, query=None, projection=None):
        return _FakeCursor(self.docs)

    async def update_one(self, flt, upd):
        self.updates.append((flt, upd))


def _machante_doc():
    return {
        "_id": "doc-1",
        "title": "Machante Maalakha",
        "imdb_id": None,
        "tmdb_id": 1503565,
        "release_year": 2025,
        "media_type": "movie",
        "telegram": [
            {
                "quality": "1080p",
                "match_candidates": [
                    {"source": "tmdb", "title": "Machante Maalakha", "year": 2025, "media_type": "movie", "imdb_id": None, "tmdb_id": 1503565},
                    {"source": "imdb", "title": "Machante Malakha", "year": 2025, "media_type": "movie", "imdb_id": "tt28364526", "tmdb_id": None},
                ],
            }
        ],
    }


def _tv_doc():
    return {
        "_id": "doc-tv-1",
        "title": "Some Show",
        "imdb_id": "",
        "tmdb_id": 999,
        "release_year": 2024,
        "media_type": "tv",
        "seasons": [
            {
                "season_number": 1,
                "episodes": [
                    {
                        "episode_number": 1,
                        "telegram": [
                            {
                                "quality": "720p",
                                "match_candidates": [
                                    {"source": "imdb", "title": "Some Show", "year": 2024, "media_type": "tv", "imdb_id": "tt7654321", "tmdb_id": None},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }


class BestMissingIdTests(unittest.TestCase):
    def test_derives_imdb_from_fuzzy_candidates(self):
        self.assertEqual(identity_repair._best_missing_id(_machante_doc()), "tt28364526")

    def test_derives_imdb_from_tv_episode_candidates(self):
        self.assertEqual(identity_repair._best_missing_id(_tv_doc()), "tt7654321")

    def test_no_candidates_returns_none(self):
        doc = {"title": "X", "imdb_id": None, "tmdb_id": 42, "media_type": "movie"}
        self.assertIsNone(identity_repair._best_missing_id(doc))

    def test_doc_with_imdb_is_skipped(self):
        doc = _machante_doc()
        doc["imdb_id"] = "tt-existing"
        self.assertIsNone(identity_repair._best_missing_id(doc))


class RepairSweepTests(unittest.IsolatedAsyncioTestCase):
    async def test_sweep_heals_real_and_synthetic_ids(self):
        real_doc = _machante_doc()
        synthetic_doc = {"_id": "doc-2", "title": "No Candidates", "imdb_id": None, "tmdb_id": -123, "media_type": "movie"}
        orphan_doc = {"_id": "doc-3", "title": "Nothing To Go On", "imdb_id": None, "media_type": "movie"}

        movie_coll = _FakeCollection([real_doc, synthetic_doc, orphan_doc])
        tv_coll = _FakeCollection([_tv_doc()])
        fake_db = SimpleNamespace(dbs={"storage_1": {"movie": movie_coll, "tv": tv_coll}})

        with patch.object(identity_repair, "db", fake_db):
            stats = await identity_repair.repair_missing_identities(notify=False)

        self.assertEqual(stats["scanned"], 4)
        self.assertEqual(stats["healed_real"], 2)  # machante movie + tv show
        self.assertEqual(stats["healed_synthetic"], 1)
        self.assertEqual(stats["unrecoverable"], 1)
        self.assertEqual(stats["errors"], 0)

        movie_sets = {u[0]["_id"]: u[1]["$set"]["imdb_id"] for u in movie_coll.updates}
        self.assertEqual(movie_sets["doc-1"], "tt28364526")
        self.assertEqual(movie_sets["doc-2"], "tg123")
        self.assertNotIn("doc-3", movie_sets)
        tv_sets = {u[0]["_id"]: u[1]["$set"]["imdb_id"] for u in tv_coll.updates}
        self.assertEqual(tv_sets["doc-tv-1"], "tt7654321")

    async def test_list_imdb_less_docs_reports_remaining(self):
        orphan = {"_id": "doc-3", "title": "Nothing To Go On", "imdb_id": None, "tmdb_id": None, "media_type": "movie"}
        movie_coll = _FakeCollection([orphan])
        fake_db = SimpleNamespace(dbs={"storage_1": {"movie": movie_coll}})

        with patch.object(identity_repair, "db", fake_db):
            result = await identity_repair.list_imdb_less_docs()

        self.assertEqual(result["count"], 1)
        self.assertFalse(result["truncated"])
        self.assertEqual(result["items"][0]["id"], "doc-3")

    async def test_sweep_without_storage_dbs_is_noop(self):
        fake_db = SimpleNamespace(dbs={})
        with patch.object(identity_repair, "db", fake_db):
            stats = await identity_repair.repair_missing_identities(notify=False)
        self.assertEqual(stats["scanned"], 0)


if __name__ == "__main__":
    unittest.main()
