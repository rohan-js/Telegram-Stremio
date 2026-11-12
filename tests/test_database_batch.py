import unittest

from Backend.helper.database import Database


class _Cursor:
    def __init__(self, documents):
        self._documents = iter(documents)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._documents)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _Collection:
    def __init__(self, documents=None):
        self.documents = list(documents or [])
        self.queries = []
        self.indexes = []

    def find(self, query):
        self.queries.append(query)
        requested = set(query["tmdb_id"]["$in"])
        return _Cursor([dict(doc) for doc in self.documents if doc.get("tmdb_id") in requested])

    async def create_index(self, fields, **kwargs):
        self.indexes.append(fields)
        return "test_index"


class _Storage:
    def __init__(self, movie=None, tv=None, custom_catalogs=None):
        self.collections = {
            "movie": _Collection(movie),
            "tv": _Collection(tv),
            "custom_catalogs": custom_catalogs or _Collection(),
        }

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _Collection()
        return self.collections[name]


class DatabaseBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_hydration_preserves_order_duplicates_and_skips_missing(self):
        database = object.__new__(Database)
        first = _Storage(movie=[{"tmdb_id": 1, "title": "One"}], tv=[{"tmdb_id": 3, "title": "Three"}])
        second = _Storage(movie=[{"tmdb_id": 2, "title": "Two"}])
        database.dbs = {"storage_1": first, "storage_2": second}

        documents = await database.get_documents([
            {"tmdb_id": 2, "db_index": 2, "media_type": "movie"},
            {"tmdb_id": 999, "db_index": 1, "media_type": "movie"},
            {"tmdb_id": 3, "db_index": 1, "media_type": "tv"},
            {"tmdb_id": 2, "db_index": 2, "media_type": "movie"},
            {"tmdb_id": 1, "db_index": 1, "media_type": "movie"},
        ])

        self.assertEqual([doc["title"] for doc in documents], ["Two", "Three", "Two", "One"])
        self.assertEqual(len(first["movie"].queries), 1)
        self.assertEqual(len(first["tv"].queries), 1)
        self.assertEqual(len(second["movie"].queries), 1)

    async def test_index_creation_is_repeatable_for_tracking_and_storage(self):
        database = object.__new__(Database)
        catalogs = _Collection()
        tracking = _Storage(custom_catalogs=catalogs)
        storage = _Storage()
        database.dbs = {"tracking": tracking, "storage_1": storage}

        await database.ensure_indexes()
        await database.ensure_indexes()

        self.assertEqual(len(catalogs.indexes), 4)
        # Per run: 3 legacy (tmdb_id, imdb_id, visibility composite) + 8
        # large-library indexes (updated_on, rating, telegram.id, parts.msg_id,
        # title/title_english/original_title/prefix name) = 11; two runs = 22.
        self.assertEqual(len(storage["movie"].indexes), 22)
        self.assertEqual(len(storage["tv"].indexes), 22)
        # Repeat runs create the same index list (idempotent on real Mongo).
        self.assertEqual(len(set(str(i) for i in storage["movie"].indexes)), 11)
        self.assertEqual(len(set(str(i) for i in storage["tv"].indexes)), 11)


if __name__ == "__main__":
    unittest.main()
