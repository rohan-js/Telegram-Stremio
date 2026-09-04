"""Tests: Family Wrapped — daily rollup math and get_wrapped_stats merge."""

import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from Backend.fastapi.routes import api_routes
from Backend.helper import production_ops as pops


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.upserts = []

    def find(self, query=None):
        return self

    def sort(self, *a, **k):
        return self

    async def to_list(self, _):
        return self.docs

    async def find_one(self, query):
        if "_id" in query:
            for d in self.docs:
                if d.get("_id") == query["_id"]:
                    return d
        return None

    def aggregate(self, pipeline):
        # synchronous cursor (like motor): results fetched via awaited to_list
        match = next((st for st in pipeline if "$match" in st), {}).get("$match", {})
        group = next((st for st in pipeline if "$group" in st), {}).get("$group", {})
        rows = self.docs
        tok_match = match.get("token")
        if isinstance(tok_match, (str, int)):
            rows = [d for d in rows if d.get("token") == tok_match]
        if group.get("_id") is None:
            docs = [{
                "plays": len(rows),
                "bytes": sum(d.get("total_bytes") or 0 for d in rows),
                "seconds": sum(d.get("duration_sec") or 0 for d in rows),
                "titles": list({d.get("title") for d in rows if d.get("title")}),
            }]
        elif group.get("_id") == "$title":
            counts = {}
            for d in rows:
                t = d.get("title")
                if t:
                    counts[t] = counts.get(t, 0) + 1
            docs = [{"_id": t, "plays": n} for t, n in counts.items()]
        elif group.get("_id") == "$token":
            docs = []
            for tok in {d.get("token") for d in rows}:
                tok_rows = [d for d in rows if d.get("token") == tok]
                titles = [d.get("title") for d in tok_rows if d.get("title")]
                docs.append({
                    "_id": tok,
                    "plays": len(tok_rows),
                    "total_bytes": sum(d.get("total_bytes") or 0 for d in tok_rows),
                    "seconds": sum(d.get("duration_sec") or 0 for d in tok_rows),
                    "titles": list(set(titles)),
                })
        else:
            docs = []
        return SimpleResult(docs)

    async def update_one(self, query, update, upsert=False):
        self.upserts.append((query, update, upsert))
        return MagicMock()


class SimpleResult:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, _):
        return list(self._docs)


class FakeTracking(dict):
    def __getitem__(self, key):
        return self.get(key)


class RollupTests(unittest.IsolatedAsyncioTestCase):
    async def test_rollup_groups_by_token_and_upserts_month(self):
        analytics = FakeCollection([
            {"token": "tokA", "logged_at": _yesterday_noon(), "title": "Rocky III",
             "total_bytes": 1000, "duration_sec": 3600},
            {"token": "tokA", "logged_at": _yesterday_noon(), "title": "Rocky III",
             "total_bytes": 500, "duration_sec": 1800},
            {"token": "tokB", "logged_at": _yesterday_noon(), "title": "Other",
             "total_bytes": 10, "duration_sec": 60},
        ])
        wrapped = FakeCollection()
        tracking = FakeTracking(stream_analytics=analytics, wrapped_monthly=wrapped)

        db = MagicMock()
        db.dbs = {"tracking": tracking}

        written = await pops.wrapped_rollup_yesterday(db)
        self.assertEqual(written, 2)

        a_upsert = next(u for u in wrapped.upserts if u[0]["_id"].startswith("tokA:"))
        inc = a_upsert[1]["$inc"]
        self.assertEqual(inc["plays"], 2)
        self.assertEqual(inc["total_bytes"], 1500)
        self.assertEqual(inc["seconds"], 5400)
        self.assertIn("Rocky III", a_upsert[1]["$addToSet"]["titles"]["$each"])

    async def test_rollup_no_data_no_writes(self):
        analytics = FakeCollection([])
        wrapped = FakeCollection()
        tracking = FakeTracking(stream_analytics=analytics, wrapped_monthly=wrapped)
        db = MagicMock()
        db.dbs = {"tracking": tracking}

        written = await pops.wrapped_rollup_yesterday(db)
        self.assertEqual(written, 0)
        self.assertEqual(len(wrapped.upserts), 0)


def _yesterday_noon():
    from datetime import timedelta
    return (datetime.utcnow() - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)


class WrappedStatsTests(unittest.IsolatedAsyncioTestCase):
    async def _stats(self, analytics_docs, rollup_docs, activity_doc=None):
        from Backend.helper.database import Database

        analytics = FakeCollection(analytics_docs)
        wrapped = FakeCollection(rollup_docs)
        user_activity = FakeCollection([activity_doc] if activity_doc else [])
        tracking = FakeTracking(stream_analytics=analytics, wrapped_monthly=wrapped,
                                user_activity=user_activity)
        db = Database.__new__(Database)  # real method, no URI parsing
        db.dbs = {"tracking": tracking}
        return await db.get_wrapped_stats("tokA")

    async def test_merge_live_and_rollup(self):
        from datetime import timedelta
        now = datetime.utcnow()
        stats = await self._stats(
            analytics_docs=[
                {"token": "tokA", "logged_at": now, "title": "Movie X",
                 "total_bytes": 2 * 1024 ** 3, "duration_sec": 7200},
            ],
            rollup_docs=[
                {"token": "tokA", "month": now.strftime("%Y-%m"),
                 "plays": 3, "total_bytes": 1024 ** 3, "seconds": 3600,
                 "titles": ["Old Movie"]},
            ],
            activity_doc={"_id": "tokA", "name": "Dad", "app": "Nuvio",
                          "device": "Fire TV", "streams": 12},
        )
        # rollup for the CURRENT month is overridden by live data
        self.assertEqual(stats["total_plays"], 1)
        self.assertEqual(stats["hours"], 2.0)
        self.assertEqual(stats["gb"], 2.0)
        self.assertEqual(stats["distinct_titles"], 1)
        self.assertEqual(stats["favorite_app"], "Nuvio")
        self.assertEqual(stats["lifetime_streams"], 12)
        self.assertEqual(stats["top_titles"][0]["title"], "Movie X")

    async def test_closed_months_from_rollup_counted(self):
        now = datetime.utcnow()
        closed_month = "2026-01" if now.month > 1 else "2025-12"
        stats = await self._stats(
            analytics_docs=[],  # nothing live
            rollup_docs=[
                {"token": "tokA", "month": closed_month,
                 "plays": 10, "total_bytes": 5 * 1024 ** 3, "seconds": 36000,
                 "titles": ["A", "B"]},
            ],
        )
        self.assertEqual(stats["total_plays"], 10)
        self.assertEqual(stats["hours"], 10.0)
        self.assertEqual(stats["distinct_titles"], 2)
        self.assertTrue(any(m["month"] == closed_month for m in stats["months"]))


class WrappedApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_token_404(self):
        with patch("Backend.fastapi.security.tokens.db.get_api_token", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                await api_routes.get_wrapped_api("badtoken")
            self.assertEqual(ctx.exception.status_code, 404)

    async def test_known_token_returns_stats(self):
        stats = {"token": "good", "total_plays": 5}
        with (
            patch("Backend.fastapi.security.tokens.db.get_api_token", new=AsyncMock(return_value={"token": "good"})),
            patch.object(api_routes.db, "get_wrapped_stats", new=AsyncMock(return_value=stats)),
        ):
            resp = await api_routes.get_wrapped_api("good")
        self.assertEqual(resp.status_code, 200)


if __name__ == "__main__":
    unittest.main()
