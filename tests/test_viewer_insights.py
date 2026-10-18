"""Tests for the Viewer Insights feature:

- uploader extraction from Telegram messages (PyroFork variants),
- stream-meta enrichment (client IP / app / device / playback id),
- session building + identity classification (per-user tokens vs shared),
- rollups (overview / title audience / viewer profile),
- CSV rows, the daily digest text, and the log_stream_stats record builder.
"""

import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from Backend.helper import insights, insights_agg
from Backend.helper.database import Database
from Backend.helper.uploader import uploader_from_message


def _ts(hours_ago=1.0):
    return datetime.utcnow() - timedelta(hours=hours_ago)


def _record(**overrides):
    base = {
        "playback_id": "pb1",
        "token": "tok-1",
        "user_name": "Rohan",
        "token_user_id": 111,
        "client_ip": "1.2.3.4",
        "device": "Android TV",
        "app": "Stremio",
        "geo": {"country": "India", "country_code": "IN", "city": "Kochi", "isp": "Jio", "proxy": False},
        "media": {"imdb_id": "tt1", "tmdb_id": 5, "db_index": 1, "media_type": "movie", "title": "Movie A"},
        "title": "Movie A",
        "source_type": "telegram",
        "completion": 0.5,
        "watched_offset": 500,
        "file_size": 1000,
        "total_bytes": 100,
        "duration_sec": 60.0,
        "logged_at": _ts(1.0),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Uploader extraction
# ---------------------------------------------------------------------------
class UploaderExtractionTests(unittest.TestCase):
    def test_user_uploader(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, first_name="Rohan", last_name="J", username="rohan_j_s"),
            sender_chat=None,
        )
        result = uploader_from_message(message)
        self.assertEqual(result["kind"], "user")
        self.assertEqual(result["id"], 42)
        self.assertEqual(result["name"], "Rohan J")
        self.assertEqual(result["username"], "rohan_j_s")

    def test_username_only_user(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=7, first_name=None, last_name=None, username="someone"),
            sender_chat=None,
        )
        self.assertEqual(uploader_from_message(message)["name"], "@someone")

    def test_channel_identity_post(self):
        message = SimpleNamespace(
            from_user=None,
            sender_chat=SimpleNamespace(id=-100123, title="My Channel", username="mychannel"),
        )
        result = uploader_from_message(message)
        self.assertEqual(result["kind"], "channel")
        self.assertEqual(result["name"], "My Channel")

    def test_forwarded_flag(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=9, first_name="Bob", last_name=None, username=None),
            sender_chat=None,
            forward_from=SimpleNamespace(id=99, first_name="Alice", last_name=None, username=None),
        )
        self.assertEqual(uploader_from_message(message)["forwarded_from"], "Alice")

    def test_forward_only(self):
        message = SimpleNamespace(
            from_user=None,
            sender_chat=None,
            forward_from_chat=SimpleNamespace(title="Origin Channel"),
        )
        result = uploader_from_message(message)
        self.assertEqual(result["kind"], "forward")
        self.assertEqual(result["forwarded_from"], "Origin Channel")

    def test_none_message(self):
        self.assertIsNone(uploader_from_message(None))

    def test_normalize_uploader_sanitizes(self):
        normalized = Database._normalize_uploader(
            {"id": "42", "name": "  Rohan  ", "username": "@rohan_j_s", "kind": "USER", "forwarded_from": None}
        )
        self.assertEqual(normalized["id"], 42)
        self.assertEqual(normalized["name"], "Rohan")
        self.assertEqual(normalized["username"], "rohan_j_s")
        self.assertEqual(normalized["kind"], "user")

    def test_normalize_uploader_empty_is_none(self):
        self.assertIsNone(Database._normalize_uploader({"id": None, "name": "", "username": None}))
        self.assertIsNone(Database._normalize_uploader("bogus"))


# ---------------------------------------------------------------------------
# Meta enrichment
# ---------------------------------------------------------------------------
class StreamMetaEnrichmentTests(unittest.TestCase):
    def test_playback_id_stable_and_scoped(self):
        a = insights.playback_id_for("tok", "hash1")
        b = insights.playback_id_for("tok", "hash1")
        c = insights.playback_id_for("tok2", "hash1")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertIsNone(insights.playback_id_for("", "hash1"))

    def test_enrich_adds_ip_app_device(self):
        request = SimpleNamespace(
            headers={"x-forwarded-for": "9.9.9.9, 10.0.0.1", "user-agent": "Stremio/1.5.0"},
            client=SimpleNamespace(host="172.18.0.1"),
        )
        with (
            patch.object(insights, "parse_app", lambda ua: "Stremio"),
            patch.object(insights, "parse_device", lambda ua: "Android TV"),
        ):
            meta = insights.enrich_stream_meta({}, request=request, token="tok", stream_id_hash="h")
        self.assertEqual(meta["client_ip"], "9.9.9.9")
        self.assertEqual(meta["app"], "Stremio")
        self.assertEqual(meta["device"], "Android TV")
        self.assertTrue(meta["playback_id"])

    def test_enrich_media_identity_attached(self):
        meta = insights.enrich_stream_meta({}, media_identity={"imdb_id": "tt1"})
        self.assertEqual(meta["media"]["imdb_id"], "tt1")


# ---------------------------------------------------------------------------
# Sessions + identities
# ---------------------------------------------------------------------------
class SessionBuildingTests(unittest.TestCase):
    def test_same_playback_merges_within_bucket(self):
        records = [
            _record(total_bytes=100, logged_at=_ts(1.0)),
            _record(total_bytes=200, completion=0.9, logged_at=_ts(0.9)),
        ]
        sessions = insights_agg.build_sessions(records)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["bytes"], 300)
        self.assertEqual(sessions[0]["completion"], 0.9)
        self.assertEqual(sessions[0]["requests"], 2)

    def test_rewatch_after_gap_splits(self):
        records = [
            _record(logged_at=_ts(5.0)),
            _record(logged_at=_ts(1.0)),
        ]
        self.assertEqual(len(insights_agg.build_sessions(records)), 2)

    def test_legacy_record_without_playback_id(self):
        record = _record(playback_id=None, token="tok-x", chat_id=-100, msg_id=5)
        sessions = insights_agg.build_sessions([record])
        self.assertEqual(len(sessions), 1)
        self.assertIn("tok-x", sessions[0]["session_key"])

    def test_empty_records_filtered(self):
        empty = _record(total_bytes=0, completion=None, watched_offset=0, file_size=0)
        self.assertEqual(insights_agg.build_sessions([empty]), [])


class IdentityClassificationTests(unittest.TestCase):
    def test_user_mode(self):
        token_map = {"tok-1": {"user_id": 111, "name": "Rohan", "full_name": "Rohan J", "username": "rohan_j_s"}}
        ident = insights_agg.classify_identity(_record(), token_map)
        self.assertEqual(ident["mode"], "user")
        self.assertEqual(ident["key"], "user:111")
        self.assertEqual(ident["label"], "Rohan J")
        self.assertEqual(ident["username"], "rohan_j_s")

    def test_shared_token_ip_mode(self):
        token_map = {"tok-1": {"user_id": None, "name": "Shared", "shared": True}}
        record = _record(token_user_id=None)
        ident = insights_agg.classify_identity(record, token_map)
        self.assertEqual(ident["mode"], "ip")
        self.assertEqual(ident["key"], "ip:1.2.3.4")
        self.assertIn("Kochi", ident["place"])
        self.assertTrue(ident["shared_token"])

    def test_ownerless_token_mode(self):
        token_map = {"tok-1": {"user_id": None, "name": "Only Token", "shared": False}}
        record = _record(token_user_id=None, client_ip=None)
        ident = insights_agg.classify_identity(record, token_map)
        self.assertEqual(ident["mode"], "token")


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------
class OverviewRollupTests(unittest.TestCase):
    def _sessions(self):
        records = [
            _record(playback_id="a", logged_at=_ts(3.0)),
            _record(playback_id="b", token="tok-2", token_user_id=222, client_ip="5.5.5.5",
                    media={"imdb_id": "tt1", "tmdb_id": 5, "db_index": 1, "media_type": "movie", "title": "Movie A"},
                    geo={"country": "UAE", "city": "Dubai", "isp": "Etisalat"}, logged_at=_ts(2.0)),
            _record(playback_id="c", token="shared", token_user_id=None, client_ip="7.7.7.7",
                    media={"imdb_id": "tt2", "media_type": "movie", "title": "Movie B"}, logged_at=_ts(1.0)),
        ]
        return insights_agg.build_sessions(records)

    def test_totals_and_top_titles(self):
        token_map = {
            "tok-1": {"user_id": 111, "full_name": "Rohan", "shared": False},
            "tok-2": {"user_id": 222, "full_name": "Alice", "shared": False},
            "shared": {"user_id": None, "name": "Shared", "shared": True},
        }
        summary = insights_agg.summarize_overview(self._sessions(), token_map)
        totals = summary["totals"]
        self.assertEqual(totals["plays"], 3)
        self.assertEqual(totals["unique_viewers"], 3)
        self.assertEqual(totals["unique_titles"], 2)
        self.assertEqual(totals["identity_mix"]["user"], 2)
        self.assertEqual(totals["identity_mix"]["ip"], 1)
        top = summary["top_titles"][0]
        self.assertEqual(top["label"], "Movie A")
        self.assertEqual(top["plays"], 2)
        self.assertEqual(top["viewer_count"], 2)
        self.assertTrue(summary["countries"])

    def test_per_day_buckets(self):
        summary = insights_agg.summarize_overview(self._sessions(), {})
        self.assertTrue(summary["per_day"])
        self.assertEqual(sum(int(d["plays"]) for d in summary["per_day"]), 3)


class TitleAudienceTests(unittest.TestCase):
    def test_filter_by_imdb_and_viewers(self):
        records = [
            _record(playback_id="a"),
            _record(playback_id="b", token="tok-2", token_user_id=222, completion=1.0),
            _record(playback_id="c", media={"imdb_id": "tt9", "title": "Other"}, title="Other", completion=0.2),
        ]
        sessions = insights_agg.build_sessions(records)
        token_map = {
            "tok-1": {"user_id": 111, "full_name": "Rohan"},
            "tok-2": {"user_id": 222, "full_name": "Alice"},
        }
        data = insights_agg.title_audience(sessions, token_map, imdb_id="tt1")
        self.assertEqual(data["plays"], 2)
        self.assertEqual(data["unique_viewers"], 2)
        labels = {v["label"] for v in data["viewers"]}
        self.assertEqual(labels, {"Rohan", "Alice"})

    def test_title_string_fallback_for_legacy(self):
        legacy = _record(media=None, title="Old Movie", playback_id="x")
        sessions = insights_agg.build_sessions([legacy])
        data = insights_agg.title_audience(sessions, {}, title="old movie")
        self.assertEqual(data["plays"], 1)


class ViewerProfileTests(unittest.TestCase):
    def test_profile_history_and_totals(self):
        records = [
            _record(playback_id="a", title="Movie A", logged_at=_ts(4.0), media={"imdb_id": "tt1", "title": "Movie A"}),
            _record(playback_id="b", title="Movie B", logged_at=_ts(2.0), completion=1.0,
                    media={"imdb_id": "tt2", "title": "Movie B"}),
        ]
        sessions = insights_agg.build_sessions(records)
        data = insights_agg.viewer_profile(sessions, {"tok-1": {"user_id": 111, "full_name": "Rohan"}},
                                           viewer_key="user:111")
        self.assertEqual(data["viewer"]["label"], "Rohan")
        self.assertEqual(data["totals"]["plays"], 2)
        self.assertEqual(data["totals"]["unique_titles"], 2)
        self.assertEqual(len(data["history"]), 2)
        self.assertEqual(data["history"][0]["title"], "Movie B")  # newest first

    def test_profile_by_token(self):
        sessions = insights_agg.build_sessions([_record()])
        data = insights_agg.viewer_profile(sessions, {}, token="tok-1")
        self.assertEqual(data["totals"]["plays"], 1)

    def test_profile_missing_viewer(self):
        data = insights_agg.viewer_profile([], {}, viewer_key="user:999")
        self.assertIsNone(data["viewer"])


# ---------------------------------------------------------------------------
# CSV + digest
# ---------------------------------------------------------------------------
class CsvTests(unittest.TestCase):
    def test_scope_headers_and_counts(self):
        sessions = insights_agg.build_sessions([_record(playback_id="a"), _record(playback_id="b", logged_at=_ts(5.0))])
        headers, rows = insights_agg.csv_rows("plays", sessions, {})
        self.assertIn("viewer", headers)
        self.assertEqual(len(rows), 2)
        headers, rows = insights_agg.csv_rows("titles", sessions, {})
        self.assertIn("plays", headers)
        self.assertEqual(len(rows), 1)
        headers, rows = insights_agg.csv_rows("viewers", sessions, {"tok-1": {"user_id": 111, "full_name": "Rohan"}})
        self.assertIn("viewer", headers)
        self.assertEqual(len(rows), 1)


class DigestTests(unittest.TestCase):
    def test_empty_day_is_silent(self):
        self.assertIsNone(insights_agg.digest_message([], {}))

    def test_digest_mentions_totals_and_top_title(self):
        sessions = insights_agg.build_sessions([_record(playback_id="a")])
        message = insights_agg.digest_message(
            sessions, {"tok-1": {"user_id": 111, "full_name": "Rohan"}}, day_label="17 Oct 2026"
        )
        self.assertIn("Viewer Insights — 17 Oct 2026", message)
        self.assertIn("1 play(s)", message)
        self.assertIn("Movie A", message)


# ---------------------------------------------------------------------------
# log_stream_stats record builder
# ---------------------------------------------------------------------------
class _FakeCollection:
    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):
        self.docs.append(doc)

    async def update_one(self, *args, **kwargs):
        return None


class LogStreamStatsInsightsTests(unittest.IsolatedAsyncioTestCase):
    def _db(self):
        database = object.__new__(Database)
        analytics = _FakeCollection()
        activity = _FakeCollection()
        database.dbs = {"tracking": {"stream_analytics": analytics, "user_activity": activity}}
        return database, analytics

    async def test_record_carries_insight_fields_and_completion(self):
        database, analytics = self._db()
        stats = {
            "stream_id": "s1",
            "file_size": 2000,
            "file_offset": 1500,
            "meta": {
                "token": "tok-1",
                "token_user_id": 111,
                "client_ip": "8.8.8.8",
                "app": "Stremio",
                "device": "Android TV",
                "playback_id": "pb",
                "media": {"imdb_id": "tt1"},
                "title": "Movie A",
                "user_name": "Rohan",
            },
        }
        with patch(
            "Backend.helper.analytics.lookup_ip",
            new=AsyncMock(return_value={"country": "India", "country_code": "IN", "city": "Kochi", "isp": "Jio", "proxy": False}),
        ):
            await database.log_stream_stats(stats)
        record = analytics.docs[0]
        self.assertEqual(record["token_user_id"], 111)
        self.assertEqual(record["client_ip"], "8.8.8.8")
        self.assertEqual(record["completion"], 0.75)
        self.assertEqual(record["geo"]["city"], "Kochi")
        self.assertEqual(record["media"]["imdb_id"], "tt1")

    async def test_completion_falls_back_to_range_end(self):
        database, analytics = self._db()
        stats = {
            "stream_id": "s2",
            "file_size": 1000,
            "meta": {"token": "tok-1", "title": "X", "request_end": 499},
        }
        with patch("Backend.helper.analytics.lookup_ip", new=AsyncMock(return_value={})):
            await database.log_stream_stats(stats)
        record = analytics.docs[0]
        self.assertEqual(record["watched_offset"], 500)
        self.assertEqual(record["completion"], 0.5)
        self.assertNotIn("geo", record)  # empty lookup adds nothing


if __name__ == "__main__":
    unittest.main()
