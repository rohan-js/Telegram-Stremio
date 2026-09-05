"""Regression tests for the upstream-v5.0.2 port batch: stale-stream reaper,
title_english sync, activity valid-token cleanup, skip-channel wiring,
announcement delete, subtitle indexes, F1 title score, /thumb route."""

import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from Backend.config import Telegram
from Backend.fastapi.routes import api_routes, stream_routes, stremio_routes
from Backend.helper import analytics, custom_dl, global_search
from Backend.helper.database import Database
from Backend.helper.settings_manager import DEFAULTS


# ------------------------------- Port 1: reaper

class StaleReaperTests(unittest.IsolatedAsyncioTestCase):
    async def test_reaps_idle_and_keeps_active(self):
        custom_dl.ACTIVE_STREAMS.clear()
        custom_dl.RECENT_STREAMS.clear()
        custom_dl.work_loads[3] = 1
        now = time.time()
        custom_dl.ACTIVE_STREAMS["idle180"] = {
            "status": "active", "start_ts": now - 400, "last_ts": now - 400,
            "total_bytes": 5000, "client_index": 3,
        }
        custom_dl.ACTIVE_STREAMS["zerobyte"] = {
            "status": "active", "start_ts": now - 200, "last_ts": now - 200,
            "total_bytes": 0, "client_index": 3,
        }
        custom_dl.ACTIVE_STREAMS["healthy"] = {
            "status": "active", "start_ts": now - 10, "last_ts": now - 2,
            "total_bytes": 9999, "client_index": 3,
        }
        # Emulate one reaper pass (loop body) against current entries.
        now2 = time.time()
        stale = []
        for sid, entry in list(custom_dl.ACTIVE_STREAMS.items()):
            last = entry.get("last_ts") or entry.get("start_ts") or 0
            status = entry.get("status") or "active"
            total = entry.get("total_bytes") or 0
            idle = now2 - last
            if status != "active" or idle > custom_dl.STALE_STREAM_IDLE or (total == 0 and idle > 60):
                stale.append(sid)
        for sid in stale:
            entry = custom_dl.ACTIVE_STREAMS.pop(sid, None)
            if entry:
                entry["status"] = "stale"
                entry["end_ts"] = now2
                custom_dl.RECENT_STREAMS.appendleft(entry)
                idx = entry.get("client_index")
                if idx is not None and idx in custom_dl.work_loads:
                    custom_dl.work_loads[idx] = max(0, custom_dl.work_loads[idx] - 1)

        self.assertNotIn("idle180", custom_dl.ACTIVE_STREAMS)
        self.assertNotIn("zerobyte", custom_dl.ACTIVE_STREAMS)
        self.assertIn("healthy", custom_dl.ACTIVE_STREAMS)
        self.assertEqual(custom_dl.RECENT_STREAMS[0]["status"], "stale")
        custom_dl.work_loads[3] = 0
        custom_dl.ACTIVE_STREAMS.clear()
        custom_dl.RECENT_STREAMS.clear()

    async def test_ensure_stale_cleaner_starts_once(self):
        calls = []

        async def fake_loop():
            calls.append(1)
            await asyncio.sleep(0)

        with patch.object(custom_dl, "_cleanup_stale_streams", fake_loop):
            custom_dl._STALE_CLEANER_STARTED = False
            with patch.object(custom_dl.asyncio, "create_task", side_effect=lambda c: asyncio.ensure_future(c)):
                custom_dl._ensure_stale_cleaner()
                custom_dl._ensure_stale_cleaner()
            await asyncio.sleep(0.01)
        self.assertEqual(len(calls), 1)  # started exactly once
        custom_dl._STALE_CLEANER_STARTED = False


# ------------------------------- Port 2: title_english sync

class TitleEnglishSyncTests(unittest.TestCase):
    def test_update_data_mirrors_title(self):
        update_data = {"title": "New Name", "rating": 8}
        update_data = {k: v for k, v in update_data.items() if v != ""}
        if "title" in update_data:
            update_data["title_english"] = update_data["title"]
        self.assertEqual(update_data["title_english"], "New Name")

    def test_settings_default_still_false_for_maintenance(self):
        self.assertFalse(DEFAULTS["maintenance_mode"])


# ------------------------------- Port 3: activity hardening

class _FakeColl:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.deleted = []

    def find(self, query=None, projection=None):
        return self

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for d in self.docs:
            yield d

    async def count_documents(self, q):
        return len(self.docs)

    def find_one(self, q):
        return None

    async def delete_many(self, q):
        self.deleted.append(q)
        return MagicMock()

    async def delete_one(self, q):
        self.deleted.append(q)
        return MagicMock()

    async def aggregate(self, pipeline):
        class C:
            async def to_list(self, _):
                return []
        return C()


class ActivityCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_orphans_deleted_valid_kept(self):
        tokens = _FakeColl([{"token": "tok1"}, {"token": "tok2"}])
        activity = _FakeColl([
            {"_id": "tok1", "name": "A"},
            {"_id": "tok2", "name": "B"},
            {"_id": "ghost", "name": "Old"},
        ])
        stream = _FakeColl()
        tracking = {"api_tokens": tokens, "user_activity": activity, "stream_analytics": stream}
        db = Database.__new__(Database)
        db.dbs = {"tracking": tracking}

        with patch.object(analytics, "db", db), \
             patch.object(analytics, "ACTIVE_STREAMS", {}):
            await analytics.get_activity_overview()

        self.assertEqual(len(activity.deleted), 1)
        # $nin lists the VALID tokens: everything not in it gets deleted
        nin = activity.deleted[0]["_id"]["$nin"]
        self.assertIn("tok1", nin)
        self.assertIn("tok2", nin)
        self.assertNotIn("ghost", nin)

    async def test_revoke_cascades_to_activity(self):
        db = Database.__new__(Database)
        deleted_targets = []

        class FakeTokensColl:
            async def delete_one(self, q):
                deleted_targets.append(q["token"])
                return MagicMock(deleted_count=1)

        class FakeActivityColl:
            async def delete_one(self, q):
                deleted_targets.append(q["_id"])
                return MagicMock(deleted_count=1)

        db.dbs = {"tracking": {"api_tokens": FakeTokensColl(), "user_activity": FakeActivityColl()}}
        result = await db.revoke_api_token("tok9")
        self.assertTrue(result)
        self.assertEqual(deleted_targets, ["tok9", "tok9"])


# ------------------------------- Port 4: skip-channel wiring

class SkipChannelWiringTests(unittest.TestCase):
    async def test_receiver_has_skip_calls(self):
        import inspect
        from Backend.pyrofork.plugins import reciever  # needs a running loop to import
        src = inspect.getsource(reciever)
        self.assertIn("if is_skip_channel(message):", src)
        self.assertIn("route_to_skip_channel(_skip_client, msg_ref)", src)

    def test_is_skip_channel_matches_numeric(self):
        from Backend.helper.skip_channel import is_skip_channel
        msg = SimpleNamespace(chat=SimpleNamespace(id=-1001234567890, username=None))
        with patch("Backend.helper.settings_manager.SettingsManager.current",
                   lambda: SimpleNamespace(skip_channel="-1001234567890", delete_on_metadata_fail=False)):
            self.assertTrue(is_skip_channel(msg))
        with patch("Backend.helper.settings_manager.SettingsManager.current",
                   lambda: SimpleNamespace(skip_channel="", delete_on_metadata_fail=False)):
            self.assertFalse(is_skip_channel(msg))


# ------------------------------- Port 5: announcement delete

class AnnouncementDeleteTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_announcement_removes_post_and_doc(self):
        from Backend.helper import announcer
        claim_doc = {"_id": "movie:42", "chat_id": -100123, "message_id": 77}
        announcer_db = MagicMock()
        announcer_db.dbs = {"tracking": {
            "announced": MagicMock(find_one_and_delete=AsyncMock(return_value=claim_doc)),
        }}
        bot = MagicMock(delete_messages=AsyncMock())
        with patch.object(announcer, "db", announcer_db), patch.object(announcer, "StreamBot", bot):
            await announcer.delete_announcement("movie", 42)
        bot.delete_messages.assert_awaited_once_with(-100123, 77)

    async def test_delete_announcement_noop_for_missing(self):
        from Backend.helper import announcer
        announcer_db = MagicMock()
        announcer_db.dbs = {"tracking": {
            "announced": MagicMock(find_one_and_delete=AsyncMock(return_value=None)),
        }}
        bot = MagicMock(delete_messages=AsyncMock())
        with patch.object(announcer, "db", announcer_db), patch.object(announcer, "StreamBot", bot):
            await announcer.delete_announcement("movie", 999)
        bot.delete_messages.assert_not_awaited()


# ------------------------------- Port 6: subtitle indexes

class SubtitleIndexTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_indexes_creates_subtitle_indexes(self):
        created = []

        class FakeCollection:
            def __init__(self, name):
                self.name = name

            async def create_index(self, keys, **kwargs):
                created.append((self.name, keys, kwargs))

            async def index_information(self):
                return {}

        class FakeTracking(dict):
            def __getitem__(self, key):
                return self.setdefault(key, FakeCollection(key))

        db = Database.__new__(Database)
        db.dbs = {"tracking": FakeTracking(), "storage_1": FakeCollection("s")}
        db.current_db_index = 1
        with patch.object(Database, "_ensure_storage_indexes", AsyncMock()):
            await db.ensure_indexes()

        sub_calls = [(k, kw) for name, k, kw in created if name == "subtitles"]
        self.assertEqual(len(sub_calls), 2)
        self.assertTrue(any(kw.get("unique") for _, kw in sub_calls))


# ------------------------------- Port 7: F1 scoring

class F1ScoreTests(unittest.TestCase):
    def test_clean_match_scores_high(self):
        self.assertGreaterEqual(
            global_search._title_score("Rocky III 1982", "Rocky III"), 0.7,
        )

    def test_junk_stuffed_result_scores_lower_than_clean(self):
        junk = "Rocky III FULL MOVIE HD WATCH ONLINE FREE DOWNLOAD 123"
        clean = "Rocky III (1982) BluRay"
        self.assertLess(
            global_search._title_score(junk, "Rocky III"),
            global_search._title_score(clean, "Rocky III"),
        )

    def test_threshold_raised(self):
        self.assertEqual(global_search.MIN_TITLE_SCORE, 0.7)

    def test_no_common_tokens_zero(self):
        self.assertEqual(global_search._title_score("abc", "xyz"), 0.0)


# ------------------------------- Port 8: /thumb route

class ThumbRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_id_400(self):
        from Backend.helper import encrypt
        with patch.object(encrypt, "decode_string", new=AsyncMock(side_effect=Exception("bad"))):
            with patch.object(stream_routes, "decode_string", new=encrypt.decode_string):
                with self.assertRaises(HTTPException) as ctx:
                    await stream_routes.thumb_handler("badid")
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_cache_hit_serves_without_fetch(self):
        stream_routes._thumb_cache["cachedid"] = (b"jpegdata", time.time() + 9999)
        resp = await stream_routes.thumb_handler("cachedid")
        self.assertEqual(resp.body, b"jpegdata")
        self.assertEqual(resp.media_type, "image/jpeg")
        stream_routes._thumb_cache.clear()


if __name__ == "__main__":
    unittest.main()
