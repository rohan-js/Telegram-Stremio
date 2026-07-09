import asyncio
import unittest

from bson import ObjectId

from Backend.config import Telegram
from Backend.fastapi.routes.stremio_routes import _token_can_view
from Backend.helper import global_search
from Backend.helper.log_tools import redact_log_text
from Backend.helper.manual_add import parse_telegram_link
from Backend.helper.passwords import hash_password, is_hashed, verify_password
from Backend.helper import requests_manager
from Backend.helper.settings_manager import Settings, SettingsManager
from Backend.helper.subtitles import detect_language, is_subtitle_file, subtitle_ext


class FakeSettingsDB:
    def __init__(self):
        self.saved = None

    async def save_settings(self, settings):
        self.saved = dict(settings)
        return True


class FakeRequestCursor:
    def __init__(self, docs):
        self.docs = [dict(doc) for doc in docs]

    def sort(self, *_args):
        self.docs.sort(key=lambda item: item.get("last_requested_at"), reverse=True)
        return self

    def limit(self, value):
        self.docs = self.docs[:value]
        return self

    async def to_list(self, _length):
        return [dict(doc) for doc in self.docs]


class FakeRequestResult:
    def __init__(self, deleted_count=0):
        self.deleted_count = deleted_count


class FakeRequestCollection:
    def __init__(self):
        self.docs = []

    def _matches(self, doc, query):
        if query.get("media_type") and doc.get("media_type") != query.get("media_type"):
            return False
        ors = query.get("$or") or []
        if ors:
            return any(all(doc.get(key) == value for key, value in item.items()) for item in ors)
        if query.get("_id") is not None and doc.get("_id") != query.get("_id"):
            return False
        return True

    async def find_one(self, query):
        return next((doc for doc in self.docs if self._matches(doc, query)), None)

    async def insert_one(self, doc):
        doc = dict(doc)
        doc["_id"] = ObjectId()
        self.docs.append(doc)
        return object()

    async def update_one(self, query, update):
        doc = await self.find_one(query)
        if not doc:
            return object()
        for key, value in (update.get("$set") or {}).items():
            doc[key] = value
        for key, value in (update.get("$addToSet") or {}).items():
            doc.setdefault(key, [])
            if value not in doc[key]:
                doc[key].append(value)
        return object()

    async def update_many(self, query, update):
        for doc in self.docs:
            if self._matches(doc, query) and doc.get("status") != "banned":
                for key, value in (update.get("$set") or {}).items():
                    doc[key] = value
        return object()

    def find(self, query):
        return FakeRequestCursor([doc for doc in self.docs if self._matches(doc, query)])

    async def find_one_and_update(self, query, update, return_document=None):
        doc = await self.find_one(query)
        if not doc:
            return None
        for key, value in (update.get("$set") or {}).items():
            doc[key] = value
        return doc

    async def delete_one(self, query):
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not self._matches(doc, query)]
        return FakeRequestResult(before - len(self.docs))


class FakeRequestDB:
    def __init__(self, collection):
        self.dbs = {"tracking": {"requests": collection}}
        self.current_db_index = 1

    async def get_media_details(self, imdb_id=None):
        return None

    async def get_document(self, media_type, tmdb_id, db_index):
        return None

    async def search_documents(self, query, page=1, page_size=8):
        return {"results": []}


class UpstreamBackportHelperTests(unittest.TestCase):
    def setUp(self):
        self._settings = SettingsManager._current
        self._admin_username = Telegram.ADMIN_USERNAME
        self._session_secret = Telegram.SESSION_SECRET
        self._auth_channels = list(Telegram.AUTH_CHANNEL)
        self._requests_enabled = getattr(Telegram, "CONTENT_REQUESTS_ENABLED", False)
        self._announce_enabled = getattr(Telegram, "ANNOUNCE_NEW_CONTENT", False)
        self._global_search_userbot = global_search.Userbot
        self._requests_db = requests_manager.db
        self._rate_bucket = dict(requests_manager._RATE_BUCKET)

    def tearDown(self):
        SettingsManager._current = self._settings
        Telegram.ADMIN_USERNAME = self._admin_username
        Telegram.SESSION_SECRET = self._session_secret
        Telegram.AUTH_CHANNEL = self._auth_channels
        Telegram.CONTENT_REQUESTS_ENABLED = self._requests_enabled
        Telegram.ANNOUNCE_NEW_CONTENT = self._announce_enabled
        global_search.Userbot = self._global_search_userbot
        requests_manager.db = self._requests_db
        requests_manager._RATE_BUCKET.clear()
        requests_manager._RATE_BUCKET.update(self._rate_bucket)

    def test_password_hash_verifies_and_plain_legacy_still_works(self):
        stored = hash_password("secret")

        self.assertTrue(is_hashed(stored))
        self.assertTrue(verify_password("secret", stored))
        self.assertFalse(verify_password("wrong", stored))
        self.assertTrue(verify_password("legacy", "legacy"))
        self.assertFalse(verify_password("wrong", "legacy"))

    def test_blank_settings_password_update_keeps_existing_hash(self):
        existing_hash = hash_password("old-password")
        SettingsManager._current = Settings(
            {
                "admin_username": "admin",
                "admin_password": existing_hash,
                "session_secret": "session-secret",
                "auth_channels": [],
            }
        )
        fake_db = FakeSettingsDB()

        asyncio.run(SettingsManager.update(fake_db, {"admin_password": ""}))

        self.assertEqual(fake_db.saved["admin_password"], existing_hash)

    def test_settings_password_update_hashes_new_plain_password(self):
        SettingsManager._current = Settings(
            {
                "admin_username": "admin",
                "admin_password": hash_password("old-password"),
                "session_secret": "session-secret",
                "auth_channels": [],
            }
        )
        fake_db = FakeSettingsDB()

        asyncio.run(SettingsManager.update(fake_db, {"admin_password": "new-password"}))

        self.assertTrue(is_hashed(fake_db.saved["admin_password"]))
        self.assertTrue(verify_password("new-password", fake_db.saved["admin_password"]))

    def test_global_search_disabled_without_userbot(self):
        SettingsManager._current = Settings(
            {
                "global_search": True,
                "global_search_channels": ["123456789"],
            }
        )
        global_search.Userbot = None

        self.assertFalse(global_search.is_global_search_enabled())

    def test_parse_private_and_public_telegram_links(self):
        self.assertEqual(
            parse_telegram_link("https://t.me/c/123456789/42"),
            (-100123456789, 42),
        )
        self.assertEqual(
            parse_telegram_link("https://t.me/example_channel/99"),
            ("example_channel", 99),
        )

    def test_settings_apply_request_and_announcement_toggles(self):
        SettingsManager.apply_to_runtime(Settings({
            "content_requests_enabled": True,
            "content_requests_beta_only": False,
            "announce_new_content": True,
            "announcement_channel": "-100123",
        }))

        self.assertTrue(Telegram.CONTENT_REQUESTS_ENABLED)
        self.assertFalse(Telegram.CONTENT_REQUESTS_BETA_ONLY)
        self.assertTrue(Telegram.ANNOUNCE_NEW_CONTENT)
        self.assertEqual(Telegram.ANNOUNCEMENT_CHANNEL, "-100123")

    def test_subtitle_helpers_detect_extension_and_language(self):
        self.assertTrue(is_subtitle_file("Movie.2024.Hindi.srt"))
        self.assertEqual(subtitle_ext("episode.ass"), ".ass")
        self.assertEqual(detect_language("Movie.2024.English.srt"), ("eng", "English"))
        self.assertEqual(detect_language("Movie.2024.unknown.srt"), ("und", "Unknown"))

    def test_visibility_helper_allows_public_and_token_restricted(self):
        self.assertTrue(_token_can_view("public", [], {"token": "abc"}))
        self.assertTrue(_token_can_view("tokens", ["abc"], {"token": "abc"}))
        self.assertFalse(_token_can_view("tokens", ["abc"], {"token": "def"}))
        self.assertFalse(_token_can_view("owner", ["abc"], {"token": "abc"}))

    def test_log_redaction_masks_tokens_and_mongo_uri(self):
        text = "BOT_TOKEN=1234567890:ABCdefghijklmnopqrstuvwxyz123456 mongodb+srv://user:pass@example/db"
        redacted = redact_log_text(text)
        self.assertNotIn("ABCdef", redacted)
        self.assertNotIn("mongodb+srv://user:pass@example/db", redacted)

    def test_content_requests_aggregate_duplicates_and_status(self):
        collection = FakeRequestCollection()
        requests_manager.db = FakeRequestDB(collection)
        requests_manager._RATE_BUCKET.clear()
        SettingsManager._current = Settings({"content_requests_enabled": True})

        first = asyncio.run(requests_manager.submit_request(
            media_type="movie",
            tmdb_id=123,
            imdb_id="tt1234567",
            title="Example Movie",
            year=2026,
            poster="",
            client_ip="1.1.1.1",
        ))
        second = asyncio.run(requests_manager.submit_request(
            media_type="movie",
            tmdb_id=123,
            imdb_id="tt1234567",
            title="Example Movie",
            year=2026,
            poster="",
            client_ip="2.2.2.2",
        ))
        popular = asyncio.run(requests_manager.popular_requests())

        self.assertEqual(first["reason"], "created")
        self.assertEqual(second["reason"], "added")
        self.assertEqual(popular[0]["request_count"], 2)
        self.assertNotIn("requesters", popular[0])

        request_id = str(collection.docs[0]["_id"])
        updated = asyncio.run(requests_manager.set_status(request_id, "uploaded"))
        self.assertEqual(updated["status"], "uploaded")


if __name__ == "__main__":
    unittest.main()
