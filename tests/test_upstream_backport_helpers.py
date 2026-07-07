import asyncio
import unittest

from Backend.config import Telegram
from Backend.helper import global_search
from Backend.helper.manual_add import parse_telegram_link
from Backend.helper.passwords import hash_password, is_hashed, verify_password
from Backend.helper.settings_manager import Settings, SettingsManager


class FakeSettingsDB:
    def __init__(self):
        self.saved = None

    async def save_settings(self, settings):
        self.saved = dict(settings)
        return True


class UpstreamBackportHelperTests(unittest.TestCase):
    def setUp(self):
        self._settings = SettingsManager._current
        self._admin_username = Telegram.ADMIN_USERNAME
        self._session_secret = Telegram.SESSION_SECRET
        self._auth_channels = list(Telegram.AUTH_CHANNEL)
        self._global_search_userbot = global_search.Userbot

    def tearDown(self):
        SettingsManager._current = self._settings
        Telegram.ADMIN_USERNAME = self._admin_username
        Telegram.SESSION_SECRET = self._session_secret
        Telegram.AUTH_CHANNEL = self._auth_channels
        global_search.Userbot = self._global_search_userbot

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


if __name__ == "__main__":
    unittest.main()
