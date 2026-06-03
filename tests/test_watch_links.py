import unittest

from Backend.helper.watch_links import (
    callback_data_fits,
    telegram_user_display_name,
    watch_callback_data,
)


class WatchLinkHelperTests(unittest.TestCase):
    def test_watch_callback_data_stays_under_telegram_limit_for_generated_ids(self):
        callback_data = watch_callback_data("AbCdEf12")

        self.assertEqual(callback_data, "watch_AbCdEf12")
        self.assertTrue(callback_data_fits(callback_data))

    def test_callback_data_limit_rejects_long_values(self):
        self.assertFalse(callback_data_fits("x" * 65))

    def test_telegram_user_display_name_prefers_full_name(self):
        self.assertEqual(
            telegram_user_display_name("Rohan", "JS", "rohan_js", 123),
            "Rohan JS",
        )

    def test_telegram_user_display_name_falls_back_to_username_and_id(self):
        self.assertEqual(telegram_user_display_name(username="rohan_js", user_id=123), "rohan_js")
        self.assertEqual(telegram_user_display_name(user_id=123), "User 123")
        self.assertEqual(telegram_user_display_name(), "Telegram User")


if __name__ == "__main__":
    unittest.main()
