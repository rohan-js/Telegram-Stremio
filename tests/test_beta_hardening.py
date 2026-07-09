import asyncio
import unittest

from Backend.config import Telegram
from Backend.fastapi.security import tokens as token_security
from Backend.helper import beta_access
from Backend.helper.custom_dl import ACTIVE_STREAMS


class BetaHardeningTests(unittest.TestCase):
    def setUp(self):
        self._orig_beta = Telegram.PUBLIC_BETA_ENABLED
        self._orig_terms = Telegram.REQUIRE_TERMS_ACCEPTANCE
        self._orig_terms_version = Telegram.TERMS_VERSION
        self._orig_exempt_names = list(Telegram.BETA_EXEMPT_TOKEN_NAMES)
        self._orig_default_daily = Telegram.DEFAULT_TOKEN_DAILY_LIMIT_GB
        self._orig_default_monthly = Telegram.DEFAULT_TOKEN_MONTHLY_LIMIT_GB
        self._orig_default_active = Telegram.DEFAULT_TOKEN_MAX_ACTIVE_STREAMS
        self._orig_global_active = Telegram.MAX_ACTIVE_STREAMS_GLOBAL
        self._orig_subscription = Telegram.SUBSCRIPTION
        self._orig_db = token_security.db
        ACTIVE_STREAMS.clear()

    def tearDown(self):
        Telegram.PUBLIC_BETA_ENABLED = self._orig_beta
        Telegram.REQUIRE_TERMS_ACCEPTANCE = self._orig_terms
        Telegram.TERMS_VERSION = self._orig_terms_version
        Telegram.BETA_EXEMPT_TOKEN_NAMES = self._orig_exempt_names
        Telegram.DEFAULT_TOKEN_DAILY_LIMIT_GB = self._orig_default_daily
        Telegram.DEFAULT_TOKEN_MONTHLY_LIMIT_GB = self._orig_default_monthly
        Telegram.DEFAULT_TOKEN_MAX_ACTIVE_STREAMS = self._orig_default_active
        Telegram.MAX_ACTIVE_STREAMS_GLOBAL = self._orig_global_active
        Telegram.SUBSCRIPTION = self._orig_subscription
        token_security.db = self._orig_db
        ACTIVE_STREAMS.clear()

    def test_autotest_temp_token_is_exempt_by_default(self):
        Telegram.BETA_EXEMPT_TOKEN_NAMES = ["autotest-temp"]
        self.assertTrue(beta_access.is_exempt_token({"name": "autotest-temp", "token": "abc"}))
        self.assertTrue(beta_access.is_exempt_token({"name": "AUTOTEST-TEMP", "token": "abc"}))

    def test_terms_acceptance_requires_current_version(self):
        Telegram.PUBLIC_BETA_ENABLED = True
        Telegram.REQUIRE_TERMS_ACCEPTANCE = True
        Telegram.TERMS_VERSION = "v2"
        self.assertFalse(beta_access.accepted_terms({"terms": {"version": "v1", "accepted_at": "now"}}))
        self.assertTrue(beta_access.accepted_terms({"terms": {"version": "v2", "accepted_at": "now"}}))

    def test_default_token_limits(self):
        Telegram.DEFAULT_TOKEN_DAILY_LIMIT_GB = 25
        Telegram.DEFAULT_TOKEN_MONTHLY_LIMIT_GB = 300
        Telegram.DEFAULT_TOKEN_MAX_ACTIVE_STREAMS = 2
        self.assertEqual(beta_access.default_token_limits(), (25.0, 300.0, 2))

    def test_verify_token_exempt_bypasses_subscription_and_limits(self):
        class FakeDb:
            async def get_api_token(self, token):
                return {
                    "token": token,
                    "name": "autotest-temp",
                    "limits": {"daily_limit_gb": 1, "monthly_limit_gb": 1, "max_active_streams": 1},
                    "usage": {"daily": {"bytes": 99 * 1024**3}, "monthly": {"bytes": 99 * 1024**3}},
                }

        Telegram.SUBSCRIPTION = True
        token_security.db = FakeDb()
        data = asyncio.run(token_security.verify_token("free-token"))
        self.assertTrue(data["is_beta_exempt"])
        self.assertFalse(data["subscription_expired"])
        self.assertIsNone(data["limit_exceeded"])

    def test_verify_token_sets_active_stream_limit(self):
        class FakeDb:
            async def get_api_token(self, token):
                return {
                    "token": token,
                    "name": "paid-user",
                    "limits": {"daily_limit_gb": 0, "monthly_limit_gb": 0, "max_active_streams": 1},
                    "usage": {"daily": {"bytes": 0}, "monthly": {"bytes": 0}},
                }

        Telegram.SUBSCRIPTION = False
        Telegram.MAX_ACTIVE_STREAMS_GLOBAL = 10
        token_security.db = FakeDb()
        ACTIVE_STREAMS["s1"] = {"status": "active", "meta": {"token": "paid-token"}}
        data = asyncio.run(token_security.verify_token("paid-token"))
        self.assertEqual(data["limit_exceeded"], "active_streams")
        self.assertEqual(data["active_streams_current"], 1)


if __name__ == "__main__":
    unittest.main()
