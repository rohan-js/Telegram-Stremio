import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from fastapi import HTTPException

from Backend.config import Telegram
from Backend.fastapi.security import tokens as token_security


class _TokenDB:
    def __init__(self, token=None, user=None):
        self.token = token
        self.user = user

    async def get_api_token(self, _token):
        return dict(self.token) if self.token else None

    async def get_user(self, _user_id):
        return dict(self.user) if self.user else None


def _token(**overrides):
    value = {
        "token": "abc",
        "name": "normal",
        "user_id": 100,
        "limits": {"daily_limit_gb": 25, "monthly_limit_gb": 300, "max_active_streams": 2},
        "usage": {
            "daily": {"bytes": 0},
            "monthly": {"bytes": 0},
        },
    }
    value.update(overrides)
    return value


class TokenAccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.subscription = Telegram.SUBSCRIPTION
        self.owner_id = Telegram.OWNER_ID
        self.exempt_names = list(Telegram.BETA_EXEMPT_TOKEN_NAMES)
        self.exempt_tokens = list(Telegram.BETA_EXEMPT_TOKENS)
        self.exempt_users = list(Telegram.BETA_EXEMPT_USER_IDS)
        Telegram.SUBSCRIPTION = True
        Telegram.OWNER_ID = 999
        Telegram.BETA_EXEMPT_TOKEN_NAMES = ["autotest-temp"]
        Telegram.BETA_EXEMPT_TOKENS = []
        Telegram.BETA_EXEMPT_USER_IDS = []

    async def asyncTearDown(self):
        Telegram.SUBSCRIPTION = self.subscription
        Telegram.OWNER_ID = self.owner_id
        Telegram.BETA_EXEMPT_TOKEN_NAMES = self.exempt_names
        Telegram.BETA_EXEMPT_TOKENS = self.exempt_tokens
        Telegram.BETA_EXEMPT_USER_IDS = self.exempt_users

    async def verify(self, token, user=None, active=(0, 0)):
        with patch.object(token_security, "db", _TokenDB(token, user)), patch.object(
            token_security, "_active_stream_counts", return_value=active
        ):
            return await token_security.verify_token("abc")

    async def test_invalid_token_is_rejected(self):
        with patch.object(token_security, "db", _TokenDB()):
            with self.assertRaises(HTTPException):
                await token_security.verify_token("missing")

    async def test_autotest_temp_remains_free_and_permanent(self):
        result = await self.verify(_token(name="autotest-temp", expires_at=datetime.utcnow() - timedelta(days=1)))
        self.assertTrue(result["is_beta_exempt"])
        self.assertFalse(result["subscription_expired"])
        self.assertEqual(result["access_source"], "internal_exemption")

    async def test_owner_admin_and_lifetime_bypass_subscription_only(self):
        owner = await self.verify(_token(user_id=999))
        lifetime = await self.verify(_token(subscription_exempt=True))
        self.assertEqual(owner["access_source"], "admin")
        self.assertEqual(lifetime["access_source"], "lifetime")

    async def test_future_independent_expiry_grants_access(self):
        result = await self.verify(_token(expires_at=datetime.utcnow() + timedelta(days=5)))
        self.assertFalse(result["subscription_expired"])
        self.assertEqual(result["access_source"], "token_expiry")

    async def test_expired_independent_expiry_is_enforced(self):
        active_user = {
            "subscription_status": "active",
            "subscription_expiry": datetime.utcnow() + timedelta(days=30),
        }
        result = await self.verify(_token(expires_at=datetime.utcnow() - timedelta(seconds=1)), active_user)
        self.assertTrue(result["subscription_expired"])
        self.assertEqual(result["access_source"], "expired_token")

    async def test_active_and_expired_linked_subscriptions(self):
        active = await self.verify(_token(), {
            "subscription_status": "active",
            "subscription_expiry": datetime.utcnow() + timedelta(days=1),
        })
        expired = await self.verify(_token(), {
            "subscription_status": "active",
            "subscription_expiry": datetime.utcnow() - timedelta(days=1),
        })
        self.assertEqual(active["access_source"], "subscription")
        self.assertFalse(active["subscription_expired"])
        self.assertTrue(expired["subscription_expired"])

    async def test_open_mode_allows_normal_token(self):
        Telegram.SUBSCRIPTION = False
        result = await self.verify(_token())
        self.assertEqual(result["access_source"], "open_mode")
        self.assertFalse(result["subscription_expired"])

    async def test_limits_still_apply_after_lifetime_grant(self):
        token = _token(
            subscription_exempt=True,
            limits={"daily_limit_gb": 1, "monthly_limit_gb": 300, "max_active_streams": 2},
            usage={"daily": {"bytes": 2 * 1024**3}, "monthly": {"bytes": 2 * 1024**3}},
        )
        result = await self.verify(token)
        self.assertEqual(result["limit_exceeded"], "daily")

    async def test_active_stream_limit_still_applies(self):
        result = await self.verify(_token(subscription_exempt=True), active=(2, 2))
        self.assertEqual(result["limit_exceeded"], "active_streams")


if __name__ == "__main__":
    unittest.main()
