"""Tests: new owner-alert hooks — ops watch loop (disk/TLS) behavior."""

import unittest
from unittest.mock import patch

from Backend.helper import production_ops as pops


class OpsWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_low_disk_alerts_with_key(self):
        sent = []

        def fake_alert(message, *, key=None, cooldown_sec=0):
            sent.append((message, key))

        with (
            patch.object(pops, "_diskinfo", lambda path: {
                "total_gb": 40.0, "free_gb": 3.0, "used_percent": 92.5,
            }),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=fake_alert),
        ):
            await pops._check_disk_paths()

        self.assertTrue(any("disk-low:" in (key or "") for _, key in sent))

    async def test_healthy_disk_no_alert(self):
        sent = []

        with (
            patch.object(pops, "_diskinfo", lambda path: {
                "total_gb": 40.0, "free_gb": 25.0, "used_percent": 37.5,
            }),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append(m)),
        ):
            await pops._check_disk_paths()

        self.assertEqual(sent, [])

    async def test_tls_none_when_base_url_unset(self):
        from Backend.config import Telegram

        with patch.object(Telegram, "BASE_URL", ""):
            self.assertIsNone(pops._tls_expiry_days())

    async def test_tls_alert_fires_when_expiring(self):
        sent = []

        with (
            patch.object(pops, "_tls_expiry_days", lambda: 3.5),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append(m)),
        ):
            await pops._check_tls_expiry()

        self.assertEqual(len(sent), 1)
        self.assertIn("TLS", sent[0])

    async def test_tls_silent_when_healthy(self):
        sent = []

        with (
            patch.object(pops, "_tls_expiry_days", lambda: 45.0),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append(m)),
        ):
            await pops._check_tls_expiry()

        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
