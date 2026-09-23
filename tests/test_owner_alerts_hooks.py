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

    # ---- Layer 2: memory/load freeze early-warning ----

    async def test_critical_memory_alert(self):
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": 90}),
            patch.object(pops, "_loadavg_1m", lambda: 0.5),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append((m, k.get("key")))),
        ):
            await pops._check_load_memory()

        self.assertEqual(len(sent), 1)
        self.assertIn("CRITICAL memory", sent[0][0])
        self.assertEqual(sent[0][1], "mem-critical")

    async def test_warn_memory_alert_below_200_not_critical(self):
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": 150}),
            patch.object(pops, "_loadavg_1m", lambda: 0.5),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append((m, k.get("key")))),
        ):
            await pops._check_load_memory()

        self.assertEqual(len(sent), 1)
        self.assertIn("Memory low", sent[0][0])
        self.assertEqual(sent[0][1], "mem-warn")

    async def test_high_load_alert(self):
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": 500}),
            patch.object(pops, "_loadavg_1m", lambda: 3.4),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append((m, k.get("key")))),
        ):
            await pops._check_load_memory()

        self.assertEqual(len(sent), 1)
        self.assertIn("High load", sent[0][0])
        self.assertEqual(sent[0][1], "load-high")

    async def test_critical_and_load_both_fire(self):
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": 80}),
            patch.object(pops, "_loadavg_1m", lambda: 4.0),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append((m, k.get("key")))),
        ):
            await pops._check_load_memory()

        keys = [k for _, k in sent]
        self.assertIn("mem-critical", keys)
        self.assertIn("load-high", keys)
        # critical must suppress the warn-tier alert (no double DM)
        self.assertNotIn("mem-warn", keys)

    async def test_healthy_box_silent(self):
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": 450}),
            patch.object(pops, "_loadavg_1m", lambda: 0.4),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append(m)),
        ):
            await pops._check_load_memory()

        self.assertEqual(sent, [])

    async def test_uncheckable_proc_silent(self):
        # Windows dev boxes / missing /proc: None values must not alert.
        sent = []

        with (
            patch.object(pops, "_meminfo", lambda: {"available_mb": None}),
            patch.object(pops, "_loadavg_1m", lambda: None),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=lambda m, **k: sent.append(m)),
        ):
            await pops._check_load_memory()

        self.assertEqual(sent, [])

    def test_watch_interval_defaults_to_5min(self):
        from Backend.config import Telegram

        with patch.object(Telegram, "OPS_WATCH_INTERVAL_MIN", 5):
            self.assertEqual(pops._ops_watch_interval_sec(), 300)

    def test_watch_interval_env_override(self):
        from Backend.config import Telegram

        with patch.object(Telegram, "OPS_WATCH_INTERVAL_MIN", 1):
            self.assertEqual(pops._ops_watch_interval_sec(), 60)


if __name__ == "__main__":
    unittest.main()
