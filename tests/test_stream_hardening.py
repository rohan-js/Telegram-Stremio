"""Tests: 2026-09-20 DC4 jitter hardening batch.

Covers: hedge-delay default 0.5s, route-failure time decay, pool-extra
ranking by live route score, and stream SLO owner alerts (hedge-storm /
buffering / slow TTFB). See PROJECT_CONTEXT §40.
"""

import time
import unittest
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper import custom_dl
from Backend.helper.custom_dl import maybe_send_stream_slo_alerts, record_route_failure
from Backend.fastapi.routes import stream_routes


class HedgeDefaultTests(unittest.TestCase):
    def test_hedge_delay_default_is_half_second(self):
        # CI dummy env never sets the knob, so the code default rules.
        self.assertEqual(float(Telegram.SMART_ROUTING_HEDGE_DELAY_SEC), 0.5)

    def test_sample_config_documents_half_second(self):
        from pathlib import Path

        sample = Path(__file__).resolve().parents[1] / "sample_config.env"
        text = sample.read_text(encoding="utf-8")
        self.assertIn('SMART_ROUTING_HEDGE_DELAY_SEC="0.5"', text)


class FailureDecayTests(unittest.TestCase):
    def setUp(self):
        self._failures_backup = dict(custom_dl.client_failures)
        self._errors_backup = {k: dict(v) for k, v in custom_dl.client_last_errors.items()}
        self._cooldowns_backup = dict(custom_dl.client_cooldowns)
        self._dc_cooldowns_backup = dict(custom_dl.client_dc_cooldowns)
        custom_dl.client_failures.clear()
        custom_dl.client_last_errors.clear()
        custom_dl.client_cooldowns.clear()
        custom_dl.client_dc_cooldowns.clear()

    def tearDown(self):
        custom_dl.client_failures.clear()
        custom_dl.client_failures.update(self._failures_backup)
        custom_dl.client_last_errors.clear()
        custom_dl.client_last_errors.update(self._errors_backup)
        custom_dl.client_cooldowns.clear()
        custom_dl.client_cooldowns.update(self._cooldowns_backup)
        custom_dl.client_dc_cooldowns.clear()
        custom_dl.client_dc_cooldowns.update(self._dc_cooldowns_backup)

    def _record(self, idx=1, dc=4):
        with patch(
            "Backend.helper.owner_alerts.schedule_owner_alert", return_value=False
        ):
            record_route_failure(idx, dc, "fallback_needed", stream_id="t", offset=0, attempt=1)

    def test_stale_failure_starts_fresh_incident(self):
        custom_dl.client_failures[1] = 247  # yesterday's storm
        custom_dl.client_last_errors[1] = {"reason": "x", "target_dc": 4, "ts": time.time() - 7200}
        with patch.object(Telegram, "SMART_ROUTING_FAILURE_DECAY_SEC", 1800):
            self._record()
        self.assertEqual(custom_dl.client_failures[1], 1)

    def test_recent_failure_keeps_escalating(self):
        custom_dl.client_failures[1] = 5
        custom_dl.client_last_errors[1] = {"reason": "x", "target_dc": 4, "ts": time.time() - 10}
        with patch.object(Telegram, "SMART_ROUTING_FAILURE_DECAY_SEC", 1800):
            self._record()
        self.assertEqual(custom_dl.client_failures[1], 6)

    def test_decay_zero_disables_reset(self):
        custom_dl.client_failures[1] = 40
        custom_dl.client_last_errors[1] = {"reason": "x", "target_dc": 4, "ts": time.time() - 99999}
        with patch.object(Telegram, "SMART_ROUTING_FAILURE_DECAY_SEC", 0):
            self._record()
        self.assertEqual(custom_dl.client_failures[1], 41)

    def test_first_failure_counts_one(self):
        with patch.object(Telegram, "SMART_ROUTING_FAILURE_DECAY_SEC", 1800):
            self._record(idx=3, dc=4)
        self.assertEqual(custom_dl.client_failures[3], 1)


class PoolRankTests(unittest.TestCase):
    def test_extras_ranked_by_route_score_not_workload(self):
        # Client 2 has the lowest workload but the worst route score;
        # it must sort last once scores rule.
        scores = {0: (0, 0, 0, 0.4, -1.2, 0, 0), 2: (0, 0, 0, 1.0, -0.5, 0, 2), 4: (0, 0, 0, 0.3, -1.5, 0, 4)}
        with patch.object(stream_routes, "smart_client_score", side_effect=lambda idx, dc: scores[idx]):
            ranked = stream_routes.rank_pool_extras([0, 2, 4], 4)
        self.assertEqual(ranked, [4, 0, 2])

    def test_rank_falls_back_to_input_order_on_error(self):
        with patch.object(stream_routes, "smart_client_score", side_effect=RuntimeError("boom")):
            self.assertEqual(stream_routes.rank_pool_extras([0, 2], 4), [0, 2])


class SLOAlertTests(unittest.TestCase):
    def _entry(self, **over):
        base = {
            "stream_id": "slo-test",
            "client_index": 1,
            "dc_id": 4,
            "title": "Ponman",
            "ttfb_sec": 0.5,
            "buffering_rate": 0.0,
            "hedge_rescues": 0,
        }
        base.update(over)
        return base

    def _run(self, entry):
        sent = []

        def fake_alert(message, *, key=None, cooldown_sec=0):
            sent.append((message, key, cooldown_sec))
            return True

        with (
            patch.object(Telegram, "STREAM_SLO_HEDGE_WARN_COUNT", 5),
            patch.object(Telegram, "STREAM_SLO_BUFFERING_WARN_RATE", 0.05),
            patch.object(Telegram, "STREAM_SLO_TTFB_WARN_SEC", 3.0),
            patch.object(Telegram, "STREAM_SLO_ALERT_COOLDOWN_SEC", 3600),
            patch("Backend.helper.owner_alerts.schedule_owner_alert", side_effect=fake_alert),
        ):
            maybe_send_stream_slo_alerts(entry)
        return sent

    def test_hedge_storm_alerts_per_dc_key(self):
        sent = self._run(self._entry(hedge_rescues=15))
        self.assertEqual(len(sent), 1)
        msg, key, cooldown = sent[0]
        self.assertEqual(key, "slo-hedge:dc4")
        self.assertIn("15", msg)
        self.assertEqual(cooldown, 3600)

    def test_hedges_below_threshold_stay_silent(self):
        self.assertEqual(self._run(self._entry(hedge_rescues=4)), [])

    def test_buffering_rate_breach_alerts(self):
        sent = self._run(self._entry(buffering_rate=0.08))
        self.assertTrue(any(key == "slo-buffering:dc4" for _, key, _ in sent))

    def test_slow_ttfb_alerts(self):
        sent = self._run(self._entry(ttfb_sec=3.78))
        self.assertTrue(any(key == "slo-ttfb:dc4" for _, key, _ in sent))

    def test_healthy_stream_sends_nothing(self):
        self.assertEqual(self._run(self._entry()), [])


if __name__ == "__main__":
    unittest.main()
