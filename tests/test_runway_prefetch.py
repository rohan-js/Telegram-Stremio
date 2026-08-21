"""Tests for runway-aware adaptive prefetch (compute_runway_prefetch + feed capacity)."""

import unittest
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper.custom_dl import compute_runway_prefetch
from Backend.fastapi.routes import stream_routes


class ComputeRunwayPrefetchTests(unittest.TestCase):
    def test_starve_boosts_to_cap(self):
        # bitrate 2 MB/s, feed only 1 MB/s → boost from 3 toward cap
        new_eff, reason = compute_runway_prefetch(base=3, current=3, cap=5, bitrate_bps=2_000_000, feed_bps=1_000_000)
        self.assertEqual(new_eff, 5)
        self.assertEqual(reason, "boost")

    def test_boost_capped_by_parallelism(self):
        new_eff, reason = compute_runway_prefetch(base=4, current=4, cap=5, bitrate_bps=2_000_000, feed_bps=1_000_000)
        self.assertEqual(new_eff, 5)
        self.assertEqual(reason, "boost")
        # already at cap → stays
        new_eff, reason = compute_runway_prefetch(base=5, current=5, cap=5, bitrate_bps=2_000_000, feed_bps=100_000)
        self.assertEqual(new_eff, 5)
        self.assertEqual(reason, "boost")

    def test_relax_returns_to_base(self):
        new_eff, reason = compute_runway_prefetch(base=3, current=5, cap=5, bitrate_bps=1_000_000, feed_bps=4_000_000)
        self.assertEqual(new_eff, 3)
        self.assertEqual(reason, "relax")

    def test_hold_between_ratios(self):
        new_eff, reason = compute_runway_prefetch(base=3, current=3, cap=5, bitrate_bps=1_000_000, feed_bps=1_500_000)
        self.assertEqual(new_eff, 3)
        self.assertEqual(reason, "hold")

    def test_invalid_inputs_hold(self):
        self.assertEqual(compute_runway_prefetch(3, 3, 5, 0, 1_000_000), (3, "hold"))
        self.assertEqual(compute_runway_prefetch(3, 3, 5, 1_000_000, 0), (3, "hold"))
        self.assertEqual(compute_runway_prefetch(3, 3, 1, 1_000_000, 1_000_000), (3, "hold"))

    def test_custom_ratios(self):
        # starve ratio 1.5 → starve threshold 1.5 MB/s: 1.2 MB/s starves,
        # 1.6 MB/s sits between starve (1.5) and relax (2.0) → hold
        self.assertEqual(
            compute_runway_prefetch(3, 3, 6, 1_000_000, 1_200_000, starve_ratio=1.5)[1], "boost"
        )
        self.assertEqual(
            compute_runway_prefetch(3, 3, 6, 1_000_000, 1_600_000, starve_ratio=1.5)[1], "hold"
        )

    def test_flag_disabled_via_zero_cap(self):
        # cap of 1 means no headroom → hold regardless
        self.assertEqual(compute_runway_prefetch(1, 1, 1, 5_000_000, 100_000), (1, "hold"))


class FeedCapacityTests(unittest.TestCase):
    def test_dc_specific_wins_over_global(self):
        saved_dc = dict(stream_routes.client_dc_avg_mbps)
        saved_g = dict(stream_routes.client_avg_mbps)
        try:
            stream_routes.client_dc_avg_mbps.clear()
            stream_routes.client_dc_avg_mbps[(0, 5)] = 4.0
            stream_routes.client_dc_avg_mbps[(1, 4)] = 9.0
            stream_routes.client_avg_mbps.clear()
            stream_routes.client_avg_mbps[2] = 12.0
            # DC5 → 4 MiB/s (dc-specific), not the higher global 12
            self.assertEqual(stream_routes._dc_feed_capacity_bps(5), 4.0 * 1024 * 1024)
            # unknown DC → global best
            self.assertEqual(stream_routes._dc_feed_capacity_bps(2), 12.0 * 1024 * 1024)
        finally:
            stream_routes.client_dc_avg_mbps.clear()
            stream_routes.client_dc_avg_mbps.update(saved_dc)
            stream_routes.client_avg_mbps.clear()
            stream_routes.client_avg_mbps.update(saved_g)

    def test_no_data_returns_none(self):
        saved_dc = dict(stream_routes.client_dc_avg_mbps)
        saved_g = dict(stream_routes.client_avg_mbps)
        try:
            stream_routes.client_dc_avg_mbps.clear()
            stream_routes.client_avg_mbps.clear()
            self.assertIsNone(stream_routes._dc_feed_capacity_bps(5))
        finally:
            stream_routes.client_dc_avg_mbps.update(saved_dc)
            stream_routes.client_avg_mbps.update(saved_g)


if __name__ == "__main__":
    unittest.main()
