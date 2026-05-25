import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("DATABASE", "mongodb://tracking,mongodb://storage")

from Backend.helper.host_outbound import (
    build_vps_outbound_sample,
    get_vps_outbound_summary,
    parse_proc_net_dev,
    read_interface_tx_bytes,
    read_tx_bytes_counter,
)


class HostOutboundTests(unittest.TestCase):
    def test_parse_proc_net_dev_reads_interface_tx_bytes(self):
        text = """
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  ens3: 100 1 0 0 0 0 0 0 900 2 0 0 0 0 0 0
"""
        counters = parse_proc_net_dev(text)

        self.assertEqual(counters["ens3"]["rx_bytes"], 100)
        self.assertEqual(counters["ens3"]["tx_bytes"], 900)

    def test_read_interface_tx_bytes_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dev"
            path.write_text("ens3: 1 0 0 0 0 0 0 0 12345 0 0 0 0 0 0 0\n", encoding="utf-8")

            self.assertEqual(read_interface_tx_bytes("ens3", path), 12345)
            self.assertIsNone(read_interface_tx_bytes("eth0", path))

    def test_read_tx_bytes_counter_from_sysfs_style_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tx_bytes"
            path.write_text("54321\n", encoding="utf-8")

            self.assertEqual(read_tx_bytes_counter(path), 54321)

    def test_build_vps_outbound_sample_computes_delta(self):
        existing = {
            "last_tx_bytes": 1000,
            "today": {"date": "2026-05-25", "bytes": 200},
            "month": {"month": "2026-05", "bytes": 300, "limit_bytes": 1000, "percent": 30},
            "total": {"bytes": 500},
            "tracking_started_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
            "reset_count": 0,
        }

        sample = build_vps_outbound_sample(
            existing,
            interface="ens3",
            current_tx_bytes=1500,
            monthly_limit_bytes=1000,
            now=datetime(2026, 5, 25, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(sample["today"]["bytes"], 700)
        self.assertEqual(sample["month"]["bytes"], 800)
        self.assertEqual(sample["total"]["bytes"], 1000)
        self.assertEqual(sample["month"]["percent"], 80.0)

    def test_build_vps_outbound_sample_handles_date_month_rollover_and_reset(self):
        existing = {
            "last_tx_bytes": 5000,
            "today": {"date": "2026-05-31", "bytes": 4000},
            "month": {"month": "2026-05", "bytes": 9000, "limit_bytes": 10000, "percent": 90},
            "total": {"bytes": 9000},
            "tracking_started_at": datetime(2026, 5, 31, tzinfo=timezone.utc),
            "reset_count": 0,
        }

        sample = build_vps_outbound_sample(
            existing,
            interface="ens3",
            current_tx_bytes=100,
            monthly_limit_bytes=10000,
            now=datetime(2026, 6, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(sample["today"], {"date": "2026-06-01", "bytes": 0})
        self.assertEqual(sample["month"]["month"], "2026-06")
        self.assertEqual(sample["month"]["bytes"], 0)
        self.assertEqual(sample["total"]["bytes"], 9000)
        self.assertEqual(sample["reset_count"], 1)


class HostOutboundAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_vps_outbound_summary_records_current_counter(self):
        class FakeDB:
            def __init__(self):
                self.args = None

            async def record_vps_outbound_sample(self, **kwargs):
                self.args = kwargs
                return {
                    "enabled": True,
                    "status": "ok",
                    "interface": kwargs["interface"],
                    "today": {"date": "2026-05-25", "bytes": 0},
                    "month": {"month": "2026-05", "bytes": 0, "limit_bytes": kwargs["monthly_limit_bytes"], "percent": 0},
                    "total": {"bytes": 0},
                }

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dev"
            path.write_text("ens3: 1 0 0 0 0 0 0 0 12345 0 0 0 0 0 0 0\n", encoding="utf-8")
            fake_db = FakeDB()

            with (
                patch("Backend.helper.host_outbound.Telegram.VPS_OUTBOUND_NET_DEV_PATH", str(path)),
                patch("Backend.helper.host_outbound.Telegram.VPS_OUTBOUND_TX_BYTES_PATH", ""),
                patch("Backend.helper.host_outbound.Telegram.VPS_OUTBOUND_INTERFACE", "ens3"),
                patch("Backend.helper.host_outbound.Telegram.VPS_OUTBOUND_MONTHLY_LIMIT_BYTES", 999),
            ):
                summary = await get_vps_outbound_summary(fake_db)

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(fake_db.args["current_tx_bytes"], 12345)
        self.assertEqual(fake_db.args["monthly_limit_bytes"], 999)


if __name__ == "__main__":
    unittest.main()
