import unittest
import asyncio
import tempfile
import shutil
from pathlib import Path
from Backend.helper.disk_cache import (
    first_cache_bytes,
    first_cache_enabled,
    disk_cache_enabled,
    is_complete_first_cache,
    get_first_cache_available_bytes,
    evict_lru,
    touch_cache_file,
)
from Backend.config import Telegram


class TestDualTierCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_dual_tier_")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_first_cache_defaults(self):
        with (
            unittest.mock.patch.object(Telegram, "DISK_CACHE_ENABLED", True),
            unittest.mock.patch.object(Telegram, "DISK_CACHE_MAX_GB", 5.0),
            unittest.mock.patch.object(Telegram, "DISK_CACHE_FIRST_MB", 20.0),
        ):
            bytes_count = first_cache_bytes()
            self.assertEqual(bytes_count, 20 * 1024 * 1024)
            self.assertTrue(first_cache_enabled())
            self.assertTrue(disk_cache_enabled())

    def test_complete_and_available_bytes(self):
        p = Path(self.temp_dir) / "test.first.bin"
        data = b"X" * (1024 * 1024)  # 1 MB
        with open(p, "wb") as f:
            f.write(data)

        self.assertEqual(get_first_cache_available_bytes(p), 1024 * 1024)
        self.assertTrue(is_complete_first_cache(p, expected_bytes=1024 * 1024))
        self.assertFalse(is_complete_first_cache(p, expected_bytes=2 * 1024 * 1024))

    async def test_lru_eviction_under_budget(self):
        root = Path(self.temp_dir)
        # Create 3 files of 1 MB each = 3 MB total
        f1 = root / "f1.bin"
        f2 = root / "f2.bin"
        f3 = root / "f3.bin"

        with open(f1, "wb") as f:
            f.write(b"1" * (1024 * 1024))
        await asyncio.sleep(0.01)
        with open(f2, "wb") as f:
            f.write(b"2" * (1024 * 1024))
        await asyncio.sleep(0.01)
        with open(f3, "wb") as f:
            f.write(b"3" * (1024 * 1024))

        # Evict to max 2 MB budget -> oldest file (f1) should be deleted
        await evict_lru(root=root, max_bytes=2 * 1024 * 1024)

        self.assertFalse(f1.exists())
        self.assertTrue(f2.exists())
        self.assertTrue(f3.exists())


if __name__ == "__main__":
    unittest.main()
