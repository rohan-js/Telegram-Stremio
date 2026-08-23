"""Tests: write_nfo_for_job (opt-in .nfo beside completed torrent files)."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper import torrent_downloads as td


class FakeDB:
    def __init__(self, doc):
        self._doc = doc

    async def get_media_details(self, imdb_id=None, **kwargs):
        return self._doc if imdb_id == (self._doc or {}).get("imdb_id") else None


def _movie_job(tmp: Path) -> dict:
    video = tmp / "Rocky III (1982).mkv"
    video.write_bytes(b"v")
    return {
        "info_hash": "abc123",
        "media_type": "movie",
        "imdb_id": "tt0084602",
        "files": [{"rel_path": video.name, "is_video": True, "name": video.name}],
    }


class WriteNfoForJobTests(unittest.IsolatedAsyncioTestCase):
    async def test_flag_off_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            job = _movie_job(tmp)
            fake_db = FakeDB({"imdb_id": "tt0084602", "title": "Rocky III"})
            with (
                patch.object(Telegram, "NFO_WRITE_ON_DOWNLOAD", False),
                patch.object(td, "download_root_dir", lambda: tmp),
                patch("Backend.db.get_media_details", new=fake_db.get_media_details),
            ):
                await td.write_nfo_for_job(job)
            self.assertFalse((tmp / "Rocky III (1982).nfo").exists())

    async def test_flag_on_movie_writes_nfo(self):
        doc = {
            "imdb_id": "tt0084602",
            "title": "Rocky III",
            "release_year": 1982,
            "rating": 6.8,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            job = _movie_job(tmp)
            fake_db = FakeDB(doc)
            with (
                patch.object(Telegram, "NFO_WRITE_ON_DOWNLOAD", True),
                patch.object(td, "download_root_dir", lambda: tmp),
                patch("Backend.db.get_media_details", new=fake_db.get_media_details),
            ):
                await td.write_nfo_for_job(job)
            nfo = tmp / "Rocky III (1982).nfo"
            self.assertTrue(nfo.exists())
            content = nfo.read_text(encoding="utf-8")
            self.assertIn("<movie>", content)
            self.assertIn("<title>Rocky III</title>", content)

    async def test_no_imdb_id_skips(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = _movie_job(Path(tmpdir))
            job["imdb_id"] = None
            with patch.object(Telegram, "NFO_WRITE_ON_DOWNLOAD", True):
                await td.write_nfo_for_job(job)  # must not raise

    async def test_db_failure_swallowed(self):
        async def boom(**kwargs):
            raise RuntimeError("db down")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            job = _movie_job(tmp)
            with (
                patch.object(Telegram, "NFO_WRITE_ON_DOWNLOAD", True),
                patch.object(td, "download_root_dir", lambda: tmp),
                patch("Backend.db.get_media_details", new=boom),
            ):
                await td.write_nfo_for_job(job)  # must not raise
            self.assertFalse((tmp / "Rocky III (1982).nfo").exists())


if __name__ == "__main__":
    unittest.main()
