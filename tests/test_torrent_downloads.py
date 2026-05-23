import os
import unittest
from collections import namedtuple
from pathlib import Path

import httpx


os.environ.setdefault("DATABASE", "mongodb://tracking,mongodb://storage")

from Backend.fastapi.routes.stremio_routes import build_downloaded_torrent_stream
from Backend.helper.torrent_downloads import (
    QBitTorrentClient,
    has_enough_download_space,
    nginx_download_redirect_uri,
    safe_download_file_path,
    select_completed_torrent_file,
    torrent_download_callback_data,
    torrent_download_keyboard,
)


class TorrentDownloadHelperTests(unittest.TestCase):
    def test_select_completed_file_uses_file_idx_first(self):
        files = [
            {"index": 0, "name": "sample.mkv", "size": 100, "progress": 1},
            {"index": 2, "name": "Show.S01E02.mkv", "size": 200, "progress": 1},
        ]

        selected = select_completed_torrent_file(files, {"file_idx": 2})

        self.assertIsNotNone(selected)
        self.assertEqual(selected["rel_path"], "Show.S01E02.mkv")

    def test_select_completed_file_matches_episode_when_no_file_idx(self):
        files = [
            {"index": 0, "name": "Show.S01E01.mkv", "size": 100, "progress": 1},
            {"index": 1, "name": "Show.S01E02.mkv", "size": 100, "progress": 1},
        ]

        selected = select_completed_torrent_file(
            files,
            {"file_idx": None},
            season_number=1,
            episode_number=2,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["rel_path"], "Show.S01E02.mkv")

    def test_select_completed_file_refuses_ambiguous_multi_video_movie(self):
        files = [
            {"index": 0, "name": "a.mkv", "size": 100, "progress": 1},
            {"index": 1, "name": "b.mkv", "size": 100, "progress": 1},
        ]

        selected = select_completed_torrent_file(files, {"file_idx": None})

        self.assertIsNone(selected)

    def test_safe_download_path_rejects_traversal(self):
        root = Path("/tmp/download-root")

        good = safe_download_file_path(root, "Movie/file.mkv")
        self.assertEqual(str(good), "/tmp/download-root/Movie/file.mkv")

        with self.assertRaises(ValueError):
            safe_download_file_path(root, "../secret.txt")

    def test_disk_space_threshold(self):
        Usage = namedtuple("Usage", "total used free")

        def usage(_path):
            return Usage(total=100 * 1024 ** 3, used=80 * 1024 ** 3, free=20 * 1024 ** 3)

        ok, free_gb, min_gb = has_enough_download_space(10, root=Path("/tmp"), usage_func=usage)

        self.assertTrue(ok)
        self.assertEqual(round(free_gb), 20)
        self.assertEqual(min_gb, 10)

    def test_nginx_download_redirect_quotes_relative_path(self):
        uri = nginx_download_redirect_uri("Movie Folder/video file.mkv")

        self.assertEqual(uri, "/_downloads/Movie%20Folder/video%20file.mkv")

    def test_download_keyboard_uses_short_info_hash_callback(self):
        info_hash = "0123456789abcdef0123456789abcdef01234567"
        keyboard = torrent_download_keyboard(info_hash, stremio_link="https://example.test/watch")

        self.assertEqual(torrent_download_callback_data(info_hash), f"tdl_{info_hash}")
        self.assertLessEqual(len(keyboard.inline_keyboard[1][0].callback_data), 64)


class QBitTorrentClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_qbit_client_add_info_files_stop_delete(self):
        requests = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append((request.method, request.url.path))
            if request.url.path == "/api/v2/torrents/add":
                return httpx.Response(200, text="Ok.")
            if request.url.path == "/api/v2/torrents/info":
                return httpx.Response(
                    200,
                    json=[
                        {
                            "hash": "abc",
                            "name": "Movie",
                            "progress": 1,
                            "size": 123,
                            "state": "uploading",
                        }
                    ],
                )
            if request.url.path == "/api/v2/torrents/files":
                return httpx.Response(200, json=[{"index": 0, "name": "Movie.mkv", "size": 123, "progress": 1}])
            if request.url.path == "/api/v2/torrents/stop":
                return httpx.Response(200, text="Ok.")
            if request.url.path == "/api/v2/torrents/delete":
                return httpx.Response(200, text="Ok.")
            return httpx.Response(404)

        client = QBitTorrentClient(
            base_url="http://qbittorrent:8080",
            client=httpx.AsyncClient(
                base_url="http://qbittorrent:8080",
                transport=httpx.MockTransport(handler),
            ),
        )
        try:
            await client.add_torrent(magnet_uri="magnet:?xt=urn:btih:abc")
            info = await client.torrent_info("abc")
            files = await client.torrent_files("abc")
            await client.stop_torrent("abc")
            await client.delete_torrent("abc", delete_files=True)
        finally:
            await client.close()

        self.assertEqual(info["name"], "Movie")
        self.assertEqual(files[0]["name"], "Movie.mkv")
        self.assertIn(("POST", "/api/v2/torrents/add"), requests)
        self.assertIn(("POST", "/api/v2/torrents/stop"), requests)
        self.assertIn(("POST", "/api/v2/torrents/delete"), requests)


class StremioDownloadedStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_downloaded_stream_only_when_completed_file_matches(self):
        quality = {
            "source_type": "torrent",
            "info_hash": "0123456789abcdef0123456789abcdef01234567",
            "file_idx": 1,
            "filename": "Show.S01E02.mkv",
        }
        job = {
            "status": "completed",
            "files": [
                {"index": 0, "name": "Show.S01E01.mkv", "size": 100, "progress": 1},
                {"index": 1, "name": "Show.S01E02.mkv", "size": 200, "progress": 1},
            ],
        }

        stream = await build_downloaded_torrent_stream(
            "token",
            quality,
            "Telegram 1080p",
            job,
            season_number=1,
            episode_number=2,
        )

        self.assertIsNotNone(stream)
        self.assertEqual(stream["name"], "Downloaded 1080p")
        self.assertIn("Downloaded to VPS", stream["title"])
        self.assertIn("/downloaded/token/", stream["url"])


if __name__ == "__main__":
    unittest.main()
