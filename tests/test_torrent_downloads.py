import os
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path
from unittest.mock import patch

import httpx


os.environ.setdefault("DATABASE", "mongodb://tracking,mongodb://storage")

from Backend.fastapi.routes import stream_routes
from Backend.config import Telegram
from Backend.fastapi.routes.stremio_routes import build_downloaded_torrent_stream, build_local_vps_stream
from Backend.helper.torrent_downloads import (
    QBitTorrentClient,
    TorrentDownloadManager,
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
        self.assertEqual(good.parts[-3:], ("download-root", "Movie", "file.mkv"))

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

    def test_nginx_download_redirect_strips_quoted_prefix(self):
        with patch.object(Telegram, "NGINX_DOWNLOAD_ACCEL_REDIRECT_LOCATION", '"/_downloads/"'):
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
            requests.append((request.method, request.url.path, request.content.decode(errors="replace")))
            if request.url.path == "/api/v2/torrents/add":
                return httpx.Response(200, text="Ok.")
            if request.url.path == "/api/v2/torrents/setShareLimits":
                return httpx.Response(200, text="Ok.")
            if request.url.path == "/api/v2/torrents/info":
                if "category=stremio" in str(request.url):
                    return httpx.Response(200, json=[])
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
            await client.set_no_seed_share_limits("abc")
            await client.stop_torrent("abc")
            await client.delete_torrent("abc", delete_files=True)
        finally:
            await client.close()

        self.assertEqual(info["name"], "Movie")
        self.assertEqual(files[0]["name"], "Movie.mkv")
        self.assertTrue(any(item[0:2] == ("POST", "/api/v2/torrents/add") and "ratioLimit=0" in item[2] and "seedingTimeLimit=0" in item[2] for item in requests))
        self.assertTrue(any(item[0:2] == ("POST", "/api/v2/torrents/setShareLimits") and "inactiveSeedingTimeLimit=0" in item[2] for item in requests))
        self.assertTrue(any(item[0:2] == ("POST", "/api/v2/torrents/stop") for item in requests))
        self.assertTrue(any(item[0:2] == ("POST", "/api/v2/torrents/delete") for item in requests))

    async def test_download_manager_stops_completed_stremio_seeders(self):
        class FakeQBit:
            def __init__(self):
                self.share_limited = []
                self.stopped = []

            async def list_torrents(self, category=None):
                self.category = category
                return [
                    {"hash": "done", "state": "uploading", "progress": 1.0},
                    {"hash": "active", "state": "downloading", "progress": 0.5},
                ]

            async def set_no_seed_share_limits(self, info_hash):
                self.share_limited.append(info_hash)

            async def stop_torrent(self, info_hash):
                self.stopped.append(info_hash)

            async def close(self):
                pass

        fake_qbit = FakeQBit()
        with patch("Backend.helper.torrent_downloads.QBitTorrentClient", lambda: fake_qbit):
            stopped = await TorrentDownloadManager().stop_completed_stremio_seeders()

        self.assertEqual(stopped, 1)
        self.assertEqual(fake_qbit.category, "stremio")
        self.assertIn("done", fake_qbit.share_limited)
        self.assertEqual(fake_qbit.stopped, ["done"])


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

    async def test_build_local_vps_stream_uses_existing_protected_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_file = root / "manual" / "Movie.mkv"
            local_file.parent.mkdir()
            local_file.write_bytes(b"video")
            quality = {
                "source_type": "local_vps",
                "local_rel_path": "manual/Movie.mkv",
                "filename": "Movie.mkv",
                "size": "5 B",
            }
            with patch("Backend.fastapi.routes.stremio_routes.download_root_dir", lambda: root):
                stream = await build_local_vps_stream("token", quality, "Telegram 1080p")

        self.assertEqual(stream["name"], "VPS Local 1080p")
        self.assertIn("Stored on VPS", stream["title"])
        self.assertIn("/downloaded/token/", stream["url"])
        self.assertEqual(stream["behaviorHints"]["videoSize"], 5)

    async def test_build_local_vps_stream_skips_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            quality = {
                "source_type": "local_vps",
                "local_rel_path": "manual/missing.mkv",
            }
            with patch("Backend.fastapi.routes.stremio_routes.download_root_dir", lambda: Path(tmp)):
                stream = await build_local_vps_stream("token", quality, "Telegram 1080p")

        self.assertIsNone(stream)


class DownloadedUsageAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def test_file_range_stream_counts_only_yielded_bytes(self):
        class FakeDB:
            def __init__(self):
                self.total = 0

            async def update_token_usage(self, _token, bytes_delta):
                self.total += bytes_delta

        fake_db = FakeDB()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "video.mkv"
            path.write_bytes(b"0123456789")

            with patch.object(stream_routes, "db", fake_db):
                chunks = []
                async for chunk in stream_routes.stream_file_range_with_usage(
                    path,
                    0,
                    9,
                    token="token",
                    read_size=4,
                ):
                    chunks.append(chunk)

        self.assertEqual(b"".join(chunks), b"0123456789")
        self.assertEqual(fake_db.total, 10)

    async def test_downloaded_nginx_offload_does_not_count_requested_range(self):
        class FakeDB:
            def __init__(self):
                self.calls = []

            async def update_token_usage(self, token, bytes_delta):
                self.calls.append((token, bytes_delta))

        class FakeRequest:
            method = "GET"
            headers = {"Range": "bytes=0-"}

        async def fake_decode(_encoded):
            return {"source_type": "downloaded_torrent", "rel_path": "video.mkv", "name": "video.mkv"}

        fake_db = FakeDB()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "video.mkv").write_bytes(b"0123456789")

            with (
                patch.object(stream_routes, "db", fake_db),
                patch.object(stream_routes, "decode_string", fake_decode),
                patch.object(stream_routes, "download_root_dir", lambda: root),
                patch.object(stream_routes.Telegram, "NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED", True),
            ):
                response = await stream_routes.downloaded_torrent_stream_handler(
                    FakeRequest(),
                    token="token",
                    id="encoded",
                    name="video.mkv",
                    token_data={"name": "test"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("x-accel-redirect", response.headers)
        self.assertNotIn("content-range", response.headers)
        self.assertNotEqual(response.headers.get("content-length"), "10")
        self.assertEqual(fake_db.calls, [])

    async def test_local_vps_nginx_offload_uses_same_protected_handler(self):
        class FakeRequest:
            method = "GET"
            headers = {"Range": "bytes=0-"}

        async def fake_decode(_encoded):
            return {"source_type": "local_vps", "rel_path": "manual/video.mkv", "name": "video.mkv"}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_file = root / "manual" / "video.mkv"
            local_file.parent.mkdir()
            local_file.write_bytes(b"0123456789")
            with (
                patch.object(stream_routes, "decode_string", fake_decode),
                patch.object(stream_routes, "download_root_dir", lambda: root),
                patch.object(stream_routes.Telegram, "NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED", True),
            ):
                response = await stream_routes.downloaded_torrent_stream_handler(
                    FakeRequest(),
                    token="token",
                    id="encoded",
                    name="video.mkv",
                    token_data={"name": "test"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-accel-redirect"], "/_downloads/manual/video.mkv")


if __name__ == "__main__":
    unittest.main()
