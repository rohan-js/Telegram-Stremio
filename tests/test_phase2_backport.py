import io
import struct
import unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import Response

from Backend.fastapi.routes import stremio_routes
from Backend.fastapi.routes.stremio_routes import parse_size_to_bytes, stream_res_label
from Backend.helper import global_search, session_auth
from Backend.helper.database import Database
from Backend.helper.encrypt import decode_string
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.split_files import split_archive_ext
from Backend.helper.zip_stream import _zip64_sizes, parse_local_header, resolve_zip_entry


def fake_request():
    return SimpleNamespace(headers={}, client=SimpleNamespace(host="1.2.3.4"))


def _build_zip(entries, default_type=zipfile.ZIP_STORED):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for entry in entries:
            if len(entry) == 2:
                name, data = entry
                ctype = default_type
            else:
                name, data, ctype = entry
            zf.writestr(name, data, compress_type=ctype)
    return buf.getvalue()


class ZipStreamTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_local_header_rejects_non_zip_bytes(self):
        self.assertIsNone(parse_local_header(b"GARBAGE-NOT-A-ZIP"))

    def test_zip64_sizes_read_extra_field_values(self):
        extra = struct.pack("<HH", 0x0001, 24)
        extra += (9999999999).to_bytes(8, "little")
        extra += (100).to_bytes(8, "little")
        extra += (12345).to_bytes(8, "little")
        uncomp, comp, offset = _zip64_sizes(extra, 0xFFFFFFFF, 0xFFFFFFFF, need_offset=True, offset=0xFFFFFFFF)
        self.assertEqual((uncomp, comp, offset), (9999999999, 100, 12345))

    @staticmethod
    def _read(data):
        async def read(start, length):
            return data[start:start + length]
        return read

    async def test_resolve_stored_entry_from_local_header(self):
        content = b"A" * 2048
        data = _build_zip([("movie.mp4", content)])
        entry = await resolve_zip_entry(self._read(data), len(data))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["name"], "movie.mp4")
        self.assertEqual(entry["size"], len(content))
        self.assertEqual(data[entry["data_offset"]:entry["data_offset"] + entry["size"]], content)

    async def test_resolve_stored_entry_via_central_directory_when_descriptor_flag_set(self):
        content = b"D" * 3000
        raw = bytearray(_build_zip([("movie.mkv", content)]))
        flag = int.from_bytes(raw[6:8], "little") | 0x08
        raw[6:8] = flag.to_bytes(2, "little")
        data = bytes(raw)
        entry = await resolve_zip_entry(self._read(data), len(data))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["name"], "movie.mkv")
        self.assertEqual(entry["size"], len(content))
        self.assertEqual(data[entry["data_offset"]:entry["data_offset"] + entry["size"]], content)

    async def test_zip_with_only_deflated_entries_is_not_streamable(self):
        data = _build_zip([("movie.mkv", b"C" * 4096, zipfile.ZIP_DEFLATED)])
        entry = await resolve_zip_entry(self._read(data), len(data))
        if entry is not None:
            self.assertNotEqual(entry["method"], 0)

    async def test_empty_zip_returns_none(self):
        data = _build_zip([])
        self.assertIsNone(await resolve_zip_entry(self._read(data), len(data)))


class SplitArchiveTests(unittest.TestCase):
    def test_zip_split_part_is_detected(self):
        self.assertEqual(split_archive_ext("Movie.2024.zip.001"), "zip")
        self.assertEqual(split_archive_ext("Movie.2024.zip.42"), "zip")

    def test_video_split_part_is_not_an_archive(self):
        self.assertIsNone(split_archive_ext("Movie.2024.mkv.001"))

    def test_plain_zip_is_not_a_split_part(self):
        self.assertIsNone(split_archive_ext("Movie.2024.zip"))
        self.assertIsNone(split_archive_ext(""))


class GlobalSearchPartTests(unittest.TestCase):
    def test_zip_split_part_is_detected(self):
        base, num, display, is_zip = global_search._split_part_info("My.Movie.2024.zip.003")
        self.assertEqual(base, "my.movie.2024.zip")
        self.assertEqual(num, 3)
        self.assertTrue(is_zip)
        self.assertIn("My.Movie", display)

    def test_video_split_part_is_detected(self):
        base, num, display, is_zip = global_search._split_part_info("My.Movie.2024.1080p.mkv.002")
        self.assertEqual(base, "my.movie.2024.1080p.mkv")
        self.assertEqual(num, 2)
        self.assertFalse(is_zip)

    def test_plain_file_is_not_a_split_part(self):
        self.assertIsNone(global_search._split_part_info("My.Movie.2024.1080p.mkv"))
        self.assertIsNone(global_search._split_part_info(""))

    def test_resolve_channel_ids_canonicalizes_to_negative_ids(self):
        self.assertEqual(global_search._resolve_channel_ids(["123", "-100456", "789"]), [-100123, -100456, -100789])

    def test_title_score_measures_token_overlap(self):
        self.assertEqual(global_search._title_score("Movie One 2024", "movie one"), 1.0)
        self.assertEqual(global_search._title_score("Unrelated", "movie one"), 0.0)


class SessionAuthTests(unittest.IsolatedAsyncioTestCase):
    class FakeStateCollection:
        def __init__(self):
            self.doc = None

        async def find_one(self, query):
            return self.doc

        async def update_one(self, query, update, upsert=False):
            self.doc = update["$set"]
            return SimpleNamespace(matched_count=1, modified_count=1)

        async def delete_one(self, query):
            self.doc = None
            return SimpleNamespace(deleted_count=1)

    def setUp(self):
        self.state = self.FakeStateCollection()
        fake_db = SimpleNamespace(dbs={"tracking": {"state": self.state}})
        self.patcher = patch.object(session_auth, "db", fake_db)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    async def test_session_status_empty_when_no_stored_session(self):
        status = await session_auth.get_session_status()
        self.assertEqual(status, {"connected": False, "profile": None})

    async def test_session_status_reports_stored_profile(self):
        self.state.doc = {
            "_id": "user_session",
            "active": True,
            "name": "Test User",
            "username": "tester",
            "phone": "+12025550123",
            "user_id": 123,
            "session": "abc",
        }
        status = await session_auth.get_session_status()
        self.assertTrue(status["connected"])
        self.assertEqual(status["profile"]["name"], "Test User")
        self.assertEqual(status["profile"]["user_id"], 123)

    async def test_disconnect_session_flags_inactive(self):
        self.state.doc = {"_id": "user_session", "active": True, "session": "abc"}
        await session_auth.disconnect_session()
        self.assertFalse(self.state.doc["active"])

    async def test_remove_session_clears_stored_document(self):
        self.state.doc = {"_id": "user_session", "active": True, "session": "abc"}
        await session_auth.remove_session()
        self.assertIsNone(self.state.doc)
        status = await session_auth.get_session_status()
        self.assertFalse(status["connected"])

    async def test_start_login_rejects_empty_phone(self):
        with self.assertRaises(ValueError):
            await session_auth.start_login("")


class DatabasePartPayloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database = object.__new__(Database)

    async def test_build_part_id_and_size_marks_zip_payload_and_sorts_parts(self):
        token, size = await self.database._build_part_id_and_size(
            [
                {"part_number": 2, "chat_id": -100123, "msg_id": 5, "size_bytes": 100},
                {"part_number": 1, "chat_id": -100123, "msg_id": 4, "size_bytes": 50},
            ],
            archive="zip",
        )
        payload = await decode_string(token)
        self.assertTrue(payload["zip"])
        self.assertEqual([p["msg_id"] for p in payload["parts"]], [4, 5])
        self.assertTrue(size)

    async def test_build_part_id_and_size_without_archive_has_no_zip_key(self):
        token, _ = await self.database._build_part_id_and_size(
            [{"part_number": 1, "chat_id": -100123, "msg_id": 4, "size_bytes": 50}]
        )
        payload = await decode_string(token)
        self.assertNotIn("zip", payload)


class StremioLabelTests(unittest.TestCase):
    def test_parse_size_to_bytes_parses_readable_sizes(self):
        self.assertEqual(parse_size_to_bytes("1.5 GB"), int(1.5 * 1024 ** 3))
        self.assertEqual(parse_size_to_bytes("512 MB"), 512 * 1024 ** 2)
        self.assertEqual(parse_size_to_bytes(""), 0)
        self.assertEqual(parse_size_to_bytes("garbage"), 0)

    def test_stream_res_label_maps_resolutions(self):
        self.assertEqual(stream_res_label("Movie 4K HDR"), "4K")
        self.assertEqual(stream_res_label("Movie 1080p"), "1080p")
        self.assertEqual(stream_res_label("Movie"), "other")


class FakeCatalogDB:
    async def get_custom_catalogs(self, visible_only=False):
        return [
            {"_id": "abc123", "name": "Tamil Picks", "visible": True},
        ]


class ManifestConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_manifest_hides_and_reorders_catalogs_per_token(self):
        fake_db = FakeCatalogDB()
        token_data = {
            "config": {
                "hidden_catalogs": ["latest_movies", "custom_abc123"],
                "catalog_order": ["top_series", "top_movies", "latest_series"],
            }
        }
        with (
            patch.object(stremio_routes, "db", fake_db),
            patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False),
        ):
            manifest = await stremio_routes.get_manifest(
                fake_request(), "token123", Response(), token_data=token_data
            )
        ids = [catalog["id"] for catalog in manifest["catalogs"]]
        self.assertNotIn("latest_movies", ids)
        self.assertNotIn("custom_abc123", ids)
        self.assertLess(ids.index("top_series"), ids.index("top_movies"))
        self.assertLess(ids.index("top_movies"), ids.index("latest_series"))

    async def test_hidden_catalog_returns_empty_result(self):
        fake_db = FakeCatalogDB()
        with (
            patch.object(stremio_routes, "db", fake_db),
            patch.object(stremio_routes.Telegram, "HIDE_CATALOG", False),
        ):
            result = await stremio_routes.get_catalog(
                "token123",
                "movie",
                "latest_movies",
                Response(),
                extra=None,
                token_data={"config": {"hidden_catalogs": ["latest_movies"]}},
            )
        self.assertEqual(result["metas"], [])


class SettingsGateTests(unittest.IsolatedAsyncioTestCase):
    class FakeSettingsDB:
        def __init__(self):
            self.saved = None

        async def save_settings(self, settings):
            self.saved = dict(settings)
            return True

    async def test_global_search_is_disabled_without_configured_session(self):
        fake_db = self.FakeSettingsDB()
        with patch.object(
            SettingsManager, "_session_configured", new_callable=AsyncMock
        ) as configured:
            configured.return_value = False
            results = await SettingsManager.update(fake_db, {"global_search": True})
        self.assertFalse(fake_db.saved["global_search"])
        self.assertIn("no user session", results["global_search"])


if __name__ == "__main__":
    unittest.main()
