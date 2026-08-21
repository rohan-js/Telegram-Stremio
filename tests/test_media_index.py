"""Tests for Backend.helper.media_index — synthetic MKV Cues / MP4 moov parsing."""

import struct
import unittest
from unittest.mock import patch

from Backend.helper.media_index import (
    MediaIndex,
    parse_mkv_index,
    parse_mp4_index,
    _store_index,
    get_media_index,
    _INDEX_CACHE,
)
from Backend.config import Telegram
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Synthetic Matroska builder
# ---------------------------------------------------------------------------
def ebml_size(n: int) -> bytes:
    if n < 1 << 7:
        return bytes([0x80 | n])
    if n < 1 << 14:
        return bytes([0x40 | (n >> 8), n & 0xFF])
    if n < 1 << 21:
        return bytes([0x20 | (n >> 16), (n >> 8) & 0xFF, n & 0xFF])
    if n < 1 << 28:
        return bytes([0x10 | (n >> 24), (n >> 16) & 0xFF, (n >> 8) & 0xFF, n & 0xFF])
    raise ValueError(n)


def ebml_element(eid: int, payload: bytes) -> bytes:
    eid_bytes = eid.to_bytes((eid.bit_length() + 7) // 8, "big")
    return eid_bytes + ebml_size(len(payload)) + payload


def build_mkv_fixture(keyframes, duration_sec=600.0, timestamp_scale_ns=1_000_000):
    """Build a minimal but structurally valid Matroska byte stream.

    ``keyframes``: list of (cue_time_scale_units, cluster_position) pairs.
    Duration is stored in TimestampScale units per the Matroska spec.
    """
    info = ebml_element(0x2AD7B1, timestamp_scale_ns.to_bytes(3, "big")) + ebml_element(
        0x4489, struct.pack(">f", duration_sec * 1e9 / timestamp_scale_ns)
    )
    info_box = ebml_element(0x1549A966, info)

    cues_entries = b""
    for cue_time, cluster_pos in keyframes:
        ctp = ebml_element(0xF1, cluster_pos.to_bytes(max(1, (cluster_pos.bit_length() + 7) // 8), "big"))
        point = ebml_element(0xB3, cue_time.to_bytes(max(1, (cue_time.bit_length() + 7) // 8), "big")) + ebml_element(0xB7, ctp)
        cues_entries += ebml_element(0xBB, point)
    cues = ebml_element(0x1C53BB6B, cues_entries)

    # Pad so every cluster_position is a valid offset inside the file.
    last_pos = max((pos for _, pos in keyframes), default=0)
    dummy_clusters = b"\x00" * max(64, last_pos + 8)
    segment_payload = info_box + dummy_clusters + cues
    segment = ebml_element(0x18538067, segment_payload)

    doctype = ebml_element(0x4282, b"matroska")
    ebml_header = ebml_element(0x1A45DFA3, doctype)
    return ebml_header + segment


# ---------------------------------------------------------------------------
# Synthetic MP4 builder
# ---------------------------------------------------------------------------
def mp4_box(typ: bytes, payload: bytes) -> bytes:
    return struct.pack(">I", 8 + len(payload)) + typ + payload


def build_mp4_fixture():
    """5 chunks x 2 samples, 0.1s per sample, sync samples 1 and 5.

    Chunk offsets: [10000, 20000, 30000, 40000, 50000]
    Expected keyframes: (0.0s, 10000) chunk1, (0.4s, 30000) chunk3.
    """
    ftyp = mp4_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2")

    mvhd_payload = (
        b"\x00\x00\x00\x00"  # version/flags
        + b"\x00\x00\x00\x00"  # creation
        + b"\x00\x00\x00\x00"  # modification
        + struct.pack(">I", 1000)  # timescale
        + struct.pack(">I", 600000)  # duration (600 s)
        + b"\x00" * 80
    )
    mvhd = mp4_box(b"mvhd", mvhd_payload)

    stts = mp4_box(b"stts", struct.pack(">II", 0, 1) + struct.pack(">II", 10, 100))
    stsc = mp4_box(b"stsc", struct.pack(">II", 0, 1) + struct.pack(">III", 1, 2, 1))
    stco = mp4_box(
        b"stco",
        struct.pack(">II", 0, 5) + b"".join(struct.pack(">I", off) for off in (10000, 20000, 30000, 40000, 50000)),
    )
    stss = mp4_box(b"stss", struct.pack(">II", 0, 2) + struct.pack(">II", 1, 5))

    stbl = mp4_box(b"stbl", stts + stsc + stco + stss)
    minf = mp4_box(b"minf", stbl)
    hdlr = mp4_box(b"hdlr", b"\x00\x00\x00\x00" + b"\x00\x00\x00\x00" + b"vide" + b"\x00" * 12 + b"\x00")
    mdia = mp4_box(b"mdia", hdlr + minf)
    trak = mp4_box(b"trak", mdia)

    moov = mp4_box(b"moov", mvhd + trak)
    return ftyp + moov + b"\x00" * 16


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class MkvIndexParsingTests(unittest.TestCase):
    def test_parse_mkv_cues(self):
        # 61 keyframes every 10s; cluster positions every 1 MB
        kfs = [(i * 10_000, i * 64) for i in range(61)]  # cue_time units of TimestampScale; clusters 64B apart
        full = build_mkv_fixture(kfs, duration_sec=600.0)

        head = full[: 256 * 1024]
        tail = full[-64 * 1024 :]
        idx = parse_mkv_index(head, tail, len(full))

        self.assertIsNotNone(idx)
        self.assertEqual(idx.container, "mkv")
        self.assertAlmostEqual(idx.duration_sec, 600.0, places=1)
        self.assertEqual(len(idx.keyframes), 61)

        # First keyframe: time 0 at segment_data_start + 0
        first_t, first_b = idx.keyframes[0]
        self.assertEqual(first_t, 0.0)
        # fifth keyframe: 40s at segment_data_start + 4 MiB
        t5, b5 = idx.keyframes[4]
        self.assertAlmostEqual(t5, 40.0, places=3)

    def test_mkv_time_byte_roundtrip(self):
        kfs = [(i * 10_000, i * 64) for i in range(61)]
        full = build_mkv_fixture(kfs)
        idx = parse_mkv_index(full[:262144], full[-65536:], len(full))
        self.assertIsNotNone(idx)

        t = idx.byte_to_time(idx.keyframes[7][1])
        self.assertAlmostEqual(t, 70.0, places=3)
        b = idx.time_to_byte(70.0)
        self.assertEqual(b, idx.keyframes[7][1])

        # Seek +10s from keyframe 7 (70s) → keyframe 8 (80s)
        target = idx.seek_delta_byte(idx.keyframes[7][1], 10.0, len(full))
        self.assertEqual(target, idx.keyframes[8][1])
        # Seek -10s → keyframe 6
        back = idx.seek_delta_byte(idx.keyframes[7][1], -10.0, len(full))
        self.assertEqual(back, idx.keyframes[6][1])

    def test_mkv_rejects_non_mkv(self):
        self.assertIsNone(parse_mkv_index(b"\x00" * 1024, b"\x00" * 1024, 4096))

    def test_mkv_rejects_missing_cues(self):
        # head with EBML magic but tail without Cues
        head = build_mkv_fixture([(0, 0)])[:512]
        idx = parse_mkv_index(head, b"\x00" * 1024, 100000)
        self.assertIsNone(idx)


class Mp4IndexParsingTests(unittest.TestCase):
    def test_parse_mp4_moov_from_head(self):
        full = build_mp4_fixture()
        head = full[:4096]
        idx = parse_mp4_index(head, b"", len(full))
        self.assertIsNotNone(idx)
        self.assertEqual(idx.container, "mp4")
        self.assertAlmostEqual(idx.duration_sec, 600.0, places=2)
        self.assertEqual(len(idx.keyframes), 2)
        self.assertEqual(idx.keyframes[0], (0.0, 10000))
        self.assertAlmostEqual(idx.keyframes[1][0], 0.4, places=3)
        self.assertEqual(idx.keyframes[1][1], 30000)

    def test_mp4_time_to_byte(self):
        full = build_mp4_fixture()
        idx = parse_mp4_index(full[:4096], b"", len(full))
        # at-or-before semantics: the keyframe a player will actually request
        self.assertEqual(idx.time_to_byte(0.0), 10000)
        self.assertEqual(idx.time_to_byte(0.3), 10000)
        self.assertEqual(idx.time_to_byte(0.4), 30000)
        self.assertEqual(idx.seek_delta_byte(10000, 0.4, len(full)), 30000)

    def test_mp4_rejects_non_mp4(self):
        self.assertIsNone(parse_mp4_index(b"\x00" * 1024, b"\x00" * 1024, 4096))


class MediaIndexUnitTests(unittest.TestCase):
    def test_downsample_caps_entries(self):
        kfs = [(float(i), i * 1000) for i in range(10000)]
        out = MediaIndex.downsample(kfs, 100)
        self.assertLessEqual(len(out), 101)
        self.assertEqual(out[0], kfs[0])
        self.assertEqual(out[-1], kfs[-1])

    def test_bitrate(self):
        idx = MediaIndex("mkv", 100.0, [(0.0, 0), (50.0, 5_000_000), (100.0, 10_000_000)])
        self.assertAlmostEqual(idx.bitrate_bps(10_000_000), 100_000.0)
        self.assertIsNone(MediaIndex("mkv", None, []).bitrate_bps(1000))


class IndexCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _INDEX_CACHE.clear()

    async def test_store_and_get(self):
        idx = MediaIndex("mkv", 100.0, [(0.0, 0), (50.0, 5000)])
        await _store_index((1, 1), idx)
        got = await get_media_index(1, 1)
        self.assertIs(got, idx)

    async def test_negative_cached(self):
        await _store_index((2, 2), None)
        self.assertIsNone(await get_media_index(2, 2))

    async def test_lru_cap(self):
        idx = MediaIndex("mkv", 10.0, [(0.0, 0)])
        # _max_index_entries enforces a floor of 8; stay above it
        with patch.object(Telegram, "STREAM_INDEX_CACHE_MAX_ENTRIES", 10):
            for i in range(12):
                await _store_index((i, i), idx)
            self.assertLessEqual(len(_INDEX_CACHE), 10)
            self.assertIsNone(await get_media_index(0, 0))
            self.assertIsNone(await get_media_index(1, 1))
            self.assertIsNotNone(await get_media_index(11, 11))


class MediaIndexSessionAndTtlTests(unittest.IsolatedAsyncioTestCase):
    """Session reuse (open uses the warm media_session) + FloodWait short negative TTL."""

    async def asyncSetUp(self):
        _INDEX_CACHE.clear()

    async def test_session_reuse_no_extra_media_session_calls(self):
        from unittest.mock import AsyncMock, MagicMock
        from Backend.helper import media_index as mi
        from Backend.helper.media_index import build_media_index, get_media_index
        _INDEX_CACHE.clear()
        fake_fid = MagicMock()
        fake_fid.file_size = 50 * 1024 * 1024
        fake_fid.file_name = "t.mkv"
        fake_session = object()
        fake_loc = object()
        fake_streamer = MagicMock()
        # head so the MKV parser rejects quickly but still exercises the fetch path
        fake_streamer._get_media_session = AsyncMock(return_value=fake_session)
        fake_streamer._get_location = AsyncMock(return_value=fake_loc)
        fake_streamer._fetch_file_bytes = AsyncMock(return_value=b"")
        await build_media_index(fake_fid, fake_streamer, chat_id=1, message_id=1, media_session=fake_session, location=fake_loc)
        # Warm session/location supplied -> no extra _get_media_session/_get_location calls
        fake_streamer._get_media_session.assert_not_called()
        fake_streamer._get_location.assert_not_called()
        self.assertIsNone(await get_media_index(1, 1))

    async def test_without_warm_session_calls_media_session(self):
        from unittest.mock import AsyncMock, MagicMock
        from Backend.helper.media_index import build_media_index, get_media_index
        _INDEX_CACHE.clear()
        fake_fid = MagicMock()
        fake_fid.file_size = 10 * 1024 * 1024
        fake_streamer = MagicMock()
        fake_streamer._get_media_session = AsyncMock(return_value=object())
        fake_streamer._get_location = AsyncMock(return_value=object())
        fake_streamer._fetch_file_bytes = AsyncMock(return_value=b"")
        await build_media_index(fake_fid, fake_streamer, chat_id=9, message_id=9)
        fake_streamer._get_media_session.assert_called()
        self.assertIsNone(await get_media_index(9, 9))

    async def test_floodwait_negative_expires_fast(self):
        from Backend.helper.media_index import _store_index, get_media_index, _INDEX_CACHE
        import time as _tm
        _INDEX_CACHE.clear()
        await _store_index((3, 3), None, cause="floodwait")
        # Still cached after 30s
        with patch("Backend.helper.media_index.time.time", return_value=_tm.time() + 30):
            self.assertIsNone(await get_media_index(3, 3))
            self.assertIn((3, 3), _INDEX_CACHE)
        # Expired after 65s (short TTL 60s)
        with patch("Backend.helper.media_index.time.time", return_value=_tm.time() + 65):
            # get_media_index pops expired negatives and returns None (absent)
            self.assertIsNone(await get_media_index(3, 3))
            self.assertNotIn((3, 3), _INDEX_CACHE)

    async def test_non_floodwait_negative_lives_30min(self):
        from Backend.helper.media_index import _store_index, get_media_index, _INDEX_CACHE
        import time as _tm
        _INDEX_CACHE.clear()
        await _store_index((4, 4), None)
        with patch("Backend.helper.media_index.time.time", return_value=_tm.time() + 65):
            self.assertIsNone(await get_media_index(4, 4))
            self.assertIn((4, 4), _INDEX_CACHE)
        with patch("Backend.helper.media_index.time.time", return_value=_tm.time() + 1801):
            self.assertIsNone(await get_media_index(4, 4))
            self.assertNotIn((4, 4), _INDEX_CACHE)


if __name__ == "__main__":
    unittest.main()

