import unittest

from Backend.helper.mkv_seek_risk import (
    CUES_ID,
    EBML_ID,
    SEGMENT_ID,
    SEEK_HEAD_ID,
    SEEK_ID,
    SEEK_ID_ID,
    SEEK_POSITION_ID,
    analyze_mkv_seek_risk,
)
from Backend.helper.reply_text import MkvSeekWarningText, build_stream_reply_text


def _id(value: int) -> bytes:
    return value.to_bytes((value.bit_length() + 7) // 8, "big")


def _size(value: int) -> bytes:
    if value < 0x7F:
        return bytes([0x80 | value])
    if value < 0x3FFF:
        return bytes([0x40 | (value >> 8), value & 0xFF])
    raise ValueError("test size too large")


def _element(element_id: int, payload: bytes) -> bytes:
    return _id(element_id) + _size(len(payload)) + payload


def _seek_to(target_id: int) -> bytes:
    payload = (
        _element(SEEK_ID_ID, _id(target_id)) +
        _element(SEEK_POSITION_ID, b"\x01")
    )
    return _element(SEEK_ID, payload)


def _mkv_head_with_first_seek(target_id: int) -> bytes:
    segment_payload = _element(SEEK_HEAD_ID, _seek_to(target_id)) + _element(0x1549A966, b"\x00")
    return _element(EBML_ID, b"") + _element(SEGMENT_ID, segment_payload)


class MkvSeekRiskTests(unittest.TestCase):
    def test_first_seek_head_points_to_cues_has_no_warning(self):
        result = analyze_mkv_seek_risk(_mkv_head_with_first_seek(CUES_ID), b"")
        self.assertFalse(result.risk)

    def test_first_seek_head_hides_cues_behind_tail_seek_head_warns(self):
        head = _mkv_head_with_first_seek(SEEK_HEAD_ID)
        tail = b"padding" + _element(SEEK_HEAD_ID, _seek_to(CUES_ID))
        result = analyze_mkv_seek_risk(head, tail)
        self.assertTrue(result.risk)
        self.assertIn(CUES_ID, result.tail_seek_targets)

    def test_tail_cues_without_first_cues_warns(self):
        head = _mkv_head_with_first_seek(SEEK_HEAD_ID)
        tail = b"padding" + _element(CUES_ID, b"\x00")
        result = analyze_mkv_seek_risk(head, tail)
        self.assertTrue(result.risk)
        self.assertTrue(result.tail_has_cues)

    def test_random_bytes_are_inconclusive(self):
        result = analyze_mkv_seek_risk(b"not an mkv", b"")
        self.assertFalse(result.risk)

    def test_truncated_ebml_does_not_crash_or_warn(self):
        result = analyze_mkv_seek_risk(_id(EBML_ID) + b"\x84test" + _id(SEGMENT_ID), b"")
        self.assertFalse(result.risk)


class StreamReplyTextTests(unittest.TestCase):
    def _base_metadata(self, **updates):
        data = {
            "source_type": "telegram",
            "media_type": "movie",
            "title": "Kaantha",
            "year": 2025,
            "quality": "1080p",
            "rate": "7.1",
        }
        data.update(updates)
        return data

    def test_risky_telegram_mkv_reply_includes_seek_warning(self):
        text = build_stream_reply_text(
            self._base_metadata(mkv_seek_risk=True),
            "3.38GB",
            "https://example.test/dl/token/id/video.mkv",
        )
        self.assertIn(MkvSeekWarningText.strip(), text)
        self.assertIn("Direct Stream Link", text)

    def test_non_risky_telegram_reply_keeps_current_shape(self):
        text = build_stream_reply_text(
            self._base_metadata(mkv_seek_risk=False),
            "3.38GB",
            "https://example.test/dl/token/id/video.mkv",
        )
        self.assertNotIn("TV/mobile seek warning", text)
        self.assertIn("Cloudflare WARP", text)

    def test_torrent_reply_does_not_show_seek_warning(self):
        text = build_stream_reply_text(
            self._base_metadata(source_type="torrent", mkv_seek_risk=True),
            "3.38GB",
            "N/A",
        )
        self.assertNotIn("TV/mobile seek warning", text)
        self.assertNotIn("Direct Stream Link", text)
        self.assertIn("seeders/peers", text)


if __name__ == "__main__":
    unittest.main()
