"""Tests for next-episode resolution (_next_episode_quality_from_doc / _pick_telegram_quality)."""

import unittest

from Backend.helper.database import _next_episode_quality_from_doc, _pick_telegram_quality

CHAT = -1003625383282


def _q(msg, recommended=False, parts=None):
    q = {
        "source_type": "telegram",
        "id": f"id-{msg}",
        "origin_chat_id": CHAT,
        "origin_msg_id": msg,
    }
    if recommended:
        q["recommended"] = True
    if parts:
        q["parts"] = parts
        q.pop("origin_chat_id")
        q.pop("origin_msg_id")
    return q


def _ep(num, qualities):
    return {"episode_number": num, "title": f"E{num}", "telegram": qualities}


def _doc(seasons):
    return {"title": "Show", "seasons": seasons}


class NextEpisodeWalkerTests(unittest.TestCase):
    def test_next_episode_in_same_season(self):
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(1, [_q(101)]), _ep(2, [_q(202)]), _ep(3, [_q(303)])]},
        ])
        self.assertEqual(_next_episode_quality_from_doc(doc, CHAT, 101), (CHAT, 202))

    def test_season_rollover_to_next_season_first_episode(self):
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(1, [_q(101)]), _ep(2, [_q(102)])]},
            {"season_number": 2, "episodes": [_ep(1, [_q(201)]), _ep(2, [_q(202)])]},
        ])
        self.assertEqual(_next_episode_quality_from_doc(doc, CHAT, 102), (CHAT, 201))

    def test_last_episode_of_show_returns_none(self):
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(1, [_q(101)]), _ep(2, [_q(102)])]},
        ])
        self.assertIsNone(_next_episode_quality_from_doc(doc, CHAT, 102))

    def test_split_parts_quality_matches(self):
        parts = [{"part_number": 1, "chat_id": 3625383282, "msg_id": 555, "size_bytes": 100}]
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(1, [_q(0, parts=parts)]), _ep(2, [_q(202)])]},
        ])
        self.assertEqual(_next_episode_quality_from_doc(doc, CHAT, 555), (CHAT, 202))

    def test_unsorted_episodes_resolved_by_number(self):
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(3, [_q(303)]), _ep(1, [_q(101)]), _ep(2, [_q(202)])]},
        ])
        # playing E1 -> next is E2 even though stored order is 3,1,2
        self.assertEqual(_next_episode_quality_from_doc(doc, CHAT, 101), (CHAT, 202))

    def test_no_match_returns_none(self):
        doc = _doc([
            {"season_number": 1, "episodes": [_ep(1, [_q(101)])]},
        ])
        self.assertIsNone(_next_episode_quality_from_doc(doc, CHAT, 999))


class PickQualityTests(unittest.TestCase):
    def test_recommended_preferred(self):
        qualities = [_q(1), _q(2, recommended=True)]
        self.assertEqual(_pick_telegram_quality(qualities), (CHAT, 2))

    def test_parts_quality_uses_first_part_full_chat(self):
        parts = [{"part_number": 1, "chat_id": 3625383282, "msg_id": 777, "size_bytes": 1}]
        q = _q(0, parts=parts)
        self.assertEqual(_pick_telegram_quality([q]), (CHAT, 777))

    def test_torrent_only_returns_none(self):
        self.assertIsNone(_pick_telegram_quality([{"source_type": "torrent", "info_hash": "x"}]))

    def test_empty_returns_none(self):
        self.assertIsNone(_pick_telegram_quality([]))


if __name__ == "__main__":
    unittest.main()
