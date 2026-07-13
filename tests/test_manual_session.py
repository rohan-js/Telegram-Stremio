import unittest

from Backend.helper.manual_session import ManualSessionManager, is_personal_media, max_episode


class ManualSessionManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.manager = ManualSessionManager()
        self.document = {
            "tmdb_id": -100,
            "seasons": [{
                "season_number": 1,
                "episodes": [{"episode_number": 1}, {"episode_number": 3}],
            }],
        }

    async def test_session_is_copied_and_cleared(self):
        source = {"tmdb_id": -100, "kind": "personal"}
        await self.manager.activate(source)
        source["kind"] = "changed"
        self.assertEqual(self.manager.current()["kind"], "personal")

        await self.manager.clear()
        self.assertIsNone(self.manager.current())

    async def test_personal_episode_assignment_is_sequential(self):
        first = await self.manager.assign_episode(self.document, 1)
        second = await self.manager.assign_episode(self.document, 1)
        self.assertEqual((first, second), (4, 5))

    async def test_split_parts_share_one_reserved_episode(self):
        first = await self.manager.assign_episode(self.document, 1, split_key="show.s01.mkv")
        second = await self.manager.assign_episode(self.document, 1, split_key="show.s01.mkv")
        next_file = await self.manager.assign_episode(self.document, 1)
        self.assertEqual((first, second, next_file), (4, 4, 5))

    async def test_explicit_episode_is_preserved(self):
        episode = await self.manager.assign_episode(self.document, 1, explicit_episode=12)
        self.assertEqual(episode, 12)

    def test_personal_helpers(self):
        self.assertTrue(is_personal_media(-1))
        self.assertFalse(is_personal_media(1))
        self.assertEqual(max_episode(self.document, 1), 3)


if __name__ == "__main__":
    unittest.main()
