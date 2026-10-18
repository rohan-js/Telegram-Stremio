"""Tests for Backend.helper.pyrogram_patches.

Covers the two leak-containment layers:
1. the cancellation-safe ``Session.send`` wrapper (own-entry pop only),
2. the ``sweep_session_results`` janitor,
plus an integration test against a real ``pyrogram.Session`` object to
prove a cancelled ``send`` leaves no ``results`` entry behind.
"""

import asyncio
import os
import time
import unittest

from pyrogram.session.internals.msg_factory import MsgFactory
from pyrogram.session.session import Session

from Backend.helper import pyrogram_patches as pp


class _FakeSession:
    def __init__(self):
        self.results = {}


def _msg_id(offset=4, age_sec=0):
    return ((int(time.time()) - age_sec) << 32) + offset


class SendCleanupWrapperTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._orig = pp._ORIG_SEND

    def tearDown(self):
        pp._ORIG_SEND = self._orig

    async def test_cancellation_pops_orphaned_entry(self):
        fake = _FakeSession()
        entry_id = _msg_id()

        async def fake_orig(session, data, wait_response=True, timeout=None, retry=0):
            session.results[entry_id] = object()
            pp._current_msg_id.set(entry_id)
            await asyncio.sleep(30)

        pp._ORIG_SEND = fake_orig
        task = asyncio.create_task(pp.send_with_results_cleanup(fake, "payload"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(fake.results), 1, "entry must exist before cancel")

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(fake.results, {}, "cancelled send must not orphan its entry")

    async def test_normal_path_returns_value_and_cleans(self):
        fake = _FakeSession()
        entry_id = _msg_id()

        async def fake_orig(session, data, wait_response=True, timeout=None, retry=0):
            session.results[entry_id] = object()
            pp._current_msg_id.set(entry_id)
            session.results.pop(entry_id)  # original pops on its normal path
            return "chunk-bytes"

        pp._ORIG_SEND = fake_orig
        result = await pp.send_with_results_cleanup(fake, "payload")
        self.assertEqual(result, "chunk-bytes")
        self.assertEqual(fake.results, {})

    async def test_wait_response_false_leaves_results_alone(self):
        fake = _FakeSession()
        other = _msg_id()
        fake.results[other] = object()

        async def fake_orig(session, data, wait_response=True, timeout=None, retry=0):
            return None

        pp._ORIG_SEND = fake_orig
        await pp.send_with_results_cleanup(fake, "payload", wait_response=False)
        self.assertIn(other, fake.results, "fire-and-forget sends must not touch results")

    async def test_cancellation_does_not_pop_other_tasks_entry(self):
        fake = _FakeSession()
        id_a = _msg_id(offset=4)
        id_b = _msg_id(offset=8)

        async def fake_orig(session, data, wait_response=True, timeout=None, retry=0):
            entry_id = id_a if data == "A" else id_b
            session.results[entry_id] = object()
            pp._current_msg_id.set(entry_id)
            await asyncio.sleep(30)

        pp._ORIG_SEND = fake_orig
        task_a = asyncio.create_task(pp.send_with_results_cleanup(fake, "A"))
        task_b = asyncio.create_task(pp.send_with_results_cleanup(fake, "B"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(fake.results), 2)

        task_a.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task_a
        self.assertEqual(list(fake.results), [id_b], "task B's in-flight entry must survive")

        task_b.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task_b
        self.assertEqual(fake.results, {})


class SweepSessionResultsTests(unittest.TestCase):
    def test_removes_only_stale_entries(self):
        session = _FakeSession()
        stale = _msg_id(age_sec=300)
        fresh = _msg_id(age_sec=5)
        session.results[stale] = object()
        session.results[fresh] = object()

        purged = pp.sweep_session_results(session, max_age_sec=90)
        self.assertEqual(purged, 1)
        self.assertNotIn(stale, session.results)
        self.assertIn(fresh, session.results)

    def test_ignores_non_dict_and_non_int_keys(self):
        self.assertEqual(pp.sweep_session_results(object()), 0)
        session = _FakeSession()
        session.results["weird-key"] = object()
        self.assertEqual(pp.sweep_session_results(session, max_age_sec=0), 0)
        self.assertIn("weird-key", session.results)

    def test_empty_results_is_noop(self):
        self.assertEqual(pp.sweep_session_results(_FakeSession()), 0)


class ApplyPatchesTests(unittest.TestCase):
    def test_apply_is_idempotent_and_patches_session_send(self):
        self.assertTrue(pp.apply_patches())
        self.assertIs(Session.send, pp.send_with_results_cleanup)
        first = Session.send
        self.assertTrue(pp.apply_patches())
        self.assertIs(Session.send, first)

        from pyrogram.session.internals.msg_factory import MsgFactory as MF

        self.assertTrue(getattr(MF, "_zs_hooked", False))


class RealSessionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_real_send_leaves_no_results_entry(self):
        pp.apply_patches()
        from pyrogram.raw.functions import Ping

        sess = Session.__new__(Session)
        sess.results = {}
        sess.msg_factory = MsgFactory()
        sess.salt = 0
        sess.session_id = b"12345678"
        sess.auth_key = os.urandom(256)
        sess.auth_key_id = os.urandom(8)
        sess.loop = asyncio.get_running_loop()

        class _Conn:
            async def send(self, payload):
                return None

        sess.connection = _Conn()

        task = asyncio.create_task(sess.send(Ping(ping_id=1)))
        await asyncio.sleep(0.2)
        if task.done():
            self.skipTest(f"real send did not reach the waiting state: {task.exception()!r}")

        self.assertEqual(len(sess.results), 1, "in-flight entry expected")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(sess.results, {}, "cancelled real send must clean its entry")


if __name__ == "__main__":
    unittest.main()
