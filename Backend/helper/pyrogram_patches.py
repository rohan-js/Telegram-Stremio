"""Runtime patches for PyroFork that bound memory growth in ``Session.results``.

Pyrogram keeps one entry in ``Session.results`` per awaited request and
removes it only on the normal completion, timeout and send-failure paths
(``pyrogram/session/session.py``, ``Session.send``).  When the awaiting
task is *cancelled* the entry is orphaned, and when the late server reply
arrives ``handle_packet`` stores the parsed payload (up to ~1 MiB for
``upload.getFile``) into it permanently.  Our streaming engine cancels
fetches constantly by design (hedge losers, first-chunk deadline, client
disconnects), so a slow-DC retry storm leaks hundreds of MB -- the
2026-10-18 RAM collapse that froze the box.

Two layers, both installed by :func:`apply_patches`:

1. ``send_with_results_cleanup`` -- a ``Session.send`` wrapper that pops
   exactly its own request's entry in a ``finally`` block.  The request's
   msg id is captured through a task-local context variable inside a
   ``MsgFactory.__call__`` hook, so a cancelled task can never pop a
   concurrent task's in-flight entry.
2. :func:`sweep_session_results` -- a janitor run from the keepalive loop
   that drops entries older than ``max_age`` seconds (the top 32 bits of
   an MTProto msg id are its creation unix time).  Belt and braces for
   any path the wrapper cannot observe (e.g. pack failure before the
   registration, or a future PyroFork internals change).

Both layers fail safe: if the installed PyroFork's ``send`` does not look
like the expected shape, the patch is skipped with a warning and the
janitor alone keeps leaks bounded by ``max_age``.
"""

from __future__ import annotations

import contextvars
import inspect
import time
from typing import Any, Optional

from Backend.logger import LOGGER

#: msg id of the message most recently built by MsgFactory in this task.
_current_msg_id: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "zs_last_msg_id", default=None
)

#: Captured original ``Session.send``.  Tests replace this to fake the
#: inner call without touching the real PyroFork class.
_ORIG_SEND = None

#: Default for ``Session.send(timeout=...)``, captured at patch time.
_SEND_TIMEOUT_DEFAULT: float = 15.0

_APPLIED = False


async def send_with_results_cleanup(
    self: Any,
    data: Any,
    wait_response: bool = True,
    timeout: Optional[float] = None,
    retry: int = 0,
) -> Any:
    """``Session.send`` replacement that cannot orphan a ``results`` entry.

    Semantics match the original exactly; the only difference is that the
    entry for this request is popped in a ``finally`` block, so
    cancellation (hedge losers, client disconnects, app timeouts) cleans
    up instead of leaking.  All other paths already popped the entry --
    the redundant pop is a no-op for them.
    """
    if timeout is None:
        timeout = _SEND_TIMEOUT_DEFAULT
    token = _current_msg_id.set(None)
    try:
        return await _ORIG_SEND(self, data, wait_response, timeout, retry)  # type: ignore[misc]
    finally:
        msg_id = _current_msg_id.get() if wait_response else None
        _current_msg_id.reset(token)
        if msg_id is not None:
            try:
                self.results.pop(msg_id, None)
            except Exception:
                pass


def _install_factory_hook() -> bool:
    """Record every Message's msg id in the current task's context."""
    from pyrogram.session.internals.msg_factory import MsgFactory

    if getattr(MsgFactory, "_zs_hooked", False):
        return True

    original = MsgFactory.__call__

    def _call(self: Any, body: Any) -> Any:
        message = original(self, body)
        try:
            _current_msg_id.set(int(message.msg_id))
        except Exception:
            pass
        return message

    MsgFactory.__call__ = _call
    MsgFactory._zs_hooked = True  # type: ignore[attr-defined]
    return True


def _install_send_cleanup() -> bool:
    from pyrogram.session.session import Session

    if getattr(Session, "_zs_send_patched", False):
        return True

    original = Session.send
    try:
        source = inspect.getsource(original)
    except (OSError, TypeError):
        source = ""
    if "self.results[msg_id] = Result()" not in source:
        LOGGER.warning(
            "pyrogram_patches: unrecognized Session.send implementation; "
            "results-cleanup wrapper NOT applied (janitor sweep remains)"
        )
        return False

    global _ORIG_SEND, _SEND_TIMEOUT_DEFAULT
    _ORIG_SEND = original
    try:
        _SEND_TIMEOUT_DEFAULT = float(
            inspect.signature(original).parameters["timeout"].default
        )
    except Exception:
        _SEND_TIMEOUT_DEFAULT = float(getattr(Session, "WAIT_TIMEOUT", 15))

    Session.send = send_with_results_cleanup  # type: ignore[method-assign]
    Session._zs_send_patched = True  # type: ignore[attr-defined]
    return True


def apply_patches() -> bool:
    """Install the cleanup wrapper + factory hook.  Idempotent."""
    global _APPLIED
    if _APPLIED:
        return True

    factory_ok = False
    send_ok = False
    try:
        factory_ok = _install_factory_hook()
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("pyrogram_patches: factory hook failed: %s", exc)
    try:
        send_ok = _install_send_cleanup()
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("pyrogram_patches: send cleanup failed: %s", exc)

    _APPLIED = factory_ok and send_ok
    if _APPLIED:
        LOGGER.info(
            "pyrogram_patches: Session.results cleanup active "
            "(cancellation-safe pop + janitor)"
        )
    return _APPLIED


def sweep_session_results(session: Any, max_age_sec: float = 90.0) -> int:
    """Drop ``session.results`` entries older than ``max_age_sec``.

    The top 32 bits of an MTProto msg id are the unix second it was
    created (``msg_id = now * 2**32 + offset``).  No legitimate request
    can still be awaited after ``max_age_sec`` (every send timeout is
    far shorter), so those entries are cancellations or leaks.
    Returns the number of purged entries.
    """
    results = getattr(session, "results", None)
    if not isinstance(results, dict) or not results:
        return 0
    cutoff = time.time() - max_age_sec
    stale = [
        msg_id
        for msg_id in list(results.keys())
        if isinstance(msg_id, int) and (msg_id >> 32) < cutoff
    ]
    for msg_id in stale:
        results.pop(msg_id, None)
    return len(stale)
