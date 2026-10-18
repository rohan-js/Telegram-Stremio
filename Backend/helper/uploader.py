"""Extract the uploader identity from an ingested Telegram message.

Captured at ingestion time (live receiver + channel scanners) and stored on
the quality subdoc so the admin Insights page can answer "who uploaded
this?".  PyroFork caveats handled here:

- channel-identity posts (anonymous admins): ``from_user`` is None; the
  ``sender_chat`` then carries the channel itself;
- forwarded uploads: ``forward_from`` / ``forward_from_chat`` identify the
  original source, recorded additionally as ``forwarded_from``;
- anything missing simply yields ``None`` — the UI renders "—".

The returned dict is sanitized by ``Database._normalize_uploader`` before
storage, so callers can pass it through unchanged.
"""

from typing import Optional


def _user_display(user) -> str:
    first = getattr(user, "first_name", None) or ""
    last = getattr(user, "last_name", None) or ""
    name = " ".join(part for part in (first, last) if part).strip()
    if name:
        return name
    username = getattr(user, "username", None)
    if username:
        return f"@{username}"
    return "User"


def uploader_from_message(message) -> Optional[dict]:
    """Uploader descriptor for a PyroFork Message (None when unknowable)."""
    if message is None:
        return None

    from_user = getattr(message, "from_user", None)
    sender_chat = getattr(message, "sender_chat", None)
    forward_from = getattr(message, "forward_from", None)
    forward_chat = getattr(message, "forward_from_chat", None)

    uploader: Optional[dict] = None
    if from_user is not None:
        uploader = {
            "id": getattr(from_user, "id", None),
            "name": _user_display(from_user),
            "username": getattr(from_user, "username", None),
            "kind": "user",
            "forwarded_from": None,
        }
    elif sender_chat is not None:
        uploader = {
            "id": getattr(sender_chat, "id", None),
            "name": getattr(sender_chat, "title", None) or "Channel",
            "username": getattr(sender_chat, "username", None),
            "kind": "channel",
            "forwarded_from": None,
        }

    forwarded_from: Optional[str] = None
    if forward_from is not None:
        forwarded_from = _user_display(forward_from)
    elif forward_chat is not None:
        forwarded_from = getattr(forward_chat, "title", None)

    if forwarded_from:
        if uploader is None:
            uploader = {
                "id": None,
                "name": "",
                "username": None,
                "kind": "forward",
                "forwarded_from": forwarded_from,
            }
        else:
            uploader["forwarded_from"] = forwarded_from

    return uploader
