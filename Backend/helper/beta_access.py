from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend.config import Telegram


def beta_enabled() -> bool:
    return bool(getattr(Telegram, "PUBLIC_BETA_ENABLED", False))


def invited_user_ids() -> set[int]:
    ids = set(getattr(Telegram, "BETA_ALLOWED_USER_IDS", []) or [])
    ids.update(getattr(Telegram, "BETA_EXEMPT_USER_IDS", []) or [])
    if getattr(Telegram, "OWNER_ID", 0):
        ids.add(int(Telegram.OWNER_ID))
    ids.update(getattr(Telegram, "APPROVER_IDS", []) or [])
    return ids


def is_beta_invited(user_id: Optional[int]) -> bool:
    if not beta_enabled() or not getattr(Telegram, "BETA_INVITE_ONLY", True):
        return True
    if user_id is None:
        return False
    return int(user_id) in invited_user_ids()


def is_exempt_token(token_data: Optional[dict]) -> bool:
    if not token_data:
        return False
    token_name = str(token_data.get("name") or "").strip().lower()
    exempt_names = {str(x).strip().lower() for x in getattr(Telegram, "BETA_EXEMPT_TOKEN_NAMES", []) or []}
    if token_name and token_name in exempt_names:
        return True
    token = str(token_data.get("token") or "").strip()
    if token and token in set(getattr(Telegram, "BETA_EXEMPT_TOKENS", []) or []):
        return True
    try:
        user_id = int(token_data.get("user_id"))
    except Exception:
        user_id = None
    return user_id is not None and user_id in set(getattr(Telegram, "BETA_EXEMPT_USER_IDS", []) or [])


def terms_required() -> bool:
    return beta_enabled() and bool(getattr(Telegram, "REQUIRE_TERMS_ACCEPTANCE", True))


def accepted_terms(user: Optional[dict]) -> bool:
    if not terms_required():
        return True
    if not user:
        return False
    accepted = user.get("terms") or {}
    return (
        accepted.get("version") == getattr(Telegram, "TERMS_VERSION", "")
        and bool(accepted.get("accepted_at"))
    )


def terms_links_text() -> str:
    base = (Telegram.BASE_URL or "").rstrip("/")
    if not base:
        return "Please review the service terms, privacy policy, acceptable-use policy, and takedown policy before continuing."
    return (
        "Please review these before continuing:\n"
        f"Terms: {base}/terms\n"
        f"Privacy: {base}/privacy\n"
        f"Acceptable use: {base}/acceptable-use\n"
        f"Takedown: {base}/takedown"
    )


def terms_keyboard() -> InlineKeyboardMarkup:
    base = (Telegram.BASE_URL or "").rstrip("/")
    rows: list[list[InlineKeyboardButton]] = []
    if base:
        rows.extend(
            [
                [
                    InlineKeyboardButton("Terms", url=f"{base}/terms"),
                    InlineKeyboardButton("Privacy", url=f"{base}/privacy"),
                ],
                [
                    InlineKeyboardButton("Acceptable Use", url=f"{base}/acceptable-use"),
                    InlineKeyboardButton("Takedown", url=f"{base}/takedown"),
                ],
            ]
        )
    rows.append([InlineKeyboardButton("I Accept", callback_data="accept_terms")])
    return InlineKeyboardMarkup(rows)


def waitlist_message() -> str:
    return str(
        getattr(
            Telegram,
            "BETA_WAITLIST_MESSAGE",
            "This private beta is invite-only right now. Please contact the admin for access.",
        )
    )


def default_token_limits() -> tuple[float, float, int]:
    return (
        float(getattr(Telegram, "DEFAULT_TOKEN_DAILY_LIMIT_GB", 25) or 25),
        float(getattr(Telegram, "DEFAULT_TOKEN_MONTHLY_LIMIT_GB", 300) or 300),
        int(getattr(Telegram, "DEFAULT_TOKEN_MAX_ACTIVE_STREAMS", 2) or 2),
    )


def terms_record(user_id: int, ip: str | None = None) -> dict[str, Any]:
    record = {
        "version": getattr(Telegram, "TERMS_VERSION", ""),
        "accepted_at": datetime.utcnow(),
        "user_id": int(user_id),
    }
    if ip:
        record["ip"] = ip
    return record
