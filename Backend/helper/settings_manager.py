from __future__ import annotations

from typing import Any, Dict, List

from Backend.config import Telegram
from Backend.helper.passwords import hash_password, is_hashed
from Backend.logger import LOGGER


DEFAULTS: Dict[str, Any] = {
    "replace_mode": True,
    "hide_catalog": False,
    "auth_channels": [],
    "admin_username": "",
    "admin_password": "",
    "session_secret": "",
    "subscription": False,
    "subscription_group_id": 0,
    "approver_ids": [],
    "payment_instructions": "",
    "payment_qr_url": "",
    "http_proxy_url": "",
    "show_proxy_and_non_proxy_both": False,
    "anime_channels": [],
    "global_search": False,
    "global_search_channels": [],
    "content_requests_enabled": False,
    "content_requests_beta_only": True,
    "announce_new_content": False,
    "announcement_channel": "",
    "updated_at": None,
}


def _seed_from_env() -> Dict[str, Any]:
    seed = dict(DEFAULTS)
    seed.update(
        {
            "replace_mode": bool(Telegram.REPLACE_MODE),
            "hide_catalog": bool(Telegram.HIDE_CATALOG),
            "auth_channels": list(Telegram.AUTH_CHANNEL),
            "admin_username": Telegram.ADMIN_USERNAME,
            "admin_password": hash_password(Telegram.ADMIN_PASSWORD),
            "session_secret": Telegram.SESSION_SECRET,
            "subscription": bool(Telegram.SUBSCRIPTION),
            "subscription_group_id": int(Telegram.SUBSCRIPTION_GROUP_ID or 0),
            "approver_ids": list(getattr(Telegram, "APPROVER_IDS", []) or []),
            "http_proxy_url": Telegram.HTTP_PROXY_URL,
            "show_proxy_and_non_proxy_both": bool(Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH),
            "anime_channels": list(getattr(Telegram, "ANIME_CHANNELS", []) or []),
            "global_search_channels": list(getattr(Telegram, "GLOBAL_SEARCH_CHANNELS", []) or []),
            "global_search": bool(getattr(Telegram, "GLOBAL_SEARCH", False)),
            "content_requests_enabled": bool(getattr(Telegram, "CONTENT_REQUESTS_ENABLED", False)),
            "content_requests_beta_only": bool(getattr(Telegram, "CONTENT_REQUESTS_BETA_ONLY", True)),
            "announce_new_content": bool(getattr(Telegram, "ANNOUNCE_NEW_CONTENT", False)),
            "announcement_channel": getattr(Telegram, "ANNOUNCEMENT_CHANNEL", ""),
        }
    )
    return seed


class Settings:
    def __init__(self, data: Dict[str, Any] | None = None) -> None:
        merged = dict(DEFAULTS)
        merged.update({k: v for k, v in (data or {}).items() if k != "_id"})
        self._data = merged

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    @property
    def replace_mode(self) -> bool:
        return bool(self._data.get("replace_mode", True))

    @property
    def hide_catalog(self) -> bool:
        return bool(self._data.get("hide_catalog", False))

    @property
    def subscription(self) -> bool:
        return bool(self._data.get("subscription", False))

    @property
    def auth_channels(self) -> List[str]:
        return [str(x).strip() for x in (self._data.get("auth_channels") or []) if str(x).strip()]

    @property
    def anime_channels(self) -> List[str]:
        return [str(x).strip() for x in (self._data.get("anime_channels") or []) if str(x).strip()]

    @property
    def global_search_channels(self) -> List[str]:
        return [str(x).strip() for x in (self._data.get("global_search_channels") or []) if str(x).strip()]

    @property
    def admin_username(self) -> str:
        return str(self._data.get("admin_username") or Telegram.ADMIN_USERNAME)

    @property
    def admin_password(self) -> str:
        return str(self._data.get("admin_password") or "")

    @property
    def session_secret(self) -> str:
        return str(self._data.get("session_secret") or Telegram.SESSION_SECRET)

    @property
    def base_url(self) -> str:
        return Telegram.BASE_URL

    @property
    def content_requests_enabled(self) -> bool:
        return bool(self._data.get("content_requests_enabled", False))

    @property
    def content_requests_beta_only(self) -> bool:
        return bool(self._data.get("content_requests_beta_only", True))

    @property
    def announce_new_content(self) -> bool:
        return bool(self._data.get("announce_new_content", False))

    @property
    def announcement_channel(self) -> str:
        return str(self._data.get("announcement_channel") or "").strip()


class SettingsManager:
    _current: Settings | None = None

    @classmethod
    async def initialize(cls, db) -> None:
        raw = await db.get_settings()
        if not raw:
            raw = _seed_from_env()
            await db.save_settings(raw)
        changed = False
        if not raw.get("admin_password"):
            raw["admin_password"] = hash_password(Telegram.ADMIN_PASSWORD)
            changed = True
        elif raw.get("admin_password") and not is_hashed(raw.get("admin_password")):
            raw["admin_password"] = hash_password(str(raw.get("admin_password")))
            changed = True
        if not raw.get("session_secret"):
            raw["session_secret"] = Telegram.SESSION_SECRET
            changed = True
        if changed:
            await db.save_settings(raw)
        cls._current = Settings(raw)
        cls.apply_to_runtime(cls._current)
        LOGGER.info("SettingsManager loaded settings.")

    @classmethod
    def current(cls) -> Settings:
        if cls._current is None:
            cls._current = Settings(_seed_from_env())
        return cls._current

    @classmethod
    def apply_to_runtime(cls, settings: Settings) -> None:
        data = settings.to_dict()
        Telegram.REPLACE_MODE = bool(data.get("replace_mode", Telegram.REPLACE_MODE))
        Telegram.HIDE_CATALOG = bool(data.get("hide_catalog", Telegram.HIDE_CATALOG))
        Telegram.ADMIN_USERNAME = str(data.get("admin_username") or Telegram.ADMIN_USERNAME)
        Telegram.SESSION_SECRET = str(data.get("session_secret") or Telegram.SESSION_SECRET)
        Telegram.AUTH_CHANNEL = [str(x).strip() for x in (data.get("auth_channels") or []) if str(x).strip()]
        Telegram.SUBSCRIPTION = bool(data.get("subscription", Telegram.SUBSCRIPTION))
        Telegram.SUBSCRIPTION_GROUP_ID = int(data.get("subscription_group_id") or 0)
        Telegram.APPROVER_IDS = [int(x) for x in (data.get("approver_ids") or []) if str(x).strip().lstrip("-").isdigit()]
        Telegram.HTTP_PROXY_URL = str(data.get("http_proxy_url") or "")
        Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH = bool(data.get("show_proxy_and_non_proxy_both", False))
        Telegram.ANIME_CHANNELS = [str(x).strip() for x in (data.get("anime_channels") or []) if str(x).strip()]
        Telegram.GLOBAL_SEARCH = bool(data.get("global_search", False))
        Telegram.GLOBAL_SEARCH_CHANNELS = [str(x).strip() for x in (data.get("global_search_channels") or []) if str(x).strip()]
        Telegram.CONTENT_REQUESTS_ENABLED = bool(data.get("content_requests_enabled", False))
        Telegram.CONTENT_REQUESTS_BETA_ONLY = bool(data.get("content_requests_beta_only", True))
        Telegram.ANNOUNCE_NEW_CONTENT = bool(data.get("announce_new_content", False))
        Telegram.ANNOUNCEMENT_CHANNEL = str(data.get("announcement_channel") or "")

    @classmethod
    async def update(cls, db, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = cls.current().to_dict()
        allowed = set(DEFAULTS)
        merged = dict(current)
        incoming = {k: v for k, v in (payload or {}).items() if k in allowed}
        if "admin_password" in incoming:
            raw_password = str(incoming.get("admin_password") or "").strip()
            if raw_password:
                incoming["admin_password"] = raw_password if is_hashed(raw_password) else hash_password(raw_password)
            else:
                incoming.pop("admin_password", None)
        if "session_secret" in incoming:
            raw_secret = str(incoming.get("session_secret") or "").strip()
            if raw_secret:
                incoming["session_secret"] = raw_secret
            else:
                incoming.pop("session_secret", None)
        merged.update(incoming)

        for key in ("auth_channels", "anime_channels", "global_search_channels"):
            merged[key] = [str(x).strip() for x in (merged.get(key) or []) if str(x).strip()]
        merged["approver_ids"] = [
            int(x) for x in (merged.get("approver_ids") or [])
            if str(x).strip().lstrip("-").isdigit()
        ]
        merged["subscription_group_id"] = int(merged.get("subscription_group_id") or 0)
        if merged.get("global_search") and not getattr(Telegram, "USER_SESSION_STRING", ""):
            merged["global_search"] = False

        await db.save_settings(merged)
        cls._current = Settings(merged)
        cls.apply_to_runtime(cls._current)

        results: Dict[str, Any] = {
            "auth_channels": f"{len(Telegram.AUTH_CHANNEL)} channel(s) active",
            "restart_required": [],
        }
        return results
