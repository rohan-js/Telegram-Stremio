from __future__ import annotations

from typing import Any, Dict, List

from Backend.config import Telegram
from Backend.logger import LOGGER


DEFAULTS: Dict[str, Any] = {
    "replace_mode": True,
    "hide_catalog": False,
    "auth_channels": [],
    "admin_username": "",
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
            "subscription": bool(Telegram.SUBSCRIPTION),
            "subscription_group_id": int(Telegram.SUBSCRIPTION_GROUP_ID or 0),
            "approver_ids": list(getattr(Telegram, "APPROVER_IDS", []) or []),
            "http_proxy_url": Telegram.HTTP_PROXY_URL,
            "show_proxy_and_non_proxy_both": bool(Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH),
            "anime_channels": list(getattr(Telegram, "ANIME_CHANNELS", []) or []),
            "global_search_channels": list(getattr(Telegram, "GLOBAL_SEARCH_CHANNELS", []) or []),
            "global_search": bool(getattr(Telegram, "GLOBAL_SEARCH", False)),
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


class SettingsManager:
    _current: Settings | None = None

    @classmethod
    async def initialize(cls, db) -> None:
        raw = await db.get_settings()
        if not raw:
            raw = _seed_from_env()
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
        Telegram.AUTH_CHANNEL = [str(x).strip() for x in (data.get("auth_channels") or []) if str(x).strip()]
        Telegram.SUBSCRIPTION = bool(data.get("subscription", Telegram.SUBSCRIPTION))
        Telegram.SUBSCRIPTION_GROUP_ID = int(data.get("subscription_group_id") or 0)
        Telegram.APPROVER_IDS = [int(x) for x in (data.get("approver_ids") or []) if str(x).strip().lstrip("-").isdigit()]
        Telegram.HTTP_PROXY_URL = str(data.get("http_proxy_url") or "")
        Telegram.SHOW_PROXY_AND_NON_PROXY_BOTH = bool(data.get("show_proxy_and_non_proxy_both", False))
        Telegram.ANIME_CHANNELS = [str(x).strip() for x in (data.get("anime_channels") or []) if str(x).strip()]
        Telegram.GLOBAL_SEARCH = bool(data.get("global_search", False))
        Telegram.GLOBAL_SEARCH_CHANNELS = [str(x).strip() for x in (data.get("global_search_channels") or []) if str(x).strip()]

    @classmethod
    async def update(cls, db, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = cls.current().to_dict()
        allowed = set(DEFAULTS)
        merged = dict(current)
        merged.update({k: v for k, v in (payload or {}).items() if k in allowed})

        for key in ("auth_channels", "anime_channels", "global_search_channels"):
            merged[key] = [str(x).strip() for x in (merged.get(key) or []) if str(x).strip()]
        merged["approver_ids"] = [
            int(x) for x in (merged.get("approver_ids") or [])
            if str(x).strip().lstrip("-").isdigit()
        ]
        merged["subscription_group_id"] = int(merged.get("subscription_group_id") or 0)

        await db.save_settings(merged)
        cls._current = Settings(merged)
        cls.apply_to_runtime(cls._current)

        results: Dict[str, Any] = {
            "auth_channels": f"{len(Telegram.AUTH_CHANNEL)} channel(s) active",
            "restart_required": [],
        }
        return results
