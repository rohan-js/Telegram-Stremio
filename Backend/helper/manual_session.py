from __future__ import annotations

from asyncio import Lock
from copy import deepcopy
from typing import Optional


def is_personal_media(tmdb_id) -> bool:
    try:
        return int(tmdb_id) < 0
    except (TypeError, ValueError):
        return False


def max_episode(document: dict, season_number: int) -> int:
    for season in document.get("seasons", []) or []:
        if int(season.get("season_number", -1)) == int(season_number):
            return max(
                (int(item.get("episode_number", 0)) for item in season.get("episodes", []) or []),
                default=0,
            )
    return 0


class ManualSessionManager:
    """Single-process, restart-safe-by-reset upload session state."""

    def __init__(self) -> None:
        self._session: Optional[dict] = None
        self._lock = Lock()
        self._next_episode: dict[tuple[int, int], int] = {}
        self._split_episode: dict[tuple[int, int, str], int] = {}

    def current(self) -> Optional[dict]:
        return deepcopy(self._session)

    async def activate(self, session: dict) -> dict:
        async with self._lock:
            self._session = deepcopy(session)
            self._next_episode.clear()
            self._split_episode.clear()
            return deepcopy(self._session)

    async def clear(self) -> None:
        async with self._lock:
            self._session = None
            self._next_episode.clear()
            self._split_episode.clear()

    async def assign_episode(
        self,
        document: dict,
        season_number: int,
        *,
        explicit_episode: int | None = None,
        split_key: str | None = None,
    ) -> int:
        async with self._lock:
            tmdb_id = int(document.get("tmdb_id"))
            season_number = int(season_number)
            if explicit_episode is not None:
                return int(explicit_episode)

            split_identity = (tmdb_id, season_number, str(split_key)) if split_key else None
            if split_identity and split_identity in self._split_episode:
                return self._split_episode[split_identity]

            counter_key = (tmdb_id, season_number)
            episode_number = self._next_episode.get(counter_key)
            if episode_number is None:
                episode_number = max_episode(document, season_number) + 1
            self._next_episode[counter_key] = episode_number + 1
            if split_identity:
                self._split_episode[split_identity] = episode_number
            return episode_number


manual_session_manager = ManualSessionManager()
