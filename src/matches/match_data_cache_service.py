import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar
from weakref import WeakSet

from src.core.models.base import Database
from src.logging_config import get_logger

if TYPE_CHECKING:
    pass

ITEM = "MATCH_DATA_CACHE"
REFERENCE_CACHE_KEY_TEAM = "team"
REFERENCE_CACHE_KEY_TOURNAMENT = "tournament"
REFERENCE_CACHE_KEY_SPORT = "sport"
REFERENCE_CACHE_KEY_PRESET = "sport_scoreboard_preset"

TEAM_CACHE_TTL_SECONDS = 5 * 60
TOURNAMENT_CACHE_TTL_SECONDS = 5 * 60
SPORT_CACHE_TTL_SECONDS = 30 * 60
PRESET_CACHE_TTL_SECONDS = 30 * 60

T = TypeVar("T")


class MatchDataCacheService:
    _instances: ClassVar[WeakSet["MatchDataCacheService"]] = WeakSet()

    def __init__(self, database: Database) -> None:
        self.db = database
        self.logger = get_logger("MatchDataCacheService", self)
        self.logger.debug("Initialized MatchDataCacheService")
        self._cache: dict[str, dict] = {}
        self._reference_cache: dict[str, tuple[float, Any]] = {}
        self.__class__._instances.add(self)

    @staticmethod
    def _reference_cache_key(entity: str, entity_id: int) -> str:
        return f"{entity}:{entity_id}"

    def _get_reference_cache_item(self, entity: str, entity_id: int) -> Any | None:
        cache_key = self._reference_cache_key(entity, entity_id)
        cached = self._reference_cache.get(cache_key)
        if cached is None:
            return None

        expires_at, payload = cached
        if time.monotonic() >= expires_at:
            self._reference_cache.pop(cache_key, None)
            self.logger.debug(f"Expired reference cache for {cache_key}")
            return None
        return payload

    def _set_reference_cache_item(
        self, entity: str, entity_id: int, ttl_seconds: int, payload: Any
    ) -> None:
        cache_key = self._reference_cache_key(entity, entity_id)
        self._reference_cache[cache_key] = (time.monotonic() + ttl_seconds, payload)
        self.logger.debug(f"Cached reference data for {cache_key} with ttl={ttl_seconds}s")

    async def _get_or_fetch_reference(
        self,
        *,
        entity: str,
        entity_id: int,
        ttl_seconds: int,
        fetcher: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        cached = self._get_reference_cache_item(entity, entity_id)
        if cached is not None:
            return cached

        result = await fetcher()
        if result is not None:
            self._set_reference_cache_item(entity, entity_id, ttl_seconds, result)
        return result

    async def get_or_fetch_team(
        self,
        team_id: int,
        fetcher: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        return await self._get_or_fetch_reference(
            entity=REFERENCE_CACHE_KEY_TEAM,
            entity_id=team_id,
            ttl_seconds=TEAM_CACHE_TTL_SECONDS,
            fetcher=fetcher,
        )

    async def get_or_fetch_tournament(
        self,
        tournament_id: int,
        fetcher: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        return await self._get_or_fetch_reference(
            entity=REFERENCE_CACHE_KEY_TOURNAMENT,
            entity_id=tournament_id,
            ttl_seconds=TOURNAMENT_CACHE_TTL_SECONDS,
            fetcher=fetcher,
        )

    async def get_or_fetch_sport(
        self,
        sport_id: int,
        fetcher: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        return await self._get_or_fetch_reference(
            entity=REFERENCE_CACHE_KEY_SPORT,
            entity_id=sport_id,
            ttl_seconds=SPORT_CACHE_TTL_SECONDS,
            fetcher=fetcher,
        )

    async def get_or_fetch_sport_scoreboard_preset(
        self,
        preset_id: int,
        fetcher: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        return await self._get_or_fetch_reference(
            entity=REFERENCE_CACHE_KEY_PRESET,
            entity_id=preset_id,
            ttl_seconds=PRESET_CACHE_TTL_SECONDS,
            fetcher=fetcher,
        )

    async def get_or_fetch_match_data(self, match_id: int) -> dict | None:
        cache_key = f"match-update:{match_id}"
        if cache_key in self._cache:
            self.logger.debug(f"Returning cached match data for match {match_id}")
            return self._cache[cache_key]

        self.logger.debug(f"Fetching match data for match {match_id}")
        from src.helpers.fetch_helpers import fetch_with_scoreboard_data

        result = await fetch_with_scoreboard_data(match_id, database=self.db)
        if result and result.get("status_code") == 200:
            self._cache[cache_key] = result
            self.logger.debug(f"Cached match data for match {match_id}")
            return result
        return None

    async def get_or_fetch_gameclock(self, match_id: int) -> dict | None:
        cache_key = f"gameclock-update:{match_id}"
        if cache_key in self._cache:
            self.logger.debug(f"Returning cached gameclock for match {match_id}")
            return self._cache[cache_key]

        self.logger.debug(f"Fetching gameclock for match {match_id}")
        from src.helpers.fetch_helpers import fetch_gameclock

        result = await fetch_gameclock(match_id, database=self.db)
        if result and "gameclock" in result:
            self._cache[cache_key] = result
            self.logger.debug(f"Cached gameclock for match {match_id}")
            return result
        return None

    async def get_or_fetch_playclock(self, match_id: int) -> dict | None:
        cache_key = f"playclock-update:{match_id}"
        if cache_key in self._cache:
            self.logger.debug(f"Returning cached playclock for match {match_id}")
            return self._cache[cache_key]

        self.logger.debug(f"Fetching playclock for match {match_id}")
        from src.helpers.fetch_helpers import fetch_playclock

        result = await fetch_playclock(match_id, database=self.db)
        if result and "playclock" in result:
            self._cache[cache_key] = result
            self.logger.debug(f"Cached playclock for match {match_id}")
            return result
        return None

    def invalidate_match_data(self, match_id: int) -> None:
        cache_key = f"match-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated match data cache for match {match_id}")

    def invalidate_gameclock(self, match_id: int) -> None:
        cache_key = f"gameclock-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated gameclock cache for match {match_id}")

    def invalidate_playclock(self, match_id: int) -> None:
        cache_key = f"playclock-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated playclock cache for match {match_id}")

    async def get_or_fetch_event_data(self, match_id: int) -> dict | None:
        cache_key = f"event-update:{match_id}"
        if cache_key in self._cache:
            self.logger.debug(f"Returning cached event data for match {match_id}")
            return self._cache[cache_key]

        self.logger.debug(f"Fetching event data for match {match_id}")
        from src.helpers.fetch_helpers import fetch_event

        result = await fetch_event(match_id, database=self.db)
        if result and result.get("status_code") == 200:
            self._cache[cache_key] = result
            self.logger.debug(f"Cached event data for match {match_id}")
            return result
        return None

    def invalidate_event_data(self, match_id: int) -> None:
        cache_key = f"event-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated event data cache for match {match_id}")

    async def get_or_fetch_stats(self, match_id: int) -> dict | None:
        cache_key = f"statistics-update:{match_id}"
        if cache_key in self._cache:
            self.logger.debug(f"Returning cached stats for match {match_id}")
            return self._cache[cache_key]

        self.logger.debug(f"Fetching stats for match {match_id}")
        from src.helpers.fetch_helpers import fetch_stats

        result = await fetch_stats(match_id, database=self.db)
        if result and "statistics" in result:
            self._cache[cache_key] = result
            self.logger.debug(f"Cached stats for match {match_id}")
            return result
        return None

    def invalidate_stats(self, match_id: int) -> None:
        cache_key = f"statistics-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated stats cache for match {match_id}")

    def invalidate_players(self, match_id: int) -> None:
        cache_key = f"players-update:{match_id}"
        if cache_key in self._cache:
            del self._cache[cache_key]
            self.logger.debug(f"Invalidated players cache for match {match_id}")

    def _invalidate_reference_cache(
        self,
        entity: str,
        entity_id: int | None = None,
    ) -> None:
        if entity_id is not None:
            cache_key = self._reference_cache_key(entity, entity_id)
            if cache_key in self._reference_cache:
                del self._reference_cache[cache_key]
                self.logger.debug(f"Invalidated reference cache for {cache_key}")
            return

        keys_to_delete = [
            cache_key
            for cache_key in self._reference_cache.keys()
            if cache_key.startswith(f"{entity}:")
        ]
        for cache_key in keys_to_delete:
            del self._reference_cache[cache_key]

        if keys_to_delete:
            self.logger.debug(
                f"Invalidated {len(keys_to_delete)} reference cache entries for {entity}"
            )

    def invalidate_team(self, team_id: int | None = None) -> None:
        self._invalidate_reference_cache(REFERENCE_CACHE_KEY_TEAM, team_id)

    def invalidate_tournament(self, tournament_id: int | None = None) -> None:
        self._invalidate_reference_cache(REFERENCE_CACHE_KEY_TOURNAMENT, tournament_id)

    def invalidate_sport(self, sport_id: int | None = None) -> None:
        self._invalidate_reference_cache(REFERENCE_CACHE_KEY_SPORT, sport_id)

    def invalidate_sport_scoreboard_preset(self, preset_id: int | None = None) -> None:
        self._invalidate_reference_cache(REFERENCE_CACHE_KEY_PRESET, preset_id)

    @classmethod
    def invalidate_team_cache_all(cls, team_id: int | None = None) -> None:
        for instance in list(cls._instances):
            instance.invalidate_team(team_id)

    @classmethod
    def invalidate_tournament_cache_all(cls, tournament_id: int | None = None) -> None:
        for instance in list(cls._instances):
            instance.invalidate_tournament(tournament_id)

    @classmethod
    def invalidate_sport_cache_all(cls, sport_id: int | None = None) -> None:
        for instance in list(cls._instances):
            instance.invalidate_sport(sport_id)

    @classmethod
    def invalidate_sport_scoreboard_preset_cache_all(cls, preset_id: int | None = None) -> None:
        for instance in list(cls._instances):
            instance.invalidate_sport_scoreboard_preset(preset_id)
