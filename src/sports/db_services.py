from typing import cast

from sqlalchemy import select

from src.core.decorators import handle_service_exceptions
from src.core.models import (
    BaseServiceDB,
    PlayerDB,
    PositionDB,
    SportDB,
    TeamDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.service_registry import ServiceRegistryAccessorMixin

from ..logging_config import get_logger
from .schemas import SportSchemaCreate, SportSchemaUpdate

ITEM = "SPORT"


def _invalidate_sport_cache(sport_id: int | None = None) -> None:
    from src.matches.match_data_cache_service import MatchDataCacheService

    MatchDataCacheService.invalidate_sport_cache_all(sport_id)


class SportServiceDB(ServiceRegistryAccessorMixin, BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(
            database,
            model=SportDB,
        )
        self.logger = get_logger("SportServiceDB", self)
        self.logger.debug("Initialized SportServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(self, item: SportSchemaCreate) -> SportDB:
        self.logger.debug(f"Creat {ITEM}:{item}")
        created = cast(SportDB, await super().create(item))
        _invalidate_sport_cache(created.id)
        return created

    @handle_service_exceptions(item_name=ITEM, operation="updating", reraise_not_found=True)
    async def update(
        self,
        item_id: int,
        item: SportSchemaUpdate,
        **kwargs,
    ) -> SportDB:
        self.logger.debug(f"Update {ITEM} with id:{item_id}")
        updated = cast(
            SportDB,
            await super().update(
                item_id,
                item,
                **kwargs,
            ),
        )
        _invalidate_sport_cache(item_id)
        return updated

    async def delete(self, item_id: int) -> dict[str, int]:
        deleted = cast(dict[str, int], await super().delete(item_id))
        _invalidate_sport_cache(item_id)
        return deleted

    async def get_tournaments_by_sport(
        self,
        sport_id: int,
        key: str = "id",
    ) -> list[TournamentDB]:
        self.logger.debug(f"Get tournaments by {ITEM} id:{sport_id}")
        return await self.get_related_item_level_one_by_key_and_value(
            key,
            sport_id,
            "tournaments",
        )

    async def get_teams_by_sport(
        self,
        sport_id: int,
        key: str = "id",
    ) -> list[TeamDB]:
        self.logger.debug(f"Get teams by {ITEM} id:{sport_id}")
        return await self.get_related_item_level_one_by_key_and_value(
            key,
            sport_id,
            "teams",
        )

    async def get_players_by_sport(
        self,
        sport_id: int,
        key: str = "id",
    ) -> list[PlayerDB]:
        self.logger.debug(f"Get players by {ITEM} id:{sport_id}")
        return await self.get_related_item_level_one_by_key_and_value(
            key,
            sport_id,
            "players",
        )

    async def get_positions_by_sport(
        self,
        sport_id: int,
        key: str = "id",
    ) -> list[PositionDB]:
        self.logger.debug(f"Get positions by {ITEM} id:{sport_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PositionDB).where(PositionDB.sport_id == sport_id).order_by(PositionDB.title)
            )
            results = await session.execute(stmt)
            return results.scalars().all()
