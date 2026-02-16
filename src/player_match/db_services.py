from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.decorators import handle_service_exceptions
from src.core.exceptions import NotFoundError
from src.core.models import (
    BaseServiceDB,
    PlayerDB,
    PlayerMatchDB,
    PlayerTeamTournamentDB,
)
from src.core.models.base import Database
from src.core.service_registry import ServiceRegistryAccessorMixin

from ..logging_config import get_logger
from ..player.schemas import PlayerSchema
from .schemas import PlayerMatchSchemaCreate, PlayerMatchSchemaUpdate

ITEM = "PLAYER_MATCH"


class PlayerMatchServiceDB(ServiceRegistryAccessorMixin, BaseServiceDB):
    def __init__(
        self,
        database: Database,
    ) -> None:
        super().__init__(database, PlayerMatchDB)
        self.logger = get_logger("PlayerMatchServiceDB", self)
        self.logger.debug("Initialized PlayerMatchServiceDB")

    @handle_service_exceptions(
        item_name=ITEM, operation="creating or updating", return_value_on_not_found=None
    )
    async def create_or_update_player_match(
        self,
        p: PlayerMatchSchemaCreate | PlayerMatchSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> PlayerMatchDB | None:
        self.logger.debug(f"Creat or update {ITEM}:{p}")
        if p.player_match_eesl_id and p.match_id:
            self.logger.debug(f"Get {ITEM} by eesl id")
            player_match_from_db = await self.get_player_match_by_match_id_and_eesl_id(
                p.match_id, p.player_match_eesl_id, session=session
            )
            if player_match_from_db and p.match_id == player_match_from_db.match_id:
                if not player_match_from_db.is_start:
                    self.logger.debug(f"{ITEM} exist, updating...")
                    return await self.update_player_match_by_eesl(
                        p.match_id, p.player_match_eesl_id, p, session=session
                    )
                else:
                    self.logger.warning(f"{ITEM} is in start, skipping...")
                    return player_match_from_db
            else:
                self.logger.debug(f"{ITEM} does not exist, creating...")
                return await self.create_new_player_match(
                    p,
                )
        else:
            if p.match_id and p.player_team_tournament_id:
                player_match_from_db = await self.get_players_match_by_match_id(
                    p.match_id, p.player_team_tournament_id
                )
                if player_match_from_db and p.match_id == player_match_from_db.match_id:
                    self.logger.debug(f"{ITEM} already in match, updating...")
                    return await self.update(player_match_from_db.id, p)
            self.logger.debug(f"{ITEM} does not exist, creating...")
            return await self.create_new_player_match(
                p,
            )

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create_new_player_match(
        self,
        p: PlayerMatchSchemaCreate,
    ) -> PlayerMatchDB:
        self.logger.debug(f"Create {ITEM} wit data {p}")
        return await super().create(p)

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching by match and eesl id", return_value_on_not_found=None
    )
    async def get_player_match_by_match_id_and_eesl_id(
        self,
        match_id: int,
        player_match_eesl_id: int | str,
        *,
        session: AsyncSession | None = None,
    ) -> PlayerMatchDB | None:
        self.logger.debug(f"Get {ITEM} by match id:{match_id} and eesl id:{player_match_eesl_id}")
        if session is not None:
            return await self._get_player_match_by_match_id_and_eesl_id_with_session(
                match_id, player_match_eesl_id, session
            )
        async with self.db.get_session_maker()() as new_session:
            return await self._get_player_match_by_match_id_and_eesl_id_with_session(
                match_id, player_match_eesl_id, new_session
            )

    async def _get_player_match_by_match_id_and_eesl_id_with_session(
        self,
        match_id: int,
        player_match_eesl_id: int | str,
        session: AsyncSession,
    ) -> PlayerMatchDB | None:
        stmt = (
            select(PlayerMatchDB)
            .where(PlayerMatchDB.match_id == match_id)
            .where(PlayerMatchDB.player_match_eesl_id == player_match_eesl_id)
        )

        result = await session.execute(stmt)
        player = result.scalars().one_or_none()
        if player:
            self.logger.debug(f"{ITEM} found {player}")
            return player
        else:
            raise NotFoundError(f"{ITEM} not found")

    @handle_service_exceptions(
        item_name=ITEM, operation="updating by eesl", return_value_on_not_found=None
    )
    async def update_player_match_by_eesl(
        self,
        match_id: int,
        eesl_id: int | str,
        new_player: PlayerMatchSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> PlayerMatchDB | None:
        player = await self.get_player_match_by_match_id_and_eesl_id(
            match_id, eesl_id, session=session
        )
        if player:
            updated_player = await self.update(player.id, new_player)
            if updated_player:
                return updated_player
        raise NotFoundError(f"{ITEM} not found to update")

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching by match and team tournament",
        return_value_on_not_found=None,
    )
    async def get_players_match_by_match_id(
        self, match_id: int, player_team_tournament_id: int
    ) -> PlayerMatchDB | None:
        self.logger.debug(f"Get {ITEM} by match id:{match_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerMatchDB)
                .where(PlayerMatchDB.match_id == match_id)
                .where(PlayerMatchDB.player_team_tournament_id == player_team_tournament_id)
            )

            results = await session.execute(stmt)
            players = results.scalars().one_or_none()
            return players

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching player in sport", return_value_on_not_found=None
    )
    async def get_player_in_sport(self, player_id: int) -> PlayerDB | None:
        player_team_tournament_service = self.service_registry.get("player_team_tournament")
        self.logger.debug(f"Get player in sport by player_id:{player_id}")
        return await self.get_nested_related_item_by_id(
            player_id,
            player_team_tournament_service,
            "player_team_tournament",
            "player",
        )

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching player person in match", return_value_on_not_found=None
    )
    async def get_player_person_in_match(self, player_id: int) -> PlayerSchema | None:
        player_service = self.service_registry.get("player")
        self.logger.debug(f"Get {ITEM} in sport with person by player_id:{player_id}")
        p = await self.get_player_in_sport(player_id)
        if p:
            return await player_service.get_player_with_person(p.id)
        return None

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching player team tournament", return_value_on_not_found=None
    )
    async def get_player_in_team_tournament(
        self,
        match_id: int,
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Get player_team_tournament by match_id:{match_id}")
        return await self.get_related_item_level_one_by_id(
            match_id,
            "player_team_tournament",
        )

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching with full data", return_value_on_not_found={}
    )
    async def get_player_in_match_full_data(self, match_player_id: int) -> dict:
        self.logger.debug(f"Get {ITEM} in match with full data")
        match_player = await self.get_by_id(match_player_id)
        team_tournament_player = await self.get_player_in_team_tournament(match_player_id)
        person = await self.get_player_person_in_match(match_player_id)
        position_service = self.service_registry.get("position")
        position = await position_service.get_by_id(match_player.match_position_id)

        return {
            "match_player": match_player,
            "player_team_tournament": team_tournament_player,
            "person": person,
            "position": position,
        }

    async def get_player_match_by_eesl_id(
        self,
        value: int | str,
        field_name: str = "player_match_eesl_id",
    ) -> PlayerMatchDB | None:
        self.logger.debug(f"Get {ITEM} {field_name}:{value}")
        return await self.get_item_by_field_value(
            value=value,
            field_name=field_name,
        )

    @handle_service_exceptions(item_name=ITEM, operation="updating", reraise_not_found=True)
    async def update(
        self,
        item_id: int,
        item: PlayerMatchSchemaUpdate,
        **kwargs,
    ) -> PlayerMatchDB:
        self.logger.debug(f"Update {ITEM}:{item_id}")
        return await super().update(
            item_id,
            item,
            **kwargs,
        )
