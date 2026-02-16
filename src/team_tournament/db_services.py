from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.decorators import handle_service_exceptions
from src.core.exceptions import NotFoundError
from src.core.models import BaseServiceDB, TeamDB, TeamTournamentDB
from src.core.models.base import Database

from ..logging_config import get_logger
from .schemas import TeamTournamentSchemaCreate

ITEM = "TEAM_TOURNAMENT"


class TeamTournamentServiceDB(BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(database, TeamTournamentDB)
        self.logger = get_logger("TeamTournamentServiceDB", self)
        self.logger.debug("Initialized TeamTournamentServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: TeamTournamentSchemaCreate,
        *,
        session: AsyncSession | None = None,
    ) -> TeamTournamentDB:
        self.logger.debug(f"Creat {ITEM} relation:{item}")
        is_relation_exist = await self.get_team_tournament_relation(
            item.team_id, item.tournament_id, session=session
        )
        if is_relation_exist:
            return is_relation_exist
        return await super().create(item)

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching relation", return_value_on_not_found=None
    )
    async def get_team_tournament_relation(
        self,
        team_id: int,
        tournament_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> TeamTournamentDB | None:
        self.logger.debug(f"Get {ITEM} relation: team_id:{team_id} tournament_id:{tournament_id}")
        if session is not None:
            return await self._get_team_tournament_relation_with_session(
                team_id, tournament_id, session
            )
        async with self.db.get_session_maker()() as new_session:
            return await self._get_team_tournament_relation_with_session(
                team_id, tournament_id, new_session
            )

    async def _get_team_tournament_relation_with_session(
        self,
        team_id: int,
        tournament_id: int,
        session: AsyncSession,
    ) -> TeamTournamentDB | None:
        result = await session.execute(
            select(TeamTournamentDB).where(
                (TeamTournamentDB.team_id == team_id)
                & (TeamTournamentDB.tournament_id == tournament_id)
            )
        )
        team_tournament = result.scalars().one_or_none()
        return team_tournament

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching related teams", return_value_on_not_found=[]
    )
    async def get_related_teams(self, tournament_id: int) -> list[TeamDB]:
        self.logger.debug(f"Get {ITEM} related teams for tournament_id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            result = await session.execute(
                select(TeamDB)
                .join(TeamTournamentDB)
                .where(TeamTournamentDB.tournament_id == tournament_id)
                .order_by(TeamDB.title)
            )
            teams = result.scalars().all()
            return teams

    @handle_service_exceptions(
        item_name=ITEM, operation="deleting relation", reraise_not_found=True
    )
    async def delete_relation_by_team_and_tournament_id(
        self, team_id: int, tournament_id: int
    ) -> TeamTournamentDB:
        self.logger.debug(f"Delete {ITEM} team_id:{team_id} tournament_id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            result = await session.execute(
                select(TeamTournamentDB).where(
                    (TeamTournamentDB.team_id == team_id)
                    & (TeamTournamentDB.tournament_id == tournament_id)
                )
            )
            item = result.scalars().one_or_none()

            if not item:
                raise NotFoundError(
                    f"Team tournament relation not found for team_id:{team_id} tournament_id:{tournament_id}"
                )

            await session.delete(item)
            await session.commit()
            return item
