from typing import cast

from fastapi import HTTPException
from sqlalchemy import not_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql import func

from src.core.decorators import handle_service_exceptions
from src.core.models import (
    BaseServiceDB,
    MatchDB,
    PlayerDB,
    PlayerMatchDB,
    PlayerTeamTournamentDB,
    PositionDB,
    SponsorDB,
    SponsorLineDB,
    SportDB,
    TeamDB,
    TeamTournamentDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.schema_helpers import PaginationMetadata

from ..logging_config import get_logger
from ..player.schemas import PaginatedPlayerWithDetailsResponse, PlayerWithDetailsSchema
from ..sponsor_lines.db_services import SponsorLineServiceDB
from ..teams.schemas import PaginatedTeamResponse, TeamSchema
from .schemas import (
    MoveTournamentConflictEntry,
    MoveTournamentConflicts,
    MoveTournamentMissingPosition,
    MoveTournamentToSportResponse,
    MoveTournamentUpdatedCounts,
    PaginatedTournamentWithDetailsResponse,
    TournamentSchemaCreate,
    TournamentSchemaUpdate,
)

ITEM = "TOURNAMENT"


def _invalidate_tournament_cache(tournament_id: int | None = None) -> None:
    from src.matches.match_data_cache_service import MatchDataCacheService

    MatchDataCacheService.invalidate_tournament_cache_all(tournament_id)


def _invalidate_team_cache(team_id: int | None = None) -> None:
    from src.matches.match_data_cache_service import MatchDataCacheService

    MatchDataCacheService.invalidate_team_cache_all(team_id)


class TournamentServiceDB(BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(
            database,
            TournamentDB,
        )
        self.logger = get_logger("TournamentServiceDB", self)
        self.logger.debug("Initialized TournamentServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: TournamentSchemaCreate | TournamentSchemaUpdate,
    ) -> TournamentDB:
        self.logger.debug(f"Create new {ITEM}:{item}")
        created = cast(TournamentDB, await super().create(item))
        _invalidate_tournament_cache(created.id)
        return created

    async def create_or_update_tournament(
        self,
        t: TournamentSchemaCreate | TournamentSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> TournamentDB:
        return await super().create_or_update(t, eesl_field_name="tournament_eesl_id")

    async def get_tournament_by_eesl_id(
        self,
        value: int | str,
        field_name: str = "tournament_eesl_id",
        *,
        session: AsyncSession | None = None,
    ) -> TournamentDB | None:
        if session is not None:
            return await self._get_tournament_by_field_with_session(value, field_name, session)
        self.logger.debug(f"Get {ITEM} {field_name}:{value}")
        return await self.get_item_by_field_value(
            value=value,
            field_name=field_name,
        )

    async def _get_tournament_by_field_with_session(
        self,
        value: int | str,
        field_name: str,
        session: AsyncSession,
    ) -> TournamentDB | None:
        self.logger.debug(f"Get {ITEM} {field_name}:{value} with session")
        result = await session.scalars(
            select(TournamentDB).where(getattr(TournamentDB, field_name) == value)
        )
        return result.one_or_none()

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching tournament with details",
        return_value_on_not_found=None,
    )
    async def get_tournament_with_details(
        self,
        tournament_id: int,
    ) -> TournamentDB | None:
        self.logger.debug(f"Get {ITEM} with details id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(TournamentDB)
                .where(TournamentDB.id == tournament_id)
                .options(
                    joinedload(TournamentDB.season),
                    joinedload(TournamentDB.sport),
                    selectinload(TournamentDB.teams),
                    joinedload(TournamentDB.main_sponsor),
                    joinedload(TournamentDB.sponsor_line),
                )
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def update(
        self,
        item_id: int,
        item: TournamentSchemaUpdate,
        **kwargs,
    ) -> TournamentDB:
        self.logger.debug(f"Update {ITEM}:{item_id}")
        updated = cast(
            TournamentDB,
            await super().update(
                item_id,
                item,
                **kwargs,
            ),
        )
        _invalidate_tournament_cache(item_id)
        return updated

    async def delete(self, item_id: int) -> dict[str, int]:
        deleted = cast(dict[str, int], await super().delete(item_id))
        _invalidate_tournament_cache(item_id)
        return deleted

    async def get_teams_by_tournament(
        self,
        tournament_id: int,
    ) -> list[TeamDB]:
        self.logger.debug(f"Get teams by {ITEM} id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(TeamDB)
                .join(TeamTournamentDB, TeamDB.id == TeamTournamentDB.team_id)
                .where(TeamTournamentDB.tournament_id == tournament_id)
                .order_by(TeamDB.title)
            )
            results = await session.execute(stmt)
            teams = results.scalars().all()
            return teams

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching teams in tournament with pagination",
        return_value_on_not_found=None,
    )
    async def get_teams_by_tournament_with_pagination(
        self,
        tournament_id: int,
        search_query: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "title",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedTeamResponse:
        self.logger.debug(
            f"Search teams in tournament id:{tournament_id}: query={search_query}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(TeamDB)
                .join(TeamTournamentDB, TeamDB.id == TeamTournamentDB.team_id)
                .where(TeamTournamentDB.tournament_id == tournament_id)
            )
            base_query = await self._apply_search_filters(
                base_query,
                [(TeamDB, "title")],
                search_query,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                TeamDB, order_by, order_by_two, ascending, TeamDB.title, TeamDB.id
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            teams = result.scalars().all()

            return PaginatedTeamResponse(
                data=[TeamSchema.model_validate(t) for t in teams],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    # async def get_players_by_tournament(
    #         self,
    #         tournament_id: int,
    # ):
    #     return await self.get_related_items_level_one_by_id(
    #         tournament_id,
    #         "players_team_tournament",
    #     )

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching players", return_value_on_not_found=[]
    )
    async def get_players_by_tournament(
        self,
        tournament_id: int,
    ) -> list[PlayerTeamTournamentDB]:
        from src.core.models.person import PersonDB

        self.logger.debug(f"Get players by {ITEM} id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerTeamTournamentDB)
                .join(PlayerDB, PlayerTeamTournamentDB.player_id == PlayerDB.id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .order_by(PersonDB.second_name, PersonDB.first_name)
            )

            results = await session.execute(stmt)
            players = results.scalars().all()
            return players

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching available players", return_value_on_not_found=[]
    )
    async def get_available_players_for_tournament(
        self,
        tournament_id: int,
    ) -> list[PlayerDB]:
        from src.core.models.person import PersonDB

        self.logger.debug(f"Get available players for {ITEM} id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            tournament = await session.get(TournamentDB, tournament_id)
            if not tournament:
                return []

            subquery = select(PlayerTeamTournamentDB.player_id).where(
                PlayerTeamTournamentDB.tournament_id == tournament_id
            )

            stmt = (
                select(PlayerDB)
                .where(PlayerDB.sport_id == tournament.sport_id)
                .options(selectinload(PlayerDB.person))
                .where(not_(PlayerDB.id.in_(subquery.scalar_subquery())))
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .order_by(PersonDB.second_name, PersonDB.first_name)
            )

            results = await session.execute(stmt)
            players = results.scalars().all()
            return players

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching available teams", return_value_on_not_found=[]
    )
    async def get_available_teams_for_tournament(
        self,
        tournament_id: int,
    ) -> list[TeamDB]:
        self.logger.debug(f"Get available teams for {ITEM} id:{tournament_id}")
        async with self.db.get_session_maker()() as session:
            tournament = await session.get(TournamentDB, tournament_id)
            if not tournament:
                return []

            subquery = select(TeamTournamentDB.team_id).where(
                TeamTournamentDB.tournament_id == tournament_id
            )

            stmt = (
                select(TeamDB)
                .where(TeamDB.sport_id == tournament.sport_id)
                .where(not_(TeamDB.id.in_(subquery.scalar_subquery())))
                .order_by(TeamDB.title)
            )

            results = await session.execute(stmt)
            teams = results.scalars().all()
            return teams

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching players without team in tournament",
        return_value_on_not_found=None,
    )
    async def get_players_without_team_in_tournament(
        self,
        tournament_id: int,
        search_query: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "second_name",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> list["PlayerWithDetailsSchema"]:
        from src.core.models.person import PersonDB

        from ..player.schemas import PlayerWithDetailsSchema

        self.logger.debug(
            f"Get players without team in tournament {tournament_id} without pagination"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(PlayerDB)
                .join(PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .where(PlayerTeamTournamentDB.team_id.is_(None))
                .options(selectinload(PlayerDB.person))
            )

            if search_query:
                search_pattern = f"%{search_query}%"
                base_query = base_query.where(
                    (PersonDB.first_name.ilike(search_pattern).collate("en-US-x-icu"))
                    | (PersonDB.second_name.ilike(search_pattern).collate("en-US-x-icu"))
                )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr = PersonDB.second_name.asc() if ascending else PersonDB.second_name.desc()
            order_expr_two = PersonDB.id.asc() if ascending else PersonDB.id.desc()
            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            return PaginatedPlayerWithDetailsResponse(
                data=[
                    PlayerWithDetailsSchema.model_validate(
                        {
                            "id": p.id,
                            "sport_id": p.sport_id,
                            "person_id": p.person_id,
                            "player_eesl_id": p.player_eesl_id,
                            "first_name": p.person.first_name if p.person else None,
                            "second_name": p.person.second_name if p.person else None,
                            "player_team_tournaments": [],
                        }
                    )
                    for p in players
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching players in tournament with pagination",
        return_value_on_not_found=None,
    )
    async def get_players_by_tournament_with_pagination(
        self,
        tournament_id: int,
        search_query: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "second_name",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> "PaginatedPlayerWithDetailsResponse":
        from src.core.models.person import PersonDB

        from ..player.schemas import (
            PaginatedPlayerWithDetailsResponse,
            PlayerWithDetailsSchema,
        )

        self.logger.debug(
            f"Get players in tournament {tournament_id} paginated: query={search_query}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(PlayerDB)
                .join(PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .options(selectinload(PlayerDB.person))
            )
            base_query = await self._apply_search_filters(
                base_query,
                [(PersonDB, "first_name"), (PersonDB, "second_name")],
                search_query,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                PersonDB, order_by, order_by_two, ascending, PersonDB.second_name, PersonDB.id
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            return PaginatedPlayerWithDetailsResponse(
                data=[
                    PlayerWithDetailsSchema.model_validate(
                        {
                            "id": p.id,
                            "sport_id": p.sport_id,
                            "person_id": p.person_id,
                            "player_eesl_id": p.player_eesl_id,
                            "first_name": p.person.first_name if p.person else None,
                            "second_name": p.person.second_name if p.person else None,
                            "player_team_tournaments": [],
                        }
                    )
                    for p in players
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching players without team in tournament without pagination",
        return_value_on_not_found=[],
    )
    async def get_players_without_team_in_tournament_simple(
        self,
        tournament_id: int,
    ) -> list["PlayerWithDetailsSchema"]:
        from src.core.models.person import PersonDB

        from ..player.schemas import PlayerWithDetailsSchema

        self.logger.debug(
            f"Get players without team in tournament {tournament_id} without pagination"
        )

        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerDB)
                .join(PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .where(PlayerTeamTournamentDB.team_id.is_(None))
                .options(selectinload(PlayerDB.person))
                .order_by(PersonDB.second_name, PersonDB.first_name)
            )

            result = await session.execute(stmt)
            players = result.scalars().all()

            return [
                PlayerWithDetailsSchema.model_validate(
                    {
                        "id": p.id,
                        "sport_id": p.sport_id,
                        "person_id": p.person_id,
                        "player_eesl_id": p.player_eesl_id,
                        "first_name": p.person.first_name if p.person else None,
                        "second_name": p.person.second_name if p.person else None,
                        "player_team_tournaments": [],
                    }
                )
                for p in players
            ]

    async def get_count_of_matches_by_tournament(
        self,
        tournament_id: int,
    ) -> int:
        self.logger.debug(f"Get matches by {ITEM} id:{tournament_id}")
        return await self.get_count_of_items_level_one_by_id(
            tournament_id,
            "matches",
        )

    async def get_matches_by_tournament(
        self,
        tournament_id: int,
    ) -> list[MatchDB]:
        self.logger.debug(f"Get matches by {ITEM} id:{tournament_id}")
        return await self.get_related_item_level_one_by_id(
            tournament_id,
            "matches",
        )

    async def get_matches_by_tournament_with_pagination(
        self,
        tournament_id: int,
        skip: int = 0,
        limit: int = 20,
        order_exp: str = "id",
        order_exp_two: str = "id",
    ) -> list[MatchDB]:
        self.logger.debug(
            f"Get matches by {ITEM} id:{tournament_id} with pagination: skip={skip}, limit={limit}"
        )
        return await self.get_related_item_level_one_by_id(
            tournament_id,
            "matches",
            skip=skip,
            limit=limit,
            order_by=order_exp,
            order_by_two=order_exp_two,
        )

    async def get_main_tournament_sponsor(self, tournament_id: int) -> SponsorDB | None:
        self.logger.debug(f"Get main tournament's sponsor by {ITEM} id:{tournament_id}")
        return await self.get_related_item_level_one_by_id(tournament_id, "main_sponsor")

    async def get_tournament_sponsor_line(self, tournament_id: int) -> SponsorLineDB | None:
        self.logger.debug(f"Get tournament's sponsor line by {ITEM} id:{tournament_id}")
        return await self.get_related_item_level_one_by_id(tournament_id, "sponsor_line")

    async def get_sponsors_of_tournament_sponsor_line(self, tournament_id: int) -> list[SponsorDB]:
        sponsor_service = SponsorLineServiceDB(self.db)
        self.logger.debug(f"Get sponsors of tournament sponsor line {ITEM} id:{tournament_id}")
        return await self.get_nested_related_item_by_id(
            tournament_id,
            sponsor_service,
            "sponsor_line",
            "sponsors",
        )

    @staticmethod
    def _normalize_position_title(title: str) -> str:
        return title.strip().lower()

    async def _move_single_tournament_to_sport(
        self,
        session: AsyncSession,
        tournament_id: int,
        target_sport_id: int,
        team_ids: list[int],
        player_ids: list[int],
    ) -> tuple[int, int]:
        """Move a single tournament to a new sport. Returns (teams_updated, players_updated)."""
        tournament = await session.get(TournamentDB, tournament_id)
        if tournament is None:
            return 0, 0

        if tournament.sport_id == target_sport_id:
            return 0, 0

        tournament.sport_id = target_sport_id

        teams_updated = 0
        if team_ids:
            team_update_result = await session.execute(
                update(TeamDB).where(TeamDB.id.in_(team_ids)).values(sport_id=target_sport_id)
            )
            teams_updated = team_update_result.rowcount

        players_updated = 0
        if player_ids:
            player_update_result = await session.execute(
                update(PlayerDB).where(PlayerDB.id.in_(player_ids)).values(sport_id=target_sport_id)
            )
            players_updated = player_update_result.rowcount

        return teams_updated, players_updated

    @handle_service_exceptions(item_name=ITEM, operation="moving tournament to another sport")
    async def move_tournament_to_sport(
        self,
        tournament_id: int,
        target_sport_id: int,
        move_conflicting_tournaments: bool = False,
        preview: bool = False,
    ) -> MoveTournamentToSportResponse:
        self.logger.debug(f"Move tournament {tournament_id} to sport {target_sport_id}")
        async with self.db.get_session_maker()() as session:
            async with session.begin():
                tournament = await session.get(TournamentDB, tournament_id)
                if tournament is None:
                    raise HTTPException(
                        status_code=404, detail=f"Tournament id {tournament_id} not found"
                    )

                target_sport = await session.get(SportDB, target_sport_id)
                if target_sport is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Sport id {target_sport_id} not found",
                    )

                if tournament.sport_id == target_sport_id:
                    return MoveTournamentToSportResponse(
                        moved=True,
                        updated_counts=MoveTournamentUpdatedCounts(
                            tournament=0,
                            teams=0,
                            players=0,
                            player_team_tournaments=0,
                            player_matches=0,
                        ),
                        conflicts=MoveTournamentConflicts(),
                        missing_positions=[],
                    )

                team_ids_result = await session.execute(
                    select(TeamTournamentDB.team_id).where(
                        TeamTournamentDB.tournament_id == tournament_id
                    )
                )
                team_ids = sorted(
                    {team_id for (team_id,) in team_ids_result if team_id is not None}
                )
                self.logger.debug(
                    f"Found {len(team_ids)} teams in tournament {tournament_id}: {team_ids}"
                )

                player_ids_result = await session.execute(
                    select(PlayerTeamTournamentDB.player_id).where(
                        PlayerTeamTournamentDB.tournament_id == tournament_id,
                        PlayerTeamTournamentDB.player_id.is_not(None),
                    )
                )
                player_ids = sorted(
                    {player_id for (player_id,) in player_ids_result if player_id is not None}
                )
                self.logger.debug(
                    f"Found {len(player_ids)} players in tournament {tournament_id}: {player_ids}"
                )

                team_conflicts: dict[int, set[int]] = {}
                if team_ids:
                    team_conflict_result = await session.execute(
                        select(TeamTournamentDB.team_id, TeamTournamentDB.tournament_id)
                        .where(TeamTournamentDB.team_id.in_(team_ids))
                        .where(TeamTournamentDB.tournament_id != tournament_id)
                    )
                    for team_id, other_tournament_id in team_conflict_result:
                        team_conflicts.setdefault(team_id, set()).add(other_tournament_id)

                player_conflicts: dict[int, set[int]] = {}
                if player_ids:
                    player_conflict_result = await session.execute(
                        select(
                            PlayerTeamTournamentDB.player_id,
                            PlayerTeamTournamentDB.tournament_id,
                        )
                        .where(PlayerTeamTournamentDB.player_id.in_(player_ids))
                        .where(PlayerTeamTournamentDB.tournament_id != tournament_id)
                    )
                    for player_id, other_tournament_id in player_conflict_result:
                        if player_id is None:
                            continue
                        player_conflicts.setdefault(player_id, set()).add(other_tournament_id)

                if team_conflicts or player_conflicts:
                    conflicting_tournament_ids: set[int] = set()
                    for tournament_ids in team_conflicts.values():
                        conflicting_tournament_ids.update(tournament_ids)
                    for tournament_ids in player_conflicts.values():
                        conflicting_tournament_ids.update(tournament_ids)

                    if preview:
                        self.logger.info(
                            f"Preview mode: would move tournaments {sorted(conflicting_tournament_ids) + [tournament_id]} -> sport {target_sport_id}"
                        )
                        return MoveTournamentToSportResponse(
                            moved=False,
                            preview=True,
                            updated_counts=MoveTournamentUpdatedCounts(
                                tournament=len(conflicting_tournament_ids) + 1,
                                teams=len(team_ids),
                                players=len(player_ids),
                                player_team_tournaments=0,
                                player_matches=0,
                            ),
                            conflicts=MoveTournamentConflicts(
                                teams=[
                                    MoveTournamentConflictEntry(
                                        entity_id=team_id,
                                        tournament_ids=sorted(tournament_ids),
                                    )
                                    for team_id, tournament_ids in sorted(team_conflicts.items())
                                ],
                                players=[
                                    MoveTournamentConflictEntry(
                                        entity_id=player_id,
                                        tournament_ids=sorted(tournament_ids),
                                    )
                                    for player_id, tournament_ids in sorted(
                                        player_conflicts.items()
                                    )
                                ],
                            ),
                            missing_positions=[],
                            moved_tournaments=sorted(conflicting_tournament_ids) + [tournament_id],
                        )

                    if move_conflicting_tournaments:
                        self.logger.info(
                            f"Moving conflicting tournaments: {sorted(conflicting_tournament_ids)}"
                        )

                        for conflicting_tid in sorted(conflicting_tournament_ids):
                            await self._move_single_tournament_to_sport(
                                session, conflicting_tid, target_sport_id, team_ids, player_ids
                            )

                        await self._move_single_tournament_to_sport(
                            session, tournament_id, target_sport_id, team_ids, player_ids
                        )

                        self.logger.info(
                            f"All tournaments moved: {sorted(conflicting_tournament_ids) + [tournament_id]} -> sport {target_sport_id}"
                        )

                        _invalidate_tournament_cache()
                        _invalidate_team_cache()

                        return MoveTournamentToSportResponse(
                            moved=True,
                            preview=False,
                            updated_counts=MoveTournamentUpdatedCounts(
                                tournament=len(conflicting_tournament_ids) + 1,
                                teams=len(team_ids),
                                players=len(player_ids),
                                player_team_tournaments=0,
                                player_matches=0,
                            ),
                            conflicts=MoveTournamentConflicts(),
                            missing_positions=[],
                            moved_tournaments=sorted(conflicting_tournament_ids) + [tournament_id],
                        )
                    else:
                        conflict_details = []
                        if team_conflicts:
                            for team_id, tournament_ids in sorted(team_conflicts.items()):
                                conflict_details.append(
                                    f"Team {team_id} is in tournaments {sorted(tournament_ids)}"
                                )
                        if player_conflicts:
                            for player_id, tournament_ids in sorted(player_conflicts.items()):
                                conflict_details.append(
                                    f"Player {player_id} is in tournaments {sorted(tournament_ids)}"
                                )

                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": "Cannot move tournament: some teams or players are in multiple tournaments. Set move_conflicting_tournaments=true to move all affected tournaments.",
                                "conflicts": {
                                    "teams": [
                                        MoveTournamentConflictEntry(
                                            entity_id=team_id,
                                            tournament_ids=sorted(tournament_ids),
                                        )
                                        for team_id, tournament_ids in sorted(
                                            team_conflicts.items()
                                        )
                                    ],
                                    "players": [
                                        MoveTournamentConflictEntry(
                                            entity_id=player_id,
                                            tournament_ids=sorted(tournament_ids),
                                        )
                                        for player_id, tournament_ids in sorted(
                                            player_conflicts.items()
                                        )
                                    ],
                                },
                            },
                        )

                if preview:
                    self.logger.info(
                        f"Preview mode: would move tournament {tournament_id} -> sport {target_sport_id}"
                    )
                    return MoveTournamentToSportResponse(
                        moved=False,
                        preview=True,
                        updated_counts=MoveTournamentUpdatedCounts(
                            tournament=1,
                            teams=len(team_ids),
                            players=len(player_ids),
                            player_team_tournaments=0,
                            player_matches=0,
                        ),
                        conflicts=MoveTournamentConflicts(),
                        missing_positions=[],
                        moved_tournaments=[tournament_id],
                    )

                tournament.sport_id = target_sport_id

                teams_updated = 0
                if team_ids:
                    self.logger.debug(f"Updating {len(team_ids)} teams to sport {target_sport_id}")
                    team_update_result = await session.execute(
                        update(TeamDB)
                        .where(TeamDB.id.in_(team_ids))
                        .values(sport_id=target_sport_id)
                    )
                    teams_updated = team_update_result.rowcount
                    self.logger.debug(
                        f"Team update affected {teams_updated} rows (expected {len(team_ids)})"
                    )
                    if teams_updated != len(team_ids):
                        self.logger.warning(
                            f"Team update rowcount mismatch: {teams_updated} updated vs {len(team_ids)} expected"
                        )
                else:
                    self.logger.warning(f"No teams found in tournament {tournament_id} to update")

                players_updated = 0
                if player_ids:
                    self.logger.debug(
                        f"Updating {len(player_ids)} players to sport {target_sport_id}"
                    )
                    player_update_result = await session.execute(
                        update(PlayerDB)
                        .where(PlayerDB.id.in_(player_ids))
                        .values(sport_id=target_sport_id)
                    )
                    players_updated = player_update_result.rowcount
                    self.logger.debug(
                        f"Player update affected {players_updated} rows (expected {len(player_ids)})"
                    )
                    if players_updated != len(player_ids):
                        self.logger.warning(
                            f"Player update rowcount mismatch: {players_updated} updated vs {len(player_ids)} expected"
                        )

                target_positions_result = await session.execute(
                    select(PositionDB.id, PositionDB.title).where(
                        PositionDB.sport_id == target_sport_id
                    )
                )
                target_positions: dict[str, int] = {}
                for position_id, title in target_positions_result:
                    normalized_title = self._normalize_position_title(title)
                    target_positions.setdefault(normalized_title, position_id)

                missing_positions: list[MoveTournamentMissingPosition] = []
                player_team_tournaments_updated = 0
                ptt_positions_result = await session.execute(
                    select(
                        PlayerTeamTournamentDB.id,
                        PlayerTeamTournamentDB.position_id,
                        PositionDB.title,
                    )
                    .join(PositionDB, PlayerTeamTournamentDB.position_id == PositionDB.id)
                    .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                )
                for ptt_id, position_id, title in ptt_positions_result:
                    normalized_title = self._normalize_position_title(title)
                    target_position_id = target_positions.get(normalized_title)
                    new_position_id = target_position_id
                    if target_position_id is None:
                        missing_positions.append(
                            MoveTournamentMissingPosition(
                                title=title,
                                source_position_id=position_id,
                                entity="player_team_tournament",
                                record_id=ptt_id,
                            )
                        )
                        new_position_id = None

                    if new_position_id != position_id:
                        await session.execute(
                            update(PlayerTeamTournamentDB)
                            .where(PlayerTeamTournamentDB.id == ptt_id)
                            .values(position_id=new_position_id)
                        )
                        player_team_tournaments_updated += 1

                player_matches_updated = 0
                match_positions_result = await session.execute(
                    select(
                        PlayerMatchDB.id,
                        PlayerMatchDB.match_position_id,
                        PositionDB.title,
                    )
                    .join(PositionDB, PlayerMatchDB.match_position_id == PositionDB.id)
                    .join(MatchDB, PlayerMatchDB.match_id == MatchDB.id)
                    .where(MatchDB.tournament_id == tournament_id)
                )
                for player_match_id, position_id, title in match_positions_result:
                    normalized_title = self._normalize_position_title(title)
                    target_position_id = target_positions.get(normalized_title)
                    new_position_id = target_position_id
                    if target_position_id is None:
                        missing_positions.append(
                            MoveTournamentMissingPosition(
                                title=title,
                                source_position_id=position_id,
                                entity="player_match",
                                record_id=player_match_id,
                            )
                        )
                        new_position_id = None

                    if new_position_id != position_id:
                        await session.execute(
                            update(PlayerMatchDB)
                            .where(PlayerMatchDB.id == player_match_id)
                            .values(match_position_id=new_position_id)
                        )
                        player_matches_updated += 1

                self.logger.info(
                    f"Tournament move completed: tournament={tournament_id} -> sport={target_sport_id}, "
                    f"updated teams={teams_updated}, players={players_updated}, "
                    f"player_team_tournaments={player_team_tournaments_updated}, "
                    f"player_matches={player_matches_updated}"
                )
                _invalidate_tournament_cache()
                _invalidate_team_cache()
                return MoveTournamentToSportResponse(
                    moved=True,
                    preview=False,
                    updated_counts=MoveTournamentUpdatedCounts(
                        tournament=1,
                        teams=teams_updated,
                        players=players_updated,
                        player_team_tournaments=player_team_tournaments_updated,
                        player_matches=player_matches_updated,
                    ),
                    conflicts=MoveTournamentConflicts(),
                    missing_positions=missing_positions,
                    moved_tournaments=[tournament_id],
                )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching tournaments with pagination and details",
        return_value_on_not_found=None,
    )
    async def search_tournaments_with_details_pagination(
        self,
        search_query: str | None = None,
        user_id: int | None = None,
        isprivate: bool | None = None,
        sport_id: int | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "title",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedTournamentWithDetailsResponse:
        self.logger.debug(
            f"Search {ITEM} with details: query={search_query}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}, sport_id={sport_id}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = select(TournamentDB).options(
                joinedload(TournamentDB.season),
                joinedload(TournamentDB.sport),
                selectinload(TournamentDB.teams),
                joinedload(TournamentDB.main_sponsor),
                joinedload(TournamentDB.sponsor_line),
            )

            if user_id is not None:
                base_query = base_query.where(TournamentDB.user_id == user_id)

            if isprivate is not None:
                base_query = base_query.where(TournamentDB.isprivate == isprivate)

            if sport_id is not None:
                base_query = base_query.where(TournamentDB.sport_id == sport_id)

            base_query = await self._apply_search_filters(
                base_query,
                [(TournamentDB, "title")],
                search_query,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                TournamentDB, order_by, order_by_two, ascending, TournamentDB.title, TournamentDB.id
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            tournaments = result.scalars().unique().all()

            from .schemas import TournamentWithDetailsSchema

            return PaginatedTournamentWithDetailsResponse(
                data=[TournamentWithDetailsSchema.model_validate(t) for t in tournaments],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    # async def get_sponsors_of_tournament_sponsor_line(self, tournament_id: int):
    #     sponsor_line = await self.get_related_items_level_one_by_id(
    #         tournament_id,
    #         'sponsor_line'
    #     )
    #
    #     if sponsor_line is not None:
    #         sponsor_line_id = sponsor_line.id
    #         sponsor_service = SponsorLineServiceDB(self.db)
    #         sponsors = await sponsor_service.get_related_items_level_one_by_id(
    #             sponsor_line_id,
    #             'sponsors'
    #         )
    #         return sponsors
    #
    #     return []

    # async def get_sponsors_of_tournament_sponsor_line(self, tournament_id: int):
    #     return await self.get_related_items_level_one_by_id(
    #         tournament_id,
    #         'sponsor_line',
    #         'sponsors'
    #     )
