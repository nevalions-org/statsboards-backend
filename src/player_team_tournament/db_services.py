from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func

from src.core.models import (
    BaseServiceDB,
    PersonDB,
    PlayerDB,
    PlayerTeamTournamentDB,
    PositionDB,
    TeamDB,
    TournamentDB,
    handle_service_exceptions,
)
from src.core.models.base import Database
from src.core.schema_helpers import PaginationMetadata
from src.player.db_services import PlayerServiceDB

from ..logging_config import get_logger
from .schemas import (
    PaginatedPlayerTeamTournamentResponse,
    PaginatedPlayerTeamTournamentWithDetailsAndPhotosResponse,
    PaginatedPlayerTeamTournamentWithDetailsResponse,
    PaginatedPlayerTeamTournamentWithFullDetailsResponse,
    PlayerTeamTournamentSchema,
    PlayerTeamTournamentSchemaCreate,
    PlayerTeamTournamentSchemaUpdate,
    PlayerTeamTournamentWithDetailsAndPhotosSchema,
    PlayerTeamTournamentWithDetailsSchema,
    PlayerTeamTournamentWithFullDetailsSchema,
)

ITEM = "PLAYER_TEAM_TOURNAMENT"


class PlayerTeamTournamentServiceDB(BaseServiceDB):
    def __init__(
        self,
        database: Database,
    ) -> None:
        super().__init__(database, PlayerTeamTournamentDB)
        self.logger = get_logger("PlayerTeamTournamentServiceDB")
        self.logger.debug("Initialized PlayerTeamTournamentServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: PlayerTeamTournamentSchemaCreate | PlayerTeamTournamentSchemaUpdate,
    ) -> PlayerTeamTournamentDB:
        self.logger.debug(f"Starting to create PlayerTeamTournamentDB with data: {item.__dict__}")
        return await super().create(item)

    async def create_or_update_player_team_tournament(
        self,
        p: PlayerTeamTournamentSchemaCreate | PlayerTeamTournamentSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> PlayerTeamTournamentDB | None:
        if session is not None:
            return await self._create_or_update_player_team_tournament_with_session(p, session)
        return await self._create_or_update_player_team_tournament_without_session(p)

    async def _create_or_update_player_team_tournament_with_session(
        self,
        p: PlayerTeamTournamentSchemaCreate | PlayerTeamTournamentSchemaUpdate,
        session: AsyncSession,
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Creat or update {ITEM} with session:{p}")
        if p.player_team_tournament_eesl_id:
            self.logger.debug(f"Get {ITEM} by eesl id")
            player_team_tournament_from_db = await self.get_player_team_tournament_by_eesl_id(
                p.player_team_tournament_eesl_id, session=session
            )
            if player_team_tournament_from_db:
                self.logger.debug(f"{ITEM} exist, updating...")
                return await self._update_player_team_tournament_with_session(
                    player_team_tournament_from_db,
                    PlayerTeamTournamentSchemaUpdate(**p.model_dump()),
                    session,
                )
            else:
                self.logger.debug(f"{ITEM} does not exist, creating...")
                return await self._create_player_team_tournament_with_session(
                    PlayerTeamTournamentSchemaCreate(**p.model_dump()),
                    session,
                )
        else:
            if p.tournament_id and p.player_id:
                self.logger.debug(f"Got {ITEM} by player id and tournament id")
                player_team_tournament_from_db = (
                    await self._get_player_team_tournaments_by_tournament_id_with_session(
                        p.tournament_id, p.player_id, session
                    )
                )
                if player_team_tournament_from_db:
                    self.logger.debug(f"{ITEM} exist, updating...")
                    return await self._update_player_team_tournament_with_session(
                        player_team_tournament_from_db,
                        PlayerTeamTournamentSchemaUpdate(**p.model_dump()),
                        session,
                    )
            self.logger.debug(f"{ITEM} does not exist, creating...")
            return await self._create_player_team_tournament_with_session(
                PlayerTeamTournamentSchemaCreate(**p.model_dump()),
                session,
            )

    async def _create_or_update_player_team_tournament_without_session(
        self,
        p: PlayerTeamTournamentSchemaCreate | PlayerTeamTournamentSchemaUpdate,
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Creat or update {ITEM}:{p}")
        if p.player_team_tournament_eesl_id:
            self.logger.debug(f"Get {ITEM} by eesl id")
            player_team_tournament_from_db = await self.get_player_team_tournament_by_eesl_id(
                p.player_team_tournament_eesl_id
            )
            if player_team_tournament_from_db:
                self.logger.debug(f"{ITEM} exist, updating...")
                return await self.update_player_team_tournament_by_eesl(
                    "player_team_tournament_eesl_id",
                    PlayerTeamTournamentSchemaUpdate(**p.model_dump()),
                )
            else:
                self.logger.debug(f"{ITEM} does not exist, creating...")
                return await self.create_new_player_team_tournament(
                    PlayerTeamTournamentSchemaCreate(**p.model_dump()),
                )
        else:
            if p.tournament_id and p.player_id:
                self.logger.debug(f"Got {ITEM} by player id and tournament id")
                player_team_tournament_from_db = (
                    await self.get_player_team_tournaments_by_tournament_id(
                        p.tournament_id, p.player_id
                    )
                )
                if player_team_tournament_from_db:
                    self.logger.debug(f"{ITEM} exist, updating...")
                    return await self.update(
                        player_team_tournament_from_db.id,
                        PlayerTeamTournamentSchemaUpdate(**p.model_dump()),
                    )
            self.logger.debug(f"{ITEM} does not exist, creating...")
            return await self.create_new_player_team_tournament(
                PlayerTeamTournamentSchemaCreate(**p.model_dump()),
            )

    async def _create_player_team_tournament_with_session(
        self,
        p: PlayerTeamTournamentSchemaCreate,
        session: AsyncSession,
    ) -> PlayerTeamTournamentDB:
        if isinstance(p, BaseModel):
            item_to_add = PlayerTeamTournamentDB(**p.model_dump())
        else:
            item_to_add = PlayerTeamTournamentDB(
                **{k: v for k, v in p.__dict__.items() if not k.startswith("_")}
            )
        session.add(item_to_add)
        await session.flush()
        await session.refresh(item_to_add)
        return item_to_add

    async def _update_player_team_tournament_with_session(
        self,
        existing_item: PlayerTeamTournamentDB,
        p: PlayerTeamTournamentSchemaUpdate,
        session: AsyncSession,
    ) -> PlayerTeamTournamentDB:
        update_data = (
            p.model_dump(exclude_unset=True, exclude_none=True)
            if isinstance(p, BaseModel)
            else {k: v for k, v in p.__dict__.items() if not k.startswith("_") and v is not None}
        )
        for key, value in update_data.items():
            setattr(existing_item, key, value)
        await session.flush()
        await session.refresh(existing_item)
        return existing_item

    @handle_service_exceptions(
        item_name=ITEM, operation="updating by eesl ID", return_value_on_not_found=None
    )
    async def update_player_team_tournament_by_eesl(
        self,
        eesl_field_name: str,
        p: PlayerTeamTournamentSchemaUpdate,
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Update {ITEM} by {eesl_field_name} {p}")
        if p.player_team_tournament_eesl_id is not None and isinstance(
            p.player_team_tournament_eesl_id, int
        ):
            return await self.update_item_by_eesl_id(
                eesl_field_name,
                p.player_team_tournament_eesl_id,
                p,
            )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid player team tournament {eesl_field_name}",
        )

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create_new_player_team_tournament(
        self,
        p: PlayerTeamTournamentSchemaCreate,
    ) -> PlayerTeamTournamentDB:
        self.logger.debug(f"Create {ITEM} wit data {p}")
        return await super().create(p)

    async def get_player_team_tournament_by_eesl_id(
        self,
        value: int | str,
        field_name: str = "player_team_tournament_eesl_id",
        *,
        session: AsyncSession | None = None,
    ) -> PlayerTeamTournamentDB | None:
        if session is not None:
            return await self._get_player_team_tournament_by_field_with_session(
                value, field_name, session
            )
        self.logger.debug(f"Get {ITEM} by {field_name}: {value}")
        return await self.get_item_by_field_value(
            value=value,
            field_name=field_name,
        )

    async def _get_player_team_tournament_by_field_with_session(
        self,
        value: int | str,
        field_name: str,
        session: AsyncSession,
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Get {ITEM} by {field_name}: {value} with session")
        result = await session.scalars(
            select(PlayerTeamTournamentDB).where(
                getattr(PlayerTeamTournamentDB, field_name) == value
            )
        )
        return result.one_or_none()

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching by tournament and player IDs",
        return_value_on_not_found=None,
    )
    async def get_player_team_tournaments_by_tournament_id(
        self, tournament_id: int, player_id: int
    ) -> PlayerTeamTournamentDB | None:
        self.logger.debug(f"Get {ITEM} by tournament id:{tournament_id} and player id:{player_id}")
        async with self.db.get_session_maker()() as session:
            return await self._get_player_team_tournaments_by_tournament_id_with_session(
                tournament_id, player_id, session
            )

    async def _get_player_team_tournaments_by_tournament_id_with_session(
        self, tournament_id: int, player_id: int, session: AsyncSession
    ) -> PlayerTeamTournamentDB | None:
        stmt = (
            select(PlayerTeamTournamentDB)
            .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
            .where(PlayerTeamTournamentDB.player_id == player_id)
        )
        results = await session.execute(stmt)
        return results.scalars().one_or_none()

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching with person", return_value_on_not_found=None
    )
    async def get_player_team_tournament_with_person(self, player_id: int) -> PlayerDB | None:
        player_service = PlayerServiceDB(self.db)
        return await self.get_nested_related_item_by_id(
            player_id,
            player_service,
            "player",
            "person",
        )

    async def _build_order_expressions_with_joins(
        self,
        order_by: str,
        order_by_two: str | None,
        ascending: bool,
    ) -> tuple:
        """Build order expressions with support for joined table fields."""
        field_mapping = {
            "id": PlayerTeamTournamentDB.id,
            "player_number": PlayerTeamTournamentDB.player_number,
            "player_team_tournament_eesl_id": PlayerTeamTournamentDB.player_team_tournament_eesl_id,
            "first_name": PersonDB.first_name,
            "second_name": PersonDB.second_name,
            "team_title": TeamDB.title,
            "position_title": PositionDB.title,
        }

        return await self._build_order_expressions_with_mapping(
            field_mapping=field_mapping,
            order_by=order_by,
            order_by_two=order_by_two,
            ascending=ascending,
            default_column=PlayerTeamTournamentDB.player_number,
            default_column_two=PlayerTeamTournamentDB.id,
        )

    async def _build_base_query_with_search(
        self,
        tournament_id: int,
        search_query: str | None = None,
        team_title: str | None = None,
    ):
        """Build base query with joins and optional person name and team title search filters."""
        search_fields = [
            (PersonDB, "first_name"),
            (PersonDB, "second_name"),
        ]

        if search_query or team_title:
            base_query = (
                select(PlayerTeamTournamentDB)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .join(PlayerDB, PlayerTeamTournamentDB.player_id == PlayerDB.id)
                .outerjoin(TeamDB, PlayerTeamTournamentDB.team_id == TeamDB.id)
                .outerjoin(PositionDB, PlayerTeamTournamentDB.position_id == PositionDB.id)
                .options(
                    selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                    selectinload(PlayerTeamTournamentDB.team),
                    selectinload(PlayerTeamTournamentDB.position),
                )
            )

            if search_query:
                base_query = base_query.join(PersonDB, PlayerDB.person_id == PersonDB.id)
                base_query = await self._apply_search_filters(
                    base_query,
                    search_fields,
                    search_query,
                )

            if team_title:
                search_pattern = await self._build_search_pattern(team_title)
                base_query = base_query.where(
                    TeamDB.title.ilike(search_pattern).collate("en-US-x-icu")
                )

            return base_query.distinct()
        else:
            return (
                select(PlayerTeamTournamentDB)
                .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                .options(
                    selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                    selectinload(PlayerTeamTournamentDB.team),
                    selectinload(PlayerTeamTournamentDB.position),
                )
            )

    async def update(
        self,
        item_id: int,
        item: PlayerTeamTournamentSchemaUpdate,
        **kwargs,
    ) -> PlayerTeamTournamentDB:
        self.logger.debug(f"Update {ITEM}:{item_id}")
        return await super().update(
            item_id,
            item,
            **kwargs,
        )

    @handle_service_exceptions(item_name="team rosters for match", operation="fetching")
    async def get_team_rosters_for_match(
        self,
        match_id: int,
        include_available: bool = True,
        include_match_players: bool = True,
    ) -> dict | None:
        """Get all rosters for a match: home, away, available."""
        from src.core.models.match import MatchDB
        from src.core.models.player import PlayerDB
        from src.core.models.player_match import PlayerMatchDB

        self.logger.debug(
            f"Get team rosters for match_id:{match_id} include_available:{include_available} include_match_players:{include_match_players}"
        )

        async with self.db.get_session_maker()() as session:
            stmt_match = (
                select(MatchDB)
                .where(MatchDB.id == match_id)
                .options(
                    selectinload(MatchDB.tournaments),
                )
            )
            result_match = await session.execute(stmt_match)
            match = result_match.scalar_one_or_none()

            if not match:
                return None

            result = {
                "match_id": match_id,
                "home_roster": [],
                "away_roster": [],
                "available_home": [],
                "available_away": [],
            }

            if include_match_players:
                home_roster_stmt = (
                    select(PlayerMatchDB)
                    .where(PlayerMatchDB.match_id == match_id)
                    .where(PlayerMatchDB.team_id == match.team_a_id)
                    .options(
                        selectinload(PlayerMatchDB.player_team_tournament)
                        .selectinload(PlayerTeamTournamentDB.player)
                        .selectinload(PlayerDB.person),
                        selectinload(PlayerMatchDB.team),
                        selectinload(PlayerMatchDB.match_position),
                    )
                )
                home_roster_result = await session.execute(home_roster_stmt)
                home_roster_players = home_roster_result.scalars().all()

                for pm in home_roster_players:
                    result["home_roster"].append(
                        {
                            "id": pm.id,
                            "player_id": (
                                pm.player_team_tournament.player_id
                                if pm.player_team_tournament
                                else None
                            ),
                            "player": (
                                pm.player_team_tournament.player.__dict__
                                if pm.player_team_tournament and pm.player_team_tournament.player
                                else None
                            ),
                            "team": (
                                {
                                    **pm.team.__dict__,
                                    "logo_url": pm.team.logo_url
                                    if hasattr(pm.team, "logo_url")
                                    else None,
                                }
                                if pm.team
                                else None
                            ),
                            "tournament": (
                                match.tournaments.__dict__ if match.tournaments else None
                            ),
                            "player_number": (
                                pm.player_team_tournament.player_number
                                if pm.player_team_tournament
                                else None
                            ),
                            "is_home_team": True,
                            "is_starting": pm.is_starting,
                            "starting_type": pm.starting_type,
                        }
                    )

                away_roster_stmt = (
                    select(PlayerMatchDB)
                    .where(PlayerMatchDB.match_id == match_id)
                    .where(PlayerMatchDB.team_id == match.team_b_id)
                    .options(
                        selectinload(PlayerMatchDB.player_team_tournament)
                        .selectinload(PlayerTeamTournamentDB.player)
                        .selectinload(PlayerDB.person),
                        selectinload(PlayerMatchDB.team),
                        selectinload(PlayerMatchDB.match_position),
                    )
                )
                away_roster_result = await session.execute(away_roster_stmt)
                away_roster_players = away_roster_result.scalars().all()

                for pm in away_roster_players:
                    result["away_roster"].append(
                        {
                            "id": pm.id,
                            "player_id": (
                                pm.player_team_tournament.player_id
                                if pm.player_team_tournament
                                else None
                            ),
                            "player": (
                                pm.player_team_tournament.player.__dict__
                                if pm.player_team_tournament and pm.player_team_tournament.player
                                else None
                            ),
                            "team": (
                                {
                                    **pm.team.__dict__,
                                    "logo_url": pm.team.logo_url
                                    if hasattr(pm.team, "logo_url")
                                    else None,
                                }
                                if pm.team
                                else None
                            ),
                            "tournament": (
                                match.tournaments.__dict__ if match.tournaments else None
                            ),
                            "player_number": (
                                pm.player_team_tournament.player_number
                                if pm.player_team_tournament
                                else None
                            ),
                            "is_home_team": False,
                            "is_starting": pm.is_starting,
                            "starting_type": pm.starting_type,
                        }
                    )

            if include_available:
                subquery_home = (
                    select(PlayerMatchDB.player_team_tournament_id)
                    .where(PlayerMatchDB.match_id == match_id)
                    .where(PlayerMatchDB.team_id == match.team_a_id)
                )

                home_available_stmt = (
                    select(PlayerTeamTournamentDB)
                    .where(PlayerTeamTournamentDB.team_id == match.team_a_id)
                    .where(PlayerTeamTournamentDB.tournament_id == match.tournament_id)
                    .where(~PlayerTeamTournamentDB.id.in_(subquery_home))
                    .options(
                        selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                        selectinload(PlayerTeamTournamentDB.team),
                        selectinload(PlayerTeamTournamentDB.tournament),
                    )
                )
                home_available_result = await session.execute(home_available_stmt)
                home_available_players = home_available_result.scalars().all()

                for pt in home_available_players:
                    result["available_home"].append(
                        {
                            "id": pt.id,
                            "player_id": pt.player_id,
                            "player": (pt.player.__dict__ if pt.player else None),
                            "team": (
                                {
                                    **pt.team.__dict__,
                                    "logo_url": pt.team.logo_url
                                    if hasattr(pt.team, "logo_url")
                                    else None,
                                }
                                if pt.team
                                else None
                            ),
                            "tournament": (pt.tournament.__dict__ if pt.tournament else None),
                            "player_number": pt.player_number,
                        }
                    )

                subquery_away = (
                    select(PlayerMatchDB.player_team_tournament_id)
                    .where(PlayerMatchDB.match_id == match_id)
                    .where(PlayerMatchDB.team_id == match.team_b_id)
                )

                away_available_stmt = (
                    select(PlayerTeamTournamentDB)
                    .where(PlayerTeamTournamentDB.team_id == match.team_b_id)
                    .where(PlayerTeamTournamentDB.tournament_id == match.tournament_id)
                    .where(~PlayerTeamTournamentDB.id.in_(subquery_away))
                    .options(
                        selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                        selectinload(PlayerTeamTournamentDB.team),
                        selectinload(PlayerTeamTournamentDB.tournament),
                    )
                )
                away_available_result = await session.execute(away_available_stmt)
                away_available_players = away_available_result.scalars().all()

                for pt in away_available_players:
                    result["available_away"].append(
                        {
                            "id": pt.id,
                            "player_id": pt.player_id,
                            "player": (pt.player.__dict__ if pt.player else None),
                            "team": (
                                {
                                    **pt.team.__dict__,
                                    "logo_url": pt.team.logo_url
                                    if hasattr(pt.team, "logo_url")
                                    else None,
                                }
                                if pt.team
                                else None
                            ),
                            "tournament": (pt.tournament.__dict__ if pt.tournament else None),
                            "player_number": pt.player_number,
                        }
                    )

            return result

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching tournament players with pagination",
        return_value_on_not_found=None,
    )
    async def search_tournament_players_with_pagination(
        self,
        tournament_id: int,
        search_query: str | None = None,
        team_title: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "player_number",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerTeamTournamentResponse:
        self.logger.debug(
            f"Search tournament players: tournament_id={tournament_id}, query={search_query}, "
            f"team_title={team_title}, skip={skip}, limit={limit}, order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = await self._build_base_query_with_search(
                tournament_id,
                search_query,
                team_title,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                PlayerTeamTournamentDB,
                order_by,
                order_by_two,
                ascending,
                PlayerTeamTournamentDB.player_number,
                PlayerTeamTournamentDB.id,
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            return PaginatedPlayerTeamTournamentResponse(
                data=[PlayerTeamTournamentSchema.model_validate(p) for p in players],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching tournament players with pagination and details",
        return_value_on_not_found=None,
    )
    async def search_tournament_players_with_pagination_details(
        self,
        tournament_id: int,
        search_query: str | None = None,
        team_title: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "player_number",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerTeamTournamentWithDetailsResponse:
        self.logger.debug(
            f"Search tournament players with details: tournament_id={tournament_id}, query={search_query}, "
            f"team_title={team_title}, skip={skip}, limit={limit}, order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = await self._build_base_query_with_search(
                tournament_id,
                search_query,
                team_title,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                PlayerTeamTournamentDB,
                order_by,
                order_by_two,
                ascending,
                PlayerTeamTournamentDB.player_number,
                PlayerTeamTournamentDB.id,
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            players_with_details = []
            for p in players:
                players_with_details.append(
                    {
                        "id": p.id,
                        "player_team_tournament_eesl_id": p.player_team_tournament_eesl_id,
                        "player_id": p.player_id,
                        "position_id": p.position_id,
                        "team_id": p.team_id,
                        "tournament_id": p.tournament_id,
                        "player_number": p.player_number,
                        "first_name": p.player.person.first_name
                        if p.player and p.player.person
                        else None,
                        "second_name": p.player.person.second_name
                        if p.player and p.player.person
                        else None,
                        "team_title": p.team.title if p.team else None,
                        "position_title": p.position.title if p.position else None,
                    }
                )

            return PaginatedPlayerTeamTournamentWithDetailsResponse(
                data=[
                    PlayerTeamTournamentWithDetailsSchema.model_validate(p)
                    for p in players_with_details
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching tournament players with pagination and full details",
        return_value_on_not_found=None,
    )
    async def search_tournament_players_with_pagination_full_details(
        self,
        tournament_id: int,
        search_query: str | None = None,
        team_title: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "player_number",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerTeamTournamentWithFullDetailsResponse:
        self.logger.debug(
            f"Search tournament players with full details: tournament_id={tournament_id}, query={search_query}, "
            f"team_title={team_title}, skip={skip}, limit={limit}, order_by={order_by}, order_by_two={order_by_two}"
        )

        search_fields = [
            (PersonDB, "first_name"),
            (PersonDB, "second_name"),
        ]

        async with self.db.get_session_maker()() as session:
            if search_query or team_title:
                base_query = (
                    select(PlayerTeamTournamentDB)
                    .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                    .join(PlayerDB, PlayerTeamTournamentDB.player_id == PlayerDB.id)
                    .outerjoin(TeamDB, PlayerTeamTournamentDB.team_id == TeamDB.id)
                    .join(TournamentDB, PlayerTeamTournamentDB.tournament_id == TournamentDB.id)
                    .outerjoin(PositionDB, PlayerTeamTournamentDB.position_id == PositionDB.id)
                    .options(
                        selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                        selectinload(PlayerTeamTournamentDB.team),
                        selectinload(PlayerTeamTournamentDB.tournament),
                        selectinload(PlayerTeamTournamentDB.position),
                    )
                )

                if search_query:
                    base_query = base_query.join(PersonDB, PlayerDB.person_id == PersonDB.id)
                    base_query = await self._apply_search_filters(
                        base_query,
                        search_fields,
                        search_query,
                    )

                if team_title:
                    search_pattern = await self._build_search_pattern(team_title)
                    base_query = base_query.where(
                        TeamDB.title.ilike(search_pattern).collate("en-US-x-icu")
                    )
            else:
                base_query = (
                    select(PlayerTeamTournamentDB)
                    .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                    .options(
                        selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                        selectinload(PlayerTeamTournamentDB.team),
                        selectinload(PlayerTeamTournamentDB.tournament),
                        selectinload(PlayerTeamTournamentDB.position),
                    )
                )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions(
                PlayerTeamTournamentDB,
                order_by,
                order_by_two,
                ascending,
                PlayerTeamTournamentDB.player_number,
                PlayerTeamTournamentDB.id,
            )

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().unique().all()

            players_with_full_details = []
            for p in players:
                players_with_full_details.append(
                    {
                        "id": p.id,
                        "player_team_tournament_eesl_id": p.player_team_tournament_eesl_id,
                        "player_id": p.player_id,
                        "position_id": p.position_id,
                        "team_id": p.team_id,
                        "tournament_id": p.tournament_id,
                        "player_number": p.player_number,
                        "team": p.team,
                        "tournament": p.tournament,
                        "position": p.position,
                    }
                )

            return PaginatedPlayerTeamTournamentWithFullDetailsResponse(
                data=[
                    PlayerTeamTournamentWithFullDetailsSchema.model_validate(p)
                    for p in players_with_full_details
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching tournament players with pagination and details with photos",
        return_value_on_not_found=None,
    )
    async def search_tournament_players_with_pagination_details_and_photos(
        self,
        tournament_id: int,
        search_query: str | None = None,
        team_title: str | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "player_number",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerTeamTournamentWithDetailsAndPhotosResponse:
        self.logger.debug(
            f"Search tournament players with details and photos: tournament_id={tournament_id}, query={search_query}, "
            f"team_title={team_title}, skip={skip}, limit={limit}, order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = await self._build_base_query_with_search(
                tournament_id,
                search_query,
                team_title,
            )

            count_stmt = select(func.count()).select_from(base_query.subquery())
            count_result = await session.execute(count_stmt)
            total_items = count_result.scalar() or 0

            order_expr, order_expr_two = await self._build_order_expressions_with_joins(
                order_by,
                order_by_two,
                ascending,
            )

            # Always include joins for order_by columns from related tables
            # since _build_order_expressions_with_joins may reference PersonDB, TeamDB, PositionDB
            order_by_needs_joins = order_by in [
                "first_name",
                "second_name",
                "team_title",
                "position_title",
            ] or (
                order_by_two
                and order_by_two in ["first_name", "second_name", "team_title", "position_title"]
            )

            if order_by_needs_joins and not (search_query or team_title):
                base_query = (
                    select(PlayerTeamTournamentDB)
                    .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
                    .join(PlayerDB, PlayerTeamTournamentDB.player_id == PlayerDB.id)
                    .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                    .outerjoin(TeamDB, PlayerTeamTournamentDB.team_id == TeamDB.id)
                    .outerjoin(PositionDB, PlayerTeamTournamentDB.position_id == PositionDB.id)
                    .options(
                        selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
                        selectinload(PlayerTeamTournamentDB.team),
                        selectinload(PlayerTeamTournamentDB.position),
                    )
                )

            # For DISTINCT queries with ORDER BY on joined table columns,
            # we need to add those columns to the SELECT list
            if search_query or team_title:
                columns_to_add = []
                if order_by == "first_name":
                    columns_to_add.append(PersonDB.first_name)
                elif order_by == "second_name":
                    columns_to_add.append(PersonDB.second_name)
                elif order_by == "team_title":
                    columns_to_add.append(TeamDB.title)
                elif order_by == "position_title":
                    columns_to_add.append(PositionDB.title)

                if order_by_two:
                    if order_by_two == "first_name" and order_by != "first_name":
                        columns_to_add.append(PersonDB.first_name)
                    elif order_by_two == "second_name" and order_by != "second_name":
                        columns_to_add.append(PersonDB.second_name)
                    elif order_by_two == "team_title" and order_by != "team_title":
                        columns_to_add.append(TeamDB.title)
                    elif order_by_two == "position_title" and order_by != "position_title":
                        columns_to_add.append(PositionDB.title)

                if columns_to_add:
                    # Add columns to the select statement while keeping the existing ones
                    base_query = base_query.add_columns(*columns_to_add)

            data_query = base_query.order_by(order_expr, order_expr_two).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            players_with_details_and_photos = []
            for p in players:
                players_with_details_and_photos.append(
                    {
                        "id": p.id,
                        "player_team_tournament_eesl_id": p.player_team_tournament_eesl_id,
                        "player_id": p.player_id,
                        "position_id": p.position_id,
                        "team_id": p.team_id,
                        "tournament_id": p.tournament_id,
                        "player_number": p.player_number,
                        "first_name": p.player.person.first_name
                        if p.player and p.player.person
                        else None,
                        "second_name": p.player.person.second_name
                        if p.player and p.player.person
                        else None,
                        "team_title": p.team.title if p.team else None,
                        "position_title": p.position.title if p.position else None,
                        "person_photo_url": p.player.person.person_photo_url
                        if p.player and p.player.person
                        else None,
                        "person_photo_icon_url": p.player.person.person_photo_icon_url
                        if p.player and p.player.person
                        else None,
                    }
                )

            return PaginatedPlayerTeamTournamentWithDetailsAndPhotosResponse(
                data=[
                    PlayerTeamTournamentWithDetailsAndPhotosSchema.model_validate(p)
                    for p in players_with_details_and_photos
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )
