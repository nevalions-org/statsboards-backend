from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func

from src.core.decorators import handle_service_exceptions
from src.core.models import (
    BaseServiceDB,
    PersonDB,
    PlayerDB,
    PlayerTeamTournamentDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.schema_helpers import PaginationMetadata

from ..logging_config import get_logger
from .schemas import (
    CareerByTeamSchema,
    CareerByTournamentSchema,
    PaginatedPlayerWithDetailsAndPhotosResponse,
    PaginatedPlayerWithDetailsResponse,
    PaginatedPlayerWithFullDetailsResponse,
    PlayerCareerResponseSchema,
    PlayerDetailInTournamentResponse,
    PlayerSchema,
    PlayerSchemaCreate,
    PlayerSchemaUpdate,
    PlayerWithDetailsAndPhotosSchema,
    PlayerWithDetailsSchema,
    PlayerWithFullDetailsSchema,
    TeamAssignmentSchema,
    TournamentAssignmentSchema,
)

if TYPE_CHECKING:
    from src.core.models import PlayerDB

ITEM = "PLAYER"


class PlayerServiceDB(BaseServiceDB):
    def __init__(
        self,
        database: Database,
    ) -> None:
        super().__init__(database, PlayerDB)
        self.logger = get_logger("PlayerServiceDB", self)
        self.logger.debug("Initialized PlayerServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: PlayerSchemaCreate | PlayerSchemaUpdate,
    ) -> PlayerDB:
        self.logger.debug(f"Starting to create PlayerDB with data: {item.__dict__}")
        return await super().create(item)

    async def create_or_update_player(
        self,
        p: PlayerSchemaCreate | PlayerSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> PlayerDB | None:
        if session is not None:
            return await self._create_or_update_player_with_session(p, session)
        return await super().create_or_update(p, eesl_field_name="player_eesl_id")

    async def _create_or_update_player_with_session(
        self,
        p: PlayerSchemaCreate | PlayerSchemaUpdate,
        session: AsyncSession,
    ) -> PlayerDB:
        field_name = "player_eesl_id"
        field_value = getattr(p, field_name, None)

        if field_value:
            existing_item = await self._get_player_by_field_with_session(
                field_value, field_name, session
            )
            if existing_item:
                update_data = (
                    p.model_dump(exclude_unset=True, exclude_none=True)
                    if isinstance(p, BaseModel)
                    else {
                        k: v
                        for k, v in p.__dict__.items()
                        if not k.startswith("_") and v is not None
                    }
                )
                for key, value in update_data.items():
                    setattr(existing_item, key, value)
                await session.flush()
                await session.refresh(existing_item)
                return existing_item

        if isinstance(p, BaseModel):
            item_to_add = PlayerDB(**p.model_dump())
        else:
            item_to_add = PlayerDB(**{k: v for k, v in p.__dict__.items() if not k.startswith("_")})
        session.add(item_to_add)
        await session.flush()
        await session.refresh(item_to_add)
        return item_to_add

    async def _get_player_by_field_with_session(
        self,
        value: int | str,
        field_name: str,
        session: AsyncSession,
    ) -> PlayerDB | None:
        self.logger.debug(f"Get {ITEM} {field_name}:{value} with session")
        result = await session.scalars(
            select(PlayerDB).where(getattr(PlayerDB, field_name) == value)
        )
        return result.one_or_none()

    async def get_player_by_eesl_id(
        self,
        value: int | str,
        field_name: str = "player_eesl_id",
        *,
        session: AsyncSession | None = None,
    ) -> PlayerDB | None:
        if session is not None:
            return await self._get_player_by_field_with_session(value, field_name, session)
        self.logger.debug(f"Get {ITEM} {field_name}:{value}")
        return await self.get_item_by_field_value(
            value=value,
            field_name=field_name,
        )

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching with person", reraise_not_found=True
    )
    async def get_player_with_person(self, player_id: int) -> PlayerSchema:
        self.logger.debug(f"Get {ITEM} with person data {player_id}")
        player_with_person_data = await self.get_related_item_level_one_by_id(player_id, "person")
        if player_with_person_data:
            self.logger.debug(f"Got {ITEM} with person data {player_with_person_data}")
            return player_with_person_data
        else:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=404,
                detail=f"Person does not exist for {ITEM} id:{player_id}",
            )

    async def update(
        self,
        item_id: int,
        item: PlayerSchemaUpdate,
        **kwargs,
    ) -> PlayerDB:
        self.logger.debug(f"Update {ITEM}:{item_id}")
        return await super().update(
            item_id,
            item,
            **kwargs,
        )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="adding person to sport",
    )
    async def add_person_to_sport(
        self,
        person_id: int,
        sport_id: int,
        isprivate: bool | None = None,
        user_id: int | None = None,
    ) -> PlayerDB:
        self.logger.debug(f"Add person {person_id} to sport {sport_id}")

        existing = await self.get_player_by_person_and_sport(person_id, sport_id)
        if existing:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=409,
                detail=f"Player already exists for person_id={person_id} and sport_id={sport_id}",
            )

        player_data = {
            "person_id": person_id,
            "sport_id": sport_id,
        }
        if isprivate is not None:
            player_data["isprivate"] = isprivate
        if user_id is not None:
            player_data["user_id"] = user_id

        player_schema = PlayerSchemaCreate(**player_data)
        return await self.create(player_schema)

    @handle_service_exceptions(
        item_name=ITEM,
        operation="removing person from sport",
        reraise_not_found=True,
    )
    async def remove_person_from_sport(
        self,
        person_id: int,
        sport_id: int,
    ) -> bool:
        self.logger.debug(f"Remove person {person_id} from sport {sport_id}")

        player = await self.get_player_by_person_and_sport(person_id, sport_id)
        if player is None:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=404,
                detail=f"Player not found for person_id={person_id} and sport_id={sport_id}",
            )

        await self.delete(player.id)
        return True

    async def get_player_by_person_and_sport(
        self,
        person_id: int,
        sport_id: int,
    ) -> PlayerDB | None:
        self.logger.debug(f"Get player for person {person_id} and sport {sport_id}")

        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerDB)
                .where(PlayerDB.person_id == person_id)
                .where(PlayerDB.sport_id == sport_id)
            )
            result = await session.execute(stmt)
            return result.scalars().one_or_none()

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching players with pagination and details",
        return_value_on_not_found=None,
    )
    async def search_players_with_pagination_details(
        self,
        sport_id: int,
        search_query: str | None = None,
        team_id: int | None = None,
        user_id: int | None = None,
        isprivate: bool | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "second_name",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerWithDetailsResponse:
        self.logger.debug(
            f"Search players with details: sport_id={sport_id}, query={search_query}, "
            f"team_id={team_id}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(PlayerDB)
                .where(PlayerDB.sport_id == sport_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .options(
                    selectinload(PlayerDB.person),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.team
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.position
                    ),
                )
            )

            if user_id is not None:
                base_query = base_query.where(PlayerDB.user_id == user_id)

            if isprivate is not None:
                base_query = base_query.where(PlayerDB.isprivate == isprivate)

            if team_id:
                base_query = base_query.join(
                    PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id
                ).where(PlayerTeamTournamentDB.team_id == team_id)

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
            data_query = base_query.order_by(order_expr).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            players_with_details = []
            for p in players:
                player_team_tournaments_info = []
                for ptt in p.player_team_tournament:
                    player_team_tournaments_info.append(
                        {
                            "id": ptt.id,
                            "player_team_tournament_eesl_id": ptt.player_team_tournament_eesl_id,
                            "player_number": ptt.player_number,
                            "team_id": ptt.team_id,
                            "team_title": ptt.team.title if ptt.team else None,
                            "position_id": ptt.position_id,
                            "position_title": ptt.position.title if ptt.position else None,
                            "tournament_id": ptt.tournament_id,
                        }
                    )

                players_with_details.append(
                    {
                        "id": p.id,
                        "sport_id": p.sport_id,
                        "person_id": p.person_id,
                        "player_eesl_id": p.player_eesl_id,
                        "first_name": p.person.first_name if p.person else None,
                        "second_name": p.person.second_name if p.person else None,
                        "player_team_tournaments": player_team_tournaments_info,
                    }
                )

            return PaginatedPlayerWithDetailsResponse(
                data=[PlayerWithDetailsSchema.model_validate(p) for p in players_with_details],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching players with pagination and full details",
        return_value_on_not_found=None,
    )
    async def search_players_with_pagination_full_details(
        self,
        sport_id: int,
        search_query: str | None = None,
        team_id: int | None = None,
        user_id: int | None = None,
        isprivate: bool | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "second_name",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerWithFullDetailsResponse:
        self.logger.debug(
            f"Search players with full details: sport_id={sport_id}, query={search_query}, "
            f"team_id={team_id}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(PlayerDB)
                .where(PlayerDB.sport_id == sport_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .options(
                    selectinload(PlayerDB.person),
                    selectinload(PlayerDB.sport),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.team
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.tournament
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.position
                    ),
                )
            )

            if user_id is not None:
                base_query = base_query.where(PlayerDB.user_id == user_id)

            if isprivate is not None:
                base_query = base_query.where(PlayerDB.isprivate == isprivate)

            if team_id:
                base_query = base_query.join(
                    PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id
                ).where(PlayerTeamTournamentDB.team_id == team_id)

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
            data_query = base_query.order_by(order_expr).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            players_with_full_details = []
            for p in players:
                player_team_tournaments_full_details = []
                for ptt in p.player_team_tournament:
                    from .schemas import (
                        PlayerTeamTournamentWithFullDetailsSchema as PTTWithFullDetails,
                    )

                    ptt_full_details = PTTWithFullDetails.model_validate(
                        {
                            "id": ptt.id,
                            "player_team_tournament_eesl_id": ptt.player_team_tournament_eesl_id,
                            "player_id": ptt.player_id,
                            "player_number": ptt.player_number,
                            "team": ptt.team,
                            "tournament": ptt.tournament,
                            "position": ptt.position,
                        }
                    )
                    player_team_tournaments_full_details.append(ptt_full_details)

                players_with_full_details.append(
                    {
                        "id": p.id,
                        "sport_id": p.sport_id,
                        "person_id": p.person_id,
                        "player_eesl_id": p.player_eesl_id,
                        "isprivate": p.isprivate,
                        "user_id": p.user_id,
                        "person": p.person,
                        "sport": p.sport,
                        "player_team_tournaments": player_team_tournaments_full_details,
                    }
                )

            return PaginatedPlayerWithFullDetailsResponse(
                data=[
                    PlayerWithFullDetailsSchema.model_validate(p) for p in players_with_full_details
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="searching players with pagination and details and photos",
        return_value_on_not_found=None,
    )
    async def search_players_with_pagination_details_and_photos(
        self,
        sport_id: int,
        search_query: str | None = None,
        team_id: int | None = None,
        user_id: int | None = None,
        isprivate: bool | None = None,
        skip: int = 0,
        limit: int = 20,
        order_by: str = "second_name",
        order_by_two: str = "id",
        ascending: bool = True,
    ) -> PaginatedPlayerWithDetailsAndPhotosResponse:
        self.logger.debug(
            f"Search players with details and photos: sport_id={sport_id}, query={search_query}, "
            f"team_id={team_id}, skip={skip}, limit={limit}, "
            f"order_by={order_by}, order_by_two={order_by_two}"
        )

        async with self.db.get_session_maker()() as session:
            base_query = (
                select(PlayerDB)
                .where(PlayerDB.sport_id == sport_id)
                .join(PersonDB, PlayerDB.person_id == PersonDB.id)
                .options(
                    selectinload(PlayerDB.person),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.team
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.position
                    ),
                )
            )

            if user_id is not None:
                base_query = base_query.where(PlayerDB.user_id == user_id)

            if isprivate is not None:
                base_query = base_query.where(PlayerDB.isprivate == isprivate)

            if team_id:
                base_query = base_query.join(
                    PlayerTeamTournamentDB, PlayerDB.id == PlayerTeamTournamentDB.player_id
                ).where(PlayerTeamTournamentDB.team_id == team_id)

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
            data_query = base_query.order_by(order_expr).offset(skip).limit(limit)
            result = await session.execute(data_query)
            players = result.scalars().all()

            players_with_details_and_photos = []
            for p in players:
                player_team_tournaments_info = []
                for ptt in p.player_team_tournament:
                    player_team_tournaments_info.append(
                        {
                            "id": ptt.id,
                            "player_team_tournament_eesl_id": ptt.player_team_tournament_eesl_id,
                            "player_number": ptt.player_number,
                            "team_id": ptt.team_id,
                            "team_title": ptt.team.title if ptt.team else None,
                            "position_id": ptt.position_id,
                            "position_title": ptt.position.title if ptt.position else None,
                            "tournament_id": ptt.tournament_id,
                        }
                    )

                players_with_details_and_photos.append(
                    {
                        "id": p.id,
                        "sport_id": p.sport_id,
                        "person_id": p.person_id,
                        "player_eesl_id": p.player_eesl_id,
                        "isprivate": p.isprivate,
                        "user_id": p.user_id,
                        "first_name": p.person.first_name if p.person else None,
                        "second_name": p.person.second_name if p.person else None,
                        "person_photo_url": p.person.person_photo_url if p.person else None,
                        "person_photo_icon_url": p.person.person_photo_icon_url
                        if p.person
                        else None,
                        "player_team_tournaments": player_team_tournaments_info,
                    }
                )

            return PaginatedPlayerWithDetailsAndPhotosResponse(
                data=[
                    PlayerWithDetailsAndPhotosSchema.model_validate(p)
                    for p in players_with_details_and_photos
                ],
                metadata=PaginationMetadata(
                    **await self._calculate_pagination_metadata(total_items, skip, limit),
                ),
            )

    @handle_service_exceptions(item_name=ITEM, operation="fetching player career data")
    async def get_player_career(self, player_id: int) -> PlayerCareerResponseSchema:
        self.logger.debug(f"Get player career data for player_id:{player_id}")

        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerDB)
                .options(
                    selectinload(PlayerDB.person),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.team
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.position
                    ),
                    selectinload(PlayerDB.player_team_tournament)
                    .selectinload(PlayerTeamTournamentDB.tournament)
                    .selectinload(TournamentDB.season),
                )
                .where(PlayerDB.id == player_id)
            )

            result = await session.execute(stmt)
            player = result.scalars().one_or_none()

            if not player:
                from fastapi import HTTPException

                raise HTTPException(
                    status_code=404,
                    detail=f"Player id:{player_id} not found",
                )

            assignments = [
                TeamAssignmentSchema(
                    id=ptt.id,
                    team_id=ptt.team_id,
                    team_title=ptt.team.title if ptt.team else None,
                    position_id=ptt.position_id,
                    position_title=ptt.position.title if ptt.position else None,
                    player_number=ptt.player_number,
                    tournament_id=ptt.tournament_id,
                    tournament_title=ptt.tournament.title if ptt.tournament else None,
                    season_id=ptt.tournament.season_id if ptt.tournament else None,
                    season_year=ptt.tournament.season.year
                    if ptt.tournament and ptt.tournament.season
                    else None,
                )
                for ptt in player.player_team_tournament
            ]

            career_by_team_dict: dict[int | None, list[TeamAssignmentSchema]] = {}
            career_by_tournament_dict: dict[
                tuple[int | None, int | None], list[TeamAssignmentSchema]
            ] = {}

            for assignment in assignments:
                team_id = assignment.team_id
                if team_id not in career_by_team_dict:
                    career_by_team_dict[team_id] = []
                career_by_team_dict[team_id].append(assignment)

                tournament_id = assignment.tournament_id
                season_id = assignment.season_id
                key = (tournament_id, season_id)
                if key not in career_by_tournament_dict:
                    career_by_tournament_dict[key] = []
                career_by_tournament_dict[key].append(assignment)

            career_by_team = sorted(
                [
                    CareerByTeamSchema(
                        team_id=team_id,
                        team_title=(
                            assignments_by_team[0].team_title if assignments_by_team else None
                        ),
                        assignments=assignments_by_team,
                    )
                    for team_id, assignments_by_team in career_by_team_dict.items()
                    if team_id is not None
                ],
                key=lambda x: x.team_title or "",
            )

            career_by_tournament = sorted(
                [
                    CareerByTournamentSchema(
                        tournament_id=tournament_id,
                        tournament_title=(
                            assignments_by_tournament[0].tournament_title
                            if assignments_by_tournament
                            else None
                        ),
                        season_id=season_id,
                        season_year=(
                            assignments_by_tournament[0].season_year
                            if assignments_by_tournament
                            else None
                        ),
                        assignments=assignments_by_tournament,
                    )
                    for (
                        tournament_id,
                        season_id,
                    ), assignments_by_tournament in career_by_tournament_dict.items()
                    if tournament_id is not None
                ],
                key=lambda x: (x.season_year or 0),
                reverse=True,
            )

            return PlayerCareerResponseSchema(
                career_by_team=career_by_team,
                career_by_tournament=career_by_tournament,
            )

    @handle_service_exceptions(
        item_name=ITEM,
        operation="fetching player detail in tournament context",
        reraise_not_found=True,
    )
    async def get_player_detail_in_tournament(
        self, player_id: int, tournament_id: int
    ) -> PlayerDetailInTournamentResponse:
        self.logger.debug(f"Get player {player_id} detail in tournament {tournament_id}")

        async with self.db.get_session_maker()() as session:
            stmt = (
                select(PlayerDB)
                .options(
                    selectinload(PlayerDB.person),
                    selectinload(PlayerDB.sport),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.team
                    ),
                    selectinload(PlayerDB.player_team_tournament).selectinload(
                        PlayerTeamTournamentDB.position
                    ),
                    selectinload(PlayerDB.player_team_tournament)
                    .selectinload(PlayerTeamTournamentDB.tournament)
                    .selectinload(TournamentDB.season),
                )
                .where(PlayerDB.id == player_id)
            )

            result = await session.execute(stmt)
            player = result.scalars().one_or_none()

            if not player:
                from fastapi import HTTPException

                raise HTTPException(
                    status_code=404,
                    detail=f"Player id:{player_id} not found",
                )

            tournament_assignment = None
            for ptt in player.player_team_tournament:
                if ptt.tournament_id == tournament_id:
                    tournament_assignment = TournamentAssignmentSchema(
                        team_title=ptt.team.title if ptt.team else None,
                        team_id=ptt.team_id,
                        position_title=ptt.position.title if ptt.position else None,
                        position_id=ptt.position_id,
                        player_number=ptt.player_number,
                        tournament_title=ptt.tournament.title if ptt.tournament else None,
                        tournament_year=str(ptt.tournament.season.year)
                        if ptt.tournament and ptt.tournament.season
                        else None,
                        tournament_id=ptt.tournament_id,
                    )
                    break

            if not tournament_assignment:
                from fastapi import HTTPException

                raise HTTPException(
                    status_code=404,
                    detail=f"Player {player_id} not found in tournament {tournament_id}",
                )

            assignments = [
                TeamAssignmentSchema(
                    id=ptt.id,
                    team_id=ptt.team_id,
                    team_title=ptt.team.title if ptt.team else None,
                    position_id=ptt.position_id,
                    position_title=ptt.position.title if ptt.position else None,
                    player_number=ptt.player_number,
                    tournament_id=ptt.tournament_id,
                    tournament_title=ptt.tournament.title if ptt.tournament else None,
                    season_id=ptt.tournament.season_id if ptt.tournament else None,
                    season_year=ptt.tournament.season.year
                    if ptt.tournament and ptt.tournament.season
                    else None,
                )
                for ptt in player.player_team_tournament
            ]

            career_by_team_dict: dict[int | None, list[TeamAssignmentSchema]] = {}
            career_by_tournament_dict: dict[
                tuple[int | None, int | None], list[TeamAssignmentSchema]
            ] = {}

            for assignment in assignments:
                team_id = assignment.team_id
                if team_id not in career_by_team_dict:
                    career_by_team_dict[team_id] = []
                career_by_team_dict[team_id].append(assignment)

                tournament_id_assignment = assignment.tournament_id
                season_id = assignment.season_id
                key = (tournament_id_assignment, season_id)
                if key not in career_by_tournament_dict:
                    career_by_tournament_dict[key] = []
                career_by_tournament_dict[key].append(assignment)

            career_by_team = sorted(
                [
                    CareerByTeamSchema(
                        team_id=team_id,
                        team_title=(
                            assignments_by_team[0].team_title if assignments_by_team else None
                        ),
                        assignments=assignments_by_team,
                    )
                    for team_id, assignments_by_team in career_by_team_dict.items()
                    if team_id is not None
                ],
                key=lambda x: x.team_title or "",
            )

            career_by_tournament = sorted(
                [
                    CareerByTournamentSchema(
                        tournament_id=tournament_id_assignment,
                        tournament_title=(
                            assignments_by_tournament[0].tournament_title
                            if assignments_by_tournament
                            else None
                        ),
                        season_id=season_id,
                        season_year=(
                            assignments_by_tournament[0].season_year
                            if assignments_by_tournament
                            else None
                        ),
                        assignments=assignments_by_tournament,
                    )
                    for (
                        tournament_id_assignment,
                        season_id,
                    ), assignments_by_tournament in career_by_tournament_dict.items()
                    if tournament_id_assignment is not None
                ],
                key=lambda x: (x.season_year or 0),
                reverse=True,
            )

            return PlayerDetailInTournamentResponse(
                id=player.id,
                sport_id=player.sport_id,
                person=player.person,
                sport=player.sport,
                tournament_assignment=tournament_assignment,
                career_by_team=career_by_team,
                career_by_tournament=career_by_tournament,
            )
