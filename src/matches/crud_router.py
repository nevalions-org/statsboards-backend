import time
from typing import Annotated

from fastapi import Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse

from src.auth.dependencies import require_roles
from src.core import BaseRouter
from src.core.models import MatchDB
from src.core.service_registry import ServiceRegistryAccessorMixin
from src.gameclocks.schemas import GameClockSchema, GameClockSchemaCreate
from src.helpers.file_service import file_service
from src.logging_config import get_logger
from src.matchdata.schemas import MatchDataSchemaCreate
from src.playclocks.schemas import PlayClockSchema, PlayClockSchemaCreate
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.scoreboards.schemas import ScoreboardSchemaCreate, ScoreboardSchemaUpdate
from src.teams.schemas import UploadResizeTeamLogoResponse, UploadTeamLogoResponse

from .db_services import MatchServiceDB
from .schemas import (
    MatchSchema,
    MatchSchemaCreate,
    MatchSchemaUpdate,
    MatchWithDetailsSchema,
    PaginatedMatchResponse,
    PaginatedMatchWithDetailsResponse,
)


class MatchCRUDRouter(
    ServiceRegistryAccessorMixin,
    BaseRouter[
        MatchSchema,
        MatchSchemaCreate,
        MatchSchemaUpdate,
    ],
):
    def __init__(self, service: MatchServiceDB | None = None, service_name: str | None = None):
        super().__init__(
            "/api/matches",
            ["matches-api"],
            service,
            service_name=service_name,
        )
        self.logger = get_logger("MatchCRUDRouter", self)
        self.logger.debug("Initialized MatchCRUDRouter")

    def get_match_stats_service(self):
        """Get match stats service using this router's database."""
        from src.core.service_registry import get_service_registry
        from src.matches.stats_service import MatchStatsServiceDB

        registry = get_service_registry()
        return MatchStatsServiceDB(registry.database)

    def route(self):
        router = super().route()

        @router.post(
            "/",
            response_model=MatchSchema,
            summary="Create a new match",
            description="Creates a new match between two teams. Returns the created match with its ID.",
            responses={
                200: {"description": "Match created successfully"},
                409: {"description": "Conflict - match creation failed"},
                400: {"description": "Bad request - validation error"},
            },
        )
        async def create_match_endpoint(
            match: MatchSchemaCreate,
        ):
            self.logger.debug(f"Create or update match endpoint got data: {match}")
            new_match = await self.loaded_service.create_or_update_match(match)
            if new_match:
                return MatchSchema.model_validate(new_match)
            else:
                self.logger.error(f"Error creating match with data: {match}")
                raise HTTPException(status_code=409, detail="Match creation fail")

        @router.post(
            "/create_with_full_data/",
            summary="Create match with full data",
            description="Creates a match and automatically creates associated match data, playclock, gameclock, and scoreboard.",
            responses={
                200: {"description": "Match created with full data successfully"},
                400: {"description": "Bad request - validation error"},
                500: {"description": "Internal server error"},
            },
        )
        async def create_match_with_full_data_endpoint(
            data: MatchSchemaCreate,
        ):
            self.logger.debug(f"Create or update match with full data endpoint got data: {data}")
            teams_service = self.service_registry.get("team")
            tournament_service = self.service_registry.get("tournament")
            sponsor_service = self.service_registry.get("sponsor")
            match_db_service = self.service_registry.get("matchdata")
            self.service_registry.get("playclock")
            self.service_registry.get("gameclock")
            scoreboard_db_service = self.service_registry.get("scoreboard")

            new_match = await self.loaded_service.create_or_update_match(data)

            default_match_data = MatchDataSchemaCreate(match_id=new_match.id)
            PlayClockSchemaCreate(match_id=new_match.id)
            GameClockSchemaCreate(match_id=new_match.id)

            tournament = await tournament_service.get_by_id(new_match.tournament_id)
            tournament_main_sponsor = await sponsor_service.get_by_id(tournament.main_sponsor_id)
            team_a = await teams_service.get_by_id(new_match.team_a_id)
            team_b = await teams_service.get_by_id(new_match.team_b_id)

            existing_scoreboard = await scoreboard_db_service.get_scoreboard_by_match_id(
                new_match.id
            )

            scoreboard_schema: ScoreboardSchemaCreate | ScoreboardSchemaUpdate  # type: ignore[assignment]
            if existing_scoreboard is None:
                scoreboard_schema = ScoreboardSchemaCreate(
                    match_id=new_match.id,
                    scale_tournament_logo=2,
                    scale_main_sponsor=tournament_main_sponsor.scale_logo,
                    scale_logo_a=2,
                    scale_logo_b=2,
                    team_a_game_color=team_a.team_color,
                    team_b_game_color=team_b.team_color,
                    team_a_game_title=team_a.title,
                    team_b_game_title=team_b.title,
                )
            else:
                scoreboard_schema = ScoreboardSchemaUpdate(
                    match_id=new_match.id,
                    scale_tournament_logo=2,
                    scale_main_sponsor=tournament_main_sponsor.scale_logo,
                    scale_logo_a=2,
                    scale_logo_b=2,
                    team_a_game_color=team_a.team_color,
                    team_b_game_color=team_b.team_color,
                    team_a_game_title=team_a.title,
                    team_b_game_title=team_b.title,
                )
            new_scoreboard = await scoreboard_db_service.create_or_update_scoreboard(
                scoreboard_schema
            )

            new_match_data = await match_db_service.create(default_match_data)
            teams_data = await self.loaded_service.get_teams_by_match(new_match_data.match_id)

            return {
                "status_code": 200,
                "match": new_match,
                "match_data": new_match_data,
                "teams_data": teams_data,
                "scoreboard": new_scoreboard,
            }

        @router.put(
            "/{item_id}/",
            response_model=MatchSchema,
        )
        async def update_match_endpoint(
            item_id: int,
            item: MatchSchemaUpdate,
        ):
            self.logger.debug(f"Update match endpoint id:{item_id} data: {item}")
            try:
                match_update = await self.loaded_service.update(item_id, item)
                if match_update is None:
                    raise HTTPException(status_code=404, detail=f"Match {item_id} not found")
                return MatchSchema.model_validate(match_update)
            except HTTPException:
                raise

        @router.get(
            "/eesl_id/{eesl_id}/",
            response_model=MatchSchema,
        )
        async def get_match_by_eesl_id_endpoint(eesl_id: int):
            self.logger.debug(f"Get match by eesl_id endpoint got eesl_id:{eesl_id}")
            match = await self.loaded_service.get_match_by_eesl_id(value=eesl_id)
            if match is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Match eesl_id({eesl_id}) not found",
                )
            return MatchSchema.model_validate(match)

        @router.get(
            "/id/{match_id}/with-details/",
            response_model=MatchWithDetailsSchema,
            summary="Get match with full details",
            description="Retrieves a match with nested team_a, team_b, tournament, and sponsor details.",
            responses={
                200: {"description": "Match found with full details"},
                404: {"description": "Match not found"},
            },
        )
        async def get_match_with_details_endpoint(match_id: int):
            self.logger.debug(f"Get match with full details endpoint id:{match_id}")
            match = await self.loaded_service.get_match_with_details(match_id)
            if match is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Match id({match_id}) not found",
                )
            return MatchWithDetailsSchema.model_validate(match)

        @router.get(
            "/id/{match_id}/sport/",
        )
        async def get_match_sport_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get sport by match id:{match_id} endpoint")
            return await self.loaded_service.get_sport_by_match_id(match_id)

        @router.get(
            "/id/{match_id}/teams/",
        )
        async def get_match_teams_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get sport by match id:{match_id} endpoint")
            return await self.loaded_service.get_teams_by_match(match_id)

        @router.get(
            "/id/{match_id}/home_away_teams/",
        )
        async def get_match_home_away_teams_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get match home and away teams by match id:{match_id} endpoint")
            return await self.loaded_service.get_teams_by_match_id(match_id)

        @router.get("/id/{match_id}/players/")
        async def get_players_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get players by match id:{match_id} endpoint")
            return await self.loaded_service.get_players_by_match(match_id)

        @router.get("/id/{match_id}/players_fulldata/")
        async def get_players_with_full_data_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get players with full data by match id:{match_id} endpoint")
            return await self.loaded_service.get_player_by_match_full_data(match_id)

        @router.get(
            "/id/{match_id}/team/{team_id}/available-players/",
            summary="Get available players for team in match",
            description="Retrieves all players in team's tournament roster who are not already connected to this specific match.",
            responses={
                200: {"description": "Available players retrieved successfully"},
                404: {"description": "Match or team not found"},
            },
        )
        async def get_available_players_for_team_in_match_endpoint(match_id: int, team_id: int):
            self.logger.debug(
                f"Get available players for team id:{team_id} in match id:{match_id} endpoint"
            )
            return await self.loaded_service.get_available_players_for_team_in_match(
                match_id, team_id
            )

        @router.get(
            "/id/{match_id}/team-rosters/",
            summary="Get team rosters for match",
            description="Retrieves all rosters for a match: home roster, away roster, and available players.",
            responses={
                200: {"description": "Team rosters retrieved successfully"},
                404: {"description": "Match not found"},
            },
        )
        async def get_team_rosters_for_match_endpoint(
            match_id: int,
            include_available: bool = Query(True, description="Include available players"),
            include_match_players: bool = Query(True, description="Include match players"),
        ):
            self.logger.debug(
                f"Get team rosters for match id:{match_id} endpoint include_available:{include_available} include_match_players:{include_match_players}"
            )
            ptt_service = PlayerTeamTournamentServiceDB(self.loaded_service.db)
            result = await ptt_service.get_team_rosters_for_match(
                match_id,
                include_available=include_available,
                include_match_players=include_match_players,
            )
            if result is None:
                raise HTTPException(status_code=404, detail=f"Match id {match_id} not found")
            return result

        @router.get("/id/{match_id}/sponsor_line")
        async def get_sponsor_line_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get sponsor_line by match id:{match_id} endpoint")
            sponsor_line = await self.loaded_service.get_match_sponsor_line(match_id)
            if sponsor_line:
                sponsor_sponsor_line_service = self.service_registry.get("sponsor_sponsor_line")
                full_sponsor_line = await sponsor_sponsor_line_service.get_related_sponsors(
                    sponsor_line.id
                )
                return full_sponsor_line

        @router.get(
            "/id/{match_id}/match_data/",
        )
        async def get_match_data_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get matchdata by match id:{match_id} endpoint")
            return await self.loaded_service.get_matchdata_by_match(match_id)

        @router.get(
            "/id/{match_id}/playclock/",
        )
        async def get_playclock_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get playclock by match id:{match_id} endpoint")
            playclock = await self.loaded_service.get_playclock_by_match(match_id)
            if playclock:
                result = PlayClockSchema.model_validate(playclock).model_dump()
                result["server_time_ms"] = int(time.time() * 1000)
                return result
            return None

        @router.get(
            "/id/{match_id}/gameclock/",
        )
        async def get_gameclock_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get gameclock by match id:{match_id} endpoint")
            gameclock = await self.loaded_service.get_gameclock_by_match(match_id)
            if gameclock:
                result = GameClockSchema.model_validate(gameclock).model_dump()
                result["server_time_ms"] = int(time.time() * 1000)
                return result
            return None

        @router.get(
            "/id/{match_id}/scoreboard_data/",
        )
        async def get_match_scoreboard_by_match_id_endpoint(match_id: int):
            self.logger.debug(f"Get scoreboard_data by match id:{match_id} endpoint")
            return await self.loaded_service.get_scoreboard_by_match(match_id)

        @router.get("/all/data/", response_class=JSONResponse)
        async def all_matches_data_endpoint_endpoint(
            all_matches: list = Depends(self.loaded_service.get_all_elements),
        ):
            from src.helpers.fetch_helpers import fetch_list_of_matches_data

            self.logger.debug("Get all match data by match endpoint")
            return await fetch_list_of_matches_data(all_matches, database=self.loaded_service.db)

        @router.get(
            "/id/{match_id}/data/",
            response_class=JSONResponse,
        )
        async def match_data_endpoint(
            match_teams_data=Depends(get_match_teams_by_match_id_endpoint),
            match_data=Depends(get_match_data_by_match_id_endpoint),
        ):
            self.logger.debug("Get teams and match data by match endpoint")
            return (
                {
                    "status_code": 200,
                    "teams_data": match_teams_data,
                    "match_data": match_data.__dict__,
                },
            )

        @router.get(
            "/id/{match_id}/scoreboard/full_data/",
            response_class=JSONResponse,
        )
        async def full_match_data_endpoint(match_id: int):
            from src.helpers.fetch_helpers import fetch_match_data

            self.logger.debug("Get full_match_data by match endpoint")
            return await fetch_match_data(match_id, database=self.loaded_service.db)

        @router.get(
            "/id/{match_id}/scoreboard/full_data/scoreboard_settings/",
            response_class=JSONResponse,
        )
        async def full_match_data_with_scoreboard_endpoint(match_id: int):
            self.logger.debug("Get full_match_data_with_scoreboard by match endpoint")
            from src.helpers.fetch_helpers import fetch_with_scoreboard_data

            return await fetch_with_scoreboard_data(match_id, database=self.loaded_service.db)

        @router.post("/id/{match_id}/upload_team_logo", response_model=UploadTeamLogoResponse)
        async def upload_team_logo(match_id: int, file: UploadFile = File(...)):
            file_location = await file_service.save_upload_image(
                file, sub_folder=f"match/{match_id}/teams_logos"
            )
            self.logger.debug(f"Upload team in match logo endpoint file location: {file_location}")
            return {"logoUrl": file_location}

        @router.post(
            "/id/{match_id}/upload_resize_logo",
            response_model=UploadResizeTeamLogoResponse,
        )
        async def upload_resize_team_logo(match_id: int, file: UploadFile = File(...)):
            uploaded_paths = await file_service.save_and_resize_upload_image(
                file,
                sub_folder=f"match/{match_id}/teams_logos",
                icon_height=100,
                web_view_height=400,
            )
            self.logger.debug(
                f"Upload and resize team in match logo endpoint file location: {uploaded_paths}"
            )
            return uploaded_paths

        @router.post("/add")
        async def create_match_with_full_data_and_scoreboard_endpoint(
            data: MatchSchemaCreate,
        ):
            self.logger.debug(f"Creat match with full data and scoreboard endpoint {data}")
            teams_service = self.service_registry.get("team")
            tournament_service = self.service_registry.get("tournament")
            sponsor_service = self.service_registry.get("sponsor")
            _ = self.service_registry.get("matchdata")
            _ = self.service_registry.get("playclock")
            _ = self.service_registry.get("gameclock")
            scoreboard_db_service = self.service_registry.get("scoreboard")

            try:
                self.logger.debug(f"Creating simple match: {data}")
                new_match = await self.loaded_service.create_or_update_match(data)

                self.logger.debug("Creating default matchdata, playclock and gameclock")
                MatchDataSchemaCreate(match_id=new_match.id)
                PlayClockSchemaCreate(match_id=new_match.id)
                GameClockSchemaCreate(match_id=new_match.id)

                self.logger.debug("Get tournament and tournament main sponsor")
                tournament = await tournament_service.get_by_id(new_match.tournament_id)
                tournament_main_sponsor = await sponsor_service.get_by_id(
                    tournament.main_sponsor_id
                )
                self.logger.debug("Get teams for match")
                team_a = await teams_service.get_by_id(new_match.team_a_id)
                team_b = await teams_service.get_by_id(new_match.team_b_id)

                scale_main_sponsor = (
                    tournament_main_sponsor.scale_logo if tournament_main_sponsor else 2.0
                )
                self.logger.debug("If scoreboard exist")
                existing_scoreboard = await scoreboard_db_service.get_scoreboard_by_match_id(
                    new_match.id
                )

                scoreboard_schema: ScoreboardSchemaCreate | ScoreboardSchemaUpdate  # type: ignore[assignment]
                if existing_scoreboard is None:
                    scoreboard_schema = ScoreboardSchemaCreate(
                        match_id=new_match.id,
                        scale_tournament_logo=2,
                        scale_main_sponsor=scale_main_sponsor,
                        scale_logo_a=2,
                        scale_logo_b=2,
                        team_a_game_color=team_a.team_color,
                        team_b_game_color=team_b.team_color,
                        team_a_game_title=team_a.title.title(),
                        team_b_game_title=team_b.title.title(),
                    )
                else:
                    scoreboard_schema = ScoreboardSchemaUpdate(
                        match_id=new_match.id,
                        scale_tournament_logo=2,
                        scale_main_sponsor=scale_main_sponsor,
                        scale_logo_a=2,
                        scale_logo_b=2,
                        team_a_game_color=team_a.team_color,
                        team_b_game_color=team_b.team_color,
                        team_a_game_title=team_a.title.title(),
                        team_b_game_title=team_b.title.title(),
                    )
                await scoreboard_db_service.create_or_update_scoreboard(scoreboard_schema)
                self.logger.debug("Scoreboard created or updated")
                self.logger.info(
                    f"Created match with full data and scoreboard {MatchSchema.model_validate(new_match)}"
                )
                return MatchSchema.model_validate(new_match)
            except Exception as e:
                self.logger.error(
                    f"Error creating match with full data and scoreboard: {str(e)}",
                    exc_info=True,
                )

        @router.get(
            "/id/{match_id}/stats/",
            summary="Get match statistics",
            description="Get complete match statistics for both teams including team, offense, QB, and defense stats",
            responses={
                200: {"description": "Stats retrieved successfully"},
                404: {"description": "Match not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def get_match_stats_endpoint(match_id: int):
            self.logger.debug(f"Get match stats endpoint for match_id:{match_id}")
            stats_service = self.get_match_stats_service()

            try:
                stats = await stats_service.get_match_with_cached_stats(match_id)
                if not stats:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Match {match_id} not found or no stats available",
                    )
                return stats
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(f"Error fetching stats for match {match_id}: {ex}", exc_info=True)
                raise HTTPException(status_code=500, detail="Internal server error") from ex

        @router.get(
            "/id/{match_id}/full-context/",
            summary="Get match initialization context",
            description="Get all data needed for match initialization: match, teams, sport, positions, players",
            responses={
                200: {"description": "Full context retrieved successfully"},
                404: {"description": "Match not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def get_match_full_context_endpoint(match_id: int):
            self.logger.debug(f"Get match full context endpoint for match_id:{match_id}")
            try:
                context = await self.loaded_service.get_match_full_context(match_id)
                if not context:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Match {match_id} not found",
                    )
                return context
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(
                    f"Error fetching full context for match {match_id}: {ex}", exc_info=True
                )
                raise HTTPException(status_code=500, detail="Internal server error") from ex

        @router.get(
            "/paginated",
            response_model=PaginatedMatchResponse,
            summary="Search matches with pagination",
            description="Search matches by week, tournament_id, match_eesl_id, or team titles with pagination support",
        )
        async def get_matches_paginated_endpoint(
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("match_date", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(
                None, description="Search query for match_eesl_id or team titles"
            ),
            week: int | None = Query(None, ge=1, description="Filter by week number"),
            tournament_id: int | None = Query(None, ge=1, description="Filter by tournament_id"),
            user_id: int | None = Query(None, description="Filter by user_id"),
            isprivate: bool | None = Query(None, description="Filter by isprivate status"),
        ):
            self.logger.debug(
                f"Get matches paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, "
                f"search={search}, week={week}, tournament_id={tournament_id}, "
                f"user_id={user_id}, isprivate={isprivate}"
            )
            skip = (page - 1) * items_per_page
            response = await self.loaded_service.search_matches_with_pagination(
                search_query=search,
                week=week,
                tournament_id=tournament_id,
                user_id=user_id,
                isprivate=isprivate,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )
            return response

        @router.get(
            "/with-details/paginated",
            response_model=PaginatedMatchWithDetailsResponse,
            summary="Search matches with pagination and full details",
            description="Search matches by week, tournament_id, match_eesl_id, or team titles with pagination and nested team, tournament, sponsor objects",
        )
        async def get_matches_with_details_paginated_endpoint(
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("match_date", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(
                None, description="Search query for match_eesl_id or team titles"
            ),
            week: int | None = Query(None, ge=1, description="Filter by week number"),
            tournament_id: int | None = Query(None, ge=1, description="Filter by tournament_id"),
            user_id: int | None = Query(None, description="Filter by user_id"),
            isprivate: bool | None = Query(None, description="Filter by isprivate status"),
        ):
            self.logger.debug(
                f"Get matches with details paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, "
                f"search={search}, week={week}, tournament_id={tournament_id}, "
                f"user_id={user_id}, isprivate={isprivate}"
            )
            skip = (page - 1) * items_per_page
            response = await self.loaded_service.search_matches_with_details_pagination(
                search_query=search,
                week=week,
                tournament_id=tournament_id,
                user_id=user_id,
                isprivate=isprivate,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )
            return response

        @router.get(
            "/id/{match_id}/comprehensive/",
            summary="Get comprehensive match data",
            description="Get all match data including match info, match data, teams, players with person data, events, and scoreboard",
            responses={
                200: {"description": "Comprehensive match data retrieved successfully"},
                404: {"description": "Match not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def get_comprehensive_match_data_endpoint(match_id: int):
            self.logger.debug(f"Get comprehensive match data endpoint for match_id:{match_id}")
            try:
                data = await self.loaded_service.get_comprehensive_match_data(match_id)
                if not data:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Match {match_id} not found",
                    )
                return data
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(
                    f"Error fetching comprehensive data for match {match_id}: {ex}", exc_info=True
                )
                raise HTTPException(status_code=500, detail="Internal server error") from ex

        @router.delete(
            "/id/{model_id}",
            summary="Delete match",
            description="Delete a match by ID. Requires admin role.",
            responses={
                200: {"description": "Match deleted successfully"},
                401: {"description": "Unauthorized"},
                403: {"description": "Forbidden - requires admin role"},
                404: {"description": "Match not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def delete_match_endpoint(
            model_id: int,
            _: Annotated[MatchDB, Depends(require_roles("admin"))],
        ):
            self.logger.debug(f"Delete match endpoint id:{model_id}")
            await self.loaded_service.delete(model_id)
            return {"detail": f"Match {model_id} deleted successfully"}

        return router
