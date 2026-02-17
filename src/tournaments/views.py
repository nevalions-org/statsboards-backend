from typing import Annotated, Optional

from fastapi import Depends, File, HTTPException, Path, Query, UploadFile
from fastapi.responses import JSONResponse

from src.auth.dependencies import require_roles
from src.core import BaseRouter, db
from src.core.dependencies import TournamentService
from src.core.models import TournamentDB, handle_view_exceptions
from src.helpers.fetch_helpers import (
    fetch_list_of_matches_data,
    fetch_matches_with_data_by_tournament_paginated,
)
from src.pars_eesl.pars_season import parse_season_and_create_jsons
from src.pars_eesl.pars_tournament import parse_tournament_and_create_jsons
from src.tournaments.parser import tournament_parser

from ..helpers.file_service import file_service
from ..logging_config import get_logger
from ..player.schemas import (
    PaginatedPlayerWithDetailsResponse,
    PlayerWithDetailsSchema,
)
from ..sponsor_lines.schemas import SponsorLineSchema
from ..sponsors.schemas import SponsorSchema
from ..teams.schemas import PaginatedTeamResponse
from .db_services import TournamentServiceDB
from .schemas import (
    MoveTournamentToSportRequest,
    MoveTournamentToSportResponse,
    PaginatedTournamentWithDetailsResponse,
    TournamentSchema,
    TournamentSchemaCreate,
    TournamentSchemaUpdate,
    TournamentWithDetailsSchema,
    UploadResizeTournamentLogoResponse,
    UploadTournamentLogoResponse,
)


class TournamentAPIRouter(
    BaseRouter[TournamentSchema, TournamentSchemaCreate, TournamentSchemaUpdate]
):
    def __init__(self, service: TournamentServiceDB | None = None, service_name: str | None = None):
        super().__init__(
            "/api/tournaments",
            ["tournaments-api"],
            service,
            service_name=service_name,
        )
        self.logger = get_logger("TournamentAPIRouter", self)
        self.logger.debug("Initialized TournamentAPIRouter")

    def route(self):
        router = super().route()

        @router.post(
            "/",
            response_model=TournamentSchema,
            summary="Create a new tournament",
            description="Creates a new tournament associated with a season and sport.",
            responses={
                200: {"description": "Tournament created successfully"},
                400: {"description": "Bad request - validation error"},
            },
        )
        async def create_tournament_endpoint(item: TournamentSchemaCreate):
            self.logger.debug(f"Create or update tournament endpoint got data: {item}")
            new_ = await self.loaded_service.create_or_update_tournament(item)
            return TournamentSchema.model_validate(new_)

        @router.put(
            "/{item_id}/",
            response_model=TournamentSchema,
            summary="Update tournament",
            description="Updates an existing tournament by ID. Only provided fields are updated.",
            responses={
                200: {"description": "Tournament updated successfully"},
                404: {"description": "Tournament not found"},
                400: {"description": "Bad request - validation error"},
            },
        )
        async def update_tournament_endpoint(
            item_id: int,
            item: TournamentSchemaUpdate,
        ):
            self.logger.debug(f"Update tournament endpoint id:{item_id} data: {item}")
            update_ = await self.loaded_service.update(item_id, item)
            if update_ is None:
                raise HTTPException(status_code=404, detail=f"Tournament id {item_id} not found")
            return TournamentSchema.model_validate(update_)

        @router.post(
            "/id/{tournament_id}/move-sport",
            response_model=MoveTournamentToSportResponse,
            summary="Move tournament to another sport",
            description="Moves a tournament to another sport with conflict checks on shared teams and players.",
            responses={
                200: {"description": "Move operation completed"},
                401: {"description": "Unauthorized"},
                403: {"description": "Forbidden - requires admin role"},
                404: {"description": "Tournament or sport not found"},
            },
        )
        async def move_tournament_to_sport_endpoint(
            tournament_id: int,
            request: MoveTournamentToSportRequest,
            _: Annotated[TournamentDB, Depends(require_roles("admin"))],
        ):
            self.logger.debug(
                f"Move tournament id:{tournament_id} to sport id:{request.target_sport_id}, move_conflicting:{request.move_conflicting_tournaments}, preview:{request.preview}"
            )
            return await self.loaded_service.move_tournament_to_sport(
                tournament_id=tournament_id,
                target_sport_id=request.target_sport_id,
                move_conflicting_tournaments=request.move_conflicting_tournaments,
                preview=request.preview,
            )

        @router.get(
            "/eesl_id/{eesl_id}",
            response_model=TournamentSchema,
            summary="Get tournament by EESL ID",
            description="Retrieves a tournament by its external EESL identifier.",
            responses={
                200: {"description": "Tournament found"},
                404: {"description": "Tournament not found with specified EESL ID"},
            },
        )
        async def get_tournament_by_eesl_id_endpoint(eesl_id: int):
            self.logger.debug(f"Get tournament by eesl_id endpoint got eesl_id:{eesl_id}")
            tournament = await self.loaded_service.get_tournament_by_eesl_id(value=eesl_id)
            if tournament is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Tournament eesl_id({eesl_id}) not found",
                )
            return TournamentSchema.model_validate(tournament)

        @router.get(
            "/id/{tournament_id}/with-details/",
            response_model=TournamentWithDetailsSchema,
            summary="Get tournament with full details",
            description="Retrieves a tournament with nested season, sport, teams, and sponsor details.",
            responses={
                200: {"description": "Tournament found with full details"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_tournament_with_details_endpoint(tournament_id: int):
            self.logger.debug(f"Get tournament with full details endpoint id:{tournament_id}")
            tournament = await self.loaded_service.get_tournament_with_details(tournament_id)
            if tournament is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Tournament id({tournament_id}) not found",
                )
            return TournamentWithDetailsSchema.model_validate(tournament)

        @router.get(
            "/with-details/paginated",
            response_model=PaginatedTournamentWithDetailsResponse,
            summary="Search tournaments with pagination and full details",
            description="Search tournaments by title, sport_id with pagination and nested season, sport, teams, and sponsor details",
            responses={
                200: {"description": "Tournaments retrieved successfully"},
            },
        )
        async def get_tournaments_with_details_paginated_endpoint(
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("title", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for tournament title"),
            user_id: int | None = Query(None, description="Filter by user_id"),
            isprivate: bool | None = Query(None, description="Filter by isprivate status"),
            sport_id: int | None = Query(None, description="Filter by sport_id"),
        ):
            self.logger.debug(
                f"Get tournaments with details paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}, "
                f"user_id={user_id}, isprivate={isprivate}, sport_id={sport_id}"
            )
            skip = (page - 1) * items_per_page
            response = await self.loaded_service.search_tournaments_with_details_pagination(
                search_query=search,
                user_id=user_id,
                isprivate=isprivate,
                sport_id=sport_id,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )
            return response

        @router.get(
            "/id/{tournament_id}/teams/",
            summary="Get teams in tournament",
            description="Retrieves all teams participating in a specific tournament.",
            responses={
                200: {"description": "Teams retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_teams_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(f"Get teams by tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_teams_by_tournament(tournament_id)

        @router.get(
            "/id/{tournament_id}/teams/paginated",
            response_model=PaginatedTeamResponse,
            summary="Get teams in tournament with pagination and search",
            description="Retrieves teams participating in a specific tournament with pagination and search by team title.",
            responses={
                200: {"description": "Teams retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_teams_by_tournament_paginated_endpoint(
            tournament_id: int,
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("title", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for team title"),
        ):
            self.logger.debug(
                f"Get teams by tournament id:{tournament_id} paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}"
            )
            skip = (page - 1) * items_per_page
            response = await self.loaded_service.get_teams_by_tournament_with_pagination(
                tournament_id=tournament_id,
                search_query=search,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )
            return response

        @router.get(
            "/id/{tournament_id}/players/",
            summary="Get players in tournament",
            description="Retrieves all players participating in a specific tournament.",
            responses={
                200: {"description": "Players retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_players_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(f"Get players by tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_players_by_tournament(tournament_id)

        @router.get(
            "/id/{tournament_id}/players/paginated",
            response_model=PaginatedPlayerWithDetailsResponse,
            summary="Get players in tournament with pagination and search",
            description="Retrieves all players participating in a specific tournament with pagination and search by player name.",
            responses={
                200: {"description": "Players retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_players_by_tournament_paginated_endpoint(
            tournament_id: int,
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("second_name", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for player name"),
        ):
            self.logger.debug(
                f"Get players by tournament id:{tournament_id} paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}"
            )
            skip = (page - 1) * items_per_page
            return await self.loaded_service.get_players_by_tournament_with_pagination(
                tournament_id=tournament_id,
                search_query=search,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )

        @router.get(
            "/id/{tournament_id}/players/available",
            summary="Get available players for tournament",
            description="Retrieves all players in the tournament's sport who are not already connected to the tournament.",
            responses={
                200: {"description": "Available players retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_available_players_for_tournament_endpoint(tournament_id: int):
            self.logger.debug(f"Get available players for tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_available_players_for_tournament(tournament_id)

        @router.get(
            "/id/{tournament_id}/teams/available",
            summary="Get available teams for tournament",
            description="Retrieves all teams in the tournament's sport who are not already connected to the tournament.",
            responses={
                200: {"description": "Available teams retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_available_teams_for_tournament_endpoint(tournament_id: int):
            self.logger.debug(f"Get available teams for tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_available_teams_for_tournament(tournament_id)

        @router.get(
            "/id/{tournament_id}/players/without-team",
            response_model=PaginatedPlayerWithDetailsResponse,
            summary="Get players in tournament without team",
            description="Retrieves all players in the tournament who are not connected to any team (team_id is NULL).",
            responses={
                200: {"description": "Players without team retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_players_without_team_in_tournament_endpoint(
            tournament_id: int,
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("second_name", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for player name"),
        ):
            self.logger.debug(
                f"Get players without team in tournament id:{tournament_id} paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}"
            )
            skip = (page - 1) * items_per_page
            return await self.loaded_service.get_players_without_team_in_tournament(
                tournament_id=tournament_id,
                search_query=search,
                skip=skip,
                limit=items_per_page,
                order_by=order_by,
                order_by_two=order_by_two,
                ascending=ascending,
            )

        @router.get(
            "/id/{tournament_id}/players/without-team/all",
            response_model=list[PlayerWithDetailsSchema],
            summary="Get all players in tournament without team",
            description="Retrieves all players in the tournament who are not connected to any team (team_id is NULL), sorted by second name.",
            responses={
                200: {"description": "Players without team retrieved successfully"},
                404: {"description": "Tournament not found"},
            },
        )
        async def get_players_without_team_in_tournament_all_endpoint(tournament_id: int):
            self.logger.debug(f"Get all players without team in tournament id:{tournament_id}")
            return await self.loaded_service.get_players_without_team_in_tournament_simple(
                tournament_id
            )

        @router.get("/id/{tournament_id}/matches/count")
        async def get_count_of_matches_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(f"Get count of matches by tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_count_of_matches_by_tournament(tournament_id)

        @router.get("/id/{tournament_id}/matches/")
        async def get_matches_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(f"Get matches by tournament id:{tournament_id} endpoint")
            return await self.loaded_service.get_matches_by_tournament(tournament_id)

        @router.get(
            "/id/{tournament_id}/matches/page/{page}/items_per_page/{items_per_page}/order_one/{order_exp}/order_two/{order_exp_two}"
        )
        async def get_tournament_matches_with_pagination_endpoint(
            tournament_id: int,
            page: int = Path(..., ge=1),
            items_per_page: int = Path(..., ge=1, le=100),
            order_exp: str = "id",
            order_exp_two: str = "id",
        ):
            self.logger.debug(
                f"Get matches by tournament id:{tournament_id} with pagination and order endpoint"
            )
            skip = (page - 1) * items_per_page
            matches = await self.loaded_service.get_matches_by_tournament_with_pagination(
                tournament_id=tournament_id,
                skip=skip,
                limit=items_per_page,
                order_exp=order_exp,
                order_exp_two=order_exp_two,
            )
            return matches

        @router.get("/id/{tournament_id}/main_sponsor/", response_model=Optional[SponsorSchema])
        async def get_main_sponsor_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(
                f"Get main_tournament_sponsor by tournament id:{tournament_id} endpoint"
            )
            return await self.loaded_service.get_main_tournament_sponsor(tournament_id)

        @router.get(
            "/id/{tournament_id}/sponsor_line/",
            response_model=Optional[SponsorLineSchema],
        )
        async def get_sponsor_line_by_tournament_id_endpoint(tournament_id: int):
            self.logger.debug(
                f"Get tournament_sponsor_line by tournament id:{tournament_id} endpoint"
            )
            return await self.loaded_service.get_tournament_sponsor_line(tournament_id)

        @router.get(
            "/id/{tournament_id}/sponsors/",
            response_model=Optional[list[SponsorSchema]],
        )
        async def get_sponsors_from_sponsor_line_by_tournament_id_endpoint(
            tournament_id: int,
        ):
            self.logger.debug(
                f"Get sponsors_from_sponsor_line by tournament id:{tournament_id} endpoint"
            )
            return await self.loaded_service.get_sponsors_of_tournament_sponsor_line(tournament_id)

        @router.get(
            "/id/{tournament_id}/matches/all/data/",
            response_class=JSONResponse,
        )
        async def all_tournament_matches_data_endpoint(
            tournament_id: int,
            all_matches: list = Depends(self.loaded_service.get_matches_by_tournament),
        ):
            if not all_matches:
                return []
            return await fetch_list_of_matches_data(all_matches)

        @router.get(
            "/id/{tournament_id}/matches_full_data/page/{page}/items_per_page/{items_per_page}/order_one/{order_exp}/order_two/{order_exp_two}",
            response_class=JSONResponse,
        )
        async def all_tournament_matches_data_paginated_endpoint(
            tournament_id: int,
            page: int = Path(..., ge=1),
            items_per_page: int = Path(..., ge=1, le=100),
            order_exp: str = "id",
            order_exp_two: str = "id",
        ):
            skip = (page - 1) * items_per_page
            return await fetch_matches_with_data_by_tournament_paginated(
                tournament_id=tournament_id,
                skip=skip,
                limit=items_per_page,
                order_exp=order_exp,
                order_exp_two=order_exp_two,
            )

        @router.post("/upload_logo", response_model=UploadTournamentLogoResponse)
        @handle_view_exceptions(
            error_message="Error uploading tournament logo",
            status_code=500,
        )
        async def upload_tournament_logo_endpoint(file: UploadFile = File(...)):
            file_location = await file_service.save_upload_image(
                file, sub_folder="tournaments/logos"
            )
            self.logger.debug(f"Upload tournament logo endpoint file location: {file_location}")
            return {"logoUrl": file_location}

        @router.post("/upload_resize_logo", response_model=UploadResizeTournamentLogoResponse)
        @handle_view_exceptions(
            error_message="Error uploading and resizing tournament logo",
            status_code=500,
        )
        async def upload_and_resize_tournament_logo_endpoint(
            file: UploadFile = File(...),
        ):
            uploaded_paths = await file_service.save_and_resize_upload_image(
                file,
                sub_folder="tournaments/logos",
                icon_height=100,
                web_view_height=400,
            )
            self.logger.debug(
                f"Upload and resize tournament logo endpoint file location: {uploaded_paths}"
            )
            return uploaded_paths

        @router.get(
            "/pars/season/{eesl_season_id}",
            response_model=list[TournamentSchemaCreate],
        )
        async def get_parsed_season_tournaments_endpoint(eesl_season_id: int):
            self.logger.debug(
                f"Get parsed tournaments from season eesl_id:{eesl_season_id} endpoint"
            )
            return await parse_season_and_create_jsons(eesl_season_id)

        @router.post("/pars_and_create/season/{eesl_season_id}")
        @handle_view_exceptions(
            error_message="Internal server error parsing and creating tournaments from season",
            status_code=500,
        )
        async def create_parsed_tournament_endpoint(
            eesl_season_id: int,
            season_id: int | None = Query(None),
            sport_id: int | None = Query(None),
        ):
            self.logger.debug(
                f"Get and Save parsed tournaments from season eesl_id:{eesl_season_id} season_id:{season_id} sport_id:{sport_id} endpoint"
            )
            parser = tournament_parser(db)
            created_tournaments = await parser.parse_and_create(
                eesl_season_id, season_id=season_id, sport_id=sport_id
            )
            return created_tournaments

        @router.get(
            "/pars/tournament/{eesl_tournament_id}",
            response_model=TournamentSchemaCreate,
        )
        @handle_view_exceptions(
            error_message="Internal server error parsing tournament basic data",
            status_code=500,
        )
        async def get_parsed_tournament_basic_data_endpoint(eesl_tournament_id: int):
            self.logger.debug(
                f"Get parsed tournament basic data from eesl_id:{eesl_tournament_id} endpoint"
            )
            return await parse_tournament_and_create_jsons(eesl_tournament_id)

        @router.post(
            "/pars_and_create/tournament/{eesl_tournament_id}",
            response_model=TournamentSchema,
        )
        @handle_view_exceptions(
            error_message="Internal server error parsing and creating tournament",
            status_code=500,
        )
        async def create_parsed_single_tournament_endpoint(
            tournament_service: TournamentService,
            eesl_tournament_id: int,
            season_id: int | None = Query(None),
            sport_id: int | None = Query(None),
        ):
            self.logger.debug(
                f"Get and Save parsed tournament from eesl_id:{eesl_tournament_id} season_id:{season_id} sport_id:{sport_id} endpoint"
            )
            tournament_data = await parse_tournament_and_create_jsons(eesl_tournament_id)

            if tournament_data:
                if season_id is not None:
                    tournament_data["season_id"] = season_id
                if sport_id is not None:
                    tournament_data["sport_id"] = sport_id
                tournament = TournamentSchemaCreate(**tournament_data)
                created_tournament = await tournament_service.create_or_update_tournament(
                    tournament
                )
                self.logger.info(f"Created tournament after parsing: {created_tournament}")
                return TournamentSchema.model_validate(created_tournament)
            else:
                self.logger.warning(f"Failed to parse tournament eesl_id:{eesl_tournament_id}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Tournament eesl_id({eesl_tournament_id}) not found or failed to parse",
                )

        @router.delete(
            "/id/{model_id}",
            summary="Delete tournament",
            description="Delete a tournament by ID. Requires admin role.",
            responses={
                200: {"description": "Tournament deleted successfully"},
                401: {"description": "Unauthorized"},
                403: {"description": "Forbidden - requires admin role"},
                404: {"description": "Tournament not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def delete_tournament_endpoint(
            model_id: int,
            _: Annotated[TournamentDB, Depends(require_roles("admin"))],
        ):
            self.logger.debug(f"Delete tournament endpoint id:{model_id}")
            await self.loaded_service.delete(model_id)
            return {"detail": f"Tournament {model_id} deleted successfully"}

        return router


# class TournamentTemplateRouter(
#     MinimalBaseRouter[TournamentSchema, TournamentSchemaCreate, TournamentSchemaUpdate]
# ):
#     def __init__(self, service: TournamentServiceDB):
#         super().__init__(
#             "/tournaments",
#             ["tournaments-templ"],
#             service,
#         )
#
#     def route(self):
#         router = super().route()
#
#         # @router.get(
#         #     "/id/{tournament_id}/matches/create/",
#         #     response_class=HTMLResponse,
#         # )
#         # async def create_match_in_tournament_endpoint(
#         #     tournament_id: int,
#         #     request: Request,
#         # ):
#         #     tournament = await self.loaded_service.get_by_id(tournament_id)
#         #     season_service_db = SeasonServiceDB(db)
#         #     season = await season_service_db.get_by_id(tournament.season_id)
#         #     tournament_dict = self.loaded_service.to_dict(tournament)
#         #     season_dict = season_service_db.to_dict(season)
#         #
#         #     return templates.TemplateResponse(
#         #         "/matches/display/create-match.html",
#         #         {
#         #             "request": request,
#         #             "tournament": json.dumps(tournament_dict),
#         #             "season": json.dumps(season_dict),
#         #             "tournament_id": tournament_id,
#         #             "season_id": season.id,
#         #             "tournament_title": tournament.title,
#         #         },
#         #         status_code=200,
#         #     )
#
#         # @router.get(
#         #     "/id/{tournament_id}/matches/all/",
#         #     response_class=HTMLResponse,
#         # )
#         # async def get_all_tournament_matches_endpoint(
#         #     tournament_id: int,
#         #     request: Request,
#         # ):
#         #     tournament = await self.loaded_service.get_by_id(tournament_id)
#         #     if tournament is None:
#         #         raise HTTPException(
#         #             status_code=404,
#         #             detail=f"Tournament id: {tournament_id} not found",
#         #         )
#         #     template = templates.TemplateResponse(
#         #         name="/matches/display/all-tournament-matches.html",
#         #         context={
#         #             "request": request,
#         #             "tournament_id": tournament_id,
#         #             "tournament_title": tournament.title,
#         #         },
#         #         status_code=200,
#         #     )
#         #     return template
#
#         return router


api_tournament_router = TournamentAPIRouter(TournamentServiceDB(db)).route()
# template_tournament_router = TournamentTemplateRouter(TournamentServiceDB(db)).route()
