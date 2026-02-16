from typing import Annotated

from fastapi import Depends, File, HTTPException, Query, UploadFile

from src.auth.dependencies import require_roles
from src.core import BaseRouter, db
from src.core.dependencies import TeamService, TeamTournamentService
from src.core.models import TeamDB, handle_view_exceptions

from ..helpers.file_service import file_service
from ..logging_config import get_logger
from ..pars_eesl.pars_tournament import (
    parse_team_and_create_jsons,
    parse_tournament_teams_index_page_eesl,
)
from ..team_tournament.schemas import TeamTournamentSchemaCreate
from .parser import team_parser
from .schemas import (
    PaginatedTeamResponse,
    PaginatedTeamWithDetailsResponse,
    TeamSchema,
    TeamSchemaCreate,
    TeamSchemaUpdate,
    TeamWithDetailsSchema,
    UploadResizeTeamLogoResponse,
    UploadTeamLogoResponse,
)


# Team backend
class TeamAPIRouter(BaseRouter[TeamSchema, TeamSchemaCreate, TeamSchemaUpdate]):
    def __init__(self):
        super().__init__("/api/teams", ["teams"], None)
        self.logger = get_logger("TeamAPIRouter", self)
        self.logger.debug("Initialized TeamAPIRouter")

    def route(self):
        router = super().route()

        @router.get(
            "/",
            response_model=list[TeamSchema],
        )
        async def get_all_teams_endpoint(team_service: TeamService):
            self.logger.debug("Get all teams endpoint")
            teams = await team_service.get_all_elements()
            return [TeamSchema.model_validate(t) for t in teams]

        @router.get(
            "/id/{item_id}",
            response_model=TeamSchema,
        )
        async def get_team_by_id_endpoint(
            team_service: TeamService,
            item_id: int,
        ):
            self.logger.debug(f"Get team by id: {item_id}")
            team = await team_service.get_by_id(item_id)
            if team is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Team id {item_id} not found",
                )
            return TeamSchema.model_validate(team)

        @router.post(
            "/",
            response_model=TeamSchema,
            summary="Create a new team",
            description="Creates a new team with optional tournament association. Returns the created team with its ID.",
            responses={
                200: {"description": "Team created successfully"},
                400: {"description": "Bad request - validation error or creation failed"},
                500: {"description": "Internal server error"},
            },
        )
        async def create_team_endpoint(
            team_service: TeamService,
            team_tournament_service: TeamTournamentService,
            team: TeamSchemaCreate,
            tour_id: int | None = None,
        ):
            self.logger.debug(f"Create team endpoint got data: {team}")
            new_team = await team_service.create_or_update_team(team)
            if not new_team:
                raise HTTPException(
                    status_code=400,
                    detail="Error creating new team",
                )
            if tour_id:
                self.logger.debug("Check if team in tournament exists")
                dict_conv = TeamTournamentSchemaCreate(
                    **{"team_id": new_team.id, "tournament_id": tour_id}
                )
                try:
                    self.logger.debug(
                        f"Try creating team_tournament connection team_id: {new_team.id} to tour_id: {tour_id}"
                    )
                    await team_tournament_service.create(dict_conv)
                except HTTPException:
                    raise
                except Exception as ex:
                    self.logger.error(
                        f"Error creating team_tournament connection "
                        f"team_id: {new_team.id} and tour_id: {tour_id} : {ex}",
                        exc_info=True,
                    )
                    raise HTTPException(
                        status_code=500,
                        detail="Error creating team tournament connection",
                    ) from ex
            return TeamSchema.model_validate(new_team)

        @router.get(
            "/eesl_id/{eesl_id}",
            response_model=TeamSchema,
            summary="Get team by EESL ID",
            description="Retrieves a team by its external EESL identifier.",
            responses={
                200: {"description": "Team found"},
                404: {"description": "Team not found with specified EESL ID"},
            },
        )
        async def get_team_by_eesl_id_endpoint(
            team_service: TeamService,
            eesl_id: int,
        ):
            self.logger.debug(f"Get team by eesl_id endpoint got eesl_id:{eesl_id}")
            team = await team_service.get_team_by_eesl_id(value=eesl_id)
            if team is None:
                self.logger.warning(f"No team found with eesl_id: {eesl_id}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Tournament eesl_id({eesl_id}) not found",
                )
            return TeamSchema.model_validate(team)

        @router.get(
            "/id/{team_id}/with-details/",
            response_model=TeamWithDetailsSchema,
            summary="Get team with full details",
            description="Retrieves a team with nested sport, main sponsor, and sponsor line details.",
            responses={
                200: {"description": "Team found with full details"},
                404: {"description": "Team not found"},
            },
        )
        async def get_team_with_details_endpoint(team_service: TeamService, team_id: int):
            self.logger.debug(f"Get team with full details endpoint id:{team_id}")
            team = await team_service.get_team_with_details(team_id)
            if team is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Team id({team_id}) not found",
                )
            return TeamWithDetailsSchema.model_validate(team)

        @router.put(
            "/{item_id}/",
            response_model=TeamSchema,
            summary="Update team",
            description="Updates an existing team by ID. Only provided fields are updated.",
            responses={
                200: {"description": "Team updated successfully"},
                404: {"description": "Team not found"},
                400: {"description": "Bad request - validation error"},
            },
        )
        async def update_team_endpoint(
            team_service: TeamService,
            item_id: int,
            item: TeamSchemaUpdate,
        ):
            self.logger.debug(f"Update team endpoint id:{item_id} data: {item}")
            update_ = await team_service.update(item_id, item)
            if update_ is None:
                raise HTTPException(status_code=404, detail=f"Team id {item_id} not found")
            return TeamSchema.model_validate(update_)

        @router.get("/id/{team_id}/matches/")
        @handle_view_exceptions(
            error_message="Internal server error fetching matches for team",
            status_code=500,
        )
        async def get_matches_by_team_endpoint(team_service: TeamService, team_id: int):
            self.logger.debug(f"Get matches by team id:{team_id} endpoint")
            return await team_service.get_matches_by_team_id(team_id)

        @router.get("/id/{team_id}/tournament/id/{tournament_id}/players/")
        @handle_view_exceptions(
            error_message="Internal server error fetching players for team and tournament",
            status_code=500,
        )
        async def get_players_by_team_and_tournament_endpoint(
            team_service: TeamService, team_id: int, tournament_id: int
        ):
            self.logger.debug(
                f"Get players by team id:{team_id} and tournament id: {tournament_id} endpoint"
            )
            return await team_service.get_players_by_team_id_tournament_id(team_id, tournament_id)

        @router.get("/id/{team_id}/tournament/id/{tournament_id}/players_with_persons/")
        @handle_view_exceptions(
            error_message="Internal server error fetching players with persons for team and tournament",
            status_code=500,
        )
        async def get_players_by_team_id_tournament_id_with_person_endpoint(
            team_service: TeamService, team_id: int, tournament_id: int
        ):
            self.logger.debug(
                f"Get players with persons by team id:{team_id} and tournament id: {tournament_id} endpoint"
            )
            return await team_service.get_players_by_team_id_tournament_id_with_person(
                team_id, tournament_id
            )

        @router.post("/upload_logo", response_model=UploadTeamLogoResponse)
        @handle_view_exceptions(
            error_message="Error uploading team logo",
            status_code=500,
        )
        async def upload_team_logo_endpoint(file: UploadFile = File(...)):
            file_location = await file_service.save_upload_image(file, sub_folder="teams/logos")
            self.logger.debug(f"Upload team logo endpoint file location: {file_location}")
            return {"logoUrl": file_location}

        @router.post("/upload_resize_logo", response_model=UploadResizeTeamLogoResponse)
        @handle_view_exceptions(
            error_message="Error uploading and resizing team logo",
            status_code=500,
        )
        async def upload_and_resize_team_logo_endpoint(file: UploadFile = File(...)):
            uploaded_paths = await file_service.save_and_resize_upload_image(
                file,
                sub_folder="teams/logos",
                icon_height=100,
                web_view_height=400,
            )
            self.logger.debug(
                f"Upload and resize team logo endpoint file location: {uploaded_paths}"
            )
            return uploaded_paths

        @router.get(
            "/pars/tournament/{eesl_tournament_id}",
            response_model=list[TeamSchemaCreate],
        )
        async def get_parse_tournament_teams_endpoint(eesl_tournament_id: int):
            self.logger.debug(
                f"Get parsed teams from tournament eesl_id:{eesl_tournament_id} endpoint"
            )
            return await parse_tournament_teams_index_page_eesl(eesl_tournament_id)

        @router.post("/pars_and_create/tournament/{eesl_tournament_id}")
        @handle_view_exceptions(
            error_message="Internal server error parsing and creating teams from tournament",
            status_code=500,
        )
        async def create_parsed_teams_endpoint(eesl_tournament_id: int):
            self.logger.debug(
                f"Get and Save parsed teams from tournament eesl_id:{eesl_tournament_id} endpoint"
            )
            parser = team_parser(db)
            created_teams, _ = await parser.parse_and_create(eesl_tournament_id)
            return created_teams

        @router.get(
            "/pars/team/{eesl_team_id}",
        )
        async def get_parsed_team_endpoint(eesl_team_id: int):
            self.logger.debug(f"Get parsed team from eesl_id:{eesl_team_id} endpoint")
            return await parse_team_and_create_jsons(eesl_team_id)

        @router.post(
            "/pars_and_create/team/{eesl_team_id}",
            response_model=TeamSchema,
        )
        @handle_view_exceptions(
            error_message="Internal server error parsing and creating team",
            status_code=500,
        )
        async def create_parsed_single_team_endpoint(
            team_service: TeamService,
            eesl_team_id: int,
            sport_id: int | None = Query(None),
        ):
            self.logger.debug(
                f"Get and Save parsed team from eesl_id:{eesl_team_id} sport_id:{sport_id} endpoint"
            )
            team_data = await parse_team_and_create_jsons(eesl_team_id)

            if team_data:
                if sport_id is not None:
                    team_data["sport_id"] = sport_id
                team = TeamSchemaCreate(**team_data)
                created_team = await team_service.create_or_update_team(team)
                self.logger.info(f"Created team after parsing: {created_team}")
                return TeamSchema.model_validate(created_team)
            else:
                self.logger.warning(f"Failed to parse team eesl_id:{eesl_team_id}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Team eesl_id({eesl_team_id}) not found or failed to parse",
                )

        @router.get(
            "/paginated",
            response_model=PaginatedTeamResponse,
        )
        async def get_all_teams_paginated_endpoint(
            team_service: TeamService,
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("title", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for Cyrillic text search"),
            user_id: int | None = Query(None, description="Filter by user_id"),
            isprivate: bool | None = Query(None, description="Filter by isprivate status"),
        ):
            self.logger.debug(
                f"Get all teams paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}, "
                f"user_id={user_id}, isprivate={isprivate}"
            )
            skip = (page - 1) * items_per_page
            response = await team_service.search_teams_with_pagination(
                search_query=search,
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
            response_model=PaginatedTeamWithDetailsResponse,
            summary="Search teams with pagination and full details",
            description="Search teams by title, sport_id with pagination and nested sport, sponsor objects",
        )
        async def get_teams_with_details_paginated_endpoint(
            team_service: TeamService,
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            items_per_page: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
            order_by: str = Query("title", description="First sort column"),
            order_by_two: str = Query("id", description="Second sort column"),
            ascending: bool = Query(True, description="Sort order (true=asc, false=desc)"),
            search: str | None = Query(None, description="Search query for Cyrillic text search"),
            user_id: int | None = Query(None, description="Filter by user_id"),
            isprivate: bool | None = Query(None, description="Filter by isprivate status"),
            sport_id: int | None = Query(None, description="Filter by sport_id"),
        ):
            self.logger.debug(
                f"Get all teams with details paginated: page={page}, items_per_page={items_per_page}, "
                f"order_by={order_by}, order_by_two={order_by_two}, ascending={ascending}, search={search}, "
                f"user_id={user_id}, isprivate={isprivate}, sport_id={sport_id}"
            )
            skip = (page - 1) * items_per_page
            response = await team_service.search_teams_with_details_pagination(
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

        @router.delete(
            "/id/{model_id}",
            summary="Delete team",
            description="Delete a team by ID. Requires admin role.",
            responses={
                200: {"description": "Team deleted successfully"},
                401: {"description": "Unauthorized"},
                403: {"description": "Forbidden - requires admin role"},
                404: {"description": "Team not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def delete_team_endpoint(
            team_service: TeamService,
            model_id: int,
            _: Annotated[TeamDB, Depends(require_roles("admin"))],
        ):
            self.logger.debug(f"Delete team endpoint id:{model_id}")
            await team_service.delete(model_id)
            return {"detail": f"Team {model_id} deleted successfully"}

        return router


api_team_router = TeamAPIRouter().route()
