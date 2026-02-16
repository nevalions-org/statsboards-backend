from typing import Annotated

from fastapi import Depends, HTTPException

from src.auth.dependencies import require_roles
from src.core import BaseRouter, db
from src.core.models import PlayerMatchDB
from src.core.service_registry import get_service_registry
from src.pars_eesl.pars_match import parse_match_and_create_jsons
from src.player.schemas import PlayerSchema
from src.player_team_tournament.schemas import PlayerTeamTournamentSchema

from ..logging_config import get_logger
from .db_services import PlayerMatchServiceDB
from .parser import PlayerMatchParser
from .schemas import PlayerMatchSchema, PlayerMatchSchemaCreate, PlayerMatchSchemaUpdate


class PlayerMatchAPIRouter(
    BaseRouter[
        PlayerMatchSchema,
        PlayerMatchSchemaCreate,
        PlayerMatchSchemaUpdate,
    ]
):
    def __init__(
        self, service: PlayerMatchServiceDB | None = None, service_name: str | None = None
    ):
        super().__init__(
            "/api/players_match", ["players_match"], service, service_name=service_name
        )
        self.logger = get_logger("PlayerMatchAPIRouter", self)
        self.logger.debug("Initialized PlayerMatchAPIRouter")

    def route(self):
        router = super().route()

        @router.post(
            "/",
            response_model=PlayerMatchSchema,
        )
        async def create_player_match_endpoint(
            player_match: PlayerMatchSchemaCreate,
        ):
            try:
                self.logger.debug(f"Create player in match endpoint with data: {player_match}")
                new_player_match = await self.loaded_service.create_or_update_player_match(
                    player_match
                )
                if new_player_match:
                    return PlayerMatchSchema.model_validate(new_player_match)
                else:
                    raise HTTPException(status_code=409, detail="Player in match creation fail")
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(
                    f"Error creating player in match endpoint with data: {player_match}",
                    exc_info=ex,
                )

        @router.get(
            "/eesl_id/{eesl_id}",
            response_model=PlayerMatchSchema,
        )
        async def get_player_match_by_eesl_id_endpoint(
            eesl_id: int,
        ):
            try:
                self.logger.debug(f"Get player in match endpoint with eesl_id:{eesl_id}")
                player_match = await self.loaded_service.get_player_match_by_eesl_id(value=eesl_id)
                if player_match is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Player match eesl_id({eesl_id}) not found",
                    )
                return PlayerMatchSchema.model_validate(player_match)
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(
                    f"Error getting player in match with match eesl_id {eesl_id} {ex}",
                    exc_info=True,
                )
                raise HTTPException(status_code=500, detail="Internal server error") from ex

        @router.put(
            "/{item_id}/",
            response_model=PlayerMatchSchema,
        )
        async def update_player_match_endpoint(
            item_id: int,
            item: PlayerMatchSchemaUpdate,
        ):
            try:
                self.logger.debug(f"Update player in match endpoint with data: {item}")
                update_ = await self.loaded_service.update(item_id, item)
                if update_ is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Player team tournament id {item_id} not found",
                    )
                return PlayerMatchSchema.model_validate(update_)
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(f"Error updating player in match with data: {item}", exc_info=ex)
                raise HTTPException(
                    status_code=500, detail="Internal server error updating player match"
                ) from ex

        @router.get(
            "/id/{player_id}/player_in_sport/",
            response_model=PlayerSchema,
        )
        async def get_player_in_sport_endpoint(player_id: int):
            self.logger.debug(f"Get player in sport endpoint with player id:{player_id}")
            return await self.loaded_service.get_player_in_sport(player_id)

        @router.get(
            "/id/{player_id}/player_in_team_tournament/",
            response_model=PlayerTeamTournamentSchema,
        )
        async def get_player_in_team_tournament_endpoint(player_id: int):
            self.logger.debug(f"Get player in tournament endpoint with player id:{player_id}")
            return await self.loaded_service.get_player_in_team_tournament(player_id)

        @router.get(
            "/id/{player_id}/full_data/",
        )
        async def get_player_in_match_full_data_endpoint(player_id: int):
            self.logger.debug(f"Get player in match full data endpoint with player id:{player_id}")
            return await self.loaded_service.get_player_in_match_full_data(player_id)

        @router.get(
            "/pars/match/{eesl_match_id}",
        )
        async def get_parsed_eesl_match_endpoint(eesl_match_id: int):
            self.logger.debug(f"Get parsed eesl match endpoint with eesl_id:{eesl_match_id}")
            return await parse_match_and_create_jsons(eesl_match_id)

        @router.get("/pars_and_create/match/{eesl_match_id}")
        async def create_parsed_eesl_match_endpoint(
            eesl_match_id: int,
        ):
            self.logger.debug(f"Start parsing eesl match endpoint with eesl_id:{eesl_match_id}")
            try:
                registry = get_service_registry()
                database = registry.database
                parser = PlayerMatchParser(database)
                return await parser.create_parsed_match_players(eesl_match_id)
            except HTTPException:
                raise
            except Exception as ex:
                self.logger.error(f"Error parsing eesl match {ex}", exc_info=True)
                return []

        @router.delete(
            "/id/{model_id}",
            summary="Delete player match",
            description="Delete a player match by ID. Requires admin role.",
            responses={
                200: {"description": "PlayerMatch deleted successfully"},
                401: {"description": "Unauthorized"},
                403: {"description": "Forbidden - requires admin role"},
                404: {"description": "PlayerMatch not found"},
                500: {"description": "Internal server error"},
            },
        )
        async def delete_player_match_endpoint(
            model_id: int,
            _: Annotated[PlayerMatchDB, Depends(require_roles("admin"))],
        ):
            self.logger.debug(f"Delete player match endpoint id:{model_id}")
            await self.loaded_service.delete(model_id)
            return {"detail": f"PlayerMatch {model_id} deleted successfully"}

        return router


api_player_match_router = PlayerMatchAPIRouter(PlayerMatchServiceDB(db)).route()
