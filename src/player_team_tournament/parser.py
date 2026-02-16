from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.models import PersonDB, PlayerDB, PlayerTeamTournamentDB, PositionDB
from src.core.models.base import Database
from src.logging_config import get_logger
from src.pars_eesl.pars_all_players_from_eesl import collect_player_full_data_eesl
from src.pars_eesl.parse_player_team_tournament import (
    ParsedPlayerTeamTournament,
    parse_players_from_team_tournament_eesl_and_create_jsons,
)
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaCreate
from src.positions.db_services import PositionServiceDB
from src.positions.schemas import PositionSchemaCreate
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB

from .db_services import PlayerTeamTournamentServiceDB
from .schemas import PlayerTeamTournamentSchemaCreate


class PlayerTeamTournamentParser:
    def __init__(self, database: Database):
        self.db = database
        self.logger = get_logger("PlayerTeamTournamentParser", self)
        self.logger.debug("Initialized PlayerTeamTournamentParser")

    async def parse_and_create(self, tournament_id: int, team_id: int) -> list[dict[str, Any]]:
        """Parse players from team tournament and create all records with batched DB.

        Reduces DB sessions from 150-175 per call to ~3-5 by:
        - Fetching tournament and team ONCE (they're constant)
        - Pre-fetching positions via bulk IN query
        - Using a single session for all operations
        - Committing once at the end

        Args:
            tournament_id: The EESL tournament ID
            team_id: The EESL team ID

        Returns:
            List of created player_team_tournament records with related data
        """
        self.logger.debug(
            f"Start parsing players from team id:{team_id} tournament id:{tournament_id}"
        )

        players_from_team_tournament = (
            await parse_players_from_team_tournament_eesl_and_create_jsons(tournament_id, team_id)
        )

        if not players_from_team_tournament:
            self.logger.warning(
                f"No players found for team id:{team_id} tournament id:{tournament_id}"
            )
            return []

        tournament_service = TournamentServiceDB(self.db)
        team_service = TeamServiceDB(self.db)
        position_service = PositionServiceDB(self.db)
        person_service = PersonServiceDB(self.db)
        player_service = PlayerServiceDB(self.db)
        ptt_service = PlayerTeamTournamentServiceDB(self.db)

        async with self.db.get_session_maker()() as session:
            tournament = await tournament_service.get_tournament_by_eesl_id(
                tournament_id, session=session
            )
            if not tournament:
                self.logger.error(f"Tournament with eesl_id {tournament_id} not found in DB")
                return []

            team = await team_service.get_team_by_eesl_id(team_id, session=session)
            if not team:
                self.logger.error(f"Team with eesl_id {team_id} not found in DB")
                return []

            all_position_titles = self._collect_position_titles(players_from_team_tournament)
            positions_by_title = await self._prefetch_positions(all_position_titles, session)

            created_players_in_team_tournament: list[dict[str, Any]] = []

            for ptt in players_from_team_tournament:
                player_eesl_id = ptt.get("player_eesl_id")
                if player_eesl_id is None:
                    self.logger.warning("Skipping player with no eesl_id")
                    continue

                player_in_team = await collect_player_full_data_eesl(player_eesl_id)
                if not player_in_team:
                    self.logger.warning(f"Could not fetch player data for {player_eesl_id}")
                    continue

                person = await self._get_or_create_person(player_in_team, session, person_service)
                if not person:
                    self.logger.warning(f"Could not create person for {player_eesl_id}")
                    continue

                player = await self._get_or_create_player(
                    player_eesl_id, person.id, session, player_service
                )
                if not player:
                    self.logger.warning(f"Could not create player for {player_eesl_id}")
                    continue

                position = await self._get_or_create_position(
                    ptt.get("player_position", ""),
                    session,
                    position_service,
                    positions_by_title,
                )
                if not position:
                    self.logger.warning(f"Could not get position for {ptt.get('player_position')}")
                    continue

                ptt_record = await self._get_or_create_ptt(
                    ptt=ptt,
                    player_eesl_id=player_eesl_id,
                    player_id=player.id,
                    position_id=position.id,
                    team_id=team.id,
                    tournament_id=tournament.id,
                    session=session,
                    ptt_service=ptt_service,
                )

                if ptt_record:
                    created_players_in_team_tournament.append(
                        {
                            "player_team_tournament": ptt_record,
                            "person": person,
                            "player": player,
                            "position": position,
                            "team": team,
                            "tournament": tournament,
                        }
                    )
                    self.logger.info(f"Created player in team tournament: {ptt_record}")

            await session.commit()
            return created_players_in_team_tournament

    def _collect_position_titles(self, players: list[ParsedPlayerTeamTournament]) -> list[str]:
        """Collect unique position titles from player list."""
        titles = set()
        for player in players:
            if player and player.get("player_position"):
                title = player["player_position"].strip().upper()
                if title:
                    titles.add(title)
        return list(titles)

    async def _prefetch_positions(
        self, titles: list[str], session: AsyncSession
    ) -> dict[str, PositionDB]:
        """Pre-fetch positions by titles using a single IN query."""
        if not titles:
            return {}

        stmt = select(PositionDB).where(func.upper(func.trim(PositionDB.title)).in_(titles))
        results = await session.execute(stmt)
        positions = results.scalars().all()

        return {pos.title.upper().strip(): pos for pos in positions}

    async def _get_or_create_position(
        self,
        title: str,
        session: AsyncSession,
        position_service: PositionServiceDB,
        positions_by_title: dict[str, PositionDB],
    ) -> PositionDB | None:
        """Get or create a position by title."""
        normalized_title = title.upper().strip()
        if not normalized_title:
            return None

        position = positions_by_title.get(normalized_title)
        if position:
            return position

        position_schema = PositionSchemaCreate(title=normalized_title, sport_id=1)
        position = await position_service.create(position_schema, session=session)
        if position:
            positions_by_title[normalized_title] = position
        return position

    async def _get_or_create_person(
        self,
        player_in_team: dict[str, Any],
        session: AsyncSession,
        person_service: PersonServiceDB,
    ) -> PersonDB | None:
        """Get or create a person from parsed player data."""
        person_data = player_in_team.get("person")
        if not person_data:
            return None

        person_schema = PersonSchemaCreate(**person_data)
        return await person_service.create_or_update_person(person_schema, session=session)

    async def _get_or_create_player(
        self,
        player_eesl_id: int,
        person_id: int,
        session: AsyncSession,
        player_service: PlayerServiceDB,
    ) -> PlayerDB | None:
        """Get or create a player."""
        player_data = {
            "sport_id": 1,
            "person_id": person_id,
            "player_eesl_id": player_eesl_id,
        }
        player_schema = PlayerSchemaCreate(**player_data)
        return await player_service.create_or_update_player(player_schema, session=session)

    async def _get_or_create_ptt(
        self,
        ptt: ParsedPlayerTeamTournament,
        player_eesl_id: int,
        player_id: int,
        position_id: int,
        team_id: int,
        tournament_id: int,
        session: AsyncSession,
        ptt_service: PlayerTeamTournamentServiceDB,
    ) -> PlayerTeamTournamentDB | None:
        """Get or create a player_team_tournament record."""
        player_number = ptt.get("player_number", "0")

        ptt_schema = PlayerTeamTournamentSchemaCreate(
            player_team_tournament_eesl_id=player_eesl_id,
            player_id=player_id,
            position_id=position_id,
            team_id=team_id,
            tournament_id=tournament_id,
            player_number=player_number,
        )
        return await ptt_service.create_or_update_player_team_tournament(
            ptt_schema, session=session
        )
