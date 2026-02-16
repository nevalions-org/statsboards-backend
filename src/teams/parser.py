from src.core.models import TeamDB, TeamTournamentDB, TournamentDB
from src.core.models.base import Database
from src.logging_config import get_logger
from src.pars_eesl.pars_tournament import parse_tournament_teams_index_page_eesl
from src.team_tournament.db_services import TeamTournamentServiceDB
from src.team_tournament.schemas import TeamTournamentSchemaCreate
from src.tournaments.db_services import TournamentServiceDB

from .db_services import TeamServiceDB
from .schemas import TeamSchemaCreate


class TeamParser:
    def __init__(self, database: Database):
        self.db = database
        self.logger = get_logger("TeamParser", self)
        self.logger.debug("Initialized TeamParser")

    async def parse_and_create(
        self, eesl_tournament_id: int
    ) -> tuple[list[TeamDB], list[TeamTournamentDB]]:
        """Parse teams from EESL tournament and create them with batched DB operations.

        Reduces DB sessions from ~24 (2 per team) to 1 by:
        - Fetching tournament once
        - Using a single session for all team + team_tournament creates
        - Committing once at the end

        Args:
            eesl_tournament_id: The EESL tournament ID to parse teams from

        Returns:
            Tuple of (created_teams, created_team_tournament_connections)
        """
        self.logger.debug(f"Parse and create teams from tournament eesl_id:{eesl_tournament_id}")

        teams_list = await parse_tournament_teams_index_page_eesl(eesl_tournament_id)

        if not teams_list:
            self.logger.warning("No teams parsed from EESL tournament")
            return [], []

        team_service = TeamServiceDB(self.db)
        tournament_service = TournamentServiceDB(self.db)
        tt_service = TeamTournamentServiceDB(self.db)

        created_teams: list[TeamDB] = []
        created_team_tournaments: list[TeamTournamentDB] = []

        async with self.db.get_session_maker()() as session:
            tournament: TournamentDB | None = await tournament_service.get_tournament_by_eesl_id(
                eesl_tournament_id, session=session
            )

            if not tournament:
                self.logger.error(f"Tournament with eesl_id {eesl_tournament_id} not found in DB")
                return [], []

            for t in teams_list:
                try:
                    team_schema = TeamSchemaCreate(**t)
                    created_team = await team_service.create_or_update_team(
                        team_schema, session=session
                    )

                    if created_team:
                        created_teams.append(created_team)
                        self.logger.debug(f"Team created/updated: {created_team.id}")

                        tt_schema = TeamTournamentSchemaCreate(
                            team_id=created_team.id,
                            tournament_id=tournament.id,
                        )
                        team_tournament = await tt_service.create(tt_schema, session=session)

                        if team_tournament:
                            created_team_tournaments.append(team_tournament)
                            self.logger.debug(
                                f"Team-tournament connection created: team_id={created_team.id}, "
                                f"tournament_id={tournament.id}"
                            )

                except Exception as ex:
                    self.logger.error(
                        f"Error processing team {t.get('team_eesl_id')}: {ex}",
                        exc_info=True,
                    )
                    continue

            await session.commit()

        self.logger.info(f"Created {len(created_teams)} teams")
        self.logger.info(f"Created {len(created_team_tournaments)} team-tournament connections")

        return created_teams, created_team_tournaments


team_parser = TeamParser
