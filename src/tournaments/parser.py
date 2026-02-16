from src.core.models import TournamentDB
from src.core.models.base import Database
from src.logging_config import get_logger
from src.pars_eesl.pars_season import parse_season_and_create_jsons

from .db_services import TournamentServiceDB
from .schemas import TournamentSchemaCreate


class TournamentParser:
    def __init__(self, database: Database):
        self.db = database
        self.logger = get_logger("TournamentParser", self)
        self.logger.debug("Initialized TournamentParser")

    async def parse_and_create(
        self,
        eesl_season_id: int,
        season_id: int | None = None,
        sport_id: int | None = None,
    ) -> list[TournamentDB]:
        """Parse tournaments from EESL season and create them with batched DB operations.

        Reduces DB sessions from ~5 (1 per tournament) to 1 by:
        - Using a single session for all tournament creates
        - Committing once at the end

        Args:
            eesl_season_id: The EESL season ID to parse tournaments from
            season_id: Optional season ID to associate tournaments with
            sport_id: Optional sport ID to associate tournaments with

        Returns:
            List of created tournaments
        """
        self.logger.debug(
            f"Parse and create tournaments from season eesl_id:{eesl_season_id}, "
            f"season_id:{season_id}, sport_id:{sport_id}"
        )

        tournaments_list = await parse_season_and_create_jsons(
            eesl_season_id, season_id=season_id, sport_id=sport_id
        )

        if not tournaments_list:
            self.logger.warning("No tournaments parsed from EESL season")
            return []

        tournament_service = TournamentServiceDB(self.db)

        created_tournaments: list[TournamentDB] = []

        async with self.db.get_session_maker()() as session:
            for t in tournaments_list:
                try:
                    tournament_schema = TournamentSchemaCreate(**t)
                    created_tournament = await tournament_service.create_or_update_tournament(
                        tournament_schema, session=session
                    )

                    if created_tournament:
                        created_tournaments.append(created_tournament)
                        self.logger.debug(f"Tournament created/updated: {created_tournament.id}")

                except Exception as ex:
                    self.logger.error(
                        f"Error processing tournament {t.get('tournament_eesl_id')}: {ex}",
                        exc_info=True,
                    )
                    continue

            await session.commit()

        self.logger.info(f"Created {len(created_tournaments)} tournaments")

        return created_tournaments


tournament_parser = TournamentParser
