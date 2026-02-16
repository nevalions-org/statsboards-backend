from src.core import db
from src.core.models import PersonDB, PlayerDB
from src.logging_config import get_logger
from src.pars_eesl.pars_all_players_from_eesl import parse_all_players_from_eesl_index_page_eesl
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate

from .db_services import PlayerServiceDB
from .schemas import PlayerSchemaCreate


class PlayerParser:
    def __init__(self):
        self.logger = get_logger("PlayerParser", self)
        self.logger.debug("Initialized PlayerParser")

    async def parse_and_create_all(
        self, start_page: int, season_id: int
    ) -> tuple[list[PlayerDB], list[PersonDB]]:
        """Parse all players from EESL and create them with batched DB operations.

        Uses a single session for all person + player creates, reducing DB sessions
        from 400+ (2 per player) to 1.

        Args:
            start_page: The starting page number for pagination
            season_id: The EESL season ID to filter players

        Returns:
            Tuple of (created_players, created_persons)
        """
        self.logger.debug(
            f"Parse and create all players from EESL - start_page={start_page}, season_id={season_id}"
        )

        players_data = await parse_all_players_from_eesl_index_page_eesl(
            start_page=start_page, limit=None, season_id=season_id
        )

        if not players_data:
            self.logger.warning("No players parsed from EESL")
            return [], []

        created_persons: list[PersonDB] = []
        created_players: list[PlayerDB] = []

        person_service = PersonServiceDB(db)
        player_service = PlayerServiceDB(db)

        async with db.get_session_maker()() as session:
            for player_with_person in players_data:
                try:
                    person_data = player_with_person.get("person", {})
                    person_schema = PersonSchemaCreate(**person_data)

                    created_person = await person_service.create_or_update_person(
                        person_schema, session=session
                    )

                    if created_person:
                        created_persons.append(created_person)
                        self.logger.debug(f"Person created/updated: {created_person.id}")

                        player_data = player_with_person.get("player", {})
                        player_data["person_id"] = created_person.id
                        player_schema = PlayerSchemaCreate(**player_data)

                        created_player = await player_service.create_or_update_player(
                            player_schema, session=session
                        )

                        if created_player:
                            created_players.append(created_player)
                            self.logger.debug(f"Player created/updated: {created_player.id}")

                except Exception as ex:
                    self.logger.error(
                        f"Error processing player {player_with_person.get('player', {}).get('player_eesl_id')}: {ex}",
                        exc_info=True,
                    )
                    continue

            await session.commit()

        self.logger.debug(f"Created {len(created_persons)} persons")
        self.logger.debug(f"Created {len(created_players)} players")

        return created_players, created_persons


player_parser = PlayerParser()
