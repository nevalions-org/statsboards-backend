from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import db
from src.core.models import ScoreboardDB, TeamDB
from src.gameclocks.db_services import GameClockServiceDB
from src.gameclocks.schemas import GameClockSchemaCreate
from src.helpers.text_helpers import safe_int_conversion
from src.logging_config import get_logger
from src.matchdata.db_services import MatchDataServiceDB
from src.matchdata.schemas import MatchDataSchemaCreate, MatchDataSchemaUpdate
from src.pars_eesl.pars_match import parse_match_and_create_jsons
from src.pars_eesl.pars_tournament import (
    ParsedMatchData,
    parse_tournament_matches_index_page_eesl,
)
from src.playclocks.db_services import PlayClockServiceDB
from src.playclocks.schemas import PlayClockSchemaCreate
from src.scoreboards.db_services import ScoreboardServiceDB
from src.scoreboards.schemas import ScoreboardSchemaCreate, ScoreboardSchemaUpdate
from src.tournaments.db_services import TournamentServiceDB

from .schemas import MatchSchemaCreate


class MatchParser:
    def __init__(self):
        self.logger = get_logger("MatchParser", self)
        self.logger.debug("Initialized MatchParser")

    async def get_parse_tournament_matches(
        self, eesl_tournament_id: int
    ) -> list[ParsedMatchData] | None:
        self.logger.debug(f"Get parsed matches from tournament eesl_id:{eesl_tournament_id}")
        return await parse_tournament_matches_index_page_eesl(eesl_tournament_id)

    async def get_parse_match(self, eesl_match_id: int):
        self.logger.debug(f"Get parsed match from eesl_id:{eesl_match_id}")
        return await parse_match_and_create_jsons(eesl_match_id)

    @staticmethod
    def _sport_supports_playclock(sport) -> bool:
        preset = sport.scoreboard_preset if sport else None
        if preset is None:
            return True
        return bool(getattr(preset, "has_playclock", True))

    async def create_parsed_matches(
        self, eesl_tournament_id: int, match_service
    ) -> list[dict] | None:
        """Create parsed matches from tournament with batched DB operations.

        Uses a single session per match for atomic transactions and reduced
        session acquisitions (from ~10-15 per match to 1 per match).
        """
        self.logger.debug(
            f"Get and Save parsed matches from tournament eesl_id:{eesl_tournament_id}"
        )

        tournament = await TournamentServiceDB(db).get_tournament_by_eesl_id(eesl_tournament_id)

        if not tournament:
            self.logger.error(f"Tournament with eesl_id {eesl_tournament_id} not found")
            return []

        playclock_service = PlayClockServiceDB(db)
        gameclock_service = GameClockServiceDB(db)
        scoreboard_service = ScoreboardServiceDB(db)
        match_data_service = MatchDataServiceDB(db)

        try:
            self.logger.debug("Start parsing tournament for matches")
            matches_list: (
                list[ParsedMatchData] | None
            ) = await parse_tournament_matches_index_page_eesl(eesl_tournament_id)

            if not matches_list:
                self.logger.warning("Matches list is empty or None")
                return []

            created_matches_full_data: list[dict] = []

            if matches_list:
                # Pre-fetch all teams in a single query
                team_eesl_ids = set()
                for m in matches_list:
                    team_eesl_ids.add(m["team_a_eesl_id"])
                    team_eesl_ids.add(m["team_b_eesl_id"])

                async with db.get_session_maker()() as session:
                    stmt = select(TeamDB).where(TeamDB.team_eesl_id.in_(list(team_eesl_ids)))
                    results = await session.execute(stmt)
                    teams_by_eesl_id = {team.team_eesl_id: team for team in results.scalars().all()}

                # Process each match with a single session per match transaction
                for m in matches_list:
                    try:
                        result = await self._create_single_parsed_match_batched(
                            m=m,
                            tournament=tournament,
                            teams_by_eesl_id=teams_by_eesl_id,
                            match_service=match_service,
                            playclock_service=playclock_service,
                            gameclock_service=gameclock_service,
                            scoreboard_service=scoreboard_service,
                            match_data_service=match_data_service,
                        )
                        if result:
                            created_matches_full_data.append(result)
                            self.logger.info(
                                f"Created match {result['id']} with full data after parsing"
                            )
                    except Exception as ex:
                        self.logger.error(
                            f"Error on parse and create match {m.get('match_eesl_id')}: {ex}",
                            exc_info=True,
                        )

                return created_matches_full_data
        except Exception as ex:
            self.logger.error(
                f"Error on parse and create matches from tournament: {ex}",
                exc_info=True,
            )
            return []

    async def _create_single_parsed_match_batched(
        self,
        m: ParsedMatchData,
        tournament,
        teams_by_eesl_id: dict[int, TeamDB],
        match_service,
        playclock_service: PlayClockServiceDB,
        gameclock_service: GameClockServiceDB,
        scoreboard_service: ScoreboardServiceDB,
        match_data_service: MatchDataServiceDB,
    ) -> dict | None:
        """Create a single parsed match with all operations in one session.

        This batches all DB operations for a match into a single transaction,
        reducing session acquisitions from ~10-15 to 1.
        """
        self.logger.debug(f"Parsed match: {m}")

        team_a = teams_by_eesl_id.get(m["team_a_eesl_id"])
        if not team_a:
            self.logger.error(f"Home team(a) not found - EESL ID: {m['team_a_eesl_id']}")
            return None

        team_b = teams_by_eesl_id.get(m["team_b_eesl_id"])
        if not team_b:
            self.logger.error(f"Away team(b) not found - EESL ID: {m['team_b_eesl_id']}")
            return None

        self.logger.debug(f"team_a: {team_a}, team_b: {team_b}")

        match_dict = {
            "week": m["week"],
            "match_eesl_id": m["match_eesl_id"],
            "team_a_id": team_a.id,
            "team_b_id": team_b.id,
            "match_date": m["match_date"],
            "tournament_id": tournament.id,
        }

        match_schema = MatchSchemaCreate(**match_dict)

        # Use a single session for all match-related operations
        async with db.get_session_maker()() as session:
            # Create or update match
            created_match = await match_service.create_or_update_match(
                match_schema, session=session
            )

            # Get sport to check playclock support
            sport = await match_service.get_sport_by_match_id(created_match.id, session=session)

            # Create playclock if supported
            if self._sport_supports_playclock(sport):
                playclock_schema = PlayClockSchemaCreate(match_id=created_match.id)
                await playclock_service.create(playclock_schema, session=session)

            # Create gameclock
            gameclock_schema = GameClockSchemaCreate(match_id=created_match.id)
            await gameclock_service.create(gameclock_schema, session=session)

            # Get or create match data
            existing_match_data = await match_data_service.get_match_data_by_match_id(
                created_match.id, session=session
            )

            if existing_match_data is None:
                match_data_schema_create = MatchDataSchemaCreate(
                    match_id=created_match.id,
                    score_team_a=m["score_team_a"],
                    score_team_b=m["score_team_b"],
                )
                match_data = await match_data_service.create(
                    match_data_schema_create, session=session
                )
            else:
                match_data_schema_update = MatchDataSchemaUpdate(
                    match_id=created_match.id,
                    score_team_a=m["score_team_a"],
                    score_team_b=m["score_team_b"],
                )
                match_data = await match_data_service.update(
                    existing_match_data.id, match_data_schema_update, session=session
                )

            # Get or create scoreboard
            existing_scoreboard = await self._get_scoreboard_by_match_id_with_session(
                created_match.id, session
            )

            if existing_scoreboard is None:
                scoreboard_schema: ScoreboardSchemaCreate | ScoreboardSchemaUpdate = (
                    ScoreboardSchemaCreate(
                        match_id=created_match.id,
                        scale_logo_a=2,
                        scale_logo_b=2,
                        team_a_game_color=team_a.team_color,
                        team_b_game_color=team_b.team_color,
                        team_a_game_title=team_a.title,
                        team_b_game_title=team_b.title,
                    )
                )
            else:
                existing_data = dict(existing_scoreboard.__dict__)
                default_data = ScoreboardSchemaCreate().model_dump()

                for key, value in default_data.items():
                    if key not in existing_data or existing_data[key] is None:
                        existing_data[key] = value

                # Remove SQLAlchemy internal keys
                existing_data = {k: v for k, v in existing_data.items() if not k.startswith("_")}
                scoreboard_schema = ScoreboardSchemaUpdate(**existing_data)

            created_scoreboard = await scoreboard_service.create_or_update_scoreboard(
                scoreboard_schema, session=session
            )

            # Get teams data
            teams_data = await match_service.get_teams_by_match(created_match.id, session=session)

            # Commit the transaction
            await session.commit()

            return {
                "id": created_match.id,
                "match_id": created_match.id,
                "status_code": 200,
                "match": created_match,
                "teams_data": teams_data,
                "match_data": match_data,
                "scoreboard_data": created_scoreboard,
            }

    async def _get_scoreboard_by_match_id_with_session(
        self,
        match_id: int,
        session: AsyncSession,
    ) -> ScoreboardDB | None:
        """Get scoreboard by match ID using provided session."""
        result = await session.scalars(
            select(ScoreboardDB).where(ScoreboardDB.match_id == match_id)
        )
        return result.one_or_none()

    async def create_parsed_single_match(self, eesl_match_id: int, match_service):
        """Create a single parsed match with batched DB operations.

        Uses a single session for all DB operations for atomic transactions
        and reduced session acquisitions.
        """
        self.logger.debug(f"Get and Save parsed match from eesl_id:{eesl_match_id}")

        try:
            self.logger.debug("Start parsing match")
            parsed_match_data = await parse_match_and_create_jsons(eesl_match_id)

            if not parsed_match_data:
                self.logger.warning("Match data is empty or None")
                return []

            playclock_service = PlayClockServiceDB(db)
            gameclock_service = GameClockServiceDB(db)
            scoreboard_service = ScoreboardServiceDB(db)
            match_data_service = MatchDataServiceDB(db)

            team_a_eesl_id = parsed_match_data.get("team_a_eesl_id")
            team_b_eesl_id = parsed_match_data.get("team_b_eesl_id")

            # Use a single session for all operations
            async with db.get_session_maker()() as session:
                # Fetch teams
                stmt = select(TeamDB).where(
                    TeamDB.team_eesl_id.in_([team_a_eesl_id, team_b_eesl_id])
                )
                results = await session.execute(stmt)
                teams_by_eesl_id = {team.team_eesl_id: team for team in results.scalars().all()}

                team_a = teams_by_eesl_id.get(team_a_eesl_id)
                if not team_a:
                    self.logger.error(f"Home team(a) not found - EESL ID: {team_a_eesl_id}")
                    return []

                team_b = teams_by_eesl_id.get(team_b_eesl_id)
                if not team_b:
                    self.logger.error(f"Away team(b) not found - EESL ID: {team_b_eesl_id}")
                    return []

                match_dict = {
                    "week": 0,
                    "match_eesl_id": eesl_match_id,
                    "team_a_id": team_a.id,
                    "team_b_id": team_b.id,
                    "match_date": parsed_match_data.get("match_date", ""),
                    "tournament_id": 1,
                }

                match_schema = MatchSchemaCreate(**match_dict)
                created_match = await match_service.create_or_update_match(
                    match_schema, session=session
                )

                sport = await match_service.get_sport_by_match_id(created_match.id, session=session)
                if self._sport_supports_playclock(sport):
                    playclock_schema = PlayClockSchemaCreate(match_id=created_match.id)
                    await playclock_service.create(playclock_schema, session=session)

                gameclock_schema = GameClockSchemaCreate(match_id=created_match.id)
                await gameclock_service.create(gameclock_schema, session=session)

                match_data_schema_create = MatchDataSchemaCreate(
                    match_id=created_match.id,
                    score_team_a=safe_int_conversion(parsed_match_data.get("score_a", "0")),
                    score_team_b=safe_int_conversion(parsed_match_data.get("score_b", "0")),
                )
                created_match_data = await match_data_service.create(
                    match_data_schema_create, session=session
                )

                scoreboard_schema = ScoreboardSchemaCreate(
                    match_id=created_match.id,
                    scale_logo_a=2,
                    scale_logo_b=2,
                    team_a_game_color=team_a.team_color,
                    team_b_game_color=team_b.team_color,
                    team_a_game_title=team_a.title,
                    team_b_game_title=team_b.title,
                )
                created_scoreboard = await scoreboard_service.create_or_update_scoreboard(
                    scoreboard_schema, session=session
                )

                teams_data = await match_service.get_teams_by_match(
                    created_match.id, session=session
                )

                # Commit the transaction
                await session.commit()

                self.logger.info(f"Created match after parsing: {created_match}")
                return [
                    {
                        "id": created_match.id,
                        "match_id": created_match.id,
                        "status_code": 200,
                        "match": created_match,
                        "teams_data": teams_data,
                        "match_data": created_match_data,
                        "scoreboard_data": created_scoreboard,
                    }
                ]
        except Exception as ex:
            self.logger.error(
                f"Error on parse and create match: {ex}",
                exc_info=True,
            )
            return []


match_parser = MatchParser()
