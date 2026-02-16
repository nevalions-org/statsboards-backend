from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.models import (
    PersonDB,
    PlayerDB,
    PlayerMatchDB,
    PlayerTeamTournamentDB,
    PositionDB,
)
from src.core.models.base import Database
from src.helpers.photo_utils import photo_files_exist
from src.logging_config import get_logger
from src.matches.db_services import MatchServiceDB
from src.matches.schemas import MatchSchemaBase
from src.pars_eesl.pars_all_players_from_eesl import collect_player_full_data_eesl
from src.pars_eesl.pars_match import ParsedMatch, parse_match_and_create_jsons
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaCreate
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.player_team_tournament.schemas import (
    PlayerTeamTournamentSchemaCreate,
    PlayerTeamTournamentSchemaUpdate,
)
from src.positions.db_services import PositionServiceDB
from src.positions.schemas import PositionSchemaCreate
from src.teams.db_services import TeamServiceDB
from src.teams.schemas import TeamSchemaBase

from .db_services import PlayerMatchServiceDB
from .schemas import PlayerMatchSchemaCreate


class PlayerMatchParser:
    def __init__(self, database: Database):
        self.db = database
        self.logger = get_logger("PlayerMatchParser", self)
        self.logger.debug("Initialized PlayerMatchParser")

    async def create_parsed_match_players(self, eesl_match_id: int) -> list[dict[str, Any]]:
        """Parse match roster and create all player_match records with batched DB.

        Reduces DB sessions from 240-400 per match to ~6-10 by:
        - Pre-fetching all entities via bulk IN queries
        - Using a single session for all operations
        - Committing once at the end

        Args:
            eesl_match_id: The EESL match ID to parse

        Returns:
            List of created player_match records with related data
        """
        self.logger.debug(f"Start parsing eesl match with eesl_id:{eesl_match_id}")

        parsed_match: ParsedMatch | None = await parse_match_and_create_jsons(eesl_match_id)
        if not parsed_match:
            self.logger.warning("Parsed match is empty or None")
            return []

        match_service = MatchServiceDB(self.db)
        team_service = TeamServiceDB(self.db)
        position_service = PositionServiceDB(self.db)
        person_service = PersonServiceDB(self.db)
        player_service = PlayerServiceDB(self.db)
        ptt_service = PlayerTeamTournamentServiceDB(self.db)
        player_match_service = PlayerMatchServiceDB(self.db)

        async with self.db.get_session_maker()() as session:
            match: MatchSchemaBase | None = await match_service.get_match_by_eesl_id(
                eesl_match_id, session=session
            )
            if not match:
                self.logger.error(f"Match with eesl_id {eesl_match_id} not found in DB")
                return []

            team_a: TeamSchemaBase | None = await team_service.get_team_by_eesl_id(
                parsed_match["team_a_eesl_id"], session=session
            )
            team_b: TeamSchemaBase | None = await team_service.get_team_by_eesl_id(
                parsed_match["team_b_eesl_id"], session=session
            )

            if not team_a or not team_b:
                self.logger.error(
                    f"Teams not found: team_a_eesl_id={parsed_match['team_a_eesl_id']}, "
                    f"team_b_eesl_id={parsed_match['team_b_eesl_id']}"
                )
                return []

            roster_a = parsed_match.get("roster_a") or []
            roster_b = parsed_match.get("roster_b") or []

            all_position_titles = self._collect_position_titles(roster_a + roster_b)
            all_eesl_ids = self._collect_player_eesl_ids(roster_a + roster_b)

            positions_by_title = await self._prefetch_positions(all_position_titles, session)
            persons_by_eesl_id = await self._prefetch_persons(all_eesl_ids, session)
            players_by_eesl_id = await self._prefetch_players(all_eesl_ids, session)
            ptts_by_eesl_id = await self._prefetch_player_team_tournaments(all_eesl_ids, session)
            existing_pms_by_eesl_id = await self._prefetch_player_matches(match.id, session)

            created_players_match: list[dict[str, Any]] = []
            existing_player_ids: set[int] = set()

            results_a = await self._process_roster(
                roster=roster_a,
                team=team_a,
                match=match,
                session=session,
                position_service=position_service,
                person_service=person_service,
                player_service=player_service,
                ptt_service=ptt_service,
                player_match_service=player_match_service,
                positions_by_title=positions_by_title,
                persons_by_eesl_id=persons_by_eesl_id,
                players_by_eesl_id=players_by_eesl_id,
                ptts_by_eesl_id=ptts_by_eesl_id,
                existing_pms_by_eesl_id=existing_pms_by_eesl_id,
                existing_player_ids=existing_player_ids,
                sport_id=1,
            )
            created_players_match.extend(results_a)

            results_b = await self._process_roster(
                roster=roster_b,
                team=team_b,
                match=match,
                session=session,
                position_service=position_service,
                person_service=person_service,
                player_service=player_service,
                ptt_service=ptt_service,
                player_match_service=player_match_service,
                positions_by_title=positions_by_title,
                persons_by_eesl_id=persons_by_eesl_id,
                players_by_eesl_id=players_by_eesl_id,
                ptts_by_eesl_id=ptts_by_eesl_id,
                existing_pms_by_eesl_id=existing_pms_by_eesl_id,
                existing_player_ids=existing_player_ids,
                sport_id=1,
            )
            created_players_match.extend(results_b)

            await session.commit()
            return created_players_match

    def _collect_position_titles(self, roster: list[Any]) -> list[str]:
        """Collect unique position titles from roster."""
        titles = set()
        for player in roster:
            if player and player.get("player_position"):
                title = player["player_position"].strip().upper()
                if title:
                    titles.add(title)
        return list(titles)

    def _collect_player_eesl_ids(self, roster: list[Any]) -> list[int]:
        """Collect unique player EESL IDs from roster."""
        eesl_ids = set()
        for player in roster:
            if player and player.get("player_eesl_id"):
                eesl_ids.add(player["player_eesl_id"])
        return list(eesl_ids)

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

    async def _prefetch_persons(
        self, eesl_ids: list[int], session: AsyncSession
    ) -> dict[int, PersonDB]:
        """Pre-fetch persons by EESL IDs using a single IN query."""
        if not eesl_ids:
            return {}

        stmt = select(PersonDB).where(PersonDB.person_eesl_id.in_(eesl_ids))
        results = await session.execute(stmt)
        persons = results.scalars().all()

        return {p.person_eesl_id: p for p in persons if p.person_eesl_id is not None}

    async def _prefetch_players(
        self, eesl_ids: list[int], session: AsyncSession
    ) -> dict[int, PlayerDB]:
        """Pre-fetch players by EESL IDs using a single IN query."""
        if not eesl_ids:
            return {}

        stmt = select(PlayerDB).where(PlayerDB.player_eesl_id.in_(eesl_ids))
        results = await session.execute(stmt)
        players = results.scalars().all()

        return {p.player_eesl_id: p for p in players if p.player_eesl_id is not None}

    async def _prefetch_player_team_tournaments(
        self, eesl_ids: list[int], session: AsyncSession
    ) -> dict[int, PlayerTeamTournamentDB]:
        """Pre-fetch player_team_tournaments by EESL IDs using a single IN query."""
        if not eesl_ids:
            return {}

        stmt = select(PlayerTeamTournamentDB).where(
            PlayerTeamTournamentDB.player_team_tournament_eesl_id.in_(eesl_ids)
        )
        results = await session.execute(stmt)
        ptts = results.scalars().all()

        return {
            p.player_team_tournament_eesl_id: p
            for p in ptts
            if p.player_team_tournament_eesl_id is not None
        }

    async def _prefetch_player_matches(
        self, match_id: int, session: AsyncSession
    ) -> dict[int, PlayerMatchDB]:
        """Pre-fetch existing player_matches for this match."""
        stmt = select(PlayerMatchDB).where(PlayerMatchDB.match_id == match_id)
        results = await session.execute(stmt)
        pms = results.scalars().all()

        return {pm.player_match_eesl_id: pm for pm in pms if pm.player_match_eesl_id is not None}

    async def _process_roster(
        self,
        roster: list[Any],
        team: TeamSchemaBase,
        match: MatchSchemaBase,
        session: AsyncSession,
        position_service: PositionServiceDB,
        person_service: PersonServiceDB,
        player_service: PlayerServiceDB,
        ptt_service: PlayerTeamTournamentServiceDB,
        player_match_service: PlayerMatchServiceDB,
        positions_by_title: dict[str, PositionDB],
        persons_by_eesl_id: dict[int, PersonDB],
        players_by_eesl_id: dict[int, PlayerDB],
        ptts_by_eesl_id: dict[int, PlayerTeamTournamentDB],
        existing_pms_by_eesl_id: dict[int, PlayerMatchDB],
        existing_player_ids: set[int],
        sport_id: int,
    ) -> list[dict[str, Any]]:
        """Process a single roster (home or away) - shared logic, DRY.

        Args:
            roster: List of parsed player data
            team: Team DB object
            match: Match DB object
            session: Database session
            *_service: Service instances for DB operations
            *_by_*: Pre-fetched lookup dictionaries
            existing_player_ids: Set to track processed players
            sport_id: Sport ID

        Returns:
            List of created player_match records with related data
        """
        results: list[dict[str, Any]] = []

        for player_data in roster:
            if not player_data:
                continue

            player_position = player_data.get("player_position", "").strip()
            if not player_position:
                self.logger.debug(
                    f"Skipping player {player_data.get('player_eesl_id')} - no position"
                )
                continue

            player_eesl_id = player_data.get("player_eesl_id")
            if player_eesl_id is None:
                continue

            position = await self._get_or_create_position(
                player_position,
                sport_id,
                session,
                position_service,
                positions_by_title,
            )
            if not position:
                self.logger.warning(f"Could not get/create position for {player_position}")
                continue

            person = await self._get_or_create_person(
                player_data,
                player_eesl_id,
                session,
                person_service,
                persons_by_eesl_id,
            )
            if not person:
                self.logger.warning(f"Could not get/create person for {player_eesl_id}")
                continue

            player = await self._get_or_create_player(
                player_eesl_id,
                person.id,
                sport_id,
                session,
                player_service,
                players_by_eesl_id,
            )
            if not player:
                self.logger.warning(f"Could not get/create player for {player_eesl_id}")
                continue

            player_in_team = await self._get_or_create_ptt(
                player_data,
                player_eesl_id,
                player.id,
                position.id,
                team.id,
                match.tournament_id,
                session,
                ptt_service,
                ptts_by_eesl_id,
            )
            if not player_in_team:
                self.logger.warning(f"Could not get/create PTT for {player_eesl_id}")
                continue

            if player_eesl_id in existing_player_ids:
                self.logger.debug(f"Player {player_eesl_id} already processed, skipping")
                continue

            existing_player_ids.add(player_eesl_id)

            player_match_result = await self._create_player_match(
                player_eesl_id,
                player_in_team.id,
                position.id,
                match.id,
                player_data.get("player_number", "0"),
                team.id,
                session,
                player_match_service,
                existing_pms_by_eesl_id,
                positions_by_title,
            )

            if player_match_result:
                results.append(
                    {
                        "match_player": player_match_result["player_match"],
                        "person": person,
                        "player_team_tournament": player_in_team,
                        "position": player_match_result["position"],
                    }
                )

        return results

    async def _get_or_create_position(
        self,
        title: str,
        sport_id: int,
        session: AsyncSession,
        position_service: PositionServiceDB,
        positions_by_title: dict[str, PositionDB],
    ) -> PositionDB | None:
        """Get or create a position by title."""
        normalized_title = title.upper().strip()

        position = positions_by_title.get(normalized_title)
        if position:
            return position

        position_schema = PositionSchemaCreate(title=normalized_title, sport_id=sport_id)
        position = await position_service.create(position_schema, session=session)
        if position:
            positions_by_title[normalized_title] = position
        return position

    async def _get_or_create_person(
        self,
        player_data: dict[str, Any],
        player_eesl_id: int,
        session: AsyncSession,
        person_service: PersonServiceDB,
        persons_by_eesl_id: dict[int, PersonDB],
    ) -> PersonDB | None:
        """Get or create a person, fetching full data from EESL if needed."""
        person = persons_by_eesl_id.get(player_eesl_id)

        needs_photo_download = (
            person is None
            or not person.person_photo_url
            or not person.person_photo_icon_url
            or not person.person_photo_web_url
            or not photo_files_exist(person.person_photo_url)
        )

        if needs_photo_download:
            self.logger.debug(f"Fetching full data for person {player_eesl_id}")
            player_in_team = await collect_player_full_data_eesl(player_eesl_id)
            if player_in_team is None:
                self.logger.warning(f"Failed to fetch player data for {player_eesl_id}")
                if person is None:
                    return None
                return person

            person_schema = PersonSchemaCreate(**player_in_team["person"])
            person = await person_service.create_or_update_person(person_schema, session=session)
            if person and person.person_eesl_id:
                persons_by_eesl_id[person.person_eesl_id] = person

        return person

    async def _get_or_create_player(
        self,
        player_eesl_id: int,
        person_id: int,
        sport_id: int,
        session: AsyncSession,
        player_service: PlayerServiceDB,
        players_by_eesl_id: dict[int, PlayerDB],
    ) -> PlayerDB | None:
        """Get or create a player."""
        player = players_by_eesl_id.get(player_eesl_id)
        if player:
            return player

        player_schema = PlayerSchemaCreate(
            sport_id=sport_id,
            person_id=person_id,
            player_eesl_id=player_eesl_id,
        )
        player = await player_service.create_or_update_player(player_schema, session=session)
        if player and player.player_eesl_id:
            players_by_eesl_id[player.player_eesl_id] = player
        return player

    async def _get_or_create_ptt(
        self,
        player_data: dict[str, Any],
        player_eesl_id: int,
        player_id: int,
        position_id: int,
        team_id: int,
        tournament_id: int,
        session: AsyncSession,
        ptt_service: PlayerTeamTournamentServiceDB,
        ptts_by_eesl_id: dict[int, PlayerTeamTournamentDB],
    ) -> PlayerTeamTournamentDB | None:
        """Get or create a player_team_tournament."""
        player_number = player_data.get("player_number", "0")

        ptt = ptts_by_eesl_id.get(player_eesl_id)
        if ptt:
            update_schema = PlayerTeamTournamentSchemaUpdate(
                player_team_tournament_eesl_id=player_eesl_id,
                player_id=player_id,
                position_id=position_id,
                team_id=team_id,
                tournament_id=tournament_id,
                player_number=player_number,
            )
            ptt = await ptt_service.create_or_update_player_team_tournament(
                update_schema, session=session
            )
            if ptt and ptt.player_team_tournament_eesl_id:
                ptts_by_eesl_id[ptt.player_team_tournament_eesl_id] = ptt
            return ptt

        create_schema = PlayerTeamTournamentSchemaCreate(
            player_team_tournament_eesl_id=player_eesl_id,
            player_id=player_id,
            position_id=position_id,
            team_id=team_id,
            tournament_id=tournament_id,
            player_number=player_number,
        )
        ptt = await ptt_service.create_or_update_player_team_tournament(
            create_schema, session=session
        )
        if ptt and ptt.player_team_tournament_eesl_id:
            ptts_by_eesl_id[ptt.player_team_tournament_eesl_id] = ptt
        return ptt

    async def _create_player_match(
        self,
        player_eesl_id: int,
        ptt_id: int,
        position_id: int,
        match_id: int,
        match_number: str,
        team_id: int,
        session: AsyncSession,
        player_match_service: PlayerMatchServiceDB,
        existing_pms_by_eesl_id: dict[int, PlayerMatchDB],
        positions_by_title: dict[str, PositionDB],
    ) -> dict[str, Any] | None:
        """Create or update a player_match record."""
        player_schema = {
            "player_match_eesl_id": player_eesl_id,
            "player_team_tournament_id": ptt_id,
            "match_position_id": position_id,
            "match_id": match_id,
            "match_number": match_number,
            "team_id": team_id,
            "is_start": False,
        }

        position: PositionDB | None = None

        existing_pm = existing_pms_by_eesl_id.get(player_eesl_id)
        if existing_pm:
            self.logger.debug(f"Player match exists for eesl_id {player_eesl_id}")
            if existing_pm.is_start:
                self.logger.warning(f"Player match {player_eesl_id} is in start")
                player_schema["player_match_eesl_id"] = existing_pm.player_match_eesl_id
                player_schema["player_team_tournament_id"] = existing_pm.player_team_tournament_id
                player_schema["match_position_id"] = existing_pm.match_position_id
                player_schema["match_id"] = existing_pm.match_id
                player_schema["match_number"] = existing_pm.match_number
                player_schema["team_id"] = existing_pm.team_id
                player_schema["is_start"] = True

                if existing_pm.match_position_id:
                    stmt = select(PositionDB).where(PositionDB.id == existing_pm.match_position_id)
                    result = await session.execute(stmt)
                    position = result.scalars().one_or_none()

        player_match_create = PlayerMatchSchemaCreate(**player_schema)
        created_player = await player_match_service.create_or_update_player_match(
            player_match_create, session=session
        )

        if not position:
            for pos in positions_by_title.values():
                if pos.id == position_id:
                    position = pos
                    break

        if created_player:
            if created_player.player_match_eesl_id:
                existing_pms_by_eesl_id[created_player.player_match_eesl_id] = created_player
            return {
                "player_match": created_player,
                "position": position,
            }

        return None
