from datetime import datetime

import pytest
import pytest_asyncio

from src.core.models.base import Database
from src.matches.db_services import MatchServiceDB
from src.matches.schemas import MatchSchemaUpdate
from src.seasons.db_services import SeasonServiceDB
from src.seasons.schemas import SeasonSchemaCreate
from src.sports.db_services import SportServiceDB
from src.sports.schemas import SportSchemaCreate
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB
from src.tournaments.schemas import TournamentSchemaCreate
from tests.factories import (
    MatchFactory,
    SeasonFactorySample,
    SportFactorySample,
    TeamFactory,
    TournamentFactory,
)


@pytest.fixture(scope="function")
def sport(test_db: Database):
    return SportFactorySample.build()


@pytest.fixture(scope="function")
def season(test_db: Database):
    return SeasonFactorySample.build(year=2024)


@pytest_asyncio.fixture(scope="function")
async def tournament_data(test_db: Database, sport: SportSchemaCreate, season: SeasonSchemaCreate):
    sport_service = SportServiceDB(test_db)
    created_sport = await sport_service.create(sport)

    season_service = SeasonServiceDB(test_db)
    created_season = await season_service.create(season)

    return TournamentFactory.build(
        sport_id=created_sport.id, season_id=created_season.id, tournament_eesl_id=900
    )


@pytest_asyncio.fixture(scope="function")
async def teams_data(test_db: Database, sport: SportSchemaCreate):
    sport_service = SportServiceDB(test_db)
    created_sport = await sport_service.create(sport)

    team_service = TeamServiceDB(test_db)

    team1 = TeamFactory.build(sport_id=created_sport.id, team_eesl_id=801, title="Team A")
    team2 = TeamFactory.build(sport_id=created_sport.id, team_eesl_id=802, title="Team B")

    created_team1 = await team_service.create_or_update_team(team1)
    created_team2 = await team_service.create_or_update_team(team2)

    return created_team1, created_team2


@pytest.mark.asyncio
class TestMatchServiceDBCreateOrUpdate:
    """Test create_or_update_match functionality."""

    async def test_create_match_with_eesl_id(
        self, test_db: Database, tournament_data: TournamentSchemaCreate, teams_data
    ):
        """Test creating a match with eesl_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=100,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )

        result = await match_service.create_or_update_match(match_data)

        assert result is not None
        assert result.id is not None
        assert result.match_eesl_id == 100
        assert result.tournament_id == created_tournament.id

    async def test_create_match_without_eesl_id(
        self, test_db: Database, tournament_data: TournamentSchemaCreate, teams_data
    ):
        """Test creating a match without eesl_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=None,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )

        result = await match_service.create_or_update_match(match_data)

        assert result is not None
        assert result.id is not None
        assert result.match_eesl_id is None

    async def test_update_existing_match_by_eesl_id(
        self, test_db: Database, tournament_data: TournamentSchemaCreate, teams_data
    ):
        """Test updating an existing match by eesl_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)

        match_data = MatchFactory.build(
            match_eesl_id=200,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            week=1,
            match_date=datetime.now(),
        )
        created = await match_service.create_or_update_match(match_data)

        update_data = MatchFactory.build(
            match_eesl_id=200,
            week=2,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )

        updated = await match_service.create_or_update_match(update_data)

        assert updated.id == created.id
        assert updated.week == 2

    async def test_create_multiple_matches(
        self, test_db: Database, tournament_data: TournamentSchemaCreate, teams_data
    ):
        """Test creating multiple matches."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)

        match1_data = MatchFactory.build(
            match_eesl_id=301,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        match2_data = MatchFactory.build(
            match_eesl_id=302,
            tournament_id=created_tournament.id,
            team_a_id=team_b.id,
            team_b_id=team_a.id,
            match_date=datetime.now(),
        )

        match1 = await match_service.create_or_update_match(match1_data)
        match2 = await match_service.create_or_update_match(match2_data)

        assert match1.match_eesl_id == 301
        assert match2.match_eesl_id == 302
        assert match1.id != match2.id

    async def test_get_match_by_eesl_id(
        self, test_db: Database, tournament_data: TournamentSchemaCreate, teams_data
    ):
        """Test retrieving match by eesl_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)

        match_data = MatchFactory.build(
            match_eesl_id=400,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create_or_update_match(match_data)

        retrieved = await match_service.get_match_by_eesl_id(400)

        assert retrieved is not None
        assert retrieved.id == created.id

    async def test_get_match_by_eesl_id_not_found(self, test_db: Database):
        """Test retrieving non-existent match by eesl_id returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_match_by_eesl_id(99999)
        assert result is None

    async def test_get_match_with_details(self, test_db: Database, tournament_data, teams_data):
        """Test retrieving match with all related details."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=500,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_match_with_details(created.id)

        assert result is not None
        assert result.id == created.id
        assert result.team_a is not None
        assert result.team_b is not None
        assert result.tournaments is not None

    async def test_get_match_with_details_not_found(self, test_db: Database):
        """Test retrieving non-existent match with details returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_match_with_details(99999)
        assert result is None

    async def test_get_scoreboard_fetch_bundle(
        self, test_db: Database, tournament_data, teams_data
    ):
        """Test bundled scoreboard fetch data includes key related entities."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        created = await match_service.create(
            MatchFactory.build(
                match_eesl_id=501,
                tournament_id=created_tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date=datetime.now(),
            )
        )

        result = await match_service.get_scoreboard_fetch_bundle(created.id)

        assert result is not None
        assert result["match"].id == created.id
        assert result["teams"]["team_a"]["id"] == team_a.id
        assert result["teams"]["team_b"]["id"] == team_b.id
        assert result["sport"] is not None
        assert result["sport"].id == created_tournament.sport_id
        assert result["scoreboard"] is None
        assert result["match_data"] is None
        assert result["players"] == []
        assert result["events"] == []

    async def test_get_scoreboard_fetch_bundle_with_players(
        self, test_db: Database, tournament_data, teams_data
    ):
        """Test bundled fetch handles match with players (regression for scalar match_players)."""
        from src.person.db_services import PersonServiceDB
        from src.person.schemas import PersonSchemaCreate
        from src.player.db_services import PlayerServiceDB
        from src.player.schemas import PlayerSchemaCreate
        from src.player_match.db_services import PlayerMatchServiceDB
        from src.player_match.schemas import PlayerMatchSchemaCreate
        from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
        from src.player_team_tournament.schemas import PlayerTeamTournamentSchemaCreate

        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        created = await match_service.create(
            MatchFactory.build(
                match_eesl_id=502,
                tournament_id=created_tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date=datetime.now(),
            )
        )

        person_service = PersonServiceDB(test_db)
        person = await person_service.create(
            PersonSchemaCreate(
                person_name="Test", person_surname="Player", person_dob=datetime(2000, 1, 1)
            )
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create(
            PlayerSchemaCreate(sport_id=created_tournament.sport_id, person_id=person.id)
        )

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                team_id=team_a.id,
                tournament_id=created_tournament.id,
                player_number="10",
            )
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        await player_match_service.create_or_update_player_match(
            PlayerMatchSchemaCreate(
                player_match_eesl_id=999,
                player_team_tournament_id=ptt.id,
                match_id=created.id,
                team_id=team_a.id,
                match_number="10",
                is_start=True,
            )
        )

        result = await match_service.get_scoreboard_fetch_bundle(created.id)

        assert result is not None
        assert result["match"].id == created.id
        assert result["teams"]["team_a"]["id"] == team_a.id
        assert isinstance(result["players"], list)
        assert len(result["players"]) == 1
        assert result["players"][0]["player_id"] == player.id

    async def test_get_scoreboard_fetch_bundle_not_found(self, test_db: Database):
        """Test bundled scoreboard fetch data returns None for missing match."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_scoreboard_fetch_bundle(99999)
        assert result is None

    async def test_get_sport_by_match_id(self, test_db: Database, tournament_data, teams_data):
        """Test retrieving sport by match_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=600,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_sport_by_match_id(created.id)

        assert result is not None
        assert result.id == created_tournament.sport_id

    async def test_get_sport_by_match_id_not_found(self, test_db: Database):
        """Test retrieving sport for non-existent match returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_sport_by_match_id(99999)
        assert result is None

    async def test_get_teams_by_match_id(self, test_db: Database, tournament_data, teams_data):
        """Test retrieving teams by match_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=700,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_teams_by_match_id(created.id)

        assert result is not None
        assert "home_team" in result
        assert "away_team" in result
        assert result["home_team"].id == team_a.id
        assert result["away_team"].id == team_b.id

    async def test_get_teams_by_match_not_found(self, test_db: Database):
        """Test retrieving teams for non-existent match returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_teams_by_match_id(99999)
        assert result is None

    async def test_get_teams_by_match_v2(self, test_db: Database, tournament_data, teams_data):
        """Test retrieving teams v2 by match_id."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=800,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_teams_by_match(created.id)

        assert result is not None
        assert "team_a" in result
        assert "team_b" in result

    async def test_get_players_by_match_empty(self, test_db: Database, tournament_data, teams_data):
        """Test retrieving players for match with no players returns empty list."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=900,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_players_by_match(created.id)

        assert result == []

    async def test_get_player_by_match_full_data_empty(
        self, test_db: Database, tournament_data, teams_data
    ):
        """Test retrieving full player data for match with no players returns empty list."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=1000,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_player_by_match_full_data(created.id)

        assert result == []

    async def test_get_players_with_full_data_optimized_empty(
        self, test_db: Database, tournament_data, teams_data
    ):
        """Test retrieving optimized full player data for match with no players returns empty list."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=1100,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        result = await match_service.get_players_with_full_data_optimized(created.id)

        assert result == []

    async def test_get_scoreboard_by_match_not_found(self, test_db: Database):
        """Test retrieving scoreboard for non-existent match returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_scoreboard_by_match(99999)
        assert result is None

    async def test_get_match_full_context_not_found(self, test_db: Database):
        """Test retrieving full context for non-existent match returns None."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_match_full_context(99999)
        assert result is None

    async def test_get_available_players_for_team_in_match_not_found(self, test_db: Database):
        """Test retrieving available players for non-existent match returns empty list."""
        match_service = MatchServiceDB(test_db)
        result = await match_service.get_available_players_for_team_in_match(
            match_id=99999, team_id=1
        )
        assert result == []

    async def test_update_match(self, test_db: Database, tournament_data, teams_data):
        """Test updating an existing match."""
        tournament_service = TournamentServiceDB(test_db)
        created_tournament = await tournament_service.create_or_update_tournament(tournament_data)

        team_a, team_b = teams_data

        match_service = MatchServiceDB(test_db)
        match_data = MatchFactory.build(
            match_eesl_id=1200,
            tournament_id=created_tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            week=1,
            match_date=datetime.now(),
        )
        created = await match_service.create(match_data)

        update_data = MatchSchemaUpdate(week=5, match_eesl_id=1200)
        updated = await match_service.update(created.id, update_data)

        assert updated.id == created.id
        assert updated.week == 5
