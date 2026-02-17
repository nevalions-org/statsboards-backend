import pytest
import pytest_asyncio

from src.auth.security import create_access_token, get_password_hash
from src.core.models import RoleDB, UserDB
from src.core.models.base import Database
from src.matches.db_services import MatchServiceDB
from src.matches.schemas import MatchSchemaCreate, MatchSchemaUpdate
from src.person.db_services import PersonServiceDB
from src.player.db_services import PlayerServiceDB
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.users.schemas import UserSchemaCreate


@pytest_asyncio.fixture
async def admin_user(test_db: Database):
    """Create a test admin user."""
    async with test_db.get_session_maker()() as db_session:
        role = RoleDB(name="admin", description="Admin role")
        db_session.add(role)
        await db_session.flush()
        await db_session.refresh(role)

        user_data = UserSchemaCreate(
            username="test_admin",
            email="admin@example.com",
            password="SecurePass123!",
        )
        user_obj = UserDB(
            username=user_data.username,
            email=user_data.email,
            hashed_password=get_password_hash(user_data.password),
        )
        user_obj.roles = [role]
        db_session.add(user_obj)
        await db_session.flush()
        await db_session.refresh(user_obj)

        token = create_access_token(data={"sub": str(user_obj.id)})

        yield {"user": user_obj, "token": token, "headers": {"Authorization": f"Bearer {token}"}}

        await db_session.delete(user_obj)
        await db_session.delete(role)


@pytest_asyncio.fixture()
async def person_service(test_db) -> PersonServiceDB:
    return PersonServiceDB(test_db)


@pytest_asyncio.fixture()
async def player_service(test_db) -> PlayerServiceDB:
    return PlayerServiceDB(test_db)


@pytest_asyncio.fixture()
async def player_team_tournament_service(test_db) -> PlayerTeamTournamentServiceDB:
    return PlayerTeamTournamentServiceDB(test_db)


@pytest.mark.asyncio
class TestMatchCRUDRouter:
    async def test_create_match_endpoint(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )

        response = await client.post("/api/matches/", json=match_data.model_dump(mode="json"))

        assert response.status_code == 200
        assert response.json()["id"] > 0

    async def test_create_match_endpoint_failure(self, client, test_db):
        invalid_data = {"team_a_id": 999, "team_b_id": 998, "tournament_id": 1, "week": 1}

        response = await client.post("/api/matches/", json=invalid_data)

        assert response.status_code == 409

    async def test_create_match_with_full_data_endpoint(
        self, client, test_db, teams_data, tournament, sponsor
    ):
        from src.tournaments.db_services import TournamentServiceDB
        from src.tournaments.schemas import TournamentSchemaUpdate

        team_a, team_b = teams_data
        tournament_service = TournamentServiceDB(test_db)
        await tournament_service.update(
            tournament.id, TournamentSchemaUpdate(main_sponsor_id=sponsor.id)
        )

        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )

        response = await client.post(
            "/api/matches/create_with_full_data/", json=match_data.model_dump(mode="json")
        )

        assert response.status_code == 200
        result = response.json()
        assert "match" in result
        assert "match_data" in result
        assert "teams_data" in result
        assert "scoreboard" in result
        assert result["status_code"] == 200

    async def test_get_match_by_eesl_id_endpoint(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
            match_eesl_id=12345,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/eesl_id/{created.match_eesl_id}/")

        assert response.status_code == 200
        assert "match_eesl_id" in response.json()

    async def test_get_match_by_eesl_id_not_found(self, client):
        response = await client.get("/api/matches/eesl_id/99999/")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_get_match_with_details_endpoint(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/with-details/")

        assert response.status_code == 200
        result = response.json()
        assert result["id"] == created.id
        assert "team_a" in result
        assert "team_b" in result
        assert "tournament" in result

    async def test_get_match_with_details_not_found(self, client):
        response = await client.get("/api/matches/id/99999/with-details/")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_get_match_sport_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament, sport
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/sport/")

        assert response.status_code == 200
        assert response.json()["id"] == sport.id

    async def test_get_match_teams_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/teams/")

        assert response.status_code == 200
        result = response.json()
        assert "team_a" in result
        assert "team_b" in result
        assert result["team_a"]["id"] == team_a.id
        assert result["team_b"]["id"] == team_b.id

    async def test_get_match_home_away_teams_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/home_away_teams/")

        assert response.status_code == 200
        result = response.json()
        assert "home_team" in result
        assert "away_team" in result
        assert result["home_team"]["id"] == team_a.id
        assert result["away_team"]["id"] == team_b.id

    async def test_get_players_by_match_id_endpoint(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/players/")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_get_players_with_full_data_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/players_fulldata/")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_get_available_players_for_team_in_match_endpoint(
        self,
        client,
        test_db,
        teams_data,
        tournament,
        person_service,
        player_service,
        player_team_tournament_service,
    ):
        from src.person.schemas import PersonSchemaCreate
        from src.player.schemas import PlayerSchemaCreate
        from src.player_team_tournament.schemas import PlayerTeamTournamentSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        person = await person_service.create(
            PersonSchemaCreate(
                first_name="Test",
                second_name="Player",
                person_dob="2000-01-01",
            )
        )
        player = await player_service.create(
            PlayerSchemaCreate(
                sport_id=team_a.sport_id,
                person_id=person.id,
            )
        )
        await player_team_tournament_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                team_id=team_a.id,
                tournament_id=tournament.id,
                player_number="10",
            )
        )

        response = await client.get(
            f"/api/matches/id/{created.id}/team/{team_a.id}/available-players/"
        )

        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["player_id"] == player.id

    async def test_get_team_rosters_for_match_endpoint(
        self,
        client,
        test_db,
        teams_data,
        tournament,
        person_service,
        player_service,
        player_team_tournament_service,
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/team-rosters/")

        assert response.status_code == 200
        result = response.json()
        assert "home_roster" in result
        assert "away_roster" in result
        assert "available_home" in result
        assert "available_away" in result

    async def test_get_team_rosters_for_match_not_found(self, client):
        response = await client.get("/api/matches/id/99999/team-rosters/")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_get_team_rosters_with_filters(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response_available_true = await client.get(
            f"/api/matches/id/{created.id}/team-rosters/?include_available=true&include_match_players=true"
        )
        assert response_available_true.status_code == 200

        response_available_false = await client.get(
            f"/api/matches/id/{created.id}/team-rosters/?include_available=false&include_match_players=false"
        )
        assert response_available_false.status_code == 200

    async def test_get_sponsor_line_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament, sponsor_line
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
            sponsor_line_id=sponsor_line.id,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/sponsor_line")

        assert response.status_code == 200
        result = response.json()
        assert "sponsor_line" in result
        assert result["sponsor_line"]["id"] == sponsor_line.id

    async def test_get_match_data_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        from src.matchdata.db_services import MatchDataServiceDB
        from src.matchdata.schemas import MatchDataSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        match_data_service = MatchDataServiceDB(test_db)
        await match_data_service.create(MatchDataSchemaCreate(match_id=created.id))

        response = await client.get(f"/api/matches/id/{created.id}/match_data/")

        assert response.status_code == 200
        assert response.json()["match_id"] == created.id

    async def test_get_playclock_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        from src.playclocks.db_services import PlayClockServiceDB
        from src.playclocks.schemas import PlayClockSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        playclock_service = PlayClockServiceDB(test_db)
        await playclock_service.create(PlayClockSchemaCreate(match_id=created.id))

        response = await client.get(f"/api/matches/id/{created.id}/playclock/")

        assert response.status_code == 200
        assert response.json()["match_id"] == created.id

    async def test_get_gameclock_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        from src.gameclocks.db_services import GameClockServiceDB
        from src.gameclocks.schemas import GameClockSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        gameclock_service = GameClockServiceDB(test_db)
        await gameclock_service.create(GameClockSchemaCreate(match_id=created.id))

        response = await client.get(f"/api/matches/id/{created.id}/gameclock/")

        assert response.status_code == 200
        assert response.json()["match_id"] == created.id

    async def test_get_scoreboard_by_match_id_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        from src.scoreboards.db_services import ScoreboardServiceDB
        from src.scoreboards.schemas import ScoreboardSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        scoreboard_service = ScoreboardServiceDB(test_db)
        await scoreboard_service.create(ScoreboardSchemaCreate(match_id=created.id))

        response = await client.get(f"/api/matches/id/{created.id}/scoreboard_data/")

        assert response.status_code == 200
        assert response.json()["match_id"] == created.id

    async def test_get_all_matches_data_endpoint(self, client, test_db, teams_data, tournament):
        from src.matchdata.db_services import MatchDataServiceDB
        from src.matchdata.schemas import MatchDataSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        match_data_service = MatchDataServiceDB(test_db)
        await match_data_service.create(MatchDataSchemaCreate(match_id=created.id))

        response = await client.get("/api/matches/all/data/")

        assert response.status_code == 200

    async def test_get_match_data_endpoint(self, client, test_db, teams_data, tournament):
        from src.matchdata.db_services import MatchDataServiceDB
        from src.matchdata.schemas import MatchDataSchemaCreate

        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        match_data_service = MatchDataServiceDB(test_db)
        await match_data_service.create(MatchDataSchemaCreate(match_id=created.id))

        response = await client.get(f"/api/matches/id/{created.id}/data/")

        assert response.status_code == 200
        result = response.json()
        # Endpoint returns tuple (dict,) due to implementation quirk
        assert isinstance(result, list) and len(result) > 0
        assert result[0]["status_code"] == 200
        assert "teams_data" in result[0]
        assert "match_data" in result[0]

    async def test_full_match_data_endpoint(self, client, test_db, teams_data, tournament):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}/scoreboard/full_data/")

        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    async def test_full_match_data_with_scoreboard_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(
            f"/api/matches/id/{created.id}/scoreboard/full_data/scoreboard_settings/"
        )

        assert response.status_code == 200
        assert isinstance(response.json(), dict)

    async def test_create_match_with_full_data_and_scoreboard_endpoint(
        self, client, test_db, teams_data, tournament, sponsor
    ):
        from src.tournaments.db_services import TournamentServiceDB
        from src.tournaments.schemas import TournamentSchemaUpdate

        team_a, team_b = teams_data
        tournament_service = TournamentServiceDB(test_db)
        await tournament_service.update(
            tournament.id, TournamentSchemaUpdate(main_sponsor_id=sponsor.id)
        )

        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )

        response = await client.post("/api/matches/add", json=match_data.model_dump(mode="json"))

        assert response.status_code == 200
        result = response.json()
        assert result["id"] > 0
        assert result["team_a_id"] == team_a.id
        assert result["team_b_id"] == team_b.id

    async def test_get_matches_with_details_paginated_endpoint(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-08",
                week=2,
            )
        )

        response = await client.get("/api/matches/with-details/paginated?page=1&items_per_page=10")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 2
        assert data["metadata"]["total_items"] == 2

    async def test_get_matches_with_details_paginated_with_week_filter(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-08",
                week=2,
            )
        )

        response = await client.get("/api/matches/with-details/paginated?week=1")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["week"] == 1
        assert data["metadata"]["total_items"] == 1

    async def test_get_matches_with_details_paginated_with_tournament_filter(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )

        response = await client.get(
            f"/api/matches/with-details/paginated?tournament_id={tournament.id}"
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["tournament_id"] == tournament.id

    async def test_delete_match_endpoint_success(
        self, client, test_db, teams_data, tournament, admin_user
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.delete(
            f"/api/matches/id/{created.id}", headers=admin_user["headers"]
        )

        assert response.status_code == 200
        assert "deleted successfully" in response.json()["detail"]

    async def test_delete_match_endpoint_not_found(self, client, admin_user):
        response = await client.delete("/api/matches/id/99999", headers=admin_user["headers"])

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_delete_match_endpoint_unauthorized(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.delete(f"/api/matches/id/{created.id}")

        assert response.status_code == 401

    async def test_update_match_endpoint_with_error_handling(
        self, client, test_db, teams_data, tournament
    ):
        team_a, team_b = teams_data
        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        update_data = MatchSchemaUpdate(week=2)
        response = await client.put(f"/api/matches/{created.id}/", json=update_data.model_dump())

        assert response.status_code == 200
        assert response.json()["week"] == 2
