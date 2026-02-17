from io import BytesIO
from unittest.mock import Mock, patch

import pytest
from PIL import Image

from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate as PersonSchemaCreateType
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaCreate as PlayerSchemaCreateType
from src.seasons.db_services import SeasonServiceDB
from src.sports.db_services import SportServiceDB
from src.teams.db_services import TeamServiceDB
from src.teams.schemas import TeamSchemaCreate as TeamSchemaCreateType
from src.tournaments.db_services import TournamentServiceDB
from src.tournaments.schemas import TournamentSchemaUpdate
from tests.factories import (
    PersonFactory,
    SeasonFactorySample,
    SportFactoryAny,
    SportFactorySample,
    TournamentFactory,
)


def create_test_image():
    img = Image.new("RGB", (100, 100), color="red")
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


async def create_admin_headers(test_db) -> dict[str, str]:
    from src.auth.security import create_access_token, get_password_hash
    from src.core.models import RoleDB, UserDB, UserRoleDB

    async with test_db.get_session_maker()() as session:
        role = RoleDB(name="admin", description="Admin role")
        session.add(role)
        await session.flush()

        user = UserDB(
            username="test_admin",
            email="admin@test.com",
            hashed_password=get_password_hash("SecurePass123!"),
        )
        session.add(user)
        await session.flush()

        session.add(UserRoleDB(user_id=user.id, role_id=role.id))
        await session.flush()
        await session.refresh(user, ["roles"])

    token = create_access_token(data={"sub": str(user.id)})
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
class TestTournamentViews:
    async def test_create_tournament_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_data = TournamentFactory.build(sport_id=sport.id, season_id=season.id)

        response = await client.post("/api/tournaments/", json=tournament_data.model_dump())

        assert response.status_code == 200
        assert response.json()["id"] > 0

    async def test_update_tournament_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament_data = TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        created = await tournament_service.create_or_update_tournament(tournament_data)

        update_data = TournamentSchemaUpdate(title="Updated Title")

        response = await client.put(
            f"/api/tournaments/{created.id}/", json=update_data.model_dump()
        )

        assert response.status_code == 200

    async def test_update_tournament_not_found(self, client):
        update_data = TournamentSchemaUpdate(title="Updated Title")

        response = await client.put("/api/tournaments/99999/", json=update_data.model_dump())

        assert response.status_code == 404

    async def test_get_tournament_by_eesl_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament_data = TournamentFactory.build(
            sport_id=sport.id, season_id=season.id, tournament_eesl_id=100
        )
        created = await tournament_service.create_or_update_tournament(tournament_data)

        response = await client.get("/api/tournaments/eesl_id/100")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_tournament_by_eesl_id_not_found(self, client):
        response = await client.get("/api/tournaments/eesl_id/99999")

        assert response.status_code == 404

    async def test_get_teams_by_tournament_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        response = await client.get(f"/api/tournaments/id/{tournament.id}/teams/")

        assert response.status_code == 200

    async def test_get_teams_by_tournament_id_sorted_by_title(self, client, test_db):
        """Test that teams are sorted by title."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team1 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Zebras",
                sport_id=sport.id,
                team_eesl_id=1003,
            )
        )
        team2 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Alpha United",
                sport_id=sport.id,
                team_eesl_id=1001,
            )
        )
        team3 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Midland FC",
                sport_id=sport.id,
                team_eesl_id=1002,
            )
        )

        from src.core.models.team_tournament import TeamTournamentDB

        async with test_db.get_session_maker()() as session:
            tt1 = TeamTournamentDB(tournament_id=tournament.id, team_id=team1.id)
            tt2 = TeamTournamentDB(tournament_id=tournament.id, team_id=team2.id)
            tt3 = TeamTournamentDB(tournament_id=tournament.id, team_id=team3.id)
            session.add_all([tt1, tt2, tt3])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/teams/")

        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 3
        assert response_data[0]["title"] == "Alpha United"
        assert response_data[1]["title"] == "Midland FC"
        assert response_data[2]["title"] == "Zebras"

    async def test_get_players_by_tournament_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/")

        assert response.status_code == 200

    async def test_get_players_by_tournament_id_sorted_by_name(self, client, test_db):
        """Test that players are sorted by name (second_name, first_name)."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB
        from src.player.schemas import PlayerSchemaCreate

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        player_service = PlayerServiceDB(test_db)

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create(
            PersonFactory.build(second_name="Zimmerman", first_name="Alice")
        )
        person2 = await person_service.create(
            PersonFactory.build(second_name="Brown", first_name="Bob")
        )
        person3 = await person_service.create(
            PersonFactory.build(second_name="Brown", first_name="Alice")
        )

        player1 = await player_service.create(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person1.id)
        )
        player2 = await player_service.create(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person2.id)
        )
        player3 = await player_service.create(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person3.id)
        )

        async with test_db.get_session_maker()() as session:
            ptt1 = PlayerTeamTournamentDB(tournament_id=tournament.id, player_id=player1.id)
            ptt2 = PlayerTeamTournamentDB(tournament_id=tournament.id, player_id=player2.id)
            ptt3 = PlayerTeamTournamentDB(tournament_id=tournament.id, player_id=player3.id)
            session.add_all([ptt1, ptt2, ptt3])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/")

        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 3
        player_ids = [p["player_id"] for p in response_data]
        assert player_ids == [player3.id, player2.id, player1.id]

    async def test_get_count_of_matches_by_tournament_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        response = await client.get(f"/api/tournaments/id/{tournament.id}/matches/count")

        assert response.status_code == 200

    async def test_get_matches_by_tournament_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        response = await client.get(f"/api/tournaments/id/{tournament.id}/matches/")

        assert response.status_code == 200

    async def test_get_all_tournaments_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )
        await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        response = await client.get("/api/tournaments/")

        assert response.status_code == 200
        assert len(response.json()) >= 2

    async def test_get_tournament_by_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament_data = TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        created = await tournament_service.create_or_update_tournament(tournament_data)

        response = await client.get(f"/api/tournaments/id/{created.id}")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_tournament_by_id_not_found(self, client):
        response = await client.get("/api/tournaments/id/99999")

        assert response.status_code == 404

    async def test_upload_tournament_logo_endpoint(self, client):
        file_content = create_test_image()
        files = {"file": ("test_logo.png", BytesIO(file_content), "image/png")}
        response = await client.post("/api/tournaments/upload_logo", files=files)

        assert response.status_code == 200
        assert "logoUrl" in response.json()
        assert "tournaments/logos" in response.json()["logoUrl"]

    async def test_upload_tournament_logo_with_invalid_file(self, client):
        file_content = b"not a valid image"
        files = {"file": ("test_invalid.txt", BytesIO(file_content), "text/plain")}
        response = await client.post("/api/tournaments/upload_logo", files=files)

        assert response.status_code == 400

    async def test_upload_and_resize_tournament_logo_endpoint(self, client):
        file_content = create_test_image()
        files = {"file": ("test_logo.png", BytesIO(file_content), "image/png")}
        response = await client.post("/api/tournaments/upload_resize_logo", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert "original" in response_data
        assert "icon" in response_data
        assert "webview" in response_data
        assert "tournaments/logos" in response_data["original"]
        assert "tournaments/logos" in response_data["icon"]
        assert "tournaments/logos" in response_data["webview"]

    async def test_upload_and_resize_tournament_logo_with_invalid_file(self, client):
        file_content = b"not a valid image"
        files = {"file": ("test_invalid.txt", BytesIO(file_content), "text/plain")}
        response = await client.post("/api/tournaments/upload_resize_logo", files=files)

        assert response.status_code == 400

    async def test_get_teams_by_tournament_paginated_endpoint(self, client, test_db):
        """Test GET /api/tournaments/id/{tournament_id}/teams/paginated endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team1 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Manchester United",
                sport_id=sport.id,
                team_eesl_id=1001,
            )
        )
        team2 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Manchester City",
                sport_id=sport.id,
                team_eesl_id=1002,
            )
        )
        team3 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Liverpool",
                sport_id=sport.id,
                team_eesl_id=1003,
            )
        )

        from src.core.models.team_tournament import TeamTournamentDB

        async with test_db.get_session_maker()() as session:
            tt1 = TeamTournamentDB(tournament_id=tournament.id, team_id=team1.id)
            tt2 = TeamTournamentDB(tournament_id=tournament.id, team_id=team2.id)
            tt3 = TeamTournamentDB(tournament_id=tournament.id, team_id=team3.id)
            session.add_all([tt1, tt2, tt3])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/teams/paginated")

        assert response.status_code == 200
        response_data = response.json()
        assert "data" in response_data
        assert "metadata" in response_data
        assert len(response_data["data"]) == 3
        assert response_data["metadata"]["total_items"] == 3
        assert response_data["metadata"]["page"] == 1
        assert response_data["metadata"]["items_per_page"] == 20

    async def test_get_teams_by_tournament_paginated_with_search_endpoint(self, client, test_db):
        """Test GET /api/tournaments/id/{tournament_id}/teams/paginated with search."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team1 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Manchester United",
                sport_id=sport.id,
                team_eesl_id=1001,
            )
        )
        team2 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Manchester City",
                sport_id=sport.id,
                team_eesl_id=1002,
            )
        )
        team3 = await team_service.create_or_update_team(
            TeamSchemaCreateType(
                title="Liverpool",
                sport_id=sport.id,
                team_eesl_id=1003,
            )
        )

        from src.core.models.team_tournament import TeamTournamentDB

        async with test_db.get_session_maker()() as session:
            tt1 = TeamTournamentDB(tournament_id=tournament.id, team_id=team1.id)
            tt2 = TeamTournamentDB(tournament_id=tournament.id, team_id=team2.id)
            tt3 = TeamTournamentDB(tournament_id=tournament.id, team_id=team3.id)
            session.add_all([tt1, tt2, tt3])
            await session.flush()

        response = await client.get(
            f"/api/tournaments/id/{tournament.id}/teams/paginated?search=Manchester"
        )

        assert response.status_code == 200
        response_data = response.json()
        assert "data" in response_data
        assert "metadata" in response_data
        assert len(response_data["data"]) == 2
        assert response_data["metadata"]["total_items"] == 2
        team_titles = [team["title"] for team in response_data["data"]]
        assert "Manchester United" in team_titles
        assert "Manchester City" in team_titles
        assert "Liverpool" not in team_titles

    async def test_get_teams_by_tournament_paginated_with_page_2_endpoint(self, client, test_db):
        """Test GET /api/tournaments/id/{tournament_id}/teams/paginated with page 2."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        teams_db = []
        for i in range(5):
            team = await team_service.create_or_update_team(
                TeamSchemaCreateType(
                    title=f"Team {i}",
                    sport_id=sport.id,
                    team_eesl_id=2000 + i,
                )
            )
            teams_db.append(team)

        from src.core.models.team_tournament import TeamTournamentDB

        async with test_db.get_session_maker()() as session:
            tt_entries = [
                TeamTournamentDB(tournament_id=tournament.id, team_id=team.id) for team in teams_db
            ]
            session.add_all(tt_entries)
            await session.flush()

        response = await client.get(
            f"/api/tournaments/id/{tournament.id}/teams/paginated?page=2&items_per_page=2"
        )

        assert response.status_code == 200
        response_data = response.json()
        assert "data" in response_data
        assert "metadata" in response_data
        assert len(response_data["data"]) == 2
        assert response_data["metadata"]["total_items"] == 5
        assert response_data["metadata"]["page"] == 2
        assert response_data["metadata"]["has_next"] is True
        assert response_data["metadata"]["has_previous"] is True

    async def test_get_available_players_for_tournament_endpoint(self, client, test_db):
        """Test retrieving available players for a tournament (not connected)."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB
        from src.person.schemas import PersonSchemaCreate as PersonSchemaCreateType
        from src.player.schemas import PlayerSchemaCreate as PlayerSchemaCreateType

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        person_service = PersonServiceDB(test_db)
        player_service = PlayerServiceDB(test_db)

        person1 = await person_service.create(
            PersonSchemaCreateType(first_name="John", second_name="Doe")
        )
        player1 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person1.id)
        )

        person2 = await person_service.create(
            PersonSchemaCreateType(first_name="Jane", second_name="Smith")
        )
        player2 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person2.id)
        )

        person3 = await person_service.create(
            PersonSchemaCreateType(first_name="Bob", second_name="Wilson")
        )
        player3 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person3.id)
        )

        async with test_db.get_session_maker()() as session:
            ptt = PlayerTeamTournamentDB(player_id=player1.id, tournament_id=tournament.id)
            session.add(ptt)
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/available")

        assert response.status_code == 200
        players = response.json()

        assert len(players) == 2
        player_ids = [p["id"] for p in players]
        assert player2.id in player_ids
        assert player3.id in player_ids
        assert player1.id not in player_ids

    async def test_get_players_without_team_in_tournament_endpoint(self, client, test_db):
        """Test retrieving players without team in tournament."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB
        from src.core.models.team_tournament import TeamTournamentDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        person_service = PersonServiceDB(test_db)
        player_service = PlayerServiceDB(test_db)

        person1 = await person_service.create(
            PersonSchemaCreateType(first_name="Alice", second_name="TeamPlayer")
        )
        player1 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person1.id, player_eesl_id=8001)
        )

        person2 = await person_service.create(
            PersonSchemaCreateType(first_name="Bob", second_name="NoTeam")
        )
        player2 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person2.id, player_eesl_id=8002)
        )

        person3 = await person_service.create(
            PersonSchemaCreateType(first_name="Charlie", second_name="TeamPlayer2")
        )
        player3 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person3.id, player_eesl_id=8003)
        )

        team_service = TeamServiceDB(test_db)
        team_data = TeamSchemaCreateType(
            title="Test Team",
            sport_id=sport.id,
            team_eesl_id=7000,
            city="City",
            team_color="#000000",
        )
        team_db = await team_service.create_or_update_team(team_data)

        async with test_db.get_session_maker()() as session:
            ptt1 = PlayerTeamTournamentDB(
                player_id=player1.id, tournament_id=tournament.id, team_id=team_db.id
            )
            ptt2 = PlayerTeamTournamentDB(
                player_id=player2.id, tournament_id=tournament.id, team_id=None
            )
            ptt3 = PlayerTeamTournamentDB(
                player_id=player3.id, tournament_id=tournament.id, team_id=team_db.id
            )
            tt = TeamTournamentDB(tournament_id=tournament.id, team_id=team_db.id)
            session.add_all([ptt1, ptt2, ptt3, tt])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/without-team")

        assert response.status_code == 200
        response_data = response.json()

        assert response_data["metadata"]["total_items"] == 1
        assert len(response_data["data"]) == 1
        assert response_data["data"][0]["first_name"] == "Bob"
        assert response_data["data"][0]["second_name"] == "NoTeam"

    async def test_get_players_by_tournament_paginated_endpoint(self, client, test_db):
        """Test retrieving all players in tournament with pagination and search."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        person_service = PersonServiceDB(test_db)
        player_service = PlayerServiceDB(test_db)

        person1 = await person_service.create(
            PersonSchemaCreateType(first_name="Alice", second_name="Smith")
        )
        player1 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person1.id, player_eesl_id=8001)
        )

        person2 = await person_service.create(
            PersonSchemaCreateType(first_name="Bob", second_name="Johnson")
        )
        player2 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person2.id, player_eesl_id=8002)
        )

        person3 = await person_service.create(
            PersonSchemaCreateType(first_name="Charlie", second_name="Anderson")
        )
        player3 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person3.id, player_eesl_id=8003)
        )

        async with test_db.get_session_maker()() as session:
            ptt1 = PlayerTeamTournamentDB(player_id=player1.id, tournament_id=tournament.id)
            ptt2 = PlayerTeamTournamentDB(player_id=player2.id, tournament_id=tournament.id)
            ptt3 = PlayerTeamTournamentDB(player_id=player3.id, tournament_id=tournament.id)
            session.add_all([ptt1, ptt2, ptt3])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/paginated")

        assert response.status_code == 200
        response_data = response.json()

        assert response_data["metadata"]["total_items"] == 3
        assert len(response_data["data"]) == 3
        assert response_data["data"][0]["second_name"] == "Anderson"

    async def test_get_players_by_tournament_paginated_with_search_endpoint(self, client, test_db):
        """Test retrieving all players in tournament with search."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        person_service = PersonServiceDB(test_db)
        player_service = PlayerServiceDB(test_db)

        person1 = await person_service.create(
            PersonSchemaCreateType(first_name="Alice", second_name="Smith")
        )
        player1 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person1.id, player_eesl_id=8001)
        )

        person2 = await person_service.create(
            PersonSchemaCreateType(first_name="Bob", second_name="Smith")
        )
        player2 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person2.id, player_eesl_id=8002)
        )

        async with test_db.get_session_maker()() as session:
            ptt1 = PlayerTeamTournamentDB(player_id=player1.id, tournament_id=tournament.id)
            ptt2 = PlayerTeamTournamentDB(player_id=player2.id, tournament_id=tournament.id)
            session.add_all([ptt1, ptt2])
            await session.flush()

        response = await client.get(
            f"/api/tournaments/id/{tournament.id}/players/paginated?search=Smith"
        )

        assert response.status_code == 200
        response_data = response.json()

        assert response_data["metadata"]["total_items"] == 2
        assert len(response_data["data"]) == 2

    async def test_get_players_without_team_in_tournament_all_endpoint(self, client, test_db):
        """Test retrieving all players without team in tournament without pagination."""
        from src.core.models.player_team_tournament import PlayerTeamTournamentDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        person_service = PersonServiceDB(test_db)
        player_service = PlayerServiceDB(test_db)

        person1 = await person_service.create(
            PersonSchemaCreateType(first_name="Alice", second_name="TeamPlayer")
        )
        player1 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person1.id, player_eesl_id=8001)
        )

        person2 = await person_service.create(
            PersonSchemaCreateType(first_name="Bob", second_name="NoTeam")
        )
        player2 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person2.id, player_eesl_id=8002)
        )

        person3 = await person_service.create(
            PersonSchemaCreateType(first_name="Charlie", second_name="AnotherNoTeam")
        )
        player3 = await player_service.create(
            PlayerSchemaCreateType(sport_id=sport.id, person_id=person3.id, player_eesl_id=8003)
        )

        async with test_db.get_session_maker()() as session:
            ptt1 = PlayerTeamTournamentDB(
                player_id=player1.id, tournament_id=tournament.id, team_id=None
            )
            ptt2 = PlayerTeamTournamentDB(
                player_id=player2.id, tournament_id=tournament.id, team_id=None
            )
            ptt3 = PlayerTeamTournamentDB(
                player_id=player3.id, tournament_id=tournament.id, team_id=None
            )
            session.add_all([ptt1, ptt2, ptt3])
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/players/without-team/all")

        assert response.status_code == 200
        response_data = response.json()

        assert len(response_data) == 3
        assert response_data[0]["second_name"] == "AnotherNoTeam"
        assert response_data[1]["second_name"] == "NoTeam"
        assert response_data[2]["second_name"] == "TeamPlayer"

    async def test_get_available_teams_for_tournament_endpoint(self, client, test_db):
        """Test retrieving available teams for a tournament (not connected)."""
        from src.core.models.team_tournament import TeamTournamentDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)

        team1_data = TeamSchemaCreateType(
            title="Team Alpha",
            sport_id=sport.id,
            team_eesl_id=7001,
        )
        team1 = await team_service.create_or_update_team(team1_data)

        team2_data = TeamSchemaCreateType(
            title="Team Beta",
            sport_id=sport.id,
            team_eesl_id=7002,
        )
        team2 = await team_service.create_or_update_team(team2_data)

        team3_data = TeamSchemaCreateType(
            title="Team Gamma",
            sport_id=sport.id,
            team_eesl_id=7003,
        )
        team3 = await team_service.create_or_update_team(team3_data)

        async with test_db.get_session_maker()() as session:
            tt = TeamTournamentDB(tournament_id=tournament.id, team_id=team1.id)
            session.add(tt)
            await session.flush()

        response = await client.get(f"/api/tournaments/id/{tournament.id}/teams/available")

        assert response.status_code == 200
        teams = response.json()

        assert len(teams) == 2
        team_ids = [t["id"] for t in teams]
        assert team2.id in team_ids
        assert team3.id in team_ids
        assert team1.id not in team_ids
        assert teams[0]["title"] < teams[1]["title"]

    async def test_move_tournament_to_sport_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        source_sport = await sport_service.create(SportFactorySample.build())
        target_sport = await sport_service.create(SportFactoryAny.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=source_sport.id, season_id=season.id)
        )

        headers = await create_admin_headers(test_db)
        response = await client.post(
            f"/api/tournaments/id/{tournament.id}/move-sport",
            json={"target_sport_id": target_sport.id},
            headers=headers,
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["moved"] is True
        assert response_data["updated_counts"]["tournament"] == 1

    @pytest.mark.asyncio
    @patch("src.pars_eesl.pars_tournament.get_url")
    @patch("src.pars_eesl.pars_tournament.file_service")
    async def test_get_parsed_tournament_basic_data_endpoint(
        self, mock_file_service, mock_get_url, client, test_db
    ):
        """Test GET endpoint for parsing tournament basic data without DB write."""
        from unittest.mock import AsyncMock

        mock_response = Mock()
        mock_response.content = """
        <html>
            <head>
                <meta property="og:image" content="https://example.com/tournaments/champ2024.png" />
            </head>
            <body>
                <section class="tournament">
                    <div class="tournament-wrapper">
                        <h2 class="tournament__title">Tournament Championship 2024</h2>
                    </div>
                </section>
            </body>
        </html>
        """
        mock_get_url.return_value = mock_response

        mock_file_service.download_and_process_image = AsyncMock(
            return_value={
                "image_url": "https://example.com/tournaments/champ2024.png",
                "image_icon_url": "https://example.com/tournaments/champ2024_icon.png",
                "image_webview_url": "https://example.com/tournaments/champ2024_web.png",
                "image_path": "/path/to/image",
            }
        )

        response = await client.get("/api/tournaments/pars/tournament/123")

        assert response.status_code == 200
        data = response.json()
        assert data["tournament_eesl_id"] == 123
        assert data["title"] == "tournament championship 2024"
        assert data["tournament_logo_url"] == "https://example.com/tournaments/champ2024.png"

    @pytest.mark.asyncio
    @patch("src.pars_eesl.pars_tournament.get_url")
    @patch("src.pars_eesl.pars_tournament.file_service")
    async def test_create_parsed_single_tournament_endpoint(
        self, mock_file_service, mock_get_url, client, test_db
    ):
        """Test POST endpoint for parsing and creating tournament with DB write."""
        from unittest.mock import AsyncMock

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        mock_response = Mock()
        mock_response.content = """
        <html>
            <head>
                <meta property="og:image" content="https://example.com/tournaments/champ2024.png" />
            </head>
            <body>
                <section class="tournament">
                    <div class="tournament-wrapper">
                        <h2 class="tournament__title">Tournament Championship 2024</h2>
                    </div>
                </section>
            </body>
        </html>
        """
        mock_get_url.return_value = mock_response

        mock_file_service.download_and_process_image = AsyncMock(
            return_value={
                "image_url": "https://example.com/tournaments/champ2024.png",
                "image_icon_url": "https://example.com/tournaments/champ2024_icon.png",
                "image_webview_url": "https://example.com/tournaments/champ2024_web.png",
                "image_path": "/path/to/image",
            }
        )

        response = await client.post(
            f"/api/tournaments/pars_and_create/tournament/123?season_id={season.id}&sport_id={sport.id}"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["tournament_eesl_id"] == 123
        assert data["title"] == "tournament championship 2024"
        assert data["season_id"] == season.id
        assert data["sport_id"] == sport.id
