from io import BytesIO

import pytest
from PIL import Image

from src.sports.db_services import SportServiceDB
from src.teams.db_services import TeamServiceDB
from tests.factories import SeasonFactorySample, SportFactorySample, TeamFactory, TournamentFactory


def create_test_image():
    img = Image.new("RGB", (100, 100), color="red")
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


@pytest.mark.asyncio
class TestTeamViews:
    async def test_create_team_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_data = TeamFactory.build(sport_id=sport.id)

        response = await client.post("/api/teams/", json=team_data.model_dump())

        assert response.status_code == 200
        assert response.json()["id"] > 0

    async def test_get_team_by_eesl_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id, team_eesl_id=100)
        created = await team_service.create_or_update_team(team_data)

        response = await client.get("/api/teams/eesl_id/100")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_team_by_eesl_id_not_found(self, client):
        response = await client.get("/api/teams/eesl_id/99999")

        assert response.status_code == 404

    async def test_update_team_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id)
        created = await team_service.create_or_update_team(team_data)

        update_data = {"title": "Updated Title"}

        response = await client.put(f"/api/teams/{created.id}/", json=update_data)

        assert response.status_code == 200
        assert response.json()["title"] == "Updated Title"

    async def test_get_all_teams_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        await team_service.create(TeamFactory.build(sport_id=sport.id))
        await team_service.create(TeamFactory.build(sport_id=sport.id))

        response = await client.get("/api/teams/")

        assert response.status_code == 200
        assert len(response.json()) == 2

    async def test_get_team_by_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id)
        created = await team_service.create_or_update_team(team_data)

        response = await client.get(f"/api/teams/id/{created.id}")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_team_by_id_not_found(self, client):
        response = await client.get("/api/teams/id/99999")

        assert response.status_code == 404

    async def test_upload_team_logo_endpoint(self, client):
        file_content = create_test_image()
        files = {"file": ("test_logo.png", BytesIO(file_content), "image/png")}
        response = await client.post("/api/teams/upload_logo", files=files)

        assert response.status_code == 200
        assert "logoUrl" in response.json()
        assert "teams/logos" in response.json()["logoUrl"]

    async def test_upload_team_logo_with_invalid_file(self, client):
        file_content = b"not a valid image"
        files = {"file": ("test_invalid.txt", BytesIO(file_content), "text/plain")}
        response = await client.post("/api/teams/upload_logo", files=files)

        assert response.status_code == 400

    async def test_upload_and_resize_team_logo_endpoint(self, client):
        file_content = create_test_image()
        files = {"file": ("test_logo.png", BytesIO(file_content), "image/png")}
        response = await client.post("/api/teams/upload_resize_logo", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert "original" in response_data
        assert "icon" in response_data
        assert "webview" in response_data
        assert "teams/logos" in response_data["original"]
        assert "teams/logos" in response_data["icon"]
        assert "teams/logos" in response_data["webview"]

    async def test_upload_and_resize_team_logo_with_invalid_file(self, client):
        file_content = b"not a valid image"
        files = {"file": ("test_invalid.txt", BytesIO(file_content), "text/plain")}
        response = await client.post("/api/teams/upload_resize_logo", files=files)

        assert response.status_code == 400

    async def test_get_team_with_details_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id)
        created = await team_service.create_or_update_team(team_data)

        response = await client.get(f"/api/teams/id/{created.id}/with-details/")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == created.id
        assert "sport" in data

    async def test_get_team_with_details_not_found(self, client):
        response = await client.get("/api/teams/id/99999/with-details/")

        assert response.status_code == 404

    async def test_get_matches_by_team_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id)
        created = await team_service.create_or_update_team(team_data)

        response = await client.get(f"/api/teams/id/{created.id}/matches/")

        assert response.status_code == 200

    async def test_get_teams_paginated_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        await team_service.create(TeamFactory.build(sport_id=sport.id, title="Team A"))
        await team_service.create(TeamFactory.build(sport_id=sport.id, title="Team B"))
        await team_service.create(TeamFactory.build(sport_id=sport.id, title="Team C"))

        response = await client.get("/api/teams/paginated?page=1&items_per_page=2")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 2
        assert data["metadata"]["total_items"] >= 3

    async def test_get_teams_with_details_paginated_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        await team_service.create(TeamFactory.build(sport_id=sport.id, title="Team A"))

        response = await client.get("/api/teams/with-details/paginated")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) >= 1
        assert data["data"][0]["sport"]["id"] == sport.id

    async def test_create_team_with_tournament_endpoint(self, client, test_db):
        from src.seasons.db_services import SeasonServiceDB
        from src.tournaments.db_services import TournamentServiceDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament_data = TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        tournament = await tournament_service.create(tournament_data)

        TeamServiceDB(test_db)
        team_data = TeamFactory.build(sport_id=sport.id, title="New Team")

        response = await client.post(
            f"/api/teams/?tour_id={tournament.id}", json=team_data.model_dump()
        )

        assert response.status_code == 200
        assert response.json()["title"] == "New Team"

    async def test_get_parsed_team_endpoint_not_found(self, client):
        from unittest.mock import AsyncMock, patch

        with patch("src.pars_eesl.pars_tournament.get_url", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            response = await client.get("/api/teams/pars/team/99999")

            assert response.status_code == 200
            assert response.json() is None

    async def test_create_parsed_single_team_endpoint_not_found(self, client):
        from unittest.mock import AsyncMock, patch

        with patch("src.pars_eesl.pars_tournament.get_url", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            response = await client.post("/api/teams/pars_and_create/team/99999")

            assert response.status_code == 404
            assert "not found or failed to parse" in response.json()["detail"]
