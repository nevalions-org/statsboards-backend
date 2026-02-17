import pytest

from src.person.db_services import PersonServiceDB
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaCreate
from src.sports.db_services import SportServiceDB
from src.users.db_services import UserServiceDB
from src.users.schemas import UserSchemaCreate
from tests.factories import PersonFactory, SportFactorySample


@pytest.mark.asyncio
class TestPlayerViewsAdditional:
    async def test_get_player_career_endpoint(self, client, test_db):
        """Test get player career endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person = await person_service.create_or_update_person(PersonFactory.build())

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person.id, player_eesl_id=100)
        )

        response = await client.get(f"/api/players/id/{player.id}/career")

        assert response.status_code == 200
        data = response.json()
        assert "career_by_team" in data
        assert "career_by_tournament" in data

    async def test_get_player_career_not_found(self, client):
        """Test get player career when player not found."""
        response = await client.get("/api/players/id/99999/career")

        assert response.status_code == 404

    @pytest.mark.slow
    @pytest.mark.timeout(120)
    async def test_get_parse_player_with_person_endpoint(self, client):
        """Test get parse player with person endpoint."""
        response = await client.get("/api/players/pars/all_eesl")

        assert response.status_code == 200

    async def test_create_parsed_players_with_person_endpoint_success(
        self, client, test_db, monkeypatch
    ):
        """Test create parsed players with person endpoint."""
        sport_service = SportServiceDB(test_db)
        _ = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        _ = await person_service.create_or_update_person(PersonFactory.build(person_eesl_id=1000))

        async def mock_parse(*args, **kwargs):
            return []

        monkeypatch.setattr(
            "src.player.views.parse_all_players_from_eesl_index_page_eesl",
            mock_parse,
        )

        response = await client.post(
            "/api/players/pars_and_create/all_eesl/start_page/0/season_id/8/"
        )

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_get_players_paginated_details_endpoint(self, client, test_db):
        """Test get players paginated with details endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(
            PersonFactory.build(first_name="John")
        )
        person2 = await person_service.create_or_update_person(
            PersonFactory.build(first_name="Jane")
        )

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person1.id)
        )
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person2.id)
        )

        response = await client.get(f"/api/players/paginated/details?sport_id={sport.id}")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data

    async def test_get_players_paginated_details_with_search(self, client, test_db):
        """Test get players paginated with details and search."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(
            PersonFactory.build(first_name="John", second_name="Doe")
        )
        person2 = await person_service.create_or_update_person(
            PersonFactory.build(first_name="Jane", second_name="Smith")
        )

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person1.id)
        )
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person2.id)
        )

        response = await client.get(
            f"/api/players/paginated/details?sport_id={sport.id}&search=John"
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        for player in data["data"]:
            person_name = (
                player.get("first_name", "") + " " + player.get("second_name", "")
            ).lower()
            assert "john" in person_name

    async def test_get_players_paginated_details_with_team_filter(self, client, test_db):
        """Test get players paginated with details and team filter."""
        from src.teams.db_services import TeamServiceDB
        from tests.factories import TeamFactory

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        team1 = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team2 = await team_service.create(TeamFactory.build(sport_id=sport.id))

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(PersonFactory.build())
        person2 = await person_service.create_or_update_person(PersonFactory.build())

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person1.id, team_id=team1.id)
        )
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person2.id, team_id=team2.id)
        )

        response = await client.get(
            f"/api/players/paginated/details?sport_id={sport.id}&team_id={team1.id}"
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    async def test_get_players_paginated_details_with_photos_endpoint(self, client, test_db):
        """Test get players paginated with details and photos endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person = await person_service.create_or_update_person(PersonFactory.build())

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person.id)
        )

        response = await client.get(
            f"/api/players/paginated/details-with-photos?sport_id={sport.id}"
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data

    async def test_get_players_paginated_full_details_endpoint(self, client, test_db):
        """Test get players paginated with full details endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person = await person_service.create_or_update_person(PersonFactory.build())

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person.id)
        )

        response = await client.get(f"/api/players/paginated/full-details?sport_id={sport.id}")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data

    async def test_get_players_paginated_with_sorting(self, client, test_db):
        """Test get players paginated with sorting."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(PersonFactory.build())
        person2 = await person_service.create_or_update_person(PersonFactory.build())

        player_service = PlayerServiceDB(test_db)
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person1.id)
        )
        await player_service.create_or_update_player(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person2.id)
        )

        response = await client.get(
            f"/api/players/paginated/details?sport_id={sport.id}&ascending=false"
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    async def test_get_players_paginated_with_pagination(self, client, test_db):
        """Test get players paginated with pagination."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        for _ in range(5):
            person = await person_service.create_or_update_person(PersonFactory.build())
            await PlayerServiceDB(test_db).create_or_update_player(
                PlayerSchemaCreate(sport_id=sport.id, person_id=person.id)
            )

        response = await client.get(
            f"/api/players/paginated/details?sport_id={sport.id}&page=1&items_per_page=3"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["metadata"]["total_items"] >= 5
        assert data["metadata"]["page"] == 1
        assert data["metadata"]["items_per_page"] == 3
        assert len(data["data"]) == 3

    async def test_add_person_to_sport_endpoint_with_isprivate(self, client, test_db):
        """Test add person to sport with isprivate flag."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person = await person_service.create_or_update_person(PersonFactory.build())

        response = await client.post(
            "/api/players/add-person-to-sport",
            json={
                "person_id": person.id,
                "sport_id": sport.id,
                "isprivate": True,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["person_id"] == person.id
        assert data["sport_id"] == sport.id
        assert data["isprivate"] is True

    async def test_add_person_to_sport_endpoint_with_user_id(self, client, test_db):
        """Test add person to sport with user_id."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        person_service = PersonServiceDB(test_db)
        person = await person_service.create_or_update_person(PersonFactory.build())

        user_service = UserServiceDB(test_db)
        user = await user_service.create(
            UserSchemaCreate(
                username="user123",
                email="user123@example.com",
                password="SecurePass123!",
            )
        )

        response = await client.post(
            "/api/players/add-person-to-sport",
            json={
                "person_id": person.id,
                "sport_id": sport.id,
                "user_id": user.id,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["person_id"] == person.id
        assert data["sport_id"] == sport.id
