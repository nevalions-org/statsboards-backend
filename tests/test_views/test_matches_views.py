from datetime import datetime

import pytest

from src.football_events.db_services import FootballEventServiceDB
from src.football_events.schemas import FootballEventSchemaCreate
from src.matches.db_services import MatchServiceDB
from src.matches.schemas import MatchSchemaCreate, MatchSchemaUpdate
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaCreate
from src.player_match.db_services import PlayerMatchServiceDB
from src.player_match.schemas import PlayerMatchSchemaCreate
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.player_team_tournament.schemas import PlayerTeamTournamentSchemaCreate
from src.seasons.db_services import SeasonServiceDB
from src.sports.db_services import SportServiceDB
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB
from tests.factories import (
    SeasonFactorySample,
    SportFactorySample,
    TeamFactory,
    TournamentFactory,
)


@pytest.mark.asyncio
class TestMatchViews:
    async def test_create_match_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

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

    async def test_update_match_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

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

    async def test_update_match_not_found(self, client):
        update_data = MatchSchemaUpdate(week=2)

        response = await client.put("/api/matches/99999/", json=update_data.model_dump())

        assert response.status_code == 404

    async def test_get_all_matches_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

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
                match_date="2025-01-01",
                week=2,
            )
        )

        response = await client.get("/api/matches/")

        assert response.status_code == 200
        assert len(response.json()) == 2

    async def test_get_match_by_id_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

        match_service = MatchServiceDB(test_db)
        match_data = MatchSchemaCreate(
            tournament_id=tournament.id,
            team_a_id=team_a.id,
            team_b_id=team_b.id,
            match_date="2025-01-01",
            week=1,
        )
        created = await match_service.create_or_update_match(match_data)

        response = await client.get(f"/api/matches/id/{created.id}")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_match_by_id_not_found(self, client):
        response = await client.get("/api/matches/id/99999")

        assert response.status_code == 404

    async def test_get_match_stats_endpoint(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )

        person_service = PersonServiceDB(test_db)
        person_a = await person_service.create(
            PersonSchemaCreate(
                first_name="John",
                second_name="Doe",
                person_dob=datetime(2000, 1, 1),
            )
        )
        person_b = await person_service.create(
            PersonSchemaCreate(
                first_name="Jane",
                second_name="Smith",
                person_dob=datetime(2000, 1, 1),
            )
        )

        player_service = PlayerServiceDB(test_db)
        player_a = await player_service.create(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person_a.id)
        )
        player_b = await player_service.create(
            PlayerSchemaCreate(sport_id=sport.id, person_id=person_b.id)
        )

        player_team_tournament_service = PlayerTeamTournamentServiceDB(test_db)
        player_tournament_a = await player_team_tournament_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player_a.id,
                team_id=team_a.id,
                tournament_id=tournament.id,
                player_number="10",
            )
        )
        player_tournament_b = await player_team_tournament_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player_b.id,
                team_id=team_b.id,
                tournament_id=tournament.id,
                player_number="20",
            )
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_a = await player_match_service.create_or_update_player_match(
            PlayerMatchSchemaCreate(
                player_match_eesl_id=100,
                player_team_tournament_id=player_tournament_a.id,
                match_id=match.id,
                team_id=team_a.id,
                match_number="10",
                is_start=True,
            )
        )
        player_match_b = await player_match_service.create_or_update_player_match(
            PlayerMatchSchemaCreate(
                player_match_eesl_id=101,
                player_team_tournament_id=player_tournament_b.id,
                match_id=match.id,
                team_id=team_b.id,
                match_number="20",
                is_start=True,
            )
        )

        football_event_service = FootballEventServiceDB(test_db)
        await football_event_service.create(
            FootballEventSchemaCreate(
                match_id=match.id,
                event_number=1,
                event_qtr=1,
                event_down=1,
                event_distance=10,
                offense_team=team_a.id,
                play_type="run",
                play_result="run",
                ball_on=20,
                ball_moved_to=30,
                run_player=player_match_a.id,
                event_qb=player_match_a.id,
            )
        )
        await football_event_service.create(
            FootballEventSchemaCreate(
                match_id=match.id,
                event_number=2,
                event_qtr=1,
                event_down=1,
                event_distance=15,
                offense_team=team_b.id,
                play_type="pass",
                play_result="completed",
                ball_on=20,
                ball_moved_to=35,
                pass_received_player=player_match_b.id,
                event_qb=player_match_b.id,
            )
        )

        response = await client.get(f"/api/matches/id/{match.id}/stats/")

        assert response.status_code == 200
        stats = response.json()
        assert "match_id" in stats
        assert stats["match_id"] == match.id
        assert "team_a" in stats
        assert "team_b" in stats
        assert "team_stats" in stats["team_a"]
        assert "offense_stats" in stats["team_a"]
        assert "qb_stats" in stats["team_a"]
        assert "defense_stats" in stats["team_a"]
        assert stats["team_a"]["team_stats"]["offence_yards"] == 10

    async def test_get_match_stats_no_events(self, client, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )

        response = await client.get(f"/api/matches/id/{match.id}/stats/")

        assert response.status_code == 200
        stats = response.json()
        assert "match_id" in stats
        assert stats["match_id"] == match.id
        assert "team_a" in stats
        assert "team_b" in stats
        assert stats["team_a"]["team_stats"]["offence_yards"] == 0
        assert stats["team_b"]["team_stats"]["offence_yards"] == 0

    async def test_get_match_stats_not_found(self, client):
        response = await client.get("/api/matches/id/99999/stats/")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_get_match_full_context_endpoint(self, client, test_db):
        """Test getting match full context endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )

        response = await client.get(f"/api/matches/id/{match.id}/full-context/")

        assert response.status_code == 200
        context = response.json()
        assert "match" in context
        assert "teams" in context
        assert "sport" in context
        assert "tournament" in context
        assert "players" in context
        assert context["match"]["id"] == match.id
        assert context["teams"]["home"]["id"] == team_a.id
        assert context["teams"]["away"]["id"] == team_b.id
        assert context["tournament"]["id"] == tournament.id

    async def test_get_match_full_context_not_found(self, client):
        """Test getting match full context for non-existent match."""
        response = await client.get("/api/matches/id/99999/full-context/")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_get_matches_paginated_endpoint(self, client, test_db):
        """Test getting matches with pagination and week filter."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

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
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-15",
                week=3,
            )
        )

        response = await client.get("/api/matches/paginated?week=2")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["week"] == 2
        assert data["metadata"]["total_items"] == 1

    async def test_get_matches_paginated_with_tournament_filter(self, client, test_db):
        """Test getting matches with tournament_id filter."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament_a = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )
        tournament_b = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id))

        match_service = MatchServiceDB(test_db)
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament_a.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-01",
                week=1,
            )
        )
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament_a.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-08",
                week=2,
            )
        )
        await match_service.create_or_update_match(
            MatchSchemaCreate(
                tournament_id=tournament_b.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_date="2025-01-15",
                week=1,
            )
        )

        response = await client.get(f"/api/matches/paginated?tournament_id={tournament_a.id}")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 2
        assert all(m["tournament_id"] == tournament_a.id for m in data["data"])
        assert data["metadata"]["total_items"] == 2

    async def test_get_parse_match_endpoint_not_found(self, client):
        from unittest.mock import AsyncMock, patch

        with patch("src.pars_eesl.pars_match.get_url", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            response = await client.get("/api/matches/pars/match/99999")

            assert response.status_code == 200
            data = response.json()
            assert data["team_a"] == ""
            assert data["team_b"] == ""
            assert data["score_a"] == ""
            assert data["score_b"] == ""
            assert data["roster_a"] == []
            assert data["roster_b"] == []

    async def test_create_parsed_single_match_endpoint_teams_not_found(self, client, test_db):
        from unittest.mock import AsyncMock, patch

        from src.sports.db_services import SportServiceDB
        from src.teams.db_services import TeamServiceDB

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        team_service = TeamServiceDB(test_db)
        await team_service.create_or_update_team(
            TeamFactory.build(sport_id=sport.id, team_eesl_id=100)
        )
        await team_service.create_or_update_team(
            TeamFactory.build(sport_id=sport.id, team_eesl_id=200)
        )

        {
            "team_a": "Team A",
            "team_b": "Team B",
            "team_a_eesl_id": 300,
            "team_b_eesl_id": 400,
            "team_logo_url_a": "http://example.com/a.png",
            "team_logo_url_b": "http://example.com/b.png",
            "score_a": "1",
            "score_b": "0",
            "roster_a": [],
            "roster_b": [],
        }

        with patch("src.pars_eesl.pars_match.get_url", new_callable=AsyncMock) as mock_get:
            with patch("src.pars_eesl.pars_match.BeautifulSoup") as mock_soup:
                mock_response = type("obj", (object,), {"content": b""})()
                mock_get.return_value = mock_response

                from bs4 import BeautifulSoup

                mock_soup.return_value = BeautifulSoup(
                    """
                    <div class="match-promo__score-main">1:0</div>
                    <a class="match-protocol__team-name match-protocol__team-name--left" href="/team/?team_id=300">Team A</a>
                    <a class="match-protocol__team-name match-protocol__team-name--right" href="/team/?team_id=400">Team B</a>
                    <img class="match-promo__team-img" src="http://example.com/a.png">
                    <img class="match-promo__team-img" src="http://example.com/b.png">
                    """,
                    "lxml",
                )

                response = await client.post("/api/matches/pars_and_create/match/99999")

                assert response.status_code == 200
                assert response.json() == []
