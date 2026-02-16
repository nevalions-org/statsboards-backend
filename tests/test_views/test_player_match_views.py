import pytest

from src.matches.db_services import MatchServiceDB
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate
from src.player.db_services import PlayerServiceDB
from src.player_match.db_services import PlayerMatchServiceDB
from src.player_match.schemas import PlayerMatchSchemaCreate, PlayerMatchSchemaUpdate
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.player_team_tournament.schemas import PlayerTeamTournamentSchemaCreate
from src.positions.db_services import PositionServiceDB
from src.positions.schemas import PositionSchemaCreate
from src.seasons.db_services import SeasonServiceDB
from src.sports.db_services import SportServiceDB
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB
from tests.factories import (
    MatchFactory,
    PlayerFactory,
    SeasonFactorySample,
    SportFactorySample,
    TeamFactory,
    TournamentFactory,
)


@pytest.mark.asyncio
class TestPlayerMatchViews:
    async def test_create_player_match_endpoint(self, client_player, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )

        response = await client_player.post(
            "/api/players_match/", json=player_match_data.model_dump()
        )

        assert response.status_code == 200
        assert response.json()["id"] > 0

    async def test_get_player_match_by_eesl_id_endpoint(self, client_player, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )
        created = await player_match_service.create_or_update_player_match(player_match_data)

        response = await client_player.get("/api/players_match/eesl_id/100")

        assert response.status_code == 200
        assert response.json()["id"] == created.id

    async def test_get_player_match_by_eesl_id_not_found(self, client_player):
        response = await client_player.get("/api/players_match/eesl_id/99999")

        assert response.status_code == 404

    async def test_update_player_match_endpoint(self, client_player, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )
        created = await player_match_service.create_or_update_player_match(player_match_data)

        update_data = PlayerMatchSchemaUpdate(match_number="99")

        response = await client_player.put(
            f"/api/players_match/{created.id}/", json=update_data.model_dump()
        )

        assert response.status_code == 200

    async def test_get_all_player_matches_endpoint(self, client_player, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        await player_match_service.create_or_update_player_match(
            PlayerMatchSchemaCreate(
                player_match_eesl_id=100,
                player_team_tournament_id=ptt.id,
                match_position_id=position.id,
                match_id=match.id,
                team_id=team.id,
            )
        )
        await player_match_service.create_or_update_player_match(
            PlayerMatchSchemaCreate(
                player_match_eesl_id=101,
                player_team_tournament_id=ptt.id,
                match_position_id=position.id,
                match_id=match.id,
                team_id=team.id,
            )
        )

        response = await client_player.get("/api/players_match/")

        assert response.status_code == 200
        assert len(response.json()) >= 2

    async def test_get_player_match_by_id_endpoint(self, client_player, test_db):
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )
        created = await player_match_service.create_or_update_player_match(player_match_data)

        response = await client_player.get(f"/api/players_match/id/{created.id}")

        assert response.status_code == 200

    async def test_get_player_match_by_id_not_found(self, client_player):
        response = await client_player.get("/api/players_match/id/99999")

        assert response.status_code == 404

    @pytest.mark.slow
    async def test_create_parsed_eesl_match_with_timeout_skip(
        self, client_player, test_db, monkeypatch
    ):
        """Test that match parsing skips players when collect_player_full_data_eesl times out."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id, team_eesl_id=100))

        match_service = MatchServiceDB(test_db)
        await match_service.create(
            MatchFactory.build(
                tournament_id=tournament.id,
                team_a_id=team.id,
                team_b_id=team.id,
                match_eesl_id=123,
            )
        )

        position_service = PositionServiceDB(test_db)
        await position_service.create(PositionSchemaCreate(title="QB", sport_id=sport.id))

        async def mock_timeout_return_none(*args, **kwargs):
            return None

        monkeypatch.setattr(
            "src.player_match.parser.collect_player_full_data_eesl",
            mock_timeout_return_none,
        )

        response = await client_player.get("/api/players_match/pars_and_create/match/123")

        assert response.status_code == 200

    async def test_get_player_match_by_eesl_id_exception(self, client_player, test_db):
        """Test get by eesl_id handles non-HTTPException errors."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        created_match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=created_match.id,
            team_id=team.id,
        )
        await player_match_service.create_or_update_player_match(player_match_data)

        async def mock_get_raises_exception(*args, **kwargs):
            raise ValueError("Database error")

        import unittest.mock

        with unittest.mock.patch.object(
            PlayerMatchServiceDB,
            "get_player_match_by_eesl_id",
            mock_get_raises_exception,
        ):
            response = await client_player.get("/api/players_match/eesl_id/100")

        assert response.status_code == 500

    async def test_get_parsed_eesl_match_endpoint(self, client_player, test_db, monkeypatch):
        """Test get parsed eesl match endpoint."""
        from src.pars_eesl.pars_match import ParsedMatch

        mock_parsed_match = ParsedMatch(
            team_a="Team A",
            team_b="Team B",
            team_a_eesl_id=100,
            team_b_eesl_id=200,
            team_logo_url_a=None,
            team_logo_url_b=None,
            score_a="1",
            score_b="0",
            roster_a=[],
            roster_b=[],
        )

        async def mock_parse(*args, **kwargs):
            return mock_parsed_match

        monkeypatch.setattr(
            "src.player_match.parser.parse_match_and_create_jsons",
            mock_parse,
        )

        response = await client_player.get("/api/players_match/pars/match/123")

        assert response.status_code == 200

    async def test_create_parsed_eesl_match_endpoint_success(
        self, client_player, test_db, monkeypatch
    ):
        """Test create parsed eesl match endpoint with successful parsing."""
        from src.pars_eesl.pars_match import ParsedMatch, ParsedMatchPlayer

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team_a = await team_service.create(TeamFactory.build(sport_id=sport.id, team_eesl_id=100))
        team_b = await team_service.create(TeamFactory.build(sport_id=sport.id, team_eesl_id=200))

        match_service = MatchServiceDB(test_db)
        await match_service.create(
            MatchFactory.build(
                tournament_id=tournament.id,
                team_a_id=team_a.id,
                team_b_id=team_b.id,
                match_eesl_id=123,
            )
        )

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        person_service = PersonServiceDB(test_db)
        person = await person_service.create(
            PersonSchemaCreate(
                first_name="John",
                second_name="Doe",
                person_eesl_id=1000,
            )
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(
            PlayerFactory.build(person_id=person.id, player_eesl_id=1000)
        )

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team_a.id,
                tournament_id=tournament.id,
                player_team_tournament_eesl_id=1000,
                player_number="10",
            )
        )

        mock_parsed_match = ParsedMatch(
            team_a="Team A",
            team_b="Team B",
            team_a_eesl_id=100,
            team_b_eesl_id=200,
            team_logo_url_a=None,
            team_logo_url_b=None,
            score_a="1",
            score_b="0",
            roster_a=[
                ParsedMatchPlayer(
                    player_number="10",
                    player_position="QB",
                    player_full_name="John Doe",
                    player_first_name="John",
                    player_second_name="Doe",
                    player_eesl_id=1000,
                    player_img_url=None,
                    player_team="Team A",
                    player_team_logo_url="",
                )
            ],
            roster_b=[],
        )

        async def mock_parse(*args, **kwargs):
            return mock_parsed_match

        async def mock_collect_player(*args, **kwargs):
            return None

        monkeypatch.setattr(
            "src.player_match.parser.parse_match_and_create_jsons",
            mock_parse,
        )
        monkeypatch.setattr(
            "src.player_match.parser.collect_player_full_data_eesl",
            mock_collect_player,
        )

        response = await client_player.get("/api/players_match/pars_and_create/match/123")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_create_parsed_eesl_match_endpoint_no_match(
        self, client_player, test_db, monkeypatch
    ):
        """Test create parsed eesl match endpoint when match not found."""
        from src.pars_eesl.pars_match import ParsedMatch

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        await team_service.create(TeamFactory.build(sport_id=sport.id, team_eesl_id=100))

        mock_parsed_match = ParsedMatch(
            team_a="Team A",
            team_b="Team B",
            team_a_eesl_id=100,
            team_b_eesl_id=200,
            team_logo_url_a=None,
            team_logo_url_b=None,
            score_a="1",
            score_b="0",
            roster_a=[],
            roster_b=[],
        )

        async def mock_parse(*args, **kwargs):
            return mock_parsed_match

        monkeypatch.setattr(
            "src.player_match.parser.parse_match_and_create_jsons",
            mock_parse,
        )

        response = await client_player.get("/api/players_match/pars_and_create/match/999")

        assert response.status_code == 200
        assert response.json() == []

    async def test_create_parsed_eesl_match_endpoint_error(
        self, client_player, test_db, monkeypatch
    ):
        """Test create parsed eesl match endpoint error handling."""

        async def mock_parse_raises(*args, **kwargs):
            raise Exception("Parse error")

        monkeypatch.setattr(
            "src.player_match.parser.parse_match_and_create_jsons",
            mock_parse_raises,
        )

        response = await client_player.get("/api/players_match/pars_and_create/match/123")

        assert response.status_code == 200

    async def test_get_player_in_sport_endpoint(self, client_player, test_db):
        """Test get player in sport endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )
        created = await player_match_service.create_or_update_player_match(player_match_data)

        response = await client_player.get(f"/api/players_match/id/{created.id}/player_in_sport/")

        assert response.status_code == 200

    async def test_get_player_in_team_tournament_endpoint(self, client_player, test_db):
        """Test get player in team tournament endpoint."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build())

        season_service = SeasonServiceDB(test_db)
        season = await season_service.create(SeasonFactorySample.build())

        tournament_service = TournamentServiceDB(test_db)
        tournament = await tournament_service.create(
            TournamentFactory.build(sport_id=sport.id, season_id=season.id)
        )

        team_service = TeamServiceDB(test_db)
        team = await team_service.create(TeamFactory.build(sport_id=sport.id))

        position_service = PositionServiceDB(test_db)
        position = await position_service.create(
            PositionSchemaCreate(title="QB", sport_id=sport.id)
        )

        player_service = PlayerServiceDB(test_db)
        player = await player_service.create_or_update_player(PlayerFactory.build())

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = await ptt_service.create(
            PlayerTeamTournamentSchemaCreate(
                player_id=player.id,
                position_id=position.id,
                team_id=team.id,
                tournament_id=tournament.id,
            )
        )

        match_service = MatchServiceDB(test_db)
        match = await match_service.create(
            MatchFactory.build(tournament_id=tournament.id, team_a_id=team.id, team_b_id=team.id)
        )

        player_match_service = PlayerMatchServiceDB(test_db)
        player_match_data = PlayerMatchSchemaCreate(
            player_match_eesl_id=100,
            player_team_tournament_id=ptt.id,
            match_position_id=position.id,
            match_id=match.id,
            team_id=team.id,
        )
        created = await player_match_service.create_or_update_player_match(player_match_data)

        response = await client_player.get(
            f"/api/players_match/id/{created.id}/player_in_team_tournament/"
        )

        assert response.status_code == 200

    async def test_delete_player_match_endpoint_unauthorized(self, client_player, test_db):
        """Test delete player match endpoint without authentication."""
        response = await client_player.delete("/api/players_match/id/1")

        assert response.status_code == 401
