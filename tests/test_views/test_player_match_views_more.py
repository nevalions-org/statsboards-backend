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
class TestPlayerMatchViewsMore:
    async def test_create_player_match_endpoint_returns_none(self, client, test_db):
        """Test create player match when service returns None - returns 409."""
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

        from unittest.mock import patch

        async def mock_create_none(*args, **kwargs):
            return None

        with patch.object(
            PlayerMatchServiceDB,
            "create_or_update_player_match",
            side_effect=mock_create_none,
        ):
            response = await client.post("/api/players_match/", json=player_match_data.model_dump())

        assert response.status_code == 409

    async def test_get_player_match_by_eesl_id_not_found_exception(self, client, test_db):
        """Test get by eesl_id when service raises exception."""
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
        await player_match_service.create_or_update_player_match(player_match_data)

        from unittest.mock import patch

        async def mock_get_exception(*args, **kwargs):
            raise Exception("Database error")

        with patch.object(
            PlayerMatchServiceDB,
            "get_player_match_by_eesl_id",
            side_effect=mock_get_exception,
        ):
            response = await client.get("/api/players_match/eesl_id/100")

        assert response.status_code == 500

    async def test_update_player_match_endpoint_not_found_exception(self, client, test_db):
        """Test update player match when service raises exception."""
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

        from unittest.mock import patch

        async def mock_update_exception(*args, **kwargs):
            raise Exception("Database error")

        with patch.object(
            PlayerMatchServiceDB,
            "update",
            side_effect=mock_update_exception,
        ):
            response = await client.put(
                f"/api/players_match/{created.id}/", json=update_data.model_dump()
            )

        assert response.status_code == 500

    async def test_get_parsed_eesl_match_endpoint_none(self, client, monkeypatch):
        """Test get parsed eesl match when parsing returns None - returns 200 empty list."""

        async def mock_parse_none(*args, **kwargs):
            return None

        monkeypatch.setattr(
            "src.player_match.parser.parse_match_and_create_jsons",
            mock_parse_none,
        )

        response = await client.get("/api/players_match/pars/match/123")

        assert response.status_code == 200

    async def test_delete_player_match_endpoint_not_found(self, client):
        """Test delete player match without auth returns 401."""
        response = await client.delete("/api/players_match/id/99999")

        assert response.status_code == 401

    async def test_create_parsed_eesl_match_endpoint_duplicate_player(
        self, client, test_db, monkeypatch
    ):
        """Test create parsed eesl match with duplicate players."""
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

        response = await client.get("/api/players_match/pars_and_create/match/123")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_create_parsed_eesl_match_endpoint_missing_position(
        self, client, test_db, monkeypatch
    ):
        """Test create parsed eesl match with missing position skips player."""
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
                    player_position="",
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

        response = await client.get("/api/players_match/pars_and_create/match/123")

        assert response.status_code == 200
        assert isinstance(response.json(), list)
