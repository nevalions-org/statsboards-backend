import pytest
from fastapi import HTTPException

from src.core.models.base import Database
from src.person.db_services import PersonServiceDB
from src.person.schemas import PersonSchemaCreate
from src.player.db_services import PlayerServiceDB
from src.player.schemas import PlayerSchemaUpdate
from src.player_team_tournament.db_services import PlayerTeamTournamentServiceDB
from src.seasons.db_services import SeasonServiceDB
from src.sports.db_services import SportServiceDB
from src.sports.schemas import SportSchemaCreate
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB
from tests.factories import (
    PersonFactory,
    PlayerFactory,
    PlayerTeamTournamentFactory,
    SeasonFactoryAny,
    SportFactorySample,
    TeamFactoryWithRelations,
    TournamentFactoryWithRelations,
)


@pytest.fixture(scope="function")
def sport(test_db: Database):
    return SportFactorySample.build()


@pytest.fixture(scope="function")
def person(test_db: Database):
    person_data = PersonFactory.build(person_eesl_id=999, first_name="Test")
    return person_data


@pytest.mark.asyncio
class TestPlayerServiceDBCreateOrUpdate:
    """Test create_or_update_player functionality."""

    async def test_create_player_with_eesl_id(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test creating a player with eesl_id."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        player_data = PlayerFactory.build(
            sport_id=created_sport.id,
            person_id=created_person.id,
            player_eesl_id=100,
        )

        result = await player_service.create_or_update_player(player_data)

        assert result is not None
        assert result.id is not None
        assert result.player_eesl_id == 100
        assert result.person_id == created_person.id

    async def test_create_player_without_eesl_id(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test creating a player without eesl_id."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        player_data = PlayerFactory.build(
            sport_id=created_sport.id, person_id=created_person.id, player_eesl_id=None
        )

        result = await player_service.create_or_update_player(player_data)

        assert result is not None
        assert result.id is not None
        assert result.player_eesl_id is None

    async def test_update_existing_player_by_eesl_id(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test updating an existing player by eesl_id."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)

        player_data = PlayerFactory.build(
            sport_id=created_sport.id, person_id=created_person.id, player_eesl_id=200
        )
        created = await player_service.create_or_update_player(player_data)

        update_data = PlayerSchemaUpdate(player_eesl_id=200)

        updated = await player_service.create_or_update_player(update_data)

        assert updated.id == created.id

    async def test_create_multiple_players(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test creating multiple players."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)

        player1_data = PlayerFactory.build(
            sport_id=created_sport.id, person_id=created_person.id, player_eesl_id=301
        )
        player2_data = PlayerFactory.build(
            sport_id=created_sport.id, person_id=created_person.id, player_eesl_id=302
        )

        player1 = await player_service.create_or_update_player(player1_data)
        player2 = await player_service.create_or_update_player(player2_data)

        assert player1.player_eesl_id == 301
        assert player2.player_eesl_id == 302
        assert player1.id != player2.id

    async def test_get_player_by_eesl_id(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test retrieving player by eesl_id."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)

        player_data = PlayerFactory.build(
            sport_id=created_sport.id, person_id=created_person.id, player_eesl_id=400
        )
        created = await player_service.create_or_update_player(player_data)

        retrieved = await player_service.get_player_by_eesl_id(400)

        assert retrieved is not None
        assert retrieved.id == created.id


@pytest.mark.asyncio
class TestPlayerServiceDBGetPlayerWithPerson:
    """Test get_player_with_person functionality."""

    async def test_get_player_with_person_success(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test getting player with person data successfully."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        player_data = PlayerFactory.build(
            sport_id=created_sport.id,
            person_id=created_person.id,
            player_eesl_id=500,
        )
        created = await player_service.create_or_update_player(player_data)

        result = await player_service.get_player_with_person(created.id)

        assert result is not None
        assert result.id == created_person.id
        assert result.first_name == created_person.first_name
        assert result.second_name == created_person.second_name

    async def test_get_player_with_person_not_found_raises_404(self, test_db: Database):
        """Test that get_player_with_person raises 404 for non-existent player."""
        player_service = PlayerServiceDB(test_db)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_with_person(99999)

        assert exc_info.value.status_code == 404


@pytest.mark.asyncio
class TestPlayerServiceDBAddRemovePersonToSport:
    """Test add_person_to_sport and remove_person_from_sport functionality."""

    async def test_add_person_to_sport_success(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test adding person to sport successfully."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        result = await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        assert result is not None
        assert result.person_id == created_person.id
        assert result.sport_id == created_sport.id

    async def test_add_person_to_sport_with_isprivate(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test adding person to sport with isprivate."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        result = await player_service.add_person_to_sport(
            person_id=created_person.id,
            sport_id=created_sport.id,
            isprivate=True,
        )

        assert result is not None
        assert result.isprivate is True

    async def test_add_person_to_sport_duplicate_raises_409(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test that adding person to sport twice raises 409."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        with pytest.raises(HTTPException) as exc_info:
            await player_service.add_person_to_sport(
                person_id=created_person.id, sport_id=created_sport.id
            )

        assert exc_info.value.status_code == 409

    async def test_remove_person_from_sport_success(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test removing person from sport successfully."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        result = await player_service.remove_person_from_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        assert result is True

        retrieved = await player_service.get_player_by_person_and_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )
        assert retrieved is None

    async def test_remove_person_from_sport_not_found_raises_404(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test that removing person from sport when not found raises 404."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.remove_person_from_sport(
                person_id=created_person.id, sport_id=created_sport.id
            )

        assert exc_info.value.status_code == 404


@pytest.mark.asyncio
class TestPlayerServiceDBSearchWithPaginationDetails:
    """Test search_players_with_pagination_details functionality."""

    async def test_search_players_basic_pagination(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test basic search with pagination."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        for i in range(5):
            person = await person_service.create_or_update_person(
                PersonFactory.build(person_eesl_id=1000 + i, first_name=f"Player{i}")
            )
            await player_service.add_person_to_sport(person_id=person.id, sport_id=created_sport.id)

        result = await player_service.search_players_with_pagination_details(
            sport_id=created_sport.id, skip=0, limit=3
        )

        assert len(result.data) == 3
        assert result.metadata.total_items == 5
        assert result.metadata.page == 1
        assert result.metadata.total_pages == 2

    async def test_search_players_with_search_query(
        self, test_db: Database, sport: SportSchemaCreate
    ):
        """Test search with query filter."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(
            PersonFactory.build(person_eesl_id=1001, first_name="John", second_name="Doe")
        )
        person2 = await person_service.create_or_update_person(
            PersonFactory.build(person_eesl_id=1002, first_name="Jane", second_name="Smith")
        )
        person3 = await person_service.create_or_update_person(
            PersonFactory.build(person_eesl_id=1003, first_name="Bob", second_name="Johnson")
        )

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(person_id=person1.id, sport_id=created_sport.id)
        await player_service.add_person_to_sport(person_id=person2.id, sport_id=created_sport.id)
        await player_service.add_person_to_sport(person_id=person3.id, sport_id=created_sport.id)

        result = await player_service.search_players_with_pagination_details(
            sport_id=created_sport.id, search_query="john"
        )

        assert len(result.data) >= 1
        for player in result.data:
            assert player.second_name is not None


@pytest.mark.asyncio
class TestPlayerServiceDBSearchWithPaginationFullDetails:
    """Test search_players_with_pagination_full_details functionality."""

    async def test_search_players_full_details_basic(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test search with full details."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        result = await player_service.search_players_with_pagination_full_details(
            sport_id=created_sport.id
        )

        assert len(result.data) >= 1
        assert result.data[0].person is not None
        assert result.data[0].sport is not None

    async def test_search_players_full_details_with_search_query(
        self, test_db: Database, sport: SportSchemaCreate
    ):
        """Test search with full details and query filter."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        person1 = await person_service.create_or_update_person(
            PersonFactory.build(person_eesl_id=2001, first_name="Alice", second_name="Williams")
        )
        person2 = await person_service.create_or_update_person(
            PersonFactory.build(person_eesl_id=2002, first_name="Charlie", second_name="Brown")
        )

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(person_id=person1.id, sport_id=created_sport.id)
        await player_service.add_person_to_sport(person_id=person2.id, sport_id=created_sport.id)

        result = await player_service.search_players_with_pagination_full_details(
            sport_id=created_sport.id, search_query="alice"
        )

        assert len(result.data) >= 1


@pytest.mark.asyncio
class TestPlayerServiceDBSearchWithPaginationDetailsAndPhotos:
    """Test search_players_with_pagination_details_and_photos functionality."""

    async def test_search_players_with_photos(
        self, test_db: Database, sport: SportSchemaCreate, person: PersonSchemaCreate
    ):
        """Test search with photos."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        person_with_photos = PersonFactory.build(
            person_eesl_id=3001,
            first_name="Photo",
            second_name="Person",
            person_photo_url="http://example.com/photo.jpg",
            person_photo_icon_url="http://example.com/icon.jpg",
        )
        created_person = await person_service.create_or_update_person(person_with_photos)

        player_service = PlayerServiceDB(test_db)
        await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        result = await player_service.search_players_with_pagination_details_and_photos(
            sport_id=created_sport.id
        )

        assert len(result.data) >= 1
        assert result.data[0].person_photo_url is not None
        assert result.data[0].person_photo_icon_url is not None


@pytest.mark.asyncio
class TestPlayerServiceDBGetPlayerCareer:
    """Test get_player_career functionality."""

    async def test_get_player_career_basic(
        self,
        test_db: Database,
        sport: SportSchemaCreate,
        person: PersonSchemaCreate,
    ):
        """Test getting player career data."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        created_player = await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        team_service = TeamServiceDB(test_db)
        team = TeamFactoryWithRelations.build(sport=created_sport)
        await team_service.create(team)

        season_service = SeasonServiceDB(test_db)
        season = SeasonFactoryAny.build()
        created_season = await season_service.create(season)

        tournament_service = TournamentServiceDB(test_db)
        tournament = TournamentFactoryWithRelations.build(
            sport=created_sport, season=created_season
        )
        created_tournament = await tournament_service.create(tournament)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_detail_in_tournament(
                created_player.id, created_tournament.id
            )

        assert exc_info.value.status_code == 404

    async def test_get_player_detail_in_tournament_success(
        self,
        test_db: Database,
        sport: SportSchemaCreate,
        person: PersonSchemaCreate,
    ):
        """Test getting player detail in tournament context."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        created_player = await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        team_service = TeamServiceDB(test_db)
        team = TeamFactoryWithRelations.build(sport=created_sport)
        created_team = await team_service.create(team)

        season_service = SeasonServiceDB(test_db)
        season = SeasonFactoryAny.build()
        created_season = await season_service.create(season)

        tournament_service = TournamentServiceDB(test_db)
        tournament = TournamentFactoryWithRelations.build(
            sport=created_sport, season=created_season
        )
        created_tournament = await tournament_service.create(tournament)

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = PlayerTeamTournamentFactory.build(
            player_id=created_player.id,
            team_id=created_team.id,
            tournament_id=created_tournament.id,
        )
        await ptt_service.create(ptt)

        result = await player_service.get_player_detail_in_tournament(
            created_player.id, created_tournament.id
        )

        assert result.id == created_player.id
        assert result.tournament_assignment is not None
        assert result.career_by_team is not None
        assert result.career_by_tournament is not None

    async def test_get_player_detail_in_tournament_player_not_found_raises_404(
        self, test_db: Database
    ):
        """Test that getting detail for non-existent player raises 404."""
        player_service = PlayerServiceDB(test_db)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_detail_in_tournament(99999, 1)

        assert exc_info.value.status_code == 404

    async def test_get_player_detail_in_tournament_not_in_tournament_raises_404(
        self,
        test_db: Database,
        sport: SportSchemaCreate,
        person: PersonSchemaCreate,
    ):
        """Test that getting detail when player not in tournament raises 404."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        created_player = await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        season_service = SeasonServiceDB(test_db)
        season = SeasonFactoryAny.build()
        created_season = await season_service.create(season)

        tournament_service = TournamentServiceDB(test_db)
        tournament = TournamentFactoryWithRelations.build(
            sport=created_sport, season=created_season
        )
        created_tournament = await tournament_service.create(tournament)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_detail_in_tournament(
                created_player.id, created_tournament.id
            )

        assert exc_info.value.status_code == 404


@pytest.mark.asyncio
class TestPlayerServiceDBGetPlayerDetailInTournament:
    """Test get_player_detail_in_tournament functionality."""

    async def test_get_player_detail_in_tournament_success(
        self,
        test_db: Database,
        sport: SportSchemaCreate,
        person: PersonSchemaCreate,
    ):
        """Test getting player detail in tournament context."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        player_service = PlayerServiceDB(test_db)
        created_player = await player_service.add_person_to_sport(
            person_id=created_person.id, sport_id=created_sport.id
        )

        team_service = TeamServiceDB(test_db)
        team = TeamFactoryWithRelations.build(sport=created_sport)
        created_team = await team_service.create(team)

        season_service = SeasonServiceDB(test_db)
        season = SeasonFactoryAny.build()
        created_season = await season_service.create(season)

        tournament_service = TournamentServiceDB(test_db)
        tournament = TournamentFactoryWithRelations.build(
            sport=created_sport, season=created_season
        )
        created_tournament = await tournament_service.create(tournament)

        ptt_service = PlayerTeamTournamentServiceDB(test_db)
        ptt = PlayerTeamTournamentFactory.build(
            player_id=created_player.id,
            team_id=created_team.id,
            tournament_id=created_tournament.id,
        )
        await ptt_service.create(ptt)

        player_service = PlayerServiceDB(test_db)
        result = await player_service.get_player_detail_in_tournament(
            created_player.id, created_tournament.id
        )

        assert result.id is not None
        assert result.tournament_assignment is not None
        assert result.career_by_team is not None
        assert result.career_by_tournament is not None

    async def test_get_player_detail_in_tournament_player_not_found_raises_404(
        self, test_db: Database
    ):
        """Test that getting detail for non-existent player raises 404."""
        player_service = PlayerServiceDB(test_db)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_detail_in_tournament(99999, 1)

        assert exc_info.value.status_code == 404

    async def test_get_player_detail_in_tournament_not_in_tournament_raises_404(
        self,
        test_db: Database,
        sport: SportSchemaCreate,
        person: PersonSchemaCreate,
    ):
        """Test that getting detail when player not in tournament raises 404."""
        sport_service = SportServiceDB(test_db)
        created_sport = await sport_service.create(sport)

        person_service = PersonServiceDB(test_db)
        created_person = await person_service.create_or_update_person(person)

        season_service = SeasonServiceDB(test_db)
        season = SeasonFactoryAny.build()
        created_season = await season_service.create(season)

        tournament_service = TournamentServiceDB(test_db)
        tournament = TournamentFactoryWithRelations.build(
            sport_id=created_sport.id, season_id=created_season.id
        )
        created_tournament = await tournament_service.create(tournament)

        player_service = PlayerServiceDB(test_db)

        with pytest.raises(HTTPException) as exc_info:
            await player_service.get_player_detail_in_tournament(
                created_person.id, created_tournament.id
            )

        assert exc_info.value.status_code == 404
