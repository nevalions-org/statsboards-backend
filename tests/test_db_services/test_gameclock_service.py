import pytest

from src.core.enums import ClockDirection, ClockStatus, PeriodClockVariant
from src.gameclocks.db_services import GameClockServiceDB
from src.gameclocks.schemas import GameClockSchemaCreate, GameClockSchemaUpdate
from src.matchdata.db_services import MatchDataServiceDB
from src.matchdata.schemas import MatchDataSchemaCreate, MatchDataSchemaUpdate
from src.matches.db_services import MatchServiceDB
from src.seasons.db_services import SeasonServiceDB
from src.sport_scoreboard_preset.db_services import SportScoreboardPresetServiceDB
from src.sport_scoreboard_preset.schemas import SportScoreboardPresetSchemaCreate
from src.sports.db_services import SportServiceDB
from src.teams.db_services import TeamServiceDB
from src.tournaments.db_services import TournamentServiceDB
from tests.factories import (
    MatchFactory,
    SeasonFactorySample,
    SportFactorySample,
    TeamFactory,
    TournamentFactory,
)


@pytest.mark.asyncio
class TestGameClockServiceDB:
    async def test_create_gameclock(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=720,
            gameclock_max=720,
            gameclock_status="stopped",
        )

        result = await gameclock_service.create(gameclock_data)

        assert result is not None
        assert result.id is not None
        assert result.match_id == match.id
        assert result.gameclock == 720

    async def test_create_gameclock_duplicate(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(match_id=match.id, gameclock=720)

        result1 = await gameclock_service.create(gameclock_data)
        result2 = await gameclock_service.create(gameclock_data)

        assert result1 is not None
        assert result2 is not None

    async def test_update_gameclock(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(match_id=match.id, gameclock=720)

        created = await gameclock_service.create(gameclock_data)
        update_data = GameClockSchemaUpdate(gameclock=600, gameclock_status="running")

        updated = await gameclock_service.update(created.id, update_data)

        assert updated.gameclock == 600
        assert updated.gameclock_status == "running"

    async def test_get_gameclock_by_match_id(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(match_id=match.id, gameclock=720)

        created = await gameclock_service.create(gameclock_data)
        result = await gameclock_service.get_gameclock_by_match_id(match.id)

        assert result is not None
        assert result.id == created.id

    async def test_get_gameclock_status(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id, gameclock=720, gameclock_status="running"
        )

        created = await gameclock_service.create(gameclock_data)
        status = await gameclock_service.get_gameclock_status(created.id)

        assert status == "running"

    async def test_enable_match_data_gameclock_queues(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(match_id=match.id, gameclock=720)

        created = await gameclock_service.create(gameclock_data)
        queue = await gameclock_service.enable_match_data_gameclock_queues(created.id)

        assert queue is not None
        assert created.id in gameclock_service.clock_manager.active_gameclock_matches

    async def test_pause_gameclock_clears_started_at_ms(self, test_db):
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(match_id=match.id, gameclock=720)

        created = await gameclock_service.create(gameclock_data)

        started_at_ms = 1234567890000
        await gameclock_service.update(
            created.id,
            GameClockSchemaUpdate(
                gameclock_status="running",
                gameclock_time_remaining=720,
                started_at_ms=started_at_ms,
            ),
        )

        running_clock = await gameclock_service.get_by_id(created.id)
        assert running_clock.started_at_ms == started_at_ms

        paused = await gameclock_service.update(
            created.id,
            GameClockSchemaUpdate(
                gameclock_status="paused",
                gameclock=720,
                gameclock_time_remaining=720,
                started_at_ms=None,
            ),
        )

        assert paused.started_at_ms is None
        assert paused.gameclock_status == "paused"

    async def test_delete_gameclock_cleans_up_resources(self, test_db):
        """Test that deleting a gameclock cleans up clock resources."""
        from src.clocks import clock_orchestrator

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=720,
            gameclock_max=720,
            gameclock_status="stopped",
        )

        created = await gameclock_service.create(gameclock_data)

        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        assert created.id in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id in gameclock_service.clock_manager.clock_state_machines

        await gameclock_service.delete(created.id)

        assert created.id not in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id not in gameclock_service.clock_manager.clock_state_machines
        assert created.id not in clock_orchestrator.running_gameclocks

    async def test_stop_gameclock_internal_unregisters_from_orchestrator(self, test_db):
        """Test that _stop_gameclock_internal properly unregisters from orchestrator (STAB-191)."""
        from src.clocks import clock_orchestrator

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id, gameclock=720, gameclock_status="running"
        )

        created = await gameclock_service.create(gameclock_data)

        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        assert created.id in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id in gameclock_service.clock_manager.clock_state_machines
        assert created.id in clock_orchestrator.running_gameclocks

        await gameclock_service._stop_gameclock_internal(created.id)

        assert created.id not in clock_orchestrator.running_gameclocks
        assert created.id not in gameclock_service.clock_manager.clock_state_machines

    async def test_stop_gameclock_internal_persists_up_direction_terminal_max(self, test_db):
        """Test up-direction stop callback persists max value instead of resetting to 0 (STAB-226)."""
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=10,
            gameclock_max=10,
            direction=ClockDirection.UP,
            gameclock_status="running",
        )

        created = await gameclock_service.create(gameclock_data)
        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        state_machine = gameclock_service.clock_manager.get_clock_state_machine(created.id)
        assert state_machine is not None
        state_machine.value = 10
        state_machine.started_at_ms = None

        await gameclock_service._stop_gameclock_internal(created.id)

        stopped = await gameclock_service.get_by_id(created.id)
        assert stopped is not None
        assert stopped.gameclock == 10
        assert stopped.gameclock_time_remaining == 0
        assert stopped.gameclock_status == "stopped"
        assert stopped.started_at_ms is None

    async def test_stop_gameclock_internal_uses_max_when_state_machine_missing(self, test_db):
        """Test up-direction terminal stop persists max even without local state machine (STAB-227)."""
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=2698,
            gameclock_max=2700,
            direction=ClockDirection.UP,
            gameclock_status=ClockStatus.RUNNING,
        )

        created = await gameclock_service.create(gameclock_data)

        await gameclock_service._stop_gameclock_internal(created.id)

        stopped = await gameclock_service.get_by_id(created.id)
        assert stopped is not None
        assert stopped.gameclock == 2700
        assert stopped.gameclock_time_remaining == 0
        assert stopped.gameclock_status == "stopped"
        assert stopped.started_at_ms is None

    async def test_stop_gameclock_internal_persists_down_direction_zero(self, test_db):
        """Test down-direction stop callback keeps existing terminal value semantics (0)."""
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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=1,
            gameclock_max=10,
            direction=ClockDirection.DOWN,
            gameclock_status="running",
        )

        created = await gameclock_service.create(gameclock_data)
        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        state_machine = gameclock_service.clock_manager.get_clock_state_machine(created.id)
        assert state_machine is not None
        state_machine.value = 0
        state_machine.started_at_ms = None

        await gameclock_service._stop_gameclock_internal(created.id)

        stopped = await gameclock_service.get_by_id(created.id)
        assert stopped is not None
        assert stopped.gameclock == 0
        assert stopped.gameclock_time_remaining == 0
        assert stopped.gameclock_status == "stopped"
        assert stopped.started_at_ms is None

    async def _setup_match_with_preset(
        self, test_db, period_clock_variant: PeriodClockVariant, direction: ClockDirection
    ):
        preset_service = SportScoreboardPresetServiceDB(test_db)
        preset = await preset_service.create(
            SportScoreboardPresetSchemaCreate(
                title=f"Preset {period_clock_variant.value}",
                gameclock_max=1200,
                period_clock_variant=period_clock_variant,
                direction=direction,
            )
        )

        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build(scoreboard_preset_id=preset.id))

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        return match, preset

    async def test_compute_reset_value_down_direction_returns_max(self, test_db):
        """Test compute_reset_value returns gameclock_max for countdown clocks."""
        match, preset = await self._setup_match_with_preset(
            test_db, PeriodClockVariant.PER_PERIOD, ClockDirection.DOWN
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=300,
                gameclock_max=1200,
                direction=ClockDirection.DOWN,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 1200

    async def test_compute_reset_value_up_per_period_returns_zero(self, test_db):
        """Test compute_reset_value returns 0 for count-up non-cumulative clocks."""
        match, preset = await self._setup_match_with_preset(
            test_db, PeriodClockVariant.PER_PERIOD, ClockDirection.UP
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=800,
                gameclock_max=1200,
                direction=ClockDirection.UP,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 0

    async def test_compute_reset_value_up_cumulative_period_1_returns_zero(self, test_db):
        """Test compute_reset_value returns 0 for cumulative clock in period 1."""
        match, preset = await self._setup_match_with_preset(
            test_db, PeriodClockVariant.CUMULATIVE, ClockDirection.UP
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=800,
                gameclock_max=1200,
                direction=ClockDirection.UP,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 0

    async def test_compute_reset_value_up_cumulative_period_2_returns_base_max(self, test_db):
        """Test compute_reset_value returns base_max for cumulative clock in period 2."""
        match, preset = await self._setup_match_with_preset(
            test_db, PeriodClockVariant.CUMULATIVE, ClockDirection.UP
        )

        matchdata_service = MatchDataServiceDB(test_db)
        matchdata = await matchdata_service.get_match_data_by_match_id(match.id)
        if matchdata is None:
            matchdata = await matchdata_service.create(MatchDataSchemaCreate(match_id=match.id))
        await matchdata_service.update(
            matchdata.id,
            MatchDataSchemaUpdate(qtr="2nd", period_key="period.2"),
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=2000,
                gameclock_max=2400,
                direction=ClockDirection.UP,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 1200

    async def test_compute_reset_value_up_cumulative_period_3_returns_double_base(self, test_db):
        """Test compute_reset_value returns 2*base_max for cumulative clock in period 3."""
        match, preset = await self._setup_match_with_preset(
            test_db, PeriodClockVariant.CUMULATIVE, ClockDirection.UP
        )

        matchdata_service = MatchDataServiceDB(test_db)
        matchdata = await matchdata_service.get_match_data_by_match_id(match.id)
        if matchdata is None:
            matchdata = await matchdata_service.create(MatchDataSchemaCreate(match_id=match.id))
        await matchdata_service.update(
            matchdata.id,
            MatchDataSchemaUpdate(qtr="3rd", period_key="period.3"),
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=4000,
                gameclock_max=3600,
                direction=ClockDirection.UP,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 2400

    async def test_compute_reset_value_no_preset_defaults_to_zero(self, test_db):
        """Test compute_reset_value defaults to 0 for count-up clocks without preset."""
        sport_service = SportServiceDB(test_db)
        sport = await sport_service.create(SportFactorySample.build(scoreboard_preset_id=None))

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock = await gameclock_service.create(
            GameClockSchemaCreate(
                match_id=match.id,
                gameclock=500,
                direction=ClockDirection.UP,
            )
        )

        reset_value = await gameclock_service.compute_reset_value(gameclock.id)
        assert reset_value == 0

    async def test_compute_reset_value_nonexistent_returns_none(self, test_db):
        """Test compute_reset_value returns None for nonexistent gameclock."""
        gameclock_service = GameClockServiceDB(test_db)
        reset_value = await gameclock_service.compute_reset_value(99999)
        assert reset_value is None

    async def test_trigger_update_gameclock_auto_unregisters_stopped_clock(self, test_db):
        """Test that trigger_update_gameclock auto-unregisters a stopped clock (STAB-250)."""
        from src.clocks import clock_orchestrator

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=720,
            gameclock_max=720,
            gameclock_status=ClockStatus.RUNNING,
        )

        created = await gameclock_service.create(gameclock_data)
        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        assert created.id in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id in clock_orchestrator.running_gameclocks

        await gameclock_service.update(
            created.id,
            GameClockSchemaUpdate(gameclock_status=ClockStatus.STOPPED),
        )

        assert created.id not in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id not in clock_orchestrator.running_gameclocks

    async def test_trigger_update_gameclock_auto_unregisters_paused_clock(self, test_db):
        """Test that trigger_update_gameclock auto-unregisters a paused clock (STAB-250)."""
        from src.clocks import clock_orchestrator

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
            MatchFactory.build(
                tournament_id=tournament.id, team_a_id=team_a.id, team_b_id=team_b.id
            )
        )

        gameclock_service = GameClockServiceDB(test_db)
        gameclock_data = GameClockSchemaCreate(
            match_id=match.id,
            gameclock=720,
            gameclock_max=720,
            gameclock_status=ClockStatus.RUNNING,
        )

        created = await gameclock_service.create(gameclock_data)
        await gameclock_service.enable_match_data_gameclock_queues(created.id)

        assert created.id in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id in clock_orchestrator.running_gameclocks

        await gameclock_service.update(
            created.id,
            GameClockSchemaUpdate(gameclock_status=ClockStatus.PAUSED),
        )

        assert created.id not in gameclock_service.clock_manager.active_gameclock_matches
        assert created.id not in clock_orchestrator.running_gameclocks
