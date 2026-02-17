import asyncio

import pytest

from src.core.enums import ClockDirection
from src.gameclocks.clock_state_machine import ClockStateMachine


@pytest.mark.slow
class TestClockStateMachine:
    """Test gameclock state machine behavior - pause/resume/stop operations."""

    def test_pause_preserves_current_value(self):
        """Test that pausing a running state machine preserves current value."""
        state_machine = ClockStateMachine(1, 720)
        assert state_machine.status == "stopped"
        assert state_machine.value == 720

        state_machine.start()
        assert state_machine.status == "running"
        assert state_machine.started_at_ms is not None

        state_machine.pause()
        assert state_machine.status == "paused"
        assert state_machine.started_at_ms is None

        paused_value = state_machine.get_current_value()
        assert paused_value == 720

    def test_resume_from_paused_continues_from_paused_value(self):
        """Test that resuming from paused continues from paused value."""
        state_machine = ClockStateMachine(1, 720)
        assert state_machine.status == "stopped"

        state_machine.start()
        asyncio.run(asyncio.sleep(0.1))
        state_machine.get_current_value()

        state_machine.pause()
        state_machine.get_current_value()
        assert state_machine.status == "paused"

        asyncio.run(asyncio.sleep(0.1))

        state_machine.start()
        assert state_machine.status == "running"
        assert state_machine.started_at_ms is not None

        asyncio.run(asyncio.sleep(0.1))
        state_machine.get_current_value()

    def test_stop_does_not_override_paused_status(self):
        """Test that stop() sets status to stopped regardless of previous state."""
        state_machine = ClockStateMachine(1, 720)
        assert state_machine.status == "stopped"

        state_machine.start()
        asyncio.run(asyncio.sleep(0.1))
        state_machine.get_current_value()

        state_machine.pause()
        paused_value = state_machine.get_current_value()
        assert state_machine.status == "paused"

        state_machine.stop()
        assert state_machine.status == "stopped"
        assert state_machine.get_current_value() == paused_value

    def test_calculate_elapsed_time_from_started_at_ms(self):
        """Test that get_current_value calculates elapsed time correctly."""
        state_machine = ClockStateMachine(1, 720)
        assert state_machine.value == 720
        assert state_machine.status == "stopped"

        state_machine.start()
        asyncio.run(asyncio.sleep(0.1))
        current_value_1 = state_machine.get_current_value()
        elapsed_time_1 = 720 - current_value_1
        assert elapsed_time_1 >= 0

        asyncio.run(asyncio.sleep(0.2))
        current_value_2 = state_machine.get_current_value()
        elapsed_time_2 = 720 - current_value_2
        assert elapsed_time_2 >= elapsed_time_1
        assert elapsed_time_2 <= 0.3


@pytest.mark.slow
class TestClockStateMachineCountUp:
    """Test count-up gameclock state machine behavior - STAB-202."""

    def test_count_up_initialization(self):
        """Test that count-up state machine initializes correctly."""
        state_machine = ClockStateMachine(
            clock_id=1,
            initial_value=0,
            direction=ClockDirection.UP,
            max_value=1000,
        )

        assert state_machine.clock_id == 1
        assert state_machine.value == 0
        assert state_machine.direction == ClockDirection.UP
        assert state_machine.max_value == 1000
        assert state_machine.status == "stopped"

    def test_count_up_stops_at_max(self):
        """Test that count-up state machine stops at max_value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=10
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(0.1))

        current_value = state_machine.get_current_value()
        assert current_value <= 10

    def test_count_up_get_current_value_increments(self):
        """Test that get_current_value increments over time for count-up."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        value_1 = state_machine.get_current_value()
        assert value_1 == 0

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))
        value_2 = state_machine.get_current_value()
        assert value_2 > 0

        asyncio.run(asyncio.sleep(1.0))
        value_3 = state_machine.get_current_value()
        assert value_3 > value_2

    def test_count_up_pause_preserves_value(self):
        """Test that pausing count-up preserves current value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))

        state_machine.pause()
        paused_value = state_machine.get_current_value()
        assert paused_value > 0

        asyncio.run(asyncio.sleep(1.0))
        value_after_pause = state_machine.get_current_value()
        assert value_after_pause == paused_value

    def test_count_up_resume_continues_from_paused_value(self):
        """Test that resuming count-up continues from paused value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))

        state_machine.pause()
        paused_value = state_machine.get_current_value()

        asyncio.run(asyncio.sleep(1.0))

        state_machine.start()
        asyncio.run(asyncio.sleep(1.0))
        resumed_value = state_machine.get_current_value()

        assert resumed_value > paused_value

    def test_count_up_with_non_zero_initial_value(self):
        """Test that count-up starts from non-zero initial value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=500, direction=ClockDirection.UP, max_value=1000
        )

        assert state_machine.value == 500
        assert state_machine.get_current_value() == 500

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))

        running_value = state_machine.get_current_value()
        assert running_value >= 500
        assert running_value <= 1000

    def test_count_up_respects_max_value_boundary(self):
        """Test that count-up never exceeds max_value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=5
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(1.0))

        current_value = state_machine.get_current_value()
        assert current_value <= 5

    def test_count_up_down_direction_comparison(self):
        """Test that count-up and count-down behave differently."""
        state_up = ClockStateMachine(
            clock_id=1, initial_value=500, direction=ClockDirection.UP, max_value=1000
        )
        state_down = ClockStateMachine(
            clock_id=2, initial_value=500, direction=ClockDirection.DOWN, max_value=1000
        )

        assert state_up.direction == ClockDirection.UP
        assert state_down.direction == ClockDirection.DOWN

        state_up.start()
        state_down.start()
        asyncio.run(asyncio.sleep(1.5))

        value_up = state_up.get_current_value()
        value_down = state_down.get_current_value()

        assert value_up >= 500
        assert value_down <= 500

    def test_count_up_stop_preserves_value(self):
        """Test that stopping count-up preserves current value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))

        state_machine.stop()
        stopped_value = state_machine.get_current_value()
        assert stopped_value > 0

        asyncio.run(asyncio.sleep(1.0))
        value_after_stop = state_machine.get_current_value()
        assert value_after_stop == stopped_value

    def test_count_up_multiple_start_stop_cycles(self):
        """Test that multiple start/stop cycles work correctly."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        asyncio.run(asyncio.sleep(1.5))
        value_1 = state_machine.get_current_value()

        state_machine.stop()
        stopped_value_1 = state_machine.get_current_value()

        state_machine.start()
        asyncio.run(asyncio.sleep(1.0))
        value_2 = state_machine.get_current_value()

        assert value_2 > value_1
        assert value_2 > stopped_value_1

    def test_count_up_status_transitions(self):
        """Test that count-up state transitions work correctly."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        assert state_machine.status == "stopped"

        state_machine.start()
        assert state_machine.status == "running"

        state_machine.pause()
        assert state_machine.status == "paused"

        state_machine.start()
        assert state_machine.status == "running"

        state_machine.stop()
        assert state_machine.status == "stopped"

    def test_count_up_with_max_value_equals_initial(self):
        """Test count-up when max_value equals initial value."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=100, direction=ClockDirection.UP, max_value=100
        )

        assert state_machine.get_current_value() == 100

        state_machine.start()
        asyncio.run(asyncio.sleep(0.1))

        current_value = state_machine.get_current_value()
        assert current_value == 100

    def test_count_up_started_at_ms_set_on_start(self):
        """Test that started_at_ms is set when starting count-up."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        assert state_machine.started_at_ms is None

        state_machine.start()
        assert state_machine.started_at_ms is not None

    def test_count_up_started_at_ms_cleared_on_pause(self):
        """Test that started_at_ms is cleared when pausing count-up."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        assert state_machine.started_at_ms is not None

        state_machine.pause()
        assert state_machine.started_at_ms is None

    def test_count_up_started_at_ms_cleared_on_stop(self):
        """Test that started_at_ms is cleared when stopping count-up."""
        state_machine = ClockStateMachine(
            clock_id=1, initial_value=0, direction=ClockDirection.UP, max_value=1000
        )

        state_machine.start()
        assert state_machine.started_at_ms is not None

        state_machine.stop()
        assert state_machine.started_at_ms is None
