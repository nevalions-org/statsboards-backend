import asyncio
import time

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.decorators import handle_service_exceptions
from src.core.enums import ClockDirection, ClockStatus, PeriodClockVariant
from src.core.models import (
    BaseServiceDB,
    GameClockDB,
    MatchDataDB,
    MatchDB,
    SportDB,
    SportScoreboardPresetDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.period_clock import extract_period_index

from ..clocks import clock_orchestrator
from ..logging_config import get_logger
from .clock_state_machine import ClockStateMachine
from .schemas import GameClockSchemaBase, GameClockSchemaCreate, GameClockSchemaUpdate


class ClockManager:
    def __init__(self) -> None:
        self.active_gameclock_matches: dict[int, asyncio.Queue] = {}
        self.clock_state_machines: dict[int, ClockStateMachine] = {}
        self.logger = get_logger("ClockManager", self)
        self.logger.debug("Initialized ClockManager")

    async def start_clock(
        self,
        gameclock_id: int,
        initial_value: int = 0,
        direction: str = "down",
        max_value: int = 720,
    ) -> None:
        self.logger.debug("Start clock in clock manager")
        if gameclock_id not in self.active_gameclock_matches:
            self.active_gameclock_matches[gameclock_id] = asyncio.Queue()
        if gameclock_id not in self.clock_state_machines:
            from src.core.enums import ClockDirection

            direction_enum = ClockDirection(direction) if isinstance(direction, str) else direction
            self.clock_state_machines[gameclock_id] = ClockStateMachine(
                gameclock_id, initial_value, direction_enum, max_value
            )

    async def end_clock(self, gameclock_id: int) -> None:
        self.logger.debug("Stop clock in clock manager")
        if gameclock_id in self.active_gameclock_matches:
            queue = self.active_gameclock_matches[gameclock_id]
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            del self.active_gameclock_matches[gameclock_id]
        if gameclock_id in self.clock_state_machines:
            del self.clock_state_machines[gameclock_id]

    async def update_queue_clock(self, gameclock_id: int, message: GameClockDB) -> None:
        if gameclock_id in self.active_gameclock_matches:
            self.logger.debug("Update clock in clock manager")
            queue = self.active_gameclock_matches[gameclock_id]
            await queue.put(message)

    def get_clock_state_machine(self, gameclock_id: int) -> ClockStateMachine | None:
        return self.clock_state_machines.get(gameclock_id)


class GameClockServiceDB(BaseServiceDB):
    def __init__(self, database: Database, disable_background_tasks: bool = False) -> None:
        super().__init__(database, GameClockDB)
        self.clock_manager = ClockManager()
        self.disable_background_tasks = disable_background_tasks
        self.logger = get_logger("GameClockServiceDB", self)
        self.logger.debug("Initialized GameClockServiceDB")
        self._setup_orchestrator_callbacks()

    def _setup_orchestrator_callbacks(self) -> None:
        clock_orchestrator.set_gameclock_update_callback(self.trigger_update_gameclock)
        clock_orchestrator.set_gameclock_stop_callback(self._stop_gameclock_internal)

    @handle_service_exceptions(item_name="GAMECLOCK", operation="creating")
    async def create(
        self,
        item: GameClockSchemaCreate,
        *,
        session: AsyncSession | None = None,
    ) -> GameClockDB:
        """Create a gameclock.

        Args:
            item: The gameclock data to create.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Create gameclock: {item}")
        if item.match_id is not None:
            is_exist = await self._get_gameclock_by_match_id_with_session(
                item.match_id, session=session
            )
            if is_exist:
                self.logger.info(f"gameclock already exists: {is_exist}")
                return is_exist

        if session is not None:
            return await self._create_with_session(item, session)
        return await super().create(item)

    async def _create_with_session(
        self,
        item: GameClockSchemaCreate,
        session: AsyncSession,
    ) -> GameClockDB:
        """Create gameclock using provided session (no commit)."""
        self.logger.debug(f"Create gameclock with session: {item}")
        if isinstance(item, BaseModel):
            item_to_add = GameClockDB(**item.model_dump())
        else:
            item_to_add = item

        session.add(item_to_add)
        await session.flush()
        await session.refresh(item_to_add)
        self.logger.info(f"Gameclock created with session: {item_to_add}")
        return item_to_add

    async def _get_gameclock_by_match_id_with_session(
        self,
        match_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> GameClockDB | None:
        """Get gameclock by match_id, optionally using provided session."""
        if session is not None:
            self.logger.debug(f"Get gameclock by match id:{match_id} with session")
            result = await session.scalars(
                select(GameClockDB).where(GameClockDB.match_id == match_id)
            )
            return result.one_or_none()
        return await self.get_gameclock_by_match_id(match_id)

    async def _stop_gameclock_internal(self, gameclock_id: int) -> None:
        """Persist terminal gameclock state and clean up runtime clock resources."""
        self.logger.info("Stopping gameclock %s at terminal value", gameclock_id)
        state_machine = self.clock_manager.get_clock_state_machine(gameclock_id)
        terminal_gameclock_value = 0
        if state_machine:
            self.logger.debug(
                "Gameclock %s state before stop: status=%s value=%s started_at_ms=%s",
                gameclock_id,
                state_machine.status,
                state_machine.value,
                state_machine.started_at_ms,
            )
            if state_machine.direction == ClockDirection.UP:
                terminal_gameclock_value = max(0, state_machine.max_value)
            state_machine.stop()
        else:
            gameclock = await self.get_by_id(gameclock_id)
            if gameclock and gameclock.direction == ClockDirection.UP:
                max_value = gameclock.gameclock_max if gameclock.gameclock_max is not None else 0
                terminal_gameclock_value = max(0, max_value)

        await self.update(
            gameclock_id,
            GameClockSchemaUpdate(
                gameclock=terminal_gameclock_value,
                gameclock_time_remaining=0,
                gameclock_status="stopped",
                started_at_ms=None,
            ),
        )
        clock_orchestrator.unregister_gameclock(gameclock_id)
        await self.clock_manager.end_clock(gameclock_id)

    async def stop_gameclock(self, gameclock_id: int) -> None:
        """Manually stop gameclock and unregister from orchestrator"""
        self.logger.debug(f"Manually stopping gameclock {gameclock_id}")
        state_machine = self.clock_manager.get_clock_state_machine(gameclock_id)
        if state_machine:
            state_machine.stop()
        clock_orchestrator.unregister_gameclock(gameclock_id)
        await self.clock_manager.end_clock(gameclock_id)

    async def enable_match_data_gameclock_queues(
        self,
        item_id: int,
    ) -> asyncio.Queue:
        self.logger.debug(f"Enable matchdata gameclock queues match id:{item_id}")
        gameclock = await self.get_by_id(item_id)

        if not gameclock:
            self.logger.warning(f"Gameclock not found: {item_id}")

        active_clock_matches = self.clock_manager.active_gameclock_matches

        if item_id not in active_clock_matches:
            self.logger.debug(f"Gameclock not in active gameclock matches: {item_id}")
            initial_value = gameclock.gameclock if gameclock and gameclock.gameclock else 0
            direction = getattr(gameclock, "direction", "down")
            max_value = getattr(gameclock, "gameclock_max", 720)
            await self.clock_manager.start_clock(item_id, initial_value, direction, max_value)

            state_machine = self.clock_manager.get_clock_state_machine(item_id)
            if state_machine and gameclock and gameclock.gameclock_status == ClockStatus.RUNNING:
                state_machine.start()

            if state_machine and state_machine.status == ClockStatus.RUNNING:
                clock_orchestrator.register_gameclock(item_id, state_machine)

            self.logger.debug(f"Gameclock added to active gameclock matches: {item_id}")
        match_queue = active_clock_matches[item_id]
        if gameclock:
            await match_queue.put(gameclock)
        self.logger.info(f"Gameclock enabled successfully {gameclock}")
        return match_queue

    async def update(
        self,
        item_id: int,
        item: BaseModel,
        **kwargs,
    ) -> GameClockDB | None:
        self.logger.debug(f"Update gameclock endpoint id:{item_id} data: {item}")
        async with self.db.get_session_maker()() as session:
            result = await session.execute(select(GameClockDB).where(GameClockDB.id == item_id))
            updated_item = result.scalars().one_or_none()

            if not updated_item:
                self.logger.warning(f"GameClock not found: {item_id}")
                return None

            update_data = item.model_dump(exclude_unset=True)
            update_data["version"] = (updated_item.version or 0) + 1

            for key, value in update_data.items():
                if value is not None or key == "started_at_ms":
                    setattr(updated_item, key, value)

            await session.flush()
            await session.commit()
            await session.refresh(updated_item)

            self.logger.debug(f"Updated gameclock: {updated_item}")
            if update_data.get("gameclock_status") == ClockStatus.RUNNING:
                await self._register_running_gameclock(updated_item)
            await self.trigger_update_gameclock(item_id)

            return updated_item

    async def _register_running_gameclock(self, gameclock: GameClockDB) -> None:
        state_machine = self.clock_manager.get_clock_state_machine(gameclock.id)
        if not state_machine:
            initial_value = gameclock.gameclock if gameclock.gameclock is not None else 0
            direction = getattr(gameclock, "direction", "down")
            max_value = getattr(gameclock, "gameclock_max", 720)
            await self.clock_manager.start_clock(gameclock.id, initial_value, direction, max_value)
            state_machine = self.clock_manager.get_clock_state_machine(gameclock.id)
        if not state_machine:
            return

        state_machine.value = gameclock.gameclock if gameclock.gameclock is not None else 0
        if gameclock.started_at_ms is not None:
            state_machine.started_at_ms = gameclock.started_at_ms
        elif state_machine.started_at_ms is None:
            state_machine.started_at_ms = int(time.time() * 1000)
        state_machine.status = ClockStatus.RUNNING
        clock_orchestrator.register_gameclock(gameclock.id, state_machine)

    async def get_gameclock_status(
        self,
        item_id: int,
    ) -> str | None:
        self.logger.debug(f"Get gameclock status for item id:{item_id}")
        gameclock: GameClockSchemaBase | None = await self.get_by_id(item_id)
        if gameclock:
            self.logger.debug(f"Gameclock status: {gameclock}")
            return gameclock.gameclock_status
        else:
            self.logger.warning(f"Gameclock not found: {item_id}")
            return None

    async def get_gameclock_by_match_id(self, match_id: int) -> GameClockDB | None:
        async with self.db.get_session_maker()() as session:
            self.logger.debug(f"Get gameclock by match id:{match_id}")
            result = await session.scalars(
                select(GameClockDB).where(GameClockDB.match_id == match_id)
            )
            if result:
                self.logger.debug(f"Gameclock in DB: {result}")
                gameclock = result.one_or_none()
                if gameclock:
                    state_machine = self.clock_manager.get_clock_state_machine(gameclock.id)
                    if state_machine and state_machine.status == ClockStatus.RUNNING:
                        gameclock.gameclock = state_machine.get_current_value()
                    self.logger.debug(f"Gameclock found: {gameclock}")
                    return gameclock
        return None

    async def delete(self, item_id: int) -> dict:
        """Delete gameclock and clean up clock resources."""
        self.logger.debug(f"Deleting gameclock with ID: {item_id}")
        await self.stop_gameclock(item_id)
        return await super().delete(item_id)

    async def trigger_update_gameclock(
        self,
        gameclock_id: int,
    ) -> None:
        self.logger.debug(f"Trigger update gameclock for gameclock id:{gameclock_id}")
        gameclock: GameClockSchemaBase | None = await self.get_by_id(gameclock_id)

        active_clock_matches = self.clock_manager.active_gameclock_matches

        if gameclock_id in active_clock_matches:
            matchdata_clock_queue = active_clock_matches[gameclock_id]
            await matchdata_clock_queue.put(gameclock)
        else:
            self.logger.warning(f"No active gameclock found with id:{gameclock_id}")

    async def compute_reset_value(self, gameclock_id: int) -> int | None:
        """
        Compute the reset value for a gameclock based on sport preset and period.

        Reset rules:
        - direction=up + period_clock_variant=cumulative => reset to period start (base_max * (period_index - 1))
        - direction=up + non-cumulative => reset to 0
        - direction=down => reset to gameclock_max
        """
        gameclock = await self.get_by_id(gameclock_id)
        if gameclock is None:
            self.logger.warning(f"Gameclock not found: {gameclock_id}")
            return None

        if gameclock.direction == ClockDirection.DOWN:
            return gameclock.gameclock_max

        preset_config = await self._get_period_clock_preset_config(gameclock.match_id)
        if preset_config is None:
            self.logger.debug("No preset config found, defaulting to 0")
            return 0

        base_max, variant = preset_config

        if variant != PeriodClockVariant.CUMULATIVE:
            return 0

        period_index = await self._get_current_period_index(gameclock.match_id)
        reset_value = base_max * (period_index - 1) if base_max else 0
        return max(0, reset_value)

    async def _get_period_clock_preset_config(
        self, match_id: int
    ) -> tuple[int | None, PeriodClockVariant] | None:
        """Get base_max and period_clock_variant from sport preset for a match."""
        async with self.db.get_session_maker()() as session:
            result = await session.execute(
                select(
                    SportScoreboardPresetDB.gameclock_max,
                    SportScoreboardPresetDB.period_clock_variant,
                )
                .select_from(MatchDB)
                .join(TournamentDB, MatchDB.tournament_id == TournamentDB.id)
                .join(SportDB, TournamentDB.sport_id == SportDB.id)
                .join(
                    SportScoreboardPresetDB,
                    SportDB.scoreboard_preset_id == SportScoreboardPresetDB.id,
                )
                .where(MatchDB.id == match_id)
            )
            row = result.one_or_none()
            if row is None:
                return None
            return row[0], PeriodClockVariant(row[1])

    async def _get_current_period_index(self, match_id: int) -> int:
        """Get current period index from matchdata."""
        async with self.db.get_session_maker()() as session:
            result = await session.execute(
                select(MatchDataDB.period_key, MatchDataDB.qtr).where(
                    MatchDataDB.match_id == match_id
                )
            )
            row = result.one_or_none()
            if row is None:
                return 1
            return extract_period_index(period_key=row[0], qtr=row[1])
