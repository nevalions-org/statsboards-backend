from typing import Any

from fastapi import BackgroundTasks
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.decorators import handle_service_exceptions
from src.core.enums import PeriodClockVariant
from src.core.models import (
    BaseServiceDB,
    MatchDataDB,
    MatchDB,
    SportDB,
    SportScoreboardPresetDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.period_clock import calculate_effective_gameclock_max, extract_period_index
from src.gameclocks.db_services import GameClockServiceDB
from src.gameclocks.schemas import GameClockSchemaUpdate

from ..logging_config import get_logger
from .schemas import MatchDataSchemaCreate

ITEM = "MATCHDATA"


class MatchDataServiceDB(BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(database, MatchDataDB)
        # self.match_manager = MatchDataManager()
        self._running_tasks = {}
        self.logger = get_logger("MatchDataServiceDB", self)
        self.logger.debug("Initialized MatchDataServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: MatchDataSchemaCreate,
        *,
        session: AsyncSession | None = None,
    ) -> MatchDataDB:
        """Create match data.

        Args:
            item: The match data to create.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Create {ITEM}:{item}")
        if session is not None:
            return await self._create_with_session(item, session)
        result = await super().create(item)
        return result  # type: ignore

    async def _create_with_session(
        self,
        item: MatchDataSchemaCreate,
        session: AsyncSession,
    ) -> MatchDataDB:
        """Create match data using provided session (no commit)."""
        from pydantic import BaseModel

        self.logger.debug(f"Create match data with session: {item}")
        if isinstance(item, BaseModel):
            item_to_add = MatchDataDB(**item.model_dump())
        else:
            item_to_add = item

        session.add(item_to_add)
        await session.flush()
        await session.refresh(item_to_add)
        self.logger.info(f"Match data created with session: {item_to_add}")
        return item_to_add

    @handle_service_exceptions(item_name=ITEM, operation="updating", return_value_on_not_found=None)
    async def update(
        self,
        item_id: int,
        item: Any,
        *,
        session: AsyncSession | None = None,
        **kwargs,
    ) -> MatchDataDB | None:
        """Update match data.

        Args:
            item_id: The match data ID to update.
            item: The update data.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Updating matchdata item_id: {item_id}")

        if session is not None:
            return await self._update_with_session(item_id, item, session)

        existing_item = await self.get_by_id(item_id)
        update_payload = item.model_dump(exclude_unset=True)
        period_transition_requested = "qtr" in update_payload or "period_key" in update_payload

        updated_ = await super().update(
            item_id,
            item,
            **kwargs,
        )
        if updated_ is None:
            return None

        if period_transition_requested and self._is_period_transition(
            existing_item, update_payload
        ):
            await self._recalculate_gameclock_effective_max(
                match_id=updated_.match_id,
                period_key=updated_.period_key,
                qtr=updated_.qtr,
            )

        """triggers for sse process, now we use websocket
        await self.trigger_update_match_data(item_id)"""
        # await self.trigger_update_match_data(item_id)
        self.logger.info("Matchdata updated successfully")
        return updated_

    async def _update_with_session(
        self,
        item_id: int,
        item: Any,
        session: AsyncSession,
    ) -> MatchDataDB | None:
        """Update match data using provided session (no commit)."""
        self.logger.debug(f"Update match data with session item_id: {item_id}")

        result = await session.execute(select(MatchDataDB).where(MatchDataDB.id == item_id))
        updated_item = result.scalars().one_or_none()

        if not updated_item:
            self.logger.warning(f"No matchdata found with id: {item_id}")
            return None

        update_data = item.model_dump(exclude_unset=True, exclude_none=True)

        for key, value in update_data.items():
            setattr(updated_item, key, value)

        await session.flush()
        await session.refresh(updated_item)
        self.logger.info(f"Matchdata updated with session: {updated_item}")
        return updated_item

    @staticmethod
    def _is_period_transition(
        existing_item: MatchDataDB | None, update_payload: dict[str, Any]
    ) -> bool:
        if existing_item is None:
            return False

        qtr_changed = "qtr" in update_payload and update_payload.get("qtr") != existing_item.qtr
        period_key_changed = (
            "period_key" in update_payload
            and update_payload.get("period_key") != existing_item.period_key
        )
        return qtr_changed or period_key_changed

    async def _get_period_clock_preset_config(
        self,
        match_id: int,
    ) -> tuple[int | None, PeriodClockVariant] | None:
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

    async def _recalculate_gameclock_effective_max(
        self,
        match_id: int,
        period_key: str | None,
        qtr: str | None,
    ) -> None:
        preset_config = await self._get_period_clock_preset_config(match_id)
        if preset_config is None:
            return

        base_max, variant = preset_config
        period_index = extract_period_index(period_key=period_key, qtr=qtr)
        effective_max = calculate_effective_gameclock_max(
            base_max=base_max,
            variant=variant,
            period_index=period_index,
        )

        if effective_max is None:
            return

        gameclock_service = GameClockServiceDB(self.db)
        gameclock = await gameclock_service.get_gameclock_by_match_id(match_id)
        if gameclock is None or not gameclock.use_sport_preset:
            return

        clamped_gameclock = (
            max(0, min(gameclock.gameclock, effective_max))
            if gameclock.gameclock is not None
            else None
        )
        clamped_remaining = (
            max(0, min(gameclock.gameclock_time_remaining, effective_max))
            if gameclock.gameclock_time_remaining is not None
            else None
        )

        if (
            gameclock.gameclock_max == effective_max
            and gameclock.gameclock == clamped_gameclock
            and gameclock.gameclock_time_remaining == clamped_remaining
        ):
            return

        await gameclock_service.update(
            gameclock.id,
            GameClockSchemaUpdate(
                gameclock_max=effective_max,
                gameclock=clamped_gameclock,
                gameclock_time_remaining=clamped_remaining,
            ),
        )

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching by match id", return_value_on_not_found=None
    )
    async def get_match_data_by_match_id(
        self,
        match_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> MatchDataDB | None:
        """Get match data by match ID.

        Args:
            match_id: The match ID to look up.
            session: Optional session for transaction batching.
        """
        self.logger.debug(f"Get {ITEM} by match id: {match_id}")

        if session is not None:
            result = await session.scalars(
                select(MatchDataDB).where(MatchDataDB.match_id == match_id)
            )
            if result:
                self.logger.debug("get_match_data_by_match_id completed successfully.")
                return result.one_or_none()
            else:
                self.logger.debug(f"No matchdata in match with match_id: {match_id}")
                return None

        async with self.db.get_session_maker()() as sess:
            result = await sess.scalars(select(MatchDataDB).where(MatchDataDB.match_id == match_id))
            if result:
                self.logger.debug("get_match_data_by_match_id completed successfully.")
                return result.one_or_none()
            else:
                self.logger.debug(f"No matchdata in match with match_id: {match_id}")
                return None

    async def enable_match_data_clock_queues(self, match_data_id: int, clock_type: str) -> None:
        self.logger.debug(
            f"Enable matchdata clock queues for id: {match_data_id}, type: {clock_type}"
        )
        await self.get_by_id(match_data_id)

    async def decrement_gameclock(
        self, background_tasks: BackgroundTasks, match_data_id: int
    ) -> None:
        self.logger.debug(f"Decrement gameclock for matchdata id: {match_data_id}")

    async def decrement_playclock(
        self, background_tasks: BackgroundTasks, match_data_id: int
    ) -> None:
        self.logger.debug(f"Decrement playclock for matchdata id: {match_data_id}")
