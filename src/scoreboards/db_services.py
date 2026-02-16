import asyncio

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.models import (
    BaseServiceDB,
    MatchDataDB,
    MatchDB,
    ScoreboardDB,
    SportDB,
    SportScoreboardPresetDB,
    TournamentDB,
    handle_service_exceptions,
)
from src.core.models.base import Database

from ..logging_config import get_logger
from .schemas import ScoreboardSchemaCreate, ScoreboardSchemaUpdate

ITEM = "SCOREBOARD"


class ScoreboardUpdateManager:
    def __init__(self) -> None:
        self.active_scoreboard_updates: dict[int, asyncio.Queue] = {}
        self.logger = get_logger("ScoreboardUpdateManager", self)
        self.logger.debug("Initialized ScoreboardUpdateManager")

    async def enable_scoreboard_update_queue(self, scoreboard_id: int) -> None:
        if scoreboard_id not in self.active_scoreboard_updates:
            self.logger.debug(f"Queue not found for Scoreboard ID:{scoreboard_id}")
            self.active_scoreboard_updates[scoreboard_id] = asyncio.Queue()
            self.logger.debug(f"Queue added for Scoreboard ID:{scoreboard_id}")

    async def update_queue_scoreboard(
        self, scoreboard_id: int, updated_scoreboard: ScoreboardDB
    ) -> None:
        if scoreboard_id in self.active_scoreboard_updates:
            scoreboard_update_queue = self.active_scoreboard_updates[scoreboard_id]
            self.logger.debug(f"Queue updated for Scoreboard ID:{scoreboard_id}")
            await scoreboard_update_queue.put(updated_scoreboard)


class ScoreboardServiceDB(BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(database, ScoreboardDB)
        self.scoreboard_update_manager = ScoreboardUpdateManager()
        self.logger = get_logger("ScoreboardServiceDB", self)
        self.logger.debug("Initialized ScoreboardServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(
        self,
        item: ScoreboardSchemaCreate,
        *,
        session: AsyncSession | None = None,
    ) -> ScoreboardDB:
        """Create a scoreboard.

        Args:
            item: The scoreboard data to create.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Create scoreboard: {item}")
        if item.match_id is not None:
            supports_playclock, supports_timeouts = await self._get_match_feature_capabilities(
                item.match_id, session=session
            )
            item = self._apply_capability_guards_on_create(
                item,
                supports_playclock=supports_playclock,
                supports_timeouts=supports_timeouts,
            )

        if item.match_id is not None:
            is_exist = await self._get_scoreboard_by_match_id_with_session(
                item.match_id, session=session
            )
            if is_exist:
                self.logger.info(f"Scoreboard already exists: {is_exist}")
                return is_exist

        if session is not None:
            return await self._create_with_session(item, session)
        result = await super().create(item)
        return result  # type: ignore

    async def _create_with_session(
        self,
        item: ScoreboardSchemaCreate,
        session: AsyncSession,
    ) -> ScoreboardDB:
        """Create scoreboard using provided session (no commit)."""
        from pydantic import BaseModel

        self.logger.debug(f"Create scoreboard with session: {item}")
        if isinstance(item, BaseModel):
            item_to_add = ScoreboardDB(**item.model_dump())
        else:
            item_to_add = item

        session.add(item_to_add)
        await session.flush()
        await session.refresh(item_to_add)
        self.logger.info(f"Scoreboard created with session: {item_to_add}")
        return item_to_add

    async def _get_scoreboard_by_match_id_with_session(
        self,
        match_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> ScoreboardDB | None:
        """Get scoreboard by match_id, optionally using provided session."""
        if session is not None:
            self.logger.debug(f"Get scoreboard by match id:{match_id} with session")
            result = await session.scalars(
                select(ScoreboardDB).where(ScoreboardDB.match_id == match_id)
            )
            return result.one_or_none()
        return await self.get_scoreboard_by_match_id(match_id)

        #     session.add(match_result)
        #     await session.commit()
        #     await session.refresh(match_result)
        #     return match_result
        # except Exception as ex:
        #     print(ex)
        #     raise HTTPException(
        #         status_code=409,
        #         detail=f"While creating result "
        #         f"for match id({scoreboard})"
        #         f"returned some error",
        #     )

    async def update(
        self,
        item_id: int,
        item: ScoreboardSchemaUpdate,
        *,
        session: AsyncSession | None = None,
        **kwargs,
    ) -> ScoreboardDB:
        """Update a scoreboard.

        Args:
            item_id: The scoreboard ID to update.
            item: The update data.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Update scoreboard id:{item_id} data: {item}")
        match_id = item.match_id
        if match_id is None:
            existing_scoreboard = await self.get_by_id(item_id)
            if existing_scoreboard is not None:
                match_id = existing_scoreboard.match_id

        supports_playclock = True
        supports_timeouts = True
        if match_id is not None:
            supports_playclock, supports_timeouts = await self._get_match_feature_capabilities(
                match_id, session=session
            )
            item = self._apply_capability_guards_on_update(
                item,
                supports_playclock=supports_playclock,
                supports_timeouts=supports_timeouts,
            )

        if session is not None:
            updated_ = await self._update_with_session(item_id, item, session)
        else:
            updated_ = await super().update(
                item_id,
                item,
                **kwargs,
            )
        if updated_ is not None:
            self.logger.debug(f"Updated scoreboard: {updated_}")
            updated_.has_timeouts = supports_timeouts
            updated_.has_playclock = supports_playclock
        # await self.trigger_update_scoreboard(item_id)
        return updated_  # type: ignore

    async def _update_with_session(
        self,
        item_id: int,
        item: ScoreboardSchemaUpdate,
        session: AsyncSession,
    ) -> ScoreboardDB | None:
        """Update scoreboard using provided session (no commit)."""
        self.logger.debug(f"Update scoreboard with session id:{item_id}")

        result = await session.execute(select(ScoreboardDB).where(ScoreboardDB.id == item_id))
        updated_item = result.scalars().one_or_none()

        if not updated_item:
            self.logger.warning(f"No scoreboard found with id: {item_id}")
            return None

        update_data = item.model_dump(exclude_unset=True, exclude_none=True)

        for key, value in update_data.items():
            setattr(updated_item, key, value)

        await session.flush()
        await session.refresh(updated_item)
        self.logger.info(f"Scoreboard updated with session: {updated_item}")
        return updated_item

    async def get_scoreboard_by_match_id(
        self,
        value: int | str,
        field_name: str = "match_id",
    ) -> ScoreboardDB | None:
        self.logger.debug(f"Get scoreboard by {field_name}:{value}")
        return await self.get_item_by_field_value(
            value=value,
            field_name=field_name,
        )

    async def create_or_update_scoreboard(
        self,
        scoreboard: ScoreboardSchemaCreate | ScoreboardSchemaUpdate,
        *,
        session: AsyncSession | None = None,
    ) -> ScoreboardDB:
        """Create or update a scoreboard.

        Args:
            scoreboard: The scoreboard data.
            session: Optional session for transaction batching.
                     If provided, caller is responsible for commit/rollback.
        """
        self.logger.debug(f"Create or update scoreboard: {scoreboard}")
        existing_scoreboard = None
        if scoreboard.match_id is not None:
            existing_scoreboard = await self._get_scoreboard_by_match_id_with_session(
                scoreboard.match_id, session=session
            )

        if existing_scoreboard:
            self.logger.info("Scoreboard already exists")
            if isinstance(scoreboard, ScoreboardSchemaUpdate):
                updated_scoreboard = await self.update(
                    existing_scoreboard.id, scoreboard, session=session
                )
                self.logger.debug("Updated scoreboard")
            else:
                self.logger.warning("Wrong Schema for updating scoreboard")
                raise ValueError("Must use ScoreboardSchemaUpdate for updating.")
            return updated_scoreboard
        else:
            self.logger.debug("Scoreboard does not exist")
            if isinstance(scoreboard, ScoreboardSchemaCreate):
                new_scoreboard = await self.create(scoreboard, session=session)
                self.logger.info("Scoreboard created")
            else:
                self.logger.warning("Wrong Schema for creating scoreboard")
                raise ValueError("Must use ScoreboardSchemaCreate for creating.")
        return new_scoreboard

    async def _get_match_feature_capabilities(
        self,
        match_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> tuple[bool, bool]:
        """Return (supports_playclock, supports_timeouts) for match's sport preset."""
        stmt = (
            select(
                SportScoreboardPresetDB.has_playclock,
                SportScoreboardPresetDB.has_timeouts,
            )
            .select_from(MatchDB)
            .join(TournamentDB, MatchDB.tournament_id == TournamentDB.id)
            .join(SportDB, TournamentDB.sport_id == SportDB.id)
            .join(
                SportScoreboardPresetDB,
                SportDB.scoreboard_preset_id == SportScoreboardPresetDB.id,
                isouter=True,
            )
            .where(MatchDB.id == match_id)
        )

        if session is not None:
            row = (await session.execute(stmt)).one_or_none()
        else:
            async with self.db.get_session_maker()() as sess:
                row = (await sess.execute(stmt)).one_or_none()

        if row is None:
            return True, True

        has_playclock, has_timeouts = row
        supports_playclock = True if has_playclock is None else bool(has_playclock)
        supports_timeouts = True if has_timeouts is None else bool(has_timeouts)
        return supports_playclock, supports_timeouts

    def _apply_capability_guards_on_create(
        self,
        item: ScoreboardSchemaCreate,
        *,
        supports_playclock: bool,
        supports_timeouts: bool,
    ) -> ScoreboardSchemaCreate:
        payload = item.model_dump()

        if not supports_playclock:
            payload["is_playclock"] = False

        if not supports_timeouts:
            payload["is_timeout_team_a"] = False
            payload["is_timeout_team_b"] = False

        return ScoreboardSchemaCreate(**payload)

    def _apply_capability_guards_on_update(
        self,
        item: ScoreboardSchemaUpdate,
        *,
        supports_playclock: bool,
        supports_timeouts: bool,
    ) -> ScoreboardSchemaUpdate:
        payload = item.model_dump(exclude_unset=True)

        if not supports_playclock:
            payload["is_playclock"] = False

        if not supports_timeouts:
            payload["is_timeout_team_a"] = False
            payload["is_timeout_team_b"] = False

        return ScoreboardSchemaUpdate(**payload)

    @handle_service_exceptions(
        item_name=ITEM, operation="fetching by matchdata ID", reraise_not_found=True
    )
    async def get_scoreboard_by_matchdata_id(
        self,
        matchdata_id: int,
    ) -> ScoreboardDB:
        async with self.db.get_session_maker()() as session:
            self.logger.debug(f"Get scoreboard by matchdata id: {matchdata_id}")
            query = select(MatchDataDB).where(MatchDataDB.id == matchdata_id)
            result = await session.execute(query)
            matchdata = result.scalars().one_or_none()

            if matchdata is None:
                self.logger.warning(f"No matchdata found for id: {matchdata_id}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Match data with id {matchdata_id} not found",
                )

            self.logger.debug(f"Select scoreboard by matchdata id: {matchdata_id}")
            scoreboard_query = select(ScoreboardDB).where(
                ScoreboardDB.match_id == matchdata.match_id
            )
            scoreboard_result = await session.execute(scoreboard_query)
            scoreboard = scoreboard_result.scalars().one_or_none()

            if scoreboard is None:
                self.logger.warning(f"No scoreboard found for matchdata id: {matchdata_id}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Scoreboard with match_id {matchdata.match_id} not found",
                )
            return scoreboard

    """triggers for sse process, now we use websocket"""
    # async def event_generator_get_scoreboard_data(self, scoreboard_id: int):
    #     await self.scoreboard_update_manager.enable_scoreboard_update_queue(
    #         scoreboard_id
    #     )
    #     try:
    #         while (
    #             scoreboard_id
    #             in self.scoreboard_update_manager.active_scoreboard_updates
    #         ):
    #             print(f"Scoreboard {scoreboard_id} is active for updates")
    #
    #             update = await self.scoreboard_update_manager.active_scoreboard_updates[
    #                 scoreboard_id
    #             ].get()
    #
    #             update_data = self.to_dict(update)
    #
    #             data = {
    #                 "type": "scoreboardData",
    #                 "scoreboard_data": update_data,
    #             }
    #
    #             json_data = json.dumps(
    #                 data,
    #                 default=self.default_serializer,
    #             )
    #             yield f"data: {json_data}\n\n"
    #
    #         print(f"Scoreboard {scoreboard_id} stopped updates")
    #     except asyncio.CancelledError:
    #         pass
    #
    # async def trigger_update_scoreboard(self, scoreboard_id):
    #     # Fetch the latest store of the scoreboard
    #     scoreboard = await self.get_by_id(scoreboard_id)
    #
    #     # Ensure that the queue for this scoreboard is available
    #     if (
    #         scoreboard_id
    #         not in self.scoreboard_update_manager.active_scoreboard_updates
    #     ):
    #         print(f"Queue not found for Scoreboard ID:{scoreboard_id}")
    #         await self.scoreboard_update_manager.enable_scoreboard_update_queue(
    #             scoreboard_id
    #         )
    #     print("scoreboard triggered")
    #     # Trigger an update by adding the current scoreboard store to the queue
    #     await self.scoreboard_update_manager.update_queue_scoreboard(
    #         scoreboard_id,
    #         scoreboard,
    #     )
