from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.models import BaseServiceDB, PositionDB
from src.core.models.base import Database

from ..logging_config import get_logger
from .schemas import PositionSchemaCreate, PositionSchemaUpdate

ITEM = "POSITION"


class PositionServiceDB(BaseServiceDB):
    def __init__(
        self,
        database: Database,
    ) -> None:
        super().__init__(database, PositionDB)
        self.logger = get_logger("PositionServiceDB", self)
        self.logger.debug("Initialized PositionServiceDB")

    async def create(
        self,
        item: PositionSchemaCreate,
        *,
        session: AsyncSession | None = None,
    ) -> PositionDB:
        self.logger.debug(f"Creating new {ITEM} {item}")
        item_dict = item.model_dump()
        item_dict["title"] = item.title.upper()
        if session is not None:
            return await self._create_with_session(self.model(**item_dict), session)
        return await super().create(self.model(**item_dict))

    async def _create_with_session(
        self,
        item: PositionDB,
        session: AsyncSession,
    ) -> PositionDB:
        session.add(item)
        await session.flush()
        await session.refresh(item)
        return item

    async def update(
        self,
        item_id: int,
        item: PositionSchemaUpdate,
        **kwargs,
    ) -> PositionDB:
        self.logger.debug(f"Update {ITEM}:{item_id}")
        return await super().update(
            item_id,
            item,
            **kwargs,
        )

    async def get_position_by_title(
        self,
        title: str,
        *,
        session: AsyncSession | None = None,
    ) -> PositionDB | None:
        if session is not None:
            return await self._get_position_by_title_with_session(title, session)
        async with self.db.get_session_maker()() as local_session:
            return await self._get_position_by_title_with_session(title, local_session)

    async def _get_position_by_title_with_session(
        self,
        title: str,
        session: AsyncSession,
    ) -> PositionDB | None:
        self.logger.debug(f"Getting position by title: {title}")
        stmt = select(PositionDB).where(
            func.lower(func.trim(PositionDB.title)) == title.lower().strip()
        )
        results = await session.execute(stmt)
        return results.scalars().one_or_none()

    async def get_by_id(
        self,
        item_id: int,
        *,
        session: AsyncSession | None = None,
    ) -> PositionDB | None:
        if session is not None:
            return await self._get_by_id_with_session(item_id, session)
        return await super().get_by_id(item_id)

    async def _get_by_id_with_session(
        self,
        item_id: int,
        session: AsyncSession,
    ) -> PositionDB | None:
        self.logger.debug(f"Getting position by id: {item_id}")
        result = await session.execute(select(PositionDB).where(PositionDB.id == item_id))
        return result.scalars().one_or_none()
