import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from src.core.db_session_manager import db_session_context, with_db_session
from src.core.models import SportDB

pytestmark = pytest.mark.asyncio(loop_scope="session")


class TestDBSessionManager:
    async def test_db_session_context_commit_success(self, test_db, worker_id, request):
        unique_title = f"Football-db-session-context-test-{worker_id}-{request.node.name}"
        async with db_session_context(test_db.get_session_maker()) as session:
            sport = SportDB(title=unique_title)
            session.add(sport)

        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is not None

    async def test_db_session_context_rollback_on_sqlalchemy_error(
        self, test_db, worker_id, request
    ):
        unique_title = f"Football-rollback-test-{worker_id}-{request.node.name}"
        with pytest.raises(SQLAlchemyError):
            async with db_session_context(test_db.get_session_maker()) as session:
                sport = SportDB(title=unique_title)
                session.add(sport)
                await session.flush()
                raise SQLAlchemyError("Simulated database error")

        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is None

    async def test_db_session_context_rollback_on_integrity_error(
        self, test_db, worker_id, request
    ):
        unique_title = f"Football-integrity-test-{worker_id}-{request.node.name}"
        with pytest.raises(IntegrityError):
            async with db_session_context(test_db.get_session_maker()) as session:
                sport = SportDB(title=unique_title)
                session.add(sport)
                await session.flush()
                raise IntegrityError("Simulated integrity error", "orig", Exception())

        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is None

    async def test_db_session_context_rollback_on_generic_exception(
        self, test_db, worker_id, request
    ):
        unique_title = f"Football-generic-test-{worker_id}-{request.node.name}"
        with pytest.raises(ValueError):
            async with db_session_context(test_db.get_session_maker()) as session:
                sport = SportDB(title=unique_title)
                session.add(sport)
                await session.flush()
                raise ValueError("Simulated generic error")

        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is None


class TestWithDBSessionDecorator:
    async def test_with_db_session_decorator_success(self, test_db, worker_id, request):
        class TestService:
            def __init__(self, db):
                self.db = db

            @with_db_session()
            async def create_sport(self, session, title):
                sport = SportDB(title=title)
                session.add(sport)
                return sport

        service = TestService(test_db)
        unique_title = f"Basketball-{worker_id}-{request.node.name}"
        sport = await service.create_sport(unique_title)

        assert sport is not None
        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is not None

    async def test_with_db_session_decorator_rollback_on_error(self, test_db, worker_id, request):
        class TestService:
            def __init__(self, db):
                self.db = db

            @with_db_session()
            async def create_sport_with_error(self, session, title):
                sport = SportDB(title=title)
                session.add(sport)
                await session.flush()
                raise ValueError("Simulated error")

        service = TestService(test_db)
        unique_title = f"Tennis-{worker_id}-{request.node.name}"

        with pytest.raises(ValueError):
            await service.create_sport_with_error(unique_title)

        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is None

    async def test_with_db_session_decorator_with_custom_db_property(
        self, test_db, worker_id, request
    ):
        class TestService:
            def __init__(self, database):
                self.custom_db = database

            @with_db_session("custom_db")
            async def create_sport(self, session, title):
                sport = SportDB(title=title)
                session.add(sport)
                return sport

        service = TestService(test_db)
        unique_title = f"Volleyball-{worker_id}-{request.node.name}"
        sport = await service.create_sport(unique_title)

        assert sport is not None
        async with test_db.get_session_maker()() as session:
            result = await session.execute(select(SportDB).where(SportDB.title == unique_title))
            assert result.scalars().one_or_none() is not None
