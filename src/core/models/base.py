import os
from typing import Any, Callable

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)

from src.core.config import settings
from src.core.models.mixins import (
    CRUDMixin,
    QueryMixin,
    RelationshipMixin,
    SearchPaginationMixin,
    SerializationMixin,
)
from src.logging_config import get_logger

db_logger_helper = get_logger("db")

# Default pool settings - can be overridden via environment variables
# Production defaults: 8 base + 12 overflow = 20 max per pod
# Test defaults: 3 base + 5 overflow = 8 max
DEFAULT_POOL_SIZE = 8
DEFAULT_MAX_OVERFLOW = 12
DEFAULT_TEST_POOL_SIZE = 3
DEFAULT_TEST_MAX_OVERFLOW = 5


class Database:
    def __init__(self, db_url: str, echo: bool = False, test_mode: bool = False):
        self.logger = get_logger("db", self)
        self.logger.info(f"Initializing Database with URL: {db_url}, Echo: {echo}")
        self.test_mode = test_mode
        self.test_async_session: Any | None = None

        try:
            is_test = "test" in db_url
            pool_size = int(
                os.getenv(
                    "DB_POOL_SIZE",
                    str(DEFAULT_TEST_POOL_SIZE if is_test else DEFAULT_POOL_SIZE),
                )
            )
            max_overflow = int(
                os.getenv(
                    "DB_POOL_MAX_OVERFLOW",
                    str(DEFAULT_TEST_MAX_OVERFLOW if is_test else DEFAULT_MAX_OVERFLOW),
                )
            )
            self.logger.info(
                f"Connection pool settings: pool_size={pool_size}, max_overflow={max_overflow}"
            )
            self.engine: AsyncEngine = create_async_engine(
                url=db_url,
                echo=echo,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
            )
            self.async_session: Any = async_sessionmaker(
                bind=self.engine, class_=AsyncSession, expire_on_commit=False
            )
        except SQLAlchemyError as e:
            self.logger.error(f"Error initializing Database engine: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Unexpected error initializing Database: {e}", exc_info=True)

    def get_session_maker(self) -> Any:
        """Get appropriate session maker for current context.

        Returns test_async_session if available (for test transaction isolation),
        otherwise returns default async_session.
        """
        if self.test_async_session is not None:
            return self.test_async_session

        if hasattr(self, "async_session"):
            return self.async_session

        raise AttributeError(
            "Database object has neither test_async_session nor async_session. "
            "Database may not be properly initialized."
        )

    async def test_connection(self, test_query: str = "SELECT 1"):
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text(test_query))
                self.logger.info("Database connection successful.")
        except SQLAlchemyError as e:
            self.logger.error(f"SQLAlchemy error during connection test: {e}", exc_info=True)
            raise
        except OSError as e:
            self.logger.critical(f"OS error during connection test: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.critical(
                f"Unexpected error during database connection test: {e}", exc_info=True
            )
            raise

    async def validate_database_connection(self) -> None:
        """
        Validate database connectivity on startup.
        Performs comprehensive checks including connection test and basic query execution.
        """
        self.logger.info("Starting database connection validation")

        try:
            await self.test_connection()

            async with self.engine.connect() as connection:
                result = await connection.execute(text("SELECT version()"))
                version = result.scalar()
                self.logger.info(f"Connected to PostgreSQL version: {version}")

                result = await connection.execute(text("SELECT current_database()"))
                db_name = result.scalar()
                self.logger.info(f"Using database: {db_name}")

                result = await connection.execute(text("SELECT current_user"))
                user = result.scalar()
                self.logger.info(f"Connected as user: {user}")

            self.logger.info("Database connection validation complete")
        except Exception as e:
            self.logger.critical(f"Database connection validation failed: {e}", exc_info=True)
            raise

    def get_pool_status(self) -> dict[str, Any]:
        pool = self.engine.pool
        status = {}

        try:
            status["pool_size"] = pool.size()  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass

        try:
            status["checked_in"] = pool.checkedin()  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass

        try:
            status["checked_out"] = pool.checkedout()  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass

        try:
            status["overflow"] = pool.overflow()  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass

        return status

    async def close(self):
        self.logger.info(f"Final pool status: {self.get_pool_status()}")
        await self.engine.dispose()
        self.logger.info("Database connection closed.")


db = Database(db_url=str(settings.db.db_url), echo=settings.db_echo)


class BaseServiceDB(
    CRUDMixin,
    QueryMixin,
    RelationshipMixin,
    SearchPaginationMixin,
    SerializationMixin,
):
    def __init__(self, database: Database, model: type):
        self.logger = get_logger("db", self)
        self.db = database
        self.model = model

    async def create_or_update(
        self,
        item_schema,
        eesl_field_name: str | None = None,
        unique_field_name: str | None = None,
        unique_field_value: Any | None = None,
        model_factory: Callable | None = None,
        **create_kwargs,
    ):
        try:
            self.logger.debug(f"Create or update {self.model.__name__}:{item_schema}")

            field_name = eesl_field_name or unique_field_name
            if not field_name:
                raise ValueError("Either eesl_field_name or unique_field_name must be provided")

            field_value = unique_field_value or getattr(item_schema, field_name, None)

            if field_value:
                self.logger.debug(f"Get {self.model.__name__} {field_name}:{field_value}")
                existing_item = await self.get_item_by_field_value(field_value, field_name)

                if existing_item:
                    self.logger.debug(
                        f"{self.model.__name__} {field_name}:{field_value} already exists, updating"
                    )
                    return await self._update_item(
                        existing_item, item_schema, field_name, field_value
                    )
                else:
                    self.logger.debug(f"No {self.model.__name__} in DB, create new")
                    return await self._create_item(item_schema, model_factory, **create_kwargs)
            else:
                self.logger.debug(f"No {field_name} in schema, create new")
                return await self._create_item(item_schema, model_factory, **create_kwargs)
        except Exception as ex:
            self.logger.error(f"{self.model.__name__} returned an error: {ex}", exc_info=True)
            raise HTTPException(
                status_code=409,
                detail=f"{self.model.__name__} ({item_schema}) returned some error",
            ) from ex

    async def _update_item(self, existing_item, item_schema, field_name: str, field_value: Any):
        if field_name.endswith("_eesl_id"):
            return await self.update_item_by_eesl_id(field_name, field_value, item_schema)
        else:
            return await self.update(existing_item.id, item_schema)

    async def _create_item(self, item_schema, model_factory, **create_kwargs):
        if model_factory:
            return await self.create(model_factory(item_schema, **create_kwargs))
        else:
            schema_data = item_schema.model_dump(exclude_unset=True)
            model = self.model(**schema_data)
            return await self.create(model)


class Base(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)
