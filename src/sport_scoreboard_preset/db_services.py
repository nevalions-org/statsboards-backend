from typing import Any, cast

from sqlalchemy import select, update

from src.core.decorators import handle_service_exceptions
from src.core.enums import ClockStatus, PeriodClockVariant
from src.core.models import (
    BaseServiceDB,
    GameClockDB,
    MatchDataDB,
    MatchDB,
    PlayClockDB,
    ScoreboardDB,
    SportDB,
    SportScoreboardPresetDB,
    TournamentDB,
)
from src.core.models.base import Database
from src.core.period_clock import calculate_effective_gameclock_max, extract_period_index
from src.core.service_registry import ServiceRegistryAccessorMixin

from ..logging_config import get_logger
from .schemas import SportScoreboardPresetSchemaCreate

ITEM = "SPORT_SCOREBOARD_PRESET"


def _invalidate_preset_cache(preset_id: int | None = None) -> None:
    from src.matches.match_data_cache_service import MatchDataCacheService

    MatchDataCacheService.invalidate_sport_scoreboard_preset_cache_all(preset_id)


class SportScoreboardPresetServiceDB(ServiceRegistryAccessorMixin, BaseServiceDB):
    def __init__(self, database: Database) -> None:
        super().__init__(
            database,
            model=SportScoreboardPresetDB,
        )
        self.logger = get_logger("SportScoreboardPresetServiceDB", self)
        self.logger.debug("Initialized SportScoreboardPresetServiceDB")

    @handle_service_exceptions(item_name=ITEM, operation="creating")
    async def create(self, item: SportScoreboardPresetSchemaCreate) -> SportScoreboardPresetDB:
        self.logger.debug(f"Create {ITEM}:{item}")
        created = cast(SportScoreboardPresetDB, await super().create(item))
        _invalidate_preset_cache(created.id)
        return created

    @handle_service_exceptions(item_name=ITEM, operation="updating", reraise_not_found=True)
    async def update(
        self,
        item_id: int,
        item: Any,
        **kwargs,
    ) -> SportScoreboardPresetDB:
        self.logger.debug(f"Update {ITEM} with id:{item_id}")

        updated_preset = cast(
            SportScoreboardPresetDB,
            await super().update(
                item_id,
                item,
                **kwargs,
            ),
        )

        await self._propagate_preset_to_matches(updated_preset)
        _invalidate_preset_cache(item_id)

        return updated_preset

    async def delete(self, item_id: int) -> dict[str, int]:
        deleted = cast(dict[str, int], await super().delete(item_id))
        _invalidate_preset_cache(item_id)
        return deleted

    async def _propagate_preset_to_matches(
        self,
        preset: SportScoreboardPresetDB,
    ) -> None:
        self.logger.debug(f"Propagating preset {preset.id} to opted-in matches")
        supports_playclock = True if preset.has_playclock is None else bool(preset.has_playclock)
        supports_timeouts = True if preset.has_timeouts is None else bool(preset.has_timeouts)

        async with self.db.get_session_maker()() as session:
            try:
                match_ids_subquery = (
                    select(MatchDB.id)
                    .join(TournamentDB, MatchDB.tournament_id == TournamentDB.id)
                    .where(TournamentDB.sport_id == SportDB.id)
                    .where(SportDB.scoreboard_preset_id == preset.id)
                )

                scoreboard_values = {
                    "is_qtr": preset.is_qtr,
                    "period_mode": preset.period_mode,
                    "period_count": preset.period_count,
                    "period_labels_json": preset.period_labels_json,
                    "is_time": preset.is_time,
                    "is_playclock": preset.is_playclock if supports_playclock else False,
                    "is_downdistance": preset.is_downdistance,
                }
                if not supports_timeouts:
                    scoreboard_values["is_timeout_team_a"] = False
                    scoreboard_values["is_timeout_team_b"] = False

                await session.execute(
                    update(ScoreboardDB)
                    .where(ScoreboardDB.match_id.in_(match_ids_subquery))
                    .where(ScoreboardDB.use_sport_preset.is_(True))
                    .values(**scoreboard_values)
                )

                await session.execute(
                    update(GameClockDB)
                    .where(GameClockDB.match_id.in_(match_ids_subquery))
                    .where(GameClockDB.use_sport_preset.is_(True))
                    .values(
                        gameclock_max=preset.gameclock_max,
                        direction=preset.direction,
                        on_stop_behavior=preset.on_stop_behavior,
                    )
                )

                if (
                    preset.period_clock_variant == PeriodClockVariant.CUMULATIVE
                    and preset.gameclock_max is not None
                ):
                    gameclock_rows = await session.execute(
                        select(
                            GameClockDB.id,
                            GameClockDB.gameclock,
                            GameClockDB.gameclock_time_remaining,
                            MatchDataDB.period_key,
                            MatchDataDB.qtr,
                        )
                        .select_from(GameClockDB)
                        .outerjoin(MatchDataDB, MatchDataDB.match_id == GameClockDB.match_id)
                        .where(GameClockDB.match_id.in_(match_ids_subquery))
                        .where(GameClockDB.use_sport_preset.is_(True))
                    )

                    for row in gameclock_rows:
                        period_index = extract_period_index(
                            period_key=row.period_key,
                            qtr=row.qtr,
                        )
                        effective_max = calculate_effective_gameclock_max(
                            base_max=preset.gameclock_max,
                            variant=preset.period_clock_variant,
                            period_index=period_index,
                        )

                        clamped_gameclock = (
                            max(0, min(row.gameclock, effective_max))
                            if row.gameclock is not None and effective_max is not None
                            else row.gameclock
                        )
                        clamped_remaining = (
                            max(0, min(row.gameclock_time_remaining, effective_max))
                            if row.gameclock_time_remaining is not None
                            and effective_max is not None
                            else row.gameclock_time_remaining
                        )

                        await session.execute(
                            update(GameClockDB)
                            .where(GameClockDB.id == row.id)
                            .values(
                                gameclock_max=effective_max,
                                gameclock=clamped_gameclock,
                                gameclock_time_remaining=clamped_remaining,
                            )
                        )

                if not supports_playclock:
                    opted_in_scoreboard_match_ids_subquery = (
                        select(ScoreboardDB.match_id)
                        .where(ScoreboardDB.match_id.in_(match_ids_subquery))
                        .where(ScoreboardDB.use_sport_preset.is_(True))
                    )
                    await session.execute(
                        update(PlayClockDB)
                        .where(PlayClockDB.match_id.in_(opted_in_scoreboard_match_ids_subquery))
                        .values(
                            playclock=None,
                            playclock_status=ClockStatus.STOPPED,
                            started_at_ms=None,
                            version=PlayClockDB.version + 1,
                        )
                    )

                await session.flush()
                is_test_mode = hasattr(self.db, "test_mode") and self.db.test_mode
                if not is_test_mode:
                    await session.commit()

                self.logger.info(f"Propagated preset {preset.id} to opted-in matches")
            except Exception as e:
                await session.rollback()
                self.logger.error(f"Error propagating preset {preset.id}: {e}", exc_info=True)
                raise

    async def get_sports_by_preset(
        self,
        preset_id: int,
        key: str = "id",
    ) -> list[SportDB]:
        self.logger.debug(f"Get sports by {ITEM} id:{preset_id}")
        async with self.db.get_session_maker()() as session:
            stmt = (
                select(SportDB)
                .where(SportDB.scoreboard_preset_id == preset_id)
                .order_by(SportDB.title)
            )
            results = await session.execute(stmt)
            return results.scalars().all()
