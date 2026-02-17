from __future__ import annotations

from typing import Annotated

from fastapi import Path
from pydantic import BaseModel, ConfigDict, field_validator

from src.core.enums import ClockDirection, ClockOnStopBehavior, ClockStatus
from src.core.schema_helpers import make_fields_optional


class GameClockSchemaBase(BaseModel):
    gameclock: Annotated[int, Path(max=10000)] = 720
    gameclock_max: int | None = 720
    direction: Annotated[ClockDirection, Path(max_length=10)] = ClockDirection.DOWN
    on_stop_behavior: Annotated[ClockOnStopBehavior, Path(max_length=10)] = ClockOnStopBehavior.HOLD
    gameclock_status: Annotated[ClockStatus, Path(max_length=50)] = ClockStatus.STOPPED
    gameclock_time_remaining: int | None = None
    match_id: int | None = None
    version: Annotated[int, Path(ge=1)] = 1
    started_at_ms: int | None = None
    use_sport_preset: bool = True

    @field_validator("direction", mode="before")
    @classmethod
    def parse_direction(cls, v):
        if isinstance(v, str):
            return ClockDirection(v)
        return v

    @field_validator("on_stop_behavior", mode="before")
    @classmethod
    def parse_on_stop_behavior(cls, v):
        if isinstance(v, str):
            return ClockOnStopBehavior(v)
        return v

    @field_validator("gameclock_status", mode="before")
    @classmethod
    def parse_gameclock_status(cls, v):
        if isinstance(v, str):
            return ClockStatus(v)
        return v


# WebSocket Message Format for gameclock-update:
# {
#   "type": "gameclock-update",
#   "match_id": int,
#   "gameclock": {
#     "id": int,
#     "match_id": int,
#     "version": int,
#     "gameclock": int,
#     "gameclock_max": int | None,
#     "gameclock_status": str,
#     "gameclock_time_remaining": int | None
#   }
# }


GameClockSchemaUpdate = make_fields_optional(GameClockSchemaBase)


class GameClockSchemaCreate(GameClockSchemaBase):
    pass


class GameClockSchema(GameClockSchemaCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    server_time_ms: int | None = None
