from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from fastapi import Path
from pydantic import BaseModel, ConfigDict, field_validator

from src.core.enums import SportPeriodMode
from src.core.schema_helpers import make_fields_optional

ScoreboardLanguageCode: TypeAlias = Literal["en", "ru"]


class ScoreboardSchemaBase(BaseModel):
    use_sport_preset: bool = True

    is_qtr: bool = True
    period_mode: Annotated[SportPeriodMode, Path(max_length=10)] = SportPeriodMode.QTR
    period_count: Annotated[int, Path(ge=1, le=99)] = 4
    period_labels_json: list[str] | None = None
    is_time: bool = True
    is_playclock: bool = True
    is_downdistance: bool = True
    is_tournament_logo: bool = True
    is_main_sponsor: bool = True
    is_sponsor_line: bool = True
    is_match_sponsor_line: bool = False

    is_team_a_start_offense: bool = False
    is_team_b_start_offense: bool = False
    is_team_a_start_defense: bool = False
    is_team_b_start_defense: bool = False

    is_home_match_team_lower: bool = False
    is_away_match_team_lower: bool = False

    is_football_qb_full_stats_lower: bool = False
    football_qb_full_stats_match_lower_id: int | None = None
    is_match_player_lower: bool = False
    player_match_lower_id: int | None = None

    team_a_game_color: Annotated[str, Path(max_length=10)] = "#c01c28"
    team_b_game_color: Annotated[str, Path(max_length=10)] = "#1c71d8"
    use_team_a_game_color: bool = False
    use_team_b_game_color: bool = False

    team_a_game_title: Annotated[str, Path(max_length=50)] | None = None
    team_b_game_title: Annotated[str, Path(max_length=50)] | None = None
    use_team_a_game_title: bool = False
    use_team_b_game_title: bool = False

    team_a_game_logo: str | None = None
    team_b_game_logo: str | None = None
    use_team_a_game_logo: bool = False
    use_team_b_game_logo: bool = False

    scale_tournament_logo: float | None = 2.0
    scale_main_sponsor: float | None = 2.0
    scale_logo_a: float | None = 2.0
    scale_logo_b: float | None = 2.0

    is_flag: bool | None = False
    is_goal_team_a: bool | None = False
    is_goal_team_b: bool | None = False
    is_timeout_team_a: bool | None = False
    is_timeout_team_b: bool | None = False

    language_code: ScoreboardLanguageCode | None = "en"

    match_id: int | None = None

    @field_validator("period_mode", mode="before")
    @classmethod
    def parse_period_mode(cls, v):
        if isinstance(v, str):
            return SportPeriodMode(v)
        return v


ScoreboardSchemaUpdate = make_fields_optional(ScoreboardSchemaBase)


class ScoreboardSchemaCreate(ScoreboardSchemaBase):
    pass


class ScoreboardSchema(ScoreboardSchemaCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    has_timeouts: bool = True
    has_playclock: bool = True
