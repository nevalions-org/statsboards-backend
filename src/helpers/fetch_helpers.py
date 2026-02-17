import asyncio
import datetime
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fastapi import status
from sqlalchemy import select

from src.core import db
from src.core.enums import ClockDirection, ClockOnStopBehavior, InitialTimeMode, PeriodClockVariant
from src.core.models import (
    MatchDataDB,
    SponsorDB,
    SponsorSponsorLineDB,
    SportScoreboardPresetDB,
    TeamDB,
)
from src.core.models.base import Database
from src.core.period_clock import calculate_effective_gameclock_max, extract_period_index
from src.gameclocks.db_services import GameClockServiceDB
from src.gameclocks.schemas import GameClockSchemaCreate
from src.logging_config import get_logger
from src.matchdata.db_services import MatchDataServiceDB
from src.matchdata.schemas import MatchDataSchemaCreate
from src.playclocks.db_services import PlayClockServiceDB
from src.playclocks.schemas import PlayClockSchemaCreate
from src.scoreboards.db_services import ScoreboardServiceDB
from src.scoreboards.schemas import ScoreboardSchemaCreate
from src.tournaments.db_services import TournamentServiceDB

if TYPE_CHECKING:
    from src.matches.db_services import MatchServiceDB

logger = get_logger("helpers")
fetch_data_logger = get_logger("fetch_data")

_db_semaphore = asyncio.Semaphore(10)


@dataclass(slots=True)
class _FetchServices:
    match_service: "MatchServiceDB"
    match_data_service: MatchDataServiceDB
    scoreboard_service: ScoreboardServiceDB


_fetch_services_by_db: dict[int, _FetchServices] = {}


def _get_fetch_services(database: Database) -> _FetchServices:
    cache_key = id(database)
    cached = _fetch_services_by_db.get(cache_key)
    if cached is not None:
        return cached

    from src.matches.db_services import MatchServiceDB

    services = _FetchServices(
        match_service=MatchServiceDB(database),
        match_data_service=MatchDataServiceDB(database),
        scoreboard_service=ScoreboardServiceDB(database),
    )
    _fetch_services_by_db[cache_key] = services
    return services


def _match_payload_dict(match: Any) -> dict[str, Any] | None:
    """Serialize match with a bounded relationship set to avoid recursive graphs."""
    allowed_relationship_keys = {"tournaments", "main_sponsor", "sponsor_line", "team_a", "team_b"}

    payload: dict[str, Any] = {}
    for column in match.__mapper__.columns:
        payload[column.key] = getattr(match, column.key)

    for rel_key in allowed_relationship_keys:
        if rel_key in match.__dict__:
            payload[rel_key] = getattr(match, rel_key)

    return deep_dict_convert(payload)


def _orm_columns_dict(instance: Any) -> dict[str, Any]:
    """Serialize only ORM column values (excludes recursive relationships)."""
    mapper = getattr(instance, "__mapper__", None)
    if mapper is None:
        return {}
    result = {}
    for column in mapper.columns:
        value = getattr(instance, column.key)
        if isinstance(value, datetime.datetime):
            result[column.key] = value.isoformat()
        else:
            result[column.key] = value
    return result


def _serialize_player_payload(player: dict[str, Any]) -> dict[str, Any]:
    position_raw = player.get("position")
    position_payload = deep_dict_convert(position_raw) if isinstance(position_raw, dict) else None

    return {
        "id": player.get("id"),
        "player_id": player.get("player_id"),
        "match_number": player.get("match_number"),
        "is_starting": player.get("is_starting"),
        "starting_type": player.get("starting_type"),
        "position": position_payload,
        "team": _orm_columns_dict(player.get("team")),
        "player": _orm_columns_dict(player.get("player")),
        "person": _orm_columns_dict(player.get("person")),
        "player_team_tournament": _orm_columns_dict(player.get("player_team_tournament")),
    }


DEFAULT_QUICK_SCORE_DELTAS: list[int] = [6, 3, 2, 1, -1]
DEFAULT_SCORE_FORM_GOAL_LABEL = "TD"
DEFAULT_SCORE_FORM_GOAL_EMOJI = "🏈"
DEFAULT_SCOREBOARD_GOAL_TEXT = "TOUCHDOWN"


def _get_preset_values_for_gameclock(
    preset: SportScoreboardPresetDB,
    period_index: int = 1,
) -> dict[str, Any]:
    """Extract preset values for gameclock creation."""
    effective_max = calculate_effective_gameclock_max(
        base_max=preset.gameclock_max,
        variant=getattr(
            preset,
            "period_clock_variant",
            PeriodClockVariant.PER_PERIOD,
        ),
        period_index=period_index,
    )
    initial_gameclock_seconds = _calculate_initial_gameclock_seconds(
        gameclock_max=effective_max,
        initial_time_mode=preset.initial_time_mode,
        initial_time_min_seconds=preset.initial_time_min_seconds,
    )
    return {
        "gameclock": initial_gameclock_seconds,
        "gameclock_time_remaining": initial_gameclock_seconds,
        "gameclock_max": effective_max,
        "direction": preset.direction,
        "on_stop_behavior": preset.on_stop_behavior,
        "use_sport_preset": True,
    }


def _get_preset_values_for_scoreboard(preset: SportScoreboardPresetDB) -> dict[str, Any]:
    """Extract preset values for scoreboard creation."""
    values: dict[str, Any] = {
        "is_qtr": preset.is_qtr,
        "period_mode": preset.period_mode,
        "period_count": preset.period_count,
        "period_labels_json": preset.period_labels_json,
        "is_time": preset.is_time,
        "is_playclock": preset.is_playclock,
        "is_downdistance": preset.is_downdistance,
        "is_timeout_team_a": False,
        "is_timeout_team_b": False,
        "use_sport_preset": True,
    }
    return _enforce_scoreboard_capabilities(values, preset)


def _get_default_gameclock_values() -> dict[str, Any]:
    """Get default gameclock values when no sport preset is available."""
    initial_gameclock_seconds = _calculate_initial_gameclock_seconds(
        gameclock_max=720,
        initial_time_mode=InitialTimeMode.MAX,
        initial_time_min_seconds=None,
    )
    return {
        "gameclock": initial_gameclock_seconds,
        "gameclock_time_remaining": initial_gameclock_seconds,
        "gameclock_max": 720,
        "direction": ClockDirection.DOWN,
        "on_stop_behavior": ClockOnStopBehavior.HOLD,
        "use_sport_preset": True,
    }


def _get_default_scoreboard_values() -> dict[str, Any]:
    """Get default scoreboard values when no sport preset is available."""
    return {
        "is_qtr": True,
        "period_mode": "qtr",
        "period_count": 4,
        "period_labels_json": None,
        "is_time": True,
        "is_playclock": True,
        "is_downdistance": True,
        "is_timeout_team_a": False,
        "is_timeout_team_b": False,
        "use_sport_preset": True,
    }


def _preset_supports_playclock(preset: SportScoreboardPresetDB) -> bool:
    return bool(getattr(preset, "has_playclock", True))


def _preset_supports_timeouts(preset: SportScoreboardPresetDB) -> bool:
    return bool(getattr(preset, "has_timeouts", True))


def _enforce_scoreboard_capabilities(
    values: dict[str, Any], preset: SportScoreboardPresetDB
) -> dict[str, Any]:
    if not _preset_supports_playclock(preset):
        values["is_playclock"] = False

    if not _preset_supports_timeouts(preset):
        values["is_timeout_team_a"] = False
        values["is_timeout_team_b"] = False

    return values


def _preset_quick_score_deltas(preset: SportScoreboardPresetDB | None) -> list[int]:
    if preset is None:
        return DEFAULT_QUICK_SCORE_DELTAS.copy()

    raw_deltas = getattr(preset, "quick_score_deltas", None)
    if not isinstance(raw_deltas, list) or len(raw_deltas) == 0:
        return DEFAULT_QUICK_SCORE_DELTAS.copy()

    if not all(isinstance(value, int) and value != 0 for value in raw_deltas):
        return DEFAULT_QUICK_SCORE_DELTAS.copy()

    return raw_deltas.copy()


def _preset_goal_metadata(preset: SportScoreboardPresetDB | None) -> dict[str, str]:
    if preset is None:
        return {
            "score_form_goal_label": DEFAULT_SCORE_FORM_GOAL_LABEL,
            "score_form_goal_emoji": DEFAULT_SCORE_FORM_GOAL_EMOJI,
            "scoreboard_goal_text": DEFAULT_SCOREBOARD_GOAL_TEXT,
        }

    raw_label = getattr(preset, "score_form_goal_label", None)
    raw_emoji = getattr(preset, "score_form_goal_emoji", None)
    raw_text = getattr(preset, "scoreboard_goal_text", None)

    label = raw_label.strip() if isinstance(raw_label, str) and raw_label.strip() else None
    emoji = raw_emoji.strip() if isinstance(raw_emoji, str) and raw_emoji.strip() else None
    text = raw_text.strip() if isinstance(raw_text, str) and raw_text.strip() else None

    return {
        "score_form_goal_label": label or DEFAULT_SCORE_FORM_GOAL_LABEL,
        "score_form_goal_emoji": emoji or DEFAULT_SCORE_FORM_GOAL_EMOJI,
        "scoreboard_goal_text": text or DEFAULT_SCOREBOARD_GOAL_TEXT,
    }


def _calculate_initial_gameclock_seconds(
    gameclock_max: int | None,
    initial_time_mode: InitialTimeMode,
    initial_time_min_seconds: int | None,
) -> int:
    initial_seconds: int

    if initial_time_mode == InitialTimeMode.ZERO:
        initial_seconds = 0
    elif initial_time_mode == InitialTimeMode.MIN:
        initial_seconds = initial_time_min_seconds or 0
    else:
        initial_seconds = gameclock_max or 0

    if gameclock_max is not None:
        return max(0, min(initial_seconds, gameclock_max))

    return max(0, initial_seconds)


async def _fetch_sponsor_line_sponsors(sponsor_line_id: int | None, database=None) -> list:
    """Fetch sponsors for a sponsor_line with their positions."""
    if not sponsor_line_id:
        return []

    _db = database or db
    async with _db.get_session_maker()() as session:
        result = await session.execute(
            select(SponsorDB, SponsorSponsorLineDB.position)
            .join(
                SponsorSponsorLineDB,
                SponsorDB.id == SponsorSponsorLineDB.sponsor_id,
            )
            .where(SponsorSponsorLineDB.sponsor_line_id == sponsor_line_id)
        )
        sponsors = [
            {
                "sponsor": {
                    "id": r[0].id,
                    "title": r[0].title,
                    "logo_url": r[0].logo_url,
                    "scale_logo": r[0].scale_logo,
                },
                "position": r[1],
            }
            for r in result.all()
        ]
        return sorted(sponsors, key=lambda x: x["position"] or 0)


async def fetch_list_of_matches_data(
    matches: list[Any], database: Database | None = None
) -> list[dict[str, Any]] | None:
    fetch_data_logger.debug("Fetching list of matches data")

    _db = database or db

    all_match_data = []

    try:
        if not matches:
            return []

        match_ids = [match.id for match in matches]

        team_ids = set()
        for match in matches:
            team_ids.add(match.team_a_id)
            team_ids.add(match.team_b_id)

        async with _db.get_session_maker()() as session:
            stmt = select(TeamDB).where(TeamDB.id.in_(list(team_ids)))
            results = await session.execute(stmt)
            teams = {team.id: team for team in results.scalars().all()}

            stmt = select(MatchDataDB).where(MatchDataDB.match_id.in_(match_ids))
            results = await session.execute(stmt)
            match_data_map = {md.match_id: md for md in results.scalars().all()}

            missing_match_ids = [mid for mid in match_ids if mid not in match_data_map]
            for match_id in missing_match_ids:
                match_data_schema = MatchDataSchemaCreate(match_id=match_id)
                new_match_data = MatchDataDB(**match_data_schema.model_dump())
                session.add(new_match_data)
                match_data_map[match_id] = new_match_data

            if missing_match_ids:
                await session.flush()
                for match_id in missing_match_ids:
                    await session.refresh(match_data_map[match_id])

            for match in matches:
                match_id = match.id
                match_data = match_data_map[match_id]
                team_a = teams.get(match.team_a_id)
                team_b = teams.get(match.team_b_id)

                match_teams_data = {"team_a": team_a, "team_b": team_b}

                all_match_data.append(
                    {
                        "match_id": match_id,
                        "id": match_id,
                        "status_code": status.HTTP_200_OK,
                        "match": match,
                        "teams_data": match_teams_data,
                        "match_data": match_data.__dict__,
                    }
                )

        return all_match_data
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching list of matches data: {e}", exc_info=True)


async def fetch_match_data(match_id: int, database=None) -> dict[str, Any] | None:
    from src.matches.db_services import MatchServiceDB

    fetch_data_logger.debug(f"Fetching matchdata by mathc_id:{match_id}")
    try:
        _db = database or db
        match_data_service_db = MatchDataServiceDB(_db)
        match_service_db = MatchServiceDB(_db)

        match, match_teams_data, match_data = await asyncio.gather(
            match_service_db.get_by_id(match_id),
            match_service_db.get_teams_by_match(match_id),
            match_service_db.get_matchdata_by_match(match_id),
        )

        if match:
            if match_data is None:
                match_data_schema = MatchDataSchemaCreate(match_id=match_id)
                match_data = await match_data_service_db.create(match_data_schema)

            return {
                "match_id": match_id,
                "id": match_id,
                "status_code": status.HTTP_200_OK,
                "match": match,
                "teams_data": match_teams_data,
                "match_data": match_data.__dict__,
            }
        else:
            return {
                "status_code": status.HTTP_404_NOT_FOUND,
            }
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching matchdata: {e}", exc_info=True)


async def fetch_with_scoreboard_data(
    match_id: int, database=None, cache_service=None
) -> dict[str, Any] | None:
    if cache_service:
        result = await cache_service.get_or_fetch_match_data(match_id)
        if result:
            return result

    fetch_data_logger.debug(f"Starting fetching match data with match_id {match_id}")
    _db = database or db
    services = _get_fetch_services(_db)
    match_service_db = services.match_service
    match_data_service_db = services.match_data_service
    scoreboard_data_service = services.scoreboard_service

    try:
        async with _db_semaphore:
            bundled_data = await match_service_db.get_scoreboard_fetch_bundle(match_id)

        if bundled_data is None:
            fetch_data_logger.error(f"Match not found for match_id:{match_id}")
            return {
                "status_code": status.HTTP_404_NOT_FOUND,
            }

        match = bundled_data["match"]
        scoreboard_data = bundled_data["scoreboard"]
        match_data = bundled_data["match_data"]
        sport = bundled_data["sport"]
        players = bundled_data["players"]
        events = bundled_data["events"]
        match_teams_data = bundled_data["teams"]

        fetch_data_logger.debug(f"Fetched bundled scoreboard payload data for match_id: {match_id}")

        # Fetch sponsor_line sponsors with positions (for match and tournament)
        match_sponsor_line_id = match.sponsor_line_id if match else None
        tournament_sponsor_line_id = (
            match.tournaments.sponsor_line_id if match and match.tournaments else None
        )
        match_sponsor_line_sponsors, tournament_sponsor_line_sponsors = await asyncio.gather(
            _fetch_sponsor_line_sponsors(match_sponsor_line_id, database=_db),
            _fetch_sponsor_line_sponsors(tournament_sponsor_line_id, database=_db),
        )

        if match:
            if match_data is None:
                fetch_data_logger.debug(
                    f"Match Data not found for match_id:{match_id}, creating new..."
                )
                match_data_schema = MatchDataSchemaCreate(match_id=match_id)
                fetch_data_logger.debug(f"Schema for match data {match_data_schema}")
                match_data = await match_data_service_db.create(match_data_schema)
            if scoreboard_data is None:
                fetch_data_logger.debug(
                    f"Scoreboard Data not found for match_id:{match_id}, creating new..."
                )
                scoreboard_data_schema = ScoreboardSchemaCreate(match_id=match_id)

                if sport and sport.scoreboard_preset:
                    preset_values = _get_preset_values_for_scoreboard(sport.scoreboard_preset)
                    fetch_data_logger.debug(
                        f"Using sport preset {sport.scoreboard_preset.id} for scoreboard"
                    )
                    for key, value in preset_values.items():
                        setattr(scoreboard_data_schema, key, value)
                else:
                    default_values = _get_default_scoreboard_values()
                    fetch_data_logger.debug("Using default scoreboard values")
                    for key, value in default_values.items():
                        setattr(scoreboard_data_schema, key, value)

                fetch_data_logger.debug(f"Schema for scoreboard data {scoreboard_data_schema}")
                scoreboard_data = await scoreboard_data_service.create(scoreboard_data_schema)
                match.match_scoreboard = scoreboard_data

            # Convert match to dict and rename 'tournaments' to 'tournament' for frontend compatibility
            match_dict = _match_payload_dict(match)
            if match_dict and "tournaments" in match_dict:
                match_dict["tournament"] = match_dict.pop("tournaments")

            # Add sponsor_line_sponsors to sponsor_line in match dict
            if match_dict and match_dict.get("sponsor_line"):
                match_dict["sponsor_line"]["sponsors"] = match_sponsor_line_sponsors
            if (
                match_dict
                and match_dict.get("tournament")
                and match_dict["tournament"].get("sponsor_line")
            ):
                match_dict["tournament"]["sponsor_line"]["sponsors"] = (
                    tournament_sponsor_line_sponsors
                )

            def _sponsor_public(sponsor: Any) -> dict[str, Any] | None:
                if not isinstance(sponsor, dict):
                    return None
                return {
                    "id": sponsor.get("id"),
                    "title": sponsor.get("title"),
                    "logo_url": sponsor.get("logo_url"),
                    "scale_logo": sponsor.get("scale_logo"),
                }

            def _sponsor_line_public(sponsor_line: Any) -> dict[str, Any] | None:
                if not isinstance(sponsor_line, dict):
                    return None
                sponsors_raw = sponsor_line.get("sponsors") or []
                sponsors: list[dict[str, Any]] = []
                if isinstance(sponsors_raw, list):
                    for item in sponsors_raw:
                        if not isinstance(item, dict):
                            continue
                        sponsors.append(
                            {
                                "position": item.get("position"),
                                "sponsor": _sponsor_public(item.get("sponsor")),
                            }
                        )
                return {
                    "id": sponsor_line.get("id"),
                    "title": sponsor_line.get("title"),
                    "is_visible": sponsor_line.get("is_visible"),
                    "sponsors": sponsors,
                }

            tournament_dict = match_dict.get("tournament") if isinstance(match_dict, dict) else None
            if not isinstance(tournament_dict, dict):
                tournament_dict = None

            sponsors_data = {
                "match": {
                    "main_sponsor": _sponsor_public(match_dict.get("main_sponsor"))
                    if match_dict
                    else None,
                    "sponsor_line": _sponsor_line_public(match_dict.get("sponsor_line"))
                    if match_dict
                    else None,
                },
                "tournament": {
                    "main_sponsor": _sponsor_public(tournament_dict.get("main_sponsor"))
                    if tournament_dict
                    else None,
                    "sponsor_line": _sponsor_line_public(tournament_dict.get("sponsor_line"))
                    if tournament_dict
                    else None,
                },
            }

            preset = sport.scoreboard_preset if sport else None
            goal_metadata = _preset_goal_metadata(preset)
            final_match_with_scoreboard_data_fetched = {
                "data": {
                    "match_id": match_id,
                    "id": match_id,
                    "status_code": status.HTTP_200_OK,
                    "match": match_dict,
                    "sponsors_data": sponsors_data,
                    "scoreboard_data": {
                        **_orm_columns_dict(scoreboard_data),
                        "has_timeouts": bool(preset.has_timeouts) if preset is not None else True,
                        "has_playclock": bool(preset.has_playclock) if preset is not None else True,
                        "quick_score_deltas": _preset_quick_score_deltas(preset),
                        **goal_metadata,
                    },
                    "teams_data": deep_dict_convert(match_teams_data),
                    "match_data": _orm_columns_dict(match_data),
                    "players": [_serialize_player_payload(player) for player in players]
                    if players
                    else [],
                    "events": [_orm_columns_dict(event) for event in events] if events else [],
                }
            }
            match_id = final_match_with_scoreboard_data_fetched.get("data", {}).get(
                "match_id", "N/A"
            )
            num_players = len(
                final_match_with_scoreboard_data_fetched.get("data", {}).get("players", [])
            )
            num_events = len(
                final_match_with_scoreboard_data_fetched.get("data", {}).get("events", [])
            )
            fetch_data_logger.debug(
                f"Final match with scoreboard fetched for match_id: {match_id}, "
                f"players: {num_players}, events: {num_events}"
            )

            return final_match_with_scoreboard_data_fetched
    except Exception as e:
        fetch_data_logger.error(
            f"Error while fetching matchdata with scoreboard: {e}", exc_info=True
        )
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


async def fetch_gameclock(
    match_id: int, database=None, cache_service=None
) -> dict[str, Any] | None:
    if cache_service:
        result = await cache_service.get_or_fetch_gameclock(match_id)
        if result:
            return result

    from src.matches.db_services import MatchServiceDB

    fetch_data_logger.debug(f"Starting fetching gameclock with match_id:{match_id}")
    _db = database or db
    gameclock_service = GameClockServiceDB(_db)
    match_service_db = MatchServiceDB(_db)

    try:
        match, gameclock, sport, match_data = await asyncio.gather(
            match_service_db.get_by_id(match_id),
            gameclock_service.get_gameclock_by_match_id(match_id),
            match_service_db.get_sport_by_match_id(match_id),
            MatchDataServiceDB(_db).get_match_data_by_match_id(match_id),
        )

        if match:
            if gameclock is None:
                gameclock_schema = GameClockSchemaCreate(match_id=match_id)

                if sport and sport.scoreboard_preset:
                    period_index = extract_period_index(
                        period_key=match_data.period_key if match_data is not None else None,
                        qtr=match_data.qtr if match_data is not None else None,
                    )
                    preset_values = _get_preset_values_for_gameclock(
                        sport.scoreboard_preset,
                        period_index=period_index,
                    )
                    fetch_data_logger.debug(
                        f"Using sport preset {sport.scoreboard_preset.id} for gameclock"
                    )
                    for key, value in preset_values.items():
                        setattr(gameclock_schema, key, value)
                else:
                    default_values = _get_default_gameclock_values()
                    fetch_data_logger.debug("Using default gameclock values")
                    for key, value in default_values.items():
                        setattr(gameclock_schema, key, value)

                gameclock = await gameclock_service.create(gameclock_schema)
            return {
                "match_id": match_id,
                "gameclock": instance_to_dict(dict(gameclock.__dict__)),
                "server_time_ms": int(time.time() * 1000),
            }
        else:
            return {
                "status_code": status.HTTP_404_NOT_FOUND,
            }
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching gameclock: {e}", exc_info=True)
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


async def fetch_playclock(
    match_id: int, database=None, cache_service=None
) -> dict[str, Any] | None:
    if cache_service:
        result = await cache_service.get_or_fetch_playclock(match_id)
        if result:
            return result

    from src.matches.db_services import MatchServiceDB

    fetch_data_logger.debug(f"Starting fetching playclock with match_id:{match_id}")
    _db = database or db
    playclock_service = PlayClockServiceDB(_db)
    match_service_db = MatchServiceDB(_db)
    try:
        match, playclock, sport = await asyncio.gather(
            match_service_db.get_by_id(match_id),
            playclock_service.get_playclock_by_match_id(match_id),
            match_service_db.get_sport_by_match_id(match_id),
        )

        if match:
            preset = sport.scoreboard_preset if sport else None
            supports_playclock = True
            if preset is not None:
                supports_playclock = _preset_supports_playclock(preset)

            if not supports_playclock:
                return {
                    "match_id": match_id,
                    "playclock": None,
                    "is_supported": False,
                    "server_time_ms": int(time.time() * 1000),
                }

            if playclock is None:
                playclock_schema = PlayClockSchemaCreate(match_id=match_id)
                playclock = await playclock_service.create(playclock_schema)
            return {
                "match_id": match_id,
                "playclock": instance_to_dict(dict(playclock.__dict__)),
                "is_supported": True,
                "server_time_ms": int(time.time() * 1000),
            }
        else:
            return {
                "status_code": status.HTTP_404_NOT_FOUND,
            }
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching playclock: {e}", exc_info=True)
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


async def fetch_event(event_id: int, database=None, cache_service=None) -> dict[str, Any] | None:
    if cache_service:
        result = await cache_service.get_or_fetch_event_data(event_id)
        if result:
            return result

    from src.football_events.db_services import FootballEventServiceDB
    from src.matches.db_services import MatchServiceDB

    fetch_data_logger.debug(f"Starting fetching event with event_id:{event_id}")
    _db = database or db
    football_event_service = FootballEventServiceDB(_db)
    match_service_db = MatchServiceDB(_db)

    try:
        match = await match_service_db.get_by_id(event_id)

        if match:
            event_data_list = await football_event_service.get_events_with_players(event_id)
            return {
                "match_id": event_id,
                "id": event_id,
                "status_code": status.HTTP_200_OK,
                "events": event_data_list,
            }
        else:
            return {
                "status_code": status.HTTP_404_NOT_FOUND,
            }
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching event: {e}", exc_info=True)
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


async def fetch_stats(match_id: int, database=None, cache_service=None) -> dict[str, Any] | None:
    if cache_service:
        result = await cache_service.get_or_fetch_stats(match_id)
        if result:
            return result

    from src.matches.stats_service import MatchStatsServiceDB

    fetch_data_logger.debug(f"Starting fetching stats with match_id:{match_id}")
    _db = database or db
    stats_service = MatchStatsServiceDB(_db)

    try:
        stats = await stats_service.get_match_with_cached_stats(match_id)
        return {
            "match_id": match_id,
            "statistics": stats,
            "server_time_ms": int(time.time() * 1000),
        }
    except Exception as e:
        fetch_data_logger.error(f"Error while fetching stats: {e}", exc_info=True)
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


async def fetch_matches_with_data_by_tournament_paginated(
    tournament_id: int,
    skip: int = 0,
    limit: int = 7,
    order_exp: str = "id",
    order_exp_two: str = "id",
    database=None,
) -> Any:
    fetch_data_logger.debug("Fetching list of matches with full data")

    _db = database or db
    tournament_service_db = TournamentServiceDB(_db)

    try:
        # Fetch matches with pagination
        paginated_matches = await tournament_service_db.get_matches_by_tournament_with_pagination(
            tournament_id, skip, limit, order_exp, order_exp_two
        )

        match_ids = [match.id for match in paginated_matches]
        fetch_data_logger.debug(f"Fetched match_ids {match_ids}")

        fetch_data_logger.debug(f"Fetched {len(match_ids)} of matches")

        if len(match_ids) == 0:
            fetch_data_logger.info(
                f"No matches in tournament id:{tournament_id} on page:{skip} with limit:{limit}"
            )
            return []

        # Fetch full match details for all matches in parallel
        full_match_data_list = await asyncio.gather(
            *[fetch_with_scoreboard_data(match_id, database=_db) for match_id in match_ids]
        )

        fetch_data_logger.debug(
            f"Fetched {len(match_ids)} matches with full data for tournament_id: {tournament_id}"
        )

        return full_match_data_list

    except Exception as e:
        fetch_data_logger.error(f"Error fetching tournament matches data: {e}", exc_info=True)
        return {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
        }


def instance_to_dict(instance: dict[str, Any]) -> dict[str, Any] | None:
    try:
        result_dict = {key: value for key, value in instance.items() if not key.startswith("_")}
        return result_dict
    except Exception as e:
        logger.error(f"Error converting instance to dictionary: {e}")
        return None


def deep_dict_convert(obj: dict[str, Any]) -> dict[str, Any] | None:
    try:
        result_dict = {}
        for key, value in obj.items():
            if not key.startswith("_"):
                if isinstance(value, datetime.datetime):  # check against datetime.datetime
                    result_dict[key] = value.isoformat()
                elif isinstance(value, dict):
                    result_dict[key] = deep_dict_convert(value)
                elif isinstance(value, list):
                    result_dict[key] = [
                        deep_dict_convert(item.__dict__)
                        if hasattr(item, "__dict__")
                        else deep_dict_convert(item)
                        if isinstance(item, dict)
                        else item
                        for item in value
                    ]
                elif hasattr(value, "__dict__"):
                    result_dict[key] = deep_dict_convert(value.__dict__)
                else:
                    result_dict[key] = value
        return result_dict
    except Exception as e:
        logger.error(f"Error in deep dictionary convert: {e}")
        return None


async def _fetch_events_by_match(match_id: int, database=None) -> list:
    from src.core.models.football_event import FootballEventDB

    _db = database or db
    fetch_data_logger.debug(f"Fetching events for match_id:{match_id}")
    async with _db.get_session_maker()() as session:
        stmt = select(FootballEventDB).where(FootballEventDB.match_id == match_id)
        result = await session.execute(stmt)
        events = result.scalars().all()
        fetch_data_logger.debug(f"Fetched {len(events)} events for match_id:{match_id}")
        return events
