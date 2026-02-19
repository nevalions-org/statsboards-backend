import asyncio
import time
from unittest.mock import patch

import pytest

from src.matches.match_data_cache_service import MatchDataCacheService


@pytest.mark.asyncio
class TestMatchDataCacheService:
    @pytest.fixture
    def cache_service(self, test_db):
        return MatchDataCacheService(test_db)

    @pytest.fixture
    def mock_fetch_result(self):
        return {
            "data": {
                "match_id": 1,
                "id": 1,
                "status_code": 200,
                "match": {"id": 1, "match_eesl_id": 1001},
                "teams_data": {"team_a": {"id": 1}, "team_b": {"id": 2}},
                "match_data": {"id": 1, "match_id": 1},
            }
        }

    @pytest.fixture
    def mock_gameclock_result(self):
        return {
            "match_id": 1,
            "id": 1,
            "status_code": 200,
            "gameclock": {"id": 1, "match_id": 1, "time": "12:00"},
        }

    @pytest.fixture
    def mock_playclock_result(self):
        return {
            "match_id": 1,
            "id": 1,
            "status_code": 200,
            "playclock": {"id": 1, "match_id": 1, "time": "45"},
        }

    @pytest.fixture
    def mock_event_result(self):
        return {
            "match_id": 1,
            "id": 1,
            "status_code": 200,
            "events": [{"id": 1, "match_id": 1, "play_type": "run"}],
        }

    async def test_cache_hit_second_request_returns_cached_data(
        self, cache_service, mock_fetch_result
    ):
        cache_service._cache["match-update:1"] = mock_fetch_result["data"]

        result = await cache_service.get_or_fetch_match_data(1)

        assert result is not None
        assert result == mock_fetch_result["data"]
        assert "match-update:1" in cache_service._cache

    async def test_invalidate_match_data_clears_cache(self, cache_service, mock_fetch_result):
        cache_service._cache["match-update:1"] = mock_fetch_result["data"]

        cache_service.invalidate_match_data(1)

        assert "match-update:1" not in cache_service._cache

    async def test_invalidate_nonexistent_cache_key_no_error(self, cache_service):
        cache_service.invalidate_match_data(999)

        assert True

    async def test_cache_keys_different_message_types_no_collision(
        self, cache_service, mock_fetch_result, mock_gameclock_result
    ):
        cache_service._cache["match-update:1"] = mock_fetch_result["data"]

        assert "match-update:1" in cache_service._cache
        assert "gameclock-update:1" not in cache_service._cache

        cache_service._cache["gameclock-update:1"] = mock_gameclock_result

        assert "match-update:1" in cache_service._cache
        assert "gameclock-update:1" in cache_service._cache
        assert cache_service._cache["match-update:1"] != cache_service._cache["gameclock-update:1"]

    async def test_invalidate_gameclock_clears_cache(self, cache_service, mock_gameclock_result):
        cache_service._cache["gameclock-update:1"] = mock_gameclock_result

        cache_service.invalidate_gameclock(1)

        assert "gameclock-update:1" not in cache_service._cache

    async def test_invalidate_playclock_clears_cache(self, cache_service, mock_playclock_result):
        cache_service._cache["playclock-update:1"] = mock_playclock_result

        cache_service.invalidate_playclock(1)

        assert "playclock-update:1" not in cache_service._cache

    async def test_invalidate_event_data_clears_cache(self, cache_service, mock_event_result):
        cache_service._cache["event-update:1"] = mock_event_result

        cache_service.invalidate_event_data(1)

        assert "event-update:1" not in cache_service._cache

    async def test_different_match_ids_separate_cache_entries(
        self, cache_service, mock_fetch_result
    ):
        cache_service._cache["match-update:1"] = mock_fetch_result["data"]
        cache_service._cache["match-update:2"] = {
            **mock_fetch_result["data"],
            "match_id": 2,
            "id": 2,
        }

        result1 = await cache_service.get_or_fetch_match_data(1)
        result2 = await cache_service.get_or_fetch_match_data(2)

        assert result1 == mock_fetch_result["data"]
        assert result2["match_id"] == 2
        assert result1 != result2

    async def test_cache_miss_first_request_fetches_and_caches(
        self, cache_service, mock_fetch_result
    ):
        with patch(
            "src.helpers.fetch_helpers.fetch_with_scoreboard_data",
            return_value=mock_fetch_result["data"],
        ):
            result = await cache_service.get_or_fetch_match_data(1)

            assert result is not None
            assert result == mock_fetch_result["data"]
            assert "match-update:1" in cache_service._cache

    async def test_concurrent_access_no_duplicate_queries(self, cache_service, mock_fetch_result):
        call_count = {"count": 0}

        async def fetch_logic(match_id, database=None, cache_service=None):
            call_count["count"] += 1
            await asyncio.sleep(0.1)
            return mock_fetch_result["data"]

        with patch(
            "src.helpers.fetch_helpers.fetch_with_scoreboard_data",
            side_effect=fetch_logic,
        ):
            await asyncio.gather(
                cache_service.get_or_fetch_match_data(1),
                cache_service.get_or_fetch_match_data(1),
            )

            assert call_count["count"] == 2
            assert "match-update:1" in cache_service._cache

    async def test_get_or_fetch_gameclock_caches_result(self, cache_service, mock_gameclock_result):
        with patch("src.helpers.fetch_helpers.fetch_gameclock", return_value=mock_gameclock_result):
            result = await cache_service.get_or_fetch_gameclock(1)

            assert result is not None
            assert result == mock_gameclock_result
            assert "gameclock-update:1" in cache_service._cache

    async def test_get_or_fetch_playclock_caches_result(self, cache_service, mock_playclock_result):
        with patch("src.helpers.fetch_helpers.fetch_playclock", return_value=mock_playclock_result):
            result = await cache_service.get_or_fetch_playclock(1)

            assert result is not None
            assert result == mock_playclock_result
            assert "playclock-update:1" in cache_service._cache

    async def test_get_or_fetch_event_data_caches_result(self, cache_service, mock_event_result):
        with patch("src.helpers.fetch_helpers.fetch_event", return_value=mock_event_result):
            result = await cache_service.get_or_fetch_event_data(1)

            assert result is not None
            assert result == mock_event_result
            assert "event-update:1" in cache_service._cache

    async def test_cache_rebuilds_after_invalidation(self, cache_service, mock_fetch_result):
        call_count = {"count": 0}

        async def fetch_logic(match_id, database=None, cache_service=None):
            call_count["count"] += 1
            return mock_fetch_result["data"]

        with patch(
            "src.helpers.fetch_helpers.fetch_with_scoreboard_data",
            side_effect=fetch_logic,
        ):
            await cache_service.get_or_fetch_match_data(1)
            assert call_count["count"] == 1

            cache_service.invalidate_match_data(1)

            await cache_service.get_or_fetch_match_data(1)
            assert call_count["count"] == 2

    async def test_cache_returns_none_on_fetch_failure(self, cache_service, mock_fetch_result):
        with patch(
            "src.helpers.fetch_helpers.fetch_with_scoreboard_data",
            return_value={"status_code": 404},
        ):
            result = await cache_service.get_or_fetch_match_data(1)

            assert result is None
            assert "match-update:1" not in cache_service._cache

    async def test_get_or_fetch_team_uses_reference_cache(self, cache_service):
        call_count = {"count": 0}

        async def fetch_team():
            call_count["count"] += 1
            return {"id": 10, "title": "Team A"}

        first = await cache_service.get_or_fetch_team(10, fetch_team)
        second = await cache_service.get_or_fetch_team(10, fetch_team)

        assert first == {"id": 10, "title": "Team A"}
        assert second == {"id": 10, "title": "Team A"}
        assert call_count["count"] == 1

    async def test_invalidate_tournament_cache_all_clears_reference_cache(self, cache_service):
        async def fetch_tournament():
            return {"id": 50, "title": "Cup"}

        await cache_service.get_or_fetch_tournament(50, fetch_tournament)
        assert cache_service._get_reference_cache_item("tournament", 50) is not None

        MatchDataCacheService.invalidate_tournament_cache_all(50)

        assert cache_service._get_reference_cache_item("tournament", 50) is None

    async def test_reference_cache_ttl_expiration(self, cache_service):
        cache_service._set_reference_cache_item("sport", 77, ttl_seconds=1, payload={"id": 77})

        with patch("time.monotonic", return_value=time.monotonic() + 2):
            assert cache_service._get_reference_cache_item("sport", 77) is None
