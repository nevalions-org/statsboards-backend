"""Tests for WebSocket manager with Redis mode."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.websocket.websocket_manager import MatchDataWebSocketManager


class TestWebSocketManagerRedisMode:
    """Tests for WebSocket manager Redis mode."""

    def test_redis_mode_initialization(self):
        """Test MatchDataWebSocketManager initialization with Redis mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        assert manager.use_redis is True
        assert manager.redis_url == "redis://localhost:6379"

    def test_postgres_mode_initialization(self):
        """Test MatchDataWebSocketManager initialization with PostgreSQL mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=False,
        )

        assert manager.use_redis is False

    @pytest.mark.asyncio
    async def test_connect_to_redis_success(self):
        """Test successful Redis connection."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.register_callback = MagicMock()

        with patch("src.utils.redis_notifier.init_redis_notifier", return_value=mock_notifier):
            await manager.connect_to_redis()

            assert manager.is_connected is True

    @pytest.mark.asyncio
    async def test_startup_with_redis_mode(self):
        """Test startup with Redis mode starts Redis listener."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.register_callback = MagicMock()
        mock_notifier.listen_loop = AsyncMock()

        with patch("src.utils.redis_notifier.init_redis_notifier", return_value=mock_notifier):
            await manager.startup()

            assert manager.is_connected is True
            assert manager._redis_notifier == mock_notifier
            assert manager._redis_listen_task is not None

            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_startup_without_redis_mode(self):
        """Test startup without Redis mode connects to PostgreSQL."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=False,
        )

        with patch("asyncpg.connect") as mock_connect:
            mock_connection = MagicMock()
            mock_connection.add_listener = AsyncMock()
            mock_connect.return_value = mock_connection

            await manager.startup()

            assert manager.is_connected is True
            mock_connect.assert_called_once()

            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_redis_mode(self):
        """Test shutdown with Redis mode cleans up properly."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.register_callback = MagicMock()
        mock_notifier.listen_loop = AsyncMock()
        mock_notifier.unsubscribe = AsyncMock()
        mock_notifier.disconnect = AsyncMock()

        with patch("src.utils.redis_notifier.init_redis_notifier", return_value=mock_notifier):
            await manager.startup()
            await manager.shutdown()

            mock_notifier.unsubscribe.assert_called_once()
            mock_notifier.disconnect.assert_called_once()
            assert manager._redis_notifier is None
            assert manager._redis_listen_task is None

    def test_get_listener_map(self):
        """Test _get_listener_map returns correct channel mapping."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
        )

        listener_map = manager._get_listener_map()

        expected_channels = [
            "matchdata_change",
            "match_change",
            "scoreboard_change",
            "playclock_change",
            "gameclock_change",
            "football_event_change",
            "player_match_change",
        ]

        for channel in expected_channels:
            assert channel in listener_map
            assert callable(listener_map[channel])

    @pytest.mark.asyncio
    async def test_startup_resilient_when_redis_unavailable(self):
        """Test startup continues when Redis connection fails in degraded mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        with patch(
            "src.utils.redis_notifier.init_redis_notifier",
            side_effect=ConnectionError("Redis unavailable"),
        ):
            await manager.startup()

            assert manager.is_connected is False
            assert manager._is_degraded is True
            assert manager._connection_retry_task is not None

            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_safe_when_partially_initialized(self):
        """Test shutdown handles partially initialized state safely."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        manager._is_degraded = True
        manager.is_connected = False

        await manager.shutdown()

        assert manager._redis_notifier is None
        assert manager._redis_listen_task is None

    @pytest.mark.asyncio
    async def test_maintain_connection_recovers_from_degraded_state(self):
        """Test maintain_connection clears degraded flag on reconnection."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        manager._is_degraded = True
        manager.is_connected = False

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.register_callback = MagicMock()
        mock_notifier.listen_loop = AsyncMock()

        with patch("src.utils.redis_notifier.init_redis_notifier", return_value=mock_notifier):
            await manager.connect_to_redis()

            assert manager.is_connected is True

            await manager.shutdown()


class TestWebSocketManagerFallback:
    """Tests for WebSocket manager Redis to PostgreSQL fallback."""

    def test_fallback_attributes_initialization(self):
        """Test MatchDataWebSocketManager initializes fallback attributes."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        assert manager._using_fallback is False
        assert hasattr(manager, "_fallback_mode_lock")

    @pytest.mark.asyncio
    async def test_switch_to_fallback_mode(self):
        """Test switching from Redis to PostgreSQL fallback mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        mock_connection = MagicMock()
        mock_connection.add_listener = AsyncMock()
        mock_connection.close = AsyncMock()

        with patch("asyncpg.connect", return_value=mock_connection):
            result = await manager._switch_to_fallback_mode()

            assert result is True
            assert manager._using_fallback is True
            assert manager.is_connected is True
            assert manager._is_degraded is False

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_switch_to_fallback_mode_already_in_fallback(self):
        """Test _switch_to_fallback_mode is idempotent."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
        )
        manager._using_fallback = True

        result = await manager._switch_to_fallback_mode()

        assert result is True
        assert manager._using_fallback is True

    @pytest.mark.asyncio
    async def test_switch_to_redis_mode_from_fallback(self):
        """Test switching back from PostgreSQL fallback to Redis mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )
        manager._using_fallback = True

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.register_callback = MagicMock()
        mock_notifier.listen_loop = AsyncMock()

        mock_connection = MagicMock()
        mock_connection.close = AsyncMock()
        manager.connection = mock_connection
        manager._listeners = {"test_channel": AsyncMock()}

        with patch("src.utils.redis_notifier.init_redis_notifier", return_value=mock_notifier):
            result = await manager._switch_to_redis_mode()

            assert result is True
            assert manager._using_fallback is False

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_switch_to_redis_mode_not_in_fallback(self):
        """Test _switch_to_redis_mode does nothing when not in fallback."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
        )
        manager._using_fallback = False

        result = await manager._switch_to_redis_mode()

        assert result is True

    @pytest.mark.asyncio
    async def test_startup_falls_back_to_postgres_when_redis_unavailable(self):
        """Test startup falls back to PostgreSQL when Redis is unavailable."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        mock_connection = MagicMock()
        mock_connection.add_listener = AsyncMock()
        mock_connection.close = AsyncMock()

        with patch(
            "src.utils.redis_notifier.init_redis_notifier",
            side_effect=ConnectionError("Redis unavailable"),
        ):
            with patch("asyncpg.connect", return_value=mock_connection):
                await manager.startup()

                assert manager._using_fallback is True
                assert manager.is_connected is True
                assert manager._is_degraded is False

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_startup_degraded_when_both_redis_and_postgres_unavailable(self):
        """Test startup is degraded when both Redis and PostgreSQL are unavailable."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )

        with patch(
            "src.utils.redis_notifier.init_redis_notifier",
            side_effect=ConnectionError("Redis unavailable"),
        ):
            with patch("asyncpg.connect", side_effect=ConnectionError("PostgreSQL unavailable")):
                await manager.startup()

                assert manager._using_fallback is False
                assert manager.is_connected is False
                assert manager._is_degraded is True

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_channel_delivery_in_fallback_mode(self):
        """Test notifications are delivered correctly in fallback mode."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
        )
        manager._using_fallback = False
        manager.is_connected = True

        listener_map = manager._get_listener_map()

        assert "matchdata_change" in listener_map
        assert "playclock_change" in listener_map
        assert "gameclock_change" in listener_map
        assert callable(listener_map["matchdata_change"])

    @pytest.mark.asyncio
    async def test_maintain_connection_switches_back_to_redis(self):
        """Test _switch_to_redis_mode works when Redis becomes available."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )
        manager._using_fallback = True
        manager.is_connected = True

        mock_notifier = MagicMock()
        mock_notifier.subscribe = AsyncMock()
        mock_notifier.disconnect = AsyncMock()
        mock_notifier.register_callback = MagicMock()
        mock_notifier.listen_loop = AsyncMock()

        mock_pg_connection = MagicMock()
        mock_pg_connection.close = AsyncMock()
        mock_pg_connection.remove_listener = AsyncMock()
        manager.connection = mock_pg_connection
        manager._listeners = {"test_channel": AsyncMock()}

        with patch(
            "src.utils.redis_notifier.init_redis_notifier",
            return_value=mock_notifier,
        ):
            result = await manager._switch_to_redis_mode()

            assert result is True
            assert manager._using_fallback is False

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_cleanup_postgres_listeners_idempotent(self):
        """Test _cleanup_postgres_listeners handles missing connection gracefully."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
        )

        manager.connection = None
        manager._listeners = {}

        await manager._cleanup_postgres_listeners()

        assert manager._listeners == {}

    @pytest.mark.asyncio
    async def test_cleanup_postgres_listeners_removes_all(self):
        """Test _cleanup_postgres_listeners removes all registered listeners."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
        )

        mock_connection = MagicMock()
        mock_connection.remove_listener = AsyncMock()
        manager.connection = mock_connection
        manager._listeners = {
            "channel1": AsyncMock(),
            "channel2": AsyncMock(),
        }

        await manager._cleanup_postgres_listeners()

        assert mock_connection.remove_listener.call_count == 2
        assert manager._listeners == {}

    @pytest.mark.asyncio
    async def test_maintain_connection_uses_fallback_on_redis_failure(self):
        """Test maintain_connection switches to fallback on Redis failure."""
        manager = MatchDataWebSocketManager(
            db_url="postgresql://test:test@localhost/test",
            use_redis=True,
            redis_url="redis://localhost:6379",
        )
        manager.is_connected = False

        mock_pg_connection = MagicMock()
        mock_pg_connection.add_listener = AsyncMock()
        mock_pg_connection.close = AsyncMock()

        with patch(
            "src.utils.redis_notifier.init_redis_notifier",
            side_effect=ConnectionError("Redis unavailable"),
        ):
            with patch("asyncpg.connect", return_value=mock_pg_connection):
                task = asyncio.create_task(manager.maintain_connection())
                await asyncio.sleep(0.3)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

                assert manager._using_fallback is True

        await manager.shutdown()
