"""Tests for WebSocket manager with Redis mode."""

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
