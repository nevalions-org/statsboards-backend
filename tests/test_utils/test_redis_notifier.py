"""Tests for RedisNotifierService."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.redis_notifier import NOTIFY_CHANNEL, RedisNotifierService, init_redis_notifier


class TestRedisNotifierService:
    """Tests for RedisNotifierService class."""

    def test_init(self):
        """Test RedisNotifierService initialization."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        assert service.redis_url == "redis://localhost:6379"
        assert service.redis is None
        assert service.pubsub is None
        assert service._listener_callbacks == {}

    @pytest.mark.asyncio
    async def test_connect_success(self):
        """Test successful Redis connection."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")

        mock_redis = AsyncMock()

        async def mock_from_url(url, decode_responses=False):
            return mock_redis

        with patch("src.utils.redis_notifier.aioredis.from_url", side_effect=mock_from_url):
            await service.connect()

            assert service.redis == mock_redis

    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test Redis disconnection."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        mock_redis = AsyncMock()
        service.redis = mock_redis
        mock_pubsub = AsyncMock()
        service.pubsub = mock_pubsub

        await service.disconnect()

        mock_pubsub.close.assert_called_once()
        mock_redis.close.assert_called_once()
        assert service.redis is None
        assert service.pubsub is None

    @pytest.mark.asyncio
    async def test_publish(self):
        """Test publishing notification to Redis."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        service.redis = AsyncMock()

        payload = {"match_id": 123, "data": "test"}
        await service.publish("player_match_change", payload)

        service.redis.publish.assert_called_once()
        call_args = service.redis.publish.call_args
        assert call_args[0][0] == NOTIFY_CHANNEL
        message = json.loads(call_args[0][1])
        assert message["channel"] == "player_match_change"
        assert message["payload"] == payload

    @pytest.mark.asyncio
    async def test_publish_not_connected(self):
        """Test publish raises error when not connected."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")

        with pytest.raises(RuntimeError, match="Redis not connected"):
            await service.publish("player_match_change", {})

    @pytest.mark.asyncio
    async def test_subscribe(self):
        """Test subscribing to Redis channel."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        mock_pubsub = AsyncMock()
        mock_redis = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub
        service.redis = mock_redis

        await service.subscribe()

        mock_redis.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_called_once_with(NOTIFY_CHANNEL)
        assert service.pubsub == mock_pubsub

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Test unsubscribing from Redis channel."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        service.pubsub = AsyncMock()

        await service.unsubscribe()

        service.pubsub.unsubscribe.assert_called_once_with(NOTIFY_CHANNEL)

    def test_register_callback(self):
        """Test registering a callback."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")

        async def callback(channel, payload):
            pass

        service.register_callback("player_match_change", callback)

        assert "player_match_change" in service._listener_callbacks
        assert service._listener_callbacks["player_match_change"] == callback

    def test_unregister_callback(self):
        """Test unregistering a callback."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")

        async def callback(channel, payload):
            pass

        service.register_callback("player_match_change", callback)
        service.unregister_callback("player_match_change")

        assert "player_match_change" not in service._listener_callbacks

    @pytest.mark.asyncio
    async def test_listen_loop_dispatches_to_callback(self):
        """Test listen loop dispatches messages to registered callbacks."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        service.pubsub = AsyncMock()

        received_channel = None
        received_payload = None

        async def callback(channel, payload):
            nonlocal received_channel, received_payload
            received_channel = channel
            received_payload = payload

        service.register_callback("player_match_change", callback)

        messages = [
            {"type": "subscribe", "channel": NOTIFY_CHANNEL},
            {
                "type": "message",
                "data": json.dumps(
                    {"channel": "player_match_change", "payload": {"match_id": 123}}
                ),
            },
        ]
        message_iter = iter(messages)

        async def mock_listen():
            for msg in message_iter:
                yield msg

        service.pubsub.listen = mock_listen

        task = asyncio.create_task(service.listen_loop())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert received_channel == "player_match_change"
        assert received_payload == {"match_id": 123}

    @pytest.mark.asyncio
    async def test_listen_loop_handles_json_error(self):
        """Test listen loop handles JSON decode errors gracefully."""
        service = RedisNotifierService(redis_url="redis://localhost:6379")
        service.pubsub = AsyncMock()

        service.register_callback("test", AsyncMock())

        messages = [
            {"type": "message", "data": "invalid json{"},
        ]
        message_iter = iter(messages)

        async def mock_listen():
            for msg in message_iter:
                yield msg

        service.pubsub.listen = mock_listen

        task = asyncio.create_task(service.listen_loop())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestInitRedisNotifier:
    """Tests for init_redis_notifier function."""

    @pytest.mark.asyncio
    async def test_init_redis_notifier(self):
        """Test initializing global Redis notifier."""
        mock_redis = AsyncMock()

        async def mock_from_url(url, decode_responses=False):
            return mock_redis

        with patch("src.utils.redis_notifier.aioredis.from_url", side_effect=mock_from_url):
            service = await init_redis_notifier("redis://test:6379")

            assert service.redis_url == "redis://test:6379"
            assert service.redis == mock_redis
