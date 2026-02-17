"""Redis-based notification service for multiplexing PostgreSQL NOTIFY across workers.

This module provides a solution to reduce PostgreSQL LISTEN connections by:
1. Using a single dedicated listener process that publishes to Redis
2. Having workers subscribe to Redis instead of PostgreSQL directly

This reduces connections from N workers × 1 connection to just 1 connection per pod.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from redis import asyncio as aioredis

from src.logging_config import get_logger

NOTIFY_CHANNEL = "pg_notify:events"


class RedisNotifierService:
    """Service for publishing and subscribing to PostgreSQL NOTIFY events via Redis."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None
        self.pubsub = None
        self.logger = get_logger("RedisNotifierService", self)
        self._listener_callbacks: dict[str, Callable[[str, dict[str, Any]], Awaitable[None]]] = {}

    async def connect(self) -> None:
        """Establish Redis connection."""
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except (aioredis.ConnectionError, OSError, ValueError) as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.pubsub:
            await self.pubsub.close()
            self.pubsub = None
        if self.redis:
            await self.redis.close()
            self.redis = None
            self.logger.info("Disconnected from Redis")

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        """Publish a notification to Redis.

        Args:
            channel: The PostgreSQL NOTIFY channel name (e.g., 'player_match_change')
            payload: The notification payload as a dictionary
        """
        if not self.redis:
            raise RuntimeError("Redis not connected")

        message = json.dumps({"channel": channel, "payload": payload})
        await self.redis.publish(NOTIFY_CHANNEL, message)
        self.logger.debug(f"Published to Redis: channel={channel}")

    async def subscribe(self) -> None:
        """Subscribe to the Redis notification channel."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(NOTIFY_CHANNEL)
        self.logger.info(f"Subscribed to Redis channel: {NOTIFY_CHANNEL}")

    async def unsubscribe(self) -> None:
        """Unsubscribe from the Redis notification channel."""
        if self.pubsub:
            await self.pubsub.unsubscribe(NOTIFY_CHANNEL)
            self.logger.info(f"Unsubscribed from Redis channel: {NOTIFY_CHANNEL}")

    def register_callback(
        self, channel: str, callback: Callable[[str, dict[str, Any]], Awaitable[None]]
    ) -> None:
        """Register a callback for a specific PostgreSQL channel.

        Args:
            channel: The PostgreSQL NOTIFY channel name
            callback: Async function to call when notification is received
        """
        self._listener_callbacks[channel] = callback
        self.logger.debug(f"Registered callback for channel: {channel}")

    def unregister_callback(self, channel: str) -> None:
        """Unregister a callback for a specific PostgreSQL channel.

        Args:
            channel: The PostgreSQL NOTIFY channel name
        """
        self._listener_callbacks.pop(channel, None)
        self.logger.debug(f"Unregistered callback for channel: {channel}")

    async def listen_loop(self) -> None:
        """Listen for messages from Redis and dispatch to registered callbacks.

        This runs in an infinite loop until cancelled.
        """
        if not self.pubsub:
            raise RuntimeError("Not subscribed to Redis channel")

        self.logger.info("Starting Redis listen loop")

        try:
            async for message in self.pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                    channel = data.get("channel")
                    payload = data.get("payload")

                    if channel and channel in self._listener_callbacks:
                        callback = self._listener_callbacks[channel]
                        await callback(channel, payload)
                    else:
                        self.logger.debug(f"No callback registered for channel: {channel}")

                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode Redis message: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing Redis message: {e}")

        except asyncio.CancelledError:
            self.logger.info("Redis listen loop cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in Redis listen loop: {e}")
            raise


redis_notifier = RedisNotifierService(redis_url="redis://localhost:6379")


def get_redis_notifier() -> RedisNotifierService:
    """Get the global Redis notifier service instance."""
    return redis_notifier


async def init_redis_notifier(redis_url: str) -> RedisNotifierService:
    """Initialize the global Redis notifier service.

    Args:
        redis_url: Redis connection URL

    Returns:
        The initialized RedisNotifierService instance
    """
    global redis_notifier
    redis_notifier = RedisNotifierService(redis_url=redis_url)
    await redis_notifier.connect()
    return redis_notifier
