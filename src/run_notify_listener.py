"""Standalone PostgreSQL NOTIFY listener that publishes to Redis.

This script runs as a separate process and:
1. Opens a single PostgreSQL connection
2. Listens on all NOTIFY channels
3. Forwards notifications to Redis pub/sub

Run this once per pod instead of having each worker open its own connection.

Usage:
    python -m src.run_notify_listener

Environment variables:
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME - PostgreSQL connection
    REDIS_URL - Redis connection URL (default: redis://localhost:6379)
"""

import asyncio
import json
import signal

import asyncpg

from src.core.config import settings
from src.logging_config import get_logger, setup_logging
from src.utils.redis_notifier import init_redis_notifier

setup_logging()
logger = get_logger("notify_listener")

NOTIFY_CHANNELS = [
    "matchdata_change",
    "match_change",
    "scoreboard_change",
    "playclock_change",
    "gameclock_change",
    "football_event_change",
    "player_match_change",
]


class NotifyListener:
    """PostgreSQL NOTIFY listener that forwards to Redis."""

    def __init__(self, pg_url: str, redis_url: str):
        self.pg_url = pg_url
        self.redis_url = redis_url
        self.pg_connection: asyncpg.Connection | None = None
        self.redis_notifier = None
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the listener."""
        logger.info("Starting PostgreSQL NOTIFY listener")

        self.redis_notifier = await init_redis_notifier(self.redis_url)
        logger.info("Connected to Redis")

        self.pg_connection = await asyncpg.connect(self.pg_url, command_timeout=30)
        logger.info("Connected to PostgreSQL")

        for channel in NOTIFY_CHANNELS:
            await self.pg_connection.add_listener(channel, self._on_notify)
            logger.info(f"Listening on channel: {channel}")

        self._running = True
        logger.info("Notify listener started successfully")

    async def _on_notify(
        self, connection: asyncpg.Connection, pid: int, channel: str, payload: str
    ) -> None:
        """Handle PostgreSQL NOTIFY callback."""
        try:
            logger.debug(f"Received NOTIFY on {channel}: {payload[:100]}...")

            if not payload or not payload.strip():
                logger.warning(f"Empty payload received on channel {channel}")
                return

            payload_data = json.loads(payload.strip())

            await self.redis_notifier.publish(channel, payload_data)
            logger.debug(f"Forwarded {channel} notification to Redis")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode payload on {channel}: {e}")
        except Exception as e:
            logger.error(f"Error processing notification: {e}", exc_info=True)

    async def stop(self) -> None:
        """Stop the listener gracefully."""
        logger.info("Stopping PostgreSQL NOTIFY listener")
        self._running = False
        self._shutdown_event.set()

        if self.pg_connection:
            for channel in NOTIFY_CHANNELS:
                try:
                    await self.pg_connection.remove_listener(channel, self._on_notify)
                    logger.debug(f"Removed listener for channel: {channel}")
                except Exception as e:
                    logger.warning(f"Error removing listener for {channel}: {e}")

            await self.pg_connection.close()
            logger.info("PostgreSQL connection closed")

        if self.redis_notifier:
            await self.redis_notifier.disconnect()
            logger.info("Redis connection closed")

        logger.info("Notify listener stopped")

    async def run_forever(self) -> None:
        """Run the listener until shutdown signal."""
        await self._shutdown_event.wait()

    async def run_with_reconnect(self) -> None:
        """Run the listener with automatic reconnection."""
        while not self._shutdown_event.is_set():
            try:
                await self.start()
                await self.run_forever()
            except asyncio.CancelledError:
                logger.info("Listener cancelled")
                break
            except Exception as e:
                logger.error(f"Connection error: {e}", exc_info=True)
                if not self._shutdown_event.is_set():
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)
            finally:
                await self.stop()


async def main() -> None:
    """Main entry point."""
    pg_url = settings.db.db_url_websocket()
    redis_url = settings.redis_url

    listener = NotifyListener(pg_url, redis_url)

    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
        asyncio.create_task(listener.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await listener.run_with_reconnect()
    except KeyboardInterrupt:
        pass
    finally:
        await listener.stop()


if __name__ == "__main__":
    asyncio.run(main())
