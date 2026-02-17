import asyncio
import json
import os
import time
from collections.abc import Callable
from typing import Any

import asyncpg
from starlette.websockets import WebSocket, WebSocketState

from src.core.config import settings
from src.logging_config import get_logger

connection_socket_logger_helper = get_logger("ConnectionManager")


class MatchDataWebSocketManager:
    def __init__(self, db_url: str, use_redis: bool = False, redis_url: str | None = None):
        self.db_url = db_url
        self.use_redis = use_redis
        self.redis_url = redis_url or settings.redis_url
        self.connection: asyncpg.Connection | None = None
        self._redis_notifier = None
        self._redis_listen_task: asyncio.Task | None = None
        self.logger = get_logger("MatchDataWebSocketManager", self)
        mode = "Redis pub/sub" if use_redis else "PostgreSQL direct"
        self.logger.info(f"MatchDataWebSocketManager initialized with mode: {mode}")
        self.is_connected = False
        self._connection_retry_task: asyncio.Task | None = None
        self._cache_service = None
        self._connection_lock = asyncio.Lock()
        self._listeners: dict[str, Callable] = {}
        self._is_degraded = False

    async def maintain_connection(self):
        while True:
            try:
                if not self.is_connected:
                    if self.use_redis:
                        await self.connect_to_redis()
                        if self._redis_notifier and (
                            not self._redis_listen_task or self._redis_listen_task.done()
                        ):
                            self._redis_listen_task = asyncio.create_task(
                                self._redis_notifier.listen_loop()
                            )
                            self.logger.info("Redis listen loop restarted")
                        if self._is_degraded:
                            self.logger.info(
                                "Redis connection restored - realtime features recovered"
                            )
                            self._is_degraded = False
                    else:
                        await self.connect_to_db()
                await asyncio.sleep(5)
            except Exception as e:
                if self.use_redis and not self._is_degraded:
                    self.logger.warning(
                        f"Redis connection maintenance error: {str(e)}. Realtime features degraded."
                    )
                    self._is_degraded = True
                else:
                    self.logger.error(f"Connection maintenance error: {str(e)}", exc_info=True)
                self.is_connected = False
                await asyncio.sleep(5)

    async def connect_to_redis(self):
        """Connect to Redis and set up pub/sub listeners."""
        from src.utils.redis_notifier import init_redis_notifier

        async with self._connection_lock:
            try:
                self._redis_notifier = await init_redis_notifier(self.redis_url)
                await self._redis_notifier.subscribe()

                listeners = self._get_listener_map()
                self._listeners = listeners.copy()

                for channel, callback in listeners.items():
                    self._redis_notifier.register_callback(channel, callback)
                    self.logger.info(f"Registered Redis callback for channel: {channel}")

                self.is_connected = True
                self.logger.info("Successfully connected to Redis pub/sub")

            except Exception as e:
                self.logger.error(f"Redis connection error: {str(e)}", exc_info=True)
                self.is_connected = False
                raise

    async def connect_to_db(self):
        async with self._connection_lock:
            try:
                if self.connection:
                    try:
                        await self.connection.close()
                    except (asyncpg.PostgresConnectionError, OSError):
                        pass

                self.connection = await asyncio.wait_for(
                    asyncpg.connect(self.db_url, command_timeout=30), timeout=30.0
                )
                self.logger.info("Successfully connected to database")

                await self.setup_listeners()
                self.is_connected = True

            except (asyncpg.PostgresConnectionError, OSError, asyncio.TimeoutError) as e:
                self.logger.error(f"Database connection error: {str(e)}", exc_info=True)
                self.is_connected = False
                raise

    async def setup_listeners(self):
        if self.connection is None:
            raise RuntimeError("Database connection not established")

        listeners = self._get_listener_map()

        self._listeners = listeners.copy()
        failed_channels = []
        for channel, listener in listeners.items():
            try:
                await self.connection.add_listener(channel, listener)
                self.logger.info(f"Successfully added listener for channel: {channel}")
            except Exception as e:
                self.logger.error(
                    f"Error setting up listener for {channel}: {str(e)}", exc_info=True
                )
                failed_channels.append(channel)

        if failed_channels:
            self.logger.warning(f"Failed to set up listeners for channels: {failed_channels}")
            if len(failed_channels) == len(listeners):
                raise RuntimeError(f"All listeners failed to setup: {failed_channels}")

    def _get_listener_map(self) -> dict[str, Callable]:
        """Get the mapping of channel names to listener callbacks.

        Returns:
            Dict mapping PostgreSQL NOTIFY channel names to listener methods.
        """
        return {
            "matchdata_change": self._create_pg_listener_wrapper(self.match_data_listener),
            "match_change": self._create_pg_listener_wrapper(self.match_data_listener),
            "scoreboard_change": self._create_pg_listener_wrapper(self.match_data_listener),
            "playclock_change": self._create_pg_listener_wrapper(self.playclock_listener),
            "gameclock_change": self._create_pg_listener_wrapper(self.gameclock_listener),
            "football_event_change": self._create_pg_listener_wrapper(self.event_listener),
            "player_match_change": self._create_pg_listener_wrapper(self.players_update_listener),
        }

    def _create_pg_listener_wrapper(self, listener_func: Callable) -> Callable:
        """Create a wrapper that adapts between PostgreSQL and Redis callback signatures.

        PostgreSQL listeners receive: (connection, pid, channel, payload)
        Redis callbacks receive: (channel, payload_dict)

        Args:
            listener_func: The original listener function with PostgreSQL signature

        Returns:
            A wrapper function that works with both callback types
        """

        async def wrapper(connection_or_channel, pid_or_payload, channel=None, payload=None):
            if self.use_redis:
                actual_channel = connection_or_channel
                actual_payload = pid_or_payload
                await listener_func(None, None, actual_channel, json.dumps(actual_payload))
            else:
                await listener_func(connection_or_channel, pid_or_payload, channel, payload)

        return wrapper

    def set_cache_service(self, cache_service):
        self._cache_service = cache_service
        self.logger.info("Cache service set for WebSocket manager")

    async def startup(self):
        async with self._connection_lock:
            if (
                self.is_connected
                and self._connection_retry_task
                and not self._connection_retry_task.done()
            ):
                self.logger.debug("WebSocket manager already started, skipping duplicate startup")
                return

        connection_succeeded = False
        try:
            if self.use_redis:
                await self.connect_to_redis()
                if self._redis_notifier:
                    self._redis_listen_task = asyncio.create_task(
                        self._redis_notifier.listen_loop()
                    )
                    self.logger.info("Redis listen loop started")
                connection_succeeded = True
            else:
                await self.connect_to_db()
                connection_succeeded = True
        except Exception as e:
            if self.use_redis:
                self.logger.warning(
                    f"Redis connection failed during startup: {str(e)}. "
                    "Realtime features degraded - backend will continue serving HTTP/API endpoints. "
                    "Background reconnection attempts will continue."
                )
                self._is_degraded = True
            else:
                self.logger.error(f"Startup error: {str(e)}", exc_info=True)
                raise

        if not self._connection_retry_task or self._connection_retry_task.done():
            self._connection_retry_task = asyncio.create_task(self.maintain_connection())
            if connection_succeeded:
                self.logger.info("WebSocket manager startup complete")
            else:
                self.logger.info(
                    "WebSocket manager started in degraded mode - realtime features unavailable"
                )
        else:
            self.logger.debug(
                "Connection retry task already exists, skipping duplicate task creation"
            )

    async def _base_listener(
        self, connection, pid, channel, payload, update_type, invalidate_func=None
    ):
        self.logger.debug(f"{update_type} notification received on channel {channel}")

        if not payload or not payload.strip():
            self.logger.warning("Empty payload received")
            return

        try:
            data = json.loads(payload.strip())
            match_id = data["match_id"]

            if "data" in data and update_type in ["gameclock-update", "playclock-update"]:
                data[update_type.replace("-update", "")] = data.pop("data")

            data["type"] = update_type

            self.logger.debug(f"Processing {update_type} for match {match_id}")

            if invalidate_func and self._cache_service:
                invalidate_func(match_id)

            await connection_manager.send_to_all(data, match_id=match_id)

        except json.JSONDecodeError as e:
            self.logger.error(
                f"JSON decode error in {update_type} listener: {str(e)}", exc_info=True
            )
        except Exception as e:
            self.logger.error(f"Error in {update_type} listener: {str(e)}", exc_info=True)

    async def playclock_listener(self, connection, pid, channel, payload):
        invalidate_func = self._cache_service.invalidate_playclock if self._cache_service else None
        await self._base_listener(
            connection,
            pid,
            channel,
            payload,
            "playclock-update",
            invalidate_func,
        )

    async def match_data_listener(self, connection, pid, channel, payload):
        from src.helpers.fetch_helpers import fetch_with_scoreboard_data

        try:
            trigger_data = json.loads(payload.strip())
            match_id = trigger_data["match_id"]

            self.logger.debug(f"Processing match-update for match {match_id}")

            if self._cache_service:
                self._cache_service.invalidate_match_data(match_id)

            if channel == "scoreboard_change":
                full_data = await fetch_with_scoreboard_data(
                    match_id, cache_service=self._cache_service
                )
                if full_data and "data" in full_data:
                    message = {"type": "match-update", "data": full_data["data"]}
                    await connection_manager.send_to_all(message, match_id=match_id)
                    self.logger.debug(f"Sent enriched scoreboard data for match {match_id}")
                else:
                    self.logger.warning(
                        f"Failed to fetch full match data for match {match_id}, sending partial data"
                    )
                    message = {"type": "match-update", "data": trigger_data.get("data", {})}
                    await connection_manager.send_to_all(message, match_id=match_id)
            elif "data" in trigger_data:
                raw_data = trigger_data["data"]

                if channel == "matchdata_change":
                    wrapped_data = raw_data
                else:
                    wrapped_data = raw_data

                message = {"type": "match-update", "data": wrapped_data}
                await connection_manager.send_to_all(message, match_id=match_id)
                self.logger.debug(f"Sent trigger data for match {match_id}")
            else:
                full_data = await fetch_with_scoreboard_data(
                    match_id, cache_service=self._cache_service
                )
                if full_data and "data" in full_data:
                    message = {"type": "match-update", "data": full_data["data"]}
                    await connection_manager.send_to_all(message, match_id=match_id)
                    self.logger.debug(f"Sent full match data for match {match_id}")
                else:
                    self.logger.warning(
                        f"Failed to fetch full match data for match {match_id}, sending partial data"
                    )
                    message = {"type": "match-update", "data": trigger_data.get("data", {})}
                    await connection_manager.send_to_all(message, match_id=match_id)
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in match_data_listener: {str(e)}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error in match_data_listener: {str(e)}", exc_info=True)

    async def gameclock_listener(self, connection, pid, channel, payload):
        invalidate_func = self._cache_service.invalidate_gameclock if self._cache_service else None
        await self._base_listener(
            connection,
            pid,
            channel,
            payload,
            "gameclock-update",
            invalidate_func,
        )

    async def players_update_listener(self, connection, pid, channel, payload):
        from src.core import db
        from src.helpers.fetch_helpers import deep_dict_convert
        from src.matches.db_services import MatchServiceDB

        try:
            trigger_data = json.loads(payload.strip())
            match_id = trigger_data["match_id"]

            self.logger.debug(f"Processing players-update for match {match_id}")

            if self._cache_service:
                self._cache_service.invalidate_players(match_id)
                self._cache_service.invalidate_match_data(match_id)

            match_service_db = MatchServiceDB(db)
            players = await match_service_db.get_players_with_full_data_optimized(match_id)
            serialized_players = (
                [deep_dict_convert(player) for player in players] if players else []
            )

            message = {
                "type": "players-update",
                "data": {"match_id": match_id, "players": serialized_players},
            }
            await connection_manager.send_to_all(message, match_id=match_id)
            self.logger.debug(f"Sent players update for match {match_id}")
        except json.JSONDecodeError as e:
            self.logger.error(
                f"JSON decode error in players_update_listener: {str(e)}", exc_info=True
            )
        except Exception as e:
            self.logger.error(f"Error in players_update_listener: {str(e)}", exc_info=True)

    async def event_listener(self, connection, pid, channel, payload):
        from src.core import db
        from src.football_events.db_services import FootballEventServiceDB

        try:
            trigger_data = json.loads(payload.strip())
            match_id = trigger_data["match_id"]

            self.logger.debug(f"Processing event-update for match {match_id}")

            if self._cache_service:
                self._cache_service.invalidate_event_data(match_id)

            event_service_db = FootballEventServiceDB(db)
            events = await event_service_db.get_events_with_players(match_id)

            message = {"type": "event-update", "match_id": match_id, "events": events or []}
            await connection_manager.send_to_all(message, match_id=match_id)
            self.logger.debug(f"Sent events update for match {match_id}")

            if self._cache_service:
                self._cache_service.invalidate_stats(match_id)
            await self._base_listener(
                connection,
                pid,
                channel,
                payload,
                "statistics-update",
                self._cache_service.invalidate_stats if self._cache_service else None,
            )
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in event_listener: {str(e)}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error in event_listener: {str(e)}", exc_info=True)

    async def shutdown(self):
        async with self._connection_lock:
            try:
                if self._connection_retry_task:
                    self._connection_retry_task.cancel()
                    try:
                        await self._connection_retry_task
                    except asyncio.CancelledError:
                        pass

                if self._redis_listen_task:
                    self._redis_listen_task.cancel()
                    try:
                        await self._redis_listen_task
                    except asyncio.CancelledError:
                        pass
                    self._redis_listen_task = None

                if self._redis_notifier:
                    await self._redis_notifier.unsubscribe()
                    await self._redis_notifier.disconnect()
                    self._redis_notifier = None
                    self.logger.info("Redis pub/sub disconnected")

                if self.connection:
                    for channel, listener in self._listeners.items():
                        try:
                            await self.connection.remove_listener(channel, listener)
                            self.logger.debug(f"Removed listener for channel: {channel}")
                        except Exception as e:
                            self.logger.warning(f"Error removing listener for {channel}: {str(e)}")
                    self._listeners.clear()

                    await self.connection.close()
                    self.logger.info("Database connection closed")

                self.is_connected = False
                self.logger.info("WebSocket manager shutdown complete")
            except Exception as e:
                self.logger.error(f"Error during shutdown: {str(e)}", exc_info=True)


use_redis_notifier = os.getenv("USE_REDIS_NOTIFIER", "false").lower() in ("true", "1", "yes")

ws_manager = MatchDataWebSocketManager(
    db_url=settings.db.db_url_websocket(),
    use_redis=use_redis_notifier,
    redis_url=settings.redis_url,
)


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.queues: dict[str, asyncio.Queue] = {}
        self.match_subscriptions: dict[str | int, list[str]] = {}
        self.last_activity: dict[str, float] = {}
        self.logger = get_logger("ConnectionManager", self)
        self.logger.info("ConnectionManager initialized")

    async def connect(self, websocket: WebSocket, client_id: str, match_id: int | None = None):
        self.logger.info(f"Active Connections len: {len(self.active_connections)}")
        self.logger.info(f"Active Connections {self.active_connections}")
        self.logger.info(
            f"Connecting to WebSocket at {websocket} with client_id: {client_id} and match_id: {match_id}"
        )

        if client_id in self.active_connections:
            self.logger.debug(
                f"Client with client_id:{client_id} in active_connections: {self.active_connections}"
            )
            self.logger.warning(f"Disconnecting existing connection for client_id:{client_id}")
            await self.disconnect(client_id)
            self.logger.debug(f"Active connections after disconnect: {self.active_connections}")

        self.logger.debug(f"Adding new connection for client with client_id: {client_id}")
        self.active_connections[client_id] = websocket
        self.queues[client_id] = asyncio.Queue()
        self.update_client_activity(client_id)
        self.logger.info(f"New connection created: {self.active_connections[client_id]}")
        self.logger.info(f"New queue created: {self.queues[client_id]}")

        if match_id:
            if match_id in self.match_subscriptions:
                self.logger.debug(
                    f"Match with match_id: {match_id} in match subscriptions {self.match_subscriptions}"
                )
                self.logger.debug(
                    f"Adding client with client_id: {client_id} to match_subscription {self.match_subscriptions[match_id]}"
                )
                self.match_subscriptions[match_id].append(client_id)
                self.logger.debug(f"Match subscription added {self.match_subscriptions[match_id]}")

            else:
                self.logger.debug(
                    f"Match with match_id: {match_id} not in match subscriptions {self.match_subscriptions}"
                )
                self.match_subscriptions[match_id] = [client_id]
                self.logger.debug(f"Match subscription added {self.match_subscriptions[match_id]}")

    async def cleanup_connection_resources(self, client_id: str):
        if client_id in self.queues:
            while not self.queues[client_id].empty():
                try:
                    self.queues[client_id].get_nowait()
                except asyncio.QueueEmpty:
                    break
            del self.queues[client_id]

        if client_id in self.active_connections:
            del self.active_connections[client_id]

        if client_id in self.last_activity:
            del self.last_activity[client_id]

        for _match_id, clients in self.match_subscriptions.items():
            if client_id in clients:
                clients.remove(client_id)

    async def disconnect(self, client_id: str):
        """
        Disconnect a client WebSocket connection safely.

        Checks WebSocket state before closing to avoid double-close errors.
        Handles RuntimeError for already-closed connections gracefully while
        propagating other unexpected errors.

        Args:
            client_id: Unique identifier for the client connection
        """
        if client_id in self.active_connections:
            self.logger.info(
                f"Disconnecting from connections for client with client_id:{client_id}"
            )
            websocket = self.active_connections[client_id]
            if (
                hasattr(websocket, "application_state")
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                try:
                    await websocket.close()
                    self.logger.debug(f"Closed WebSocket connection for client {client_id}")
                except RuntimeError as e:
                    if "websocket.close" in str(e):
                        self.logger.debug(f"WebSocket already closed for client {client_id}: {e}")
                    else:
                        self.logger.warning(f"WebSocket close error for client {client_id}: {e}")
                        raise
            elif hasattr(websocket, "application_state"):
                self.logger.debug(
                    f"WebSocket already disconnected (state: {websocket.application_state}), skipping close for client {client_id}"
                )
            else:
                self.logger.debug(
                    f"WebSocket has no application_state attribute, attempting close for client {client_id}"
                )
                try:
                    await websocket.close()
                except RuntimeError as e:
                    if "websocket.close" in str(e):
                        self.logger.debug(f"WebSocket already closed for client {client_id}: {e}")
                    else:
                        raise
            await self.cleanup_connection_resources(client_id)

    async def get_active_connections(self):
        return self.active_connections

    async def get_match_subscriptions(self, match_id: int):
        return self.match_subscriptions.get(match_id, [])

    async def get_queue_for_client(self, client_id: str):
        self.logger.debug(f"Getting queue for client_id: {client_id}")
        queue = self.queues[client_id]
        return queue

    def update_client_activity(self, client_id: str):
        self.last_activity[client_id] = time.time()
        self.logger.debug(
            f"Updated activity for client {client_id}: {self.last_activity[client_id]}"
        )

    async def cleanup_stale_connections(self, timeout_seconds: float = 90.0):
        now = time.time()
        stale_clients = [
            client_id
            for client_id, last_seen in self.last_activity.items()
            if now - last_seen > timeout_seconds
        ]
        for client_id in stale_clients:
            self.logger.warning(
                f"Cleaning up stale connection for client {client_id} (inactive for {now - self.last_activity[client_id]:.1f}s)"
            )
            await self.disconnect(client_id)

    async def send_to_all(self, data: dict[str, Any] | str, match_id: str | None = None):
        data_type = data["type"] if isinstance(data, dict) and "type" in data else "unknown"
        match_key = match_id or ""
        self.logger.debug(
            f"Sending {data_type} data for match_id: {match_id} to {len(self.match_subscriptions.get(match_key, []))} clients"
        )
        if match_id:
            for client_id in self.match_subscriptions.get(match_key, []):
                if client_id in self.queues:
                    self.logger.debug(
                        f"Client with client_id: {client_id} in queues: {self.queues[client_id]}"
                    )

                    await self.queues[client_id].put(data)
                    self.logger.debug(
                        f"Data sent to all clients in queues with match id:{match_id}"
                    )

    async def send_to_match_id_channels(self, data):
        match_id = data["match_id"]
        await self.send_to_all(data, match_id=match_id)


connection_manager = ConnectionManager()


async def process_client_queue(client_id: str | int, handlers: dict[str | int, Callable[[], None]]):
    client_id_str = str(client_id)
    if client_id_str in connection_manager.queues:
        queue = connection_manager.queues[client_id_str]
        while True:
            try:
                message = await queue.get()
                connection_socket_logger_helper.debug(
                    f"Dequeued message for client_id {client_id}: {message}"
                )

                message_type = message.get("type")

                get_handlers = handlers.get(message_type)

                if get_handlers:
                    connection_socket_logger_helper.info(
                        f"Handler executed successfully for message: {message}"
                    )
                    return get_handlers
                else:
                    connection_socket_logger_helper.warning(
                        f"No handler found for message type {message_type} in client_id {client_id}"
                    )

                queue.task_done()
            except asyncio.CancelledError:
                connection_socket_logger_helper.warning(
                    f"Queue processing canceled for client_id {client_id}"
                )
                break
            except Exception as e:
                connection_socket_logger_helper.error(
                    f"Error processing queue for client_id {client_id}: {e}"
                )
                break
