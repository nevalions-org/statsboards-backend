# WebSocket and Real-time

- Use the existing `ws_manager` for connection management
- Follow existing event notification patterns in domain modules
- Test connection handling and reconnection scenarios
- Use PostgreSQL LISTEN/NOTIFY for real-time updates (see `src/utils/websocket/websocket_manager.py`)
- For high-connection deployments, use Redis pub/sub mode to reduce PostgreSQL connections (see `docs/REDIS_NOTIFY_IMPLEMENTATION.md`)
- Always clean up connections on disconnect
- WebSocket compression (permessage-deflate) is enabled and logged per connection

For endpoint and message formats, see `docs/api/websockets.md`.
