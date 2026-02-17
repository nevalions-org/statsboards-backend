# WebSocket and Real-time

- Use the existing `ws_manager` for connection management
- Follow existing event notification patterns in domain modules
- Test connection handling and reconnection scenarios
- Use PostgreSQL LISTEN/NOTIFY for real-time updates (see `src/utils/websocket/websocket_manager.py`)
- For high-connection deployments, use Redis pub/sub mode to reduce PostgreSQL connections (see `docs/REDIS_NOTIFY_IMPLEMENTATION.md`)
- Always clean up connections on disconnect
- WebSocket compression (permessage-deflate) is enabled and logged per connection

## Failure Modes

When Redis is used for pub/sub:
- **CRUD operations unaffected** - PostgreSQL is the source of truth
- **Realtime notifications degraded** during Redis outage (message loss possible)
- **Automatic fallback** to direct PostgreSQL LISTEN maintains updates

For detailed failure behavior and k8s validation steps, see `docs/REDIS_NOTIFY_IMPLEMENTATION.md#failure-modes-and-behavior`.

For endpoint and message formats, see `docs/api/websockets.md`.
