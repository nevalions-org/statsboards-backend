# Redis-based PostgreSQL NOTIFY Multiplexing

This document describes the Redis-based notification system that reduces PostgreSQL LISTEN connections from N per pod to 1 per pod.

## Problem

Each backend pod was opening **4 persistent LISTEN connections** to PostgreSQL (one per gunicorn worker). With 2 pods, this meant **8 LISTEN connections permanently held** on a database with only 100 `max_connections`.

## Solution

Use Redis pub/sub as an intermediary:
1. A **single listener process** opens 1 PostgreSQL connection and listens on all 7 NOTIFY channels
2. The listener forwards notifications to **Redis pub/sub**
3. Worker processes subscribe to Redis instead of PostgreSQL

This reduces connections from **4 per pod to 1 per pod**.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           Pod                                    │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────────────────────┐  │
│  │ Notify Listener  │    │         Worker Processes         │  │
│  │    (1 process)   │    │    (4 gunicorn workers)          │  │
│  │                  │    │                                  │  │
│  │  PG LISTEN (1)   │    │  Redis Subscribe (shared Redis)  │  │
│  │       │          │    │         │                        │  │
│  │       ▼          │    │         ▼                        │  │
│  │  Redis Publish ──┼────┼──▶ Redis Pub/Sub ──▶ Workers     │  │
│  └──────────────────┘    └──────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Connection Count Comparison

| Configuration | Connections/Pod | Connections (2 pods) |
|---------------|-----------------|----------------------|
| Before (4 workers) | 4 | 8 |
| After (1 listener) | 1 | 2 |
| **Savings** | **3** | **6** |

## Components

### 1. RedisNotifierService (`src/utils/redis_notifier.py`)

Service for publishing and subscribing to PostgreSQL NOTIFY events via Redis.

```python
from src.utils.redis_notifier import RedisNotifierService, init_redis_notifier

# Initialize
notifier = await init_redis_notifier("redis://localhost:6379")

# Publish a notification
await notifier.publish("player_match_change", {"match_id": 123})

# Subscribe and listen
await notifier.subscribe()
notifier.register_callback("player_match_change", my_callback)
await notifier.listen_loop()
```

### 2. Notify Listener (`src/run_notify_listener.py`)

Standalone process that listens to PostgreSQL and forwards to Redis.

```bash
python -m src.run_notify_listener
```

Listens on all 7 channels:
- `matchdata_change`
- `match_change`
- `scoreboard_change`
- `playclock_change`
- `gameclock_change`
- `football_event_change`
- `player_match_change`

### 3. WebSocketManager Redis Mode (`src/utils/websocket/websocket_manager.py`)

The `MatchDataWebSocketManager` now supports two modes:

**PostgreSQL Mode (default):** Direct LISTEN connections
```python
manager = MatchDataWebSocketManager(db_url=pg_url, use_redis=False)
```

**Redis Mode:** Subscribe to Redis instead
```python
manager = MatchDataWebSocketManager(
    db_url=pg_url,
    use_redis=True,
    redis_url="redis://localhost:6379"
)
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_REDIS_NOTIFIER` | `false` | Enable Redis mode in workers |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |

### Settings (`src/core/config.py`)

```python
class Settings(BaseSettings):
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL for pub/sub",
    )
```

## Deployment

### Docker Compose

Add the notify listener as a separate service:

```yaml
services:
  notify-listener:
    image: statsboards-backend-prod:latest
    command: python -m src.run_notify_listener
    environment:
      - DB_HOST=${DB_HOST}
      - DB_PORT=${DB_PORT}
      - DB_USER=${DB_USER}
      - DB_PASSWORD=${DB_PASSWORD}
      - DB_NAME=${DB_NAME}
      - REDIS_URL=${REDIS_URL}
    depends_on:
      - redis
    restart: always

  statsboards-backend:
    environment:
      - USE_REDIS_NOTIFIER=true
      - REDIS_URL=${REDIS_URL}
    # ... rest of config
```

### Kubernetes

Deploy as a sidecar container:

```yaml
spec:
  containers:
    - name: notify-listener
      image: statsboards-backend:latest
      command: ["python", "-m", "src.run_notify_listener"]
      env:
        - name: DB_HOST
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: host
        - name: REDIS_URL
          value: redis://redis-service:6379

    - name: backend
      image: statsboards-backend:latest
      env:
        - name: USE_REDIS_NOTIFIER
          value: "true"
        - name: REDIS_URL
          value: redis://redis-service:6379
```

## Migration Guide

### Step 1: Deploy Redis (if not already available)

Ensure Redis is accessible to all backend pods.

### Step 2: Deploy Notify Listener

Deploy `run_notify_listener.py` as a sidecar or separate service.

### Step 3: Enable Redis Mode

Set environment variables for workers:
```
USE_REDIS_NOTIFIER=true
REDIS_URL=redis://your-redis-host:6379
```

### Step 4: Verify

Check that:
1. Only 1 LISTEN connection per pod in `pg_stat_activity`
2. WebSocket notifications still work correctly
3. Redis pub/sub messages are being published/received

## Monitoring

### PostgreSQL Connections

```sql
SELECT pid, state, query 
FROM pg_stat_activity 
WHERE query LIKE 'LISTEN%';
```

Should show 1 connection per pod (down from 4).

### Redis Pub/Sub

```bash
# Subscribe to see all notifications
redis-cli SUBSCRIBE "pg_notify:events"
```

## Testing

Tests are located in:
- `tests/test_utils/test_redis_notifier.py` - RedisNotifierService unit tests
- `tests/test_utils/test_websocket_manager_redis.py` - WebSocket manager Redis mode tests

Run tests:
```bash
./run-tests.sh tests/test_utils/test_redis_notifier.py tests/test_utils/test_websocket_manager_redis.py
```

## Rollback

To rollback to direct PostgreSQL LISTEN:

1. Remove `USE_REDIS_NOTIFIER` environment variable (or set to `false`)
2. Stop the notify-listener process
3. Workers will fall back to direct PostgreSQL connections

No code changes required - the system automatically uses PostgreSQL mode when `USE_REDIS_NOTIFIER` is not set.
