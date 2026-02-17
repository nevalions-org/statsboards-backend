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

## Failure Modes and Behavior

### Critical vs Non-Critical Paths

| Component | Impact of Redis Outage | Data Loss Risk |
|-----------|------------------------|----------------|
| **CRUD APIs** | None - PostgreSQL is source of truth | None |
| **Database writes** | None | None |
| **WebSocket connections** | None - clients remain connected | None |
| **Realtime notifications** | **Degraded** - no live updates | Message loss during outage |
| **Historical data queries** | None | None |

### What Remains Functional

When Redis is unavailable:

- All REST API endpoints continue working normally
- Database reads and writes succeed
- WebSocket clients stay connected (connection is independent of Redis)
- Historical data queries work unchanged
- Authentication and authorization unaffected

### What Is Impacted

- **Live score updates** stop flowing to connected WebSocket clients
- **Match state changes** are not pushed in real-time
- **Game clock updates** pause
- Any event that would normally trigger a notification is **not delivered** during the outage

### Message Loss Window

Redis pub/sub is **fire-and-forget**. Messages published while subscribers are disconnected are **lost permanently**.

**Scenarios causing message loss:**
1. Redis pod restart - all in-flight messages lost
2. Network partition between backend and Redis - messages during partition lost
3. Redis memory pressure leading to eviction - pub/sub messages may be dropped

**Recovery behavior:**
- When Redis reconnects, **only new messages** are received
- Missed messages are **not replayed**
- Clients must refresh/re-query to get current state

### Automatic Fallback Behavior

The system implements automatic fallback to maintain realtime updates:

1. **Redis unavailable detected** (connection fails or times out)
2. **Automatic fallback** to direct PostgreSQL LISTEN (per worker)
3. **Realtime notifications resume** via PostgreSQL directly
4. **Background reconnection** attempts to Redis every 5 seconds
5. **Automatic switch back** to Redis when connectivity restores

**Fallback triggers:**
- Redis connection timeout
- Connection refused errors
- Pub/sub channel subscription failure

**Fallback limitations:**
- Each worker opens its own PostgreSQL LISTEN connection (reverts to pre-Redis connection count)
- Notify listener sidecar may continue running but messages won't reach workers

### Notify Listener Resilience

The `run_notify_listener.py` process:
- Attempts automatic reconnection every 5 seconds on connection failure
- Logs all connection errors with full stack traces
- Requires manual restart only on unrecoverable errors (e.g., credentials changed)

## Kubernetes Validation Runbook

### Prerequisites

```bash
# Set your namespace (adjust as needed)
export NAMESPACE=statsboard
export REDIS_POD=$(kubectl get pods -n $NAMESPACE -l app=redis -o jsonpath='{.items[0].metadata.name}')
export BACKEND_POD=$(kubectl get pods -n $NAMESPACE -l app=statsboards-backend -o jsonpath='{.items[0].metadata.name}')
```

### Test 1: Redis Pod Kill (Planned Restart)

Simulates a Redis pod restart (e.g., during node upgrade).

```bash
# Step 1: Start watching backend logs for Redis events
kubectl logs -n $NAMESPACE $BACKEND_POD -f | grep -E "Redis|fallback|realtime" &
LOG_PID=$!

# Step 2: Kill Redis pod
kubectl delete pod -n $NAMESPACE $REDIS_POD

# Step 3: Wait for Redis to restart (usually 10-30 seconds)
kubectl rollout status deployment/redis -n $NAMESPACE --timeout=60s

# Step 4: Verify backend detects Redis recovery
# Look for: "Redis connection restored" or "Switch back to Redis mode successful"

# Step 5: Verify WebSocket clients receive updates
# Connect a WebSocket client and trigger a database change

# Cleanup
kill $LOG_PID 2>/dev/null
```

**Expected behavior:**
- Backend logs show fallback to PostgreSQL LISTEN
- WebSocket clients temporarily miss updates (message loss window)
- Backend logs show reconnection to Redis after pod is ready
- Realtime updates resume automatically

### Test 2: Simulate Node Unavailability

Simulates a network partition or node failure.

```bash
# Step 1: Block traffic to Redis using a network policy or by scaling to 0
kubectl scale deployment redis -n $NAMESPACE --replicas=0

# Step 2: Watch backend logs
kubectl logs -n $NAMESPACE $BACKEND_POD -f | grep -E "Redis|fallback" &

# Step 3: Verify CRUD APIs still work
kubectl exec -n $NAMESPACE $BACKEND_POD -- curl -s localhost:8000/health
# Or port-forward and test locally
kubectl port-forward -n $NAMESPACE svc/statsboards-backend 8000:8000 &
curl http://localhost:8000/api/v1/sports

# Step 4: Verify fallback mode activates
# Look for: "Switching to PostgreSQL direct LISTEN fallback mode"

# Step 5: Restore Redis
kubectl scale deployment redis -n $NAMESPACE --replicas=1
kubectl rollout status deployment/redis -n $NAMESPACE --timeout=60s

# Step 6: Verify recovery
# Look for: "Switch back to Redis mode successful"
```

### Test 3: Verify WebSocket Recovery

```bash
# Step 1: Connect WebSocket client (use wscat or browser)
# Install: npm install -g wscat
kubectl port-forward -n $NAMESPACE svc/statsboards-backend 8000:8000 &
wscat -c ws://localhost:8000/ws/match/1/stats

# Step 2: In another terminal, kill Redis
kubectl delete pod -n $NAMESPACE $REDIS_POD

# Step 3: Observe WebSocket stays connected (no disconnect message)
# Step 4: Make a database change - updates pause (expected)
# Step 5: Wait for Redis recovery
# Step 6: Make another change - updates resume
```

### Expected Log Patterns

**Redis connection lost:**
```
WARNING  Redis connection failed: [error]. Attempting fallback to PostgreSQL direct LISTEN.
INFO     Switching to PostgreSQL direct LISTEN fallback mode
INFO     Fallback to PostgreSQL direct LISTEN successful - realtime features recovered
```

**Redis recovered:**
```
INFO     Redis connectivity restored, switching back to Redis mode
INFO     Switch back to Redis mode successful
```

**Notify listener reconnecting:**
```
ERROR    Connection error: [error]
INFO     Reconnecting in 5 seconds...
INFO     Connected to Redis
INFO     Connected to PostgreSQL
```

### Troubleshooting

| Symptom | Check | Resolution |
|---------|-------|-------------|
| WebSocket updates never resume | Backend logs for fallback activation | Verify `USE_REDIS_NOTIFIER=true` is set |
| High PostgreSQL connections | `pg_stat_activity` for LISTEN queries | Check if stuck in fallback mode |
| Notify listener crashloop | Pod logs for connection errors | Verify DB and Redis credentials |
| Updates delayed by 5+ seconds | Reconnection interval | Expected during Redis outage |

### Health Check Commands

```bash
# Check PostgreSQL LISTEN connections (should be 1-2 per pod in Redis mode, 4+ if in fallback)
kubectl exec -n $NAMESPACE -it <postgres-pod> -- psql -U <user> -d <db> -c \
  "SELECT pid, state, query FROM pg_stat_activity WHERE query LIKE 'LISTEN%';"

# Check Redis connectivity from backend
kubectl exec -n $NAMESPACE $BACKEND_POD -- python -c "
import asyncio
from redis import asyncio as aioredis
async def test():
    r = await aioredis.from_url('redis://redis:6379')
    await r.ping()
    print('Redis OK')
asyncio.run(test())
"

# Check notify listener status
kubectl logs -n $NAMESPACE -l app=statsboards-backend -c notify-listener --tail=20
```
