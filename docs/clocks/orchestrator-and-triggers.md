# Orchestrator and Triggers

## Orchestrator

Location: `src/clocks/clock_orchestrator.py`

The orchestrator runs a single loop and triggers callbacks once per second per clock.

```python
async def _run_loop(self) -> None:
    while self._is_running:
        for clock_id, state_machine in list(self.running_playclocks.items()):
            if self._should_update(clock_id, state_machine):
                await self._update_playclock(clock_id, state_machine)

        for clock_id, state_machine in list(self.running_gameclocks.items()):
            if self._should_update(clock_id, state_machine):
                await self._update_gameclock(clock_id, state_machine)

        await asyncio.sleep(0.1)
```

Update decision logic (once per second):

```python
def _should_update(self, clock_id: int, state_machine: object) -> bool:
    if state_machine.started_at_ms is None:
        return False

    now_ms = time.time() * 1000
    elapsed_sec = (now_ms - state_machine.started_at_ms) / 1000.0
    current_second = int(elapsed_sec)

    last_second = self._last_updated_second.get(clock_id, -1)
    if current_second > last_second:
        self._last_updated_second[clock_id] = current_second
        return True
    return False
```

Callbacks are set by services:

- PlayClock: `trigger_update_playclock`, `_stop_playclock_internal`
- GameClock: `trigger_update_gameclock`, `_stop_gameclock_internal`

Self-healing behavior (STAB-250): If a pod misses a stop/pause event due to multi-pod deployment desync, the `trigger_update_*` callbacks detect `STOPPED` or `PAUSED` status from the database and auto-unregister from the local orchestrator, preventing excessive logging.

Gameclock stop persistence semantics:

- `direction=down`: terminal value is persisted as `0`
- `direction=up`: terminal value is persisted as `gameclock_max` (stable max, no rewind)
- In both directions, service persists `gameclock_status='stopped'` and clears `started_at_ms`

## Database Triggers

Triggers fire only on state/value changes, not every second.

Location: `alembic/versions/2026_01_24_1600-stab133_clock_status_and_value_notify.py`

Key behavior:

- INSERT: always notify
- UPDATE: notify only when status or value changes (`IS DISTINCT FROM`)
- DELETE: always notify

## WebSocket Delivery

WebSocket listeners parse trigger payloads, invalidate cache, and enqueue messages:

```python
async def _base_listener(self, connection, pid, channel, payload, update_type, invalidate_func=None):
    data = json.loads(payload.strip())
    match_id = data["match_id"]
    data["type"] = update_type

    if invalidate_func and self._cache_service:
        invalidate_func(match_id)

    await connection_manager.send_to_all(data, match_id=match_id)
```
