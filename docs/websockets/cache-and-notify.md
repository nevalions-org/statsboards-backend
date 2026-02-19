# Cache and Notify

## PostgreSQL Triggers

Triggers emit notifications on data changes (matchdata, match, scoreboard, playclock, gameclock, football events, player match).

## Listener Behavior

The `MatchDataWebSocketManager` listens on channels and routes events:

- `matchdata_change` → `match-update`
- `match_change` → `match-update`
- `scoreboard_change` → `match-update`
- `playclock_change` → `playclock-update`
- `gameclock_change` → `gameclock-update`
- `football_event_change` → `event-update`
- `player_match_change` → `players-update`

## Cache Invalidation

Before sending messages, caches are invalidated to force fresh reads:

- `match-update` → `invalidate_match_data(match_id)`
- `gameclock-update` → `invalidate_gameclock(match_id)`
- `playclock-update` → `invalidate_playclock(match_id)`
- `event-update` → `invalidate_event_data(match_id)`
- `statistics-update` → `invalidate_stats(match_id)`
- `players-update` → `invalidate_players(match_id)`

## Reference Data Cache (TTL)

`MatchDataCacheService` also keeps a small in-memory TTL cache for slowly changing reference entities used by websocket fetch paths:

- `team` entries: 5 minutes
- `tournament` entries: 5 minutes
- `sport` entries: 30 minutes
- `sport_scoreboard_preset` entries: 30 minutes

These caches are invalidated on writes (create/update/delete) in the corresponding services:

- `TeamServiceDB`
- `TournamentServiceDB` (including tournament move operations)
- `SportServiceDB`
- `SportScoreboardPresetServiceDB`
