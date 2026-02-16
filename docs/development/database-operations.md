# Database Operations

- Always use async context managers: `async with self.db.get_session_maker()() as session:`
- Use SQLAlchemy 2.0 select API: `select(Model).where(Model.field == value)`
- Use `session.execute(stmt)` and `results.scalars().all()` for queries
- Never commit manually in service methods - let BaseServiceDB handle it
- Use relationships defined in models rather than manual joins
- Use eager loading (`selectinload()`) to prevent N+1 query problems
- Add `order_by` to relationships for predictable ordering

## Connection Pool Configuration

Default pool settings (configurable via environment variables):

| Setting | Production | Test |
|---------|------------|------|
| `pool_size` | 8 | 3 |
| `max_overflow` | 12 | 5 |
| `pool_timeout` | 30s | 30s |
| **Max connections** | 20 | 8 |

Environment variables: `DB_POOL_SIZE`, `DB_POOL_MAX_OVERFLOW`

### Pool Timeout with Retry

Use `get_connection_with_retry()` for resilient connection acquisition with exponential backoff:

```python
from src.core.models.base import get_connection_with_retry

session = await get_connection_with_retry(
    db.get_session_maker(),
    max_retries=3,
    base_delay=0.1,
)
```

This handles `PoolTimeout` exceptions gracefully with automatic retries.

## Batch Transactions with Session Parameter

Service methods support optional `session` parameter for atomic batch operations:

```python
async def create(
    self,
    item: SomeSchemaCreate,
    *,
    session: AsyncSession | None = None,
) -> SomeDB:
    """Create item with optional session for transaction batching.

    Args:
        item: The item data to create.
        session: Optional session for transaction batching.
                 If provided, caller is responsible for commit/rollback.
    """
    if session is not None:
        return await self._create_with_session(item, session)
    return await super().create(item)
```

### Batched Operations Example

When creating multiple related entities, use a single session per match for atomicity:

```python
async def _create_single_parsed_match_batched(
    self,
    match_data: dict,
    match_service,
    scoreboard_service,
    match_data_service,
) -> dict | None:
    """Create match with all related entities in one transaction."""
    async with db.get_session_maker()() as session:
        async with session.begin():
            match = await match_service.create(
                match_schema,
                session=session,
            )

            scoreboard = await scoreboard_service.create(
                ScoreboardSchemaCreate(match_id=match.id),
                session=session,
            )

            match_data = await match_data_service.create(
                MatchDataSchemaCreate(match_id=match.id),
                session=session,
            )

            return {"match": match, "scoreboard": scoreboard}
```

Benefits:
- **Atomicity**: All operations succeed or fail together
- **Performance**: ~10-15 session acquisitions reduced to 1 per match
- **Connection pool efficiency**: Fewer connection checkouts

## File Structure Per Domain

Each domain module must contain:

- `schemas.py`: Pydantic models for API contracts
- `db_services.py`: Service class inheriting from `BaseServiceDB`
- `views.py`: Router class inheriting from `BaseRouter`
- `__init__.py`: Exports router as `api_<domain>_router`

## Fetching Complex Relationships in Services

### Loading Strategy Recommendations

- Prefer `selectinload()` over `joinedload()` for most relationship loading
- Chain `selectinload()` for 2+ levels of relationships
- Use base mixin methods (`get_related_item_level_one_by_id()`, `get_nested_related_item_by_id()`) when possible
- Add indexes on frequently queried foreign key combinations

### Level 1 Relationship Loading

```python
async def get_teams_by_tournament(self, tournament_id: int) -> list[TeamDB]:
    return await self.get_related_item_level_one_by_id(
        tournament_id,
        "teams",
    )
```

### Nested Relationship Loading (2+ Levels Deep)

```python
async def get_player_by_match_full_data(self, match_id: int) -> list[dict]:
    async with self.db.get_session_maker()() as session:
        stmt = (
            select(PlayerMatchDB)
            .where(PlayerMatchDB.match_id == match_id)
            .options(
                selectinload(PlayerMatchDB.player_team_tournament)
                .selectinload(PlayerTeamTournamentDB.player)
                .selectinload(PlayerDB.person),
                selectinload(PlayerMatchDB.match_position),
                selectinload(PlayerMatchDB.team),
            )
        )

        results = await session.execute(stmt)
        players = results.scalars().all()

        players_with_data = []
        for player in players:
            players_with_data.append(
                {
                    "id": player.id,
                    "player_id": (
                        player.player_team_tournament.player_id
                        if player.player_team_tournament
                        else None
                    ),
                    "player": (
                        player.player_team_tournament.player
                        if player.player_team_tournament
                        else None
                    ),
                    "team": player.team,
                    "position": (
                        {
                            **player.match_position.__dict__,
                            "category": player.match_position.category,
                        }
                        if player.match_position
                        else None
                    ),
                    "player_team_tournament": player.player_team_tournament,
                    "person": (
                        player.player_team_tournament.player.person
                        if player.player_team_tournament
                        and player.player_team_tournament.player
                        else None
                    ),
                    "is_starting": player.is_starting,
                    "starting_type": player.starting_type,
                }
            )

        return players_with_data
```

### Many-to-Many Relationship Loading

```python
async def get_players_by_team_id_tournament_id(
    self,
    team_id: int,
    tournament_id: int,
) -> list[PlayerTeamTournamentDB]:
    async with self.db.get_session_maker()() as session:
        stmt = (
            select(PlayerTeamTournamentDB)
            .where(PlayerTeamTournamentDB.team_id == team_id)
            .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
        )

        results = await session.execute(stmt)
        return results.scalars().all()
```

### Custom Relationship Queries with Subqueries

```python
async def _get_available_players(
    self, session, team_id: int, tournament_id: int, match_id: int
) -> list[dict]:
    subquery = (
        select(PlayerMatchDB.player_team_tournament_id)
        .where(PlayerMatchDB.match_id == match_id)
        .where(PlayerMatchDB.team_id == team_id)
    )

    stmt = (
        select(PlayerTeamTournamentDB)
        .where(PlayerTeamTournamentDB.team_id == team_id)
        .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
        .where(~PlayerTeamTournamentDB.id.in_(subquery))
        .options(
            selectinload(PlayerTeamTournamentDB.player).selectinload(PlayerDB.person),
            selectinload(PlayerTeamTournamentDB.position),
            selectinload(PlayerTeamTournamentDB.team),
        )
    )

    results = await session.execute(stmt)
    available_players = results.scalars().all()

    return [
        {
            "id": pt.id,
            "player_id": pt.player_id,
            "player_team_tournament": pt,
            "person": pt.player.person if pt.player else None,
            "position": pt.position,
            "team": pt.team,
        }
        for pt in available_players
    ]
```

### Joins with Nullable Foreign Keys

Use `outerjoin()` when the FK is nullable:

```python
base_query = (
    select(PlayerTeamTournamentDB)
    .join(PlayerDB, PlayerTeamTournamentDB.player_id == PlayerDB.id)
    .join(PersonDB, PlayerDB.person_id == PersonDB.id)
    .outerjoin(TeamDB, PlayerTeamTournamentDB.team_id == TeamDB.id)
    .where(PlayerTeamTournamentDB.tournament_id == tournament_id)
)
```

Rules:

- `join()` for required relationships (non-nullable FK)
- `outerjoin()` for optional relationships (nullable FK)

### Nested Related Items Using Service Registry

```python
async def get_sponsors_of_tournament_sponsor_line(self, tournament_id: int) -> list[SponsorDB]:
    sponsor_line_service = self.service_registry.get("sponsor_line")
    return await self.get_nested_related_item_by_id(
        tournament_id,
        sponsor_line_service,
        "sponsor_line",
        "sponsors",
    )
```

### Complex Multi-Query Assembly

```python
async def get_match_full_context(self, match_id: int) -> dict | None:
    async with self.db.get_session_maker()() as session:
        stmt = (
            select(MatchDB)
            .where(MatchDB.id == match_id)
            .options(
                selectinload(MatchDB.team_a),
                selectinload(MatchDB.team_b),
                selectinload(MatchDB.tournaments),
            )
        )

        result = await session.execute(stmt)
        match = result.scalar_one_or_none()
        if not match:
            return None

        tournament = match.tournaments if match.tournaments else None

        if tournament:
            stmt_sport = (
                select(SportDB)
                .where(SportDB.id == tournament.sport_id)
                .options(selectinload(SportDB.positions))
            )
            result_sport = await session.execute(stmt_sport)
            sport = result_sport.scalar_one_or_none()
        else:
            sport = None

        stmt_players = (
            select(PlayerMatchDB)
            .where(PlayerMatchDB.match_id == match_id)
            .options(
                selectinload(PlayerMatchDB.player_team_tournament)
                .selectinload(PlayerTeamTournamentDB.player)
                .selectinload(PlayerDB.person),
                selectinload(PlayerMatchDB.match_position),
                selectinload(PlayerMatchDB.team),
            )
        )
        result_players = await session.execute(stmt_players)
        player_matches = result_players.scalars().all()

        home_available = await self._get_available_players(
            session, match.team_a_id, match.tournament_id, match_id
        )
        away_available = await self._get_available_players(
            session, match.team_b_id, match.tournament_id, match_id
        )

        return {
            "match": match.__dict__,
            "teams": {
                "home": match.team_a.__dict__ if match.team_a else None,
                "away": match.team_b.__dict__ if match.team_b else None,
            },
            "sport": {
                **(sport.__dict__ if sport else {}),
                "positions": [pos.__dict__ for pos in sport.positions] if sport else [],
            },
            "tournament": tournament.__dict__ if tournament else None,
            "players": {
                "home_roster": home_roster,
                "away_roster": away_roster,
                "available_home": home_available,
                "available_away": away_available,
            },
        }
```

### Pagination with Relationship Loading

```python
async def get_matches_by_tournament_with_pagination(
    self,
    tournament_id: int,
    skip: int = 0,
    limit: int = 20,
    order_exp: str = "id",
    order_exp_two: str = "id",
):
    return await self.get_related_item_level_one_by_id(
        tournament_id,
        "matches",
        skip=skip,
        limit=limit,
        order_by=order_exp,
        order_by_two=order_exp_two,
    )
```

### Base Utility Methods from RelationshipMixin

| Method | Purpose |
|--------|---------|
| `get_related_item_level_one_by_id(id, relation)` | Fetch level 1 relationships with `selectinload()` |
| `get_nested_related_item_by_id(id, service, rel1, rel2)` | Fetch 2-level nested relationships |
| `create_m2m_relation(...)` | Create many-to-many relationships |
| `find_relation(...)` | Check if relation exists in junction table |
| `get_related_items(...)` | Fetch related items with optional property loading |
| `get_related_items_by_two(...)` | Fetch 2-level relationships using `joinedload()` |
| `first_or_none(result)` | Normalize relationship query results |

### Key Patterns Summary

1. `selectinload()` for eager loading
2. Chained `selectinload()` for deep relationships
3. Base mixin methods for common queries
4. Direct `select()` for many-to-many junction tables
5. Subqueries with `NOT IN` for exclusion queries
6. Service registry for cross-service relationships
7. Multi-query assembly for composite structures

## Combined Pydantic Schemas

See `docs/schemas/index.md` for:

- Full list of combined schemas
- How to use them in endpoints
- Creating new complex schemas
- Performance considerations
- Best practices

Quick reference:

| Schema | Endpoint | Use Case |
|--------|----------|-----------|
| `MatchWithDetailsSchema` | `GET /api/matches/{id}/with-details/` | Full match display |
| `TeamWithDetailsSchema` | `GET /api/teams/{id}/with-details/` | Team page with sport, sponsors |
| `TournamentWithDetailsSchema` | `GET /api/tournaments/{id}/with-details/` | Tournament page with all teams |
| `PlayerTeamTournamentWithDetailsAndPhotosSchema` | `GET /api/players_team_tournament/tournament/{tournament_id}/players/paginated/details-with-photos` | Tournament player list with photos |

Example pattern:

```python
async def get_match_with_details(self, match_id: int) -> MatchDB | None:
    async with self.db.get_session_maker()() as session:
        stmt = (
            select(MatchDB)
            .where(MatchDB.id == match_id)
            .options(
                joinedload(MatchDB.team_a),
                joinedload(MatchDB.team_b),
                joinedload(MatchDB.tournaments),
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

@router.get("/id/{match_id}/with-details/", response_model=MatchWithDetailsSchema)
async def get_match_with_details_endpoint(match_id: int):
    match = await self.service.get_match_with_details(match_id)
    return MatchWithDetailsSchema.model_validate(match)
```
