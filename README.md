# StatsBoards Backend

#

A FastAPI-based backend service for sports statistics and scoreboarding, designed to manage multi-sport tournaments, matches, teams, and real-time game data.

## Supported Sports

Currently implements:

- **American Football** - Full game statistics including downs, yards, plays, and scoring
- **Flag Football** - Adapted rules and statistics for flag football leagues
- **Extensible Architecture** - Easy to add new sports through the sport management system

## Features

- **Real-time Updates**: WebSocket support for live match data, scoreboards, and game clocks
- **Multi-Sport Support**: Manages American football, flag football, and extensible for other sports
- **Comprehensive Data Management**: Teams, players, tournaments, seasons, and detailed match statistics
- **RESTful API**: Well-structured API endpoints following OpenAPI specifications
- **Database Integration**: PostgreSQL with async SQLAlchemy ORM
- **External Data Integration**: EESL system integration for data synchronization
- **File Management**: Secure file upload and static file serving
- **Production Ready**: Docker deployment with nginx reverse proxy and SSL support
- **Robust Error Handling**: Structured exception hierarchy and global error handlers
- **Comprehensive Logging**: Standardized logging levels across all services
- **Service Registry Pattern**: Dependency injection for decoupled service architecture
- **Configuration Validation**: Automatic validation of database settings, paths, and environment variables on startup

## Tech Stack

- **Backend**: FastAPI, Python 3.11+
- **Database**: PostgreSQL with asyncpg
- **ORM**: SQLAlchemy 2.0 (async)
- **Real-time**: WebSockets, Redis pub/sub
- **Testing**: pytest with pytest-asyncio
- **Deployment**: Docker, Gunicorn, Nginx
- **Code Quality**: Ruff
- **Configuration**: Pydantic Settings with validation

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL
- Docker and Docker Compose (for testing and deployment)

### Installation

1. **Clone repository**

   ```bash
   git clone <repository-url>
   cd statsboards-backend
   ```

2. **Create and activate virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   poetry install
   ```

4. **Set up environment**

   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Set up database**

   ```bash
   # Apply database migrations
   alembic upgrade head
   ```

6. **Run development server**
   ```bash
   python -m src.runserver
   ```

The API will be available at `http://localhost:9000`

## API Documentation

Once running, visit:

- **Swagger UI**: `http://localhost:9000/docs`
- **ReDoc**: `http://localhost:9000/redoc`

## Development

### Running Tests

```bash
# Start test database (creates 4 databases for parallel execution: test_db, test_db2, test_db3, test_db4)
docker-compose -f docker-compose.test-db-only.yml up -d

# Run all tests (4 workers across 4 databases by default)
pytest

# Run tests for specific directory
pytest tests/test_db_services/
pytest tests/test_views/

# Run a single test file
pytest tests/test_db_services/test_tournament_service.py

# Run a specific test function
pytest tests/test_db_services/test_tournament_service.py::TestTournamentServiceDB::test_create_tournament_with_relations

# Run tests with coverage
pytest --cov=src

# Run tests with live logs enabled (for debugging)
pytest tests/test_db_services/test_tournament_service.py::TestTournamentServiceDB::test_create_tournament_with_relations -o log_cli=true
```

**Important:**

- Test database must be started before running tests: `docker-compose -f docker-compose.test-db-only.yml up -d`
- Tests use 4 parallel databases (test_db, test_db2, test_db3, test_db4) with 4 workers for faster execution
- Tests use PostgreSQL (not SQLite) to ensure production compatibility
- The `pytest.ini` file includes performance optimizations: `-x --tb=short -n 4`
- Database echo is disabled in test fixtures for faster execution

### Code Quality

```bash
# Lint with Ruff
source venv/bin/activate && ruff check src/ tests/

# Auto-fix Ruff issues where possible
source venv/bin/activate && ruff check --fix src/ tests/
```

**Note:** After making code changes, run the linting command to ensure code quality standards are met. Ruff provides fast Python linting with automatic fixing capabilities.

### Database Migrations

```bash
# Create new migration
poetry run alembic revision --autogenerate -m "Add new feature"

# Apply migrations
poetry run alembic upgrade head

# Check current version
poetry run alembic current
```

## Docker Deployment

### Development

```bash
docker-compose -f docker-compose.dev.yml up --build
```

### Production

```bash
docker-compose -f docker-compose.prod.yml up --build
```

### Testing

```bash
# Start test database only
docker-compose -f docker-compose.test-db-only.yml up -d

# Run tests against test database
pytest

# Stop test database
docker-compose -f docker-compose.test-db-only.yml down
```

### Database Testing

```bash
# Full test environment with database in container
docker-compose -f docker-compose.test.yml up --build
```

## Project Structure

```
src/
├── core/                        # Core application components
│   ├── config.py               # Configuration management
│   ├── models/                 # SQLAlchemy database models and mixins
│   │   ├── mixins/            # CRUD, query, and relationship mixins
│   │   └── [model files]     # Database model definitions
│   ├── exceptions.py          # Custom exception hierarchy
│   ├── exception_handler.py    # Global exception handlers
│   ├── base_router.py         # Base FastAPI router classes
│   ├── service_registry.py    # Service registry for dependency injection
│   └── service_initialization.py  # Service registration
│ ├── teams/                      # Team management
│   ├── db_services.py         # Team service layer
│   ├── schemas.py            # Pydantic schemas
│   └── views.py              # API endpoints
│ ├── matches/                    # Match management and scheduling
│ ├── players/                    # Player data and statistics
│ ├── tournaments/                # Tournament organization
│ ├── seasons/                    # Season management
│ ├── scoreboards/                # Scoreboard management
│ ├── clocks/                     # Centralized clock orchestration
│ │   ├── clock_orchestrator.py   # Single loop for all running clocks
│ │   └── __init__.py
│ ├── gameclocks/                 # Game clock functionality
│ ├── playclocks/                 # Play clock functionality
│ ├── matchdata/                  # Real-time match data
│ ├── football_events/            # Game event tracking
│ ├── sponsors/                   # Sponsor management
│ ├── helpers/                    # Utility functions
│   ├── file_service.py        # File upload/download
│   ├── image_processing_service.py
│   └── upload_service.py
│ ├── pars_eesl/                  # EESL data integration
│ ├── utils/                      # WebSocket handlers
│ ├── logging_config.py           # Logging configuration
│ ├── main.py                     # FastAPI application entry point
│ ├── runserver.py                # Development server
│ └── run_prod_server.py          # Production server
tests/                          # Test suite
├── test_db_services/          # Service layer tests
├── test_views/                # API endpoint tests
└── [test files]
AGENTS.md                       # Quick reference for AI assistants
README.md                       # Project overview and setup
docs/                          # Documentation
    ├── API_DOCUMENTATION.md             # API endpoints reference
    ├── architecture/index.md          # Architecture documentation index
    ├── CONFIGURATION_VALIDATION.md      # Configuration settings and validation
    ├── development/index.md            # Development guidelines index
    ├── schema-standardization/index.md # Schema standardization plan index
    ├── PG_TRGM_SEARCH_OPTIMIZATION.md # PostgreSQL search optimization guide
    ├── ROUTER_REGISTRY.md             # Router registry system
    └── SERVICE_LAYER_DECOUPLING.md    # Service layer architecture
```

## Key Features

### Real-time Capabilities

- Live match data streaming via WebSockets
- Real-time scoreboard updates
- Game clock and play clock synchronization via centralized ClockOrchestrator
- Event notifications for football events
- Match data queue management for concurrent updates

### Data Management

- Complex many-to-many relationships (teams-tournaments, players-teams)
- Player statistics tracking across matches and tournaments
- Tournament and season management
- Sponsor and advertisement management with logo uploads
- Position and sport management (American football, flag football)
- Match data with detailed event tracking (downs, yards, plays, scores, turnovers)

### Error Handling & Logging

- Structured custom exception hierarchy
- Global exception handlers with appropriate HTTP status codes
- Standardized logging levels across all services
- Detailed error messages for debugging
- Proper exception chaining with stack traces

### External Integration

- EESL system data parsing and synchronization
- Automated data import/export capabilities
- Tournament team and player data parsing
- Match data parsing from external sources

### Code Quality

- Consistent service layer pattern with `BaseServiceDB`
- Service registry pattern for dependency injection and decoupling
- Comprehensive test coverage (~76% overall, 1465 tests passing in ~160s with 4 parallel workers)
- Type hints throughout codebase
- Async/await for all database operations
- Mixin-based CRUD, query, and relationship operations

## API Endpoints

The API provides comprehensive endpoints for:

- **Sports**: `GET /api/sports/`, `POST /api/sports/`, `PUT /api/sports/{id}/`
- **Seasons**: `GET /api/seasons/`, `POST /api/seasons/`, `PUT /api/seasons/{id}/`
- **Tournaments**: `GET /api/tournaments/`, `POST /api/tournaments/`, `PUT /api/tournaments/{id}/`
- **Teams**: `GET /api/teams/`, `POST /api/teams/`, `PUT /api/teams/{id}/`
- **Players**: `GET /api/players/`, `POST /api/players/`, `PUT /api/players/{id}/`
- **Matches**: `GET /api/matches/`, `POST /api/matches/`, `PUT /api/matches/{id}/`
- **Scoreboards**: `GET /api/scoreboards/`, `POST /api/scoreboards/`, `PUT /api/scoreboards/{id}/`
- **Sponsors**: `GET /api/sponsors/`, `POST /api/sponsors/`, `PUT /api/sponsors/{id}/`
- **Positions**: `GET /api/positions/`, `POST /api/positions/`, `PUT /api/positions/{id}/`
- **Gameclocks**: `GET /api/gameclocks/`, `POST /api/gameclocks/`, `PUT /api/gameclocks/{id}/`
- **Playclocks**: `GET /api/playclocks/`, `POST /api/playclocks/`, `PUT /api/playclocks/{id}/`
- **Football Events**: `GET /api/football_events/`, `POST /api/football_events/`, `PUT /api/football_events/{id}/`
- **Global Settings**: `GET /api/settings/`, `POST /api/settings/`, `PUT /api/settings/{id}/`
  - Public: `GET /api/settings/grouped`, `GET /api/settings/category/{category}`, `GET /api/settings/value/{key}`
  - Admin-only: `POST /api/settings/`, `PUT /api/settings/{id}/`, `DELETE /api/settings/id/{id}`

### Combined Schema Endpoints

For endpoints that return full nested relationship data:

**Single Resource Endpoints:**

- **Matches with Details**: `GET /api/matches/{id}/with-details/`
- **Teams with Details**: `GET /api/teams/{id}/with-details/`
- **Tournaments with Details**: `GET /api/tournaments/{id}/with-details/`

**Paginated Search Endpoints:**

- **Matches with Details**: `GET /api/matches/with-details/paginated`
- **Teams with Details**: `GET /api/teams/with-details/paginated`
- **Tournaments with Details**: `GET /api/tournaments/with-details/paginated`

These endpoints return nested objects (teams, tournaments, sponsors) instead of just foreign key IDs, providing richer data in a single API call.

**See [docs/schemas/index.md](docs/schemas/index.md)** for detailed guide on:

- All available combined schemas
- When to use basic vs. combined schemas
- Creating new complex schemas with eager loading
- Performance optimization strategies
- Frontend integration examples
- Paginated combined schema usage

Each domain module follows the standard CRUD pattern with additional custom endpoints as needed.

## WebSocket Endpoints

For comprehensive WebSocket documentation including message formats, connection examples, and troubleshooting guide, see **[WebSocket Endpoints section in API_DOCUMENTATION.md](docs/API_DOCUMENTATION.md#websocket-endpoints)**.

**Note:** The following URLs are deprecated and no longer exist:

- ❌ `ws://localhost:9000/ws/matchdata/{match_id}` (does not exist)
- ❌ `ws://localhost:9000/ws/scoreboard/{scoreboard_id}` (does not exist)

**Actual WebSocket endpoints:**

- ✅ **Match Data**: `/api/matches/ws/id/{match_id}/{client_id}/` - Real-time match data, scores, clocks, events
- ✅ **Match Statistics**: `/api/matches/ws/matches/{match_id}/stats` - Real-time match statistics with conflict resolution

## Development Guidelines

For comprehensive development guidelines, coding standards and best practices, see:

- **[docs/development/index.md](docs/development/index.md)** - General development patterns, service layer, testing
- **[docs/schemas/index.md](docs/schemas/index.md)** - Using and creating combined schemas with nested relationships
- **[docs/schema-standardization/index.md](docs/schema-standardization/index.md)** - Schema standardization plan and checklist

These documents cover:

- Code style and naming conventions
- Service layer and router patterns
- Model patterns and relationship types
- Error handling patterns
- Database operations and relationship loading
- Search implementation guidelines
- Testing guidelines and test markers
- Configuration validation details

### Quick Reference

**Testing:**

```bash
# Start test database
docker-compose -f docker-compose.test-db-only.yml up -d
# Run tests
pytest
```

**Code Quality:**

```bash
# Lint with Ruff
source venv/bin/activate && ruff check src/ tests/
```

**Database Migrations:**

```bash
# Apply migrations
alembic upgrade head
```

## Configuration

### Environment Variables

Key environment variables (see `.env.example`):

- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `ALLOWED_ORIGINS`: CORS allowed origins
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

### Configuration Validation

The application includes comprehensive configuration validation that runs automatically on startup:

- **Database Settings**: Validates connection parameters and connectivity
- **Paths**: Checks required paths exist and are accessible
- **SSL Configuration**: Ensures SSL certificate files are provided together
- **CORS Origins**: Validates CORS origin format

Run configuration validation manually:

```bash
python validate_config.py
```

Configuration validation also runs automatically on application startup before services are initialized. See `docs/CONFIGURATION_VALIDATION.md` for complete documentation.

### Logging

Logging is configured via YAML files (`logging-config_dev.yaml`, `logging-config_info.yaml`). Call `setup_logging()` at module level in services and routers.

**Log Levels:**

- **debug**: Detailed operation tracking and successful HTTP requests
- **info**: Significant operations (creates, updates, application startup)
- **warning**: Expected but noteworthy situations (validation errors, 4xx HTTP responses)
- **error**: Unexpected errors (database errors, 5xx HTTP responses)
- **critical**: Unexpected errors that should rarely trigger

**Log Format:**

- Console: `[LEVEL] HH:MM:SS logger_name - message` (concise, level-first format)
- File: `YYYY-MM-DD HH:MM:SS [LEVEL] logger_name - message - Class: ClassName - Func: function_name` (detailed with context)
- Logs written to console and log files in `logs/` directory
- Use `exc_info=True` for exception logging to capture stack traces
- Successful HTTP requests logged at DEBUG level; errors logged at WARNING/ERROR level

## Contributing

1. Fork repository
2. Create a feature branch
3. Make your changes following the guidelines in `docs/development/index.md`
4. Add tests for new functionality
5. Run tests: `docker-compose -f docker-compose.test-db-only.yml up -d && pytest`
6. Lint code: `ruff check src/ tests/` (use `--fix` to auto-fix issues)
7. Ensure all tests pass
8. Submit a pull request

**Important:**

- All commits must be by linroot with email nevalions@gmail.com
- Follow existing code patterns and conventions
- Read `docs/development/index.md` for comprehensive development guidelines

## Recent Improvements

### Test Coverage Enhancements (January 2026)

- Added parser tests for EESL player-team-tournament parsing (STAB-85)
  - 10 new tests covering multiple players, single player, no players, malformed HTML
  - Fixed TypedDict bug: `player_number` changed from `int` to `str`
- Added service layer tests for matches (STAB-82)
  - 20 new tests covering match details, sport/teams retrieval, player data, scoreboard
- Added service layer tests for player_match (STAB-87)
  - 11 new tests covering create/update, retrieval by eesl_id, person/team lookup
- Added service layer tests for teams (STAB-86)
  - 15 new tests covering team details, search with pagination, player queries
- Total: 56 new tests added to improve coverage

### Test Suite Fixes

- Fixed 33 previously failing tests across multiple test suites
  - **test_player_match_views_helpers.py**: Fixed photo file handling tests (9 tests passing)
    - Corrected patch paths for uploads_path in tests
    - Updated photo_files_exist function signature for type safety
  - **test_utils.py**: Fixed logging and WebSocket manager tests (16 tests passing)
    - Fixed logging setup test to properly mock module-level variables
    - Added AsyncMock for async operations in WebSocket tests
    - Improved exception handling for connection failure scenarios
  - **test_views/test_websocket_views.py**: Fixed WebSocket functionality tests (47 tests passing)
    - Corrected import path for MatchDataWebSocketManager
    - Marked integration test requiring real database connections
    - **test_pars_integration.py**: Integration tests working with proper markers (5 tests passing)
    - Tests run correctly with `-m integration` flag
    - All 1465 tests now passing when excluding integration tests (in ~160s with 4 parallel workers)
- Integration tests pass when run with appropriate markers

### Exception Handling Refactoring

- Replaced generic `except Exception` clauses with specific exception types across all services
- Implemented custom exception hierarchy in `src/core/exceptions.py`
- Added global exception handlers with appropriate HTTP status codes
- Improved error detection and debugging capabilities
- Enhanced logging with appropriate levels for each exception type

### Logging Standardization

- Established clear logging guidelines with consistent levels
- Updated all database services to use appropriate log levels
- Changed successful operations from debug to info
- Changed validation/schema errors from error to warning
- Maintained error level for database failures
- Maintained critical level for unexpected errors

### Performance Optimizations

- Optimized match data fetching with parallel database calls
- Transaction rollback per test for faster execution
- Database echo disabled in test fixtures
- Session-scoped database engine for tests
- Improved query performance in service layer

### Testing Improvements

- 1465 comprehensive tests passing (in ~160s with 4 parallel workers across 4 databases)
- Test database optimization with Docker (creates test_db, test_db2, test_db3, test_db4)
- PostgreSQL requirement for tests (not SQLite)
- Factory pattern for test data generation
- Comprehensive test coverage across all services
- Added 56 new tests for parser and service layer coverage (STAB-85, STAB-82, STAB-87, STAB-86)
- Parallel testing stability improvements (STAB-174):
  - Replaced fcntl with filelock library for cross-platform compatibility
  - Fixed lock scope to prevent race conditions
  - Added PostgreSQL health check with pg_isready
  - Added stale lock file cleanup
  - Added pytest-random-order for detecting order-dependent tests

### Service Layer Decoupling

- Implemented service registry pattern for dependency injection
- Services no longer directly import or instantiate other services
- Centralized service registration in `src/core/service_initialization.py`
- Lazy initialization to avoid circular dependencies
- Improved testability through dependency injection
- See `docs/SERVICE_LAYER_DECOUPLING.md` for complete documentation

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions, please open an issue in the repository or contact the development team.
