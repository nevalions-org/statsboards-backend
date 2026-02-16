# Configuration Validation Implementation

## Overview

Implemented comprehensive configuration validation for the StatsBoard Backend application. This includes Pydantic validators for critical settings, path validation, database connectivity checks, and startup validation.

## Changes Made

### 1. Enhanced Configuration Validation (`src/core/config.py`)

#### Database Settings Validators

Added Pydantic validators to `DbSettings` and `TestDbSettings`:

- **Empty string validation**: Ensures `host`, `user`, `password`, and `name` fields are not empty
- **Port range validation**: Validates that port is between 1 and 65535
- **Whitespace trimming**: Automatically trims whitespace from field values
- **Connection string validation**: Validates that database connection strings are valid

```python
@field_validator("host", "user", "password", "name")
@classmethod
def validate_not_empty(cls, v: str, info) -> str:
    if not v or not v.strip():
        raise ConfigurationError(
            f"Database {info.field_name} cannot be empty",
            {"field": info.field_name},
        )
    return v.strip()
```

#### Application Settings Validators

Added validators to `Settings` class:

- **CORS origins validation**: Validates `allowed_origins` format
  - Accepts `*` for all origins
  - Validates individual origins start with `http://`, `https://`, or `*`
  - Rejects empty origins in the list

- **SSL files validation**: Ensures both SSL key and cert are provided together or neither

```python
@model_validator(mode="after")
def validate_ssl_files(self) -> Self:
    if bool(self.ssl_keyfile) != bool(self.ssl_certfile):
        raise ConfigurationError(
            "Both SSL_KEYFILE and SSL_CERTFILE must be provided or neither",
            {
                "ssl_keyfile": self.ssl_keyfile,
                "ssl_certfile": self.ssl_certfile,
            },
        )
    return self
```

#### Path Validation

Implemented comprehensive path validation in `validate_paths_exist()` method:

**Required paths**:

- `static_main_path`: Static files directory
- `uploads_path`: Uploads directory

**Optional paths** (logged as warnings if missing):

- `template_path`: Frontend template directory
- `static_path`: Frontend static directory
- `ssl_keyfile`: SSL private key (if SSL is configured)
- `ssl_certfile`: SSL certificate (if SSL is configured)

Validation checks:

- Path existence
- Readability permissions

#### Comprehensive Validation Method

Added `validate_all()` method that runs all validations:

1. Path validation
2. Database settings validation
3. Detailed logging for each validation step

```python
def validate_all(self) -> None:
    """
    Perform all configuration validations.

    This should be called during application startup to ensure
    all configuration is valid before the application starts.
    """
    self.logger = get_logger("config", self)
    self.logger.info("Starting configuration validation")

    try:
        self.validate_paths_exist()
        self.logger.info("Path validation successful")
    except ConfigurationError as e:
        self.logger.error(f"Path validation failed: {e.message}", exc_info=True)
        raise

    try:
        self.validate_database_settings()
        self.logger.info("Database settings validation successful")
    except ConfigurationError as e:
        self.logger.error(f"Database settings validation failed: {e.message}", exc_info=True)
        raise

    self.logger.info("Configuration validation complete")
```

### 2. Enhanced Database Connection Validation (`src/core/models/base.py`)

Added `validate_database_connection()` method to `Database` class:

- Runs basic connection test
- Retrieves and logs PostgreSQL version
- Retrieves and logs current database name
- Retrieves and logs current user
- Comprehensive error handling and logging

```python
async def validate_database_connection(self) -> None:
    """
    Validate database connectivity on startup.
    Performs comprehensive checks including connection test and basic query execution.
    """
    self.logger.info("Starting database connection validation")

    try:
        await self.test_connection()

        async with self.engine.connect() as connection:
            result = await connection.execute(text("SELECT version()"))
            version = result.scalar()
            self.logger.info(f"Connected to PostgreSQL version: {version}")

            result = await connection.execute(text("SELECT current_database()"))
            db_name = result.scalar()
            self.logger.info(f"Using database: {db_name}")

            result = await connection.execute(text("SELECT current_user"))
            user = result.scalar()
            self.logger.info(f"Connected as user: {user}")

        self.logger.info("Database connection validation complete")
    except Exception as e:
        self.logger.critical(
            f"Database connection validation failed: {e}", exc_info=True
        )
        raise
```

### 3. Application Startup Integration (`src/main.py`)

Integrated configuration validation into FastAPI lifespan:

```python
@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Args:
        _app (FastAPI): The FastAPI application instance (unused).
    """
    db_logger.info("Starting application lifespan.")
    try:
        settings.validate_all()
        init_service_registry(db)
        register_all_services(db)
        logger.info("Service registry initialized and all services registered")
        await db.validate_database_connection()
        yield
    except Exception as e:
        db_logger.critical(f"Critical error during startup: {e}", exc_info=True)
        raise e
    finally:
        db_logger.info("Shutting down application lifespan after test connection.")
        await db.close()
```

### 4. Validation Script (`validate_config.py`)

Created standalone configuration validation script:

- Can be run independently to validate configuration
- Returns appropriate exit codes (0 for success, 1 for failure)
- Useful for CI/CD pipelines and pre-startup checks

```bash
./validate_config.py
```

### 5. Comprehensive Test Coverage

Created extensive test suites:

#### Configuration Tests (`tests/test_config.py`)

- 26 tests covering all configuration validators
- Database settings validation tests
- Application settings validation tests
- Path validation tests
- SSL configuration tests
- CORS origins validation tests

#### Database Connection Validation Tests (`tests/test_database_connection_validation.py`)

- 4 tests for database connection validation
- Version retrieval test
- Database name retrieval test
- Current user retrieval test

## Benefits

1. **Early Error Detection**: Configuration issues are caught at startup before the application accepts requests
2. **Clear Error Messages**: Detailed error messages with context help developers identify and fix issues quickly
3. **Comprehensive Validation**: Covers all critical configuration aspects
4. **Logging Integration**: All validation steps are logged for debugging and monitoring
5. **Type Safety**: Leverages Pydantic's type system for robust validation
6. **Testing**: Full test coverage ensures validation logic is correct
7. **Standalone Validation**: Script can be used in CI/CD pipelines
8. **Graceful Handling**: Optional paths are logged as warnings rather than errors

## WebSocket Settings

### Statistics Throttling

Added `stats_throttle_seconds` configuration setting to limit statistics broadcast frequency:

- **Setting**: `stats_throttle_seconds`
- **Type**: `int`
- **Default**: `2`
- **Description**: Minimum seconds between statistics broadcasts
- **Purpose**: Prevents excessive CPU usage during high-frequency event periods

When football events occur rapidly (10-30 events per minute), this throttle ensures statistics are only recalculated and broadcast at most once every 2 seconds (configurable).

**Environment Variable**: `STATS_THROTTLE_SECONDS`

Example:

```bash
# Default: 2 seconds
export STATS_THROTTLE_SECONDS=2

# For more aggressive throttling: 5 seconds
export STATS_THROTTLE_SECONDS=5
```

## Database Connection Pool Settings

### Pool Configuration

Connection pool settings are configurable via environment variables:

| Environment Variable   | Description              | Production Default | Test Default |
| ---------------------- | ------------------------ | ------------------ | ------------ |
| `DB_POOL_SIZE`         | Base pool size           | 5                  | 3            |
| `DB_POOL_MAX_OVERFLOW` | Max overflow connections | 5                  | 5            |
| (hardcoded)            | `pool_timeout`           | 30s                | 30s          |

**Total max connections**: `pool_size + max_overflow` (10 for production, 8 for test)

### Pool Timeout Handling

The `get_connection_with_retry()` function provides exponential backoff retry on pool exhaustion:

```python
from src.core.models.base import get_connection_with_retry

session = await get_connection_with_retry(
    session_maker=db.get_session_maker(),
    max_retries=3,
    base_delay=0.1,
)
```

**Behavior**:

- Attempts to acquire a connection up to `max_retries` times
- Uses exponential backoff: `delay = base_delay * (2 ** attempt)`
- Logs warnings on each retry attempt
- Raises `PoolTimeout` if all retries exhausted

**Example retry sequence** (default settings):

1. Attempt 1: Immediate
2. Attempt 2: Wait 0.1s
3. Attempt 3: Wait 0.2s
4. Raise `PoolTimeout`

### DB Concurrency Semaphore

A semaphore limits concurrent DB operations in fetch helpers:

```python
# src/helpers/fetch_helpers.py
_db_semaphore = asyncio.Semaphore(10)

async def fetch_with_scoreboard_data(match_id: int, ...):
    async with _db_semaphore:
        # Concurrent DB operations limited to 10
        ...
```

This prevents connection pool exhaustion during batch operations.

## Usage

### Automatic Validation

Configuration validation runs automatically when the application starts via the FastAPI lifespan.

### Manual Validation

To validate configuration manually:

```bash
python validate_config.py
```

### Environment Variables

Configuration is loaded from environment variables:

- `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT`: Main database settings
- `DB_TEST_HOST`, `DB_TEST_USER`, `DB_TEST_PASSWORD`, `DB_TEST_NAME`, `DB_TEST_PORT`: Test database settings
- `ALLOWED_ORIGINS`: CORS origins (comma-separated)
- `SSL_KEYFILE`, `SSL_CERTFILE`: SSL certificate paths
- `CURRENT_SEASON_ID`: Current season ID
- `LOGS_CONFIG`: Logging configuration file name

## Error Handling

All configuration validation errors raise `ConfigurationError` from `src.core.exceptions`, which includes:

- Clear error message
- Details dictionary with context information
- Proper logging at appropriate levels

## Testing

Run configuration validation tests:

```bash
pytest tests/test_config.py -v
pytest tests/test_database_connection_validation.py -v
```

## Notes

- Frontend paths are optional and logged as warnings if missing (allows backend to run without frontend)
- Main database validation is skipped when `TESTING` environment variable is set
- SSL files are optional but must be provided together if used
- All validations run before service initialization and database connection
