"""Application configuration management using Pydantic settings."""

import os
from pathlib import Path
from typing import Self

from dotenv import load_dotenv
from pydantic import Field, PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.exceptions import ConfigurationError
from src.logging_config import get_logger

load_dotenv()
logger = get_logger("config")


class DbSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="DB_", extra="allow")
    host: str = ""
    user: str = ""
    password: str = ""
    name: str = ""
    port: int = 5432

    @field_validator("host", "user", "password", "name")
    @classmethod
    def validate_not_empty(cls, v: str, info) -> str:
        """
        Validate that database configuration fields are not empty.

        Args:
            v: Field value to validate.
            info: Pydantic field info object.

        Returns:
            str: Stripped field value.

        Raises:
            ConfigurationError: If field is empty or whitespace only.
        """
        if not v or not v.strip():
            raise ConfigurationError(
                f"Database {info.field_name} cannot be empty",
                {"field": info.field_name},
            )
        return v.strip()

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """
        Validate that database port is within valid range.

        Args:
            v: Port number to validate.

        Returns:
            int: Validated port number.

        Raises:
            ConfigurationError: If port is not between 1 and 65535.
        """
        if not 1 <= v <= 65535:
            raise ConfigurationError(
                f"Database port must be between 1 and 65535, got {v}",
                {"port": v},
            )
        return v

    @property
    def db_url(self) -> str:
        """
        Build PostgreSQL database connection URL for async operations.

        Returns:
            str: Database connection URL with asyncpg driver.
        """
        url = str(
            PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                path=self.name,
            )
        )
        return url

    def db_url_websocket(self) -> str:
        """
        Build PostgreSQL database connection URL for WebSocket operations.

        Returns:
            str: Database connection URL for WebSocket.
        """
        url = str(
            PostgresDsn.build(
                scheme="postgresql",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                path=self.name,
            )
        )
        return url

    def validate_connection_string(self) -> None:
        """
        Validate that the database connection string can be built.

        Raises:
            ConfigurationError: If connection string is invalid.
        """
        try:
            self.db_url
        except Exception as ex:
            raise ConfigurationError(
                f"Invalid database connection string: {ex}",
                {
                    "host": self.host,
                    "port": self.port,
                    "database": self.name,
                    "user": self.user,
                },
            ) from ex


class TestDbSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.test.local", env_prefix="DB_TEST_", extra="allow"
    )
    host: str = ""
    user: str = ""
    password: str = ""
    name: str = ""
    port: int = 5432

    @field_validator("host", "user", "password", "name")
    @classmethod
    def validate_not_empty(cls, v: str, info) -> str:
        """
        Validate that test database configuration fields are not empty.

        Args:
            v: Field value to validate.
            info: Pydantic field info object.

        Returns:
            str: Stripped field value.

        Raises:
            ConfigurationError: If field is empty or whitespace only.
        """
        if not v or not v.strip():
            raise ConfigurationError(
                f"Test database {info.field_name} cannot be empty",
                {"field": info.field_name},
            )
        return v.strip()

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """
        Validate that test database port is within valid range.

        Args:
            v: Port number to validate.

        Returns:
            int: Validated port number.

        Raises:
            ConfigurationError: If port is not between 1 and 65535.
        """
        if not 1 <= v <= 65535:
            raise ConfigurationError(
                f"Test database port must be between 1 and 65535, got {v}",
                {"port": v},
            )
        return v

    @property
    def test_db_url(self) -> str:
        """
        Build test PostgreSQL database connection URL for async operations.

        Returns:
            str: Test database connection URL with asyncpg driver.
        """
        url = str(
            PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                path=self.name,
            )
        )
        return url

    def test_db_url_websocket(self) -> str:
        """
        Build test PostgreSQL database connection URL for WebSocket operations.

        Returns:
            str: Test database connection URL for WebSocket.
        """
        url = str(
            PostgresDsn.build(
                scheme="postgresql",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                path=self.name,
            )
        )
        return url

    def validate_connection_string(self) -> None:
        """
        Validate that the test database connection string can be built.

        Raises:
            ConfigurationError: If connection string is invalid.
        """
        try:
            self.test_db_url
        except Exception as ex:
            raise ConfigurationError(
                f"Invalid test database connection string: {ex}",
                {
                    "host": self.host,
                    "port": self.port,
                    "database": self.name,
                    "user": self.user,
                },
            ) from ex


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="allow")
    db: DbSettings = Field(default_factory=DbSettings)
    test_db: TestDbSettings = Field(default_factory=TestDbSettings)
    db_echo: bool = False
    allowed_origins: str = Field(
        default="*", description="Comma-separated list of allowed CORS origins"
    )
    ssl_keyfile: str | None = Field(default=None, description="Path to SSL private key file")
    ssl_certfile: str | None = Field(default=None, description="Path to SSL certificate file")
    current_season_id: str | None = Field(
        default=None, description="Current season ID for EESL parsing"
    )
    logs_config: str = Field(
        default="logging-config_info.yaml",
        description="Logging configuration file name",
    )
    static_main_path_str: str = Field(default="static", description="Static files path")
    secret_key: str = Field(
        default="your-secret-key-here-change-in-production",
        description="Secret key for JWT token generation",
    )
    algorithm: str = Field(
        default="HS256",
        description="JWT algorithm",
    )
    access_token_expire_minutes: int = Field(
        default=60 * 24,
        description="Access token expiration time in minutes (default 1 day)",
    )
    rate_limit_requests_per_second: float = Field(
        default=0.5,
        description="Rate limit for requests per second",
    )
    rate_limit_max_concurrent: int = Field(
        default=5,
        description="Maximum number of concurrent requests",
    )
    proxy_list: str | None = Field(
        default=None,
        description="Comma-separated list of proxy URLs (e.g., http://user:pass@host:port,http://host2:port2)",
    )
    proxy_timeout: int = Field(
        default=10,
        description="Timeout for proxy connections in seconds",
    )
    proxy_max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts when a proxy fails",
    )
    proxy_source_urls: str | None = Field(
        default="https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        description="Comma-separated list of proxy source URLs to fetch from",
    )
    proxy_source_cache_ttl: int = Field(
        default=3600,
        description="Proxy source cache time-to-live in seconds (default: 1 hour)",
    )
    proxy_source_max_proxies: int = Field(
        default=100,
        description="Maximum number of proxies to fetch from sources",
    )
    proxy_source_fetch_timeout: int = Field(
        default=10,
        description="Timeout for fetching proxy sources in seconds",
    )
    stats_throttle_seconds: int = Field(
        default=2,
        description="Minimum seconds between statistics broadcasts",
    )
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL for pub/sub",
    )

    @property
    def static_main_path(self) -> Path:
        """Get the static main path as a Path object."""
        return Path(self.static_main_path_str)

    @property
    def uploads_path(self) -> Path:
        """Get the uploads path as a Path object."""
        return self.static_main_path / "uploads"

    @field_validator("allowed_origins")
    @classmethod
    def validate_allowed_origins(cls, v: str) -> str:
        """
        Validate CORS allowed origins format.

        Args:
            v: Comma-separated list of origins or "*".

        Returns:
            str: Validated origins string.

        Raises:
            ConfigurationError: If origin format is invalid.
        """
        if v == "*":
            return v
        origins = [origin.strip() for origin in v.split(",")]
        for origin in origins:
            if not origin:
                raise ConfigurationError(
                    "Empty origin found in ALLOWED_ORIGINS",
                    {"allowed_origins": v},
                )
            if not origin.startswith(("http://", "https://", "*")):
                raise ConfigurationError(
                    f"Invalid origin format: {origin}. Must start with http://, https://, or *",
                    {"allowed_origins": v},
                )
        return v

    @model_validator(mode="after")
    def validate_ssl_files(self) -> Self:
        """
        Validate that SSL files are both provided or both absent.

        SSL is optional for Kubernetes deployments where TLS is handled by Ingress.
        For Docker Compose/local deployments, SSL can be enabled via environment variables.

        Returns:
            Self: Validated settings instance.

        Raises:
            ConfigurationError: If only one SSL file is provided.
        """
        # Allow SSL to be optional - both set or neither set is valid
        # This enables running behind Ingress (Kubernetes) or with SSL (Docker Compose)
        if bool(self.ssl_keyfile) != bool(self.ssl_certfile):
            raise ConfigurationError(
                "Both SSL_KEYFILE and SSL_CERTFILE must be provided or neither",
                {
                    "ssl_keyfile": self.ssl_keyfile,
                    "ssl_certfile": self.ssl_certfile,
                },
            )
        return self

    def validate_paths_exist(self) -> None:
        """Validate that all required paths exist and are accessible."""
        validation_errors = []

        # Create runtime directories if they are missing.
        # In containerized deployments, /app/static may not exist in the image by default.
        for path_name, path_value in (
            ("static_main_path", self.static_main_path),
            ("uploads_path", self.uploads_path),
        ):
            if not path_value.exists():
                try:
                    path_value.mkdir(parents=True, exist_ok=True)
                except Exception as ex:
                    validation_errors.append(
                        f"Failed to create directory '{path_name}' at {path_value}: {ex}"
                    )

        paths_to_validate = [
            ("static_main_path", str(self.static_main_path), "required"),
            ("uploads_path", str(self.uploads_path), "required"),
        ]

        # SSL termination is typically handled by Ingress in Kubernetes.
        # Keep SSL file checks non-blocking so stale env values do not crash startup.
        if self.ssl_keyfile:
            paths_to_validate.append(("ssl_keyfile", self.ssl_keyfile, "optional"))
        if self.ssl_certfile:
            paths_to_validate.append(("ssl_certfile", self.ssl_certfile, "optional"))

        for path_name, path_value, requirement in paths_to_validate:
            if path_value and not Path(path_value).exists():
                if requirement == "required":
                    validation_errors.append(
                        f"Required path '{path_name}' does not exist: {path_value}"
                    )
                else:
                    logger.warning(f"Optional path '{path_name}' does not exist: {path_value}")
            elif path_value and not os.access(path_value, os.R_OK):
                if requirement == "required":
                    validation_errors.append(f"Path '{path_name}' is not readable: {path_value}")
                else:
                    logger.warning(f"Optional path '{path_name}' is not readable: {path_value}")

        if validation_errors:
            raise ConfigurationError(
                "Path validation failed",
                {"errors": validation_errors},
            )

    def validate_database_settings(self) -> None:
        """Validate database connection strings."""
        if not os.getenv("TESTING"):
            self.db.validate_connection_string()
        self.test_db.validate_connection_string()

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
        except ConfigurationError as ex:
            self.logger.error(
                f"Path validation failed: {ex.message}; details: {ex.details}",
                exc_info=True,
            )
            raise

        try:
            self.validate_database_settings()
            self.logger.info("Database settings validation successful")
        except ConfigurationError as ex:
            self.logger.error(f"Database settings validation failed: {ex.message}", exc_info=True)
            raise

        self.logger.info("Configuration validation complete")


settings = Settings()
