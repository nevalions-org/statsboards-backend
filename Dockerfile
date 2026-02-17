# Build stage
FROM python:3.12.3-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Install Poetry
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry
RUN poetry config virtualenvs.create false

# Install all dependencies (including dev/test/prod)
RUN poetry install --no-interaction --no-ansi --no-root

# Production stage
FROM python:3.12.3-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -u 1000 appuser

WORKDIR /app

# Create directories with correct ownership
RUN mkdir -p /app && chown -R appuser:appuser /app

# Copy dependencies and application from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --chown=appuser:appuser alembic.ini /app/alembic.ini
COPY --chown=appuser:appuser alembic /app/alembic
COPY --chown=appuser:appuser src /app/src

WORKDIR /app

# Expose port
EXPOSE 9000

# Use environment variables from the .env file
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:9000/health || exit 1

# Default command: Production mode with 4 workers
# For dev mode, override with: python -m src.runserver
CMD ["python", "-m", "src.run_prod_server"]
