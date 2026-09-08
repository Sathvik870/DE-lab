# ==============================================================================
# Pipeline as Code: ETL Pipeline & Testing Container
# Earbuds Data Engineering
# ==============================================================================

FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies required for psycopg2 compilation if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copy application code and tests
COPY etl/ ./etl/
COPY tests/ ./tests/
COPY scripts/ ./scripts/

# Default entrypoint runs test suite
CMD ["pytest", "tests/", "-v", "--cov=etl", "--cov-report=term-missing"]

