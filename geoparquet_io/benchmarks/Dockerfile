# Docker image for running benchmarks with memory constraints
# Security: Runs as non-root user

FROM python:3.11-slim

# Install system dependencies for GDAL/spatial
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security (avoid running as root)
RUN groupadd --gid 1000 benchmarker \
    && useradd --uid 1000 --gid benchmarker --shell /bin/bash --create-home benchmarker

WORKDIR /app

# Copy and install package
COPY --chown=benchmarker:benchmarker . .
RUN pip install --no-cache-dir -e ".[dev]"

# Pre-download DuckDB extensions (as root before switching user)
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL spatial'); c.execute('INSTALL httpfs')"

# Switch to non-root user
USER benchmarker

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD gpio --version || exit 1

ENTRYPOINT ["gpio", "benchmark"]
CMD ["suite", "--help"]
