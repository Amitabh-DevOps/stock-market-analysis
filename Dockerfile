# Stock Market Analytics Dashboard - Production Dockerfile
# Optimized for AWS ECS deployment with security best practices

# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Add labels for image metadata
LABEL maintainer="Stock Dashboard Team"
LABEL description="Stock Market Analytics Dashboard - Educational Demo"
LABEL version="1.0.0"

# Set working directory
WORKDIR /app

# Build-time ARG for Alpha Vantage API key (optional)
# WARNING: providing the API key at build time will bake it into the image layers
# and may expose the secret in your image/ECR. Prefer runtime secrets injection.
ARG ALPHA_VANTAGE_API_KEY
# If provided at build time, set as env var inside the image
ENV ALPHA_VANTAGE_API_KEY=${ALPHA_VANTAGE_API_KEY}

# Install system dependencies and security updates
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r appuser && \
    useradd -r -g appuser -u 1001 appuser && \
    chown -R appuser:appuser /app

# Copy requirements first for better layer caching
COPY --chown=appuser:appuser requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy package configuration files
COPY --chown=appuser:appuser setup.py .
COPY --chown=appuser:appuser pyproject.toml .
COPY --chown=appuser:appuser README.md .

# Copy source code
COPY --chown=appuser:appuser src/ ./src/

# Install the package
RUN pip install --no-cache-dir -e .

# Switch to non-root user
USER appuser

# Expose port 8080
EXPOSE 8080

# Set environment variables for Streamlit
ENV STREAMLIT_SERVER_PORT=8080 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_FILE_WATCHER_TYPE=none \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Health check for container orchestration
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Run the application
CMD ["streamlit", "run", "src/app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
