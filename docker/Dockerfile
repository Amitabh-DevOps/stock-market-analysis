# Stock Market Analytics Dashboard - Dockerfile

# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Optional build-time ARG for Alpha Vantage API key
# WARNING: providing the API key at build time will bake it into the image layers
# and may expose the secret in your image/ECR. Prefer runtime secrets injection.
ARG ALPHA_VANTAGE_API_KEY
ENV ALPHA_VANTAGE_API_KEY=${ALPHA_VANTAGE_API_KEY}

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy package files
COPY setup.py .
COPY pyproject.toml .
COPY README.md .
COPY LICENSE .

# Copy source code
COPY src/ ./src/

# Install the package in editable mode
RUN pip install -e .

# Expose port 8080
EXPOSE 8080

# Set environment variables for Streamlit
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true

# Set PYTHONPATH to include the app directory
ENV PYTHONPATH=/app

# Run the application
CMD ["streamlit", "run", "src/app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]
