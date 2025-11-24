# Stock Market Analytics Dashboard - Dockerfile

# Use Python 3.11 slim image from the Amazon ECR Public mirror to avoid
# Docker Hub anonymous pull rate limits in CI.
FROM public.ecr.aws/docker/library/python:3.11-slim

# Set working directory
WORKDIR /app

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
