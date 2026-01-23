FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
  gcc \
  libxml2-dev \
  libxslt-dev \
  postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=10m --timeout=3s \
  CMD python -c "import psycopg2; import bs4" || exit 1

# Set AWS region
ENV AWS_DEFAULT_REGION=us-east-1

# Create non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command
ENTRYPOINT ["python", "main.py"]