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

# Run application
CMD ["python", "app.py"]