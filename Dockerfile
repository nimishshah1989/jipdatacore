FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"

# Application code
COPY . .

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8010

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010", "--workers", "4"]
