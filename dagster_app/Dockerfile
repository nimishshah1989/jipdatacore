FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY dagster_app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy code
COPY dagster_app /opt/dagster/dagster_app

# Wire up dagster.yaml (instance config) into DAGSTER_HOME
RUN mkdir -p ${DAGSTER_HOME} && \
    cp /opt/dagster/dagster_app/dagster.yaml ${DAGSTER_HOME}/dagster.yaml && \
    cp /opt/dagster/dagster_app/workspace.yaml /opt/dagster/workspace.yaml

EXPOSE 3000

# Entrypoint is overridden per-service in docker-compose
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/workspace.yaml"]
