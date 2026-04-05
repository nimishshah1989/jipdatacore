#!/bin/bash
# Forge OS — EC2 Deployment Script
# Deploys JIP Data Engine to EC2, runs migrations, starts ingestion
set -euo pipefail

EC2_HOST="ubuntu@13.206.34.214"
SSH_KEY="$HOME/.ssh/jsl-wealth-key.pem"
PROJECT_DIR="/home/ubuntu/jip-data-engine"
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPTS_DIR")"

log() { echo "[$(date '+%H:%M:%S')] $1"; }

# Update dashboard
update_dash() {
    python3 "$SCRIPTS_DIR/update_dashboard.py" --phase "$1" --detail "$2" 2>/dev/null || true
}

# Test SSH connectivity
log "Testing SSH connectivity..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$EC2_HOST" "echo 'SSH OK'" || {
    log "ERROR: Cannot SSH to $EC2_HOST"
    update_dash "deploying" "SSH connection failed"
    exit 1
}
log "SSH connection successful"

# Phase 1: Deploy code
update_dash "deploying" "Pushing code to EC2..."
log "Syncing project to EC2..."
rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='.env' --exclude='venv' \
    -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
    "$PROJECT_ROOT/" "$EC2_HOST:$PROJECT_DIR/" 2>&1 | tail -5

# Phase 2: Docker build on EC2
update_dash "deploying" "Building Docker image on EC2..."
log "Building Docker image..."
ssh -i "$SSH_KEY" "$EC2_HOST" "cd $PROJECT_DIR && docker build -t jip-data-engine:latest . 2>&1 | tail -10"

# Phase 3: Run Alembic migrations
update_dash "deploying" "Running database migrations..."
log "Running Alembic migrations..."
ssh -i "$SSH_KEY" "$EC2_HOST" "cd $PROJECT_DIR && docker run --env-file .env --rm jip-data-engine:latest alembic upgrade head 2>&1 | tail -10"

# Phase 4: Start the service
update_dash "deploying" "Starting JIP Data Engine service..."
log "Starting service..."
ssh -i "$SSH_KEY" "$EC2_HOST" << 'REMOTE'
cd /home/ubuntu/jip-data-engine
# Stop existing container if running
docker stop jip-data-engine 2>/dev/null || true
docker rm jip-data-engine 2>/dev/null || true
# Start new container
docker run -d \
    --name jip-data-engine \
    --env-file .env \
    -p 8010:8010 \
    --restart unless-stopped \
    jip-data-engine:latest
# Wait for health check
sleep 5
curl -s http://localhost:8010/ | python3 -m json.tool || echo "Health check pending..."
REMOTE

# Phase 5: Run data migrations (legacy DB -> new schema)
update_dash "migrating" "Starting legacy data migration..."
log "Running data migrations..."
ssh -i "$SSH_KEY" "$EC2_HOST" << 'REMOTE'
cd /home/ubuntu/jip-data-engine
# Run migration runner inside container
docker exec jip-data-engine python3 -m app.migrations.runner --all 2>&1 | tail -20
REMOTE
update_dash "migrating" "Data migration complete"

# Phase 6: Start initial data ingestion
update_dash "ingesting" "Starting pipeline ingestion..."
log "Triggering initial data ingestion..."
ssh -i "$SSH_KEY" "$EC2_HOST" << 'REMOTE'
cd /home/ubuntu/jip-data-engine
# Run the DAG orchestrator for today's data
docker exec -d jip-data-engine python3 -m app.orchestrator.dag 2>&1 | tail -20
echo "Ingestion pipelines started in background"
REMOTE

update_dash "complete" "Deployed, migrated, ingestion running"
log "DEPLOYMENT COMPLETE"
log "Service: http://13.206.34.214:8010"
log "Dashboard: SSH tunnel + http://localhost:8099"
