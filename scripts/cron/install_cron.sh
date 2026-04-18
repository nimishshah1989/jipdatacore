#!/bin/bash
# Install JIP Data Engine crontab on EC2
# Usage: bash scripts/cron/install_cron.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/home/ubuntu/jip-data-engine/logs"

echo "Creating log directory..."
mkdir -p "$LOG_DIR"

echo "Installing crontab..."
crontab "$SCRIPT_DIR/jip_scheduler.cron"

echo "Verifying..."
crontab -l | head -5
echo "..."
crontab -l | wc -l
echo "lines installed"

echo ""
echo "Done. Logs will be written to: $LOG_DIR"
echo "Check status: crontab -l"
echo "Remove: crontab -r"
