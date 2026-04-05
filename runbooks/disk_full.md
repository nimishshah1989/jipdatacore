---
title: Disk Full (EC2 or RDS Storage)
severity: critical
oncall: infra
---

# Runbook: Disk Full

## Symptoms
- Pipeline logs: `No space left on device`
- Docker container exits with storage error
- PostgreSQL WAL logs stop
- CloudWatch alert: `DiskQueueDepth` high on RDS

## Immediate Actions (< 5 minutes)

1. Check disk usage:
   ```bash
   df -h
   du -sh /var/lib/docker/*
   du -sh /app/logs/*
   ```

2. Check Docker volume usage:
   ```bash
   docker system df
   ```

3. Check PostgreSQL storage (RDS):
   ```bash
   aws cloudwatch get-metric-statistics \
     --namespace AWS/RDS \
     --metric-name FreeStorageSpace \
     --dimensions Name=DBInstanceIdentifier,Value=fie-db \
     --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
     --period 60 --statistics Minimum
   ```

## EC2 Disk Cleanup

### Remove old Docker images/containers:
```bash
docker system prune -f
# If still full:
docker system prune -af --volumes
# WARNING: this removes ALL unused containers, images, and volumes
```

### Clean up pipeline downloads (S3 archive confirmed):
```bash
# Check if temp downloads exist
ls -lah /tmp/*.csv /tmp/*.zip 2>/dev/null
rm -f /tmp/*.csv /tmp/*.zip

# Clean application logs older than 7 days
find /app/logs -name "*.log" -mtime +7 -delete
```

### Free up space quickly (last resort):
```bash
# Clear Docker build cache
docker builder prune -f

# Clear journal logs
journalctl --vacuum-time=2d
```

## RDS Storage Expansion

If RDS storage is > 85% full:

1. Enable RDS storage autoscaling via AWS Console:
   - RDS Console → fie-db → Modify
   - Enable "Enable storage autoscaling"
   - Maximum storage: 500 GB

2. Or manual resize:
   ```bash
   aws rds modify-db-instance \
     --db-instance-identifier fie-db \
     --allocated-storage 300 \
     --apply-immediately
   ```

## Prevention
- Set up CloudWatch alarm for EC2 disk > 80%
- Enable RDS storage autoscaling (up to 500 GB)
- Log rotation configured in Docker (max 100MB, 5 files per container)
- S3 archival for pipeline source files (delete local after 7 days)

## Log Rotation Config
Add to docker-compose.yml for each service:
```yaml
logging:
  driver: "json-file"
  options:
    max-size: "100m"
    max-file: "5"
```

## Escalation
- EC2 disk > 95% → critical, page infra immediately
- RDS storage < 20 GB free → critical, expand immediately (WAL can fill quickly)
