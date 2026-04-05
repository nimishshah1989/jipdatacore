---
title: Redis Down
severity: critical
oncall: infra
---

# Runbook: Redis Down

## Symptoms
- API returning 500 errors on cached endpoints
- Alert: `preflight_redis_failed` in logs
- Orchestrator preflight check fails
- DAG execution will not start (preflight gate)

## Impact Assessment
- Cache MISS for all endpoints → DB load spikes
- Rate limiting disabled → potential abuse
- Pipeline advisory locks use Redis (if configured) → may allow duplicate runs

## Immediate Actions (< 5 minutes)

1. Check Redis container:
   ```bash
   docker ps | grep redis
   docker logs jip-redis --tail=50
   ```

2. Attempt ping:
   ```bash
   docker exec jip-redis redis-cli -a ${REDIS_PASSWORD} PING
   ```

3. Check disk space (Redis AOF logs can fill disk):
   ```bash
   df -h
   docker exec jip-redis redis-cli -a ${REDIS_PASSWORD} INFO persistence
   ```

## Investigation

### If Redis OOM (out of memory):
```bash
docker exec jip-redis redis-cli -a ${REDIS_PASSWORD} INFO memory
# Check used_memory vs maxmemory
# If eviction policy is allkeys-lru, this should be auto-managed
```

### If Redis crashed (exit code non-zero):
```bash
docker inspect jip-redis | jq '.[0].State'
# Restart the container
docker restart jip-redis
# Verify it comes back
docker exec jip-redis redis-cli -a ${REDIS_PASSWORD} PING
```

### If Redis AOF file corrupted:
```bash
docker exec jip-redis redis-check-aof --fix /data/appendonly.aof
docker restart jip-redis
```

## Mitigation (App continues without Redis)

The FastAPI app falls back gracefully when Redis is unavailable:
- Cache endpoints return DB results directly (higher latency acceptable)
- Rate limiting disabled (monitor for abuse)

To confirm app is operational without Redis:
```bash
curl http://localhost:8010/health
# Should still return 200 even if Redis is down
```

## Recovery

1. Restart Redis:
   ```bash
   docker restart jip-redis
   ```

2. Verify connection:
   ```bash
   docker exec jip-redis redis-cli -a ${REDIS_PASSWORD} PING
   # Expected: PONG
   ```

3. Warm critical caches:
   ```bash
   curl http://localhost:8010/api/v1/equity/eod?limit=10  # warm equity cache
   curl http://localhost:8010/api/v1/mf/nav?limit=10       # warm MF cache
   ```

## Escalation
- Redis down > 30 min → escalate to infra lead
- DB CPU > 80% during Redis outage → implement emergency rate limiting at nginx level

## Post-Incident
- Review Redis maxmemory setting (should be 2GB for production)
- Enable Redis Sentinel if single-node uptime insufficient
