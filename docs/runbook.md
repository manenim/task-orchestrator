# Operational Runbook

This runbook covers day-to-day operations for Task Orchestrator.

It covers a single server and disposable local examples. Production hardening and multi-server ownership remain future work.

## 1. Prerequisites

Required:
- Go 1.24+
- Docker + Docker Compose

Recommended tools:
- `grpcurl` for gRPC checks
- `psql` for direct Postgres inspection
- `redis-cli` for Redis checks

## 2. Deployment Modes

Current server supports a single process with pluggable storage:
- `postgres` (default): recommended for persistence and control-plane querying
- `redis`: core task flow support; no rich `ListTasks`
- `memory`: ephemeral local/testing mode

## 3. Bootstrap Procedure (Local)

### 3.1 Start Infra

```bash
docker compose up -d
```

### 3.2 Set Environment

Postgres mode example:

```bash
export STORAGE_DRIVER=postgres
export DATABASE_URL='postgres://user:password@localhost:5432/orchestrator?sslmode=disable'
export PORT=50051
export TENANT_ID=default
export NAMESPACE_ID=default
```

### 3.3 Apply Schema

```bash
go run cmd/migrate/main.go
```

### 3.4 Optional Seed Data

```bash
go run cmd/seed/main.go
```

### 3.5 Start Server

```bash
go run cmd/server/main.go
```

### 3.6 Start Worker

```bash
go run cmd/worker/main.go
```

### 3.7 Optional gRPC-Web Proxy (for browser clients)

```bash
docker compose -f deploy/envoy/docker-compose.yaml up -d
```

## 4. Health Checks

## 4.1 gRPC service health

```bash
grpcurl -plaintext -d '{"service":"readiness"}' localhost:50051 grpc.health.v1.Health/Check
```

Expected status: `SERVING`. The standard gRPC health service has `liveness` and `readiness` names. Readiness checks storage every two seconds with a one-second check timeout; liveness stays healthy during a storage outage to avoid restart storms. Reflection is enabled for local inspection.

## 4.2 Postgres health

```bash
docker compose exec postgres pg_isready -U user -d orchestrator
```

## 4.3 Redis health

```bash
docker compose exec redis redis-cli ping
```

Expected output: `PONG`

## 4.4 Envoy health

```bash
curl -sSf http://localhost:9901/ready
```

## 5. Routine Operations

## 5.1 Run tests before deploy

```bash
go test -race ./... -count=1
```

## 5.2 Run scenario clients

Load/distribution test:

```bash
go run cmd/client/loadtest/main.go
```

Retry behavior test:

```bash
go run cmd/client/retrytest/main.go
```

Timeout behavior test:

```bash
go run cmd/client/timeouttest/main.go
```

Graceful worker shutdown test:

```bash
go run cmd/client/shutdowntest/main.go
```

## 5.3 Graceful shutdown sequence

Server:
1. send `SIGTERM` / `Ctrl+C`,
2. health becomes `NOT_SERVING` and background loops stop via context cancellation,
3. the server gives RPCs up to `SHUTDOWN_TIMEOUT` (default `10s`) to finish,
4. it closes remaining streams when the deadline expires. Long-lived worker/event streams otherwise prevent `GracefulStop` from returning. Kubernetes uses a 20-second termination grace period.

Worker:
1. send `SIGTERM` / `Ctrl+C`,
2. worker stops accepting new stream events,
3. in-flight handlers drain,
4. stream closes.

## 6. Observability and Inspection

## 6.1 Check current tasks in Postgres

```sql
SELECT id, task_type, state, worker_id, retry_count, run_at, updated_at
FROM tasks
ORDER BY updated_at DESC
LIMIT 100;
```

## 6.2 Check event stream records

```sql
SELECT event_id, occurred_at, event_type, task_id, previous_state, current_state, worker_id, reason
FROM task_events
ORDER BY event_id DESC
LIMIT 100;
```

## 6.3 Check task logs

```sql
SELECT sequence, task_id, timestamp, level, component, message, fields
FROM task_logs
ORDER BY sequence DESC
LIMIT 100;
```

## 7. Incident Playbooks

## 7.1 Symptom: cannot connect to `:50051`

Checks:
1. confirm server process is running,
2. verify `PORT` value,
3. run `lsof -i :50051`.

Remediation:
- restart server with correct environment.

## 7.2 Symptom: tasks stuck in `PENDING`

Likely causes:
- `run_at` is in the future,
- `StateManager` not running,
- repository query path failing.

Checks:
1. inspect `run_at` timestamps,
2. inspect server logs for `failed to list eligible tasks`,
3. verify DB/Redis connectivity.

## 7.3 Symptom: tasks stuck in `SCHEDULED`

Likely causes:
- no active workers at dispatch time,
- dispatch send error.

Checks:
1. check worker count in logs (`Worker connected`/`disconnected`),
2. look for `No worker available to dispatch task`,
3. inspect task rows with `state='SCHEDULED'`.

Current behavior:
- no-worker dispatch and stream send failures requeue tasks to `PENDING`,
- queued snapshots are refreshed before dispatch so already-cancelled work is skipped,
- worker disconnect releases its running/scheduled work,
- startup recovers abandoned assignments before scheduling begins.

Bring workers online and inspect storage errors if recovery does not progress. Do not start a second server against the same repository.

## 7.4 Symptom: `ListTasks` fails in Redis mode

Expected behavior currently.

Remediation:
- use Postgres or in-memory storage for control-plane list/query workflows.

## 7.5 Symptom: control-plane RPC returns `Unimplemented`

Likely call to currently unimplemented methods:
- `RetryTask`
- `CancelTask` with nonzero `expected_version`
- `ListTaskLogs` outside Postgres mode
- worker-plane `RegisterWorker` (the SDK registers by opening `StreamTasks`)

Remediation:
- use unconditional cancellation through either plane,
- replay finalized work under a new task ID only after checking side effects.

## 8. Backup and Recovery (Postgres)

Simple logical backup:

```bash
pg_dump 'postgres://user:password@localhost:5432/orchestrator?sslmode=disable' > orchestrator.sql
```

Restore:

```bash
psql 'postgres://user:password@localhost:5432/orchestrator?sslmode=disable' < orchestrator.sql
```

## Recovery Guarantees and Limits

Startup recovery resets all `SCHEDULED`/`RUNNING` tasks before accepting work. Run exactly one server; the Kubernetes Deployment uses `replicas: 1` and `Recreate`. Two servers can dispatch the same work and startup recovery can interfere with another live server.

Execution is at least once: a worker may finish a side effect before the server receives its completion. Use idempotent handlers or a durable side-effect deduplication key. A completion from a different worker ID is rejected, but reconnecting with the same ID does not fence an old attempt. Task state updates are not atomic compare-and-swap; cancellation and completion can still race. Race-detector success addresses memory races, not these distributed correctness limits.

Postgres migration adds `error_message` and `version` to existing databases. Reapply `go run ./cmd/migrate` before upgrading the server. Legacy versions default to `1`; they are not historical revision reconstructions. Events/logs are best-effort writes after state persistence, not a transactional outbox. Event streams are live only and slow subscribers may lose events. Redis durability depends on the external Redis persistence configuration; memory storage is lost on restart.

## Failure Demonstrations

- Run `TEST_DATABASE_URL=... TEST_REDIS_ADDR=... go test -race ./... -count=1` against disposable services. Missing variables skip backend integration tests; CI always supplies them.
- `TestStreamDisconnectRequeuesAndReplacementCompletes` opens actual gRPC streams, disconnects a worker, reassigns its task, and verifies the replacement result.
- `TestPostgresPersistenceAndRecovery` reopens storage through a fresh connection, verifies full task metadata and result, and resets abandoned running work.
- `TestRedisReleasePreservesTerminalTasks` checks that disconnect cannot revive completed, failed, or cancelled tasks.
- `TestServerHealthAndBoundedShutdown` starts a real server process and verifies health and exit with a live worker stream.
- Follow [the Kubernetes demo](../deploy/kubernetes/README.md) for server restart and database outage/readiness recovery. The script verifies the same completed result after both disruptions.

## 9. Performance and Tuning Notes

Current defaults in server setup:
- scheduler batch size: `10`
- queue buffer size: `100`
- state polling interval: `500ms`

Tuning considerations:
1. increase worker count for throughput,
2. tune batch size and queue buffer based on backlog profile,
3. index and monitor task query patterns in Postgres,
4. keep worker handler code idempotent and timeout-aware.

## 10. Security Checklist

For public/open-source deployments:
1. do not run plaintext gRPC across untrusted networks,
2. use TLS and authenticated clients,
3. restrict network exposure for Postgres/Redis,
4. sanitize and validate task payloads in worker handlers,
5. avoid logging sensitive payload data.

## 11. Useful Commands Summary

```bash
# Infra
docker compose up -d
docker compose down

# Schema and seed
go run cmd/migrate/main.go
go run cmd/seed/main.go

# Run services
go run cmd/server/main.go
go run cmd/worker/main.go

# Validation
go test -race ./... -count=1
grpcurl -plaintext -d '{"service":"readiness"}' localhost:50051 grpc.health.v1.Health/Check
```
