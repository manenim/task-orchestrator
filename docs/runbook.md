# Operational Runbook

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d                          # Postgres + Redis

# 2. Run migrations
go run cmd/migrate/main.go

# 3. Seed sample data (optional)
go run cmd/seed/main.go

# 4. Start server
STORAGE_DRIVER=postgres go run cmd/server/main.go

# 5. Start Envoy (gRPC-Web for browsers)
docker compose -f deploy/envoy/docker-compose.yaml up -d

# 6. Start a worker
go run cmd/worker/main.go
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `50051` | gRPC server port |
| `STORAGE_DRIVER` | No | `memory` | `memory`, `postgres`, or `redis` |
| `DATABASE_URL` | If postgres | — | PostgreSQL connection string |
| `REDIS_ADDR` | If redis | — | Redis address (host:port) |

## Storage Drivers

| Driver | Use Case | Limitations |
|--------|----------|-------------|
| `memory` | Dev/testing | Data lost on restart |
| `postgres` | Production | Full feature support |
| `redis` | Caching layer | `ListTasks` not supported (returns error) |

## Health Checks

### gRPC Server
```bash
grpcurl -plaintext localhost:50051 list
```

### Envoy Proxy
```bash
curl http://localhost:9901/ready    # Envoy admin
```

### Database
```bash
docker compose exec postgres pg_isready -U postgres
```

## Common Operations

### Submit a Task (via SDK)
```bash
go run cmd/client/loadtest/main.go
```

### Test Retries
```bash
go run cmd/client/retrytest/main.go
```

### Test Timeouts
```bash
go run cmd/client/timeouttest/main.go
```

### Test Graceful Shutdown
```bash
go run cmd/client/shutdowntest/main.go
# Then Ctrl+C the worker while the slow task runs
```

### Run Tests
```bash
go test ./internal/... -v -count=1
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `connection refused :50051` | Server not running | `go run cmd/server/main.go` |
| `connection refused :8080` | Envoy not running | `docker compose -f deploy/envoy/docker-compose.yaml up -d` |
| `failed to connect to postgres` | DB not running or wrong URL | `docker compose up -d` and check `DATABASE_URL` |
| `port already in use :6379` | Another Redis instance | `sudo lsof -i :6379` and stop it |
| `no handler registered` | Worker missing handler for task type | Add `w.Handle("type", handler)` |
| `ListTasks: not supported` | Redis storage driver | Switch to `postgres` or `memory` |

## Ports Summary

| Port | Service | Protocol |
|------|---------|----------|
| 50051 | gRPC Server | HTTP/2 (gRPC) |
| 8080 | Envoy Proxy | HTTP/1.1 (gRPC-Web) |
| 9901 | Envoy Admin | HTTP |
| 5432 | PostgreSQL | TCP |
| 6379 | Redis | TCP |
