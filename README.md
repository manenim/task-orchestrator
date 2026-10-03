# Task Orchestrator

[![Go Reference](https://pkg.go.dev/badge/github.com/manenim/task-orchestrator.svg)](https://pkg.go.dev/github.com/manenim/task-orchestrator)
[![CI](https://github.com/manenim/task-orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/manenim/task-orchestrator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Go task orchestration system built with gRPC streaming, with one server coordinating a remote worker pool.

It lets you:
- submit tasks from clients,
- dispatch tasks to a dynamic worker pool,
- enforce task timeouts,
- retry transient failures with backoff,
- cancel in-flight work,
- inspect task state through a control-plane API.

## Project Status

This repository is a working engineering project with tested local recovery paths. It is not a production-readiness or high-availability claim.

Implemented core paths:
- Worker-plane RPCs: `SubmitTask`, `StreamTasks`, `CompleteTask`, `CancelTask`
- Task scheduling and dispatch loop
- Worker SDK with reconnect and graceful drain
- Storage backends: Postgres, Redis, in-memory

Partially implemented control-plane surface (see `docs/api-reference.md` for details):
- Implemented: `ListTasks`, `GetTask`, `StreamTaskEvents`, `GetClusterStats`, unconditional `CancelTask`
- Postgres only: `ListTaskLogs`
- Explicitly unsupported: manual `RetryTask`, conditional cancellation (`expected_version`), event replay/resume, and multi-tenant isolation

The server must run as **one replica**. Startup resets abandoned `SCHEDULED`/`RUNNING` assignments to `PENDING`, so handlers must tolerate repeated execution. Atomic task transitions, attempt fencing, and multi-server ownership are future work. See [recovery limits](docs/runbook.md#recovery-guarantees-and-limits).

## Why This Exists

Task queues are common, but many projects need tighter control over:
- task deadlines,
- explicit cancellation,
- deterministic state transitions,
- worker lifecycle behavior during disconnects and shutdown.

This project focuses on those concerns with a simple Go-native architecture that is easy to embed and extend.

## Architecture At A Glance

```text
┌─────────────────────────────── Clients ────────────────────────────────┐
│                                                                         │
│  Go Client SDK (pkg/client)         Browser / UI (gRPC-Web via Envoy)  │
│                                                                         │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
                         ┌──────────────────────┐
                         │ gRPC Server :50051   │
                         │                      │
                         │ OrchestratorService  │
                         │ ControlPlaneService  │
                         │ StateManager         │
                         │ Dispatcher           │
                         │ WorkerManager        │
                         └──────────┬───────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 ▼                  ▼                  ▼
          Postgres Repo         Redis Repo        In-Memory Repo
                 │
                 ▼
          task_events/task_logs (control-plane persistence)
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ Worker Pool          │
                         │ (pkg/worker)         │
                         └──────────────────────┘
```

For detailed internals, see [`docs/architecture.md`](docs/architecture.md).

## Task Lifecycle

State model:

```text
PENDING -> SCHEDULED -> RUNNING -> COMPLETED
                     \-> PENDING   (retry)
                     \-> FAILED
PENDING/SCHEDULED/RUNNING -> CANCELLED
```

Behavioral details:
- New tasks start in `PENDING`.
- `StateManager` periodically promotes eligible `PENDING` tasks to `SCHEDULED`.
- `Dispatcher` assigns `SCHEDULED` tasks to a worker and marks them `RUNNING`.
- Worker success marks task `COMPLETED`.
- Worker failure:
  - retryable + retries remaining -> `PENDING` with exponential backoff (`2^retry_count` seconds)
  - otherwise -> `FAILED`
- Cancel requests move non-terminal tasks to `CANCELLED`.

## Quick Start (Local)

### 1) Prerequisites

- Go 1.24+
- Docker + Docker Compose
- `grpcurl` (optional, for manual checks)

### 2) Start Infrastructure

```bash
docker compose up -d
```

This starts:
- Postgres on `localhost:5432`
- Redis on `localhost:6379`

### 3) Configure Environment

Example for Postgres storage:

```bash
export STORAGE_DRIVER=postgres
export DATABASE_URL='postgres://user:password@localhost:5432/orchestrator?sslmode=disable'
export PORT=50051
```

### 4) Run DB Schema Migration

```bash
go run cmd/migrate/main.go
```

### 5) Start Server

```bash
go run cmd/server/main.go
```

### 6) Start Worker

```bash
go run cmd/worker/main.go
```

### 7) Submit Example Tasks

```bash
go run cmd/client/loadtest/main.go
```

## Kubernetes Demo

[deploy/kubernetes/README.md](deploy/kubernetes/README.md) describes a disposable kind deployment with PostgreSQL persistence, migrations, probes, resource limits, task-flow checks, server restart, and a database outage demonstration. CI executes that deployment separately from the Go race and storage tests. It uses private cluster networking and is not a public production deployment.

## SDK Usage

### Worker SDK (`pkg/worker`)

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/manenim/task-orchestrator/pkg/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w, err := worker.New("localhost:50051")
	if err != nil {
		panic(err)
	}

	w.Handle("email", func(ctx context.Context, t worker.Task) ([]byte, error) {
		if len(t.Payload) == 0 {
			return nil, worker.Retryable(fmt.Errorf("missing payload"))
		}
		return []byte("sent"), nil
	})

	// Optional fallback for unknown task types
	w.Handle("*", func(ctx context.Context, t worker.Task) ([]byte, error) {
		return nil, fmt.Errorf("unsupported task type: %s", t.Type)
	})

	if err := w.Run(ctx); err != nil {
		panic(err)
	}
}
```

### Client SDK (`pkg/client`)

```go
package main

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/manenim/task-orchestrator/pkg/client"
)

func main() {
	c, err := client.New("localhost:50051")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_, err = c.SubmitTask(context.Background(), client.SubmitRequest{
		TaskID:         uuid.NewString(),
		Type:           "email",
		Payload:        []byte(`{"to":"user@example.com"}`),
		RunAt:          time.Now().Add(10 * time.Second),
		TimeoutSeconds: 30,
		MaxRetries:     5,
	})
	if err != nil {
		panic(err)
	}
}
```

`SubmitRequest.MaxRetries` is honoured: `0` (including omission) disables automatic retries; valid values are `0..30`. Set it explicitly when retries are wanted. This changes the previous behavior, which ignored the field and always used `3`. Negative timeouts are rejected.

## Configuration

Server configuration is loaded from environment variables (with `.env` auto-discovery from current directory upward).

| Variable | Default | Required | Description |
|---|---|---|---|
| `PORT` | `50051` | No | gRPC server listen port |
| `STORAGE_DRIVER` | `postgres` | No | One of `postgres`, `redis`, `memory` |
| `DATABASE_URL` | none | Yes when `STORAGE_DRIVER=postgres` | Postgres DSN |
| `REDIS_ADDR` | none | Yes when `STORAGE_DRIVER=redis` | Redis address (`host:port`) |
| `TENANT_ID` | `default` | No | Default tenant label attached by control-plane |
| `NAMESPACE_ID` | `default` | No | Default namespace label attached by control-plane |
| `SHUTDOWN_TIMEOUT` | `10s` | No | Positive duration before remaining gRPC streams are forced closed |

## Storage Drivers

### Postgres

Use for persistent environments and control-plane querying.

Supports:
- full task persistence,
- `ListTasks` filtering/sorting/pagination,
- event and log persistence (`task_events`, `task_logs`),
- cluster stats queries.

### Redis

Supports core worker-plane operations but currently does not support `ListTasks` filtering API.

`ListTasks` will return an explicit error on Redis storage.

### In-memory

Best for local development and tests.

Caveats:
- data is lost on restart,
- process-local only,
- advanced filtering behavior is not as complete as Postgres-backed querying.

## Open-Source Development Workflow

### Run tests

```bash
go test -race ./... -count=1
```

### Regenerate protobuf code

```bash
buf generate
```

### Regenerate SQLC code

```bash
sqlc generate
```

### Seed sample data

```bash
go run cmd/seed/main.go
```

## Repository Layout

```text
api/proto/                         # protobuf definitions
cmd/server/                        # orchestrator server entrypoint
cmd/worker/                        # example worker using pkg/worker
cmd/client/*/                      # load/retry/timeout/shutdown test clients
cmd/migrate/                       # schema migration tool
cmd/seed/                          # seed sample tasks/events/logs
internal/domain/                   # task model and state machine
internal/service/                  # orchestration, scheduling, dispatch, control-plane
internal/adapter/postgres/         # Postgres task repository + schema/sqlc
internal/adapter/redis/            # Redis task repository
internal/adapter/memory/           # in-memory task repository
pkg/client/                        # embeddable Go client SDK
pkg/worker/                        # embeddable Go worker SDK
pkg/api/                           # generated protobuf Go bindings
docs/                              # architecture, API, and runbook docs
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md) - internal component design, data flow, and lifecycle details
- [`docs/api-reference.md`](docs/api-reference.md) - RPC-level reference with current implementation status
- [`docs/runbook.md`](docs/runbook.md) - operational procedures and troubleshooting
- [`docs/verification.md`](docs/verification.md) - commands, tested behavior, and evidence limits

## License

MIT. See [`LICENSE`](LICENSE).
