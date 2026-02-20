# Task Orchestrator

[![Go Reference](https://pkg.go.dev/badge/github.com/manenim/task-orchestrator.svg)](https://pkg.go.dev/github.com/manenim/task-orchestrator)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A distributed task processing system built on gRPC streaming. Submit tasks
from any client, distribute them across a pool of workers, and let the
orchestrator handle scheduling, retries, timeouts, cancellation, and
graceful shutdown.

## Features

- **gRPC streaming** — workers receive tasks over a persistent bidirectional stream
- **Pluggable storage** — PostgreSQL, Redis, or in-memory (swap with one env var)
- **Automatic retries** — exponential backoff with configurable max retries
- **Task timeouts** — per-task deadlines enforced at the worker level
- **Live cancellation** — cancel running tasks from any client
- **Graceful shutdown** — workers drain in-flight tasks before disconnecting
- **Delayed scheduling** — submit tasks with a future `run_at` timestamp
- **Real-time control plane** — query, filter, stream events, and view cluster stats via gRPC-Web
- **Embeddable SDKs** — drop `pkg/worker` or `pkg/client` into your own Go services

## Quick Start

### Install

```bash
go get github.com/manenim/task-orchestrator
```

### Worker

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/manenim/task-orchestrator/pkg/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w, _ := worker.New("localhost:50051")
	w.Handle("email", func(ctx context.Context, t worker.Task) ([]byte, error) {
		// process the task...
		return []byte("sent"), nil
	})
	w.Run(ctx)
}
```

### Client

```go
package main

import (
	"context"

	"github.com/google/uuid"
	"github.com/manenim/task-orchestrator/pkg/client"
)

func main() {
	c, _ := client.New("localhost:50051")
	defer c.Close()

	c.SubmitTask(context.Background(), client.SubmitRequest{
		TaskID:     uuid.New().String(),
		Type:       "email",
		Payload:    []byte(`{"to":"user@example.com"}`),
		MaxRetries: 3,
	})
}
```

### Server

```bash
# Start infrastructure
docker compose up -d

# Run migrations (PostgreSQL)
go run cmd/migrate/main.go

# Start the orchestrator server
STORAGE_DRIVER=postgres go run cmd/server/main.go
```

The server listens on `localhost:50051` (gRPC).

## Architecture

```
┌─────────────┐     ┌─────────────┐
│  Client SDK │     │  Browser UI │
│  pkg/client │     │  (gRPC-Web) │
└──────┬──────┘     └──────┬──────┘
       │                   │
       │              ┌────▼────┐
       │              │  Envoy  │
       │              │  :8080  │
       │              └────┬────┘
       │                   │
  ┌────▼───────────────────▼────┐
  │     gRPC Server  :50051     │
  │                             │
  │  OrchestratorService        │
  │  ControlPlaneService        │
  │  StateManager · Dispatcher  │
  └──────────┬──────────────────┘
             │
  ┌──────────▼──────────┐
  │  TaskRepository     │
  │  (postgres/redis/   │
  │   memory)           │
  └─────────────────────┘
             │
  ┌──────────▼──────────┐
  │  Worker Pool        │
  │  pkg/worker         │
  │  (N instances)      │
  └─────────────────────┘
```

### Task Lifecycle

```
PENDING → SCHEDULED → RUNNING → COMPLETED
                        ↓          ↑
                      FAILED    (retry)
                        ↓
                    CANCELLED
```

Tasks can be cancelled from any non-terminal state. Retryable failures
return the task to PENDING with exponential backoff.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `50051` | gRPC listen port |
| `STORAGE_DRIVER` | `memory` | `memory`, `postgres`, or `redis` |
| `DATABASE_URL` | — | PostgreSQL connection string |
| `REDIS_ADDR` | — | Redis address |

## Project Structure

```
cmd/
  server/         Server entrypoint
  worker/         Example worker
  client/         Test utilities (loadtest, retrytest, etc.)
  migrate/        Database migration tool
  seed/           Sample data seeder
internal/
  domain/         Task model and state machine
  port/           Repository and logger interfaces
  adapter/        Storage and logging implementations
  service/        Orchestrator, Dispatcher, StateManager, ControlPlane
pkg/
  worker/         Embeddable worker library
  client/         Go client SDK
  api/            Generated protobuf Go code
docs/
  architecture.md System architecture diagrams
  api-reference.md  gRPC and SDK API reference
  runbook.md      Operational guide
```

## Testing

```bash
go test ./... -count=1
```

## Documentation

Full API reference and architecture diagrams are in the [`docs/`](docs/) directory.

- [Architecture](docs/architecture.md) — system diagrams and CQRS data flow
- [API Reference](docs/api-reference.md) — gRPC services and SDK usage
- [Runbook](docs/runbook.md) — operations, health checks, and troubleshooting

## License

[MIT](LICENSE) — see the LICENSE file for details.
