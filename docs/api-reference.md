# API Reference

This document describes the current API surface and behavior of Task Orchestrator.

It includes:
- worker-plane RPCs (`api.v1.Orchestrator`),
- control-plane RPCs (`orchestrator.v1.ControlPlaneService`),
- implementation status for each declared RPC,
- SDK mappings for `pkg/client` and `pkg/worker`.

## 1. Endpoints and Transports

Server listens on one gRPC endpoint by default:
- `localhost:50051` (native gRPC)

Optional browser access:
- Envoy gRPC-Web proxy at `localhost:8080` (see `deploy/envoy/`).

## 2. Worker-Plane Service: `api.v1.Orchestrator`

Proto: [`api/proto/v1/orchestrator.proto`](../api/proto/v1/orchestrator.proto)
Go bindings: [`pkg/api/v1`](../pkg/api/v1)

### 2.1 RPC Status

| RPC | Type | Status | Notes |
|---|---|---|---|
| `SubmitTask` | Unary | Implemented | Creates a task in `PENDING` |
| `RegisterWorker` | Unary | Unimplemented | Proto declared; no server handler override yet |
| `StreamTasks` | Server streaming | Implemented | Worker receives task/cancel events |
| `CompleteTask` | Unary | Implemented | Handles success/failure/retry transitions |
| `CancelTask` | Unary | Implemented | Cancels task and signals worker if assigned |

### 2.2 `SubmitTask`

Request fields:

| Field | Required | Behavior |
|---|---|---|
| `task_id` | Yes | Must be non-empty. Expected to be client-generated for idempotency. |
| `type` | Yes | Task type used by worker handler lookup. |
| `payload` | No | Opaque bytes passed to worker. |
| `client_id` | No | Stored on task metadata. |
| `run_at` | No | Zero or omitted means immediate scheduling (`now`). |
| `max_retries` | No | Present in proto, currently not applied by create path. |
| `timeout_seconds` | No | Stored and passed to worker stream event. |

Response:
- `task_id`

Errors:
- `InvalidArgument` when `task_id` or `type` is empty.
- `Internal` on repository failure.

Semantics:
- new task state is `PENDING`.
- creation event may be published to control-plane publisher when configured.

### 2.3 `StreamTasks`

Request:
- `worker_id`

Stream payload:
- `TaskEvent { task_id, job_type, payload, is_cancellation, timeout_seconds }`

Semantics:
- worker opens one long-lived stream.
- server registers worker in `WorkerManager`.
- on disconnect, server removes worker and releases worker-owned tasks.
- cancellation is signaled via `is_cancellation=true` and `task_id`.

Errors:
- duplicate `worker_id` connection returns an error from `WorkerManager.Add`.

### 2.4 `CompleteTask`

Request fields:

| Field | Required | Behavior |
|---|---|---|
| `task_id` | Yes | Task to complete. |
| `worker_id` | Yes | Used for worker active-count decrement. |
| `error_message` | No | If set, completion is treated as failure path. |
| `result` | No | Stored on success path. |
| `is_retryable` | No | Failure is retried only when true and retries remain. |

Response:
- `stop_stream` (currently always `false`)

Failure and retry behavior:
- if `error_message == ""`: task transitions to `COMPLETED`.
- else:
  - if `is_retryable=true` and `retry_count < max_retries`:
    - increment `retry_count`,
    - set `run_at = now + 2^retry_count seconds`,
    - transition to `PENDING`.
  - otherwise transition to `FAILED`.

Errors:
- `NotFound` if task does not exist.
- `FailedPrecondition` for invalid state transitions.
- `Internal` for persistence errors.

### 2.5 `CancelTask`

Request:
- `task_id`

Response:
- `success` boolean

Semantics:
- loads current task.
- if task has `worker_id`, server sends cancellation event to that worker.
- transitions task to `CANCELLED` when legal.
- canceling an already terminal task is treated as successful/idempotent.

Errors:
- `InvalidArgument` for empty task id.
- `NotFound` if task does not exist.
- `FailedPrecondition` for illegal transition conditions.

### 2.6 `RegisterWorker`

Declared in proto, but currently not implemented in `internal/service/orchestrator.go`.
Current behavior:
- returns `Unimplemented` from embedded gRPC stub.

## 3. Control-Plane Service: `orchestrator.v1.ControlPlaneService`

Proto: [`api/proto/orchestrator/v1/control_plane.proto`](../api/proto/orchestrator/v1/control_plane.proto)
Go bindings: [`pkg/api/orchestrator/v1`](../pkg/api/orchestrator/v1)

### 3.1 RPC Status Matrix

| RPC | Type | Declared | Implemented in service | Runtime behavior today |
|---|---|---|---|---|
| `ListTasks` | Unary | Yes | Yes | Fully callable |
| `GetTask` | Unary | Yes | Yes | Fully callable |
| `StreamTaskEvents` | Server streaming | Yes | Yes | Live push stream |
| `CancelTask` | Unary | Yes | No | Returns `Unimplemented` |
| `RetryTask` | Unary | Yes | No | Returns `Unimplemented` |
| `GetClusterStats` | Unary | Yes | Yes | Callable; partial metrics in non-Postgres mode |
| `ListTaskLogs` | Unary | Yes | No | Returns `Unimplemented` |

### 3.2 `ListTasks`

Supports:
- filter by states, task types, worker id, id prefix, text query,
- optional created/updated/run time ranges,
- sort by `created_at`, `run_at`, `updated_at`,
- cursor pagination.

Implementation details:
- page size defaults to 50, max 500,
- cursor encodes an offset internally,
- response fetches one extra row to compute `has_more`.

Response fields:
- `tasks`
- `next_cursor`
- `has_more`
- `snapshot_time`
- `snapshot_event_id` (from `MAX(event_id)` when Postgres event table is available)

Storage caveat:
- Redis repository returns an explicit error for `ListTasks`.

### 3.3 `GetTask`

Request:
- `task_id` required

Response:
- single task payload

Implementation caveat:
- request flags `include_payload`/`include_result` exist in proto,
- current service always includes payload and result.

### 3.4 `StreamTaskEvents`

Current implementation behavior:
- subscriber receives events published after stream subscription,
- events are sent one-by-one in `StreamTaskEventsResponse.events` array,
- stream exits when client context is canceled.

Important caveats:
- request-side filter fields are currently ignored,
- resume options (`resume_token`, `after_event_id`) are currently ignored,
- no historical catch-up snapshot is emitted by current handler.

### 3.5 `GetClusterStats`

Behavior depends on storage mode:

Postgres mode:
- queries current `PENDING` and `RUNNING` counts from `tasks`,
- computes 24h window counts for completed and failed tasks,
- includes active worker count from `WorkerManager`.

Non-Postgres mode:
- returns active worker count only.

Proto includes additional fields (`overdue_tasks`, `cancelled_in_window`, etc.); these are currently zero/unset in current server logic.

### 3.6 `CancelTask`, `RetryTask`, `ListTaskLogs`

These are declared in proto for control-plane UX, but not overridden in the service yet.
Current behavior:
- gRPC `Unimplemented` status from embedded stub methods.

## 4. Domain and State Semantics

Domain state enum (internal):
- `PENDING`, `SCHEDULED`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`

Transition rules enforced in `internal/domain/task.go`.

Retry policy:
- exponential delay in seconds: `2^retry_count` after increment.

Cancellation policy:
- allowed from non-terminal states.
- terminal states reject transition as finalized; cancel RPC remains idempotent at service boundary.

## 5. SDK Surface Mapping

## 5.1 Go Client SDK (`pkg/client`)

Main methods:
- `client.New(serverAddr, opts...)`
- `(*Client).SubmitTask(ctx, SubmitRequest)`
- `(*Client).CancelTask(ctx, taskID)`

`SubmitRequest` fields map directly to `SubmitTaskRequest`.

## 5.2 Go Worker SDK (`pkg/worker`)

Main methods:
- `worker.New(serverAddr, opts...)`
- `(*Worker).Handle(taskType, handler)`
- `(*Worker).Run(ctx)`

Worker behavior includes:
- automatic reconnect loop,
- per-task timeout context,
- cancellation handling via stream events,
- graceful drain on shutdown.

Marking retryable handler errors:

```go
return nil, worker.Retryable(err)
```

## 6. Useful `grpcurl` Commands

List services:

```bash
grpcurl -plaintext localhost:50051 list
```

Submit task:

```bash
grpcurl -plaintext -d '{
  "task_id":"11111111-1111-1111-1111-111111111111",
  "type":"email",
  "payload":"aGVsbG8="
}' localhost:50051 api.v1.Orchestrator/SubmitTask
```

List tasks (control-plane direct gRPC):

```bash
grpcurl -plaintext -d '{"page_size": 20}' \
  localhost:50051 orchestrator.v1.ControlPlaneService/ListTasks
```

## 7. Compatibility Notes

1. Proto files may expose fields and methods ahead of full server implementation.
2. Always check runtime status in this document when integrating control-plane operations.
3. If building UI/API clients, treat unimplemented control-plane methods as expected until completed.
