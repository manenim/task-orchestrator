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
| `task_id` | Yes | Must be non-empty. Client-generated. Postgres requires a UUID; duplicate semantics currently differ between backends. |
| `type` | Yes | Task type used by worker handler lookup. |
| `payload` | No | Opaque bytes passed to worker. |
| `client_id` | No | Stored on task metadata. |
| `run_at` | No | Zero or omitted means immediate scheduling (`now`). |
| `max_retries` | No | Honoured; `0` (also omitted) disables retries; values `0..30` accepted. |
| `timeout_seconds` | No | Non-negative; stored and passed to worker stream event. |

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
| `worker_id` | Yes | Must match the current assignment; stale workers receive `FailedPrecondition`. |
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
| `CancelTask` | Unary | Yes | Yes | Unconditional cancel; nonzero `expected_version` returns `Unimplemented` |
| `RetryTask` | Unary | Yes | No | Returns `Unimplemented` |
| `GetClusterStats` | Unary | Yes | Yes | Callable; partial metrics in non-Postgres mode |
| `ListTaskLogs` | Unary | Yes | Postgres only | Paginated persistent logs; other storage returns `Unimplemented` |

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

Payload and result are returned only when the corresponding `include_payload`/`include_result` flag is true.

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

### 3.6 `CancelTask`

Unconditional cancellation persists `CANCELLED`, signals an assigned worker, and records the request reason. `accepted=true` means the state change was saved, not that the handler has stopped. A terminal task returns `already_terminal=true`, `accepted=false`, and its unchanged snapshot. Responses omit payload and result. Worker signalling is best effort; handlers must respect cancellation contexts.

Nonzero `expected_version` returns `Unimplemented`, because repositories do not enforce atomic compare-and-swap. `request_id` is not stored; terminal-state handling provides idempotence for repeated unconditional cancellation. Scope labels do not provide tenant isolation.

### 3.7 `ListTaskLogs`

Postgres only. Requires `task_id`; returns persisted orchestrator event reasons, not arbitrary worker stdout. Page size defaults to 50 and caps at 500. Entries are newest first, with an opaque offset cursor and one extra row to determine `has_more`. Concurrent new logs can shift offset pages; this is not a stable snapshot cursor. Other storage drivers return `Unimplemented`.

### 3.8 `RetryTask`

Manual retry is deliberately unsupported and returns gRPC `Unimplemented`. Automatic retries follow `max_retries`. To replay a finalized task, submit a new task ID after checking whether its side effects already occurred. Safe mutation of finalized tasks requires atomic updates and attempt fencing, which are future work.

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
3. `RetryTask` and conditional cancellation remain explicitly unsupported.
4. Scope is descriptive metadata, not an authorization boundary. Request-side event filters/resume are not implemented; `GetClusterStats` ignores a custom window and uses 24 hours.
5. `Version` is persisted in Postgres but is not an atomic concurrency guard or an execution-attempt token.
