# API Reference

## Worker-Plane: `api.v1.Orchestrator`

Proto: [`api/proto/v1/orchestrator.proto`](../api/proto/v1/orchestrator.proto)
Endpoint: `localhost:50051`

| RPC | Type | Description |
|-----|------|-------------|
| `SubmitTask` | Unary | Submit a new task for processing |
| `StreamTasks` | Server streaming | Worker connects and receives assigned tasks |
| `CompleteTask` | Unary | Worker reports task result (success or failure) |
| `CancelTask` | Unary | Cancel a running or pending task |
| `RegisterWorker` | Unary | Register worker capabilities (reserved) |

### SubmitTask
```
Request: { task_id, type, payload, client_id, run_at?, max_retries?, timeout_seconds? }
Response: { task_id }
```
- `task_id` must be client-generated (UUID) for idempotency
- `run_at` zero → schedule immediately
- `timeout_seconds` zero → server default (30m)

### StreamTasks
```
Request: { worker_id }
Response stream: { task_id, job_type, payload, is_cancellation, timeout_seconds }
```
- Bidirectional lifecycle: worker connects, receives tasks, receives cancellations
- `is_cancellation=true` means cancel a previously assigned task

### CompleteTask
```
Request: { task_id, worker_id, error_message?, result?, is_retryable? }
Response: { stop_stream }
```
- `error_message` set → task failed
- `is_retryable=true` + retries remaining → re-enqueue with exponential backoff
- `is_retryable=true` + retries exhausted → mark as FAILED

---

## Control-Plane: `orchestrator.v1.ControlPlaneService`

Proto: [`api/proto/orchestrator/v1/control_plane.proto`](../api/proto/orchestrator/v1/control_plane.proto)
Endpoint: `localhost:8080` (via Envoy gRPC-Web)

| RPC | Type | Description |
|-----|------|-------------|
| `ListTasks` | Unary | Query tasks with filters, sorting, pagination |
| `GetTask` | Unary | Get a single task by ID |
| `StreamTaskEvents` | Server streaming | Real-time task state change events |
| `CancelTask` | Unary | Cancel a task from the UI |
| `RetryTask` | Unary | Retry a failed task |
| `GetClusterStats` | Unary | Cluster health: active workers, state counts |
| `ListTaskLogs` | Unary | Structured logs for a task |

### ListTasks Filters
- `states[]` — filter by state (PENDING, RUNNING, etc.)
- `task_types[]` — filter by task type
- `worker_id` — tasks assigned to a specific worker
- `text_query` — full-text search across ID, type, client
- `sort_by` — created_at, run_at, updated_at
- `sort_direction` — ASC or DESC
- `page_size` / `page_token` — cursor-based pagination

---

## Go SDK: `pkg/client`

```go
import "github.com/manenim/task-orchestrator/pkg/client"

c, _ := client.New("localhost:50051")
defer c.Close()

// Submit
id, _ := c.SubmitTask(ctx, client.SubmitRequest{
    TaskID: uuid.New().String(),
    Type:   "email",
    Payload: []byte(`{"to":"user@example.com"}`),
})

// Cancel
c.CancelTask(ctx, id)
```

## Worker SDK: `pkg/worker`

```go
import "github.com/manenim/task-orchestrator/pkg/worker"

w, _ := worker.New("localhost:50051")
w.Handle("email", func(ctx context.Context, t worker.Task) ([]byte, error) {
    // process...
    return []byte("sent"), nil
})
w.Handle("*", fallbackHandler) // wildcard
w.Run(ctx) // blocks, auto-reconnects
```

Mark errors as retryable:
```go
return nil, worker.Retryable(fmt.Errorf("transient failure"))
```
