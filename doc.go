// Package taskorch provides a distributed task orchestration system built on
// gRPC streaming.
//
// Core behavior:
//   - clients submit tasks through the worker-plane API,
//   - the server schedules eligible tasks and dispatches them to connected workers,
//   - workers execute typed handlers and report success/failure,
//   - the orchestrator enforces state transitions, retries, timeouts, and cancellation.
//
// High-level state flow:
//
//	PENDING -> SCHEDULED -> RUNNING -> COMPLETED
//	                     -> PENDING   (retry)
//	                     -> FAILED
//	PENDING/SCHEDULED/RUNNING -> CANCELLED
//
// Main packages:
//   - pkg/worker: Embeddable worker SDK with reconnect and graceful drain.
//   - pkg/client: Embeddable Go client SDK for submit/cancel.
//   - internal/domain: Task model and state-machine validation.
//   - internal/service: Orchestrator, dispatcher, scheduler, control-plane.
//   - internal/adapter: Storage adapters (Postgres, Redis, in-memory) and logging.
//
// Minimal worker example:
//
//	w, err := worker.New("localhost:50051")
//	if err != nil {
//		panic(err)
//	}
//	w.Handle("email", func(ctx context.Context, t worker.Task) ([]byte, error) {
//		return []byte("sent"), nil
//	})
//	if err := w.Run(ctx); err != nil {
//		panic(err)
//	}
//
// Minimal client example:
//
//	c, err := client.New("localhost:50051")
//	if err != nil {
//		panic(err)
//	}
//	defer c.Close()
//
//	_, err = c.SubmitTask(ctx, client.SubmitRequest{
//		TaskID: uuid.New().String(),
//		Type:   "email",
//	})
//	if err != nil {
//		panic(err)
//	}
//
// For full operational and API details, see:
//   - README.md
//   - docs/architecture.md
//   - docs/api-reference.md
//   - docs/runbook.md
package taskorch
