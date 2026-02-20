// Package taskorch is the root module for the Task Orchestrator, a
// distributed task processing system built on gRPC streaming.
//
// The module is organized as follows:
//
//   - pkg/worker: Embeddable worker library. Connect to an orchestrator,
//     register typed handlers, and process tasks with automatic reconnection
//     and graceful shutdown.
//   - pkg/client: Go SDK for submitting and cancelling tasks.
//     Wraps the gRPC stubs behind clean Go types.
//   - internal/domain: Core domain model including Task, TaskState, and
//     the state machine that governs task lifecycle transitions.
//   - internal/service: Orchestrator, Dispatcher, StateManager, and
//     ControlPlane services.
//   - internal/adapter: Storage implementations (PostgreSQL, Redis,
//     in-memory) and logging adapters.
//
// Getting started with the worker SDK:
//
//	w, _ := worker.New("localhost:50051")
//	w.Handle("email", func(ctx context.Context, t worker.Task) ([]byte, error) {
//		return []byte("sent"), nil
//	})
//	w.Run(ctx)
//
// Getting started with the client SDK:
//
//	c, _ := client.New("localhost:50051")
//	id, _ := c.SubmitTask(ctx, client.SubmitRequest{
//		TaskID: uuid.New().String(),
//		Type:   "email",
//	})
package taskorch
