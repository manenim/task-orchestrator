// Package worker provides an embeddable worker library for the Task Orchestrator.
//
// Users implement TaskHandler functions and register them by task type.
// The Worker handles gRPC streaming, reconnection, cancellation, timeouts,
// and graceful shutdown automatically.
//
// Example:
//
//	w, _ := worker.New("localhost:50051",
//	    worker.WithLogger(myLogger),
//	)
//	w.Handle("email", sendEmail)
//	w.Handle("resize", resizeImage)
//	w.Run(ctx) // blocks until ctx is cancelled
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Task is the read-only view of an assigned task delivered to a handler.
type Task struct {
	ID      string
	Type    string
	Payload []byte
	Timeout time.Duration
}

// TaskHandler processes a single task. Return the result bytes on success,
// or an error on failure. The context carries the task's timeout and
// cancellation signal.
type TaskHandler func(ctx context.Context, task Task) ([]byte, error)

// RetryableError wraps an error to signal that the orchestrator should
// retry the task with exponential backoff.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps an error to mark it as retryable.
func Retryable(err error) error {
	return &RetryableError{Err: err}
}

// Worker connects to a Task Orchestrator server, receives tasks via
// gRPC streaming, and executes registered handlers.
type Worker struct {
	serverAddr string
	opts       *options
	handlers   map[string]TaskHandler
	fallback   TaskHandler
}

// New creates a new Worker targeting the given server address.
func New(serverAddr string, opts ...Option) (*Worker, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(o)
	}

	if o.workerID == "" {
		o.workerID = "worker-" + uuid.New().String()
	}

	return &Worker{
		serverAddr: serverAddr,
		opts:       o,
		handlers:   make(map[string]TaskHandler),
	}, nil
}

// Handle registers a handler for a specific task type.
// Use "*" as the taskType to set a fallback handler for unregistered types.
func (w *Worker) Handle(taskType string, handler TaskHandler) {
	if taskType == "*" {
		w.fallback = handler
		return
	}
	w.handlers[taskType] = handler
}

// ID returns the worker's identifier.
func (w *Worker) ID() string {
	return w.opts.workerID
}

// Run connects to the orchestrator and starts processing tasks.
// It blocks until ctx is cancelled and all in-flight tasks complete.
// It automatically reconnects on stream errors.
func (w *Worker) Run(ctx context.Context) error {
	dialOpts := w.opts.dialOpts
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conn, err := grpc.NewClient(w.serverAddr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewOrchestratorClient(conn)
	w.opts.logger.Info("Worker started", "worker_id", w.opts.workerID, "server", w.serverAddr)

	for {
		if err := w.stream(ctx, client); err != nil {
			if ctx.Err() != nil {
				w.opts.logger.Info("Worker shutting down", "worker_id", w.opts.workerID)
				return nil
			}
			w.opts.logger.Error("Stream error, reconnecting", err,
				"delay", w.opts.reconnectDelay.String())

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(w.opts.reconnectDelay):
			}
		}
	}
}

// stream runs a single streaming session. It returns when the stream
// breaks or the context is cancelled (after draining in-flight tasks).
func (w *Worker) stream(ctx context.Context, client pb.OrchestratorClient) error {
	runningTasks := make(map[string]context.CancelFunc)
	var mu sync.Mutex
	var wg sync.WaitGroup
	defer wg.Wait()

	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()

	stream, err := client.StreamTasks(streamCtx, &pb.StreamTasksRequest{
		WorkerId: w.opts.workerID,
	})
	if err != nil {
		return err
	}

	w.opts.logger.Info("Connected to orchestrator, waiting for tasks...")

	go func() {
		select {
		case <-ctx.Done():
			w.opts.logger.Info("Shutdown signal received, draining tasks...")
			wg.Wait()
			cancelStream()
			w.opts.logger.Info("All tasks drained, stream closed")
		case <-streamCtx.Done():
		}
	}()

	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		if event.IsCancellation {
			mu.Lock()
			if cancel, exists := runningTasks[event.TaskId]; exists {
				cancel()
				delete(runningTasks, event.TaskId)
				w.opts.logger.Info("Task cancelled", "task_id", event.TaskId)
			}
			mu.Unlock()
			continue
		}

		timeout := w.opts.defaultTimeout
		if event.TimeoutSeconds > 0 {
			timeout = time.Duration(event.TimeoutSeconds) * time.Second
		}

		taskCtx, cancel := context.WithTimeout(context.Background(), timeout)

		mu.Lock()
		runningTasks[event.TaskId] = cancel
		mu.Unlock()

		wg.Add(1)
		go w.executeTask(ctx, client, event, taskCtx, cancel, &mu, runningTasks, &wg)
	}
}

// executeTask runs a single task handler and reports the result back.
func (w *Worker) executeTask(
	_ context.Context,
	client pb.OrchestratorClient,
	event *pb.TaskEvent,
	taskCtx context.Context,
	cancel context.CancelFunc,
	mu *sync.Mutex,
	runningTasks map[string]context.CancelFunc,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer cancel()
	defer func() {
		mu.Lock()
		delete(runningTasks, event.TaskId)
		mu.Unlock()
	}()

	task := Task{
		ID:      event.TaskId,
		Type:    event.JobType,
		Payload: event.Payload,
	}
	if event.TimeoutSeconds > 0 {
		task.Timeout = time.Duration(event.TimeoutSeconds) * time.Second
	} else {
		task.Timeout = w.opts.defaultTimeout
	}

	handler, ok := w.handlers[event.JobType]
	if !ok {
		handler = w.fallback
	}
	if handler == nil {
		w.opts.logger.Error("No handler registered", nil, "task_type", event.JobType, "task_id", event.TaskId)
		_, _ = client.CompleteTask(context.Background(), &pb.CompleteTaskRequest{
			TaskId:       event.TaskId,
			WorkerId:     w.opts.workerID,
			ErrorMessage: "no handler registered for task type: " + event.JobType,
		})
		return
	}

	w.opts.logger.Info("Executing task", "task_id", event.TaskId, "type", event.JobType, "timeout", task.Timeout.String())

	result, handlerErr := handler(taskCtx, task)

	req := &pb.CompleteTaskRequest{
		TaskId:   event.TaskId,
		WorkerId: w.opts.workerID,
	}

	if handlerErr != nil {
		req.ErrorMessage = handlerErr.Error()
		if _, ok := handlerErr.(*RetryableError); ok {
			req.IsRetryable = true
		}
		w.opts.logger.Error("Task failed", handlerErr, "task_id", event.TaskId, "retryable", req.IsRetryable)
	} else {
		req.Result = result
		w.opts.logger.Info("Task completed", "task_id", event.TaskId)
	}

	if _, err := client.CompleteTask(context.Background(), req); err != nil {
		w.opts.logger.Error("Failed to report task completion", err, "task_id", event.TaskId)
	}
}
