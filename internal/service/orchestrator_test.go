package service

import (
	"context"
	"testing"
	"time"

	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
)

type noopLogger struct{}

func (noopLogger) Info(string, ...port.Field)         {}
func (noopLogger) Error(string, error, ...port.Field) {}
func (noopLogger) Sync() error                        { return nil }

func newTestOrchestrator() (*Orchestrator, port.TaskRepository) {
	logger := &noopLogger{}
	repo := memory.New(logger)
	wm := NewWorkerManager(logger)
	return New(repo, logger, wm, nil), repo
}

func TestSubmitTask_Success(t *testing.T) {
	orch, repo := newTestOrchestrator()

	resp, err := orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:   "task-1",
		Type:     "email",
		ClientId: "client-1",
		Payload:  []byte("hello"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.TaskId != "task-1" {
		t.Errorf("expected task-1, got %s", resp.TaskId)
	}

	task, err := repo.Get(context.Background(), "task-1")
	if err != nil {
		t.Fatalf("task not found in repo: %v", err)
	}
	if task.State != domain.Pending {
		t.Errorf("expected Pending, got %v", task.State)
	}
	if task.Type != "email" {
		t.Errorf("expected type 'email', got %q", task.Type)
	}
}

func TestSubmitTask_MissingType(t *testing.T) {
	orch, _ := newTestOrchestrator()

	_, err := orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "task-2",
		Type:   "",
	})
	if err == nil {
		t.Fatal("expected error for missing type")
	}
}

func TestSubmitTask_MissingID(t *testing.T) {
	orch, _ := newTestOrchestrator()

	_, err := orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "",
		Type:   "email",
	})
	if err == nil {
		t.Fatal("expected error for missing task ID")
	}
}

func TestSubmitTask_WithRunAt(t *testing.T) {
	orch, repo := newTestOrchestrator()

	future := time.Now().Add(1 * time.Hour)
	_, err := orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "delayed-1",
		Type:   "email",
		RunAt:  nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	task, _ := repo.Get(context.Background(), "delayed-1")
	if task.RunAt.After(future) {
		t.Error("task without RunAt should have RunAt ≤ now, not in the future")
	}
}

func TestCancelTask_Success(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "cancel-1",
		Type:   "email",
	})

	resp, err := orch.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TaskId: "cancel-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Error("expected Success=true")
	}

	task, _ := repo.Get(context.Background(), "cancel-1")
	if task.State != domain.Cancelled {
		t.Errorf("expected Cancelled, got %v", task.State)
	}
}

func TestCancelTask_AlreadyCompleted(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "done-1",
		Type:   "email",
	})

	task, _ := repo.Get(context.Background(), "done-1")
	task.State = domain.Completed
	repo.Update(context.Background(), task)

	resp, err := orch.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TaskId: "done-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Error("cancelling a completed task should return Success=true (idempotent)")
	}
}

func TestCancelTask_MissingID(t *testing.T) {
	orch, _ := newTestOrchestrator()

	_, err := orch.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TaskId: "",
	})
	if err == nil {
		t.Fatal("expected error for missing task ID")
	}
}

func TestCancelTask_NotFound(t *testing.T) {
	orch, _ := newTestOrchestrator()

	_, err := orch.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TaskId: "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestCompleteTask_Success(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "comp-1",
		Type:   "email",
	})

	task, _ := repo.Get(context.Background(), "comp-1")
	task.State = domain.Running
	repo.Update(context.Background(), task)

	resp, err := orch.CompleteTask(context.Background(), &pb.CompleteTaskRequest{
		TaskId:   "comp-1",
		WorkerId: "worker-1",
		Result:   []byte("done"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopStream {
		t.Error("StopStream should be false")
	}

	updated, _ := repo.Get(context.Background(), "comp-1")
	if updated.State != domain.Completed {
		t.Errorf("expected Completed, got %v", updated.State)
	}
	if string(updated.Result) != "done" {
		t.Errorf("expected result 'done', got %q", string(updated.Result))
	}
}

func TestCompleteTask_WithRetryableError(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "retry-1",
		Type:   "email",
	})

	task, _ := repo.Get(context.Background(), "retry-1")
	task.State = domain.Running
	repo.Update(context.Background(), task)

	resp, err := orch.CompleteTask(context.Background(), &pb.CompleteTaskRequest{
		TaskId:       "retry-1",
		WorkerId:     "worker-1",
		ErrorMessage: "transient failure",
		IsRetryable:  true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopStream {
		t.Error("StopStream should be false")
	}

	updated, _ := repo.Get(context.Background(), "retry-1")
	if updated.State != domain.Pending {
		t.Errorf("expected Pending (retry), got %v", updated.State)
	}
	if updated.RetryCount != 1 {
		t.Errorf("expected RetryCount=1, got %d", updated.RetryCount)
	}
}

func TestCompleteTask_FatalError(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "fatal-1",
		Type:   "email",
	})

	task, _ := repo.Get(context.Background(), "fatal-1")
	task.State = domain.Running
	repo.Update(context.Background(), task)

	_, err := orch.CompleteTask(context.Background(), &pb.CompleteTaskRequest{
		TaskId:       "fatal-1",
		WorkerId:     "worker-1",
		ErrorMessage: "bad input",
		IsRetryable:  false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := repo.Get(context.Background(), "fatal-1")
	if updated.State != domain.Failed {
		t.Errorf("expected Failed, got %v", updated.State)
	}
}

func TestCompleteTask_ExhaustedRetries(t *testing.T) {
	orch, repo := newTestOrchestrator()

	orch.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: "exhaust-1",
		Type:   "email",
	})

	task, _ := repo.Get(context.Background(), "exhaust-1")
	task.State = domain.Running
	task.RetryCount = 3
	task.MaxRetries = 3
	repo.Update(context.Background(), task)

	_, err := orch.CompleteTask(context.Background(), &pb.CompleteTaskRequest{
		TaskId:       "exhaust-1",
		WorkerId:     "worker-1",
		ErrorMessage: "still failing",
		IsRetryable:  true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := repo.Get(context.Background(), "exhaust-1")
	if updated.State != domain.Failed {
		t.Errorf("expected Failed after exhausting retries, got %v", updated.State)
	}
}

func TestStatusFromError(t *testing.T) {
	orch, _ := newTestOrchestrator()

	err := orch.statusFromError(domain.ErrTaskNotFound)
	if err == nil {
		t.Fatal("expected non-nil error")
	}

	err = orch.statusFromError(domain.ErrInvalidTransition)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
}
