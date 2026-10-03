package service

import (
	"context"
	"testing"
	"time"

	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/domain"
)

func TestDispatcher_NoWorkersRequeues(t *testing.T) {
	logger := noopLogger{}
	repo := memory.New(logger)
	task := domain.NewTask("orphan", "", "job", nil, time.Time{}, 0)
	task.State = domain.Scheduled
	if err := repo.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(NewWorkerManager(logger), nil, logger, repo, nil)
	dispatcher.dispatch(context.Background(), task)
	got, err := repo.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.Pending || got.WorkerID != "" {
		t.Fatalf("orphan not requeued: %+v", got)
	}
}

func TestDispatcher_DoesNotReviveCancelledQueuedTask(t *testing.T) {
	logger := noopLogger{}
	repo := memory.New(logger)
	task := domain.NewTask("cancel", "", "job", nil, time.Time{}, 0)
	task.State = domain.Scheduled
	if err := repo.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	cancelled := *task
	cancelled.State = domain.Cancelled
	if err := repo.Update(context.Background(), &cancelled); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(NewWorkerManager(logger), nil, logger, repo, nil)
	dispatcher.dispatch(context.Background(), task)
	got, _ := repo.Get(context.Background(), task.ID)
	if got.State != domain.Cancelled {
		t.Fatalf("state = %s", got.State)
	}
}

func TestStateManager_ShutdownWithFullQueue(t *testing.T) {
	logger := noopLogger{}
	repo := memory.New(logger)
	if err := repo.Create(context.Background(), domain.NewTask("blocked", "", "job", nil, time.Time{}, 0)); err != nil {
		t.Fatal(err)
	}
	manager := NewStateManager(repo, logger, 1, make(chan *domain.Task), nil)
	manager.interval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { manager.Run(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		task, _ := repo.Get(context.Background(), "blocked")
		if task.State == domain.Scheduled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduler did not reach queue")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop with a full queue")
	}
}
