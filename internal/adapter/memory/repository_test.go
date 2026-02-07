package memory

import (
	"context"
	"testing"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
)

func TestInMemoryTaskRepository_ListEligible(t *testing.T) {
	repo := New()
	ctx := context.Background()

	now := time.Now().UTC()
	future := now.Add(1 * time.Hour)
	past := now.Add(-1 * time.Hour)

	tasks := []*domain.Task{
		domain.NewTask("1", "immediate", nil, past),
		domain.NewTask("2", "future", nil, future),
		domain.NewTask("3", "immediate_2", nil, past),
	}

	tasks[2].State = domain.Completed

	for _, task := range tasks {
		if err := repo.Create(ctx, task); err != nil {
			t.Fatalf("failed to create task: %v", err)
		}
	}

	eligible, err := repo.ListEligible(ctx, now, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(eligible) != 1 {
		t.Errorf("expected 1 eligible task (Task 1), got %d", len(eligible))
		for _, et := range eligible {
			t.Logf("Got eligible task: %s (State: %s, RunAt: %s)", et.ID, et.State, et.RunAt)
		}
	}

	if len(eligible) > 0 && eligible[0].ID != "1" {
		t.Errorf("expected task 1, got %s", eligible[0].ID)
	}
}
