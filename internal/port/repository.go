package port

import (
	"context"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
)

type TaskRepository interface {
	Create(ctx context.Context, task *domain.Task) error
	Get(ctx context.Context, id string) (*domain.Task, error)
	Update(ctx context.Context, task *domain.Task) error
	ListEligible(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error)
	ReleaseTasks(ctx context.Context, workerID string) error
	// RecoverTasks resets abandoned assignments at startup. Requires exclusive server ownership.
	RecoverTasks(ctx context.Context) error
	ListTasks(ctx context.Context, filter *domain.TaskFilter) ([]*domain.Task, error)
}
