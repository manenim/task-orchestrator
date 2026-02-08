package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
)

type InMemoryTaskRepository struct {
	mu     sync.RWMutex
	store  map[string]*domain.Task
	logger port.Logger
}

func New(logger port.Logger) *InMemoryTaskRepository {
	return &InMemoryTaskRepository{
		store:  make(map[string]*domain.Task),
		logger: logger,
	}
}
func (r *InMemoryTaskRepository) Create(ctx context.Context, t *domain.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.store[t.ID]; exists {
		return fmt.Errorf("task already exists: %s", t.ID)
	}
	r.store[t.ID] = t
	return nil
}

func (r *InMemoryTaskRepository) ListEligible(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var tasks []*domain.Task

	for _, t := range r.store {
		if t.State == domain.Pending && (t.RunAt.Before(now) || t.RunAt.Equal(now)) {
			tasks = append(tasks, t)
			if len(tasks) >= limit {
				break
			}
		}
	}
	return tasks, nil
}
func (r *InMemoryTaskRepository) Get(ctx context.Context, id string) (*domain.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	task, exists := r.store[id]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}
	return task, nil
}
func (r *InMemoryTaskRepository) Update(ctx context.Context, t *domain.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.store[t.ID]; !exists {
		return fmt.Errorf("task %s not found", t.ID)
	}
	r.store[t.ID] = t
	return nil
}

func (r *InMemoryTaskRepository) ReleaseTasks(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, task := range r.store {
		if task.WorkerID == workerID && (task.State == domain.Running || task.State == domain.Scheduled) {

			if err := task.UpdateState(domain.Pending); err != nil {
				r.logger.Error("Failed to release task", err, port.String("task_id", task.ID))
				continue
			}
			task.WorkerID = ""
			r.logger.Info("Released task from dead worker", port.String("task_id", task.ID), port.String("worker_id", workerID))
		}
	}

	return nil
}
