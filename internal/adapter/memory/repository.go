package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
)
type InMemoryTaskRepository struct {
	mu    sync.RWMutex
	store map[string]*domain.Task
}

func New() *InMemoryTaskRepository {
	return &InMemoryTaskRepository{
		store: make(map[string]*domain.Task),
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