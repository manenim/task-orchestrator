package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	r.store[t.ID] = cloneTask(t)
	return nil
}

func (r *InMemoryTaskRepository) ListEligible(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var tasks []*domain.Task

	for _, t := range r.store {
		if t.State == domain.Pending && (t.RunAt.Before(now) || t.RunAt.Equal(now)) {
			tasks = append(tasks, cloneTask(t))
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
	return cloneTask(task), nil
}
func (r *InMemoryTaskRepository) Update(ctx context.Context, t *domain.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.store[t.ID]; !exists {
		return fmt.Errorf("task %s not found", t.ID)
	}
	r.store[t.ID] = cloneTask(t)
	return nil
}

func (r *InMemoryTaskRepository) ReleaseTasks(ctx context.Context, workerID string) error {
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

func (r *InMemoryTaskRepository) ListTasks(ctx context.Context, filter *domain.TaskFilter) ([]*domain.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if filter == nil {
		filter = &domain.TaskFilter{}
	}

	tasks := make([]*domain.Task, 0, len(r.store))

	for _, t := range r.store {
		if len(filter.States) > 0 {
			match := false
			for _, s := range filter.States {
				if t.State == s {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		if len(filter.TaskTypes) > 0 {
			match := false
			for _, typ := range filter.TaskTypes {
				if t.Type == typ {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		if filter.WorkerID != "" && t.WorkerID != filter.WorkerID {
			continue
		}

		if filter.TaskIDPrefix != "" && !strings.HasPrefix(t.ID, filter.TaskIDPrefix) {
			continue
		}

		if filter.TextQuery != "" {
			q := strings.ToLower(filter.TextQuery)
			if !strings.Contains(strings.ToLower(t.ID), q) &&
				!strings.Contains(strings.ToLower(t.Type), q) &&
				!strings.Contains(strings.ToLower(t.ClientID), q) {
				continue
			}
		}

		if filter.CreatedAt != nil {
			if !filter.CreatedAt.Start.IsZero() && t.CreatedAt.Before(filter.CreatedAt.Start) {
				continue
			}
			if !filter.CreatedAt.End.IsZero() && !t.CreatedAt.Before(filter.CreatedAt.End) {
				continue
			}
		}

		if filter.RunAt != nil {
			if !filter.RunAt.Start.IsZero() && t.RunAt.Before(filter.RunAt.Start) {
				continue
			}
			if !filter.RunAt.End.IsZero() && !t.RunAt.Before(filter.RunAt.End) {
				continue
			}
		}

		tasks = append(tasks, cloneTask(t))
	}

	sort.Slice(tasks, func(i, j int) bool {
		var less bool
		switch filter.SortBy {
		case domain.SortByCreatedAt:
			less = tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
		case domain.SortByRunAt:
			less = tasks[i].RunAt.Before(tasks[j].RunAt)
		case domain.SortByUpdatedAt:
			less = tasks[i].UpdatedAt.Before(tasks[j].UpdatedAt)
		default:
			less = tasks[i].UpdatedAt.Before(tasks[j].UpdatedAt)
		}

		if filter.SortDir == domain.SortAsc {
			return less
		}
		return !less
	})

	start := filter.Offset
	if start > len(tasks) {
		start = len(tasks)
	}
	end := start + filter.Limit
	if filter.Limit <= 0 {
		end = start + 50
	}
	if end > len(tasks) {
		end = len(tasks)
	}

	return tasks[start:end], nil
}

// cloneTask keeps repository state private after the lock is released.
func cloneTask(task *domain.Task) *domain.Task {
	snapshot := *task
	snapshot.Payload = append([]byte(nil), task.Payload...)
	snapshot.Result = append([]byte(nil), task.Result...)
	return &snapshot
}

func (r *InMemoryTaskRepository) RecoverTasks(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, task := range r.store {
		if task.State == domain.Scheduled || task.State == domain.Running {
			if err := task.UpdateState(domain.Pending); err != nil {
				return err
			}
			task.WorkerID = ""
		}
	}
	return nil
}
