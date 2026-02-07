package service

import (
	"context"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
)

type StateManager struct {
	repo     port.TaskRepository
	logger   port.Logger
	interval time.Duration
	batchSize int
}

func NewStateManager(repo port.TaskRepository, logger port.Logger, batchSize int) *StateManager {
	return &StateManager{
		repo:     repo,
		logger:   logger,
		interval: time.Duration(500 * time.Millisecond),
		batchSize: batchSize,
	}
}

func (s *StateManager) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := s.repo.ListEligible(ctx, time.Now().UTC(), s.batchSize)
			if err != nil {
				s.logger.Error("failed to list eligible tasks", err)
			} else if len(tasks) > 0 {
				s.logger.Info("found eligible tasks", port.Int("count", len(tasks)))
				for _, task := range tasks {
					s.logger.Info("Scheduling Task", port.String("task_id", task.ID))
					if err := task.UpdateState(domain.Scheduled); err != nil {
						s.logger.Error("Failed to update state", err, port.String("task_id", task.ID))
						continue
					}
					if err := s.repo.Update(ctx, task); err != nil {
						s.logger.Error("failed to save task", err, port.String("task_id", task.ID))
						continue
					}

				}
			}
		}
	}
}
