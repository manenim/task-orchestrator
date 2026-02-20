package service

import (
	"context"

	"github.com/manenim/task-orchestrator/internal/domain"
	cpb "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
)

type TaskStatePublisher interface {
	PublishTaskEvent(ctx context.Context, eventType cpb.TaskEventType, previousState domain.TaskState, task *domain.Task, reason string)
	PublishWorkerEvent(ctx context.Context, workerID string, connected bool)
}
