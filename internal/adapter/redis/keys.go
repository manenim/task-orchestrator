package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	scheduleKey   = "tasks:schedule"
	taskKeyPrefix = "task:"
)

func taskKey(id string) string {
	return taskKeyPrefix + id
}

func workerTasksKey(workerID string) string {
	return fmt.Sprintf("worker:%s:tasks", workerID)
}

func marshalTask(task *domain.Task) ([]byte, error) {
	return json.Marshal(task)
}

func unmarshalTask(data []byte) (*domain.Task, error) {
	var task domain.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}

	task.RunAt = task.RunAt.UTC()
	task.CreatedAt = task.CreatedAt.UTC()
	task.UpdatedAt = task.UpdatedAt.UTC()
	if !task.LastFailedAt.IsZero() {
		task.LastFailedAt = task.LastFailedAt.UTC()
	}
	return &task, nil
}

func addToSchedule(ctx context.Context, pipe redis.Pipeliner, task *domain.Task) {
	pipe.ZAdd(ctx, scheduleKey, redis.Z{
		Score:  float64(task.RunAt.Unix()),
		Member: task.ID,
	})
}

func removeFromSchedule(ctx context.Context, pipe redis.Pipeliner, taskID string) {
	pipe.ZRem(ctx, scheduleKey, taskID)
}

func assignToWorker(ctx context.Context, pipe redis.Pipeliner, workerID, taskID string) {
	pipe.SAdd(ctx, workerTasksKey(workerID), taskID)
}

func unassignFromWorker(ctx context.Context, pipe redis.Pipeliner, workerID, taskID string) {
	pipe.SRem(ctx, workerTasksKey(workerID), taskID)
}
