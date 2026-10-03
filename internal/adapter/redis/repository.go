package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	"github.com/redis/go-redis/v9"
)

type RedisTaskRepository struct {
	client *redis.Client
	logger port.Logger
}

func New(addr string, logger port.Logger) port.TaskRepository {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisTaskRepository{client: client, logger: logger}
}

func (r *RedisTaskRepository) Create(ctx context.Context, task *domain.Task) error {
	data, err := marshalTask(task)
	if err != nil {
		return fmt.Errorf("failed to encode task: %w", err)
	}

	_, err = r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, taskKey(task.ID), data, 0)

		if task.State == domain.Pending || task.State == domain.Scheduled {
			addToSchedule(ctx, pipe, task)
		}
		if task.WorkerID != "" {
			assignToWorker(ctx, pipe, task.WorkerID, task.ID)
		}
		return nil
	})
	return err
}

func (r *RedisTaskRepository) Get(ctx context.Context, id string) (*domain.Task, error) {
	val, err := r.client.Get(ctx, taskKey(id)).Result()
	if err == redis.Nil {
		return nil, domain.ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}

	return unmarshalTask([]byte(val))
}

func (r *RedisTaskRepository) Update(ctx context.Context, task *domain.Task) error {
	data, err := marshalTask(task)
	if err != nil {
		return fmt.Errorf("failed to encode task: %w", err)
	}

	_, err = r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, taskKey(task.ID), data, 0)

		if task.State == domain.Pending || task.State == domain.Scheduled {
			addToSchedule(ctx, pipe, task)
		} else {
			removeFromSchedule(ctx, pipe, task.ID)
		}

		if task.WorkerID != "" {
			assignToWorker(ctx, pipe, task.WorkerID, task.ID)
		}
		return nil
	})
	return err
}

func (r *RedisTaskRepository) ListEligible(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error) {
	ids, err := r.client.ZRangeByScore(ctx, scheduleKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now.Unix()),
		Count:  int64(limit),
		Offset: 0,
	}).Result()

	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []*domain.Task{}, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = taskKey(id)
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*domain.Task, 0, len(vals))
	for _, val := range vals {
		if val == nil {
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			continue
		}

		task, err := unmarshalTask([]byte(strVal))
		if err != nil {
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (r *RedisTaskRepository) ReleaseTasks(ctx context.Context, workerID string) error {
	taskIDs, err := r.client.SMembers(ctx, workerTasksKey(workerID)).Result()
	if err != nil {
		return err
	}

	if len(taskIDs) == 0 {
		return nil
	}

	for _, id := range taskIDs {
		task, err := r.Get(ctx, id)
		if err != nil {
			continue
		}

		if task.WorkerID == workerID && (task.State == domain.Running || task.State == domain.Scheduled) {
			if err := task.UpdateState(domain.Pending); err != nil {
				return err
			}
			task.WorkerID = ""
			task.UpdatedAt = time.Now().UTC()

			if err := r.Update(ctx, task); err != nil {
				r.logger.Error("failed to update", err, port.String("taskID", task.ID))
			}
		}
	}

	r.client.Del(ctx, workerTasksKey(workerID))

	return nil
}

func (r *RedisTaskRepository) ListTasks(ctx context.Context, filter *domain.TaskFilter) ([]*domain.Task, error) {
	return nil, fmt.Errorf("listing tasks is not supported with Redis storage yet; use Postgres for advanced filtering")
}

// RecoverTasks is only safe before a single server starts dispatching.
func (r *RedisTaskRepository) RecoverTasks(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(ctx, cursor, taskKeyPrefix+"*", 100).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			raw, err := r.client.Get(ctx, key).Bytes()
			if err != nil {
				return err
			}
			task, err := unmarshalTask(raw)
			if err != nil {
				return err
			}
			if task.State != domain.Running && task.State != domain.Scheduled {
				continue
			}
			oldWorker := task.WorkerID
			if err := task.UpdateState(domain.Pending); err != nil {
				return err
			}
			task.WorkerID = ""
			if err := r.Update(ctx, task); err != nil {
				return err
			}
			if oldWorker != "" {
				if err := r.client.SRem(ctx, workerTasksKey(oldWorker), task.ID).Err(); err != nil {
					return err
				}
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func (r *RedisTaskRepository) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }
func (r *RedisTaskRepository) Close() error                   { return r.client.Close() }
