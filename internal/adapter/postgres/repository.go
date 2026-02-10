package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
)
	
type PostgresTaskRepository struct {
	pool    *pgxpool.Pool
	queries *Queries
}

func NewPostgresTaskRepository(pool *pgxpool.Pool) port.TaskRepository {
	return &PostgresTaskRepository{
		pool:    pool,
		queries: New(pool),
	}
}

func (r *PostgresTaskRepository) Create(ctx context.Context, task *domain.Task) error {
	id, err := uuid.Parse(task.ID)
	if err != nil {
		return fmt.Errorf("invalid task ID: %w", err)
	}

	params := CreateTaskParams{
		ID:             pgtype.UUID{Bytes: id, Valid: true},
		ClientID:       task.ClientID,
		TaskType:       task.Type,
		Payload:        task.Payload,
		State:          string(task.State),
		RunAt:          pgtype.Timestamptz{Time: task.RunAt, Valid: !task.RunAt.IsZero()},
		RetryCount:     int32(task.RetryCount),
		MaxRetries:     int32(task.MaxRetries),
		TimeoutSeconds: task.TimeoutSeconds,
		CreatedAt:      pgtype.Timestamptz{Time: task.CreatedAt, Valid: !task.CreatedAt.IsZero()},
		UpdatedAt:      pgtype.Timestamptz{Time: task.UpdatedAt, Valid: !task.UpdatedAt.IsZero()},
	}

	if task.WorkerID != "" {
		params.WorkerID = pgtype.Text{String: task.WorkerID, Valid: true}
	}

	if len(task.Result) > 0 {
		params.Result = task.Result
	}

	if !task.LastFailedAt.IsZero() {
		params.LastFailedAt = pgtype.Timestamptz{Time: task.LastFailedAt, Valid: true}
	}

	return r.queries.CreateTask(ctx, params)
}

func (r *PostgresTaskRepository) Get(ctx context.Context, id string) (*domain.Task, error) {
	uuidBytes, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid task ID: %w", err)
	}

	row, err := r.queries.GetTask(ctx, pgtype.UUID{Bytes: uuidBytes, Valid: true})
	if err != nil {
		return nil, err
	}

	return mapToDomain(row), nil
}

func (r *PostgresTaskRepository) Update(ctx context.Context, task *domain.Task) error {
	id, err := uuid.Parse(task.ID)
	if err != nil {
		return fmt.Errorf("invalid task ID: %w", err)
	}

	params := UpdateTaskParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		State:        string(task.State),
		RetryCount:   int32(task.RetryCount),
		RunAt:        pgtype.Timestamptz{Time: task.RunAt, Valid: !task.RunAt.IsZero()},
	}

	if task.WorkerID != "" {
		params.WorkerID = pgtype.Text{String: task.WorkerID, Valid: true}
	}

	if len(task.Result) > 0 {
		params.Result = task.Result
	}

	if !task.LastFailedAt.IsZero() {
		params.LastFailedAt = pgtype.Timestamptz{Time: task.LastFailedAt, Valid: true}
	}

	return r.queries.UpdateTask(ctx, params)
}

func (r *PostgresTaskRepository) ListEligible(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error) {
	rows, err := r.queries.ListEligibleTasks(ctx, ListEligibleTasksParams{
		RunAt: pgtype.Timestamptz{Time: now, Valid: true},
		Limit: int32(limit),
	})
	if err != nil {
		return nil, err
	}

	tasks := make([]*domain.Task, len(rows))
	for i, row := range rows {
		tasks[i] = mapToDomain(row)
	}
	return tasks, nil
}

func (r *PostgresTaskRepository) ReleaseTasks(ctx context.Context, workerID string) error {
	return r.queries.ReleaseTasks(ctx, pgtype.Text{String: workerID, Valid: true})
}

func mapToDomain(row Task) *domain.Task {
	id, _ := uuid.FromBytes(row.ID.Bytes[:])

	return &domain.Task{
		ID:             id.String(),
		ClientID:       row.ClientID,
		Type:           row.TaskType,
		Payload:        row.Payload,
		State:          domain.TaskState(row.State),
		RunAt:          row.RunAt.Time,
		WorkerID:       row.WorkerID.String,
		Result:         row.Result,
		RetryCount:     int(row.RetryCount),
		MaxRetries:     int(row.MaxRetries),
		TimeoutSeconds: row.TimeoutSeconds,
		LastFailedAt:   row.LastFailedAt.Time,
		CreatedAt:      row.CreatedAt.Time,
		UpdatedAt:      row.UpdatedAt.Time,
	}
}