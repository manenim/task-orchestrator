package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
		ErrorMessage:   task.ErrorMessage,
		Version:        int32(task.Version),
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

	if params.Payload == nil {
		params.Payload = []byte{}
	}
	return r.queries.CreateTask(ctx, params)
}

func (r *PostgresTaskRepository) Get(ctx context.Context, id string) (*domain.Task, error) {
	uuidBytes, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid task ID: %w", err)
	}

	row, err := r.queries.GetTask(ctx, pgtype.UUID{Bytes: uuidBytes, Valid: true})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	}
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
		ErrorMessage: task.ErrorMessage,
		Version:      int32(task.Version),
		MaxRetries:   int32(task.MaxRetries),
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

func (r *PostgresTaskRepository) ListTasks(ctx context.Context, filter *domain.TaskFilter) ([]*domain.Task, error) {
	if filter == nil {
		filter = &domain.TaskFilter{}
	}

	orderBy := "updated_at"
	switch filter.SortBy {
	case domain.SortByCreatedAt:
		orderBy = "created_at"
	case domain.SortByRunAt:
		orderBy = "run_at"
	case domain.SortByUpdatedAt:
		orderBy = "updated_at"
	}

	direction := "DESC"
	if filter.SortDir == domain.SortAsc {
		direction = "ASC"
	}

	args := make([]any, 0, 12)
	where := make([]string, 0, 12)

	if len(filter.States) > 0 {
		states := make([]string, len(filter.States))
		for i, s := range filter.States {
			states[i] = string(s)
		}
		args = append(args, states)
		where = append(where, fmt.Sprintf("state = ANY($%d)", len(args)))
	}

	if len(filter.TaskTypes) > 0 {
		args = append(args, filter.TaskTypes)
		where = append(where, fmt.Sprintf("task_type = ANY($%d)", len(args)))
	}

	if filter.WorkerID != "" {
		args = append(args, filter.WorkerID)
		where = append(where, fmt.Sprintf("worker_id = $%d", len(args)))
	}

	if filter.TaskIDPrefix != "" {
		args = append(args, filter.TaskIDPrefix+"%")
		where = append(where, fmt.Sprintf("id::text ILIKE $%d", len(args)))
	}

	if filter.TextQuery != "" {
		args = append(args, "%"+filter.TextQuery+"%")
		where = append(where, fmt.Sprintf("(id::text ILIKE $%d OR task_type ILIKE $%d OR client_id ILIKE $%d)", len(args), len(args), len(args)))
	}

	if tr := filter.CreatedAt; tr != nil {
		if !tr.Start.IsZero() {
			args = append(args, tr.Start)
			where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
		}
		if !tr.End.IsZero() {
			args = append(args, tr.End)
			where = append(where, fmt.Sprintf("created_at < $%d", len(args)))
		}
	}

	if tr := filter.UpdatedAt; tr != nil {
		if !tr.Start.IsZero() {
			args = append(args, tr.Start)
			where = append(where, fmt.Sprintf("updated_at >= $%d", len(args)))
		}
		if !tr.End.IsZero() {
			args = append(args, tr.End)
			where = append(where, fmt.Sprintf("updated_at < $%d", len(args)))
		}
	}

	if tr := filter.RunAt; tr != nil {
		if !tr.Start.IsZero() {
			args = append(args, tr.Start)
			where = append(where, fmt.Sprintf("run_at >= $%d", len(args)))
		}
		if !tr.End.IsZero() {
			args = append(args, tr.End)
			where = append(where, fmt.Sprintf("run_at < $%d", len(args)))
		}
	}

	query := `
SELECT
	id::text,
	client_id,
	task_type,
	payload,
	state,
	run_at,
	worker_id,
	result,
	retry_count,
	max_retries,
	timeout_seconds,
	last_failed_at,
	created_at,
	updated_at,
	error_message,
	version
FROM tasks
`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY %s %s, id ASC", orderBy, direction)

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)
	query += fmt.Sprintf(" LIMIT $%d", len(args))

	if filter.Offset > 0 {
		args = append(args, filter.Offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tasks query failed: %w", err)
	}
	defer rows.Close()

	var tasks []*domain.Task
	for rows.Next() {
		var (
			id, clientID, taskType, state, workerIDStr string
			errorMessage                               string
			version                                    int32
			payload, result                            []byte
			runAt, createdAt, updatedAt                time.Time
			retryCount, maxRetries                     int
			timeoutSeconds                             int32
			lastFailedAt                               time.Time

			workerID                   pgtype.Text
			lastFailedAtPG             pgtype.Timestamptz
			retryCount32, maxRetries32 int32
		)

		if err := rows.Scan(
			&id,
			&clientID,
			&taskType,
			&payload,
			&state,
			&runAt,
			&workerID,
			&result,
			&retryCount32,
			&maxRetries32,
			&timeoutSeconds,
			&lastFailedAtPG,
			&createdAt,
			&updatedAt,
			&errorMessage,
			&version,
		); err != nil {
			return nil, fmt.Errorf("scan task row failed: %w", err)
		}

		if workerID.Valid {
			workerIDStr = workerID.String
		}
		if lastFailedAtPG.Valid {
			lastFailedAt = lastFailedAtPG.Time
		}
		retryCount = int(retryCount32)
		maxRetries = int(maxRetries32)

		task := &domain.Task{
			ID:             id,
			ClientID:       clientID,
			Type:           taskType,
			Payload:        payload,
			State:          domain.TaskState(state),
			RunAt:          runAt,
			WorkerID:       workerIDStr,
			Result:         result,
			RetryCount:     retryCount,
			MaxRetries:     maxRetries,
			TimeoutSeconds: timeoutSeconds,
			LastFailedAt:   lastFailedAt,
			CreatedAt:      createdAt,
			UpdatedAt:      updatedAt,
			ErrorMessage:   errorMessage,
			Version:        int(version),
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
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
		ErrorMessage:   row.ErrorMessage,
		Version:        int(row.Version),
	}
}

// RecoverTasks requires exclusive ownership of the repository by this server.
func (r *PostgresTaskRepository) RecoverTasks(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `UPDATE tasks SET state='PENDING', worker_id=NULL, version=version+1, updated_at=NOW() WHERE state IN ('SCHEDULED','RUNNING')`)
	return err
}
