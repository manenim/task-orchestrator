package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type seedTask struct {
	ID             string
	ClientID       string
	TaskType       string
	Payload        []byte
	State          string
	RunAt          time.Time
	WorkerID       *string
	Result         []byte
	RetryCount     int32
	MaxRetries     int32
	TimeoutSeconds int32
	LastFailedAt   *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type seedEvent struct {
	OccurredAt    time.Time
	EventType     string
	TaskID        string
	PreviousState string
	CurrentState  string
	TaskVersion   int64
	WorkerID      string
	Reason        string
}

type seedLog struct {
	TaskID     string
	Timestamp  time.Time
	Level      string
	Component  string
	Message    string
	FieldsJSON string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "seed failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("seed completed")
}

func run() error {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	if err := applySchema(ctx, pool); err != nil {
		return err
	}

	now := time.Now().UTC()
	workerA := "worker-node-a1"
	workerB := "worker-node-b2"

	tasks := []seedTask{
		{
			ID:             "11111111-1111-1111-1111-111111111111",
			ClientID:       "tenant-alpha",
			TaskType:       "email_notification",
			Payload:        []byte(`{"template":"invoice_reminder","user_id":1234}`),
			State:          "PENDING",
			RunAt:          now.Add(-2 * time.Minute),
			RetryCount:     0,
			MaxRetries:     3,
			TimeoutSeconds: 45,
			CreatedAt:      now.Add(-20 * time.Minute),
			UpdatedAt:      now.Add(-2 * time.Minute),
		},
		{
			ID:             "22222222-2222-2222-2222-222222222222",
			ClientID:       "tenant-alpha",
			TaskType:       "db_backup_shard",
			Payload:        []byte(`{"shard":"eu-west-2","priority":"high"}`),
			State:          "SCHEDULED",
			RunAt:          now.Add(10 * time.Minute),
			RetryCount:     0,
			MaxRetries:     3,
			TimeoutSeconds: 120,
			CreatedAt:      now.Add(-15 * time.Minute),
			UpdatedAt:      now.Add(-1 * time.Minute),
		},
		{
			ID:             "33333333-3333-3333-3333-333333333333",
			ClientID:       "tenant-alpha",
			TaskType:       "payment_reconciliation",
			Payload:        []byte(`{"batch_id":"pmt-2026-02-11"}`),
			State:          "RUNNING",
			RunAt:          now.Add(-8 * time.Minute),
			WorkerID:       &workerA,
			RetryCount:     1,
			MaxRetries:     5,
			TimeoutSeconds: 180,
			CreatedAt:      now.Add(-25 * time.Minute),
			UpdatedAt:      now.Add(-30 * time.Second),
		},
		{
			ID:             "44444444-4444-4444-4444-444444444444",
			ClientID:       "tenant-alpha",
			TaskType:       "nightly_export",
			Payload:        []byte(`{"target":"s3://exports/nightly"}`),
			State:          "COMPLETED",
			RunAt:          now.Add(-40 * time.Minute),
			WorkerID:       &workerB,
			Result:         []byte(`{"objects":128,"size_mb":512}`),
			RetryCount:     0,
			MaxRetries:     3,
			TimeoutSeconds: 90,
			CreatedAt:      now.Add(-45 * time.Minute),
			UpdatedAt:      now.Add(-30 * time.Minute),
		},
		{
			ID:             "55555555-5555-5555-5555-555555555555",
			ClientID:       "tenant-alpha",
			TaskType:       "critical_payment_sync",
			Payload:        []byte(`{"region":"us-east-1","mode":"strict"}`),
			State:          "FAILED",
			RunAt:          now.Add(-50 * time.Minute),
			RetryCount:     3,
			MaxRetries:     3,
			TimeoutSeconds: 120,
			LastFailedAt:   ptrTime(now.Add(-5 * time.Minute)),
			CreatedAt:      now.Add(-55 * time.Minute),
			UpdatedAt:      now.Add(-5 * time.Minute),
		},
		{
			ID:             "66666666-6666-6666-6666-666666666666",
			ClientID:       "tenant-alpha",
			TaskType:       "report_generation",
			Payload:        []byte(`{"report":"daily-ledger"}`),
			State:          "CANCELLED",
			RunAt:          now.Add(-70 * time.Minute),
			RetryCount:     0,
			MaxRetries:     3,
			TimeoutSeconds: 60,
			CreatedAt:      now.Add(-80 * time.Minute),
			UpdatedAt:      now.Add(-60 * time.Minute),
		},
		{
			ID:             "77777777-7777-7777-7777-777777777777",
			ClientID:       "tenant-alpha",
			TaskType:       "image_resize_batch",
			Payload:        []byte(`{"images":4029}`),
			State:          "PENDING",
			RunAt:          now.Add(-4 * time.Minute),
			RetryCount:     2,
			MaxRetries:     5,
			TimeoutSeconds: 90,
			LastFailedAt:   ptrTime(now.Add(-6 * time.Minute)),
			CreatedAt:      now.Add(-30 * time.Minute),
			UpdatedAt:      now.Add(-4 * time.Minute),
		},
	}

	events := []seedEvent{
		{OccurredAt: now.Add(-44 * time.Minute), EventType: "CREATED", TaskID: tasks[3].ID, PreviousState: "UNSPECIFIED", CurrentState: "PENDING", TaskVersion: 1, Reason: "task submitted"},
		{OccurredAt: now.Add(-41 * time.Minute), EventType: "STATE_CHANGED", TaskID: tasks[3].ID, PreviousState: "PENDING", CurrentState: "SCHEDULED", TaskVersion: 2, Reason: "task scheduled"},
		{OccurredAt: now.Add(-39 * time.Minute), EventType: "ASSIGNMENT_CHANGED", TaskID: tasks[3].ID, PreviousState: "SCHEDULED", CurrentState: "RUNNING", TaskVersion: 3, WorkerID: workerB, Reason: "task assigned to worker"},
		{OccurredAt: now.Add(-30 * time.Minute), EventType: "STATE_CHANGED", TaskID: tasks[3].ID, PreviousState: "RUNNING", CurrentState: "COMPLETED", TaskVersion: 4, WorkerID: workerB, Reason: "task completed"},

		{OccurredAt: now.Add(-54 * time.Minute), EventType: "CREATED", TaskID: tasks[4].ID, PreviousState: "UNSPECIFIED", CurrentState: "PENDING", TaskVersion: 1, Reason: "task submitted"},
		{OccurredAt: now.Add(-53 * time.Minute), EventType: "STATE_CHANGED", TaskID: tasks[4].ID, PreviousState: "PENDING", CurrentState: "SCHEDULED", TaskVersion: 2, Reason: "task scheduled"},
		{OccurredAt: now.Add(-51 * time.Minute), EventType: "ASSIGNMENT_CHANGED", TaskID: tasks[4].ID, PreviousState: "SCHEDULED", CurrentState: "RUNNING", TaskVersion: 3, WorkerID: workerA, Reason: "task assigned to worker"},
		{OccurredAt: now.Add(-5 * time.Minute), EventType: "STATE_CHANGED", TaskID: tasks[4].ID, PreviousState: "RUNNING", CurrentState: "FAILED", TaskVersion: 4, WorkerID: workerA, Reason: "database timeout exceeded"},

		{OccurredAt: now.Add(-7 * time.Minute), EventType: "RETRIED", TaskID: tasks[6].ID, PreviousState: "FAILED", CurrentState: "PENDING", TaskVersion: 5, WorkerID: workerA, Reason: "retry triggered by policy"},
		{OccurredAt: now.Add(-2 * time.Minute), EventType: "CANCEL_REQUESTED", TaskID: tasks[5].ID, PreviousState: "RUNNING", CurrentState: "CANCELLED", TaskVersion: 3, Reason: "cancelled by operator"},

		{OccurredAt: now.Add(-90 * time.Second), EventType: "ASSIGNMENT_CHANGED", TaskID: "worker:worker-node-a1", PreviousState: "UNSPECIFIED", CurrentState: "UNSPECIFIED", WorkerID: "worker-node-a1", Reason: "worker joined"},
		{OccurredAt: now.Add(-80 * time.Second), EventType: "ASSIGNMENT_CHANGED", TaskID: "worker:worker-node-b2", PreviousState: "UNSPECIFIED", CurrentState: "UNSPECIFIED", WorkerID: "worker-node-b2", Reason: "worker joined"},
	}

	logs := []seedLog{
		{TaskID: tasks[2].ID, Timestamp: now.Add(-90 * time.Second), Level: "INFO", Component: "dispatcher", Message: "task claimed by worker", FieldsJSON: `{"worker":"worker-node-a1"}`},
		{TaskID: tasks[2].ID, Timestamp: now.Add(-75 * time.Second), Level: "INFO", Component: "worker", Message: "processing records", FieldsJSON: `{"batch_size":"1000"}`},
		{TaskID: tasks[4].ID, Timestamp: now.Add(-5 * time.Minute), Level: "ERROR", Component: "worker", Message: "database timeout exceeded", FieldsJSON: `{"db":"payments-primary"}`},
		{TaskID: tasks[6].ID, Timestamp: now.Add(-7 * time.Minute), Level: "WARN", Component: "retry", Message: "retry triggered", FieldsJSON: `{"retry_count":"2"}`},
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `TRUNCATE TABLE task_logs, task_events, tasks RESTART IDENTITY`); err != nil {
		return fmt.Errorf("truncate data: %w", err)
	}

	for _, task := range tasks {
		_, err := tx.Exec(ctx, `
			INSERT INTO tasks (
				id, client_id, task_type, payload, state, run_at,
				worker_id, result, retry_count, max_retries,
				timeout_seconds, last_failed_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2, $3, $4, $5, $6,
				$7, $8, $9, $10,
				$11, $12, $13, $14
			)
		`,
			task.ID,
			task.ClientID,
			task.TaskType,
			task.Payload,
			task.State,
			task.RunAt,
			task.WorkerID,
			task.Result,
			task.RetryCount,
			task.MaxRetries,
			task.TimeoutSeconds,
			task.LastFailedAt,
			task.CreatedAt,
			task.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert task %s: %w", task.ID, err)
		}
	}

	for _, event := range events {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_events (
				occurred_at, event_type, task_id, previous_state, current_state, task_version, worker_id, reason
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`,
			event.OccurredAt,
			event.EventType,
			event.TaskID,
			event.PreviousState,
			event.CurrentState,
			event.TaskVersion,
			event.WorkerID,
			event.Reason,
		)
		if err != nil {
			return fmt.Errorf("insert event for task %s: %w", event.TaskID, err)
		}
	}

	for _, log := range logs {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_logs (task_id, timestamp, level, component, message, fields)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		`,
			log.TaskID,
			log.Timestamp,
			log.Level,
			log.Component,
			log.Message,
			log.FieldsJSON,
		)
		if err != nil {
			return fmt.Errorf("insert log for task %s: %w", log.TaskID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit seed tx: %w", err)
	}

	return nil
}

func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	schemaBytes, err := os.ReadFile("internal/adapter/postgres/schema.sql")
	if err != nil {
		return fmt.Errorf("read schema file: %w", err)
	}

	if _, err := pool.Exec(ctx, string(schemaBytes)); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	return nil
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
