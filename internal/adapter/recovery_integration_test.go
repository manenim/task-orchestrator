package adapter_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/internal/adapter/postgres"
	redisrepo "github.com/manenim/task-orchestrator/internal/adapter/redis"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
)

type logger struct{}

func (logger) Info(string, ...port.Field)         {}
func (logger) Error(string, error, ...port.Field) {}
func (logger) Sync() error                        { return nil }

// These tests require dedicated test services; never point the variables at production.
func TestPostgresPersistenceAndRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := os.ReadFile("postgres/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(schema)); err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewPostgresTaskRepository(pool)
	task := domain.NewTask(uuid.NewString(), "test", "recovery", []byte("payload"), time.Time{}, 5)
	defer func() { _, _ = pool.Exec(context.Background(), "DELETE FROM tasks WHERE id=$1", task.ID) }()
	if err = repo.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	task.State = domain.Running
	task.WorkerID = "crashed-worker"
	task.ErrorMessage = "previous failure"
	task.Version = 4
	task.RetryCount = 2
	task.Result = []byte("partial")
	if err = repo.Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	// A fresh connection simulates loss of process-local state.
	second, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	reopened := postgres.NewPostgresTaskRepository(second)
	got, err := reopened.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ErrorMessage != "previous failure" || got.Version != 4 || got.RetryCount != 2 || string(got.Result) != "partial" {
		t.Fatalf("incomplete persisted snapshot: %+v", got)
	}
	listed, err := reopened.ListTasks(ctx, &domain.TaskFilter{TaskIDPrefix: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Version != 4 || listed[0].ErrorMessage != "previous failure" {
		t.Fatalf("incomplete listing: %+v", listed)
	}
	if err = reopened.RecoverTasks(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = reopened.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.Pending || got.WorkerID != "" {
		t.Fatalf("startup recovery failed: %+v", got)
	}
	_, err = reopened.Get(ctx, uuid.NewString())
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("missing task: %v", err)
	}
}

func TestRedisReleasePreservesTerminalTasks(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repo := redisrepo.New(addr, logger{})
	workerID := uuid.NewString()
	for _, state := range []domain.TaskState{domain.Running, domain.Scheduled, domain.Completed, domain.Failed, domain.Cancelled} {
		task := domain.NewTask(uuid.NewString(), "", "test", nil, time.Time{}, 0)
		task.State = state
		task.WorkerID = workerID
		if err := repo.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
		if err := repo.ReleaseTasks(ctx, workerID); err != nil {
			t.Fatal(err)
		}
		got, err := repo.Get(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := state
		if state == domain.Running || state == domain.Scheduled {
			want = domain.Pending
		}
		if got.State != want {
			t.Errorf("release changed %s to %s; want %s", state, got.State, want)
		}
	}
}
