package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/domain"
	cpb "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControlPlaneCancelAndPayloadFlags(t *testing.T) {
	ctx := context.Background()
	logger := noopLogger{}
	repo := memory.New(logger)
	task := domain.NewTask("control-cancel", "", "job", []byte("private payload"), time.Time{}, 0)
	task.Result = []byte("private result")
	if err := repo.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	service := NewControlPlane(repo, logger, NewWorkerManager(logger), nil, "", "")
	got, err := service.GetTask(ctx, &cpb.GetTaskRequest{TaskId: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Task.Payload) != 0 || len(got.Task.Result) != 0 {
		t.Fatal("GetTask ignored payload/result opt-in flags")
	}
	response, err := service.CancelTask(ctx, &cpb.CancelTaskRequest{TaskId: task.ID, Reason: "operator request"})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Accepted || response.Task.State != cpb.TaskState_TASK_STATE_CANCELLED {
		t.Fatalf("cancel response: %+v", response)
	}
	response, err = service.CancelTask(ctx, &cpb.CancelTaskRequest{TaskId: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if !response.AlreadyTerminal || response.Accepted {
		t.Fatalf("terminal cancellation: %+v", response)
	}
}

func TestControlPlaneLogsPagination(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := noopLogger{}
	service := NewControlPlane(memory.New(logger), logger, NewWorkerManager(logger), pool, "", "")
	if err := service.Init(ctx); err != nil {
		t.Fatal(err)
	}
	taskID := uuid.NewString()
	defer func() { _, _ = pool.Exec(context.Background(), "DELETE FROM task_logs WHERE task_id=$1", taskID) }()
	service.recordTaskLog(ctx, taskID, cpb.LogLevel_LOG_LEVEL_INFO, "scheduler", "first", map[string]string{"attempt": "1"})
	service.recordTaskLog(ctx, taskID, cpb.LogLevel_LOG_LEVEL_ERROR, "worker", "second", nil)
	page, err := service.ListTaskLogs(ctx, &cpb.ListTaskLogsRequest{TaskId: taskID, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Message != "second" || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("first page: %+v", page)
	}
	page, err = service.ListTaskLogs(ctx, &cpb.ListTaskLogsRequest{TaskId: taskID, PageSize: 1, Cursor: page.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Message != "first" || page.Entries[0].Fields["attempt"] != "1" || page.HasMore {
		t.Fatalf("second page: %+v", page)
	}
}

func TestControlPlaneUnsupportedCapabilities(t *testing.T) {
	logger := noopLogger{}
	service := NewControlPlane(memory.New(logger), logger, NewWorkerManager(logger), nil, "", "")
	_, err := service.RetryTask(context.Background(), &cpb.RetryTaskRequest{TaskId: "task"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("manual retry: %v", err)
	}
	_, err = service.ListTaskLogs(context.Background(), &cpb.ListTaskLogsRequest{TaskId: "task"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("non-persistent logs: %v", err)
	}
	_, err = service.CancelTask(context.Background(), &cpb.CancelTaskRequest{TaskId: "task", ExpectedVersion: 1})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("conditional cancel: %v", err)
	}
}
