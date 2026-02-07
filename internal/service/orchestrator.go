package service

import (
	"context"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Orchestrator struct {
	pb.UnimplementedOrchestratorServer
	repo   port.TaskRepository
	logger port.Logger
}

func New(repo port.TaskRepository, logger port.Logger) *Orchestrator {
	return &Orchestrator{
		repo:   repo,
		logger: logger,
	}
}

func (s *Orchestrator) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "Task type cannot be empty")
	}
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "Task ID cannot be empty (client must generate ID for idempotency)")
	}

	var runAt time.Time
	if req.RunAt != nil {
		runAt = req.RunAt.AsTime()
	}

	task := domain.NewTask(req.TaskId, req.ClientId, req.Type, req.Payload, runAt)

	if err := s.repo.Create(ctx, task); err != nil {
		return nil, err
	}
	s.logger.Info("Task Submitted", port.String("id", task.ID))

	return &pb.SubmitTaskResponse{
		TaskId: task.ID,
	}, nil
}
