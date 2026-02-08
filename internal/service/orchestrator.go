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
	repo          port.TaskRepository
	logger        port.Logger
	workerManager *WorkerManager
}

func New(repo port.TaskRepository, logger port.Logger, wm *WorkerManager) *Orchestrator {
	return &Orchestrator{
		repo:          repo,
		logger:        logger,
		workerManager: wm,
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

func (s *Orchestrator) PollTask(ctx context.Context, req *pb.PollTaskRequest) (*pb.PollTaskResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "Worker ID cannot be empty")
	}
	task, err := s.repo.AcquireTask(ctx, req.WorkerId, req.TaskTypes)
	if err != nil {
		s.logger.Error("unable to acquire a task", err)
		return nil, err
	}

	if task == nil {
		return &pb.PollTaskResponse{}, nil
	}

	s.logger.Info("Task successfuly acquired", port.String("TaskID", task.ID), port.String("workerID", req.WorkerId))
	return &pb.PollTaskResponse{
		TaskId:  task.ID,
		Type:    task.Type,
		Payload: task.Payload,
	}, nil
}

func (s *Orchestrator) StreamTasks(req *pb.StreamTasksRequest, stream pb.Orchestrator_StreamTasksServer) error {

	if err := s.workerManager.Add(req.WorkerId, stream); err != nil {
		return err
	}
	defer func() {
		if err := s.workerManager.Remove(req.WorkerId); err != nil {
			s.logger.Error("Failed to remove worker", err)
		}
	}()

	<-stream.Context().Done()

	return nil
}
