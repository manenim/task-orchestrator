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

func (s *Orchestrator) StreamTasks(req *pb.StreamTasksRequest, stream pb.Orchestrator_StreamTasksServer) error {

	if err := s.workerManager.Add(req.WorkerId, stream); err != nil {
		return err
	}
	defer func() {
		if err := s.workerManager.Remove(req.WorkerId); err != nil {
			s.logger.Error("Failed to remove worker", err)
		}
		s.repo.ReleaseTasks(req.WorkerId)
	}()

	<-stream.Context().Done()

	return nil
}

func (s *Orchestrator) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "Task ID cannot be empty (client must generate ID for idempotency)")
	}
	task, err := s.repo.Get(ctx, req.TaskId)
	if err != nil {
		return &pb.CancelTaskResponse{}, err
	}

	if task.State == domain.Completed || task.State == domain.Cancelled {
		return &pb.CancelTaskResponse{
			Success: true,
		}, nil
	}
	if task.WorkerID == "" {
		if err := task.UpdateState(domain.Cancelled); err != nil {
			s.logger.Error("Failed to update task", err)
			return &pb.CancelTaskResponse{}, err
		}
		if err := s.repo.Update(ctx, task); err != nil {
			s.logger.Error("unable to update task state to cancelled.", err, port.String("taskID", req.TaskId))
			return &pb.CancelTaskResponse{}, err
		}
		return &pb.CancelTaskResponse{Success: true}, nil
	} else {
		if err := s.workerManager.CancelTask(task.WorkerID, task.ID); err != nil {
			s.logger.Error("Failed to send cancel signal to worker", err)
		}
		if err := task.UpdateState(domain.Cancelled); err != nil {
			s.logger.Error("Failed to update task", err)
			return &pb.CancelTaskResponse{}, err
		}
		if err := s.repo.Update(ctx, task); err != nil {
			s.logger.Error("unable to update task state to cancelled.", err, port.String("taskID", req.TaskId))
			return &pb.CancelTaskResponse{}, err
		}
		return &pb.CancelTaskResponse{Success: true}, nil
	}
}
