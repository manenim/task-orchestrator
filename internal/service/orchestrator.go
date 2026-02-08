package service

import (
	"context"
	"math"
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
		return nil, s.statusFromError(err)
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
		return nil, status.Error(codes.InvalidArgument, "Task ID cannot be empty")
	}
	task, err := s.repo.Get(ctx, req.TaskId)
	if err != nil {
		return nil, s.statusFromError(err)
	}

	if task.WorkerID != "" {
		if err := s.workerManager.CancelTask(task.WorkerID, task.ID); err != nil {
			s.logger.Error("Failed to send cancel signal to worker", err)
		}
	}

	if err := task.UpdateState(domain.Cancelled); err != nil {
		if err == domain.ErrTaskFinalized {
			return &pb.CancelTaskResponse{Success: true}, nil
		}
		return nil, s.statusFromError(err)
	}

	if err := s.repo.Update(ctx, task); err != nil {
		return nil, s.statusFromError(err)
	}

	return &pb.CancelTaskResponse{Success: true}, nil
}

func (s *Orchestrator) CompleteTask(ctx context.Context, req *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	task, err := s.repo.Get(ctx, req.TaskId)
	if err != nil {
		return nil, s.statusFromError(err)
	}

	task.WorkerID = ""

	if req.ErrorMessage != "" {
		s.logger.Info("Task failed", port.String("error", req.ErrorMessage))
		if req.IsRetryable && task.RetryCount < task.MaxRetries {
			task.RetryCount++
			backoffDuration := time.Duration(math.Pow(2, float64(task.RetryCount))) * time.Second
			task.RunAt = time.Now().Add(backoffDuration)
			task.LastFailedAt = time.Now()

			if err := task.UpdateState(domain.Pending); err != nil {
				return nil, s.statusFromError(err)
			}
		} else {
			task.LastFailedAt = time.Now()
			if err := task.UpdateState(domain.Failed); err != nil {
				return nil, s.statusFromError(err)
			}
		}
	} else {
		task.Result = req.Result
		if err := task.UpdateState(domain.Completed); err != nil {
			return nil, s.statusFromError(err)
		}
	}

	if err := s.repo.Update(ctx, task); err != nil {
		return nil, s.statusFromError(err)
	}

	s.workerManager.DecrementActiveTasks(req.WorkerId)

	return &pb.CompleteTaskResponse{StopStream: false}, nil
}

func (s *Orchestrator) statusFromError(err error) error {
	switch err {
	case domain.ErrTaskNotFound:
		return status.Error(codes.NotFound, err.Error())
	case domain.ErrInvalidTransition:
		return status.Error(codes.FailedPrecondition, err.Error())
	case domain.ErrTaskFinalized:
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
