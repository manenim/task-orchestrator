package service

import (
	"context"
	"math"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	orchestratorv1 "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Orchestrator struct {
	pb.UnimplementedOrchestratorServer
	repo          port.TaskRepository
	logger        port.Logger
	workerManager *WorkerManager
	publisher     TaskStatePublisher
}

func New(repo port.TaskRepository, logger port.Logger, wm *WorkerManager, publisher TaskStatePublisher) *Orchestrator {
	return &Orchestrator{
		repo:          repo,
		logger:        logger,
		workerManager: wm,
		publisher:     publisher,
	}
}

func (s *Orchestrator) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	if req.MaxRetries < 0 || req.MaxRetries > 30 {
		return nil, status.Error(codes.InvalidArgument, "max_retries must be between 0 and 30")
	}
	if req.TimeoutSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "timeout_seconds cannot be negative")
	}
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

	task := domain.NewTask(req.TaskId, req.ClientId, req.Type, req.Payload, runAt, req.TimeoutSeconds)
	task.MaxRetries = int(req.MaxRetries)

	if err := s.repo.Create(ctx, task); err != nil {
		return nil, s.statusFromError(err)
	}

	if s.publisher != nil {
		s.publisher.PublishTaskEvent(ctx, orchestratorv1.TaskEventType_TASK_EVENT_TYPE_CREATED, domain.TaskState(""), task, "task submitted")
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
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.repo.ReleaseTasks(releaseCtx, req.WorkerId); err != nil {
			s.logger.Error("Failed to release disconnected worker tasks", err)
		}
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

	previousState := task.State

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

	if s.publisher != nil {
		s.publisher.PublishTaskEvent(ctx, orchestratorv1.TaskEventType_TASK_EVENT_TYPE_CANCEL_REQUESTED, previousState, task, "task cancelled")
	}

	return &pb.CancelTaskResponse{Success: true}, nil
}

func (s *Orchestrator) CompleteTask(ctx context.Context, req *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	task, err := s.repo.Get(ctx, req.TaskId)
	if err != nil {
		return nil, s.statusFromError(err)
	}

	if req.WorkerId == "" || task.WorkerID != req.WorkerId {
		return nil, status.Error(codes.FailedPrecondition, "task is not assigned to this worker")
	}
	previousState := task.State
	eventType := orchestratorv1.TaskEventType_TASK_EVENT_TYPE_STATE_CHANGED
	eventReason := "task completed"

	task.WorkerID = ""

	if req.ErrorMessage != "" {
		task.ErrorMessage = req.ErrorMessage
		eventReason = req.ErrorMessage
		s.logger.Info("Task failed", port.String("error", req.ErrorMessage))
		if req.IsRetryable && task.RetryCount < task.MaxRetries {
			task.RetryCount++
			backoffDuration := time.Duration(math.Pow(2, float64(task.RetryCount))) * time.Second
			task.RunAt = time.Now().Add(backoffDuration)
			task.LastFailedAt = time.Now()

			if err := task.UpdateState(domain.Pending); err != nil {
				return nil, s.statusFromError(err)
			}
			eventType = orchestratorv1.TaskEventType_TASK_EVENT_TYPE_RETRIED
		} else {
			task.LastFailedAt = time.Now()
			if err := task.UpdateState(domain.Failed); err != nil {
				return nil, s.statusFromError(err)
			}
		}
	} else {
		task.ErrorMessage = ""
		task.Result = req.Result
		if err := task.UpdateState(domain.Completed); err != nil {
			return nil, s.statusFromError(err)
		}
	}

	if err := s.repo.Update(ctx, task); err != nil {
		return nil, s.statusFromError(err)
	}

	if s.publisher != nil {
		s.publisher.PublishTaskEvent(ctx, eventType, previousState, task, eventReason)
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
