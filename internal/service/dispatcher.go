package service

import (
	"context"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	orchestratorv1 "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
)

type Dispatcher struct {
	workerManager *WorkerManager
	logger        port.Logger
	taskQueue     <-chan *domain.Task
	repo          port.TaskRepository
	publisher     TaskStatePublisher
}

func NewDispatcher(wm *WorkerManager, taskQueue <-chan *domain.Task, logger port.Logger, repo port.TaskRepository, publisher TaskStatePublisher) *Dispatcher {
	return &Dispatcher{
		workerManager: wm,
		taskQueue:     taskQueue,
		logger:        logger,
		repo:          repo,
		publisher:     publisher,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-d.taskQueue:
			if !ok {
				return
			}
			d.dispatch(ctx, task)
		}

	}

}

func (d *Dispatcher) dispatch(ctx context.Context, queued *domain.Task) {
	// Queue entries are snapshots. Cancellation may have changed persisted state.
	task, err := d.repo.Get(ctx, queued.ID)
	if err != nil {
		d.logger.Error("Failed to refresh queued task", err)
		return
	}
	if task.State != domain.Scheduled {
		return
	}
	stream, workerID, err := d.workerManager.GetNextWorker()
	if err != nil {
		d.requeue(ctx, task, "no worker available")
		return
	}
	task.WorkerID = workerID
	previousState := task.State
	if err := task.UpdateState(domain.Running); err != nil {
		d.workerManager.DecrementActiveTasks(workerID)
		d.logger.Error("Failed to update task state to RUNNING", err)
		return
	}
	if err := d.repo.Update(ctx, task); err != nil {
		d.workerManager.DecrementActiveTasks(workerID)
		d.logger.Error("Failed to update task worker ID", err)
		return
	}
	if d.publisher != nil {
		d.publisher.PublishTaskEvent(ctx, orchestratorv1.TaskEventType_TASK_EVENT_TYPE_ASSIGNMENT_CHANGED, previousState, task, "task assigned to worker")
	}
	event := &pb.TaskEvent{TaskId: task.ID, JobType: task.Type, Payload: task.Payload, TimeoutSeconds: task.TimeoutSeconds}
	if err := stream.Send(event); err != nil {
		d.workerManager.DecrementActiveTasks(workerID)
		d.logger.Error("Failed to dispatch task", err)
		d.requeue(ctx, task, "worker send failed")
	} else {
		d.logger.Info("Task dispatched successfully")
	}
}

func (d *Dispatcher) requeue(ctx context.Context, task *domain.Task, reason string) {
	previousState := task.State
	task.WorkerID = ""
	if err := task.UpdateState(domain.Pending); err != nil {
		d.logger.Error("Failed to requeue task", err)
		return
	}
	if err := d.repo.Update(ctx, task); err != nil {
		d.logger.Error("Failed to persist requeued task", err)
		return
	}
	if d.publisher != nil {
		d.publisher.PublishTaskEvent(ctx, orchestratorv1.TaskEventType_TASK_EVENT_TYPE_STATE_CHANGED, previousState, task, reason)
	}
}
