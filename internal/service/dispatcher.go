package service

import (
	"context"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
)

type Dispatcher struct {
	workerManager *WorkerManager
	logger        port.Logger
	taskQueue     <-chan *domain.Task
}

func NewDispatcher(wm *WorkerManager, taskQueue <-chan *domain.Task, logger port.Logger) *Dispatcher {
	return &Dispatcher{
		workerManager: wm,
		taskQueue:     taskQueue,
		logger:        logger,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return

		case task := <-d.taskQueue:
			d.dispatch(task)
		}

	}

}

func (d *Dispatcher) dispatch(task *domain.Task) {
	stream, err := d.workerManager.GetNextWorker()
	if err != nil {
		d.logger.Error("No worker available to dispatch task", err)
		return
	}
	event := &pb.TaskEvent{
		TaskId:  task.ID,
		JobType: task.Type,
		Payload: task.Payload,
	}
	if err := stream.Send(event); err != nil {
		d.logger.Error("Failed to dispatch task", err)

	} else {
		d.logger.Info("Task dispatched successfully")
	}
}
