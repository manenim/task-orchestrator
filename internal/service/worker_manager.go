package service

import (
	"fmt"
	"math"
	"sync"

	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
)

type SafeStream struct {
	stream pb.Orchestrator_StreamTasksServer
	mu     sync.Mutex
}

func (s *SafeStream) Send(event *pb.TaskEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(event)
}

type WorkerState struct {
	Stream      *SafeStream
	ActiveTasks int
}

type WorkerManager struct {
	mu        sync.RWMutex
	workers   map[string]*WorkerState
	workerIDs []string
	logger    port.Logger
}

func NewWorkerManager(logger port.Logger) *WorkerManager {
	return &WorkerManager{
		workers:   make(map[string]*WorkerState),
		workerIDs: make([]string, 0),
		logger:    logger,
	}
}

func (w *WorkerManager) Add(id string, stream pb.Orchestrator_StreamTasksServer) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.workers[id]; exists {
		return fmt.Errorf("worker with ID %q is already connected", id)
	}

	w.workers[id] = &WorkerState{
		Stream: &SafeStream{
			stream: stream,
		},
		ActiveTasks: 0,
	}
	w.workerIDs = append(w.workerIDs, id)

	w.logger.Info("Worker connected", port.String("worker_id", id), port.Int("total_workers", len(w.workerIDs)))
	return nil
}

func (w *WorkerManager) Remove(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.workers[id]; !exists {
		return fmt.Errorf("worker with ID %q not found for removal", id)
	}

	delete(w.workers, id)

	for i, wID := range w.workerIDs {
		if wID == id {
			w.workerIDs = append(w.workerIDs[:i], w.workerIDs[i+1:]...)
			break
		}
	}

	w.logger.Info("Worker disconnected", port.String("worker_id", id), port.Int("total_workers", len(w.workerIDs)))
	return nil
}

func (w *WorkerManager) GetNextWorker() (*SafeStream, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.workerIDs) == 0 {
		return nil, "", fmt.Errorf("no active workers available to accept tasks")
	}

	var selectedWorkerID string
	minActiveTasks := math.MaxInt

	for _, id := range w.workerIDs {
		state, exists := w.workers[id]
		if !exists {
			continue 
		}
		if state.ActiveTasks < minActiveTasks {
			minActiveTasks = state.ActiveTasks
			selectedWorkerID = id
		}
	}

	if selectedWorkerID == "" {
		selectedWorkerID = w.workerIDs[0]
	}

	w.workers[selectedWorkerID].ActiveTasks++
	w.logger.Info("Worker Selected (Least Connections)",
		port.String("worker_id", selectedWorkerID),
		port.Int("active_tasks", w.workers[selectedWorkerID].ActiveTasks))

	return w.workers[selectedWorkerID].Stream, selectedWorkerID, nil
}

func (w *WorkerManager) DecrementActiveTasks(workerID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if state, exists := w.workers[workerID]; exists {
		if state.ActiveTasks > 0 {
			state.ActiveTasks--
			w.logger.Info("Worker Task Completed", port.String("worker_id", workerID), port.Int("active_tasks", state.ActiveTasks))
		}
	}
}

func (w *WorkerManager) CancelTask(workerID string, taskID string) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	workerState, exists := w.workers[workerID]

	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}
	event := &pb.TaskEvent{
		TaskId:         taskID,
		IsCancellation: true,
	}
	if err := workerState.Stream.Send(event); err != nil {
		return fmt.Errorf("failed to send cancel event: %w", err)
	}

	w.logger.Info("Sent cancellation signal", port.String("worker_id", workerID), port.String("task_id", taskID))
	return nil
}
