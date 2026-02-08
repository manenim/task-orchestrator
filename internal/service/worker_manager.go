package service

import (
	"fmt"
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

type WorkerManager struct {
	mu        sync.RWMutex
	workers   map[string]*SafeStream
	workerIDs []string
	rrIndex   int
	logger    port.Logger
}

func NewWorkerManager(logger port.Logger) *WorkerManager {
	return &WorkerManager{
		workers:   make(map[string]*SafeStream),
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

	w.workers[id] = &SafeStream{
		stream: stream,
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
			if w.rrIndex >= i && w.rrIndex > 0 {
				w.rrIndex--
			}
			break
		}
	}

	w.logger.Info("Worker disconnected", port.String("worker_id", id), port.Int("total_workers", len(w.workerIDs)))
	return nil
}

func (w *WorkerManager) GetNextWorker() (*SafeStream, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.workerIDs) == 0 {
		return nil, fmt.Errorf("no active workers available to accept tasks")
	}

	w.rrIndex = (w.rrIndex + 1) % len(w.workerIDs)
	workerID := w.workerIDs[w.rrIndex]

	stream, exists := w.workers[workerID]
	if !exists {
		return nil, fmt.Errorf("internal inconsistency: worker ID %q found in list but missing from map", workerID)
	}

	return stream, nil
}
