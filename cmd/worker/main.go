package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/manenim/task-orchestrator/internal/adapter/zap"
	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	workerID := "worker-" + uuid.New().String()
	logger, err := zap.New()
	if err != nil {
		log.Fatalf("unable to initialize logger %v", err)
	}
	defer logger.Sync()

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewOrchestratorClient(conn)
	for {
		if err := stream(client, workerID, logger); err != nil {
			logger.Error("streaming error:", err)
			time.Sleep(5 * time.Second)
			continue
		}
	}

}

func stream(client pb.OrchestratorClient, workerID string, logger port.Logger) error {
	runningTasks := make(map[string]context.CancelFunc)
	var mu sync.Mutex
	stream, err := client.StreamTasks(context.Background(), &pb.StreamTasksRequest{WorkerId: workerID})
	if err != nil {
		return err
	}

	logger.Info("Connected to Orchestrator, waiting for tasks...")
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		if event.IsCancellation {
			mu.Lock()
			if cancel, exists := runningTasks[event.TaskId]; exists {
				cancel()
				delete(runningTasks, event.TaskId)
				logger.Info("Cancelled task", port.String("task_id", event.TaskId))
			}
			mu.Unlock()
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		mu.Lock()
		runningTasks[event.TaskId] = cancel
		mu.Unlock()
		go func(taskID string, taskCtx context.Context) {
			defer func() {
				mu.Lock()
				delete(runningTasks, taskID)
				mu.Unlock()
			}()
			logger.Info("Starting task...", port.String("task_id", taskID))

			select {
			case <-time.After(5 * time.Second):
				logger.Info("Task Completed!", port.String("task_id", taskID))
			case <-taskCtx.Done():
				logger.Info("Task Aborted by Cancellation!", port.String("task_id", taskID))
			}
		}(event.TaskId, ctx)
	}
}
