package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/manenim/task-orchestrator/internal/adapter/zap"
	"github.com/manenim/task-orchestrator/internal/port"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {


	 ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

	
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
		if err := stream(ctx, client, workerID, logger); err != nil {
			 if ctx.Err() != nil {
                     return 
                }
                logger.Error("streaming error:", err)
                time.Sleep(5 * time.Second)
		}
	}

}

func stream(ctx context.Context, client pb.OrchestratorClient, workerID string, logger port.Logger) error {
	runningTasks := make(map[string]context.CancelFunc)
	var mu sync.Mutex
	  var wg sync.WaitGroup
	      defer wg.Wait() 

	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()

	stream, err := client.StreamTasks(streamCtx, &pb.StreamTasksRequest{WorkerId: workerID})
	if err != nil {
		return err
	}

	logger.Info("Connected to Orchestrator, waiting for tasks...")

	go func() {
		select {
		case <-ctx.Done(): 
			logger.Info("Shutdown signal received. Waiting for active tasks...")
			wg.Wait() 
			cancelStream() 
			logger.Info("All tasks finished. Closing stream.")
		case <-streamCtx.Done(): 
			
		}
	}()

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
		wg.Add(1)
        go func(taskID, taskType string, taskCtx context.Context) {
            defer wg.Done()
			defer func() {
				mu.Lock()
				delete(runningTasks, taskID)
				mu.Unlock()
			}()

			logger.Info("Starting task...", port.String("task_id", taskID), port.String("type", taskType))

			select {
			case <-taskCtx.Done():
				logger.Info("Task Aborted by Cancellation!", port.String("task_id", taskID))
				return
			case <-time.After(2 * time.Second):
			}

			result, err, isRetryable := processTask(taskType)

			req := &pb.CompleteTaskRequest{
				TaskId:   taskID,
				WorkerId: workerID,
			}

			if err != nil {
				logger.Error("Task Failed", err, port.String("task_id", taskID), port.Bool("retryable", isRetryable))
				req.ErrorMessage = err.Error()
				req.IsRetryable = isRetryable
			} else {
				logger.Info("Task Completed Successfully", port.String("task_id", taskID))
				req.Result = []byte(result)
			}

			_, rpcErr := client.CompleteTask(context.Background(), req)
			if rpcErr != nil {
				logger.Error("Failed to report completion", rpcErr)
			}

		}(event.TaskId, event.JobType, ctx)
	}
}

func processTask(taskType string) (result string, err error, isRetryable bool) {
	if taskType == "slow_job" {
		time.Sleep(10 * time.Second)
		return "I took my time", nil, false
	}
	if taskType == "unstable_job" {
		r := rand.Intn(100)
		if r > 30 && r <= 70 {
			return "", fmt.Errorf("simulated transient error (network glitch)"), true
		} else if r > 70 {
			return "", fmt.Errorf("simulated fatal error (invalid input)"), false
		}
	}
	return "success result", nil, false
}
