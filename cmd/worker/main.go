package main

import (
	"context"
	"log"
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
		if err := poll(client, workerID, logger); err != nil {
			logger.Error("Poll error:", err)
			time.Sleep(5 * time.Second)
			continue
		}
	}

}

func poll(client pb.OrchestratorClient, workerID string, logger port.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.PollTask(ctx, &pb.PollTaskRequest{
		WorkerId:  workerID,
		TaskTypes: []string{"any"},
	})

	if err != nil {
		return err
	}
	if resp.TaskId == "" {
		logger.Info("No tasks found. Sleeping...")
		time.Sleep(1 * time.Second)
		return nil
	}

	logger.Info("Received Task: ", port.String("TaskID", resp.TaskId), port.String("Type", resp.Type))

	logger.Info("Executing task ...", port.String("Type", resp.Type))
	time.Sleep(2 * time.Second)
	logger.Info("Task Completed!", port.String("TaskID", resp.TaskId))
	return nil

}
