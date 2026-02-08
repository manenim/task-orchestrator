package main

import (
	"context"
	"log"

	"github.com/google/uuid"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorClient(conn)

	// Submitting 2 Slow Tasks and 1 Fast Task
	// This will test if the fast task finishes and frees up the worker
	for i := 0; i < 2; i++ {
		taskID := uuid.New().String()
		log.Printf("Submitting Slow Task: %s", taskID)
		
		_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
			TaskId:     taskID,
			Type:       "slow_job",
			MaxRetries: 3, 
		})
		if err != nil {
			log.Fatalf("Failed to submit task: %v", err)
		}
	}

	taskID := uuid.New().String()
	log.Printf("Submitting Fast Task: %s", taskID)
	_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:     taskID,
		Type:       "job-1", // Fast job
		MaxRetries: 3, 
	})
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	log.Println("✅ Tasks Submitted! Monitor worker logs to see distribution.")
}
