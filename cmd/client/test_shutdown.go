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

	// Submit Slow Task
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

	log.Println("✅ Slow Task Submitted! Kill the worker now (Ctrl+C) and watch it wait!")
}
