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

	// Submit Unstable Task
	taskID := uuid.New().String()
	log.Printf("Submitting Unstable Task: %s", taskID)
	
	_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:     taskID,
		Type:       "unstable_job",
		MaxRetries: 3, // Retry up to 3 times
	})
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	log.Println("✅ Task Submitted! Check Server/Worker logs for Retries/Failure.")
}
