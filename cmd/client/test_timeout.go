package main

import (
	"context"
	"log"
	"time"

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

	// 1. Submit a Slow Task with a SHORT Timeout (Should FAIL)
	taskID := uuid.New().String()
	log.Printf("Submitting Slow Task (10s) with 2s Timeout: %s", taskID)

	_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         taskID,
		Type:           "slow_job",
		TimeoutSeconds: 2, // Timeout < Duration
	})
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}
	
	// 2. Submit a Fast Task to ensure worker is still alive
	time.Sleep(1 * time.Second)
	taskID2 := uuid.New().String()
	log.Printf("Submitting Fast Task: %s", taskID2)
	_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         taskID2,
		Type:           "job-1",
		TimeoutSeconds: 10,
	})
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}


	log.Println("✅ Tasks Submitted! Monitor worker logs to see Timeout Failure.")
}
