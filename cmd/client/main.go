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

	// 1. Submit Task
	taskID := uuid.New().String()
	log.Printf("Submitting Task: %s", taskID)
	_, err = client.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId: taskID,
		Type:   "long_running_job",
	})
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	log.Println("Waiting for 2 seconds (let it start)...")
	time.Sleep(2 * time.Second)

	log.Printf("Cancelling Task: %s", taskID)
	resp, err := client.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TaskId: taskID,
	})
	if err != nil {
		log.Fatalf("Failed to cancel task: %v", err)
	}

	if resp.Success {
		log.Println("✅ Cancellation Request Sent Successfully!")
	} else {
		log.Println("❌ Cancellation Request Failed (Success=false)")
	}
}
