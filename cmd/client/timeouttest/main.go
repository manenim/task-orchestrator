package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/manenim/task-orchestrator/pkg/client"
)

func main() {
	c, err := client.New("localhost:50051")
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	taskID := uuid.New().String()
	log.Printf("Submitting Slow Task (10s) with 2s Timeout: %s", taskID)

	if _, err := c.SubmitTask(context.Background(), client.SubmitRequest{
		TaskID:         taskID,
		Type:           "slow_job",
		TimeoutSeconds: 2,
	}); err != nil {
		log.Fatalf("Failed to submit: %v", err)
	}

	time.Sleep(1 * time.Second)
	taskID2 := uuid.New().String()
	log.Printf("Submitting Fast Task: %s", taskID2)

	if _, err := c.SubmitTask(context.Background(), client.SubmitRequest{
		TaskID:         taskID2,
		Type:           "job-1",
		TimeoutSeconds: 10,
	}); err != nil {
		log.Fatalf("Failed to submit: %v", err)
	}

	log.Println("✅ Tasks Submitted! Monitor worker logs to see Timeout Failure.")
}
