package main

import (
	"context"
	"log"

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
	log.Printf("Submitting Slow Task: %s", taskID)

	if _, err := c.SubmitTask(context.Background(), client.SubmitRequest{
		TaskID:     taskID,
		Type:       "slow_job",
		MaxRetries: 3,
	}); err != nil {
		log.Fatalf("Failed to submit: %v", err)
	}

	log.Println("✅ Slow Task Submitted! Kill the worker now (Ctrl+C) and watch it wait!")
}
