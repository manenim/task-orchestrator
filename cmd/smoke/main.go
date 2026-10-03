// Command smoke verifies the externally observable worker flow against a running
// server. Each run uses a new task ID and fails if completion is not observed.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	cpb "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := os.Getenv("SERVER_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	health, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: "readiness"})
	if err != nil {
		return err
	}
	if health.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server is not ready: %s", health.Status)
	}
	taskID := os.Getenv("SMOKE_TASK_ID")
	if taskID == "" {
		taskID = uuid.NewString()
	}
	if os.Getenv("SMOKE_VERIFY_ONLY") != "true" {
		_, err = pb.NewOrchestratorClient(conn).SubmitTask(ctx, &pb.SubmitTaskRequest{TaskId: taskID, Type: "smoke", Payload: []byte("smoke"), TimeoutSeconds: 5})
		if err != nil {
			return err
		}
	}
	client := cpb.NewControlPlaneServiceClient(conn)
	for {
		response, err := client.GetTask(ctx, &cpb.GetTaskRequest{TaskId: taskID, IncludeResult: true})
		if err != nil {
			return err
		}
		switch response.Task.State {
		case cpb.TaskState_TASK_STATE_COMPLETED:
			if string(response.Task.Result) != "success" {
				return fmt.Errorf("unexpected worker result %q", response.Task.Result)
			}
			fmt.Printf("PASS: readiness and task %s completed with expected result\n", taskID)
			return nil
		case cpb.TaskState_TASK_STATE_FAILED, cpb.TaskState_TASK_STATE_CANCELLED:
			return fmt.Errorf("task entered %s: %s", response.Task.State, response.Task.ErrorMessage)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("completion not observed: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
