package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manenim/task-orchestrator/pkg/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverAddr := os.Getenv("SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = "localhost:50051"
	}
	w, err := worker.New(serverAddr,
		worker.WithLogger(&stdLogger{}),
	)
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}

	w.Handle("slow_job", handleSlowJob)
	w.Handle("unstable_job", handleUnstableJob)
	w.Handle("*", handleDefault)

	log.Printf("Worker %s starting...", w.ID())
	if err := w.Run(ctx); err != nil {
		log.Fatalf("worker error: %v", err)
	}
}

func handleSlowJob(ctx context.Context, task worker.Task) ([]byte, error) {
	select {
	case <-time.After(10 * time.Second):
		return []byte("slow job done"), nil
	case <-ctx.Done():
		return nil, worker.Retryable(ctx.Err())
	}
}

func handleUnstableJob(ctx context.Context, task worker.Task) ([]byte, error) {
	select {
	case <-time.After(2 * time.Second):
		r := rand.Intn(100)
		if r > 30 && r <= 70 {
			return nil, worker.Retryable(fmt.Errorf("simulated transient error"))
		} else if r > 70 {
			return nil, fmt.Errorf("simulated fatal error (non-retryable)")
		}
		return []byte("unstable job succeeded"), nil
	case <-ctx.Done():
		return nil, worker.Retryable(ctx.Err())
	}
}

func handleDefault(ctx context.Context, task worker.Task) ([]byte, error) {
	select {
	case <-time.After(100 * time.Millisecond):
		return []byte("success"), nil
	case <-ctx.Done():
		return nil, worker.Retryable(ctx.Err())
	}
}

type stdLogger struct{}

func (l *stdLogger) Info(msg string, keysAndValues ...any) {
	log.Printf("[INFO] %s %v", msg, keysAndValues)
}

func (l *stdLogger) Error(msg string, err error, keysAndValues ...any) {
	log.Printf("[ERROR] %s: %v %v", msg, err, keysAndValues)
}
