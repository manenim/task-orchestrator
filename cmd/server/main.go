package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/adapter/zap"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	"github.com/manenim/task-orchestrator/internal/service"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
)

func main() {

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run() error {
	grpc_port := 50051
	batchSize := 10
	taskQueueBufferSize := 100

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := zap.New()
	if err != nil {
		return err
	}
	defer logger.Sync()

	taskRepo := memory.New(logger)
	taskQueue := make(chan *domain.Task, taskQueueBufferSize)
	workerManger := service.NewWorkerManager(logger)
	dispatcher := service.NewDispatcher(workerManger, taskQueue, logger, taskRepo)
	taskService := service.New(taskRepo, logger, workerManger)
	stateMgr := service.NewStateManager(taskRepo, logger, batchSize, taskQueue)
	go stateMgr.Run(ctx)
	go dispatcher.Run(ctx)

	grpcServer := grpc.NewServer()

	pb.RegisterOrchestratorServer(grpcServer, taskService)

	srvErr := make(chan error, 1)
	address := fmt.Sprintf(":%d", grpc_port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	go func() {
		logger.Info("gRPC server started", port.Int("port", grpc_port))
		srvErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		logger.Info("Shutting down gracefully...")
		stop()

		grpcServer.GracefulStop()
		logger.Info("Server stopped")
	case err := <-srvErr:
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}
