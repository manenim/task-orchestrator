package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/adapter/zap"
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
	port := 50051
	batchSize := 10

	logger, err := zap.New()
	if err != nil {
		return err
	}
	defer logger.Sync()

	taskRepo := memory.New()
	taskService := service.New(taskRepo, logger)

	stateMgr := service.NewStateManager(taskRepo, logger, batchSize)
	go stateMgr.Run(context.Background())

	grpcServer := grpc.NewServer()

	pb.RegisterOrchestratorServer(grpcServer, taskService)

	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer.Serve(listener)

	return nil
}
