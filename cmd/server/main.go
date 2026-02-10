package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/cmd/server/config"
	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/adapter/postgres"
	"github.com/manenim/task-orchestrator/internal/adapter/redis"
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
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	grpcPort, err := strconv.Atoi(cfg.Port)
	if err != nil {
		return fmt.Errorf("invalid port: %v", err)
	}

	batchSize := 10
	taskQueueBufferSize := 100

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := zap.New()
	if err != nil {
		return err
	}
	defer logger.Sync()

	var taskRepo port.TaskRepository

	switch cfg.StorageDriver {
	case "postgres":
		logger.Info("Using Postgres Storage", port.String("url", cfg.DatabaseURL))
		pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("failed to connect to postgres: %w", err)
		}
		defer pool.Close()
		taskRepo = postgres.NewPostgresTaskRepository(pool)

	case "redis":
		logger.Info("Using Redis Storage", port.String("addr", cfg.RedisAddr))
		taskRepo = redis.New(cfg.RedisAddr, logger)

	default:
		logger.Info("Using In-Memory Storage")
		taskRepo = memory.New(logger)
	}

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
	address := fmt.Sprintf(":%d", grpcPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	go func() {
		logger.Info("gRPC server started", port.Int("port", grpcPort))
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
