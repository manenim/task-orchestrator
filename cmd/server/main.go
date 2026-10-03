package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/cmd/server/config"
	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/adapter/postgres"
	"github.com/manenim/task-orchestrator/internal/adapter/redis"
	"github.com/manenim/task-orchestrator/internal/adapter/zap"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	"github.com/manenim/task-orchestrator/internal/service"
	cpb "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
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
	var pool *pgxpool.Pool
	var backendCheck = func(context.Context) error { return nil }

	switch cfg.StorageDriver {
	case "postgres":
		logger.Info("Using Postgres Storage")
		pool, err = pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("failed to connect to postgres: %w", err)
		}
		defer pool.Close()
		taskRepo = postgres.NewPostgresTaskRepository(pool)
		backendCheck = pool.Ping

	case "redis":
		logger.Info("Using Redis Storage", port.String("addr", cfg.RedisAddr))
		taskRepo = redis.New(cfg.RedisAddr, logger)
		redisBackend := taskRepo.(*redis.RedisTaskRepository)
		defer redisBackend.Close()
		backendCheck = redisBackend.Ping

	default:
		logger.Info("Using In-Memory Storage")
		taskRepo = memory.New(logger)
	}

	startupCtx, startupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer startupCancel()
	if err := backendCheck(startupCtx); err != nil {
		return fmt.Errorf("storage health: %w", err)
	}
	if err := taskRepo.RecoverTasks(startupCtx); err != nil {
		return fmt.Errorf("recover abandoned assignments: %w", err)
	}
	logger.Info("Recovered abandoned assignments; single server ownership required")
	taskQueue := make(chan *domain.Task, taskQueueBufferSize)
	workerManager := service.NewWorkerManager(logger)
	controlPlane := service.NewControlPlane(taskRepo, logger, workerManager, pool, cfg.TenantID, cfg.NamespaceID)
	if err := controlPlane.Init(startupCtx); err != nil {
		return fmt.Errorf("failed to initialize control-plane: %w", err)
	}
	workerManager.SetPublisher(controlPlane)

	dispatcher := service.NewDispatcher(workerManager, taskQueue, logger, taskRepo, controlPlane)
	taskService := service.New(taskRepo, logger, workerManager, controlPlane)
	stateMgr := service.NewStateManager(taskRepo, logger, batchSize, taskQueue, controlPlane)

	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); stateMgr.Run(ctx) }()
	go func() { defer loops.Done(); dispatcher.Run(ctx) }()

	grpcServer := grpc.NewServer()
	pb.RegisterOrchestratorServer(grpcServer, taskService)
	cpb.RegisterControlPlaneServiceServer(grpcServer, controlPlane)
	healthService := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthService)
	reflection.Register(grpcServer)
	healthService.SetServingStatus("liveness", healthpb.HealthCheckResponse_SERVING)
	healthService.SetServingStatus("readiness", healthpb.HealthCheckResponse_SERVING)
	go monitorReadiness(ctx, healthService, backendCheck)

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
		healthService.Shutdown()
		stop()
		drained := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(drained) }()
		select {
		case <-drained:
		case <-time.After(cfg.ShutdownTimeout):
			logger.Info("Shutdown deadline reached; closing remaining streams")
			grpcServer.Stop()
		}
		loops.Wait()
		logger.Info("Server stopped")
	case err := <-srvErr:
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Storage outages affect readiness but do not cause liveness restart storms.
func monitorReadiness(ctx context.Context, healthService *health.Server, check func(context.Context) error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, time.Second)
			err := check(checkCtx)
			cancel()
			serving := healthpb.HealthCheckResponse_SERVING
			if err != nil {
				serving = healthpb.HealthCheckResponse_NOT_SERVING
			}
			healthService.SetServingStatus("readiness", serving)
		}
	}
}
