package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerProcess(t *testing.T) {
	if os.Getenv("ORCHESTRATOR_TEST_PROCESS") != "1" {
		return
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestServerHealthAndBoundedShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServerProcess$")
	cmd.Env = append(os.Environ(), "ORCHESTRATOR_TEST_PROCESS=1", "STORAGE_DRIVER=memory", "PORT="+strconv.Itoa(port), "SHUTDOWN_TIMEOUT=100ms")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	conn, err := grpc.NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := healthpb.NewHealthClient(conn)
	for {
		checkCtx, checkCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		response, err := client.Check(checkCtx, &healthpb.HealthCheckRequest{Service: "readiness"})
		checkCancel()
		if err == nil && response.Status == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("readiness never became SERVING: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// GracefulStop alone never finishes while this worker stream is open.
	stream, err := pb.NewOrchestratorClient(conn).StreamTasks(ctx, &pb.StreamTasksRequest{WorkerId: "shutdown-test"})
	if err != nil {
		t.Fatal(err)
	}
	_ = stream
	time.Sleep(30 * time.Millisecond)
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server hung with a live worker stream")
	}
}
