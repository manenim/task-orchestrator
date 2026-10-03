package service

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/manenim/task-orchestrator/internal/adapter/memory"
	"github.com/manenim/task-orchestrator/internal/domain"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise real gRPC stream disconnects and assignment to a replacement worker.
func TestStreamDisconnectRequeuesAndReplacementCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger := noopLogger{}
	repo := memory.New(logger)
	wm := NewWorkerManager(logger)
	server := grpc.NewServer()
	pb.RegisterOrchestratorServer(server, New(repo, logger, wm, nil))
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///recovery", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorClient(conn)
	firstCtx, disconnect := context.WithCancel(ctx)
	defer disconnect()
	stream, err := client.StreamTasks(firstCtx, &pb.StreamTasksRequest{WorkerId: "first"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return wm.Count() == 1 })
	task := domain.NewTask("disconnect", "", "job", []byte("work"), time.Time{}, 0)
	task.State = domain.Scheduled
	if err := repo.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(wm, nil, logger, repo, nil)
	dispatcher.dispatch(ctx, task)
	event, err := stream.Recv()
	if err != nil || event.TaskId != task.ID {
		t.Fatalf("assignment: %v %v", event, err)
	}
	disconnect()
	waitFor(t, ctx, func() bool {
		got, err := repo.Get(ctx, task.ID)
		return err == nil && got.State == domain.Pending && got.WorkerID == "" && wm.Count() == 0
	})
	replacement, err := client.StreamTasks(ctx, &pb.StreamTasksRequest{WorkerId: "replacement"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return wm.Count() == 1 })
	task, err = repo.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.UpdateState(domain.Scheduled); err != nil {
		t.Fatal(err)
	}
	if err := repo.Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	dispatcher.dispatch(ctx, task)
	if _, err := replacement.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteTask(ctx, &pb.CompleteTaskRequest{TaskId: task.ID, WorkerId: "replacement", Result: []byte("recovered")}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.Completed || string(got.Result) != "recovered" {
		t.Fatalf("replacement completion: %+v", got)
	}
}

func waitFor(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal("condition not reached before timeout")
		case <-time.After(time.Millisecond):
		}
	}
}
