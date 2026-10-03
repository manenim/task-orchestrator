// Package client provides a typed Go SDK for interacting with the Task Orchestrator.
//
// It wraps the raw gRPC stubs behind clean Go types, so consumers
// never need to import protobuf directly.
//
// Example:
//
//	c, _ := client.New("localhost:50051")
//	defer c.Close()
//
//	taskID, _ := c.SubmitTask(ctx, client.SubmitRequest{
//	    TaskID:  uuid.New().String(),
//	    Type:    "email",
//	    Payload: []byte(`{"to":"user@example.com"}`),
//	})
//	fmt.Println("Submitted:", taskID)
package client

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/manenim/task-orchestrator/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SubmitRequest describes a task to submit.
type SubmitRequest struct {
	TaskID         string    // Required. Client-generated for idempotency.
	Type           string    // Required. Task type (must match a registered handler).
	Payload        []byte    // Optional. Arbitrary data for the handler.
	ClientID       string    // Optional. Overrides the client-level default.
	RunAt          time.Time // Optional. Zero = schedule immediately.
	MaxRetries     int32     // Automatic retry budget (0..30). Zero disables retries.
	TimeoutSeconds int32     // Optional. Per-task timeout in seconds.
}

// Client is a typed SDK for the Task Orchestrator.
type Client struct {
	conn *grpc.ClientConn
	stub pb.OrchestratorClient
	opts *options
}

// New creates a new Client connected to the given orchestrator address.
func New(serverAddr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(o)
	}

	if o.clientID == "" {
		o.clientID = "client-" + uuid.New().String()
	}

	dialOpts := o.dialOpts
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conn, err := grpc.NewClient(serverAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to orchestrator at %s: %w", serverAddr, err)
	}

	return &Client{
		conn: conn,
		stub: pb.NewOrchestratorClient(conn),
		opts: o,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SubmitTask submits a task to the orchestrator and returns the assigned task ID.
func (c *Client) SubmitTask(ctx context.Context, req SubmitRequest) (string, error) {
	if req.TaskID == "" {
		return "", fmt.Errorf("TaskID is required (generate with uuid.New().String())")
	}
	if req.Type == "" {
		return "", fmt.Errorf("Type is required")
	}

	clientID := req.ClientID
	if clientID == "" {
		clientID = c.opts.clientID
	}

	pbReq := &pb.SubmitTaskRequest{
		TaskId:         req.TaskID,
		Type:           req.Type,
		Payload:        req.Payload,
		ClientId:       clientID,
		MaxRetries:     req.MaxRetries,
		TimeoutSeconds: req.TimeoutSeconds,
	}

	if !req.RunAt.IsZero() {
		pbReq.RunAt = timestamppb.New(req.RunAt)
	}

	resp, err := c.stub.SubmitTask(ctx, pbReq)
	if err != nil {
		return "", fmt.Errorf("SubmitTask RPC failed: %w", err)
	}

	return resp.TaskId, nil
}

// CancelTask cancels a task by ID.
func (c *Client) CancelTask(ctx context.Context, taskID string) error {
	if taskID == "" {
		return fmt.Errorf("taskID is required")
	}

	_, err := c.stub.CancelTask(ctx, &pb.CancelTaskRequest{
		TaskId: taskID,
	})
	if err != nil {
		return fmt.Errorf("CancelTask RPC failed: %w", err)
	}

	return nil
}
