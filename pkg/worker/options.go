package worker

import (
	"time"

	"google.golang.org/grpc"
)

// Option configures a Worker.
type Option func(*options)

type options struct {
	workerID       string
	logger         Logger
	dialOpts       []grpc.DialOption
	reconnectDelay time.Duration
	defaultTimeout time.Duration
}

func defaultOptions() *options {
	return &options{
		reconnectDelay: 5 * time.Second,
		defaultTimeout: 30 * time.Minute,
		logger:         noopLogger{},
	}
}

// WithWorkerID sets a fixed worker ID instead of auto-generating one.
func WithWorkerID(id string) Option {
	return func(o *options) { o.workerID = id }
}

// WithLogger sets a structured logger.
func WithLogger(l Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithDialOptions appends gRPC dial options (e.g. TLS credentials).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// WithReconnectDelay sets the delay between reconnection attempts.
func WithReconnectDelay(d time.Duration) Option {
	return func(o *options) { o.reconnectDelay = d }
}

// WithDefaultTimeout sets the fallback task timeout when the server doesn't specify one.
func WithDefaultTimeout(d time.Duration) Option {
	return func(o *options) { o.defaultTimeout = d }
}
