package client

import (
	"google.golang.org/grpc"
)

// Option configures a Client.
type Option func(*options)

type options struct {
	clientID string
	dialOpts []grpc.DialOption
}

func defaultOptions() *options {
	return &options{}
}

// WithDialOptions appends gRPC dial options (e.g. TLS credentials).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// WithClientID sets a client identifier used for idempotency tracking.
func WithClientID(id string) Option {
	return func(o *options) { o.clientID = id }
}
