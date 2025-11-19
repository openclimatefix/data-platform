// This module defines custom GRPC interceptors.
package interceptors

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

// loggingInterceptorBuilder builds gRPC interceptors for logging.
// It isn't strictly needed here, but I thought I'd add it in a) to match the style of the postgres
// interceptor, and b) in case we ever want to pass logging configuration to it.
//
// A zerolog logger can be accessed from the context in handlers using it's inbuilt context
// injection features, e.g.:
// l := zerolog.Ctx(ctx).
type loggingInterceptorBuilder struct{}

func NewLoggingInterceptor() *loggingInterceptorBuilder {
	return &loggingInterceptorBuilder{}
}

// UnaryServerInterceptor is the gRPC interceptor for logging unary calls.
func (li *loggingInterceptorBuilder) UnaryServerInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	callStart := time.Now().UTC()
	// Use zerolog's context integration to add a logger to the request context.
	// See https://github.com/rs/zerolog#contextcontext-integration
	l := log.With().
		Str("grpc.method", info.FullMethod).
		Str("grpc.requestid", uuid.New().String()).
		Logger()
	l.Debug().Msg("started call")
	ctx = l.WithContext(ctx)

	// Call the handler to proceed with the normal execution of the RPC.
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, err
	}

	// Log the duration of the call.
	duration := time.Since(callStart)
	l.Debug().Str("grpc.send.duration", duration.String()).Msg("finished call")

	return resp, nil
}

// StreamServerInterceptor is the gRPC interceptor for logging streaming calls.
func (li *loggingInterceptorBuilder) StreamServerInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	callStart := time.Now().UTC()
	// Use zerolog's context integration to add a logger to the request context.
	l := log.With().
		Str("grpc.method", info.FullMethod).
		Str("grpc.requestid", uuid.New().String()).
		Logger()
	l.Debug().Msg("started call")
	ctx := l.WithContext(ss.Context())

	// Wrap the ServerStream to inject our new context.
	err := handler(srv, &wrappedStream{ServerStream: ss, newCtx: ctx})
	if err != nil {
		return err
	}

	// Log the duration of the call.
	duration := time.Since(callStart)
	l.Debug().Str("grpc.send.duration", duration.String()).Msg("finished call")

	return nil
}

// wrappedStream wraps a grpc.ServerStream to override its Context method.
type wrappedStream struct {
	grpc.ServerStream
	newCtx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.newCtx
}

func (w *wrappedStream) RecvMsg(m any) error {
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m any) error {
	return w.ServerStream.SendMsg(m)
}
