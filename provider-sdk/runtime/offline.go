package runtime

import (
	"context"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Offline services share atomic batch validation. The caller retains the
// original input while the owner receives a clone, so owner mutation cannot
// rewrite the provenance used to validate its response. Both sides validate
// with the description negotiated with the host.
func serveOffline[Q proto.Message, R proto.Message](
	ctx context.Context, s *serverAdapter, request Q,
	validateInput func(*protocol.Description, Q) error, validateOutput func(*protocol.Description, Q, R) error,
	invoke func(context.Context, Q) (R, error),
) (R, error) {
	var zero R
	description := s.negotiated()
	if err := validateInput(description, request); err != nil {
		return zero, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	result, err := invoke(s.owner(ctx), proto.CloneOf(request))
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	if err := validateOutput(description, request, result); err != nil {
		return zero, status.Error(codes.Internal, err.Error())
	}
	return result, nil
}

func callOffline[Q proto.Message, R proto.Message](
	ctx context.Context, description *protocol.Description, request Q, operation string,
	validateInput func(*protocol.Description, Q) error, validateOutput func(*protocol.Description, Q, R) error,
	invoke func(context.Context, Q, ...grpc.CallOption) (R, error),
) (R, error) {
	return callWithin(ctx, CallTimeout, description, request, operation, validateInput, validateOutput, invoke)
}

// callWithin is callOffline with the bound a slow operation needs.
func callWithin[Q proto.Message, R proto.Message](
	ctx context.Context, timeout time.Duration, description *protocol.Description, request Q, operation string,
	validateInput func(*protocol.Description, Q) error, validateOutput func(*protocol.Description, Q, R) error,
	invoke func(context.Context, Q, ...grpc.CallOption) (R, error),
) (R, error) {
	var zero R
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := validateInput(description, request); err != nil {
		return zero, err
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := invoke(call, request)
	if err != nil {
		return zero, remoteError(call, operation, err)
	}
	if err := call.Err(); err != nil {
		return zero, err
	}
	if err := validateOutput(description, request, result); err != nil {
		return zero, err
	}
	return result, nil
}
