package runtime

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
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
	result, err := invoke(s.owner(ctx), provider.CloneSharingModels(request))
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

// callOffline makes an offline call, which the caller's context alone
// bounds: what an offline service works on is a schema or a migration, and
// its work grows with it.
func callOffline[Q proto.Message, R proto.Message](
	ctx context.Context, description *protocol.Description, request Q, operation string,
	validateInput func(*protocol.Description, Q) error, validateOutput func(*protocol.Description, Q, R) error,
	invoke func(context.Context, Q, ...grpc.CallOption) (R, error),
) (R, error) {
	return callWithin(ctx, 0, description, request, operation, validateInput, validateOutput, invoke)
}

// callWithin is callOffline with the bound a call needs, when the caller
// supplied no earlier deadline. A zero timeout sets none.
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
	call, cancel := withinBound(ctx, timeout)
	defer cancel()
	result, err := invoke(call, request)
	if err != nil {
		return zero, remoteError(call, description, operation, err)
	}
	if err := call.Err(); err != nil {
		return zero, err
	}
	if err := validateOutput(description, request, result); err != nil {
		return zero, err
	}
	return result, nil
}

// withinBound is ctx with timeout as its deadline when the caller supplied no
// earlier one, or ctx alone for a zero timeout.
func withinBound(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
