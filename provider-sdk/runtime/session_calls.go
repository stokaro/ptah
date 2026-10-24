package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// sessionCall is one call of a session-bound service. The caller's request is
// copied, so a later change to it cannot alter the binding the reply is
// checked against, and the reply is copied before it is returned.
type sessionCall[Q proto.Message, R proto.Message] struct {
	feature   string
	operation string
	timeout   time.Duration
	// transaction is the transaction the request names, empty for none.
	transaction    string
	validateInput  func(Q) error
	validateOutput func(Q, R) error
	// uncertain returns the error joined to every failure that leaves a
	// write's effect unknown: a lost or rejected reply. It is nil for a read.
	uncertain func() error
}

// call sends the request once. Cancellation before dispatch sends nothing.
// A transport failure or an invalid reply exposes no result.
func (s sessionCall[Q, R]) call(ctx context.Context, c *Client, request Q, invoke func(context.Context, Q, ...grpc.CallOption) (R, error)) (R, error) {
	var zero R
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !slices.Contains(c.description.GetFeatures(), s.feature) {
		return zero, fmt.Errorf("%w: %s was not negotiated", provider.ErrProtocol, s.feature)
	}
	if s.transaction != "" {
		if err := c.requireTransactions(); err != nil {
			return zero, err
		}
	}
	if err := s.validateInput(request); err != nil {
		return zero, err
	}
	call, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	result, err := invoke(call, proto.CloneOf(request))
	if err != nil {
		return zero, s.join(remoteError(call, s.operation, err))
	}
	if err := s.validateOutput(request, result); err != nil {
		return zero, s.join(err)
	}
	return proto.CloneOf(result), nil
}

func (s sessionCall[Q, R]) join(err error) error {
	if s.uncertain == nil {
		return err
	}
	return errors.Join(s.uncertain(), err)
}

// serve is the server half: the same checks, with the owner given its own
// copy of the request. The caller has already refused a missing service. A
// call whose feature is two-way is refused unless the host listed it.
func (s sessionCall[Q, R]) serve(ctx context.Context, server *serverAdapter, request Q, invoke func(context.Context, Q) (R, error)) (R, error) {
	var zero R
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	if slices.Contains(provider.TwoWayFeatures(), s.feature) && !slices.Contains(server.negotiated().GetFeatures(), s.feature) {
		return zero, status.Error(codes.FailedPrecondition, s.feature+" was not negotiated")
	}
	if s.transaction != "" && nilService(server.server.Transactions) {
		return zero, status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	request = proto.CloneOf(request)
	if err := s.validateInput(request); err != nil {
		return zero, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := invoke(server.owner(ctx), proto.CloneOf(request))
	if err != nil {
		return zero, err
	}
	if err := s.validateOutput(request, result); err != nil {
		return zero, status.Error(codes.Internal, "provider returned an invalid "+s.operation+" reply")
	}
	return proto.CloneOf(result), nil
}

// unavailable is the answer of a server that does not implement a service.
func unavailable(name string) error {
	return status.Error(codes.Unimplemented, name+" is unavailable")
}
