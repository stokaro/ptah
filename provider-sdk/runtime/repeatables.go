package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func (c *Client) requireRepeatables(scope *protocol.RepeatableScope) error {
	if !slices.Contains(c.description.GetFeatures(), provider.RepeatableRecordFeature) {
		return fmt.Errorf("%w: the repeatable migration record was not negotiated", provider.ErrProtocol)
	}
	if scope.GetTransactionId() != "" {
		return c.requireTransactions()
	}
	return nil
}

func (s *serverAdapter) requireRepeatables(scope *protocol.RepeatableScope) error {
	if nilService(s.server.Repeatables) || !slices.Contains(s.negotiated().GetFeatures(), provider.RepeatableRecordFeature) {
		return status.Error(codes.Unimplemented, "the repeatable migration record is unavailable")
	}
	if scope.GetTransactionId() != "" && nilService(s.server.Transactions) {
		return status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	return nil
}

// repeatableCallResult checks a write's receipt on the client. A failed call,
// or a receipt that does not answer the request, leaves the outcome unknown.
func repeatableCallResult(ctx context.Context, description *protocol.Description, check func(*protocol.RepeatableResult) error,
	result *protocol.RepeatableResult, err error) (*protocol.RepeatableResult, error) {
	if err != nil {
		return nil, errors.Join(&provider.RepeatableError{}, remoteError(ctx, description, "write the repeatable migration record", err))
	}
	if err := check(result); err != nil {
		return nil, errors.Join(err, &provider.RepeatableError{})
	}
	result = proto.CloneOf(result)
	return result, provider.CheckRepeatableResult(result)
}

func repeatableServerResult(check func(*protocol.RepeatableResult) error, result *protocol.RepeatableResult, err error) (*protocol.RepeatableResult, error) {
	if err != nil {
		return nil, err
	}
	if err := check(result); err != nil {
		return nil, status.Error(codes.Internal, "provider returned an invalid repeatable migration record outcome")
	}
	return proto.CloneOf(result), nil
}

// ReadRepeatables reads every row of the record of repeatable migrations, as
// one bounded snapshot without bodies. Invalid or incomplete replies expose
// no usable rows.
func (c *Client) ReadRepeatables(ctx context.Context, request *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateReadRepeatablesRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.ReadRepeatables(call, proto.CloneOf(request))
	if err != nil {
		return nil, remoteError(call, c.description, "read the repeatable migration record", err)
	}
	if err := provider.ValidateRepeatableSnapshotReply(request, result); err != nil {
		return nil, err
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) ReadRepeatables(ctx context.Context, request *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error) {
	return repeatableServerRead(ctx, s, request, request.GetScope(), provider.ValidateReadRepeatablesRequest,
		func(call context.Context, message *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error) {
			return s.server.Repeatables.ReadRepeatables(call, message)
		},
		provider.ValidateRepeatableSnapshotReply, "provider returned an invalid repeatable migration snapshot")
}

// repeatableServerRead serves a read of the record: it refuses a call the
// negotiation or the request does not allow before the provider's service
// runs, and a reply that does not answer the request after it.
func repeatableServerRead[Q, R proto.Message](ctx context.Context, s *serverAdapter, request Q, scope *protocol.RepeatableScope,
	validate func(Q) error, invoke func(context.Context, Q) (R, error), check func(Q, R) error, invalid string) (R, error) {
	var zero R
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireRepeatables(scope); err != nil {
		return zero, err
	}
	if err := validate(request); err != nil {
		return zero, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := invoke(s.owner(ctx), proto.CloneOf(request))
	if err != nil {
		return zero, err
	}
	if err := check(request, result); err != nil {
		return zero, status.Error(codes.Internal, invalid)
	}
	return proto.CloneOf(result), nil
}

// PrepareRepeatables creates a missing record or verifies an existing one,
// once. A lost acknowledgment leaves the outcome unknown; the client never
// retries a database operation.
func (c *Client) PrepareRepeatables(ctx context.Context, request *protocol.PrepareRepeatablesRequest) (*protocol.RepeatableResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidatePrepareRepeatablesRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.PrepareRepeatables(call, proto.CloneOf(request))
	return repeatableCallResult(call, c.description, prepareRepeatablesCheck(request), result, err)
}

func (s *serverAdapter) PrepareRepeatables(ctx context.Context, request *protocol.PrepareRepeatablesRequest) (*protocol.RepeatableResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidatePrepareRepeatablesRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.Repeatables.PrepareRepeatables(s.owner(ctx), proto.CloneOf(request))
	return repeatableServerResult(prepareRepeatablesCheck(request), result, err)
}

func prepareRepeatablesCheck(request *protocol.PrepareRepeatablesRequest) func(*protocol.RepeatableResult) error {
	return func(result *protocol.RepeatableResult) error {
		return provider.ValidatePrepareRepeatablesResult(request, result)
	}
}

// ReadRepeatableBody reads the bytes the record keeps for one identity.
func (c *Client) ReadRepeatableBody(ctx context.Context, request *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateReadRepeatableBodyRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.ReadRepeatableBody(call, proto.CloneOf(request))
	if err != nil {
		return nil, remoteError(call, c.description, "read a repeatable migration body", err)
	}
	if err := provider.ValidateRepeatableBodyReply(request, result); err != nil {
		return nil, err
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) ReadRepeatableBody(ctx context.Context, request *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error) {
	return repeatableServerRead(ctx, s, request, request.GetScope(), provider.ValidateReadRepeatableBodyRequest,
		func(call context.Context, message *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error) {
			return s.server.Repeatables.ReadRepeatableBody(call, message)
		},
		provider.ValidateRepeatableBodyReply, "provider returned an invalid repeatable migration body")
}

// MutateRepeatable sends one atomic write to the record. APPLIED inside a
// transaction confirms execution only; FinishTransaction confirms durability.
// A write that did not apply returns a [*provider.RepeatableError] carrying
// its outcome.
func (c *Client) MutateRepeatable(ctx context.Context, request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateMutateRepeatableRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.MutateRepeatable(call, proto.CloneOf(request))
	return repeatableCallResult(call, c.description, mutateRepeatableCheck(request), result, err)
}

func (s *serverAdapter) MutateRepeatable(ctx context.Context, request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireRepeatables(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateMutateRepeatableRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.Repeatables.MutateRepeatable(s.owner(ctx), proto.CloneOf(request))
	return repeatableServerResult(mutateRepeatableCheck(request), result, err)
}

func mutateRepeatableCheck(request *protocol.MutateRepeatableRequest) func(*protocol.RepeatableResult) error {
	return func(result *protocol.RepeatableResult) error {
		return provider.ValidateRepeatableResult(request, result)
	}
}
