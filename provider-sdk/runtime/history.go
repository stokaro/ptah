package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (c *Client) requireHistory(scope *protocol.HistoryScope) error {
	if !slices.Contains(c.description.GetFeatures(), provider.HistoryFeature) {
		return fmt.Errorf("%w: history service was not negotiated", provider.ErrProtocol)
	}
	if scope.GetTransactionId() != "" {
		return c.requireTransactions()
	}
	return nil
}

func (s *serverAdapter) requireHistory(scope *protocol.HistoryScope) error {
	if nilService(s.server.History) {
		return status.Error(codes.Unimplemented, "history is unavailable")
	}
	if scope.GetTransactionId() != "" && nilService(s.server.Transactions) {
		return status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	return nil
}

func historyCallResult(ctx context.Context, scope *protocol.HistoryScope, requestID string, result *protocol.HistoryResult, err error) (*protocol.HistoryResult, error) {
	if err != nil {
		return nil, errors.Join(&provider.HistoryError{}, remoteError(ctx, "write history", err))
	}
	if err := provider.ValidateHistoryResult(scope, requestID, result); err != nil {
		return nil, errors.Join(err, &provider.HistoryError{})
	}
	result = proto.CloneOf(result)
	return result, provider.CheckHistoryResult(scope, requestID, result)
}

func historyServerResult(scope *protocol.HistoryScope, requestID string, result *protocol.HistoryResult, err error) (*protocol.HistoryResult, error) {
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateHistoryResult(scope, requestID, result); err != nil {
		return nil, status.Error(codes.Internal, "provider returned an invalid history outcome")
	}
	return proto.CloneOf(result), nil
}

// ReadHistory reads a complete bounded snapshot on the selected session and
// transaction. Invalid or incomplete replies expose no usable rows.
func (c *Client) ReadHistory(ctx context.Context, request *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateReadHistoryRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.ReadHistory(call, proto.CloneOf(request))
	if err != nil {
		return nil, remoteError(call, "read history", err)
	}
	if err := provider.ValidateHistorySnapshot(request, result); err != nil {
		return nil, err
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) ReadHistory(ctx context.Context, request *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateReadHistoryRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.History.ReadHistory(ctx, proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateHistorySnapshot(request, result); err != nil {
		return nil, status.Error(codes.Internal, "provider returned an invalid history snapshot")
	}
	return proto.CloneOf(result), nil
}

// PrepareHistory prepares storage once. A lost acknowledgment leaves the
// outcome unknown; the client never retries a database operation.
func (c *Client) PrepareHistory(ctx context.Context, request *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidatePrepareHistoryRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.PrepareHistory(call, proto.CloneOf(request))
	return historyCallResult(call, request.GetScope(), request.GetRequestId(), result, err)
}

func (s *serverAdapter) PrepareHistory(ctx context.Context, request *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidatePrepareHistoryRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.History.PrepareHistory(ctx, proto.CloneOf(request))
	return historyServerResult(request.GetScope(), request.GetRequestId(), result, err)
}

// MutateHistory sends one atomic comparison and write. APPLIED inside a
// transaction confirms execution only; FinishTransaction confirms durability.
func (c *Client) MutateHistory(ctx context.Context, request *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateMutateHistoryRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.MutateHistory(call, proto.CloneOf(request))
	return historyCallResult(call, request.GetScope(), request.GetRequestId(), result, err)
}

func (s *serverAdapter) MutateHistory(ctx context.Context, request *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request = proto.CloneOf(request)
	if err := s.requireHistory(request.GetScope()); err != nil {
		return nil, err
	}
	if err := provider.ValidateMutateHistoryRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.History.MutateHistory(ctx, proto.CloneOf(request))
	return historyServerResult(request.GetScope(), request.GetRequestId(), result, err)
}
