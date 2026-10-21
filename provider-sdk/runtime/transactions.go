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

func (c *Client) requireTransactions() error {
	if !slices.Contains(c.description.GetFeatures(), provider.TransactionsFeature) {
		return fmt.Errorf("%w: transaction service was not negotiated", provider.ErrProtocol)
	}
	return nil
}

// BeginTransaction opens the caller's session-bound handle once. A failed or
// malformed reply triggers bounded rollback of that exact handle, which also
// fences a late begin. The returned handle is independent of transport storage.
// Cancellation before dispatch sends nothing. Cleanup errors remain visible.
func (c *Client) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireTransactions(); err != nil {
		return nil, err
	}
	if err := provider.ValidateTransactionRequest(request.GetSessionId(), request.GetTransactionId()); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.BeginTransaction(call, proto.CloneOf(request))
	if err != nil {
		err = remoteError(call, "begin transaction", err)
	} else {
		err = provider.ValidateTransaction(request, result)
	}
	if err != nil {
		_, cleanupErr := c.FinishTransaction(context.WithoutCancel(ctx), &protocol.FinishTransactionRequest{
			SessionId: request.GetSessionId(), TransactionId: request.GetTransactionId(), Action: protocol.FinishTransactionRequest_ROLLBACK,
		})
		return nil, errors.Join(err, cleanupErr)
	}
	return proto.CloneOf(result), nil
}

// FinishTransaction sends a commit or rollback once. A lost reply leaves the
// outcome unknown; this method never retries it. A confirmed result is returned
// with an error when it contradicts the requested action. Malformed replies
// expose no result. The returned result is an independent copy.
func (c *Client) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.requireTransactions(); err != nil {
		return nil, err
	}
	if err := validateFinishRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.FinishTransaction(call, proto.CloneOf(request))
	if err != nil {
		return nil, errors.Join(&provider.TransactionError{}, remoteError(call, "finish transaction", err))
	}
	if err := provider.ValidateTransactionResult(result); err != nil {
		return nil, errors.Join(err, &provider.TransactionError{})
	}
	result = proto.CloneOf(result)
	return result, provider.CheckTransactionResult(request.GetAction(), result)
}

func validateFinishRequest(request *protocol.FinishTransactionRequest) error {
	if err := provider.ValidateTransactionRequest(request.GetSessionId(), request.GetTransactionId()); err != nil {
		return err
	}
	return provider.ValidateTransactionAction(request.GetAction())
}

func (s *serverAdapter) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if nilService(s.server.Transactions) {
		return nil, status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	request = proto.CloneOf(request)
	if err := provider.ValidateTransactionRequest(request.GetSessionId(), request.GetTransactionId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.Transactions.BeginTransaction(ctx, proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateTransaction(request, result); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), CallTimeout)
		defer cancel()
		_, _ = s.server.Transactions.FinishTransaction(cleanup, &protocol.FinishTransactionRequest{
			SessionId: request.GetSessionId(), TransactionId: request.GetTransactionId(), Action: protocol.FinishTransactionRequest_ROLLBACK,
		})
		return nil, status.Error(codes.Internal, "provider returned an invalid transaction handle")
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if nilService(s.server.Transactions) {
		return nil, status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	request = proto.CloneOf(request)
	if err := validateFinishRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.Transactions.FinishTransaction(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateTransactionResult(result); err != nil {
		return nil, status.Error(codes.Internal, "provider returned an invalid transaction outcome")
	}
	return proto.CloneOf(result), nil
}
