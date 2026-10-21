package runtime

// White-box testing required: malformed peer replies and dispatch counts must
// be observed before serialization to prove exact cleanup and no retries.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type transactionPeer struct {
	protocol.ProviderClient
	begin           *protocol.Transaction
	beginErr        error
	finish          *protocol.TransactionResult
	finishErr       error
	begins          []*protocol.BeginTransactionRequest
	finishes        []*protocol.FinishTransactionRequest
	cleanupCanceled error
}

func (p *transactionPeer) BeginTransaction(_ context.Context, request *protocol.BeginTransactionRequest, _ ...grpc.CallOption) (*protocol.Transaction, error) {
	p.begins = append(p.begins, proto.CloneOf(request))
	return p.begin, p.beginErr
}
func (p *transactionPeer) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest, _ ...grpc.CallOption) (*protocol.TransactionResult, error) {
	p.finishes = append(p.finishes, proto.CloneOf(request))
	p.cleanupCanceled = ctx.Err()
	return p.finish, p.finishErr
}
func transactionClient(peer *transactionPeer) *Client {
	return &Client{rpc: peer, description: &protocol.Description{Features: []string{provider.ExecuteFeature, provider.TransactionsFeature}}}
}
func beginRequest() *protocol.BeginTransactionRequest {
	return &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session", TransactionId: "transaction"}
}
func finishRequest(action protocol.FinishTransactionRequest_Action) *protocol.FinishTransactionRequest {
	return &protocol.FinishTransactionRequest{SessionId: "session", TransactionId: "transaction", Action: action}
}
func TestClientBeginCleansOnlyRequestedHandleAfterUnusableReply(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *protocol.Transaction
		err    error
	}{
		{"wrong handle", &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "other", Id: "unrelated"}, nil},
		{"missing handle", nil, nil},
		{"lost reply", nil, status.Error(codes.Unavailable, "lost")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &transactionPeer{begin: test.result, beginErr: test.err, finish: &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK}}
			result, err := transactionClient(peer).BeginTransaction(c.Context(), beginRequest())
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
			c.Assert(peer.begins, qt.HasLen, 1)
			c.Assert(peer.finishes, qt.HasLen, 1)
			c.Assert(proto.Equal(peer.finishes[0], finishRequest(protocol.FinishTransactionRequest_ROLLBACK)), qt.IsTrue)
			c.Assert(peer.cleanupCanceled, qt.IsNil)
		})
	}
}
func TestClientBeginRetainsUncertainCleanupFailure(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{beginErr: status.Error(codes.Canceled, "begin lost"), finishErr: status.Error(codes.Unavailable, "rollback lost")}
	result, err := transactionClient(peer).BeginTransaction(c.Context(), beginRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(peer.begins, qt.HasLen, 1)
	c.Assert(peer.finishes, qt.HasLen, 1)
}
func TestClientTransactionRepliesAreIndependentCopies(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{begin: &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session", Id: "transaction"}, finish: &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}}
	client := transactionClient(peer)
	handle, err := client.BeginTransaction(c.Context(), beginRequest())
	c.Assert(err, qt.IsNil)
	peer.begin.Id = "changed"
	c.Assert(handle.GetId(), qt.Equals, "transaction")
	result, err := client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.IsNil)
	peer.finish.Outcome = protocol.TransactionResult_UNKNOWN
	c.Assert(result.GetOutcome(), qt.Equals, protocol.TransactionResult_COMMITTED)
}
func TestClientFinishDoesNotRetryUncertainOutcome(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *protocol.TransactionResult
		err    error
	}{
		{"lost reply", nil, status.Error(codes.Unavailable, "lost")},
		{"missing reply", nil, nil},
		{"malformed reply", &protocol.TransactionResult{Outcome: 99}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &transactionPeer{finish: test.result, finishErr: test.err}
			result, err := transactionClient(peer).FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
			c.Assert(result, qt.IsNil)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(peer.finishes, qt.HasLen, 1)
		})
	}
}
func TestClientFinishKeepsConfirmedOppositeState(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{finish: &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}}
	result, err := transactionClient(peer).FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_ROLLBACK))
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.TransactionResult_COMMITTED)
	c.Assert(peer.finishes, qt.HasLen, 1)
}
func TestClientTransactionsRefuseCanceledMalformedAndUnnegotiatedRequests(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{}
	client := transactionClient(peer)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := client.BeginTransaction(ctx, beginRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	_, err = client.FinishTransaction(ctx, finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	_, err = client.BeginTransaction(c.Context(), nil)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	_, err = client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_ACTION_UNSPECIFIED))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	client.description.Features = nil
	_, err = client.BeginTransaction(c.Context(), beginRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	_, err = client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	_, err = client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: "session", RequestId: "request", TransactionId: "transaction", Units: []*protocol.Unit{{ChangeId: "statement", Body: &protocol.Unit_Sql{Sql: "CREATE TABLE records (id INTEGER);"}}}})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.begins, qt.HasLen, 0)
	c.Assert(peer.finishes, qt.HasLen, 0)
}

type transactionOwner struct{ peer *transactionPeer }

func (o *transactionOwner) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	return o.peer.BeginTransaction(ctx, request)
}
func (o *transactionOwner) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	return o.peer.FinishTransaction(ctx, request)
}
func TestServerTransactionValidationAndCleanup(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{begin: &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "wrong", Id: "wrong"}, finish: &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK}}
	server := &serverAdapter{server: Server{Transactions: &transactionOwner{peer: peer}}}
	_, err := server.BeginTransaction(c.Context(), nil)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	_, err = server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_ACTION_UNSPECIFIED))
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(peer.begins, qt.HasLen, 0)
	c.Assert(peer.finishes, qt.HasLen, 0)
	result, err := server.BeginTransaction(c.Context(), beginRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.begins, qt.HasLen, 1)
	c.Assert(peer.finishes, qt.HasLen, 1)
	c.Assert(proto.Equal(peer.finishes[0], finishRequest(protocol.FinishTransactionRequest_ROLLBACK)), qt.IsTrue)
	peer.finish = &protocol.TransactionResult{}
	terminal, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.IsNil)
	c.Assert(terminal.GetOutcome(), qt.Equals, protocol.TransactionResult_UNKNOWN)
	peer.finish = &protocol.TransactionResult{Outcome: 99}
	terminal, err = server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(terminal, qt.IsNil)
}
func TestServerTransactionCancellationDoesNotDispatch(t *testing.T) {
	c := qt.New(t)
	peer := &transactionPeer{}
	server := &serverAdapter{server: Server{Transactions: &transactionOwner{peer: peer}}}
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := server.BeginTransaction(ctx, beginRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	_, err = server.FinishTransaction(ctx, finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(peer.begins, qt.HasLen, 0)
	c.Assert(peer.finishes, qt.HasLen, 0)
}

func TestTransactionIsolationValidationPrecedesBothDispatches(t *testing.T) {
	for _, isolation := range []protocol.TransactionIsolation{0, 99} {
		t.Run(isolation.String(), func(t *testing.T) {
			c := qt.New(t)
			peer := &transactionPeer{}
			request := beginRequest()
			request.Isolation = isolation
			_, err := transactionClient(peer).BeginTransaction(c.Context(), request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			server := &serverAdapter{server: Server{Transactions: &transactionOwner{peer: peer}}}
			_, err = server.BeginTransaction(c.Context(), request)
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(peer.begins, qt.HasLen, 0)
			c.Assert(peer.finishes, qt.HasLen, 0)
		})
	}
}

func TestTransactionIsolationDowngradeCleansRequestedHandleAtBothBoundaries(t *testing.T) {
	c := qt.New(t)
	request := beginRequest()
	request.Isolation = protocol.TransactionIsolation_SERIALIZABLE
	peer := &transactionPeer{
		begin:  &protocol.Transaction{SessionId: "session", Id: "transaction", Isolation: protocol.TransactionIsolation_DEFAULT},
		finish: &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK},
	}
	result, err := transactionClient(peer).BeginTransaction(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.begins, qt.HasLen, 1)
	c.Assert(peer.begins[0].GetIsolation(), qt.Equals, protocol.TransactionIsolation_SERIALIZABLE)
	c.Assert(peer.finishes, qt.HasLen, 1)
	c.Assert(proto.Equal(peer.finishes[0], finishRequest(protocol.FinishTransactionRequest_ROLLBACK)), qt.IsTrue)
	server := &serverAdapter{server: Server{Transactions: &transactionOwner{peer: peer}}}
	result, err = server.BeginTransaction(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.begins, qt.HasLen, 2)
	c.Assert(peer.begins[1].GetIsolation(), qt.Equals, protocol.TransactionIsolation_SERIALIZABLE)
	c.Assert(peer.finishes, qt.HasLen, 2)
	c.Assert(proto.Equal(peer.finishes[1], finishRequest(protocol.FinishTransactionRequest_ROLLBACK)), qt.IsTrue)
	peer.begin.Isolation = protocol.TransactionIsolation_SERIALIZABLE
	result, err = server.BeginTransaction(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetIsolation(), qt.Equals, protocol.TransactionIsolation_SERIALIZABLE)
	result, err = transactionClient(peer).BeginTransaction(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetIsolation(), qt.Equals, protocol.TransactionIsolation_SERIALIZABLE)
	c.Assert(peer.finishes, qt.HasLen, 2)
}
