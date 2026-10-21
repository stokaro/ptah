package runtime

// White-box testing required: dispatch counts, mutable peer replies, and
// malformed receipts cannot be controlled through an external subprocess.

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

type historyPeer struct {
	protocol.ProviderClient
	result                  *protocol.HistoryResult
	snapshot                *protocol.HistorySnapshot
	err                     error
	reads, prepares, writes int
	mutateRequest           bool
}

func (p *historyPeer) ReadHistory(_ context.Context, request *protocol.ReadHistoryRequest, _ ...grpc.CallOption) (*protocol.HistorySnapshot, error) {
	p.reads++
	if p.mutateRequest {
		request.Scope.Table = "changed"
	}
	return p.snapshot, p.err
}
func (p *historyPeer) PrepareHistory(_ context.Context, request *protocol.PrepareHistoryRequest, _ ...grpc.CallOption) (*protocol.HistoryResult, error) {
	p.prepares++
	if p.mutateRequest {
		request.Scope.Table = "changed"
		request.RequestId = "changed"
	}
	return p.result, p.err
}
func (p *historyPeer) MutateHistory(_ context.Context, request *protocol.MutateHistoryRequest, _ ...grpc.CallOption) (*protocol.HistoryResult, error) {
	p.writes++
	if p.mutateRequest {
		request.Scope.Table = "changed"
		request.RequestId = "changed"
		request.Replacement.Description = "changed"
	}
	return p.result, p.err
}
func historyClient(peer *historyPeer) *Client {
	return &Client{rpc: peer, description: &protocol.Description{Features: []string{provider.HistoryFeature, provider.TransactionsFeature}}}
}
func testHistoryScope() *protocol.HistoryScope {
	return &protocol.HistoryScope{SessionId: "session", Table: "history", Format: protocol.HistoryScope_NATIVE}
}
func testHistoryMutation() *protocol.MutateHistoryRequest {
	return &protocol.MutateHistoryRequest{Scope: testHistoryScope(), RequestId: "write", Replacement: &protocol.HistoryRevision{Version: "1", State: protocol.HistoryRevision_PENDING, Direction: protocol.HistoryRevision_UP}}
}
func testHistoryResult() *protocol.HistoryResult {
	return &protocol.HistoryResult{Scope: testHistoryScope(), RequestId: "write", Outcome: protocol.HistoryResult_APPLIED}
}
func TestHistoryClientDoesNotRetryUnknownWrites(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *protocol.HistoryResult
		err    error
	}{
		{"lost reply", nil, status.Error(codes.Unavailable, "password=secret")},
		{"missing reply", nil, nil},
		{"invalid binding", &protocol.HistoryResult{Scope: testHistoryScope(), RequestId: "other", Outcome: protocol.HistoryResult_APPLIED}, nil},
		{"invalid outcome", &protocol.HistoryResult{Scope: testHistoryScope(), RequestId: "write", Outcome: 99}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &historyPeer{result: test.result, err: test.err}
			result, err := historyClient(peer).MutateHistory(c.Context(), testHistoryMutation())
			c.Assert(result, qt.IsNil)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(peer.writes, qt.Equals, 1)
			c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
			result, err = historyClient(peer).PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: testHistoryScope(), RequestId: "write"})
			c.Assert(result, qt.IsNil)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(peer.prepares, qt.Equals, 1)
		})
	}
}
func TestHistoryClientRefusesCanceledCallsBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{}
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	client := historyClient(peer)
	_, err := client.MutateHistory(ctx, testHistoryMutation())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	_, err = client.PrepareHistory(ctx, &protocol.PrepareHistoryRequest{Scope: testHistoryScope(), RequestId: "write"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	_, err = client.ReadHistory(ctx, &protocol.ReadHistoryRequest{Scope: testHistoryScope(), MaxRevisions: 10})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.writes+peer.prepares+peer.reads, qt.Equals, 0)
}
func TestHistoryClientRequiresServicesBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{}
	client := historyClient(peer)
	client.description.Features = nil
	_, err := client.MutateHistory(c.Context(), testHistoryMutation())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	client.description.Features = []string{provider.HistoryFeature}
	request := testHistoryMutation()
	request.Scope.TransactionId = "tx"
	_, err = client.MutateHistory(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.writes, qt.Equals, 0)
}
func TestHistoryClientClonesInputsAndOutputs(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{result: testHistoryResult(), mutateRequest: true, snapshot: &protocol.HistorySnapshot{Scope: testHistoryScope(), Exists: true, Complete: true}}
	request := testHistoryMutation()
	result, err := historyClient(peer).MutateHistory(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(request, testHistoryMutation()), qt.IsTrue)
	peer.result.Scope.Table = "changed"
	c.Assert(result.GetScope().GetTable(), qt.Equals, "history")
	read := &protocol.ReadHistoryRequest{Scope: testHistoryScope(), MaxRevisions: 10}
	snapshot, err := historyClient(peer).ReadHistory(c.Context(), read)
	c.Assert(err, qt.IsNil)
	c.Assert(read.GetScope().GetTable(), qt.Equals, "history")
	peer.snapshot.Scope.Table = "changed"
	c.Assert(snapshot.GetScope().GetTable(), qt.Equals, "history")
}
func TestHistoryClientDiscardsIncompleteSnapshot(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{snapshot: &protocol.HistorySnapshot{Scope: testHistoryScope(), Exists: true}}
	result, err := historyClient(peer).ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: testHistoryScope(), MaxRevisions: 10})
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrIncomplete)
}

type historyOwner struct{ peer *historyPeer }

func (o *historyOwner) ReadHistory(ctx context.Context, r *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	return o.peer.ReadHistory(ctx, r)
}
func (o *historyOwner) PrepareHistory(ctx context.Context, r *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	return o.peer.PrepareHistory(ctx, r)
}
func (o *historyOwner) MutateHistory(ctx context.Context, r *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	return o.peer.MutateHistory(ctx, r)
}
func TestHistoryServerValidatesBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{result: testHistoryResult()}
	server := &serverAdapter{server: Server{History: &historyOwner{peer: peer}}}
	request := testHistoryMutation()
	request.Scope.TransactionId = "unsupported"
	_, err := server.MutateHistory(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: testHistoryScope()})
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	_, err = server.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: testHistoryScope()})
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(peer.writes+peer.reads+peer.prepares, qt.Equals, 0)
}
func TestHistoryServerPreservesExpectedBindingAndCopiesResults(t *testing.T) {
	c := qt.New(t)
	peer := &historyPeer{result: testHistoryResult(), mutateRequest: true}
	server := &serverAdapter{server: Server{History: &historyOwner{peer: peer}}}
	request := testHistoryMutation()
	result, err := server.MutateHistory(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(request, testHistoryMutation()), qt.IsTrue)
	peer.result.Scope.Table = "changed"
	c.Assert(result.GetScope().GetTable(), qt.Equals, "history")
	_, err = server.MutateHistory(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
}
