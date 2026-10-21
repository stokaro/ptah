package runtime

// White-box testing required: malformed peer receipts, dispatch counts, and
// cancellation during acquisition cannot be controlled through a real provider.

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
	"google.golang.org/protobuf/testing/protocmp"
)

type lockService struct {
	result                  *protocol.LockResult
	err                     error
	releaseOutcome          protocol.LockResult_Outcome
	releaseErr              error
	onTry                   func()
	mutateRequest           bool
	tries, checks, releases int
	cleanup                 *protocol.LockRequest
	cleanupContextError     error
	cleanupBounded          bool
}

func (s *lockService) TryLock(_ context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	s.tries++
	if s.onTry != nil {
		s.onTry()
	}
	if s.mutateRequest {
		request.Scope.Name = "changed"
	}
	return s.result, s.err
}
func (s *lockService) CheckLock(context.Context, *protocol.LockRequest) (*protocol.LockResult, error) {
	s.checks++
	return s.result, s.err
}
func (s *lockService) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	s.releases++
	s.cleanup = proto.CloneOf(request)
	s.cleanupContextError = ctx.Err()
	_, s.cleanupBounded = ctx.Deadline()
	return &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: s.releaseOutcome}, s.releaseErr
}
func (*lockService) Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error) {
	return nil, nil
}
func (*lockService) Close(context.Context, *protocol.CloseRequest) error { return nil }

type lockPeer struct {
	protocol.ProviderClient
	owner *lockService
}

func (p *lockPeer) TryLock(ctx context.Context, request *protocol.LockRequest, _ ...grpc.CallOption) (*protocol.LockResult, error) {
	return p.owner.TryLock(ctx, request)
}
func (p *lockPeer) CheckLock(ctx context.Context, request *protocol.LockRequest, _ ...grpc.CallOption) (*protocol.LockResult, error) {
	return p.owner.CheckLock(ctx, request)
}
func (p *lockPeer) ReleaseLock(ctx context.Context, request *protocol.LockRequest, _ ...grpc.CallOption) (*protocol.LockResult, error) {
	return p.owner.ReleaseLock(ctx, request)
}
func lockClient(owner *lockService) *Client {
	return &Client{rpc: &lockPeer{owner: owner}, description: &protocol.Description{Features: []string{provider.LocksFeature}}}
}
func testLockRequest() *protocol.LockRequest {
	return &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: "session", LockId: "attempt", Name: "migration"}, RequestId: "request"}
}
func testLockReceipt(outcome protocol.LockResult_Outcome) *protocol.LockResult {
	request := testLockRequest()
	return &protocol.LockResult{Scope: request.Scope, RequestId: request.RequestId, Outcome: outcome}
}

func TestLockClientFencesUncertainAcquisitionWithoutRetry(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *protocol.LockResult
		err    error
	}{
		{"lost receipt", nil, status.Error(codes.Unavailable, "password=secret")},
		{"missing receipt", nil, nil},
		{"other session", &protocol.LockResult{Scope: &protocol.LockScope{SessionId: "other", LockId: "attempt", Name: "migration"}, RequestId: "request", Outcome: protocol.LockResult_ACQUIRED}, nil},
		{"other request", &protocol.LockResult{Scope: testLockRequest().Scope, RequestId: "stale", Outcome: protocol.LockResult_ACQUIRED}, nil},
		{"unknown outcome", testLockReceipt(protocol.LockResult_UNKNOWN), nil},
		{"unknown enum", testLockReceipt(99), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &lockService{result: test.result, err: test.err, releaseOutcome: protocol.LockResult_NOT_ACQUIRED}
			ctx, cancel := context.WithCancel(c.Context())
			defer cancel()
			owner.onTry = cancel
			request := testLockRequest()
			result, err := lockClient(owner).TryLock(ctx, request)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
			c.Assert(result, qt.IsNil)
			c.Assert(owner.tries, qt.Equals, 1)
			c.Assert(owner.releases, qt.Equals, 1)
			c.Assert(owner.cleanup.Scope, qt.CmpEquals(protocmp.Transform()), request.Scope)
			c.Assert(owner.cleanup.RequestId, qt.Not(qt.Equals), request.RequestId)
			c.Assert(owner.cleanupContextError, qt.IsNil)
			c.Assert(owner.cleanupBounded, qt.IsTrue)
		})
	}
}

func TestLockClientKeepsConfirmedContentionAndIndependentReceipts(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{result: testLockReceipt(protocol.LockResult_BUSY), mutateRequest: true}
	client := lockClient(owner)
	request := testLockRequest()
	result, err := client.TryLock(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
	c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrOutcomeUnknown)
	c.Assert(result.Outcome, qt.Equals, protocol.LockResult_BUSY)
	c.Assert(owner.releases, qt.Equals, 0)
	c.Assert(request.Scope.Name, qt.Equals, "migration")
	result.Scope.Name = "caller change"
	c.Assert(owner.result.Scope.Name, qt.Equals, "migration")
	request.Scope.LockId, request.RequestId = "new attempt", "new request"
	owner.result = &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: protocol.LockResult_ACQUIRED}
	result, err = client.TryLock(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Outcome, qt.Equals, protocol.LockResult_ACQUIRED)
	c.Assert(owner.tries, qt.Equals, 2)
	c.Assert(owner.releases, qt.Equals, 0)
}

func TestLockClientRefusesBeforeDispatch(t *testing.T) {
	for _, call := range []struct {
		name   string
		invoke func(*Client, context.Context, *protocol.LockRequest) (*protocol.LockResult, error)
	}{
		{"try", (*Client).TryLock},
		{"check", (*Client).CheckLock},
		{"release", (*Client).ReleaseLock},
	} {
		t.Run(call.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &lockService{}
			client := lockClient(owner)
			_, err := call.invoke(client, c.Context(), nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			ctx, cancel := context.WithCancel(c.Context())
			cancel()
			_, err = call.invoke(client, ctx, testLockRequest())
			c.Assert(err, qt.ErrorIs, context.Canceled)
			client.description.Features = nil
			_, err = call.invoke(client, c.Context(), testLockRequest())
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(owner.tries+owner.checks+owner.releases, qt.Equals, 0)
		})
	}
}

func TestLockClientNeverReusesEarlierOwnershipOrRetriesRelease(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{result: testLockReceipt(protocol.LockResult_ACQUIRED)}
	client := lockClient(owner)
	_, err := client.CheckLock(c.Context(), testLockRequest())
	c.Assert(err, qt.IsNil)
	owner.err = status.Error(codes.Unavailable, "private diagnostic")
	result, err := client.CheckLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(owner.checks, qt.Equals, 2)
	owner.releaseErr = owner.err
	result, err = client.ReleaseLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(owner.releases, qt.Equals, 1)
	owner.err = nil
	owner.result = testLockReceipt(protocol.LockResult_LOST)
	_, err = client.CheckLock(c.Context(), testLockRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrLockLost)
}

func TestLockServerValidatesBeforeDispatchAndCleansUncertainHandle(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{releaseOutcome: protocol.LockResult_NOT_ACQUIRED}
	adapter := &serverAdapter{server: Server{Locks: owner}}
	_, err := adapter.TryLock(c.Context(), nil)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(owner.tries, qt.Equals, 0)
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	owner.onTry = cancel
	result, err := adapter.TryLock(ctx, testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(owner.tries, qt.Equals, 1)
	c.Assert(owner.releases, qt.Equals, 1)
	c.Assert(owner.cleanup.Scope, qt.CmpEquals(protocmp.Transform()), testLockRequest().Scope)
	c.Assert(owner.cleanupContextError, qt.IsNil)
	c.Assert(owner.cleanupBounded, qt.IsTrue)
	_, err = adapter.TryLock(ctx, testLockRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(owner.tries, qt.Equals, 1)
}

func TestLockServerRejectsFalseReleaseAndRetainsCleanupFailure(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{releaseOutcome: protocol.LockResult_ACQUIRED}
	adapter := &serverAdapter{server: Server{Locks: owner}}
	result, err := adapter.ReleaseLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	result, err = adapter.TryLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(err.Error(), qt.Contains, "cleanup could not be confirmed")
	owner.result = testLockReceipt(protocol.LockResult_BUSY)
	result, err = adapter.CheckLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	owner.result = testLockReceipt(protocol.LockResult_ACQUIRED)
	result, err = adapter.TryLock(c.Context(), testLockRequest())
	c.Assert(err, qt.IsNil)
	result.Scope.Name = "caller change"
	c.Assert(owner.result.Scope.Name, qt.Equals, "migration")
	c.Assert(owner.releases, qt.Equals, 2)
}

func TestLockServiceMustMatchItsAdvertisement(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{}
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "1", Features: []string{provider.LocksFeature}, Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.LocksFeature}, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := Server{Description: description, Sessions: owner}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Locks = (*lockService)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Locks = owner
	c.Assert(server.Validate(), qt.IsNil)
	description.Features, description.Engines[0].Features = nil, nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Locks = nil
	c.Assert(server.Validate(), qt.IsNil)
}

func TestLockClientPreservesAcquisitionAndCleanupFailures(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{
		err:        status.Error(codes.Unavailable, "acquisition password=secret"),
		releaseErr: status.Error(codes.DeadlineExceeded, "cleanup password=secret"),
	}
	result, err := lockClient(owner).TryLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err.Error(), qt.Contains, "Unavailable")
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
	c.Assert(owner.tries, qt.Equals, 1)
	c.Assert(owner.releases, qt.Equals, 1)
}

func TestLockServerCleansAnErroredAcquisition(t *testing.T) {
	c := qt.New(t)
	owner := &lockService{
		result:         testLockReceipt(protocol.LockResult_ACQUIRED),
		err:            status.Error(codes.Unavailable, "acquisition failed after dispatch"),
		releaseOutcome: protocol.LockResult_RELEASED,
	}
	adapter := &serverAdapter{server: Server{Locks: owner}}
	result, err := adapter.TryLock(c.Context(), testLockRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Unavailable)
	c.Assert(owner.tries, qt.Equals, 1)
	c.Assert(owner.releases, qt.Equals, 1)
	c.Assert(owner.cleanup.Scope, qt.CmpEquals(protocmp.Transform()), testLockRequest().Scope)
}
