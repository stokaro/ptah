package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, and malformed peers must reach the client
// without passing the SDK server's own checks.

import (
	"context"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func devCleanupDescription() *protocol.Description {
	description, _, _ := runtimeDevExchange()
	description.Features = append(description.Features, provider.DevCleanupFeature, provider.ReplayGuardFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.DevCleanupFeature, provider.ReplayGuardFeature, provider.DevRealmsFeature)
	return description
}

// memoryRealm holds a dev database's objects. A cleanup removes all of them,
// or none when one has no statement that drops it.
type memoryRealm struct {
	objects []*protocol.DevObject
	cleans  int
}

func (m *memoryRealm) ListDevObjects(context.Context, *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error) {
	return &protocol.DevObjects{Complete: true, Objects: m.objects}, nil
}

func (m *memoryRealm) CleanDevDatabase(_ context.Context, request *protocol.CleanDevRequest) (*protocol.DevCleanup, error) {
	m.cleans++
	var blocking []*protocol.DevObject
	for _, object := range m.objects {
		if object.Refusal != "" {
			blocking = append(blocking, object)
		}
	}
	if len(blocking) > 0 {
		return &protocol.DevCleanup{RequestId: request.RequestId, Outcome: protocol.DevCleanup_REFUSED, Blocking: blocking}, nil
	}
	m.objects = nil
	return &protocol.DevCleanup{RequestId: request.RequestId, Outcome: protocol.DevCleanup_CLEANED}, nil
}

// memoryGuard refuses a statement that creates or alters a user, as a
// guard confined to one database refuses what the whole server shares.
type memoryGuard struct{}

func (memoryGuard) GuardReplay(_ context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	result := &protocol.GuardedReplay{}
	for _, statement := range request.Statements {
		verdict := &protocol.ReplayVerdict{}
		if strings.Contains(statement, "USER") && !request.GetScope().GetServer() {
			verdict.Refused, verdict.Operation = true, "a user, which the whole server shares"
		}
		result.Verdicts = append(result.Verdicts, verdict)
	}
	return result, nil
}

type devLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
}

func (l *devLoopback) ListDevObjects(ctx context.Context, request *protocol.ListDevObjectsRequest, _ ...grpc.CallOption) (*protocol.DevObjects, error) {
	l.calls++
	return l.server.ListDevObjects(ctx, request)
}

func (l *devLoopback) CleanDevDatabase(ctx context.Context, request *protocol.CleanDevRequest, _ ...grpc.CallOption) (*protocol.DevCleanup, error) {
	l.calls++
	return l.server.CleanDevDatabase(ctx, request)
}

func (l *devLoopback) GuardReplay(ctx context.Context, request *protocol.GuardReplayRequest, _ ...grpc.CallOption) (*protocol.GuardedReplay, error) {
	l.calls++
	return l.server.GuardReplay(ctx, request)
}

func newDevLoopback(realm *memoryRealm) (*Client, *devLoopback) {
	description := devCleanupDescription()
	peer := &devLoopback{server: &serverAdapter{server: Server{Description: description, DevCleaner: realm, ReplayGuard: memoryGuard{}}}}
	return &Client{description: description, rpc: peer}, peer
}

func listRequest() *protocol.ListDevObjectsRequest {
	return &protocol.ListDevObjectsRequest{SessionId: "session"}
}

func cleanRequest(id string) *protocol.CleanDevRequest {
	return &protocol.CleanDevRequest{SessionId: "session", RequestId: id}
}

// The list a host judges a dev database clean by and the cleanup read the same
// objects: an object no statement drops stops the cleanup before anything is
// removed, and once it is gone the cleanup leaves the list empty.
func TestDevCleanupConformanceListsWhatItRemoves(t *testing.T) {
	c := qt.New(t)
	realm := &memoryRealm{objects: []*protocol.DevObject{
		{Object: &protocol.Reference{Kind: "table", Name: "rates"}},
		{Object: &protocol.Reference{Kind: "column-store", Name: "events"}, Refusal: "no statement drops a column store"},
	}}
	client, peer := newDevLoopback(realm)
	listed, err := client.ListDevObjects(c.Context(), listRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(listed.Objects, qt.HasLen, 2)
	refused, err := client.CleanDevDatabase(c.Context(), cleanRequest("1"))
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	var failed *provider.DevCleanupError
	c.Assert(err, qt.ErrorAs, &failed)
	c.Assert(failed.Outcome, qt.Equals, protocol.DevCleanup_REFUSED)
	c.Assert(refused.Blocking, qt.HasLen, 1)
	c.Assert(refused.Blocking[0].Object.Name, qt.Equals, "events")
	still, err := client.ListDevObjects(c.Context(), listRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(still.Objects, qt.HasLen, 2)
	realm.objects = realm.objects[:1]
	cleaned, err := client.CleanDevDatabase(c.Context(), cleanRequest("2"))
	c.Assert(err, qt.IsNil)
	c.Assert(cleaned.Outcome, qt.Equals, protocol.DevCleanup_CLEANED)
	empty, err := client.ListDevObjects(c.Context(), listRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(empty.Objects, qt.HasLen, 0)
	c.Assert(peer.calls, qt.Equals, 5)
}

type devCleanupPeer struct {
	protocol.ProviderClient
	objects *protocol.DevObjects
	cleanup *protocol.DevCleanup
	guarded *protocol.GuardedReplay
	err     error
	calls   int
}

func (p *devCleanupPeer) ListDevObjects(context.Context, *protocol.ListDevObjectsRequest, ...grpc.CallOption) (*protocol.DevObjects, error) {
	p.calls++
	return p.objects, p.err
}

func (p *devCleanupPeer) CleanDevDatabase(context.Context, *protocol.CleanDevRequest, ...grpc.CallOption) (*protocol.DevCleanup, error) {
	p.calls++
	return p.cleanup, p.err
}

func (p *devCleanupPeer) GuardReplay(context.Context, *protocol.GuardReplayRequest, ...grpc.CallOption) (*protocol.GuardedReplay, error) {
	p.calls++
	return p.guarded, p.err
}

// A lost cleanup reply leaves the dev database's state unknown. The client
// sends the cleanup once and exposes no result; an incomplete list is an
// error, never an empty dev database.
func TestDevCleanupClientReportsALostReplyAsUnknownWithoutRetry(t *testing.T) {
	c := qt.New(t)
	peer := &devCleanupPeer{err: status.Error(codes.DeadlineExceeded, "dropped 3 of 5")}
	client := &Client{description: devCleanupDescription(), rpc: peer}
	result, err := client.CleanDevDatabase(c.Context(), cleanRequest("1"))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 1)
	peer.err, peer.cleanup = nil, &protocol.DevCleanup{RequestId: "other", Outcome: protocol.DevCleanup_CLEANED}
	result, err = client.CleanDevDatabase(c.Context(), cleanRequest("1"))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.objects = &protocol.DevObjects{}
	listed, err := client.ListDevObjects(c.Context(), listRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrIncomplete)
	c.Assert(listed, qt.IsNil)
	client.description.Features = slices.DeleteFunc(client.description.Features, func(name string) bool { return name == provider.DevCleanupFeature })
	_, err = client.ListDevObjects(c.Context(), listRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.calls, qt.Equals, 3)
}

// The guard answers every statement for the scope it was given: a user is
// refused in one database and accepted on a server the run owns.
func TestReplayGuardConformanceJudgesTheScope(t *testing.T) {
	c := qt.New(t)
	client, _ := newDevLoopback(&memoryRealm{})
	request := &protocol.GuardReplayRequest{Engine: "test", Scope: &protocol.ReplayScope{Database: "local"},
		Statements: []string{"CREATE TABLE a (id Int32)", "CREATE USER u"}}
	confined, err := client.GuardReplay(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(confined, &protocol.GuardedReplay{Verdicts: []*protocol.ReplayVerdict{
		{}, {Refused: true, Operation: "a user, which the whole server shares"},
	}}), qt.IsTrue)
	request.Scope = &protocol.ReplayScope{Server: true, Provisioned: true}
	owned, err := client.GuardReplay(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(owned.Verdicts, qt.HasLen, 2)
	c.Assert(owned.Verdicts[1].Refused, qt.IsFalse)
}

// A guard reply that does not answer every statement exposes no verdict at
// either boundary, so no statement is replayed on a partial answer.
func TestReplayGuardRefusesAPartialAnswer(t *testing.T) {
	c := qt.New(t)
	peer := &devCleanupPeer{guarded: &protocol.GuardedReplay{Verdicts: []*protocol.ReplayVerdict{{}}}}
	client := &Client{description: devCleanupDescription(), rpc: peer}
	request := &protocol.GuardReplayRequest{Engine: "test", Statements: []string{"CREATE TABLE a (id Int32)", "CREATE USER u"}}
	got, err := client.GuardReplay(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(got, qt.IsNil)
	server := &serverAdapter{server: Server{Description: devCleanupDescription(), ReplayGuard: partialGuard{}}}
	served, err := server.GuardReplay(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(served, qt.IsNil)
	unguarded := &serverAdapter{server: Server{Description: devCleanupDescription()}}
	_, err = unguarded.GuardReplay(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = unguarded.ListDevObjects(c.Context(), listRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = unguarded.CleanDevDatabase(c.Context(), cleanRequest("1"))
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

type partialGuard struct{}

func (partialGuard) GuardReplay(context.Context, *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	return &protocol.GuardedReplay{Verdicts: []*protocol.ReplayVerdict{{}}}, nil
}

// A realm reply that points the operator's credentials at another server is
// refused at the client, and at the server, which releases the realm the host
// never receives.
func TestDevRealmReplyStaysWithTheOperatorsDatabase(t *testing.T) {
	c := qt.New(t)
	description := devCleanupDescription()
	request := &protocol.ProvisionDevRequest{Engine: "test", Address: "grpcs://admin:secret@ydb.example:2135/local"}  // #nosec G101 -- fixture address, not a credential
	valid := &protocol.DevDatabase{Lease: "realm-1", Url: "grpcs://admin:secret@ydb.example:2135/local?dev_realm=r1"} // #nosec G101 -- fixture address, not a credential
	peer := &devPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result = &protocol.DevDatabase{Lease: "realm-2", Url: "grpcs://admin:secret@other.example:2135/local"} // #nosec G101 -- fixture address, not a credential
	result, err = client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	owner := &devOwner{result: proto.CloneOf(peer.result), afterProvision: func() {}}
	server := negotiatedServer(Server{Description: description, DevProvisioner: owner}, provider.DevRealmsFeature)
	served, err := server.ProvisionDevDatabase(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(served, qt.IsNil)
	c.Assert(owner.released, qt.DeepEquals, []string{"realm-2"})
}

// A host that does not list dev realms cannot send an address: the field is
// an addition of contract 25.1, which a provider uses only when both sides
// list it.
func TestDevRealmNeedsBothSides(t *testing.T) {
	c := qt.New(t)
	description := devCleanupDescription()
	request := &protocol.ProvisionDevRequest{Engine: "test", Address: "grpcs://ydb.example:2135/local"}
	owner := &devOwner{result: &protocol.DevDatabase{Lease: "realm-1", Url: "grpcs://ydb.example:2135/local?dev_realm=r1"}, afterProvision: func() {}}
	unlisted := negotiatedServer(Server{Description: description, DevProvisioner: owner})
	_, err := unlisted.ProvisionDevDatabase(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	listed := negotiatedServer(Server{Description: description, DevProvisioner: owner}, provider.DevRealmsFeature)
	_, err = listed.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.IsNil)
	client := &Client{description: provider.NegotiatedDescription(description, nil), rpc: &devPeer{}}
	_, err = client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}
