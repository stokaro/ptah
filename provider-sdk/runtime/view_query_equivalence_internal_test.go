package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

import (
	"context"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// viewQueriesDescription offers view query comparison on one engine that
// carries views as common objects.
func viewQueriesDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/views", Version: "1", SemanticIdentity: "views-1",
		Features: []string{provider.CommonObjectsFeature, provider.ViewQueryEquivalenceFeature},
		Engines: []*protocol.Engine{{
			Id: "views", Features: []string{provider.CommonObjectsFeature, provider.ViewQueryEquivalenceFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
			CommonObjects: &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW}},
		}},
	}
}

// unviewedDescription is viewQueriesDescription without the feature.
func unviewedDescription() *protocol.Description {
	d := viewQueriesDescription()
	views := func(f string) bool { return f == provider.ViewQueryEquivalenceFeature }
	d.Features = slices.DeleteFunc(d.Features, views)
	d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, views)
	return d
}

// compareViewQueriesRequest asks about view active, held as `SELECT Id FROM
// users` and declared as `SELECT id FROM users`.
func compareViewQueriesRequest() *protocol.CompareViewQueriesRequest {
	rules := proto.CloneOf(viewQueriesDescription().Engines[0].Identifiers)
	return &protocol.CompareViewQueriesRequest{Engine: "views", Identifiers: &protocol.ComparisonIdentifiers{Rules: rules},
		Pairs: []*protocol.ViewQueryPair{{
			Current:  &protocol.ObservedView{Identity: &protocol.Reference{Engine: "views", Kind: "view", Name: "active"}, Body: "SELECT Id FROM users"},
			Declared: "SELECT id FROM users",
		}}}
}

// viewQueryOwner answers every pair by comparing the two texts exactly, or
// with its canned reply. It counts its calls, and refuses to prepare common
// objects, which no test asks of it.
type viewQueryOwner struct {
	calls int
	reply *protocol.ComparedViewQueries
}

func (o *viewQueryOwner) CompareViewQueries(_ context.Context, request *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	result := &protocol.ComparedViewQueries{Complete: true}
	for _, pair := range request.GetPairs() {
		result.Queries = append(result.Queries, &protocol.ComparedViewQuery{Identity: proto.CloneOf(pair.GetCurrent().GetIdentity()),
			Equivalent: pair.GetCurrent().GetBody() == pair.GetDeclared()})
	}
	return result, nil
}

func (o *viewQueryOwner) PrepareCommonObjects(context.Context, *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	return nil, errors.New("the view query owner prepares no common objects")
}

// viewQueryLoopback forwards CompareViewQueries to a server adapter, or
// answers with a canned reply when one is set, as a peer that is not the SDK
// server could.
type viewQueryLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.ComparedViewQueries
}

func (l *viewQueryLoopback) CompareViewQueries(ctx context.Context, request *protocol.CompareViewQueriesRequest, _ ...grpc.CallOption) (*protocol.ComparedViewQueries, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.CompareViewQueries(ctx, request)
}

// newViewQueryLoopback is a client and a server for a provider that offers
// description, negotiated with a host that lists common objects: the views
// travel as common objects, and the feature itself is the provider's alone.
func newViewQueryLoopback(description *protocol.Description, owner *viewQueryOwner) (*Client, *viewQueryLoopback) {
	server := negotiatedServer(Server{Description: description, ViewComparer: owner, CommonObjects: owner}, provider.CommonObjectsFeature)
	peer := &viewQueryLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func TestCompareViewQueriesConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &viewQueryOwner{}
	client, peer := newViewQueryLoopback(viewQueriesDescription(), owner)
	request := compareViewQueriesRequest()
	before := proto.CloneOf(request)
	compared, err := client.CompareViewQueries(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(compared.GetQueries(), qt.HasLen, 1)
	c.Assert(compared.GetQueries()[0].GetIdentity().GetName(), qt.Equals, "active")
	c.Assert(compared.GetQueries()[0].GetEquivalent(), qt.IsFalse)
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// A host never sends the call to a provider that does not list the feature,
// and that provider's server refuses it before its owner sees it.
func TestCompareViewQueriesNeedsTheFeature(t *testing.T) {
	c := qt.New(t)
	owner := &viewQueryOwner{}
	client, peer := newViewQueryLoopback(unviewedDescription(), owner)
	compared, err := client.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(err, qt.ErrorMatches, `.*view query equivalence: the provider does not compare view queries`)
	c.Assert(compared, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)
	compared, err = peer.server.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(compared, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
}

// A host that did not list common objects has no common view to ask about,
// and the server refuses a request that holds one before its owner sees it.
func TestCompareViewQueriesNeedsCommonViews(t *testing.T) {
	c := qt.New(t)
	owner := &viewQueryOwner{}
	server := negotiatedServer(Server{Description: viewQueriesDescription(), ViewComparer: owner, CommonObjects: owner})
	compared, err := server.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(compared, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
}

// An invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server. A canceled call
// sends nothing.
func TestCompareViewQueriesConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &viewQueryOwner{reply: &protocol.ComparedViewQueries{Complete: true}}
	client, peer := newViewQueryLoopback(viewQueriesDescription(), owner)
	compared, err := peer.server.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(compared, qt.IsNil)
	compared, err = client.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(err, qt.ErrorMatches, `provider compare view queries failed \(Internal\)`)
	c.Assert(compared, qt.IsNil)

	peer.canned = owner.reply
	compared, err = client.CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*view query equivalence: the reply does not answer every pair`)
	c.Assert(compared, qt.IsNil)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.calls = 0
	_, err = client.CompareViewQueries(canceled, compareViewQueriesRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)
}

// The service and its feature are advertised together, and a server without
// the service answers the call as unavailable.
func TestViewQueryComparerMatchesItsAdvertisement(t *testing.T) {
	c := qt.New(t)
	owner := &viewQueryOwner{}
	server := Server{Description: viewQueriesDescription(), Sessions: &sessionOwner{}, ViewComparer: owner, CommonObjects: owner}
	c.Assert(server.Validate(), qt.IsNil)
	without := server
	without.ViewComparer = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*schema\.view-query-equivalence advertisement differs from implementation`)
	unadvertised := server
	unadvertised.Description = unviewedDescription()
	c.Assert(unadvertised.Validate(), qt.ErrorMatches, `.*schema\.view-query-equivalence advertisement differs from implementation`)
	_, err := negotiatedServer(without, provider.CommonObjectsFeature).CompareViewQueries(c.Context(), compareViewQueriesRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}
