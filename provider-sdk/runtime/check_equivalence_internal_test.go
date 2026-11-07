package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// checksDescription offers CHECK comparison on one engine.
func checksDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/checks", Version: "1", SemanticIdentity: "checks-1", Features: []string{provider.CheckEquivalenceFeature},
		Engines: []*protocol.Engine{{
			Id: "checks", Features: []string{provider.CheckEquivalenceFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
		}},
	}
}

// uncheckedDescription is checksDescription without the feature.
func uncheckedDescription() *protocol.Description {
	d := checksDescription()
	checks := func(f string) bool { return f == provider.CheckEquivalenceFeature }
	d.Features = slices.DeleteFunc(d.Features, checks)
	d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, checks)
	return d
}

// compareChecksRequest asks about the CHECK items_score_check of table
// items, held as `(score >= '0')` and declared as `score >= 0`.
func compareChecksRequest() *protocol.CompareChecksRequest {
	rules := proto.CloneOf(checksDescription().Engines[0].Identifiers)
	return &protocol.CompareChecksRequest{Engine: "checks", Identifiers: &protocol.ComparisonIdentifiers{Rules: rules},
		Tables: []*protocol.CheckComparisonTable{{
			Subject: &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "items", Normalized: "items"}},
			Current: &protocol.ObservedTable{Name: "items", Columns: []*protocol.ObservedColumn{{Name: "score", DataType: "bigint"}},
				Constraints: []*protocol.ObservedConstraint{{Name: "items_score_check", TableName: "items", Type: "CHECK", CheckClause: new("(score >= '0')")}}},
			Pairs: []*protocol.CheckComparisonPair{{Constraint: "items_score_check", Declared: "score >= 0"}},
		}}}
}

// checkOwner answers every pair as equivalent, or with its canned reply. It
// counts its calls.
type checkOwner struct {
	calls int
	reply *protocol.ComparedChecks
}

func (o *checkOwner) CompareChecks(_ context.Context, request *protocol.CompareChecksRequest) (*protocol.ComparedChecks, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	result := &protocol.ComparedChecks{Complete: true}
	for _, table := range request.GetTables() {
		answered := &protocol.ComparedCheckTable{Subject: proto.CloneOf(table.GetSubject())}
		for _, pair := range table.GetPairs() {
			answered.Checks = append(answered.Checks, &protocol.ComparedCheck{Constraint: pair.GetConstraint(), Equivalent: true})
		}
		result.Tables = append(result.Tables, answered)
	}
	return result, nil
}

// checkLoopback forwards CompareChecks to a server adapter, or answers with a
// canned reply when one is set, as a peer that is not the SDK server could.
type checkLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.ComparedChecks
}

func (l *checkLoopback) CompareChecks(ctx context.Context, request *protocol.CompareChecksRequest, _ ...grpc.CallOption) (*protocol.ComparedChecks, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.CompareChecks(ctx, request)
}

// newCheckLoopback is a client and a server for a provider that offers
// description. The feature is one-way, so the host lists none.
func newCheckLoopback(description *protocol.Description, owner *checkOwner) (*Client, *checkLoopback) {
	server := negotiatedServer(Server{Description: description, CheckComparer: owner})
	peer := &checkLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func TestCompareChecksConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &checkOwner{}
	client, peer := newCheckLoopback(checksDescription(), owner)
	request := compareChecksRequest()
	before := proto.CloneOf(request)
	compared, err := client.CompareChecks(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(compared.GetTables(), qt.HasLen, 1)
	c.Assert(compared.GetTables()[0].GetChecks()[0].GetConstraint(), qt.Equals, "items_score_check")
	c.Assert(compared.GetTables()[0].GetChecks()[0].GetEquivalent(), qt.IsTrue)
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// A host never sends the call to a provider that does not list the feature,
// and that provider's server refuses it before its owner sees it.
func TestCompareChecksNeedsTheFeature(t *testing.T) {
	c := qt.New(t)
	owner := &checkOwner{}
	client, peer := newCheckLoopback(uncheckedDescription(), owner)
	compared, err := client.CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(err, qt.ErrorMatches, `.*CHECK equivalence: the provider does not compare checks`)
	c.Assert(compared, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)
	compared, err = peer.server.CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(compared, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
}

// An invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server. A canceled call
// sends nothing.
func TestCompareChecksConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &checkOwner{reply: &protocol.ComparedChecks{Complete: true}}
	client, peer := newCheckLoopback(checksDescription(), owner)
	compared, err := peer.server.CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(compared, qt.IsNil)
	compared, err = client.CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(err, qt.ErrorMatches, `provider compare checks failed \(Internal\)`)
	c.Assert(compared, qt.IsNil)

	peer.canned = owner.reply
	compared, err = client.CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*CHECK equivalence: the reply does not answer every table`)
	c.Assert(compared, qt.IsNil)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.calls = 0
	_, err = client.CompareChecks(canceled, compareChecksRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)
}

// The service and its feature are advertised together, and a server without
// the service answers the call as unavailable.
func TestCheckComparerMatchesItsAdvertisement(t *testing.T) {
	c := qt.New(t)
	server := Server{Description: checksDescription(), Sessions: &sessionOwner{}, CheckComparer: &checkOwner{}}
	c.Assert(server.Validate(), qt.IsNil)
	without := server
	without.CheckComparer = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*schema\.check-equivalence advertisement differs from implementation`)
	unadvertised := server
	unadvertised.Description = uncheckedDescription()
	c.Assert(unadvertised.Validate(), qt.ErrorMatches, `.*schema\.check-equivalence advertisement differs from implementation`)
	_, err := negotiatedServer(without).CompareChecks(c.Context(), compareChecksRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}
