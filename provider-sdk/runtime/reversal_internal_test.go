package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

import (
	"context"
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

// reversalDescription offers the reversal of one change kind of the
// comparison exchange's engine, beside the planning a reversed change goes
// through.
func reversalDescription() *protocol.Description {
	d, _, _ := runtimeComparisonExchange()
	d.Features = append(d.Features, provider.PlanFeaturesFeature, provider.ReverseChangesFeature)
	engine := d.Engines[0]
	change := proto.CloneOf(engine.ValueModels[0])
	change.Kind, change.Representation = "example.org/test/value-change", protocol.ValueInput_CHANGE
	engine.ValueModels = append(engine.ValueModels, change)
	engine.Features = append(engine.Features, provider.ReverseChangesFeature)
	engine.Reversals = []*protocol.ModelReversal{{Id: "values", ChangeKinds: []string{change.Kind}}}
	return d
}

// unreversedDescription is reversalDescription without the feature.
func unreversedDescription() *protocol.Description {
	d := reversalDescription()
	unlisted := func(f string) bool { return f == provider.ReverseChangesFeature }
	d.Features = slices.DeleteFunc(d.Features, unlisted)
	d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, unlisted)
	d.Engines[0].Reversals = nil
	return d
}

// reversalChange is one change of the engine's change kind at table
// "original".
func reversalChange(id string) *protocol.NormalizedValue {
	value := &protocol.Envelope{DefinitionSha256: testModelDigest, ProviderId: "example.org/test", Kind: "example.org/test/value-change", SchemaVersion: 1, Encoding: "json",
		Owner: &protocol.Reference{Engine: "test", Kind: "table", Name: "original"}, Payload: []byte("{}")}
	return &protocol.NormalizedValue{Id: id, Value: value, NormalizedOwner: proto.CloneOf(value.Owner), CanonicalFormat: "example.org/value-v1", CanonicalProjection: []byte("canonical"),
		ChangeSemantics: &protocol.ChangeSemantics{ReplacesOwner: new(false), RemovesState: new(false), Impact: protocol.ChangeSemantics_BEHAVIORAL, Reason: "The value changes",
			Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "No access change"}}
}

func reverseRequest() *protocol.ReverseChangesRequest {
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	subject := &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "original", Normalized: "original"}}
	return &protocol.ReverseChangesRequest{Engine: "test", ReversalId: "values", TargetFeatures: []string{provider.ReverseChangesFeature},
		Identifiers: &protocol.ComparisonIdentifiers{Rules: rules}, Changes: []*protocol.PlanningChange{{Subject: subject, Value: reversalChange("forward")}}}
}

// reversalOwner undoes each change with a change of its kind and states no
// forward state, or answers with its canned reply. It counts its calls.
type reversalOwner struct {
	calls int
	reply *protocol.ReversedChanges
}

func (o *reversalOwner) ReverseChanges(_ context.Context, request *protocol.ReverseChangesRequest) (*protocol.ReversedChanges, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	result := &protocol.ReversedChanges{Complete: true}
	for _, change := range request.Changes {
		result.Changes = append(result.Changes, &protocol.ReversedChange{Subject: proto.CloneOf(change.Subject), Value: reversalChange("reverse"), Strategy: "Set the value back"})
	}
	return result, nil
}

// reversalLoopback forwards ReverseChanges to a server adapter, or answers
// with a canned reply when one is set, as a peer that is not the SDK server
// could.
type reversalLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.ReversedChanges
}

func (l *reversalLoopback) ReverseChanges(ctx context.Context, request *protocol.ReverseChangesRequest, _ ...grpc.CallOption) (*protocol.ReversedChanges, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.ReverseChanges(ctx, request)
}

func newReversalLoopback(description *protocol.Description, owner *reversalOwner, hostFeatures ...string) (*Client, *reversalLoopback) {
	server := negotiatedServer(Server{Description: description, ChangeReverser: owner}, hostFeatures...)
	peer := &reversalLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func TestReverseChangesConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &reversalOwner{}
	client, peer := newReversalLoopback(reversalDescription(), owner, provider.ReverseChangesFeature)
	request := reverseRequest()
	before := proto.CloneOf(request)

	reversed, err := client.ReverseChanges(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(reversed.Changes, qt.HasLen, 1)
	c.Assert(reversed.Changes[0].Value.Id, qt.Equals, "reverse")
	c.Assert(reversed.Changes[0].Strategy, qt.Equals, "Set the value back")
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// The call is used only when both sides list the feature: the client sends
// nothing to a provider that does not offer it, and the server refuses a host
// that did not list it.
func TestReverseChangesNeedsBothSides(t *testing.T) {
	for _, test := range []struct {
		name        string
		description *protocol.Description
		listed      []string
	}{
		{"a provider that does not list it", unreversedDescription(), []string{provider.ReverseChangesFeature}},
		{"a host that does not list it", reversalDescription(), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &reversalOwner{}
			client, peer := newReversalLoopback(test.description, owner, test.listed...)
			reversed, err := client.ReverseChanges(c.Context(), reverseRequest())
			c.Assert(err, qt.ErrorMatches, `.*change reversal: the feature was not negotiated`)
			c.Assert(reversed, qt.IsNil)
			c.Assert(peer.calls, qt.Equals, 0)
			reversed, err = peer.server.ReverseChanges(c.Context(), reverseRequest())
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(reversed, qt.IsNil)
			c.Assert(owner.calls, qt.Equals, 0)
		})
	}
}

// An invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server.
func TestReverseChangesConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	invalid := &protocol.ReversedChanges{Complete: true, Changes: []*protocol.ReversedChange{{Subject: reverseRequest().Changes[0].Subject}}}
	owner := &reversalOwner{reply: invalid}
	client, peer := newReversalLoopback(reversalDescription(), owner, provider.ReverseChangesFeature)

	reversed, err := peer.server.ReverseChanges(c.Context(), reverseRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(reversed, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)

	peer.canned = invalid
	reversed, err = client.ReverseChanges(c.Context(), reverseRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*a strategy or a limitation is not one line`)
	c.Assert(reversed, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 1)
}
