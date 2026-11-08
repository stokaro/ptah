package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a snapshot from a peer
// that is not the SDK server must reach the client's own checks.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// databaseKindsExchange is a read filtered to the schema app of an engine
// whose native kind is a database kind, reporting one object named at the
// database beside the scope.
func databaseKindsExchange() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, request, snapshot := inspectionExchange()
	engine := description.Engines[0]
	value := snapshot.NativeObjects[0]
	description.Features = append(description.Features, provider.DatabaseKindsFeature)
	engine.Features = append(engine.Features, provider.DatabaseKindsFeature)
	engine.DatabaseKinds = []string{value.Kind}
	snapshot.TargetFeatures = []string{provider.DatabaseKindsFeature}
	value.Owner = &protocol.Reference{Engine: "test", Database: "db", Kind: value.Kind, Name: "pool"}
	snapshot.Coverage.Subjects[0].Subject, snapshot.Coverage.Subjects[0].NormalizedSubject = proto.CloneOf(value.Owner), proto.CloneOf(value.Owner)
	part := func(text string) *protocol.NamespaceComponent {
		return &protocol.NamespaceComponent{Source: text, Normalized: text}
	}
	request.Schemas = []string{"app"}
	snapshot.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: "app", Namespaces: []*protocol.ModelNamespace{{Database: part("db"), Catalog: part(""), Schema: part("app")}}}}
	snapshot.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the database alone"}
	snapshot.Coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{{Scope: &protocol.ModelNamespace{Database: part("db"), Catalog: part(""), Schema: part("")},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
	return description, request, snapshot
}

// databaseInspector answers every inspection with its snapshot.
type databaseInspector struct{ snapshot *protocol.Schema }

func (i *databaseInspector) Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error) {
	return proto.CloneOf(i.snapshot), nil
}

// databaseLoopback forwards Inspect to a server adapter, or answers with a
// canned snapshot when one is set, as a peer that is not the SDK server could.
type databaseLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	canned *protocol.Schema
}

func (l *databaseLoopback) Inspect(ctx context.Context, request *protocol.InspectRequest, _ ...grpc.CallOption) (*protocol.Schema, error) {
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.Inspect(ctx, request)
}

func TestInspectDatabaseKindsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := databaseKindsExchange()
	server := negotiatedServer(Server{Description: description, Inspector: &databaseInspector{snapshot: snapshot}}, provider.DatabaseKindsFeature)
	client := &Client{description: server.negotiated(), rpc: &databaseLoopback{server: server}}

	result, err := client.Inspect(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, snapshot), qt.IsTrue)
}

// The object passes only when both sides list the feature: the server
// refuses its owner's snapshot for a host that did not list it, and a client
// whose negotiated description lacks it refuses the same snapshot from a peer
// that is not the SDK server. The snapshot carries the target features of a
// session opened without the feature.
func TestInspectDatabaseKindsConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := databaseKindsExchange()
	snapshot.TargetFeatures = nil
	server := negotiatedServer(Server{Description: description, Inspector: &databaseInspector{snapshot: snapshot}})
	peer := &databaseLoopback{server: server}
	client := &Client{description: server.negotiated(), rpc: peer}

	result, err := server.Inspect(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(status.Convert(err).Message(), qt.Matches, `.*inspected subject is outside the requested scope`)
	c.Assert(result, qt.IsNil)

	peer.canned = snapshot
	result, err = client.Inspect(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*inspected subject is outside the requested scope`)
	c.Assert(result, qt.IsNil)
}
