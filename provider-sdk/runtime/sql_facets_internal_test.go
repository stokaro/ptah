package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

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

const sqlFacetModel = "example.org/test/feeds"

// sqlFacetDescription is the SQL schema engine with SQLFacetsFeature listed
// and a DESIRED model a SQL document declares.
func sqlFacetDescription() *protocol.Description {
	d := sqlSchemaDescription()
	d.Features = append(d.Features, provider.SQLFacetsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.SQLFacetsFeature)
	engine.ValueModels = []*protocol.ValueModel{{Kind: sqlFacetModel, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json",
		DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: testModelDigest, CanonicalFormat: "example.org/feeds-v1",
		SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}}}
	return d
}

// facetReader declares table a with one facet, whatever the documents say.
type facetReader struct{}

func (facetReader) ReadSQLSchema(_ context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	return sqlFacetReply(request), nil
}

func sqlFacetReply(request *protocol.ReadSQLSchemaRequest) *protocol.SQLSchema {
	model := sqlFacetDescription().Engines[0].ValueModels[0]
	span := &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 1, Line: 1, Column: 2}}
	trace := func(collection protocol.SQLObjectTrace_Collection) *protocol.SQLObjectTrace {
		return &protocol.SQLObjectTrace{Collection: collection, Source: &protocol.SourceTrace{DocumentId: request.GetDocuments()[0].GetId(), Span: span}}
	}
	result := &protocol.SQLSchema{Complete: true,
		Tables: []*protocol.DeclaredTable{{Name: "a", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "Int"}}}},
		Facets: []*protocol.Envelope{{ProviderId: "example.org/test", Kind: sqlFacetModel, SchemaVersion: 1, Encoding: "json", DefinitionSha256: testModelDigest,
			Owner: &protocol.Reference{Engine: "test", Kind: "table", Name: "a"}, Payload: []byte(`{}`)}},
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED,
			Kinds: []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}},
		Traces: []*protocol.SQLObjectTrace{trace(protocol.SQLObjectTrace_TABLES), trace(protocol.SQLObjectTrace_FACETS)},
	}
	for _, document := range request.GetDocuments() {
		result.Declarations = append(result.Declarations, &protocol.SQLDocumentDeclarations{DocumentId: document.GetId()})
	}
	return result
}

// facetLoopback forwards ReadSQLSchema to a server adapter, or answers with a
// canned reply when one is set, as a peer that is not the SDK server could.
type facetLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	canned *protocol.SQLSchema
}

func (l *facetLoopback) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest, _ ...grpc.CallOption) (*protocol.SQLSchema, error) {
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.ReadSQLSchema(ctx, request)
}

func newFacetLoopback(hostFeatures ...string) (*Client, *facetLoopback) {
	server := negotiatedServer(Server{Description: sqlFacetDescription(), SQLSchemaReader: facetReader{}}, hostFeatures...)
	peer := &facetLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func TestSQLSchemaFacetsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	client, _ := newFacetLoopback(provider.SQLFacetsFeature)
	request := sqlSchemaRequest("CREATE TABLE a (id Int);\n")
	request.TargetFeatures = []string{provider.SQLFacetsFeature}

	result, err := client.ReadSQLSchema(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Facets, qt.HasLen, 1)
	c.Assert(result.Facets[0].Owner.Name, qt.Equals, "a")
}

// Facets pass only when both sides list the feature: the server refuses its
// owner's reply for a host that did not list it, and a client whose
// negotiated description lacks it refuses the same reply from a peer that is
// not the SDK server. The request carries the target features of a session
// opened without the feature.
func TestSQLSchemaFacetsConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	client, peer := newFacetLoopback()
	request := sqlSchemaRequest("CREATE TABLE a (id Int);\n")

	result, err := peer.server.ReadSQLSchema(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(status.Convert(err).Message(), qt.Matches, `.*SQL schema: facets were not negotiated`)
	c.Assert(result, qt.IsNil)

	peer.canned = sqlFacetReply(request)
	result, err = client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*SQL schema: facets were not negotiated`)
	c.Assert(result, qt.IsNil)
}
