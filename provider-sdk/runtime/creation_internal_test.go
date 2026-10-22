package runtime

// White-box testing required: malformed peer replies must bypass the SDK server,
// and owner mutation must be checked against the adapter's original capture.

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

func runtimeCreationExchange() (*protocol.Description, *protocol.ProjectTableCreationsRequest, *protocol.ProjectedTableCreations) {
	description, _, _ := normalizationExchange()
	description.Features = append(description.Features, provider.ProjectTableCreationsFeature)
	description.Engines[0].Features = []string{provider.ProjectTableCreationsFeature}
	subject := &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "items", Normalized: "items"}}
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	empty := &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}}
	request := &protocol.ProjectTableCreationsRequest{Engine: "test", Identifiers: &protocol.ComparisonIdentifiers{Rules: rules},
		Tables: []*protocol.TableCreationInput{{Subject: subject,
			Declaration: &protocol.DeclaredTable{Name: "items", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "INTEGER"}},
				Features: &protocol.CapturedFeatures{Objects: proto.CloneOf(empty), Facets: empty}},
			FacetOwners: []*protocol.ComparisonIdentity{proto.CloneOf(subject)}}}}
	result := &protocol.ProjectedTableCreations{Complete: true, Tables: []*protocol.ProjectedTableCreation{{Subject: proto.CloneOf(subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: []string{"id"}}}}
	return description, request, result
}

func TestCreationClientDiscardsUntrustedReply(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimeCreationExchange()
	peer := &creationPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = proto.CloneOf(valid)
	peer.result.Tables[0].ColumnPrimaryKeys = []string{"foreign"}
	result, err = client.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider project table creations failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	request.Tables[0].FacetOwners = nil
	result, err = client.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestCreationServerPreservesEvidenceAndCancellation(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimeCreationExchange()
	original := proto.CloneOf(request)
	owner := &mutatingProjector{}
	server := &serverAdapter{server: Server{Description: description, TableCreations: owner}}
	result, err := server.ProjectTableCreations(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.ProjectTableCreations(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
	ctx, cancel = context.WithCancel(c.Context())
	defer cancel()
	owner.cancel = cancel
	result, err = server.ProjectTableCreations(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 2)
}

type creationPeer struct {
	protocol.ProviderClient
	result *protocol.ProjectedTableCreations
	err    error
	calls  int
}

func (p *creationPeer) ProjectTableCreations(context.Context, *protocol.ProjectTableCreationsRequest, ...grpc.CallOption) (*protocol.ProjectedTableCreations, error) {
	p.calls++
	return p.result, p.err
}

type mutatingProjector struct {
	calls  int
	cancel context.CancelFunc
}

func (p *mutatingProjector) ProjectTableCreations(_ context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	p.calls++
	request.Tables[0].Declaration.Columns[0].Name = "foreign"
	if p.cancel != nil {
		p.cancel()
	}
	return &protocol.ProjectedTableCreations{Complete: true, Tables: []*protocol.ProjectedTableCreation{{Subject: request.Tables[0].Subject, ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: []string{"foreign"}}}}, nil
}
