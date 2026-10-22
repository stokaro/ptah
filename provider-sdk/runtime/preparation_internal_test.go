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

func runtimePreparationExchange() (*protocol.Description, *protocol.PrepareTablesRequest, *protocol.PreparedTables) {
	description, _, _ := normalizationExchange()
	description.Features = append(description.Features, provider.PrepareTablesFeature)
	description.Engines[0].Features = []string{provider.PrepareTablesFeature}
	subject := &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "items", Normalized: "items"}}
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	empty := &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}}
	request := &protocol.PrepareTablesRequest{Engine: "test", Identifiers: &protocol.ComparisonIdentifiers{Rules: rules},
		Tables: []*protocol.TablePreparationInput{{Subject: subject,
			Desired: &protocol.DeclaredTable{Name: "items", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "INTEGER"}},
				Features: &protocol.CapturedFeatures{Objects: proto.CloneOf(empty), Facets: empty}},
			CurrentKnowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_ABSENT}}}}
	result := &protocol.PreparedTables{Complete: true, Tables: []*protocol.PreparedTable{{Subject: proto.CloneOf(subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: []string{"id"}}}}
	return description, request, result
}

func TestPreparationClientDiscardsUntrustedReply(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimePreparationExchange()
	peer := &preparationPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.PrepareTables(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.PrepareTables(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = proto.CloneOf(valid)
	peer.result.Tables[0].ColumnPrimaryKeys = []string{"foreign"}
	result, err = client.PrepareTables(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.PrepareTables(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider prepare tables failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	request.Tables[0].CurrentKnowledge = nil
	result, err = client.PrepareTables(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestPreparationServerPreservesEvidenceAndCancellation(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimePreparationExchange()
	original := proto.CloneOf(request)
	owner := &mutatingPreparer{}
	server := &serverAdapter{server: Server{Description: description, TablePreparer: owner}}
	result, err := server.PrepareTables(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.PrepareTables(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
	ctx, cancel = context.WithCancel(c.Context())
	defer cancel()
	owner.cancel = cancel
	result, err = server.PrepareTables(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 2)
}

type preparationPeer struct {
	protocol.ProviderClient
	result *protocol.PreparedTables
	err    error
	calls  int
}

func (p *preparationPeer) PrepareTables(context.Context, *protocol.PrepareTablesRequest, ...grpc.CallOption) (*protocol.PreparedTables, error) {
	p.calls++
	return p.result, p.err
}

type mutatingPreparer struct {
	calls  int
	cancel context.CancelFunc
}

func (p *mutatingPreparer) PrepareTables(_ context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	p.calls++
	request.Tables[0].Desired.Columns[0].Name = "foreign"
	if p.cancel != nil {
		p.cancel()
	}
	return &protocol.PreparedTables{Complete: true, Tables: []*protocol.PreparedTable{{Subject: request.Tables[0].Subject, ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: []string{"foreign"}}}}, nil
}
