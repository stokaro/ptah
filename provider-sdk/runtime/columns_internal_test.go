package runtime

// White-box testing required: malformed peers and mutating owners must reach
// both validation boundaries without first being rejected by a public server.

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

func runtimeColumnsExchange() (*protocol.Description, *protocol.CompareColumnsRequest, *protocol.ComparedColumns) {
	d, creation, _ := runtimeCreationExchange()
	d.Features = append(d.Features, provider.ColumnsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ColumnsFeature)
	table := creation.Tables[0]
	column := proto.CloneOf(table.Subject)
	column.Kind, column.Parent, column.Name = "column", proto.CloneOf(table.Subject.Name), &protocol.ComparisonPart{Source: "id", Normalized: "id"}
	observed := proto.CloneOf(table.Declaration.Features)
	observed.Objects.Representation, observed.Facets.Representation = protocol.ValueInput_OBSERVED, protocol.ValueInput_OBSERVED
	request := &protocol.CompareColumnsRequest{Engine: "test", Identifiers: creation.Identifiers,
		Tables: []*protocol.ColumnComparisonTable{{Subject: table.Subject, CurrentSubject: proto.CloneOf(table.Subject), Desired: table.Declaration,
			Current: &protocol.ObservedTable{Name: "items", Columns: []*protocol.ObservedColumn{{Name: "id", DataType: "INTEGER", IsNullable: "NO"}}, Features: observed},
			Pairs:   []*protocol.ColumnComparisonPair{{Subject: column, CurrentSubject: proto.CloneOf(column), DesiredColumn: "id", CurrentColumn: "id", Aspects: []protocol.ColumnAspect{protocol.ColumnAspect_COLUMN_NULLABILITY}}}}}}
	result := &protocol.ComparedColumns{Complete: true, Tables: []*protocol.ComparedColumnTable{{Subject: proto.CloneOf(table.Subject), CurrentSubject: proto.CloneOf(table.Subject),
		Columns: []*protocol.ComparedColumn{{Subject: proto.CloneOf(column), CurrentSubject: proto.CloneOf(column), Aspects: []*protocol.ColumnAspectReceipt{{Aspect: protocol.ColumnAspect_COLUMN_NULLABILITY,
			Receipt: &protocol.ColumnAspectReceipt_Nullability{Nullability: &protocol.ColumnNullability{Nullable: proto.Bool(false)}}}}}}}}}
	return d, request, result
}

func runtimeDefaults(request *protocol.CompareColumnsRequest) (*protocol.ReadColumnDefaultsRequest, *protocol.ReadColumnDefaultsResult) {
	subject := request.Tables[0].Pairs[0].CurrentSubject
	input := &protocol.ReadColumnDefaultsRequest{Engine: request.Engine, Identifiers: request.Identifiers,
		Columns: []*protocol.ObservedColumnDefaultInput{{Subject: proto.CloneOf(subject), Column: proto.CloneOf(request.Tables[0].Current.Columns[0])}}}
	output := &protocol.ReadColumnDefaultsResult{Complete: true, Columns: []*protocol.ColumnDefaultReceipt{{Subject: proto.CloneOf(subject), Kind: protocol.ColumnDefaultReceipt_ABSENT}}}
	return input, output
}

type columnsPeer struct {
	protocol.ProviderClient
	comparison *protocol.ComparedColumns
	defaults   *protocol.ReadColumnDefaultsResult
	err        error
	calls      int
}

func (p *columnsPeer) CompareColumns(context.Context, *protocol.CompareColumnsRequest, ...grpc.CallOption) (*protocol.ComparedColumns, error) {
	p.calls++
	return proto.CloneOf(p.comparison), p.err
}
func (p *columnsPeer) ReadColumnDefaults(context.Context, *protocol.ReadColumnDefaultsRequest, ...grpc.CallOption) (*protocol.ReadColumnDefaultsResult, error) {
	p.calls++
	return proto.CloneOf(p.defaults), p.err
}

type columnOwner struct {
	comparison *protocol.ComparedColumns
	defaults   *protocol.ReadColumnDefaultsResult
	cancel     context.CancelFunc
	mutate     bool
	calls      int
}

func (p *columnOwner) CompareColumns(_ context.Context, request *protocol.CompareColumnsRequest) (*protocol.ComparedColumns, error) {
	p.calls++
	result := proto.CloneOf(p.comparison)
	if p.cancel != nil {
		p.cancel()
	}
	if p.mutate {
		request.Tables[0].Pairs[0].Subject.Name.Source = "other"
		result.Tables[0].Columns[0].Subject = request.Tables[0].Pairs[0].Subject
	}
	return result, nil
}
func (p *columnOwner) ReadColumnDefaults(_ context.Context, request *protocol.ReadColumnDefaultsRequest) (*protocol.ReadColumnDefaultsResult, error) {
	p.calls++
	result := proto.CloneOf(p.defaults)
	if p.cancel != nil {
		p.cancel()
	}
	if p.mutate {
		request.Columns[0].Subject.Name.Source = "other"
		result.Columns[0].Subject = request.Columns[0].Subject
	}
	return result, nil
}

func TestColumnClientsDiscardPartialAndTransportFailures(t *testing.T) {
	c := qt.New(t)
	d, request, valid := runtimeColumnsExchange()
	defaults, validDefaults := runtimeDefaults(request)
	peer := &columnsPeer{comparison: valid, defaults: validDefaults}
	client := &Client{description: d, rpc: peer}
	result, err := client.CompareColumns(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	read, err := client.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(read, validDefaults), qt.IsTrue)
	peer.comparison.Complete, peer.defaults.Complete = false, false
	result, err = client.CompareColumns(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	read, err = client.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(read, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "private value")
	result, err = client.CompareColumns(c.Context(), request)
	c.Assert(err.Error(), qt.Not(qt.Contains), "private value")
	c.Assert(result, qt.IsNil)
	read, err = client.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(err.Error(), qt.Not(qt.Contains), "private value")
	c.Assert(read, qt.IsNil)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = client.CompareColumns(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	read, err = client.ReadColumnDefaults(ctx, defaults)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(read, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 6)
}

func TestColumnServersRetainInputsAndHonorCancellation(t *testing.T) {
	c := qt.New(t)
	d, request, valid := runtimeColumnsExchange()
	defaults, validDefaults := runtimeDefaults(request)
	before, beforeDefaults := proto.CloneOf(request), proto.CloneOf(defaults)
	owner := &columnOwner{comparison: valid, defaults: validDefaults, mutate: true}
	server := negotiatedServer(Server{Description: d, Columns: owner}, provider.ColumnsFeature)
	result, err := server.CompareColumns(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	read, err := server.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(read, qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(proto.Equal(beforeDefaults, defaults), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	t.Cleanup(cancel)
	owner.mutate, owner.cancel = false, cancel
	result, err = server.CompareColumns(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	read, err = server.ReadColumnDefaults(ctx, defaults)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(read, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 3)
	ctx, cancel = context.WithCancel(c.Context())
	t.Cleanup(cancel)
	owner.cancel = cancel
	read, err = server.ReadColumnDefaults(ctx, defaults)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(read, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 4)
}

func TestColumnServerRefusesWithoutNegotiationOrImplementation(t *testing.T) {
	c := qt.New(t)
	d, request, valid := runtimeColumnsExchange()
	defaults, validDefaults := runtimeDefaults(request)
	owner := &columnOwner{comparison: valid, defaults: validDefaults}
	server := negotiatedServer(Server{Description: d, Columns: owner})
	result, err := server.CompareColumns(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(result, qt.IsNil)
	read, err := server.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(read, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
	server.server.Columns = nil
	result, err = server.CompareColumns(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	c.Assert(result, qt.IsNil)
	read, err = server.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	c.Assert(read, qt.IsNil)
}
