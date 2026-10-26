package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func defaultExchange() (*protocol.Description, *protocol.ReadColumnDefaultsRequest, *protocol.ReadColumnDefaultsResult) {
	d, comparison, _ := columnExchange()
	pair, table := comparison.Tables[0].Pairs[0], comparison.Tables[0]
	request := &protocol.ReadColumnDefaultsRequest{Engine: comparison.Engine, TargetFeatures: comparison.TargetFeatures, Identifiers: comparison.Identifiers,
		Capabilities: comparison.Capabilities, Columns: []*protocol.ObservedColumnDefaultInput{{Subject: proto.CloneOf(pair.CurrentSubject), Column: proto.CloneOf(table.Current.Columns[0])}}}
	result := &protocol.ReadColumnDefaultsResult{Complete: true, Columns: []*protocol.ColumnDefaultReceipt{{Subject: proto.CloneOf(pair.CurrentSubject), Kind: protocol.ColumnDefaultReceipt_ABSENT}}}
	return d, request, result
}

func TestColumnDefaultReadbackPreservesAbsentLiteralAndExpression(t *testing.T) {
	for _, row := range []struct {
		name  string
		input *string
		kind  protocol.ColumnDefaultReceipt_Kind
		value string
	}{
		{"absent", nil, protocol.ColumnDefaultReceipt_ABSENT, ""},
		{"literal zero", proto.String("0t"), protocol.ColumnDefaultReceipt_LITERAL, "0"},
		{"empty literal", proto.String("''"), protocol.ColumnDefaultReceipt_LITERAL, ""},
		{"expression", proto.String("Now()"), protocol.ColumnDefaultReceipt_EXPRESSION, "Now()"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := defaultExchange()
			request.Columns[0].Column.DefaultExpression = row.input
			result.Columns[0].Kind, result.Columns[0].Value = row.kind, row.value
			before := proto.CloneOf(request)
			c.Assert(provider.ValidateColumnDefaults(d, request, result), qt.IsNil)
			c.Assert(proto.Equal(before, request), qt.IsTrue)
		})
	}
}

func TestColumnDefaultReadbackRejectsMissingAndInventedFacts(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*protocol.ReadColumnDefaultsRequest, *protocol.ReadColumnDefaultsResult)
	}{
		{"missing column", func(r *protocol.ReadColumnDefaultsRequest, _ *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Column = nil
		}},
		{"mismatched name", func(r *protocol.ReadColumnDefaultsRequest, _ *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Column.Name = "other"
		}},
		{"missing parent", func(r *protocol.ReadColumnDefaultsRequest, _ *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Subject.Parent = nil
		}},
		{"duplicate input", func(r *protocol.ReadColumnDefaultsRequest, _ *protocol.ReadColumnDefaultsResult) {
			r.Columns = append(r.Columns, proto.CloneOf(r.Columns[0]))
		}},
		{"missing receipt", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) { r.Columns = nil }},
		{"unknown kind", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Kind = 99
		}},
		{"absent with value", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Value = "5"
		}},
		{"absent with expression", func(r *protocol.ReadColumnDefaultsRequest, _ *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Column.DefaultExpression = proto.String("5t")
		}},
		{"invented literal", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Kind = protocol.ColumnDefaultReceipt_LITERAL
		}},
		{"empty expression", func(r *protocol.ReadColumnDefaultsRequest, out *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Column.DefaultExpression = proto.String("Now()")
			out.Columns[0].Kind = protocol.ColumnDefaultReceipt_EXPRESSION
		}},
		{"expression without input", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Kind, r.Columns[0].Value = protocol.ColumnDefaultReceipt_EXPRESSION, "Now()"
		}},
		{"changed subject", func(_ *protocol.ReadColumnDefaultsRequest, r *protocol.ReadColumnDefaultsResult) {
			r.Columns[0].Subject.Name.Normalized = "other"
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := defaultExchange()
			c.Assert(provider.ValidateColumnDefaults(d, request, result), qt.IsNil)
			row.mutate(request, result)
			c.Assert(provider.ValidateColumnDefaults(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestCreationColumnsRequireCompleteNegotiatedPredictions(t *testing.T) {
	c := qt.New(t)
	d, request, result := creationExchange()
	result.Tables[0].Columns = []*protocol.ObservedColumn{{Name: "id", DataType: "INT64", IsNullable: "NO", DefaultExpression: proto.String("0t")}}
	c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	d.Features = append(d.Features, provider.ColumnsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ColumnsFeature)
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(provider.ValidateProjectedCreations(unlisted, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestCreationColumnsRejectIncompleteOrReorderedPredictions(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*protocol.ProjectedTableCreation)
	}{
		{"missing", func(r *protocol.ProjectedTableCreation) { r.Columns = nil }},
		{"extra", func(r *protocol.ProjectedTableCreation) { r.Columns = append(r.Columns, proto.CloneOf(r.Columns[0])) }},
		{"different name", func(r *protocol.ProjectedTableCreation) { r.Columns[0].Name = "other" }},
		{"unknown nullability", func(r *protocol.ProjectedTableCreation) { r.Columns[0].IsNullable = "" }},
		{"missing type", func(r *protocol.ProjectedTableCreation) { r.Columns[0].DataType = "" }},
		{"nil", func(r *protocol.ProjectedTableCreation) { r.Columns[0] = nil }},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := creationExchange()
			d.Features = append(d.Features, provider.ColumnsFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.ColumnsFeature)
			result.Tables[0].Columns = []*protocol.ObservedColumn{{Name: "id", DataType: "INT64", IsNullable: "NO"}}
			c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.IsNil)
			row.mutate(result.Tables[0])
			c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
