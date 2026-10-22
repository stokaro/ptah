package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func preparationExchange() (*protocol.Description, *protocol.PrepareTablesRequest, *protocol.PreparedTables) {
	description, planning, _ := planningExchange()
	description.Features = append(description.Features, provider.PrepareTablesFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.PrepareTablesFeature)
	table := planning.Tables[0]
	request := &protocol.PrepareTablesRequest{
		Engine: planning.Engine, TargetFeatures: planning.TargetFeatures,
		Identifiers: planning.Identifiers, Capabilities: planning.Capabilities,
		Tables: []*protocol.TablePreparationInput{{Subject: table.Subject, Desired: table.Desired, Current: table.Current,
			CurrentKnowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
	}
	result := &protocol.PreparedTables{Complete: true, Tables: []*protocol.PreparedTable{{
		Subject: proto.CloneOf(table.Subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: []string{"id"},
		ResolvedFacets: []*protocol.ComparisonRecord{proto.CloneOf(table.Desired.Features.Facets.Records[0])},
	}}}
	return description, request, result
}

func TestPreparationRetainsCapturedEvidence(t *testing.T) {
	c := qt.New(t)
	description, request, result := preparationExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidatePreparedTables(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, before), qt.IsTrue)
	request.Tables[0].Current = nil
	request.Tables[0].CurrentKnowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "No catalog read"}
	c.Assert(provider.ValidatePreparedTables(description, request, result), qt.IsNil)
	request.Tables[0].CurrentKnowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_ABSENT}
	c.Assert(provider.ValidatePreparedTables(description, request, result), qt.IsNil)
	request.Identifiers.DefaultSchema = "a.b"
	request.Tables[0].Desired.Schema = ""
	request.Tables[0].Subject.Schema.Defaulted = true
	result.Tables[0].Subject.Schema.Defaulted = true
	c.Assert(provider.ValidatePreparedTables(description, request, result), qt.IsNil)
	request.Tables = nil
	result.Tables = nil
	c.Assert(provider.ValidatePreparedTables(description, request, result), qt.IsNil)
}

func TestPreparationRefusesUnboundInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PrepareTablesRequest)
	}{
		{"unadvertised service", func(d *protocol.Description, _ *protocol.PrepareTablesRequest) { d.Features = nil }},
		{"unadvertised engine", func(d *protocol.Description, _ *protocol.PrepareTablesRequest) {
			d.Engines[0].Features = []string{"retention"}
		}},
		{"missing identifiers", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Identifiers = nil }},
		{"duplicate parent", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
		{"nil parent", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0] = nil }},
		{"missing declaration", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0].Desired = nil }},
		{"foreign source", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0].Desired.Name = "other" }},
		{"missing existence evidence", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0].CurrentKnowledge = nil }},
		{"missing current", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0].Current = nil }},
		{"absent with capture", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.Tables[0].CurrentKnowledge.State = protocol.ModelKnowledge_ABSENT
		}},
		{"unknown with capture", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.Tables[0].CurrentKnowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "No read"}
		}},
		{"unknown without reason", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.Tables[0].Current = nil
			r.Tables[0].CurrentKnowledge.State = protocol.ModelKnowledge_UNINSPECTED
		}},
		{"defaulted existence", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.Tables[0].CurrentKnowledge.State = protocol.ModelKnowledge_DEFAULTED
		}},
		{"missing feature capture", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) { r.Tables[0].Desired.Features = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := preparationExchange()
			c.Assert(provider.ValidatePreparationRequest(d, request), qt.IsNil)
			test.mutate(d, request)
			c.Assert(provider.ValidatePreparationRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPreparationRefusesForgedResults(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.PrepareTablesRequest, *protocol.PreparedTables)
	}{
		{"incomplete", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) { r.Complete = false }},
		{"missing table", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) { r.Tables = nil }},
		{"nil table", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) { r.Tables[0] = nil }},
		{"foreign identity", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].Subject.Name.Source = "other"
		}},
		{"undeclared key", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ColumnPrimaryKeys = []string{"other"}
		}},
		{"duplicate key", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ColumnPrimaryKeys = []string{"id", "id"}
		}},
		{"missing key receipt", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ColumnPrimaryKeysPrepared = false
		}},
		{"changed owner", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets[0].Value.Value.Owner.Name = "foreign"
		}},
		{"changed normalized owner", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets[0].Value.NormalizedOwner.Name = "foreign"
		}},
		{"missing value", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets[0].Value = nil
		}},
		{"target override", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets[0].Targets = []string{"fixture"}
		}},
		{"new kind", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets[0].Kind = "other"
		}},
		{"excluded kind", func(q *protocol.PrepareTablesRequest, _ *protocol.PreparedTables) {
			original := q.Tables[0].Desired.Features.Facets.Records[0]
			original.Value = nil
			original.Targets = []string{"other"}
		}},
		{"duplicate facet", func(_ *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			r.Tables[0].ResolvedFacets = append(r.Tables[0].ResolvedFacets, proto.CloneOf(r.Tables[0].ResolvedFacets[0]))
		}},
		{"column adjustment", func(q *protocol.PrepareTablesRequest, r *protocol.PreparedTables) {
			q.Tables[0].Desired.Features.Facets.Records[0].Subject.Kind = "column"
			r.Tables[0].ResolvedFacets[0].Subject.Kind = "column"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := preparationExchange()
			c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
			test.mutate(request, result)
			c.Assert(provider.ValidatePreparedTables(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
