package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func creationExchange() (*protocol.Description, *protocol.ProjectTableCreationsRequest, *protocol.ProjectedTableCreations) {
	description, preparation, prepared := preparationExchange()
	description.Features = append(description.Features, provider.ProjectTableCreationsFeature, provider.ConvertValuesFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.ProjectTableCreationsFeature)
	table, output := preparation.Tables[0], prepared.Tables[0]
	request := &protocol.ProjectTableCreationsRequest{Engine: preparation.Engine, Identifiers: preparation.Identifiers,
		Capabilities: preparation.Capabilities, TargetFeatures: preparation.TargetFeatures,
		Tables: []*protocol.TableCreationInput{{Subject: table.Subject, Declaration: table.Desired, FacetOwners: []*protocol.ComparisonIdentity{proto.CloneOf(table.Subject)}}}}
	result := &protocol.ProjectedTableCreations{Complete: true, Tables: []*protocol.ProjectedTableCreation{{
		Subject: output.Subject, ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: output.ColumnPrimaryKeys,
		DesiredFacets: output.ResolvedFacets, ObservedFacets: []*protocol.ComparisonRecord{proto.CloneOf(output.ResolvedFacets[0])},
	}}}
	return description, request, result
}

func TestCreationPredictionRetainsDeclarationsAndAllowsComputedKinds(t *testing.T) {
	c := qt.New(t)
	description, request, result := creationExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidateProjectedCreations(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, before), qt.IsTrue)
	request.Tables[0].Declaration.Features.Facets.Records = nil
	c.Assert(provider.ValidateProjectedCreations(description, request, result), qt.IsNil)
	request.Tables = nil
	result.Tables = nil
	c.Assert(provider.ValidateProjectedCreations(description, request, result), qt.IsNil)
}

func TestCreationPredictionRefusesIncompleteContext(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ProjectTableCreationsRequest)
	}{
		{"unadvertised service", func(d *protocol.Description, _ *protocol.ProjectTableCreationsRequest) { d.Features = nil }},
		{"unadvertised engine", func(d *protocol.Description, _ *protocol.ProjectTableCreationsRequest) {
			d.Engines[0].Features = []string{"retention"}
		}},
		{"missing identifiers", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) { r.Identifiers = nil }},
		{"nil table", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) { r.Tables[0] = nil }},
		{"missing declaration", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) { r.Tables[0].Declaration = nil }},
		{"different table", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			r.Tables[0].Declaration.Name = "other"
		}},
		{"missing inventory", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) { r.Tables[0].FacetOwners = nil }},
		{"duplicate inventory", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			r.Tables[0].FacetOwners = append(r.Tables[0].FacetOwners, proto.CloneOf(r.Tables[0].FacetOwners[0]))
		}},
		{"foreign table in inventory", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			owner := proto.CloneOf(r.Tables[0].Subject)
			owner.Name.Source, owner.Name.Normalized = "other", "other"
			r.Tables[0].FacetOwners = append(r.Tables[0].FacetOwners, owner)
		}},
		{"column in inventory", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			owner := proto.CloneOf(r.Tables[0].Subject)
			owner.Kind = "column"
			r.Tables[0].FacetOwners = append(r.Tables[0].FacetOwners, owner)
		}},
		{"declared owner omitted", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			r.Tables[0].Declaration.Features.Facets.Records[0].Subject.Name.Source = "other"
		}},
		{"duplicate table", func(_ *protocol.Description, r *protocol.ProjectTableCreationsRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := creationExchange()
			c.Assert(provider.ValidateCreationRequest(d, request), qt.IsNil)
			test.mutate(d, request)
			c.Assert(provider.ValidateCreationRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestCreationPredictionRefusesForgedEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ProjectTableCreationsRequest, *protocol.ProjectedTableCreations)
	}{
		{"incomplete", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Complete = false
		}},
		{"missing table", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) { r.Tables = nil }},
		{"nil table", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) { r.Tables[0] = nil }},
		{"changed identity", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].Subject.Name.Source = "other"
		}},
		{"unknown key", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].ColumnPrimaryKeys = []string{"other"}
		}},
		{"unprepared keys", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].ColumnPrimaryKeysPrepared = false
		}},
		{"unbound owner", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].DesiredFacets[0].Subject.Name.Source = "other"
		}},
		{"binding override", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].DesiredFacets[0].Targets = []string{"fixture"}
		}},
		{"missing desired value", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].DesiredFacets[0].Value = nil
		}},
		{"missing observed value", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].ObservedFacets[0].Value = nil
		}},
		{"duplicate facet", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].DesiredFacets = append(r.Tables[0].DesiredFacets, proto.CloneOf(r.Tables[0].DesiredFacets[0]))
		}},
		{"changed source owner", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].DesiredFacets[0].Value.Value.Owner.Name = "other"
		}},
		{"changed observed owner", func(_ *protocol.ProjectTableCreationsRequest, r *protocol.ProjectedTableCreations) {
			r.Tables[0].ObservedFacets[0].Value.NormalizedOwner.Name = "other"
		}},
		{"restored exclusion", func(q *protocol.ProjectTableCreationsRequest, _ *protocol.ProjectedTableCreations) {
			original := q.Tables[0].Declaration.Features.Facets.Records[0]
			original.Value = nil
			original.Targets = []string{"other"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := creationExchange()
			c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.IsNil)
			test.mutate(request, result)
			c.Assert(provider.ValidateCreationRequest(d, request), qt.IsNil)
			c.Assert(provider.ValidateProjectedCreations(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestCreationObservationsRequireTargetConversion(t *testing.T) {
	c := qt.New(t)
	description, request, result := creationExchange()
	description.Features = slices.DeleteFunc(description.Features, func(feature string) bool { return feature == provider.ConvertValuesFeature })
	c.Assert(provider.ValidateCreationRequest(description, request), qt.IsNil)
	c.Assert(provider.ValidateProjectedCreations(description, request, result), qt.ErrorIs, provider.ErrProtocol)
	description.Features = append(description.Features, provider.ConvertValuesFeature)
	model := proto.CloneOf(description.Engines[0].ValueModels[1])
	model.Kind = "example.org/fixture/observed-only"
	description.Engines[0].ValueModels = append(description.Engines[0].ValueModels, model)
	result.Tables[0].ObservedFacets[0].Kind = model.Kind
	result.Tables[0].ObservedFacets[0].Value.Value.Kind = model.Kind
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidateCreationRequest(description, request), qt.IsNil)
	c.Assert(provider.ValidateProjectedCreations(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}
