package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func contextInputs() (*protocol.Description, *protocol.NormalizeValuesRequest) {
	d, request := valueInputs()
	d.Features = append(d.Features, provider.ValueContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ValueContextFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.ValueContextFeature)
	return d, request
}

func databaseContext() *protocol.DatabaseContext {
	return &protocol.DatabaseContext{Database: "/database/realm", SourceDatabase: "/database"}
}

func TestValueContextPreservesAuthoredIdentityAndRoundTrips(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := contextInputs()
	owner := proto.CloneOf(request.Values[0].Value.Owner)
	request.Values[0].Value.Context = databaseContext()
	result := normalizedValues(request)
	c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(result.Values[0].Value.Owner, owner), qt.IsTrue)
	encoded, err := proto.Marshal(result)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.NormalizedValues{}
	c.Assert(proto.Unmarshal(encoded, decoded), qt.IsNil)
	c.Assert(provider.ValidateNormalizedValues(d, request, decoded), qt.IsNil)
	decoded.Values[0].Value.Context.Database = "/other"
	c.Assert(provider.ValidateNormalizedValues(d, request, decoded), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(request.Values[0].Value.Context.Database, qt.Equals, "/database/realm")
}

func TestValueContextRefusesChangedOrUnnegotiatedFacts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.NormalizeValuesRequest, *protocol.NormalizedValues)
	}{
		{"provider feature", func(d *protocol.Description, _ *protocol.NormalizeValuesRequest, _ *protocol.NormalizedValues) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ValueContextFeature })
		}},
		{"engine feature", func(d *protocol.Description, _ *protocol.NormalizeValuesRequest, _ *protocol.NormalizedValues) {
			d.Engines[0].Features = []string{"retention"}
		}},
		{"request feature", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest, _ *protocol.NormalizedValues) {
			r.TargetFeatures = []string{"retention"}
		}},
		{"missing source root", func(_ *protocol.Description, r *protocol.NormalizeValuesRequest, _ *protocol.NormalizedValues) {
			r.Values[0].Value.Context.SourceDatabase = ""
		}},
		{"changed selected root", func(_ *protocol.Description, _ *protocol.NormalizeValuesRequest, r *protocol.NormalizedValues) {
			r.Values[0].Value.Context.Database = "/other"
		}},
		{"changed source root", func(_ *protocol.Description, _ *protocol.NormalizeValuesRequest, r *protocol.NormalizedValues) {
			r.Values[0].Value.Context.SourceDatabase = "/other"
		}},
		{"removed context", func(_ *protocol.Description, _ *protocol.NormalizeValuesRequest, r *protocol.NormalizedValues) {
			r.Values[0].Value.Context = nil
		}},
		{"unresolved bound result", func(_ *protocol.Description, _ *protocol.NormalizeValuesRequest, r *protocol.NormalizedValues) {
			r.Values[0].RequiresDatabaseContext = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := contextInputs()
			request.Values[0].Value.Context = databaseContext()
			result := normalizedValues(request)
			c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.IsNil)
			test.mutate(d, request, result)
			c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestUnboundDependenciesAreExplicitAndCannotReachComparison(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := contextInputs()
	result := normalizedValues(request)
	result.Values[0].RequiresDatabaseContext = true
	c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.IsNil)
	result.Values[0].RequiredObjects = []*protocol.ComparisonIdentity{{Kind: "secret", Name: &protocol.ComparisonPart{Source: "one", Normalized: "one"}}}
	c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Values[0].RequiredObjects = nil
	request.TargetFeatures = []string{"retention"}
	c.Assert(provider.ValidateNormalizedValues(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	d, comparison, compared := comparisonExchange()
	d.Features = append(d.Features, provider.ValueContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ValueContextFeature)
	comparison.TargetFeatures = append(comparison.TargetFeatures, provider.ValueContextFeature)
	c.Assert(provider.ValidateComparisonRequest(d, comparison), qt.IsNil)
	comparison.Desired.Records[0].Value.RequiresDatabaseContext = true
	c.Assert(provider.ValidateComparisonRequest(d, comparison), qt.ErrorIs, provider.ErrProtocol)
	comparison.Desired.Records[0].Value.RequiresDatabaseContext = false
	compared.Desired.Records[0].Value.RequiresDatabaseContext = true
	c.Assert(provider.ValidateComparedModels(d, comparison, compared), qt.ErrorIs, provider.ErrProtocol)
}

func TestUnresolvedContextStopsEveryOperationConsumer(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, planning, _ := planningExchange()
	c.Assert(provider.ValidatePlanningRequest(d, planning), qt.IsNil)
	planning.Tables[0].Desired.Features.Facets.Records[0].Value.RequiresDatabaseContext = true
	c.Assert(provider.ValidatePlanningRequest(d, planning), qt.ErrorMatches, ".*dependency consumer requires resolved database context")
	d, declarations, _ := declarationsExchange()
	c.Assert(provider.ValidateDeclarationsRequest(d, declarations), qt.IsNil)
	declarations.Objects[0].Value.RequiresDatabaseContext = true
	c.Assert(provider.ValidateDeclarationsRequest(d, declarations), qt.ErrorMatches, ".*dependency consumer requires resolved database context")
	d, render := operationRender()
	c.Assert(provider.ValidateRenderRequest(d, render), qt.IsNil)
	render.Changes[0].GetNative().Value.RequiresDatabaseContext = true
	c.Assert(provider.ValidateRenderRequest(d, render), qt.ErrorMatches, ".*dependency consumer requires resolved database context")
}
