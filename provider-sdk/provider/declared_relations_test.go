package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// declaredRelationsExchange is an object comparison under negotiated declared
// relations, when listed is true: the provider, the engine and the request
// list the feature. The request names one declared relation of each kind
// given.
func declaredRelationsExchange(listed bool, kinds ...string) (*protocol.Description, *protocol.CompareModelsRequest) {
	d, request, _ := comparisonExchange()
	service := d.Engines[0].Comparisons[0]
	service.Placement, service.OwnerKinds = protocol.ModelComparison_OBJECTS, nil
	d.Features = append(d.Features, provider.DeclaredRelationsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.DeclaredRelationsFeature)
	if listed {
		request.TargetFeatures = append(request.TargetFeatures, provider.DeclaredRelationsFeature)
	}
	for _, kind := range kinds {
		request.DeclaredRelations = append(request.DeclaredRelations, &protocol.ComparisonIdentity{Kind: kind,
			Name: &protocol.ComparisonPart{Source: "orders", Normalized: "orders"}, Schema: &protocol.ComparisonPart{Source: "app", Normalized: "app"}})
	}
	return d, request
}

func TestDeclaredRelations_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		listed bool
		kinds  []string
	}{
		{"every relation under the feature", true, []string{"table", "view", "materialized view"}},
		{"views alone for a target that does not list the feature", false, []string{"view", "materialized view"}},
		{"no relation", true, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredRelationsExchange(test.listed, test.kinds...)

			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
		})
	}
}

// A table is a declared relation only under the feature, and no list carries
// another kind.
func TestDeclaredRelations_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		listed bool
		kind   string
		want   string
	}{
		{"a table for a target that does not list the feature", false, "table", `.*a declared relation of kind "table" is not one this comparison carries`},
		{"an index under the feature", true, "index", `.*a declared relation of kind "index" is not one this comparison carries`},
		{"a provider model's kind", true, "example.org/fixture/topic", `.*a declared relation of kind "example\.org/fixture/topic" is not one this comparison carries`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredRelationsExchange(test.listed, test.kind)

			err := provider.ValidateComparisonRequest(d, request)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the feature sees an engine without it, so a table
// it is sent is refused; and the engine's feature needs the provider's and a
// model comparison.
func TestDeclaredRelationsNegotiation_FailurePath(t *testing.T) {
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request := declaredRelationsExchange(false, "table")
		negotiated := provider.NegotiatedDescription(d, slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
			return feature == provider.DeclaredRelationsFeature
		}))

		c.Assert(provider.ValidateComparisonRequest(negotiated, request), qt.ErrorMatches, `.*a declared relation of kind "table" is not one this comparison carries`)
	})
	t.Run("an engine feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d, _ := declaredRelationsExchange(true)
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.DeclaredRelationsFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*declared relations require the provider's feature and a model comparison`)
	})
	t.Run("an engine without a model comparison", func(t *testing.T) {
		c := qt.New(t)
		d, _ := declaredRelationsExchange(true)
		d.Engines[0].Comparisons = nil

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*declared relations require the provider's feature and a model comparison`)
	})
}
