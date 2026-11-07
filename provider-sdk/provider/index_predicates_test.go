package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// predicatePreparation is a preparation negotiated with index predicates,
// whose first declared index is partial, and a reply that states its
// predicate as the engine holds it.
func predicatePreparation() (*protocol.Description, *protocol.PrepareTablesRequest, *protocol.PreparedTables) {
	d, request, result := indexMethodsExchange()
	d.Features = append(d.Features, provider.IndexPredicatesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.IndexPredicatesFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.IndexPredicatesFeature)
	request.Tables[0].Desired.Indexes[0].Condition = " id > 0 "
	result.Tables[0].Indexes[0].Condition = "(id > '0'::bigint)"
	return d, request, result
}

// predicateInspection is a read negotiated with index predicates whose second
// index is partial.
func predicateInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := indexInspection()
	for _, feature := range []string{provider.PrepareTablesFeature, provider.NormalizeValuesFeature, provider.IndexPredicatesFeature} {
		if !slices.Contains(d.Features, feature) {
			d.Features = append(d.Features, feature)
		}
	}
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PrepareTablesFeature, provider.IndexPredicatesFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.IndexPredicatesFeature)
	snapshot.Tables[0].Indexes[1].Condition = "(label IS NOT NULL)"
	return d, request, snapshot
}

func TestIndexPredicates_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a prepared index states its predicate as the engine holds it", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := predicatePreparation()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
	})
	t.Run("without index methods an entry states the predicate and no kind", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := predicatePreparation()
		d.Features = withoutIndexMethods(d.Features)
		d.Engines[0].Features = withoutIndexMethods(d.Engines[0].Features)
		request.TargetFeatures = withoutIndexMethods(request.TargetFeatures)
		result.Tables[0].Indexes[0].Method = ""
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
	})
	t.Run("a negotiated read states a held predicate", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := predicateInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a read without the feature is checked as before", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := predicateInspection()
		snapshot.TargetFeatures = withoutFeature(snapshot.TargetFeatures, provider.IndexPredicatesFeature)
		snapshot.Tables[0].Indexes[1].Condition = " label IS NOT NULL "
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := predicatePreparation()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.IndexPredicatesFeature))
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.IndexPredicatesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.IndexPredicatesFeature)
	})
}

// A negotiated preparation states a predicate exactly for a partial index, in
// a trimmed form, and one without the feature states none.
func TestPreparedIndexPredicates_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.PrepareTablesRequest, *protocol.PreparedTable)
	}{
		{"a partial index without its predicate", `.*states a predicate for index "by_label" exactly when the declaration states one`,
			func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) { p.Indexes[0].Condition = "" }},
		{"a predicate for an index without one", `.*states a predicate for index "by_id" exactly when the declaration states one`,
			func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) { p.Indexes[1].Condition = "id > 0" }},
		{"surrounding white space", `.*states the predicate of index "by_label" with surrounding white space`,
			func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) { p.Indexes[0].Condition = "id > 0 " }},
		{"a predicate the host did not ask for", `.*prepared index predicates need schema\.index-predicates`,
			func(r *protocol.PrepareTablesRequest, _ *protocol.PreparedTable) {
				r.TargetFeatures = withoutFeature(r.TargetFeatures, provider.IndexPredicatesFeature)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := predicatePreparation()
			test.mutate(request, result.Tables[0])
			err := provider.ValidatePreparedTables(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestInspectedIndexPredicates_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := predicateInspection()
	snapshot.Tables[0].Indexes[1].Condition = " (label IS NOT NULL)"
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*observed index "items_label_unique" states its predicate with surrounding white space`)
}

func TestIndexPredicatesDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.IndexPredicatesFeature)
		}},
		{"without observed indexes", func(d *protocol.Description) {
			d.Engines[0].Features = withoutIndexMethods(withoutFeature(d.Engines[0].Features, provider.ObservedIndexesFeature))
		}},
		{"without table preparation", func(d *protocol.Description) {
			d.Engines[0].Features = withoutIndexMethods(withoutFeature(d.Engines[0].Features, provider.PrepareTablesFeature))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := predicatePreparation()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*index predicates require the provider's feature, observed indexes and table preparation`)
		})
	}
}
