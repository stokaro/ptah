package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// indexMethodsExchange is a preparation whose table declares two common
// indexes, negotiated with index methods, and a reply that states the kind of
// each in the declared order.
func indexMethodsExchange() (*protocol.Description, *protocol.PrepareTablesRequest, *protocol.PreparedTables) {
	d, request, result := preparationExchange()
	features := []string{provider.InspectFeature, provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.IndexMethodsFeature}
	for _, feature := range features {
		if !slices.Contains(d.Features, feature) {
			d.Features = append(d.Features, feature)
		}
	}
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.IndexMethodsFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.IndexMethodsFeature)
	request.Tables[0].Desired.Indexes = []*protocol.DeclaredIndex{
		{Name: "by_label", Fields: []string{"id"}, Type: "GLOBAL ASYNC"},
		{Name: "by_id", Fields: []string{"id"}},
	}
	result.Tables[0].Indexes = []*protocol.PreparedIndex{{Name: "by_label", Method: "async"}, {Name: "by_id"}}
	return d, request, result
}

func withoutIndexMethods(features []string) []string {
	return slices.DeleteFunc(slices.Clone(features), func(feature string) bool { return feature == provider.IndexMethodsFeature })
}

func TestIndexMethods_HappyPath(t *testing.T) {
	t.Run("a negotiated reply states the kind of each declared index", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := indexMethodsExchange()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
		data, err := proto.Marshal(result)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.PreparedTables{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, decoded), qt.IsNil)
		c.Assert(proto.Equal(decoded, result), qt.IsTrue)
	})
	t.Run("a table that declares no index states none", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := indexMethodsExchange()
		request.Tables[0].Desired.Indexes = nil
		result.Tables[0].Indexes = nil
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := indexMethodsExchange()
		negotiated := provider.NegotiatedDescription(d, withoutIndexMethods(provider.TwoWayFeatures()))
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.IndexMethodsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.IndexMethodsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedIndexesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// Index methods the exchange did not negotiate are refused, and the same
// reply without them is accepted.
func TestIndexMethodsNegotiation_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PrepareTablesRequest)
	}{
		{"the provider did not list the feature", func(d *protocol.Description, _ *protocol.PrepareTablesRequest) {
			d.Features = withoutIndexMethods(d.Features)
		}},
		{"the engine did not list the feature", func(d *protocol.Description, r *protocol.PrepareTablesRequest) {
			// A request lists only target features its engine offers.
			d.Engines[0].Features = withoutIndexMethods(d.Engines[0].Features)
			r.TargetFeatures = withoutIndexMethods(r.TargetFeatures)
		}},
		{"the request did not list the feature", func(_ *protocol.Description, r *protocol.PrepareTablesRequest) {
			r.TargetFeatures = withoutIndexMethods(r.TargetFeatures)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := indexMethodsExchange()
			test.mutate(d, request)
			c.Assert(provider.ValidatePreparedTables(d, request, result), qt.ErrorMatches, `.*prepared index methods need schema\.index-methods`)
			c.Assert(provider.ValidatePreparedTables(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			result.Tables[0].Indexes = nil
			c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
		})
	}
}

// A negotiated reply states one kind for each declared index, in the declared
// order, and nothing else.
func TestIndexMethodsList_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.PreparedTable)
	}{
		{"none stated", `.*states 0 index methods for 2 declared indexes`, func(p *protocol.PreparedTable) { p.Indexes = nil }},
		{"one missing", `.*states 1 index methods for 2 declared indexes`, func(p *protocol.PreparedTable) { p.Indexes = p.Indexes[:1] }},
		{"one extra", `.*states 3 index methods for 2 declared indexes`, func(p *protocol.PreparedTable) {
			p.Indexes = append(p.Indexes, &protocol.PreparedIndex{Name: "by_other"})
		}},
		{"an undeclared index", `.*prepared index method 1 names "by_other", and the declared index there is "by_id"`, func(p *protocol.PreparedTable) {
			p.Indexes[1].Name = "by_other"
		}},
		{"an index stated twice", `.*prepared index method 1 names "by_label", and the declared index there is "by_id"`, func(p *protocol.PreparedTable) {
			p.Indexes[1].Name = "by_label"
		}},
		{"another order", `.*prepared index method 0 names "by_id", and the declared index there is "by_label"`, func(p *protocol.PreparedTable) {
			p.Indexes[0], p.Indexes[1] = p.Indexes[1], p.Indexes[0]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := indexMethodsExchange()
			test.mutate(result.Tables[0])
			err := provider.ValidatePreparedTables(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestIndexMethodsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"the provider did not list the feature", func(d *protocol.Description) { d.Features = withoutIndexMethods(d.Features) }},
		{"the engine does not observe indexes", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.ObservedIndexesFeature })
		}},
		{"the engine does not prepare tables", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.PrepareTablesFeature })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := indexMethodsExchange()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*index methods require observed indexes and table preparation`)
		})
	}
}
