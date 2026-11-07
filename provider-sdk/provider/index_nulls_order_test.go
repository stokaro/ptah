package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// nullsOrderPreparation is a preparation negotiated with index methods and the
// NULLS order, whose table declares an index of two parts and one of one
// field, and a reply that states the order of each key part: FIRST where it is
// not the engine's default, empty where it is.
func nullsOrderPreparation() (*protocol.Description, *protocol.PrepareTablesRequest, *protocol.PreparedTables) {
	d, request, result := indexMethodsExchange()
	d.Features = append(d.Features, provider.IndexNullsOrderFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.IndexNullsOrderFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.IndexNullsOrderFeature)
	request.Tables[0].Desired.Indexes[0].Parts = []*protocol.CapturedIndexPart{{Column: "id", Descending: true}, {Column: "id", Nulls: "FIRST"}}
	result.Tables[0].Indexes[0].Nulls = []string{"", "FIRST"}
	result.Tables[0].Indexes[1].Nulls = []string{""}
	return d, request, result
}

// nullsOrderInspection is a read negotiated with the NULLS order of two
// indexes, one keyed by a part in the default order and one by a descending
// part that sorts NULLs first.
func nullsOrderInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := indexInspection()
	features := []string{provider.PrepareTablesFeature, provider.NormalizeValuesFeature, provider.IndexNullsOrderFeature}
	for _, feature := range features {
		if !slices.Contains(d.Features, feature) {
			d.Features = append(d.Features, feature)
		}
	}
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PrepareTablesFeature, provider.IndexNullsOrderFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.IndexNullsOrderFeature)
	snapshot.Tables[0].Indexes[0].Parts = []*protocol.CapturedIndexPart{{Column: "label"}}
	snapshot.Tables[0].Indexes[1].Parts = []*protocol.CapturedIndexPart{{Column: "label", Descending: true, Nulls: "FIRST"}}
	return d, request, snapshot
}

func TestIndexNullsOrder_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a prepared index states the order of each key part", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := nullsOrderPreparation()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
		data, err := proto.Marshal(result)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.PreparedTables{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, decoded), qt.IsNil)
		c.Assert(decoded.Tables[0].Indexes[0].GetNulls(), qt.DeepEquals, []string{"", "FIRST"})
	})
	t.Run("without index methods an entry states the order and no kind", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := nullsOrderPreparation()
		d.Features = withoutIndexMethods(d.Features)
		d.Engines[0].Features = withoutIndexMethods(d.Engines[0].Features)
		request.TargetFeatures = withoutIndexMethods(request.TargetFeatures)
		result.Tables[0].Indexes[0].Method = ""
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
	})
	t.Run("a negotiated read states the order of a part where it is not the default", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := nullsOrderInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a negotiated read may name its keys without parts, each in the default order", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := nullsOrderInspection()
		snapshot.Tables[0].Indexes[0].Parts = nil
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	// A read without the feature keeps what contract 25.27 accepts, whatever
	// a part's order says.
	t.Run("a read without the feature is checked as before", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := nullsOrderInspection()
		snapshot.TargetFeatures = withoutFeature(snapshot.TargetFeatures, provider.IndexNullsOrderFeature)
		snapshot.Tables[0].Indexes[1].Parts[0].Nulls = "NULLS FIRST"
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := nullsOrderPreparation()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.IndexNullsOrderFeature))
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.IndexNullsOrderFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.IndexMethodsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.IndexNullsOrderFeature)
	})
}

// A negotiated preparation states FIRST, LAST or empty for each key part of
// each declared index, and one without the feature states none.
func TestPreparedIndexNullsOrder_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.PrepareTablesRequest, *protocol.PreparedTable)
	}{
		{"an order missing", `.*states 1 NULLS orders for the 2 key parts of index "by_label"`, func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) {
			p.Indexes[0].Nulls = p.Indexes[0].Nulls[:1]
		}},
		{"an order per field of an index with parts", `.*states 1 NULLS orders for the 2 key parts of index "by_label"`, func(r *protocol.PrepareTablesRequest, p *protocol.PreparedTable) {
			p.Indexes[0].Nulls = []string{"LAST"}
		}},
		{"no order for an index of fields", `.*states 0 NULLS orders for the 1 key parts of index "by_id"`, func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) {
			p.Indexes[1].Nulls = nil
		}},
		{"another spelling", `.*states NULLS order "first" for index "by_id"; it is FIRST, LAST or empty for the default`, func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) {
			p.Indexes[1].Nulls = []string{"first"}
		}},
		{"the keyword spelled out", `.*states NULLS order "NULLS LAST" for index "by_label"; it is FIRST, LAST or empty for the default`, func(_ *protocol.PrepareTablesRequest, p *protocol.PreparedTable) {
			p.Indexes[0].Nulls[0] = "NULLS LAST"
		}},
		{"orders the host did not ask for", `.*prepared index NULLS orders need schema\.index-nulls-order`, func(r *protocol.PrepareTablesRequest, _ *protocol.PreparedTable) {
			r.TargetFeatures = withoutFeature(r.TargetFeatures, provider.IndexNullsOrderFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := nullsOrderPreparation()
			test.mutate(request, result.Tables[0])
			err := provider.ValidatePreparedTables(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// Without index methods, an entry that states a kind is refused: the host did
// not ask for one.
func TestPreparedIndexMethodWithoutTheFeature_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := nullsOrderPreparation()
	request.TargetFeatures = withoutIndexMethods(request.TargetFeatures)
	err := provider.ValidatePreparedTables(d, request, result)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*prepared index "by_label" states a method, which needs schema\.index-methods`)
}

// A negotiated read spells each key part's order FIRST, LAST or empty for the
// default.
func TestInspectedIndexNullsOrder_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Schema)
	}{
		{"lower case", `.*observed index "items_label" has a key part with NULLS order "last"; it is FIRST, LAST or empty for the default`, func(s *protocol.Schema) {
			s.Tables[0].Indexes[0].Parts[0].Nulls = "last"
		}},
		{"the keyword spelled out", `.*observed index "items_label_unique" has a key part with NULLS order "NULLS FIRST"; it is FIRST, LAST or empty for the default`, func(s *protocol.Schema) {
			s.Tables[0].Indexes[1].Parts[0].Nulls = "NULLS FIRST"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := nullsOrderInspection()
			test.mutate(snapshot)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// An engine states the order with the provider's feature, the indexes it
// holds and the preparation that states the declared side.
func TestIndexNullsOrderDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.IndexNullsOrderFeature)
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
			d, _, _ := nullsOrderPreparation()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*index NULLS order requires the provider's feature, observed indexes and table preparation`)
		})
	}
}
