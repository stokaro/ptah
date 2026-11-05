package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// indexFacetInspection is a read whose first index carries one facet of the
// fixture model, owned by that index, with the feature negotiated.
func indexFacetInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := indexInspection()
	d.Features = append(d.Features, provider.ObservedIndexFacetsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedIndexFacetsFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedIndexFacetsFeature)
	value := snapshot.NativeObjects[0]
	snapshot.NativeObjects = nil
	value.Owner = provider.IndexReference(snapshot.Tables[0].Identity, "items_label")
	snapshot.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(value)}
	snapshot.Tables[0].Indexes[0].Facets = []*protocol.Envelope{value}
	return d, request, snapshot
}

func TestIndexReference_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	table := &protocol.Reference{Engine: "fixture", Database: "/local", Kind: "table", Schema: "app", Name: "items", DisplayName: "Items"}
	got := provider.IndexReference(table, "items_label")
	c.Assert(proto.Equal(got, &protocol.Reference{Engine: "fixture", Database: "/local", Kind: "index", Schema: "app", Path: []string{"items"}, Name: "items_label"}), qt.IsTrue)
	c.Assert(table.GetKind(), qt.Equals, "table")
	c.Assert(table.GetPath(), qt.HasLen, 0)
	c.Assert(provider.IndexReference(nil, "items_label"), qt.IsNil)
}

// TestObservedIndexFacetsFollowTheirIndex reads an index's facet into the
// values the host normalizes, after the table's own and its columns', and keeps
// it through the wire.
func TestObservedIndexFacetsFollowTheirIndex(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := indexFacetInspection()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	batch, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(batch.Values, qt.HasLen, 1)
	c.Assert(proto.Equal(batch.Values[0].Value, snapshot.Tables[0].Indexes[0].Facets[0]), qt.IsTrue)
	data, err := proto.Marshal(snapshot)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.Schema{}
	c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
	_, err = provider.InspectionValues(d, request, decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Tables[0].Indexes[0].Facets, qt.HasLen, 1)
	c.Assert(proto.Equal(decoded.Tables[0].Indexes[0].Facets[0], snapshot.Tables[0].Indexes[0].Facets[0]), qt.IsTrue)
}

// TestObservedIndexFacetsRequireNegotiationAndTheirIndex refuses a facet that
// either side did not negotiate, and one its index does not own.
func TestObservedIndexFacetsRequireNegotiationAndTheirIndex(t *testing.T) {
	t.Parallel()
	without := func(features []string, name string) []string {
		return slices.DeleteFunc(features, func(feature string) bool { return feature == name })
	}
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"provider opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = without(d.Features, provider.ObservedIndexFacetsFeature)
		}},
		{"engine opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Engines[0].Features = without(d.Engines[0].Features, provider.ObservedIndexFacetsFeature)
		}},
		{"host opt out", func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = without(s.TargetFeatures, provider.ObservedIndexFacetsFeature)
		}},
		{"another index", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[1].Facets, s.Tables[0].Indexes[0].Facets = s.Tables[0].Indexes[0].Facets, nil
		}},
		{"the table", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Facets[0].Owner = proto.CloneOf(s.Tables[0].Identity)
			s.Coverage.Subjects[0].Subject = proto.CloneOf(s.Tables[0].Identity)
			s.Coverage.Subjects[0].NormalizedSubject = proto.CloneOf(s.Tables[0].Identity)
		}},
		{"a column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Facets[0].Owner = provider.ColumnReference(s.Tables[0].Identity, "items_label")
		}},
		{"another table's index", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Facets[0].Owner.Path = []string{"other"}
		}},
		{"missing", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Facets[0] = nil
		}},
		{"twice", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Facets = append(s.Tables[0].Indexes[0].Facets, proto.CloneOf(s.Tables[0].Indexes[0].Facets[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := indexFacetInspection()
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.IsNil)
			test.mutate(d, snapshot)
			_, err = provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// TestObservedIndexFacetsDescriptionRequiresObservedIndexes refuses an engine
// that lists the feature without its provider, and one that reports no
// indexes for the facets to belong to.
func TestObservedIndexFacetsDescriptionRequiresObservedIndexes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"provider feature", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedIndexFacetsFeature })
		}},
		{"engine observed indexes", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ObservedIndexesFeature })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := indexFacetInspection()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*observed index facets require the provider's feature and observed indexes`)
		})
	}
}

// TestObservedIndexFacetsAreTwoWay removes the feature from a description
// whose host did not list it, as every two-way feature is removed.
func TestObservedIndexFacetsAreTwoWay(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ObservedIndexFacetsFeature)
	d, _, _ := indexFacetInspection()
	negotiated := provider.NegotiatedDescription(d, []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature})
	c.Assert(negotiated.GetFeatures(), qt.Not(qt.Contains), provider.ObservedIndexFacetsFeature)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.ObservedIndexFacetsFeature)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Contains, provider.ObservedIndexesFeature)
}
