package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// blockerInspection is a read negotiated with removal blockers of table
// items, which one stream names whole and another names by its label column.
func blockerInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := columnInspection()
	d.Features = append(d.Features, provider.RemovalBlockersFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.RemovalBlockersFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.RemovalBlockersFeature)
	snapshot.Tables[0].RemovalBlockers = []*protocol.RemovalBlocker{
		{Kind: "change_stream", Name: "everything"},
		{Kind: "change_stream", Name: "Labels", Columns: []string{"label"}},
	}
	return d, request, snapshot
}

// blockerPlanning is planningExchange on an engine with complete column
// observations and removal blockers negotiated: the held table carries the
// blockers its read reported.
func blockerPlanning() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	d.Features = append(d.Features, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	request.Tables[0].Current.Columns = []*protocol.ObservedColumn{{Name: "id", DataType: "integer", IsNullable: "NO"}}
	request.Tables[0].Current.RemovalBlockers = []*protocol.RemovalBlocker{{Kind: "change_stream", Name: "cs1", Columns: []string{"id"}}}
	return d, request, result
}

func TestRemovalBlockers_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read reports its blockers and they survive the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := blockerInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		before := proto.CloneOf(snapshot)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
		data, err := proto.Marshal(snapshot)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.Schema{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		_, err = provider.InspectionValues(d, request, decoded)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(decoded.Tables[0].RemovalBlockers[1], before.Tables[0].RemovalBlockers[1]), qt.IsTrue)
	})
	t.Run("a negotiated read of a table nothing blocks", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := blockerInspection()
		snapshot.Tables[0].RemovalBlockers = nil
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("one object of two kinds", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := blockerInspection()
		snapshot.Tables[0].RemovalBlockers[1].Name = "everything"
		snapshot.Tables[0].RemovalBlockers[1].Kind = "publication"
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a planning request carries the held table's blockers", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := blockerPlanning()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		before := proto.CloneOf(request)
		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
		c.Assert(proto.Equal(request, before), qt.IsTrue)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := blockerInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.RemovalBlockersFeature))
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.RemovalBlockersFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.RemovalBlockersFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedColumnsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.RemovalBlockersFeature)
	})
}

// An engine lists the feature only beside the provider's and with complete
// column observations.
func TestRemovalBlockersDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", `.*removal blockers: the engine's feature requires the provider's`, func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.RemovalBlockersFeature)
		}},
		{"without column observations", `.*removal blockers: engine fixture does not report complete column observations`, func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedColumnsFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := blockerInspection()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, test.want)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A read reports blockers only where both sides listed the feature, and each
// one names its kind as a token, its object by a name a host can show, and
// only columns of the table, each once; no object is listed twice.
func TestRemovalBlockersInspection_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"the provider did not list the feature", `.*removal blockers: the blockers of table items were not negotiated`, func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = withoutFeature(d.Features, provider.RemovalBlockersFeature)
		}},
		{"the host did not list the feature", `.*removal blockers: the blockers of table items were not negotiated`, func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.RemovalBlockersFeature)
		}},
		{"columns were not negotiated", `.*removal blockers: the blockers require negotiated column observations`, func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.ObservedColumnsFeature)
			s.Tables[0].Columns = []*protocol.Column{{Name: "id", Type: "bigint"}, {Name: "label", Type: "text"}}
		}},
		{"no kind", `.*removal blockers: a blocker of table "items" has no kind token`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Kind = ""
		}},
		{"a kind that is not a token", `.*removal blockers: a blocker of table "items" has no kind token`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Kind = "Change Stream"
		}},
		{"no name", `.*removal blockers: a change_stream blocker of table "items" has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Name = ""
		}},
		{"a blank name", `.*removal blockers: a change_stream blocker of table "items" has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Name = " \t"
		}},
		{"a name with a control character", `.*removal blockers: a change_stream blocker of table "items" has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Name = "cs\n1"
		}},
		{"a name that is not UTF-8", `.*removal blockers: a change_stream blocker of table "items" has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[0].Name = "cs\xff"
		}},
		{"an object listed twice", `.*removal blockers: change_stream "everything" is listed twice for table "items"`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[1].Name = "everything"
		}},
		{"a column the table does not have", `.*removal blockers: change_stream "Labels" names an unknown or repeated column of table "items"`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[1].Columns = []string{"missing"}
		}},
		{"a column named twice", `.*removal blockers: change_stream "Labels" names an unknown or repeated column of table "items"`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].RemovalBlockers[1].Columns = []string{"label", "label"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := blockerInspection()
			test.mutate(d, snapshot)
			got, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A planning request carries blockers only where both sides listed the
// feature, checked against the held table's columns as a read's are.
func TestRemovalBlockersPlanning_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
	}{
		{"the provider did not list the feature", `.*removal blockers: the blockers of table C were not negotiated`, func(d *protocol.Description, r *protocol.PlanFeaturesRequest) {
			d.Features = withoutFeature(d.Features, provider.RemovalBlockersFeature)
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.RemovalBlockersFeature)
			r.TargetFeatures = withoutFeature(r.TargetFeatures, provider.RemovalBlockersFeature)
		}},
		{"the host did not list the feature", `.*removal blockers: the blockers of table C were not negotiated`, func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.TargetFeatures = withoutFeature(r.TargetFeatures, provider.RemovalBlockersFeature)
		}},
		{"a column the held table does not have", `.*removal blockers: change_stream "cs1" names an unknown or repeated column of table "C"`, func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Current.RemovalBlockers[0].Columns = []string{"missing"}
		}},
		{"a kind that is not a token", `.*removal blockers: a blocker of table "C" has no kind token`, func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Current.RemovalBlockers[0].Kind = "1stream"
		}},
		{"an object listed twice", `.*removal blockers: change_stream "cs1" is listed twice for table "C"`, func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Current.RemovalBlockers = append(r.Tables[0].Current.RemovalBlockers, &protocol.RemovalBlocker{Kind: "change_stream", Name: "cs1"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := blockerPlanning()
			test.mutate(d, request)
			c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, test.want)
		})
	}
}

// Only a planning request carries blockers: a CHECK comparison's held table
// carries none, even on an engine that negotiated them.
func TestRemovalBlockersInACheckComparison_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := checkEquivalenceExchange()
	d.Features = append(d.Features, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.ObservedColumnsFeature, provider.RemovalBlockersFeature)
	request.Tables[0].Current.RemovalBlockers = []*protocol.RemovalBlocker{{Kind: "change_stream", Name: "cs1"}}
	c.Assert(provider.ValidateComparedChecks(d, request, result), qt.ErrorMatches, `.*a held table carries no removal blockers, which only a planning request carries`)
}
