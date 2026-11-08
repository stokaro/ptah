package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func indexInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := columnInspection()
	d.Features = append(d.Features, provider.ObservedIndexesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedIndexesFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedIndexesFeature)
	table := snapshot.Tables[0]
	table.Indexes = []*protocol.ObservedIndex{
		{Name: "items_label", TableName: "items", Schema: "app", Columns: []string{"label"}, IncludeColumns: []string{"id"}, Method: "global"},
		{Name: "items_label_unique", TableName: "items", Schema: "app", Columns: []string{"label"}, Unique: true},
	}
	return d, request, snapshot
}

func TestObservedIndexesSurviveTheWire(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := indexInspection()
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
	c.Assert(decoded.Tables[0].Indexes, qt.HasLen, 2)
	c.Assert(proto.Equal(decoded.Tables[0].Indexes[0], before.Tables[0].Indexes[0]), qt.IsTrue)
}

func TestObservedIndexesRequireACompleteNegotiatedRecord(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"provider opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedIndexesFeature })
		}},
		{"engine opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ObservedIndexesFeature })
		}},
		{"host opt out", func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.ObservedIndexesFeature })
		}},
		{"without column negotiation", func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
		{"unnamed", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].Name = "" }},
		{"named twice", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[1].Name = "items_label" }},
		{"another table", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].TableName = "other" }},
		{"another schema", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].Schema = "other" }},
		{"primary", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].Primary = true }},
		{"no key", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].Columns = nil }},
		{"unknown key column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Columns = []string{"missing"}
		}},
		{"repeated key column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Columns = []string{"label", "label"}
		}},
		{"unknown covered column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].IncludeColumns = []string{"missing"}
		}},
		{"covered key column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].IncludeColumns = []string{"label"}
		}},
		{"empty key part", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].Parts = []*protocol.CapturedIndexPart{{}}
		}},
		{"negative shards", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Indexes[0].HashShardBuckets = -1 }},
		{"invalid owner", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Indexes[0].ManagedBy = &protocol.ComparisonIdentity{Kind: "constraint"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := indexInspection()
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.IsNil)
			test.mutate(d, snapshot)
			_, err = provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// With the feature negotiated, a table that lists no index holds none.
func TestObservedIndexAbsenceIsExplicit(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := indexInspection()
	snapshot.Tables[0].Indexes = nil
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
}

func TestObservedIndexIsNotACreationOperand(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := operationRender()
	table := &protocol.Table{Identity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "items"}}
	request.Changes = []*protocol.Change{{Id: "create", Object: proto.CloneOf(table.Identity), Operation: &protocol.Change_CreateTable{CreateTable: table}}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	table.Indexes = []*protocol.ObservedIndex{{Name: "items_label", TableName: "items", Columns: []string{"label"}}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
}

func TestObservedIndexDescriptionRequiresColumnSupport(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"provider index feature", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedIndexesFeature })
		}},
		{"engine columns", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := indexInspection()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
