package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func columnInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := inspection()
	d.Features = append(d.Features, provider.ObservedColumnsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedColumnsFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedColumnsFeature)
	identity := &protocol.Reference{Engine: "fixture", Kind: "table", Schema: "app", Name: "items"}
	snapshot.Tables = []*protocol.Table{{Identity: identity, NormalizedIdentity: proto.CloneOf(identity), Columns: []*protocol.Column{
		{Name: "id", Observed: &protocol.ObservedColumn{Name: "id", DataType: "bigint", IsNullable: "NO", OrdinalPosition: 1,
			AutoIncrement: true, PrimaryKey: true, IdentityStart: "7", IdentityIncrement: "3", SequenceRestart: "42", OwnedSequence: "items_id_seq"}},
		{Name: "label", Observed: &protocol.ObservedColumn{Name: "label", DataType: "text", IsNullable: "YES", OrdinalPosition: 2, DefaultExpression: new("")}},
	}}}
	return d, request, snapshot
}

func TestCompleteObservedColumnsPreserveSequenceAndDefaultPresence(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := columnInspection()
	batch, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(batch.Values, qt.HasLen, 1)
	data, err := proto.Marshal(snapshot)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.Schema{}
	c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
	_, err = provider.InspectionValues(d, request, decoded)
	c.Assert(err, qt.IsNil)
	id, label := decoded.Tables[0].Columns[0].Observed, decoded.Tables[0].Columns[1].Observed
	c.Assert(id.SequenceRestart, qt.Equals, "42")
	c.Assert(id.IdentityIncrement, qt.Equals, "3")
	c.Assert(id.DefaultExpression, qt.IsNil)
	c.Assert(label.DefaultExpression, qt.DeepEquals, new(""))
}

func TestCompleteObservedColumnsRequireOneOrderedRepresentation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"feature opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
		{"host opt out", func(_ *protocol.Description, s *protocol.Schema) { s.TargetFeatures = []string{"retention"} }},
		{"missing observation", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Columns[0].Observed = nil
			s.Tables[0].Columns[0].Type = "bigint"
		}},
		{"changed identity", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Observed.Name = "other" }},
		{"missing ordinal", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Observed.OrdinalPosition = 0 }},
		{"reordered ordinal", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Observed.OrdinalPosition = 2 }},
		{"missing type", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Observed.DataType = "" }},
		{"unknown nullable", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Columns[0].Observed.IsNullable = "UNKNOWN"
		}},
		{"legacy type", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Type = "bigint" }},
		{"legacy nullable", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Nullable = true }},
		{"legacy primary", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].PrimaryKey = true }},
		{"legacy empty default", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].DefaultExpression = new("") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := columnInspection()
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.IsNil)
			test.mutate(d, snapshot)
			_, err = provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
