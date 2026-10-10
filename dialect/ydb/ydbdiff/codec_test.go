package ydbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
)

func TestChangefeedChange_WireRoundTripPreservesDirectionalState(t *testing.T) {
	c := qt.New(t)
	var codecs []schemaext.OwnedCodec
	for _, codec := range ydbdiff.Codecs() {
		codecs = append(codecs, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(codecs...)
	c.Assert(err, qt.IsNil)
	before := ydbschema.ChangefeedSpec{Name: "updates", Mode: "NEW_IMAGE", Format: "JSON", Disabled: true, RetentionPeriod: "PT24H", Consumers: []ydbtopic.ConsumerSpec{{Name: "worker", SupportedCodecs: []string{"zstd", "raw"}, AvailabilityPeriod: "PT1H"}}}
	after := before.Clone()
	after.Disabled = false
	after.RetentionPeriod = "PT48H"
	original := &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: before}, After: &ydbschema.DesiredChangefeed{Spec: after}}
	encoded, err := registry.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{original})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{original})
	changes, err := registry.SnapshotChanges(t.Context(), []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("a.b", "orders", "updates"), Value: original}})
	c.Assert(err, qt.IsNil)
	original.Before.Spec.Consumers[0].SupportedCodecs[0] = "gzip"
	c.Assert(changes[0].Value.(*ydbdiff.Changefeed).Before.Spec.Consumers[0].SupportedCodecs[0], qt.Equals, "zstd")
	c.Assert(changes[0].Subject.Schema.Source, qt.Equals, "a.b")
}

func TestChangefeedChange_RefusesUnboundAndEmptyChanges(t *testing.T) {
	cases := []struct {
		name   string
		change schemaext.ChangeRecord
	}{
		{name: "missing subject", change: schemaext.ChangeRecord{Value: &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}}}}},
		{name: "missing both sides", change: schemaext.ChangeRecord{Subject: ydbschema.ChangefeedRef("", "orders", "updates"), Value: &ydbdiff.Changefeed{}}},
		{name: "rename inside payload", change: schemaext.ChangeRecord{Subject: ydbschema.ChangefeedRef("", "orders", "updates"), Value: &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "old", Mode: "UPDATES", Format: "JSON"}}, After: &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbdiff.Codecs()[0]})
			c.Assert(err, qt.IsNil)
			changes, err := registry.SnapshotChanges(t.Context(), []schemaext.ChangeRecord{test.change})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(changes, qt.IsNil)
		})
	}
}
