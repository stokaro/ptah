package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

// TestTopicConversion_KeepsSettingsAndConsumers converts a declaration into
// the observation a read would report once it is applied, without the holder,
// and that observation back into a declaration that keeps the topic.
func TestTopicConversion_KeepsSettingsAndConsumers(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbtopic.Codecs(),
		Conversions: []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{ydbtopic.Kind}, Service: ydbconvert.TopicService{}}}})
	c.Assert(err, qt.IsNil)
	spec := ydbtopic.Spec{RetentionPeriod: "PT2H", SupportedCodecs: []string{"raw"}, Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}}}

	observed, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&ydbtopic.Desired{Spec: spec, StructName: "Events"}}})
	c.Assert(err, qt.IsNil)
	restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&ydbtopic.Observed{Spec: spec}})
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{&ydbtopic.Desired{Spec: spec}})
}
