package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/engine"
)

func TestCoordinationConversionPreservesStoredDefaults(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbcoordination.Codecs(),
		Conversions: []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind}, Service: ydbconvert.CoordinationService{}}}})
	c.Assert(err, qt.IsNil)
	original := &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}
	converted, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{original}})
	c.Assert(err, qt.IsNil)
	c.Assert(converted, qt.DeepEquals, []schemaext.Value{&ydbcoordination.Desired{Spec: original.Spec}})
	restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed, Values: converted})
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{original})
	converted[0].(*ydbcoordination.Desired).Spec.ReadConsistencyMode = "relaxed"
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{original})
	c.Assert(original.Spec.SelfCheckPeriodMillis, qt.Equals, uint32(0))
}

func TestCoordinationConversionDiscardsPrefixBeforeInvalidValue(t *testing.T) {
	c := qt.New(t)
	result, err := (ydbconvert.CoordinationService{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&ydbcoordination.Desired{}, &ydbcoordination.Observed{}},
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
