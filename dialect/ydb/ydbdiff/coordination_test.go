package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine"
)

func TestCoordinationChangePreservesRawOperandsInExplicitWire(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbdiff.Codecs()})).Codecs()
	original := &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}, After: &ydbcoordination.Desired{}}
	document, err := registry.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{original})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), document)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{original})
	change := decoded[0].(*ydbdiff.CoordinationNode)
	change.Before.Spec.ReadConsistencyMode = "relaxed"
	c.Assert(original.Before.Spec.ReadConsistencyMode, qt.Equals, "strict")
	c.Assert(change.After.Spec.IsZero(), qt.IsTrue)
	c.Assert(ydbcoordination.Changes(change.After.Spec, original.Before.Spec).ReadConsistencyMode, qt.Equals, "relaxed")
}

func TestCoordinationChangeRejectsMissingAndMalformedOperands(t *testing.T) {
	codec := ydbdiff.CoordinationCodec()
	for _, input := range []string{
		`null`, `{}`, `{"after":{"spec":{}}}`, `{"before":null,"after":null}`,
		`{"before":{},"after":{"spec":{}}}`, `{"before":null,"after":{"spec":null}}`,
		`{"before":null,"after":{"spec":{}},"extra":true}`,
		`{"before":null,"before":{"spec":{}},"after":null}`,
		`{"before":null,"after":{"spec":{"self_check_period_millis":1}}}`,
		`{"before":null,"after":{"spec":{"read_consistency_mode":null}}}`,
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			value, err := codec.Decode(json.RawMessage(input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}
