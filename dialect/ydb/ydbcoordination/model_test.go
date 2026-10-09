package ydbcoordination_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/engine"
)

func TestCoordinationModelRetainsUnsetSettingsAndSchemaIdentity(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbcoordination.Codecs()})).Codecs()
	original := &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}
	objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("app", "locks"), Value: original}))
	data, err := registry.EncodeObjects(t.Context(), schemaext.Observed, objects)
	c.Assert(err, qt.IsNil)
	decoded, err := registry.DecodeObjects(t.Context(), schemaext.Observed, data)
	c.Assert(err, qt.IsNil)
	values, err := decoded.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Ref.Parent.Source, qt.Equals, "")
	c.Assert(values[0].Ref, qt.DeepEquals, ydbcoordination.Ref("app", "locks"))
	c.Assert(values[0].Value, qt.DeepEquals, original)
	values[0].Value.(*ydbcoordination.Observed).Spec.ReadConsistencyMode = "relaxed"
	c.Assert(original.Spec.ReadConsistencyMode, qt.Equals, "strict")
	c.Assert(original.Spec.SelfCheckPeriodMillis, qt.Equals, uint32(0))
	c.Assert(original.Desired().Spec, qt.DeepEquals, original.Spec)
	c.Assert(ydbcoordination.Ref("app", "locks").Equal(ydbcoordination.Ref("", "app.locks")), qt.IsFalse)
}

func TestCoordinationModelSeparatesRawAndEffectiveEquality(t *testing.T) {
	c := qt.New(t)
	unset := &ydbcoordination.Desired{}
	explicit := &ydbcoordination.Desired{Spec: ydbcoordination.Defaults()}
	c.Assert(unset.Equal(explicit), qt.IsFalse)
	c.Assert(ydbcoordination.Effective(unset.Spec), qt.DeepEquals, ydbcoordination.Effective(explicit.Spec))
	c.Assert(unset.Equal(&ydbcoordination.Observed{}), qt.IsFalse)
}

func TestCoordinationCodecsRefuseIncompleteOrInvalidState(t *testing.T) {
	for _, codec := range ydbcoordination.Codecs() {
		for _, input := range []string{
			`null`, `{}`, `{"spec":{},"struct_name":null}`, `{"spec":null}`, `{"spec":{},"extra":1}`, `{"spec":{},"spec":{}}`,
			`{"spec":{"unknown":true}}`, `{"spec":{"self_check_period_millis":-1}}`,
			`{"spec":{"self_check_period_millis":4294967296}}`,
			`{"spec":{"self_check_period_millis":1}}`,
			`{"spec":{"self_check_period_millis":10000,"session_grace_period_millis":10000}}`,
			`{"spec":{"read_consistency_mode":"unexpected"}}`,
			`{"spec":{"read_consistency_mode":null}}`, `{"spec":{"Self_Check_Period_Millis":1000}}`,
		} {
			t.Run(string(codec.Representation)+"/"+input, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(input))
				c.Assert(err, qt.IsNotNil)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestCoordinationCoverageDoesNotNeedATableParent(t *testing.T) {
	c := qt.New(t)
	unknown := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "mode not understood"}
	coverage, err := ydbcoordination.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
		Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("app", "locks"), Knowledge: unknown,
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks")), qt.DeepEquals, unknown)
	c.Assert(coverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("other", "locks")).State, qt.Equals, schemaext.Complete)
}

func TestCoordinationDesiredCodecPreservesSourceIdentity(t *testing.T) {
	c := qt.New(t)
	codec := ydbcoordination.Codecs()[0]
	original := &ydbcoordination.Desired{StructName: "Locks", Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}
	encoded, err := codec.Encode(original)
	c.Assert(err, qt.IsNil)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, original)
	c.Assert(original.Equal(&ydbcoordination.Desired{Spec: original.Spec}), qt.IsFalse)
	c.Assert(original.Observed(), qt.DeepEquals, &ydbcoordination.Observed{Spec: original.Spec})
}

func TestCoordinationObservedCodecRefusesSourceIdentity(t *testing.T) {
	c := qt.New(t)
	decoded, err := ydbcoordination.Codecs()[1].Decode(json.RawMessage(`{"spec":{},"struct_name":"Locks"}`))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(decoded, qt.IsNil)
}
