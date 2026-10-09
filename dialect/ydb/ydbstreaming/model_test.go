package ydbstreaming_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
)

func TestModelTransportPreservesRawSettingsAndPermission(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbstreaming.Codecs()}))
	original := &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}, StructName: "Events", AllowStateReset: true}
	objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbstreaming.Ref("app", "query.v1"), Value: original}))
	data, err := runtime.Codecs().EncodeObjects(t.Context(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().DecodeObjects(t.Context(), schemaext.Desired, data)
	c.Assert(err, qt.IsNil)
	values, err := decoded.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Ref, qt.DeepEquals, ydbstreaming.Ref("app", "query.v1"))
	c.Assert(values[0].Value, qt.DeepEquals, original)
	*values[0].Value.(*ydbstreaming.Desired).Spec.Run = true
	c.Assert(*original.Spec.Run, qt.IsFalse)
	c.Assert(original.Observed().Desired().AllowStateReset, qt.IsFalse)
	c.Assert(original.Observed().Desired().StructName, qt.Equals, "")
	c.Assert(original.Observed().Spec, qt.DeepEquals, original.Spec)
	c.Assert(ydbstreaming.Ref("app", "query").Equal(ydbstreaming.Ref("", "app.query")), qt.IsFalse)
}

func TestRawEqualityDoesNotApplyDefaults(t *testing.T) {
	c := qt.New(t)
	unset := &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}
	explicit := &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(true), ResourcePool: "default"}}
	c.Assert(unset.Equal(explicit), qt.IsFalse)
	c.Assert(ydbstreaming.Equal(unset.Spec, explicit.Spec), qt.IsTrue)
	c.Assert(unset.Equal(unset.Observed()), qt.IsFalse)
	c.Assert(unset.Equal(&ydbstreaming.Desired{Spec: unset.Spec, AllowStateReset: true}), qt.IsFalse)
	c.Assert(unset.Clone(), qt.DeepEquals, unset)
}

func TestModelCodecsRefuseAmbiguousWire(t *testing.T) {
	for _, codec := range ydbstreaming.Codecs() {
		for _, input := range []string{
			`null`, `{}`, `{"spec":null}`, `{"spec":{}}`, `{"spec":{"text":"SELECT 1;"},"extra":true}`,
			`{"spec":{"text":"SELECT 1;"},"spec":{"text":"SELECT 2;"}}`,
			`{"spec":{"text":"SELECT 1;","run":null},"allow_state_reset":false}`,
			`{"spec":{"text":"SELECT 1;","unknown":false},"allow_state_reset":false}`,
			`{"spec":{"Text":"SELECT 1;"},"allow_state_reset":false}`,
			`{"spec":{"text":"DROP TABLE events;"},"allow_state_reset":false}`,
			`{"spec":{"text":"SELECT 1;"},"allow_state_reset":null}`,
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

func TestObservedWireCannotGrantResetPermission(t *testing.T) {
	c := qt.New(t)
	value, err := ydbstreaming.Codecs()[1].Decode(json.RawMessage(`{"spec":{"text":"SELECT 1;"},"allow_state_reset":true}`))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.IsNil)
	value, err = ydbstreaming.Codecs()[0].Decode(json.RawMessage(`{"spec":{"text":"SELECT 1;"}}`))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.IsNil)
}

func TestCoveragePreservesUnreadableSubjects(t *testing.T) {
	c := qt.New(t)
	unknown := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "metadata access denied"}
	coverage, err := ydbstreaming.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("app", "hidden"), Knowledge: unknown},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(ydbstreaming.Kind, ydbstreaming.Ref("app", "hidden")), qt.DeepEquals, unknown)
	c.Assert(coverage.Lookup(ydbstreaming.Kind, ydbstreaming.Ref("app", "missing")).State, qt.Equals, schemaext.Complete)
}
