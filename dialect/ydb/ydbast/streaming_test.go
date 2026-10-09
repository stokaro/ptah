package ydbast_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
)

const streamingBody = "INSERT INTO sink SELECT * FROM source;"

func TestStreamingOperationCodecPreservesOperandsAndIndependentRunSettings(t *testing.T) {
	c := qt.New(t)
	value := &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Schema: "jobs.daily", Name: "copy.events",
		Spec:     ast.StreamingQuerySpec{Text: streamingBody, Run: new(false), ResourcePool: "workload"},
		Previous: ast.StreamingQuerySpec{Text: streamingBody + " SELECT 1;", Run: new(true)}, AllowStateReset: true}
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.StreamingCodec()})
	c.Assert(err, qt.IsNil)
	data, err := registry.Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `"schema":"jobs.daily"`)
	c.Assert(string(data), qt.Contains, `"name":"copy.events"`)
	decoded, err := registry.Unmarshal(c.Context(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{value})
	clone := decoded[0].(*ydbast.StreamingQuery).CloneExtension().(*ydbast.StreamingQuery)
	*clone.Spec.Run, *clone.Previous.Run = true, false
	c.Assert(*value.Spec.Run, qt.IsFalse)
	c.Assert(*value.Previous.Run, qt.IsTrue)
	c.Assert(*decoded[0].(*ydbast.StreamingQuery).Spec.Run, qt.IsFalse)
	c.Assert(*decoded[0].(*ydbast.StreamingQuery).Previous.Run, qt.IsTrue)
}

func TestStreamingCodecRefusesIncompleteOrAmbiguousWire(t *testing.T) {
	wire := `{"operation":"create","schema":"jobs","name":"copy","creation":{"or_replace":false,"if_not_exists":false},"spec":{"text":"INSERT INTO sink SELECT * FROM source;"},"previous":{"text":""},"allow_state_reset":false}`
	for _, test := range []struct{ name, data string }{
		{"missing path", strings.Replace(wire, `"name":"copy",`, "", 1)},
		{"null path", strings.Replace(wire, `"name":"copy"`, `"name":null`, 1)},
		{"null spec", strings.Replace(wire, `"spec":{"text":"INSERT INTO sink SELECT * FROM source;"}`, `"spec":null`, 1)},
		{"null run", strings.Replace(wire, `"spec":{`, `"spec":{"run":null,`, 1)},
		{"null permission", strings.Replace(wire, `"allow_state_reset":false`, `"allow_state_reset":null`, 1)},
		{"missing guard", strings.Replace(wire, `"or_replace":false,`, "", 1)},
		{"unknown guard", strings.Replace(wire, `"or_replace"`, `"replace"`, 1)},
		{"unknown setting", strings.Replace(wire, `"spec":{`, `"spec":{"unexpected":true,`, 1)},
		{"duplicate field", strings.Replace(wire, `"name":"copy"`, `"name":"copy","name":"other"`, 1)},
		{"unknown operation", strings.Replace(wire, `"operation":"create"`, `"operation":"restart"`, 1)},
		{"alter without previous", strings.Replace(wire, `"operation":"create"`, `"operation":"alter"`, 1)},
		{"drop with settings", strings.Replace(wire, `"operation":"create"`, `"operation":"drop"`, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbast.StreamingCodec().Decode(json.RawMessage(test.data))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestStreamingEffectsRetainCheckpointRisk(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  *ydbast.StreamingQuery
		impact schemaext.Impact
	}{
		{"create", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody}}, schemaext.Additive},
		{"replace", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Creation: ydbast.StreamingCreation{OrReplace: true}, Spec: ast.StreamingQuerySpec{Text: streamingBody}}, schemaext.Destructive},
		{"guarded replace", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Creation: ydbast.StreamingCreation{OrReplace: true, IfNotExists: true}, Spec: ast.StreamingQuerySpec{Text: streamingBody}}, schemaext.Additive},
		{"settings", &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody, Run: new(false)}, Previous: ast.StreamingQuerySpec{Text: streamingBody}}, schemaext.Behavioral},
		{"body", &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody + " SELECT 1;"}, Previous: ast.StreamingQuerySpec{Text: streamingBody}, AllowStateReset: true}, schemaext.Destructive},
		{"drop", &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Name: "q"}, schemaext.Destructive},
		{"nil", nil, ""},
		{"missing previous", &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody}}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.value.Effect().Impact, qt.Equals, test.impact)
		})
	}
}

func TestStreamingCodecRefusesLossyOrMisplacedOperands(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *ydbast.StreamingQuery
	}{
		{"nil", nil},
		{"empty", &ydbast.StreamingQuery{}},
		{"invalid UTF-8 directory", &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Schema: "\xff", Name: "q"}},
		{"invalid UTF-8 pool", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody, ResourcePool: "\xff"}}},
		{"unsplit path", &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Name: "jobs/q"}},
		{"drop guard", &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Name: "q", Creation: ydbast.StreamingCreation{IfNotExists: true}}},
		{"create reset permission", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody}, AllowStateReset: true}},
		{"create previous", &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "q", Spec: ast.StreamingQuerySpec{Text: streamingBody}, Previous: ast.StreamingQuerySpec{Text: streamingBody}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := ydbast.StreamingCodec().Encode(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			modelError, found := errors.AsType[*schemaext.InvalidModelError](err)
			c.Assert(found, qt.IsTrue)
			c.Assert(modelError.Kind, qt.Equals, ydbast.StreamingQueryKind)
			c.Assert(modelError.Representation, qt.Equals, schemaext.Operation)
			c.Assert(data, qt.IsNil)
		})
	}
}
