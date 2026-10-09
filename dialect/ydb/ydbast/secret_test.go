package ydbast_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbsecret"
)

// TestSecretCodec_HappyPath round-trips each statement with its separate path
// parts; a dot stays in the part it is written in.
func TestSecretCodec_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value *ydbast.Secret
	}{
		{name: "create", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: "jobs.daily", Name: "pg.password", ValueEnv: "PTAH_SECRET_PG"}},
		{name: "rotate", value: &ydbast.Secret{Operation: ydbast.SecretRotate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"}},
		{name: "drop", value: &ydbast.Secret{Operation: ydbast.SecretDrop, Schema: "ext", Name: "pw"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.SecretCodec()})
			c.Assert(err, qt.IsNil)
			data, err := registry.Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := registry.Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.value})
			c.Assert(decoded[0].(*ydbast.Secret).Subject(), qt.DeepEquals, ydbsecret.Ref(test.value.Schema, test.value.Name))
		})
	}
}

// TestSecretCodec_FailurePath refuses a wire that is incomplete, ambiguous or
// carries a field no statement takes, a value among them.
func TestSecretCodec_FailurePath(t *testing.T) {
	wire := `{"operation":"create","schema":"ext","name":"pw","value_env":"PTAH_SECRET_PW"}`
	tests := []struct{ name, data string }{
		{name: "missing variable", data: strings.Replace(wire, `,"value_env":"PTAH_SECRET_PW"`, "", 1)},
		{name: "null name", data: strings.Replace(wire, `"name":"pw"`, `"name":null`, 1)},
		{name: "a value", data: strings.Replace(wire, `"value_env"`, `"value":"s3cr3t","value_env"`, 1)},
		{name: "unknown operation", data: strings.Replace(wire, `"operation":"create"`, `"operation":"replace"`, 1)},
		{name: "drop naming a variable", data: strings.Replace(wire, `"operation":"create"`, `"operation":"drop"`, 1)},
		{name: "variable outside the prefix", data: strings.Replace(wire, `PTAH_SECRET_PW`, `HOME`, 1)},
		{name: "path in the name", data: strings.Replace(wire, `"name":"pw"`, `"name":"ext/pw"`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbast.SecretCodec().Decode(json.RawMessage(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestSecretEffect classifies each statement by what it does to the value: a
// drop loses what nothing can read back, and an invalid statement has unknown
// effects rather than an additive default.
func TestSecretEffect(t *testing.T) {
	tests := []struct {
		name  string
		value *ydbast.Secret
		want  schemaext.Effect
	}{
		{name: "create", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: ydbsecret.CreateReason}},
		{name: "rotate", value: &ydbast.Secret{Operation: ydbast.SecretRotate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbsecret.RotateReason}},
		{name: "drop", value: &ydbast.Secret{Operation: ydbast.SecretDrop, Name: "pw"},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbsecret.DropReason}},
		{name: "invalid", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pw"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.value.Effect(), qt.DeepEquals, test.want)
		})
	}
}
