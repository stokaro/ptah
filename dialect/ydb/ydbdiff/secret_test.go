package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

// TestSecretChange_RoundTrip keeps both operands and the variable through the
// registered change codec, and the decoded change is independent of the
// original. A change with both operands is a rotation.
func TestSecretChange_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		change *ydbdiff.Secret
	}{
		{name: "create", change: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}}},
		{name: "drop", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}},
		{name: "rotate", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbdiff.Codecs()})).Codecs()
			document, err := registry.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{test.change})
			c.Assert(err, qt.IsNil)
			decoded, err := registry.Unmarshal(t.Context(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.change})
		})
	}
}

// TestSecretChange_FailurePath refuses a change without explicit operands, a
// rotation flag, which no operand carries, and an operand that could carry a
// value.
func TestSecretChange_FailurePath(t *testing.T) {
	for _, input := range []string{
		`null`, `{}`, `{"before":null,"after":null}`, `{"after":{}}`,
		`{"before":{},"after":{"value_env":"PTAH_SECRET_PW","rotate":true}}`,
		`{"before":{"value":"s3cr3t"},"after":null}`,
		`{"before":null,"after":{"value_env":"PTAH_SECRET_PW","value":"s3cr3t"}}`,
		`{"before":null,"after":{"value_env":"HOME"}}`,
		`{"before":null,"after":{},"extra":true}`,
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbdiff.SecretCodec().Decode(json.RawMessage(input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestSecretChange_Effect reports what each change does: dropping loses a
// value nothing can read back, and a rotation changes what every data source
// naming the secret signs in with.
func TestSecretChange_Effect(t *testing.T) {
	tests := []struct {
		name   string
		change *ydbdiff.Secret
		want   schemaext.Impact
	}{
		{name: "create", change: &ydbdiff.Secret{After: &ydbsecret.Desired{}}, want: schemaext.Additive},
		{name: "drop", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}, want: schemaext.Destructive},
		{name: "rotate", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{}}, want: schemaext.Behavioral},
		{name: "invalid", change: &ydbdiff.Secret{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Effect().Impact, qt.Equals, test.want)
		})
	}
}
