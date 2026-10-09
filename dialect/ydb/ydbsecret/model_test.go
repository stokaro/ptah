package ydbsecret_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

// TestModelTransport_KeepsTheVariableAndNoValue round-trips a declaration
// through the registered codecs: the variable, the holder and a rotation
// request survive, the decoded value is independent of the original, and an
// observation carries nothing at all.
func TestModelTransport_KeepsTheVariableAndNoValue(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbsecret.Codecs()}))
	original := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG", StructName: "Credentials", Rotate: true}
	objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbsecret.Ref("ext", "pg.password"), Value: original}))

	data, err := runtime.Codecs().EncodeObjects(t.Context(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().DecodeObjects(t.Context(), schemaext.Desired, data)
	c.Assert(err, qt.IsNil)
	values, err := decoded.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Ref, qt.DeepEquals, ydbsecret.Ref("ext", "pg.password"))
	c.Assert(values[0].Value, qt.DeepEquals, original)
	values[0].Value.(*ydbsecret.Desired).ValueEnv = "PTAH_SECRET_OTHER"
	c.Assert(original.ValueEnv, qt.Equals, "PTAH_SECRET_PG")

	observed, err := ydbsecret.Codecs()[1].Encode(&ydbsecret.Observed{})
	c.Assert(err, qt.IsNil)
	c.Assert(string(observed), qt.Equals, `{}`)
	c.Assert(ydbsecret.Ref("ext", "pw").Equal(ydbsecret.Ref("", "ext.pw")), qt.IsFalse)
}

// TestConversion_NamesTheDefaultVariable turns an observation into a
// declaration that keeps the secret: it names no variable, which selects the
// default one for the secret's path, and never asks for a rotation.
func TestConversion_NamesTheDefaultVariable(t *testing.T) {
	c := qt.New(t)
	declared := (&ydbsecret.Observed{}).Desired()
	c.Assert(declared, qt.DeepEquals, &ydbsecret.Desired{})
	c.Assert(declared.Variable(ydbsecret.Ref("app/ext", "pg.pass-1")), qt.Equals, "PTAH_SECRET_APP_EXT_PG_PASS_1")
	c.Assert((&ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG"}).Variable(ydbsecret.Ref("", "pw")), qt.Equals, "PTAH_SECRET_PG")
	c.Assert((&ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG", Rotate: true}).Observed(), qt.DeepEquals, &ydbsecret.Observed{})
}

// TestModelCodecs_RefuseAWireThatCouldCarryAValue refuses an unknown field --
// a value among them -- a null, and a variable a value may not come from, in
// either representation.
func TestModelCodecs_RefuseAWireThatCouldCarryAValue(t *testing.T) {
	tests := []struct {
		name           string
		representation int
		input          string
	}{
		{name: "desired value", representation: 0, input: `{"value_env":"PTAH_SECRET_PW","value":"s3cr3t"}`},
		{name: "desired null variable", representation: 0, input: `{"value_env":null}`},
		{name: "desired variable outside the prefix", representation: 0, input: `{"value_env":"HOME"}`},
		{name: "desired null", representation: 0, input: `null`},
		{name: "observed value", representation: 1, input: `{"value":"s3cr3t"}`},
		{name: "observed variable", representation: 1, input: `{"value_env":"PTAH_SECRET_PW"}`},
		{name: "observed rotation", representation: 1, input: `{"rotate":true}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbsecret.Codecs()[test.representation].Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestValidateIdentity_FailurePath refuses an identity that is not a
// directory and a leaf relative to the database root.
func TestValidateIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		schema, leaf string
	}{
		{name: "a slash in the leaf", leaf: "ext/pw"},
		{name: "an absolute directory", schema: "/ext", leaf: "pw"},
		{name: "a parent directory", schema: "../ext", leaf: "pw"},
		{name: "an unclean directory", schema: "ext//aws", leaf: "pw"},
		{name: "an empty leaf", schema: "ext"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsecret.ValidateIdentity(ydbsecret.Ref(test.schema, test.leaf)), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func declaredSecrets(c *qt.C, objects ...schemaext.Object) *schemamodel.Database {
	c.Helper()
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...))}
}

// TestRequestRotation_HappyPath marks each named declared secret, and only
// those, and leaves the input untouched. A root name keeps its dot, and asking
// twice rotates once.
func TestRequestRotation_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := declaredSecrets(c,
		ydbsecret.DesiredObject("ext", "pg", "", "PTAH_SECRET_PG"),
		ydbsecret.DesiredObject("", "s3.key", "", "PTAH_SECRET_S3"),
		ydbsecret.DesiredObject("", "kept", "", "PTAH_SECRET_KEPT"))

	rotated, err := ydbsecret.RequestRotation(desired, []string{" /ext/pg/ ", "s3.key", "ext/pg"})
	c.Assert(err, qt.IsNil)

	for _, want := range []struct {
		schema, name string
		rotate       bool
	}{{"ext", "pg", true}, {"", "s3.key", true}, {"", "kept", false}} {
		object, found, err := rotated.FeatureObjects.Get(ydbsecret.Ref(want.schema, want.name))
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(object.Value.(*ydbsecret.Desired).Rotate, qt.Equals, want.rotate, qt.Commentf("%s/%s", want.schema, want.name))
		original, _, err := desired.FeatureObjects.Get(ydbsecret.Ref(want.schema, want.name))
		c.Assert(err, qt.IsNil)
		c.Assert(original.Value.(*ydbsecret.Desired).Rotate, qt.IsFalse)
	}
	unchanged, err := ydbsecret.RequestRotation(desired, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged, qt.Equals, desired)
}

// TestRequestRotation_FailurePath refuses a path the declaration does not
// hold, so a typo cannot read as a rotation done. A dotted root name is not a
// directory and a name.
func TestRequestRotation_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "an undeclared secret", path: "ext/missing", wantErr: `rotate secret "ext/missing": the desired schema declares no such secret`},
		{name: "a dot is not a directory", path: "ext.pg", wantErr: `rotate secret "ext.pg": the desired schema declares no such secret`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := declaredSecrets(c, ydbsecret.DesiredObject("ext", "pg", "", "PTAH_SECRET_PG"))
			rotated, err := ydbsecret.RequestRotation(desired, []string{test.path})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ydbsecret.ErrRotateUndeclared)
			c.Assert(rotated, qt.IsNil)
		})
	}
}
