package ydbsecret_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

// TestModelTransport_KeepsTheVariableAndNoValue round-trips a declaration
// through the registered codecs: the variable and the holder survive, the
// decoded value is independent of the original, and an observation carries
// nothing at all.
func TestModelTransport_KeepsTheVariableAndNoValue(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbsecret.Codecs()}))
	original := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG", StructName: "Credentials"}
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
// default one for the secret's path.
func TestConversion_NamesTheDefaultVariable(t *testing.T) {
	c := qt.New(t)
	declared := (&ydbsecret.Observed{}).Desired()
	c.Assert(declared, qt.DeepEquals, &ydbsecret.Desired{})
	c.Assert(declared.Variable(ydbsecret.Ref("app/ext", "pg.pass-1")), qt.Equals, "PTAH_SECRET_APP_EXT_PG_PASS_1")
	c.Assert((&ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG"}).Variable(ydbsecret.Ref("", "pw")), qt.Equals, "PTAH_SECRET_PG")
	c.Assert((&ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG"}).Observed(), qt.DeepEquals, &ydbsecret.Observed{})
}

// TestModelCodecs_RefuseAWireThatCouldCarryAValue refuses an unknown field --
// a value among them -- a null, and a variable a value may not come from, in
// either representation. A rotation is a request of one comparison, so no
// representation carries one.
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
		{name: "desired rotation", representation: 0, input: `{"value_env":"PTAH_SECRET_PW","rotate":true}`},
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

// TestParsePath_HappyPath reads every secret spelling as a path: a slash
// separates directories and a dot stays in its segment.
func TestParsePath_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		schema, leaf string
	}{
		{name: "a dotted root name", path: "pg.pw", leaf: "pg.pw"},
		{name: "a directory", path: "ext/pg", schema: "ext", leaf: "pg"},
		{name: "dotted segments", path: "a/b.c/d.e", schema: "a/b.c", leaf: "d.e"},
		{name: "surrounding space", path: " ext/pg ", schema: "ext", leaf: "pg"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbsecret.ParsePath(test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(ref, qt.DeepEquals, ydbsecret.Ref(test.schema, test.leaf))
		})
	}
}

// TestParsePath_FailurePath refuses a path with no name or with an empty,
// current or parent segment, a trailing slash included.
func TestParsePath_FailurePath(t *testing.T) {
	for _, path := range []string{"", "..", "ext/..", "./pw", "ext//pg", "ext/../pg", "ext/pg/"} {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbsecret.ParsePath(path)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `".*" is not a secret path \(dir/name\): .*`)
			c.Assert(ref, qt.DeepEquals, objectidentity.ID{})
		})
	}
}

// TestParsePath_RefusesAnAbsolutePath refuses a path written from the server
// root: nothing that reads it this way knows the database it lies in, and
// stripping the slash would name another secret.
func TestParsePath_RefusesAnAbsolutePath(t *testing.T) {
	for _, path := range []string{"/", "/ext/pg", " /local/ext/pg"} {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbsecret.ParsePath(path)
			c.Assert(err, qt.ErrorIs, ydbsecret.ErrAbsolutePath)
			c.Assert(err, qt.ErrorMatches, `".*" is not a secret path \(dir/name\): .*write the path relative to the database root.*`)
			c.Assert(ref, qt.DeepEquals, objectidentity.ID{})
		})
	}
}

// TestResolvePath_HappyPath reads an absolute path against the database root
// it lies under, and a relative one as ParsePath does.
func TestResolvePath_HappyPath(t *testing.T) {
	tests := []struct {
		name, root, path string
		schema, leaf     string
	}{
		{name: "absolute under the root", root: "/local", path: "/local/ext/pg", schema: "ext", leaf: "pg"},
		{name: "absolute through a current segment", root: "/local/", path: "/local/./pg.pw", leaf: "pg.pw"},
		{name: "relative", root: "/local", path: "ext/pg", schema: "ext", leaf: "pg"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbsecret.ResolvePath(test.root, test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(ref, qt.DeepEquals, ydbsecret.Ref(test.schema, test.leaf))
		})
	}
}

// TestResolvePath_FailurePath refuses an absolute path outside the root, and
// any absolute path where the root is not known.
func TestResolvePath_FailurePath(t *testing.T) {
	tests := []struct {
		name, root, path string
		wantIs           error
	}{
		{name: "another database", root: "/local", path: "/other/pg", wantIs: ydbsecret.ErrOutsideDatabase},
		{name: "out through a parent segment", root: "/local", path: "/local/../other/pg", wantIs: ydbsecret.ErrOutsideDatabase},
		{name: "no root", path: "/local/ext/pg", wantIs: ydbsecret.ErrAbsolutePath},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbsecret.ResolvePath(test.root, test.path)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(ref, qt.DeepEquals, objectidentity.ID{})
		})
	}
}

// TestRotationRequests_HappyPath asks once per secret, in the order given,
// with the rotate action.
func TestRotationRequests_HappyPath(t *testing.T) {
	c := qt.New(t)
	requests, err := ydbsecret.RotationRequests([]string{" ext/pg ", "pg.pw", "ext/pg"})
	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []schemaext.ChangeRequest{
		{Subject: ydbsecret.Ref("ext", "pg"), Action: ydbsecret.RotateAction},
		{Subject: ydbsecret.Ref("", "pg.pw"), Action: ydbsecret.RotateAction},
	})
	none, err := ydbsecret.RotationRequests(nil)
	c.Assert(err, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

// TestRotationRequests_FailurePath refuses a path that names no secret before
// any comparison runs.
func TestRotationRequests_FailurePath(t *testing.T) {
	c := qt.New(t)
	requests, err := ydbsecret.RotationRequests([]string{"ext/pg", "ext//pg"})
	c.Assert(err, qt.ErrorMatches, `rotate secret: "ext//pg" is not a secret path \(dir/name\): .*`)
	c.Assert(requests, qt.IsNil)
}

// TestWithRotations_HappyPath asks once per secret: a secret the options
// already ask to rotate, and one the paths name twice, is asked for once, and
// the options' other requests stay.
func TestWithRotations_HappyPath(t *testing.T) {
	c := qt.New(t)
	held := schemaext.ChangeRequest{Subject: ydbsecret.Ref("ext", "pg"), Action: ydbsecret.RotateAction}
	opts := &config.CompareOptions{FeatureRequests: []schemaext.ChangeRequest{held}}

	err := ydbsecret.WithRotations(opts, []string{"ext/pg", "pw", "pw"})

	c.Assert(err, qt.IsNil)
	c.Assert(opts.FeatureRequests, qt.DeepEquals, []schemaext.ChangeRequest{
		held, {Subject: ydbsecret.Ref("", "pw"), Action: ydbsecret.RotateAction},
	})
}

// TestWithRotations_FailurePath refuses a path that names no secret and
// leaves the options as they were.
func TestWithRotations_FailurePath(t *testing.T) {
	c := qt.New(t)
	held := schemaext.ChangeRequest{Subject: ydbsecret.Ref("ext", "pg"), Action: ydbsecret.RotateAction}
	opts := &config.CompareOptions{FeatureRequests: []schemaext.ChangeRequest{held}}

	err := ydbsecret.WithRotations(opts, []string{"pw", "/local/pw"})

	c.Assert(err, qt.ErrorIs, ydbsecret.ErrAbsolutePath)
	c.Assert(opts.FeatureRequests, qt.DeepEquals, []schemaext.ChangeRequest{held})
}
