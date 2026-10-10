package ydbrender_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbrender"
)

func secretRegistry(c *qt.C) renderer.Extensions {
	registry, err := renderer.NewExtensions(ydbrender.SecretHandler())
	c.Assert(err, qt.IsNil)
	return registry
}

// TestSecretHandler_HappyPath pins how a secret is written: its value is the
// named expression for its variable and never a literal, so the rendered text
// holds no value whatever the environment holds. Each rendering was applied to
// local-ydb 26.2.1.14 through the connection, which defined the value. A dot
// stays part of the name it is written in.
func TestSecretHandler_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value *ydbast.Secret
		want  string
	}{
		{name: "a secret at the root", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
			want: "CREATE SECRET `pg_password` WITH (value = $PTAH_SECRET_PG_PASSWORD);"},
		{name: "a secret in a directory", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: "ext", Name: "s3", ValueEnv: "PTAH_SECRET_S3"},
			want: "CREATE SECRET `ext/s3` WITH (value = $PTAH_SECRET_S3);"},
		{name: "a dotted name at the root", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pg.password", ValueEnv: "PTAH_SECRET_PG"},
			want: "CREATE SECRET `pg.password` WITH (value = $PTAH_SECRET_PG);"},
		{name: "a rotation", value: &ydbast.Secret{Operation: ydbast.SecretRotate, Schema: "ext", Name: "s3", ValueEnv: "PTAH_SECRET_S3"},
			want: "ALTER SECRET `ext/s3` WITH (value = $PTAH_SECRET_S3);"},
		{name: "a drop", value: &ydbast.Secret{Operation: ydbast.SecretDrop, Schema: "ext", Name: "s3"}, want: "DROP SECRET `ext/s3`;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := secretRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262()}, ast.StatementExtension, test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestSecretHandler_RefusesByCapability names the key a line lacks: 25.1 and
// 25.2 have no CREATE SECRET, and 25.3 has it behind a flag that is off.
func TestSecretHandler_RefusesByCapability(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		value   *ydbast.Secret
		wantErr string
	}{
		{name: "a creation on 25.1", caps: capability.YDB251(), value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a rotation on 25.3", caps: capability.YDB253(), value: &ydbast.Secret{Operation: ydbast.SecretRotate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantErr: "ALTER SECRET pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a drop on 25.2", caps: capability.YDB252(), value: &ydbast.Secret{Operation: ydbast.SecretDrop, Schema: "ext", Name: "pw"},
			wantErr: "DROP SECRET ext/pw, which requires target capability secrets, unavailable on this ydb target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := secretRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: test.caps}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(capability.Secrets))
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestSecretHandler_FailurePath refuses an operation that could only be
// written with a value Ptah would have nowhere to define from, and a target
// other than YDB, which has no secret Ptah models.
func TestSecretHandler_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		value   *ydbast.Secret
		wantErr string
	}{
		{name: "a creation naming no variable", target: "ydb", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pw"},
			wantErr: `.*secret pw: invalid value_env: a secret names the environment variable that holds its value`},
		{name: "a rotation naming a variable outside the prefix", target: "ydb",
			value:   &ydbast.Secret{Operation: ydbast.SecretRotate, Name: "pw", ValueEnv: "AWS_SECRET_ACCESS_KEY"},
			wantErr: `.*secret pw: invalid value_env: "AWS_SECRET_ACCESS_KEY" does not start with PTAH_SECRET_ .*`},
		{name: "a drop naming a variable", target: "ydb", value: &ydbast.Secret{Operation: ydbast.SecretDrop, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantErr: `.*dropping secret pw names no variable`},
		{name: "a path in the name", target: "ydb", value: &ydbast.Secret{Operation: ydbast.SecretDrop, Name: "ext/pw"},
			wantErr: `.*a secret requires a schema-scoped YDB identity`},
		{name: "another target", target: "postgres", value: &ydbast.Secret{Operation: ydbast.SecretCreate, Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantErr: `.*secrets require YDB`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := secretRegistry(c).Render(renderer.ExtensionContext{Target: test.target, Capabilities: capability.YDB262()}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}
