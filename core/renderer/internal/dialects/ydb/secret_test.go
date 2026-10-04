package ydb_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// TestRender_Secret_HappyPath pins how a secret is written: its value is the
// named expression for its variable and never a literal, so the rendered text
// holds no value whatever the environment holds. Each rendering was applied to
// local-ydb 26.2.1.14 through the connection, which defined the value.
func TestRender_Secret_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a secret at the root", node: ast.NewCreateSecret("pg_password", "PTAH_SECRET_PG_PASSWORD"),
			want: "CREATE SECRET `pg_password` WITH (value = $PTAH_SECRET_PG_PASSWORD);\n"},
		{name: "a secret in a directory", node: ast.NewCreateSecret("ext.s3", "PTAH_SECRET_S3"),
			want: "CREATE SECRET `ext/s3` WITH (value = $PTAH_SECRET_S3);\n"},
		{name: "a rotation", node: ast.NewAlterSecret("ext.s3", "PTAH_SECRET_S3"),
			want: "ALTER SECRET `ext/s3` WITH (value = $PTAH_SECRET_S3);\n"},
		{name: "a drop", node: ast.NewDropSecret("ext.s3"), want: "DROP SECRET `ext/s3`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_Secret_RefusesByCapability names the key a line lacks: 25.1 and
// 25.2 have no CREATE SECRET, and 25.3 has it behind a flag that is off.
func TestRender_Secret_RefusesByCapability(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{name: "a creation on 25.1", caps: capability.YDB251(), node: ast.NewCreateSecret("pw", "PTAH_SECRET_PW"),
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a rotation on 25.3", caps: capability.YDB253(), node: ast.NewAlterSecret("pw", "PTAH_SECRET_PW"),
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a drop on 25.2", caps: capability.YDB252(), node: ast.NewDropSecret("pw"),
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(capability.Secrets))
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_Secret_FailurePath refuses a node that could only be written with
// a value Ptah would have nowhere to define from.
func TestRender_Secret_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "a creation naming no variable", node: ast.NewCreateSecret("pw", ""),
			wantErr: "a secret: it names no environment variable to take its value from"},
		{name: "a rotation naming a variable outside the prefix", node: ast.NewAlterSecret("pw", "AWS_SECRET_ACCESS_KEY"),
			wantErr: `secret pw: invalid value_env: "AWS_SECRET_ACCESS_KEY" does not start with PTAH_SECRET_ .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
