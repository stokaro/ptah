package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// TestCockroachDBRenderer_RoleOptions_HappyPath pins the role options the
// renderer writes for CockroachDB: every option the engine takes, and neither
// SUPERUSER nor INHERIT in any spelling, because CockroachDB v26.3.2 refuses
// both words as a syntax error.
func TestCockroachDBRenderer_RoleOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a role with the defaults",
			node: ast.NewCreateRole("app_reader"),
			want: "CREATE ROLE \"app_reader\" WITH NOLOGIN NOCREATEDB NOCREATEROLE NOREPLICATION;\n",
		},
		{
			name: "a role with every option CockroachDB takes",
			node: ast.NewCreateRole("ops").SetLogin(true).
				SetCreateDB(true).SetCreateRole(true).SetReplication(true),
			want: "CREATE ROLE \"ops\" WITH LOGIN CREATEDB CREATEROLE REPLICATION;\n",
		},
		{
			name: "an option change CockroachDB takes",
			node: ast.NewAlterRole("ops").AddOperation(ast.NewSetLoginOperation(false)).
				AddOperation(ast.NewSetCreateDBOperation(true)),
			want: "ALTER ROLE \"ops\" NOLOGIN;\nALTER ROLE \"ops\" CREATEDB;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(capability.CockroachDB263(), platform.CockroachDB)

			got, err := renderer.Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCockroachDBRenderer_RoleOptions_FailurePath refuses what leaving the two
// words out would not honor: a superuser, a role that does not inherit, and a
// change to either.
func TestCockroachDBRenderer_RoleOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{
			name: "a superuser",
			node: ast.NewCreateRole("boss").SetSuperuser(true),
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE boss: declares SUPERUSER, ` +
				`which CockroachDB has no role option for: .*`,
		},
		{
			name:    "a role that does not inherit",
			node:    ast.NewCreateRole("child").SetInherit(false),
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE child: declares NOINHERIT, .*`,
		},
		{
			name:    "both at once",
			node:    ast.NewCreateRole("both").SetSuperuser(true).SetInherit(false),
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE both: declares SUPERUSER, NOINHERIT, .*`,
		},
		{
			name: "a superuser change",
			node: ast.NewAlterRole("ops").AddOperation(ast.NewSetSuperuserOperation(false)),
			wantErr: `unsupported feature: cockroachdb: ALTER ROLE ops: cannot change SUPERUSER, ` +
				`which CockroachDB has no role option for: a CockroachDB role is a superuser only as a member of admin, .*`,
		},
		{
			name:    "an inherit change",
			node:    ast.NewAlterRole("ops").AddOperation(ast.NewSetInheritOperation(true)),
			wantErr: `unsupported feature: cockroachdb: ALTER ROLE ops: cannot change INHERIT, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(capability.CockroachDB263(), platform.CockroachDB)

			got, err := renderer.Render(test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestPostgreSQLRenderer_RoleOptions_KeepsSuperuserAndInherit is the control:
// PostgreSQL has both options, and the CockroachDB rule does not reach it.
func TestPostgreSQLRenderer_RoleOptions_KeepsSuperuserAndInherit(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

	got, err := renderer.Render(ast.NewCreateRole("boss").SetSuperuser(true))

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "CREATE ROLE \"boss\" WITH NOLOGIN SUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION;\n")
}
