package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// TestCockroachDBRoles_HappyPath renders declared roles for CockroachDB
// through the whole-schema path. CockroachDB v26.3.2 refuses NOSUPERUSER and
// INHERIT as a syntax error, so a statement carrying either created no role.
func TestCockroachDBRoles_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		role schemamodel.Role
		want string
	}{
		{
			name: "a login role",
			role: schemamodel.Role{Name: "app_user", Login: true, Inherit: true},
			want: "CREATE ROLE \"app_user\" WITH LOGIN NOCREATEDB NOCREATEROLE NOREPLICATION;\n",
		},
		{
			name: "a role with every option CockroachDB takes",
			role: schemamodel.Role{Name: "ops", CreateDB: true, CreateRole: true, Replication: true, Inherit: true},
			want: "CREATE ROLE \"ops\" WITH NOLOGIN CREATEDB CREATEROLE REPLICATION;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(
				&schemamodel.Database{Roles: []schemamodel.Role{test.role}},
				"cockroachdb",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestCockroachDBRoles_PostgreSQLKeepsBothAttributes is the control for the
// refusal below: the same declarations validate on PostgreSQL, which has both
// options.
func TestCockroachDBRoles_PostgreSQLKeepsBothAttributes(t *testing.T) {
	c := qt.New(t)

	err := builtin.ValidateSchema(&schemamodel.Database{Roles: []schemamodel.Role{
		{Name: "boss", Superuser: true, Inherit: true},
		{Name: "child"},
	}}, "postgres")

	c.Assert(err, qt.IsNil)
}

// TestCockroachDBRoles_FailurePath refuses a declared attribute CockroachDB has
// no role option for, in the validation phase whole-schema rendering and
// migration planning both run before they emit a statement.
func TestCockroachDBRoles_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		roles   []schemamodel.Role
		wantErr string
	}{
		{
			name:  "a superuser",
			roles: []schemamodel.Role{{Name: "boss", Superuser: true, Inherit: true}},
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE boss: declares SUPERUSER, ` +
				`which CockroachDB has no role option for: a CockroachDB role is a superuser only as a member of admin, ` +
				`and always inherits the privileges of the roles granted to it`,
		},
		{
			name:    "a role that does not inherit",
			roles:   []schemamodel.Role{{Name: "child"}},
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE child: declares NOINHERIT, .*`,
		},
		{
			name: "the lexicographically first of several",
			roles: []schemamodel.Role{
				{Name: "zeta", Superuser: true, Inherit: true},
				{Name: "fine", Inherit: true},
				{Name: "alpha"},
			},
			wantErr: `unsupported feature: cockroachdb: CREATE ROLE alpha: declares NOINHERIT, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchema(&schemamodel.Database{Roles: test.roles}, "cockroachdb")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
