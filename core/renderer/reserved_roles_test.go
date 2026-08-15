package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/reservedrole"
)

// TestGetOrderedCreateStatementsRefusesAReservedRole is the regression for
// stokaro/ptah#1312 on the generate path.
//
// Whole-schema rendering and migration planning share this validation phase --
// migration/planner calls renderer.ValidateSchemaWithCapabilities before it
// emits an AST node -- so refusing here refuses before any statement exists,
// which is what the issue asks for. Without it, a declared reserved role is
// rendered as `CREATE ROLE "pg_monitor" WITH NOLOGIN ...`, a statement
// PostgreSQL 17.10 refuses at SQLSTATE 42939 whoever runs it.
func TestGetOrderedCreateStatementsRefusesAReservedRole(t *testing.T) {
	tests := []struct {
		name    string
		roles   []goschema.Role
		wantErr string
	}{
		{
			name:    "the reserved prefix",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor".*SQLSTATE 42939.*`,
		},
		{
			name:    "the bootstrap superuser",
			roles:   []goschema.Role{{Name: "postgres", Login: true}},
			wantErr: `.*declares reserved PostgreSQL role "postgres".*SQLSTATE 42710.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				&goschema.Database{Roles: test.roles},
				"postgres",
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)

			var renderErr *ptaherr.RenderError
			c.Assert(err, qt.ErrorAs, &renderErr)
		})
	}
}

// TestValidateSchemaRefusesAReservedRole covers the same phase through the
// entry point migration/planner uses, so the refusal is proven on the path that
// turns a diff into statements and not only on the whole-schema render.
func TestValidateSchemaRefusesAReservedRole(t *testing.T) {
	c := qt.New(t)

	err := renderer.ValidateSchema(
		&goschema.Database{Roles: []goschema.Role{{Name: "pg_monitor"}}},
		"postgres",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err.Error(), qt.Contains, reservedrole.AllowEnvVar)
}

// TestGetOrderedCreateStatementsStillRendersAnOrdinaryRole is the control: the
// statement the refusal exists to prevent is the only one it prevents.
func TestGetOrderedCreateStatementsStillRendersAnOrdinaryRole(t *testing.T) {
	tests := []struct {
		name string
		role goschema.Role
		want string
	}{
		{
			name: "an ordinary application role",
			role: goschema.Role{Name: "app_user", Login: true},
			want: `CREATE ROLE "app_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
`,
		},
		{
			name: "pgbouncer, whose underscore-free name is not the prefix",
			role: goschema.Role{Name: "pgbouncer", Login: true},
			want: `CREATE ROLE "pgbouncer" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				&goschema.Database{Roles: []goschema.Role{test.role}},
				"postgres",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestGetOrderedCreateStatementsOptInRendersTheReservedRoleAnyway keeps the
// capability reachable on the same surface, as AGENTS.md requires of a refusal
// that removes one.
func TestGetOrderedCreateStatementsOptInRendersTheReservedRoleAnyway(t *testing.T) {
	c := qt.New(t)

	c.Setenv(reservedrole.AllowEnvVar, "1")

	statements, err := renderer.GetOrderedCreateStatements(
		&goschema.Database{Roles: []goschema.Role{{Name: "postgres"}}},
		"postgres",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, `CREATE ROLE "postgres"`)
}
