package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
)

// scopedSchema is one table every target can host plus one PostgreSQL-only
// function, a role no MySQL-family target will plan, and an extension, all
// scoped to postgres.
func scopedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Tenant", Name: "tenants"}},
		Fields: []goschema.Field{{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true}},
		Extensions: []goschema.Extension{
			{Name: "pgcrypto", Dialects: []string{"postgres"}},
		},
		Functions: []goschema.Function{{
			StructName: "Fn",
			Name:       "get_current_tenant_id",
			Returns:    "TEXT",
			Language:   "plpgsql",
			Body:       "BEGIN RETURN 'x'; END;",
			Dialects:   []string{"postgres"},
		}},
		Roles: []goschema.Role{
			{StructName: "Rol", Name: "app_reader", Inherit: true, Dialects: []string{"postgres"}},
		},
	}
}

// TestGetOrderedCreateStatements_AScopedObjectIsAbsentFromTargetsItDoesNotName
// holds the render half of the scope.
//
// Absent is stronger than skipped, and the difference is the whole point. The
// MySQL-family renderer used to answer a plpgsql function with a named comment,
// which reads well and converges never: the comparator kept asking for the
// function, so `schema apply` planned the same comment forever. Here the object
// is not part of this target's schema at all, so there is nothing to comment
// on.
func TestGetOrderedCreateStatements_AScopedObjectIsAbsentFromTargetsItDoesNotName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		absent  bool
	}{
		{name: "the dialect the scope names", dialect: "postgres", absent: false},
		{name: "an accepted spelling of the dialect the scope names", dialect: "postgresql", absent: false},
		{name: "mysql", dialect: "mysql", absent: true},
		{name: "mariadb", dialect: "mariadb", absent: true},
		{name: "sqlite", dialect: "sqlite", absent: true},
		{name: "a postgres-family target the scope did not name", dialect: "cockroachdb", absent: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(scopedSchema(), test.dialect)
			c.Assert(err, qt.IsNil)

			rendered := strings.Join(statements, "\n")
			c.Assert(strings.Contains(rendered, "get_current_tenant_id"), qt.Equals, !test.absent)
			c.Assert(strings.Contains(rendered, "app_reader"), qt.Equals, !test.absent)
			c.Assert(strings.Contains(rendered, "pgcrypto"), qt.Equals, !test.absent)
			// The unscoped table is the control: it proves the projection
			// removed the scoped objects rather than the whole schema.
			c.Assert(rendered, qt.Contains, "tenants")
		})
	}
}

// TestGetOrderedCreateStatements_AnUnscopedSchemaRendersAsItAlwaysDid is the
// compatibility control for the render seam.
//
// Every schema that exists today is unscoped, so the projection must be
// invisible to all of them. The two behaviors asserted here are exactly the two
// the scope replaces, and both must survive where no scope was written: the
// named skip comment that never converges, and, on the next line down, the
// refusal that stops the render outright.
func TestGetOrderedCreateStatements_AnUnscopedSchemaRendersAsItAlwaysDid(t *testing.T) {
	c := qt.New(t)

	unscoped := scopedSchema()
	unscoped.Extensions[0].Dialects = nil
	unscoped.Functions[0].Dialects = nil
	unscoped.Roles = nil

	statements, err := renderer.GetOrderedCreateStatements(unscoped, "mysql")
	c.Assert(err, qt.IsNil)

	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "get_current_tenant_id")
	c.Assert(rendered, qt.Contains, "which this target does not run; skipped")
}

// TestGetOrderedCreateStatements_AnUnscopedRoleStillRefusesTheMySQLFamily pins
// the refusal a scope is the only way to avoid.
//
// A declared role is not skipped on MySQL or MariaDB, it stops the render:
// Ptah neither reads nor compares MySQL-family role state, so planning a
// CREATE ROLE would never converge and refusing is the honest answer. That
// refusal is also what made one schema across postgres and mariadb impossible
// before a declaration could say which targets a role is for -- so it must
// still fire for a role that names none.
func TestGetOrderedCreateStatements_AnUnscopedRoleStillRefusesTheMySQLFamily(t *testing.T) {
	c := qt.New(t)

	unscoped := scopedSchema()
	unscoped.Extensions[0].Dialects = nil
	unscoped.Functions[0].Dialects = nil
	unscoped.Roles[0].Dialects = nil

	_, err := renderer.GetOrderedCreateStatements(unscoped, "mysql")

	c.Assert(err, qt.ErrorMatches, `.*CREATE ROLE app_reader.*`)
}
