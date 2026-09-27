//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/reservedrole"
)

// TestRoleScope_LiveDescribesTheRoleAGrantNames holds a schema-scoped read to
// the rule the role scoping exists for: a description defines every role it
// names.
//
// Each row gives one role exactly one link to the inspected schema, a grant of
// one kind. The role scoping reads the catalog place each kind of grant is
// kept in, so a kind it does not read leaves its grantee out: the description
// grants to a role it never creates, and applying it to a server without that
// role fails. Routine grants live in pg_proc.proacl and column grants in
// pg_attribute.attacl, and neither was read (stokaro/ptah#3837). CockroachDB
// takes its grants from information_schema, and refuses column privileges.
//
// The table grant rows are the control: that kind was read, so a row failing
// there is a broken fixture rather than this defect. A second role with no
// grant stays out of the description, so the rule is not met by describing
// every role on the server.
func TestRoleScope_LiveDescribesTheRoleAGrantNames(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		// setup creates the object and grants on it. %[1]s is the schema and
		// %[2]s the grantee, both quoted.
		setup []string
	}{
		{
			name:   "postgres function",
			engine: dbtarget.PostgreSQL,
			setup: []string{
				"CREATE FUNCTION %[1]s.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
				"GRANT EXECUTE ON FUNCTION %[1]s.f() TO %[2]s",
			},
		},
		{
			name:   "postgres procedure",
			engine: dbtarget.PostgreSQL,
			setup: []string{
				"CREATE PROCEDURE %[1]s.p() LANGUAGE sql AS 'SELECT 1'",
				"GRANT EXECUTE ON PROCEDURE %[1]s.p() TO %[2]s",
			},
		},
		{
			name:   "postgres column",
			engine: dbtarget.PostgreSQL,
			setup: []string{
				"CREATE TABLE %[1]s.t (id int, secret text)",
				"GRANT SELECT (id) ON %[1]s.t TO %[2]s",
			},
		},
		{
			name:   "postgres table",
			engine: dbtarget.PostgreSQL,
			setup: []string{
				"CREATE TABLE %[1]s.t (id int)",
				"GRANT SELECT ON %[1]s.t TO %[2]s",
			},
		},
		{
			name:   "cockroachdb function",
			engine: dbtarget.CockroachDB,
			setup: []string{
				"CREATE FUNCTION %[1]s.f() RETURNS INT8 LANGUAGE SQL AS 'SELECT 1'",
				"GRANT EXECUTE ON FUNCTION %[1]s.f() TO %[2]s",
			},
		},
		{
			name:   "cockroachdb table",
			engine: dbtarget.CockroachDB,
			setup: []string{
				"CREATE TABLE %[1]s.t (id INT8 PRIMARY KEY)",
				"GRANT SELECT ON TABLE %[1]s.t TO %[2]s",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			defer cancel()
			fixture := newRoleScopeFixture(c, ctx, test.engine, test.setup)

			conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{fixture.schema})

			c.Assert(err, qt.IsNil)
			c.Assert(rolesNamedByGrants(live.Grants), qt.Contains, fixture.grantee,
				qt.Commentf("the grant is not described, so the row measures nothing"))
			c.Assert(roleNamesOf(live.Roles), qt.Contains, fixture.grantee)
			c.Assert(roleNamesOf(live.Roles), qt.Not(qt.Contains), fixture.bystander)
			c.Assert(undefinedRoles(live), qt.HasLen, 0)
		})
	}
}

// roleScopeFixture is a schema holding one granted object, the role the grant
// goes to, and a role with no link to the schema.
type roleScopeFixture struct {
	schema    string
	grantee   string
	bystander string
}

// newRoleScopeFixture creates the fixture on engine and registers its removal.
// The names are unique to the run because roles are cluster-wide.
func newRoleScopeFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine, setup []string) roleScopeFixture {
	c.Helper()
	db, err := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	fixture := roleScopeFixture{
		schema:    fmt.Sprintf("ptah_rolescope_%d", stamp),
		grantee:   fmt.Sprintf("ptah_rolescope_grantee_%d", stamp),
		bystander: fmt.Sprintf("ptah_rolescope_bystander_%d", stamp),
	}
	schema := pgx.Identifier{fixture.schema}.Sanitize()
	grantee := pgx.Identifier{fixture.grantee}.Sanitize()
	bystander := pgx.Identifier{fixture.bystander}.Sanitize()
	c.Cleanup(func() {
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
			"DROP ROLE IF EXISTS " + grantee,
			"DROP ROLE IF EXISTS " + bystander,
		} {
			_, cleanupErr := db.ExecContext(context.Background(), statement)
			c.Check(cleanupErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
	})
	statements := []string{
		"CREATE ROLE " + grantee,
		"CREATE ROLE " + bystander,
		"CREATE SCHEMA " + schema,
	}
	for _, statement := range setup {
		statements = append(statements, fmt.Sprintf(statement, schema, grantee))
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return fixture
}

// rolesNamedByGrants lists the roles the grants a description carries name,
// grantee and grantor, sorted and without duplicates. An implicit grant is
// left out, because a description leaves it out.
func rolesNamedByGrants(grants []catalog.Grant) []string {
	var names []string
	for _, grant := range grants {
		if grant.Implicit {
			continue
		}
		names = append(names, grant.Role, grant.GrantedBy)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// undefinedRoles lists the roles the grants of a description name that it does
// not define. A name that is not a role Ptah describes is left out: PUBLIC,
// the reserved PostgreSQL roles, and CockroachDB's built-in admin and root.
func undefinedRoles(live *catalog.Database) []string {
	described := roleNamesOf(live.Roles)
	return slices.DeleteFunc(rolesNamedByGrants(live.Grants), func(name string) bool {
		return name == "" || name == "PUBLIC" || name == "admin" || name == "root" ||
			reservedrole.Is(name) || slices.Contains(described, name)
	})
}
