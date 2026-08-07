//go:build integration

package gonative_test

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/postgres"
)

// pgPrefixRole is a role name PostgreSQL does NOT reserve and the reader's
// system-role filter used to swallow.
//
// PostgreSQL reserves the prefix `pg_`, with the underscore. SQL LIKE reads a
// bare `_` as a single-character wildcard, so the pattern `pg_%` matches `pg`
// followed by any character: pgbouncer, pgadmin, pgpool, pguser. This name has
// the same shape and is unique enough to run beside other work on a shared
// cluster.
const pgPrefixRole = "pgb1291_probe"

// pgPrefixTable holds the grant, so the run covers both questions the filter is
// asked: which roles exist, and who holds privileges.
const pgPrefixTable = "pgb1291_granted"

// TestPostgreSQLReaderDescribesRolesNamedPgSomething pins that a user role whose
// name merely begins with `pg` is described.
//
// It is a live test because the defect is in SQL rather than in Go: measured on
// PostgreSQL 17.10,
//
//	SELECT 'pgbouncer' LIKE 'pg_%';    -- true
//	SELECT 'pgbouncer' LIKE 'pg\_%';   -- false
//
// so nothing short of asking a server can tell the two patterns apart. The role
// was invisible to the reader, which made the comparator read it as absent and
// plan CREATE ROLE against a database that already had it -- and grants held by
// it were invisible for the same reason, which is the more serious half given
// what a role like pgbouncer is usually granted (stokaro/ptah#1291).
func TestPostgreSQLReaderDescribesRolesNamedPgSomething(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	dropPgPrefixFixture(c, db)
	c.Cleanup(func() { dropPgPrefixFixture(c, db) })

	_, err = db.Exec(fmt.Sprintf("CREATE ROLE %q", pgPrefixRole))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %q (id integer PRIMARY KEY)", pgPrefixTable))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf("GRANT SELECT ON %q TO %q", pgPrefixTable, pgPrefixRole))
	c.Assert(err, qt.IsNil)

	// The catalog is asked directly first, so a reader that returns nothing
	// cannot be confused with a database that holds nothing.
	var seeded int
	c.Assert(db.QueryRow(
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", pgPrefixRole,
	).Scan(&seeded), qt.IsNil)
	c.Assert(seeded, qt.Equals, 1)

	schema, err := postgres.NewPostgreSQLReader(db, "public").ReadSchema()
	c.Assert(err, qt.IsNil)

	roleNames := make([]string, 0, len(schema.Roles))
	for _, role := range schema.Roles {
		roleNames = append(roleNames, role.Name)
	}
	c.Assert(slices.Contains(roleNames, pgPrefixRole), qt.IsTrue,
		qt.Commentf("reader described %d roles and none of them is %q", len(roleNames), pgPrefixRole))

	grantRefs := make([]string, 0, len(schema.Grants))
	for _, grant := range schema.Grants {
		grantRefs = append(grantRefs, grant.Role+" -> "+grant.ObjectName)
	}
	c.Assert(slices.Contains(grantRefs, pgPrefixRole+" -> "+pgPrefixTable), qt.IsTrue,
		qt.Commentf("reader described %d grants and none is %q on %q: %v",
			len(grantRefs), pgPrefixRole, pgPrefixTable, grantRefs))

	// The control in the other direction: a genuinely reserved role, which the
	// filter must still exclude. Without it this test would pass against a
	// reader that had simply stopped filtering.
	c.Assert(slices.ContainsFunc(roleNames, func(name string) bool {
		return len(name) > 3 && name[:3] == "pg_"
	}), qt.IsFalse,
		qt.Commentf("reader described a reserved pg_ role: %v", roleNames))
}

// dropPgPrefixFixture removes the fixture, tolerating its absence. The role is
// cluster-scoped, so it outlives the database and has to be dropped by name.
//
// The existence check is in SQL rather than in Go because DROP OWNED BY has no
// IF EXISTS form and fails outright on a role that is not there, which is the
// ordinary state on the first run. Every statement here is unconditional from
// the caller's side, so a failure is a real failure.
func dropPgPrefixFixture(c *qt.C, db *sql.DB) {
	c.Helper()

	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %q", pgPrefixTable))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(`
		DO $$ BEGIN
			IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
				EXECUTE 'DROP OWNED BY %q';
			END IF;
		END $$`, quoteSQLLiteral(pgPrefixRole), pgPrefixRole))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf("DROP ROLE IF EXISTS %q", pgPrefixRole))
	c.Assert(err, qt.IsNil)
}

// quoteSQLLiteral renders a name as a single-quoted SQL string. The fixture's
// names are constants in this file, so this only has to be correct for them.
func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
