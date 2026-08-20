package postgres_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/dbschema/postgres"
)

// cleanupQueryFor captures the catalog query DropAllTables sends for a given
// capability set.
//
// The query is the subject rather than the drops: a relation the server does
// not have is a PARSE failure, so a cleanup that names one drops nothing at all
// rather than missing one object (stokaro/ptah#1811).
func cleanupQueryFor(c *qt.C, caps capability.Capabilities) string {
	c.Helper()
	var queries []string
	db := dbtest.OpenWithExec(c.TB.(*testing.T), func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return postgresCleanupQuery(query, args)
	}, func(string, []driver.NamedValue) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public", caps)

	c.Assert(writer.DropAllTables(c.TB.Context()), qt.IsNil)
	c.Assert(len(queries) >= 3, qt.IsTrue)
	return queries[2]
}

// fullCatalogCaps is a server whose catalog answers everything the cleanup can
// ask, which is every PostgreSQL-family engine except Spanner.
func fullCatalogCaps() capability.Capabilities {
	return capability.Capabilities{
		capability.DDLInsideTransaction:     true,
		capability.CatalogRecursiveCTE:      true,
		capability.CatalogDependencies:      true,
		capability.CatalogDefaultPrivileges: true,
		capability.PostgresCatalogFunctions: true,
	}
}

// TestCleanupQuery_OmitsRelationsTheCatalogLacks is the acceptance case.
//
// Each row names one relation and the capability that reports it. Turning the
// capability off must take the relation out of the statement entirely — not
// reduce it to a null column, which is what a missing FUNCTION allows and a
// missing RELATION does not.
func TestCleanupQuery_OmitsRelationsTheCatalogLacks(t *testing.T) {
	tests := []struct {
		name     string
		off      capability.Capability
		relation string
	}{
		{
			name:     "pg_depend",
			off:      capability.CatalogDependencies,
			relation: "pg_depend",
		},
		{
			name:     "pg_default_acl",
			off:      capability.CatalogDefaultPrivileges,
			relation: "pg_default_acl",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := fullCatalogCaps()

			with := cleanupQueryFor(c, caps)
			caps[test.off] = false
			without := cleanupQueryFor(c, caps)

			// Present when the server has it...
			c.Assert(with, qt.Contains, test.relation)
			// ...and gone, not merely unused, when it does not.
			c.Assert(without, qt.Not(qt.Contains), test.relation)
		})
	}
}

// TestCleanupQuery_KeepsEveryOtherBranchWhenOneIsOmitted is the control.
//
// Without it, a change that dropped the whole query on a missing capability
// would satisfy every assertion above. The branches named here are the ones a
// server without pg_depend still has.
func TestCleanupQuery_KeepsEveryOtherBranchWhenOneIsOmitted(t *testing.T) {
	c := qt.New(t)
	caps := fullCatalogCaps()
	caps[capability.CatalogDependencies] = false
	caps[capability.CatalogDefaultPrivileges] = false

	query := cleanupQueryFor(c, caps)

	for _, kept := range []string{
		"pg_class", "pg_namespace", "pg_extension", "pg_constraint",
		"pg_proc", "pg_type", "pg_collation", "cleanup_objects",
	} {
		c.Assert(query, qt.Contains, kept)
	}
	// The routine branch survives; only its extension filter is gone.
	c.Assert(query, qt.Contains, "p.prokind IN ('f', 'p', 'a', 'w')")
}

// TestNewPostgreSQLWriterForRunner_DeclaresTheCatalogItAssumes pins the default
// capability set the plain constructor hands out.
//
// The constructor's doc says every PostgreSQL-family engine except Spanner has
// these, and a Spanner caller passes its own set. Leaving a key out of that
// default silently removed the branch it gates from every caller using this
// constructor — which is most of them — on servers that do have the relation.
func TestNewPostgreSQLWriterForRunner_DeclaresTheCatalogItAssumes(t *testing.T) {
	c := qt.New(t)

	query := cleanupQueryForDefaultWriter(c)

	c.Assert(query, qt.Contains, "pg_depend")
	c.Assert(query, qt.Contains, "pg_default_acl")
}

// cleanupQueryForDefaultWriter is cleanupQueryFor through the constructor that
// chooses the capabilities itself.
func cleanupQueryForDefaultWriter(c *qt.C) string {
	c.Helper()
	var queries []string
	db := dbtest.OpenWithExec(c.TB.(*testing.T), func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return postgresCleanupQuery(query, args)
	}, func(string, []driver.NamedValue) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	c.Assert(writer.DropAllTables(c.TB.Context()), qt.IsNil)
	c.Assert(len(queries) >= 3, qt.IsTrue)
	return queries[2]
}
