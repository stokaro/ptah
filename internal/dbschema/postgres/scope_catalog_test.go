package postgres_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/pgsystemschema"
)

// TestScopeCatalog_ServesEveryMemberOfTheFamily pins where the PostgreSQL
// family's writer offers its scope catalog: on every dialect it serves, in
// any spelling, and on a writer built without a dialect. None of those
// dialects states a CleanCheck.Tables query, which the clean check would
// otherwise read as a second answer about the same tables.
func TestScopeCatalog_ServesEveryMemberOfTheFamily(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, "postgresql", platform.CockroachDB, platform.YugabyteDB, platform.Spanner, ""} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			described, _ := platform.Lookup(dialect)

			c.Assert(postgresWriter(dialect).ScopeCatalog(), qt.IsNotNil)
			c.Assert(described.CleanCheck.Tables, qt.Equals, "")
		})
	}
}

// scopeReads answers every read with the same columns and rows, and records
// each query and its arguments.
type scopeReads struct {
	columns []string
	rows    [][]driver.Value
	err     error
	queries []string
	args    [][]any
}

func (r *scopeReads) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	r.queries = append(r.queries, query)
	values := make([]any, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	r.args = append(r.args, values)
	return dbtest.QueryResult{Columns: r.columns, Rows: r.rows}, r.err
}

// TestScopeCatalog_Tables_HappyPath reads the base tables of the schema the
// URL pinned, or of the session's current schema for the empty one, and
// counts the tables the pinned binary counts: never a partition, read so
// that CockroachDB's NULL relispartition keeps its tables, and never a table
// an extension owns where the server answers pg_depend.
func TestScopeCatalog_Tables_HappyPath(t *testing.T) {
	tests := []struct {
		name               string
		caps               capability.Capabilities
		schema             string
		wantDependencyRead bool
	}{
		{
			name:               "a server that answers pg_depend",
			caps:               capability.Of(map[capability.Capability]bool{capability.CatalogDependencies: true}),
			schema:             "app",
			wantDependencyRead: true,
		},
		{
			name:               "a server that does not",
			caps:               capability.Capabilities{},
			schema:             "",
			wantDependencyRead: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reads := &scopeReads{
				columns: []string{"nspname", "relname"},
				rows:    [][]driver.Value{{"app", "orders"}, {"app", "customers"}},
			}
			db := dbtest.Open(c, reads.answer)

			tables, err := postgresWriter(platform.Postgres).ScopeCatalog().Tables(c.Context(), db.SQL, test.caps, test.schema)

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.DeepEquals, []dbreset.ScopeTable{{Schema: "app", Name: "orders"}, {Schema: "app", Name: "customers"}})
			c.Assert(reads.args, qt.DeepEquals, [][]any{{test.schema}})
			c.Assert(reads.queries, qt.HasLen, 1)
			c.Assert(reads.queries[0], qt.Contains, "WHERE n.nspname = COALESCE(NULLIF($1, ''), current_schema())")
			c.Assert(reads.queries[0], qt.Contains, "c.relkind IN ('r', 'p')")
			c.Assert(reads.queries[0], qt.Contains, "c.relispartition IS NOT TRUE")
			c.Assert(strings.Contains(reads.queries[0], "FROM pg_depend d"), qt.Equals, test.wantDependencyRead)
		})
	}
}

// TestScopeCatalog_Realm_HappyPath lists every schema of the database with
// its tables, a schema holding none once with no table, and leaves out the
// system schemas of the dialect the writer is for: CockroachDB and
// YugabyteDB own schemas PostgreSQL does not.
func TestScopeCatalog_Realm_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForDialect(dialect)
			reads := &scopeReads{
				columns: []string{"nspname", "relname"},
				rows:    [][]driver.Value{{"extra", ""}, {"public", "orders"}, {"public", "customers"}},
			}
			db := dbtest.Open(c, reads.answer)

			tables, err := postgresWriter(dialect).ScopeCatalog().Realm(c.Context(), db.SQL, caps)

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.DeepEquals, []dbreset.ScopeTable{
				{Schema: "extra"}, {Schema: "public", Name: "orders"}, {Schema: "public", Name: "customers"},
			})
			c.Assert(reads.args, qt.DeepEquals, [][]any{{}})
			c.Assert(reads.queries, qt.HasLen, 1)
			c.Assert(reads.queries[0], qt.Contains, "LEFT JOIN pg_class c")
			c.Assert(reads.queries[0], qt.Contains, "c.relispartition IS NOT TRUE")
			c.Assert(reads.queries[0], qt.Contains, "WHERE "+pgsystemschema.PostgresDescribedSchemasPredicate(dialect, caps))
		})
	}
}

// TestScopeCatalog_RealmSchemas_HappyPath lists every schema of the database
// in the order the catalog returns them, through the predicate Realm reads.
func TestScopeCatalog_RealmSchemas_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForDialect(dialect)
			reads := &scopeReads{columns: []string{"nspname"}, rows: [][]driver.Value{{"public"}, {"extra"}}}
			db := dbtest.Open(c, reads.answer)

			names, err := postgresWriter(dialect).ScopeCatalog().RealmSchemas(c.Context(), db.SQL, caps)

			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, []string{"public", "extra"})
			c.Assert(reads.queries, qt.HasLen, 1)
			c.Assert(reads.queries[0], qt.Contains, "FROM pg_namespace n")
			c.Assert(reads.queries[0], qt.Contains, "WHERE "+pgsystemschema.PostgresDescribedSchemasPredicate(dialect, caps))
		})
	}
}

// TestScopeCatalog_FailurePath hands back the error a read failed with,
// unwrapped, and no answer: the clean check and the realm listing word it.
func TestScopeCatalog_FailurePath(t *testing.T) {
	refused := errors.New("permission denied for table pg_class")
	caps := capability.ForDialect(platform.Postgres)

	t.Run("tables", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		tables, err := postgresWriter(platform.Postgres).ScopeCatalog().Tables(c.Context(), db.SQL, caps, "public")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(tables, qt.IsNil)
	})
	t.Run("realm", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		tables, err := postgresWriter(platform.Postgres).ScopeCatalog().Realm(c.Context(), db.SQL, caps)

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(tables, qt.IsNil)
	})
	t.Run("realm schemas", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		names, err := postgresWriter(platform.Postgres).ScopeCatalog().RealmSchemas(c.Context(), db.SQL, caps)

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(names, qt.IsNil)
	})
}
