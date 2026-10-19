package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// namespaceFixture are the schema names the fake catalog holds. Each is a
// user's schema on PostgreSQL 18; crdb_internal is CockroachDB's own and
// spanner_sys is Spanner's, as their descriptors state.
var namespaceFixture = []string{"app", "crdb_internal", "public", "spanner_sys"}

// answerNamespaces answers the user schema query with the fixture names the
// query does not leave out by name.
func answerNamespaces(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	var rows [][]driver.Value
	for _, name := range namespaceFixture {
		if !strings.Contains(query, "n.nspname <> '"+name+"'") {
			rows = append(rows, []driver.Value{name})
		}
	}
	return dbtest.QueryResult{Columns: []string{"nspname"}, Rows: rows}, nil
}

// TestUserSchemas_LeavesTheDialectsSystemSchemas pins the schemas a realm
// cleanup leaves to the server: the ones the writer's dialect owns, which is
// what the reader leaves out of a description too, and every member's for a
// writer that does not know its dialect.
func TestUserSchemas_LeavesTheDialectsSystemSchemas(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "PostgreSQL owns neither", dialect: platform.Postgres, want: []string{"app", "crdb_internal", "public", "spanner_sys"}},
		{name: "CockroachDB owns crdb_internal", dialect: platform.CockroachDB, want: []string{"app", "public", "spanner_sys"}},
		{name: "Spanner owns spanner_sys", dialect: platform.Spanner, want: []string{"app", "crdb_internal", "public"}},
		{name: "a writer without a dialect leaves both", dialect: "", want: []string{"app", "public"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, answerNamespaces)
			writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public", test.dialect,
				capability.Of(map[capability.Capability]bool{capability.CatalogDependencies: true}))

			schemas, err := writer.UserSchemas(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schemas, qt.DeepEquals, test.want)
		})
	}
}
