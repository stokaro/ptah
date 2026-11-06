package postgres

// White-box testing required: the property under test is a predicate in the
// text of the catalog queries that find a relation by its name, and a fake
// server answers a query the same way whatever predicate it carries. Only the
// query the reader issued says whether its lookup can find an index, and the
// readers that issue them are unexported. The behavior on a live CockroachDB is
// integration/dbschema/postgres/cockroachdb_index_table_name_live_test.go.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestRelationByName_EveryNameLookupLeavesIndexesOut pins the predicate on
// each read that joins pg_class to an information_schema name.
//
// CockroachDB names an index per table and lists it in pg_class under the
// table's schema, so a table and an index of another table can share a name.
// A lookup by namespace and name alone then finds both rows: the table read
// returns the table twice and the read fails with `duplicate feature
// identity: subject coverage for table public.i`, and the column and grant
// reads describe the table's columns and grants twice (stokaro/ptah#5043).
func TestRelationByName_EveryNameLookupLeavesIndexesOut(t *testing.T) {
	c := qt.New(t)
	cockroachTableRead := issuedQueries(c, platform.CockroachDB, capability.CockroachDB263(), func(r *Reader) error {
		_, err := r.readTablesForSchema(c.Context(), "public")
		return err
	})
	cockroachGrantRead := issuedQueries(c, platform.CockroachDB, capability.CockroachDB263(), func(r *Reader) error {
		_, err := r.readCockroachRelationGrants(c.Context(), "public")
		return err
	})
	postgresTableRead := issuedQueries(c, platform.Postgres, capability.Postgres18(), func(r *Reader) error {
		_, err := r.readTablesForSchema(c.Context(), "public")
		return err
	})
	postgresGrantRead := issuedQueries(c, platform.Postgres, capability.Postgres18(), func(r *Reader) error {
		_, err := r.readTableGrantsForSchema(c.Context(), "public")
		return err
	})
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "tables",
			query: queryContaining(c, cockroachTableRead, "FROM information_schema.tables t"),
			want:  "c.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "columns",
			query: queryContaining(c, cockroachTableRead, "FROM information_schema.columns col"),
			want:  "cls.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "constraints",
			query: NewPostgreSQLWireReaderWithCapabilities(nil, "public", platform.CockroachDB, capability.CockroachDB263()).basicConstraintsQuery(),
			want:  "constraint_table.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "CockroachDB relation grants",
			query: queryContaining(c, cockroachGrantRead, "FROM information_schema.role_table_grants g"),
			want:  "c.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "CockroachDB roles in scope",
			query: cockroachRolesInScopeClauses()[0],
			want:  "c.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "PostgreSQL tables",
			query: queryContaining(c, postgresTableRead, "AS row_stats_unknown"),
			want:  "c.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "PostgreSQL columns",
			query: queryContaining(c, postgresTableRead, "AS udt_schema"),
			want:  "cls.relkind NOT IN ('i', 'I')",
		},
		{
			name:  "PostgreSQL table grants",
			query: queryContaining(c, postgresGrantRead, "AS with_option"),
			want:  "c.relkind NOT IN ('i', 'I')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.query, qt.Contains, test.want)
		})
	}
}

// issuedQueries runs read through a reader of dialect and caps over a fake
// server that answers every query with no rows, and the relation-size probe
// with false, and returns every query the read issued.
func issuedQueries(c *qt.C, dialect string, caps capability.Capabilities, read func(*Reader) error) []string {
	c.Helper()
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		if strings.Contains(query, relationSizeProbeMarker) {
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{false}}}, nil
		}
		return dbtest.QueryResult{}, nil
	})
	c.Assert(read(NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", dialect, caps)), qt.IsNil)
	return queries
}

// queryContaining returns the one query in queries that contains marker.
func queryContaining(c *qt.C, queries []string, marker string) string {
	c.Helper()
	var found []string
	for _, query := range queries {
		if strings.Contains(query, marker) {
			found = append(found, query)
		}
	}
	c.Assert(found, qt.HasLen, 1, qt.Commentf("queries containing %q", marker))
	return found[0]
}
