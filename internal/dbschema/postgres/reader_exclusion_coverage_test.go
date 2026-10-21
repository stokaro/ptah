package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// TestReadSchemaContext_ExclusionCoverageWithoutCatalogFunctions pins what a
// read records for EXCLUDE constraints when the server cannot query
// pg_constraint. Spanner holds none, so its namespace is described, and an
// HCL export of it has no gap to refuse. PostgreSQL may hold some, so with the
// same capabilities its namespace stays uninspected and cannot authorize a
// drop.
func TestReadSchemaContext_ExclusionCoverageWithoutCatalogFunctions(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		want    schemaext.KnowledgeState
	}{
		{name: "Spanner holds no EXCLUDE constraint", dialect: platform.Spanner, want: schemaext.Complete},
		{name: "PostgreSQL may hold one", dialect: platform.Postgres, want: schemaext.Uninspected},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, emptyCatalog)
			reader := postgres.NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", test.dialect, capability.SpannerPostgres())

			database, err := reader.ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			records := database.FeatureCoverage.SelectKinds([]schemaext.Kind{pgtable.ExclusionKind}).KindRecords()
			c.Assert(records, qt.HasLen, 1)
			c.Assert(records[0].Knowledge.Unscoped().State, qt.Equals, test.want)
		})
	}
}

// emptyCatalog answers every query with no rows, except the one probe the
// reader scans a single row from.
func emptyCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, "p.proname = 'pg_relation_size'") {
		return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
	}
	return dbtest.QueryResult{}, nil
}
