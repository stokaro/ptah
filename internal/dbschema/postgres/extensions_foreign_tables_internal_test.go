package postgres

// White-box testing required: the foreign table read is package-local and
// its result is carried inside an extension observation.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

func TestReadExtensionForeignTables_FindsWrapperDependents(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(strings.Contains(query, "JOIN pg_foreign_table ft"), qt.IsTrue)
		c.Assert(strings.Contains(query, "d.deptype = 'e'"), qt.IsTrue)
		return dbtest.QueryResult{Columns: []string{"extension", "schema", "table"}, Rows: [][]driver.Value{
			{"postgres_fdw", "public", "external_events"},
			{"postgres_fdw", "archive", "old_events"},
		}}, nil
	})
	reader := NewPostgreSQLReader(db.SQL, "public")

	tables, err := reader.readExtensionForeignTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(tables["postgres_fdw"], qt.DeepEquals, []string{"public.external_events", "archive.old_events"})
}
