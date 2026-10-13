package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// A backing index is identified by conindid even when its name differs from
// the constraint. The same relation name in another schema stays independent.
func TestReadSchemaContext_ExclusionIndexOwnership(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, exclusionIndexCatalog())
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres18())
	reader.SetSchemas([]string{"public", "tenant"})
	database, err := reader.ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(database.Indexes, qt.HasLen, 2)
	c.Assert(database.Indexes[0].Name, qt.Equals, "slots_index")
	c.Assert(database.Indexes[0].ManagedBy, qt.DeepEquals, pgtable.ExclusionRef("slots", "ex_r", identifier.ForDialect("postgres")))
	c.Assert(database.Indexes[0].Comment, qt.Equals, "Index metadata")
	c.Assert(database.Indexes[1].Schema, qt.Equals, "tenant")
	c.Assert(database.Indexes[1].ManagedBy, qt.DeepEquals, objectidentity.ID{})
}

func exclusionIndexCatalog() dbtest.QueryHandler {
	base := deferredExcludeCatalog(false, false, "EXCLUDE USING btree (r WITH =)")
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "FROM pg_index ix") {
			return dbtest.QueryResult{
				Columns: []string{"schema", "table", "name", "definition", "keys", "attnums", "opclasses", "options", "hidden", "include", "method", "storage", "requires", "comment", "predicate", "primary", "unique", "attached"},
				Rows:    [][]driver.Value{{args[0].Value, "slots", "slots_index", "CREATE INDEX slots_index ON slots USING btree (r)", `["r"]`, `[1]`, `[]`, `[0]`, `[]`, `[]`, "btree", `[]`, `[]`, "Index metadata", "", false, false, false}},
			}, nil
		}
		result, err := base(query, args)
		if strings.Contains(query, "c.contype IN ('x')") {
			if args[0].Value != "public" {
				result.Rows = nil
			} else {
				result.Rows[0][9] = "slots_index"
			}
		}
		return result, err
	}
}
