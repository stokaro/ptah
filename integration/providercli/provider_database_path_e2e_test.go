//go:build integration

package providercli_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The catalog keeps the opened database binding even when a scope contains no
// objects. Planning must not infer that binding from a table or a URL later.
func TestProviderInspectionRetainsEachSessionDatabase(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	for range 2 {
		connection, address := transactionConnection(c, host, "")
		parsed, err := url.Parse(address)
		c.Assert(err, qt.IsNil)
		assertScopedDatabaseBinding(c, connection, parsed.Path, 0)
		c.Assert(connection.SchemaWriter().ExecuteSQL(c.Context(), `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
		assertScopedDatabaseBinding(c, connection, parsed.Path, 1)
	}
}

func assertScopedDatabaseBinding(c *qt.C, connection *dbschema.DatabaseConnection, database string, tables int) {
	c.Helper()
	for _, scopes := range [][]string{nil, {"main"}} {
		observed, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), connection, scopes)
		c.Assert(err, qt.IsNil)
		c.Assert(observed.Tables, qt.HasLen, tables)
		c.Assert(observed.DatabasePath, qt.Equals, database)
	}
}
