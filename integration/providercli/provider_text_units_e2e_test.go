//go:build integration

package providercli_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	// The test creates the fixture's table through a driver of its own, so the
	// starting state does not depend on the provider under test.
	_ "modernc.org/sqlite"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/testutils"
)

// textUnitsFixture serves the operation-models program in mode, over a fixture
// database holding the table items, with a desired schema that adds a column
// the provider plans as a native operation.
func textUnitsFixture(c *qt.C, mode, schema string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "operation-models", testutils.FixtureRelease{})
	spec.Environment = []string{"FIXTURE_RENDER_MODE=" + mode}
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	c.Assert(os.WriteFile(f.schemaPath, []byte(schema), 0o600), qt.IsNil)
	return f
}

const addedColumnSchema = "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      added: {type: REAL}\n"

// generateMigration runs migrations generate against the fixture and returns
// the command's result and the directory it writes to.
func (f providerCLI) generateMigration(c *qt.C, extra ...string) (out, diagnostics string, code int, dir string) {
	c.Helper()
	dir = filepath.Join(filepath.Dir(f.schemaPath), "migrations")
	args := append([]string{"migrations", "generate", "--provider-config", f.manifestPath, "--db-url", f.address,
		"--schema-file", f.schemaPath, "--migrations-dir", dir}, extra...)
	out, diagnostics, code = executeProviderCLI(c.Context(), args)
	f.assertStopped(c)
	return out, diagnostics, code, dir
}

// upMigration returns the text of the one up migration in dir.
func upMigration(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	data, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(data)
}

// TestProviderTextUnits_HappyPath writes a migration file through a provider
// that plans the new column as a native operation and renders it as text when
// the host asks for text. The file holds the provider's statement, and the
// provider runs that statement read back from the file.
func TestProviderTextUnits_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := textUnitsFixture(c, "native-text", addedColumnSchema)

	_, diagnostics, code, dir := f.generateMigration(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	migration := upMigration(c, dir)
	c.Assert(migration, qt.Contains, `ALTER TABLE "items" ADD COLUMN "added" REAL;`)
	c.Assert(migration, qt.Not(qt.Contains), ";;")
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native-text"})
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	connection, err := dbschema.ConnectToDatabase(ctx, f.address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	c.Assert(connection.Writer().ExecuteSQL(ctx, `ALTER TABLE "items" ADD COLUMN "added" REAL;`), qt.IsNil)
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	items := observed.Tables[slices.IndexFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "items" })]
	columns := make([]string, 0, len(items.Columns))
	for _, column := range items.Columns {
		columns = append(columns, column.Name)
	}
	c.Assert(columns, qt.DeepEquals, []string{"id", "added"})
}

// TestProviderTextUnits_FailurePath refuses to write a migration file that
// would hold a native operation: for a provider that renders no text units,
// and for a change a provider that renders them performs only natively. It
// names the operation and writes no file.
func TestProviderTextUnits_FailurePath(t *testing.T) {
	rebuild := "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      flag: {type: INTEGER, not_null: true}\n"
	tests := []struct {
		name, mode, schema string
		extra              []string
		want               string
	}{
		{name: "a provider without text units", mode: "native-cli", schema: addedColumnSchema,
			want: "native operations cannot be represented as SQL: a text rendering of input 0 returned a native operation"},
		{name: "a change without a text form", mode: "policy", schema: rebuild, extra: []string{"--allow-table-rebuild"},
			want: "native operations cannot be represented as SQL: the fixture provider has no text form for operation example.org/fixture/table-operation (Table rebuild): the fixture runs a table rebuild only as a native operation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := textUnitsFixture(c, test.mode, test.schema)

			_, diagnostics, code, dir := f.generateMigration(c, test.extra...)

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(diagnostics, qt.Contains, test.want)
			files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(files, "\n"), qt.Equals, "")
		})
	}
}
