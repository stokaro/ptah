package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSchemaInspectSchemaFileNormalizedOnDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "inspect",
		"--schema-file", schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--format", "hcl",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "orders"`)
}

func TestSchemaInspectOutDirExportsFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	outDir := filepath.Join(dir, "exported")
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--format", "sql",
		"--out-dir", outDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	entries, err := os.ReadDir(outDir)
	c.Assert(err, qt.IsNil)
	c.Assert(len(entries) > 0, qt.IsTrue, qt.Commentf("no files exported to %s", outDir))
}

func TestSchemaInspectRequiresSource(t *testing.T) {
	c := qt.New(t)

	out, err := runSchema("", "inspect")

	c.Assert(err, qt.ErrorMatches, "an inspection source is required: .*", qt.Commentf("%s", out))
}

func TestSchemaInspectRejectsTemplateFormats(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--format", "{{ sql . }}",
	)

	c.Assert(err, qt.ErrorMatches, `unsupported --format .*: expected hcl, sql, or json`, qt.Commentf("%s", out))
}

func TestSchemaInspectRejectsSplitWithoutOutDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--split", "schema",
	)

	c.Assert(err, qt.ErrorMatches, "--split requires --out-dir", qt.Commentf("%s", out))
}

func TestSchemaInspectRejectsNonMigrationDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	out, err := runSchema("", "inspect",
		"--migrations-dir", dir,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(err, qt.ErrorMatches, `--migrations-dir .* is not recognized as a migration directory .*`, qt.Commentf("%s", out))
}

// nativeInspectIncludeDDL gives the include tests a selectable table, a
// dependent table, and an unrelated table.
const nativeInspectIncludeDDL = `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES users(id));
CREATE TABLE archive (id INTEGER PRIMARY KEY);`

func TestSchemaInspectIncludeSelectsResources(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, nativeInspectIncludeDDL)

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--include", "users",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "users"`)
	c.Assert(out, qt.Contains, `column "email"`)
	c.Assert(out, qt.Not(qt.Contains), `table "archive"`)
}

func TestSchemaInspectIncludeValidatesSelectors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{
			name:    "type selector spelling",
			pattern: "*[type=column]",
			wantErr: `unsupported Atlas include selector .*column resources ride along with their parent.*`,
		},
		{
			// The dotted spelling is not refused on its shape: it is
			// indistinguishable from a table literally named "users.email", so
			// it reaches the projection and the closed port is what fails.
			name:    "positional spelling reaches the connection",
			pattern: "main.users.email",
			wantErr: `(?s).*failed to connect.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			// The URL points at a closed port: reaching it would fail with a
			// connection error instead of the selector error asserted below.
			out, err := runSchema("", "inspect",
				"--db-url", "postgres://127.0.0.1:1/unreachable",
				"--include", test.pattern,
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
		})
	}
}
