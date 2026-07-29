package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestSchemaInspectLiveDatabaseWritesSQL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--format", "sql",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(out, qt.Not(qt.Contains), "Database:")
}

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

// TestSchemaInspectMatchesAtlasSchemaInspect proves the native verb and its
// Atlas twin produce identical machine output from the same live database:
// both wrap atlasschema.InspectSource.
func TestSchemaInspectMatchesAtlasSchemaInspect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);")

	nativeOut, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath,
		"--format", "hcl",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))

	atlasCmd := atlas.NewCompatCommand("atlas")
	var atlasOut bytes.Buffer
	atlasCmd.SetOut(&atlasOut)
	atlasCmd.SetErr(&atlasOut)
	atlasCmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath, "--format", "hcl"})
	c.Assert(atlasCmd.Execute(), qt.IsNil, qt.Commentf("%s", atlasOut.String()))

	c.Assert(nativeOut, qt.Equals, atlasOut.String())
}
