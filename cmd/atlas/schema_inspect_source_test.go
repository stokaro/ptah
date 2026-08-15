package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// inspectSourceFixtureDDL seeds the live database used by the inspect source
// round-trip and split tests.
const inspectSourceFixtureDDL = `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL
);
CREATE UNIQUE INDEX users_email_key ON users (email);
`

// TestSchemaInspectLocalFileRequiresDevURL mirrors the pinned Atlas
// cli-inspect-file fixture: inspecting a schema file without a dev database
// fails with Atlas's exact message.
func TestSchemaInspectLocalFileRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	schemaPath := filepath.Join(t.TempDir(), "a.sql")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE users (id int);\n"), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "file://" + schemaPath})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--dev-url cannot be empty`)
}

// TestSchemaInspectLocalSQLFileWithDevURL mirrors the pinned fixture's happy
// path: the schema file is materialized on the dev database and the
// introspected result renders as HCL with the dev database's schema scope.
func TestSchemaInspectLocalSQLFileWithDevURL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "a.sql")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  email TEXT NOT NULL\n);\n"), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "users"`)
	c.Assert(out.String(), qt.Contains, `column "email"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "Reading schema from database")
}

func TestSchemaInspectMigrationDirWithDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasFormatMigrations(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "file://" + migrationsDir,
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "replayed_users"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "atlas_schema_revisions")
}

// TestSchemaInspectFileExportRoundTrip proves the CLI round-trip: a live
// inspection exported to a file re-inspects (through the dev database) to
// byte-identical output.
func TestSchemaInspectFileExportRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := seedSQLiteDB(c, inspectSourceFixtureDDL)

	liveCmd := atlas.NewCompatCommand("atlas")
	var live bytes.Buffer
	liveCmd.SetOut(&live)
	liveCmd.SetErr(&live)
	liveCmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath})
	c.Assert(liveCmd.Execute(), qt.IsNil)
	exported := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(exported, live.Bytes(), 0o600), qt.IsNil)

	reloadCmd := atlas.NewCompatCommand("atlas")
	var reloaded bytes.Buffer
	reloadCmd.SetOut(&reloaded)
	reloadCmd.SetErr(&reloaded)
	reloadCmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "file://" + exported,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
	})

	err := reloadCmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(reloaded.String(), qt.Equals, live.String())
}

func TestSchemaInspectSplitTypeModeWritesGroupedFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := seedSQLiteDB(c, inspectSourceFixtureDDL)
	outDir := filepath.Join(dir, "schema")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ hcl . | split "type" ".sqlite.hcl" | write "` + outDir + `" }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	tables, err := os.ReadFile(filepath.Join(outDir, "tables.sqlite.hcl"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(tables), qt.Contains, `table "users"`)
	c.Assert(string(tables), qt.Contains, `table "posts"`)
}
