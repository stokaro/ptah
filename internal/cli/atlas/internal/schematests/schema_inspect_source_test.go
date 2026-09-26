package schematests_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
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
// fails with Atlas's exact message. The fixture matches the message's prefix;
// for a SQL file the pinned binary v1.3.0 appends a link to its dev-database
// page, measured on 2026-09-26, and so does this surface.
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

	c.Assert(err, qt.ErrorMatches, `--dev-url cannot be empty\. See: https://atlasgo\.io/atlas-schema/sql#dev-database`)
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
	dbPath := atlastest.SeedSQLiteDB(c, inspectSourceFixtureDDL)

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
	dbPath := atlastest.SeedSQLiteDB(c, inspectSourceFixtureDDL)
	outDir := filepath.Join(dir, "schema")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ hcl . | split "type" ".sqlite.hcl" | write ` + strconv.Quote(outDir) + ` }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	tables, err := os.ReadFile(filepath.Join(outDir, "tables.sqlite.hcl"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(tables), qt.Contains, `table "users"`)
	c.Assert(string(tables), qt.Contains, `table "posts"`)
}

// TestSchemaInspectWithoutDevURL_FailurePath is the pinned binary's answer to
// `schema inspect --url <source>` with no dev database, measured on 2026-09-26
// on PostgreSQL (stokaro/ptah#3680): a source a dev database has to run first
// gets the sentence with the link to that binary's dev-database page, and an
// HCL source gets the bare sentence. Both exit 1 with standard output empty.
func TestSchemaInspectWithoutDevURL_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		source     func(fx schemaDiffDevURLFixture) string
		devURL     []string
		wantStderr string
	}{
		{
			name:       "a SQL file",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.sqlFile },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "a directory of SQL files",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.sqlDir },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "a migration directory",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.migrationDir },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "an HCL file",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.hclFile },
			wantStderr: diffDevURLEmpty,
		},
		{
			name:       "a directory of HCL files",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.hclDir },
			wantStderr: diffDevURLEmpty,
		},
		{
			name:       "a SQL file with an explicitly empty --dev-url",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.sqlFile },
			devURL:     []string{"--dev-url", ""},
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "a SQL file with a --dev-url made of spaces",
			source:     func(fx schemaDiffDevURLFixture) string { return fx.sqlFile },
			devURL:     []string{"--dev-url", " "},
			wantStderr: "Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newSchemaDiffDevURLFixture(c)

			stdout, stderr, err := atlastest.RunCompat(append([]string{
				"schema", "inspect", "--url", tt.source(fx),
			}, tt.devURL...)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
