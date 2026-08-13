//go:build integration

package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestCompatSchemaApplyCreatesExtensionInDeclaredSchemaPostgres proves the
// default compatibility profile retains the shared Pro-like placement
// capability. Strict CE policy remains covered by its separate refusal suite.
func TestCompatSchemaApplyCreatesExtensionInDeclaredSchemaPostgres(t *testing.T) {
	c := qt.New(t)
	dsn := livePostgresURLForRLSSpelling(t)
	targetURL, devURL := createRLSSpellingDatabases(t, dsn)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
schema "extensions" {}
extension "pgcrypto" {
  schema = schema.extensions
}
`), 0o600), qt.IsNil)

	first, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	second, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(first, qt.Contains, `CREATE EXTENSION "pgcrypto" WITH SCHEMA "extensions";`)
	c.Assert(second, qt.Contains, "Schema is synced, no changes to be made")
	c.Assert(compatExtensionInstallations(c, targetURL), qt.DeepEquals, []string{"extensions.pgcrypto"})
}

// TestCompatSchemaApplyRefusesDeclaredSystemSchemaBeforePlanningPostgres pins
// the no-op path. A standalone schema declaration produces no ordinary object
// diff, but it is still a request to create a server-owned namespace and must
// fail before schema apply reports a synced state or emits SQL.
func TestCompatSchemaApplyRefusesDeclaredSystemSchemaBeforePlanningPostgres(t *testing.T) {
	c := qt.New(t)
	dsn := livePostgresURLForRLSSpelling(t)
	targetURL, devURL := createRLSSpellingDatabases(t, dsn)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "pg_catalog" {}`), 0o600), qt.IsNil)

	out, err := runCompatSchemaApply(targetURL, devURL, path)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
	c.Assert(out, qt.Not(qt.Contains), "CREATE SCHEMA")
}

// TestCompatSchemaApplyScopeCannotHideDeclaredSystemSchemaPostgres proves
// selectors cannot remove an invalid declaration before apply validates the
// authored desired state. The public table makes every selector meaningful;
// without the pre-scope validation each case plans that table successfully.
func TestCompatSchemaApplyScopeCannotHideDeclaredSystemSchemaPostgres(t *testing.T) {
	c := qt.New(t)
	dsn := livePostgresURLForRLSSpelling(t)
	targetURL, devURL := createRLSSpellingDatabases(t, dsn)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
schema "pg_catalog" {}
schema "public" {}
table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	tests := []struct {
		name string
		args []string
	}{
		{name: "schema", args: []string{"--schema", "public"}},
		{name: "include", args: []string{"--include", "public.users"}},
		{name: "exclude", args: []string{"--exclude", "pg_catalog"}},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			out, err := runCompatSchemaApply(targetURL, devURL, path, test.args...)
			c.Assert(err, qt.ErrorMatches,
				`.*validate --to schema:.*declares server-owned PostgreSQL schema "pg_catalog".*`)
			c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
			c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
		})
	}
}

// TestCompatSchemaDiffRefusesDeclaredSystemSchemaBeforePlanningPostgres pins
// the equivalent preview path. The invalid declaration must be rejected even
// when schema comparison itself has no modeled change to render.
func TestCompatSchemaDiffRefusesDeclaredSystemSchemaBeforePlanningPostgres(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := filepath.Join(dir, "from.hcl")
	toPath := filepath.Join(dir, "to.hcl")
	tests := []struct {
		name     string
		from     string
		to       string
		wantFlag string
	}{
		{
			name:     "desired side",
			from:     `schema "public" {}`,
			to:       `schema "pg_catalog" {}`,
			wantFlag: "--to",
		},
		{
			name:     "current side",
			from:     `schema "pg_catalog" {}`,
			to:       `schema "public" {}`,
			wantFlag: "--from",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(os.WriteFile(fromPath, []byte(test.from), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(toPath, []byte(test.to), 0o600), qt.IsNil)

			out, err := executeCompatSchemaDiff(c,
				"--from", "file://"+fromPath,
				"--to", "file://"+toPath,
				"--dev-url", "postgres://invalid.invalid/db",
			)
			c.Assert(err, qt.ErrorMatches,
				`.*validate `+test.wantFlag+` schema:.*declares server-owned PostgreSQL schema "pg_catalog".*`)
			c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
			c.Assert(out, qt.Not(qt.Contains), "CREATE SCHEMA")
		})
	}
}

// TestCompatSchemaDiffRefusesUnsafeIntrospectedSystemSchemasPostgres proves
// database URLs are not misreported as authored declarations and still fail
// closed before SQL: catalog objects cannot round-trip through migration IR.
func TestCompatSchemaDiffRefusesUnsafeIntrospectedSystemSchemasPostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	targetURL := createDisposableDatabase(c, dbURL, "ptah_system_diff_"+uniqueScopeSuffix())

	for _, schema := range []string{"pg_catalog", "information_schema"} {
		c.Run(schema, func(c *qt.C) {
			out, err := executeCompatSchemaDiff(c,
				"--from", targetURL,
				"--to", targetURL,
				"--dev-url", targetURL,
				"--schema", schema,
			)
			c.Assert(err, qt.ErrorMatches,
				`.*observed server-owned PostgreSQL schema "`+schema+`" cannot be compared safely.*`)
			c.Assert(out, qt.Not(qt.Contains), "declares server-owned PostgreSQL schema")
			c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
			c.Assert(out, qt.Not(qt.Contains), "CREATE ")
			c.Assert(out, qt.Not(qt.Contains), "ALTER ")
			c.Assert(out, qt.Not(qt.Contains), "DROP ")
		})
	}
}

// TestCompatSchemaDiffMigrationReplayRefusesAuthoredSystemSchemaPostgres pins
// the other side of the State.DB boundary. A migration directory becomes an
// introspected state only after its authored SQL replays successfully; an
// invalid CREATE SCHEMA must still fail during replay.
func TestCompatSchemaDiffMigrationReplayRefusesAuthoredSystemSchemaPostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	suffix := uniqueScopeSuffix()
	currentURL := createDisposableDatabase(c, dbURL, "ptah_system_replay_current_"+suffix)
	devURL := createDisposableDatabase(c, dbURL, "ptah_system_replay_dev_"+suffix)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20260813000000_system.sql"),
		[]byte("CREATE SCHEMA pg_catalog;\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	out, err := executeCompatSchemaDiff(c,
		"--from", currentURL,
		"--to", "file://"+migrationsDir,
		"--dev-url", devURL,
	)
	c.Assert(err, qt.ErrorMatches,
		`(?s).*replay migration 20260813000000 on dev database:.*protected namespace "pg_catalog".*`)
	c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
	c.Assert(out, qt.Not(qt.Contains), "CREATE SCHEMA IF NOT EXISTS")
}

func compatExtensionInstallations(c *qt.C, dbURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), `
SELECT n.nspname || '.' || e.extname
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
 WHERE e.extname = 'pgcrypto'
 ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		found = append(found, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}
