package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// seedSQLiteDB creates a SQLite database with the given DDL and returns its
// path.
func seedSQLiteDB(t *testing.T, ddl string) string {
	t.Helper()
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "seed.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), ddl)
	c.Assert(err, qt.IsNil)
	return dbPath
}

func sqliteHasTable(t *testing.T, dbPath, table string) bool {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func writeAtlasFormatMigrations(t *testing.T, ddl string) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte(ddl), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestSchemaApplyDatabaseURLSource(t *testing.T) {
	c := qt.New(t)
	sourcePath := seedSQLiteDB(t, "CREATE TABLE mirrored_users (id INTEGER PRIMARY KEY)")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + targetPath,
		"--to", "sqlite://" + sourcePath,
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteHasTable(t, targetPath, "mirrored_users"), qt.IsTrue)
}

func TestSchemaApplyMigrationDirSource(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasFormatMigrations(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	devPath := filepath.Join(t.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + targetPath,
		"--to", "file://" + migrationsDir,
		"--dev-url", "sqlite://" + devPath,
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteHasTable(t, targetPath, "replayed_users"), qt.IsTrue)
}

func TestSchemaApplyMigrationDirSourceRequiresDevURLBeforeTarget(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasFormatMigrations(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + targetPath,
		"--to", "file://" + migrationsDir,
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`--to "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`)
	// The failure happened before the target database was contacted: the
	// SQLite target file was never created.
	_, statErr := os.Stat(targetPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaApplyUnsupportedSchemeFailsBeforeTarget(t *testing.T) {
	c := qt.New(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + targetPath,
		"--to", "atlas://remote/app",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--to "atlas://remote/app": atlas:// registry URLs are not supported; use oci://.*`)
	_, statErr := os.Stat(targetPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaApplyMixedSourceKindsFailBeforeTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(""), 0o600), qt.IsNil)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + targetPath,
		"--to", "file://" + schemaPath,
		"--to", "sqlite://other.db",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`--to mixes desired-state source kinds: "file://.*" is a local schema file, but "sqlite://other\.db" is a database URL; use one source kind per flag`)
	_, statErr := os.Stat(targetPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaDiffDatabaseURLFromSource(t *testing.T) {
	c := qt.New(t)
	sourcePath := seedSQLiteDB(t, "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	dir := t.TempDir()
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(to, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
CREATE TABLE audit_logs (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "sqlite://" + sourcePath,
		"--to", "file://" + to,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
	c.Assert(out.String(), qt.Contains, "audit_logs")
	c.Assert(out.String(), qt.Not(qt.Contains), "CREATE TABLE users")
}

func TestSchemaDiffMigrationDirFromSource(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasFormatMigrations(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	dir := t.TempDir()
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(to, []byte(""), 0o600), qt.IsNil)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + migrationsDir,
		"--to", "file://" + to,
		"--dev-url", "sqlite://" + devPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "DROP TABLE")
	c.Assert(out.String(), qt.Contains, "replayed_users")
}

func TestSchemaDiffEnvSrcSourceResolvesVarsAndRelativePaths(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`variable "schema_file" {}

env "dev" {
  src = "file://${var.schema_file}"
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(baseDir, "desired.sql"), []byte(`
CREATE TABLE env_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	from := filepath.Join(t.TempDir(), "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--var", "schema_file=desired.sql",
		"--from", "file://" + from,
		"--to", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "env_users")
}

func TestSchemaApplyEnvSrcSource(t *testing.T) {
	allowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`env "dev" {
  src = "file://desired.sql"
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(baseDir, "desired.sql"), []byte(`
CREATE TABLE env_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--url", "sqlite://" + targetPath,
		"--to", "env://src",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteHasTable(t, targetPath, "env_users"), qt.IsTrue)
}

func TestSchemaDiffEnvSrcWithoutConfigFails(t *testing.T) {
	c := qt.New(t)
	from := filepath.Join(t.TempDir(), "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`--to "env://src": env:// desired-state references require an evaluated atlas.hcl project configuration; pass --config and --env to select one`)
}
