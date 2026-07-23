package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestGenerateDiff_HappyPathCreatesAtlasMigrationFromLocalSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPath, qt.Contains, "_add_email.sql")
	c.Assert(result.SumPath, qt.Equals, filepath.Join(migrationsDir, "atlas.sum"))
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 2)
	newSQL, err := os.ReadFile(result.MigrationPath)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(newSQL), "  ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
	sum, err := os.ReadFile(result.SumPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, filepath.Base(result.MigrationPath))
}

func TestGenerateDiff_CustomFormatWritesFormattedMigrationSQL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		Format:      `{{ sql . "" }}`,
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	newSQL, err := os.ReadFile(result.MigrationPath)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(newSQL), "ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
}

func TestGenerateDiff_DryRunReturnsSQLWithoutWritingMigration(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		LockTimeout: time.Second,
		DryRun:      true,
	})
	_, statErr := os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	_, lockStatErr := os.Stat(filepath.Join(migrationsDir, ".ptah-migrate-diff.lock"))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.SQL, qt.Contains, "ALTER TABLE")
	c.Assert(result.SQL, qt.Contains, "ADD COLUMN")
	c.Assert(result.SQL, qt.Contains, "email")
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	c.Assert(lockStatErr, qt.ErrorIs, os.ErrNotExist)
}

func TestGenerateDiff_DryRunPreservesExistingAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	sumPath := filepath.Join(migrationsDir, "atlas.sum")
	beforeSum, err := os.ReadFile(sumPath)
	c.Assert(err, qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		LockTimeout: time.Second,
		DryRun:      true,
	})
	afterSum, readErr := os.ReadFile(sumPath)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.SQL, qt.Contains, "email")
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(afterSum), qt.Equals, string(beforeSum))
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestGenerateDiff_SyncedReturnsNoChange(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "noop",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsTrue)
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 1)
}

func TestGenerateDiff_SchemaFilterIgnoresOutOfScopeDesiredSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(`
schema "auth" {}

table "users" {
  schema = schema.auth
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "out_of_scope",
		Schemas:     []string{"billing"},
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsTrue)
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
}

func TestGenerateDiff_RejectsChecksumDrift(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `(?s)migration directory checksum verification failed:.*migration directory does not match atlas\.sum:.*changed: 1_init\.sql.*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestGenerateDiff_LockTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, ".ptah-migrate-diff.lock"), []byte("held\n"), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE locked_diff (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "locked_diff",
		LockTimeout: time.Millisecond,
	})

	c.Assert(err, qt.ErrorMatches, `migration directory lock timeout after 1ms: .*\.ptah-migrate-diff\.lock`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
}

func TestGenerateDiff_RejectsInvalidFormatBeforeCreatingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:    migrationsDir,
		ToURLs: []string{"file://" + filepath.Join(dir, "schema.sql")},
		Format: `{{ json . }}`,
	})

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "json" not defined.*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(fileExists(migrationsDir), qt.IsFalse)
}

func TestGenerateDiff_RejectsFormatExecutionErrorWithoutWritingMigration(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + schemaPath},
		Name:        "add_email",
		Format:      `{{ sql . "  " "extra" }}`,
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	c.Assert(fileExists(filepath.Join(migrationsDir, "atlas.sum")), qt.IsFalse)
}

func TestGenerateDiff_ReleasesLockAfterError(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      []string{"file://" + filepath.Join(dir, "missing.sql")},
		Name:        "missing_schema",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `load --to schema: .*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(fileExists(filepath.Join(migrationsDir, ".ptah-migrate-diff.lock")), qt.IsFalse)
}

func TestGenerateDiff_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil dev database connection", func(c *qt.C) {
		result, err := atlasmigrate.GenerateDiff(context.Background(), nil, atlasmigrate.DiffOptions{
			Dir:    c.TempDir(),
			ToURLs: []string{"file://schema.sql"},
		})
		c.Assert(err, qt.ErrorMatches, "migrate diff requires dev database connection")
		c.Assert(result.Synced, qt.IsFalse)
	})

	c.Run("missing migration directory", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "dev.db"))
		defer dbschema.CloseAndWarn(conn)

		result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
			ToURLs: []string{"file://schema.sql"},
		})
		c.Assert(err, qt.ErrorMatches, "migrate diff requires migration directory")
		c.Assert(result.Synced, qt.IsFalse)
	})

	c.Run("missing desired schema URLs", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "dev.db"))
		defer dbschema.CloseAndWarn(conn)

		result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
			Dir: c.TempDir(),
		})
		c.Assert(err, qt.ErrorMatches, "migrate diff requires desired schema URLs")
		c.Assert(result.Synced, qt.IsFalse)
	})
}

func connectSQLite(c *qt.C, dbPath string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	return conn
}

func atlasSQLFiles(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		files = append(files, filepath.Join(dir, name))
	}
	files = slices.DeleteFunc(files, func(path string) bool {
		return !strings.HasSuffix(path, ".sql")
	})
	slices.Sort(files)
	return files
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
