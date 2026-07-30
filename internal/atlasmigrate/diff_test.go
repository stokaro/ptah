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
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/testutils"
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	c.Assert(result.MigrationPaths[0], qt.Contains, "_add_email.sql")
	c.Assert(result.SumPath, qt.Equals, filepath.Join(migrationsDir, "atlas.sum"))
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 2)
	newSQL, err := os.ReadFile(result.MigrationPaths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(newSQL), "  ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
	sum, err := os.ReadFile(result.SumPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, filepath.Base(result.MigrationPaths[0]))
	assertDevDatabaseEmpty(c, conn)
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		Format:      `{{ sql . "" }}`,
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	newSQL, err := os.ReadFile(result.MigrationPaths[0])
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
		DryRun:      true,
	})
	_, statErr := os.Stat(filepath.Join(migrationsDir, "atlas.sum"))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.SQL, qt.Contains, "ALTER TABLE")
	c.Assert(result.SQL, qt.Contains, "ADD COLUMN")
	c.Assert(result.SQL, qt.Contains, "email")
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	assertDiffLockReleased(c, migrationsDir)
	assertDevDatabaseEmpty(c, conn)
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
		DryRun:      true,
	})
	afterSum, readErr := os.ReadFile(sumPath)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.SQL, qt.Contains, "email")
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(afterSum), qt.Equals, string(beforeSum))
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	assertDevDatabaseEmpty(c, conn)
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "noop",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsTrue)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 1)
	assertDevDatabaseEmpty(c, conn)
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "out_of_scope",
		Schemas:     []string{"billing"},
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsTrue)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
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
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE protected_users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `(?s)migration directory checksum verification failed:.*migration directory does not match atlas\.sum:.*changed: 1_init\.sql.*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	var protectedCount int
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM pragma_table_list WHERE schema = ? AND name = ?`,
		"main",
		"protected_users",
	).Scan(&protectedCount)
	c.Assert(err, qt.IsNil)
	c.Assert(protectedCount, qt.Equals, 1)
}

func TestGenerateDiff_LockTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	releaseLock, err := testutils.AcquireExclusiveFileLock(diffLockPath(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(releaseLock(), qt.IsNil)
	})
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE locked_diff (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "locked_diff",
		LockTimeout: time.Millisecond,
	})

	c.Assert(err, qt.ErrorMatches, `migration directory lock timeout after 1ms: .*\.ptah-migrate-diff\.lock`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
}

func TestGenerateDiff_LockCoversMigrationDirectoryDesiredResolution(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	releaseLock, err := testutils.AcquireExclusiveFileLock(diffLockPath(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(releaseLock(), qt.IsNil)
	})
	desiredDir := filepath.Join(dir, "desired")
	c.Assert(os.MkdirAll(desiredDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(desiredDir, "1_desired.sql"),
		[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err = migratesum.WriteWithFormat(desiredDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE protected_users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	result, err := atlasmigrate.GenerateDiff(t.Context(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+desiredDir),
		Name:        "locked_diff",
		LockTimeout: time.Millisecond,
	})

	c.Assert(err, qt.ErrorMatches, `migration directory lock timeout after 1ms: .*\.ptah-migrate-diff\.lock`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	var protectedCount int
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM pragma_table_list WHERE schema = ? AND name = ?`,
		"main",
		"protected_users",
	).Scan(&protectedCount)
	c.Assert(err, qt.IsNil)
	c.Assert(protectedCount, qt.Equals, 1)
}

func TestGenerateDiff_RejectsInvalidFormatBeforeCreatingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:     migrationsDir,
		Desired: localDesiredSet(c, "file://"+filepath.Join(dir, "schema.sql")),
		Format:  `{{ json . }}`,
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		Format:      `{{ sql . "  " "extra" }}`,
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	c.Assert(fileExists(filepath.Join(migrationsDir, "atlas.sum")), qt.IsFalse)
	assertDevDatabaseEmpty(c, conn)
}

func TestGenerateDiff_ReleasesLockAfterError(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
THIS IS NOT SQL;
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "invalid_replay",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `(?s)replay migration 1 on dev database: .*`)
	c.Assert(result.Synced, qt.IsFalse)
	assertDiffLockReleased(c, migrationsDir)
	assertDevDatabaseEmpty(c, conn)
}

func TestGenerateDiff_PreCanceledContextPreservesDevAndSkipsDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)
	_, err := conn.ExecContext(t.Context(),
		"CREATE TABLE stale_users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := atlasmigrate.GenerateDiff(ctx, conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "canceled",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(fileExists(diffLockPath(migrationsDir)), qt.IsFalse)
	c.Assert(fileExists(migrationsDir), qt.IsFalse)
	var count int
	err = conn.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM pragma_table_list WHERE schema = ? AND name = ?",
		"main",
		"stale_users",
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

func TestGenerateDiff_InvalidMigrationSnapshotDoesNotResetDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.Symlink(
		filepath.Join(dir, "missing.sql"),
		filepath.Join(migrationsDir, "1_broken.sql"),
	), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(
		schemaPath,
		[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)
	_, err := conn.ExecContext(context.Background(), "CREATE TABLE protected_users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "must_not_reset",
		LockTimeout: time.Second,
	})
	devSchema, readErr := dbschema.ReadSchemaWithSchemas(conn, nil)

	c.Assert(err, qt.ErrorMatches, `capture migration directory: capture filesystem snapshot: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(readErr, qt.IsNil)
	c.Assert(slices.ContainsFunc(devSchema.Tables, func(table dbschematypes.DBTable) bool {
		return table.Name == "protected_users"
	}), qt.IsTrue)
}

func TestGenerateDiff_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil dev database connection", func(c *qt.C) {
		result, err := atlasmigrate.GenerateDiff(context.Background(), nil, atlasmigrate.DiffOptions{
			Dir:     c.TempDir(),
			Desired: localDesiredSet(c, "file://schema.sql"),
		})
		c.Assert(err, qt.ErrorMatches, "migrate diff requires dev database connection")
		c.Assert(result.Synced, qt.IsFalse)
	})

	c.Run("missing migration directory", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "dev.db"))
		defer dbschema.CloseAndWarn(conn)

		result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
			Desired: localDesiredSet(c, "file://schema.sql"),
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
		c.Assert(err, qt.ErrorMatches, "migrate diff requires desired state")
		c.Assert(result.Synced, qt.IsFalse)
	})
}

func TestGenerateDiff_QualifierRejectedBeforeAnyWrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	qualifier, err := atlasmigrate.ParseQualifier("tenant")
	c.Assert(err, qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "qualified",
		Qualifier:   qualifier,
		LockTimeout: time.Second,
	})

	// The dialect gate fails before the migration directory, any migration
	// file, or any checksum is created.
	c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier is not supported for dialect "sqlite"`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(fileExists(migrationsDir), qt.IsFalse)
}

func TestGenerateDiff_QualifierRejectsMultiSchemaScopeBeforeAnyWrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	qualifier, err := atlasmigrate.ParseQualifier("tenant")
	c.Assert(err, qt.IsNil)

	conn := connectSQLite(c, filepath.Join(dir, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "qualified",
		Schemas:     []string{"app", "audit"},
		Qualifier:   qualifier,
		LockTimeout: time.Second,
	})

	// SQLite is rejected by the dialect gate before the schema-scope check,
	// and either way nothing is written.
	c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier is not supported for dialect "sqlite"`)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(fileExists(migrationsDir), qt.IsFalse)
}

// TestGenerateDiff_AtomicallyReplacesReadOnlySum verifies that checksum
// publication replaces the old file instead of truncating it in place.
func TestGenerateDiff_AtomicallyReplacesReadOnlySum(t *testing.T) {
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
	previousSum, err := os.ReadFile(sumPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chmod(sumPath, 0o400), qt.IsNil)
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
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsFalse)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	c.Assert(result.SumPath, qt.Equals, sumPath)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 2)
	afterSum, sumErr := os.ReadFile(sumPath)
	c.Assert(sumErr, qt.IsNil)
	c.Assert(afterSum, qt.Not(qt.DeepEquals), previousSum)
	assertDiffLockReleased(c, migrationsDir)
	assertDevDatabaseEmpty(c, conn)
}

func connectSQLite(c *qt.C, dbPath string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	return conn
}

func diffLockPath(migrationsDir string) string {
	cleanDir := filepath.Clean(migrationsDir)
	return filepath.Join(
		filepath.Dir(cleanDir),
		"."+filepath.Base(cleanDir)+".ptah-migrate-diff.lock",
	)
}

func assertDiffLockReleased(c *qt.C, migrationsDir string) {
	c.Helper()
	release, err := testutils.AcquireExclusiveFileLock(diffLockPath(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(release(), qt.IsNil)
}

func assertDevDatabaseEmpty(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func localDesiredSet(c *qt.C, rawURL string) atlassource.Set {
	c.Helper()
	set, err := atlassource.ClassifySet("--to", []string{rawURL}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)
	return set
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
