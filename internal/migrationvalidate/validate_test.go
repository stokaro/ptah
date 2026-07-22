package migrationvalidate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/migrationvalidate"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestValidate_WithDevURLReplaysAtlasMigration(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_create_validate_table.sql",
		"CREATE TABLE validate_dev_url_runtime (id INTEGER PRIMARY KEY);\n")

	result, err := migrationvalidate.Validate(ctx, migrationvalidate.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsTrue)
	c.Assert(result.Integrity.SumFileName, qt.Equals, migratesum.AtlasFileName)
	c.Assert(result.DevSQLValidated, qt.IsTrue)
	assertSQLiteTableCount(c, devDBPath, "validate_dev_url_runtime", 1)
}

func TestValidate_WithDevURLReportsSQLExecutionFailure(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_bad_statement.sql",
		"INSERT INTO missing_validate_table (id) VALUES (1);\n")

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "error validating migration SQL on dev database")
	c.Assert(result.Integrity.OK(), qt.IsTrue)
	c.Assert(result.DevSQLValidated, qt.IsFalse)
}

func TestValidate_ChecksumDriftSkipsDevDatabaseConnection(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	writeAtlasMigration(c, migrationsDir, "1_create_validate_table.sql",
		"CREATE TABLE validate_drift_short_circuit (id INTEGER PRIMARY KEY);\n")
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_create_validate_table.sql"),
		[]byte("CREATE TABLE validate_drift_short_circuit (id TEXT);\n"), 0o600), qt.IsNil)

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "unsupported://must-not-connect",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsFalse)
	c.Assert(result.Integrity.Describe(), qt.Contains, "changed: 1_create_validate_table.sql")
	c.Assert(result.DevSQLValidated, qt.IsFalse)
}

func writeAtlasMigration(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
}

func assertSQLiteTableCount(c *qt.C, dbPath, table string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
