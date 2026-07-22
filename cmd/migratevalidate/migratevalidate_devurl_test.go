package migratevalidate_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateValidate_DevURLReplaysAtlasMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_create_native_validate_table.sql",
		"CREATE TABLE native_validate_dev_url (id INTEGER PRIMARY KEY);\n")

	stdout, _, err := executeValidate(
		"--dir", migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "OK: migrations directory matches atlas.sum")
	c.Assert(stdout, qt.Contains, "OK: migration SQL validated on dev database")
	assertSQLiteTableCount(c, devDBPath, "native_validate_dev_url", 1)
}

func TestMigrateValidate_DevURLFailureExitsTwo(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_bad_statement.sql",
		"INSERT INTO missing_native_validate_table (id) VALUES (1);\n")

	_, stderr, err := executeValidate(
		"--dir", migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "error validating migration SQL on dev database")
}

func TestMigrateValidate_DriftSkipsDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	writeAtlasMigration(c, migrationsDir, "1_create_native_validate_table.sql",
		"CREATE TABLE native_validate_drift (id INTEGER PRIMARY KEY);\n")
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_create_native_validate_table.sql"),
		[]byte("CREATE TABLE native_validate_drift (id TEXT);\n"), 0o600), qt.IsNil)

	_, stderr, err := executeValidate(
		"--dir", migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", "unsupported://must-not-connect",
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "changed: 1_create_native_validate_table.sql")
	c.Assert(stderr, qt.Not(qt.Contains), "unsupported database dialect")
}

func executeValidate(args ...string) (stdout, stderr string, err error) {
	cmd := migratevalidate.NewMigrateValidateCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
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
