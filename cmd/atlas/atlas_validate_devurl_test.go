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

func TestCompatCommand_MigrateValidateDevURLReplaysAtlasMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_create_atlas_validate_table.sql",
		"CREATE TABLE atlas_validate_dev_url (id INTEGER PRIMARY KEY);\n")

	var out bytes.Buffer
	cmd := atlas.NewCompatCommand("atlas")
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "validate",
		"--dir", "file://" + migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", "sqlite://" + devDBPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	assertSQLiteTableCount(c, devDBPath, "atlas_validate_dev_url", 0)
}

func TestNewCompatCommand_MigrateValidateDevURLReplaysAtlasMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasMigration(c, migrationsDir, "1_create_compat_validate_table.sql",
		"CREATE TABLE compat_validate_dev_url (id INTEGER PRIMARY KEY);\n")

	var out bytes.Buffer
	cmd := atlas.NewCompatCommand("atlas")
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "validate",
		"--dir", "file://" + migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", "sqlite://" + devDBPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	assertSQLiteTableCount(c, devDBPath, "compat_validate_dev_url", 0)
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
