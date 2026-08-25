package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/atlas/internal/atlastest"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrationfile"
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
	atlastest.AssertSQLiteTableCount(c, devDBPath, "atlas_validate_dev_url", 0)
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
	atlastest.AssertSQLiteTableCount(c, devDBPath, "compat_validate_dev_url", 0)
}

func writeAtlasMigration(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
}
