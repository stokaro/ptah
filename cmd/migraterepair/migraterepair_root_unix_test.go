//go:build unix

package migraterepair_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migraterepair"
)

func TestMigrateRepairCommand_RejectsEscapingSymlinkBeforeDatabaseConnection(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	outside := c.TempDir()
	c.Assert(
		os.WriteFile(
			filepath.Join(outside, "1_escape.sql"),
			[]byte("CREATE TABLE escaped (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.Symlink(outside, filepath.Join(root, "migrations")), qt.IsNil)
	t.Chdir(root)

	dbPath := filepath.Join(root, "must-not-exist.db")
	cmd := migraterepair.NewMigrateRepairCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", "migrations",
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "1",
		"--force",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `error registering migrations: invalid migrations directory: .* is outside allowed root .*`)
	c.Assert(output.String(), qt.Contains, "error: error registering migrations:")
	c.Assert(dbPath, qt.Satisfies, pathDoesNotExist)
}

func pathDoesNotExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
