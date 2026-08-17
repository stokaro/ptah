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

// TestMigrateRepairCommand_RejectsAnUnopenableDirectoryBeforeDatabaseConnection
// pins the ordering claim: an unusable --migrations-dir is found before the
// database is opened, so a bad argument leaves no file behind.
//
// The fixture used to be a directory symlink leaving the working directory.
// stokaro/ptah#1622 removed the relative-only confinement that refused it --
// the identical destination spelled absolutely was always accepted, so the rule
// filtered a spelling -- and the escape is followed now. A regular file named
// as the directory is not a directory on any spelling, so the ordering claim
// keeps a fixture that cannot be respelled away.
func TestMigrateRepairCommand_RejectsAnUnopenableDirectoryBeforeDatabaseConnection(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "migrations"), []byte("not a directory\n"), 0o600), qt.IsNil)
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

	c.Assert(err, qt.ErrorMatches, `error registering migrations: .*not a directory`)
	c.Assert(output.String(), qt.Contains, "error: error registering migrations:")
	c.Assert(dbPath, qt.Satisfies, pathDoesNotExist)
}

func pathDoesNotExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
