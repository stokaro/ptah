//go:build !windows

package migratetests_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// The targets of an env read the directory confined to the project directory,
// as one target does: a migration directory that is a symbolic link out of it
// is refused before any target is opened.
func TestCompatMigrateDrift_FailurePathRefusesADirectoryOutsideTheProject(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	projectDir := c.TempDir()
	outsideDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(outsideDir, "20240101000000_one.sql"), []byte("CREATE TABLE one (id integer);\n"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(outsideDir, filepath.Join(projectDir, "migrations")), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(driftProject), 0o600), qt.IsNil)
	c.Chdir(projectDir)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "--env", "tenants")

	c.Assert(err, qt.ErrorMatches, `(?s).*outside allowed root.*`)
	c.Assert(stdout, qt.Equals, "")
	_, statErr := os.Stat(filepath.Join(projectDir, "a.db"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
