//go:build !windows

package lint_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestRunLint_RejectsLocalDirectorySymlinkEscapeBeforeOpeningDevDatabase(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
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
	devDBPath := filepath.Join(root, "must-not-exist.db")

	_, stderr, err := execute(
		"--dir", "migrations",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "invalid migrations directory")
	_, statErr := os.Stat(devDBPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
