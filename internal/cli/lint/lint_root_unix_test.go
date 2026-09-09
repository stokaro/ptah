//go:build !windows

package lint_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
)

// TestRunLint_RejectsAnUnopenableDirectoryBeforeOpeningDevDatabase pins the
// ordering claim: whatever makes a migrations directory unusable is found
// before the dev database is created, so a bad --dir does not leave a file
// behind.
//
// A directory symlink leaving the working directory cannot serve as the
// fixture: stokaro/ptah#1622 removed pathguard's relative-only confinement --
// it refused "migrations" and accepted the identical destination spelled
// absolutely, so it filtered a spelling rather than an escape -- and the
// symlink is followed, which the companion test below pins. A regular file
// named as --dir is not a directory on any spelling, so the ordering claim
// keeps a fixture that cannot be respelled away.
func TestRunLint_RejectsAnUnopenableDirectoryBeforeOpeningDevDatabase(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "migrations"), []byte("not a directory\n"), 0o600), qt.IsNil)
	t.Chdir(root)
	devDBPath := filepath.Join(root, "must-not-exist.db")

	_, stderr, err := execute(
		"--dir", "migrations",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "not a directory")
	_, statErr := os.Stat(devDBPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestRunLint_ReadsADirectorySymlinkThatLeavesTheWorkingDirectory is the other
// half of stokaro/ptah#1622: the escape is a supported spelling, and the
// migration behind it is linted rather than refused.
func TestRunLint_ReadsADirectorySymlinkThatLeavesTheWorkingDirectory(t *testing.T) {
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

	_, stderr, err := execute(
		"--dir", "migrations",
		"--dev-url", "sqlite://"+filepath.Join(root, "dev.db"),
	)

	c.Assert(stderr, qt.Not(qt.Contains), "outside allowed root")
	c.Assert(stderr, qt.Not(qt.Contains), "not a directory")
	c.Assert(exitcode.Code(err, 0), qt.Not(qt.Equals), 2,
		qt.Commentf("the directory is readable now; got stderr:\n%s", stderr))
}
