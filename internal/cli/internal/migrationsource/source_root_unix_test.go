//go:build !windows

package migrationsource_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/migration/migrationfile"
)

func TestCaptureLocal_RejectsSymlinkedFileEscape(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	outside := filepath.Join(t.TempDir(), "outside.sql")
	writeFile(c, outside, "SELECT 'outside';\n")
	c.Assert(os.Symlink(outside, filepath.Join(dir, "1_init.sql")), qt.IsNil)

	source, err := migrationsource.CaptureLocal(
		dir,
		migrationsource.LocalOptions{AllowedRoot: root},
	)

	c.Assert(err, qt.ErrorMatches, `capture migrations directory: .*`)
	c.Assert(source, qt.DeepEquals, migrationsource.LocalSource{})
}

// escapingDirectory returns a path inside a root of its own that is a symlink
// to a migration directory outside that root, and the root.
func escapingDirectory(c *qt.C) (dir, root string) {
	c.Helper()
	root = c.TempDir()
	outside := c.TempDir()
	writeFile(c, filepath.Join(outside, "1_init.sql"), "SELECT 'outside';\n")
	dir = filepath.Join(root, "migrations")
	c.Assert(os.Symlink(outside, dir), qt.IsNil)
	return dir, root
}

// A context binds the root of every path it was given, not only the last
// one, so a command that reads one directory per target confines each.
func TestResolve_FailurePathConfinesEveryBoundLocalDirectory(t *testing.T) {
	c := qt.New(t)
	first, firstRoot := escapingDirectory(c)
	second, secondRoot := escapingDirectory(c)
	ctx := migrationsource.WithLocalRoot(context.Background(), first, firstRoot)
	ctx = migrationsource.WithLocalRoot(ctx, second, secondRoot)

	for _, dir := range []string{first, second} {
		source, err := migrationsource.Resolve(ctx, dir, migrationsource.Options{DirFormat: migrationfile.DirFormatPtah})

		c.Assert(err, qt.ErrorMatches, `invalid migrations directory: .*`, qt.Commentf("%s", dir))
		c.Assert(source, qt.DeepEquals, migrationsource.Source{})
	}
}

// The control: a path no root is bound to is read without one, so the
// refusal above is the binding's.
func TestResolve_HappyPathReadsAnUnboundLocalDirectory(t *testing.T) {
	c := qt.New(t)
	dir, _ := escapingDirectory(c)

	source, err := migrationsource.Resolve(context.Background(), dir, migrationsource.Options{DirFormat: migrationfile.DirFormatPtah})

	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(source.FileSystem, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 'outside';\n")
}
