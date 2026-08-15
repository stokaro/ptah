//go:build !windows

package migrationsource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
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
