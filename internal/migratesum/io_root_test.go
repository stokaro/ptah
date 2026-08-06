package migratesum_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

func openSumDirectory(c *qt.C, dir string) *pathguard.OpenedDirectory {
	c.Helper()
	opened, err := pathguard.OpenDirectory(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	return opened
}

func TestWritePrecomputedWithFormatIn_WritesAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte("SELECT 1;"), 0o600), qt.IsNil)
	sum, err := migratesum.ComputeWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	opened := openSumDirectory(c, dir)

	writeErr := migratesum.WritePrecomputedWithFormatIn(
		opened,
		migrator.MigrationDirFormatAtlas,
		sum,
	)

	c.Assert(writeErr, qt.IsNil)
	contents, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.DeepEquals, sum.Bytes())
	result, err := migratesum.VerifyDirWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
}

func TestWritePrecomputedWithFormatIn_RejectsNilSum(t *testing.T) {
	c := qt.New(t)
	opened := openSumDirectory(c, c.TempDir())

	err := migratesum.WritePrecomputedWithFormatIn(
		opened,
		migrator.MigrationDirFormatAtlas,
		nil,
	)

	c.Assert(err, qt.ErrorMatches, `migration checksum must not be nil`)
}

// TestWritePrecomputedWithFormatIn_WritesIntoTheRetainedDirectory pins that the
// checksum lands in the directory the handle was opened on even when a
// replacement has taken over its pathname.
func TestWritePrecomputedWithFormatIn_WritesIntoTheRetainedDirectory(t *testing.T) {
	c := qt.New(t)
	parent := c.TempDir()
	dir := filepath.Join(parent, "retained")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte("SELECT 1;"), 0o600), qt.IsNil)
	sum, err := migratesum.ComputeWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	opened := openSumDirectory(c, dir)
	moved := filepath.Join(parent, "moved")
	c.Assert(os.Rename(dir, moved), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)

	writeErr := migratesum.WritePrecomputedWithFormatIn(
		opened,
		migrator.MigrationDirFormatAtlas,
		sum,
	)

	c.Assert(writeErr, qt.IsNil)
	contents, err := os.ReadFile(filepath.Join(moved, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.DeepEquals, sum.Bytes())
	_, err = os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
