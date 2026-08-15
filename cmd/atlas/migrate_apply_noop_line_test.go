package atlas_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCompatMigrateApply_NoopLineIsByteExact pins the whole stdout of a second
// `migrate apply` over a directory that is already applied.
//
// It asserts EQUALITY, not containment, and that is the entire point of the
// row. Every other assertion on this sentence in the package is
// `qt.Contains, "No migration files to execute"`, which matches just as
// happily when the line carries a trailing period — so with the period put
// back in migrate_apply.go, not one of them fails. Finding 9.3 of
// stokaro/ptah#1235 is exactly that period, so a `Contains` assertion cannot
// hold it.
//
// Measured against the pinned Atlas community binary v1.3.0, both writing 30
// bytes and nothing else on stdout at exit 0, read with xxd from an unpiped
// invocation, on an empty directory and on the shape below:
//
//	4e6f 206d 6967 7261 7469 6f6e 2066 696c   No migration fil
//	6573 2074 6f20 6578 6563 7574 650a        es to execute\n
func TestCompatMigrateApply_NoopLineIsByteExact(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_noop")
	dbPath := filepath.Join(tempDir, "noop.db")
	writeCoveredSetFile(c.TB, dir, "1_a.sql", coveredSetTopLevelSQL)
	hashCoveredSetDir(c.TB, dir)

	first, firstErr, err := compatApply(dir, dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", first, firstErr))
	c.Assert(userTables(c.TB, dbPath), qt.DeepEquals, []string{"a"})

	second, secondErr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", second, secondErr))
	c.Assert(second, qt.Equals, "No migration files to execute\n")
}
