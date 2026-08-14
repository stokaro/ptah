//go:build !windows

package migrationsnapshot_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationsnapshot"
)

func TestCaptureDirectory_DoesNotTreatMissingChildAsMissingRoot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	name := "0000000001_missing.up.sql"
	c.Assert(os.Symlink(filepath.Join(dir, "missing-target"), filepath.Join(dir, name)), qt.IsNil)

	_, err := migrationsnapshot.CaptureDirectory(dir)

	c.Assert(err, qt.ErrorMatches, `.*0000000001_missing\.up\.sql.*no such file or directory.*`)
}
