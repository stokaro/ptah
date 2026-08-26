package generator

// White-box testing required: the scan for a free migration version reads the
// bound directory handle rather than a pathname, and neither the handle nor the
// version it settles on is reachable through the exported generation API.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrationfile"
)

func TestNextAvailableMigrationVersionChecksUpAndDownFiles(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, migrationfile.FileName(100, "add_email", "down")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(dir, migrationfile.FileName(105, "future", "up")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)

	writer, err := bindPlannedMigrationDir("", dir)
	c.Assert(err, qt.IsNil)
	defer func() { _ = writer.Close() }()

	version, err := nextAvailableMigrationVersion(writer, 100, "add_email")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(106))
}
