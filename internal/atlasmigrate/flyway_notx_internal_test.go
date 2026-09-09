package atlasmigrate

// White-box testing required: publication is driven by unexported functions, and
// the property under test is that the whole batch reaches the directory — which
// cannot be stated from the exported surface, because the failing run wrote
// nothing at all.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/migrationsnapshot"
)

// TestWriteDiffArtifacts_FlywayNoTransactionPublishesItsSidecar is the
// reproduction from stokaro/ptah#3115.
//
// Flyway opts a migration out of a transaction through a `<migration>.sql.conf`
// sidecar rather than through a marker inside the file. The batch composed that
// sidecar, so the expected publication snapshot held it, and the recapture that
// verifies the directory afterwards selects `.sql` files and the canonical
// metadata names only -- so the file existed on one side of the comparison and
// not the other, and every such run ended with `migration directory changed
// during migrate diff planning` having written nothing.
func TestWriteDiffArtifacts_FlywayNoTransactionPublishesItsSidecar(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifacts(
		t.Context(),
		writer,
		"concurrent_index",
		[]MigrationFileContent{{
			SQL:                  "CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id);",
			DownSQL:              "DROP INDEX CONCURRENTLY idx_widgets_id;",
			NoTransaction:        true,
			ReverseNoTransaction: true,
			Statements:           []string{"CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id)"},
			ReverseStatements:    []string{"DROP INDEX CONCURRENTLY idx_widgets_id"},
		}},
		baseSnapshot,
		nil,
		diffWriteLayout{format: atlasmigrateimport.FormatFlyway},
	)

	c.Assert(err, qt.IsNil)
	// Two migrations and the sidecar each direction needs.
	c.Assert(result.MigrationPaths, qt.HasLen, 4)
	_, statErr := os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)
	c.Assert(flywaySidecarNames(c, dir), qt.HasLen, 2)
}

// TestWriteDiffArtifacts_FlywayInTransactionWritesNoSidecar is the control: the
// sidecar is what a no-transaction migration needs, not something every Flyway
// publication carries.
func TestWriteDiffArtifacts_FlywayInTransactionWritesNoSidecar(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifacts(
		t.Context(),
		writer,
		"create_widgets",
		[]MigrationFileContent{{
			SQL:        "CREATE TABLE widgets (id INTEGER);",
			DownSQL:    "DROP TABLE widgets;",
			Statements: []string{"CREATE TABLE widgets (id INTEGER)"},
		}},
		baseSnapshot,
		nil,
		diffWriteLayout{format: atlasmigrateimport.FormatFlyway},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 2)
	c.Assert(flywaySidecarNames(c, dir), qt.HasLen, 0)
}

// flywaySidecarNames lists the Flyway script-config files a publication left in
// dir, keeping the loop out of the tests that count them.
func flywaySidecarNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".conf" {
			names = append(names, entry.Name())
		}
	}
	return names
}
