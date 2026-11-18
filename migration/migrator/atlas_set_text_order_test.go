package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// setAtlasRevision sets the revision of the directory holding 1_a.sql,
// 2_b.sql and 10_c.sql to 10 and then to 2, on the migrator rule returns, and
// returns the rows the second set leaves.
func setAtlasRevision(c *qt.C, rule func(*migrator.Migrator) *migrator.Migrator) []string {
	c.Helper()
	dbPath := filepath.Join(c.TempDir(), "app.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles("1_a.sql", "2_b.sql", "10_c.sql"),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = rule(mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas))
	for _, version := range []int64{10, 2} {
		_, err := mig.SetRevision(c.Context(), version)
		c.Assert(err, qt.IsNil)
	}
	return atlasRevisionVersions(c, dbPath)
}

// TestSetRevision_TextOrderKeepsWhatSortsBelowTheBoundary orders the set as
// text under the string rule, as the pinned community binary v1.3.0 does: 10
// sorts below 2 as text, so a set to 2 keeps it.
func TestSetRevision_TextOrderKeepsWhatSortsBelowTheBoundary(t *testing.T) {
	c := qt.New(t)

	c.Assert(setAtlasRevision(c, (*migrator.Migrator).WithAtlasStringLinearity), qt.DeepEquals, []string{"1", "10", "2"})
}

// TestSetRevision_NumericOrderByDefault keeps the numeric order the native
// commands use: a set to 2 removes 10.
func TestSetRevision_NumericOrderByDefault(t *testing.T) {
	c := qt.New(t)

	c.Assert(setAtlasRevision(c, func(m *migrator.Migrator) *migrator.Migrator { return m }), qt.DeepEquals, []string{"1", "2"})
}
