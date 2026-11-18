package migrator_test

import (
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// prepareRevisionIdentity applies 10_b.sql and 1R__rep.sql, which run under
// the numbers 10 and 1, and returns the directory with 1_a.sql added, which
// spells the number 1 as well. The three then run under their positions in
// file-name order: 10_b.sql, 1R__rep.sql, 1_a.sql.
func prepareRevisionIdentity(c *qt.C, dbPath string) []string {
	c.Helper()
	c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles("10_b.sql", "1R__rep.sql")), qt.IsNil)
	return []string{"10_b.sql", "1R__rep.sql", "1_a.sql"}
}

// runAtlasUpOrder is runAtlasUp under the execution order given.
func runAtlasUpOrder(c *qt.C, dbPath string, names []string, order migrator.ExecOrder) error {
	c.Helper()
	return atlasUpOrderMigrator(c, dbPath, names, order).MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})
}

// atlasUpOrderMigrator reads names over an Atlas revision table under the
// execution order given; the connection closes when the test ends.
func atlasUpOrderMigrator(c *qt.C, dbPath string, names []string, order migrator.ExecOrder) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles(names...), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).WithExecOrder(order)
}

// TestAtlasRevisionIdentity_FailurePath refuses 1_a.sql as out of order where
// linearity reads the version strings, as the pinned community binary does:
// "1" sorts below the applied "1R". The applied 1R row records the string 1R,
// so 1_a.sql, which spells the same number, is pending; without the string
// identity it reads as applied and never runs.
func TestAtlasRevisionIdentity_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	names := prepareRevisionIdentity(c, dbPath)

	err := atlasUpOrderMigrator(c, dbPath, names, migrator.ExecOrderLinear).
		WithAtlasStringLinearity().
		MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})

	_, ok := errors.AsType[*migrator.OutOfOrderError](err)
	c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"10", "1R"})
}

// TestAtlasRevisionIdentity_RunsInPositionOrder applies 1_a.sql under the
// native linear order: 1_a.sql sorts after both applied files, so it is the
// next file in execution order, and it runs under a position of its own
// rather than under the number 1R__rep.sql spells.
func TestAtlasRevisionIdentity_RunsInPositionOrder(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	names := prepareRevisionIdentity(c, dbPath)

	err := runAtlasUpOrder(c, dbPath, names, migrator.ExecOrderLinear)

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "10", "1R"})
}

// TestAtlasRevisionIdentity_HappyPath applies 1_a.sql under --exec-order
// non-linear and records it as 1 beside 1R, as the pinned community binary
// v1.3.0 does.
func TestAtlasRevisionIdentity_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	names := prepareRevisionIdentity(c, dbPath)

	err := runAtlasUpOrder(c, dbPath, names, migrator.ExecOrderNonLinear)

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "10", "1R"})
}
