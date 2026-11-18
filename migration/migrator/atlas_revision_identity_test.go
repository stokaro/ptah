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
// runs under 1 as well.
func prepareRevisionIdentity(c *qt.C, dbPath string) []string {
	c.Helper()
	c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles("10_b.sql", "1R__rep.sql")), qt.IsNil)
	return []string{"10_b.sql", "1R__rep.sql", "1_a.sql"}
}

// runAtlasUpOrder is runAtlasUp under the execution order given.
func runAtlasUpOrder(c *qt.C, dbPath string, names []string, order migrator.ExecOrder) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles(names...), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithExecOrder(order).
		MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})
}

// TestAtlasRevisionIdentity_FailurePath refuses 1_a.sql as out of order. The
// applied 1R row records the string 1R, so 1_a.sql, which shares the number 1,
// is pending; without the string identity it reads as applied and never runs.
func TestAtlasRevisionIdentity_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	names := prepareRevisionIdentity(c, dbPath)

	err := runAtlasUpOrder(c, dbPath, names, migrator.ExecOrderLinear)

	_, ok := errors.AsType[*migrator.OutOfOrderError](err)
	c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"10", "1R"})
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
