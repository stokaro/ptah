//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// Oracle moves a tag with MERGE. ClickHouse appends versions to a replacing
// table. Both paths must resolve one current version and honor a deletion.
func TestOracleMigrationTagsE2E(t *testing.T) {
	c := qt.New(t)
	checkMigrationTags(c, oracleDevDatabase(c))
}

func TestClickHouseMigrationTagsE2E(t *testing.T) {
	c := qt.New(t)
	checkMigrationTags(c, clickHouseDevDatabase(c))
}

func checkMigrationTags(c *qt.C, database devDialectDatabase) {
	c.Helper()
	runner := migrator.NewMigrator(database.conn, nil)
	ctx := c.Context()

	c.Assert(runner.RecordMigrationTag(ctx, "release", 1), qt.IsNil)
	c.Assert(runner.RecordMigrationTag(ctx, "release", 2), qt.IsNil)
	version, err := runner.ResolveMigrationTag(ctx, "release")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))

	tags, err := runner.MigrationTags(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 1)
	c.Assert(tags[0].Tag, qt.Equals, "release")
	c.Assert(tags[0].Version, qt.Equals, int64(2))

	c.Assert(runner.DeleteMigrationTag(ctx, "release"), qt.IsNil)
	_, err = runner.ResolveMigrationTag(ctx, "release")
	c.Assert(err, qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(runner.DeleteMigrationTag(ctx, "release"), qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(runner.RecordMigrationTag(ctx, "release", 3), qt.IsNil)
	version, err = runner.ResolveMigrationTag(ctx, "release")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(3))
}
