//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// A derived rollback moves the revision boundary after its statements, and on
// ClickHouse that move is refused: the revision history cannot be updated
// atomically there. MigrateDownDerived gives the refusal before the
// statements run, so the table the statements would drop and the revisions
// that record it are both still there.

// TestMigrateDownDerivedRefusesTheBoundaryMoveFirstClickHouse_FailurePath
// rolls back version 2 on ClickHouse. The refusal comes first, so the
// statements never run.
func TestMigrateDownDerivedRefusesTheBoundaryMoveFirstClickHouse_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionClickHouse(t)
	defer dbschema.CloseAndWarn(conn)
	const revisionsTable = "ptah_derived_rollback_revisions"
	const userTable = "ptah_derived_rollback_two"
	dropTables := func() {
		for _, table := range append(revisiontable.Configured("", revisionsTable), userTable) {
			dropNoTransactionClickHouseTable(c, conn, table)
		}
	}
	dropTables()
	defer dropTables()
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"1_one.sql": {Data: []byte("SELECT 1;\n")},
		"2_two.sql": {Data: []byte("CREATE TABLE " + userTable + " (id UInt64) ENGINE = MergeTree ORDER BY id;\n")},
	}, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).WithMigrationsTable("", revisionsTable)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	var calls int

	err = mig.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
		TargetVersion: 1,
		Versions:      []int64{2},
		Apply: func(ctx context.Context) error {
			calls++
			return conn.Writer().ExecuteSQL(ctx, "DROP TABLE "+userTable+" SYNC")
		},
	}, nil)

	c.Assert(err, qt.ErrorMatches, `move the revision boundary to version 1: setting an Atlas revision is not supported `+
		`for ClickHouse because revision history cannot be updated atomically`)
	c.Assert(calls, qt.Equals, 0)
	applied, err := mig.GetAppliedMigrations(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(applied, qt.DeepEquals, []int64{1, 2})
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '"+userTable+"'",
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 1)
}
