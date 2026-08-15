//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// The two cases below separate the surviving-body classes, which the
// single-statement bodies cannot. They exist because internal/ddltx answers two
// different questions -- did any of the body survive, and did all of it -- and
// the MySQL family answers them differently.
//
// A MySQL-family body of DDL followed by DML keeps both statements: the server
// commits before the DDL, which ends the migrator's transaction, and the DML
// that follows therefore runs in autocommit. A body of DML alone never reaches
// an implicit commit and rolls back whole. Only the second shape can tell a
// correct implementation from one that reads "the body is durable on this
// class" as "the whole body is applied", so both are pinned.
func runRevisionCompletionAfterDDL(t *testing.T, target revisionCompletionTarget) {
	c := qt.New(t)
	ctx := context.Background()
	fixture := newRevisionCompletionFixture(t, c, target, "ddl")

	up := fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB; INSERT INTO %s (id) VALUES (1)",
		fixture.names.body,
		fixture.names.body,
	)
	down := fmt.Sprintf("DROP TABLE %s", fixture.names.body)
	mig := fixture.begin(c, up, down)

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to record migration 1")

	// The implicit commit before the CREATE ends the transaction, so the INSERT
	// after it autocommits and both statements are durable.
	c.Assert(target.bodyPresent(c, fixture.conn, fixture.names), qt.IsTrue)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(1))

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 2)

	fixture.recoverWithRetry(c, up, down)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(1))
}

func runRevisionCompletionDMLOnly(t *testing.T, target revisionCompletionTarget) {
	c := qt.New(t)
	ctx := context.Background()
	fixture := newRevisionCompletionFixture(t, c, target, "dml")

	// The table is created outside the migration, so the body contains no DDL
	// and never triggers an implicit commit.
	execRevisionCompletionSQL(c, fixture.faultConn, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB",
		fixture.names.body,
	))

	up := fmt.Sprintf(
		"INSERT INTO %s (id) VALUES (1); INSERT INTO %s (id) VALUES (2)",
		fixture.names.body,
		fixture.names.body,
	)
	down := fmt.Sprintf("DELETE FROM %s", fixture.names.body)
	mig := fixture.begin(c, up, down)

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to record migration 1")

	// Nothing in this body is DDL, so the rollback took all of it. A revision
	// claiming otherwise would send the retry past rows that are not there.
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(0))

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 0)

	fixture.recoverWithRetry(c, up, down)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(2))
}

// revisionCompletionFixture holds the connections and names shared by the two
// MySQL-family shape cases above.
type revisionCompletionFixture struct {
	target    revisionCompletionTarget
	conn      *dbschema.DatabaseConnection
	faultConn *dbschema.DatabaseConnection
	names     revisionCompletionNames
}

func newRevisionCompletionFixture(
	t *testing.T,
	tb testing.TB,
	target revisionCompletionTarget,
	shape string,
) revisionCompletionFixture {
	t.Helper()
	conn := target.connect(t)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	faultConn, closeFaultConn := target.faultConn(t, conn)
	t.Cleanup(closeFaultConn)

	names := newRevisionCompletionNames(target.name + "_" + shape)
	target.dropObjects(faultConn, names)
	t.Cleanup(func() { target.dropObjects(faultConn, names) })

	return revisionCompletionFixture{target: target, conn: conn, faultConn: faultConn, names: names}
}

func (f revisionCompletionFixture) begin(c *qt.C, up, down string) *migrator.Migrator {
	c.Helper()
	mig := revisionCompletionMigrator(f.conn, f.names, up, down)
	f.target.installFault(c, f.faultConn, f.names)
	c.Assert(mig.Initialize(context.Background()), qt.IsNil)
	return mig
}

func (f revisionCompletionFixture) recoverWithRetry(c *qt.C, up, down string) {
	c.Helper()
	f.target.removeFault(c, f.faultConn, f.names)
	recovery := retryAfterFixingTheRevisionWrite()
	c.Assert(recovery.run(revisionCompletionMigrator(f.conn, f.names, up, down)), qt.IsNil)

	status, err := revisionCompletionMigrator(f.conn, f.names, up, down).GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
}

func revisionCompletionRowCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) int64 {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}

// markAppliedAfterTheBodyCommitted is the manual recovery, and it is only
// correct where the body survived: it records the migration applied without
// running anything, so on a transactional target -- whose body rolled back with
// the revision -- it would sign off a schema change that is not in the
// database. The matrix drives it for the surviving-body classes only, which is
// the repair guidance issue #999 asks to keep aligned with dialect semantics.
func markAppliedAfterTheBodyCommitted() revisionCompletionRecovery {
	return revisionCompletionRecovery{
		name: "repair",
		run: func(mig *migrator.Migrator) error {
			return mig.RepairMigration(context.Background(), migrator.RepairMigrationOptions{Version: 1})
		},
	}
}
