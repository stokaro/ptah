//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestCommitFailure_PostgresTransactionalLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(t.Context(), postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	target := postgresRevisionCompletionTarget()
	names := newRevisionCompletionNames("commit")
	target.dropObjects(conn, names)
	t.Cleanup(func() { target.dropObjects(conn, names) })

	up, down := target.createBody(names)
	mig := revisionCompletionMigrator(conn, names, up, down)

	installPostgresDeferredCommitFault(c, conn, names)
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	// The distinguishing assertion: this is the commit, not the record.
	c.Assert(err.Error(), qt.Contains, "failed to commit transaction for migration 1")
	c.Assert(err.Error(), qt.Not(qt.Contains), "failed to record migration 1")
	c.Assert(err.Error(), qt.Contains, "reject applied revision at commit")

	// A failed commit rolls the whole transaction back, so a transactional
	// target loses the body exactly as it does on a completion failure.
	c.Assert(target.bodyPresent(c, conn, names), qt.IsFalse)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision.State, qt.Equals, "failed")
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 0)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 1)

	retryErr := mig.MigrateUp(ctx)
	c.Assert(retryErr, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(retryErr), qt.IsTrue)

	removePostgresDeferredCommitFault(c, conn, names)
	recovery := retryAfterFixingTheRevisionWrite()
	c.Assert(recovery.run(revisionCompletionMigrator(conn, names, up, down)), qt.IsNil)

	finalStatus, err := revisionCompletionMigrator(conn, names, up, down).GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.DirtyRevision, qt.IsNil)
	c.Assert(finalStatus.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(target.bodyPresent(c, conn, names), qt.IsTrue)
}
