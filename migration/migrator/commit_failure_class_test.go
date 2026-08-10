package migrator_test

// Commit failures, kept as their own case (issue #999 acceptance item 4).
//
// A revision-completion failure and a commit failure are different faults with
// different diagnostics: the first fails the UPDATE that records the migration
// applied, the second fails the COMMIT that makes that UPDATE and the whole
// migration body durable. Ptah spells them differently -- "failed to record
// migration N" against "failed to commit transaction for migration N" -- and
// before this file the second string had no test anywhere in the repository.
//
// Only ddltx.Transactional can be driven into it. On ddltx.ImplicitCommit the
// server has already committed the DDL before the migrator's commit is
// reached, and on ddltx.NoTransaction the commit is a writer no-op that cannot
// fail; ddltx.HasCommitStep is that rule, and
// TestCommitFailure_OnlyTheTransactionalClassHasACommitStep pins it so the
// absence of a MySQL or ClickHouse case here is a stated decision rather than
// an omission.
//
// PostgreSQL is the target because a deferred constraint trigger is a fault
// that fires at COMMIT and nowhere else. A BEFORE UPDATE trigger -- what the
// revision-completion cases use -- fires during the statement instead, and
// would produce the other fault.

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/ddltx"
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

// TestCommitFailure_OnlyTheTransactionalClassHasACommitStep states why this
// file has one target while the revision-completion matrix has five.
func TestCommitFailure_OnlyTheTransactionalClassHasACommitStep(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		class ddltx.Class
		want  bool
	}{
		{name: "transactional", class: ddltx.Transactional, want: true},
		{name: "implicit commit", class: ddltx.ImplicitCommit, want: false},
		{name: "no transaction", class: ddltx.NoTransaction, want: false},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(ddltx.HasCommitStep(test.class), qt.Equals, test.want)
		})
	}
	c.Assert(ddltx.HasCommitStep(ddltx.ClassOf("postgres")), qt.IsTrue)
}

// installPostgresDeferredCommitFault installs a constraint trigger deferred to
// the end of the transaction. The completion UPDATE succeeds, and the trigger
// it queued raises when the migrator commits.
func installPostgresDeferredCommitFault(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) {
	c.Helper()
	execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger AS $fault$
BEGIN
	IF NEW.state = 'applied' THEN
		RAISE EXCEPTION 'reject applied revision at commit';
	END IF;
	RETURN NEW;
END;
$fault$ LANGUAGE plpgsql`, names.fault))
	execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)`, names.revisions))
	execRevisionCompletionSQL(c, conn, fmt.Sprintf(
		`CREATE CONSTRAINT TRIGGER %s AFTER UPDATE ON %s
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION %s()`,
		names.fault,
		names.revisions,
		names.fault,
	))
}

func removePostgresDeferredCommitFault(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) {
	c.Helper()
	execRevisionCompletionSQL(c, conn, fmt.Sprintf("DROP TRIGGER %s ON %s", names.fault, names.revisions))
}
