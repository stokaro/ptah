//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// The log's whole value rests on a claim only a server can settle: the entries
// are written outside the migration's transaction, so a migration that failed
// and rolled back still leaves the record that it was attempted. A log written
// inside the transaction would describe only the runs that succeeded, which is
// the one thing nobody needs it for (stokaro/ptah#3406).
func TestMigrationLogSurvivesARolledBackMigrationLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	table := "ptah_log_notes"
	revisions := "ptah_log_revisions"
	dropMigrationLogFixture(c, ctx, conn, table, revisions)
	t.Cleanup(func() { dropMigrationLogFixture(c, context.Background(), conn, table, revisions) })

	// The body creates a table and then fails, so PostgreSQL rolls the whole
	// migration back: nothing it wrote survives except what was written
	// outside its transaction.
	up := fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY);\nDROP TABLE ptah_log_absent;\n", table)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "broken", up, fmt.Sprintf("DROP TABLE %s;\n", table)),
	)).WithMigrationsTable("", revisions).
		WithMigrationLockTimeout(10 * time.Second).
		WithActor("release-bot")
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	c.Assert(mig.MigrateUp(ctx), qt.IsNotNil)

	// The body's own work is gone, which is what makes the surviving entry
	// evidence that the entry was not written by the transaction.
	c.Assert(migrationLogTableExists(c, ctx, conn, table), qt.IsFalse)
	attempts, err := mig.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.Version, qt.Equals, int64(1))
	c.Assert(attempts[0].Outcome.State, qt.Equals, migrator.MigrationLogFailed)
	c.Assert(attempts[0].Start.Actor, qt.Equals, "release-bot")
	c.Assert(attempts[0].Start.ActorSource, qt.Equals, migrator.ActorProvided)
}

func dropMigrationLogFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	table, revisions string,
) {
	c.Helper()
	for _, name := range []string{table, revisions, revisions + "_log"} {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
		c.Check(err, qt.IsNil)
	}
}

func migrationLogTableExists(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	table string,
) bool {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(
		ctx, "SELECT count(*) FROM information_schema.tables WHERE table_name = $1", table,
	).Scan(&count), qt.IsNil)
	return count > 0
}

// A run that runs out of time is the case the log exists for: the process is
// interrupted, and whoever looks next has to know whether the migration ran.
// The outcome entry is written on a context detached from the migration's, so
// the record says failed. Written through the migration's own context the
// write would be refused with it, and the attempt would read as undetermined
// -- the one answer that tells a reader nothing.
func TestMigrationLogRecordsATimedOutRunAsFailedLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	table := "ptah_log_slow_notes"
	revisions := "ptah_log_slow_revisions"
	dropMigrationLogFixture(c, ctx, conn, table, revisions)
	t.Cleanup(func() { dropMigrationLogFixture(c, context.Background(), conn, table, revisions) })

	up := fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY);\nSELECT pg_sleep(30);\n", table)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "slow", up, fmt.Sprintf("DROP TABLE %s;\n", table)),
	)).WithMigrationsTable("", revisions).
		WithMigrationLockTimeout(60 * time.Second).
		WithActor("release-bot")
	c.Assert(mig.Initialize(ctx), qt.IsNil)
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	c.Assert(mig.MigrateUp(short), qt.ErrorIs, context.DeadlineExceeded)

	attempts, err := mig.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Undetermined(), qt.IsFalse)
	c.Assert(attempts[0].Outcome.State, qt.Equals, migrator.MigrationLogFailed)
	// The body rolled back with the transaction, so the table the migration
	// created is gone and only the entry outside it survives.
	c.Assert(migrationLogTableExists(c, ctx, conn, table), qt.IsFalse)
}
