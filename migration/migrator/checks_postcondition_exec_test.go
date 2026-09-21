package migrator_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A migration that backfills a column, with a postcondition saying what the
// backfill was for. The predicate is false before the body and true after it,
// which is what makes it a postcondition rather than a precondition written in
// the wrong place.
const backfillUpSQL = `-- +ptah check name="every_row_has_a_tier" phase=after ` +
	`assert="SELECT count(*) = 0 FROM accounts WHERE tier IS NULL"` + "\n" +
	"ALTER TABLE accounts ADD COLUMN tier TEXT;\n" +
	"UPDATE accounts SET tier = 'free' WHERE id > 0;\n"

// newSQLitePostconditionMigrator builds a migrator over an accounts table with
// seededRows rows, whose up body is upSQL.
func newSQLitePostconditionMigrator(
	t *testing.T,
	seededRows int,
	upSQL string,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "postconditions.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	for i := range seededRows {
		_, err = conn.Exec("INSERT INTO accounts (id) VALUES (?)", i+1)
		c.Assert(err, qt.IsNil)
	}

	migration := migrator.CreateMigrationFromSQL(1, "backfill_tier", upSQL,
		"ALTER TABLE accounts DROP COLUMN tier;\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	return conn, m
}

func accountsWithoutATier(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRow("SELECT count(*) FROM accounts WHERE tier IS NULL").Scan(&count), qt.IsNil)
	return count
}

// TestMigrateUp_PostconditionHoldsOverTheStateTheBodyProduced is the property
// the phase exists for: the predicate is evaluated against the database the
// body left behind, so a migration can state what it produced.
func TestMigrateUp_PostconditionHoldsOverTheStateTheBodyProduced(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLitePostconditionMigrator(t, 2, backfillUpSQL)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(accountsWithoutATier(c, conn), qt.Equals, 0)
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
}

// TestMigrateUp_FailingPostconditionReportsAnAppliedMigration is the outcome
// the issue names: applied, postcondition failed. The row the backfill missed
// is still missing, the migration is recorded as applied, and nothing was
// rolled back or retried (stokaro/ptah#3405).
func TestMigrateUp_FailingPostconditionReportsAnAppliedMigration(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	// The body backfills `id > 0`, so a row with id 0 keeps its null tier.
	conn, m := newSQLitePostconditionMigrator(t, 0, backfillUpSQL)
	_, err := conn.Exec("INSERT INTO accounts (id) VALUES (0)")
	c.Assert(err, qt.IsNil)

	err = m.MigrateUp(ctx)

	var postErr *migrator.PostMigrationCheckFailedError
	c.Assert(err, qt.ErrorAs, &postErr, qt.Commentf("want PostMigrationCheckFailedError, got %v", err))
	c.Assert(postErr.Version, qt.Equals, int64(1))
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "every_row_has_a_tier")
	c.Assert(checkErr.Phase, qt.Equals, migrator.CheckPhaseAfter)
	c.Assert(err.Error(), qt.Contains, "applied")
	// The body committed and stays committed.
	c.Assert(accountsWithoutATier(c, conn), qt.Equals, 1)
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

// TestMigrateUp_FailingPostconditionLeavesNothingToRerun is the other half of
// that outcome. A failure that recorded the migration as pending would offer a
// retry, and re-running a committed body is exactly what must not happen.
func TestMigrateUp_FailingPostconditionLeavesNothingToRerun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLitePostconditionMigrator(t, 0, backfillUpSQL)
	_, err := conn.Exec("INSERT INTO accounts (id) VALUES (0)")
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	// A second run has nothing pending, so it neither re-runs the body nor
	// re-evaluates the postcondition.
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	var columns int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM pragma_table_info('accounts') WHERE name = 'tier'",
	).Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, 1, qt.Commentf("the body ran once"))
}

// TestMigrateUp_APreconditionAndAPostconditionReadDifferentStates is the
// control that separates the phases. One assertion is written twice, and the
// phase alone decides which state it is asked about.
func TestMigrateUp_APreconditionAndAPostconditionReadDifferentStates(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	up := `-- +ptah check name="no_tier_column_yet" ` +
		`assert="SELECT count(*) = 0 FROM pragma_table_info('accounts') WHERE name = 'tier'"` + "\n" +
		`-- +ptah check name="tier_column_exists" phase=after ` +
		`assert="SELECT count(*) = 1 FROM pragma_table_info('accounts') WHERE name = 'tier'"` + "\n" +
		"ALTER TABLE accounts ADD COLUMN tier TEXT;\n"
	_, m := newSQLitePostconditionMigrator(t, 1, up)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
}

// TestMigrateUp_PostconditionIsRefusedForWhatItDoesNotParse keeps the two
// phases under one grammar: a phase the directive spells wrong is refused
// where every other malformed key is, before anything runs.
func TestMigrateUp_PostconditionIsRefusedForWhatItDoesNotParse(t *testing.T) {
	c := qt.New(t)
	up := `-- +ptah check name="later" phase=afterwards assert="SELECT 1"` + "\n" +
		"ALTER TABLE accounts ADD COLUMN tier TEXT;\n"
	conn, m := newSQLitePostconditionMigrator(t, 1, up)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported \+ptah check phase="afterwards" \(want before or after\).*`)
	var columns int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM pragma_table_info('accounts') WHERE name = 'tier'",
	).Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, 0, qt.Commentf("the refusal arrives before the body"))
}

// TestMigrateUp_DryRunDefersEveryPostcondition is where the two phases part
// company in a preview. A precondition on the first migration in the run sees
// exactly the state a real apply would give it, so it is evaluated; a
// postcondition asks about the state the body produces, and a dry run has
// refused to produce it — including for the first migration.
func TestMigrateUp_DryRunDefersEveryPostcondition(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLitePostconditionMigrator(t, 1, backfillUpSQL)
	conn.SchemaWriter().SetDryRun(true)

	var deferred []int64
	err := m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		ChecksDeferredObserver: func(_ context.Context, versions []int64) {
			deferred = versions
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(deferred, qt.DeepEquals, []int64{1})
	var columns int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM pragma_table_info('accounts') WHERE name = 'tier'",
	).Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, 0,
		qt.Commentf("the dry run changed nothing, which is why the assertion could not be asked"))
}

// TestMigrateUp_DryRunStillRefusesAPostconditionItCanRead is the control:
// deferring the answer is not deferring the verdict. Whether an assertion is
// write-shaped is decided by its text, so a preview reports it.
func TestMigrateUp_DryRunStillRefusesAPostconditionItCanRead(t *testing.T) {
	c := qt.New(t)
	up := `-- +ptah check name="writes" phase=after assert="UPDATE accounts SET tier = 'x'"` + "\n" +
		"ALTER TABLE accounts ADD COLUMN tier TEXT;\n"
	conn, m := newSQLitePostconditionMigrator(t, 1, up)
	conn.SchemaWriter().SetDryRun(true)

	err := m.MigrateUp(context.Background())

	var postErr *migrator.PostMigrationCheckFailedError
	c.Assert(err, qt.ErrorAs, &postErr, qt.Commentf("want PostMigrationCheckFailedError, got %v", err))
	c.Assert(err, qt.ErrorMatches, `(?s).*read-only SELECT.*`)
}

// TestMigrateDown_PostconditionReadsTheStateTheRollbackProduced gives the
// rollback the same symmetry the apply has: a down body states what it needs
// before it runs and what it produced after.
func TestMigrateDown_PostconditionReadsTheStateTheRollbackProduced(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "down.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	down := `-- +ptah check name="users_gone" phase=after ` +
		`assert="SELECT count(*) = 0 FROM sqlite_master WHERE type = 'table' AND name = 'users'"` + "\n" +
		"DROP TABLE users;\n"
	migration := migrator.CreateMigrationFromSQL(1, "create_users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", down)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrateDown(ctx), qt.IsNil)

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
}

// TestMigrateDown_FailingPostconditionReportsARolledBackMigration is the same
// outcome the apply path reports, on the other side: the rollback ran, its
// revision is gone, and the assertion that did not hold is a statement about a
// database nobody is going to rewind further.
func TestMigrateDown_FailingPostconditionReportsARolledBackMigration(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "down.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	down := `-- +ptah check name="audit_gone" phase=after ` +
		`assert="SELECT count(*) = 0 FROM sqlite_master WHERE type = 'table' AND name = 'audit'"` + "\n" +
		"DROP TABLE users;\n"
	migration := migrator.CreateMigrationFromSQL(1, "create_users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE audit (id INTEGER PRIMARY KEY);\n", down)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	err = m.MigrateDown(ctx)

	var postErr *migrator.PostMigrationCheckFailedError
	c.Assert(err, qt.ErrorAs, &postErr, qt.Commentf("want PostMigrationCheckFailedError, got %v", err))
	c.Assert(postErr.Version, qt.Equals, int64(1))
	// The message says what this database now holds, which is the opposite of
	// what the up direction's says: a rollback that ran, not a migration that
	// applied.
	c.Assert(postErr.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(err.Error(), qt.Contains, "rollback of migration 1 completed")
	c.Assert(err.Error(), qt.Not(qt.Contains), "recorded as applied")
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0),
		qt.Commentf("the rollback ran and is recorded; the assertion is about what it left"))
}
