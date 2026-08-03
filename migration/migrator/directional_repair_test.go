package migrator_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// directionalRepairFixture is a migration whose up and down bodies have
// different statement counts, so every assertion about which body ran can be
// read off the schema and off the recorded totals without ambiguity.
//
// up:   3 statements, the last of which inserts a row into log
// down: 4 statements, the third of which fails against any database
func directionalRepairFixture() fstest.MapFS {
	return fstest.MapFS{
		"000001_setup.up.sql": {Data: []byte(
			"CREATE TABLE parent (id INTEGER PRIMARY KEY);\n" +
				"CREATE TABLE log (v INTEGER);\n" +
				"INSERT INTO log (v) VALUES (1);\n",
		)},
		"000001_setup.down.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE gone (id INTEGER PRIMARY KEY);\n" +
				"DROP TABLE gone;\n" +
				"DROP TABLE definitely_missing_table;\n" +
				"DROP TABLE parent;\n",
		)},
	}
}

// directionalRepairMigrator opens a fresh SQLite database, applies the fixture,
// and runs a rollback that fails at the third of four down statements.
func directionalRepairMigrator(c *qt.C, name string) (*migrator.Migrator, *dbschema.DatabaseConnection) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(conn, directionalRepairFixture())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(mig.MigrateDownTo(c.Context(), 0), qt.IsNotNil)
	return mig, conn
}

func directionalRepairRevision(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
) (state string, applied int, total int) {
	c.Helper()
	c.Assert(
		conn.QueryRow("SELECT state, applied, total FROM schema_migrations WHERE version = 1").
			Scan(&state, &applied, &total),
		qt.IsNil,
	)
	return state, applied, total
}

func directionalRepairLogRows(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRow("SELECT COUNT(*) FROM log").Scan(&count), qt.IsNil)
	return count
}

// TestDirectionalRepair_RecordsRollbackDirection pins the storage encoding a
// failed rollback leaves behind. Before the direction was recorded this row was
// byte-identical to the one a failed apply leaves, and the assertion prints
// got "failed" / want "failed:down".
func TestDirectionalRepair_RecordsRollbackDirection(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "records-direction.db")

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(applied, qt.Equals, 2)
	c.Assert(total, qt.Equals, 4)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.State, qt.Equals, "failed")
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
}

// TestDirectionalRepair_ResumeRunsDownBodyAndDeletesRevision is the defect in
// stokaro/ptah#995. Reverted, --resume-from runs up statement 3 instead: log
// holds two rows rather than none, parent survives, and the revision reads
// state applied with applied=3 total=3 instead of being gone.
func TestDirectionalRepair_ResumeRunsDownBodyAndDeletesRevision(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "resume-down.db")
	c.Assert(directionalRepairLogRows(c, conn), qt.Equals, 1)

	// Statement 3 is the one that failed; the operator skips it and finishes.
	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 4}), qt.IsNil)

	c.Assert(noTransactionTableExists(c, conn, "parent"), qt.IsFalse)
	c.Assert(noTransactionTableExists(c, conn, "log"), qt.IsTrue)
	c.Assert(directionalRepairLogRows(c, conn), qt.Equals, 1)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
}

// TestDirectionalRepair_ResumeBoundIsDownStatementCount covers the statement
// the rollback still owes, which the up body's length cannot reach. Reverted,
// the call returns "resume-from must be between 1 and 3".
func TestDirectionalRepair_ResumeBoundIsDownStatementCount(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "resume-bound.db")
	_, _, total := directionalRepairRevision(c, conn)
	c.Assert(total, qt.Equals, 4)

	err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 5})
	c.Assert(err, qt.ErrorMatches, `resume-from must be between 1 and 4`)
}

// TestDirectionalRepair_PartialRollbackRefusesToRecordApplied pins the refusal
// to guess. Reverted, RepairMigration returns nil and rewrites the row to
// state applied, applied=3, total=3 over a schema whose objects the rollback
// already dropped.
func TestDirectionalRepair_PartialRollbackRefusesToRecordApplied(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "refuse-applied.db")

	err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1})
	c.Assert(err, qt.ErrorMatches,
		`migration 1 stopped while rolling back: 2 of 4 down statements committed; `+
			`rerun with --resume-from 3 .*with --force to record it applied.*migrations set --version.*`)

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(applied, qt.Equals, 2)
	c.Assert(total, qt.Equals, 4)
}

// TestDirectionalRepair_ForceRecordsPartialRollbackApplied keeps the escape
// hatch for the operator who restored the schema by hand. Reverted this passes
// unchanged, which is the point: --force is not what the fix took away.
func TestDirectionalRepair_ForceRecordsPartialRollbackApplied(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "force-applied.db")

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "applied")
	c.Assert(applied, qt.Equals, 3)
	c.Assert(total, qt.Equals, 3)
}

// TestDirectionalRepair_ResumedRollbackFailureKeepsAbsoluteProgress proves a
// resumed rollback that fails again is resumable in turn: the row counts in the
// same numbers --resume-from speaks, not in offsets into the resumed tail.
// Reverted, the first call runs the up body and execution never reaches the
// check below.
func TestDirectionalRepair_ResumedRollbackFailureKeepsAbsoluteProgress(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "resume-refail.db")

	// Resuming at the statement that failed reruns it, and it fails again.
	err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3})
	c.Assert(err, qt.ErrorMatches, `(?s)failed to resume rollback of migration 1: .*definitely_missing_table.*`)

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(applied, qt.Equals, 2)
	c.Assert(total, qt.Equals, 4)

	// The row still describes a rollback, so the next resume finishes it.
	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 4}), qt.IsNil)
	c.Assert(noTransactionTableExists(c, conn, "parent"), qt.IsFalse)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
}

// TestDirectionalRepair_UpFailureStillResumesUpBody is the non-interference
// control. The same lever on the same shape of dirty row must behave exactly as
// it did before: run the remaining up statements and record the migration
// applied. Reverted, this passes unchanged.
func TestDirectionalRepair_UpFailureStillResumesUpBody(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "up-resume.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_setup.up.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE parent (id INTEGER PRIMARY KEY);\n" +
				"CREATE TABLE child (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO definitely_missing_table (v) VALUES (1);\n" +
				"CREATE INDEX idx_child ON child (id);\n",
		)},
		"000001_setup.down.sql": {Data: []byte(
			"DROP INDEX IF EXISTS idx_child;\nDROP TABLE IF EXISTS child;\nDROP TABLE IF EXISTS parent;\n",
		)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed")
	c.Assert(applied, qt.Equals, 2)
	c.Assert(total, qt.Equals, 4)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 4}), qt.IsNil)

	c.Assert(directionalRepairIndexExists(c, conn, "idx_child"), qt.IsTrue)
	state, applied, total = directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "applied")
	c.Assert(applied, qt.Equals, 4)
	c.Assert(total, qt.Equals, 4)
}

// TestDirectionalRepair_RefusesUpResumeOverLegacyRollbackRow covers the rows
// written before the direction was recorded, which read as up rows. The totals
// still discriminate when the two bodies differ in length. Reverted, the first
// call returns nil after replaying up statement 3 into the half-reverted
// schema.
func TestDirectionalRepair_RefusesUpResumeOverLegacyRollbackRow(t *testing.T) {
	c := qt.New(t)
	mig, conn := directionalRepairMigrator(c, "legacy-row.db")

	// Rewrite the row the way a Ptah that did not record directions wrote it.
	_, err := conn.ExecContext(c.Context(), "UPDATE schema_migrations SET state = 'failed' WHERE version = 1")
	c.Assert(err, qt.IsNil)

	err = mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3})
	c.Assert(err, qt.ErrorMatches,
		`migration 1 records 4 statements, which matches its down body \(4\) and not its up body \(3\).*--force.*`)
	c.Assert(directionalRepairLogRows(c, conn), qt.Equals, 1)

	// --force is the documented override for a claim about the metadata.
	c.Assert(
		mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3, Force: true}),
		qt.IsNil,
	)
	c.Assert(directionalRepairLogRows(c, conn), qt.Equals, 2)
}

// TestDirectionalRepair_ResumedRollbackMarksEachStatementInFlight proves the
// resumed rollback keeps the same crash contract the original rollback has:
// before each statement the revision durably says which statement is running
// and that its outcome is unknown, so a process death mid-resume cannot be
// mistaken for a clean stop after the previous statement.
//
// The probe is the database itself. A down statement copies the revision row
// into a side table as it runs, which is the only vantage point from which the
// in-flight marker is visible -- every way the resume can end overwrites it.
// Without the marker the captured row is the previous failure instead:
// "failed:down", applied 1, and the old error text.
func TestDirectionalRepair_ResumedRollbackMarksEachStatementInFlight(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "in-flight.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	const capture = "INSERT INTO probe SELECT state, applied, COALESCE(error, '') FROM schema_migrations WHERE version = 1;\n"
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_setup.up.sql": {Data: []byte(
			"CREATE TABLE parent (id INTEGER PRIMARY KEY);\n" +
				"CREATE TABLE probe (state TEXT, applied INTEGER, err TEXT);\n",
		)},
		"000001_setup.down.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				capture +
				"DROP TABLE definitely_missing_table;\n" +
				capture +
				"DROP TABLE parent;\n",
		)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(mig.MigrateDownTo(c.Context(), 0), qt.IsNotNil)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3}), qt.IsNil)

	rows, err := conn.QueryContext(c.Context(), "SELECT state, applied, err FROM probe ORDER BY rowid")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var captured []string
	for rows.Next() {
		var state, failure string
		var applied int
		c.Assert(rows.Scan(&state, &applied, &failure), qt.IsNil)
		captured = append(captured, state+"/"+strconv.Itoa(applied)+"/"+failure)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(captured, qt.HasLen, 2)
	// Statement 1 of the original rollback, and statement 3 of the resume.
	c.Assert(captured[0], qt.Matches, `pending:down/0/statement execution outcome is unknown.*`)
	c.Assert(captured[1], qt.Matches, `pending:down/2/statement execution outcome is unknown.*`)
}

// TestDirectionalRepair_UpResumeSurvivesEqualLengthBodies is the fixture that
// separates the legacy-row guard from a guard that just fires on down-length
// rows: here the two bodies are the same length, so the row's total matches the
// down body as well, and the resume must still run. A guard that dropped the
// "matches the up body" half prints
// `migration 1 records 3 statements, which matches its down body (3)`.
func TestDirectionalRepair_UpResumeSurvivesEqualLengthBodies(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "equal-bodies.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_setup.up.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE parent (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO definitely_missing_table (v) VALUES (1);\n" +
				"CREATE TABLE child (id INTEGER PRIMARY KEY);\n",
		)},
		"000001_setup.down.sql": {Data: []byte(
			"DROP TABLE IF EXISTS child;\nDROP TABLE IF EXISTS parent;\nDROP TABLE IF EXISTS extra;\n",
		)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 3)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3}), qt.IsNil)
	c.Assert(noTransactionTableExists(c, conn, "child"), qt.IsTrue)
	state, applied, total = directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "applied")
	c.Assert(applied, qt.Equals, 3)
	c.Assert(total, qt.Equals, 3)
}

// TestDirectionalRepair_CancelledMidRollbackResumesFromCommittedStatement
// covers the cancellation half of the durability requirement: the existing
// cancellation coverage stops a single-statement down, which cannot show
// whether a partially committed rollback is resumable. Reverted, the resume
// runs the up body and parent is still present at the end.
func TestDirectionalRepair_CancelledMidRollbackResumesFromCommittedStatement(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "cancel-mid-down.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	fsys := fstest.MapFS{
		"000001_setup.up.sql": {Data: []byte(
			"CREATE TABLE parent (id INTEGER PRIMARY KEY);\nCREATE TABLE child (id INTEGER PRIMARY KEY);\n",
		)},
		"000001_setup.down.sql": {Data: []byte(
			"-- +ptah no_transaction\nDROP TABLE child;\nDROP TABLE parent;\n",
		)},
	}
	plain, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(plain.MigrateUp(c.Context()), qt.IsNil)

	ctx, cancel := context.WithCancel(c.Context())
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithStatementObserver(
		migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
			cancel()
			return context.Canceled
		}),
	))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(conn, provider).MigrateDownTo(ctx, 0), qt.ErrorIs, context.Canceled)

	// The first down statement committed and survived the cancellation.
	c.Assert(noTransactionTableExists(c, conn, "child"), qt.IsFalse)
	c.Assert(noTransactionTableExists(c, conn, "parent"), qt.IsTrue)
	state, applied, total := directionalRepairRevision(c, conn)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)

	c.Assert(plain.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
	c.Assert(noTransactionTableExists(c, conn, "parent"), qt.IsFalse)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
}

func directionalRepairIndexExists(c *qt.C, conn *dbschema.DatabaseConnection, name string) bool {
	c.Helper()
	var count int
	c.Assert(
		conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", name).Scan(&count),
		qt.IsNil,
	)
	return count == 1
}
