package migrator_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A SQLite trigger stands in for whatever a server refuses a log row for, and a
// migration plants it, so the refusal starts part-way through one run.
const refuseLogRowsOfVersionTwo = `CREATE TRIGGER refuse_version_two BEFORE INSERT ON schema_migrations_log
WHEN NEW.version = 2
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END;
`

const refuseFailedLogRows = `CREATE TRIGGER refuse_failed BEFORE INSERT ON schema_migrations_log
WHEN NEW.state = 'failed'
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END;
`

const refuseDownLogRows = `CREATE TRIGGER refuse_down BEFORE INSERT ON schema_migrations_log
WHEN NEW.operation = 'down'
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END;
`

// newReportMigrator builds a migrator over a fresh SQLite database.
func newReportMigrator(c *qt.C, migrations ...*migrator.Migration) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "report.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...))
}

// lossesWithoutMessages drops the one field that repeats what the driver said,
// so a table row can spell the rest.
func lossesWithoutMessages(c *qt.C, report *migrator.MigrationLogReport) []migrator.MigrationLogLoss {
	c.Helper()
	c.Assert(report, qt.IsNotNil)
	losses := make([]migrator.MigrationLogLoss, 0, len(report.Lost))
	for _, loss := range report.Lost {
		c.Assert(loss.Message, qt.Contains, "this log row is refused")
		loss.Message = ""
		losses = append(losses, loss)
	}
	return losses
}

// attemptOutcomes reads the log back as "version:outcome", oldest first, with an
// attempt that has no outcome spelled as undetermined.
func attemptOutcomes(c *qt.C, ctx context.Context, m *migrator.Migrator) []string {
	c.Helper()
	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	outcomes := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		outcome := string(attempt.Outcome.State)
		if attempt.Undetermined() {
			outcome = "undetermined"
		}
		outcomes = append([]string{attemptLabel(attempt.Start.Version) + ":" + outcome}, outcomes...)
	}
	return outcomes
}

func attemptLabel(version int64) string {
	return map[int64]string{1: "v1", 2: "v2"}[version]
}

// TestMigrationLogReport_CountsWhatReachedTheTable is the control the losses are
// read against: a report with nothing lost says every entry is in the table, so
// a run with no rows and a run that lost them are different reports.
func TestMigrationLogReport_CountsWhatReachedTheTable(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	m := newReportMigrator(c,
		migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
		migrator.CreateMigrationFromSQL(2, "two", "CREATE TABLE two (id INTEGER PRIMARY KEY);\n", "DROP TABLE two;\n"),
	)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{},
		qt.Commentf("a migrator that keeps a log and has nothing to record says so"))

	// The run goes through a copy, and the report through the original: every
	// With* builder copies the migrator, and so does the run session.
	c.Assert(m.WithActor("release-bot").MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{Written: 4})
	c.Assert(attemptOutcomes(c, ctx, m), qt.DeepEquals, []string{"v1:applied", "v2:applied"})
}

// TestMigrationLogReport_NamesTheFileThatLostItsEntries is why the report is per
// entry. A run that lost only its second file's entries must say so against that
// file, and the row-level read back shows which attempts the table is missing.
func TestMigrationLogReport_NamesTheFileThatLostItsEntries(t *testing.T) {
	lost := func(state migrator.MigrationLogState) migrator.MigrationLogLoss {
		return migrator.MigrationLogLoss{Operation: "up", Version: 2, VersionKey: "2", State: state}
	}
	tests := []struct {
		name         string
		txMode       migrator.MigrationTxMode
		wantWritten  int
		wantLost     []migrator.MigrationLogLoss
		wantAttempts []string
	}{
		{
			name:         "one transaction per file",
			txMode:       migrator.MigrationTxModeFile,
			wantWritten:  2,
			wantLost:     []migrator.MigrationLogLoss{lost(migrator.MigrationLogStarted), lost(migrator.MigrationLogApplied)},
			wantAttempts: []string{"v1:applied"},
		},
		{
			// The starts are written before the batch transaction opens, so
			// only the outcome of the second file meets the trigger.
			name:         "one transaction for the batch",
			txMode:       migrator.MigrationTxModeAll,
			wantWritten:  3,
			wantLost:     []migrator.MigrationLogLoss{lost(migrator.MigrationLogApplied)},
			wantAttempts: []string{"v1:applied", "v2:undetermined"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()
			m := newReportMigrator(c,
				migrator.CreateMigrationFromSQL(1, "one",
					"CREATE TABLE one (id INTEGER PRIMARY KEY);\n"+refuseLogRowsOfVersionTwo, "DROP TABLE one;\n"),
				migrator.CreateMigrationFromSQL(2, "two", "CREATE TABLE two (id INTEGER PRIMARY KEY);\n", "DROP TABLE two;\n"),
			).WithTransactionMode(test.txMode)
			c.Assert(m.Initialize(ctx), qt.IsNil)

			c.Assert(m.MigrateUp(ctx), qt.IsNil, qt.Commentf("a lost entry does not fail the migration"))

			report := m.MigrationLogReport()
			c.Assert(lossesWithoutMessages(c, report), qt.DeepEquals, test.wantLost)
			c.Assert(report.Written, qt.Equals, test.wantWritten)
			c.Assert(attemptOutcomes(c, ctx, m), qt.DeepEquals, test.wantAttempts)
			applied, err := m.GetAppliedMigrations(ctx)
			c.Assert(err, qt.IsNil)
			c.Assert(applied, qt.DeepEquals, []int64{1, 2})
		})
	}
}

// A run that failed can lose the outcome that says so. The start is in the
// table, so a reader of the log alone sees an attempt nobody can decide, and the
// report is what says the failure was recorded nowhere.
func TestMigrationLogReport_NamesALostFailure(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	m := newReportMigrator(c,
		migrator.CreateMigrationFromSQL(1, "one", refuseFailedLogRows, "SELECT 1;\n"),
		migrator.CreateMigrationFromSQL(2, "broken", "DROP TABLE absent;\n", "SELECT 1;\n"),
	)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	report := m.MigrationLogReport()
	c.Assert(lossesWithoutMessages(c, report), qt.DeepEquals, []migrator.MigrationLogLoss{
		{Operation: "up", Version: 2, VersionKey: "2", State: migrator.MigrationLogFailed},
	})
	c.Assert(report.Written, qt.Equals, 3)
	c.Assert(attemptOutcomes(c, ctx, m), qt.DeepEquals, []string{"v1:applied", "v2:undetermined"})
}

// A rollback deletes the revision row, so its log entry is the only record that
// the version was ever applied. Losing that entry is the loss that matters most.
func TestMigrationLogReport_NamesALostRollback(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	m := newReportMigrator(c,
		migrator.CreateMigrationFromSQL(1, "one", refuseDownLogRows, "SELECT 1;\n"),
	)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrateDown(ctx), qt.IsNil)

	report := m.MigrationLogReport()
	c.Assert(lossesWithoutMessages(c, report), qt.DeepEquals, []migrator.MigrationLogLoss{
		{Operation: "down", Version: 1, VersionKey: "1", State: migrator.MigrationLogStarted},
		{Operation: "down", Version: 1, VersionKey: "1", State: migrator.MigrationLogRolledBack},
	})
	c.Assert(report.Written, qt.Equals, 2)
}

// A derived rollback runs its own statements and logs through its own entry
// point, which is the path a report that only watched the migrator's runs would
// miss.
func TestMigrationLogReport_NamesALostDerivedRollback(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	m := newReportMigrator(c,
		migrator.CreateMigrationFromSQL(1, "one", refuseDownLogRows, "SELECT 1;\n"),
	)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	settle := m.LogDerivedRollback(ctx, 0)
	settle(nil)

	c.Assert(lossesWithoutMessages(c, m.MigrationLogReport()), qt.DeepEquals, []migrator.MigrationLogLoss{
		{Operation: "down", Version: 1, VersionKey: "1", State: migrator.MigrationLogStarted},
		{Operation: "down", Version: 1, VersionKey: "1", State: migrator.MigrationLogRolledBack},
	})
}

// A derived rollback that cannot name the migrations it undoes writes no entry
// for any of them. It has no version to report against, and it is still a loss.
func TestMigrationLogReport_SaysWhenARollbackCouldNotNameItsMigrations(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "report.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	one := migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n")
	two := migrator.CreateMigrationFromSQL(2, "two", "CREATE TABLE two (id INTEGER PRIMARY KEY);\n", "DROP TABLE two;\n")
	full := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one, two))
	c.Assert(full.Initialize(ctx), qt.IsNil)
	c.Assert(full.MigrateUp(ctx), qt.IsNil)
	short := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one))

	short.LogDerivedRollback(ctx, 0)(nil)

	report := short.MigrationLogReport()
	c.Assert(report.Lost, qt.HasLen, 1)
	c.Assert(report.Lost[0].Operation, qt.Equals, "down")
	c.Assert(report.Lost[0].State, qt.Equals, migrator.MigrationLogState(""))
	c.Assert(report.Lost[0].Version, qt.Equals, int64(0))
	c.Assert(report.Lost[0].Message, qt.Contains, "missing from the migration provider")
}

// The same loss, when the history itself cannot be read: the rollback may go on,
// and nothing can say which migrations its entries would have been about.
func TestMigrationLogReport_SaysWhenARollbackCouldNotReadItsHistory(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "report.db"))
	c.Assert(err, qt.IsNil)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
	))
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)

	m.LogDerivedRollback(ctx, 0)(nil)

	report := m.MigrationLogReport()
	c.Assert(report.Lost, qt.HasLen, 1)
	c.Assert(report.Lost[0].Operation, qt.Equals, "down")
	c.Assert(report.Lost[0].State, qt.Equals, migrator.MigrationLogState(""))
	c.Assert(report.Lost[0].Message, qt.Contains, "closed")
}

// A migrator that writes no log has nothing to report, which is not the same
// answer as a report with nothing in it: the first says no row was meant to
// exist, and the second says every row that was meant to exist does.
func TestMigrationLogReport_IsAbsentWhereNoLogIsKept(t *testing.T) {
	tests := []struct {
		name  string
		build func(*migrator.Migrator) *migrator.Migrator
	}{
		{
			name:  "the log is turned off",
			build: func(m *migrator.Migrator) *migrator.Migrator { return m.WithMigrationLog(false) },
		},
		{
			name: "the revision format is the Atlas-compatible one",
			build: func(m *migrator.Migrator) *migrator.Migrator {
				return m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := t.Context()
			m := test.build(newReportMigrator(c,
				migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
			))
			c.Assert(m.Initialize(ctx), qt.IsNil)
			c.Assert(m.MigrateUp(ctx), qt.IsNil)

			c.Assert(m.MigrationLogReport(), qt.IsNil)
		})
	}
}

// A dry run writes nothing and is told apart from a run that lost what it wrote.
func TestMigrationLogReport_IsAbsentFromADryRun(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "report.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	conn.SchemaWriter().SetDryRun(true)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
	))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrationLogReport(), qt.IsNil)
}

// A migrator with no connection only inspects its provider, and asking it is not
// a way to reach a nil connection.
func TestMigrationLogReport_IsAbsentWithoutAConnection(t *testing.T) {
	c := qt.New(t)

	m := migrator.NewMigrator(nil, migrator.NewRegisteredMigrationProvider())

	c.Assert(m.MigrationLogReport(), qt.IsNil)
}

// The returned report is a copy, so a caller that holds it cannot change what a
// later reader is told.
func TestMigrationLogReport_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	m := newReportMigrator(c,
		migrator.CreateMigrationFromSQL(1, "one", refuseDownLogRows, "SELECT 1;\n"),
	)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.MigrateDown(ctx), qt.IsNil)

	first := m.MigrationLogReport()
	first.Lost[0].Version = 99

	c.Assert(m.MigrationLogReport().Lost[0].Version, qt.Equals, int64(1))
}
