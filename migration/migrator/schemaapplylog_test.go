package migrator_test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// applyLogRow is one row of ptah_schema_apply_log, with NULL read as empty or
// zero.
type applyLogRow struct {
	Seq            int64
	RunReference   string
	State          string
	Actor          string
	ActorSource    string
	ActorOrigin    string
	PlanName       string
	PlanDigest     string
	StatementCount int
	ErrorPosition  int
	Error          string
	ErrorStatement string
}

// newApplyLogDatabase opens a fresh SQLite database.
func newApplyLogDatabase(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "apply.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

// applyLogRows reads the table back in the order the apply wrote it, with
// each row's run_id checked against the first.
func applyLogRows(c *qt.C, conn *dbschema.DatabaseConnection) []applyLogRow {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `SELECT run_id, seq, run_reference, state, actor, actor_source,
actor_origin, plan_name, plan_digest, statement_count, error_position, error, error_stmt
FROM ptah_schema_apply_log ORDER BY seq`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var (
		found  []applyLogRow
		runIDs []string
	)
	for rows.Next() {
		var (
			row                                                    applyLogRow
			runID                                                  string
			reference, actor, origin, name, digest, failure, query sql.NullString
			position                                               sql.NullInt64
		)
		c.Assert(rows.Scan(&runID, &row.Seq, &reference, &row.State, &actor, &row.ActorSource,
			&origin, &name, &digest, &row.StatementCount, &position, &failure, &query), qt.IsNil)
		row.RunReference, row.Actor, row.ActorOrigin = reference.String, actor.String, origin.String
		row.PlanName, row.PlanDigest, row.Error, row.ErrorStatement = name.String, digest.String, failure.String, query.String
		row.ErrorPosition = int(position.Int64)
		runIDs = append(runIDs, runID)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	for _, runID := range runIDs {
		c.Assert(runID, qt.Equals, runIDs[0], qt.Commentf("one apply writes under one run_id"))
	}
	return found
}

// applyLogTableCount says whether the table exists.
func applyLogTableCount(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var tables int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ptah_schema_apply_log'",
	).Scan(&tables), qt.IsNil)
	return tables
}

var appliedPlan = migrator.SchemaApplyLogPlan{
	Name:       "20261113120000",
	Digest:     "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
	Statements: 2,
}

// An apply that ran records its start and its outcome under one run, each
// with the plan, the caller's reference, and who the caller said it is.
func TestSchemaApplyLog_RecordsAnApplyThatRan_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{
		Actor: "alice@example.com", ActorOrigin: "admission", RunReference: operationID,
	})

	log.Start(ctx, conn, appliedPlan)
	log.Finish(ctx, migrator.SchemaApplyLogOutcome{State: migrator.SchemaApplyLogApplied})

	written := applyLogRow{
		RunReference: operationID, Actor: "alice@example.com", ActorSource: string(migrator.ActorProvided),
		ActorOrigin: "admission", PlanName: appliedPlan.Name, PlanDigest: appliedPlan.Digest, StatementCount: 2,
	}
	started, applied := written, written
	started.Seq, started.State = 1, "started"
	applied.Seq, applied.State = 2, "applied"
	c.Assert(applyLogRows(c, conn), qt.DeepEquals, []applyLogRow{started, applied})
	c.Assert(log.Report(), qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: operationID})
}

// A failed apply records where it stopped: the position, the statement, and
// what the database said.
func TestSchemaApplyLog_RecordsWhereAnApplyStopped_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{Actor: "alice@example.com", RunReference: operationID})

	log.Start(ctx, conn, migrator.SchemaApplyLogPlan{Statements: 3})
	log.Finish(ctx, migrator.SchemaApplyLogOutcome{
		State:     migrator.SchemaApplyLogFailed,
		Error:     "apply schema changes: UNIQUE constraint failed: users.email",
		Position:  2,
		Statement: `CREATE UNIQUE INDEX "users_email" ON "users" ("email")`,
	})

	rows := applyLogRows(c, conn)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[0].State, qt.Equals, "started")
	c.Assert(rows[0].ErrorPosition, qt.Equals, 0)
	c.Assert(rows[1], qt.DeepEquals, applyLogRow{
		Seq: 2, RunReference: operationID, State: "failed", Actor: "alice@example.com",
		ActorSource: string(migrator.ActorProvided), StatementCount: 3, ErrorPosition: 2,
		Error:          "apply schema changes: UNIQUE constraint failed: users.email",
		ErrorStatement: `CREATE UNIQUE INDEX "users_email" ON "users" ("email")`,
	})
}

// An origin describes a name the caller supplied, so beside the process user
// none is recorded.
func TestSchemaApplyLog_RecordsNoOriginBesideTheProcessUser_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{ActorOrigin: "admission"})

	log.Start(ctx, conn, appliedPlan)

	rows := applyLogRows(c, conn)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].ActorSource, qt.Not(qt.Equals), string(migrator.ActorProvided))
	c.Assert(rows[0].ActorOrigin, qt.Equals, "")
	c.Assert(rows[0].RunReference, qt.Equals, "")
	c.Assert(log.Report(), qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 1})
}

// An apply that never started has nothing to record and nothing to report:
// its outcome is not written, and the table is not created.
func TestSchemaApplyLog_WritesNothingForAnApplyThatNeverStarted_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{RunReference: operationID})

	log.Finish(ctx, migrator.SchemaApplyLogOutcome{State: migrator.SchemaApplyLogApplied})

	c.Assert(log.Report(), qt.IsNil)
	c.Assert(applyLogTableCount(c, conn), qt.Equals, 0)
}

// A nil log writes nothing and reports nothing, so a caller that keeps no log
// calls it the same way.
func TestSchemaApplyLog_NilRecordsNothing_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	var log *migrator.SchemaApplyLog

	log.Start(ctx, conn, appliedPlan)
	log.Finish(ctx, migrator.SchemaApplyLogOutcome{State: migrator.SchemaApplyLogApplied})

	c.Assert(log.Report(), qt.IsNil)
	c.Assert(applyLogTableCount(c, conn), qt.Equals, 0)
}

// The table is Ptah's bookkeeping, so the next apply's reader must not see it:
// read as a table nothing declares, it would be planned for a DROP.
func TestSchemaApplyLog_IsInvisibleToTheSchemaReader_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	_, err := conn.ExecContext(ctx, "CREATE TABLE notes (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{}).Start(ctx, conn, appliedPlan)
	c.Assert(applyLogTableCount(c, conn), qt.Equals, 1)

	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)

	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	c.Assert(names, qt.Not(qt.Contains), "ptah_schema_apply_log")
	c.Assert(names, qt.Contains, "notes")
}

// A refused row does not stop the apply: the entry is lost, a person is told,
// and the report names it against the state it would have recorded.
func TestSchemaApplyLog_ReportsAnEntryTheDatabaseRefused_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	_, err := conn.ExecContext(ctx, `CREATE TABLE ptah_schema_apply_log (
    run_id VARCHAR(64) NOT NULL, seq BIGINT NOT NULL, run_reference VARCHAR(255) NULL,
    state VARCHAR(32) NOT NULL, actor VARCHAR(256) NULL, actor_source VARCHAR(32) NOT NULL,
    actor_origin VARCHAR(64) NULL, plan_name VARCHAR(255) NULL, plan_digest VARCHAR(80) NULL,
    statement_count INTEGER NOT NULL, error_position INTEGER NULL, logged_at TIMESTAMP NOT NULL,
    error TEXT NULL, error_stmt TEXT NULL, PRIMARY KEY (run_id, seq))`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TRIGGER refuse_outcomes BEFORE INSERT ON ptah_schema_apply_log
WHEN NEW.state <> 'started'
BEGIN
  SELECT RAISE(ABORT, 'this log row is refused');
END`)
	c.Assert(err, qt.IsNil)
	var warnings bytes.Buffer
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{
		RunReference: operationID,
		Logger:       slog.New(slog.NewTextHandler(&warnings, nil)),
	})

	log.Start(ctx, conn, appliedPlan)
	log.Finish(ctx, migrator.SchemaApplyLogOutcome{State: migrator.SchemaApplyLogApplied})

	report := log.Report()
	c.Assert(report, qt.IsNotNil)
	c.Assert(report.Written, qt.Equals, 1)
	c.Assert(report.RunReference, qt.Equals, operationID)
	c.Assert(report.Lost, qt.HasLen, 1)
	c.Assert(report.Lost[0].State, qt.Equals, migrator.SchemaApplyLogApplied)
	c.Assert(report.Lost[0].Message, qt.Contains, "this log row is refused")
	c.Assert(warnings.String(), qt.Contains, "Could not append to the schema apply log")
	c.Assert(applyLogRows(c, conn), qt.HasLen, 1)
}

// Report hands out a copy, so a caller that keeps one is not changed by a
// later write.
func TestSchemaApplyLog_ReportReturnsACopy_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newApplyLogDatabase(c)
	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{})
	log.Start(ctx, conn, appliedPlan)
	first := log.Report()

	log.Finish(ctx, migrator.SchemaApplyLogOutcome{State: migrator.SchemaApplyLogApplied})

	c.Assert(first, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 1})
	c.Assert(log.Report(), qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2})
}
