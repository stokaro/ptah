package atlasschema_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

// A row of data, as a server's message and a failing statement quote it.
const reportRowValue = "row-value@example.test"

// statementFailure is the error a statement that a server answered leaves, wrapped
// the way the statement loop and the command wrap it.
func statementFailure(index, total int, answer error) error {
	return fmt.Errorf("apply schema changes: %w", &migrator.MigrationExecutionError{
		Err:            fmt.Errorf("failed to execute SQL statement: %w", answer),
		Statement:      "INSERT INTO accounts (email) VALUES ('" + reportRowValue + "')",
		StatementIndex: index,
		Total:          total,
	})
}

// TestNewApplyReport_NamesWhereADispatchedRunStopped pins what each kind of failure
// after dispatch reports. The record reads the failing step's own error, so an
// answer beside it, a rollback that failed too, is not its code.
func TestNewApplyReport_NamesWhereADispatchedRunStopped(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want *atlasschema.ApplyFailure
	}{
		{
			name: "a statement PostgreSQL refused",
			err:  statementFailure(3, 5, &pgconn.PgError{Code: "23505", Message: "duplicate key " + reportRowValue}),
			want: &atlasschema.ApplyFailure{Statement: 3, Statements: 5, SQLState: "23505"},
		},
		{
			name: "a statement MySQL refused",
			err: statementFailure(3, 5, &mysqldriver.MySQLError{
				Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry '" + reportRowValue + "'",
			}),
			want: &atlasschema.ApplyFailure{Statement: 3, Statements: 5, SQLState: "23000", ErrorNumber: 1062},
		},
		{
			name: "a statement SQL Server refused",
			err:  statementFailure(1, 1, mssql.Error{Number: 2627, Message: "The duplicate key value is (" + reportRowValue + ")."}),
			want: &atlasschema.ApplyFailure{Statement: 1, Statements: 1, ErrorNumber: 2627},
		},
		{
			name: "a statement no server answered",
			err:  statementFailure(2, 2, errors.New("connection reset by peer")),
			want: &atlasschema.ApplyFailure{Statement: 2, Statements: 2},
		},
		{
			name: "a statement whose rollback failed too",
			err: errors.Join(
				statementFailure(2, 2, &pgconn.PgError{Code: "23505"}),
				&pgconn.PgError{Code: "57P01", Message: "terminating connection"},
			),
			want: &atlasschema.ApplyFailure{Statement: 2, Statements: 2, SQLState: "23505"},
		},
		{
			name: "a rollback's failure ahead of the statement's",
			err: errors.Join(
				&pgconn.PgError{Code: "57P01", Message: "terminating connection"},
				statementFailure(2, 2, &pgconn.PgError{Code: "23505"}),
			),
			want: &atlasschema.ApplyFailure{Statement: 2, Statements: 2, SQLState: "23505"},
		},
		{
			name: "a provider that confirmed nothing",
			err:  &dbschema.UnitExecutionError{Err: errors.New("NOT_APPLIED"), Total: 4},
			want: &atlasschema.ApplyFailure{Statement: 1, Statements: 4},
		},
		{
			name: "a provider that confirmed a prefix",
			err:  &dbschema.UnitExecutionError{Err: errors.New("PARTIAL"), Applied: 2, Total: 4},
			want: &atlasschema.ApplyFailure{Statement: 3, Statements: 4},
		},
		{
			name: "a provider that confirmed every unit and did not call it a success",
			err:  &dbschema.UnitExecutionError{Err: errors.New("UNKNOWN"), Applied: 4, Total: 4},
			want: &atlasschema.ApplyFailure{Statements: 4},
		},
		{name: "a failure that is neither a statement nor a commit", err: errors.New("the lock was lost while it was held")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Dispatched: true, Err: test.err})

			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
			c.Assert(report.Failure, qt.DeepEquals, test.want)
		})
	}
}

// An error can name a statement before anything left: a preflight does, and so
// does a rehearsal that failed on the dev database. The target never saw it, so
// the record stays empty. The same error after dispatch is a failure the database
// answered, which is the control for the row.
func TestNewApplyReport_HasNoRecordBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	named := statementFailure(1, 1, &pgconn.PgError{Code: "42P01"})

	before := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Err: named})
	after := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Dispatched: true, Err: named})

	c.Assert(before.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
	c.Assert(before.Failure, qt.IsNil)
	c.Assert(after.Failure, qt.IsNotNil)
}

// The record holds no text of the statement or the server's message: that is what
// it is for. The message beside it holds both, so the fixture has something to
// keep out.
func TestApplyFailure_CarriesNoText(t *testing.T) {
	c := qt.New(t)
	report := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{
		Dispatched: true,
		Err:        statementFailure(3, 5, &pgconn.PgError{Code: "23505", Message: "duplicate key " + reportRowValue, Detail: reportRowValue}),
	})

	encoded, err := json.Marshal(report.Failure)

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"statement":3,"statements":5,"sqlstate":"23505"}`)
	c.Assert(report.Error.Message, qt.Contains, reportRowValue)
}

// SQLite's driver gives no SQLSTATE and no number the record reads, so the keys
// are absent rather than empty, and the failure is what a real statement leaves on
// its way up, in each mode that runs it.
func TestApplyStatements_RecordsTheFailedStatementOfARealRun(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll, migrator.MigrationTxModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			var evidence atlasschema.ApplyEvidence

			witnessedApply(c.Context(), conn, mode, []string{
				"CREATE TABLE first_table (id INTEGER);", "CREATE TABLE broken (", "CREATE TABLE third_table (id INTEGER);",
			}, &evidence)

			encoded, err := json.Marshal(atlasschema.NewApplyReport(evidence).Failure)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, `{"statement":2,"statements":3}`)
		})
	}
}

// A statement that holds only comments is not sent, and a plan file written by hand
// can list one. The report's units list it all the same, so the position counts it:
// units[statement-1] stays the statement that failed.
func TestApplyStatements_CountsAStatementItDoesNotSend(t *testing.T) {
	statements := []string{"-- a note, and no statement", "CREATE TABLE first_table (id INTEGER);", "CREATE TABLE broken (", "-- another note"}
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			var evidence atlasschema.ApplyEvidence

			witnessedApply(c.Context(), conn, mode, statements, &evidence)

			c.Assert(atlasschema.NewApplyReport(evidence).Failure, qt.DeepEquals, &atlasschema.ApplyFailure{Statement: 3, Statements: 4})
		})
	}
}

// A deferred foreign key is answered at the commit. Every statement ran, so no
// position is to blame, and the commit is what the record names.
func TestApplyStatements_RecordsAFailedCommit(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err := conn.ExecContext(c.Context(), "CREATE TABLE parents (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE children (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents (id) DEFERRABLE INITIALLY DEFERRED)")
	c.Assert(err, qt.IsNil)
	var evidence atlasschema.ApplyEvidence

	witnessedApply(c.Context(), conn, migrator.MigrationTxModeFile, []string{
		"INSERT INTO children (id, parent) VALUES (1, 999);", "INSERT INTO children (id, parent) VALUES (2, 999);",
	}, &evidence)

	c.Assert(evidence.Err, qt.ErrorMatches, "(?s)commit schema apply transaction: .*")
	report := atlasschema.NewApplyReport(evidence)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
	c.Assert(report.Failure, qt.DeepEquals, &atlasschema.ApplyFailure{Statements: 2, Commit: true})
}

// A computed plan reaches the commit by another way than a list of statements does,
// so the count of its units is its own.
func TestCapturedApply_RecordsAFailedCommit(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err := conn.ExecContext(c.Context(), "CREATE TABLE parents (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE children (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents (id) DEFERRABLE INITIALLY DEFERRED)")
	c.Assert(err, qt.IsNil)
	info := conn.Info()
	statements := []string{"INSERT INTO children (id, parent) VALUES (1, 999);", "INSERT INTO children (id, parent) VALUES (2, 999);"}
	captured, err := atlasschema.CaptureApplyPlan(info, planner.RenderedPlan{
		Request: renderer.Request{Target: info.Dialect, Capabilities: info.Capabilities, Nodes: []ast.Node{ast.NewRawSQL(statements[0]), ast.NewRawSQL(statements[1])}},
		Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: renderer.SQLFragments(statements)},
		Dialect: info.Dialect,
	})
	c.Assert(err, qt.IsNil)
	evidence := atlasschema.ApplyEvidence{Dispatched: true}

	evidence.Err = captured.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(atlasschema.NewApplyReport(evidence).Failure, qt.DeepEquals, &atlasschema.ApplyFailure{Statements: 2, Commit: true})
}
