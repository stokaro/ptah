//go:build integration

package dbschema_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/probetext"
)

// probeStatementEngines are the PostgreSQL-family engines whose probe
// statements run through probetext.Exec.
var probeStatementEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// probeStatementSequence creates a sequence on engine's server for the test,
// dropped when it ends, and returns its name. A sequence keeps what nextval
// takes from it when the transaction that took it rolls back, so it says
// whether a statement ran.
func probeStatementSequence(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	name := "ptah_probe_statement_" + strings.ToLower(rand.Text())
	_, err := conn.ExecContext(c.Context(), "CREATE SEQUENCE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.WithoutCancel(c.Context()), "DROP SEQUENCE IF EXISTS "+name)
		c.Check(err, qt.IsNil)
	})
	return name
}

// nextValue takes the next value of sequence, outside any transaction.
func nextValue(c *qt.C, conn *dbschema.DatabaseConnection, sequence string) int64 {
	c.Helper()
	var value int64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT nextval('"+sequence+"')").Scan(&value), qt.IsNil)
	return value
}

// twoStatements takes a value of sequence and then creates a table, the shape
// a declaration takes when the server ends its statement where the text check
// does not.
func twoStatements(sequence, table string) string {
	return "SELECT nextval('" + sequence + "'); CREATE TABLE " + table + " (x int)"
}

// tablesNamed counts the tables named table that tx sees.
func tablesNamed(ctx context.Context, tx *sql.Tx, table string) (int, error) {
	var made int
	err := tx.QueryRowContext(ctx, "SELECT count(*) FROM pg_class WHERE relname = $1", table).Scan(&made)
	return made, err
}

// refusedInSavepoint runs statement through probetext.Exec inside a
// savepoint and returns to the savepoint, so the transaction goes on after a
// refusal. refused is what the server answered the statement; err is a
// failure of the savepoint.
func refusedInSavepoint(ctx context.Context, tx *sql.Tx, statement string) (refused, err error) {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT ptah_probe_statement"); err != nil {
		return nil, err
	}
	refused = probetext.Exec(ctx, tx, statement)
	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT ptah_probe_statement")
	return refused, err
}

// plainExec is tx.ExecContext, which pgx sends on the simple query protocol
// for a statement with no arguments.
func plainExec(ctx context.Context, tx *sql.Tx, statement string) error {
	_, err := tx.ExecContext(ctx, statement)
	return err
}

// madeBy runs statement with run and counts the tables named table that tx
// then sees.
func madeBy(ctx context.Context, tx *sql.Tx, run func(context.Context, *sql.Tx, string) error, statement, table string) (int, error) {
	if err := run(ctx, tx, statement); err != nil {
		return 0, err
	}
	return tablesNamed(ctx, tx, table)
}

// sqlState is the SQLSTATE err carries, empty when it carries none.
func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) {
		return ""
	}
	return coded.SQLState()
}

// TestExec_LiveRefusesTwoStatements runs a statement that holds two through
// probetext.Exec on each PostgreSQL-family server. The server parses it on
// the extended protocol and refuses it before it runs either: PostgreSQL and
// YugabyteDB answer SQLSTATE 42601, CockroachDB 42P14, and the sequence the
// first statement takes a value of has given none, which no rollback could
// have undone. Exec reads each server's refusal as text the server read as
// more than one statement, so the caller warns about the declaration. The
// savepoint returns the aborted transaction to the state the transaction
// check reads.
func TestExec_LiveRefusesTwoStatements(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
		code   string
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, code: "42601"},
		{name: "CockroachDB", engine: dbtarget.CockroachDB, code: "42P14"},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB, code: "42601"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)
			sequence := probeStatementSequence(c, conn)
			var refused error

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				var err error
				refused, err = refusedInSavepoint(ctx, tx, twoStatements(sequence, sequence+"_made"))
				return err
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
			c.Assert(sqlState(refused), qt.Equals, test.code, qt.Commentf("%v", refused))
			c.Assert(refused, qt.ErrorIs, probetext.ErrUncontained)
			c.Assert(refused, qt.ErrorMatches, "the server reads more than one statement in its text")
			c.Assert(nextValue(c, conn, sequence), qt.Equals, int64(1))
		})
	}
}

// TestExec_LiveTwoStatementsRunWithoutAPrepare is the control for
// TestExec_LiveRefusesTwoStatements: the same text through tx.ExecContext,
// which pgx sends on the simple query protocol, runs both statements. The
// transaction sees the table the second creates, and the value the first
// took stays taken after the rollback. The next value is 2 on PostgreSQL and
// CockroachDB; YugabyteDB 2026.1.2 hands a session values from a cache of
// 100, so another session's next value there is 101.
func TestExec_LiveTwoStatementsRunWithoutAPrepare(t *testing.T) {
	for _, test := range probeStatementEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)
			sequence := probeStatementSequence(c, conn)
			var made int

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				var err error
				made, err = madeBy(ctx, tx, plainExec, twoStatements(sequence, sequence+"_made"), sequence+"_made")
				return err
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
			c.Assert(made, qt.Equals, 1)
			c.Assert(nextValue(c, conn, sequence), qt.Not(qt.Equals), int64(1))
		})
	}
}

// TestExec_LiveRunsOneStatement runs a probe statement that is one statement
// through probetext.Exec, and the transaction sees what it made.
func TestExec_LiveRunsOneStatement(t *testing.T) {
	for _, test := range probeStatementEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)
			sequence := probeStatementSequence(c, conn)
			var made int

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				var err error
				made, err = madeBy(ctx, tx, probetext.Exec, "CREATE TABLE "+sequence+"_made (x int)", sequence+"_made")
				return err
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
			c.Assert(made, qt.Equals, 1)
		})
	}
}
