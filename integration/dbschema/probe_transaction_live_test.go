//go:build integration

package dbschema_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/probetx"
)

// probeTransactionEngines are the engines whose server names the transaction
// a probe runs in, each with the text that ends the transaction and begins
// another inside one statement.
var probeTransactionEngines = []struct {
	name   string
	engine dbtarget.Engine
	// restart commits the probe transaction and begins another.
	restart string
	// keep creates a savepoint and rolls back to it, as every probe does.
	keep string
	// restartAndFail is restart followed by a statement the server refuses
	// in the transaction restart began.
	restartAndFail string
	// restartAndFailErr is the error a body running restartAndFail fails
	// with. PostgreSQL, CockroachDB and YugabyteDB leave the new transaction
	// aborted, so nothing names it, and the savepoint set before the body is
	// not in it; SQL Server goes on in it and names it.
	restartAndFailErr string
}{
	{
		name: "PostgreSQL", engine: dbtarget.PostgreSQL, restart: "COMMIT; BEGIN", keep: "SAVEPOINT ptah_keep; ROLLBACK TO SAVEPOINT ptah_keep",
		restartAndFail: "COMMIT; BEGIN; SELECT 1/0", restartAndFailErr: `(?s)probe: .*so a probe may have ended the transaction: .*ptah_probe_transaction.*`,
	},
	{
		name: "CockroachDB", engine: dbtarget.CockroachDB, restart: "COMMIT; BEGIN", keep: "SAVEPOINT ptah_keep; ROLLBACK TO SAVEPOINT ptah_keep",
		restartAndFail: "COMMIT; BEGIN; SELECT 1/0", restartAndFailErr: `(?s)probe: .*so a probe may have ended the transaction: .*ptah_probe_transaction.*`,
	},
	{
		name: "YugabyteDB", engine: dbtarget.YugabyteDB, restart: "COMMIT; BEGIN", keep: "SAVEPOINT ptah_keep; ROLLBACK TO SAVEPOINT ptah_keep",
		restartAndFail: "COMMIT; BEGIN; SELECT 1/0", restartAndFailErr: `(?s)probe: .*so a probe may have ended the transaction: .*ptah_probe_transaction.*`,
	},
	{
		name: "SQL Server", engine: dbtarget.SQLServer,
		restart: "COMMIT BEGIN TRANSACTION", keep: "SAVE TRANSACTION ptah_keep ROLLBACK TRANSACTION ptah_keep",
		restartAndFail: "COMMIT BEGIN TRANSACTION SELECT 1/0", restartAndFailErr: `probe: a probe ended the transaction it ran in or changed how it reads statement text; .*`,
	},
}

// probeTransactionConnection connects to engine's server for the test.
func probeTransactionConnection(c *qt.C, engine dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// TestWithRolledBackTransaction_LiveBodyThatEndsTheTransactionFails is the
// check behind the probe text check: a probe whose statements ended the
// transaction it ran in and began another, which the final rollback cannot
// undo, fails the command whatever the probe answered. Without the check the
// body's nil is the answer, and a plan goes on as if nothing had run.
func TestWithRolledBackTransaction_LiveBodyThatEndsTheTransactionFails(t *testing.T) {
	for _, test := range probeTransactionEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, execErr := tx.ExecContext(ctx, test.restart)
				return execErr
			})

			c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
			c.Assert(ran, qt.IsTrue)
		})
	}
}

// TestWithRolledBackTransaction_LiveBodyThatKeepsTheTransactionPasses is the
// control: the server names a transaction the body left alone the same way
// before and after a savepoint the body rolled back to.
func TestWithRolledBackTransaction_LiveBodyThatKeepsTheTransactionPasses(t *testing.T) {
	for _, test := range probeTransactionEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, execErr := tx.ExecContext(ctx, test.keep)
				return execErr
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
		})
	}
}

// TestWithRolledBackTransaction_LiveBodyTheServerRefusedPasses is the control
// for a body that answers the server's refusal of its statement and returns
// nil, as a single-probe body does. PostgreSQL leaves the transaction aborted
// then, and nothing names it until the rollback to the savepoint set before
// the body; without that rollback the check fails every such probe.
func TestWithRolledBackTransaction_LiveBodyTheServerRefusedPasses(t *testing.T) {
	for _, test := range probeTransactionEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, refused := tx.ExecContext(ctx, "SELECT 1/0")
				c.Check(refused, qt.IsNotNil)
				return nil
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
		})
	}
}

// TestWithRolledBackTransaction_LiveBodyThatEndsTheTransactionAndFailsFails
// ends the transaction, begins another and has the server refuse a statement
// in it, which leaves a transaction nothing names on the PostgreSQL family.
// The savepoint set before the body is not in it, so the check fails the body
// rather than taking the unnamed transaction for the one it began.
func TestWithRolledBackTransaction_LiveBodyThatEndsTheTransactionAndFailsFails(t *testing.T) {
	for _, test := range probeTransactionEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, refused := tx.ExecContext(ctx, test.restartAndFail)
				c.Check(refused, qt.IsNotNil)
				return nil
			})

			c.Assert(err, qt.ErrorMatches, test.restartAndFailErr)
			c.Assert(ran, qt.IsTrue)
		})
	}
}

// standardStringEngines are the engines whose sessions can turn
// standard_conforming_strings off; CockroachDB accepts no value but on.
var standardStringEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// TestWithRolledBackTransaction_LiveReadsStringsAsStandard runs the
// transaction with standard_conforming_strings on, the reading probe text is
// checked under, on a session that turned it off, and leaves the session's own
// setting as it was.
func TestWithRolledBackTransaction_LiveReadsStringsAsStandard(t *testing.T) {
	for _, test := range standardStringEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)
			var inside, after string

			err := conn.WithSession(c.Context(), func(session *dbschema.DatabaseConnection) error {
				_, err := session.ExecContext(c.Context(), "SET standard_conforming_strings = off")
				c.Assert(err, qt.IsNil)
				ran, err := session.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, "SELECT current_setting('standard_conforming_strings')").Scan(&inside)
				})
				c.Assert(err, qt.IsNil)
				c.Assert(ran, qt.IsTrue)
				return session.QueryRowContext(c.Context(), "SELECT current_setting('standard_conforming_strings')").Scan(&after)
			})

			c.Assert(err, qt.IsNil)
			c.Assert(inside, qt.Equals, "on")
			c.Assert(after, qt.Equals, "off")
		})
	}
}

// TestWithRolledBackTransaction_LiveBodyThatTurnsStandardStringsOffFails turns
// the setting off from inside the transaction, as a partition bound evaluated
// at creation can. The next probe's text would be read another way than it
// was checked, so the body fails as one that ended the transaction does.
func TestWithRolledBackTransaction_LiveBodyThatTurnsStandardStringsOffFails(t *testing.T) {
	for _, test := range standardStringEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, test.engine)

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, execErr := tx.ExecContext(ctx, "SELECT set_config('standard_conforming_strings', 'off', true)")
				return execErr
			})

			c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
			c.Assert(ran, qt.IsTrue)
		})
	}
}
