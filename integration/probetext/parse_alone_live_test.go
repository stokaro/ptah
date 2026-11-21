//go:build integration

package probetext_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/probetext"
)

// parseAloneEngines are the engines whose routine probes have the server
// parse a statement before it runs.
var parseAloneEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// parseAloneTransaction begins a transaction on engine's server, rolled back
// when the test ends.
func parseAloneTransaction(c *qt.C, engine dbtarget.Engine) *sql.Tx {
	c.Helper()
	db, err := sql.Open("pgx", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	// The transaction outlives the test's context, which ends before the
	// cleanup rolls it back.
	tx, err := db.BeginTx(context.WithoutCancel(c.Context()), nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(tx.Rollback(), qt.IsNil) })
	return tx
}

// parseAloneTablesMade counts the tables named ptah_parse_alone_made the
// transaction sees.
func parseAloneTablesMade(c *qt.C, tx *sql.Tx) int {
	c.Helper()
	var made int
	c.Assert(tx.QueryRowContext(c.Context(),
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'ptah_parse_alone_made'").Scan(&made), qt.IsNil)
	return made
}

// TestParseAlone_LiveOneStatementMayRun accepts one statement and runs none of
// it: the table it would create does not exist afterwards, and the
// transaction goes on.
func TestParseAlone_LiveOneStatementMayRun(t *testing.T) {
	for _, test := range parseAloneEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tx := parseAloneTransaction(c, test.engine)

			refusal, err := probetext.ParseAlone(c.Context(), tx, "CREATE TABLE ptah_parse_alone_made (x int)")

			c.Assert(err, qt.IsNil)
			c.Assert(refusal, qt.IsNil)
			c.Assert(parseAloneTablesMade(c, tx), qt.Equals, 0)
		})
	}
}

// TestParseAlone_LiveSeveralStatementsAreRefused refuses text the server reads
// as two statements, which is what text that leaves a SQL-standard routine
// body looks like, and leaves the transaction usable: the server aborts a
// transaction whose prepare failed, and the savepoint takes the failure back.
func TestParseAlone_LiveSeveralStatementsAreRefused(t *testing.T) {
	for _, test := range parseAloneEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tx := parseAloneTransaction(c, test.engine)

			refusal, err := probetext.ParseAlone(c.Context(), tx,
				"CREATE FUNCTION ptah_parse_alone_f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END; CREATE TABLE ptah_parse_alone_made (x int)")

			c.Assert(err, qt.IsNil)
			c.Assert(refusal, qt.ErrorIs, probetext.ErrUncontained)
			c.Assert(refusal, qt.ErrorMatches, "the server reads more than one statement in its text")
			c.Assert(parseAloneTablesMade(c, tx), qt.Equals, 0)
		})
	}
}

// TestParseAlone_LiveUnparseableTextIsTheServersRefusal answers the server's
// own error for text it cannot parse, which is not a refusal of this package:
// the caller compares the declaration as written without a warning, as it
// does for a statement the server refuses to run.
func TestParseAlone_LiveUnparseableTextIsTheServersRefusal(t *testing.T) {
	for _, test := range parseAloneEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tx := parseAloneTransaction(c, test.engine)

			refusal, err := probetext.ParseAlone(c.Context(), tx, "CREATE TABLE (")

			c.Assert(err, qt.IsNil)
			c.Assert(refusal, qt.ErrorMatches, `.*(syntax error|at or near).*`)
			c.Assert(refusal, qt.Not(qt.ErrorIs), probetext.ErrUncontained)
			c.Assert(parseAloneTablesMade(c, tx), qt.Equals, 0)
		})
	}
}
