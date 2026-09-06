package dbtest_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

var errTerminal = errors.New("connection reset by peer")

// answerEndingIn scripts one result set carrying rows and ending in a terminal
// error rather than in the end of the set.
func answerEndingIn(rows [][]driver.Value) dbtest.QueryHandler {
	return func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns:     []string{"value"},
			Rows:        rows,
			TerminalErr: errTerminal,
		}, nil
	}
}

// TestTerminalErr_ReachesTheCallerThroughRowsErrHappyPath pins what
// QueryResult.TerminalErr is for. It has to arrive the way a dropped connection
// does and no other way: through Rows.Err, after Rows.Next has answered false,
// and NOT out of Query or out of Scan. A harness that surfaced it earlier would
// let a reader missing its terminal check look correct.
func TestTerminalErr_ReachesTheCallerThroughRowsErrHappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, answerEndingIn([][]driver.Value{{int64(1)}}))

	rows, err := db.SQL.Query("SELECT value FROM t")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	c.Assert(rows.Next(), qt.IsTrue)
	var value int64
	c.Assert(rows.Scan(&value), qt.IsNil)
	c.Assert(value, qt.Equals, int64(1))

	c.Assert(rows.Next(), qt.IsFalse)
	c.Assert(rows.Err(), qt.ErrorIs, errTerminal)
}

// TestTerminalErr_IsInvisibleUntilNextRunsFailurePath is the fact three call
// sites in this repository are shaped by, and the one a review of
// stokaro/ptah#2720 and its author both had to rediscover: Rows.Err reads a
// field Rows.Next is the only writer of.
//
// So a result set nobody advanced reports nil however the statement fared, and
// a check placed there reads as handling while being unable to fire. Where a
// query is run for its column list or its side effect alone -- an `ATTACH`, a
// `WHERE 1 = 0` probe -- the set has to be DRIVEN before its terminal error
// means anything.
//
// The two assertions are one measurement: the same result set, the same error,
// answering differently either side of a single Next.
func TestTerminalErr_IsInvisibleUntilNextRunsFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, answerEndingIn(nil))

	rows, err := db.SQL.Query("SELECT value FROM t WHERE 1 = 0")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"value"})
	c.Assert(rows.Err(), qt.IsNil)

	c.Assert(rows.Next(), qt.IsFalse)
	c.Assert(rows.Err(), qt.ErrorIs, errTerminal)
}

// TestTerminalErr_AbsentLeavesAnOrdinaryResultSetHappyPath is the control. Both
// tests above would pass against a harness that failed every query it was
// given, and this is what separates the scripted error from a broken driver.
func TestTerminalErr_AbsentLeavesAnOrdinaryResultSetHappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"value"}, Rows: [][]driver.Value{{int64(1)}}}, nil
	})

	var value int64
	c.Assert(db.SQL.QueryRow("SELECT value FROM t").Scan(&value), qt.IsNil)
	c.Assert(value, qt.Equals, int64(1))

	rows, err := db.SQL.Query("SELECT value FROM t")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	c.Assert(rows.Next(), qt.IsTrue)
	c.Assert(rows.Next(), qt.IsFalse)
	c.Assert(rows.Err(), qt.IsNil)
}
