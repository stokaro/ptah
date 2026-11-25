package mysqlconnect_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/connector/mysqlconnect"
	"ptah.run/internal/mysqlstored"
)

// reportedSession is what a session answers the query the window reads it
// with: its sql_mode, the server's VERSION() and its client character set.
type reportedSession struct {
	sqlMode, version, charset string
}

// fakeSessionDriver is a database/sql driver whose every session answers each
// query with one row, the reported session, and records each statement it
// executes. The window asks one query, so one answer serves it.
type fakeSessionDriver struct {
	reported reportedSession
	executed *[]string
}

func (d fakeSessionDriver) Connect(context.Context) (driver.Conn, error) {
	return fakeSessionConn(d), nil
}
func (d fakeSessionDriver) Driver() driver.Driver { return nil }

type fakeSessionConn fakeSessionDriver

var errNoPrepare = errors.New("the fake session prepares no statement")

func (fakeSessionConn) Prepare(string) (driver.Stmt, error) { return nil, errNoPrepare }
func (fakeSessionConn) Close() error                        { return nil }
func (fakeSessionConn) Begin() (driver.Tx, error)           { return nil, errNoPrepare }

func (s fakeSessionConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeSessionRow{values: []driver.Value{s.reported.sqlMode, s.reported.version, s.reported.charset}}, nil
}

func (s fakeSessionConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	*s.executed = append(*s.executed, query)
	return driver.RowsAffected(0), nil
}

// fakeSessionRow is one row of three columns.
type fakeSessionRow struct {
	values []driver.Value
	read   bool
}

func (*fakeSessionRow) Columns() []string { return []string{"sql_mode", "version", "charset"} }
func (*fakeSessionRow) Close() error      { return nil }

func (r *fakeSessionRow) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}

// fakeSession is one session of a server that reports reported, and the
// statements executed on it.
func fakeSession(c *qt.C, reported reportedSession) (*sql.Conn, *[]string) {
	c.Helper()
	executed := new([]string)
	db := sql.OpenDB(fakeSessionDriver{reported: reported, executed: executed})
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	return session, executed
}

// windowTrigger is a trigger after its CREATE and DEFINER clause.
const windowTrigger = " TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1"

// windowQuoteRules are definers whose quote ends only under the sql_mode the
// session reports: NO_BACKSLASH_ESCAPES ends 'a\' at its second quote,
// ANSI_QUOTES ends "a\" there as a name, and MariaDB's MSSQL mode reads
// [a'b] as one name.
var windowQuoteRules = []struct {
	name      string
	session   reportedSession
	statement string
}{
	{name: "NO_BACKSLASH_ESCAPES", session: reportedSession{sqlMode: "NO_BACKSLASH_ESCAPES", version: "8.4.13", charset: "utf8mb4"},
		statement: `CREATE DEFINER='a\'@'%'` + windowTrigger},
	{name: "ANSI_QUOTES", session: reportedSession{sqlMode: "ANSI_QUOTES", version: "8.4.13", charset: "utf8mb4"},
		statement: `CREATE DEFINER="a\"@'%'` + windowTrigger},
	{name: "MSSQL on MariaDB", session: reportedSession{sqlMode: "MSSQL", version: "11.8.9-MariaDB-ubu2404", charset: "utf8mb3"},
		statement: `CREATE DEFINER=[a'b]@'%'` + windowTrigger},
}

// The window reads a statement by the quote rules of the session's own
// sql_mode, which it keeps while it makes the session strict, so it finds the
// trigger each definer is followed by and runs it under that mode.
func TestKeepStoredValues_ReadsTheSessionsQuoteRules(t *testing.T) {
	for _, test := range windowQuoteRules {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session, executed := fakeSession(c, test.session)

			window, err := mysqlconnect.Service{}.KeepStoredValues(c.Context(), session)
			c.Assert(err, qt.IsNil)
			c.Assert(*executed, qt.DeepEquals, []string{"SET SESSION sql_mode = ?"})
			settings, err := window.StoredWith(test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(settings, qt.IsNotNil)
		})
	}
}

// The control: the same statements on a session whose sql_mode holds none of
// those modes run on past the quote, and the window cannot read them. MySQL
// reads no bracketed name whatever its sql_mode says.
func TestKeepStoredValues_FailurePathDefaultQuoteRules(t *testing.T) {
	tests := []struct {
		name      string
		session   reportedSession
		statement string
	}{
		{name: "a backslash", session: reportedSession{sqlMode: "NO_ENGINE_SUBSTITUTION", version: "8.4.13", charset: "utf8mb4"},
			statement: `CREATE DEFINER='a\'@'%'` + windowTrigger},
		{name: "a double quote", session: reportedSession{sqlMode: "NO_ENGINE_SUBSTITUTION", version: "8.4.13", charset: "utf8mb4"},
			statement: `CREATE DEFINER="a\"@'%'` + windowTrigger},
		{name: "a bracket on MariaDB", session: reportedSession{sqlMode: "NO_ENGINE_SUBSTITUTION", version: "11.8.9-MariaDB", charset: "utf8mb4"},
			statement: `CREATE DEFINER=[a'b]@'%'` + windowTrigger},
		{name: "a bracket on MySQL in MSSQL mode", session: reportedSession{sqlMode: "MSSQL", version: "8.4.13", charset: "utf8mb4"},
			statement: `CREATE DEFINER=[a'b]@'%'` + windowTrigger},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session, _ := fakeSession(c, test.session)

			window, err := mysqlconnect.Service{}.KeepStoredValues(c.Context(), session)
			c.Assert(err, qt.IsNil)
			settings, err := window.StoredWith(test.statement)

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableDefinition)
			c.Assert(settings, qt.IsNil)
		})
	}
}

// A loose session whose client character set is outside the UTF-8 family
// reads a statement's bytes by other rules than the window does, so the
// window refuses to open, before it makes the session strict.
func TestKeepStoredValues_FailurePathClientCharacterSet(t *testing.T) {
	for _, charset := range []string{"latin1", "gbk", "binary"} {
		t.Run(charset, func(t *testing.T) {
			c := qt.New(t)
			session, executed := fakeSession(c, reportedSession{sqlMode: "NO_ENGINE_SUBSTITUTION", version: "8.4.13", charset: charset})

			window, err := mysqlconnect.Service{}.KeepStoredValues(c.Context(), session)

			c.Assert(err, qt.ErrorMatches, `the session's client character set is "`+charset+`", and the statements are read as UTF-8 .*`)
			c.Assert(window, qt.IsNil)
			c.Assert(*executed, qt.HasLen, 0)
		})
	}
}

// A strict session needs no window and reads no statement, so its client
// character set does not matter; a loose session in any UTF-8 spelling opens
// the window.
func TestKeepStoredValues_ClientCharacterSetOnlyMattersToAWindow(t *testing.T) {
	c := qt.New(t)
	strict, executed := fakeSession(c, reportedSession{sqlMode: "STRICT_TRANS_TABLES", version: "8.4.13", charset: "latin1"})
	window, err := mysqlconnect.Service{}.KeepStoredValues(c.Context(), strict)
	c.Assert(err, qt.IsNil)
	c.Assert(window, qt.IsNil)
	c.Assert(*executed, qt.HasLen, 0)

	for _, charset := range []string{"utf8mb4", "utf8mb3", "utf8", "UTF8MB4"} {
		t.Run(charset, func(t *testing.T) {
			c := qt.New(t)
			session, executed := fakeSession(c, reportedSession{sqlMode: "NO_ENGINE_SUBSTITUTION", version: "8.4.13", charset: charset})

			window, err := mysqlconnect.Service{}.KeepStoredValues(c.Context(), session)

			c.Assert(err, qt.IsNil)
			c.Assert(window, qt.IsNotNil)
			c.Assert(*executed, qt.DeepEquals, []string{"SET SESSION sql_mode = ?"})
		})
	}
}
