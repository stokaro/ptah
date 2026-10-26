package dbschema_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

// statementHandle is a native session whose provider runs operator
// statements on an in-memory SQLite database, recording the access each
// handle was opened with.
type statementHandle struct {
	*nativeHandle
	opened []bool
	db     *sql.DB
}

func (h *statementHandle) OperatorStatements(readOnly bool) (*sql.DB, error) {
	h.opened = append(h.opened, readOnly)
	db, err := sql.Open("sqlite", ":memory:")
	h.db = db
	return db, err
}

type statementFixture struct {
	nativeFixture
	handle *statementHandle
}

func (f *statementFixture) Open(ctx context.Context, request dbschema.NativeRequest) (dbschema.NativeSession, error) {
	session, err := f.nativeFixture.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	f.handle = &statementHandle{nativeHandle: session.(*nativeHandle)}
	return f.handle, nil
}

func statementConnection(c *qt.C, fixture dbschema.NativeConnector) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"example": {Target: "example", Connector: fixture}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "example://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// A native connection reaches its provider's statement service: an isolated
// query session opens a read-only handle, the operator's statements a
// read-write one, and each is closed when its caller is done.
func TestOperatorStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	fixture := &statementFixture{}
	conn := statementConnection(c, fixture)

	var answer int
	c.Assert(conn.WithIsolatedQuerySession(c.Context(), nil, func(queryer dbschema.IsolatedQueryer) error {
		rows, err := queryer.QueryContext(c.Context(), "SELECT 1")
		c.Assert(err, qt.IsNil)
		defer func() { c.Assert(rows.Close(), qt.IsNil) }()
		c.Assert(rows.Next(), qt.IsTrue)
		c.Assert(rows.Scan(&answer), qt.IsNil)
		return rows.Err()
	}), qt.IsNil)
	c.Assert(answer, qt.Equals, 1)
	c.Assert(fixture.handle.db.PingContext(c.Context()), qt.ErrorMatches, "sql: database is closed")

	statements, release, err := conn.OperatorStatements()
	c.Assert(err, qt.IsNil)
	_, err = statements.ExecContext(c.Context(), "CREATE TABLE t (id INTEGER)")
	c.Assert(err, qt.IsNil)
	c.Assert(release(), qt.IsNil)
	c.Assert(fixture.handle.opened, qt.DeepEquals, []bool{true, false})
}

// A database/sql connection is its own handle, and a native connection whose
// provider runs no statement refuses both entries.
func TestOperatorStatements_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := statementConnection(c, &nativeFixture{})
	statements, release, err := conn.OperatorStatements()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
	c.Assert(release, qt.IsNil)
	called := false
	err = conn.WithIsolatedQuerySession(c.Context(), nil, func(dbschema.IsolatedQueryer) error {
		called = true
		return nil
	})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(called, qt.IsFalse)

	sqlite, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sqlite.Close(), qt.IsNil) })
	own, done, err := sqlite.OperatorStatements()
	c.Assert(err, qt.IsNil)
	c.Assert(own, qt.Equals, dbschema.OperatorStatements(sqlite))
	c.Assert(done(), qt.IsNil)
}
