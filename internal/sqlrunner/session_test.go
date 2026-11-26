package sqlrunner_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/sqlrunner"
)

// noRows answers every question with no rows.
func noRows(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{}, nil
}

// heldSession opens a pool and takes one session of it.
func heldSession(c *qt.C) (*sql.DB, *sql.Conn) {
	c.Helper()
	db := dbtest.Open(c, noRows)
	conn, err := db.SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	return db.SQL, conn
}

// A released session stays open in the pool, idle, and releasing it again
// changes nothing.
func TestRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	pool, conn := heldSession(c)

	err := sqlrunner.Release(conn, "the session")

	c.Assert(err, qt.IsNil)
	c.Assert(pool.Stats().OpenConnections, qt.Equals, 1)
	c.Assert(pool.Stats().Idle, qt.Equals, 1)
	c.Assert(sqlrunner.Release(conn, "the session"), qt.IsNil)
	c.Assert(pool.Stats().OpenConnections, qt.Equals, 1)
}

// A discarded session is closed, and the pool keeps nothing of it.
func TestDiscard_HappyPath(t *testing.T) {
	c := qt.New(t)
	pool, conn := heldSession(c)

	err := sqlrunner.Discard(conn, "the session")

	c.Assert(err, qt.IsNil)
	c.Assert(pool.Stats().OpenConnections, qt.Equals, 0)
	c.Assert(pool.Stats().InUse, qt.Equals, 0)
}

// A session already given back has nothing left to discard: the pool keeps it
// idle for its next caller.
func TestDiscard_ASessionAlreadyGivenBack_HappyPath(t *testing.T) {
	c := qt.New(t)
	pool, conn := heldSession(c)
	c.Assert(sqlrunner.Release(conn, "the session"), qt.IsNil)

	err := sqlrunner.Discard(conn, "the session")

	c.Assert(err, qt.IsNil)
	c.Assert(pool.Stats().OpenConnections, qt.Equals, 1)
	c.Assert(pool.Stats().Idle, qt.Equals, 1)
}
