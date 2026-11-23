package dbschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// TestDatabaseConnectionSingleSession_InMemory names the databases that live
// in their connection's one session. Each in-memory spelling is one, the
// shared cache included, since that database still ends with its last
// connection.
func TestDatabaseConnectionSingleSession_InMemory(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "the memory path", url: "sqlite://:memory:"},
		{name: "a named database in memory", url: "sqlite://file?mode=memory"},
		{name: "memory beside another parameter", url: "sqlite://dev?mode=memory&_fk=1"},
		{name: "memory with a shared cache", url: "sqlite://shared?mode=memory&cache=shared"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), test.url)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			c.Assert(conn.SingleSession(), qt.IsTrue)
		})
	}
}

// TestDatabaseConnectionSingleSession_File is the control: a database file is
// reached from any connection, so the same URL opened again reads it.
func TestDatabaseConnectionSingleSession_File(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "file.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	c.Assert(conn.SingleSession(), qt.IsFalse)
}

// TestDatabaseConnectionSingleSession_Nil reads a nil connection as one that
// holds no database of its own, so a caller that holds none may ask.
func TestDatabaseConnectionSingleSession_Nil(t *testing.T) {
	c := qt.New(t)
	var conn *dbschema.DatabaseConnection

	c.Assert(conn.SingleSession(), qt.IsFalse)
}

// TestDatabaseConnectionSharesPool_HappyPath pairs a connection with itself,
// with the session copy WithSession hands its callback, and with the copy
// WithExecutor returns. A private in-memory database is reached through the
// pool that opened it, so a pair sharing one reaches one database.
func TestDatabaseConnectionSharesPool_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://pool?mode=memory")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	var scopedShares bool

	err = conn.WithSession(c.Context(), func(scoped *dbschema.DatabaseConnection) error {
		scopedShares = scoped.SharesPool(conn)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedShares, qt.IsTrue)
	c.Assert(conn.WithExecutor(conn.Writer()).SharesPool(conn), qt.IsTrue)
	c.Assert(conn.SharesPool(conn), qt.IsTrue)
}

// TestDatabaseConnectionSharesPool_FailurePath pairs two opens of one URL,
// which reach two private in-memory databases, and a nil connection.
func TestDatabaseConnectionSharesPool_FailurePath(t *testing.T) {
	c := qt.New(t)
	const memoryURL = "sqlite://pool?mode=memory"
	first, err := dbschema.ConnectToDatabase(c.Context(), memoryURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(first) })
	second, err := dbschema.ConnectToDatabase(c.Context(), memoryURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(second) })
	var none *dbschema.DatabaseConnection

	c.Assert(first.SharesPool(second), qt.IsFalse)
	c.Assert(first.SharesPool(none), qt.IsFalse)
	c.Assert(none.SharesPool(first), qt.IsFalse)
}
