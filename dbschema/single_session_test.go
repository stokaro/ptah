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
