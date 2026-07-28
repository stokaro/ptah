package shadowdb_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/internal/shadowdb"
)

func TestDatabase_CloseZeroValue(t *testing.T) {
	c := qt.New(t)
	c.Assert(new(shadowdb.Database).Close(), qt.IsNil)
}

func TestOpen_EphemeralSQLite(t *testing.T) {
	c := qt.New(t)
	database, err := shadowdb.Open(t.Context(), "", "ptah-shadowdb-test-*")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Connection().Info().Dialect, qt.Equals, "sqlite")

	_, err = database.Connection().ExecContext(t.Context(), "CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Close(), qt.IsNil)
}

func TestOpen_ExplicitDatabaseIsPreserved(t *testing.T) {
	c := qt.New(t)
	databaseURL := "sqlite://" + filepath.Join(t.TempDir(), "explicit.db")
	database, err := shadowdb.Open(t.Context(), databaseURL, "")
	c.Assert(err, qt.IsNil)

	_, err = database.Connection().ExecContext(t.Context(), "CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Close(), qt.IsNil)

	connection, err := dbschema.ConnectToDatabase(t.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(connection)
	rows, err := connection.QueryContext(t.Context(), "SELECT id FROM widgets")
	c.Assert(err, qt.IsNil)
	c.Assert(rows.Close(), qt.IsNil)
}
