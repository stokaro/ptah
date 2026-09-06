package dbschematogo_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
)

// openSQLite returns a connection to a fresh database file under the checker's
// own temporary directory.
func openSQLite(c *qt.C, name string) *dbschema.DatabaseConnection {
	url := "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), name))
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

// describeSQLite writes ddl into a fresh database, reads the catalog back and
// converts it the way `ptah db read` does.
func describeSQLite(c *qt.C, ddl string) []string {
	conn := openSQLite(c, "source.db")
	c.Assert(conn.Writer().ExecuteSQL(context.Background(), ddl), qt.IsNil)

	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	model := dbschematogo.ConvertDBSchemaToGoSchema(live, conn.Info().Dialect)
	names := make([]string, 0, len(model.Indexes))
	for _, index := range model.Indexes {
		names = append(names, index.Name)
	}
	return names
}

// TestConvert_SQLiteNamedUniqueIsOneObject covers the shape that made an
// inspected SQLite database impossible to replay.
//
// SQLite enforces a UNIQUE constraint with an index it names itself. Where the
// constraint carries a name of its own, pragma index_list reports
// `sqlite_autoindex_<table>_N` while the constraint keeps the DDL's name, so a
// converter deciding ownership by name equality sees two objects and describes
// both (stokaro/ptah#2894).
func TestConvert_SQLiteNamedUniqueIsOneObject(t *testing.T) {
	c := qt.New(t)

	names := describeSQLite(c, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		CONSTRAINT uq_users_email UNIQUE (email)
	)`)

	c.Assert(names, qt.HasLen, 0)
}

// TestConvert_SQLiteDescribesAnOrdinaryIndex is the control. The rule above
// removes an index the server named for itself, and a rule that removed every
// unique index would satisfy the assertion above while describing nothing.
func TestConvert_SQLiteDescribesAnOrdinaryIndex(t *testing.T) {
	c := qt.New(t)

	names := describeSQLite(c, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL
	);
	CREATE UNIQUE INDEX idx_users_email ON users (email)`)

	c.Assert(names, qt.DeepEquals, []string{"idx_users_email"})
}

// TestConvert_SQLiteDescriptionReplaysIntoAFreshDatabase is the property the
// two tests above exist for: what a read describes, SQLite accepts back.
//
// The assertion is the replay rather than the object count, because the count
// can be right while the statements are not -- and the failure this covers was
// SQLite refusing a name reserved for its own use, at exit 0 from the render.
func TestConvert_SQLiteDescriptionReplaysIntoAFreshDatabase(t *testing.T) {
	c := qt.New(t)

	source := openSQLite(c, "source.db")
	c.Assert(source.Writer().ExecuteSQL(context.Background(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		CONSTRAINT uq_users_email UNIQUE (email)
	)`), qt.IsNil)

	live, err := source.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	model := dbschematogo.ConvertDBSchemaToGoSchema(live, source.Info().Dialect)
	statements, err := renderer.GetOrderedCreateStatements(model, source.Info().Dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(len(statements) > 0, qt.IsTrue)

	replay := openSQLite(c, "replay.db")
	for _, statement := range statements {
		c.Assert(replay.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil,
			qt.Commentf("statement: %s", statement))
	}
}
