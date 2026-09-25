//go:build integration

package dbschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// These tests apply a SQLite schema file whose ALTER TABLE statements spell a
// table and a column in another ASCII case than the CREATE TABLE did. SQLite
// resolves every such spelling to the declared object, quoted or not, so the
// file is one SQLite runs; the reader resolves the names the same way
// (stokaro/ptah#3642). Compared exactly, the reader refuses the file.

const sqliteTableIdentityFile = `CREATE TABLE Docs (id integer PRIMARY KEY, a text);
ALTER TABLE docs ADD COLUMN x int;
ALTER TABLE DOCS ADD COLUMN y int;
CREATE TABLE t (id integer PRIMARY KEY, Note text);
ALTER TABLE t DROP COLUMN note;
`

// sqliteTableIdentityColumns is what the file creates, measured by sending it
// to SQLite as written.
var sqliteTableIdentityColumns = map[string][]string{
	"Docs": {"id", "a", "x", "y"},
	"t":    {"id"},
}

// sqliteTableIdentityFixture is one throwaway SQLite database.
type sqliteTableIdentityFixture struct {
	conn *dbschema.DatabaseConnection
}

func newSQLiteTableIdentityFixture(c *qt.C) sqliteTableIdentityFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "identity.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return sqliteTableIdentityFixture{conn: conn}
}

func (f sqliteTableIdentityFixture) load(c *qt.C) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sqliteTableIdentityFile), 0o600), qt.IsNil)
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "sqlite"})
	c.Assert(err, qt.IsNil)
	return desired
}

// execute runs statements one at a time, the way a client applies a file.
func (f sqliteTableIdentityFixture) execute(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// columns reads each table and its columns back from the catalog, in the
// order the server holds them.
func (f sqliteTableIdentityFixture) columns(c *qt.C) map[string][]string {
	c.Helper()
	rows, err := f.conn.QueryContext(c.Context(), `
		SELECT m.name, p.name
		FROM sqlite_master m JOIN pragma_table_info(m.name) p
		WHERE m.type = 'table'
		ORDER BY m.name, p.cid`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	columns := make(map[string][]string)
	for rows.Next() {
		var table, column string
		c.Assert(rows.Scan(&table, &column), qt.IsNil)
		columns[table] = append(columns[table], column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

func (f sqliteTableIdentityFixture) assertNothingPlanned(c *qt.C) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, nil)
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(f.load(c), live, "sqlite")
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesModified))
}

// TestSQLiteTableIdentity_LiveRenderedFileCreatesEachColumn renders the file,
// applies the render, and reads the tables back: each column the ALTER TABLE
// statements add reaches the table they named, the dropped one is gone, and
// nothing is left to plan.
func TestSQLiteTableIdentity_LiveRenderedFileCreatesEachColumn(t *testing.T) {
	c := qt.New(t)
	f := newSQLiteTableIdentityFixture(c)
	statements, err := renderer.GetOrderedCreateStatements(f.load(c), "sqlite")
	c.Assert(err, qt.IsNil)

	f.execute(c, statements)

	c.Assert(f.columns(c), qt.DeepEquals, sqliteTableIdentityColumns)
	f.assertNothingPlanned(c)
}

// TestSQLiteTableIdentity_LiveFileTheServerAppliedComparesEqual sends the file
// to SQLite as written and compares the same file with the result. The
// read-back is the control: it is SQLite's own answer to which object each
// spelling names.
func TestSQLiteTableIdentity_LiveFileTheServerAppliedComparesEqual(t *testing.T) {
	c := qt.New(t)
	f := newSQLiteTableIdentityFixture(c)

	f.execute(c, strings.Split(strings.TrimSpace(sqliteTableIdentityFile), "\n"))

	c.Assert(f.columns(c), qt.DeepEquals, sqliteTableIdentityColumns)
	f.assertNothingPlanned(c)
}
