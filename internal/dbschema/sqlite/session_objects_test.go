package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/sqlite"
)

func TestWriterDropSessionObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	for _, statement := range []string{
		`CREATE TABLE main.users (id INTEGER PRIMARY KEY)`,
		`CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY)`,
		`CREATE INDEX temp.scratch_id ON scratch (id)`,
		`CREATE TEMP VIEW scratch_view AS SELECT id FROM scratch`,
		`CREATE TEMP TRIGGER scratch_trigger AFTER INSERT ON main.users BEGIN SELECT 1; END`,
	} {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	writer := sqlite.NewSQLiteWriterForConnection(conn, "main")
	err = writer.DropSessionObjects(t.Context())

	c.Assert(err, qt.IsNil)
	var temp, user int
	c.Assert(conn.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM temp.sqlite_schema WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\'`).Scan(&temp), qt.IsNil)
	c.Assert(temp, qt.Equals, 0)
	user = sqliteConnSchemaObjectCount(c, conn, "main", "users")
	c.Assert(user, qt.Equals, 1)
}

func TestWriterDropSessionObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)

	writer := sqlite.NewSQLiteWriterForRunner(db, "main")
	err := writer.DropSessionObjects(t.Context())

	c.Assert(err, qt.ErrorMatches, `sqlite: dropping session objects requires a pinned connection`)
}

func TestWriterDropSessionObjects_ParentAndChildWithForeignKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	for _, statement := range []string{
		`PRAGMA foreign_keys = ON`,
		`CREATE TEMP TABLE a (id INTEGER PRIMARY KEY)`,
		`CREATE TEMP TABLE b (id INTEGER, a_id INTEGER REFERENCES a (id))`,
		`INSERT INTO a VALUES (1)`,
		`INSERT INTO b VALUES (1, 1)`,
	} {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	err = sqlite.NewSQLiteWriterForConnection(conn, "main").DropSessionObjects(t.Context())

	c.Assert(err, qt.IsNil)
	var temp, foreignKeys int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM temp.sqlite_schema`).Scan(&temp), qt.IsNil)
	c.Assert(temp, qt.Equals, 0)
	c.Assert(conn.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&foreignKeys), qt.IsNil)
	c.Assert(foreignKeys, qt.Equals, 1)
}

func TestWriterDropSessionObjects_IndexAndTriggerOnTempTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	for _, statement := range []string{
		`CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, note TEXT)`,
		`CREATE INDEX scratch_note ON scratch (note)`,
		`CREATE TRIGGER scratch_log AFTER INSERT ON scratch BEGIN SELECT 1; END`,
	} {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	err = sqlite.NewSQLiteWriterForConnection(conn, "main").DropSessionObjects(t.Context())

	c.Assert(err, qt.IsNil)
	var temp int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM temp.sqlite_schema`).Scan(&temp), qt.IsNil)
	c.Assert(temp, qt.Equals, 0)
}
