package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/sqlite"
)

// reservedPrefixCase is one object name and whether DropAllTables owes it a
// drop.
//
// SQLite reserves the prefix `sqlite_`, with the underscore. The cleanup query
// spelled that as `NOT LIKE 'sqlite_%'`, and LIKE reads a bare `_` as a
// single-character wildcard, so the pattern also matched `sqlite` followed by
// any character. Measured with sqlite3:
//
//	'sqlitedata'   LIKE 'sqlite_%'             -> 1
//	'sqlitedata'   LIKE 'sqlite\_%' ESCAPE '\' -> 0
//	'sqlite_stat1' LIKE 'sqlite_%'             -> 1
//
// so a user table called `sqlitedata` survived a drop-all silently, and the
// caller was told the database was clean (stokaro/ptah#1291).
type reservedPrefixCase struct {
	name      string
	create    string
	table     string
	wantAfter int
}

// TestWriterDropAllTables_DropsUserObjectsNamedSqliteSomething pins that only
// the genuinely reserved prefix is spared.
//
// A reserved `sqlite_` object cannot be created directly -- SQLite refuses
// `CREATE TABLE sqlite_x` with "object name reserved for internal use" -- so the
// control in the other direction is that the drop leaves the internal schema
// intact and reports success, which the surrounding suite already covers.
func TestWriterDropAllTables_DropsUserObjectsNamedSqliteSomething(t *testing.T) {

	tests := []reservedPrefixCase{
		{
			name:      "a table whose name begins sqlite plus one character",
			create:    `CREATE TABLE sqlitedata (id INTEGER PRIMARY KEY)`,
			table:     "sqlitedata",
			wantAfter: 0,
		},
		{
			// The digit form, because `sqlite3_` reads as reserved to a person
			// and is not.
			name:      "a table named sqlite3_cache",
			create:    `CREATE TABLE sqlite3_cache (id INTEGER PRIMARY KEY)`,
			table:     "sqlite3_cache",
			wantAfter: 0,
		},
		{
			// The control that keeps this from becoming "drop everything": an
			// ordinary name is dropped too, so a passing run cannot mean the
			// drop stopped working altogether.
			name:      "an ordinary table",
			create:    `CREATE TABLE users (id INTEGER PRIMARY KEY)`,
			table:     "users",
			wantAfter: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			db := openFileSQLiteDB(t)
			execSQL(t, db, test.create)

			var before int
			c.Assert(db.QueryRow(
				`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`, test.table,
			).Scan(&before), qt.IsNil)
			c.Assert(before, qt.Equals, 1, qt.Commentf("fixture did not create %q", test.table))

			c.Assert(sqlite.NewSQLiteWriter(db, "main").DropAllTables(c.Context()), qt.IsNil)

			var after int
			c.Assert(db.QueryRow(
				`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`, test.table,
			).Scan(&after), qt.IsNil)
			c.Assert(after, qt.Equals, test.wantAfter,
				qt.Commentf("%q survived DropAllTables", test.table))
		})
	}
}
