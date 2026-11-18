//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// SQLite takes a foreign key whose parent key is the primary key, a UNIQUE
// constraint, or a unique index with no WHERE clause and no expression key, in
// any column order, as long as the index compares each column under the
// collation the column declares. Any other parent key passes CREATE TABLE and
// fails the first write to the child with `foreign key mismatch`. Measured with
// sqlite3 3.51.0 and `PRAGMA foreign_keys=1`; the community binary v1.3.0 plans
// every document below, since SQLite creates each one. `ptah schema render`
// accepts the documents SQLite writes rows into and refuses the rest, and each
// row reads its verdict back from a SQLite database. SQLite needs no server,
// so the rows run on every platform.

// sqliteReferencedKeyChild references parent (code).
const sqliteReferencedKeyChild = "CREATE TABLE child (id integer PRIMARY KEY, code text REFERENCES parent (code));\n"

// sqliteReferencedKeyPairChild references parent (b, a), the reverse of the
// order its key lists the columns in.
const sqliteReferencedKeyPairChild = "CREATE TABLE child (id integer PRIMARY KEY, x integer, y integer, " +
	"FOREIGN KEY (x, y) REFERENCES parent (b, a));\n"

// sqliteReferencedKeyAccepted are the documents SQLite writes a child row
// into, with the rows that write it.
var sqliteReferencedKeyAccepted = []struct {
	name     string
	document string
	rows     string
}{
	{
		name:     "a standalone unique index",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE UNIQUE INDEX pc ON parent (code);\n" + sqliteReferencedKeyChild,
		rows:     "INSERT INTO parent VALUES (1, 'a'); INSERT INTO child VALUES (1, 'a');",
	},
	{
		name:     "a standalone unique index over a NOCASE column",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL COLLATE NOCASE);\nCREATE UNIQUE INDEX pc ON parent (code);\n" + sqliteReferencedKeyChild,
		rows:     "INSERT INTO parent VALUES (1, 'a'); INSERT INTO child VALUES (1, 'a');",
	},
	{
		name:     "a descending unique index",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE UNIQUE INDEX pc ON parent (code DESC);\n" + sqliteReferencedKeyChild,
		rows:     "INSERT INTO parent VALUES (1, 'a'); INSERT INTO child VALUES (1, 'a');",
	},
	{
		name:     "a composite primary key in another order",
		document: "CREATE TABLE parent (a integer NOT NULL, b integer NOT NULL, PRIMARY KEY (a, b));\n" + sqliteReferencedKeyPairChild,
		rows:     "INSERT INTO parent VALUES (1, 2); INSERT INTO child VALUES (1, 2, 1);",
	},
	{
		name:     "a composite UNIQUE constraint in another order",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, a integer NOT NULL, b integer NOT NULL, UNIQUE (a, b));\n" + sqliteReferencedKeyPairChild,
		rows:     "INSERT INTO parent VALUES (1, 1, 2); INSERT INTO child VALUES (1, 2, 1);",
	},
	{
		name:     "a composite unique index in another order",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, a integer NOT NULL, b integer NOT NULL);\nCREATE UNIQUE INDEX pab ON parent (a, b);\n" + sqliteReferencedKeyPairChild,
		rows:     "INSERT INTO parent VALUES (1, 1, 2); INSERT INTO child VALUES (1, 2, 1);",
	},
}

// sqliteReferencedKeyRefused are the documents SQLite creates and then
// refuses the first child row of, each with what the refusal says of the
// unique index it refuses.
var sqliteReferencedKeyRefused = []struct {
	name     string
	document string
	refused  string
}{
	{
		name:     "a plain index",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE INDEX pc ON parent (code);\n" + sqliteReferencedKeyChild,
	},
	{
		name:     "a partial unique index",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE UNIQUE INDEX pc ON parent (code) WHERE code <> '';\n" + sqliteReferencedKeyChild,
		refused:  `; unique index "pc" has a WHERE clause`,
	},
	{
		name:     "an expression unique index",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE UNIQUE INDEX pc ON parent (lower(code));\n" + sqliteReferencedKeyChild,
		refused:  `; unique index "pc" keys lower(code) rather than a column`,
	},
	{
		name:     "a NOCASE unique index over a BINARY column",
		document: "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\nCREATE UNIQUE INDEX pc ON parent (code COLLATE NOCASE);\n" + sqliteReferencedKeyChild,
		refused:  `; unique index "pc" keys code COLLATE NOCASE rather than a column`,
	},
}

// sqliteReferencedKeyRefusal is the line `schema render` writes for
// sqliteReferencedKeyRefused up to what it says of a refused unique index.
const sqliteReferencedKeyRefusal = `error: error rendering sqlite schema: sqlite requires referenced columns code on table "parent" to be declared unique: ` +
	`a primary key, UNIQUE constraint or unique index must key exactly those columns`

// renderSQLiteDocument runs `ptah schema render --dialect sqlite` over
// document.
func renderSQLiteDocument(c *qt.C, document string) clirun.Result {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(document), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"schema", "render", "--dialect", "sqlite", "--schema-file", "schema.sql")
}

// sqliteForeignKeyDatabase opens a new SQLite database on one connection with
// foreign keys enforced, and runs statements on it.
func sqliteForeignKeyDatabase(c *qt.C, statements string) *sql.DB {
	c.Helper()
	db, err := sql.Open("sqlite", filepath.Join(c.TempDir(), "keys.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(c.Context(), "PRAGMA foreign_keys = ON")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), statements)
	c.Assert(err, qt.IsNil)
	return db
}

// TestSchemaRenderAcceptsAParentKeySQLiteAccepts_HappyPathE2E renders each
// document SQLite writes rows into, creates the rendered tables on SQLite and
// writes the child row through the foreign key the render kept.
func TestSchemaRenderAcceptsAParentKeySQLiteAccepts_HappyPathE2E(t *testing.T) {
	for _, test := range sqliteReferencedKeyAccepted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := renderSQLiteDocument(c, test.document)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			db := sqliteForeignKeyDatabase(c, result.Stdout)
			_, err := db.ExecContext(c.Context(), test.rows)
			c.Assert(err, qt.IsNil)
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM pragma_foreign_key_list('child')"), qt.Not(qt.Equals), 0)
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM child"), qt.Equals, 1)
		})
	}
}

// TestSchemaRenderRefusesAParentKeySQLiteMismatches_FailurePathE2E renders
// each document SQLite refuses the first child row of. The refusal is the
// render's; the control creates the document itself on SQLite and shows the
// server's `foreign key mismatch` at that row.
func TestSchemaRenderRefusesAParentKeySQLiteMismatches_FailurePathE2E(t *testing.T) {
	for _, test := range sqliteReferencedKeyRefused {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := renderSQLiteDocument(c, test.document)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, sqliteReferencedKeyRefusal+test.refused+"\n")
			c.Assert(result.Stdout, qt.Equals, "")
			db := sqliteForeignKeyDatabase(c, test.document+"INSERT INTO parent VALUES (1, 'a');")
			_, err := db.ExecContext(c.Context(), "INSERT INTO child VALUES (1, 'a')")
			c.Assert(err, qt.ErrorMatches, `.*foreign key mismatch - "child" referencing "parent".*`)
		})
	}
}

// TestSchemaRenderRefusesACollateKeyItCannotCompare_FailurePathE2E is where the
// render is stricter than SQLite, deliberately. A COLLATE on an index key
// reaches the schema model as the key's text, which the rule cannot compare
// with the column's collation, so the render refuses the key even where the
// collation is the column's own and SQLite writes the child row.
func TestSchemaRenderRefusesACollateKeyItCannotCompare_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	document := "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL COLLATE NOCASE);\n" +
		"CREATE UNIQUE INDEX pc ON parent (code COLLATE NOCASE);\n" + sqliteReferencedKeyChild

	result := renderSQLiteDocument(c, document)

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, sqliteReferencedKeyRefusal+`; unique index "pc" keys code COLLATE NOCASE rather than a column`+"\n")
	db := sqliteForeignKeyDatabase(c, document+"INSERT INTO parent VALUES (1, 'a');")
	_, err := db.ExecContext(c.Context(), "INSERT INTO child VALUES (1, 'a')")
	c.Assert(err, qt.IsNil)
}

// readSQLiteCount reads the one integer query answers on db.
func readSQLiteCount(c *qt.C, db *sql.DB, query string) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}
