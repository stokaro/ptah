//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// SQLite changes neither an index nor a table's STRICT and WITHOUT ROWID in
// place. A changed index definition is dropped and created again, and a
// changed option rebuilds the table, which keeps its rows. A column comment,
// which SQLite does not store, plans nothing. Each step applies a document
// and diffs the database against it again, which has to be in sync.

// sqliteComparisonDatabase is a scratch SQLite file holding one row in t.
func sqliteComparisonDatabase(c *qt.C, document string) string {
	c.Helper()
	url := "sqlite://" + filepath.Join(c.TempDir(), "t.db")
	applySQLiteDocument(c, url, document)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t (id, a, b) VALUES (1, 'x', 'y')")
	c.Assert(err, qt.IsNil)
	return url
}

// applySQLiteDocument writes document, applies it to url, and diffs url
// against it again. It returns what the apply printed.
func applySQLiteDocument(c *qt.C, url, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

	applied, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	out, err := runPtahNativeWithError("schema", "diff", "--from", url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
	return applied
}

// sqliteStored reads the CREATE statement SQLite stored for name, and the
// rows of t.
func sqliteStored(c *qt.C, url, name string) (definition string, rows int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_master WHERE name = ?", name).Scan(&definition), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM t").Scan(&rows), qt.IsNil)
	return definition, rows
}

const sqliteComparisonTable = "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a TEXT, b TEXT)"

// TestSchemaApplyReplacesAChangedSQLiteIndexE2E changes the index t_k one
// property at a time, and reads the stored definition after each apply.
func TestSchemaApplyReplacesAChangedSQLiteIndexE2E(t *testing.T) {
	c := qt.New(t)
	url := sqliteComparisonDatabase(c, sqliteComparisonTable+";\nCREATE INDEX t_k ON t (a);\n")

	steps := []struct {
		index string
		want  string
	}{
		{index: "CREATE UNIQUE INDEX t_k ON t (a);", want: `CREATE UNIQUE INDEX "t_k" ON "t" ("a")`},
		{index: "CREATE UNIQUE INDEX t_k ON t (a DESC);", want: `CREATE UNIQUE INDEX "t_k" ON "t" ("a" DESC)`},
		{index: "CREATE UNIQUE INDEX t_k ON t (b);", want: `CREATE UNIQUE INDEX "t_k" ON "t" ("b")`},
		{index: "CREATE INDEX t_k ON t (lower(a));", want: `CREATE INDEX "t_k" ON "t" (lower(a))`},
		{index: "CREATE INDEX t_k ON t (upper(a));", want: `CREATE INDEX "t_k" ON "t" (upper(a))`},
	}
	for _, step := range steps {
		applySQLiteDocument(c, url, sqliteComparisonTable+";\n"+step.index+"\n")
		definition, rows := sqliteStored(c, url, "t_k")
		c.Assert(definition, qt.Equals, step.want)
		c.Assert(rows, qt.Equals, 1)
	}
}

// TestSchemaApplyRebuildsASQLiteTableForItsOptionsE2E turns the options on
// and off from SQL and from HCL, and the table keeps its row each time.
func TestSchemaApplyRebuildsASQLiteTableForItsOptionsE2E(t *testing.T) {
	c := qt.New(t)
	url := sqliteComparisonDatabase(c, sqliteComparisonTable+";\n")

	applySQLiteDocument(c, url, sqliteComparisonTable+" STRICT, WITHOUT ROWID;\n")
	definition, rows := sqliteStored(c, url, "t")
	c.Assert(definition, qt.Matches, `(?s).*\) STRICT, WITHOUT ROWID`)
	c.Assert(rows, qt.Equals, 1)

	applySQLiteDocument(c, url, sqliteComparisonTable+";\n")
	definition, rows = sqliteStored(c, url, "t")
	c.Assert(definition, qt.Not(qt.Contains), "STRICT")
	c.Assert(rows, qt.Equals, 1)

	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "main" {}
table "t" {
  schema = schema.main
  strict = true
  column "id" {
    type = integer
    null = false
  }
  column "a" {
    type = text
    null = true
  }
  column "b" {
    type = text
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
	definition, rows = sqliteStored(c, url, "t")
	c.Assert(definition, qt.Matches, `(?s).*\) STRICT`)
	c.Assert(rows, qt.Equals, 1)
}

// TestSchemaApplyLeavesASQLiteTableAloneForAColumnCommentE2E declares a
// comment on a column, which SQLite does not store, and changes it: neither
// apply rebuilds the table, and each diff is in sync.
func TestSchemaApplyLeavesASQLiteTableAloneForAColumnCommentE2E(t *testing.T) {
	c := qt.New(t)
	url := sqliteComparisonDatabase(c, sqliteComparisonTable+";\n")
	before, _ := sqliteStored(c, url, "t")

	for _, comment := range []string{"first", "second"} {
		applied := applySQLiteDocument(c, url, sqliteComparisonTable+";\nCOMMENT ON COLUMN t.a IS '"+comment+"';\n")
		c.Assert(applied, qt.Not(qt.Contains), "__ptah_rebuild_t")
	}
	after, rows := sqliteStored(c, url, "t")
	c.Assert(after, qt.Equals, before)
	c.Assert(rows, qt.Equals, 1)
}
