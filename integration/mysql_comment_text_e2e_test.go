//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlCommentEngines are the servers a schema file's comments are applied to.
var mysqlCommentEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// commentedSchema carries a column comment with a doubled quote, a column with
// two COMMENT clauses, and a table comment.
const commentedSchema = "CREATE TABLE c (id int PRIMARY KEY, x int COMMENT 'it''s', " +
	"y int COMMENT 'first' COMMENT 'second') COMMENT='tbl';\n"

// TestSchemaApplyWritesAMySQLCommentAsItsTextE2E applies a schema file whose
// columns and table carry comments, and reads the comments back from the
// catalog: each holds its text, as the pinned community binary v1.3.0 writes
// it, measured on MySQL 8.4.11 and MariaDB 11.8.9. Read with its keyword and
// quotes, the column comment reached the server with both still in it, and
// the table comment with its quotes (stokaro/ptah#3874). Planned again, the
// file is synced with the database it built.
func TestSchemaApplyWritesAMySQLCommentAsItsTextE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, url := scratch.database(c, "comment")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(commentedSchema), 0o600), qt.IsNil)

			applied := runPtahNative(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")

			var x, y, table string
			err := scratch.admin.QueryRowContext(c.Context(), `
				SELECT
					(SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND COLUMN_NAME = 'x'),
					(SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND COLUMN_NAME = 'y'),
					(SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c')`,
				name, name, name).Scan(&x, &y, &table)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(x, qt.Equals, "it's")
			c.Assert(y, qt.Equals, "second")
			c.Assert(table, qt.Equals, "tbl")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}
