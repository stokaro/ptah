//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// sqliteExpressionModel declares expression defaults SQLite takes only in
// parentheses: an operation and a call.
var sqliteExpressionModel = declaredModel{
	source: "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"a\" type=\"INTEGER\" default_expr=\"1 + 2\"\n" +
		"\tA int\n" +
		"\t//ptah:schema:field name=\"b\" type=\"TEXT\" default_expr=\"upper('x')\"\n" +
		"\tB string\n" +
		"}\n",
	file: "notes.go",
	flag: "--root-dir",
}

// TestApplyEnclosesASQLiteExpressionDefaultE2E applies a Go model whose
// expression defaults SQLite takes only in parentheses, twice. Written bare,
// the first apply fails with a syntax error; enclosed, the second plans
// nothing, and a row that takes the defaults reads them back computed.
func TestApplyEnclosesASQLiteExpressionDefaultE2E(t *testing.T) {
	c := qt.New(t)
	database := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "notes.db"))

	created := applyModel(c, database, sqliteExpressionModel)
	again := applyModel(c, database, sqliteExpressionModel)

	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT a || ',' || b FROM notes").Scan(&got), qt.IsNil)
	c.Assert(got, qt.Equals, "3,X")
}
