//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// refusedOnLine6 holds a statement every dialect refuses, on line 6, past a
// comment in another script and a body that runs over several lines. Its column
// is 17 counted in characters.
const refusedOnLine6 = "-- Каталог: таблицы\nCREATE TABLE заказы (\n  note TEXT DEFAULT 'one\ntwo'\n);\nCREATE TABLE e1 PARTITION BY RANGE (a);\n"

// TestSQLSchemaRefusalNamesTheLineE2E drives the two programs over one file: both
// name its line and column, and neither writes a byte offset. Atlas CE v1.3.0 has
// no sentence to match here: it runs the statement on the dev database and
// repeats the server's answer, with no place in the file. `ptah-compat` reads
// the file first, so it says what `ptah` says.
func TestSQLSchemaRefusalNamesTheLineE2E(t *testing.T) {
	c := qt.New(t)
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(refusedOnLine6), 0o600), qt.IsNil)
	const sentence = `read SQL schema file .*schema\.sql:6:17: expected '\(' after table name: expected Operator, got Identifier`

	_, native := runPtahNativeWithError("schema", "render", "--schema-file", file, "--dialect", "postgres")
	c.Assert(native, qt.ErrorMatches, `(?s).*`+sentence)

	_, compat := runAtlasCompat("schema", "inspect", "--url", "file://"+filepath.ToSlash(file), "--dev-url", "sqlite://dev?mode=memory")
	c.Assert(compat, qt.ErrorMatches, sentence)

	_, diff := runAtlasCompat("schema", "diff", "--from", "file://"+filepath.ToSlash(file), "--to", "file://"+filepath.ToSlash(file), "--dev-url", "sqlite://dev?mode=memory")
	c.Assert(diff, qt.ErrorMatches, `load --from schema: `+sentence)
}

// TestSQLLintNamesTheSameLineE2E holds `ptah sql lint` to the same place in the
// same file, with the position in front and none in the message.
func TestSQLLintNamesTheSameLineE2E(t *testing.T) {
	c := qt.New(t)
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(refusedOnLine6), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("sql", "lint", file, "--dialect", "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Matches, `(?s).*schema\.sql:6:17: error SQL001: expected '\(' after table name: expected Operator, got Identifier\n.*`)
}
