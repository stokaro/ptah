//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
)

// dbmlDesiredState is a schema written in DBML rather than in any format Ptah
// has read before, so what converges below is the DBML path and not a fixture
// that happens to be SQL underneath.
const dbmlDesiredState = `Table users {
  id INTEGER [pk]
  email TEXT [not null, unique]
}

Table posts {
  id INTEGER [pk]
  author_id INTEGER [not null]
  title TEXT [not null]
}

Ref posts_author_fk: posts.author_id > users.id
`

// TestDBMLApplyThenCompareConvergesE2E is the convergence claim of
// stokaro/ptah#2065, measured against a live database rather than asserted.
//
// A desired state is only usable if applying it and then comparing the result
// against the same document reports nothing left to do. Anything the reader
// cannot express, the renderer spells differently, or the comparator folds
// another way shows up here as a second plan that never empties -- and a format
// that plans forever is worse than one that refuses, because it looks like it
// works.
func TestDBMLApplyThenCompareConvergesE2E(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.dbml")
	dbURL := "sqlite://" + filepath.Join(dir, "live.db")
	c.Assert(os.WriteFile(schemaPath, []byte(dbmlDesiredState), 0o600), qt.IsNil)

	applied := runPtahNative(c, "schema", "apply",
		"--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(applied, qt.Contains, "users")

	// The second pass is the assertion. The first one creating tables proves
	// only that something ran.
	compared := runPtahNative(c, "schema", "compare", "--db-url", dbURL, "--schema-file", schemaPath)

	c.Assert(compared, qt.Not(qt.Contains), "CREATE TABLE")
	c.Assert(compared, qt.Not(qt.Contains), "ALTER TABLE")
	c.Assert(compared, qt.Not(qt.Contains), "DROP TABLE")
}

// TestDBMLApplyIsIdempotentE2E is the control the convergence test needs.
//
// A compare that reported nothing because it read nothing would satisfy the
// test above. This applies a CHANGED document to the same database and requires
// the comparison to have something to say, so silence is measured as an answer
// rather than as absence.
func TestDBMLApplyIsIdempotentE2E(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.dbml")
	dbURL := "sqlite://" + filepath.Join(dir, "live.db")
	c.Assert(os.WriteFile(schemaPath, []byte(dbmlDesiredState), 0o600), qt.IsNil)
	runPtahNative(c, "schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

	widened := dbmlDesiredState + "\nTable comments {\n  id integer [pk]\n}\n"
	c.Assert(os.WriteFile(schemaPath, []byte(widened), 0o600), qt.IsNil)

	compared := runPtahNative(c, "schema", "compare", "--db-url", dbURL, "--schema-file", schemaPath)

	c.Assert(compared, qt.Contains, "comments",
		qt.Commentf("a widened document produced no plan, so the empty plan above proved nothing"))
}

// runPtahNative runs one native command in-process and returns its combined output.
func runPtahNative(c *qt.C, args ...string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.String(), errOut.String()))
	return out.String() + errOut.String()
}

// equivalentSQLState is the same schema as [dbmlDesiredState], written in a
// format read by a different code path.
// The type spellings and the constraint name match dbmlDesiredState on purpose.
// A fixture that differed in either would make this test fail for a spelling
// rather than for a meaning, and the point is the meaning: whether a column
// declared NOT NULL, UNIQUE or sized in one format arrives the same way from the
// other.
const equivalentSQLState = `CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL UNIQUE
);
CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  author_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  CONSTRAINT posts_author_fk FOREIGN KEY (author_id) REFERENCES users(id)
);
`

// TestDBMLAgreesWithTheSameSchemaWrittenInSQLE2E is what convergence alone
// cannot prove.
//
// Applying a DBML document and comparing it against itself uses one reader for
// both halves, so a property the reader drops is missing from the database AND
// from the desired state, and the two agree. Measured: mutants that drop NOT
// NULL, drop UNIQUE, or discard a type's arguments all survive
// TestDBMLApplyThenCompareConvergesE2E, because self-consistency is not
// correctness.
//
// This applies the DBML document and then compares the result against the same
// schema written in SQL, which reaches the model through a different code path.
// A property either reader loses now shows up as a plan.
func TestDBMLAgreesWithTheSameSchemaWrittenInSQLE2E(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbmlPath := filepath.Join(dir, "schema.dbml")
	sqlPath := filepath.Join(dir, "schema.sql")
	dbURL := "sqlite://" + filepath.Join(dir, "live.db")
	c.Assert(os.WriteFile(dbmlPath, []byte(dbmlDesiredState), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(sqlPath, []byte(equivalentSQLState), 0o600), qt.IsNil)

	runPtahNative(c, "schema", "apply",
		"--db-url", dbURL, "--schema-file", dbmlPath, "--auto-approve")

	compared := runPtahNative(c, "schema", "compare", "--db-url", dbURL, "--schema-file", sqlPath)

	c.Assert(compared, qt.Not(qt.Contains), "CREATE TABLE")
	c.Assert(compared, qt.Not(qt.Contains), "ALTER TABLE")
	c.Assert(compared, qt.Not(qt.Contains), "DROP TABLE")
}

// TestTheCrossFormatComparisonCanFailE2E is the control on the test above.
//
// Both of its assertions are negatives, so a comparison that always reported
// nothing would satisfy it. This compares the DBML-built database against an SQL
// document that says something different, and requires a plan.
func TestTheCrossFormatComparisonCanFailE2E(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbmlPath := filepath.Join(dir, "schema.dbml")
	sqlPath := filepath.Join(dir, "schema.sql")
	dbURL := "sqlite://" + filepath.Join(dir, "live.db")
	c.Assert(os.WriteFile(dbmlPath, []byte(dbmlDesiredState), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(sqlPath, []byte(equivalentSQLState+
		"CREATE TABLE comments (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	runPtahNative(c, "schema", "apply",
		"--db-url", dbURL, "--schema-file", dbmlPath, "--auto-approve")

	compared := runPtahNative(c, "schema", "compare", "--db-url", dbURL, "--schema-file", sqlPath)

	c.Assert(compared, qt.Contains, "comments",
		qt.Commentf("the cross-format comparison reported nothing for a real difference"))
}
