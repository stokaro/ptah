//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A --from document is held to the referenced-key rule only on an engine
// whose server applies the rule when it creates the foreign key; SQLite does
// not. It takes a CREATE TABLE whose foreign key references a column no
// unique key covers and refuses the first write to child with `foreign key
// mismatch`, so a SQLite database can hold that state, and the community
// binary v1.3.0 plans a diff from it and exits 0. A diff from such a document
// to one that adds the key is the repair, and refusing it would be stricter
// than that binary for no gain. The --to document is refused on SQLite, which
// is stricter than that binary, deliberately: it would plan a table no row can
// be written to. SQLite needs no server, so the rows run on every platform;
// the PostgreSQL rows, where the --from document is refused too, are in
// schema_fault_table_name_e2e_test.go.

// sqliteDiffReferenceSound declares parent and child with no foreign key.
const sqliteDiffReferenceSound = "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\n" +
	"CREATE TABLE child (id integer PRIMARY KEY, code text);\n"

// sqliteDiffReferenceFaulty has child reference parent.code, which no unique
// key covers.
const sqliteDiffReferenceFaulty = "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL);\n" +
	"CREATE TABLE child (id integer PRIMARY KEY, code text REFERENCES parent (code));\n"

// sqliteDiffReferenceKeyed is sqliteDiffReferenceFaulty with parent.code
// declared unique.
const sqliteDiffReferenceKeyed = "CREATE TABLE parent (id integer PRIMARY KEY, code text NOT NULL UNIQUE);\n" +
	"CREATE TABLE child (id integer PRIMARY KEY, code text REFERENCES parent (code));\n"

// sqliteDiffReferenceFaultLine is the diagnostic for
// sqliteDiffReferenceFaulty, without the surface's prefix.
const sqliteDiffReferenceFaultLine = `sqlite requires referenced columns code on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`

// TestSchemaDiffRefusesAReferenceFaultInTheToDocument_FailurePathE2E plans
// toward the faulty document on every surface of [documentDiffRuns].
func TestSchemaDiffRefusesAReferenceFaultInTheToDocument_FailurePathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			result := runSQLiteDocumentDiff(c, run.target, sqliteDiffReferenceSound, sqliteDiffReferenceFaulty, run.env)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, run.wantExit, comment)
			c.Assert(result.Stderr, qt.Equals, run.prefix+sqliteDiffReferenceFaultLine+"\n", comment)
			c.Assert(result.Stdout, qt.Equals, "", comment)
		})
	}
}

// TestSchemaDiffPlansFromAReferenceFaultInTheFromDocument_HappyPathE2E plans
// from the faulty document on every surface of [documentDiffRuns]: to the
// document that declares parent.code unique, the repair, and to the one that
// drops the foreign key. The community binary v1.3.0 exits 0 on both. Holding
// the --from document to the rule refuses both rows.
func TestSchemaDiffPlansFromAReferenceFaultInTheFromDocument_HappyPathE2E(t *testing.T) {
	targets := []struct {
		name string
		to   string
		want string
	}{
		{name: "the repair", to: sqliteDiffReferenceKeyed, want: "NOT NULL UNIQUE"},
		{name: "the foreign key dropped", to: sqliteDiffReferenceSound, want: `ALTER TABLE "new_child" RENAME TO "child";`},
	}
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			for _, target := range targets {
				t.Run(target.name, func(t *testing.T) {
					c := qt.New(t)
					result := runSQLiteDocumentDiff(c, run.target, sqliteDiffReferenceFaulty, target.to, run.env)
					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
					c.Assert(result.ExitCode, qt.Equals, 0, comment)
					c.Assert(result.Stdout, qt.Contains, target.want, comment)
					c.Assert(result.Stderr, qt.Equals, "", comment)
				})
			}
		})
	}
}
