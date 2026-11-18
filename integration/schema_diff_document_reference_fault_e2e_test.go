//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A document on either side of `schema diff` is checked with the target
// validation a --to declaration gets, so a foreign key to a column no unique
// key covers is refused on the --from side as on the --to side. The community
// binary v1.3.0 creates each document on its dev database: on PostgreSQL both
// sides exit 1 with the server's 42830. SQLite accepts the CREATE TABLE and
// refuses the first write to child with `foreign key mismatch`, so there that
// binary plans either side and exits 0. Ptah refuses both sides on SQLite
// too, deliberately: the document describes a table no row can be written to.
// SQLite needs no server, so the rows run on every platform; the PostgreSQL
// line, which names the table by the dev database's schema, is pinned in
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
const sqliteDiffReferenceFaultLine = `sqlite requires referenced columns code on table "parent" to be declared unique`

// TestSchemaDiffRefusesAReferenceFaultInEitherDocument_FailurePathE2E puts the
// faulty document on each side, on every surface of [documentDiffRuns].
// Without the check of the --from side, the diff of a faulty --from plans the
// rebuild of child without its foreign key and exits 0.
func TestSchemaDiffRefusesAReferenceFaultInEitherDocument_FailurePathE2E(t *testing.T) {
	sides := []struct {
		name     string
		from, to string
	}{
		{name: "faulty --to", from: sqliteDiffReferenceSound, to: sqliteDiffReferenceFaulty},
		{name: "faulty --from", from: sqliteDiffReferenceFaulty, to: sqliteDiffReferenceSound},
	}
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			for _, side := range sides {
				t.Run(side.name, func(t *testing.T) {
					c := qt.New(t)
					result := runSQLiteDocumentDiff(c, run.target, side.from, side.to, run.env)
					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
					c.Assert(result.ExitCode, qt.Equals, run.wantExit, comment)
					c.Assert(result.Stderr, qt.Equals, run.prefix+sqliteDiffReferenceFaultLine+"\n", comment)
					c.Assert(result.Stdout, qt.Equals, "", comment)
				})
			}
		})
	}
}

// TestSchemaDiffComparesAKeyedReference_HappyPathE2E is the control: the same
// foreign key with parent.code declared unique, as the --from document, plans
// the rebuild of child without it on every surface, so the refusals above are
// the missing key's doing.
func TestSchemaDiffComparesAKeyedReference_HappyPathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			result := runSQLiteDocumentDiff(c, run.target, sqliteDiffReferenceKeyed, sqliteDiffReferenceSound, run.env)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 0, comment)
			c.Assert(result.Stdout, qt.Contains, `ALTER TABLE "new_child" RENAME TO "child";`, comment)
			c.Assert(result.Stderr, qt.Equals, "", comment)
		})
	}
}
