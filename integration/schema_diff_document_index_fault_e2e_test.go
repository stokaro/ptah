//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A document on either side of `schema diff` is checked for an index its
// server would refuse. The community binary creates each document on its dev
// database, so a document that declares an index on a column its table lacks
// fails there: measured with v1.3.0 on SQLite, a faulty --from exits 1 with
// `no such column: missing`. A diff that planned around the index would accept
// what that binary refuses. SQLite needs no server, so the rows run on every
// platform.

// sqliteDiffSound declares t with no fault.
const sqliteDiffSound = "CREATE TABLE t (id integer PRIMARY KEY);\n"

// sqliteDiffFaulty declares t with an index on a column t does not declare.
const sqliteDiffFaulty = "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE INDEX i ON t (missing);\n"

// sqliteDiffFaultLine is the diagnostic for sqliteDiffFaulty, without the
// surface's prefix.
const sqliteDiffFaultLine = `sqlite: index "i": names column "missing", which table "t" does not declare`

// runSQLiteDocumentDiff runs `schema diff --from file://from.sql --to
// file://to.sql` on an in-memory SQLite dev database, with env added to the
// environment.
func runSQLiteDocumentDiff(c *qt.C, target clirun.Target, from, to string, env []string) clirun.Result {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "from.sql"), []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "to.sql"), []byte(to), 0o600), qt.IsNil)
	return clirun.Run(c, target, clirun.Options{Dir: root, Env: slices.Concat(environmentWithoutPtahVariables(), env)},
		"schema", "diff", "--from", "file://from.sql", "--to", "file://to.sql", "--dev-url", "sqlite://dev?mode=memory")
}

// documentDiffRuns are the surfaces a document diff is run on: native ptah,
// ptah-compat, and ptah-compat under the CE-only policy, which refuses the same
// way, since the community binary refuses the document too.
var documentDiffRuns = []struct {
	name     string
	target   clirun.Target
	env      []string
	wantExit int
	prefix   string
}{
	{name: "ptah", target: clirun.Ptah, wantExit: 2, prefix: "error: "},
	{name: "ptah-compat", target: clirun.Compat, wantExit: 1, prefix: "Error: "},
	{name: "ptah-compat strict", target: clirun.Compat, env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}, wantExit: 1, prefix: "Error: "},
}

// TestSchemaDiffRefusesAnIndexFaultInEitherDocument_FailurePathE2E puts the
// faulty document on each side. Without the check of the --from side, the
// diff of a faulty --from plans DROP INDEX "i" and exits 0.
func TestSchemaDiffRefusesAnIndexFaultInEitherDocument_FailurePathE2E(t *testing.T) {
	sides := []struct {
		name     string
		from, to string
	}{
		{name: "faulty --to", from: sqliteDiffSound, to: sqliteDiffFaulty},
		{name: "faulty --from", from: sqliteDiffFaulty, to: sqliteDiffSound},
	}
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			for _, side := range sides {
				t.Run(side.name, func(t *testing.T) {
					c := qt.New(t)
					result := runSQLiteDocumentDiff(c, run.target, side.from, side.to, run.env)
					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
					c.Assert(result.ExitCode, qt.Equals, run.wantExit, comment)
					c.Assert(result.Stderr, qt.Equals, run.prefix+sqliteDiffFaultLine+"\n", comment)
					c.Assert(result.Stdout, qt.Equals, "", comment)
				})
			}
		})
	}
}

// TestSchemaDiffComparesSoundDocuments_HappyPathE2E is the control: the same
// two sides without the fault plan the index's removal on every surface, so
// the refusals above are the fault's doing.
func TestSchemaDiffComparesSoundDocuments_HappyPathE2E(t *testing.T) {
	const indexed = "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE INDEX i ON t (id);\n"
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			result := runSQLiteDocumentDiff(c, run.target, indexed, sqliteDiffSound, run.env)
			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
			c.Assert(result.ExitCode, qt.Equals, 0, comment)
			c.Assert(result.Stdout, qt.Contains, `DROP INDEX "i"`, comment)
		})
	}
}
