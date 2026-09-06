package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/atlas"
	"ptah.run/cmd/atlas/internal/atlastest"
	"ptah.run/cmd/internal/exitcode"
	"ptah.run/internal/atlascompatpolicy"
)

// scriptFixture writes a script file and a database beside it, and returns both.
func scriptFixture(c *qt.C, document string) (dbURL, file string) {
	c.Helper()

	dir := c.TempDir()
	file = filepath.Join(dir, "script.hcl")
	c.Assert(os.WriteFile(file, []byte(document), 0o600), qt.IsNil)
	return "sqlite://" + filepath.Join(dir, "app.db"), file
}

// runScript executes the compat tree and returns stdout, stderr and the code.
func runScript(c *qt.C, args ...string) (stdout, stderr string, code int) {
	c.Helper()

	cmd := atlas.NewCompatCommand("ptah-compat")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := atlastest.ExecuteAtlasTestCommand(cmd)
	return out.String(), errOut.String(), exitcode.Code(err, 0)
}

// The verb is registered outside the strict CE profile and absent inside it.
//
// The pinned community binary does not register `script` at all, and the strict
// profile is where the conformance measurement runs -- registering it there
// would report a divergence Ptah introduced rather than one it found.
func TestAtlasScript_IsRegisteredOutsideStrictCEAndAbsentInside(t *testing.T) {
	c := qt.New(t)

	full := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full())
	script, _, err := full.Find([]string{"script"})
	c.Assert(err, qt.IsNil)
	c.Assert(script.CommandPath(), qt.Equals, "ptah-compat script")

	kinds := make([]string, 0, 3)
	for _, child := range script.Commands() {
		kinds = append(kinds, child.Name())
	}
	c.Assert(kinds, qt.Contains, "query")
	c.Assert(kinds, qt.Contains, "exec")
	c.Assert(kinds, qt.Contains, "loop")

	// Asserted over the registered command names rather than through Find:
	// cobra's Find returns the root with the argument as a leftover when no
	// subcommand matches, so it reports no error for a verb that does not
	// exist and would pass here whatever the policy did.
	strict := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.StrictCE())
	registered := make([]string, 0, len(strict.Commands()))
	for _, child := range strict.Commands() {
		registered = append(registered, child.Name())
	}
	c.Assert(registered, qt.Not(qt.Contains), "script")
	// The control: the full tree's own list DOES carry it, so the assertion
	// above is about the policy and not about how the names are read.
	fullNames := make([]string, 0, len(full.Commands()))
	for _, child := range full.Commands() {
		fullNames = append(fullNames, child.Name())
	}
	c.Assert(fullNames, qt.Contains, "script")
}

// A missing required flag is refused before a database is opened.
func TestAtlasScript_RefusesAMissingFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		says string
	}{
		{name: "no file", args: []string{"script", "query", "--url", "sqlite://x.db"}, says: `"file" not set`},
		{name: "no url", args: []string{"script", "query", "--file", "x.hcl"}, says: `"url" not set`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, code := runScript(c, test.args...)

			c.Assert(code, qt.Equals, 1)
		})
	}
}

// A file declaring several scripts of one kind is refused rather than guessed.
//
// Guessing would run a data operation the operator did not name, and the
// refusal lists what is available so they can.
func TestAtlasScript_RefusesToGuessBetweenTwoScripts(t *testing.T) {
	c := qt.New(t)
	dbURL, file := scriptFixture(c, `
script "query" "first" {
  query "rows" { sql = "SELECT 1" }
}
script "query" "second" {
  query "rows" { sql = "SELECT 2" }
}
`)

	_, stderr, code := runScript(c, "script", "query", "--url", dbURL, "--file", file)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "--run")
	c.Assert(stderr, qt.Contains, `query "first"`)
	c.Assert(stderr, qt.Contains, `query "second"`)
}

// --run names one of them, and it runs.
func TestAtlasScript_RunNamesOneOfSeveral(t *testing.T) {
	c := qt.New(t)
	dbURL, file := scriptFixture(c, `
script "query" "first" {
  query "rows" { sql = "SELECT 1 AS n" }
}
script "query" "second" {
  query "rows" { sql = "SELECT 2 AS n" }
}
`)

	stdout, _, code := runScript(c, "script", "query", "--url", dbURL, "--file", file, "--run", "second")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stdout, qt.Contains, "2")
	c.Assert(stdout, qt.Not(qt.Contains), "1\n")
}

// Asking for a kind the file does not declare says so, and lists what it has.
func TestAtlasScript_NamesWhatTheFileActuallyDeclares(t *testing.T) {
	c := qt.New(t)
	dbURL, file := scriptFixture(c, `
script "query" "only" {
  query "rows" { sql = "SELECT 1" }
}
`)

	_, stderr, code := runScript(c, "script", "exec", "--url", dbURL, "--file", file)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "declares no exec script")
	c.Assert(stderr, qt.Contains, `query "only"`)
}

// --quiet drops the report and keeps the product.
//
// Two writers rather than one with a flag, so quiet cannot suppress the rows
// the script was run for.
func TestAtlasScript_QuietKeepsTheProduct(t *testing.T) {
	c := qt.New(t)
	dbURL, file := scriptFixture(c, `
script "query" "rows" {
  query "r" { sql = "SELECT 7 AS n" }
}
`)

	stdout, stderr, code := runScript(c, "script", "query", "--url", dbURL, "--file", file, "--quiet")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stdout, qt.Contains, "7")
	c.Assert(stderr, qt.Not(qt.Contains), "Executing script")
}

// A guard that does not hold exits 0.
//
// A purge guarded by "only if there is something to purge" that finds nothing
// did its job. Exiting non-zero would page somebody for a script that worked.
func TestAtlasScript_AGuardThatDoesNotHoldIsNotAFailure(t *testing.T) {
	c := qt.New(t)
	dbURL, file := scriptFixture(c, `
script "exec" "guarded" {
  condition "never" {
    sql = "SELECT 0"
  }
  exec "e" {
    sql = "SELECT 1"
  }
}
`)

	_, stderr, code := runScript(c, "script", "exec", "--url", dbURL, "--file", file)

	c.Assert(code, qt.Equals, 0)
	c.Assert(stderr, qt.Contains, "Nothing to do.")
}
