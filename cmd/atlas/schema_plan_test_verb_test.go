package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// planTestFixture writes a directory holding everything a plan case needs: the
// starting-state snapshot, a plan file computed against it, and the test.
//
// The plan is produced by `schema plan` rather than hand-written, which is the
// point of the fixture: a plan test checks a plan the planner actually made,
// and a hand-written one could agree with the test while disagreeing with
// what Ptah produces.
func planTestFixture(c *qt.C, caseBody string) string {
	c.Helper()
	dir := c.TB.TempDir()
	write := func(name, content string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	c.Assert(os.MkdirAll(filepath.Join(dir, "snapshots"), 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "plans"), 0o750), qt.IsNil)

	write("snapshots/v1.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	write("desired.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);\n")

	seed := sqliteURLFromPath(filepath.Join(dir, "seed.db"))
	// A dev URL under the case's own directory. A relative one (the bare name
	// "dev") resolves against the process working directory, which under `go
	// test` is the package directory -- so it leaves a stray database in the
	// source tree.
	devURL := sqliteURLFromPath(filepath.Join(dir, "dev.db"))
	out, err := runAtlasArgs(
		"schema", "apply",
		"--url", seed,
		"--to", "file://"+filepath.Join(dir, "snapshots/v1.sql"),
		"--dev-url", devURL,
		"--auto-approve",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// --dev-url is given explicitly: without one, planning opens a dev database
	// at a RELATIVE path, which lands in the package directory and leaves a
	// stray file in the source tree.
	out, err = runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", seed,
		"--to", "file://"+filepath.Join(dir, "desired.sql"),
		"--dev-url", devURL,
		"--output", filepath.Join(dir, "plans/add_email.plan.json"),
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	write("plan.test.hcl", caseBody)
	return dir
}

// planCaseAddEmail establishes the snapshot, seeds a row, applies the plan, and
// reads the row back THROUGH THE COLUMN THE PLAN ADDS.
//
// Reading it through a column the snapshot already had would pass whether or
// not the plan ran, which is exactly what a mutation making the apply step a
// no-op proved: the case has to depend on the change it is testing. The
// UPDATE below fails with "no such column: email" when the plan is skipped.
const planCaseAddEmail = `test "plan" "add_email" {
  schema {
    url = "file://snapshots/v1.sql"
  }

  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'Ada')"
  }

  apply {
    url = "file://plans/add_email.plan.json"
  }

  exec {
    sql = "UPDATE users SET email = 'ada@example.com' WHERE id = 1"
  }

  exec {
    sql    = "SELECT email FROM users WHERE id = 1"
    output = "ada@example.com"
  }
}
`

// TestSchemaPlanTest_RunsAPlanCaseEndToEnd is the verb doing its job, on a plan
// the planner produced.
//
// The row survives the apply, which is the substance: a plan test exists to
// check that a reviewed plan does what it claims against data that was already
// there (stokaro/ptah#1211).
func TestSchemaPlanTest_RunsAPlanCaseEndToEnd(t *testing.T) {
	c := qt.New(t)
	dir := planTestFixture(c, planCaseAddEmail)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "=== PLAN TEST ===")
	c.Assert(out, qt.Contains, `PASS  case "add_email"`)
	// The steps name what they did, so a failing report says which block.
	c.Assert(out, qt.Contains, `step "schema file://snapshots/v1.sql"`)
	c.Assert(out, qt.Contains, `step "apply file://plans/add_email.plan.json"`)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaPlanTest_AFailedAssertionFailsTheRun is the control that the verb
// can report a failure at all: a runner that passed everything would pass the
// case above too.
func TestSchemaPlanTest_AFailedAssertionFailsTheRun(t *testing.T) {
	c := qt.New(t)
	wrong := `test "plan" "add_email" {
  schema {
    url = "file://snapshots/v1.sql"
  }

  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'Ada')"
  }

  apply {
    url = "file://plans/add_email.plan.json"
  }

  exec {
    sql = "UPDATE users SET email = 'ada@example.com' WHERE id = 1"
  }

  exec {
    sql    = "SELECT email FROM users WHERE id = 1"
    output = "grace@example.com"
  }
}
`
	dir := planTestFixture(c, wrong)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, `expected scalar "grace@example.com", got "ada@example.com"`)
	c.Assert(out, qt.Contains, "1 cases, 0 passed, 1 failed")
}

// TestSchemaPlanTest_OtherKindsAreLeftToTheirOwnVerbs proves the run is scoped.
//
// A `test "schema"` case in the same file belongs to `schema test`. Running it
// here would execute work its author did not ask this command for, which is the
// same rule the loader has always applied between schema and migrate cases.
func TestSchemaPlanTest_OtherKindsAreLeftToTheirOwnVerbs(t *testing.T) {
	c := qt.New(t)
	mixed := planCaseAddEmail + `
test "schema" "not_this_one" {
  exec {
    sql    = "SELECT 1"
    output = "1"
  }
}
`
	dir := planTestFixture(c, mixed)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
	c.Assert(out, qt.Not(qt.Contains), "not_this_one")
}

// TestSchemaPlanTest_RunSelectsACase covers --run selecting the case it names.
func TestSchemaPlanTest_RunSelectsACase(t *testing.T) {
	c := qt.New(t)
	dir := planTestFixture(c, planCaseAddEmail)

	out, err := runSchemaPlanSubverb(
		atlas.NewCompatCommand("atlas"), "test", dir, "--run", "add_email")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaPlanTest_RunMatchingNothingIsRefused is the paired case: a filter
// that selects no case is a mistake worth naming, not a run that passes
// because it did nothing.
func TestSchemaPlanTest_RunMatchingNothingIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := planTestFixture(c, planCaseAddEmail)

	out, err := runSchemaPlanSubverb(
		atlas.NewCompatCommand("atlas"), "test", dir, "--run", "nope")

	c.Assert(err, qt.ErrorMatches, `no test cases match --run "nope"`, qt.Commentf("%s", out))
}

// TestSchemaPlanTest_RefusesARegistryURL answers a plan URL Ptah has no store
// for by name, rather than with a path-parsing failure.
func TestSchemaPlanTest_RefusesARegistryURL(t *testing.T) {
	c := qt.New(t)
	registry := `test "plan" "remote" {
  schema {
    url = "file://snapshots/v1.sql"
  }

  apply {
    url = "atlas://app/plans/add_email"
  }
}
`
	dir := planTestFixture(c, registry)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "only file:// URLs are supported")
	c.Assert(out, qt.Contains, "Ptah has no plan registry")
}

// TestSchemaPlanTest_NoCasesIsRefused keeps an empty directory from exiting 0.
// A test command that finds nothing and says so is the difference between a
// green pipeline and one that never ran.
func TestSchemaPlanTest_NoCasesIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.ErrorMatches, `no test "plan" cases found in .*`, qt.Commentf("%s", out))
}

// TestSchemaPlanTest_RefusesAPlanComputedForAnotherState is the "verify plan
// From against initial state" step of the documented workflow.
//
// A plan describes a transition FROM a state. A case whose snapshot has
// drifted away from the one the plan was computed for is testing that plan
// against a state it was never meant for, and would report whatever the
// statements happened to do there — which is worse than reporting nothing,
// because it looks like a pass.
func TestSchemaPlanTest_RefusesAPlanComputedForAnotherState(t *testing.T) {
	c := qt.New(t)
	dir := planTestFixture(c, planCaseAddEmail)
	// A second snapshot the plan was NOT computed from: same table, one more
	// column, so the plan's statements would still run and the assertions
	// would still pass.
	c.Assert(os.WriteFile(
		filepath.Join(dir, "snapshots/v2.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);\n"),
		0o600), qt.IsNil)
	drifted := `test "plan" "add_email" {
  schema {
    url = "file://snapshots/v2.sql"
  }

  apply {
    url = "file://plans/add_email.plan.json"
  }
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "plan.test.hcl"), []byte(drifted), 0o600), qt.IsNil)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "test", dir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out, qt.Contains, "stale")
	c.Assert(out, qt.Contains, "1 cases, 0 passed, 1 failed")
}
