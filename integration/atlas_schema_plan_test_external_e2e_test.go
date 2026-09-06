//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/atlas"
	"ptah.run/migration/dbtest"
)

// TestSchemaPlanTest_ExternalStepNeedsTheAuthorization drives the verb rather
// than the engine, and it is the coverage the fix it guards did not have.
//
// The authorization is resolved by each command that runs test cases, and
// `schema plan test` was the third such command: two resolved it and this one
// did not, so an `external` step was refused there even with the variable set.
// The engine's own tests could not see that -- they pass the option directly,
// which is precisely the caller this defect lived in.
//
// Two halves. The refusal proves the run declines without the variable, and the
// control proves setting it actually reaches the runner: without the second,
// deleting the wiring entirely would read as a passing security test.
func TestSchemaPlanTest_ExternalStepNeedsTheAuthorization(t *testing.T) {
	c := qt.New(t)
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("the fixture names a POSIX program"))

	dir := writePlanTestSuiteWithExternalStep(c)

	t.Run("refused without the variable", func(t *testing.T) {
		c := qt.New(t)

		err, output := runCompatPlanTest(c, dir)

		c.Assert(err, qt.IsNotNil)
		c.Assert(output, qt.Contains, dbtest.AllowExternalCommandsEnvVar)
	})

	t.Run("runs with it", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(dbtest.AllowExternalCommandsEnvVar, "1")

		err, output := runCompatPlanTest(c, dir)

		c.Assert(err, qt.IsNil, qt.Commentf("output: %s", output))
		c.Assert(output, qt.Contains, "external output")
	})
}

// writePlanTestSuiteWithExternalStep builds the smallest plan case that runs a
// program: no plan file is applied, because the authorization is decided before
// any case executes and a plan would only add a way for the fixture to fail for
// an unrelated reason.
func writePlanTestSuiteWithExternalStep(c *qt.C) string {
	dir := c.TB.(*testing.T).TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "e.test.hcl"), []byte(`
test "plan" "runs a program" {
  external {
    program = ["/bin/echo", "hello"]
    output  = "hello"
  }
}
`), 0o600), qt.IsNil)
	return dir
}

// runCompatPlanTest executes `schema plan test` over dir and returns its error
// and combined output.
func runCompatPlanTest(c *qt.C, dir string) (error, string) {
	cmd := atlas.NewCompatCommand("atlas")

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "plan", "test", "--dev-url", "sqlite://dev?mode=memory", dir})

	err := cmd.Execute()
	c.Logf("schema plan test output: %s", out.String())
	return err, out.String()
}
