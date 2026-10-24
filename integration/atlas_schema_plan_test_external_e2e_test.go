//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/migration/dbtest"
)

// TestSchemaPlanTest_CompatRunsAnExternalStepAsAtlas drives the verb rather
// than the engine. ptah-compat runs a `.test.hcl` `external` step as Atlas
// does, with no Ptah-only variable: an `atlas.hcl` and its tests written for
// Atlas must work on the drop-in. Native ptah asks for a permission instead;
// internal/execpolicy holds that rule. The strict CE profile has no `schema plan
// test` verb at all, as the community binary has none.
//
// The step's own output in the report is the proof the program ran; a refusal
// or a skipped step leaves it out.
func TestSchemaPlanTest_CompatRunsAnExternalStepAsAtlas(t *testing.T) {
	c := qt.New(t)
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("the fixture names a POSIX program"))

	dir := writePlanTestSuiteWithExternalStep(c)

	output, err := runCompatPlanTest(c, dir)

	c.Assert(err, qt.IsNil, qt.Commentf("output: %s", output))
	c.Assert(output, qt.Contains, `external output "hello"`)
	c.Assert(output, qt.Not(qt.Contains), dbtest.AllowExternalCommandsEnvVar)
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

// runCompatPlanTest executes `schema plan test` over dir and returns its
// combined output and its error.
func runCompatPlanTest(c *qt.C, dir string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "plan", "test", "--dev-url", "sqlite://dev?mode=memory", dir})

	err := cmd.Execute()
	c.Logf("schema plan test output: %s", out.String())
	return out.String(), err
}
