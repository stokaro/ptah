//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A `schema diff` of two schema files for an engine an executable provider
// serves compares the documents offline. A `--dev-url docker://<engine>/<tag>`
// names the engine, and the command checks that engine against the target's
// and asks the provider for no dev database. The fixture's dev databases are
// files in FIXTURE_DEV_DIR, so an empty directory says none was left behind,
// and the tag `none`, which the fixture refuses to provision, says none was
// requested.

// dockerDiffFrom declares items alone.
const dockerDiffFrom = "tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n"

// dockerDiffTo adds the table second to dockerDiffFrom.
const dockerDiffTo = dockerDiffFrom + "  second:\n    columns:\n      value:\n        type: TEXT\n"

// dockerFileDiff runs `schema diff` of dockerDiffFrom and dockerDiffTo through
// the devdb fixture, with extra added to the command line, and returns what
// it wrote to each stream and its exit code.
func dockerFileDiff(c *qt.C, f devCLI, extra ...string) (stdout, stderr string, code int) {
	c.Helper()
	dir := filepath.Dir(f.schemaPath)
	fromPath, toPath := filepath.Join(dir, "from.yaml"), filepath.Join(dir, "to.yaml")
	c.Assert(os.WriteFile(fromPath, []byte(dockerDiffFrom), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(dockerDiffTo), 0o600), qt.IsNil)
	args := append([]string{"schema", "diff", "--provider-config", f.manifestPath, "--from", fromPath, "--to", toPath}, extra...)
	return executeProviderCLI(c.Context(), args)
}

// TestProviderFileDiffTakesADockerDevURL_HappyPathE2E plans the change with a
// docker dev URL for the provider's engine and leaves the dev directory empty.
// The `none` row fails if the command asks the provider for the database,
// and both rows fail if the command opens the URL as an engine Ptah starts.
func TestProviderFileDiffTakesADockerDevURL_HappyPathE2E(t *testing.T) {
	for _, devURL := range []string{"docker://fixture/1", "docker://fixture/none"} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)

			stdout, stderr, code := dockerFileDiff(c, f, "--dev-url", devURL)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "CREATE TABLE \"second\" (\"value\" TEXT);\n")
			c.Assert(entries(c, f.dir), qt.HasLen, 0)
		})
	}
}

// TestProviderFileDiffTakesADockerDevURL_FailurePathE2E leaves the dev URL out:
// two schema files name no engine, so the command refuses before it compares.
func TestProviderFileDiffTakesADockerDevURL_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)

	stdout, stderr, code := dockerFileDiff(c, f)

	c.Assert(code, qt.Equals, 2)
	c.Assert(stderr, qt.Equals, "error: --dev-url is required for local schema file diffing\n")
	c.Assert(stdout, qt.Equals, "")
	c.Assert(entries(c, f.dir), qt.HasLen, 0)
}
