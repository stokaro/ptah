//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A `schema diff` of two schema files for an engine an executable provider
// serves compares the documents offline. A docker dev URL names the engine in
// either form, `docker://<engine>/<tag>` and `docker+<engine>://...`, and the
// command checks that engine against the target's and asks the provider for
// no dev database. The fixture's dev databases are files in FIXTURE_DEV_DIR,
// so an empty directory says none was left behind, and a spec ending in
// `none`, which the fixture refuses to provision, says none was requested.

// formsDiffFrom declares items alone.
const formsDiffFrom = "tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n"

// formsDiffTo adds the table second to formsDiffFrom.
const formsDiffTo = formsDiffFrom + "  second:\n    columns:\n      value:\n        type: TEXT\n"

// diffSchemaFilesOnDevURL runs `schema diff` of formsDiffFrom and formsDiffTo
// through the devdb fixture with devURL as --dev-url, and returns what it
// wrote to each stream and its exit code.
func diffSchemaFilesOnDevURL(c *qt.C, f devCLI, devURL string) (stdout, stderr string, code int) {
	c.Helper()
	dir := filepath.Dir(f.schemaPath)
	fromPath, toPath := filepath.Join(dir, "from.yaml"), filepath.Join(dir, "to.yaml")
	c.Assert(os.WriteFile(fromPath, []byte(formsDiffFrom), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(formsDiffTo), 0o600), qt.IsNil)
	return executeProviderCLI(c.Context(), []string{
		"schema", "diff", "--provider-config", f.manifestPath, "--from", fromPath, "--to", toPath, "--dev-url", devURL,
	})
}

// TestProviderFileDiffTakesEitherDockerDevURLForm_HappyPathE2E plans the change
// with a docker dev URL for the provider's engine in each form, and leaves the
// dev directory empty. A `none` row fails if the command asks the provider for
// the database. A `docker+fixture` row fails if the command reads only the
// `docker://` form as the provider's and connects to the URL as an address.
func TestProviderFileDiffTakesEitherDockerDevURLForm_HappyPathE2E(t *testing.T) {
	for _, devURL := range []string{
		"docker://fixture/1",
		"docker://fixture/none",
		"docker+fixture://_/example.org/fixture:2.0",
		"docker+fixture://_/example.org/fixture:none",
	} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)

			stdout, stderr, code := diffSchemaFilesOnDevURL(c, f, devURL)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "CREATE TABLE \"second\" (\"value\" TEXT);\n")
			c.Assert(entries(c, f.dir), qt.HasLen, 0)
		})
	}
}
