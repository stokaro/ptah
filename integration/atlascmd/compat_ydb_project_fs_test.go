//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// compatYDBURL names a YDB database with a parameter Ptah's YDB connector
// refuses, so a run that reaches the connector fails there at once, offline,
// and names the parameter. Nothing is dialed.
const compatYDBURL = "ydb://127.0.0.1:1/local?bogus=1"

// compatYDBConnectorRefusal is what the connector answers for compatYDBURL. A
// run that prints it got past every refusal on the compatibility surface.
const compatYDBConnectorRefusal = `invalid YDB URL: parameter "bogus" is not one Ptah reads on a YDB URL`

// compatYDBUnknownDriver is what the pinned community binary answers for a
// ydb:// URL, and what the strict profile answers with it.
const compatYDBUnknownDriver = `sql/sqlclient: unknown driver "ydb". See: https://atlasgo.io/url`

// runCompatIn runs the compatibility tree under policy in dir, where the
// project file is the default atlas.hcl.
func runCompatIn(c *qt.C, dir string, policy atlascompatpolicy.Policy, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	c.Chdir(dir)
	cmd := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// writeProject writes atlas.hcl and an empty migration directory into a new
// directory and returns it.
func writeProject(c *qt.C, project string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	return dir
}

// compatYDBDataSourceProject connects to YDB while the project file is parsed,
// before any verb reads an env.
const compatYDBDataSourceProject = `data "sql" "tenants" {` + "\n" +
	`url   = "` + compatYDBURL + `"` + "\n" +
	`query = "SELECT 1"` + "\n" +
	"}\n" +
	`env "x" {` + "\n" +
	`url     = "sqlite://target.db"` + "\n" +
	`schemas = data.sql.tenants.values` + "\n" +
	"}\n"

// The strict profile refuses a YDB URL an atlas.hcl env carries, in the pinned
// binary's words, whichever attribute holds it and whether the file wrote it or
// read it through getenv. The collection an env with for_each loads is refused
// the same way.
func TestStrictCompatRefusesAYDBURLFromTheProjectFile(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{
			name:    "url",
			project: `env "x" { url = "` + compatYDBURL + `" }`,
			args:    []string{"schema", "inspect", "--env", "x"},
		},
		{
			name:    "dev",
			project: `env "x" {` + "\n" + `url = "sqlite://target.db"` + "\n" + `dev = "` + compatYDBURL + `"` + "\n}",
			args:    []string{"schema", "inspect", "--env", "x"},
		},
		{
			name:    "src",
			project: `env "x" {` + "\n" + `url = "sqlite://target.db"` + "\n" + `src = "` + compatYDBURL + `"` + "\n}",
			args:    []string{"schema", "inspect", "--env", "x"},
		},
		{
			name:    "a value read through getenv",
			project: `env "x" { url = getenv("PTAH_TEST_TENANT_DATABASE") }`,
			args:    []string{"schema", "inspect", "--env", "x"},
		},
		{
			name:    "an env with for_each",
			project: `env "fan" {` + "\n" + `for_each = toset(["a", "b"])` + "\n" + `url = "` + compatYDBURL + `"` + "\n}",
			args:    []string{"migrate", "apply", "--env", "fan", "--dir", "file://migrations"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_TEST_TENANT_DATABASE", compatYDBURL)
			dir := writeProject(c, test.project)

			stdout, stderr, err := runCompatIn(c, dir, atlascompatpolicy.StrictCE(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: "+compatYDBUnknownDriver+"\n")
		})
	}
}

// A data "sql" source connects while the project file is parsed, where no URL
// check on this surface looks. The strict profile refuses the connection
// itself, so the source is refused in the binary's words too.
func TestStrictCompatRefusesADataSourceThatNamesYDB(t *testing.T) {
	c := qt.New(t)
	dir := writeProject(c, compatYDBDataSourceProject)

	stdout, stderr, err := runCompatIn(c, dir, atlascompatpolicy.StrictCE(), "schema", "inspect", "--env", "x")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: data.sql.tenants: opening database: "+compatYDBUnknownDriver+"\n")
}

// The default profile keeps YDB: the same project files reach the connector,
// for an env's URL, a value read through getenv and a data source.
func TestCompatHandsAYDBURLFromTheProjectFileToTheConnector(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{
			name:    "url",
			project: `env "x" { url = "` + compatYDBURL + `" }`,
			args:    []string{"schema", "inspect", "--env", "x"},
		},
		{
			name:    "a value read through getenv",
			project: `env "x" { url = getenv("PTAH_TEST_TENANT_DATABASE") }`,
			args:    []string{"migrate", "status", "--env", "x", "--dir", "file://migrations"},
		},
		{
			name:    "a data source",
			project: compatYDBDataSourceProject,
			args:    []string{"schema", "inspect", "--env", "x"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_TEST_TENANT_DATABASE", compatYDBURL)
			dir := writeProject(c, test.project)

			stdout, stderr, err := runCompatIn(c, dir, atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*`+compatYDBConnectorRefusal+`.*`)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Not(qt.Contains), "unknown driver")
		})
	}
}
