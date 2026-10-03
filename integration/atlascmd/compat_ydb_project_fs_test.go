//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// The phrase every YDB refusal on the compatibility surface ends with.
const compatYDBGap = "names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
	"(stokaro/ptah#4015, phase 11)"

// None of these URLs is dialed: the port is one nothing listens on, so a run
// that reached a connection would fail with a dial error instead.
const compatYDBURL = "ydb://127.0.0.1:1/local"

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

// A YDB URL an atlas.hcl env carries is refused as soon as the file is
// parsed, whichever attribute holds it and whether the file wrote it or read it
// through getenv, under either policy. A forwarded verb and the collection an
// env with for_each loads are refused the same way.
func TestCompatRefusesAYDBURLFromTheProjectFile(t *testing.T) {
	tests := []struct {
		name    string
		project string
		policy  atlascompatpolicy.Policy
		args    []string
		want    string
	}{
		{
			name:    "url",
			project: `env "x" { url = "` + compatYDBURL + `" }`,
			policy:  atlascompatpolicy.Full(),
			args:    []string{"schema", "inspect", "--env", "x"},
			want:    `Error: atlas.hcl env "x": url ` + compatYDBGap + "\n",
		},
		{
			name:    "url under strict compatibility",
			project: `env "x" { url = "` + compatYDBURL + `" }`,
			policy:  atlascompatpolicy.StrictCE(),
			args:    []string{"schema", "inspect", "--env", "x"},
			want:    `Error: atlas.hcl env "x": url ` + compatYDBGap + "\n",
		},
		{
			name:    "dev",
			project: `env "x" {` + "\n" + `url = "sqlite://target.db"` + "\n" + `dev = "` + compatYDBURL + `"` + "\n}",
			policy:  atlascompatpolicy.Full(),
			args:    []string{"schema", "inspect", "--env", "x"},
			want:    `Error: atlas.hcl env "x": dev ` + compatYDBGap + "\n",
		},
		{
			name:    "src",
			project: `env "x" {` + "\n" + `url = "sqlite://target.db"` + "\n" + `src = "` + compatYDBURL + `"` + "\n}",
			policy:  atlascompatpolicy.Full(),
			args:    []string{"schema", "inspect", "--env", "x"},
			want:    `Error: atlas.hcl env "x": src ` + compatYDBGap + "\n",
		},
		{
			name:    "an env with for_each",
			project: `env "fan" {` + "\n" + `for_each = toset(["a", "b"])` + "\n" + `url = "` + compatYDBURL + `"` + "\n}",
			policy:  atlascompatpolicy.Full(),
			args:    []string{"migrate", "apply", "--env", "fan", "--dir", "file://migrations"},
			want:    `Error: atlas.hcl env "fan": url ` + compatYDBGap + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeProject(c, test.project)

			stdout, stderr, err := runCompatIn(c, dir, test.policy, test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// getenv inside atlas.hcl is how an environment variable reaches an env, and
// the value it reads is refused like one written in the file.
func TestCompatRefusesAYDBURLAProjectFileReadsFromTheEnvironment(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_TEST_TENANT_DATABASE", compatYDBURL)
	dir := writeProject(c, `env "x" { url = getenv("PTAH_TEST_TENANT_DATABASE") }`)

	stdout, stderr, err := runCompatIn(c, dir, atlascompatpolicy.Full(), "schema", "inspect", "--env", "x")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `Error: atlas.hcl env "x": url `+compatYDBGap+"\n")
}

// A forwarded verb returns its error to the process rather than printing it.
// migrate down reads the project file the compatibility surface loads for it,
// and with no --env it is the native command that finds the single env.
func TestCompatRefusesAYDBURLFromTheProjectFileOnAForwardedVerb(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "an env the verb names", args: []string{"migrate", "down", "--env", "x", "--dir", "file://migrations"},
			wantErr: regexp.QuoteMeta(`atlas.hcl env "x": url ` + compatYDBGap)},
		{name: "the single env the native command finds", args: []string{"migrate", "down", "--dir", "file://migrations"},
			wantErr: `(?s).*using a YDB database through ptah-compat is not implemented yet \(stokaro/ptah#4015, phase 11\)`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeProject(c, `env "x" { url = "`+compatYDBURL+`" }`)

			stdout, _, err := runCompatIn(c, dir, atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// A data "sql" source connects while the project file is parsed, before any
// verb reads an env, so the refusal the surface's commands run under is what
// stops it, under either policy.
func TestCompatRefusesADataSourceThatNamesYDB(t *testing.T) {
	tests := []struct {
		name   string
		policy atlascompatpolicy.Policy
	}{
		{name: "full", policy: atlascompatpolicy.Full()},
		{name: "strict", policy: atlascompatpolicy.StrictCE()},
	}
	project := `data "sql" "tenants" {` + "\n" +
		`url   = "` + compatYDBURL + `"` + "\n" +
		`query = "SELECT 1"` + "\n" +
		"}\n" +
		`env "x" {` + "\n" +
		`url     = "sqlite://target.db"` + "\n" +
		`schemas = data.sql.tenants.values` + "\n" +
		"}\n"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeProject(c, project)

			stdout, stderr, err := runCompatIn(c, dir, test.policy, "schema", "inspect", "--env", "x")

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Matches, `Error: data\.sql\.tenants: opening database: `+
				regexp.QuoteMeta("using a YDB database through ptah-compat is not implemented yet (stokaro/ptah#4015, phase 11)")+`\n`)
		})
	}
}
