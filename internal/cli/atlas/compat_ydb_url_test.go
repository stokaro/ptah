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

// ydbConnectorRefusedURL names a YDB database with a parameter Ptah's YDB
// connector refuses. A run that reaches the connector fails there, offline and
// at once, naming the parameter. That failure is the proof that the
// compatibility surface handed the URL on: a refusal on this surface would
// answer before the connector is asked.
const ydbConnectorRefusedURL = "ydb://127.0.0.1:1/local?bogus=1"

// ydbConnectorRefusal is what the connector answers for ydbConnectorRefusedURL.
const ydbConnectorRefusal = `invalid YDB URL: parameter "bogus" is not one Ptah reads on a YDB URL`

// ydbUnknownDriver is what the pinned community binary answers for a ydb://
// URL on every verb, and what the strict profile answers with it.
const ydbUnknownDriver = "Error: sql/sqlclient: unknown driver \"ydb\". See: https://atlasgo.io/url\n"

// runCompatWithPolicy runs the compatibility tree under policy.
func runCompatWithPolicy(policy atlascompatpolicy.Policy, args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// ydbCompatFixture writes a hashed migration directory, a desired schema in
// HCL and a query script into a new directory, and returns their paths.
func ydbCompatFixture(c *qt.C) (migrations, schema, script string) {
	c.Helper()
	dir := c.TempDir()
	migrations = filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20260101000000_init.sql"),
		[]byte("CREATE TABLE `users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`));\n"), 0o600), qt.IsNil)
	_, _, err := runCompatWithPolicy(atlascompatpolicy.Full(), "migrate", "hash", "--dir", "file://"+filepath.ToSlash(migrations))
	c.Assert(err, qt.IsNil)
	schema = filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schema, []byte(`table "users" {
  column "id" {
    type = Int64
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)
	script = filepath.Join(dir, "script.hcl")
	c.Assert(os.WriteFile(script, []byte(`script "query" "report" {
  query "rows" {
    sql = "SELECT 1"
  }
}

script "exec" "fix" {
  exec "upd" {
    sql = "UPDATE users SET id = id WHERE id = 1"
  }
}
`), 0o600), qt.IsNil)
	return migrations, schema, script
}

// A YDB URL on this surface is a Ptah extension, since no Atlas edition has a
// YDB driver, and the default policy keeps it: every verb that takes a database
// URL hands it to the connector, whichever flag carries it. The rows run
// offline, and the connector's own refusal of the URL is what they read.
func TestCompatHandsAYDBURLToTheConnector(t *testing.T) {
	c := qt.New(t)
	migrations, schema, script := ydbCompatFixture(c)
	dir := "file://" + filepath.ToSlash(migrations)
	to := "file://" + filepath.ToSlash(schema)
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema inspect --url", args: []string{"schema", "inspect", "--url", ydbConnectorRefusedURL}},
		{name: "schema apply --url", args: []string{"schema", "apply", "--url", ydbConnectorRefusedURL,
			"--to", to, "--dry-run"}},
		{name: "schema diff --from", args: []string{"schema", "diff", "--from", ydbConnectorRefusedURL, "--to", to,
			"--dev-url", ydbConnectorRefusedURL}},
		{name: "schema clean --url", args: []string{"schema", "clean", "--url", ydbConnectorRefusedURL, "--dry-run"}},
		{name: "schema stats inspect --db-url", args: []string{"schema", "stats", "inspect", "--db-url",
			ydbConnectorRefusedURL}},
		{name: "migrate apply --url", args: []string{"migrate", "apply", "--url", ydbConnectorRefusedURL, "--dir", dir}},
		{name: "migrate status --url", args: []string{"migrate", "status", "--url", ydbConnectorRefusedURL, "--dir", dir}},
		{name: "migrate set --url", args: []string{"migrate", "set", "--url", ydbConnectorRefusedURL, "--dir", dir,
			"20260101000000"}},
		{name: "migrate down --url", args: []string{"migrate", "down", "--url", ydbConnectorRefusedURL, "--dir", dir}},
		{name: "script query --url", args: []string{"script", "query", "--url", ydbConnectorRefusedURL,
			"--file", script}},
		{name: "script exec --url", args: []string{"script", "exec", "--url", ydbConnectorRefusedURL,
			"--file", script}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), test.args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*`+ydbConnectorRefusal+`.*`)
			c.Assert(stderr, qt.Not(qt.Contains), "unknown driver")
			c.Assert(stdout, qt.Not(qt.Contains), "CREATE")
		})
	}
}

// The strict profile reproduces the pinned community binary, which has no
// YDB driver, so it refuses a YDB URL in that binary's words before anything
// is opened, with the scheme as the URL spells it. A strict run of the same
// arguments the default run hands to the connector stops here instead.
func TestStrictCompatRefusesAYDBURL(t *testing.T) {
	c := qt.New(t)
	migrations, schema, _ := ydbCompatFixture(c)
	dir := "file://" + filepath.ToSlash(migrations)
	to := "file://" + filepath.ToSlash(schema)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "schema inspect --url", args: []string{"schema", "inspect", "--url", ydbConnectorRefusedURL},
			want: ydbUnknownDriver},
		{name: "schema inspect --url over TLS", args: []string{"schema", "inspect", "--url", "ydbs://127.0.0.1:1/local"},
			want: "Error: sql/sqlclient: unknown driver \"ydbs\". See: https://atlasgo.io/url\n"},
		{name: "schema apply --url", args: []string{"schema", "apply", "--url", ydbConnectorRefusedURL,
			"--to", to, "--dry-run"}, want: ydbUnknownDriver},
		{name: "schema apply --dev-url", args: []string{"schema", "apply", "--url", "sqlite://target?mode=memory",
			"--to", to, "--dev-url", ydbConnectorRefusedURL}, want: ydbUnknownDriver},
		{name: "schema diff --from", args: []string{"schema", "diff", "--from", ydbConnectorRefusedURL, "--to", to,
			"--dev-url", "sqlite://dev?mode=memory"}, want: ydbUnknownDriver},
		{name: "schema clean --url", args: []string{"schema", "clean", "--url", ydbConnectorRefusedURL, "--dry-run"},
			want: ydbUnknownDriver},
		{name: "migrate apply --url", args: []string{"migrate", "apply", "--url", ydbConnectorRefusedURL, "--dir", dir},
			want: ydbUnknownDriver},
		{name: "migrate status --url", args: []string{"migrate", "status", "--url", ydbConnectorRefusedURL,
			"--dir", dir}, want: ydbUnknownDriver},
		{name: "migrate set --url", args: []string{"migrate", "set", "--url", ydbConnectorRefusedURL, "--dir", dir,
			"20260101000000"}, want: ydbUnknownDriver},
		// After the integrity gate, as the binary orders it: the directory is
		// hashed, so the dev URL is what this run answers.
		{name: "migrate validate --dev-url", args: []string{"migrate", "validate", "--dir", dir,
			"--dev-url", ydbConnectorRefusedURL}, want: ydbUnknownDriver},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.StrictCE(), test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// Strict mode refuses docker://ydb in the pinned community binary's words:
// measured on v1.3.0, `--dev-url docker://ydb/...` answers `unsupported docker
// image "ydb"` at exit 1. docker://sqlite is the control: an engine that binary
// does not start either, refused in the same words before and after.
func TestStrictCompatRefusesDockerYDBAsTheBinaryDoes(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
		want   string
	}{
		{name: "ydb", devURL: "docker://ydb/26.2.1.14/local", want: "Error: unsupported docker image \"ydb\"\n"},
		{name: "ydb with no path", devURL: "docker://ydb", want: "Error: unsupported docker image \"ydb\"\n"},
		{name: "sqlite", devURL: "docker://sqlite/dev", want: "Error: unsupported docker image \"sqlite\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{"schema", "inspect", "--url", "file://schema.sql", "--dev-url", test.devURL})

			err := cmd.Execute()

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Equals, test.want)
		})
	}
}
