//go:build integration && unix

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestCompatMasksASensitiveProgramArgumentE2E runs `ptah-compat` over a
// project that passes the sensitive variable token to a program as a bare
// argument, and reads stderr. The program prints the argument alone and
// fails, so nothing beside the value says it is a secret: the variable's
// sensitive flag is what has to mask it, in the error of a data "external", of
// a data "external_schema", and of an external_schema that is a part of a
// composite_schema. The program is a shell script, so the test runs where sh
// does.
func TestCompatMasksASensitiveProgramArgumentE2E(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
		want    string
	}{
		{
			name: "a data external program",
			project: `data "external" "probe" {
  program = ["sh", "-c", "echo \"denied for $1\" >&2; exit 3", "probe", var.token]
}

env "dev" {
  url = "postgres://app:${data.external.probe}@127.0.0.1:1/db?sslmode=disable"
}
`,
			args: []string{"migrate", "status", "--env", "dev"},
			want: "data.external.probe: running program sh: denied for redacted",
		},
		{
			name: "a data external_schema program",
			project: `data "external_schema" "app" {
  program = ["sh", "-c", "echo \"denied for $1\" >&2; exit 3", "probe", var.token]
}

env "dev" {
  src = data.external_schema.app.url
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
}
`,
			args: []string{"schema", "inspect", "--env", "dev", "--url", "env://src"},
			want: `schema command "sh" failed: exit status 3: denied for redacted`,
		},
		{
			name: "a data external_schema program that is a part of a composite_schema",
			project: `data "external_schema" "app" {
  program = ["sh", "-c", "echo \"denied for $1\" >&2; exit 3", "probe", var.token]
}

data "composite_schema" "all" {
  schema "app" {
    url = data.external_schema.app.url
  }
}

env "dev" {
  src = data.composite_schema.all.url
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
}
`,
			args: []string{"schema", "inspect", "--env", "dev", "--url", "env://src"},
			want: `composite schema "app" at atlas.hcl:11: --url "ptah-external-schema://app": schema command "sh" failed: exit status 3: denied for redacted`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			project := `variable "token" {
  type      = string
  sensitive = true
}

` + test.project
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				append(test.args, "--var", "token=Tok3nValueX9")...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Tok3nValueX9")
		})
	}
}

// TestCompatKeepsWhatAnObjectLocalHoldsBesideAPasswordE2E is the control for
// the test above: a local that is an object holds a password beside a user, a
// host and a database name, and a program prints those with the word
// "password". Only the password item is a secret, so the program's line keeps
// its text.
func TestCompatKeepsWhatAnObjectLocalHoldsBesideAPasswordE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	project := `variable "pw" {
  type    = string
  default = "x"
}

locals {
  db = {
    user     = "appuser"
    host     = "dbhost.internal"
    name     = "appdb"
    password = var.pw
  }
  conn = "postgres://${local.db.user}:${local.db.password}@${local.db.host}:5432/${local.db.name}"
}

data "external_schema" "app" {
  program = ["sh", "-c", "echo 'FATAL: password authentication failed for user appuser on dbhost.internal database appdb' >&2; exit 3"]
}

env "dev" {
  src = data.external_schema.app.url
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"schema", "inspect", "--env", "dev", "--url", "env://src", "--var", "pw=Pa55wordX9")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "FATAL: password authentication failed for user appuser on dbhost.internal database appdb")
}

// TestCompatKeepsTheNamesOfASensitiveMapE2E is a control for the tests above:
// the items of a sensitive map hold a user name and a password, and a program
// prints a server's line with the word "password", the user name and the
// password. The names of the items name values and are not secrets, and a
// value shorter than five characters is masked only where it stands as a
// credential, so the line keeps its text and loses the password alone.
func TestCompatKeepsTheNamesOfASensitiveMapE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	project := `variable "db" {
  type      = map(string)
  sensitive = true
  default   = { username = "app", password = "S3cretPassQ" }
}

data "external_schema" "app" {
  program = ["sh", "-c", "echo 'FATAL: password authentication failed for user app (S3cretPassQ)' >&2; exit 3"]
}

env "dev" {
  src = data.external_schema.app.url
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "schema", "inspect", "--env", "dev", "--url", "env://src")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "FATAL: password authentication failed for user app (redacted)")
	c.Assert(got.Stderr, qt.Not(qt.Contains), "S3cretPassQ")
}
