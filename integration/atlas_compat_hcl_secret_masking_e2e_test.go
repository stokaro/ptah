//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// hclSecretMaskingProject writes an atlas.hcl that declares the variable pw
// and the sensitive variable token, with env "dev" taking url, and returns its
// directory.
func hclSecretMaskingProject(c *qt.C, preamble, url string) string {
	c.Helper()
	dir := c.TempDir()
	project := `variable "pw" {
  type    = string
  default = "x"
}

variable "token" {
  type      = string
  default   = "x"
  sensitive = true
}

` + preamble + `
env "dev" {
  url = ` + url + `
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	return dir
}

// TestCompatMasksASecretInAnAtlasHCLErrorE2E runs `ptah-compat migrate status`
// over a project whose evaluation fails on a secret, and reads stderr. Each
// error quotes the value it failed on, and each value is a secret: a --var
// value that reaches the password of the env's url, which no variable declares
// sensitive, written in an address of the file's own or after one a variable
// gives, and a sensitive variable a data source quotes with no name or address
// beside it. Each fails before anything connects.
func TestCompatMasksASecretInAnAtlasHCLErrorE2E(t *testing.T) {
	tests := []struct {
		name     string
		preamble string
		url      string
		want     string
	}{
		{
			name: "a call in the password of the url",
			url:  `"postgres://app:${tonumber(var.pw)}@127.0.0.1:1/db?sslmode=disable"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a call in a password parameter after an address a variable gives",
			preamble: `variable "base" {
  type    = string
  default = "postgres://app@127.0.0.1:1/app?sslmode=disable"
}
`,
			url:  `"${var.base}&password=${tonumber(var.pw)}"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a sensitive variable a data source quotes",
			preamble: `data "runtimevar" "probe" {
  url = "constant://?val=${var.token}&decoder=json"
}
`,
			url:  `"postgres://app:${data.runtimevar.probe}@127.0.0.1:1/db?sslmode=disable"`,
			want: `data.runtimevar.probe: opening variable: open variable constant:?val=redacted&decoder=json`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := hclSecretMaskingProject(c, test.preamble, test.url)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				"migrate", "status", "--env", "dev", "--var", "pw=Pa55wordX9", "--var", "token=Tok3nValueX9")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Pa55wordX9")
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Tok3nValueX9")
		})
	}
}

// TestCompatKeepsAValueThatReachesNoPasswordInAnAtlasHCLErrorE2E is the control
// for the test above: a --var value in a query parameter that keeps no secret
// stays in the error, so the masking reads where the value goes rather than
// taking out every --var value.
func TestCompatKeepsAValueThatReachesNoPasswordInAnAtlasHCLErrorE2E(t *testing.T) {
	c := qt.New(t)
	dir := hclSecretMaskingProject(c, "", `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"migrate", "status", "--env", "dev", "--var", "pw=Visib1eValue")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `cannot convert "Visib1eValue" to number`)
}

// TestCompatKeepsWhatAProjectDoesNotHoldSecretInALaterErrorE2E runs
// `ptah-compat migrate status` over a project whose migration directory is
// missing, and reads stderr. The command masks its later errors against what
// the project holds secret and nothing else, so the directory's name keeps a
// word that names an item of a sensitive map, and a value a format() takes by
// index, which may reach a password but is not known to.
func TestCompatKeepsWhatAProjectDoesNotHoldSecretInALaterErrorE2E(t *testing.T) {
	tests := []struct {
		name    string
		project string
		want    string
	}{
		{
			name: "the name of an item of a sensitive map",
			project: `variable "db" {
  type      = map(string)
  sensitive = true
  default   = { username = "app", password = "S3cretPassQ" }
}

env "dev" {
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://username-migrations"
  }
}
`,
			want: "username-migrations",
		},
		{
			name: "a value a format() takes by index",
			project: `variable "name" {
  type    = string
  default = "x"
}

env "dev" {
  url = format("sqlite://%[1]s.db", var.name)
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://${var.name}-migrations"
  }
}
`,
			want: "inventory-migrations",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(test.project), 0o600), qt.IsNil)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				"migrate", "status", "--env", "dev", "--var", "name=inventory")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "open migrations directory: ")
			c.Assert(got.Stderr, qt.Contains, test.want)
		})
	}
}

// hclSchemaSecretProject writes an atlas.hcl whose env "dev" takes its desired
// state from a data "hcl_schema" block that passes variable to the schema
// file, and a schema file that converts the value to a number, and returns
// its directory.
func hclSchemaSecretProject(c *qt.C, variable string) string {
	c.Helper()
	dir := c.TempDir()
	project := `variable "token" {
  type      = string
  default   = "x"
  sensitive = true
}

variable "label" {
  type    = string
  default = "x"
}

data "hcl_schema" "app" {
  path = "schema.hcl"
  vars = {
    tok = ` + variable + `
  }
}

env "dev" {
  src = data.hcl_schema.app.url
  url = "sqlite://file?mode=memory"
  dev = "sqlite://dev?mode=memory"
}
`
	schema := `variable "tok" {
  type = string
}

schema "main" {}

table "t" {
  schema = schema.main
  column "id" {
    type    = int
    default = tonumber(var.tok)
  }
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(schema), 0o600), qt.IsNil)
	return dir
}

// TestCompatMasksASensitiveValueAnHCLSchemaFileQuotesE2E runs `ptah-compat
// schema apply` over a schema file that a data "hcl_schema" block passes a
// variable to, and reads stderr. The schema file's parser quotes the value it
// fails on long after the project's own parser is gone, so a sensitive value
// is masked by the command, and the control's plain value keeps its text.
func TestCompatMasksASensitiveValueAnHCLSchemaFileQuotesE2E(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		want     string
	}{
		{name: "a sensitive variable", variable: "var.token", want: `cannot convert "***" to number`},
		{name: "the control, a plain variable", variable: "var.label", want: `cannot convert "Visib1eValue" to number`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := hclSchemaSecretProject(c, test.variable)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				"schema", "apply", "--env", "dev", "--dry-run", "--var", "token=Tok3nValueX9", "--var", "label=Visib1eValue")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Tok3nValueX9")
		})
	}
}
