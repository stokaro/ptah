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
		vars     []string
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
		{
			name: "a password a join() over a concat() writes",
			url:  `join("", concat(["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db?sslmode=disable"]))`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a join() over a flatten() writes",
			url:  `join("", flatten([["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db?sslmode=disable"]]))`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a join() over a variable list writes",
			preamble: `variable "head" {
  type    = list(string)
  default = ["postgres://app:"]
}
`,
			url:  `join("", concat(var.head, [tonumber(var.pw)], ["@127.0.0.1:1/db?sslmode=disable"]))`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a join() over a local list writes",
			preamble: `locals {
  parts = ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db?sslmode=disable"]
}
`,
			url:  `join("", local.parts)`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password after an address a failing call was to give",
			preamble: `variable "base" {
  type    = string
  default = "postgres://app@127.0.0.1:1/app?sslmode=disable"
}
`,
			url:  `"${trimsuffix(var.base, tonumber(var.pw))}&password=${var.pw}"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password parameter a join() over a concat() writes",
			url:  `"postgres://app@127.0.0.1:1/db?${join("", concat(["password="], [tonumber(var.pw)]))}"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a join() over a for expression writes",
			url:  `join("", [for p in ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"] : p])`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a for directive writes",
			url:  `"%{ for p in ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"] }${p}%{ endfor }"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password between the head and the tail of an address two variables give",
			preamble: `variable "head" {
  default = "postgres://app:"
}

variable "tail" {
  default = "@127.0.0.1:1/db"
}
`,
			url:  `"${var.head}${tonumber(var.pw)}${var.tail}"`,
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a join() over the elements of a variable list writes",
			preamble: `variable "parts" {
  type    = list(string)
  default = ["x"]
}
`,
			url:  `join("", [var.parts[0], tonumber(var.parts[1]), var.parts[2]])`,
			vars: []string{"--var", "parts=postgres://app:", "--var", "parts=Pa55wordX9", "--var", "parts=@127.0.0.1:1/db"},
			want: `cannot convert "redacted" to number`,
		},
		{ // #nosec G101 -- a made-up address of a port nothing listens on, not a credential
			name: "a password replace() puts in an address a variable holds",
			preamble: `variable "tpl" {
  default = "postgres://app:PW@127.0.0.1:1/db"
}
`,
			url:  `replace(var.tpl, "PW", tonumber(var.pw))`,
			want: `cannot convert "redacted" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := hclSecretMaskingProject(c, test.preamble, test.url)

			args := append([]string{"migrate", "status", "--env", "dev", "--var", "pw=Pa55wordX9", "--var", "token=Tok3nValueX9"}, test.vars...)
			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, args...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Pa55wordX9")
			c.Assert(got.Stderr, qt.Not(qt.Contains), "Tok3nValueX9")
		})
	}
}

// TestCompatRunsAProjectWhoseAddressAForExpressionWritesE2E runs `ptah-compat
// migrate hash` over a project whose url a for expression helps write, and
// reads the sum file it writes. The masking reads every address a project
// writes on each run, so the for's scope among the nodes it reads must not stop
// a valid project.
func TestCompatRunsAProjectWhoseAddressAForExpressionWritesE2E(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{ // #nosec G101 -- a made-up address of a port nothing listens on, not a credential
			name: "a for expression in the query",
			url:  `"postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=${join(",", [for s in ["a", "b"] : s])}"`,
		},
		{ // #nosec G101 -- a made-up address of a port nothing listens on, not a credential
			name: "a for directive in the query",
			url:  `"postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=%{ for s in ["a", "b"] }${s}%{ endfor }"`,
		},
		{
			name: "a for expression in the password",
			url:  `"postgres://app:${join("", [for s in ["p", "w"] : s])}@127.0.0.1:1/db"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := hclSecretMaskingProject(c, "", test.url)
			c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o700), qt.IsNil)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "migrate", "hash", "--env", "dev")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			_, err := os.Stat(filepath.Join(dir, "migrations", "atlas.sum"))
			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCompatMasksAShortPasswordAFailingCallQuotesE2E runs `ptah-compat migrate
// status` over a project whose url a join() over a concat() writes, with a
// password too short to take out of every word of an error. The diagnostic of
// the call that refused it quotes it whole, and it is taken out there.
func TestCompatMasksAShortPasswordAFailingCallQuotesE2E(t *testing.T) {
	const url = `join("", concat(["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db?sslmode=disable"]))`
	for _, password := range []string{"x", "ab"} {
		t.Run(password, func(t *testing.T) {
			c := qt.New(t)
			dir := hclSecretMaskingProject(c, "", url)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				"migrate", "status", "--env", "dev", "--var", "pw="+password, "--var", "token=Tok3nValueX9")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `cannot convert "redacted" to number`)
			c.Assert(got.Stderr, qt.Not(qt.Contains), `"`+password+`"`)
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
