package projectconfig_test

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/dburldisplay"
)

// addressSecretsProject is a project whose env "dev" takes url, with the
// variables pw, cred, pre and base, a local that converts pw to a number, a
// local that writes a user info around that conversion, and a local that
// writes the scheme and the user of an address.
func addressSecretsProject(url string) []byte {
	return []byte(`variable "pw" {
  type    = string
  default = "x"
}

variable "base" {
  type    = string
  default = "x"
}

variable "cred" {
  type    = string
  default = "x"
}

variable "pre" {
  type    = string
  default = "x"
}

locals {
  port = tonumber(var.pw)
  ui   = "app:${tonumber(var.pw)}"
  base = "postgres://app:"
}

env "dev" {
  url = ` + url + `
}
`)
}

// A value that reaches the password of an address the file writes is masked
// in the error HCL writes when a call on it fails, though no variable declares
// it sensitive. The value never becomes an address, so only the place it
// takes in one says it is a password.
func TestParseAtlas_MasksWhatReachesAPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		vars []string
		want string
	}{
		{
			name: "a call in the password",
			url:  `"postgres://app:${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a call on the password elsewhere in the address",
			url:  `"postgres://app:${var.pw}@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password query parameter",
			url:  `"sqlserver://app@127.0.0.1:1?database=db&password=${tonumber(var.pw)}"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a local in the password whose own expression fails",
			url:  `"postgres://app:${local.port}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a user info that carries the password",
			url:  `"postgres://${var.cred}@127.0.0.1:1/db?connect_timeout=${tonumber(var.cred)}"`,
			vars: []string{"cred=app:Cr3dPassX9"},
			want: `cannot convert "app:redacted" to number`,
		},
		{
			name: "a format() verb in the password",
			url:  `format("postgres://%s:%s@127.0.0.1:1/db", "app", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a format() verb that takes its argument by index",
			url:  `format("postgres://app:%[1]s@127.0.0.1:1/db", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "the password of urluserinfo()",
			url:  `urluserinfo("postgres://127.0.0.1:1/db", "app", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a --var value that is an address",
			url:  `"${var.cred}&x=${tonumber(var.cred)}"`,
			vars: []string{"cred=postgres://app:Cr3dPassX9@127.0.0.1:1/db?sslmode=disable"},
			want: `cannot convert "postgres://app:redacted@127.0.0.1:1/db?sslmode=disable" to number`,
		},
		{
			name: "a whole user info whose value does not evaluate",
			url:  `"postgres://${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a local that writes the user info and whose own expression fails",
			url:  `"postgres://${local.ui}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password after a local that writes the scheme and the user",
			url:  `"${local.base}${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a format() writes after a local that writes the scheme",
			url:  `format("%s%s@127.0.0.1:1/db", local.base, tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a value that writes the user info and the host",
			url:  `"postgres://${lower("APP:CR3DPASSX9@127.0.0.1:1")}/db?connect_timeout=${tonumber(lower("APP:CR3DPASSX9@127.0.0.1:1"))}"`,
			want: `cannot convert "app:redacted@127.0.0.1:1" to number`,
		},
		{
			name: "the second of two values in one password",
			url:  `"postgres://app:${var.pre}${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password urlqueryset() sets",
			url:  `urlqueryset("sqlserver://app@127.0.0.1:1?database=app", "password", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a value urlqueryset() sets under a key the file does not write out",
			url:  `urlqueryset("sqlserver://app@127.0.0.1:1?database=app", var.cred, tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password join() writes",
			url:  `join("", ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"])`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{ // #nosec G101 -- a placeholder replace() fills, not a credential
			name: "a password replace() writes",
			url:  `replace("postgres://app:PASS@127.0.0.1:1/db", "PASS", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a user info a format() in the address writes",
			url:  `"postgres://${format("%s:%s", "app", tonumber(var.pw))}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password after an address a value gives",
			url:  `"${var.base}&password=${tonumber(var.pw)}"`,
			vars: []string{"pw=Pa55wordX9", "base=postgres://app@127.0.0.1:1/app?sslmode=disable"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password urlqueryset() sets on an address a value gives",
			url:  `urlqueryset(var.base, "password", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9", "base=postgres://app@127.0.0.1:1/app?sslmode=disable"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password replace() names in the query parameter urlqueryset() sets",
			url:  `replace(urlqueryset("postgres://app@127.0.0.1:1/db", "app_name", tonumber(var.pw)), "app_name=", "password=")`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: test.vars})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl ".*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), "Pa55wordX9")
			c.Assert(err.Error(), qt.Not(qt.Contains), "Cr3dPassX9")
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The value of getenv() in a password is masked as a --var value is: the
// variable's name says nothing about what it holds.
func TestParseAtlas_MasksAnEnvironmentValueInAPassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_ADDRESS_PART", "Gh7envPassX9")

	got, err := projectconfig.ParseAtlasWithOptions(
		addressSecretsProject(`"postgres://app:${tonumber(getenv("PTAH_TEST_ADDRESS_PART"))}@127.0.0.1:1/db"`),
		"atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "dev"})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Gh7envPassX9")
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A --var value that a typed variable refuses is masked when the variable
// reaches a password, before any expression that reads it runs, whether the
// address names it by an attribute or by a string index.
func TestParseAtlas_MasksATypedVariableThatReachesAPassword_FailurePath(t *testing.T) {
	for _, url := range []string{
		`"postgres://app:${var.pw}@127.0.0.1:1/db"`,
		`"postgres://app:${var["pw"]}@127.0.0.1:1/db"`,
	} {
		t.Run(url, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "pw" {
  type = number
}

env "dev" {
  url = ` + url + `
}
`)

			got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

			c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "pw" expects number, got --var value "redacted"`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The control for the tests above: a value in a place where an address keeps
// no secret keeps its text in the error, so the masking reads the place and
// does not take out every value the file is given.
func TestParseAtlas_KeepsWhatReachesNoPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "the user name beside a password", url: `"postgres://${tonumber(var.pw)}:x@127.0.0.1:1/db"`},
		{name: "a query parameter", url: `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`},
		{name: "a format() verb in the path", url: `format("postgres://app@127.0.0.1:1/%s", tonumber(var.pw))`},
		{name: "the address of urluserinfo()", url: `urluserinfo(tostring(tonumber(var.pw)), "app", "x")`},
		{name: "a query parameter urlqueryset() sets", url: `urlqueryset("postgres://app@127.0.0.1:1/db", "application_name", tonumber(var.pw))`},
		{name: "a path join() writes", url: `join("/", ["postgres://app:x@127.0.0.1:1", tonumber(var.pw)])`}, // #nosec G101 -- a made-up value, not a credential
		{name: "a port replace() writes", url: `replace("postgres://app:x@127.0.0.1:PORT/db", "PORT", tonumber(var.pw))`},
		{name: "a port with no user info", url: `"postgres://127.0.0.1:${tonumber(var.pw)}/db"`},
		{name: "a query parameter after a port another value fills", url: `"postgres://127.0.0.1:${var.pre}/db?application_name=${tonumber(var.pw)}"`},
		{name: "a query parameter after an address a value gives", url: `"${var.base}&application_name=${tonumber(var.pw)}"`},
		{name: "a whole user info whose value evaluates", url: `"postgres://${var.pw}@127.0.0.1:1/db?application_name=${tonumber(var.pw)}"`},
		{name: "a query parameter urlqueryset() sets on an address a value gives", url: `urlqueryset(var.base, "application_name", tonumber(var.pw))`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Visib1eValue", "base=postgres://app@127.0.0.1:1/app?sslmode=disable"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "Visib1eValue" to number.*`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// A flag value a variable gives a parameter named for a secret keeps its text,
// as the same flag written in the address does: the place of the slot reads
// as a secret's, but a flag holds none.
func TestParseAtlas_KeepsAFlagAVariableGives_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		pw   string
	}{
		{name: "a SQLite foreign keys flag", url: `"sqlite://x.db?_foreign_keys=${tonumber(var.pw)}"`, pw: "false"},
		{name: "a MySQL public key flag", url: `"mysql://app@127.0.0.1:1/db?allowPublicKeyRetrieval=${tonumber(var.pw)}"`, pw: "true"},
		{name: "a token flag in upper case", url: `"postgres://app@127.0.0.1:1/db?token=${tonumber(var.pw)}"`, pw: "YES"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=" + test.pw}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "`+test.pw+`" to number.*`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The control for the test above: a flag spelling in a password is still the
// password, so it is masked.
func TestParseAtlas_MasksAPasswordSpelledAsAFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "the user info password", url: `"postgres://app:${tonumber(var.pw)}@127.0.0.1:1/db"`},
		{name: "a password query parameter", url: `"sqlserver://app@127.0.0.1:1?database=db&password=${tonumber(var.pw)}"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=true"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// A sensitive value a data source is given as a bare value, with no name or
// address beside it, is masked in the error the data source fails with.
func TestParseAtlas_MasksASensitiveValueADataSourceQuotes_FailurePath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "runtimevar" "probe" {
  url = "constant://?val=${var.token}&decoder=json"
}

env "dev" {
  url = "postgres://app:${data.runtimevar.probe}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.ErrorMatches,
		`data\.runtimevar\.probe: opening variable: open variable constant:\?val=redacted&decoder=json: .*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A data source error longer than the display bound loses its middle, and a
// mask that runs after the cut cannot find a secret the cut split. The token
// here starts a few bytes before the end of the part the bound keeps, so the
// data source masks it before the bound, or a prefix of it reaches the error.
func TestParseAtlas_MasksASensitiveValueBeforeTheDisplayBound_FailurePath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "runtimevar" "probe" {
  url = "constant://?val=` + strings.Repeat("a", 955) + `${var.token}` + strings.Repeat("b", 1100) + `&decoder=json"
}

env "dev" {
  url = "postgres://app:${data.runtimevar.probe}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.ErrorMatches, `data\.runtimevar\.probe: opening variable: open variable constant:\?val=a+redacted\.\.\.\[truncated\]\.\.\.b+&decoder=json: .*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// externalSchemaSecretsProject declares the sensitive variable token, passes
// it to an external_schema program, and selects the program with src.
func externalSchemaSecretsProject(src string) []byte {
	return []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "external_schema" "app" {
  program = ["./export.sh", var.token]
}

data "composite_schema" "all" {
  schema "app" {
    url = data.external_schema.app.url
  }
}

env "dev" {
  src = ` + src + `
  url = "sqlite://file?mode=memory"
}
`)
}

// An external_schema program carries the values its diagnostics are masked
// against, so the program can print a sensitive argument alone and still not
// show it. They survive a merge with a ptah.yaml config, which is how the
// native binary reads them.
func TestParseAtlas_ExternalSchemaCarriesItsSecrets_HappyPath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlasWithOptions(externalSchemaSecretsProject("data.external_schema.app.url"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
	merged := projectconfig.Merge(projectconfig.Config{}, cfg)
	c.Assert(merged.ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
}

// An external_schema program that is a part of a composite_schema carries the
// same values as one an env selects itself.
func TestParseAtlas_CompositeExternalSchemaCarriesItsSecrets_HappyPath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlasWithOptions(externalSchemaSecretsProject("data.composite_schema.all.url"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.HasLen, 1)
	composite, ok := cfg.CompositeSchema(cfg.SchemaSources[0])
	c.Assert(ok, qt.IsTrue)
	c.Assert(composite.Parts, qt.HasLen, 1)
	c.Assert(composite.Parts[0].ExternalSchema, qt.IsNotNil)
	c.Assert(composite.Parts[0].ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
}

// An external_schema program carries what the project holds secret and
// nothing else: the values of a sensitive map, not the names of its items, and
// not the argument of a format() that takes its arguments by index, which may
// reach a password but is not known to.
func TestParseAtlas_ExternalSchemaCarriesOnlyItsSecrets_HappyPath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "db" {
  type      = map(string)
  sensitive = true
  default   = { username = "app", password = "S3cretPassQ" }
}

variable "name" {
  type    = string
  default = "x"
}

data "external_schema" "app" {
  program = ["./export.sh"]
}

env "dev" {
  src = data.external_schema.app.url
  url = format("sqlite://%[1]s.db", var.name)
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"name=inventory"}})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Secrets, qt.DeepEquals, []string{"S3cretPassQ", "app"})
}

// objectLocalProject is a project whose local db is an object holding a
// password beside a user, a host, a database name and a label, with env "dev"
// writing them into its url and passing field, a traversal of local.db, to a
// call that fails. The user and the host are --var values, so a reading of
// the whole object would find them.
func objectLocalProject(field string) []byte {
	return []byte(`variable "pw" {
  type    = string
  default = "x"
}

variable "user" {
  type    = string
  default = "x"
}

variable "host" {
  type    = string
  default = "x"
}

locals {
  db = {
    user     = var.user
    host     = var.host
    name     = "appdb"
    label    = "password"
    password = var.pw
  }
}

env "dev" {
  url = "postgres://${local.db.user}:${local.db.password}@${local.db.host}:5432/${local.db.name}?application_name=${tonumber(` + field + `)}"
}
`)
}

// The control for a local that is an object: only the item the password slot
// names is a secret. The user, the host, the database name, a value that is
// the word "password", and the names of the items keep their text.
func TestParseAtlas_KeepsWhatAnObjectLocalHoldsBesideAPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  string
	}{
		{name: "the user", field: "local.db.user", want: `cannot convert "appuser" to number`},
		{name: "the host", field: "local.db.host", want: `cannot convert "dbhost.internal" to number`},
		{name: "the database name", field: "local.db.name", want: `cannot convert "appdb" to number`},
		{name: "the word password", field: "local.db.label", want: `cannot convert "password" to number`},
		{name: "a key the file writes as an index", field: `local.db["label"]`, want: `cannot convert "password" to number`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(objectLocalProject(test.field), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9", "user=appuser", "host=dbhost.internal"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The item of an object local that the password slot names is masked when a
// call on it fails.
func TestParseAtlas_MasksThePasswordAnObjectLocalHolds_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := projectconfig.ParseAtlasWithOptions(objectLocalProject("local.db.password"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A value a for_each env gives each instance through each is read in the
// instance's context, though the error is masked after the instance is gone.
func TestParseAtlas_MasksAValueEachGivesAPassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_EACH_VALUE", "Gh7eachValX9")
	raw := []byte(`env {
  for_each = toset([getenv("PTAH_TEST_EACH_VALUE")])
  name     = atlas.env
  url      = "postgres://app:${tonumber(each.value)}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "dev"})

	c.Assert(err, qt.ErrorMatches, `schemahcl: evaluate env block instance 1: cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Gh7eachValX9")
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// recordedSecretsProject is a project with the sensitive variable token, the
// sensitive map db whose items hold a user name and a password, the variables
// pw, name and base, and a local that writes a user info around a conversion
// of pw, with env "dev" taking url and env "other", which a command for "dev"
// does not evaluate, taking other.
func recordedSecretsProject(url, other string) []byte {
	return []byte(`variable "token" {
  type      = string
  default   = "x"
  sensitive = true
}

variable "db" {
  type      = map(string)
  sensitive = true
  default   = { username = "app", password = "S3cretPassQ" }
}

variable "pw" {
  type    = string
  default = "x"
}

variable "name" {
  type    = string
  default = "x"
}

variable "base" {
  type    = string
  default = "x"
}

locals {
  ui = "app:${tonumber(var.pw)}"
}

env "dev" {
  url = ` + url + `
}

env "other" {
  url = ` + other + `
}
`)
}

// The command that loads a project masks its later errors against what the
// project holds secret: a sensitive value a data "hcl_schema" block passes to
// its schema file is quoted by the schema file's parser, which runs long after
// the project's own parser is gone. What it holds secret is a sensitive value,
// the value of an item of a sensitive map, and what reaches a password, of an
// address the file writes or of one a value gives it. A password the file
// writes through a format() argument or a local is known to be one though it
// does not evaluate, in an env the command does not evaluate too.
func TestParseAtlas_RecordsItsSecretsForTheCommand_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		url   string
		other string
		vars  []string
		text  string
		want  string
	}{
		{
			name:  "a sensitive value",
			url:   `"sqlite://file?mode=memory"`,
			other: `"sqlite://other?mode=memory"`,
			vars:  []string{"token=Tok3nValueX9"},
			text:  `cannot convert "Tok3nValueX9" to number`,
			want:  `cannot convert "***" to number`,
		},
		{
			name:  "the value of an item of a sensitive map",
			url:   `"sqlite://file?mode=memory"`,
			other: `"sqlite://other?mode=memory"`,
			text:  `denied for S3cretPassQ`,
			want:  `denied for ***`,
		},
		{
			name:  "a password the address takes",
			url:   `"postgres://app:${var.pw}@127.0.0.1:1/db?sslmode=disable"`,
			other: `"sqlite://other?mode=memory"`,
			vars:  []string{"pw=Pa55wordX9"},
			text:  `cannot convert "Pa55wordX9" to number`,
			want:  `cannot convert "***" to number`,
		},
		{
			name:  "a password after an address a value gives",
			url:   `"${var.base}&password=${var.pw}"`,
			other: `"sqlite://other?mode=memory"`,
			vars:  []string{"pw=Pa55wordX9", "base=postgres://app@127.0.0.1:1/app?sslmode=disable"},
			text:  `cannot convert "Pa55wordX9" to number`,
			want:  `cannot convert "***" to number`,
		},
		{
			name:  "the password of an address a value gives",
			url:   `"${var.base}&application_name=x"`,
			other: `"sqlite://other?mode=memory"`,
			vars:  []string{"base=postgres://app:Bas3PassX9@127.0.0.1:1/app?sslmode=disable"},
			text:  `cannot convert "Bas3PassX9" to number`,
			want:  `cannot convert "***" to number`,
		},
		{
			name:  "a password a format() argument writes in a user info",
			url:   `"sqlite://file?mode=memory"`,
			other: `format("postgres://%s@127.0.0.1:1/db", "app:${tonumber(var.pw)}")`,
			vars:  []string{"pw=Pa55wordX9"},
			text:  `cannot convert "Pa55wordX9" to number`,
			want:  `cannot convert "***" to number`,
		},
		{
			name:  "a password a local writes in a user info",
			url:   `"sqlite://file?mode=memory"`,
			other: `"postgres://${local.ui}@127.0.0.1:1/db"`,
			vars:  []string{"pw=Pa55wordX9"},
			text:  `cannot convert "Pa55wordX9" to number`,
			want:  `cannot convert "***" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := dburldisplay.WithErrorRedaction(context.Background())

			_, err := projectconfig.ParseAtlasWithOptions(recordedSecretsProject(test.url, test.other), "atlas.hcl",
				projectconfig.AtlasLoadOptions{Context: ctx, EnvName: "dev", Vars: test.vars})

			c.Assert(err, qt.IsNil)
			c.Assert(dburldisplay.RedactRecordedText(ctx, test.text), qt.Equals, test.want)
		})
	}
}

// The control for the test above: the command masks its later errors against
// what the project holds secret and nothing else. The name of an item of a
// sensitive map names a value and is not one. A value whose place in an
// address is unknown may reach a password but is not known to: the argument of
// a format() that takes its arguments by index, the value urlqueryset() sets
// under a key the file does not write out, and the value of a whole user info
// that does not evaluate. So a server's line and a directory name that hold
// one keep their text.
func TestParseAtlas_RecordsNothingTheProjectDoesNotHoldSecret_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		url   string
		other string
		text  string
	}{
		{
			name:  "the name of an item of a sensitive map in a server's line",
			url:   `"sqlite://file?mode=memory"`,
			other: `"sqlite://other?mode=memory"`,
			text:  `FATAL: password authentication failed for user app`,
		},
		{
			name:  "the name of an item of a sensitive map in a directory name",
			url:   `"sqlite://file?mode=memory"`,
			other: `"sqlite://other?mode=memory"`,
			text:  `openat username-migrations: no such file or directory`,
		},
		{
			name:  "the argument of a format() that takes it by index",
			url:   `format("sqlite://%[1]s.db", var.name)`,
			other: `"sqlite://other?mode=memory"`,
			text:  `openat inventory-migrations: no such file or directory`,
		},
		{
			name:  "the value urlqueryset() sets under a key the file does not write out",
			url:   `urlqueryset("sqlite://file?mode=memory", lower("X"), var.name)`,
			other: `"sqlite://other?mode=memory"`,
			text:  `openat inventory-migrations: no such file or directory`,
		},
		{
			name:  "the value of a whole user info that does not evaluate",
			url:   `"sqlite://file?mode=memory"`,
			other: `"postgres://${tonumber(var.name)}@127.0.0.1:1/db"`,
			text:  `openat inventory-migrations: no such file or directory`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := dburldisplay.WithErrorRedaction(context.Background())

			_, err := projectconfig.ParseAtlasWithOptions(recordedSecretsProject(test.url, test.other), "atlas.hcl",
				projectconfig.AtlasLoadOptions{Context: ctx, EnvName: "dev", Vars: []string{"name=inventory"}})

			c.Assert(err, qt.IsNil)
			c.Assert(dburldisplay.RedactRecordedText(ctx, test.text), qt.Equals, test.text)
		})
	}
}

// countingFS is a project directory that counts the opens of each file. It
// offers Open alone, so every read of a file opens it.
type countingFS struct {
	files fstest.MapFS
	opens map[string]int
}

func (f countingFS) Open(name string) (fs.File, error) {
	f.opens[name]++
	return f.files.Open(name)
}

// instancesProject is a project whose env "app" a for_each expands to one
// instance for each of count names, each reading its desired state from a data
// "external_schema" program and the password of its url and of its dev from
// a file named for it: the url writes it after an address base.txt gives, and
// the dev writes it in an address of its own. Its env "other", which no
// for_each expands, reads the password of its url from other.txt. It returns
// the project and its directory.
func instancesProject(count int) ([]byte, countingFS) {
	dir := countingFS{
		files: fstest.MapFS{
			"other.txt": {Data: []byte("Oth3rPassX9")},
			"base.txt":  {Data: []byte("postgres://app@127.0.0.1:1/app?sslmode=disable")},
		},
		opens: make(map[string]int),
	}
	names := make([]string, 0, count)
	for i := range count {
		name := fmt.Sprintf("n%02d", i)
		names = append(names, `"`+name+`"`)
		dir.files["pw-"+name+".txt"] = &fstest.MapFile{Data: []byte("Pa55wordX9")}
	}
	raw := `data "external_schema" "app" {
  program = ["./export.sh"]
}

env "app" {
  for_each = toset([` + strings.Join(names, ", ") + `])
  src      = data.external_schema.app.url
  url      = "${file("base.txt")}&password=${file("pw-${each.value}.txt")}"
  dev      = "postgres://app:${file("pw-${each.value}.txt")}@127.0.0.1:1/${each.value}?sslmode=disable"
}

env "other" {
  url = "postgres://app:${file("other.txt")}@127.0.0.1:1/other?sslmode=disable"
}
`
	return []byte(raw), dir
}

// Reading what a project holds secret evaluates each slot of an address once
// for each context the slot evaluates in, after every instance is expanded,
// rather than for each instance in the context of every instance before it;
// and a slot after an address a value gives is evaluated in the context that
// gave the address alone. So the file the password of an instance comes from
// is opened as often when a for_each expands one instance as when it expands
// twelve, the last instance's as often as the first's, and the file of an env
// no for_each expands as often too.
func TestParseAtlasFSCollection_EvaluatesEachSlotOncePerInstance_HappyPath(t *testing.T) {
	c := qt.New(t)
	oneRaw, one := instancesProject(1)
	manyRaw, many := instancesProject(12)
	opts := projectconfig.AtlasLoadOptions{EnvName: "app"}

	oneConfigs, oneErr := projectconfig.ParseAtlasFSCollectionWithOptions(oneRaw, "atlas.hcl", one, opts)
	manyConfigs, manyErr := projectconfig.ParseAtlasFSCollectionWithOptions(manyRaw, "atlas.hcl", many, opts)

	c.Assert(oneErr, qt.IsNil)
	c.Assert(manyErr, qt.IsNil)
	c.Assert(oneConfigs, qt.HasLen, 1)
	c.Assert(manyConfigs, qt.HasLen, 12)
	c.Assert(manyConfigs[11].ExternalSchema.Secrets, qt.Contains, "Pa55wordX9")
	c.Assert(one.opens["pw-n00.txt"], qt.Not(qt.Equals), 0)
	c.Assert(one.opens["other.txt"], qt.Not(qt.Equals), 0)
	c.Assert(many.opens["pw-n00.txt"], qt.Equals, one.opens["pw-n00.txt"])
	c.Assert(many.opens["pw-n11.txt"], qt.Equals, one.opens["pw-n00.txt"])
	c.Assert(many.opens["other.txt"], qt.Equals, one.opens["other.txt"])
}

// The control for a password read from a local the file does not write out
// item by item: a local built by merge() is read by its value, so its user
// keeps its text; and a password read from an object whole takes the values
// of its items, never their names.
func TestParseAtlas_KeepsWhatALocalReadByItsValueHolds_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		locals string
		url    string
		want   string
	}{
		{
			name:   "the user of a local merge() builds",
			locals: `db = merge({ user = lower("APPUSER") }, { password = var.pw })`,
			url:    `"postgres://${local.db.user}:${local.db.password}@127.0.0.1:1/db?application_name=${tonumber(local.db.user)}"`,
			want:   `cannot convert "appuser" to number`,
		},
		{
			name: "the user of a local whose password is read by a key the file does not write out",
			locals: `db = merge({ user = "appuser" }, { password = var.pw })
  k  = "password"`,
			url:  `"postgres://${local.db.user}:${local.db[local.k]}@127.0.0.1:1/db?application_name=${tonumber(local.db.user)}"`,
			want: `cannot convert "appuser" to number`,
		},
		{
			name: "the name of an item of an object read whole",
			locals: `db   = { username = "appuser", password = var.pw }
  word = "username"`,
			url:  `"postgres://app:${join("", values(local.db))}@127.0.0.1:1/db?application_name=${tonumber(local.word)}"`,
			want: `cannot convert "username" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "pw" {
  type    = string
  default = "x"
}

locals {
  ` + test.locals + `
}

env "dev" {
  url = ` + test.url + `
}
`)

			got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// callAddressProject is a project whose env "dev" takes url and the lines of
// env after it, with the variables pw, host and base, the list variable head,
// and the blocks of preamble before the env.
func callAddressProject(preamble, url, env string) []byte {
	return []byte(`variable "pw" {
  type    = string
  default = "x"
}

variable "host" {
  type    = string
  default = "x"
}

variable "base" {
  type    = string
  default = "postgres://app@127.0.0.1:1/db?sslmode=disable"
}

variable "head" {
  type    = list(string)
  default = ["postgres://app:"]
}

` + preamble + `
env "dev" {
  url = ` + url + `
  ` + env + `
}
`)
}

// An address a call writes that the masking does not read, and that fails,
// gives every value it holds as one that may be a secret: the call's value
// would have said which part of the address each value fills, and the error is
// raised where it has none. So the password is masked however the address is
// assembled, and the host such an address writes is masked too, which is the
// cost of not reading the call.
func TestParseAtlas_MasksWhatAnAddressACallWritesHolds_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		preamble string
		url      string
		vars     []string
	}{
		{
			name: "a password a join() over a concat() writes",
			url:  `join("", concat(["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db"]))`,
		},
		{
			name: "a password a join() over a flatten() writes",
			url:  `join("", flatten([["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db"]]))`,
		},
		{
			name: "a password a join() over a variable list writes",
			url:  `join("", concat(var.head, [tonumber(var.pw)], ["@127.0.0.1:1/db"]))`,
		},
		{
			name: "a password a join() over a concat() writes from a string index of a variable",
			url:  `join("", concat(["postgres://app:"], [tonumber(var["pw"])], ["@127.0.0.1:1/db"]))`,
		},
		{
			name:     "a password a join() over a local list a string index names writes",
			preamble: "locals {\n  parts = [\"postgres://app:\", tonumber(var.pw), \"@127.0.0.1:1/db\"]\n}\n",
			url:      `join("", local["parts"])`,
		},
		{
			name:     "a password a join() over a local list writes",
			preamble: "locals {\n  parts = [\"postgres://app:\", tonumber(var.pw), \"@127.0.0.1:1/db\"]\n}\n",
			url:      `join("", local.parts)`,
		},
		{
			name: "a password after an address a failing call was to give",
			url:  `"${trimsuffix(var.base, tonumber(var.pw))}&password=${var.pw}"`,
		},
		{
			name: "a password parameter a join() over a concat() writes in the query",
			url:  `"postgres://app@127.0.0.1:1/db?${join("", concat(["password="], [tonumber(var.pw)]))}"`,
		},
		{
			name: "a host a join() over a concat() writes",
			url:  `join("", concat(["postgres://app:x@"], [tonumber(var.host)], ["/db"]))`,
		},
		{
			name:     "a password between the head and the tail of an address two variables give",
			preamble: "variable \"prefix\" {\n  default = \"postgres://app:\"\n}\nvariable \"suffix\" {\n  default = \"@127.0.0.1:1/db\"\n}\n",
			url:      `"${var.prefix}${tonumber(var.pw)}${var.suffix}"`,
		},
		{
			name:     "a password before a tail a failing call was to give",
			preamble: "variable \"prefix\" {\n  default = \"postgres://app:\"\n}\nvariable \"suffix\" {\n  default = \"@127.0.0.1:1/db\"\n}\n",
			url:      `"${var.prefix}${tonumber(var.pw)}${tonumber(var.suffix)}"`,
		},
		{
			name:     "a password a join() over the elements of a variable list writes",
			preamble: "variable \"parts\" {\n  type    = list(string)\n  default = [\"x\"]\n}\n",
			url:      `join("", [var.parts[0], tonumber(var.parts[1]), var.parts[2]])`,
			vars:     []string{"parts=postgres://app:", "parts=Pa55wordX9", "parts=@127.0.0.1:1/db"},
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name:     "a password replace() puts in an address a variable holds",
			preamble: "variable \"tpl\" {\n  default = \"postgres://app:PW@127.0.0.1:1/db\"\n}\n",
			url:      `replace(var.tpl, "PW", tonumber(var.pw))`,
		},
		{
			name: "a password a join() writes after a scheme two strings split",
			url:  `join("", concat(["postgres:"], ["//app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db"]))`,
		},
		{
			name: "a password a join() over a for expression writes",
			url:  `join("", [for p in ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"] : p])`,
		},
		{
			name: "a password a for directive writes",
			url:  `"%{ for p in ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"] }${p}%{ endfor }"`,
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name:     "a password a format() writes with a format a variable gives",
			preamble: "variable \"fmt\" {\n  default = \"postgres://app:%s@127.0.0.1:1/db\"\n}\n",
			url:      `format(var.fmt, tonumber(var.pw))`,
		},
		{
			name:     "a password a format() writes with a verb a variable gives",
			preamble: "variable \"verb\" {\n  default = \"s\"\n}\n",
			url:      `format("postgres://app:%${var.verb}@127.0.0.1:1/db", tonumber(var.pw))`,
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name: "a password a formatlist() writes",
			url:  `join("", formatlist("postgres://app:%s@127.0.0.1:1/db", [tonumber(var.pw)]))`,
		},
		{
			name: "a password a coalescelist() passes on",
			url:  `join("", coalescelist(["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"], []))`,
		},
		{
			name: "a password a try() passes on",
			url:  `"postgres://app:${try(tonumber(var.pw))}@127.0.0.1:1/db"`,
		},
		{
			name: "a password strrev() turns around",
			url:  `"postgres://app:${tonumber(strrev(var.pw))}@127.0.0.1:1/db"`,
		},
		{
			name: "a password upper() changes",
			url:  `"postgres://app:${tonumber(upper(var.pw))}@127.0.0.1:1/db"`,
		},
		{
			name:     "a password element() takes out of a split()",
			preamble: "variable \"pws\" {\n  default = \"x;y\"\n}\n",
			url:      `"postgres://app:${tonumber(element(split(";", var.pws), 0))}@127.0.0.1:1/db"`,
			vars:     []string{"pws=Pa55wordX9;other"},
		},
		{
			name:     "a password a chain of locals gives",
			preamble: "locals {\n  a = var.pw\n  b = local.a\n}\n",
			url:      `"postgres://app:${tonumber(local.b)}@127.0.0.1:1/db"`,
		},
		{
			name: "a password parameter urlqueryset() sets",
			url:  `urlqueryset("postgres://app@127.0.0.1:1/db", "password", tonumber(var.pw))`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(callAddressProject(test.preamble, test.url, ""), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: append([]string{"pw=Pa55wordX9", "host=dbhost.internal"}, test.vars...)})

			c.Assert(err, qt.ErrorMatches, `(?s)cannot evaluate atlas\.hcl ".*`)
			c.Assert(err.Error(), qt.Contains, `cannot convert "redacted" to number`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "Pa55wordX9")
			c.Assert(err.Error(), qt.Not(qt.Contains), "dbhost.internal")
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The control for the test above: a call the masking does not read keeps the
// text of what it holds where it converts one value in a place the masking
// reads, where it writes no address, and where it writes text beside the
// scheme and evaluates, so a value an address takes is read by where it stands
// rather than masked for being near a call.
func TestParseAtlas_KeepsWhatACallThatWritesNoAddressPartHolds_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		env  string
		want string
	}{
		{
			name: "a value two calls convert in a query parameter",
			url:  `"postgres://app@127.0.0.1:1/db?application_name=${tonumber(lower(var.pw))}"`,
			want: `cannot convert "visib1evalue" to number`,
		},
		{
			name: "a value a call converts in an address a join() writes out",
			url:  `join("", ["postgres://app@127.0.0.1:1/db?connect_timeout=", tonumber(var.pw)])`,
			want: `cannot convert "Visib1eValue" to number`,
		},
		{
			name: "a value a join() over a concat() writes beside no scheme",
			url:  `join("", concat(["db-"], [tonumber(var.pw)]))`,
			want: `cannot convert "Visib1eValue" to number`,
		},
		{
			name: "a value another attribute writes beside the scheme through a call that evaluates",
			url:  `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			env:  `dev = "db-${trimprefix(var.pw, "postgres://")}"`,
			want: `cannot convert "Visib1eValue" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(callAddressProject("", test.url, test.env), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Visib1eValue"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// A value too short to take out of every word of an error is taken out where
// the diagnostic of a call that refused it quotes it whole, which is where an
// address that a call the masking does not read writes puts it. A value equal
// to the database the address names is one of them.
func TestParseAtlas_MasksAShortValueAFailingCallQuotes_FailurePath(t *testing.T) {
	const url = `join("", concat(["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db"]))`
	for _, password := range []string{"x", "ab", "db"} {
		t.Run(password, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(callAddressProject("", url, ""), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=" + password}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, `cannot convert "redacted" to number`)
			c.Assert(err.Error(), qt.Not(qt.Contains), `"`+password+`"`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The control for the test above: a short value in a place of the address that
// holds no secret is quoted as it is.
func TestParseAtlas_KeepsAShortValueOfAPlaceThatIsNoSecret_FailurePath(t *testing.T) {
	c := qt.New(t)
	const url = `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`

	got, err := projectconfig.ParseAtlasWithOptions(callAddressProject("", url, ""), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=ab"}})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
	c.Assert(err.Error(), qt.Contains, `cannot convert "ab" to number`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A for expression gives the parser a scope node among the nodes it reads for
// secrets. The address it helps write evaluates, and the file is read whole,
// with the for in a template, in the password, in a for directive and in an
// object key.
func TestParseAtlas_ReadsAnAddressAForExpressionWrites_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{ // #nosec G101 -- a made-up value, not a credential
			name: "a for expression in a template",
			url:  `"postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=${join(",", [for s in ["a", "b"] : s])}"`,
			want: "postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=a,b",
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name: "a for expression in the password",
			url:  `"postgres://app:${join("", [for s in ["p", "w"] : s])}@127.0.0.1:1/db"`,
			want: "postgres://app:pw@127.0.0.1:1/db",
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name: "a for directive",
			url:  `"postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=%{ for s in ["a", "b"] }${s}%{ endfor }"`,
			want: "postgres://app:pw@127.0.0.1:1/db?sslmode=disable&search_path=ab",
		},
		{ // #nosec G101 -- a made-up value, not a credential
			name: "a for expression in an object key",
			url:  `"postgres://app:pw@127.0.0.1:1/db?search_path=${{ (join("", [for s in ["k"] : s])) = "a" }["k"]}"`,
			want: "postgres://app:pw@127.0.0.1:1/db?search_path=a",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(callAddressProject("", test.url, ""), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev"})

			c.Assert(err, qt.IsNil)
			c.Assert(got.DatabaseURL, qt.Equals, test.want)
		})
	}
}

// A short value is taken out where the HCL diagnostic quotes it, and only
// there: Ptah's words before the diagnostic quote the attribute's name, which
// keeps its text when a sensitive variable is spelled like it.
func TestParseAtlas_KeepsTheNameOfAnAttributeAShortSensitiveValueSpells_FailurePath(t *testing.T) {
	c := qt.New(t)
	preamble := "variable \"token\" {\n  sensitive = true\n  default   = \"x\"\n}\n"

	got, err := projectconfig.ParseAtlasWithOptions(
		callAddressProject(preamble, `join("", concat(["postgres://app:"], [tonumber(var.pw)], ["@127.0.0.1:1/db"]))`, ""),
		"atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=url", "pw=ab"}})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" at atlas\.hcl:\d+: .*`)
	c.Assert(err.Error(), qt.Contains, `cannot convert "redacted" to number`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}
